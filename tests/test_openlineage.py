from __future__ import annotations

import http.server
import json
import socket
import threading
import warnings
from contextlib import closing
from datetime import datetime, timezone
from unittest import mock

import pytest

from skifer.core.config import LineageConfig
from skifer.core.schema_loader import parse_schema
from skifer.lineage.tracker import LineageEdge, LineageGraph
from skifer.observability.checks import CheckResult, CheckStatus, NullCheck, UniqueCheck
from skifer.observability.metadata_index import index_schema
from skifer.observability.metadata_store import ColumnRecord, DatasetRecord
from skifer.observability.openlineage import (
    COLUMN_LINEAGE_FACET_URL,
    DATA_QUALITY_ASSERTIONS_FACET_URL,
    PRODUCER,
    RUN_EVENT_SCHEMA_URL,
    SCHEMA_FACET_URL,
    SKIFER_FACET_URL,
    HttpEmitter,
    InMemoryEmitter,
    NoOpEmitter,
    build_run_event,
    create_lineage_emitter,
)

JOB_NAMESPACE = "skifer"
DATASET_NAMESPACE = "unitycatalog://workspace"
TARGET_FQN = "gold.fact_orders"
EVENT_TIME = datetime(2026, 9, 13, 10, 0, 0, tzinfo=timezone.utc)


def _record(*, columns, edges, contract_version=None, data_product_id=None) -> DatasetRecord:
    graph = LineageGraph()
    for edge in edges:
        graph.add_edge(edge)
    return DatasetRecord(
        target_fqn=TARGET_FQN,
        pipeline_path="schemas/gold/fact_orders.yaml",
        data_product_id=data_product_id,
        contract_version=contract_version,
        definition_hash="sha256:test-definition",
        owner="data-platform",
        columns=tuple(columns),
        indexed_at=datetime(2026, 9, 13, 9, 0, tzinfo=timezone.utc),
        last_run_id=None,
        lineage=graph.to_dict(),
    )


def _golden_record() -> DatasetRecord:
    columns = [
        ColumnRecord(name="order_id", logical_type="identifier", classification="internal"),
        ColumnRecord(name="amount_eur", logical_type="double"),
        ColumnRecord(name="customer_region", classification="confidential"),
        ColumnRecord(name="is_high_value"),
    ]
    edges = [
        LineageEdge(
            source_table="silver.orders",
            source_column="id",
            target_table=TARGET_FQN,
            target_column="order_id",
            transformations=[],
            edge_type="select",
        ),
        LineageEdge(
            source_table="silver.orders",
            source_column="amount",
            target_table=TARGET_FQN,
            target_column="amount_eur",
            transformations=["cast:double"],
            edge_type="select",
        ),
        LineageEdge(
            source_table="silver.customers",
            source_column="region",
            target_table=TARGET_FQN,
            target_column="customer_region",
            transformations=[],
            edge_type="join",
        ),
        LineageEdge(
            source_table="silver.orders",
            source_column="amount",
            target_table=TARGET_FQN,
            target_column="is_high_value",
            transformations=["rule:flag_high_value"],
            edge_type="rule",
        ),
    ]
    return _record(
        columns=columns,
        edges=edges,
        contract_version="1.0.0",
        data_product_id="sales.orders",
    )


def test_golden_event_select_cast_join_and_rule():
    record = _golden_record()

    event = build_run_event(
        event_type="COMPLETE",
        run_id="run-123",
        event_time=EVENT_TIME,
        record=record,
        job_namespace=JOB_NAMESPACE,
        dataset_namespace=DATASET_NAMESPACE,
        certification_status="CERTIFIED",
    )

    assert event == {
        "eventTime": "2026-09-13T10:00:00Z",
        "eventType": "COMPLETE",
        "producer": PRODUCER,
        "schemaURL": RUN_EVENT_SCHEMA_URL,
        "run": {"runId": "run-123"},
        "job": {"namespace": JOB_NAMESPACE, "name": TARGET_FQN},
        "inputs": [
            {"namespace": DATASET_NAMESPACE, "name": "silver.customers"},
            {"namespace": DATASET_NAMESPACE, "name": "silver.orders"},
        ],
        "outputs": [
            {
                "namespace": DATASET_NAMESPACE,
                "name": TARGET_FQN,
                "facets": {
                    "schema": {
                        "_producer": PRODUCER,
                        "_schemaURL": SCHEMA_FACET_URL,
                        "fields": [
                            {"name": "order_id", "type": "identifier"},
                            {"name": "amount_eur", "type": "double"},
                            {"name": "customer_region"},
                            {"name": "is_high_value"},
                        ],
                    },
                    "columnLineage": {
                        "_producer": PRODUCER,
                        "_schemaURL": COLUMN_LINEAGE_FACET_URL,
                        "fields": {
                            "order_id": {
                                "inputFields": [
                                    {
                                        "namespace": DATASET_NAMESPACE,
                                        "name": "silver.orders",
                                        "field": "id",
                                        "transformations": [
                                            {"type": "DIRECT", "subtype": "IDENTITY"}
                                        ],
                                    }
                                ]
                            },
                            "amount_eur": {
                                "inputFields": [
                                    {
                                        "namespace": DATASET_NAMESPACE,
                                        "name": "silver.orders",
                                        "field": "amount",
                                        "transformations": [
                                            {
                                                "type": "DIRECT",
                                                "subtype": "TRANSFORMATION",
                                                "description": "cast",
                                            }
                                        ],
                                    }
                                ]
                            },
                            "customer_region": {
                                "inputFields": [
                                    {
                                        "namespace": DATASET_NAMESPACE,
                                        "name": "silver.customers",
                                        "field": "region",
                                        "transformations": [
                                            {"type": "INDIRECT", "subtype": "JOIN"}
                                        ],
                                    }
                                ]
                            },
                            "is_high_value": {
                                "inputFields": [
                                    {
                                        "namespace": DATASET_NAMESPACE,
                                        "name": "silver.orders",
                                        "field": "amount",
                                        "transformations": [
                                            {
                                                "type": "DIRECT",
                                                "subtype": "TRANSFORMATION",
                                                "description": "rule",
                                            }
                                        ],
                                    }
                                ]
                            },
                        },
                    },
                    "skifer": {
                        "_producer": PRODUCER,
                        "_schemaURL": SKIFER_FACET_URL,
                        "definitionHash": "sha256:test-definition",
                        "contractVersion": "1.0.0",
                        "dataProductId": "sales.orders",
                        "certification": "CERTIFIED",
                        "classifications": {
                            "order_id": "internal",
                            "customer_region": "confidential",
                        },
                    },
                },
            }
        ],
    }


def test_metric_edge_maps_to_direct_aggregation():
    record = _record(
        columns=[ColumnRecord(name="total_amount")],
        edges=[
            LineageEdge(
                source_table="gold.orders_model",
                source_column="amount",
                target_table=TARGET_FQN,
                target_column="total_amount",
                transformations=["sum"],
                edge_type="metric",
            )
        ],
    )

    event = build_run_event(
        event_type="COMPLETE",
        run_id="run-1",
        event_time=EVENT_TIME,
        record=record,
        job_namespace=JOB_NAMESPACE,
        dataset_namespace=DATASET_NAMESPACE,
    )

    transformation = event["outputs"][0]["facets"]["columnLineage"]["fields"]["total_amount"][
        "inputFields"
    ][0]["transformations"][0]
    assert transformation == {"type": "DIRECT", "subtype": "AGGREGATION", "description": "sum"}


def test_inputs_exclude_target_and_synthetic_rule_origin():
    record = _record(
        columns=[ColumnRecord(name="flag")],
        edges=[
            LineageEdge(
                source_table="<rule>",
                source_column="flag",
                target_table=TARGET_FQN,
                target_column="flag",
                transformations=[],
                edge_type="select",
            ),
            LineageEdge(
                source_table=TARGET_FQN,
                source_column="self",
                target_table=TARGET_FQN,
                target_column="flag",
                transformations=[],
                edge_type="select",
            ),
        ],
    )

    event = build_run_event(
        event_type="COMPLETE",
        run_id="run-1",
        event_time=EVENT_TIME,
        record=record,
        job_namespace=JOB_NAMESPACE,
        dataset_namespace=DATASET_NAMESPACE,
    )

    assert event["inputs"] == []


def test_rule_origin_excluded_from_column_lineage():
    record = _record(
        columns=[ColumnRecord(name="is_high_value")],
        edges=[
            LineageEdge(
                source_table="<rule>",
                source_column="is_high_value",
                target_table=TARGET_FQN,
                target_column="is_high_value",
                transformations=["rule:flag_high_value"],
                edge_type="rule",
            ),
        ],
    )

    event = build_run_event(
        event_type="COMPLETE",
        run_id="run-1",
        event_time=EVENT_TIME,
        record=record,
        job_namespace=JOB_NAMESPACE,
        dataset_namespace=DATASET_NAMESPACE,
    )

    assert "<rule>" not in json.dumps(event)
    assert event["inputs"] == []
    assert "columnLineage" not in event["outputs"][0]["facets"]


def test_literal_column_excluded_from_column_lineage_but_kept_in_schema():
    record = _record(
        columns=[ColumnRecord(name="source_system")],
        edges=[
            LineageEdge(
                source_table="silver.orders",
                source_column="<literal>",
                target_table=TARGET_FQN,
                target_column="source_system",
                transformations=["lit:ERP"],
                edge_type="select",
            ),
        ],
    )

    event = build_run_event(
        event_type="COMPLETE",
        run_id="run-1",
        event_time=EVENT_TIME,
        record=record,
        job_namespace=JOB_NAMESPACE,
        dataset_namespace=DATASET_NAMESPACE,
    )

    assert "<literal>" not in json.dumps(event)
    schema_fields = event["outputs"][0]["facets"]["schema"]["fields"]
    assert {"name": "source_system"} in schema_fields
    assert "columnLineage" not in event["outputs"][0]["facets"]


def test_unknown_column_excluded_from_column_lineage():
    record = _record(
        columns=[ColumnRecord(name="mystery")],
        edges=[
            LineageEdge(
                source_table="silver.orders",
                source_column="<unknown>",
                target_table=TARGET_FQN,
                target_column="mystery",
                transformations=[],
                edge_type="select",
            ),
        ],
    )

    event = build_run_event(
        event_type="COMPLETE",
        run_id="run-1",
        event_time=EVENT_TIME,
        record=record,
        job_namespace=JOB_NAMESPACE,
        dataset_namespace=DATASET_NAMESPACE,
    )

    assert "<unknown>" not in json.dumps(event)
    assert "columnLineage" not in event["outputs"][0]["facets"]


def test_only_synthetic_edges_means_empty_inputs_and_no_facet():
    record = _record(
        columns=[ColumnRecord(name="flag"), ColumnRecord(name="source_system")],
        edges=[
            LineageEdge(
                source_table="<rule>",
                source_column="flag",
                target_table=TARGET_FQN,
                target_column="flag",
                transformations=[],
                edge_type="rule",
            ),
            LineageEdge(
                source_table="silver.orders",
                source_column="<literal>",
                target_table=TARGET_FQN,
                target_column="source_system",
                transformations=["lit:ERP"],
                edge_type="select",
            ),
        ],
    )

    event = build_run_event(
        event_type="COMPLETE",
        run_id="run-1",
        event_time=EVENT_TIME,
        record=record,
        job_namespace=JOB_NAMESPACE,
        dataset_namespace=DATASET_NAMESPACE,
    )

    assert event["inputs"] == []
    assert "columnLineage" not in event["outputs"][0]["facets"]


def test_no_lineage_edges_means_no_column_lineage_facet():
    record = _record(columns=[ColumnRecord(name="c")], edges=[])

    event = build_run_event(
        event_type="COMPLETE",
        run_id="run-1",
        event_time=EVENT_TIME,
        record=record,
        job_namespace=JOB_NAMESPACE,
        dataset_namespace=DATASET_NAMESPACE,
    )

    assert event["inputs"] == []
    assert "columnLineage" not in event["outputs"][0]["facets"]


class TestAssertions:
    def _record_no_lineage(self):
        return _record(columns=[ColumnRecord(name="id")], edges=[])

    def test_pass_fail_error_mapped_skipped_omitted(self):
        record = self._record_no_lineage()
        results = [
            CheckResult(
                contract=NullCheck(table="silver.orders", column="id"),
                status=CheckStatus.PASS,
                severity="warning",
            ),
            CheckResult(
                contract=NullCheck(table="silver.orders", column="amount"),
                status=CheckStatus.FAIL,
                severity="critical",
            ),
            CheckResult(
                contract=NullCheck(table="silver.orders", column="amount"),
                status=CheckStatus.ERROR,
                severity="warning",
            ),
            CheckResult(
                contract=NullCheck(table="silver.orders", column="amount"),
                status=CheckStatus.SKIPPED,
                severity="warning",
            ),
        ]

        event = build_run_event(
            event_type="COMPLETE",
            run_id="run-1",
            event_time=EVENT_TIME,
            record=record,
            job_namespace=JOB_NAMESPACE,
            dataset_namespace=DATASET_NAMESPACE,
            check_results=results,
        )

        facet = event["outputs"][0]["facets"]["dataQualityAssertions"]
        assert facet["_producer"] == PRODUCER
        assert facet["_schemaURL"] == DATA_QUALITY_ASSERTIONS_FACET_URL
        assertions = facet["assertions"]
        assert len(assertions) == 3
        assert assertions[0] == {
            "assertion": "NullCheck",
            "success": True,
            "severity": "warn",
            "column": "id",
        }
        assert assertions[1] == {
            "assertion": "NullCheck",
            "success": False,
            "severity": "error",
            "column": "amount",
        }
        assert assertions[2] == {
            "assertion": "NullCheck",
            "success": False,
            "severity": "warn",
            "column": "amount",
        }

    def test_column_omitted_when_contract_has_no_column(self):
        record = self._record_no_lineage()
        results = [
            CheckResult(
                contract=UniqueCheck(table="silver.orders", columns=["id"]),
                status=CheckStatus.PASS,
                severity="warning",
            )
        ]

        event = build_run_event(
            event_type="COMPLETE",
            run_id="run-1",
            event_time=EVENT_TIME,
            record=record,
            job_namespace=JOB_NAMESPACE,
            dataset_namespace=DATASET_NAMESPACE,
            check_results=results,
        )

        assertion = event["outputs"][0]["facets"]["dataQualityAssertions"]["assertions"][0]
        assert "column" not in assertion

    def test_all_skipped_omits_facet_entirely(self):
        record = self._record_no_lineage()
        results = [
            CheckResult(
                contract=NullCheck(table="silver.orders", column="id"),
                status=CheckStatus.SKIPPED,
                severity="warning",
            )
        ]

        event = build_run_event(
            event_type="COMPLETE",
            run_id="run-1",
            event_time=EVENT_TIME,
            record=record,
            job_namespace=JOB_NAMESPACE,
            dataset_namespace=DATASET_NAMESPACE,
            check_results=results,
        )

        assert "dataQualityAssertions" not in event["outputs"][0]["facets"]

    def test_no_check_results_omits_facet(self):
        record = self._record_no_lineage()

        event = build_run_event(
            event_type="COMPLETE",
            run_id="run-1",
            event_time=EVENT_TIME,
            record=record,
            job_namespace=JOB_NAMESPACE,
            dataset_namespace=DATASET_NAMESPACE,
        )

        assert "dataQualityAssertions" not in event["outputs"][0]["facets"]


def test_canary_no_secret_leaks_into_event():
    record = _record(
        columns=[ColumnRecord(name="amount_eur"), ColumnRecord(name="flagged")],
        edges=[
            LineageEdge(
                source_table="silver.orders",
                source_column="amount",
                target_table=TARGET_FQN,
                target_column="amount_eur",
                transformations=["expr:SELECT SECRET_SQL"],
                edge_type="select",
            ),
            LineageEdge(
                source_table="silver.orders",
                source_column="amount",
                target_table=TARGET_FQN,
                target_column="flagged",
                transformations=["lit:SECRET_LITERAL"],
                edge_type="select",
            ),
        ],
    )
    check_results = [
        CheckResult(
            contract=NullCheck(table="silver.orders", column="amount"),
            status=CheckStatus.FAIL,
            actual_value="SECRET_ACTUAL",
            expected_value="SECRET_EXPECTED",
            message="SECRET_MESSAGE",
            severity="critical",
        )
    ]

    event = build_run_event(
        event_type="FAIL",
        run_id="run-1",
        event_time=EVENT_TIME,
        record=record,
        job_namespace=JOB_NAMESPACE,
        dataset_namespace=DATASET_NAMESPACE,
        check_results=check_results,
    )

    dumped = json.dumps(event)
    assert "SECRET_SQL" not in dumped
    assert "SECRET_LITERAL" not in dumped
    assert "SECRET_ACTUAL" not in dumped
    assert "SECRET_EXPECTED" not in dumped
    assert "SECRET_MESSAGE" not in dumped

    for column_lineage in event["outputs"][0]["facets"]["columnLineage"]["fields"].values():
        for input_field in column_lineage["inputFields"]:
            for transformation in input_field["transformations"]:
                description = transformation.get("description")
                if description is not None:
                    assert ":" not in description


@pytest.mark.parametrize("event_type", ["RUNNING", "ABORT", "OTHER", "", "complete"])
def test_invalid_event_type_refused(event_type):
    record = _record(columns=[ColumnRecord(name="id")], edges=[])

    with pytest.raises(ValueError, match="event_type"):
        build_run_event(
            event_type=event_type,
            run_id="run-1",
            event_time=EVENT_TIME,
            record=record,
            job_namespace=JOB_NAMESPACE,
            dataset_namespace=DATASET_NAMESPACE,
        )


def test_naive_datetime_refused():
    record = _record(columns=[ColumnRecord(name="id")], edges=[])

    with pytest.raises(ValueError, match="event_time"):
        build_run_event(
            event_type="COMPLETE",
            run_id="run-1",
            event_time=datetime(2026, 9, 13, 10, 0, 0),
            record=record,
            job_namespace=JOB_NAMESPACE,
            dataset_namespace=DATASET_NAMESPACE,
        )


@pytest.mark.parametrize(
    "kwargs, match",
    [
        ({"run_id": ""}, "run_id"),
        ({"job_namespace": ""}, "job_namespace"),
        ({"dataset_namespace": ""}, "dataset_namespace"),
    ],
)
def test_empty_identifiers_refused(kwargs, match):
    record = _record(columns=[ColumnRecord(name="id")], edges=[])
    base_kwargs = dict(
        event_type="COMPLETE",
        run_id="run-1",
        event_time=EVENT_TIME,
        record=record,
        job_namespace=JOB_NAMESPACE,
        dataset_namespace=DATASET_NAMESPACE,
    )
    base_kwargs.update(kwargs)

    with pytest.raises(ValueError, match=match):
        build_run_event(**base_kwargs)


def test_op_name_with_trailing_newline_rejected():
    record = _record(
        columns=[ColumnRecord(name="amount_eur")],
        edges=[
            LineageEdge(
                source_table="silver.orders",
                source_column="amount",
                target_table=TARGET_FQN,
                target_column="amount_eur",
                transformations=["cast\n"],
                edge_type="select",
            ),
        ],
    )

    event = build_run_event(
        event_type="COMPLETE",
        run_id="run-1",
        event_time=EVENT_TIME,
        record=record,
        job_namespace=JOB_NAMESPACE,
        dataset_namespace=DATASET_NAMESPACE,
    )

    transformation = event["outputs"][0]["facets"]["columnLineage"]["fields"]["amount_eur"][
        "inputFields"
    ][0]["transformations"][0]
    assert "description" not in transformation


def test_deterministic_json_regardless_of_edge_insertion_order():
    record_a = _golden_record()

    columns = [
        ColumnRecord(name="order_id", logical_type="identifier", classification="internal"),
        ColumnRecord(name="amount_eur", logical_type="double"),
        ColumnRecord(name="customer_region", classification="confidential"),
        ColumnRecord(name="is_high_value"),
    ]
    edges_reversed = list(
        LineageGraph.from_dict(record_a.lineage).edges
    )[::-1]
    record_b = _record(
        columns=columns,
        edges=edges_reversed,
        contract_version="1.0.0",
        data_product_id="sales.orders",
    )

    kwargs = dict(
        event_type="COMPLETE",
        run_id="run-123",
        event_time=EVENT_TIME,
        job_namespace=JOB_NAMESPACE,
        dataset_namespace=DATASET_NAMESPACE,
        certification_status="CERTIFIED",
    )

    event_a = build_run_event(record=record_a, **kwargs)
    event_b = build_run_event(record=record_b, **kwargs)
    event_a_again = build_run_event(record=record_a, **kwargs)

    assert json.dumps(event_a, sort_keys=True) == json.dumps(event_b, sort_keys=True)
    assert json.dumps(event_a, sort_keys=True) == json.dumps(event_a_again, sort_keys=True)


class TestAllowlistThroughRealTracker:
    """_is_real_edge as defense in depth: exercised through index_schema (the real
    LineageTracker), not hand-built edges — a raw, un-normalised schema dict can reach
    index_schema directly (run_process_to_table accepts any dict), so the builder must
    reject a literal shorthand copied verbatim even though the normal YAML path never
    produces it (plan 36.1 redev finding)."""

    def _event_for(self, schema: dict, target_fqn: str) -> dict:
        record = index_schema(schema, "schemas/test.yaml", target_fqn=target_fqn)
        return build_run_event(
            event_type="COMPLETE",
            run_id="run-1",
            event_time=EVENT_TIME,
            record=record,
            job_namespace=JOB_NAMESPACE,
            dataset_namespace=DATASET_NAMESPACE,
        )

    def test_raw_dict_literal_shorthand_never_leaves_the_process(self):
        schema = {
            "tables": [{"name": "silver.orders", "alias": "o"}],
            "select_final": [
                ["o.amount", "amount", ["cast:double"]],
                ["literal:ERP", "source_system"],
                ["lit:SECRETLIT", "flag"],
            ],
        }

        event = self._event_for(schema, "gold.raw_target")

        dumped = json.dumps(event)
        assert "ERP" not in dumped
        assert "SECRETLIT" not in dumped
        assert "literal:" not in dumped
        assert "lit:" not in dumped

        lineage_fields = event["outputs"][0]["facets"]["columnLineage"]["fields"]
        assert "source_system" not in lineage_fields
        assert "flag" not in lineage_fields

        schema_field_names = {f["name"] for f in event["outputs"][0]["facets"]["schema"]["fields"]}
        assert {"amount", "source_system", "flag"} <= schema_field_names

        assert lineage_fields["amount"]["inputFields"] == [
            {
                "namespace": DATASET_NAMESPACE,
                "name": "silver.orders",
                "field": "amount",
                "transformations": [
                    {"type": "DIRECT", "subtype": "TRANSFORMATION", "description": "cast"}
                ],
            }
        ]

    def test_normal_path_join_resolves_alias_to_bare_column(self):
        yaml_text = """
tables:
  - name: silver.orders
    alias: o
  - name: silver.customers
    alias: c
join:
  - table_from: [o, customer_id]
    table_to: [c, id]
select_final:
  - [o.amount, amount, [cast:double]]
  - [c.name, customer_name]
"""
        schema = parse_schema(yaml_text)

        event = self._event_for(schema, "gold.normal_target")

        lineage_fields = event["outputs"][0]["facets"]["columnLineage"]["fields"]
        assert lineage_fields["customer_name"]["inputFields"] == [
            {
                "namespace": DATASET_NAMESPACE,
                "name": "silver.customers",
                "field": "name",
                "transformations": [{"type": "DIRECT", "subtype": "IDENTITY"}],
            }
        ]
        assert event["inputs"] == [
            {"namespace": DATASET_NAMESPACE, "name": "silver.customers"},
            {"namespace": DATASET_NAMESPACE, "name": "silver.orders"},
        ]

    def test_nested_field_with_unknown_prefix_excluded(self):
        schema = {
            "tables": [{"name": "silver.orders", "alias": "o"}],
            "select_final": [
                ["address.city", "city"],
                ["o.amount", "amount"],
            ],
        }

        event = self._event_for(schema, "gold.nested_target")

        dumped = json.dumps(event)
        assert "address.city" not in dumped

        lineage_fields = event["outputs"][0]["facets"]["columnLineage"]["fields"]
        assert "city" not in lineage_fields


class _CapturingHandler(http.server.BaseHTTPRequestHandler):
    """Records the last POST it received; body/path/headers are read back in tests."""

    captured = None

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)
        type(self).captured = {"path": self.path, "headers": self.headers, "body": body}
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b"{}")

    def log_message(self, format, *args):  # noqa: A002 - stdlib signature
        pass


class _FailingHandler(http.server.BaseHTTPRequestHandler):
    """Always answers with a server error, to exercise the emitter's failure path."""

    def do_POST(self):
        self.send_response(500)
        self.end_headers()

    def log_message(self, format, *args):  # noqa: A002 - stdlib signature
        pass


def _run_server(handler_class):
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler_class)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


def _closed_port() -> int:
    """A local TCP port nothing listens on, for an unreachable-url test."""
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture
def http_server():
    _CapturingHandler.captured = None
    server, thread = _run_server(_CapturingHandler)
    yield server
    server.shutdown()
    server.server_close()
    thread.join(timeout=5)


@pytest.fixture
def failing_http_server():
    server, thread = _run_server(_FailingHandler)
    yield server
    server.shutdown()
    server.server_close()
    thread.join(timeout=5)


class TestNoOpEmitter:
    def test_emit_returns_none_and_keeps_no_reference(self):
        emitter = NoOpEmitter()
        event = {"eventType": "COMPLETE"}

        assert emitter.emit(event) is None
        with pytest.raises(AttributeError):
            emitter.__dict__

    def test_holds_no_state_via_slots(self):
        assert NoOpEmitter.__slots__ == ()


class TestInMemoryEmitter:
    def test_stores_independent_deep_copies(self):
        emitter = InMemoryEmitter()
        event = {"outputs": [{"name": "gold.t"}]}

        emitter.emit(event)
        event["outputs"][0]["name"] = "mutated-after-emit"

        assert emitter.events == [{"outputs": [{"name": "gold.t"}]}]
        assert emitter.events[0] is not event
        assert emitter.events[0]["outputs"] is not event["outputs"]


class TestHttpEmitterSuccess:
    def test_posts_event_body_path_and_content_type(self, http_server):
        host, port = http_server.server_address
        emitter = HttpEmitter(f"http://{host}:{port}", endpoint="/api/v1/lineage")
        event = {"eventType": "COMPLETE", "run": {"runId": "abc-123"}}

        emitter.emit(event)

        captured = _CapturingHandler.captured
        assert captured is not None
        assert captured["path"] == "/api/v1/lineage"
        assert json.loads(captured["body"]) == event
        assert captured["headers"].get("Content-Type") == "application/json"
        assert "Authorization" not in captured["headers"]

    def test_bearer_header_present_when_api_key_set(self, http_server):
        host, port = http_server.server_address
        emitter = HttpEmitter(
            f"http://{host}:{port}", environ={"OPENLINEAGE_API_KEY": "k"}
        )

        emitter.emit({"eventType": "COMPLETE"})

        assert _CapturingHandler.captured["headers"].get("Authorization") == "Bearer k"

    def test_bearer_header_absent_when_api_key_missing(self, http_server):
        host, port = http_server.server_address
        emitter = HttpEmitter(f"http://{host}:{port}", environ={})

        emitter.emit({"eventType": "COMPLETE"})

        assert "Authorization" not in _CapturingHandler.captured["headers"]

    def test_bearer_header_absent_when_api_key_empty(self, http_server):
        host, port = http_server.server_address
        emitter = HttpEmitter(
            f"http://{host}:{port}", environ={"OPENLINEAGE_API_KEY": ""}
        )

        emitter.emit({"eventType": "COMPLETE"})

        assert "Authorization" not in _CapturingHandler.captured["headers"]


class TestHttpEmitterFailure:
    def test_server_error_never_raises_and_warns_once_per_kind(
        self, failing_http_server, recwarn
    ):
        host, port = failing_http_server.server_address
        emitter = HttpEmitter(f"http://{host}:{port}", timeout_seconds=2.0)

        for _ in range(3):
            emitter.emit({"eventType": "COMPLETE"})

        runtime_warnings = [w for w in recwarn.list if issubclass(w.category, RuntimeWarning)]
        assert len(runtime_warnings) == 1
        message = str(runtime_warnings[0].message)
        assert "HTTPError" in message
        assert host not in message
        assert str(port) not in message

    def test_unreachable_url_never_raises_and_warns_once_per_kind(self, recwarn):
        port = _closed_port()
        emitter = HttpEmitter(f"http://127.0.0.1:{port}", timeout_seconds=1.0)

        for _ in range(3):
            emitter.emit({"eventType": "COMPLETE"})

        runtime_warnings = [w for w in recwarn.list if issubclass(w.category, RuntimeWarning)]
        assert len(runtime_warnings) == 1
        message = str(runtime_warnings[0].message)
        assert str(port) not in message


class TestHttpEmitterRepr:
    def test_repr_never_discloses_url_or_credentials(self):
        emitter = HttpEmitter("https://user:s3cret@host")

        text = repr(emitter)

        assert text == "HttpEmitter(url=<configured>, endpoint='/api/v1/lineage')"
        assert "s3cret" not in text
        assert "user" not in text
        assert "host" not in text


class TestCreateLineageEmitter:
    def test_none_config_returns_noop(self):
        assert isinstance(create_lineage_emitter(LineageConfig()), NoOpEmitter)

    def test_http_config_returns_http_emitter_with_endpoint_and_timeout(self):
        config = LineageConfig(
            emitter="http",
            url="https://catalog.example.com",
            endpoint="/custom/endpoint",
            timeout_seconds=2.5,
        )

        emitter = create_lineage_emitter(config)

        assert isinstance(emitter, HttpEmitter)
        assert repr(emitter) == "HttpEmitter(url=<configured>, endpoint='/custom/endpoint')"
        assert emitter._timeout_seconds == 2.5

    def test_construction_error_warns_with_class_name_only_and_returns_noop(self, recwarn):
        class _BrokenConfig:
            emitter = "http"

            @property
            def url(self):
                raise RuntimeError("boom")

        emitter = create_lineage_emitter(_BrokenConfig())

        assert isinstance(emitter, NoOpEmitter)
        runtime_warnings = [w for w in recwarn.list if issubclass(w.category, RuntimeWarning)]
        assert len(runtime_warnings) == 1
        assert "RuntimeError" in str(runtime_warnings[0].message)


class TestWarningsNeverPropagateUnderWarningsAsErrors:
    """redev finding 1: warnings.warn itself raises under a warnings-as-errors filter,
    so every warning path must go through `_warn_best_effort`, not a bare `warnings.warn`."""

    def test_emit_to_unreachable_url_does_not_raise(self):
        port = _closed_port()
        emitter = HttpEmitter(f"http://127.0.0.1:{port}", timeout_seconds=1.0)

        with warnings.catch_warnings():
            warnings.simplefilter("error")
            emitter.emit({"eventType": "COMPLETE"})

    def test_emit_to_failing_server_does_not_raise(self, failing_http_server):
        host, port = failing_http_server.server_address
        emitter = HttpEmitter(f"http://{host}:{port}", timeout_seconds=2.0)

        with warnings.catch_warnings():
            warnings.simplefilter("error")
            emitter.emit({"eventType": "COMPLETE"})

    def test_create_lineage_emitter_construction_failure_does_not_raise(self, monkeypatch):
        def _raise_init(self, *args, **kwargs):
            raise RuntimeError("boom")

        monkeypatch.setattr(HttpEmitter, "__init__", _raise_init)
        config = LineageConfig(emitter="http", url="https://catalog.example.com")

        with warnings.catch_warnings():
            warnings.simplefilter("error")
            emitter = create_lineage_emitter(config)

        assert isinstance(emitter, NoOpEmitter)


class TestHttpEmitterConcurrentWarnOnce:
    """redev finding 2 (redev 2/2): the check-then-add on `_warned_kinds` is only
    race-free under a lock. A prior version of this test asserted on
    `emitter._warned_kinds` after the fact — but adding the same key to a set twice
    is idempotent, so that assertion held whether or not the lock existed; it never
    proved "at most one warning per failure kind". This test forces the race
    deterministically instead of hoping for it: `_warned_kinds` is swapped for a set
    whose `__contains__` rendezvous on a two-party barrier, so neither thread's
    membership check can complete before the other thread has started its own —
    exactly the interleaving the lock must rule out. `_warn_best_effort` is patched
    with a counting stub so the outcome is "how many times it was actually called",
    not a state snapshot a lock-free implementation could still satisfy."""

    def test_two_concurrent_failures_call_warn_best_effort_exactly_once(self):
        class _RendezvousSet(set):
            """A `set` whose `in` check rendezvous with a sibling thread's check
            before returning, so two concurrent `_warn_once` calls are forced to
            both be inside the (unlocked) critical section at once — or, under the
            real lock, forced to serialize because the second thread cannot even
            reach its `__contains__` call until the first has released the lock."""

            def __init__(self, barrier: threading.Barrier) -> None:
                super().__init__()
                self._barrier = barrier

            def __contains__(self, item: object) -> bool:
                present = super().__contains__(item)
                try:
                    self._barrier.wait(timeout=2)
                except threading.BrokenBarrierError:
                    pass
                return present

        class _AlwaysRaisingOpener:
            def __call__(self, request, timeout):
                raise OSError("connection refused")

        emitter = HttpEmitter("http://127.0.0.1:1", opener=_AlwaysRaisingOpener())
        barrier = threading.Barrier(2)
        emitter._warned_kinds = _RendezvousSet(barrier)

        def _worker():
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                emitter.emit({"eventType": "COMPLETE"})

        with mock.patch("skifer.observability.openlineage._warn_best_effort") as warn_mock:
            threads = [threading.Thread(target=_worker) for _ in range(2)]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=5)

        assert warn_mock.call_count == 1


class TestHttpEmitterInjectedOpenerNonSuccess:
    """redev finding 3: the explicit non-2xx branch is only reachable with an injected
    opener — the default `urllib.request.urlopen` raises `HTTPError` itself for a
    non-2xx status, so it never falls through to this branch."""

    def test_non_2xx_status_warns_once_and_does_not_raise(self):
        class _Response:
            status = 500

            def __enter__(self):
                return self

            def __exit__(self, *exc_info):
                return False

        def _opener(request, timeout):
            return _Response()

        emitter = HttpEmitter("http://example.invalid", opener=_opener)

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            emitter.emit({"eventType": "COMPLETE"})

        runtime_warnings = [w for w in caught if issubclass(w.category, RuntimeWarning)]
        assert len(runtime_warnings) == 1
        message = str(runtime_warnings[0].message)
        assert "_NonSuccessResponse" in message
        assert "example.invalid" not in message
