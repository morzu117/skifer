from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from skifer.lineage.tracker import LineageEdge, LineageGraph
from skifer.observability.checks import CheckResult, CheckStatus, NullCheck, UniqueCheck
from skifer.observability.metadata_store import ColumnRecord, DatasetRecord
from skifer.observability.openlineage import (
    COLUMN_LINEAGE_FACET_URL,
    DATA_QUALITY_ASSERTIONS_FACET_URL,
    PRODUCER,
    RUN_EVENT_SCHEMA_URL,
    SCHEMA_FACET_URL,
    SKIFER_FACET_URL,
    build_run_event,
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
