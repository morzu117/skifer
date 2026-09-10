"""Tests for the standalone semantic evidence envelope (Plan 29, slice 4.1)."""

import json
import os
import subprocess
import sys
import textwrap
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
import yaml

from skifer.agentic.resolver import QueryResolver, SemanticQuery
from skifer.semantic.access_policy import (
    CertificationDecision,
    ConsumerContext,
    PolicyEvaluation,
    SemanticAccessDenied,
)
from skifer.semantic.evidence import (
    EvidencePolicy,
    EvidenceRedactionError,
    MissingCertificationSnapshot,
    ExecutionResult,
    SemanticEvidence,
    SemanticExecutionError,
    SourceEvidence,
    hash_definition,
    hash_sql,
    normalize_sql,
)
from skifer.observability.certification_store import Certification
from skifer.semantic.semantic import SemanticEngine


UTC_NOW = datetime(2026, 9, 4, 12, 30, tzinfo=timezone.utc)


def _evidence(**overrides) -> SemanticEvidence:
    values = {
        "evidence_id": "request-uuid",
        "trace_id": "trace-1",
        "model_keys": ("sales.orders",),
        "metrics": (),
        "dimensions": ("region",),
        "normalized_filters": ({"column": "region", "operator": "eq", "value": "EMEA"},),
        "sources": (
            SourceEvidence(
                dataset="gold.fact_orders",
                contract_id=None,
                contract_version="v1",
                definition_hash="definition-1",
                certification_status="CERTIFIED",
                certified_at=UTC_NOW,
                load_age_seconds=None,
                data_age_seconds=None,
                certification_run_id=None,
            ),
        ),
        "policy": PolicyEvaluation(CertificationDecision.ALLOW, (), UTC_NOW),
        "sql_hash": "not-yet-computed",
        "sql_text": "SELECT secret FROM orders",
        "statement_id": None,
        "compiled_at": UTC_NOW,
        "executed_at": None,
    }
    values.update(overrides)
    return SemanticEvidence(**values)


class FakeBackend:
    def __init__(self):
        self.sql = []

    def execute_sql(self, sql):
        self.sql.append(sql)
        return {"dataframe": True}


def _semantic_engine(tmp_path, *, policy="off", extra_metrics=False, metric_sql="amount"):
    models_dir = tmp_path / "semantic_models"
    models_dir.mkdir()
    metrics = [{"name": "revenue", "sql": metric_sql, "type": "sum"}]
    if extra_metrics:
        metrics += [
            {"name": "order_count", "sql": "order_id", "type": "count"},
            {"name": "unused", "sql": "never_selected", "type": "sum"},
        ]
    (models_dir / "sales_orders.yaml").write_text(
        yaml.safe_dump(
            {
                "models": [
                    {
                        "key": "sales.orders",
                        "table": "gold.fact_orders",
                        "dimensions": [
                            {"name": "region", "sql": "region", "type": "string"}
                        ],
                        "metrics": metrics,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    (models_dir / "semantic_catalog.yaml").write_text(
        yaml.safe_dump(
            {
                "models": [
                    {
                        "key": "sales.orders",
                        "file": "sales_orders.yaml",
                        "dimensions": ["region"],
                        "metrics": [item["name"] for item in metrics],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    backend = FakeBackend()
    core = SimpleNamespace(
        db="main",
        env="DEV",
        config={"environments": {"dev": {"semantic_certification_policy": policy}}},
        _get_backend=lambda: backend,
    )
    return SemanticEngine(core, models_dir=str(models_dir)), backend


def _query():
    return SemanticQuery(
        model_name="sales.orders",
        metrics=["revenue"],
        group_by=["region"],
        filters=[{"column": "region", "operator": "eq", "value": "EMEA"}],
    )


def test_evidence_json_round_trip_is_utc_and_redacted_by_default():
    payload = _evidence().to_dict()

    assert json.loads(json.dumps(payload)) == payload
    assert payload["compiled_at"] == "2026-09-04T12:30:00+00:00"
    assert payload["sources"][0]["certified_at"] == "2026-09-04T12:30:00+00:00"
    assert "sql_text" not in payload
    assert payload["normalized_filters"] == [
        {"column": "region", "operator": "eq", "value": "<redacted>"}
    ]


def test_evidence_includes_only_requested_sql_and_filter_values():
    payload = _evidence().to_dict(include_sql=True, include_filter_values=True)

    assert payload["sql_text"] == "SELECT secret FROM orders"
    assert payload["normalized_filters"][0] == {
        "column": "region",
        "operator": "eq",
        "value": "EMEA",
    }


def test_evidence_refuses_naive_datetime():
    with pytest.raises(ValueError, match="compiled_at.*timezone-aware"):
        _evidence(compiled_at=datetime(2026, 9, 4, 12, 30)).to_dict()


def test_evidence_serialization_has_stable_mapping_order():
    evidence = _evidence(
        normalized_filters=(
            {"value": "EMEA", "operator": "eq", "column": "region"},
        )
    )

    assert list(evidence.to_dict(include_filter_values=True)["normalized_filters"][0]) == [
        "column",
        "operator",
        "value",
    ]


def test_evidence_refuses_non_allowlisted_serializer_type():
    with pytest.raises(TypeError, match="sql_hash.*object"):
        _evidence(sql_hash=object()).to_dict()


def test_evidence_is_serializable_without_a_live_spark_session():
    evidence = _evidence()

    assert json.dumps(evidence.to_dict())


def test_failed_evidence_serialization_keeps_its_explicit_partial_status():
    payload = _evidence(execution_status="failed", executed_at=UTC_NOW).to_dict()

    assert payload["execution_status"] == "failed"


def test_query_keeps_returning_the_dataframe(tmp_path):
    engine, backend = _semantic_engine(tmp_path)

    dataframe = engine.query(_query())

    assert dataframe == {"dataframe": True}
    assert len(backend.sql) == 1


def test_query_with_evidence_is_injectable_and_contains_no_sql_by_default(tmp_path):
    engine, _backend = _semantic_engine(tmp_path)
    context = ConsumerContext("consumer", "dashboard", trace_id="trace-42")

    result = engine.query_with_evidence(
        _query(),
        consumer_context=context,
        evidence_id="fixed-request-id",
        compiled_at=UTC_NOW,
    )

    assert result.dataframe == {"dataframe": True}
    assert result.evidence.evidence_id == "fixed-request-id"
    assert result.evidence.trace_id == "trace-42"
    # Slice 4.1 left sql_hash empty; 4.2 fills it in, so assert the stronger
    # property: the hash is present and carries its algorithm and version.
    assert result.evidence.to_dict()["sql_hash"].startswith("sha256:v1:")
    assert "sql_text" not in result.evidence.to_dict()


def test_certification_denial_happens_before_sql_generation(tmp_path, monkeypatch):
    engine, backend = _semantic_engine(tmp_path, policy="enforce")
    calls = []

    def fail_if_resolved(*args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("SQL must not be generated after a gate denial")

    monkeypatch.setattr(QueryResolver, "resolve", fail_if_resolved)

    with pytest.raises(SemanticAccessDenied):
        engine.query_with_evidence(_query())

    assert calls == []
    assert backend.sql == []


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_floats_are_refused_instead_of_emitting_invalid_json(bad):
    # json.dumps happily writes a bare NaN/Infinity token, which no strict JSON
    # parser accepts — the evidence would be unreadable by its own consumers.
    evidence = _evidence(
        sources=(
            SourceEvidence(
                dataset="gold.orders",
                contract_id=None,
                contract_version=None,
                definition_hash=None,
                certification_status="MISSING",
                certified_at=None,
                load_age_seconds=bad,
                data_age_seconds=None,
                certification_run_id=None,
            ),
        )
    )

    with pytest.raises(ValueError, match="evidence must stay valid JSON"):
        evidence.to_dict()


def test_serialized_evidence_is_readable_by_a_strict_json_parser():
    def reject_constant(token):
        raise AssertionError(f"non-JSON constant emitted: {token}")

    payload = json.dumps(_evidence().to_dict())

    assert json.loads(payload, parse_constant=reject_constant)


# ---------------------------------------------------------------------------
# Slice 4.2 — compilation evidence
# ---------------------------------------------------------------------------


def test_sql_hash_ignores_formatting():
    assert hash_sql("SELECT  a,\n   b\nFROM t") == hash_sql("SELECT a, b FROM t")


def test_sql_hash_does_not_normalize_inside_string_literals():
    # A blanket \s+ collapse would make these two hash alike, attesting two
    # different queries as the same one.
    assert hash_sql("SELECT a FROM t WHERE x = 'a  b'") != hash_sql(
        "SELECT a FROM t WHERE x = 'a b'"
    )
    assert normalize_sql("SELECT   a FROM t WHERE x = 'a  b'") == (
        "SELECT a FROM t WHERE x = 'a  b'"
    )


def test_sql_hash_carries_its_algorithm_and_normalization_version():
    # Without the prefix, a hash computed under a future normalization could be
    # compared against an old one as if they were the same kind of value.
    assert hash_sql("SELECT 1").startswith("sha256:v1:")


@pytest.mark.parametrize(
    "changed",
    [
        ("model", "revenue", "amount * 1.2", "sum"),
        ("model", "revenue", "amount", "avg"),
        ("model", "revenue", "amount", "sum", "status = 'done'"),
    ],
    ids=["formula", "aggregate_type", "inline_filter"],
)
def test_metric_definition_hash_changes_with_the_definition(changed):
    baseline = hash_definition("model", "revenue", "amount", "sum")

    assert hash_definition(*changed) != baseline


def test_compilation_evidence_covers_two_metrics_and_their_source_columns(tmp_path):
    engine, _ = _semantic_engine(tmp_path, extra_metrics=True)

    result = engine.query_with_evidence(
        SemanticQuery(model_name="sales.orders", metrics=["revenue", "order_count"])
    )

    by_name = {metric.name: metric for metric in result.evidence.metrics}
    assert by_name["revenue"].source_columns == ("amount",)
    assert by_name["order_count"].source_columns == ("order_id",)
    assert all(metric.lineage_status == "resolved" for metric in by_name.values())
    # The unselected metric must not ride along into the evidence.
    assert "unused" not in by_name


def test_metric_without_resolvable_lineage_is_marked_not_silently_empty(tmp_path):
    # An empty source_columns alone reads as "this metric has no sources",
    # which is a different claim from "lineage could not be resolved".
    engine, _ = _semantic_engine(tmp_path, metric_sql="")

    result = engine.query_with_evidence(
        SemanticQuery(model_name="sales.orders", metrics=["revenue"])
    )

    assert result.evidence.metrics[0].lineage_status == "unresolved"
    assert result.evidence.metrics[0].source_columns == ()


def test_evidence_sql_hash_covers_the_sql_actually_executed(tmp_path):
    engine, backend = _semantic_engine(tmp_path)

    result = engine.query_with_evidence(_query())

    assert result.evidence.sql_hash == hash_sql(backend.sql[-1])


def test_raw_sql_request_is_refused_rather_than_returning_none(tmp_path):
    # A consumer handed None cannot tell "there was no SQL" from "the SQL was
    # withheld" — the distinction an audit turns on.
    engine, _ = _semantic_engine(tmp_path)

    result = engine.query_with_evidence(_query())

    with pytest.raises(EvidenceRedactionError, match="withheld by the evidence policy"):
        result.evidence.to_dict(include_sql=True)


def test_evidence_policy_defaults_to_disclosing_nothing():
    policy = EvidencePolicy.redacted()

    assert policy.include_sql is False
    assert policy.include_filter_values is False


def test_evidence_policy_opens_sql_and_filter_values_when_asked(tmp_path):
    engine, _ = _semantic_engine(tmp_path)

    result = engine.query_with_evidence(
        _query(),
        evidence_policy=EvidencePolicy(include_sql=True, include_filter_values=True),
    )
    payload = result.evidence.to_dict(include_sql=True, include_filter_values=True)

    assert payload["sql_text"].startswith("SELECT")
    assert payload["normalized_filters"][0]["value"] == "EMEA"


def test_compilation_evidence_is_identical_across_hash_seeds(tmp_path):
    # The lineage subgraph iterates sets internally, so nothing about the
    # evidence may depend on string hash order.
    script = textwrap.dedent(
        """
        import contextlib, io, json, sys, tempfile, datetime, yaml
        from pathlib import Path
        from types import SimpleNamespace
        from skifer.agentic.resolver import SemanticQuery
        from skifer.semantic.semantic import SemanticEngine
        from skifer.semantic.evidence import EvidencePolicy

        directory = Path(tempfile.mkdtemp()) / "sm"
        directory.mkdir(parents=True)
        model = {
            "key": "orders", "table": "gold.orders",
            "dimensions": [{"name": "region", "sql": "region", "type": "string"}],
            "metrics": [
                {"name": "revenue", "sql": "amount", "type": "sum"},
                {"name": "nb", "sql": "order_id", "type": "count"},
            ],
        }
        (directory / "orders.yaml").write_text(yaml.safe_dump({"models": [model]}))
        (directory / "semantic_catalog.yaml").write_text(yaml.safe_dump({"models": [
            {"key": "orders", "file": "orders.yaml",
             "dimensions": ["region"], "metrics": ["revenue", "nb"]}]}))

        class Backend:
            def execute_sql(self, sql):
                return {"df": True}

        core = SimpleNamespace(
            db="main", env="DEV",
            config={"environments": {"dev": {"semantic_certification_policy": "off"}}},
            _get_backend=lambda: Backend(),
        )
        with contextlib.redirect_stdout(io.StringIO()):
            engine = SemanticEngine(core, models_dir=str(directory))
            result = engine.query_with_evidence(
                SemanticQuery(model_name="orders", metrics=["revenue", "nb"],
                              group_by=["region"]),
                evidence_policy=EvidencePolicy(include_sql=True),
                evidence_id="fixed",
                compiled_at=datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
            )
            payload = result.evidence.to_dict(include_sql=True)
        payload.pop("executed_at")
        payload.pop("execution_duration_seconds")
        payload["policy"].pop("evaluated_at")
        print(json.dumps(payload, sort_keys=False))
        """
    )
    outputs = set()
    for seed in ("1", "7", "101", "9999"):
        env = os.environ.copy()
        env["PYTHONHASHSEED"] = seed
        outputs.add(
            subprocess.check_output([sys.executable, "-c", script], text=True, env=env)
        )

    assert len(outputs) == 1


# ---------------------------------------------------------------------------
# Slice 4.3 — execution and certification evidence
# ---------------------------------------------------------------------------


class MetadataBackend(FakeBackend):
    def __init__(self, dataframe=None, statement_id="statement-42"):
        super().__init__()
        self.dataframe = dataframe
        self.statement_id = statement_id

    def execute_sql_with_metadata(self, sql):
        self.sql.append(sql)
        return ExecutionResult(
            dataframe=self.dataframe,
            statement_id=self.statement_id,
            metadata={"backend": "test"},
        )


class MutableCertificationStore:
    def __init__(self, certification):
        self.certification = certification

    def get_certification(self, dataset, consumer_class):
        return self.certification


def _install_backend(engine, backend):
    engine.core._get_backend = lambda: backend


def _certification(**overrides):
    values = {
        "dataset": "gold.fact_orders",
        "consumer_class": "dashboard",
        "status": "CERTIFIED",
        "contract_version": "2.1.0",
        "definition_hash": "sha256:contract",
        "certified_at": UTC_NOW,
        "checks_passed": True,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_backend_without_metadata_hook_has_no_statement_id(tmp_path):
    engine, _ = _semantic_engine(tmp_path)

    result = engine.query_with_evidence(_query())

    assert result.evidence.statement_id is None
    assert result.evidence.execution_status == "succeeded"
    assert result.evidence.executed_at.tzinfo is not None


def test_backend_metadata_hook_supplies_statement_id(tmp_path):
    engine, _ = _semantic_engine(tmp_path)
    backend = MetadataBackend(dataframe={"rows": []})
    _install_backend(engine, backend)

    result = engine.query_with_evidence(_query())

    assert result.dataframe == {"rows": []}
    assert result.evidence.statement_id == "statement-42"


def test_empty_result_is_successful_evidence(tmp_path):
    engine, _ = _semantic_engine(tmp_path)
    _install_backend(engine, MetadataBackend(dataframe=[]))

    result = engine.query_with_evidence(_query())

    assert result.dataframe == []
    assert result.evidence.execution_status == "succeeded"


def test_sql_error_carries_failed_partial_evidence_without_backend_message(tmp_path):
    engine, backend = _semantic_engine(tmp_path)
    secret = "customer-email@example.test"

    def fail(_sql):
        raise ValueError(f"invalid customer value {secret}")

    backend.execute_sql = fail

    with pytest.raises(SemanticExecutionError, match=secret) as raised:
        engine.query_with_evidence(
            _query(),
            evidence_policy=EvidencePolicy(
                include_sql=True, include_filter_values=True
            ),
        )

    evidence = raised.value.evidence
    payload = evidence.to_dict(include_sql=True, include_filter_values=True)
    assert evidence.execution_status == "failed"
    assert payload["execution_status"] == "failed"
    assert evidence.execution_error_type == "ValueError"
    assert secret not in json.dumps(payload)
    assert payload["sql_text"].startswith("SELECT")
    assert payload["normalized_filters"][0]["value"] == "EMEA"


def test_certification_expired_at_recheck_prevents_execution(tmp_path):
    engine, backend = _semantic_engine(tmp_path, policy="enforce")
    engine.core.config["environments"]["dev"]["semantic_certification_max_age"] = "1h"
    fresh = _certification(certified_at=UTC_NOW)
    expired = _certification(certified_at=datetime(2026, 9, 4, 9, tzinfo=timezone.utc))

    class ChangingStore:
        def __init__(self):
            self.calls = 0

        def get_certification(self, dataset, consumer_class):
            self.calls += 1
            return fresh if self.calls == 1 else expired

    engine.certification_store = ChangingStore()
    engine._utc_now = lambda: UTC_NOW

    with pytest.raises(SemanticAccessDenied) as raised:
        engine.query_with_evidence(_query())

    assert raised.value.reasons == ("EXPIRED",)
    assert backend.sql == []


def test_warn_decision_and_missing_snapshot_are_recorded(tmp_path):
    engine, _ = _semantic_engine(tmp_path, policy="warn")

    result = engine.query_with_evidence(_query())

    assert result.evidence.policy.decision == CertificationDecision.WARN
    assert result.evidence.sources[0].certification_status == "MISSING"
    assert engine.certification_warning_count == 1


def test_certification_evidence_is_a_frozen_snapshot(tmp_path):
    engine, _ = _semantic_engine(tmp_path, policy="enforce")
    certification = _certification()
    engine.certification_store = MutableCertificationStore(certification)

    evidence = engine.query_with_evidence(_query()).evidence
    certification.status = "REVOKED"
    certification.contract_version = "9.9.9"
    certification.definition_hash = "changed"
    certification.certified_at = datetime(2030, 1, 1, tzinfo=timezone.utc)

    source = evidence.sources[0]
    assert source.certification_status == "CERTIFIED"
    assert source.contract_version == "2.1.0"
    assert source.definition_hash == "sha256:contract"
    assert source.certified_at == UTC_NOW


def test_certification_evidence_uses_the_recheck_snapshot(tmp_path):
    engine, _ = _semantic_engine(tmp_path, policy="enforce")
    preflight = _certification(contract_version="1.0.0")
    recheck = _certification(contract_version="2.0.0")

    class ChangingStore:
        def __init__(self):
            self.calls = 0

        def get_certification(self, dataset, consumer_class):
            self.calls += 1
            return preflight if self.calls == 1 else recheck

    engine.certification_store = ChangingStore()

    result = engine.query_with_evidence(_query())

    assert result.evidence.sources[0].contract_version == "2.0.0"


def test_allow_without_snapshot_fails_closed_when_policy_is_not_off(
    tmp_path, monkeypatch
):
    engine, backend = _semantic_engine(tmp_path, policy="enforce")
    engine._utc_now = lambda: UTC_NOW

    monkeypatch.setattr(
        "skifer.semantic.semantic.evaluate",
        lambda *args, **kwargs: PolicyEvaluation(
            CertificationDecision.ALLOW, (), UTC_NOW
        ),
    )

    with pytest.raises(MissingCertificationSnapshot, match="snapshot missing under ALLOW"):
        engine.query_with_evidence(_query())

    assert backend.sql == []


def test_off_mode_without_certification_is_legitimate(tmp_path):
    engine, _ = _semantic_engine(tmp_path, policy="off")

    result = engine.query_with_evidence(_query())

    assert result.evidence.policy.decision == CertificationDecision.ALLOW
    assert result.evidence.sources[0].certification_status == "NOT_EVALUATED"


def test_duration_uses_monotonic_clock_even_if_wall_clock_moves_back(tmp_path):
    engine, _ = _semantic_engine(tmp_path)
    wall_times = iter(
        [
            datetime(2026, 9, 4, 12, 0, tzinfo=timezone.utc),
            datetime(2026, 9, 4, 11, 0, tzinfo=timezone.utc),
            datetime(2026, 9, 4, 10, 0, tzinfo=timezone.utc),
        ]
    )
    monotonic_times = iter([100.0, 102.5])
    engine._utc_now = lambda: next(wall_times)
    engine._monotonic = lambda: next(monotonic_times)

    result = engine.query_with_evidence(_query(), compiled_at=UTC_NOW)

    assert result.evidence.execution_duration_seconds == 2.5
    assert result.evidence.execution_duration_seconds >= 0


# ---------------------------------------------------------------------------
# Multi-model certification coverage (Plan 29, features 3 + 4 + 6 interaction)
# ---------------------------------------------------------------------------


def _joined_engine(tmp_path, *, certified):
    """orders joined to customers, with a store certifying only `certified`."""
    models_dir = tmp_path / "semantic_models"
    models_dir.mkdir()
    models = [
        {
            "key": "orders",
            "table": "gold.orders",
            "grain": ["order"],
            "dimensions": [{"name": "region", "sql": "region", "type": "string"}],
            "metrics": [{"name": "revenue", "sql": "amount", "type": "sum"}],
            "entities": [
                {"name": "order", "type": "primary", "key": "order_id"},
                {"name": "customer", "type": "foreign", "key": "cid"},
            ],
            "relationships": [
                {
                    "name": "oc",
                    "from_entity": "customer",
                    "to_model": "customers",
                    "to_entity": "customer",
                    "cardinality": "many_to_one",
                    "join_type": "left",
                    "verified_by_contract": True,
                }
            ],
        },
        {
            "key": "customers",
            "table": "gold.customers",
            "dimensions": [{"name": "segment", "sql": "segment", "type": "string"}],
            "metrics": [],
            "entities": [{"name": "customer", "type": "primary", "key": "id"}],
        },
    ]
    for model in models:
        (models_dir / f"{model['key']}.yaml").write_text(
            yaml.safe_dump({"models": [model]}), encoding="utf-8"
        )
    (models_dir / "semantic_catalog.yaml").write_text(
        yaml.safe_dump(
            {
                "models": [
                    {
                        "key": model["key"],
                        "file": f"{model['key']}.yaml",
                        "dimensions": [d["name"] for d in model["dimensions"]],
                        "metrics": [m["name"] for m in model["metrics"]],
                        "entities": [e["name"] for e in model.get("entities", [])],
                        "related_models": [
                            r["to_model"] for r in model.get("relationships", [])
                        ],
                    }
                    for model in models
                ]
            }
        ),
        encoding="utf-8",
    )

    class Store:
        def get_certification(self, dataset, consumer_class="default"):
            return Certification(
                dataset=dataset,
                consumer_class=consumer_class,
                status="CERTIFIED" if dataset in certified else "MISSING",
                contract_version="1.0.0",
                definition_hash="h",
                certified_at=UTC_NOW,
                checks_passed=True,
            )

    backend = FakeBackend()
    core = SimpleNamespace(
        db="main",
        env="DEV",
        config={"environments": {"dev": {"semantic_certification_policy": "enforce"}}},
        _get_backend=lambda: backend,
    )
    return SemanticEngine(core, models_dir=str(models_dir), certification_store=Store())


def _joined_query():
    return SemanticQuery(
        model_name="orders", metrics=["revenue"], group_by=["customers.segment"]
    )


def test_multi_model_query_certifies_all_of_its_datasets(tmp_path):
    engine = _joined_engine(tmp_path, certified={"gold.orders", "gold.customers"})

    result = engine.query_with_evidence(_joined_query())

    assert [
        (source.dataset, source.certification_status)
        for source in result.evidence.sources
    ] == [("gold.orders", "CERTIFIED"), ("gold.customers", "CERTIFIED")]


def test_uncertified_joined_dataset_is_denied_not_silently_read(tmp_path):
    # The preflight only sees the root model, so an ALLOW decided there covers a
    # strict subset of what a multi-model query reads. Without the plan-wide
    # recheck, `gold.customers` was read under `enforce` having never been
    # certified at all.
    engine = _joined_engine(tmp_path, certified={"gold.orders"})

    with pytest.raises(SemanticAccessDenied) as error:
        engine.query_with_evidence(_joined_query())

    assert "gold.customers" in str(error.value)


def test_certification_snapshot_is_frozen_against_later_store_changes(tmp_path):
    engine = _joined_engine(tmp_path, certified={"gold.orders", "gold.customers"})
    result = engine.query_with_evidence(_joined_query())
    before = result.evidence.to_dict()

    engine.certification_store = None  # the registry moves on

    assert result.evidence.to_dict() == before
