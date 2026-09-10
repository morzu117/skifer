"""Tests for Plan 29 slice 8.1 usage events and append-only stores."""

from dataclasses import fields
from datetime import datetime, timezone
import json
import os
import subprocess
import sys
import textwrap
from unittest.mock import MagicMock

import pytest

from skifer.adaptive import (
    DeltaUsageEventStore,
    SemanticUsageEvent,
    SqliteUsageEventStore,
    fingerprint_query,
    usage_event_from_evidence,
)
from skifer.core.spark_backend import SparkBackend
from skifer.semantic.access_policy import (
    CertificationDecision,
    PolicyEvaluation,
)
from skifer.semantic.evidence import (
    MetricEvidence,
    SemanticEvidence,
    SourceEvidence,
    hash_sql,
)
from tests.fakes.fake_backend import FakeBackend


NOW = datetime(2026, 9, 7, 10, 30, tzinfo=timezone.utc)


def _evidence(**overrides) -> SemanticEvidence:
    values = {
        "evidence_id": "event-1",
        "trace_id": "trace-1",
        "model_keys": ("sales.orders",),
        "metrics": (
            MetricEvidence(
                name="revenue",
                model_key="sales.orders",
                definition_hash="metric-definition-1",
                source_columns=("amount",),
            ),
        ),
        "dimensions": ("region", "order_date"),
        "normalized_filters": (
            {"column": "region", "operator": "eq", "value": "EMEA"},
            {"column": "status", "operator": "in", "value": ["PAID"]},
        ),
        "sources": (
            SourceEvidence(
                dataset="gold.orders",
                contract_id="orders",
                contract_version="1.0.0",
                definition_hash="model-definition-1",
                certification_status="CERTIFIED",
                certified_at=NOW,
                load_age_seconds=2.0,
                data_age_seconds=3.0,
                certification_run_id="run-1",
            ),
        ),
        "policy": PolicyEvaluation(CertificationDecision.ALLOW, (), NOW),
        "sql_hash": hash_sql("SELECT revenue FROM gold.orders"),
        "sql_text": "SELECT revenue FROM gold.orders",
        "statement_id": "statement-1",
        "compiled_at": NOW,
        "executed_at": NOW,
        "execution_status": "succeeded",
        "execution_duration_seconds": 1.234,
    }
    values.update(overrides)
    return SemanticEvidence(**values)


def _event(**overrides) -> SemanticUsageEvent:
    values = {
        "event_id": "event-1",
        "occurred_at": NOW,
        "environment": "prod",
        "consumer_class": "dashboard",
        "model_hashes": ("model-definition-1",),
        "metric_ids": ("sales.orders.revenue",),
        "dimension_ids": ("order_date", "region"),
        "normalized_filter_shape": ("region:eq", "status:in"),
        "duration_ms": 1234,
        "rows_returned": 42,
        "bytes_scanned": 4096,
        "status": "succeeded",
    }
    values.update(overrides)
    values.setdefault(
        "query_fingerprint",
        fingerprint_query(
            model_hashes=values["model_hashes"],
            metric_ids=values["metric_ids"],
            dimension_ids=values["dimension_ids"],
            normalized_filter_shape=values["normalized_filter_shape"],
        ),
    )
    return SemanticUsageEvent(**values)


def _stores(retention_days=90):
    return (
        SqliteUsageEventStore(":memory:", retention_days=retention_days),
        DeltaUsageEventStore(FakeBackend(), retention_days=retention_days),
    )


def test_usage_event_has_exactly_the_plan_fields():
    assert tuple(field.name for field in fields(SemanticUsageEvent)) == (
        "event_id",
        "occurred_at",
        "environment",
        "consumer_class",
        "model_hashes",
        "metric_ids",
        "dimension_ids",
        "normalized_filter_shape",
        "query_fingerprint",
        "duration_ms",
        "rows_returned",
        "bytes_scanned",
        "status",
    )


def test_sensitive_canaries_never_enter_event_or_fingerprint():
    filter_secret = "FILTER_CANARY_customer@example.test"
    sql_secret = "SQL_CANARY_token_abc123"
    question_secret = "QUESTION_CANARY_show_private_salary"
    evidence = _evidence(
        normalized_filters=(
            {"column": "region", "operator": "eq", "value": filter_secret},
        ),
        sql_text=f"SELECT '{sql_secret}'",
        sql_hash=hash_sql(f"SELECT '{sql_secret}'"),
    )
    object.__setattr__(evidence, "question", question_secret)

    event = usage_event_from_evidence(
        evidence, environment="prod", consumer_class="dashboard"
    )
    serialized = json.dumps(event.to_dict(), sort_keys=True)

    for sentinel in (filter_secret, sql_secret, question_secret):
        assert sentinel not in serialized
        assert sentinel not in event.query_fingerprint

    same_shape = _evidence(
        normalized_filters=(
            {"column": "region", "operator": "eq", "value": "another value"},
        ),
        sql_text="SELECT something_completely_different",
        sql_hash=hash_sql("SELECT something_completely_different"),
    )
    object.__setattr__(same_shape, "question", "another question")
    assert usage_event_from_evidence(
        same_shape, environment="prod", consumer_class="dashboard"
    ).query_fingerprint == event.query_fingerprint


def test_future_evidence_field_is_not_admitted_by_event_allowlist():
    secret = "FUTURE_FIELD_CANARY"
    evidence = _evidence()
    object.__setattr__(evidence, "future_sensitive_field", secret)

    payload = usage_event_from_evidence(
        evidence, environment="dev", consumer_class="agent"
    ).to_dict()

    assert "future_sensitive_field" not in payload
    assert secret not in json.dumps(payload)


def test_fingerprint_ignores_non_semantic_order():
    first = fingerprint_query(
        model_hashes=("hash-b", "hash-a"),
        metric_ids=("orders.count", "orders.revenue"),
        dimension_ids=("region", "date"),
        normalized_filter_shape=("status:in", "region:eq"),
    )
    second = fingerprint_query(
        model_hashes=("hash-a", "hash-b"),
        metric_ids=("orders.revenue", "orders.count"),
        dimension_ids=("date", "region"),
        normalized_filter_shape=("region:eq", "status:in"),
    )

    assert first == second
    assert first.startswith("sha256:v1:")


def test_fingerprint_changes_for_metric_or_operator_change():
    base = {
        "model_hashes": ("hash-a",),
        "metric_ids": ("orders.revenue",),
        "dimension_ids": ("region",),
        "normalized_filter_shape": ("region:eq",),
    }
    original = fingerprint_query(**base)

    assert original != fingerprint_query(
        **{**base, "metric_ids": ("orders.count",)}
    )
    assert original != fingerprint_query(
        **{**base, "normalized_filter_shape": ("region:in",)}
    )


def test_fingerprint_is_stable_across_python_hash_seeds():
    script = textwrap.dedent("""
        from skifer.adaptive import fingerprint_query
        print(fingerprint_query(
            model_hashes=tuple({'hash-a', 'hash-b'}),
            metric_ids=tuple({'orders.revenue', 'orders.count'}),
            dimension_ids=tuple({'region', 'date'}),
            normalized_filter_shape=tuple({'region:eq', 'status:in'}),
        ))
    """)
    outputs = set()
    for seed in ("1", "17", "999"):
        environment = os.environ.copy()
        environment["PYTHONHASHSEED"] = seed
        outputs.add(
            subprocess.check_output(
                [sys.executable, "-c", script], text=True, env=environment
            ).strip()
        )

    assert len(outputs) == 1


def test_conversion_refuses_naive_datetime_and_incomplete_evidence():
    with pytest.raises(ValueError, match="timezone-aware"):
        usage_event_from_evidence(
            _evidence(executed_at=datetime(2026, 9, 7, 10, 30)),
            environment="prod",
            consumer_class="dashboard",
        )
    with pytest.raises(ValueError, match="executed_at is required"):
        usage_event_from_evidence(
            _evidence(executed_at=None),
            environment="prod",
            consumer_class="dashboard",
        )
    with pytest.raises(ValueError, match="definition_hash"):
        usage_event_from_evidence(
            _evidence(
                sources=(
                    SourceEvidence(
                        "gold.orders", None, None, None, "CERTIFIED", NOW, None, None, None
                    ),
                )
            ),
            environment="prod",
            consumer_class="dashboard",
        )


def test_event_itself_refuses_a_naive_datetime():
    with pytest.raises(ValueError, match="timezone-aware"):
        _event(occurred_at=datetime(2026, 9, 7, 10, 30))


def test_event_refuses_a_fingerprint_for_another_query_shape():
    with pytest.raises(ValueError, match="does not match"):
        _event(query_fingerprint=fingerprint_query(
            model_hashes=("another-model",),
            metric_ids=("orders.count",),
            dimension_ids=(),
            normalized_filter_shape=(),
        ))


@pytest.mark.parametrize("store", _stores())
def test_stores_round_trip_every_type_without_loss(store):
    event = _event()

    store.append(event)
    stored = store.list_events()

    assert stored == [event]
    assert isinstance(stored[0].occurred_at, datetime)
    assert isinstance(stored[0].model_hashes, tuple)
    assert isinstance(stored[0].duration_ms, int)
    assert isinstance(stored[0].rows_returned, int)
    assert isinstance(stored[0].bytes_scanned, int)


@pytest.mark.parametrize("store", _stores(retention_days=7))
def test_configured_retention_is_read_but_never_deletes(store):
    old_event = _event(
        event_id="old-event",
        occurred_at=datetime(2000, 1, 1, tzinfo=timezone.utc),
    )
    store.append(old_event)
    store.append(_event(event_id="new-event"))

    assert store.retention_days == 7
    assert {event.event_id for event in store.list_events()} == {
        "old-event",
        "new-event",
    }


@pytest.mark.parametrize("store", _stores())
def test_environment_and_consumer_class_are_strict_partitions(store):
    store.append(_event(event_id="prod-dashboard"))
    store.append(
        _event(event_id="dev-dashboard", environment="dev")
    )
    store.append(
        _event(event_id="prod-agent", consumer_class="agent")
    )

    assert [
        event.event_id
        for event in store.list_events(
            environment="prod", consumer_class="dashboard"
        )
    ] == ["prod-dashboard"]
    assert [
        event.event_id
        for event in store.list_events(environment="dev", consumer_class="dashboard")
    ] == ["dev-dashboard"]
    assert [
        event.event_id
        for event in store.list_events(environment="prod", consumer_class="agent")
    ] == ["prod-agent"]


def test_delta_store_uses_fake_backend_without_counting():
    class NoCountBackend(FakeBackend):
        def count(self, df):
            raise AssertionError("usage persistence must never count Spark rows")

    store = DeltaUsageEventStore(NoCountBackend())
    store.append(_event())

    assert store.list_events() == [_event()]


def test_delta_lookup_escapes_backslashes_and_apostrophes_in_sql():
    spark = MagicMock()
    spark.catalog.tableExists.return_value = True
    spark.sql.return_value.collect.return_value = []
    backend = SparkBackend(spark=spark)

    backend.list_semantic_usage_events(
        "adaptive", environment="prod\\' OR 1=1 --"
    )

    statement = spark.sql.call_args.args[0]
    assert "prod\\\\'' OR 1=1 --" in statement


def test_store_filters_refuse_naive_datetime():
    store = SqliteUsageEventStore(":memory:")
    with pytest.raises(ValueError, match="timezone-aware"):
        store.list_events(since=datetime(2026, 9, 7))
