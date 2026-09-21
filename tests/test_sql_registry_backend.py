"""Shared store behaviour for SparkBackend and the portable SQL registry shim."""

from __future__ import annotations

from datetime import datetime, timezone
import inspect
from uuid import uuid4

import pytest

from skifer.adaptive import DeltaUsageEventStore, SemanticUsageEvent, fingerprint_query
from skifer.core.adapters.duckdb import DuckDBAdapter
from skifer.core.spark_backend import SparkBackend
from skifer.observability.certification import ContractDefinition
from skifer.observability.certification_store import (
    DeltaCertificationStore,
    RunEvent,
    StoredCheckResult,
)
from skifer.observability.checks import CheckStatus, ContractScope
from skifer.observability.incidents import Incident, IncidentStatus
from skifer.observability.sql_registry import SqlRegistryBackend


BACKEND_METHODS = (
    "append_certification_check",
    "append_certification_contract",
    "append_certification_run",
    "append_semantic_usage_event",
    "get_certification_check_results",
    "get_certification_contract",
    "get_certification_contract_by_hash",
    "get_certification_run",
    "get_incident",
    "get_latest_certification_promotion",
    "get_open_incident",
    "list_certification_history",
    "list_incidents",
    "list_open_incidents",
    "list_semantic_usage_events",
    "upsert_incident",
)


@pytest.fixture(params=("spark", "duckdb"))
def registry_backend(request):
    schema_prefix = f"registry_backend_{uuid4().hex}"
    if request.param == "spark":
        spark = request.getfixturevalue("spark")
        backend = SparkBackend(spark=spark, is_local=True)
        yield backend, schema_prefix
        for suffix in ("certification", "usage"):
            spark.sql(f"DROP SCHEMA IF EXISTS `{schema_prefix}_{suffix}` CASCADE")
        return

    duckdb = pytest.importorskip("duckdb")
    connection = duckdb.connect()
    try:
        yield SqlRegistryBackend(DuckDBAdapter(connection)), schema_prefix
    finally:
        connection.close()


def _contract(definition_hash: str = "sha256:contract-v1") -> ContractDefinition:
    return ContractDefinition(
        contract_id="orders",
        contract_version="1.0.0",
        definition_hash=definition_hash,
        canonical_json='{"contract":"orders"}',
        data_product_id="orders",
        owner="data-platform",
        created_at=datetime(2026, 9, 18, 8, 0, tzinfo=timezone.utc),
    )


def _run(
    event_id: str,
    run_id: str,
    state: str,
    occurred_at: datetime,
) -> RunEvent:
    return RunEvent(
        event_id=event_id,
        run_id=run_id,
        dataset="gold.orders",
        state=state,
        contract_id="orders",
        contract_version="1.0.0",
        definition_hash="sha256:contract-v1",
        occurred_at=occurred_at,
        target_fqn="gold.orders",
        staging_fqn="staging.orders",
        quarantine_fqn="quarantine.orders",
    )


def _usage_event(
    event_id: str,
    occurred_at: datetime,
    *,
    environment: str = "prod",
    consumer_class: str = "dashboard",
) -> SemanticUsageEvent:
    model_hashes = ("sha256:model",)
    metric_ids = ("orders.revenue",)
    dimension_ids = ("region",)
    filter_shape = ("region:eq",)
    return SemanticUsageEvent(
        event_id=event_id,
        occurred_at=occurred_at,
        environment=environment,
        consumer_class=consumer_class,
        model_hashes=model_hashes,
        metric_ids=metric_ids,
        dimension_ids=dimension_ids,
        normalized_filter_shape=filter_shape,
        query_fingerprint=fingerprint_query(
            model_hashes=model_hashes,
            metric_ids=metric_ids,
            dimension_ids=dimension_ids,
            normalized_filter_shape=filter_shape,
        ),
        duration_ms=25,
        rows_returned=4,
        bytes_scanned=100,
        status="succeeded",
    )


def test_sql_registry_backend_has_exactly_the_spark_store_method_signatures():
    public_methods = {
        name
        for name, member in inspect.getmembers(SqlRegistryBackend, inspect.isfunction)
        if not name.startswith("_")
    }

    assert public_methods == set(BACKEND_METHODS)
    for name in BACKEND_METHODS:
        assert inspect.signature(getattr(SqlRegistryBackend, name)) == inspect.signature(
            getattr(SparkBackend, name)
        )


def test_certification_store_behaves_identically_on_both_backends(registry_backend):
    backend, schema_prefix = registry_backend
    store = DeltaCertificationStore(
        backend=backend,
        schema=f"{schema_prefix}_certification",
    )
    t0 = datetime(2026, 9, 18, 8, 0, tzinfo=timezone.utc)
    t1 = datetime(2026, 9, 18, 9, 0, tzinfo=timezone.utc)
    t2 = datetime(2026, 9, 18, 10, 0, tzinfo=timezone.utc)
    t3 = datetime(2026, 9, 18, 11, 0, tzinfo=timezone.utc)

    assert store.get_contract("orders", "1.0.0") is None
    store.register_contract(_contract())
    store.register_contract(_contract())
    assert store.get_contract("orders", "1.0.0") == _contract()
    assert store.get_contract_by_hash("orders", "sha256:contract-v1") == _contract()

    failed = _run("event-1", "run-shared", "FAILED", t0)
    promoted = _run("event-2", "run-shared", "PROMOTED", t1)
    store.append_run_event(failed)
    store.append_run_event(promoted)
    store.append_run_event(promoted)
    assert store.get_run("run-shared") == promoted
    assert store.get_latest_promoted("gold.orders") == promoted
    assert store.list_history("gold.orders", limit=1) == [promoted]

    check = StoredCheckResult(
        event_id="check-1",
        run_id="run-shared",
        check_type="NullCheck",
        scope=ContractScope.ROW,
        severity="critical",
        status=CheckStatus.PASS,
        actual_value="0",
        expected_value="0",
        message="No null order identifiers",
    )
    store.append_check_results([check, check])
    assert store.get_check_results("run-shared") == [check]

    first = Incident(
        id="incident-1",
        target_fqn="gold.orders",
        run_id="run-shared",
        check_name="NullCheck:order_id",
        severity="critical",
        status=IncidentStatus.NEW,
        opened_at=t1,
    )
    later_duplicate = Incident(
        id="incident-duplicate",
        target_fqn="gold.orders",
        run_id="run-later",
        check_name="NullCheck:order_id",
        severity="critical",
        status=IncidentStatus.NEW,
        opened_at=t2,
    )
    second = Incident(
        id="incident-2",
        target_fqn="gold.orders",
        run_id="run-shared",
        check_name="UniqueCheck:order_id",
        severity="critical",
        status=IncidentStatus.NEW,
        opened_at=t2,
    )
    assert store.open_incident(first) == first
    assert store.open_incident(later_duplicate) == first
    assert store.open_incident(second) == second
    assert store.get_open_incident(first.target_fqn, first.check_name) == first
    assert store.list_open_incidents("gold.orders") == [second, first]
    assert store.list_incidents(target_fqn="gold.orders", limit=1) == [second]

    resolved = store.update_incident(
        second.id,
        IncidentStatus.RESOLVED,
        at=t3,
        root_cause="source repaired",
    )
    backend.upsert_incident(store.schema, store._row(resolved))
    assert store.get_incident(second.id) == resolved
    assert store.get_open_incident(second.target_fqn, second.check_name) is None
    assert store.list_open_incidents("gold.orders") == [first]
    assert store.list_incidents(status="RESOLVED", target_fqn="gold.orders") == [
        resolved
    ]

    store.register_contract(_contract("sha256:contract-v2"))
    with pytest.raises(
        ValueError,
        match="Contract 'orders' version '1.0.0' has ambiguous definitions",
    ):
        store.get_contract("orders", "1.0.0")


def test_usage_store_behaves_identically_on_both_backends(registry_backend):
    backend, schema_prefix = registry_backend
    store = DeltaUsageEventStore(
        backend=backend,
        schema=f"{schema_prefix}_usage",
    )
    t0 = datetime(2026, 9, 18, 8, 0, tzinfo=timezone.utc)
    t1 = datetime(2026, 9, 18, 9, 0, tzinfo=timezone.utc)
    t2 = datetime(2026, 9, 18, 10, 0, tzinfo=timezone.utc)
    old = _usage_event("event-old", t0)
    tied_a = _usage_event("event-a", t1)
    tied_b = _usage_event("event-b", t1)
    dev = _usage_event("event-dev", t2, environment="dev")

    for event in (old, tied_a, tied_b, dev, tied_b):
        store.append(event)

    assert store.list_events() == [dev, tied_b, tied_a, old]
    assert store.list_events(
        environment="prod",
        consumer_class="dashboard",
        since=t1,
        until=t1,
        limit=1,
    ) == [tied_b]
