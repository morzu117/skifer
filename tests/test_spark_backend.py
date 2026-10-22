from __future__ import annotations

import dataclasses
from dataclasses import replace
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from skifer.core.spark_backend import SparkBackend
from skifer.observability.certification import ContractDefinition
from skifer.observability.certification_store import (
    DeltaCertificationStore,
    RunEvent,
    StoredCheckResult,
)
from skifer.observability.checks import CheckStatus, ContractScope
from skifer.observability.incidents import Incident, IncidentStatus


@pytest.fixture
def incident_store(spark):
    schema = f"skifer_incidents_{uuid4().hex}"
    backend = SparkBackend(spark=spark, is_local=True)
    try:
        yield DeltaCertificationStore(backend, schema=schema), backend, schema
    finally:
        spark.sql(f"DROP SCHEMA IF EXISTS `{schema}` CASCADE")


@pytest.fixture
def cert_store(spark):
    schema = f"skifer_certification_{uuid4().hex}"
    backend = SparkBackend(spark=spark, is_local=True)
    try:
        yield DeltaCertificationStore(backend, schema=schema), backend, schema
    finally:
        spark.sql(f"DROP SCHEMA IF EXISTS `{schema}` CASCADE")


def test_real_backend_incident_open_dedup_and_resolve(incident_store, spark):
    store, _backend, schema = incident_store
    opened_at = datetime(2026, 9, 11, 12, 0, tzinfo=timezone.utc)
    incident = Incident(
        id="run-1:NullCheck:amount",
        target_fqn="silver.orders",
        run_id="run-1",
        check_name="NullCheck:amount",
        severity="critical",
        status=IncidentStatus.NEW,
        opened_at=opened_at,
    )

    opened = store.open_incident(incident)
    duplicate = store.open_incident(replace(incident, id="run-2", run_id="run-2"))

    assert opened == incident
    assert duplicate == incident
    assert store.get_open_incident("silver.orders", "NullCheck:amount") == incident
    assert spark.table(f"`{schema}`.`incidents`").count() == 1

    resolved = store.update_incident(
        incident.id,
        IncidentStatus.RESOLVED,
        at=datetime(2026, 9, 11, 13, 0, tzinfo=timezone.utc),
        root_cause="recovered",
    )

    assert resolved.status is IncidentStatus.RESOLVED
    assert store.get_open_incident("silver.orders", "NullCheck:amount") is None
    assert store.get_incident(incident.id) == resolved
    assert spark.table(f"`{schema}`.`incidents`").count() == 1


def test_real_backend_incident_filter_values_round_trip_quotes_and_backslashes(
    incident_store,
):
    _store, backend, schema = incident_store
    target_fqn = "silver.team's\\orders"
    check_name = "ValueCheck:owner's\\code"
    incident = Incident(
        id="run-quoted:ValueCheck",
        target_fqn=target_fqn,
        run_id="run-quoted",
        check_name=check_name,
        severity="critical",
        status=IncidentStatus.NEW,
        opened_at=datetime(2026, 9, 11, 12, 0, tzinfo=timezone.utc),
    )

    row = {
        "id": incident.id,
        "target_fqn": incident.target_fqn,
        "run_id": incident.run_id,
        "check_name": incident.check_name,
        "severity": incident.severity,
        "status": incident.status.value,
        "opened_at": incident.opened_at.isoformat(),
        "assignee": None,
        "root_cause": None,
        "resolved_at": None,
    }
    backend.upsert_incident(schema, row)

    assert backend.get_open_incident(schema, target_fqn, check_name) == row


def test_real_backend_run_event_with_all_optional_fqns_none_does_not_crash_inference(
    cert_store,
):
    # A STARTED RunEvent carries target_fqn/staging_fqn/quarantine_fqn unset — a
    # single-row DataFrame built by inference then has a column that is entirely
    # None, which fails with CANNOT_DETERMINE_TYPE. _append_certification must
    # supply an explicit schema instead of relying on inference.
    store, _backend, _schema = cert_store
    event = RunEvent(
        event_id="run-none:STARTED",
        run_id="run-none",
        dataset="gold.kpi_orders",
        state="STARTED",
        contract_id="sales.kpi_orders",
        contract_version="1.0.0",
        definition_hash="sha256:v1:hash",
        occurred_at=datetime(2026, 10, 19, 12, 0, tzinfo=timezone.utc),
        target_fqn=None,
        staging_fqn=None,
        quarantine_fqn=None,
    )

    store.append_run_event(event)

    assert store.get_run("run-none") == event


def test_real_backend_contract_with_optional_fields_none_does_not_crash_inference(
    cert_store,
):
    store, _backend, _schema = cert_store
    definition = ContractDefinition(
        contract_id="sales.kpi_orders",
        contract_version="1.0.0",
        definition_hash="sha256:v1:hash",
        canonical_json="{}",
        data_product_id="sales.kpi_orders",
        owner=None,
    )

    store.register_contract(definition)

    assert store.get_contract("sales.kpi_orders", "1.0.0") == definition


def test_real_backend_check_result_with_none_values_does_not_crash_inference(
    cert_store,
):
    store, _backend, _schema = cert_store
    result = StoredCheckResult(
        event_id="run-none:check-1",
        run_id="run-none",
        check_type="NullCheck",
        scope=ContractScope.ROW,
        severity="critical",
        status=CheckStatus.PASS,
        actual_value=None,
        expected_value=None,
        message="ok",
    )

    store.append_check_results([result])

    assert store.get_check_results("run-none") == [result]


def _ddl_columns(ddl: str) -> list[str]:
    return [column.split()[0] for column in ddl.split(",")]


@pytest.mark.parametrize(
    ("ddl_name", "dataclass_type"),
    [
        ("_CONTRACT_DEFINITION_SCHEMA", ContractDefinition),
        ("_MATERIALIZATION_RUN_SCHEMA", RunEvent),
        ("_CHECK_RESULT_SCHEMA", StoredCheckResult),
    ],
)
def test_certification_ddl_matches_its_dataclass(ddl_name, dataclass_type):
    """Un champ ajouté à la dataclass sans l'être au DDL serait perdu sans erreur à l'écriture."""
    ddl = getattr(SparkBackend, ddl_name)

    assert _ddl_columns(ddl) == [f.name for f in dataclasses.fields(dataclass_type)]


def test_certification_contract_ddl_types_the_canonicalization_version_as_bigint():
    assert "canonicalization_version BIGINT" in SparkBackend._CONTRACT_DEFINITION_SCHEMA
