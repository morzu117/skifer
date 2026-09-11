from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from skifer.core.spark_backend import SparkBackend
from skifer.observability.certification_store import DeltaCertificationStore
from skifer.observability.incidents import Incident, IncidentStatus


@pytest.fixture
def incident_store(spark):
    schema = f"skifer_incidents_{uuid4().hex}"
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
