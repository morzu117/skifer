from dataclasses import asdict
from datetime import datetime, timezone

import pytest

from skifer.observability.certification import ContractDefinition
from skifer.observability.certification_store import SqliteCertificationStore
from skifer.observability.checks import CheckResult, CheckStatus, NullCheck, UniqueCheck
from skifer.observability.incidents import (
    Incident,
    IncidentStatus,
    InvalidIncidentTransition,
    incidents_from_report,
)
from skifer.observability.monitor import MonitorReport
from skifer.observability.publication import PublicationCoordinator
from skifer.observability.publication import start_publication_run, stage_dataframe
from tests.fakes.fake_backend import FakeBackend, FakeDataFrame


def _definition() -> ContractDefinition:
    return ContractDefinition("sales.orders", "1.0.0", "hash", "{}", "sales.orders", None)


def _result(
    contract,
    *,
    status: CheckStatus = CheckStatus.FAIL,
    severity: str = "critical",
    message: str = "failed",
    actual_value=None,
    expected_value=None,
) -> CheckResult:
    return CheckResult(
        contract,
        status=status,
        severity=severity,
        message=message,
        actual_value=actual_value,
        expected_value=expected_value,
    )


def _null_result(fqn: str, column: str = "id", *, status=CheckStatus.FAIL) -> CheckResult:
    return _result(
        NullCheck(table=fqn, column=column, severity="critical"),
        status=status,
        severity="critical",
        actual_value=1 if status is CheckStatus.FAIL else 0,
        expected_value=0,
        message=f"{column} null",
    )


class _Monitor:
    def __init__(self, results):
        self.results = list(results)

    def check_from_schema(self, fqn, schema_dict, raise_on_critical=False):
        return MonitorReport(fqn, self.results)


class _BrokenIncidentStore(SqliteCertificationStore):
    def open_incident(self, incident):
        raise RuntimeError("incident store unavailable")


class _BrokenResolveStore(SqliteCertificationStore):
    def resolve_open_incidents(self, target_fqn, *, root_cause, resolved_at):
        raise RuntimeError("incident store unavailable")


def test_incident_opened_on_quarantine_one_per_critical_check():
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    report = MonitorReport(
        "staging.orders",
        [
            _result(NullCheck(table="t", column="id"), actual_value="secret-a"),
            _result(NullCheck(table="t", column="amount")),
            _result(UniqueCheck(table="t", columns=["id"]), severity="warning"),
            _result(
                NullCheck(table="t", column="id"),
                message="duplicate critical check in same report",
            ),
            _result(
                NullCheck(table="t", column="status"),
                status=CheckStatus.SKIPPED,
                severity="critical",
            ),
        ],
    )

    incidents = incidents_from_report(
        report, run_id="run-1", target_fqn="gold.orders", at=now
    )

    assert [incident.check_name for incident in incidents] == [
        "NullCheck:id",
        "NullCheck:amount",
    ]
    assert all(incident.status is IncidentStatus.NEW for incident in incidents)


def test_incident_dedup_while_open():
    store = SqliteCertificationStore(":memory:")
    opened_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    first = Incident(
        "run-1:NullCheck:id",
        "gold.orders",
        "run-1",
        "NullCheck:id",
        "critical",
        IncidentStatus.NEW,
        opened_at,
    )
    second = Incident(
        "run-2:NullCheck:id",
        "gold.orders",
        "run-2",
        "NullCheck:id",
        "critical",
        IncidentStatus.NEW,
        opened_at,
    )

    assert store.open_incident(first) == first
    assert store.open_incident(second).id == first.id

    assert store.list_open_incidents("gold.orders") == [first]
    assert len(store.list_incidents(target_fqn="gold.orders")) == 1


def test_resume_opens_no_duplicate():
    definition = _definition()
    store, backend = SqliteCertificationStore(":memory:"), FakeBackend()
    opened_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    incident = Incident(
        "run-1:NullCheck:id",
        "gold.orders",
        "run-1",
        "NullCheck:id",
        "critical",
        IncidentStatus.NEW,
        opened_at,
    )
    store.open_incident(incident)
    store.open_incident(incident)
    run = stage_dataframe(
        backend,
        start_publication_run("gold.orders", definition, store),
        definition,
        store,
        FakeDataFrame([{"id": 1}]),
    )

    result = PublicationCoordinator(backend, _Monitor([]), store).resume(run, definition)

    assert result.state == "PROMOTED"
    assert len(store.list_incidents(target_fqn="gold.orders")) == 1
    resolved = store.get_incident(incident.id)
    assert resolved.status is IncidentStatus.RESOLVED
    assert resolved.root_cause == "recovered"


def test_recovered_on_next_promoted():
    store = SqliteCertificationStore(":memory:")
    opened = store.open_incident(
        Incident(
            "run-1:NullCheck:id",
            "gold.orders",
            "run-1",
            "NullCheck:id",
            "critical",
            IncidentStatus.NEW,
            datetime(2026, 1, 1, tzinfo=timezone.utc),
        )
    )

    resolved = store.resolve_open_incidents(
        "gold.orders",
        root_cause="recovered",
        resolved_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
    )

    assert len(resolved) == 1
    assert resolved[0].id == opened.id
    assert resolved[0].status is IncidentStatus.RESOLVED
    assert resolved[0].root_cause == "recovered"
    assert resolved[0].resolved_at == datetime(2026, 1, 2, tzinfo=timezone.utc)
    assert store.list_open_incidents("gold.orders") == []


def test_coordinator_quarantine_opens_incident():
    store, backend = SqliteCertificationStore(":memory:"), FakeBackend()
    monitor = _Monitor([_null_result("staging.orders")])
    coordinator = PublicationCoordinator(backend, monitor, store)

    result = coordinator.publish(
        FakeDataFrame([{"id": None}]), "gold.orders", {}, _definition()
    )

    assert result.state == "QUARANTINED"
    incidents = store.list_open_incidents("gold.orders")
    assert len(incidents) == 1
    assert incidents[0].run_id == result.run.run_id
    assert incidents[0].check_name == "NullCheck:id"


def test_coordinator_promoted_resolves_open_incidents():
    store, backend = SqliteCertificationStore(":memory:"), FakeBackend()
    store.open_incident(
        Incident(
            "run-1:NullCheck:id",
            "gold.orders",
            "run-1",
            "NullCheck:id",
            "critical",
            IncidentStatus.NEW,
            datetime(2026, 1, 1, tzinfo=timezone.utc),
        )
    )
    coordinator = PublicationCoordinator(
        backend, _Monitor([_null_result("staging.orders", status=CheckStatus.PASS)]), store
    )

    result = coordinator.publish(
        FakeDataFrame([{"id": 1}]), "gold.orders", {}, _definition()
    )

    assert result.state == "PROMOTED"
    incident = store.get_incident("run-1:NullCheck:id")
    assert incident.status is IncidentStatus.RESOLVED
    assert incident.root_cause == "recovered"
    assert incident.resolved_at is not None


def test_incident_hook_is_non_blocking():
    store, backend = _BrokenIncidentStore(":memory:"), FakeBackend()
    coordinator = PublicationCoordinator(
        backend, _Monitor([_null_result("staging.orders")]), store
    )

    with pytest.warns(RuntimeWarning, match="failed to open incident"):
        result = coordinator.publish(
            FakeDataFrame([{"id": None}]), "gold.orders", {}, _definition()
        )

    assert result.state == "QUARANTINED"


def test_incident_resolve_hook_is_non_blocking():
    store, backend = _BrokenResolveStore(":memory:"), FakeBackend()
    coordinator = PublicationCoordinator(
        backend, _Monitor([_null_result("staging.orders", status=CheckStatus.PASS)]), store
    )

    with pytest.warns(RuntimeWarning, match="failed to resolve incidents"):
        result = coordinator.publish(
            FakeDataFrame([{"id": 1}]), "gold.orders", {}, _definition()
        )

    assert result.state == "PROMOTED"
    assert backend._written["gold.orders"] == [{"id": 1}]


def test_invalid_transition_refused():
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    resolved = Incident(
        "run-1:NullCheck:id",
        "gold.orders",
        "run-1",
        "NullCheck:id",
        "critical",
        IncidentStatus.RESOLVED,
        now,
        resolved_at=now,
    )
    assigned = Incident(
        "run-2:NullCheck:id",
        "gold.orders",
        "run-2",
        "NullCheck:id",
        "critical",
        IncidentStatus.ASSIGNED,
        now,
    )

    with pytest.raises(InvalidIncidentTransition):
        resolved.transition(IncidentStatus.NEW, at=now)
    with pytest.raises(InvalidIncidentTransition):
        assigned.transition(IncidentStatus.NEW, at=now)


def test_no_data_value_persisted():
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    report = MonitorReport(
        "staging.orders",
        [
            _result(
                NullCheck(table="t", column="email"),
                message="bad value customer@example.com",
                actual_value="customer@example.com",
                expected_value="redacted",
            )
        ],
    )

    incident = incidents_from_report(
        report, run_id="run-1", target_fqn="gold.orders", at=now
    )[0]

    payload = asdict(incident)
    assert "message" not in payload
    assert "actual_value" not in payload
    assert "expected_value" not in payload
    assert "customer@example.com" not in repr(payload)
    assert "redacted" not in repr(payload)
