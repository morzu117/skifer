"""Contract tests for Plan 29 certification stores."""
from datetime import datetime, timezone
import threading

import pytest

from skifer.observability.certification import ContractDefinition
from skifer.observability.certification_store import (
    DeltaCertificationStore, RunEvent, SqliteCertificationStore, StoredCheckResult,
)
from skifer.observability.checks import CheckStatus, ContractScope
from skifer.observability.incidents import Incident, IncidentStatus
from skifer.observability.uc_mirror import mirror_certification
from skifer.observability.certification_store import Certification


def _event(state="STARTED"):
    return RunEvent("evt-1", "run-1", "sales.orders", state, "sales.orders", "1.0.0", "hash", datetime.now(timezone.utc))


def _run_event(event_id, run_id, state, occurred_at):
    return RunEvent(event_id, run_id, "sales.orders", state, "sales.orders", "1.0.0", "hash", occurred_at)


def _check_result(
    event_id="check-1",
    run_id="run-1",
    *,
    severity="critical",
    status=CheckStatus.FAIL,
):
    return StoredCheckResult(
        event_id,
        run_id,
        "NullCheck",
        ContractScope.ROW,
        severity,
        status,
        "1",
        "0",
        "null",
    )


def _contract_definition(definition_hash="hash"):
    return ContractDefinition(
        "sales.orders",
        "1.0.0",
        definition_hash,
        "{}",
        "sales.orders",
        "data-team",
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )


def test_sqlite_store_is_usable_from_a_different_thread_than_the_one_that_created_it():
    # Model Serving builds the store on the init thread and queries it from request
    # worker threads — check_same_thread=False must be set (review bug_010).
    store = SqliteCertificationStore(":memory:")
    store.append_run_event(_event("PROMOTED"))
    errors = []

    def _query():
        try:
            store.get_certification("sales.orders")
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    thread = threading.Thread(target=_query)
    thread.start()
    thread.join()

    assert errors == []


def test_sqlite_get_contract_rebuilds_definition_and_returns_none_when_missing():
    store = SqliteCertificationStore(":memory:")
    definition = _contract_definition()
    store.register_contract(definition)

    assert store.get_contract("sales.orders", "1.0.0") == definition
    assert store.get_contract("missing", "1.0.0") is None


def test_sqlite_get_contract_refuses_ambiguous_version():
    store = SqliteCertificationStore(":memory:")
    store.register_contract(_contract_definition("hash-a"))
    store.register_contract(_contract_definition("hash-b"))

    with pytest.raises(ValueError, match="ambiguous"):
        store.get_contract("sales.orders", "1.0.0")


def test_sqlite_run_events_are_append_only_and_idempotent():
    store = SqliteCertificationStore(":memory:")
    store.append_run_event(_event())
    store.append_run_event(_event("PROMOTED"))
    run = store.get_run("run-1")
    assert run is not None
    assert run.state == "STARTED"


def test_sqlite_check_events_are_idempotent():
    store = SqliteCertificationStore(":memory:")
    result = StoredCheckResult("check-1", "run-1", "NullCheck", ContractScope.ROW, "critical", CheckStatus.FAIL, "1", "0", "null")
    store.append_check_results([result, result])
    assert store._conn.execute("SELECT COUNT(*) FROM check_results").fetchone()[0] == 1


def test_sqlite_get_check_results_rebuilds_typed_results():
    store = SqliteCertificationStore(":memory:")
    result = _check_result()
    store.append_check_results([result])

    rows = store.get_check_results("run-1")

    assert rows == [result]
    assert rows[0].scope is ContractScope.ROW
    assert rows[0].status is CheckStatus.FAIL


def test_sqlite_returns_latest_successful_promotion_only():
    store = SqliteCertificationStore(":memory:")
    store.append_run_event(_event("FAILED"))
    store.append_run_event(
        RunEvent("evt-2", "run-2", "sales.orders", "PROMOTED", "sales.orders", "1.0.0", "hash", datetime.now(timezone.utc))
    )
    assert store.get_latest_promoted("sales.orders").run_id == "run-2"


def test_sqlite_get_certification_marks_failed_critical_checks():
    store = SqliteCertificationStore(":memory:")
    store.append_run_event(_run_event("evt-2", "run-2", "PROMOTED", datetime.now(timezone.utc)))
    store.append_check_results([
        _check_result("check-1", "run-2", status=CheckStatus.FAIL),
    ])

    cert = store.get_certification("sales.orders")

    assert cert.status == "CERTIFIED"
    assert cert.checks_passed is False


def test_sqlite_get_certification_allows_non_critical_or_non_failed_checks():
    store = SqliteCertificationStore(":memory:")
    store.append_run_event(_run_event("evt-2", "run-2", "PROMOTED", datetime.now(timezone.utc)))
    store.append_check_results([
        _check_result("check-1", "run-2", severity="warning", status=CheckStatus.FAIL),
        _check_result("check-2", "run-2", severity="critical", status=CheckStatus.PASS),
    ])

    cert = store.get_certification("sales.orders")

    assert cert.status == "CERTIFIED"
    assert cert.checks_passed is True


def test_incident_roundtrip_sqlite():
    store = SqliteCertificationStore(":memory:")
    opened_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    incident = Incident(
        "run-1:NullCheck:id",
        "sales.orders",
        "run-1",
        "NullCheck:id",
        "critical",
        IncidentStatus.NEW,
        opened_at,
    )

    opened = store.open_incident(incident)
    acknowledged = store.update_incident(
        incident.id,
        IncidentStatus.ACKNOWLEDGED,
        at=datetime(2026, 1, 2, tzinfo=timezone.utc),
    )
    assigned = store.update_incident(
        incident.id,
        IncidentStatus.ASSIGNED,
        at=datetime(2026, 1, 3, tzinfo=timezone.utc),
        assignee="alice",
    )
    resolved = store.update_incident(
        incident.id,
        IncidentStatus.RESOLVED,
        at=datetime(2026, 1, 4, tzinfo=timezone.utc),
        root_cause="recovered",
    )

    assert opened == incident
    assert acknowledged.status is IncidentStatus.ACKNOWLEDGED
    assert assigned.assignee == "alice"
    assert resolved.status is IncidentStatus.RESOLVED
    assert resolved.root_cause == "recovered"
    assert resolved.resolved_at == datetime(2026, 1, 4, tzinfo=timezone.utc)
    reloaded = store.get_incident(incident.id)
    assert reloaded == resolved
    assert store.list_incidents(status=IncidentStatus.RESOLVED) == [resolved]
    assert store._conn.execute(
        "SELECT 1 FROM schema_migrations WHERE version = 2"
    ).fetchone() == (1,)


def test_delta_adapter_delegates_to_certification_specific_backend_methods():
    class Backend:
        def __init__(self): self.calls = []
        def append_certification_run(self, schema, row): self.calls.append((schema, row))
    backend = Backend()
    DeltaCertificationStore(backend).append_run_event(_event())
    assert backend.calls[0][0] == "_skifer_certification"
    assert backend.calls[0][1]["run_id"] == "run-1"


def test_delta_incident_adapter_roundtrip():
    class Backend:
        def __init__(self):
            self.rows = {}

        def upsert_incident(self, schema, row):
            self.rows[row["id"]] = dict(row)

        def get_open_incident(self, schema, target_fqn, check_name):
            for row in self.rows.values():
                if (
                    row["target_fqn"] == target_fqn
                    and row["check_name"] == check_name
                    and row["status"] != "RESOLVED"
                ):
                    return row
            return None

        def list_open_incidents(self, schema, target_fqn):
            return [
                row
                for row in self.rows.values()
                if row["target_fqn"] == target_fqn and row["status"] != "RESOLVED"
            ]

        def get_incident(self, schema, incident_id):
            return self.rows.get(incident_id)

        def list_incidents(self, schema, *, status=None, target_fqn=None, limit=50):
            rows = list(self.rows.values())
            if status is not None:
                rows = [row for row in rows if row["status"] == status]
            if target_fqn is not None:
                rows = [row for row in rows if row["target_fqn"] == target_fqn]
            return rows[:limit]

    store = DeltaCertificationStore(Backend())
    incident = Incident(
        "run-1:NullCheck:id",
        "sales.orders",
        "run-1",
        "NullCheck:id",
        "critical",
        IncidentStatus.NEW,
        datetime(2026, 1, 1, tzinfo=timezone.utc),
    )

    assert store.open_incident(incident) == incident
    assert store.open_incident(
        Incident(
            "run-2:NullCheck:id",
            "sales.orders",
            "run-2",
            "NullCheck:id",
            "critical",
            IncidentStatus.NEW,
            datetime(2026, 1, 2, tzinfo=timezone.utc),
        )
    ).id == incident.id
    resolved = store.resolve_open_incidents(
        "sales.orders",
        root_cause="recovered",
        resolved_at=datetime(2026, 1, 3, tzinfo=timezone.utc),
    )

    assert resolved[0].status is IncidentStatus.RESOLVED
    assert store.list_open_incidents("sales.orders") == []
    assert store.get_incident(incident.id).root_cause == "recovered"


def test_delta_get_contract_delegates_and_rebuilds_definition():
    definition = _contract_definition()

    class Backend:
        def get_certification_contract(self, schema, contract_id, version):
            assert (schema, contract_id, version) == (
                "_skifer_certification",
                "sales.orders",
                "1.0.0",
            )
            return DeltaCertificationStore._row(definition)

    result = DeltaCertificationStore(Backend()).get_contract("sales.orders", "1.0.0")

    assert result == definition


def test_delta_get_certification_returns_certified_after_promoted_run():
    class Backend:
        def __init__(self): self.rows = []
        def append_certification_run(self, schema, row): self.rows.append(row)
        def get_latest_certification_promotion(self, schema, dataset):
            rows = [r for r in self.rows if r["dataset"] == dataset and r["state"] == "PROMOTED"]
            return sorted(rows, key=lambda r: r["occurred_at"], reverse=True)[0] if rows else None
        def get_certification_check_results(self, schema, run_id): return []

    store = DeltaCertificationStore(Backend())
    promoted_at = datetime(2024, 1, 2, tzinfo=timezone.utc)
    store.append_run_event(_run_event("evt-2", "run-2", "PROMOTED", promoted_at))

    cert = store.get_certification("sales.orders", "agent_read")

    assert cert.status == "CERTIFIED"
    assert cert.consumer_class == "agent_read"
    assert cert.certified_at == promoted_at
    assert cert.checks_passed is True


def test_delta_get_certification_returns_uncertified_without_promotion():
    class Backend:
        def get_latest_certification_promotion(self, schema, dataset): return None

    cert = DeltaCertificationStore(Backend()).get_certification("sales.orders")

    assert cert.status == "UNCERTIFIED"
    assert cert.contract_version is None
    assert cert.certified_at is None
    assert cert.checks_passed is True


def test_delta_get_certification_marks_failed_critical_checks():
    class Backend:
        def get_latest_certification_promotion(self, schema, dataset):
            return _run_event(
                "evt-2",
                "run-2",
                "PROMOTED",
                datetime(2024, 1, 2, tzinfo=timezone.utc),
            ).__dict__

        def get_certification_check_results(self, schema, run_id):
            return [_check_result("check-1", run_id, status=CheckStatus.ERROR).__dict__]

    cert = DeltaCertificationStore(Backend()).get_certification("sales.orders")

    assert cert.status == "CERTIFIED"
    assert cert.checks_passed is False


def test_delta_get_certification_allows_non_critical_or_non_failed_checks():
    class Backend:
        def get_latest_certification_promotion(self, schema, dataset):
            return _run_event(
                "evt-2",
                "run-2",
                "PROMOTED",
                datetime(2024, 1, 2, tzinfo=timezone.utc),
            ).__dict__

        def get_certification_check_results(self, schema, run_id):
            return [
                _check_result(
                    "check-1",
                    run_id,
                    severity="warning",
                    status=CheckStatus.ERROR,
                ).__dict__,
                _check_result(
                    "check-2",
                    run_id,
                    severity="critical",
                    status=CheckStatus.PASS,
                ).__dict__,
            ]

    cert = DeltaCertificationStore(Backend()).get_certification("sales.orders")

    assert cert.status == "CERTIFIED"
    assert cert.checks_passed is True


def test_delta_list_history_returns_events_in_backend_order_with_datetime_conversion():
    class Backend:
        def list_certification_history(self, schema, dataset, limit):
            return [
                _run_event("evt-2", "run-2", "PROMOTED", datetime(2024, 1, 2, tzinfo=timezone.utc)).__dict__,
                {
                    **_run_event("evt-1", "run-1", "FAILED", datetime(2024, 1, 1, tzinfo=timezone.utc)).__dict__,
                    "occurred_at": "2024-01-01T00:00:00+00:00",
                },
            ]

    history = DeltaCertificationStore(Backend()).list_history("sales.orders")

    assert [event.run_id for event in history] == ["run-2", "run-1"]
    assert all(isinstance(event.occurred_at, datetime) for event in history)


def test_delta_get_check_results_rebuilds_typed_results():
    class Backend:
        def get_certification_check_results(self, schema, run_id):
            return [{
                "event_id": "check-1",
                "run_id": run_id,
                "check_type": "NullCheck",
                "scope": "ROW",
                "severity": "critical",
                "status": "FAIL",
                "actual_value": "1",
                "expected_value": "0",
                "message": "null",
            }]

    rows = DeltaCertificationStore(Backend()).get_check_results("run-1")

    assert rows == [StoredCheckResult("check-1", "run-1", "NullCheck", ContractScope.ROW, "critical", CheckStatus.FAIL, "1", "0", "null")]
    assert rows[0].scope is ContractScope.ROW
    assert rows[0].status is CheckStatus.FAIL


def test_uc_mirror_is_non_sensitive_and_permission_failure_is_non_blocking():
    class Backend:
        def execute_sql(self, sql): raise PermissionError("denied")
    result = mirror_certification(Backend(), "main.gold.orders", Certification("sales.orders", "agent", "CERTIFIED", "1.0.0", "hash", None), "team")
    assert result.status == "SYNC_ERROR"
