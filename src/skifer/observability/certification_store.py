"""Append-only persistence for Plan 29 certification events."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
import json
import sqlite3
from typing import Protocol, Sequence

from skifer.observability.certification import ContractDefinition
from skifer.observability.checks import CheckStatus, ContractScope
from skifer.observability.incidents import Incident, IncidentStatus


@dataclass(frozen=True)
class RunEvent:
    event_id: str
    run_id: str
    dataset: str
    state: str
    contract_id: str
    contract_version: str
    definition_hash: str
    occurred_at: datetime
    target_fqn: str | None = None
    staging_fqn: str | None = None
    quarantine_fqn: str | None = None


@dataclass(frozen=True)
class StoredCheckResult:
    event_id: str
    run_id: str
    check_type: str
    scope: ContractScope
    severity: str
    status: CheckStatus
    actual_value: str | None
    expected_value: str | None
    message: str


@dataclass(frozen=True)
class Certification:
    dataset: str
    consumer_class: str
    status: str
    contract_version: str | None
    definition_hash: str | None
    certified_at: datetime | None
    checks_passed: bool = True


class CertificationStore(Protocol):
    def register_contract(self, definition: ContractDefinition) -> None: ...
    def get_contract(self, contract_id: str, version: str) -> ContractDefinition | None: ...
    def append_run_event(self, event: RunEvent) -> None: ...
    def append_check_results(self, results: Sequence[StoredCheckResult]) -> None: ...
    def get_check_results(self, run_id: str) -> list[StoredCheckResult]: ...
    def get_run(self, run_id: str) -> RunEvent | None: ...
    def get_latest_promoted(self, dataset: str) -> RunEvent | None: ...
    def get_certification(self, dataset: str, consumer_class: str = "default") -> Certification: ...
    def list_history(self, dataset: str, limit: int = 50) -> list[RunEvent]: ...
    def open_incident(self, incident: Incident) -> Incident: ...
    def get_open_incident(self, target_fqn: str, check_name: str) -> Incident | None: ...
    def list_open_incidents(self, target_fqn: str) -> list[Incident]: ...
    def resolve_open_incidents(
        self, target_fqn: str, *, root_cause: str, resolved_at: datetime
    ) -> list[Incident]: ...
    def get_incident(self, incident_id: str) -> Incident | None: ...
    def update_incident(
        self,
        incident_id: str,
        new_status: IncidentStatus,
        *,
        at: datetime,
        assignee: str | None = None,
        root_cause: str | None = None,
    ) -> Incident: ...
    def list_incidents(
        self,
        *,
        status: str | None = None,
        target_fqn: str | None = None,
        limit: int = 50,
    ) -> list[Incident]: ...


def _coerce_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        return value
    if hasattr(value, "to_pydatetime"):
        return value.to_pydatetime()
    if isinstance(value, str):
        return datetime.fromisoformat(value)
    raise TypeError(f"Cannot deserialize datetime from {type(value).__name__}")


def _contract_definition_from_row(row: dict) -> ContractDefinition:
    """Rebuild only the canonical contract fields admitted by the store API."""
    created_at = row.get("created_at")
    return ContractDefinition(
        contract_id=row["contract_id"],
        contract_version=row["contract_version"],
        definition_hash=row["definition_hash"],
        canonical_json=row["canonical_json"],
        data_product_id=row["data_product_id"],
        owner=row.get("owner"),
        hash_algorithm=row.get("hash_algorithm", "sha256"),
        canonicalization_version=int(row.get("canonicalization_version", 1)),
        status=row.get("status", "DRAFT"),
        created_at=_coerce_datetime(created_at) if created_at is not None else None,
    )


def _run_event_from_row(row: Sequence | dict) -> RunEvent:
    if isinstance(row, dict):
        return RunEvent(
            event_id=row["event_id"],
            run_id=row["run_id"],
            dataset=row["dataset"],
            state=row["state"],
            contract_id=row["contract_id"],
            contract_version=row["contract_version"],
            definition_hash=row["definition_hash"],
            occurred_at=_coerce_datetime(row["occurred_at"]),
            target_fqn=row.get("target_fqn"),
            staging_fqn=row.get("staging_fqn"),
            quarantine_fqn=row.get("quarantine_fqn"),
        )
    return RunEvent(*row[:7], _coerce_datetime(row[7]), *row[8:])


def _check_result_from_row(row: Sequence | dict) -> StoredCheckResult:
    if isinstance(row, dict):
        return StoredCheckResult(
            event_id=row["event_id"],
            run_id=row["run_id"],
            check_type=row["check_type"],
            scope=ContractScope(row["scope"]),
            severity=row["severity"],
            status=CheckStatus(row["status"]),
            actual_value=row.get("actual_value"),
            expected_value=row.get("expected_value"),
            message=row["message"],
        )
    return StoredCheckResult(
        row[0],
        row[1],
        row[2],
        ContractScope(row[3]),
        row[4],
        CheckStatus(row[5]),
        row[6],
        row[7],
        row[8],
    )


def _incident_from_row(row: Sequence | dict) -> Incident:
    if isinstance(row, dict):
        resolved_at = row.get("resolved_at")
        return Incident(
            id=row["id"],
            target_fqn=row["target_fqn"],
            run_id=row["run_id"],
            check_name=row["check_name"],
            severity=row["severity"],
            status=IncidentStatus(row["status"]),
            opened_at=_coerce_datetime(row["opened_at"]),
            assignee=row.get("assignee"),
            root_cause=row.get("root_cause"),
            resolved_at=_coerce_datetime(resolved_at) if resolved_at else None,
        )
    return Incident(
        id=row[0],
        target_fqn=row[1],
        run_id=row[2],
        check_name=row[3],
        severity=row[4],
        status=IncidentStatus(row[5]),
        opened_at=_coerce_datetime(row[6]),
        assignee=row[7],
        root_cause=row[8],
        resolved_at=_coerce_datetime(row[9]) if row[9] else None,
    )


class SqliteCertificationStore:
    """Local certification store; rows are immutable and event IDs are idempotency keys."""

    def __init__(self, db_path: str = ".skifer_certification.db"):
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._migrate()

    def _migrate(self) -> None:
        self._conn.executescript("""
        CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY);
        CREATE TABLE IF NOT EXISTS contract_definitions (
          contract_id TEXT, contract_version TEXT, definition_hash TEXT, payload TEXT NOT NULL,
          PRIMARY KEY (contract_id, contract_version, definition_hash));
        CREATE TABLE IF NOT EXISTS materialization_runs (
          event_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, dataset TEXT NOT NULL, state TEXT NOT NULL,
          contract_id TEXT NOT NULL, contract_version TEXT NOT NULL, definition_hash TEXT NOT NULL,
          occurred_at TEXT NOT NULL, target_fqn TEXT, staging_fqn TEXT, quarantine_fqn TEXT);
        CREATE TABLE IF NOT EXISTS check_results (
          event_id TEXT PRIMARY KEY, run_id TEXT NOT NULL, check_type TEXT NOT NULL, scope TEXT NOT NULL,
          severity TEXT NOT NULL, status TEXT NOT NULL, actual_value TEXT, expected_value TEXT, message TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS incidents (
          id TEXT PRIMARY KEY, target_fqn TEXT NOT NULL, run_id TEXT NOT NULL,
          check_name TEXT NOT NULL, severity TEXT NOT NULL, status TEXT NOT NULL,
          opened_at TEXT NOT NULL, assignee TEXT, root_cause TEXT, resolved_at TEXT);
        CREATE INDEX IF NOT EXISTS idx_incidents_open ON incidents(target_fqn, check_name, status);
        INSERT OR IGNORE INTO schema_migrations(version) VALUES (1);
        INSERT OR IGNORE INTO schema_migrations(version) VALUES (2);
        """)
        self._conn.commit()

    def register_contract(self, definition: ContractDefinition) -> None:
        self._conn.execute(
            "INSERT OR IGNORE INTO contract_definitions VALUES (?, ?, ?, ?)",
            (definition.contract_id, definition.contract_version, definition.definition_hash,
             json.dumps(asdict(definition), default=str, sort_keys=True)),
        )
        self._conn.commit()

    def get_contract(self, contract_id: str, version: str) -> ContractDefinition | None:
        rows = self._conn.execute(
            "SELECT payload FROM contract_definitions "
            "WHERE contract_id = ? AND contract_version = ? LIMIT 2",
            (contract_id, version),
        ).fetchall()
        if not rows:
            return None
        if len(rows) != 1:
            raise ValueError(
                f"Contract '{contract_id}' version '{version}' has ambiguous definitions."
            )
        payload = json.loads(rows[0][0])
        if not isinstance(payload, dict):
            raise ValueError("Stored contract payload must be an object.")
        return _contract_definition_from_row(payload)

    def append_run_event(self, event: RunEvent) -> None:
        self._conn.execute(
            "INSERT OR IGNORE INTO materialization_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (event.event_id, event.run_id, event.dataset, event.state, event.contract_id,
             event.contract_version, event.definition_hash, event.occurred_at.isoformat(),
             event.target_fqn, event.staging_fqn, event.quarantine_fqn),
        )
        self._conn.commit()

    def append_check_results(self, results: Sequence[StoredCheckResult]) -> None:
        self._conn.executemany(
            "INSERT OR IGNORE INTO check_results VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [(r.event_id, r.run_id, r.check_type, r.scope.value, r.severity, r.status.value,
              r.actual_value, r.expected_value, r.message) for r in results],
        )
        self._conn.commit()

    def get_run(self, run_id: str) -> RunEvent | None:
        row = self._conn.execute(
            "SELECT event_id, run_id, dataset, state, contract_id, contract_version, definition_hash, occurred_at, target_fqn, staging_fqn, quarantine_fqn "
            "FROM materialization_runs WHERE run_id = ? ORDER BY occurred_at DESC LIMIT 1", (run_id,)
        ).fetchone()
        if row is None:
            return None
        return _run_event_from_row(row)

    def get_latest_promoted(self, dataset: str) -> RunEvent | None:
        row = self._conn.execute(
            "SELECT event_id, run_id, dataset, state, contract_id, contract_version, definition_hash, occurred_at, target_fqn, staging_fqn, quarantine_fqn "
            "FROM materialization_runs WHERE dataset = ? AND state = 'PROMOTED' ORDER BY occurred_at DESC LIMIT 1", (dataset,)
        ).fetchone()
        return _run_event_from_row(row) if row else None

    def get_certification(self, dataset: str, consumer_class: str = "default") -> Certification:
        event = self.get_latest_promoted(dataset)
        checks_passed = True
        if event:
            results = self.get_check_results(event.run_id)
            checks_passed = not any(
                r.status in (CheckStatus.FAIL, CheckStatus.ERROR)
                and r.severity == "critical"
                for r in results
            )
        return Certification(dataset, consumer_class, "CERTIFIED" if event else "UNCERTIFIED",
                             event.contract_version if event else None, event.definition_hash if event else None,
                             event.occurred_at if event else None, checks_passed=checks_passed)

    def list_history(self, dataset: str, limit: int = 50) -> list[RunEvent]:
        rows = self._conn.execute(
            "SELECT event_id, run_id, dataset, state, contract_id, contract_version, definition_hash, occurred_at, target_fqn, staging_fqn, quarantine_fqn FROM materialization_runs WHERE dataset = ? ORDER BY occurred_at DESC LIMIT ?",
            (dataset, limit),
        ).fetchall()
        return [_run_event_from_row(row) for row in rows]

    def get_check_results(self, run_id: str) -> list[StoredCheckResult]:
        rows = self._conn.execute(
            "SELECT event_id, run_id, check_type, scope, severity, status, actual_value, expected_value, message "
            "FROM check_results WHERE run_id = ?",
            (run_id,),
        ).fetchall()
        return [_check_result_from_row(row) for row in rows]

    def open_incident(self, incident: Incident) -> Incident:
        existing = self.get_open_incident(incident.target_fqn, incident.check_name)
        if existing is not None:
            return existing
        self._conn.execute(
            "INSERT OR IGNORE INTO incidents "
            "(id, target_fqn, run_id, check_name, severity, status, opened_at, assignee, root_cause, resolved_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                incident.id,
                incident.target_fqn,
                incident.run_id,
                incident.check_name,
                incident.severity,
                incident.status.value,
                incident.opened_at.isoformat(),
                incident.assignee,
                incident.root_cause,
                incident.resolved_at.isoformat() if incident.resolved_at else None,
            ),
        )
        self._conn.commit()
        return self.get_incident(incident.id) or incident

    def get_open_incident(self, target_fqn: str, check_name: str) -> Incident | None:
        row = self._conn.execute(
            "SELECT id, target_fqn, run_id, check_name, severity, status, opened_at, assignee, root_cause, resolved_at "
            "FROM incidents WHERE target_fqn = ? AND check_name = ? AND status != ? "
            "ORDER BY opened_at DESC LIMIT 1",
            (target_fqn, check_name, IncidentStatus.RESOLVED.value),
        ).fetchone()
        return _incident_from_row(row) if row else None

    def list_open_incidents(self, target_fqn: str) -> list[Incident]:
        rows = self._conn.execute(
            "SELECT id, target_fqn, run_id, check_name, severity, status, opened_at, assignee, root_cause, resolved_at "
            "FROM incidents WHERE target_fqn = ? AND status != ? ORDER BY opened_at DESC",
            (target_fqn, IncidentStatus.RESOLVED.value),
        ).fetchall()
        return [_incident_from_row(row) for row in rows]

    def resolve_open_incidents(
        self, target_fqn: str, *, root_cause: str, resolved_at: datetime
    ) -> list[Incident]:
        updated = [
            incident.transition(
                IncidentStatus.RESOLVED, at=resolved_at, root_cause=root_cause
            )
            for incident in self.list_open_incidents(target_fqn)
        ]
        self._conn.executemany(
            "UPDATE incidents SET status = ?, assignee = ?, root_cause = ?, resolved_at = ? WHERE id = ?",
            [
                (
                    incident.status.value,
                    incident.assignee,
                    incident.root_cause,
                    incident.resolved_at.isoformat() if incident.resolved_at else None,
                    incident.id,
                )
                for incident in updated
            ],
        )
        self._conn.commit()
        return updated

    def get_incident(self, incident_id: str) -> Incident | None:
        row = self._conn.execute(
            "SELECT id, target_fqn, run_id, check_name, severity, status, opened_at, assignee, root_cause, resolved_at "
            "FROM incidents WHERE id = ?",
            (incident_id,),
        ).fetchone()
        return _incident_from_row(row) if row else None

    def update_incident(
        self,
        incident_id: str,
        new_status: IncidentStatus,
        *,
        at: datetime,
        assignee: str | None = None,
        root_cause: str | None = None,
    ) -> Incident:
        current = self.get_incident(incident_id)
        if current is None:
            raise KeyError(incident_id)
        updated = current.transition(
            new_status, at=at, assignee=assignee, root_cause=root_cause
        )
        self._conn.execute(
            "UPDATE incidents SET status = ?, assignee = ?, root_cause = ?, resolved_at = ? WHERE id = ?",
            (
                updated.status.value,
                updated.assignee,
                updated.root_cause,
                updated.resolved_at.isoformat() if updated.resolved_at else None,
                incident_id,
            ),
        )
        self._conn.commit()
        return updated

    def list_incidents(
        self,
        *,
        status: str | None = None,
        target_fqn: str | None = None,
        limit: int = 50,
    ) -> list[Incident]:
        clauses = []
        values: list[object] = []
        if status is not None:
            status_value = status.value if hasattr(status, "value") else status
            clauses.append("status = ?")
            values.append(status_value)
        if target_fqn is not None:
            clauses.append("target_fqn = ?")
            values.append(target_fqn)
        query = (
            "SELECT id, target_fqn, run_id, check_name, severity, status, opened_at, assignee, root_cause, resolved_at "
            "FROM incidents"
        )
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY opened_at DESC LIMIT ?"
        values.append(limit)
        rows = self._conn.execute(query, values).fetchall()
        return [_incident_from_row(row) for row in rows]

    def close(self) -> None:
        self._conn.close()


class DeltaCertificationStore:
    """Small adapter for a backend exposing certification-specific append operations.

    Spark/Databricks wiring lands with the backend primitives; this class keeps
    the store contract testable without broadening the runtime backend API.
    """

    def __init__(self, backend, schema: str = "_skifer_certification"):
        self.backend, self.schema = backend, schema

    @staticmethod
    def _row(value: object) -> dict:
        row = asdict(value)
        return {
            key: item.value if hasattr(item, "value") else item.isoformat() if hasattr(item, "isoformat") else item
            for key, item in row.items()
        }

    def register_contract(self, definition: ContractDefinition) -> None:
        self.backend.append_certification_contract(self.schema, self._row(definition))

    def get_contract(self, contract_id: str, version: str) -> ContractDefinition | None:
        row = self.backend.get_certification_contract(self.schema, contract_id, version)
        return _contract_definition_from_row(row) if row else None

    def append_run_event(self, event: RunEvent) -> None:
        self.backend.append_certification_run(self.schema, self._row(event))

    def append_check_results(self, results: Sequence[StoredCheckResult]) -> None:
        for result in results:
            self.backend.append_certification_check(self.schema, self._row(result))

    def get_run(self, run_id: str) -> RunEvent | None:
        row = self.backend.get_certification_run(self.schema, run_id)
        if row is None:
            return None
        return _run_event_from_row(row)

    def get_latest_promoted(self, dataset: str) -> RunEvent | None:
        row = self.backend.get_latest_certification_promotion(self.schema, dataset)
        return _run_event_from_row(row) if row else None

    def get_certification(self, dataset: str, consumer_class: str = "default") -> Certification:
        event = self.get_latest_promoted(dataset)
        checks_passed = True
        if event:
            results = self.get_check_results(event.run_id)
            checks_passed = not any(
                r.status in (CheckStatus.FAIL, CheckStatus.ERROR)
                and r.severity == "critical"
                for r in results
            )
        return Certification(dataset, consumer_class, "CERTIFIED" if event else "UNCERTIFIED",
                             event.contract_version if event else None, event.definition_hash if event else None,
                             event.occurred_at if event else None, checks_passed=checks_passed)

    def list_history(self, dataset: str, limit: int = 50) -> list[RunEvent]:
        rows = self.backend.list_certification_history(self.schema, dataset, limit)
        return [_run_event_from_row(row) for row in rows]

    def get_check_results(self, run_id: str) -> list[StoredCheckResult]:
        rows = self.backend.get_certification_check_results(self.schema, run_id)
        return [_check_result_from_row(row) for row in rows]

    def open_incident(self, incident: Incident) -> Incident:
        existing = self.get_open_incident(incident.target_fqn, incident.check_name)
        if existing is not None:
            return existing
        self.backend.upsert_incident(self.schema, self._row(incident))
        return self.get_incident(incident.id) or incident

    def get_open_incident(self, target_fqn: str, check_name: str) -> Incident | None:
        row = self.backend.get_open_incident(self.schema, target_fqn, check_name)
        return _incident_from_row(row) if row else None

    def list_open_incidents(self, target_fqn: str) -> list[Incident]:
        rows = self.backend.list_open_incidents(self.schema, target_fqn)
        return [_incident_from_row(row) for row in rows]

    def resolve_open_incidents(
        self, target_fqn: str, *, root_cause: str, resolved_at: datetime
    ) -> list[Incident]:
        resolved = []
        for incident in self.list_open_incidents(target_fqn):
            resolved.append(
                self.update_incident(
                    incident.id,
                    IncidentStatus.RESOLVED,
                    at=resolved_at,
                    root_cause=root_cause,
                )
            )
        return resolved

    def get_incident(self, incident_id: str) -> Incident | None:
        row = self.backend.get_incident(self.schema, incident_id)
        return _incident_from_row(row) if row else None

    def update_incident(
        self,
        incident_id: str,
        new_status: IncidentStatus,
        *,
        at: datetime,
        assignee: str | None = None,
        root_cause: str | None = None,
    ) -> Incident:
        current = self.get_incident(incident_id)
        if current is None:
            raise KeyError(incident_id)
        updated = current.transition(
            new_status, at=at, assignee=assignee, root_cause=root_cause
        )
        self.backend.upsert_incident(self.schema, self._row(updated))
        return updated

    def list_incidents(
        self,
        *,
        status: str | None = None,
        target_fqn: str | None = None,
        limit: int = 50,
    ) -> list[Incident]:
        status_value = status.value if hasattr(status, "value") else status
        rows = self.backend.list_incidents(
            self.schema, status=status_value, target_fqn=target_fqn, limit=limit
        )
        return [_incident_from_row(row) for row in rows]
