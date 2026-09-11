"""Transport-neutral quality definitions and report history."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from skifer.observability.checks import CheckResult, DataContract
from skifer.observability.contracts import ContractExtractor
from skifer.observability.incidents import IncidentStatus
from skifer.observability.monitor import MonitorReport
from skifer.services.context import (
    InvalidRequest,
    RequestContext,
    ResourceNotFound,
    ResourceUnavailable,
    SCOPE_CONTRACTS_READ,
    SCOPE_INCIDENTS_WRITE,
    require_scope,
)
from skifer.services.serialization import to_json_value


@dataclass(frozen=True)
class CheckDefinitionView:
    name: str
    kind: str
    column: str | None
    params: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "kind": self.kind,
            "column": self.column,
            "params": to_json_value(self.params, "params"),
        }


@dataclass(frozen=True)
class CheckRunView:
    check_type: str
    severity: str
    passed: bool
    status: str
    actual_value: Any
    expected_value: Any
    message: str
    run_at: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "check_type": self.check_type,
            "severity": self.severity,
            "passed": self.passed,
            "status": self.status,
            "actual_value": to_json_value(self.actual_value, "actual_value"),
            "expected_value": to_json_value(self.expected_value, "expected_value"),
            "message": self.message,
            "run_at": self.run_at,
        }


@dataclass(frozen=True)
class QualityReportView:
    table: str
    passed: bool
    checks: tuple[dict[str, Any], ...]
    run_at: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "passed": self.passed,
            "checks": [to_json_value(check, "checks") for check in self.checks],
            "run_at": self.run_at,
        }


@dataclass(frozen=True)
class IncidentView:
    id: str
    target_fqn: str
    check_name: str
    severity: str
    status: str
    assignee: str | None
    root_cause: str | None
    opened_at: str
    resolved_at: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "target_fqn": self.target_fqn,
            "check_name": self.check_name,
            "severity": self.severity,
            "status": self.status,
            "assignee": self.assignee,
            "root_cause": self.root_cause,
            "opened_at": self.opened_at,
            "resolved_at": self.resolved_at,
        }


class QualityService:
    """Service over quality definitions, report history, and incident transitions."""

    def __init__(self, history_store, *, contract_extractor=None, incident_store=None):
        self._history = history_store
        self._extractor = contract_extractor or ContractExtractor()
        self._incidents = incident_store

    def checks_from_schema(
        self, ctx: RequestContext, schema_dict: dict
    ) -> tuple[CheckDefinitionView, ...]:
        require_scope(ctx, SCOPE_CONTRACTS_READ)
        extractor = getattr(self._extractor, "extract", None)
        if extractor is None or not callable(extractor):
            raise ResourceUnavailable("The contract extractor is unavailable.")
        contracts = extractor(schema_dict)
        if not isinstance(contracts, (list, tuple)) or not all(
            isinstance(contract, DataContract) for contract in contracts
        ):
            raise ResourceUnavailable("The contract extractor returned invalid checks.")
        return tuple(_definition_view(contract) for contract in contracts)

    def history(
        self, ctx: RequestContext, table: str, limit: int = 20
    ) -> tuple[QualityReportView, ...]:
        require_scope(ctx, SCOPE_CONTRACTS_READ)
        reader = self._history_reader("get_last_n")
        reports = reader(table, limit)
        if not isinstance(reports, (list, tuple)) or not all(
            isinstance(report, MonitorReport) for report in reports
        ):
            raise ResourceUnavailable("The history store returned invalid reports.")
        return tuple(_report_view(report) for report in reports)

    def last_report(
        self, ctx: RequestContext, table: str
    ) -> QualityReportView | None:
        require_scope(ctx, SCOPE_CONTRACTS_READ)
        reader = self._history_reader("get_latest")
        report = reader(table)
        if report is None:
            return None
        if not isinstance(report, MonitorReport):
            raise ResourceUnavailable("The history store returned an invalid report.")
        return _report_view(report)

    def list_incidents(
        self,
        ctx: RequestContext,
        *,
        status: str | None = None,
        target_fqn: str | None = None,
        limit: int = 50,
    ) -> tuple[IncidentView, ...]:
        require_scope(ctx, SCOPE_CONTRACTS_READ)
        return tuple(
            _incident_view(incident)
            for incident in self._require_incidents().list_incidents(
                status=status,
                target_fqn=target_fqn,
                limit=limit,
            )
        )

    def acknowledge_incident(self, ctx: RequestContext, incident_id: str) -> IncidentView:
        require_scope(ctx, SCOPE_INCIDENTS_WRITE)
        return self._transition(incident_id, IncidentStatus.ACKNOWLEDGED)

    def assign_incident(
        self, ctx: RequestContext, incident_id: str, assignee: str
    ) -> IncidentView:
        require_scope(ctx, SCOPE_INCIDENTS_WRITE)
        if not assignee:
            raise InvalidRequest("assignee is required.")
        return self._transition(
            incident_id, IncidentStatus.ASSIGNED, assignee=assignee
        )

    def resolve_incident(
        self, ctx: RequestContext, incident_id: str, root_cause: str
    ) -> IncidentView:
        require_scope(ctx, SCOPE_INCIDENTS_WRITE)
        if not root_cause:
            raise InvalidRequest("root_cause is required.")
        return self._transition(
            incident_id, IncidentStatus.RESOLVED, root_cause=root_cause
        )

    def _transition(
        self,
        incident_id: str,
        new_status: IncidentStatus,
        *,
        assignee: str | None = None,
        root_cause: str | None = None,
    ) -> IncidentView:
        store = self._require_incidents()
        try:
            updated = store.update_incident(
                incident_id,
                new_status,
                at=datetime.now(timezone.utc),
                assignee=assignee,
                root_cause=root_cause,
            )
        except KeyError as exc:
            raise ResourceNotFound(f"Incident '{incident_id}' not found.") from exc
        return _incident_view(updated)

    def _require_incidents(self):
        if self._incidents is None:
            raise ResourceUnavailable("Incident store not configured.")
        return self._incidents

    def _history_reader(self, name: str):
        if self._history is None:
            raise ResourceUnavailable("The history store is unavailable.")
        reader = getattr(self._history, name, None)
        if reader is None or not callable(reader):
            raise ResourceUnavailable(
                f"The history store does not support '{name}'."
            )
        return reader


def _definition_view(contract: DataContract) -> CheckDefinitionView:
    kind = type(contract).__name__
    column = getattr(contract, "column", None)
    if column is None:
        column = getattr(contract, "timestamp_column", None)
    params: dict[str, Any] = {
        "table": contract.table,
        "severity": contract.severity,
    }
    for field_name in (
        "columns",
        "expected_type",
        "operator",
        "value",
        "max_delay",
        "min_rows",
        "max_rows",
        "variation_threshold",
        "previous_count",
        "expected_columns",
        "sql",
        "expect",
    ):
        if hasattr(contract, field_name):
            params[field_name] = to_json_value(
                getattr(contract, field_name), f"params.{field_name}"
            )
    return CheckDefinitionView(name=kind, kind=kind, column=column, params=params)


def _report_view(report: MonitorReport) -> QualityReportView:
    checks = tuple(_check_run_view(result).to_dict() for result in report.results)
    return QualityReportView(
        table=report.table,
        passed=all(result.passed for result in report.results),
        checks=checks,
        run_at=report.timestamp.isoformat() if report.timestamp is not None else None,
    )


def _check_run_view(result: CheckResult) -> CheckRunView:
    status = result.status.value if isinstance(result.status, Enum) else str(result.status)
    return CheckRunView(
        check_type=getattr(result.contract, "check_type", type(result.contract).__name__),
        severity=result.severity,
        passed=result.passed,
        status=status,
        actual_value=result.actual_value,
        expected_value=result.expected_value,
        message=result.message,
        run_at=result.timestamp.isoformat() if result.timestamp is not None else None,
    )


def _incident_view(incident) -> IncidentView:
    status = (
        incident.status.value
        if isinstance(incident.status, IncidentStatus)
        else str(incident.status)
    )
    opened_at = incident.opened_at.isoformat()
    resolved_at = (
        incident.resolved_at.isoformat()
        if incident.resolved_at is not None
        else None
    )
    return IncidentView(
        id=incident.id,
        target_fqn=incident.target_fqn,
        check_name=incident.check_name,
        severity=incident.severity,
        status=status,
        assignee=incident.assignee,
        root_cause=incident.root_cause,
        opened_at=opened_at,
        resolved_at=resolved_at,
    )


__all__ = [
    "CheckDefinitionView",
    "CheckRunView",
    "IncidentView",
    "QualityReportView",
    "QualityService",
]
