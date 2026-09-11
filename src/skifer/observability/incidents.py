"""Mutable incident records for certified publication failures."""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from enum import Enum


class IncidentStatus(str, Enum):
    NEW = "NEW"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    ASSIGNED = "ASSIGNED"
    RESOLVED = "RESOLVED"


_ALLOWED: dict[IncidentStatus, frozenset[IncidentStatus]] = {
    IncidentStatus.NEW: frozenset({
        IncidentStatus.ACKNOWLEDGED,
        IncidentStatus.ASSIGNED,
        IncidentStatus.RESOLVED,
    }),
    IncidentStatus.ACKNOWLEDGED: frozenset({
        IncidentStatus.ASSIGNED,
        IncidentStatus.RESOLVED,
    }),
    IncidentStatus.ASSIGNED: frozenset({
        IncidentStatus.ASSIGNED,
        IncidentStatus.RESOLVED,
    }),
    IncidentStatus.RESOLVED: frozenset(),
}


class InvalidIncidentTransition(ValueError):
    """Raised when an incident transition is not allowed."""


@dataclass(frozen=True)
class Incident:
    id: str
    target_fqn: str
    run_id: str
    check_name: str
    severity: str
    status: IncidentStatus
    opened_at: datetime
    assignee: str | None = None
    root_cause: str | None = None
    resolved_at: datetime | None = None

    def transition(
        self,
        new_status: IncidentStatus,
        *,
        at: datetime,
        assignee: str | None = None,
        root_cause: str | None = None,
    ) -> "Incident":
        current_status = IncidentStatus(self.status)
        new_status = IncidentStatus(new_status)
        if new_status not in _ALLOWED[current_status]:
            raise InvalidIncidentTransition(
                f"Illegal incident transition {current_status.value} -> {new_status.value}."
            )
        resolved_at = at if new_status is IncidentStatus.RESOLVED else self.resolved_at
        return replace(
            self,
            status=new_status,
            assignee=assignee if assignee is not None else self.assignee,
            root_cause=root_cause if root_cause is not None else self.root_cause,
            resolved_at=resolved_at,
        )


def incident_check_name(contract) -> str:
    """Return a stable redacted check name: contract type plus optional column."""
    column = getattr(contract, "column", None)
    return f"{type(contract).__name__}:{column}" if column else type(contract).__name__


def incidents_from_report(report, *, run_id: str, target_fqn: str, at: datetime) -> list[Incident]:
    """Build one NEW incident per failed critical check, deduped within the report."""
    seen: set[str] = set()
    out: list[Incident] = []
    for result in report.results:
        status = getattr(result.status, "value", result.status)
        if result.passed or result.severity != "critical" or status == "SKIPPED":
            continue
        name = incident_check_name(result.contract)
        if name in seen:
            continue
        seen.add(name)
        out.append(
            Incident(
                id=f"{run_id}:{name}",
                target_fqn=target_fqn,
                run_id=run_id,
                check_name=name,
                severity=result.severity,
                status=IncidentStatus.NEW,
                opened_at=at,
            )
        )
    return out
