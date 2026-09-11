"""Mutable incident records for certified publication failures."""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from enum import Enum
import inspect
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from skifer.observability.alerts import AlertDispatcher


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


@dataclass(frozen=True)
class Recipient:
    contact: str
    team: str | None = None
    via: str = "email"
    source_fqn: str | None = None


@dataclass(frozen=True)
class _BreakingChangeAlert:
    id: str
    check_name: str
    severity: str
    from_version: str | None = None
    to_version: str | None = None


class AlertRouter:
    """Resolve alert recipients from governed ownership and downstream lineage."""

    def __init__(self, governance, dispatcher: "AlertDispatcher", *, max_depth: int = 3):
        self._gov = governance
        self._dispatcher = dispatcher
        self._max_depth = max(0, int(max_depth))

    def resolve_recipients(self, ctx, target_fqn: str) -> list[Recipient]:
        recipients: list[Recipient] = []
        seen: set[tuple[str, str]] = set()

        def add(owner, source_fqn: str) -> None:
            recipient = _recipient_from_owner(owner, source_fqn=source_fqn)
            if recipient is None:
                return
            key = (recipient.contact, recipient.via)
            if key in seen:
                return
            seen.add(key)
            recipients.append(recipient)

        add(self._owner_for(ctx, target_fqn), target_fqn)

        edges = self._downstream_edges(ctx, target_fqn)
        for downstream_fqn in _bounded_downstream_datasets(
            edges,
            root_fqn=target_fqn,
            max_depth=self._max_depth,
        ):
            add(self._owner_for(ctx, downstream_fqn), downstream_fqn)

        return recipients

    def alert_incident(self, ctx, *, target_fqn: str, incidents, config: dict) -> list[str]:
        recipients = self.resolve_recipients(ctx, target_fqn)
        return self._dispatcher.dispatch_incident(
            target_fqn=target_fqn,
            incidents=incidents,
            recipients=recipients,
            config=config,
        )

    def alert_breaking_change(self, ctx, *, target_fqn: str, diff, config: dict) -> list[str]:
        if not getattr(diff, "breaking", False):
            return []
        recipients = self.resolve_recipients(ctx, target_fqn)
        from_version = _first_attr(
            diff,
            "from_version",
            "old_version",
            "previous_version",
            "source_version",
        )
        to_version = _first_attr(diff, "to_version", "new_version", "next_version", "target_version")
        alert = _BreakingChangeAlert(
            id=f"breaking_contract_change:{from_version or 'unknown'}:{to_version or 'unknown'}",
            check_name="breaking contract change",
            severity="critical",
            from_version=from_version,
            to_version=to_version,
        )
        return self._dispatcher.dispatch_incident(
            target_fqn=target_fqn,
            incidents=[alert],
            recipients=recipients,
            config=config,
        )

    def _owner_for(self, ctx, target_fqn: str):
        for name in (
            "get_dataset_owner",
            "registry_owner",
            "get_owner",
            "dataset_owner",
        ):
            method = getattr(self._gov, name, None)
            if not callable(method):
                continue
            owner = _owner_payload(_call_context_method(method, ctx, target_fqn))
            if owner is not None:
                return owner

        record = self._record_for(ctx, target_fqn)
        return _owner_payload(record)

    def _record_for(self, ctx, target_fqn: str):
        for name in (
            "get_dataset",
            "get_metadata_record",
            "registry_get",
            "dataset_record",
        ):
            method = getattr(self._gov, name, None)
            if not callable(method):
                continue
            record = _call_context_method(method, ctx, target_fqn)
            if record is not None:
                return record

        registry = getattr(self._gov, "_registry", None)
        store = getattr(registry, "_store", None)
        get = getattr(store, "get", None)
        if callable(get):
            return get(target_fqn)

        metadata_store = getattr(self._gov, "_metadata_store", None)
        get = getattr(metadata_store, "get", None)
        if callable(get):
            return get(target_fqn)

        return None

    def _downstream_edges(self, ctx, target_fqn: str) -> list:
        columns = _columns_for(self._record_for(ctx, target_fqn))
        edges: list = []
        downstream = getattr(self._gov, "registry_downstream", None)
        if callable(downstream) and columns:
            seen: set[tuple[str | None, str | None, str | None, str | None]] = set()
            for column in sorted(set(columns)):
                for edge in self._call_downstream(downstream, ctx, target_fqn, column):
                    key = _edge_key(edge)
                    if key in seen:
                        continue
                    seen.add(key)
                    edges.append(edge)
            return edges

        impact = getattr(self._gov, "registry_impact", None)
        if callable(impact):
            return _extract_edges(_call_context_method(impact, ctx, target_fqn))

        if callable(downstream):
            return list(self._call_downstream(downstream, ctx, target_fqn, "*"))

        return []

    def _call_downstream(self, downstream, ctx, target_fqn: str, column: str):
        try:
            parameters = inspect.signature(downstream).parameters
        except (TypeError, ValueError):
            parameters = {}
        supports_max_depth = (
            "max_depth" in parameters
            or any(p.kind is inspect.Parameter.VAR_KEYWORD for p in parameters.values())
        )
        if supports_max_depth:
            return downstream(ctx, target_fqn, column, max_depth=self._max_depth)
        return downstream(ctx, target_fqn, column)


def _call_context_method(method, ctx, target_fqn: str):
    try:
        inspect.signature(method).bind(ctx, target_fqn)
    except (TypeError, ValueError):
        return method(target_fqn)
    return method(ctx, target_fqn)


def _owner_payload(value):
    if value is None:
        return None
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        value = to_dict()
    if isinstance(value, dict):
        if any(key in value for key in ("contact", "team", "steward", "via")):
            return value
        if "owner" in value:
            return _owner_payload(value["owner"])
        return None
    owner = getattr(value, "owner", None)
    if owner is not None:
        return _owner_payload(owner)
    contact = getattr(value, "contact", None)
    team = getattr(value, "team", None)
    steward = getattr(value, "steward", None)
    via = getattr(value, "via", None)
    if contact or team or steward or via:
        return {
            "contact": contact,
            "team": team,
            "steward": steward,
            "via": via,
        }
    if isinstance(value, str) and value:
        return {"contact": value}
    return None


def _recipient_from_owner(owner, *, source_fqn: str) -> Recipient | None:
    payload = _owner_payload(owner)
    if payload is None:
        return None
    contact = payload.get("contact")
    if not isinstance(contact, str) or not contact.strip():
        return None
    team = payload.get("team") or payload.get("steward")
    via = payload.get("via") or "email"
    return Recipient(
        contact=contact.strip(),
        team=team.strip() if isinstance(team, str) and team.strip() else None,
        via=via.strip() if isinstance(via, str) and via.strip() else "email",
        source_fqn=source_fqn,
    )


def _columns_for(record) -> list[str]:
    if record is None:
        return []
    if isinstance(record, dict):
        columns = record.get("columns", [])
    else:
        columns = getattr(record, "columns", [])
    out: list[str] = []
    for column in columns or []:
        name = column.get("name") if isinstance(column, dict) else getattr(column, "name", None)
        if isinstance(name, str) and name:
            out.append(name)
    return out


def _extract_edges(value) -> list:
    if value is None:
        return []
    if isinstance(value, dict):
        edges = value.get("edges", [])
    else:
        edges = getattr(value, "edges", value)
    return list(edges or [])


def _edge_key(edge) -> tuple[str | None, str | None, str | None, str | None]:
    return (
        _edge_value(edge, "source_table", "source_fqn"),
        _edge_value(edge, "source_column", "column"),
        _edge_value(edge, "target_table", "target_fqn"),
        _edge_value(edge, "target_column", "column"),
    )


def _bounded_downstream_datasets(edges: list, *, root_fqn: str, max_depth: int) -> list[str]:
    if max_depth <= 0:
        return []
    edge_list = _extract_edges(edges)
    depths: dict[str, int] = {root_fqn: 0}
    for _ in range(len(edge_list) + 1):
        changed = False
        for edge in edge_list:
            source = _edge_value(edge, "source_table", "source_fqn")
            target = _edge_value(edge, "target_table", "target_fqn")
            if not source or not target or source not in depths:
                continue
            depth = depths[source] + 1
            if depth > max_depth:
                continue
            if target not in depths or depth < depths[target]:
                depths[target] = depth
                changed = True
        if not changed:
            break
    return sorted(fqn for fqn, depth in depths.items() if fqn != root_fqn and depth <= max_depth)


def _edge_value(edge, key: str, fallback: str):
    if isinstance(edge, dict):
        return edge.get(key) or edge.get(fallback)
    return getattr(edge, key, None) or getattr(edge, fallback, None)


def _first_attr(value, *names: str) -> str | None:
    for name in names:
        current = getattr(value, name, None)
        if isinstance(current, str) and current:
            return current
    return None
