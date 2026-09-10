"""Snapshot-based quarantine for blocked certified-publication runs."""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
from skifer.observability.certification_store import RunEvent
from skifer.observability.checks import ContractScope

RESERVED_QUARANTINE_COLUMNS = frozenset({"_violations", "_run_id", "_contract_version"})

@dataclass(frozen=True)
class QuarantineResult:
    run_id: str
    staging_fqn: str
    quarantine_fqn: str | None
    state: str
    error: str | None = None

def quarantine_staging(backend, run, definition, store, results=None) -> QuarantineResult:
    parts = run.staging_fqn.split(".")
    prefix = parts[:1] if len(parts) == 3 else []
    destination = ".".join(prefix + ["_skifer_quarantine", f"{parts[-1]}_snapshot"])
    try:
        staged = backend.read_staging(run.staging_fqn)
    except Exception as exc:
        store.append_run_event(RunEvent(f"{run.run_id}:CHECK_ERROR", run.run_id, run.target_fqn, "CHECK_ERROR", definition.contract_id, definition.contract_version, definition.definition_hash, datetime.now(timezone.utc), target_fqn=run.target_fqn, staging_fqn=run.staging_fqn))
        return QuarantineResult(run.run_id, run.staging_fqn, None, "CHECK_ERROR", str(exc))
    predicates = row_violation_predicates(results) if results is not None else {}
    if predicates:
        validate_reserved_columns(getattr(staged, "columns", None) or backend.list_columns(run.staging_fqn))
    try:
        if predicates:
            staged = backend.tag_row_violations(
                staged,
                predicates,
                run.run_id,
                definition.contract_version,
            )
        backend.write_staging(staged, destination)
    except Exception as exc:
        store.append_run_event(RunEvent(f"{run.run_id}:CHECK_ERROR", run.run_id, run.target_fqn, "CHECK_ERROR", definition.contract_id, definition.contract_version, definition.definition_hash, datetime.now(timezone.utc), target_fqn=run.target_fqn, staging_fqn=run.staging_fqn))
        return QuarantineResult(run.run_id, run.staging_fqn, None, "CHECK_ERROR", str(exc))
    store.append_run_event(RunEvent(f"{run.run_id}:QUARANTINED", run.run_id, run.target_fqn, "QUARANTINED", definition.contract_id, definition.contract_version, definition.definition_hash, datetime.now(timezone.utc), target_fqn=run.target_fqn, staging_fqn=run.staging_fqn, quarantine_fqn=destination))
    backend.drop_staging(run.staging_fqn)
    return QuarantineResult(run.run_id, run.staging_fqn, destination, "QUARANTINED")


def row_violation_predicates(results) -> dict[str, str]:
    """Return failed row-check predicates keyed by deterministic check labels."""
    predicates = {}
    for index, result in enumerate(results):
        if result.passed or result.contract.scope is not ContractScope.ROW:
            continue
        predicate = result.contract.violation_predicate()
        if predicate:
            predicates[f"{type(result.contract).__name__}:{index}"] = predicate
    return predicates


def validate_reserved_columns(columns) -> None:
    collision = RESERVED_QUARANTINE_COLUMNS.intersection(columns)
    if collision:
        raise ValueError(f"Reserved quarantine column collision: {sorted(collision)}")
