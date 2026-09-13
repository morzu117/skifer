"""Staging run identity and state transitions for certified publication (Plan 29)."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
import re
from typing import TYPE_CHECKING
from uuid import UUID, uuid4
import warnings

from skifer.observability.certification import ContractDefinition
from skifer.observability.certification_store import RunEvent, StoredCheckResult
from skifer.observability.checks import CheckStatus
from skifer.observability.quarantine import quarantine_staging
from skifer.observability.tracing import (
    NoOpTracer,
    TraceContext,
    configured_span_scope,
    current_trace_context,
    new_trace_id,
    set_span_attribute,
    trace_context_scope,
)

if TYPE_CHECKING:
    from skifer.observability.monitor import MonitorReport

_FQN = re.compile(r"^[A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+){1,2}$")


class RunState(str, Enum):
    STARTED = "STARTED"
    STAGING = "STAGING"
    STAGED = "STAGED"
    PROMOTING = "PROMOTING"
    PROMOTED = "PROMOTED"


@dataclass(frozen=True)
class PublicationRun:
    run_id: str
    target_fqn: str
    staging_fqn: str
    state: RunState


@dataclass(frozen=True)
class PublicationResult:
    run: PublicationRun
    report: "MonitorReport | None"
    state: str


class PublicationCoordinator:
    """Stage, validate, persist check results, then promote or quarantine."""

    def __init__(self, backend, monitor, store, metadata_store=None):
        self.backend = backend
        self.monitor = monitor
        self.store = store
        self.metadata_store = metadata_store
        # Plain getattr, not monitor.__dict__: a monitor exposing `tracer` as a
        # property was silently downgraded to NoOpTracer, i.e. tracing quietly
        # off with no way to notice.
        self.tracer = getattr(monitor, "tracer", None) or NoOpTracer()
        self.tracing_required = bool(getattr(monitor, "tracing_required", False))

    def publish(
        self,
        df,
        target_fqn: str,
        schema_dict: dict,
        definition: ContractDefinition,
        run_id: str | None = None,
    ) -> PublicationResult:
        # One path, traced or not. Duplicating the flow to skip a no-op context
        # manager let the two branches drift: the traced one derived the
        # business run_id from the ambient trace context, so turning tracing on
        # changed the run_id persisted in the certification store — and raised
        # outright when that ambient id was not a UUID.
        run = start_publication_run(target_fqn, definition, self.store, run_id)
        active = current_trace_context(self.tracer)
        # Correlation follows the run, never the reverse.
        context = TraceContext(
            trace_id=active.trace_id or new_trace_id(),
            run_id=run.run_id,
            evidence_id=active.evidence_id,
            session_id=active.session_id,
        )
        with trace_context_scope(self.tracer, context, required=self.tracing_required):
            return self._publish_run(df, schema_dict, definition, run)

    def _publish_run(self, df, schema_dict, definition, run) -> PublicationResult:
        attributes = {
            "run_id": run.run_id,
            "contract_id": definition.contract_id,
            "contract_version": definition.contract_version,
        }
        with configured_span_scope(
            self.tracer,
            "skifer.publication.stage",
            attributes=attributes,
            required=self.tracing_required,
        ):
            run = stage_dataframe(self.backend, run, definition, self.store, df)

        # No span here: DataMonitor already emits skifer.contract.evaluate,
        # and wrapping the call would report the same boundary twice.
        report = self.monitor.check_from_schema(
            run.staging_fqn, schema_dict, raise_on_critical=False
        )
        self.store.append_check_results([
            StoredCheckResult(
                event_id=f"{run.run_id}:{type(result.contract).__name__}:{index}",
                run_id=run.run_id,
                check_type=type(result.contract).__name__,
                scope=result.contract.scope,
                severity=result.severity,
                status=result.status,
                actual_value=str(result.actual_value) if result.actual_value is not None else None,
                expected_value=str(result.expected_value) if result.expected_value is not None else None,
                message=result.message,
            )
            for index, result in enumerate(report.results)
        ])

        with configured_span_scope(
            self.tracer,
            "skifer.publication.promote",
            attributes=attributes,
            required=self.tracing_required,
        ) as span:
            if report.has_critical_failures():
                outcome = quarantine_staging(
                    self.backend, run, definition, self.store, results=report.results
                )
                if outcome.state == "QUARANTINED":
                    self._record_incidents(run, definition, report)
                set_span_attribute(
                    span, "decision", "QUARANTINED", required=self.tracing_required
                )
                return PublicationResult(run=run, report=report, state=outcome.state)
            promoted = promote_staging(self.backend, run, definition, self.store)
            self._resolve_recovered(promoted)
            set_span_attribute(
                span, "decision", "PROMOTED", required=self.tracing_required
            )
        return PublicationResult(run=promoted, report=report, state="PROMOTED")

    def resume(self, run: PublicationRun, definition: ContractDefinition) -> PublicationResult:
        """Resume a previously staged/promoting run idempotently."""
        if isinstance(self.tracer, NoOpTracer):
            promoted = promote_staging(self.backend, run, definition, self.store)
            self._resolve_recovered(promoted)
            self._index_resumed_metadata(promoted, definition)
            return PublicationResult(run=promoted, report=None, state="PROMOTED")
        attributes = {
            "run_id": run.run_id,
            "contract_id": definition.contract_id,
            "contract_version": definition.contract_version,
        }
        with configured_span_scope(
            self.tracer,
            "skifer.publication.promote",
            attributes=attributes,
            required=self.tracing_required,
        ):
            promoted = promote_staging(self.backend, run, definition, self.store)
        self._resolve_recovered(promoted)
        self._index_resumed_metadata(promoted, definition)
        return PublicationResult(run=promoted, report=None, state="PROMOTED")

    def _index_resumed_metadata(self, run, definition) -> None:
        """Index contract metadata recovered without the original pipeline schema."""
        if self.metadata_store is None:
            return
        try:
            from dataclasses import replace

            from skifer.observability.metadata_index import (
                dataset_record_from_definition,
                upsert_index_record,
            )

            record = dataset_record_from_definition(
                definition, run.target_fqn, run.run_id
            )
            existing = self.metadata_store.get(run.target_fqn)
            if (
                existing is not None
                and existing.definition_hash == definition.definition_hash
            ):
                record = replace(existing, last_run_id=run.run_id)
            upsert_index_record(self.metadata_store, record)
        except Exception as exc:
            warnings.warn(
                f"[Metadata] failed to index resumed publication: {type(exc).__name__}",
                RuntimeWarning,
            )

    def _record_incidents(self, run, definition, report) -> None:
        """Open incidents for failed critical checks without blocking publication."""
        try:
            from skifer.observability.incidents import incidents_from_report

            now = datetime.now(timezone.utc)
            for incident in incidents_from_report(
                report, run_id=run.run_id, target_fqn=run.target_fqn, at=now
            ):
                self.store.open_incident(incident)
        except Exception as exc:
            warnings.warn(
                f"[Incidents] failed to open incident(s): {type(exc).__name__}",
                RuntimeWarning,
            )

    def _resolve_recovered(self, run) -> None:
        """Resolve open incidents as recovered without blocking publication."""
        try:
            self.store.resolve_open_incidents(
                run.target_fqn,
                root_cause="recovered",
                resolved_at=datetime.now(timezone.utc),
            )
        except Exception as exc:
            warnings.warn(
                f"[Incidents] failed to resolve incidents: {type(exc).__name__}",
                RuntimeWarning,
            )


def start_publication_run(target_fqn: str, definition: ContractDefinition, store, run_id: str | None = None) -> PublicationRun:
    """Create and persist a safely named staging run before any write occurs."""
    target_fqn = target_fqn.replace("`", "")
    if not _FQN.fullmatch(target_fqn):
        raise ValueError("Publication target must be a 2- or 3-part identifier without quotes or wildcards.")
    run_id = run_id or str(uuid4())
    try:
        run_id = str(UUID(run_id))
    except ValueError as exc:
        raise ValueError("run_id must be a UUID.") from exc
    parts = target_fqn.split(".")
    prefix = parts[:1] if len(parts) == 3 else []
    staging = ".".join(prefix + ["_skifer_staging", f"{parts[-1]}_{run_id.replace('-', '')}"])
    run = PublicationRun(run_id, target_fqn, staging, RunState.STARTED)
    store.append_run_event(RunEvent(f"{run_id}:STARTED", run_id, target_fqn, run.state.value,
                                    definition.contract_id, definition.contract_version, definition.definition_hash,
                                    datetime.now(timezone.utc), target_fqn=target_fqn, staging_fqn=staging))
    return run


def stage_dataframe(backend, run: PublicationRun, definition: ContractDefinition, store, df) -> PublicationRun:
    """Persist STAGING/STAGED transitions around one exact staging write."""
    for state in (RunState.STAGING, RunState.STAGED):
        if state is RunState.STAGING:
            store.append_run_event(RunEvent(f"{run.run_id}:STAGING", run.run_id, run.target_fqn, state.value, definition.contract_id, definition.contract_version, definition.definition_hash, datetime.now(timezone.utc), target_fqn=run.target_fqn, staging_fqn=run.staging_fqn))
            backend.write_staging(df, run.staging_fqn)
        else:
            store.append_run_event(RunEvent(f"{run.run_id}:STAGED", run.run_id, run.target_fqn, state.value, definition.contract_id, definition.contract_version, definition.definition_hash, datetime.now(timezone.utc), target_fqn=run.target_fqn, staging_fqn=run.staging_fqn))
    return PublicationRun(run.run_id, run.target_fqn, run.staging_fqn, RunState.STAGED)


def promote_staging(backend, run: PublicationRun, definition: ContractDefinition, store) -> PublicationRun:
    """Promote a staged/promoting run; a completed promotion is idempotent."""
    current = store.get_run(run.run_id)
    if current and current.state == RunState.PROMOTED.value:
        backend.drop_staging(run.staging_fqn)
        return PublicationRun(run.run_id, run.target_fqn, run.staging_fqn, RunState.PROMOTED)
    if not current or current.state not in (RunState.STAGED.value, RunState.PROMOTING.value):
        raise ValueError(f"Illegal publication transition from {current.state if current else 'missing'} to PROMOTING.")
    check_results = store.get_check_results(run.run_id)
    if any(
        r.status in (CheckStatus.FAIL, CheckStatus.ERROR) and r.severity == "critical"
        for r in check_results
    ):
        raise ValueError(
            f"Cannot promote run {run.run_id}: recorded critical check failures block promotion."
        )
    for state in (RunState.PROMOTING, RunState.PROMOTED):
        store.append_run_event(RunEvent(f"{run.run_id}:{state.value}", run.run_id, run.target_fqn, state.value, definition.contract_id, definition.contract_version, definition.definition_hash, datetime.now(timezone.utc), target_fqn=run.target_fqn, staging_fqn=run.staging_fqn))
        if state is RunState.PROMOTING:
            backend.write_table(backend.read_staging(run.staging_fqn), run.target_fqn)
    backend.drop_staging(run.staging_fqn)
    return PublicationRun(run.run_id, run.target_fqn, run.staging_fqn, RunState.PROMOTED)
