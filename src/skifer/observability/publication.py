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
from skifer.observability.certification import diff_contracts, schema_from_definition
from skifer.observability.certification_store import RunEvent, StoredCheckResult, next_run_event_time
from skifer.observability.checks import CheckStatus
from skifer.observability.openlineage import emit_run_event_best_effort
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


@dataclass(frozen=True)
class _BreakingChangeView:
    breaking: bool
    from_version: str
    to_version: str


class PublicationCoordinator:
    """Stage, validate, persist check results, then promote or quarantine."""

    def __init__(self, backend, monitor, store, metadata_store=None, alert_router=None, alert_config=None,
                 lineage_emitter=None, lineage_context=None):
        self.backend = backend
        self.monitor = monitor
        self.store = store
        self.metadata_store = metadata_store
        self.alert_router = alert_router
        self.alert_config = alert_config if isinstance(alert_config, dict) else {}
        self.lineage_emitter = lineage_emitter
        self.lineage_context = lineage_context
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
        lineage_record = _schema_record_factory(schema_dict, run.target_fqn)
        self._emit_lineage("START", run.run_id, lineage_record)
        active = current_trace_context(self.tracer)
        # Correlation follows the run, never the reverse.
        context = TraceContext(
            trace_id=active.trace_id or new_trace_id(),
            run_id=run.run_id,
            evidence_id=active.evidence_id,
            session_id=active.session_id,
        )
        try:
            with trace_context_scope(self.tracer, context, required=self.tracing_required):
                result = self._publish_run(df, schema_dict, definition, run)
        except Exception:
            # Observed, never altered: the original exception propagates unchanged.
            self._emit_lineage("FAIL", run.run_id, lineage_record)
            raise
        check_results = getattr(result.report, "results", None)
        if result.state == "PROMOTED":
            self._emit_lineage(
                "COMPLETE", run.run_id, lineage_record,
                check_results=check_results, certification_status="CERTIFIED",
            )
        else:
            self._emit_lineage(
                "FAIL", run.run_id, lineage_record,
                check_results=check_results, certification_status="UNCERTIFIED",
            )
        return result

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
                    incidents = self._record_incidents(run, definition, report)
                    if self.alert_router is not None and incidents:
                        try:
                            self.alert_router.alert_incident(
                                None,
                                target_fqn=run.target_fqn,
                                incidents=incidents,
                                config=self.alert_config,
                            )
                        except Exception as exc:
                            warnings.warn(
                                f"[Alerts] failed to alert incident: {type(exc).__name__}",
                                RuntimeWarning,
                            )
                set_span_attribute(
                    span, "decision", "QUARANTINED", required=self.tracing_required
                )
                return PublicationResult(run=run, report=report, state=outcome.state)
            previous = None
            if self.alert_router is not None:
                try:
                    previous = self.store.get_latest_promoted(run.target_fqn)
                except Exception as exc:
                    warnings.warn(
                        f"[Alerts] failed to read previous publication: {type(exc).__name__}",
                        RuntimeWarning,
                    )
            promoted = promote_staging(self.backend, run, definition, self.store)
            self._resolve_recovered(promoted)
            if self.alert_router is not None:
                try:
                    previous_definition = (
                        self.store.get_contract_by_hash(
                            previous.contract_id,
                            previous.definition_hash,
                        )
                        if (
                            previous is not None
                            and previous.definition_hash != definition.definition_hash
                        )
                        else None
                    )
                    if previous_definition is not None:
                        diff = diff_contracts(
                            schema_from_definition(previous_definition),
                            schema_from_definition(definition),
                        )
                        if diff.breaking:
                            self.alert_router.alert_breaking_change(
                                None,
                                target_fqn=run.target_fqn,
                                diff=_BreakingChangeView(
                                    breaking=diff.breaking,
                                    from_version=previous.contract_version,
                                    to_version=definition.contract_version,
                                ),
                                config=self.alert_config,
                            )
                except Exception as exc:
                    warnings.warn(
                        f"[Alerts] failed to alert breaking change: {type(exc).__name__}",
                        RuntimeWarning,
                    )
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
            self._emit_resumed_lineage(promoted, definition)
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
        self._emit_resumed_lineage(promoted, definition)
        return PublicationResult(run=promoted, report=None, state="PROMOTED")

    def _emit_lineage(self, event_type, run_id, record_factory, *, check_results=None,
                      certification_status=None) -> None:
        """Best-effort OpenLineage emission; never changes the publication outcome (Plan 36.3)."""
        if self.lineage_emitter is None or self.lineage_context is None:
            return
        emit_run_event_best_effort(
            self.lineage_emitter,
            event_type=event_type,
            run_id=run_id,
            record_factory=record_factory,
            job_namespace=getattr(self.lineage_context, "job_namespace", None),
            dataset_namespace=getattr(self.lineage_context, "dataset_namespace", None),
            check_results=check_results,
            certification_status=certification_status,
        )

    def _emit_resumed_lineage(self, run, definition) -> None:
        """COMPLETE for a resumed run: no schema dict, so the record comes from the contract."""
        def record_factory():
            from skifer.observability.metadata_index import dataset_record_from_definition

            return dataset_record_from_definition(definition, run.target_fqn, run.run_id)

        self._emit_lineage(
            "COMPLETE", run.run_id, record_factory, certification_status="CERTIFIED"
        )

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

    def _record_incidents(self, run, definition, report):
        """Open incidents for failed critical checks without blocking publication."""
        opened = []
        try:
            from skifer.observability.incidents import incidents_from_report

            now = datetime.now(timezone.utc)
            for incident in incidents_from_report(
                report, run_id=run.run_id, target_fqn=run.target_fqn, at=now
            ):
                opened_incident = self.store.open_incident(incident)
                if opened_incident is not None:
                    opened.append(opened_incident)
        except Exception as exc:
            warnings.warn(
                f"[Incidents] failed to open incident(s): {type(exc).__name__}",
                RuntimeWarning,
            )
            return opened
        return opened

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


def _schema_record_factory(schema_dict, target_fqn):
    """Lazy DatasetRecord for lineage events; only called when an emitter is configured.

    The path hint lives in core/patterns.py, which observability must not import, so the
    target FQN stands in for it — the path never reaches a RunEvent.
    """
    def factory():
        from skifer.observability.metadata_index import index_schema

        return index_schema(schema_dict, target_fqn, target_fqn=target_fqn)

    return factory


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
    store.register_contract(definition)
    store.append_run_event(RunEvent(f"{run_id}:STARTED", run_id, target_fqn, run.state.value,
                                    definition.contract_id, definition.contract_version, definition.definition_hash,
                                    next_run_event_time(store, run_id), target_fqn=target_fqn, staging_fqn=staging))
    return run


def stage_dataframe(backend, run: PublicationRun, definition: ContractDefinition, store, df) -> PublicationRun:
    """Persist STAGING/STAGED transitions around one exact staging write."""
    for state in (RunState.STAGING, RunState.STAGED):
        if state is RunState.STAGING:
            store.append_run_event(RunEvent(f"{run.run_id}:STAGING", run.run_id, run.target_fqn, state.value, definition.contract_id, definition.contract_version, definition.definition_hash, next_run_event_time(store, run.run_id), target_fqn=run.target_fqn, staging_fqn=run.staging_fqn))
            backend.write_staging(df, run.staging_fqn)
        else:
            store.append_run_event(RunEvent(f"{run.run_id}:STAGED", run.run_id, run.target_fqn, state.value, definition.contract_id, definition.contract_version, definition.definition_hash, next_run_event_time(store, run.run_id), target_fqn=run.target_fqn, staging_fqn=run.staging_fqn))
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
        store.append_run_event(RunEvent(f"{run.run_id}:{state.value}", run.run_id, run.target_fqn, state.value, definition.contract_id, definition.contract_version, definition.definition_hash, next_run_event_time(store, run.run_id), target_fqn=run.target_fqn, staging_fqn=run.staging_fqn))
        if state is RunState.PROMOTING:
            backend.write_table(backend.read_staging(run.staging_fqn), run.target_fqn)
    backend.drop_staging(run.staging_fqn)
    return PublicationRun(run.run_id, run.target_fqn, run.staging_fqn, RunState.PROMOTED)
