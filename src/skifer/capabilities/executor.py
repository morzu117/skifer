"""Idempotent, audited execution and compensation for governed capabilities."""

from __future__ import annotations

from datetime import datetime, timedelta
from threading import RLock
from typing import Any, Callable, Iterable, Mapping
from uuid import uuid4

from .autonomy import (
    ApprovalRecord,
    AutonomyMode,
    AutonomyStateMachine,
    ExecutionState,
    request_hash,
    resolve_autonomy_mode,
)
from .credentials import CredentialBroker, CredentialError, CredentialLease
from .executors import CapabilityExecutorRegistry, CapabilityResult, _copy_json_safe
from .history import (
    MAX_EVENT_DETAIL_BYTES,
    MAX_HISTORY_PAGE_SIZE,
    CapabilityHistoryStore,
    CapabilityStateEvent,
    _copy_bounded_detail,
)
from .invoker import validate_arguments
from .models import CapabilityDefinition, CapabilityMode, Reversibility
from .preconditions import (
    PreconditionDecision,
    PreconditionError,
    PreconditionEvaluator,
    PreconditionReport,
)
from .registry import CapabilityRegistry


class GovernedExecutionError(RuntimeError):
    """A governed call cannot safely proceed or be attested."""


class DuplicateRequestError(GovernedExecutionError):
    """A completed or failed identity cannot be implicitly executed again."""


class LostResponseError(GovernedExecutionError):
    """A prior call may have happened and must not be repeated blindly."""


REDACTED_CREDENTIAL = "<redacted-credential>"


def _redact_credential_echo(value: Any, secret: str) -> tuple[Any, bool]:
    """Strip any echo of the live credential out of an executor's output.

    An executor that returns its own lease is a bug in that executor, but the
    durable record must not depend on every executor being careful: the history is
    append-only, so a secret written there cannot be scrubbed afterwards, and
    section 8 of the plan forbids credential material in history outright.

    Redacting beats failing here. The external side effect has already happened;
    discarding the record of it would invite exactly the duplicate this slice
    exists to prevent. The returned result is redacted identically to the stored
    one so an idempotent replay stays indistinguishable from the first call.
    """
    if isinstance(value, str):
        if secret and secret in value:
            return REDACTED_CREDENTIAL, True
        return value, False
    if isinstance(value, Mapping):
        redacted = False
        result: dict[str, Any] = {}
        for key, item in value.items():
            result[key], changed = _redact_credential_echo(item, secret)
            redacted = redacted or changed
        return result, redacted
    if isinstance(value, list):
        redacted = False
        items = []
        for item in value:
            cleaned, changed = _redact_credential_echo(item, secret)
            items.append(cleaned)
            redacted = redacted or changed
        return items, redacted
    return value, False


class GovernedExecutor:
    """The sole write boundary: validate, attest, call once, then reconcile."""

    def __init__(
        self,
        registry: CapabilityRegistry,
        history: CapabilityHistoryStore,
        *,
        clock: Callable[[], datetime],
        scopes: Iterable[str] = (),
        precondition_evaluator: PreconditionEvaluator | None = None,
        state_reader: Callable[[], Mapping[str, Any]] | None = None,
        credential_broker: CredentialBroker | None = None,
        credential_audience: str | None = None,
        max_detail_bytes: int = MAX_EVENT_DETAIL_BYTES,
        max_retries: int = 3,
    ) -> None:
        if not isinstance(registry, CapabilityRegistry):
            raise TypeError("registry must be a CapabilityRegistry.")
        if not callable(getattr(history, "append", None)) or not callable(
            getattr(history, "events_for", None)
        ) or not callable(getattr(history, "latest_state", None)):
            raise TypeError("history must implement CapabilityHistoryStore.")
        if not callable(clock):
            raise TypeError("Governed executor clock must be callable.")
        _copy_bounded_detail({}, max_bytes=max_detail_bytes)
        if (
            not isinstance(max_retries, int)
            or isinstance(max_retries, bool)
            or max_retries < 0
        ):
            raise ValueError("Governed executor max_retries must be a non-negative integer.")
        self._registry = registry
        self._history = history
        self._clock = clock
        self._scopes = frozenset(scopes)
        self._precondition_evaluator = precondition_evaluator
        self._state_reader = state_reader
        self._credential_broker = credential_broker
        self._credential_audience = credential_audience
        self._max_detail_bytes = max_detail_bytes
        self._max_retries = max_retries
        # Serializes the history check and pre-call append for callers sharing this
        # boundary. Durable history remains authoritative across process restarts.
        self._execution_lock = RLock()

    def execute(
        self,
        definition: CapabilityDefinition,
        *,
        subject: str,
        arguments: Mapping[str, Any],
        mode: AutonomyMode,
        approval: ApprovalRecord | None = None,
        precondition_report: PreconditionReport | None = None,
        lease: CredentialLease | None = None,
    ) -> CapabilityResult:
        """Execute one exact request at most once, or reconcile its prior call."""
        return self._execute(
            definition,
            subject=subject,
            arguments=arguments,
            mode=mode,
            approval=approval,
            precondition_report=precondition_report,
            lease=lease,
        )

    def _execute(
        self,
        definition: CapabilityDefinition,
        *,
        subject: str,
        arguments: Mapping[str, Any],
        mode: AutonomyMode,
        approval: ApprovalRecord | None,
        precondition_report: PreconditionReport | None,
        lease: CredentialLease | None,
        known_request_hash: str | None = None,
    ) -> CapabilityResult:
        if not isinstance(definition, CapabilityDefinition):
            raise TypeError("definition must be a CapabilityDefinition.")
        if not isinstance(subject, str) or not subject:
            raise GovernedExecutionError("Capability subject must be non-empty text.")
        if not isinstance(arguments, Mapping):
            raise GovernedExecutionError("Capability arguments must be a mapping.")
        if not isinstance(mode, AutonomyMode):
            raise GovernedExecutionError("Capability mode must be an AutonomyMode.")

        # This is the single computation of the idempotency identity for a normal
        # run. Compensation can pass the identity it computed for its audit link.
        digest = known_request_hash or request_hash(
            capability_id=definition.id,
            capability_version=definition.version,
            subject=subject,
            arguments=arguments,
        )
        copied_arguments = _copy_json_safe(dict(arguments), path="arguments")

        with self._execution_lock:
            prior = self._history.latest_state(digest)
            if prior is not None:
                self._assert_same_identity(prior, definition, subject)
                return self._resolve_prior(
                    prior,
                    definition=definition,
                    subject=subject,
                    arguments=copied_arguments,
                    request_digest=digest,
                )

            self._authorize_definition(definition, copied_arguments)
            effective_mode = resolve_autonomy_mode(definition, configured=mode)
            if effective_mode is AutonomyMode.SHADOW:
                if lease is not None:
                    raise CredentialError("Credential use is forbidden in SHADOW mode.")
                return CapabilityResult(
                    capability_id=definition.id,
                    capability_version=definition.version,
                    status="succeeded",
                    output={"state": ExecutionState.PROPOSED.value},
                    error_type=None,
                )
            if (
                effective_mode is AutonomyMode.GUARDED
                and definition.reversibility is Reversibility.IRREVERSIBLE
            ):
                raise GovernedExecutionError(
                    "An irreversible capability cannot execute in GUARDED mode."
                )

            report = self._prepare_report(
                definition, copied_arguments, precondition_report
            )
            machine = AutonomyStateMachine(clock=self._clock)
            machine.preconditions_passed(report)
            if effective_mode is AutonomyMode.SUPERVISED and approval is None:
                raise GovernedExecutionError(
                    "SUPERVISED capability execution requires an approval."
                )
            machine.begin_execution(
                definition=definition,
                subject=subject,
                arguments=copied_arguments,
                report=report,
                approval=approval if effective_mode is AutonomyMode.SUPERVISED else None,
                request_digest=digest,
            )

            executor = CapabilityExecutorRegistry.get(definition.executor)
            executing = self._event(
                request_digest=digest,
                definition=definition,
                subject=subject,
                state=ExecutionState.EXECUTING,
                detail={"kind": "external_call"},
            )
            self._history.append(executing)

            active_lease = lease
            echo_redacted = False
            try:
                needs_credential = CapabilityExecutorRegistry.needs_credential(
                    definition.executor
                )
                if needs_credential:
                    active_lease = self._acquire_lease(
                        definition, subject, effective_mode, active_lease
                    )
                    raw_output = executor(copied_arguments, active_lease)
                else:
                    if active_lease is not None:
                        raise CredentialError(
                            "A lease was supplied to an executor that does not declare credentials."
                        )
                    raw_output = executor(copied_arguments)
                if not isinstance(raw_output, Mapping):
                    raise TypeError("Capability executor output must be a mapping.")
                output = _copy_json_safe(raw_output)
                if active_lease is not None:
                    output, echo_redacted = _redact_credential_echo(
                        output, active_lease.reveal()
                    )
                result = CapabilityResult(
                    capability_id=definition.id,
                    capability_version=definition.version,
                    status="succeeded",
                    output=output,
                    error_type=None,
                )
                machine.record_success(result)
                state = ExecutionState.EXECUTED
            except Exception as exc:
                result = CapabilityResult(
                    capability_id=definition.id,
                    capability_version=definition.version,
                    status="failed",
                    output={},
                    error_type=type(exc).__name__,
                )
                machine.record_failure(exc)
                state = ExecutionState.FAILED
            finally:
                active_lease = None

            # Constructing and appending the bounded outcome can fail after the
            # side effect. That failure deliberately crosses the boundary instead
            # of presenting an unattested call as successful.
            self._history.append(
                self._event(
                    request_digest=digest,
                    definition=definition,
                    subject=subject,
                    state=state,
                    detail={
                        "kind": "external_outcome",
                        "result": result.to_dict(),
                        "echo_redacted": echo_redacted,
                    },
                )
            )
            return result

    def compensate(
        self,
        definition: CapabilityDefinition,
        *,
        subject: str,
        original_request_hash: str,
        reason: str,
        mode: AutonomyMode | None = None,
        approval: ApprovalRecord | None = None,
        precondition_report: PreconditionReport | None = None,
        lease: CredentialLease | None = None,
    ) -> CapabilityResult:
        """Run a declared compensation as a separately identified capability."""
        if not isinstance(definition, CapabilityDefinition):
            raise TypeError("definition must be a CapabilityDefinition.")
        if not definition.compensation:
            raise GovernedExecutionError(
                "Compensation requires a declared compensation capability."
            )
        if not isinstance(reason, str) or not reason:
            raise GovernedExecutionError("Compensation reason must be non-empty text.")

        with self._execution_lock:
            original = self._history.latest_state(original_request_hash)
            if original is None or original.state is not ExecutionState.EXECUTED:
                raise GovernedExecutionError(
                    "Compensation is allowed only from an EXECUTED request."
                )
            self._assert_same_identity(original, definition, subject)
            compensation = self._registry.get(definition.compensation)
            arguments = {
                "original_request_hash": original_request_hash,
                "reason": reason,
            }
            compensation_hash = request_hash(
                capability_id=compensation.id,
                capability_version=compensation.version,
                subject=subject,
                arguments=arguments,
            )
            self._history.append(
                self._event(
                    request_digest=original_request_hash,
                    definition=definition,
                    subject=subject,
                    state=ExecutionState.COMPENSATING,
                    detail={
                        "kind": "compensation_started",
                        "compensation_request_hash": compensation_hash,
                        "reason": reason,
                    },
                )
            )

            selected_mode = mode or AutonomyMode[compensation.approval.name]
            result = self._execute(
                compensation,
                subject=subject,
                arguments=arguments,
                mode=selected_mode,
                approval=approval,
                precondition_report=precondition_report,
                lease=lease,
                known_request_hash=compensation_hash,
            )
            if result.status == "succeeded":
                self._history.append(
                    self._event(
                        request_digest=original_request_hash,
                        definition=definition,
                        subject=subject,
                        state=ExecutionState.COMPENSATED,
                        detail={
                            "kind": "compensation_completed",
                            "compensation_request_hash": compensation_hash,
                        },
                    )
                )
            return result

    def _resolve_prior(
        self,
        prior: CapabilityStateEvent,
        *,
        definition: CapabilityDefinition,
        subject: str,
        arguments: Mapping[str, Any],
        request_digest: str,
    ) -> CapabilityResult:
        if prior.state is ExecutionState.EXECUTED:
            return self._recorded_result(prior)
        if prior.state is ExecutionState.EXECUTING:
            return self._reconcile_lost_response(
                definition=definition,
                subject=subject,
                arguments=arguments,
                request_digest=request_digest,
            )
        if prior.state is ExecutionState.FAILED:
            raise DuplicateRequestError(
                "A FAILED request cannot execute again without an explicit reconciliation event."
            )
        if prior.state in {ExecutionState.COMPENSATING, ExecutionState.COMPENSATED}:
            raise DuplicateRequestError(
                f"A request in {prior.state.value} cannot be executed again."
            )
        raise DuplicateRequestError(
            f"A request already recorded in {prior.state.value} cannot be executed implicitly."
        )

    def _reconcile_lost_response(
        self,
        *,
        definition: CapabilityDefinition,
        subject: str,
        arguments: Mapping[str, Any],
        request_digest: str,
    ) -> CapabilityResult:
        lookup = CapabilityExecutorRegistry.idempotency_lookup(definition.executor)
        if lookup is None:
            raise LostResponseError(
                "A prior external call has no recorded outcome and no declared idempotency lookup."
            )
        events = self._history.events_for(
            request_digest, limit=MAX_HISTORY_PAGE_SIZE
        )
        attempts = sum(
            event.detail.get("kind") == "reconciliation_attempt" for event in events
        )
        if attempts >= self._max_retries:
            raise LostResponseError(
                f"The enforced reconciliation retry limit of {self._max_retries} has been reached."
            )
        self._history.append(
            self._event(
                request_digest=request_digest,
                definition=definition,
                subject=subject,
                state=ExecutionState.EXECUTING,
                detail={"kind": "reconciliation_attempt", "attempt": attempts + 1},
            )
        )
        try:
            raw = lookup(_copy_json_safe(dict(arguments), path="arguments"))
            if raw is None:
                raise LostResponseError(
                    "The idempotency lookup could not determine the external outcome."
                )
            if isinstance(raw, CapabilityResult):
                result = raw
                if (
                    result.capability_id != definition.id
                    or result.capability_version != definition.version
                ):
                    raise LostResponseError(
                        "The idempotency lookup returned an outcome for another capability."
                    )
            elif isinstance(raw, Mapping):
                result = CapabilityResult(
                    capability_id=definition.id,
                    capability_version=definition.version,
                    status="succeeded",
                    output=_copy_json_safe(raw),
                    error_type=None,
                )
            else:
                raise TypeError("Idempotency lookup output must be a mapping or CapabilityResult.")
        except LostResponseError:
            raise
        except Exception as exc:
            raise LostResponseError(
                f"Idempotency lookup failed with {type(exc).__name__}."
            ) from None

        reconciled_state = (
            ExecutionState.EXECUTED
            if result.status == "succeeded"
            else ExecutionState.FAILED
        )
        self._history.append(
            self._event(
                request_digest=request_digest,
                definition=definition,
                subject=subject,
                state=reconciled_state,
                detail={"kind": "reconciliation", "result": result.to_dict()},
            )
        )
        return result

    def _authorize_definition(
        self, definition: CapabilityDefinition, arguments: Mapping[str, Any]
    ) -> None:
        if definition.mode is not CapabilityMode.WRITE:
            raise GovernedExecutionError(
                "GovernedExecutor is the write boundary and accepts only write capabilities."
            )
        missing_scopes = sorted(set(definition.required_scopes) - self._scopes)
        if missing_scopes:
            raise GovernedExecutionError(
                f"Capability '{definition.id}' requires missing scopes: {', '.join(missing_scopes)}."
            )
        errors = validate_arguments(definition.input_schema, arguments)
        if errors:
            raise GovernedExecutionError(
                "Invalid capability arguments: " + "; ".join(errors)
            )

    def _prepare_report(
        self,
        definition: CapabilityDefinition,
        arguments: Mapping[str, Any],
        report: PreconditionReport | None,
    ) -> PreconditionReport:
        evaluator = self._precondition_evaluator or PreconditionEvaluator(clock=self._clock)
        if report is None:
            state = self._read_state() if definition.preconditions else {}
            report = evaluator.evaluate(definition, arguments, state)
        if (
            report.capability_id != definition.id
            or report.capability_version != definition.version
            or report.decision is not PreconditionDecision.ALLOW
        ):
            raise PreconditionError(
                "Capability execution requires a matching ALLOW precondition report."
            )
        if definition.preconditions:
            if self._precondition_evaluator is None or self._state_reader is None:
                raise PreconditionError(
                    f"Capability '{definition.id}' requires a precondition evaluator and state reader."
                )
            report = self._precondition_evaluator.assert_unchanged(
                definition, arguments, self._read_state(), report
            )
        return report

    def _read_state(self) -> Mapping[str, Any]:
        if self._state_reader is None:
            raise PreconditionError("Capability execution requires a state reader.")
        try:
            state = self._state_reader()
        except Exception as exc:
            raise PreconditionError(
                f"Capability state reader failed with {type(exc).__name__}."
            ) from None
        if not isinstance(state, Mapping):
            raise PreconditionError("Capability state reader must return a mapping.")
        return state

    def _acquire_lease(
        self,
        definition: CapabilityDefinition,
        subject: str,
        mode: AutonomyMode,
        lease: CredentialLease | None,
    ) -> CredentialLease:
        if self._credential_broker is None:
            raise CredentialError("Credential executor requires a configured broker.")
        if lease is not None:
            self._credential_broker.assert_valid(lease)
            return lease
        if not self._credential_audience:
            raise CredentialError("Credential executor requires a configured audience.")
        issued = self._credential_broker.issue_for(
            definition,
            subject=subject,
            audience=self._credential_audience,
            ttl_seconds=self._credential_broker.max_ttl_seconds,
            mode=mode,
        )
        self._credential_broker.assert_valid(issued)
        return issued

    def _event(
        self,
        *,
        request_digest: str,
        definition: CapabilityDefinition,
        subject: str,
        state: ExecutionState,
        detail: Mapping[str, Any],
    ) -> CapabilityStateEvent:
        bounded = _copy_bounded_detail(detail, max_bytes=self._max_detail_bytes)
        return CapabilityStateEvent(
            event_id=uuid4().hex,
            request_hash=request_digest,
            capability_id=definition.id,
            capability_version=definition.version,
            subject=subject,
            state=state,
            occurred_at=self._now(),
            detail=bounded,
        )

    def _now(self) -> datetime:
        now = self._clock()
        if (
            not isinstance(now, datetime)
            or now.tzinfo is None
            or now.utcoffset() != timedelta(0)
        ):
            raise GovernedExecutionError(
                "Governed executor clock must return timezone-aware UTC."
            )
        return now

    @staticmethod
    def _assert_same_identity(
        event: CapabilityStateEvent,
        definition: CapabilityDefinition,
        subject: str,
    ) -> None:
        if (
            event.capability_id != definition.id
            or event.capability_version != definition.version
            or event.subject != subject
        ):
            raise GovernedExecutionError(
                "Stored request identity does not match the requested capability."
            )

    @staticmethod
    def _recorded_result(event: CapabilityStateEvent) -> CapabilityResult:
        payload = event.detail.get("result")
        if not isinstance(payload, Mapping):
            raise GovernedExecutionError("Recorded terminal outcome is missing its result.")
        try:
            return CapabilityResult(
                capability_id=payload["capability_id"],
                capability_version=payload["capability_version"],
                status=payload["status"],
                output=payload["output"],
                error_type=payload["error_type"],
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise GovernedExecutionError("Recorded terminal outcome is invalid.") from exc
