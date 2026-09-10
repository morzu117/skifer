"""Fail-closed autonomy modes, approvals, and capability execution states."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
import re
from typing import Any, Callable, Literal, Mapping

from .executors import CapabilityExecutorRegistry, _copy_json_safe
from .models import CapabilityDefinition, Reversibility
from .preconditions import (
    PreconditionDecision,
    PreconditionReport,
    _StateSnapshotError,
    _canonical_state,
)


class AutonomyMode(str, Enum):
    SHADOW = "SHADOW"
    SUPERVISED = "SUPERVISED"
    GUARDED = "GUARDED"


class ExecutionState(str, Enum):
    PROPOSED = "PROPOSED"
    PRECONDITIONS_PASSED = "PRECONDITIONS_PASSED"
    PENDING_APPROVAL = "PENDING_APPROVAL"
    APPROVED = "APPROVED"
    EXECUTING = "EXECUTING"
    EXECUTED = "EXECUTED"
    FAILED = "FAILED"
    COMPENSATING = "COMPENSATING"
    COMPENSATED = "COMPENSATED"


class AutonomyError(RuntimeError):
    """An autonomy policy or bounded record is invalid."""


class ApprovalError(AutonomyError):
    """An approval is invalid for the exact request and live-state review."""


class TransitionError(AutonomyError):
    """A requested execution-state transition is not legal."""


MAX_APPROVAL_WINDOW = timedelta(hours=4)
# One state-machine instance governs one request and therefore stores at most one
# approval. Keeping this as an explicit enforced bound prevents a later lifecycle
# extension from silently turning the record list into unbounded attacker input.
MAX_STORED_APPROVALS = 1
MAX_APPROVAL_REASON_LENGTH = 2_000
MAX_APPROVAL_ACTOR_LENGTH = 128

_HASH = re.compile(r"^sha256:v1:[0-9a-f]{64}$")
_CONTROL_CHARACTER = re.compile(r"[\x00-\x1f\x7f-\x9f]")

# The entire legal graph is deliberately visible here. Guards on the named
# methods further constrain edges which require an ALLOW report, an approval,
# or a declared compensation capability.
TRANSITIONS: Mapping[ExecutionState, frozenset[ExecutionState]] = {
    ExecutionState.PROPOSED: frozenset({ExecutionState.PRECONDITIONS_PASSED}),
    ExecutionState.PRECONDITIONS_PASSED: frozenset(
        {ExecutionState.PENDING_APPROVAL, ExecutionState.EXECUTING}
    ),
    ExecutionState.PENDING_APPROVAL: frozenset({ExecutionState.APPROVED}),
    ExecutionState.APPROVED: frozenset({ExecutionState.EXECUTING}),
    ExecutionState.EXECUTING: frozenset(
        {ExecutionState.EXECUTED, ExecutionState.FAILED}
    ),
    ExecutionState.EXECUTED: frozenset({ExecutionState.COMPENSATING}),
    ExecutionState.FAILED: frozenset({ExecutionState.COMPENSATING}),
    ExecutionState.COMPENSATING: frozenset(
        {ExecutionState.COMPENSATED, ExecutionState.FAILED}
    ),
    ExecutionState.COMPENSATED: frozenset(),
}


def _require_aware(value: datetime, field_name: str, error_type: type[AutonomyError]) -> None:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise error_type(f"Approval field '{field_name}' must be timezone-aware.")


def _validate_text(value: str, field_name: str, maximum: int) -> None:
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise ApprovalError(
            f"Approval {field_name} must contain 1 to {maximum} characters."
        )
    if _CONTROL_CHARACTER.search(value) is not None:
        raise ApprovalError(f"Approval {field_name} must not contain control characters.")


@dataclass(frozen=True)
class ApprovalRecord:
    capability_id: str
    capability_version: str
    actor: str
    decision: Literal["approved", "denied"]
    reason: str
    granted_at: datetime
    expires_at: datetime
    request_hash: str
    precondition_hash: str

    def __post_init__(self) -> None:
        if not isinstance(self.capability_id, str) or not self.capability_id:
            raise ApprovalError("Approval capability_id must be non-empty text.")
        if not isinstance(self.capability_version, str) or not self.capability_version:
            raise ApprovalError("Approval capability_version must be non-empty text.")
        _validate_text(self.actor, "actor", MAX_APPROVAL_ACTOR_LENGTH)
        if self.decision not in {"approved", "denied"}:
            raise ApprovalError("Approval decision must be 'approved' or 'denied'.")
        _validate_text(self.reason, "reason", MAX_APPROVAL_REASON_LENGTH)
        _require_aware(self.granted_at, "granted_at", ApprovalError)
        _require_aware(self.expires_at, "expires_at", ApprovalError)
        if self.expires_at.astimezone(timezone.utc) <= self.granted_at.astimezone(
            timezone.utc
        ):
            raise ApprovalError("Approval expires_at must be after granted_at.")
        if self.expires_at.astimezone(timezone.utc) - self.granted_at.astimezone(
            timezone.utc
        ) > MAX_APPROVAL_WINDOW:
            raise ApprovalError("Approval validity window exceeds the enforced 4-hour maximum.")
        for field_name in ("request_hash", "precondition_hash"):
            if _HASH.fullmatch(getattr(self, field_name)) is None:
                raise ApprovalError(f"Approval {field_name} must use sha256:v1.")

    def to_dict(self) -> dict[str, Any]:
        """Serialize only deliberately public approval fields."""
        return {
            "capability_id": self.capability_id,
            "capability_version": self.capability_version,
            "actor": self.actor,
            "decision": self.decision,
            "reason": self.reason,
            "granted_at": self.granted_at.astimezone(timezone.utc).isoformat(),
            "expires_at": self.expires_at.astimezone(timezone.utc).isoformat(),
            "request_hash": self.request_hash,
            "precondition_hash": self.precondition_hash,
        }


def _bounded_hash(payload: Mapping[str, Any], label: str) -> str:
    try:
        _, digest = _canonical_state(payload)
    except _StateSnapshotError as exc:
        reason = "too large" if str(exc) == "too_large" else "not valid JSON data"
        raise ApprovalError(f"{label} snapshot is {reason}.") from exc
    return digest


def request_hash(
    *,
    capability_id: str,
    capability_version: str,
    subject: str,
    arguments: Mapping[str, Any],
) -> str:
    """Hash the exact bounded request using the precondition canonicalizer."""
    return _bounded_hash(
        {
            "capability_id": capability_id,
            "capability_version": capability_version,
            "subject": subject,
            "arguments": arguments,
        },
        "Approval request",
    )


def precondition_hash(report: PreconditionReport) -> str:
    """Hash what an approver actually approved: rules, verdicts and observed state.

    Deliberately excludes ``evaluated_at`` and each outcome's ``observed_at``. The
    approval is bound to this hash and the report is re-evaluated immediately before
    execution, so hashing wall-clock time would change the hash on every recheck even
    when the state and every verdict are strictly identical — an approval could then
    never validate against a fresh report, and supervised mode would be unusable. What
    an approver approves is that these rules observed this state and reached these
    verdicts; that a moment has passed since is precisely what the separate expiry
    check is for.
    """
    if not isinstance(report, PreconditionReport):
        raise ApprovalError("Approval precondition report must be a PreconditionReport.")
    identity = {
        "capability_id": report.capability_id,
        "capability_version": report.capability_version,
        "decision": report.decision.value,
        "requires_escalation": report.requires_escalation,
        "outcomes": [
            {
                "rule": outcome.rule,
                "rule_version": outcome.rule_version,
                "decision": outcome.decision.value,
                "reason_code": outcome.reason_code,
                "observed_state_hash": outcome.observed_state_hash,
            }
            for outcome in report.outcomes
        ],
    }
    return _bounded_hash(identity, "Approval precondition")


def resolve_autonomy_mode(
    definition: CapabilityDefinition,
    *,
    configured: AutonomyMode | None,
) -> AutonomyMode:
    """Resolve the stricter of document and environment autonomy policies."""
    if not isinstance(definition, CapabilityDefinition):
        raise TypeError("definition must be a CapabilityDefinition.")
    if configured is not None and not isinstance(configured, AutonomyMode):
        raise TypeError("configured must be an AutonomyMode or None.")
    declared = AutonomyMode[definition.approval.name]
    environment = configured or AutonomyMode.SHADOW
    rank = {
        AutonomyMode.SHADOW: 2,
        AutonomyMode.SUPERVISED: 1,
        AutonomyMode.GUARDED: 0,
    }
    effective = max((declared, environment), key=rank.__getitem__)
    if (
        definition.reversibility is Reversibility.IRREVERSIBLE
        and effective is AutonomyMode.GUARDED
    ):
        raise AutonomyError("An irreversible capability can never resolve to GUARDED.")
    return effective


def validate_approval(
    approval: ApprovalRecord,
    *,
    definition: CapabilityDefinition,
    subject: str,
    arguments: Mapping[str, Any],
    report: PreconditionReport,
    now: datetime,
    expected_request_hash: str | None = None,
) -> None:
    """Refuse unless every field binds to the exact request and current report."""
    if not isinstance(approval, ApprovalRecord):
        raise ApprovalError("Approval must be an ApprovalRecord.")
    _require_aware(now, "now", ApprovalError)
    _require_aware(approval.granted_at, "granted_at", ApprovalError)
    _require_aware(approval.expires_at, "expires_at", ApprovalError)
    _validate_text(approval.actor, "actor", MAX_APPROVAL_ACTOR_LENGTH)
    _validate_text(approval.reason, "reason", MAX_APPROVAL_REASON_LENGTH)
    if approval.decision != "approved":
        raise ApprovalError("Approval decision is not approved.")
    now_utc = now.astimezone(timezone.utc)
    granted_utc = approval.granted_at.astimezone(timezone.utc)
    expires_utc = approval.expires_at.astimezone(timezone.utc)
    if expires_utc <= now_utc:
        raise ApprovalError("Approval has expired.")
    if expires_utc - granted_utc > MAX_APPROVAL_WINDOW:
        raise ApprovalError("Approval validity window exceeds the enforced 4-hour maximum.")
    if approval.capability_id != definition.id:
        raise ApprovalError("Approval capability id does not match the definition.")
    if approval.capability_version != definition.version:
        raise ApprovalError("Approval capability version does not match the definition.")
    expected_request = expected_request_hash or request_hash(
        capability_id=definition.id,
        capability_version=definition.version,
        subject=subject,
        arguments=arguments,
    )
    if _HASH.fullmatch(expected_request) is None:
        raise ApprovalError("Expected approval request hash must use sha256:v1.")
    if approval.request_hash != expected_request:
        raise ApprovalError("Approval request_hash does not match the current request.")
    if approval.precondition_hash != precondition_hash(report):
        raise ApprovalError(
            "Approval precondition_hash does not match the current precondition report."
        )


class AutonomyStateMachine:
    """One governed request moving only through explicit named transitions."""

    def __init__(self, *, clock: Callable[[], datetime]) -> None:
        if not callable(clock):
            raise TypeError("Autonomy state-machine clock must be callable.")
        self._clock = clock
        self._state = ExecutionState.PROPOSED
        self._report: PreconditionReport | None = None
        self._approval_context: tuple[
            CapabilityDefinition, str, dict[str, Any], PreconditionReport
        ] | None = None
        self._approval: ApprovalRecord | None = None
        self._approvals: list[ApprovalRecord] = []
        self._proposal: dict[str, Any] | None = None
        self._simulated_result: dict[str, Any] | None = None

    @property
    def state(self) -> ExecutionState:
        return self._state

    @property
    def approvals(self) -> tuple[ApprovalRecord, ...]:
        return tuple(self._approvals)

    @property
    def proposal(self) -> Mapping[str, Any] | None:
        return _copy_json_safe(self._proposal) if self._proposal is not None else None

    @property
    def simulated_result(self) -> Mapping[str, Any] | None:
        if self._simulated_result is None:
            return None
        return _copy_json_safe(self._simulated_result)

    def preconditions_passed(self, report: PreconditionReport) -> None:
        if not isinstance(report, PreconditionReport):
            raise TransitionError("PROPOSED requires a PreconditionReport before execution.")
        if report.decision is not PreconditionDecision.ALLOW:
            raise TransitionError("PROPOSED requires an ALLOW precondition report.")
        self._move(ExecutionState.PRECONDITIONS_PASSED)
        self._report = report

    def request_approval(
        self,
        *,
        definition: CapabilityDefinition,
        subject: str,
        arguments: Mapping[str, Any],
    ) -> str:
        self._require_report(definition)
        copied_arguments, digest = self._copy_request(definition, subject, arguments)
        self._move(ExecutionState.PENDING_APPROVAL)
        assert self._report is not None
        self._approval_context = (definition, subject, copied_arguments, self._report)
        return digest

    def record_approval(self, approval: ApprovalRecord) -> None:
        if self._state is not ExecutionState.PENDING_APPROVAL:
            self._illegal(ExecutionState.APPROVED)
        if len(self._approvals) >= MAX_STORED_APPROVALS:
            raise ApprovalError(
                f"Stored approval limit of {MAX_STORED_APPROVALS} has been reached."
            )
        assert self._approval_context is not None
        definition, subject, arguments, report = self._approval_context
        validate_approval(
            approval,
            definition=definition,
            subject=subject,
            arguments=arguments,
            report=report,
            now=self._now(),
        )
        self._approvals.append(approval)
        self._approval = approval
        self._move(ExecutionState.APPROVED)

    def begin_execution(
        self,
        *,
        definition: CapabilityDefinition,
        subject: str,
        arguments: Mapping[str, Any],
        report: PreconditionReport | None = None,
        approval: ApprovalRecord | None = None,
        request_digest: str | None = None,
    ) -> None:
        current_report = report or self._report
        if current_report is None or current_report.decision is not PreconditionDecision.ALLOW:
            raise TransitionError("Execution requires an ALLOW precondition report.")
        self._require_matching_report(definition, current_report)
        if self._state is ExecutionState.PENDING_APPROVAL:
            raise TransitionError("PENDING_APPROVAL cannot execute without a valid approval.")
        if self._state is ExecutionState.APPROVED:
            selected = approval or self._approval
            if selected is None:
                raise TransitionError("APPROVED execution requires a valid approval record.")
            validate_approval(
                selected,
                definition=definition,
                subject=subject,
                arguments=arguments,
                report=current_report,
                now=self._now(),
                expected_request_hash=request_digest,
            )
        elif approval is not None:
            validate_approval(
                approval,
                definition=definition,
                subject=subject,
                arguments=arguments,
                report=current_report,
                now=self._now(),
                expected_request_hash=request_digest,
            )
        self._move(ExecutionState.EXECUTING)

    def record_success(self, result: Any = None) -> None:
        self._move(ExecutionState.EXECUTED)

    def record_failure(self, error: Any = None) -> None:
        self._move(ExecutionState.FAILED)

    def begin_compensation(self, *, definition: CapabilityDefinition) -> None:
        if not definition.compensation:
            raise TransitionError("Compensation requires a declared compensation capability.")
        self._move(ExecutionState.COMPENSATING)

    def record_compensated(self, result: Any = None) -> None:
        self._move(ExecutionState.COMPENSATED)

    def record_shadow(
        self,
        *,
        definition: CapabilityDefinition,
        subject: str,
        arguments: Mapping[str, Any],
    ) -> Mapping[str, Any] | None:
        """Record a proposal and optionally simulate, without changing state."""
        if self._state is not ExecutionState.PROPOSED:
            raise TransitionError("Shadow proposals can only be recorded from PROPOSED.")
        copied_arguments, digest = self._copy_request(definition, subject, arguments)
        self._proposal = {
            "capability_id": definition.id,
            "capability_version": definition.version,
            "subject": subject,
            "arguments": copied_arguments,
            "request_hash": digest,
        }
        if not CapabilityExecutorRegistry.supports_dry_run(definition.executor):
            return None
        executor = CapabilityExecutorRegistry.get(definition.executor)
        try:
            raw = executor(_copy_json_safe(copied_arguments), dry_run=True)
            if not isinstance(raw, Mapping):
                raise TypeError("Dry-run result must be a mapping.")
            output = _copy_json_safe(raw, path="simulated_result")
            self._simulated_result = {"status": "SIMULATED", "output": output}
        except Exception as exc:
            self._simulated_result = {
                "status": "SIMULATED",
                "output": {},
                "error_type": type(exc).__name__,
            }
        return self.simulated_result

    def _copy_request(
        self,
        definition: CapabilityDefinition,
        subject: str,
        arguments: Mapping[str, Any],
    ) -> tuple[dict[str, Any], str]:
        digest = request_hash(
            capability_id=definition.id,
            capability_version=definition.version,
            subject=subject,
            arguments=arguments,
        )
        try:
            copied, _ = _canonical_state(arguments)
        except _StateSnapshotError as exc:  # already checked by request_hash
            raise ApprovalError("Approval request arguments are invalid.") from exc
        return copied, digest

    def _require_report(self, definition: CapabilityDefinition) -> None:
        if self._report is None:
            raise TransitionError("Approval requires passed preconditions.")
        self._require_matching_report(definition, self._report)

    @staticmethod
    def _require_matching_report(
        definition: CapabilityDefinition, report: PreconditionReport
    ) -> None:
        if (
            report.capability_id != definition.id
            or report.capability_version != definition.version
            or report.decision is not PreconditionDecision.ALLOW
        ):
            raise TransitionError("Precondition report does not match this capability.")

    def _now(self) -> datetime:
        now = self._clock()
        _require_aware(now, "now", AutonomyError)
        return now

    def _move(self, target: ExecutionState) -> None:
        if target not in TRANSITIONS[self._state]:
            self._illegal(target)
        self._state = target

    def _illegal(self, target: ExecutionState) -> None:
        raise TransitionError(
            f"Transition {self._state.value} -> {target.value} is not allowed."
        )
