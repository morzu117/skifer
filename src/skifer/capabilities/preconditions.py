"""Deterministic, bounded precondition evaluation for governed capabilities."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
import hashlib
import json
import math
import re
from typing import Any, Callable, Mapping

from .models import CapabilityDefinition


_PLAIN_NAME = re.compile(r"^[a-z][a-z0-9_]*$")
_REASON_CODE = re.compile(r"^[a-z][a-z0-9_]*$")
_STATE_HASH = re.compile(r"^sha256:v1:[0-9a-f]{64}$")
_MAX_RULE_NAME_LENGTH = 128
_MAX_RULE_VERSION_LENGTH = 64
_MAX_REASON_CODE_LENGTH = 64
_MAX_STATE_BYTES = 65_536
_MAX_STATE_NODES = 4_096
_MAX_STATE_DEPTH = 16


class PreconditionDecision(str, Enum):
    """A deterministic rule's three possible decisions."""

    ALLOW = "ALLOW"
    DENY = "DENY"
    UNKNOWN = "UNKNOWN"


class PreconditionError(RuntimeError):
    """A precondition cannot be selected, evaluated, or safely rechecked."""


def _require_utc(value: datetime, field_name: str) -> None:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() != timedelta(0)
    ):
        raise ValueError(f"Precondition field '{field_name}' must be timezone-aware UTC.")


@dataclass(frozen=True)
class PreconditionOutcome:
    """One bounded, typed rule verdict over an attested state snapshot."""

    rule: str
    rule_version: str
    decision: PreconditionDecision
    reason_code: str
    observed_state_hash: str
    observed_at: datetime

    def __post_init__(self) -> None:
        if (
            not isinstance(self.rule, str)
            or len(self.rule) > _MAX_RULE_NAME_LENGTH
            or _PLAIN_NAME.fullmatch(self.rule) is None
        ):
            raise ValueError("Precondition outcome rule must be a bounded plain name.")
        if (
            not isinstance(self.rule_version, str)
            or not self.rule_version
            or len(self.rule_version) > _MAX_RULE_VERSION_LENGTH
        ):
            raise ValueError("Precondition outcome rule_version must be 1 to 64 characters.")
        if not isinstance(self.decision, PreconditionDecision):
            raise TypeError("Precondition outcome decision must be PreconditionDecision.")
        if (
            not isinstance(self.reason_code, str)
            or len(self.reason_code) > _MAX_REASON_CODE_LENGTH
            or _REASON_CODE.fullmatch(self.reason_code) is None
        ):
            raise ValueError(
                "Precondition outcome reason_code must match ^[a-z][a-z0-9_]*$ "
                "and contain at most 64 characters."
            )
        if (
            not isinstance(self.observed_state_hash, str)
            or _STATE_HASH.fullmatch(self.observed_state_hash) is None
        ):
            raise ValueError(
                "Precondition outcome observed_state_hash must use sha256:v1."
            )
        _require_utc(self.observed_at, "observed_at")

    def to_dict(self) -> dict[str, Any]:
        """Serialize only the stable public outcome fields."""
        return {
            "rule": self.rule,
            "rule_version": self.rule_version,
            "decision": self.decision.value,
            "reason_code": self.reason_code,
            "observed_state_hash": self.observed_state_hash,
            "observed_at": self.observed_at.astimezone(timezone.utc).isoformat(),
        }


@dataclass(frozen=True)
class PreconditionReport:
    """Aggregate precondition verdict for one capability evaluation.

    A capability with no declared preconditions returns ``ALLOW`` with an empty
    outcomes tuple. That means "nothing to check", not "rules ran and allowed";
    consumers can distinguish the cases from ``outcomes``.
    """

    capability_id: str
    capability_version: str
    decision: PreconditionDecision
    outcomes: tuple[PreconditionOutcome, ...]
    evaluated_at: datetime
    requires_escalation: bool

    def __post_init__(self) -> None:
        if not isinstance(self.capability_id, str) or not self.capability_id:
            raise ValueError("Precondition report capability_id must be non-empty text.")
        if not isinstance(self.capability_version, str) or not self.capability_version:
            raise ValueError("Precondition report capability_version must be non-empty text.")
        if not isinstance(self.decision, PreconditionDecision):
            raise TypeError("Precondition report decision must be PreconditionDecision.")
        if not isinstance(self.outcomes, tuple) or not all(
            isinstance(outcome, PreconditionOutcome) for outcome in self.outcomes
        ):
            raise TypeError("Precondition report outcomes must be a tuple of outcomes.")
        _require_utc(self.evaluated_at, "evaluated_at")
        if not isinstance(self.requires_escalation, bool):
            raise TypeError("Precondition report requires_escalation must be boolean.")

    def to_dict(self) -> dict[str, Any]:
        """Serialize only the stable public report fields."""
        return {
            "capability_id": self.capability_id,
            "capability_version": self.capability_version,
            "decision": self.decision.value,
            "outcomes": [outcome.to_dict() for outcome in self.outcomes],
            "evaluated_at": self.evaluated_at.astimezone(timezone.utc).isoformat(),
            "requires_escalation": self.requires_escalation,
        }


class PreconditionRegistry:
    """Process-local allowlist of explicitly registered precondition rules."""

    _rules: dict[str, Callable[[Mapping[str, Any], Mapping[str, Any]], Any]] = {}

    @classmethod
    def register(cls, name: str) -> Callable:
        """Return a decorator registering one callable under a bounded plain name."""
        if (
            not isinstance(name, str)
            or len(name) > _MAX_RULE_NAME_LENGTH
            or _PLAIN_NAME.fullmatch(name) is None
        ):
            raise PreconditionError(
                "Precondition rule name must match ^[a-z][a-z0-9_]*$ "
                "and contain at most 128 characters."
            )

        def decorator(
            func: Callable[[Mapping[str, Any], Mapping[str, Any]], Any],
        ) -> Callable:
            if not callable(func):
                raise PreconditionError(f"Precondition rule '{name}' must be callable.")
            if name in cls._rules:
                raise PreconditionError(f"Precondition rule '{name}' is already registered.")
            cls._rules[name] = func
            return func

        return decorator

    @classmethod
    def get(cls, name: str) -> Callable[[Mapping[str, Any], Mapping[str, Any]], Any]:
        """Return an explicitly registered rule or fail closed."""
        try:
            return cls._rules[name]
        except (KeyError, TypeError) as exc:
            raise PreconditionError(f"Precondition rule '{name}' is not registered.") from exc

    @classmethod
    def list_rules(cls) -> list[str]:
        """Return registered names in deterministic order."""
        return sorted(cls._rules)


class _StateSnapshotError(ValueError):
    pass


def _canonical_state(state: Mapping[str, Any]) -> tuple[dict[str, Any], str]:
    """Copy and serialize a bounded JSON state snapshot before hashing it."""
    if not isinstance(state, Mapping):
        raise _StateSnapshotError("invalid")

    nodes = 0
    estimated_bytes = 0

    def copy(value: Any, depth: int) -> Any:
        nonlocal nodes, estimated_bytes
        if depth > _MAX_STATE_DEPTH:
            raise _StateSnapshotError("too_large")
        nodes += 1
        if nodes > _MAX_STATE_NODES:
            raise _StateSnapshotError("too_large")

        if value is None or isinstance(value, (str, bool, int)):
            copied = value
        elif isinstance(value, float):
            if not math.isfinite(value):
                raise _StateSnapshotError("invalid")
            copied = value
        elif isinstance(value, list):
            copied = [copy(item, depth + 1) for item in value]
        elif isinstance(value, Mapping):
            keys = []
            for key in value:
                nodes += 1
                if nodes > _MAX_STATE_NODES:
                    raise _StateSnapshotError("too_large")
                if not isinstance(key, str):
                    raise _StateSnapshotError("invalid")
                estimated_bytes += len(key.encode("utf-8"))
                if estimated_bytes > _MAX_STATE_BYTES:
                    raise _StateSnapshotError("too_large")
                keys.append(key)
            copied = {}
            for key in sorted(keys):
                copied[key] = copy(value[key], depth + 1)
        else:
            raise _StateSnapshotError("invalid")

        if value is None or isinstance(value, (str, bool, int, float)):
            estimated_bytes += len(
                json.dumps(copied, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            )
            if estimated_bytes > _MAX_STATE_BYTES:
                raise _StateSnapshotError("too_large")
        return copied

    copied_state = copy(state, 0)
    canonical = json.dumps(
        copied_state,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    encoded = canonical.encode("utf-8")
    if len(encoded) > _MAX_STATE_BYTES:
        raise _StateSnapshotError("too_large")
    digest = hashlib.sha256(encoded).hexdigest()
    return copied_state, f"sha256:v1:{digest}"


def _unavailable_hash(reason_code: str) -> str:
    encoded = json.dumps(
        {"state": reason_code}, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")
    return f"sha256:v1:{hashlib.sha256(encoded).hexdigest()}"


def _exception_reason(exc: Exception) -> str:
    class_name = type(exc).__name__.lower()
    normalized = re.sub(r"[^a-z0-9_]+", "_", class_name).strip("_") or "exception"
    return f"rule_error_{normalized}"[:_MAX_REASON_CODE_LENGTH].rstrip("_")


def _aggregate(outcomes: tuple[PreconditionOutcome, ...]) -> PreconditionDecision:
    if any(outcome.decision is PreconditionDecision.DENY for outcome in outcomes):
        return PreconditionDecision.DENY
    if any(outcome.decision is PreconditionDecision.UNKNOWN for outcome in outcomes):
        return PreconditionDecision.UNKNOWN
    return PreconditionDecision.ALLOW


class PreconditionEvaluator:
    """Run bounded allowlisted rules and attest the exact supplied state snapshot."""

    def __init__(self, *, clock: Callable[[], datetime], max_rules: int = 16) -> None:
        if not callable(clock):
            raise TypeError("Precondition evaluator clock must be callable.")
        if (
            not isinstance(max_rules, int)
            or isinstance(max_rules, bool)
            or max_rules < 0
        ):
            raise ValueError("Precondition max_rules must be a non-negative integer.")
        self._clock = clock
        self._max_rules = max_rules

    def evaluate(
        self,
        definition: CapabilityDefinition,
        arguments: Mapping[str, Any],
        state: Mapping[str, Any],
    ) -> PreconditionReport:
        """Evaluate declared rules; no declarations explicitly aggregates to ALLOW."""
        if not isinstance(definition, CapabilityDefinition):
            raise TypeError("definition must be a CapabilityDefinition.")
        rule_names = definition.preconditions
        if len(rule_names) > self._max_rules:
            raise PreconditionError(
                f"Capability '{definition.id}' declares {len(rule_names)} preconditions; "
                f"the enforced maximum is {self._max_rules}."
            )
        if any(len(rule_name) > _MAX_RULE_NAME_LENGTH for rule_name in rule_names):
            raise PreconditionError(
                f"Capability '{definition.id}' declares an over-length precondition rule name."
            )

        evaluated_at = self._clock()
        _require_utc(evaluated_at, "evaluated_at")
        if not rule_names:
            return PreconditionReport(
                capability_id=definition.id,
                capability_version=definition.version,
                decision=PreconditionDecision.ALLOW,
                outcomes=(),
                evaluated_at=evaluated_at,
                requires_escalation=False,
            )

        try:
            copied_state, state_hash = _canonical_state(state)
        except _StateSnapshotError as exc:
            reason = (
                "state_snapshot_too_large"
                if str(exc) == "too_large"
                else "state_snapshot_invalid"
            )
            state_hash = _unavailable_hash(reason)
            outcomes = tuple(
                self._unknown(rule_name, reason, state_hash, evaluated_at)
                for rule_name in rule_names
            )
            return self._report(definition, outcomes, evaluated_at)

        outcomes = tuple(
            self._evaluate_rule(
                rule_name,
                arguments,
                json.loads(
                    json.dumps(
                        copied_state,
                        ensure_ascii=False,
                        separators=(",", ":"),
                        sort_keys=True,
                    )
                ),
                state_hash,
                evaluated_at,
            )
            for rule_name in rule_names
        )
        return self._report(definition, outcomes, evaluated_at)

    def assert_unchanged(
        self,
        definition: CapabilityDefinition,
        arguments: Mapping[str, Any],
        state: Mapping[str, Any],
        report: PreconditionReport,
    ) -> PreconditionReport:
        """Re-run rules and refuse changed, missing, or no-longer-ALLOW results."""
        if not isinstance(report, PreconditionReport):
            raise TypeError("report must be a PreconditionReport.")
        if (
            report.capability_id != definition.id
            or report.capability_version != definition.version
            or report.decision is not PreconditionDecision.ALLOW
        ):
            raise PreconditionError("Earlier precondition report is not an ALLOW for this capability.")

        current = self.evaluate(definition, arguments, state)
        previous_hashes = [(item.rule, item.observed_state_hash) for item in report.outcomes]
        current_hashes = [(item.rule, item.observed_state_hash) for item in current.outcomes]
        if current.decision is not PreconditionDecision.ALLOW:
            raise PreconditionError(
                f"Precondition recheck is no longer ALLOW: {current.decision.value}."
            )
        if previous_hashes != current_hashes:
            raise PreconditionError("Precondition state changed before capability execution.")
        return current

    def _evaluate_rule(
        self,
        rule_name: str,
        arguments: Mapping[str, Any],
        state: Mapping[str, Any],
        state_hash: str,
        evaluated_at: datetime,
    ) -> PreconditionOutcome:
        try:
            rule = PreconditionRegistry.get(rule_name)
        except PreconditionError:
            return self._unknown(rule_name, "rule_not_registered", state_hash, evaluated_at)
        try:
            outcome = rule(arguments, state)
        except Exception as exc:
            return self._unknown(
                rule_name,
                _exception_reason(exc),
                state_hash,
                evaluated_at,
            )
        if not isinstance(outcome, PreconditionOutcome):
            return self._unknown(rule_name, "invalid_rule_return", state_hash, evaluated_at)
        try:
            if outcome.rule != rule_name:
                return self._unknown(rule_name, "invalid_rule_return", state_hash, evaluated_at)
            # Reconstruct field by field rather than calling a method a hostile
            # subclass could override. The first construction validates every
            # bounded return field, including the rule-supplied attestation.
            PreconditionOutcome(
                rule=outcome.rule,
                rule_version=outcome.rule_version,
                decision=outcome.decision,
                reason_code=outcome.reason_code,
                observed_state_hash=outcome.observed_state_hash,
                observed_at=outcome.observed_at,
            )
            return PreconditionOutcome(
                rule=outcome.rule,
                rule_version=outcome.rule_version,
                decision=outcome.decision,
                reason_code=outcome.reason_code,
                observed_state_hash=state_hash,
                observed_at=evaluated_at,
            )
        except (TypeError, ValueError, AttributeError):
            return self._unknown(rule_name, "invalid_rule_return", state_hash, evaluated_at)

    @staticmethod
    def _unknown(
        rule_name: str,
        reason_code: str,
        state_hash: str,
        evaluated_at: datetime,
    ) -> PreconditionOutcome:
        return PreconditionOutcome(
            rule=rule_name,
            rule_version="unknown",
            decision=PreconditionDecision.UNKNOWN,
            reason_code=reason_code,
            observed_state_hash=state_hash,
            observed_at=evaluated_at,
        )

    @staticmethod
    def _report(
        definition: CapabilityDefinition,
        outcomes: tuple[PreconditionOutcome, ...],
        evaluated_at: datetime,
    ) -> PreconditionReport:
        decision = _aggregate(outcomes)
        return PreconditionReport(
            capability_id=definition.id,
            capability_version=definition.version,
            decision=decision,
            outcomes=outcomes,
            evaluated_at=evaluated_at,
            requires_escalation=decision is PreconditionDecision.UNKNOWN,
        )
