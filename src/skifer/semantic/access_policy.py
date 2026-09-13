"""Pure certification policy for semantic reads."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from enum import Enum
import logging
import re

from skifer.core.constants import VALID_CONTRACT_STATUSES


MAX_OVERRIDE_WINDOW = timedelta(hours=4)
OVERRIDE_SCOPE = "certification_override"

logger = logging.getLogger(__name__)


class CertificationDecision(str, Enum):
    ALLOW = "ALLOW"
    WARN = "WARN"
    DENY = "DENY"
    REQUIRE_HUMAN = "REQUIRE_HUMAN"


class LifecycleReason(str, Enum):
    DEPRECATED = "DEPRECATED"
    NOT_YET_EFFECTIVE = "NOT_YET_EFFECTIVE"
    EXPIRED_WINDOW = "EXPIRED_WINDOW"


@dataclass(frozen=True)
class ConsumerContext:
    consumer_id: str
    consumer_class: str
    user_id: str | None = None
    scopes: frozenset[str] = frozenset()
    trace_id: str | None = None


@dataclass(frozen=True)
class PolicyEvaluation:
    decision: CertificationDecision
    reasons: tuple[str, ...]
    evaluated_at: datetime


@dataclass(frozen=True)
class CertificationOverride:
    reason: str
    actor: str
    trace_id: str
    expires_at: datetime


class SemanticAccessDenied(Exception):
    """Structured, non-sensitive semantic certification denial."""

    def __init__(
        self,
        *,
        model_key: str,
        datasets: tuple[str, ...],
        decision: CertificationDecision,
        reasons: tuple[str, ...],
        evaluated_at: datetime,
        recommended_action: str,
    ):
        self.model_key = model_key
        self.datasets = datasets
        self.decision = decision
        self.reasons = reasons
        self.evaluated_at = evaluated_at
        self.recommended_action = recommended_action
        super().__init__(str(self))

    def __str__(self) -> str:
        return (
            "SemanticAccessDenied("
            f"model_key={self.model_key!r}, "
            f"datasets={self.datasets!r}, "
            f"decision={self.decision.value!r}, "
            f"reasons={self.reasons!r}, "
            f"evaluated_at={self.evaluated_at.isoformat()!r}, "
            f"recommended_action={self.recommended_action!r}"
            ")"
        )

    def __repr__(self) -> str:
        return str(self)


def _validate_override(
    override: CertificationOverride,
    now: datetime,
    context: ConsumerContext,
) -> None:
    if OVERRIDE_SCOPE not in context.scopes:
        raise ValueError(
            "Certification override requires the 'certification_override' scope "
            "on the consumer context."
        )
    if not override.reason.strip():
        raise ValueError("Certification override requires a non-empty reason.")
    if not override.actor.strip():
        raise ValueError("Certification override requires a non-empty actor.")
    if not override.trace_id.strip():
        raise ValueError("Certification override requires a non-empty trace_id.")
    if override.actor != context.consumer_id:
        raise ValueError(
            "Certification override actor must match the consumer_id of the "
            "requesting context."
        )
    if override.expires_at.tzinfo is None or override.expires_at.utcoffset() is None:
        raise ValueError("Certification override expires_at must be timezone-aware.")
    comparable_now = now if now.tzinfo else now.replace(tzinfo=timezone.utc)
    if override.expires_at <= comparable_now:
        raise ValueError("Certification override has already expired.")
    if override.expires_at - comparable_now > MAX_OVERRIDE_WINDOW:
        raise ValueError(
            "Certification override expiration exceeds the maximum short window."
        )


def _parse_duration(value: str) -> timedelta:
    match = re.fullmatch(r"(\d+)([hmds])", value.strip())
    if not match:
        raise ValueError(f"Unsupported duration format: {value!r}")
    amount = int(match.group(1))
    unit = match.group(2)
    if unit == "h":
        return timedelta(hours=amount)
    if unit == "m":
        return timedelta(minutes=amount)
    if unit == "d":
        return timedelta(days=amount)
    return timedelta(seconds=amount)


def _parse_lifecycle_date(value: str | None, location: str) -> date | None:
    if value is None:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{location} must be an ISO date string (YYYY-MM-DD).") from exc


def evaluate_lifecycle(
    *,
    status: str,
    effective_from: str | None,
    effective_until: str | None,
    now: datetime,
) -> PolicyEvaluation:
    """Evaluate lifecycle metadata without touching the certification gate."""
    normalized_status = status.strip() if isinstance(status, str) else ""
    if normalized_status not in VALID_CONTRACT_STATUSES:
        raise ValueError(f"Invalid contract lifecycle status: {status!r}")

    comparable_now = now if now.tzinfo else now.replace(tzinfo=timezone.utc)
    today = comparable_now.astimezone(timezone.utc).date()
    start = _parse_lifecycle_date(effective_from, "effective_from")
    end = _parse_lifecycle_date(effective_until, "effective_until")

    reasons: list[str] = []
    if start is not None and today < start:
        reasons.append(LifecycleReason.NOT_YET_EFFECTIVE.value)
    if end is not None and today > end:
        reasons.append(LifecycleReason.EXPIRED_WINDOW.value)
    if normalized_status == "deprecated":
        reasons.append(LifecycleReason.DEPRECATED.value)

    if any(
        reason
        in {
            LifecycleReason.NOT_YET_EFFECTIVE.value,
            LifecycleReason.EXPIRED_WINDOW.value,
        }
        for reason in reasons
    ):
        decision = CertificationDecision.DENY
    elif LifecycleReason.DEPRECATED.value in reasons:
        decision = CertificationDecision.WARN
    else:
        decision = CertificationDecision.ALLOW
    return PolicyEvaluation(decision, tuple(reasons), now)


def _reason_for(
    value,
    *,
    now: datetime,
    max_age_delta: timedelta | None,
) -> str | None:
    if value is None or getattr(value, "status", None) != "CERTIFIED":
        return "MISSING"
    if getattr(value, "checks_passed", True) is False:
        return "FAILED_CHECK"
    if max_age_delta is None:
        return None

    certified_at = getattr(value, "certified_at", None)
    if certified_at is None:
        return "EXPIRED"
    comparable_now = now if now.tzinfo else now.replace(tzinfo=timezone.utc)
    comparable_certified_at = (
        certified_at if certified_at.tzinfo else certified_at.replace(tzinfo=timezone.utc)
    ).astimezone(timezone.utc)
    if comparable_now - comparable_certified_at > max_age_delta:
        return "EXPIRED"
    return None


def evaluate(
    certifications,
    context,
    mode: str,
    now: datetime,
    override: CertificationOverride | None = None,
    max_age: str | None = None,
    count_warning: bool = True,
) -> PolicyEvaluation:
    if mode not in {"off", "warn", "enforce", "supervised"}:
        raise ValueError(f"Invalid semantic certification policy: {mode}")
    if override is not None:
        try:
            _validate_override(override, now, context)
        except ValueError as exc:
            logger.error(
                "Certification override rejected: actor=%s trace_id=%s error=%s",
                override.actor,
                override.trace_id,
                str(exc),
            )
            raise
        if count_warning:
            logger.warning(
                "Certification override accepted: actor=%s trace_id=%s reason=%s "
                "datasets=%s",
                override.actor,
                override.trace_id,
                override.reason,
                [value.dataset for value in certifications if value is not None],
            )
        return PolicyEvaluation(CertificationDecision.ALLOW, ("OVERRIDDEN",), now)
    if mode == "off":
        return PolicyEvaluation(CertificationDecision.ALLOW, (), now)
    max_age_delta = _parse_duration(max_age) if max_age is not None else None
    bad = tuple(
        reason
        for value in certifications
        if (
            reason := _reason_for(value, now=now, max_age_delta=max_age_delta)
        ) is not None
    )
    if not bad:
        return PolicyEvaluation(CertificationDecision.ALLOW, (), now)
    decision = {
        "warn": CertificationDecision.WARN,
        "enforce": CertificationDecision.DENY,
        "supervised": CertificationDecision.REQUIRE_HUMAN,
    }[mode]
    return PolicyEvaluation(decision, bad, now)
