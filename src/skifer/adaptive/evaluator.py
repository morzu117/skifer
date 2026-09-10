"""Post-delivery measurement for supervised adaptive Gold proposals."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import math
from typing import Any, Callable, Literal, Mapping, Sequence

from .aggregator import nearest_rank_percentile
from .models import OptimizationProposal, SemanticUsageEvent
from .store import UsageEventStore


class OutcomeEvaluationError(RuntimeError):
    """A delivered proposal cannot be evaluated from trustworthy inputs."""


def _non_empty_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string.")
    return value


def _aware_utc(value: Any, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{field_name} must be a datetime.")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware.")
    return value.astimezone(timezone.utc)


def _positive_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field_name} must be a positive integer.")
    return value


def _ratio(value: Any, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{field_name} must be numeric.")
    result = float(value)
    if not math.isfinite(result) or not 0 <= result <= 1:
        raise ValueError(f"{field_name} must be between 0 and 1 inclusive.")
    return result


def _percentile(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError("duration_percentile must be numeric.")
    result = float(value)
    if not math.isfinite(result) or not 0 <= result <= 100:
        raise ValueError("duration_percentile must be between 0 and 100 inclusive.")
    return result


@dataclass(frozen=True)
class DeliveryRecord:
    """The human-owned asset path and time recorded during acceptance."""

    proposal_id: str
    output_path: str
    delivered_at: datetime

    def __post_init__(self) -> None:
        _non_empty_string(self.proposal_id, "proposal_id")
        _non_empty_string(self.output_path, "output_path")
        _aware_utc(self.delivered_at, "delivered_at")

    def to_dict(self) -> dict[str, Any]:
        """Serialize only the deliberately public delivery fields."""
        return {
            "proposal_id": self.proposal_id,
            "output_path": self.output_path,
            "delivered_at": self.delivered_at.astimezone(timezone.utc).isoformat(),
        }

    @classmethod
    def from_dict(cls, mapping: Mapping[str, Any]) -> DeliveryRecord:
        """Load one delivery record from its exact persisted shape."""
        if not isinstance(mapping, Mapping):
            raise TypeError("Delivery record must be a mapping.")
        expected = {"proposal_id", "output_path", "delivered_at"}
        if set(mapping) != expected:
            raise ValueError("Delivery record fields do not match the required schema.")
        delivered_at = mapping["delivered_at"]
        if not isinstance(delivered_at, str):
            raise TypeError("delivered_at must be an ISO datetime string.")
        try:
            parsed = datetime.fromisoformat(delivered_at)
        except ValueError as exc:
            raise ValueError("delivered_at must be an ISO datetime string.") from exc
        return cls(
            proposal_id=mapping["proposal_id"],
            output_path=mapping["output_path"],
            delivered_at=parsed,
        )


@dataclass(frozen=True)
class WindowMetrics:
    """Allowlisted observations for one side of the delivery boundary."""

    started_at: datetime
    ended_at: datetime
    event_count: int
    succeeded_count: int
    failed_count: int
    duration_point_count: int
    duration_percentile: float | None
    failure_rate: float | None

    def __post_init__(self) -> None:
        started = _aware_utc(self.started_at, "started_at")
        ended = _aware_utc(self.ended_at, "ended_at")
        if started > ended:
            raise ValueError("started_at must not be after ended_at.")
        counts = (
            ("event_count", self.event_count),
            ("succeeded_count", self.succeeded_count),
            ("failed_count", self.failed_count),
            ("duration_point_count", self.duration_point_count),
        )
        for field_name, value in counts:
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{field_name} must be a non-negative integer.")
        if self.succeeded_count + self.failed_count != self.event_count:
            raise ValueError("succeeded_count and failed_count must equal event_count.")
        if self.duration_point_count > self.succeeded_count:
            raise ValueError("duration_point_count cannot exceed succeeded_count.")
        if self.duration_percentile is not None:
            if (
                isinstance(self.duration_percentile, bool)
                or not isinstance(self.duration_percentile, (int, float))
                or not math.isfinite(float(self.duration_percentile))
                or self.duration_percentile < 0
            ):
                raise ValueError("duration_percentile must be non-negative or None.")
        expected_rate = (
            None if self.event_count == 0 else self.failed_count / self.event_count
        )
        if self.failure_rate != expected_rate:
            raise ValueError("failure_rate does not match the observed counts.")

    def to_dict(self) -> dict[str, Any]:
        """Serialize by allowlist so later fields remain private by default."""
        return {
            "started_at": self.started_at.astimezone(timezone.utc).isoformat(),
            "ended_at": self.ended_at.astimezone(timezone.utc).isoformat(),
            "event_count": self.event_count,
            "succeeded_count": self.succeeded_count,
            "failed_count": self.failed_count,
            "duration_point_count": self.duration_point_count,
            "duration_percentile": self.duration_percentile,
            "failure_rate": self.failure_rate,
        }


@dataclass(frozen=True)
class OutcomeEvaluation:
    """Standalone before/after result with an explicit interpretation."""

    proposal_id: str
    evaluated_at: datetime
    outcome: Literal["improved", "regressed", "inconclusive"]
    reasons: tuple[str, ...]
    before: WindowMetrics
    after: WindowMetrics
    review_recommendation: str | None

    def __post_init__(self) -> None:
        _non_empty_string(self.proposal_id, "proposal_id")
        _aware_utc(self.evaluated_at, "evaluated_at")
        if self.outcome not in {"improved", "regressed", "inconclusive"}:
            raise ValueError("outcome is not supported.")
        if not isinstance(self.reasons, tuple) or not self.reasons:
            raise ValueError("reasons must be a non-empty tuple.")
        for index, reason in enumerate(self.reasons):
            _non_empty_string(reason, f"reasons[{index}]")
        if not isinstance(self.before, WindowMetrics) or not isinstance(
            self.after, WindowMetrics
        ):
            raise TypeError("before and after must be WindowMetrics instances.")
        if (self.review_recommendation is not None) != (
            self.outcome == "regressed"
        ):
            raise ValueError(
                "review_recommendation must be present exactly for a regressed outcome."
            )
        if self.review_recommendation is not None:
            _non_empty_string(self.review_recommendation, "review_recommendation")

    def to_dict(self) -> dict[str, Any]:
        """Serialize the public evaluation contract field by field."""
        return {
            "proposal_id": self.proposal_id,
            "evaluated_at": self.evaluated_at.astimezone(timezone.utc).isoformat(),
            "outcome": self.outcome,
            "reasons": list(self.reasons),
            "before": self.before.to_dict(),
            "after": self.after.to_dict(),
            "review_recommendation": self.review_recommendation,
        }


class OutcomeEvaluator:
    """Compare bounded usage windows without asserting unsupported causality."""

    def __init__(
        self,
        store: UsageEventStore,
        *,
        clock: Callable[[], datetime],
        window_days: int = 30,
        min_events_per_window: int = 5,
        min_duration_points: int = 5,
        duration_percentile: float = 95.0,
        improvement_ratio: float = 0.10,
        regression_ratio: float = 0.10,
    ) -> None:
        if not callable(getattr(store, "list_events", None)):
            raise TypeError("store must provide list_events().")
        if not callable(clock):
            raise TypeError("clock must be callable.")
        self._store = store
        self._clock = clock
        self._window_days = _positive_int(window_days, "window_days")
        self._min_events = _positive_int(
            min_events_per_window, "min_events_per_window"
        )
        self._min_duration_points = _positive_int(
            min_duration_points, "min_duration_points"
        )
        self._duration_percentile = _percentile(duration_percentile)
        self._improvement_ratio = _ratio(improvement_ratio, "improvement_ratio")
        self._regression_ratio = _ratio(regression_ratio, "regression_ratio")

    def evaluate(
        self, proposal: OptimizationProposal, delivery: DeliveryRecord
    ) -> OutcomeEvaluation:
        """Measure one delivered proposal from a single bounded store read."""
        if not isinstance(proposal, OptimizationProposal):
            raise TypeError("proposal must be an OptimizationProposal instance.")
        if not isinstance(delivery, DeliveryRecord):
            raise TypeError("delivery must be a DeliveryRecord instance.")
        if delivery.proposal_id != proposal.proposal_id:
            raise OutcomeEvaluationError("Delivery proposal_id does not match proposal.")

        now = self._clock_value()
        delivered_at = _aware_utc(delivery.delivered_at, "delivery.delivered_at")
        if delivered_at > now:
            raise OutcomeEvaluationError("delivery is in the future")
        before_start = delivered_at - timedelta(days=self._window_days)
        retention_days = _positive_int(
            getattr(self._store, "retention_days", self._window_days),
            "store.retention_days",
        )
        evidence_start = now - timedelta(days=retention_days)
        delivery_evidence_start = delivered_at - timedelta(days=retention_days)
        events = self._store.list_events(
            since=min(before_start, evidence_start, delivery_evidence_start),
            until=now,
        )
        for event in events:
            if not isinstance(event, SemanticUsageEvent):
                raise OutcomeEvaluationError(
                    "UsageEventStore returned a non-SemanticUsageEvent value."
                )

        evidence_ids = set(proposal.evidence_event_ids)
        evidence = [event for event in events if event.event_id in evidence_ids]
        empty_before = self._metrics((), before_start, delivered_at)
        empty_after = self._metrics((), delivered_at, now)
        if not evidence:
            return OutcomeEvaluation(
                proposal_id=proposal.proposal_id,
                evaluated_at=now,
                outcome="inconclusive",
                reasons=("evidence_unavailable",),
                before=empty_before,
                after=empty_after,
                review_recommendation=None,
            )

        partitions = {
            (event.environment, event.consumer_class, event.query_fingerprint)
            for event in evidence
        }
        if len(partitions) != 1:
            raise OutcomeEvaluationError(
                "Proposal evidence spans more than one usage partition."
            )
        partition = next(iter(partitions))
        matching = [
            event
            for event in events
            if (event.environment, event.consumer_class, event.query_fingerprint)
            == partition
        ]
        before_events = [
            event
            for event in matching
            if before_start <= _aware_utc(event.occurred_at, "event.occurred_at")
            < delivered_at
        ]
        after_events = [
            event
            for event in matching
            if delivered_at <= _aware_utc(event.occurred_at, "event.occurred_at")
            <= now
        ]
        before = self._metrics(before_events, before_start, delivered_at)
        after = self._metrics(after_events, delivered_at, now)
        insufficient = self._insufficient_reasons(before, after)
        if insufficient:
            return OutcomeEvaluation(
                proposal_id=proposal.proposal_id,
                evaluated_at=now,
                outcome="inconclusive",
                reasons=insufficient,
                before=before,
                after=after,
                review_recommendation=None,
            )

        assert before.duration_percentile is not None
        assert after.duration_percentile is not None
        assert before.failure_rate is not None
        assert after.failure_rate is not None
        # A zero baseline has no meaningful multiplicative comparison. Every
        # non-negative observation satisfies ``after >= 0 * (1 + ratio)``, so the
        # plain formula calls an unchanged zero-millisecond window a regression -
        # on exactly the queries an optimization made too fast to measure. Compare
        # against zero directly, and report no ratio rather than a misleading one.
        if before.duration_percentile == 0:
            duration_ratio_text = (
                "undefined" if after.duration_percentile == 0 else "inf"
            )
            duration_improved = False
            duration_regressed = after.duration_percentile > 0
        else:
            ratio = after.duration_percentile / before.duration_percentile
            duration_ratio_text = f"{ratio:.2f}"
            duration_improved = (
                after.duration_percentile
                <= before.duration_percentile * (1 - self._improvement_ratio)
            )
            duration_regressed = (
                after.duration_percentile
                >= before.duration_percentile * (1 + self._regression_ratio)
            )
        duration_reason = (
            f"duration_p{self._duration_percentile:g}:"
            f"before={before.duration_percentile}:"
            f"after={after.duration_percentile}:ratio={duration_ratio_text}"
        )
        failure_delta = after.failure_rate - before.failure_rate
        failure_reason = (
            f"failure_rate:before={before.failure_rate:.4f}:"
            f"after={after.failure_rate:.4f}:delta={failure_delta:.4f}"
        )
        failure_regressed = (
            after.failure_rate >= before.failure_rate + self._regression_ratio
        )
        observed_reasons = (duration_reason, failure_reason)
        if duration_regressed or failure_regressed:
            return OutcomeEvaluation(
                proposal_id=proposal.proposal_id,
                evaluated_at=now,
                outcome="regressed",
                reasons=observed_reasons,
                before=before,
                after=after,
                review_recommendation=(
                    f"Human review recommended for proposal '{proposal.proposal_id}' "
                    "because observed usage regressed."
                ),
            )
        if duration_improved:
            return OutcomeEvaluation(
                proposal_id=proposal.proposal_id,
                evaluated_at=now,
                outcome="improved",
                reasons=observed_reasons,
                before=before,
                after=after,
                review_recommendation=None,
            )
        return OutcomeEvaluation(
            proposal_id=proposal.proposal_id,
            evaluated_at=now,
            outcome="inconclusive",
            reasons=(*observed_reasons, "no_significant_change"),
            before=before,
            after=after,
            review_recommendation=None,
        )

    def _clock_value(self) -> datetime:
        try:
            return _aware_utc(self._clock(), "clock result")
        except (TypeError, ValueError) as exc:
            raise OutcomeEvaluationError(str(exc)) from exc

    def _metrics(
        self,
        events: Sequence[SemanticUsageEvent],
        started_at: datetime,
        ended_at: datetime,
    ) -> WindowMetrics:
        succeeded = [event for event in events if event.status == "succeeded"]
        failed_count = sum(event.status == "failed" for event in events)
        durations = [
            event.duration_ms
            for event in succeeded
            if event.duration_ms is not None
        ]
        duration_value = (
            nearest_rank_percentile(durations, self._duration_percentile)
            if durations
            else None
        )
        event_count = len(events)
        return WindowMetrics(
            started_at=started_at,
            ended_at=ended_at,
            event_count=event_count,
            succeeded_count=len(succeeded),
            failed_count=failed_count,
            duration_point_count=len(durations),
            duration_percentile=duration_value,
            failure_rate=None if event_count == 0 else failed_count / event_count,
        )

    def _insufficient_reasons(
        self, before: WindowMetrics, after: WindowMetrics
    ) -> tuple[str, ...]:
        reasons = []
        for name, metrics in (("before", before), ("after", after)):
            if metrics.event_count < self._min_events:
                reasons.append(
                    f"insufficient_data:{name}:events="
                    f"{metrics.event_count}<{self._min_events}"
                )
            if metrics.duration_point_count < self._min_duration_points:
                reasons.append(
                    f"insufficient_data:{name}:duration_points="
                    f"{metrics.duration_point_count}<{self._min_duration_points}"
                )
        return tuple(reasons)
