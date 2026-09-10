"""Deterministic aggregation of privacy-safe semantic usage patterns."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import math
import re
from typing import Any, Callable, Iterable

from .models import SemanticUsageEvent
from .store import UsageEventStore


DEFAULT_WINDOWS = ("7d", "30d")
DEFAULT_DURATION_PERCENTILE = 95.0
DEFAULT_PERCENTILE_MIN_POINTS = 5
_WINDOW_PATTERN = re.compile(r"^([1-9][0-9]*)d$")


@dataclass(frozen=True)
class PatternAggregate:
    """One status-specific pattern inside one strict usage partition."""

    window: str
    window_started_at: datetime
    window_ended_at: datetime
    environment: str
    consumer_class: str
    model_hashes: tuple[str, ...]
    metric_ids: tuple[str, ...]
    dimension_ids: tuple[str, ...]
    normalized_filter_shape: tuple[str, ...]
    query_fingerprint: str
    status: str
    event_count: int
    duration_percentile: float | None
    duration_point_count: int
    event_ids: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        """Serialize by allowlist so later fields are private by default."""
        return {
            "window": self.window,
            "window_started_at": self.window_started_at.astimezone(
                timezone.utc
            ).isoformat(),
            "window_ended_at": self.window_ended_at.astimezone(timezone.utc).isoformat(),
            "environment": self.environment,
            "consumer_class": self.consumer_class,
            "model_hashes": list(self.model_hashes),
            "metric_ids": list(self.metric_ids),
            "dimension_ids": list(self.dimension_ids),
            "normalized_filter_shape": list(self.normalized_filter_shape),
            "query_fingerprint": self.query_fingerprint,
            "status": self.status,
            "event_count": self.event_count,
            "duration_percentile": self.duration_percentile,
            "duration_point_count": self.duration_point_count,
            "event_ids": list(self.event_ids),
        }


@dataclass(frozen=True)
class PatternAggregationResult:
    """Frozen, standalone aggregate output ordered canonically."""

    generated_at: datetime
    patterns: tuple[PatternAggregate, ...]

    def to_dict(self) -> dict[str, Any]:
        """Serialize only the deliberately public aggregate fields."""
        return {
            "generated_at": self.generated_at.astimezone(timezone.utc).isoformat(),
            "patterns": [pattern.to_dict() for pattern in self.patterns],
        }


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


def _parse_windows(windows: Iterable[str]) -> tuple[tuple[str, int], ...]:
    if isinstance(windows, (str, bytes)):
        raise TypeError("windows must be an iterable of '<days>d' strings.")
    try:
        supplied = tuple(windows)
    except TypeError as exc:
        raise TypeError("windows must be an iterable of '<days>d' strings.") from exc
    if not supplied:
        raise ValueError("windows must contain at least one window.")

    parsed: list[tuple[str, int]] = []
    seen_days: set[int] = set()
    for value in supplied:
        if not isinstance(value, str):
            raise TypeError("Each window must be a '<days>d' string.")
        match = _WINDOW_PATTERN.fullmatch(value)
        if match is None:
            raise ValueError(f"Invalid aggregation window {value!r}; expected '<days>d'.")
        days = int(match.group(1))
        if days in seen_days:
            raise ValueError(f"Duplicate aggregation window {value!r}.")
        seen_days.add(days)
        parsed.append((value, days))
    return tuple(sorted(parsed, key=lambda item: (item[1], item[0])))


def _validate_percentile(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError("duration_percentile must be numeric.")
    percentile = float(value)
    if not math.isfinite(percentile) or not 0 <= percentile <= 100:
        raise ValueError("duration_percentile must be between 0 and 100 inclusive.")
    return percentile


def nearest_rank_percentile(values: list[int], percentile: float) -> float:
    """Return the nearest-rank percentile; p=0 selects the observed minimum.

    Nearest rank avoids inventing a duration between observations. For ``n`` sorted
    points the one-based rank is ``max(1, ceil(p / 100 * n))``. Thus both bounds are
    explicit: p0 is the minimum and p100 is the maximum.
    """
    ordered = sorted(values)
    rank = max(1, math.ceil(percentile / 100 * len(ordered)))
    return float(ordered[rank - 1])


class PatternAggregator:
    """Build deterministic windowed patterns without scanning outside the window."""

    def __init__(
        self,
        store: UsageEventStore,
        *,
        clock: Callable[[], datetime],
        windows: Iterable[str] = DEFAULT_WINDOWS,
        min_count: int = 1,
        duration_percentile: float = DEFAULT_DURATION_PERCENTILE,
        percentile_min_points: int = DEFAULT_PERCENTILE_MIN_POINTS,
    ):
        if not callable(clock):
            raise TypeError("clock must be callable.")
        if not callable(getattr(store, "list_events", None)):
            raise TypeError("store must provide list_events().")
        self._store = store
        self._clock = clock
        self._windows = _parse_windows(windows)
        self._min_count = _positive_int(min_count, "min_count")
        self._duration_percentile = _validate_percentile(duration_percentile)
        self._percentile_min_points = _positive_int(
            percentile_min_points, "percentile_min_points"
        )

    def aggregate(self) -> PatternAggregationResult:
        """Read the bounded event range once and aggregate every configured window."""
        now = _aware_utc(self._clock(), "clock result")
        earliest = now - timedelta(days=max(days for _, days in self._windows))
        events = self._store.list_events(since=earliest, until=now)
        for event in events:
            if not isinstance(event, SemanticUsageEvent):
                raise TypeError("UsageEventStore returned a non-SemanticUsageEvent value.")

        patterns: list[PatternAggregate] = []
        for window, days in self._windows:
            window_start = now - timedelta(days=days)
            groups: dict[tuple[Any, ...], list[SemanticUsageEvent]] = {}
            for event in events:
                occurred_at = _aware_utc(event.occurred_at, "event.occurred_at")
                if not window_start <= occurred_at <= now:
                    continue
                key = (
                    event.environment,
                    event.consumer_class,
                    tuple(sorted(event.model_hashes)),
                    tuple(sorted(event.metric_ids)),
                    tuple(sorted(event.dimension_ids)),
                    tuple(sorted(event.normalized_filter_shape)),
                    event.query_fingerprint,
                    event.status,
                )
                groups.setdefault(key, []).append(event)

            for key in sorted(groups):
                grouped_events = groups[key]
                if len(grouped_events) < self._min_count:
                    continue
                (
                    environment,
                    consumer_class,
                    model_hashes,
                    metric_ids,
                    dimension_ids,
                    filter_shape,
                    fingerprint,
                    status,
                ) = key
                durations = (
                    sorted(
                        event.duration_ms
                        for event in grouped_events
                        if event.duration_ms is not None
                    )
                    if status == "succeeded"
                    else []
                )
                duration_value = None
                if len(durations) >= self._percentile_min_points:
                    duration_value = nearest_rank_percentile(
                        durations, self._duration_percentile
                    )
                patterns.append(
                    PatternAggregate(
                        window=window,
                        window_started_at=window_start,
                        window_ended_at=now,
                        environment=environment,
                        consumer_class=consumer_class,
                        model_hashes=model_hashes,
                        metric_ids=metric_ids,
                        dimension_ids=dimension_ids,
                        normalized_filter_shape=filter_shape,
                        query_fingerprint=fingerprint,
                        status=status,
                        event_count=len(grouped_events),
                        duration_percentile=duration_value,
                        duration_point_count=len(durations),
                        event_ids=tuple(sorted(event.event_id for event in grouped_events)),
                    )
                )

        return PatternAggregationResult(generated_at=now, patterns=tuple(patterns))
