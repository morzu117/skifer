"""Tests for Plan 29 slice 8.2 deterministic pattern aggregation."""

from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone
import json
import os
import subprocess
import sys
import textwrap
from unittest.mock import Mock

import pytest

from skifer.adaptive import (
    PatternAggregator,
    SemanticUsageEvent,
    fingerprint_query,
)


NOW = datetime(2026, 9, 8, 12, 0, tzinfo=timezone.utc)


def _event(**overrides) -> SemanticUsageEvent:
    values = {
        "event_id": "event-1",
        "occurred_at": NOW - timedelta(days=1),
        "environment": "prod",
        "consumer_class": "dashboard",
        "model_hashes": ("model-v1",),
        "metric_ids": ("orders.revenue",),
        "dimension_ids": ("region",),
        "normalized_filter_shape": ("region:eq",),
        "duration_ms": 100,
        "rows_returned": 10,
        "bytes_scanned": 1000,
        "status": "succeeded",
    }
    values.update(overrides)
    values.setdefault(
        "query_fingerprint",
        fingerprint_query(
            model_hashes=values["model_hashes"],
            metric_ids=values["metric_ids"],
            dimension_ids=values["dimension_ids"],
            normalized_filter_shape=values["normalized_filter_shape"],
        ),
    )
    return SemanticUsageEvent(**values)


def _store(events):
    store = Mock()
    store.list_events.return_value = list(events)
    return store


def _aggregate(events, **options):
    store = _store(events)
    result = PatternAggregator(store, clock=lambda: NOW, **options).aggregate()
    return result, store


def test_environment_is_a_strict_partition():
    result, _ = _aggregate(
        [_event(event_id="prod"), _event(event_id="dev", environment="dev")],
        windows=("7d",),
    )

    assert len(result.patterns) == 2
    assert {pattern.environment for pattern in result.patterns} == {"dev", "prod"}
    assert {pattern.event_count for pattern in result.patterns} == {1}


def test_consumer_class_is_a_strict_partition():
    result, _ = _aggregate(
        [
            _event(event_id="dashboard"),
            _event(event_id="agent", consumer_class="agent"),
        ],
        windows=("7d",),
    )

    assert len(result.patterns) == 2
    assert {pattern.consumer_class for pattern in result.patterns} == {
        "agent",
        "dashboard",
    }
    assert {pattern.event_count for pattern in result.patterns} == {1}


def test_model_definition_hash_is_a_strict_partition():
    result, _ = _aggregate(
        [
            _event(event_id="v1"),
            _event(
                event_id="v2",
                model_hashes=("model-v2",),
            ),
        ],
        windows=("7d",),
    )

    assert [pattern.model_hashes for pattern in result.patterns] == [
        ("model-v1",),
        ("model-v2",),
    ]
    assert [pattern.event_count for pattern in result.patterns] == [1, 1]


def test_min_count_excludes_just_below_and_includes_at_threshold():
    below, _ = _aggregate(
        [_event(event_id="one"), _event(event_id="two")],
        windows=("7d",),
        min_count=3,
    )
    at_threshold, _ = _aggregate(
        [_event(event_id=f"event-{index}") for index in range(3)],
        windows=("7d",),
        min_count=3,
    )

    assert below.patterns == ()
    assert len(at_threshold.patterns) == 1
    assert at_threshold.patterns[0].event_count == 3


def test_7d_and_30d_windows_are_inclusive_and_store_read_is_bounded_once():
    events = [
        _event(event_id="at-7d", occurred_at=NOW - timedelta(days=7)),
        _event(
            event_id="outside-7d",
            occurred_at=NOW - timedelta(days=7, microseconds=1),
        ),
        _event(event_id="at-30d", occurred_at=NOW - timedelta(days=30)),
        _event(
            event_id="outside-30d",
            occurred_at=NOW - timedelta(days=30, microseconds=1),
        ),
    ]
    result, store = _aggregate(events)

    seven, thirty = result.patterns
    assert seven.window == "7d"
    assert seven.event_ids == ("at-7d",)
    assert thirty.window == "30d"
    assert thirty.event_ids == ("at-30d", "at-7d", "outside-7d")
    store.list_events.assert_called_once_with(
        since=NOW - timedelta(days=30), until=NOW
    )


def test_failures_are_separate_and_never_supply_success_volume_or_duration():
    events = [
        _event(event_id="success", duration_ms=200),
        _event(event_id="failure-1", status="failed", duration_ms=9000),
        _event(event_id="failure-2", status="failed", duration_ms=8000),
    ]
    result, _ = _aggregate(
        events,
        windows=("7d",),
        percentile_min_points=1,
    )

    succeeded = next(pattern for pattern in result.patterns if pattern.status == "succeeded")
    failed = next(pattern for pattern in result.patterns if pattern.status == "failed")
    assert succeeded.event_count == 1
    assert succeeded.duration_percentile == 200
    assert failed.event_count == 2
    assert failed.duration_point_count == 0
    assert failed.duration_percentile is None


def test_a_fully_failed_pattern_never_appears_as_a_successful_popular_pattern():
    result, _ = _aggregate(
        [_event(event_id=f"failure-{index}", status="failed") for index in range(3)],
        windows=("7d",),
        min_count=3,
    )

    assert len(result.patterns) == 1
    assert result.patterns[0].status == "failed"


def test_percentile_is_absent_below_point_threshold_and_present_at_threshold():
    events = [
        _event(event_id="one", duration_ms=10),
        _event(event_id="two", duration_ms=None),
        _event(event_id="three", duration_ms=30),
    ]
    below, _ = _aggregate(
        events, windows=("7d",), percentile_min_points=3, duration_percentile=50
    )
    present, _ = _aggregate(
        events, windows=("7d",), percentile_min_points=2, duration_percentile=50
    )

    assert below.patterns[0].duration_point_count == 2
    assert below.patterns[0].duration_percentile is None
    assert present.patterns[0].duration_percentile == 10


@pytest.mark.parametrize(("percentile", "expected"), [(0, 10), (100, 30)])
def test_nearest_rank_percentile_has_explicit_bounds(percentile, expected):
    result, _ = _aggregate(
        [
            _event(event_id="one", duration_ms=10),
            _event(event_id="two", duration_ms=20),
            _event(event_id="three", duration_ms=30),
        ],
        windows=("7d",),
        percentile_min_points=3,
        duration_percentile=percentile,
    )

    assert result.patterns[0].duration_percentile == expected


def test_output_is_byte_stable_across_python_hash_seeds():
    script = textwrap.dedent(
        """
        import json
        from datetime import datetime, timedelta, timezone
        from skifer.adaptive import PatternAggregator, SemanticUsageEvent, fingerprint_query

        now = datetime(2026, 9, 8, 12, tzinfo=timezone.utc)
        events = []
        for event_id in {'event-c', 'event-a', 'event-b'}:
            models = tuple({'model-b', 'model-a'})
            metrics = tuple({'orders.count', 'orders.revenue'})
            dimensions = tuple({'date', 'region'})
            filters = tuple({'region:eq', 'status:in'})
            events.append(SemanticUsageEvent(
                event_id=event_id, occurred_at=now - timedelta(days=1),
                environment='prod', consumer_class='dashboard', model_hashes=models,
                metric_ids=metrics, dimension_ids=dimensions,
                normalized_filter_shape=filters,
                query_fingerprint=fingerprint_query(
                    model_hashes=models, metric_ids=metrics,
                    dimension_ids=dimensions, normalized_filter_shape=filters,
                ),
                duration_ms={'event-a': 30, 'event-b': 10, 'event-c': 20}[event_id],
                rows_returned=None, bytes_scanned=None, status='succeeded',
            ))

        class Store:
            def list_events(self, **bounds):
                return events

        result = PatternAggregator(
            Store(), clock=lambda: now, windows=tuple({'30d', '7d'}),
            duration_percentile=50, percentile_min_points=3,
        ).aggregate()
        print(json.dumps(result.to_dict(), sort_keys=True, separators=(',', ':')))
        """
    )
    outputs = set()
    for seed in ("1", "17", "999"):
        environment = os.environ.copy()
        environment["PYTHONHASHSEED"] = seed
        outputs.add(
            subprocess.check_output(
                [sys.executable, "-c", script], text=True, env=environment
            ).strip()
        )

    assert len(outputs) == 1
    payload = json.loads(outputs.pop())
    assert [pattern["window"] for pattern in payload["patterns"]] == ["7d", "30d"]


def test_result_is_frozen_and_serializes_only_allowlisted_non_sensitive_fields():
    event = _event()
    object.__setattr__(event, "future_sensitive_field", "SECRET_CANARY")

    result, _ = _aggregate([event], windows=("7d",))
    serialized = json.dumps(result.to_dict(), sort_keys=True)

    with pytest.raises(FrozenInstanceError):
        result.generated_at = NOW + timedelta(days=1)
    with pytest.raises(FrozenInstanceError):
        result.patterns[0].event_count = 99
    assert "future_sensitive_field" not in serialized
    assert "SECRET_CANARY" not in serialized


@pytest.mark.parametrize(
    ("options", "error"),
    [
        ({"windows": ()}, ValueError),
        ({"windows": ("week",)}, ValueError),
        ({"windows": ("0d",)}, ValueError),
        ({"min_count": -1}, ValueError),
        ({"duration_percentile": -0.1}, ValueError),
        ({"duration_percentile": 100.1}, ValueError),
        ({"percentile_min_points": 0}, ValueError),
    ],
)
def test_invalid_configuration_is_refused(options, error):
    with pytest.raises(error):
        PatternAggregator(_store([]), clock=lambda: NOW, **options)


def test_naive_clock_result_is_refused_before_store_access():
    store = _store([])
    aggregator = PatternAggregator(
        store,
        clock=lambda: datetime(2026, 9, 8, 12),
    )

    with pytest.raises(ValueError, match="timezone-aware"):
        aggregator.aggregate()
    store.list_events.assert_not_called()
