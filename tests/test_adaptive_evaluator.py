"""Plan 29 slice 8.6: delivered outcomes remain supervised measurements."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import inspect

import pytest

from skifer.adaptive import (
    DeliveryRecord,
    OptimizationProposal,
    OutcomeEvaluation,
    OutcomeEvaluationError,
    OutcomeEvaluator,
    SemanticUsageEvent,
    WindowMetrics,
    fingerprint_query,
)
import skifer.adaptive.evaluator as evaluator_module


DELIVERED = datetime(2026, 8, 20, 12, tzinfo=timezone.utc)
NOW = DELIVERED + timedelta(days=10)
PROPOSAL_ID = "proposal:v1:" + "6" * 64
FINGERPRINT = fingerprint_query(
    model_hashes=("orders-v1",),
    metric_ids=("orders.revenue",),
    dimension_ids=("region",),
    normalized_filter_shape=("region:eq",),
)


class MemoryStore:
    retention_days = 90

    def __init__(self, events):
        self.events = list(events)
        self.calls = []

    def list_events(self, **bounds):
        self.calls.append(bounds)
        return [
            event
            for event in self.events
            if bounds["since"] <= event.occurred_at <= bounds["until"]
        ]


def _event(event_id, offset_days, duration=100, status="succeeded", **overrides):
    values = {
        "event_id": event_id,
        "occurred_at": DELIVERED + timedelta(days=offset_days),
        "environment": "prod",
        "consumer_class": "dashboard",
        "model_hashes": ("orders-v1",),
        "metric_ids": ("orders.revenue",),
        "dimension_ids": ("region",),
        "normalized_filter_shape": ("region:eq",),
        "query_fingerprint": FINGERPRINT,
        "duration_ms": duration,
        "rows_returned": 10,
        "bytes_scanned": 100,
        "status": status,
    }
    values.update(overrides)
    return SemanticUsageEvent(**values)


def _proposal(**overrides):
    values = {
        "proposal_id": PROPOSAL_ID,
        "kind": "materialized_view",
        "rule_id": "frequent_aggregate",
        "rule_version": "1",
        "evidence_event_ids": ("before-0",),
        "expected_benefit": {"action": "review_materialized_view"},
        "risks": ("human_review_required",),
        "generated_schema_path": f"{PROPOSAL_ID}/pipeline.yaml",
        "generated_semantic_draft_path": None,
        "source_definition_hashes": ("orders-v1",),
        "status": "accepted",
    }
    values.update(overrides)
    return OptimizationProposal(**values)


def _delivery(**overrides):
    values = {
        "proposal_id": PROPOSAL_ID,
        "output_path": "schemas/gold/orders_summary.yaml",
        "delivered_at": DELIVERED,
    }
    values.update(overrides)
    return DeliveryRecord(**values)


def _window_events(before_durations, after_durations):
    before = [
        _event(f"before-{index}", -index - 1, duration)
        for index, duration in enumerate(before_durations)
    ]
    after = [
        _event(f"after-{index}", index, duration)
        for index, duration in enumerate(after_durations)
    ]
    return before + after


def _evaluate(events, **options):
    store = MemoryStore(events)
    evaluation = OutcomeEvaluator(store, clock=lambda: NOW, **options).evaluate(
        _proposal(), _delivery()
    )
    assert len(store.calls) == 1
    return evaluation


def test_insufficient_before_data_is_explicitly_inconclusive():
    result = _evaluate(_window_events([900] * 4, [300] * 5))

    assert result.outcome == "inconclusive"
    assert "insufficient_data:before:events=4<5" in result.reasons
    assert "insufficient_data:before:duration_points=4<5" in result.reasons
    assert result.before.event_count == 4


def test_insufficient_after_data_is_explicitly_inconclusive():
    result = _evaluate(_window_events([900] * 5, [300] * 4))

    assert result.outcome == "inconclusive"
    assert "insufficient_data:after:events=4<5" in result.reasons
    assert "insufficient_data:after:duration_points=4<5" in result.reasons


def test_clear_speedup_is_improved_with_observed_numbers():
    result = _evaluate(_window_events([900] * 5, [300] * 5))

    assert result.outcome == "improved"
    assert "duration_p95:before=900.0:after=300.0:ratio=0.33" in result.reasons
    assert result.review_recommendation is None


def test_clear_slowdown_recommends_only_human_review():
    result = _evaluate(_window_events([300] * 5, [900] * 5))

    assert result.outcome == "regressed"
    assert result.review_recommendation is not None
    assert PROPOSAL_ID in result.review_recommendation
    assert "human review" in result.review_recommendation.lower()
    assert "rollback" not in result.review_recommendation.lower()
    assert "drop" not in result.review_recommendation.lower()


def test_failure_rate_rise_with_flat_duration_is_regressed():
    events = _window_events([500] * 10, [500] * 10)
    for index in (5, 6):
        events[10 + index] = _event(
            f"after-{index}", index, duration=None, status="failed"
        )

    result = _evaluate(events)

    assert result.outcome == "regressed"
    assert "failure_rate:before=0.0000:after=0.2000:delta=0.2000" in result.reasons


def test_regression_on_failure_axis_wins_over_duration_improvement():
    events = _window_events([900] * 10, [300] * 10)
    for index in (5, 6):
        events[10 + index] = _event(
            f"after-{index}", index, duration=None, status="failed"
        )

    assert _evaluate(events).outcome == "regressed"


def test_purged_evidence_is_inconclusive_without_an_exception():
    events = [
        _event(f"unrelated-before-{index}", -index - 1, 900)
        for index in range(5)
    ] + [
        _event(f"unrelated-after-{index}", index, 300)
        for index in range(5)
    ]

    result = _evaluate(events)

    assert result.outcome == "inconclusive"
    assert result.reasons == ("evidence_unavailable",)


def test_evidence_spanning_partitions_is_corrupt_input():
    events = _window_events([900] * 5, [300] * 5)
    events.append(
        _event(
            "evidence-other",
            -2,
            900,
            environment="dev",
        )
    )
    proposal = _proposal(evidence_event_ids=("before-0", "evidence-other"))

    with pytest.raises(OutcomeEvaluationError, match="more than one usage partition"):
        OutcomeEvaluator(MemoryStore(events), clock=lambda: NOW).evaluate(
            proposal, _delivery()
        )


def test_delivery_in_the_future_is_broken_input():
    with pytest.raises(OutcomeEvaluationError, match="delivery is in the future"):
        OutcomeEvaluator(MemoryStore([]), clock=lambda: NOW).evaluate(
            _proposal(), _delivery(delivered_at=NOW + timedelta(seconds=1))
        )


def test_delivery_boundary_event_counts_only_in_after_window():
    events = _window_events([900] * 5, [300] * 4)
    events.append(_event("boundary", 0, 300))

    result = _evaluate(events)

    assert result.before.event_count == 5
    assert result.after.event_count == 5


def test_all_evaluator_dataclass_serializers_are_closed_allowlists():
    delivery = _delivery()
    window = WindowMetrics(
        started_at=DELIVERED,
        ended_at=NOW,
        event_count=1,
        succeeded_count=1,
        failed_count=0,
        duration_point_count=1,
        duration_percentile=100.0,
        failure_rate=0.0,
    )
    evaluation = OutcomeEvaluation(
        proposal_id=PROPOSAL_ID,
        evaluated_at=NOW,
        outcome="improved",
        reasons=("duration_p95:before=200.0:after=100.0:ratio=0.50",),
        before=window,
        after=window,
        review_recommendation=None,
    )
    for value in (delivery, window, evaluation):
        object.__setattr__(value, "future_private_field", "secret")
        assert "future_private_field" not in value.to_dict()


def test_delivery_record_round_trip_is_strict_and_timezone_aware():
    assert DeliveryRecord.from_dict(_delivery().to_dict()) == _delivery()
    with pytest.raises(ValueError, match="required schema"):
        DeliveryRecord.from_dict({**_delivery().to_dict(), "unknown": True})
    with pytest.raises(ValueError, match="timezone-aware"):
        DeliveryRecord(PROPOSAL_ID, "output.yaml", datetime(2026, 8, 20))


def test_evaluator_source_has_no_production_action_primitives():
    source = inspect.getsource(evaluator_module)
    for forbidden in ("subprocess", "rollback", "drop", "DROP", "engine.run_", "git "):
        assert forbidden not in source


def test_unchanged_zero_baseline_is_not_reported_as_a_regression():
    """An unmeasurably fast window must not be called a regression.

    ``after >= before * (1 + ratio)`` is satisfied by every non-negative
    observation once the baseline is zero, so identical zero-millisecond windows
    were reported as regressed - on exactly the queries an optimization made too
    fast to measure, and while the same result's own reason line said the ratio
    had improved.
    """
    result = _evaluate(_window_events([0] * 5, [0] * 5))

    assert result.outcome == "inconclusive"
    assert result.review_recommendation is None
    assert "no_significant_change" in result.reasons
    assert "duration_p95:before=0.0:after=0.0:ratio=undefined" in result.reasons


def test_regression_away_from_a_zero_baseline_is_still_reported():
    result = _evaluate(_window_events([0] * 5, [500] * 5))

    assert result.outcome == "regressed"
    assert result.review_recommendation is not None
    assert "duration_p95:before=0.0:after=500.0:ratio=inf" in result.reasons


def test_zero_baseline_can_never_be_reported_as_improved():
    result = _evaluate(_window_events([0] * 5, [0] * 4 + [0]))

    assert result.outcome != "improved"
