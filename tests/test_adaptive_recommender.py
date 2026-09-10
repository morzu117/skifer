"""Tests for Plan 29 slice 8.3 explainable recommendation rules."""

from dataclasses import FrozenInstanceError, fields
from datetime import datetime, timedelta, timezone
import json
import os
import subprocess
import sys
import textwrap
from unittest.mock import patch

import pytest

from skifer.adaptive import (
    RULE_REGISTRY,
    OptimizationProposal,
    PatternAggregate,
    RecommendationContext,
    RecommendationEngine,
    RecommendationThresholds,
    fingerprint_query,
)
from skifer.semantic.planner import MetricRef, PlannedJoin, SemanticPlan


NOW = datetime(2026, 9, 8, 12, tzinfo=timezone.utc)


def _pattern(**overrides) -> PatternAggregate:
    values = {
        "window": "7d",
        "window_started_at": NOW - timedelta(days=7),
        "window_ended_at": NOW,
        "environment": "prod",
        "consumer_class": "dashboard",
        "model_hashes": ("orders-v1",),
        "metric_ids": ("orders.revenue",),
        "dimension_ids": ("orders.region",),
        "normalized_filter_shape": ("orders.status:eq",),
        "status": "succeeded",
        "event_count": 10,
        "duration_percentile": 1_000.0,
        "duration_point_count": 10,
    }
    values.update(overrides)
    values.setdefault(
        "event_ids",
        tuple(f"event-{index:03}" for index in range(values["event_count"])),
    )
    values.setdefault(
        "query_fingerprint",
        fingerprint_query(
            model_hashes=values["model_hashes"],
            metric_ids=values["metric_ids"],
            dimension_ids=values["dimension_ids"],
            normalized_filter_shape=values["normalized_filter_shape"],
        ),
    )
    return PatternAggregate(**values)


def _join_plan(*, metric_model="lines", cardinality="one_to_many"):
    return SemanticPlan(
        root_model="orders",
        required_models=("orders", "lines"),
        joins=(
            PlannedJoin(
                relationship_name="order_lines",
                source_model="orders",
                target_model="lines",
                source_entity="order",
                target_entity="order",
                source_key_columns=("order_id",),
                target_key_columns=("order_id",),
                cardinality=cardinality,
                join_type="left",
            ),
        ),
        metrics=(
            MetricRef(
                model_key=metric_model,
                name="revenue",
                reference=f"{metric_model}.revenue",
            ),
        ),
        dimensions=(),
        grain=("order",),
    )


def _context(pattern, **overrides):
    values = {
        "certified_source_hashes": pattern.model_hashes,
        "source_grains": tuple(
            (source_hash, ("row",)) for source_hash in pattern.model_hashes
        ),
    }
    values.update(overrides)
    return RecommendationContext(**values)


def _recommend(pattern, context, **engine_options):
    return RecommendationEngine(**engine_options).recommend(
        (pattern,), contexts={pattern.query_fingerprint: context}
    )


def test_frequent_aggregate_proposes_a_materialized_view():
    pattern = _pattern()

    result = _recommend(pattern, _context(pattern))

    proposal = result.proposals[0]
    assert proposal.kind == "materialized_view"
    assert proposal.rule_id == "frequent_aggregate"
    assert proposal.generated_schema_path is None
    assert proposal.generated_semantic_draft_path is None


def test_repeated_safe_join_path_proposes_prepared_gold():
    pattern = _pattern(
        model_hashes=("orders-v1", "lines-v1"),
        metric_ids=("lines.revenue",),
        event_count=5,
        duration_percentile=1_500.0,
        duration_point_count=5,
    )

    result = _recommend(
        pattern,
        _context(pattern, semantic_plan=_join_plan(metric_model="lines")),
    )

    assert [(item.kind, item.rule_id) for item in result.proposals] == [
        ("aggregate_table", "repeated_join_path")
    ]


def test_missing_dimension_is_always_a_semantic_only_gap():
    pattern = _pattern(
        status="failed",
        event_count=5,
        duration_percentile=None,
        duration_point_count=0,
    )

    result = _recommend(
        pattern,
        _context(pattern, missing_dimension_concept="customer_segment"),
    )

    assert len(result.proposals) == 1
    proposal = result.proposals[0]
    assert proposal.kind == "semantic_gap"
    assert proposal.expected_benefit["action"] == "review_semantic_dimension_gap"
    assert proposal.risks == ("semantic_only:no_physical_asset_creation",)
    assert proposal.generated_schema_path is None


def test_unused_generated_asset_is_review_only_and_never_a_drop():
    pattern = _pattern(event_count=1, duration_point_count=1)

    result = _recommend(
        pattern,
        _context(
            pattern,
            generated_asset_id="gold.orders_rollup",
            unused_observation_count=3,
        ),
    )

    deprecation = next(
        item for item in result.proposals if item.rule_id == "unused_generated_asset"
    )
    assert deprecation.kind == "deprecation"
    assert deprecation.expected_benefit["action"] == "review_deprecation"
    assert deprecation.risks == ("review_only:no_automatic_drop",)
    assert "drop" not in json.dumps(deprecation.expected_benefit).lower()


def test_uncertified_source_refuses_every_candidate_explicitly():
    pattern = _pattern()

    result = _recommend(pattern, _context(pattern, certified_source_hashes=()))

    assert result.proposals == ()
    refusal = next(item for item in result.refusals if item.rule_id == "frequent_aggregate")
    assert refusal.contraindications == ("source_not_certified:orders-v1",)


def test_directional_fanout_refuses_join_proposal():
    pattern = _pattern(
        model_hashes=("orders-v1", "lines-v1"),
        event_count=5,
        duration_percentile=1_500.0,
        duration_point_count=5,
    )

    result = _recommend(
        pattern,
        _context(pattern, semantic_plan=_join_plan(metric_model="orders")),
    )

    assert result.proposals == ()
    refusal = next(item for item in result.refusals if item.rule_id == "repeated_join_path")
    assert "unsafe_fanout:order_lines:forward" in refusal.contraindications


@pytest.mark.parametrize(
    ("context_override", "contraindication"),
    [
        ({"uses_raw_sql": True}, "raw_sql_not_compilable"),
        ({"uses_python_rule": True}, "python_rule_not_compilable"),
        ({"source_grains": ()}, "unsafe_grain:missing:orders-v1"),
    ],
)
def test_non_compilable_or_unsafe_physical_pattern_is_refused(
    context_override, contraindication
):
    pattern = _pattern()

    result = _recommend(pattern, _context(pattern, **context_override))

    assert result.proposals == ()
    assert any(
        contraindication in refusal.contraindications
        for refusal in result.refusals
        if refusal.rule_id == "frequent_aggregate"
    )


def test_benefit_below_threshold_is_refused_and_named():
    pattern = _pattern(event_count=9, duration_point_count=9)

    result = _recommend(pattern, _context(pattern))

    assert result.proposals == ()
    refusal = next(item for item in result.refusals if item.rule_id == "frequent_aggregate")
    assert "benefit_not_proven:event_count" in refusal.contraindications


@pytest.mark.parametrize(
    ("field_name", "rule_id", "below_pattern", "at_pattern", "context_options"),
    [
        (
            "frequent_min_event_count",
            "frequent_aggregate",
            {"event_count": 9, "duration_point_count": 9},
            {"event_count": 10, "duration_point_count": 10},
            {},
        ),
        (
            "frequent_min_duration_ms",
            "frequent_aggregate",
            {"duration_percentile": 999.0},
            {"duration_percentile": 1_000.0},
            {},
        ),
        (
            "join_min_event_count",
            "repeated_join_path",
            {
                "model_hashes": ("orders-v1", "lines-v1"),
                "event_count": 4,
                "duration_point_count": 4,
                "duration_percentile": 1_500.0,
            },
            {
                "model_hashes": ("orders-v1", "lines-v1"),
                "event_count": 5,
                "duration_point_count": 5,
                "duration_percentile": 1_500.0,
            },
            {"semantic_plan": _join_plan(metric_model="lines")},
        ),
        (
            "join_min_duration_ms",
            "repeated_join_path",
            {
                "model_hashes": ("orders-v1", "lines-v1"),
                "event_count": 5,
                "duration_point_count": 5,
                "duration_percentile": 1_499.0,
            },
            {
                "model_hashes": ("orders-v1", "lines-v1"),
                "event_count": 5,
                "duration_point_count": 5,
                "duration_percentile": 1_500.0,
            },
            {"semantic_plan": _join_plan(metric_model="lines")},
        ),
        (
            "missing_dimension_min_rejections",
            "missing_dimension",
            {
                "status": "failed",
                "event_count": 4,
                "duration_point_count": 0,
                "duration_percentile": None,
            },
            {
                "status": "failed",
                "event_count": 5,
                "duration_point_count": 0,
                "duration_percentile": None,
            },
            {"missing_dimension_concept": "customer_segment"},
        ),
    ],
)
def test_minimum_thresholds_refuse_just_below_and_propose_at_threshold(
    field_name, rule_id, below_pattern, at_pattern, context_options
):
    below = _pattern(**below_pattern)
    at_threshold = _pattern(**at_pattern)

    below_result = _recommend(below, _context(below, **context_options))
    at_result = _recommend(at_threshold, _context(at_threshold, **context_options))

    assert not any(item.rule_id == rule_id for item in below_result.proposals)
    assert any(item.rule_id == rule_id for item in at_result.proposals), field_name


def test_unused_observation_minimum_has_both_sides():
    pattern = _pattern(event_count=1, duration_point_count=1)

    below = _recommend(
        pattern,
        _context(
            pattern, generated_asset_id="gold.rollup", unused_observation_count=2
        ),
    )
    at_threshold = _recommend(
        pattern,
        _context(
            pattern, generated_asset_id="gold.rollup", unused_observation_count=3
        ),
    )

    assert not any(item.rule_id == "unused_generated_asset" for item in below.proposals)
    assert any(
        item.rule_id == "unused_generated_asset" for item in at_threshold.proposals
    )


def test_unused_query_count_upper_bound_has_both_sides():
    allowed = _pattern(event_count=1, duration_point_count=1)
    above = _pattern(event_count=2, duration_point_count=2)

    allowed_result = _recommend(
        allowed,
        _context(
            allowed, generated_asset_id="gold.rollup", unused_observation_count=3
        ),
    )
    above_result = _recommend(
        above,
        _context(
            above, generated_asset_id="gold.rollup", unused_observation_count=3
        ),
    )

    assert any(
        item.rule_id == "unused_generated_asset" for item in allowed_result.proposals
    )
    assert not any(
        item.rule_id == "unused_generated_asset" for item in above_result.proposals
    )
    refusal = next(
        item
        for item in above_result.refusals
        if item.rule_id == "unused_generated_asset"
    )
    assert "benefit_not_proven:query_event_count" in refusal.contraindications


def test_score_is_decomposable_and_refusal_keeps_every_contraindication():
    passing = _pattern()
    proposal = _recommend(passing, _context(passing)).proposals[0]

    assert proposal.expected_benefit["score_method"] == "named_thresholds_v1"
    assert proposal.expected_benefit["thresholds"] == {
        "event_count": {
            "observed": 10,
            "operator": ">=",
            "required": 10,
            "passed": True,
        },
        "duration_percentile_ms": {
            "observed": 1_000.0,
            "operator": ">=",
            "required": 1_000.0,
            "passed": True,
        },
    }
    assert proposal.risks == ("human_review_required:no_automatic_deployment",)

    blocked = _pattern(event_count=9, duration_point_count=9)
    refusal = _recommend(
        blocked,
        _context(
            blocked,
            certified_source_hashes=(),
            uses_raw_sql=True,
            uses_python_rule=True,
            source_grains=(),
        ),
    ).refusals[0]
    assert refusal.reasons
    assert set(refusal.contraindications) == {
        "benefit_not_proven:event_count",
        "python_rule_not_compilable",
        "raw_sql_not_compilable",
        "source_not_certified:orders-v1",
        "unsafe_grain:missing:orders-v1",
    }


def test_proposal_contract_is_exact_frozen_and_allowlisted():
    expected_fields = [
        "proposal_id",
        "kind",
        "rule_id",
        "rule_version",
        "evidence_event_ids",
        "expected_benefit",
        "risks",
        "generated_schema_path",
        "generated_semantic_draft_path",
        "source_definition_hashes",
        "status",
    ]
    proposal = _recommend(_pattern(), _context(_pattern())).proposals[0]
    object.__setattr__(proposal, "future_sensitive_field", "SECRET_CANARY")

    with pytest.raises(FrozenInstanceError):
        proposal.status = "accepted"
    assert [field.name for field in fields(OptimizationProposal)] == expected_fields
    serialized = json.dumps(proposal.to_dict(), sort_keys=True)
    assert "future_sensitive_field" not in serialized
    assert "SECRET_CANARY" not in serialized


def test_output_and_content_derived_id_are_stable_across_hash_seeds():
    script = textwrap.dedent(
        """
        import json
        from datetime import datetime, timedelta, timezone
        from skifer.adaptive import (
            PatternAggregate, RecommendationContext, RecommendationEngine,
            fingerprint_query,
        )

        now = datetime(2026, 9, 8, 12, tzinfo=timezone.utc)
        models = tuple({'orders-v1'})
        metrics = tuple({'orders.revenue', 'orders.count'})
        dimensions = tuple({'orders.region', 'orders.day'})
        filters = tuple({'orders.status:eq', 'orders.region:in'})
        fingerprint = fingerprint_query(
            model_hashes=models, metric_ids=metrics, dimension_ids=dimensions,
            normalized_filter_shape=filters,
        )
        pattern = PatternAggregate(
            window='7d', window_started_at=now - timedelta(days=7),
            window_ended_at=now, environment='prod', consumer_class='dashboard',
            model_hashes=models, metric_ids=metrics, dimension_ids=dimensions,
            normalized_filter_shape=filters, query_fingerprint=fingerprint,
            status='succeeded', event_count=10, duration_percentile=1000.0,
            duration_point_count=10,
            event_ids=tuple({'e09', 'e02', 'e07', 'e00', 'e04', 'e06', 'e01', 'e08', 'e03', 'e05'}),
        )
        context = RecommendationContext(
            certified_source_hashes=models,
            source_grains=tuple((item, ('order',)) for item in models),
        )
        result = RecommendationEngine().recommend(
            tuple({pattern}), contexts={fingerprint: context}
        )
        print(json.dumps(result.to_dict(), sort_keys=True, separators=(',', ':')))
        """
    )
    outputs = set()
    for seed in ("1", "17", "999"):
        environment = os.environ.copy()
        environment["PYTHONHASHSEED"] = seed
        outputs.add(
            subprocess.check_output(
                [sys.executable, "-c", script], env=environment, text=True
            ).strip()
        )

    assert len(outputs) == 1
    proposal_id = json.loads(outputs.pop())["proposals"][0]["proposal_id"]
    assert proposal_id.startswith("proposal:v1:")


def test_recommendation_has_no_write_deploy_or_git_collaborator():
    pattern = _pattern()

    with (
        patch("builtins.open", side_effect=AssertionError("disk write attempted")),
        patch("subprocess.run", side_effect=AssertionError("command attempted")),
        patch(
            "skifer.core.core.SkiferEngine.run_from_yaml",
            side_effect=AssertionError("deployment attempted"),
        ),
    ):
        result = _recommend(pattern, _context(pattern))

    assert result.proposals


def test_registry_is_static_versioned_and_unknown_rule_is_an_error():
    assert tuple(RULE_REGISTRY.items()) == (
        ("frequent_aggregate", "1"),
        ("repeated_join_path", "1"),
        ("missing_dimension", "1"),
        ("unused_generated_asset", "1"),
    )
    assert RecommendationEngine().registered_rules == tuple(RULE_REGISTRY.items())
    with pytest.raises(ValueError, match="Unknown recommendation rule"):
        RecommendationEngine(rules=("not_a_rule",))
    with pytest.raises(TypeError):
        RULE_REGISTRY["injected"] = "1"


@pytest.mark.parametrize(
    "options",
    [
        {"frequent_min_event_count": 0},
        {"frequent_min_duration_ms": -1},
        {"join_min_event_count": 0},
        {"join_min_duration_ms": float("nan")},
        {"missing_dimension_min_rejections": 0},
        {"unused_asset_min_observations": 0},
        {"unused_asset_max_query_count": -1},
    ],
)
def test_invalid_thresholds_fail_closed(options):
    with pytest.raises((TypeError, ValueError)):
        RecommendationThresholds(**options)


def test_malformed_aggregate_and_missing_context_fail_closed():
    valid = _pattern()
    malformed = PatternAggregate(
        **{
            **valid.__dict__,
            "event_count": 11,
        }
    )

    with pytest.raises(ValueError, match="event_count must match"):
        RecommendationEngine().recommend(
            (malformed,), contexts={malformed.query_fingerprint: _context(malformed)}
        )
    with pytest.raises(ValueError, match="Missing RecommendationContext"):
        RecommendationEngine().recommend((valid,), contexts={})
