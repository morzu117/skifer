"""Explainable, side-effect-free recommendation rules for adaptive Gold."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
import math
import re
from types import MappingProxyType
from typing import Any, Iterable, Mapping

from skifer.semantic.domain import RELATIONSHIP_CARDINALITIES
from skifer.semantic.planner import SemanticPlan, find_unsafe_fanout

from .aggregator import PatternAggregate
from .models import OptimizationProposal, fingerprint_query


FREQUENT_AGGREGATE_RULE_ID = "frequent_aggregate"
REPEATED_JOIN_PATH_RULE_ID = "repeated_join_path"
MISSING_DIMENSION_RULE_ID = "missing_dimension"
UNUSED_GENERATED_ASSET_RULE_ID = "unused_generated_asset"
RULE_VERSION_V1 = "1"
PROPOSAL_ID_VERSION = "v1"

DEFAULT_FREQUENT_MIN_EVENT_COUNT = 10
DEFAULT_FREQUENT_MIN_DURATION_MS = 1_000.0
DEFAULT_JOIN_MIN_EVENT_COUNT = 5
DEFAULT_JOIN_MIN_DURATION_MS = 1_500.0
DEFAULT_MISSING_DIMENSION_MIN_REJECTIONS = 5
DEFAULT_UNUSED_ASSET_MIN_OBSERVATIONS = 3
DEFAULT_UNUSED_ASSET_MAX_QUERY_COUNT = 1

_LOGICAL_ID = re.compile(r"^[A-Za-z_][A-Za-z0-9_.-]*$")
_RULE_REGISTRY = {
    FREQUENT_AGGREGATE_RULE_ID: RULE_VERSION_V1,
    REPEATED_JOIN_PATH_RULE_ID: RULE_VERSION_V1,
    MISSING_DIMENSION_RULE_ID: RULE_VERSION_V1,
    UNUSED_GENERATED_ASSET_RULE_ID: RULE_VERSION_V1,
}
RULE_REGISTRY: Mapping[str, str] = MappingProxyType(_RULE_REGISTRY)


def _positive_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field_name} must be a positive integer.")
    return value


def _non_negative_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field_name} must be a non-negative integer.")
    return value


def _non_negative_float(value: Any, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{field_name} must be numeric.")
    number = float(value)
    if not math.isfinite(number) or number < 0:
        raise ValueError(f"{field_name} must be finite and non-negative.")
    return number


def _string_tuple(value: Any, field_name: str) -> tuple[str, ...]:
    if not isinstance(value, tuple):
        raise TypeError(f"{field_name} must be a tuple of strings.")
    if any(not isinstance(item, str) or not item for item in value):
        raise ValueError(f"{field_name} must contain only non-empty strings.")
    if len(set(value)) != len(value):
        raise ValueError(f"{field_name} must not contain duplicates.")
    return value


def _logical_id(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not _LOGICAL_ID.fullmatch(value):
        raise ValueError(f"{field_name} must be a non-sensitive logical identifier.")
    return value


@dataclass(frozen=True)
class RecommendationThresholds:
    """Named v1 thresholds; every rule publishes its observed decomposition."""

    frequent_min_event_count: int = DEFAULT_FREQUENT_MIN_EVENT_COUNT
    frequent_min_duration_ms: float = DEFAULT_FREQUENT_MIN_DURATION_MS
    join_min_event_count: int = DEFAULT_JOIN_MIN_EVENT_COUNT
    join_min_duration_ms: float = DEFAULT_JOIN_MIN_DURATION_MS
    missing_dimension_min_rejections: int = (
        DEFAULT_MISSING_DIMENSION_MIN_REJECTIONS
    )
    unused_asset_min_observations: int = DEFAULT_UNUSED_ASSET_MIN_OBSERVATIONS
    unused_asset_max_query_count: int = DEFAULT_UNUSED_ASSET_MAX_QUERY_COUNT

    def __post_init__(self) -> None:
        _positive_int(self.frequent_min_event_count, "frequent_min_event_count")
        _non_negative_float(
            self.frequent_min_duration_ms, "frequent_min_duration_ms"
        )
        _positive_int(self.join_min_event_count, "join_min_event_count")
        _non_negative_float(self.join_min_duration_ms, "join_min_duration_ms")
        _positive_int(
            self.missing_dimension_min_rejections,
            "missing_dimension_min_rejections",
        )
        _positive_int(
            self.unused_asset_min_observations, "unused_asset_min_observations"
        )
        _non_negative_int(
            self.unused_asset_max_query_count, "unused_asset_max_query_count"
        )

    def to_dict(self) -> dict[str, int | float]:
        return {
            "frequent_min_event_count": self.frequent_min_event_count,
            "frequent_min_duration_ms": self.frequent_min_duration_ms,
            "join_min_event_count": self.join_min_event_count,
            "join_min_duration_ms": self.join_min_duration_ms,
            "missing_dimension_min_rejections": (
                self.missing_dimension_min_rejections
            ),
            "unused_asset_min_observations": self.unused_asset_min_observations,
            "unused_asset_max_query_count": self.unused_asset_max_query_count,
        }


@dataclass(frozen=True)
class RecommendationContext:
    """Governance facts deliberately absent from privacy-safe usage aggregates."""

    certified_source_hashes: tuple[str, ...]
    source_grains: tuple[tuple[str, tuple[str, ...]], ...] = ()
    semantic_plan: SemanticPlan | None = None
    uses_raw_sql: bool = False
    uses_python_rule: bool = False
    missing_dimension_concept: str | None = None
    generated_asset_id: str | None = None
    unused_observation_count: int = 0

    def __post_init__(self) -> None:
        _string_tuple(self.certified_source_hashes, "certified_source_hashes")
        if not isinstance(self.source_grains, tuple):
            raise TypeError("source_grains must be a tuple.")
        seen_sources: set[str] = set()
        for index, entry in enumerate(self.source_grains):
            if not isinstance(entry, tuple) or len(entry) != 2:
                raise TypeError(
                    f"source_grains[{index}] must be a (source_hash, grain) tuple."
                )
            source_hash, grain = entry
            if not isinstance(source_hash, str) or not source_hash:
                raise ValueError("source_grains source hashes must be non-empty strings.")
            if source_hash in seen_sources:
                raise ValueError("source_grains must not repeat a source hash.")
            seen_sources.add(source_hash)
            _string_tuple(grain, f"source_grains[{source_hash}]")
        if self.semantic_plan is not None and not isinstance(
            self.semantic_plan, SemanticPlan
        ):
            raise TypeError("semantic_plan must be a SemanticPlan or None.")
        if self.semantic_plan is not None:
            self._validate_semantic_plan(self.semantic_plan)
        if not isinstance(self.uses_raw_sql, bool):
            raise TypeError("uses_raw_sql must be a bool.")
        if not isinstance(self.uses_python_rule, bool):
            raise TypeError("uses_python_rule must be a bool.")
        if self.missing_dimension_concept is not None:
            _logical_id(
                self.missing_dimension_concept, "missing_dimension_concept"
            )
        if self.generated_asset_id is not None:
            _logical_id(self.generated_asset_id, "generated_asset_id")
        _non_negative_int(self.unused_observation_count, "unused_observation_count")

    @staticmethod
    def _validate_semantic_plan(plan: SemanticPlan) -> None:
        _logical_id(plan.root_model, "semantic_plan.root_model")
        _string_tuple(plan.required_models, "semantic_plan.required_models")
        for metric in plan.metrics:
            _logical_id(metric.model_key, "semantic_plan.metrics.model_key")
            _logical_id(metric.name, "semantic_plan.metrics.name")
            _logical_id(metric.reference, "semantic_plan.metrics.reference")
        for join in plan.joins:
            _logical_id(
                join.relationship_name, "semantic_plan.joins.relationship_name"
            )
            _logical_id(join.source_model, "semantic_plan.joins.source_model")
            _logical_id(join.target_model, "semantic_plan.joins.target_model")
            if join.cardinality not in RELATIONSHIP_CARDINALITIES:
                raise ValueError("semantic_plan has an unsupported cardinality.")

    def to_dict(self) -> dict[str, Any]:
        plan = self.semantic_plan
        return {
            "certified_source_hashes": list(self.certified_source_hashes),
            "source_grains": [
                {"source_definition_hash": source_hash, "grain": list(grain)}
                for source_hash, grain in self.source_grains
            ],
            "semantic_plan": None
            if plan is None
            else {
                "root_model": plan.root_model,
                "required_models": list(plan.required_models),
                "metrics": [
                    {
                        "model_key": metric.model_key,
                        "name": metric.name,
                        "reference": metric.reference,
                    }
                    for metric in plan.metrics
                ],
                "joins": [
                    {
                        "relationship_name": join.relationship_name,
                        "source_model": join.source_model,
                        "target_model": join.target_model,
                        "cardinality": join.cardinality,
                    }
                    for join in plan.joins
                ],
            },
            "uses_raw_sql": self.uses_raw_sql,
            "uses_python_rule": self.uses_python_rule,
            "missing_dimension_concept": self.missing_dimension_concept,
            "generated_asset_id": self.generated_asset_id,
            "unused_observation_count": self.unused_observation_count,
        }


@dataclass(frozen=True)
class RecommendationRefusal:
    """A traced decision that deliberately did not become a proposal."""

    rule_id: str
    rule_version: str
    query_fingerprint: str
    evidence_event_ids: tuple[str, ...]
    reasons: tuple[str, ...]
    contraindications: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "rule_version": self.rule_version,
            "query_fingerprint": self.query_fingerprint,
            "evidence_event_ids": list(self.evidence_event_ids),
            "reasons": list(self.reasons),
            "contraindications": list(self.contraindications),
        }


@dataclass(frozen=True)
class RecommendationResult:
    """Standalone deterministic recommendations and their explicit refusals."""

    proposals: tuple[OptimizationProposal, ...]
    refusals: tuple[RecommendationRefusal, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "proposals": [proposal.to_dict() for proposal in self.proposals],
            "refusals": [refusal.to_dict() for refusal in self.refusals],
        }


def _threshold(
    *, observed: int | float | None, operator: str, required: int | float
) -> dict[str, int | float | str | bool | None]:
    if operator == ">=":
        passed = observed is not None and observed >= required
    elif operator == "<=":
        passed = observed is not None and observed <= required
    else:  # fixed internal vocabulary; never config-driven
        raise ValueError(f"Unsupported threshold operator {operator!r}.")
    return {
        "observed": observed,
        "operator": operator,
        "required": required,
        "passed": passed,
    }


class RecommendationEngine:
    """Apply the four static v1 rules without writing or deploying anything."""

    def __init__(
        self,
        *,
        thresholds: RecommendationThresholds | None = None,
        rules: Iterable[str] = tuple(_RULE_REGISTRY),
    ) -> None:
        self.thresholds = thresholds or RecommendationThresholds()
        if not isinstance(self.thresholds, RecommendationThresholds):
            raise TypeError("thresholds must be RecommendationThresholds.")
        if isinstance(rules, (str, bytes)):
            raise TypeError("rules must be an iterable of registered rule IDs.")
        try:
            selected = tuple(rules)
        except TypeError as exc:
            raise TypeError("rules must be an iterable of registered rule IDs.") from exc
        if not selected:
            raise ValueError("rules must select at least one registered rule.")
        if any(not isinstance(rule_id, str) or not rule_id for rule_id in selected):
            raise TypeError("rules must contain only non-empty string rule IDs.")
        if len(set(selected)) != len(selected):
            raise ValueError("rules must not contain duplicates.")
        unknown = sorted(set(selected) - set(_RULE_REGISTRY))
        if unknown:
            raise ValueError(f"Unknown recommendation rule: {unknown[0]!r}.")
        self.rules = tuple(rule_id for rule_id in _RULE_REGISTRY if rule_id in selected)

    @property
    def registered_rules(self) -> tuple[tuple[str, str], ...]:
        return tuple(_RULE_REGISTRY.items())

    def recommend(
        self,
        patterns: Iterable[PatternAggregate],
        *,
        contexts: Mapping[str, RecommendationContext],
    ) -> RecommendationResult:
        """Return proposals and refusals ordered independently of input/hash order."""
        if isinstance(patterns, (str, bytes)):
            raise TypeError("patterns must be an iterable of PatternAggregate values.")
        try:
            supplied = tuple(patterns)
        except TypeError as exc:
            raise TypeError(
                "patterns must be an iterable of PatternAggregate values."
            ) from exc
        if not isinstance(contexts, Mapping):
            raise TypeError("contexts must map query fingerprints to contexts.")

        ordered = sorted(supplied, key=self._pattern_sort_key)
        proposals: list[OptimizationProposal] = []
        refusals: list[RecommendationRefusal] = []
        for pattern in ordered:
            self._validate_pattern(pattern)
            context = contexts.get(pattern.query_fingerprint)
            if not isinstance(context, RecommendationContext):
                raise ValueError(
                    "Missing RecommendationContext for query fingerprint "
                    f"'{pattern.query_fingerprint}'."
                )
            for rule_id in self.rules:
                outcome = self._evaluate(rule_id, pattern, context)
                if outcome is None:
                    continue
                proposal, refusal = outcome
                if proposal is not None:
                    proposals.append(proposal)
                if refusal is not None:
                    refusals.append(refusal)

        proposals.sort(key=lambda proposal: proposal.proposal_id)
        refusals.sort(
            key=lambda refusal: (
                refusal.rule_id,
                refusal.query_fingerprint,
                refusal.evidence_event_ids,
            )
        )
        return RecommendationResult(tuple(proposals), tuple(refusals))

    def _evaluate(self, rule_id, pattern, context):
        if rule_id == FREQUENT_AGGREGATE_RULE_ID:
            return self._frequent_aggregate(pattern, context)
        if rule_id == REPEATED_JOIN_PATH_RULE_ID:
            return self._repeated_join_path(pattern, context)
        if rule_id == MISSING_DIMENSION_RULE_ID:
            return self._missing_dimension(pattern, context)
        if rule_id == UNUSED_GENERATED_ASSET_RULE_ID:
            return self._unused_generated_asset(pattern, context)
        raise ValueError(f"Unknown recommendation rule: {rule_id!r}.")

    def _frequent_aggregate(self, pattern, context):
        if (
            pattern.status != "succeeded"
            or not pattern.metric_ids
            or len(pattern.model_hashes) != 1
        ):
            return None
        checks = {
            "event_count": _threshold(
                observed=pattern.event_count,
                operator=">=",
                required=self.thresholds.frequent_min_event_count,
            ),
            "duration_percentile_ms": _threshold(
                observed=pattern.duration_percentile,
                operator=">=",
                required=self.thresholds.frequent_min_duration_ms,
            ),
        }
        return self._finish(
            rule_id=FREQUENT_AGGREGATE_RULE_ID,
            kind="materialized_view",
            action="review_materialized_view",
            pattern=pattern,
            context=context,
            checks=checks,
            physical=True,
        )

    def _repeated_join_path(self, pattern, context):
        if (
            pattern.status != "succeeded"
            or not pattern.metric_ids
            or len(pattern.model_hashes) < 2
        ):
            return None
        checks = {
            "event_count": _threshold(
                observed=pattern.event_count,
                operator=">=",
                required=self.thresholds.join_min_event_count,
            ),
            "duration_percentile_ms": _threshold(
                observed=pattern.duration_percentile,
                operator=">=",
                required=self.thresholds.join_min_duration_ms,
            ),
        }
        return self._finish(
            rule_id=REPEATED_JOIN_PATH_RULE_ID,
            kind="aggregate_table",
            action="review_prepared_gold",
            pattern=pattern,
            context=context,
            checks=checks,
            physical=True,
            require_join_plan=True,
        )

    def _missing_dimension(self, pattern, context):
        if pattern.status != "failed" or context.missing_dimension_concept is None:
            return None
        checks = {
            "rejected_event_count": _threshold(
                observed=pattern.event_count,
                operator=">=",
                required=self.thresholds.missing_dimension_min_rejections,
            )
        }
        return self._finish(
            rule_id=MISSING_DIMENSION_RULE_ID,
            kind="semantic_gap",
            action="review_semantic_dimension_gap",
            pattern=pattern,
            context=context,
            checks=checks,
            physical=False,
            detail={"known_concept": context.missing_dimension_concept},
            review_risk="semantic_only:no_physical_asset_creation",
        )

    def _unused_generated_asset(self, pattern, context):
        if context.generated_asset_id is None or context.unused_observation_count == 0:
            return None
        checks = {
            "unused_observation_count": _threshold(
                observed=context.unused_observation_count,
                operator=">=",
                required=self.thresholds.unused_asset_min_observations,
            ),
            "query_event_count": _threshold(
                observed=pattern.event_count,
                operator="<=",
                required=self.thresholds.unused_asset_max_query_count,
            ),
        }
        return self._finish(
            rule_id=UNUSED_GENERATED_ASSET_RULE_ID,
            kind="deprecation",
            action="review_deprecation",
            pattern=pattern,
            context=context,
            checks=checks,
            physical=False,
            detail={"generated_asset_id": context.generated_asset_id},
            review_risk="review_only:no_automatic_drop",
        )

    def _finish(
        self,
        *,
        rule_id,
        kind,
        action,
        pattern,
        context,
        checks,
        physical,
        require_join_plan=False,
        detail=None,
        review_risk="human_review_required:no_automatic_deployment",
    ):
        reasons = tuple(
            f"threshold:{name}:observed={check['observed']}:"
            f"{check['operator']}{check['required']}:passed={str(check['passed']).lower()}"
            for name, check in sorted(checks.items())
        )
        contraindications = self._common_guardrails(pattern, context)
        contraindications.extend(
            f"benefit_not_proven:{name}"
            for name, check in sorted(checks.items())
            if not check["passed"]
        )
        if physical:
            contraindications.extend(self._physical_guardrails(pattern, context))
        if require_join_plan:
            contraindications.extend(self._join_guardrails(context))
        contraindications = sorted(set(contraindications))
        if contraindications:
            return None, RecommendationRefusal(
                rule_id=rule_id,
                rule_version=_RULE_REGISTRY[rule_id],
                query_fingerprint=pattern.query_fingerprint,
                evidence_event_ids=tuple(sorted(pattern.event_ids)),
                reasons=reasons,
                contraindications=tuple(contraindications),
            )

        expected_benefit = {
            "score_method": "named_thresholds_v1",
            "action": action,
            "thresholds": checks,
        }
        if detail:
            expected_benefit["detail"] = detail
        risks = (review_risk,)
        proposal_id = self._proposal_id(
            rule_id=rule_id,
            rule_version=_RULE_REGISTRY[rule_id],
            kind=kind,
            pattern=pattern,
            expected_benefit=expected_benefit,
            risks=risks,
        )
        return OptimizationProposal(
            proposal_id=proposal_id,
            kind=kind,
            rule_id=rule_id,
            rule_version=_RULE_REGISTRY[rule_id],
            evidence_event_ids=tuple(sorted(pattern.event_ids)),
            expected_benefit=expected_benefit,
            risks=risks,
            generated_schema_path=None,
            generated_semantic_draft_path=None,
            source_definition_hashes=tuple(sorted(pattern.model_hashes)),
            status="proposed",
        ), None

    @staticmethod
    def _common_guardrails(pattern, context):
        certified = set(context.certified_source_hashes)
        contraindications = [
            f"source_not_certified:{source_hash}"
            for source_hash in pattern.model_hashes
            if source_hash not in certified
        ]
        if context.uses_raw_sql:
            contraindications.append("raw_sql_not_compilable")
        if context.uses_python_rule:
            contraindications.append("python_rule_not_compilable")
        return contraindications

    @staticmethod
    def _physical_guardrails(pattern, context):
        grain_by_source = dict(context.source_grains)
        return [
            f"unsafe_grain:missing:{source_hash}"
            for source_hash in pattern.model_hashes
            if not grain_by_source.get(source_hash)
        ]

    @staticmethod
    def _join_guardrails(context):
        plan = context.semantic_plan
        if plan is None or len(plan.required_models) < 2 or not plan.joins:
            return ["join_plan_missing"]
        contraindications = []
        for join in plan.joins:
            if join.cardinality in {"unknown", "many_to_many"}:
                contraindications.append(
                    f"unsafe_cardinality:{join.relationship_name}:{join.cardinality}"
                )
        risk = find_unsafe_fanout(plan.metrics, plan.joins)
        if risk is not None:
            direction = "forward" if risk.forward else "backward"
            contraindications.append(
                f"unsafe_fanout:{risk.join.relationship_name}:{direction}"
            )
        return contraindications

    @staticmethod
    def _proposal_id(
        *, rule_id, rule_version, kind, pattern, expected_benefit, risks
    ):
        payload = {
            "rule_id": rule_id,
            "rule_version": rule_version,
            "kind": kind,
            "pattern": {
                "window": pattern.window,
                "window_started_at": pattern.window_started_at.isoformat(),
                "window_ended_at": pattern.window_ended_at.isoformat(),
                "environment": pattern.environment,
                "consumer_class": pattern.consumer_class,
                "model_hashes": sorted(pattern.model_hashes),
                "metric_ids": sorted(pattern.metric_ids),
                "dimension_ids": sorted(pattern.dimension_ids),
                "normalized_filter_shape": sorted(
                    pattern.normalized_filter_shape
                ),
                "query_fingerprint": pattern.query_fingerprint,
                "status": pattern.status,
                "event_count": pattern.event_count,
                "duration_percentile": pattern.duration_percentile,
                "duration_point_count": pattern.duration_point_count,
                "event_ids": sorted(pattern.event_ids),
            },
            "expected_benefit": expected_benefit,
            "risks": list(risks),
            "source_definition_hashes": sorted(pattern.model_hashes),
        }
        canonical = json.dumps(
            payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True
        )
        digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        return f"proposal:{PROPOSAL_ID_VERSION}:{digest}"

    @staticmethod
    def _pattern_sort_key(pattern):
        if not isinstance(pattern, PatternAggregate):
            raise TypeError("patterns must contain only PatternAggregate values.")
        return (
            pattern.query_fingerprint,
            pattern.window_started_at.isoformat()
            if hasattr(pattern.window_started_at, "isoformat")
            else "",
            pattern.window,
            pattern.status,
            pattern.event_ids,
        )

    @staticmethod
    def _validate_pattern(pattern):
        if not isinstance(pattern, PatternAggregate):
            raise TypeError("patterns must contain only PatternAggregate values.")
        for field_name in (
            "environment",
            "consumer_class",
            "query_fingerprint",
            "window",
        ):
            value = getattr(pattern, field_name)
            if not isinstance(value, str) or not value:
                raise ValueError(f"PatternAggregate.{field_name} must be non-empty.")
        for field_name in (
            "model_hashes",
            "metric_ids",
            "dimension_ids",
            "normalized_filter_shape",
            "event_ids",
        ):
            _string_tuple(getattr(pattern, field_name), f"PatternAggregate.{field_name}")
        if not pattern.model_hashes:
            raise ValueError("PatternAggregate.model_hashes must not be empty.")
        if not pattern.event_ids:
            raise ValueError("PatternAggregate.event_ids must not be empty.")
        for field_name in ("window_started_at", "window_ended_at"):
            value = getattr(pattern, field_name)
            if not isinstance(value, datetime):
                raise TypeError(f"PatternAggregate.{field_name} must be a datetime.")
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError(f"PatternAggregate.{field_name} must be timezone-aware.")
        if pattern.window_started_at > pattern.window_ended_at:
            raise ValueError("PatternAggregate window bounds are reversed.")
        if pattern.status not in {"succeeded", "failed"}:
            raise ValueError("PatternAggregate.status is not supported.")
        _positive_int(pattern.event_count, "PatternAggregate.event_count")
        if pattern.event_count != len(pattern.event_ids):
            raise ValueError("PatternAggregate.event_count must match event_ids.")
        _non_negative_int(
            pattern.duration_point_count, "PatternAggregate.duration_point_count"
        )
        if pattern.duration_point_count > pattern.event_count:
            raise ValueError("PatternAggregate duration points exceed event count.")
        if pattern.duration_percentile is not None:
            _non_negative_float(
                pattern.duration_percentile, "PatternAggregate.duration_percentile"
            )
            if pattern.duration_point_count == 0:
                raise ValueError(
                    "PatternAggregate duration percentile requires duration points."
                )
        if pattern.status == "failed" and (
            pattern.duration_point_count != 0
            or pattern.duration_percentile is not None
        ):
            raise ValueError("Failed PatternAggregate cannot carry duration evidence.")
        expected_fingerprint = fingerprint_query(
            model_hashes=pattern.model_hashes,
            metric_ids=pattern.metric_ids,
            dimension_ids=pattern.dimension_ids,
            normalized_filter_shape=pattern.normalized_filter_shape,
        )
        if pattern.query_fingerprint != expected_fingerprint:
            raise ValueError("PatternAggregate query fingerprint is inconsistent.")
