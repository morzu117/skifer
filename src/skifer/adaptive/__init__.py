"""Supervised adaptive Gold foundations."""

from .aggregator import PatternAggregate, PatternAggregationResult, PatternAggregator
from .generator import ProposalGenerationError, ProposalGenerator
from .evaluator import (
    DeliveryRecord,
    OutcomeEvaluation,
    OutcomeEvaluationError,
    OutcomeEvaluator,
    WindowMetrics,
)
from .models import (
    OptimizationProposal,
    SemanticUsageEvent,
    fingerprint_query,
    usage_event_from_evidence,
)
from .recommender import (
    RULE_REGISTRY,
    RecommendationContext,
    RecommendationEngine,
    RecommendationRefusal,
    RecommendationResult,
    RecommendationThresholds,
)
from .store import DeltaUsageEventStore, SqliteUsageEventStore, UsageEventStore
from .workflow import (
    AdaptiveWorkflow,
    AdaptiveWorkflowConflict,
    AdaptiveWorkflowError,
    StaleProposalError,
)

__all__ = [
    "DeltaUsageEventStore",
    "DeliveryRecord",
    "AdaptiveWorkflow",
    "AdaptiveWorkflowConflict",
    "AdaptiveWorkflowError",
    "PatternAggregate",
    "PatternAggregationResult",
    "PatternAggregator",
    "OptimizationProposal",
    "OutcomeEvaluation",
    "OutcomeEvaluationError",
    "OutcomeEvaluator",
    "ProposalGenerationError",
    "ProposalGenerator",
    "RULE_REGISTRY",
    "RecommendationContext",
    "RecommendationEngine",
    "RecommendationRefusal",
    "RecommendationResult",
    "RecommendationThresholds",
    "SemanticUsageEvent",
    "SqliteUsageEventStore",
    "StaleProposalError",
    "UsageEventStore",
    "WindowMetrics",
    "fingerprint_query",
    "usage_event_from_evidence",
]
