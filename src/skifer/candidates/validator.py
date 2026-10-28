"""Validation of a metric candidate: coherence, frequency, stability (Plan 44, S05).

Each criterion is explainable in the ``adaptive`` way: it returns a verdict and its
named reasons, each with the observed value and the threshold. Nothing here
changes a candidate's state — the caller (``promotion``, the CLI job) writes the
transitions from the ``ValidationReport``. The only writes are the stability runs
themselves, which are part of the audit trail, never of the state.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import os
from typing import Any, Callable
import uuid

import yaml

from .models import CandidateRun, MetricCandidate
from .promotion import level1_sql, metric_definition

CRITERIA = ("coherence", "frequency", "stability")
OUTCOMES = ("validated", "needs_review", "pending")
STABILITY_ROW_LIMIT = 10_000


@dataclass(frozen=True)
class CriterionResult:
    criterion: str                 # "coherence" | "frequency" | "stability"
    passed: bool
    reasons: tuple[dict, ...]      # {"code", "observed", "threshold", "verdict"}

    def to_dict(self) -> dict:
        return {"criterion": self.criterion, "passed": self.passed,
                "reasons": [dict(reason) for reason in self.reasons]}


@dataclass(frozen=True)
class ValidationReport:
    candidate_id: str
    results: tuple[CriterionResult, CriterionResult, CriterionResult]
    outcome: str                   # "validated" | "needs_review" | "pending"
    semantic_layer_version: str    # sha256:v1: of semantic_catalog.yaml when judged

    def criterion(self, name: str) -> CriterionResult:
        return next(result for result in self.results if result.criterion == name)

    def to_dict(self) -> dict:
        return {
            "candidate_id": self.candidate_id,
            "outcome": self.outcome,
            "semantic_layer_version": self.semantic_layer_version,
            "results": [result.to_dict() for result in self.results],
        }


@dataclass(frozen=True)
class ValidationThresholds:
    min_requests: int = 5
    min_distinct_requesters: int = 2
    min_days: int = 2
    stability_runs: int = 3
    min_stability: float = 1.0

    def __post_init__(self) -> None:
        for name in ("min_requests", "min_distinct_requesters", "min_days"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer; got {value!r}.")
        runs = self.stability_runs
        if isinstance(runs, bool) or not isinstance(runs, int) or runs < 1:
            raise ValueError(f"stability_runs must be a positive integer; got {runs!r}.")
        score = self.min_stability
        if isinstance(score, bool) or not isinstance(score, (int, float)) or not 0 <= score <= 1:
            raise ValueError(f"min_stability must be within [0, 1]; got {score!r}.")


def _reason(code: str, observed: Any, threshold: Any, passed: bool) -> dict:
    return {"code": code, "observed": observed, "threshold": threshold,
            "verdict": "pass" if passed else "fail"}


def semantic_layer_version(models_dir: str | None) -> str:
    """``sha256:v1:`` of ``semantic_catalog.yaml`` as it is now (empty when absent)."""
    content = b""
    if models_dir:
        path = os.path.join(models_dir, "semantic_catalog.yaml")
        if os.path.exists(path):
            with open(path, "rb") as handle:
                content = handle.read()
    return "sha256:v1:" + hashlib.sha256(content).hexdigest()


class CandidateValidator:
    """Judges one candidate against injected thresholds; never changes its state."""

    def __init__(
        self,
        store: Any,
        engine: Any,
        index: Any,
        *,
        thresholds: ValidationThresholds,
        clock: Callable[[], datetime],
    ):
        if not isinstance(thresholds, ValidationThresholds):
            raise TypeError("thresholds must be a ValidationThresholds.")
        if clock is None:
            raise ValueError("clock is required: a validation must be reproducible.")
        from skifer.agentic.candidate_agent import MetricCandidateAgent

        self.store = store
        self.engine = engine
        self.index = index
        self.thresholds = thresholds
        self.clock = clock
        # Same join, fanout and execution rules as the agent that built the candidate.
        self._checker = MetricCandidateAgent(
            engine, None, index, store, row_limit=STABILITY_ROW_LIMIT, clock=clock
        )

    # --- coherence ---------------------------------------------------------------------

    def coherence(self, candidate: MetricCandidate) -> CriterionResult:
        from skifer import parse_schema
        from skifer.agentic.candidate_agent import ProposalError

        reasons = [_reason("residue_present", len(candidate.residues), 0, not candidate.residues)]
        try:
            # What is judged is what will be published: the {{ pN }} filters of one
            # question are dimensions there, and parse_schema would read them as
            # missing template parameters.
            definition, _ = metric_definition(candidate.schema_dict)
            parsed = parse_schema(yaml.safe_dump(definition, sort_keys=False))
        except Exception as exc:
            reasons.append(_reason("invalid_schema", type(exc).__name__, None, False))
            return CriterionResult("coherence", False, tuple(reasons))
        reasons.append(_reason("invalid_schema", None, None, True))

        missing = sorted(t for t in candidate.tables_used if self.index.get(f"table:{t}") is None)
        reasons.append(_reason("missing_table", missing, [], not missing))

        aliases = {
            key: table["name"]
            for table in parsed["tables"]
            for key in (table.get("alias") or table["name"], table["name"])
        }
        try:
            _, planned, undeclared = self._checker._check_joins(parsed, aliases)
            reasons.append(_reason("undeclared_join", sorted(undeclared), [], not undeclared))
            self._checker._check_fanout(parsed, aliases, planned)
            reasons.append(_reason("unsafe_fanout", None, None, True))
        except ProposalError as exc:
            reasons.append(_reason(exc.code.value, str(exc), None, False))

        reasons.append(self._certification_reason(candidate))
        passed = all(reason["verdict"] == "pass" for reason in reasons)
        return CriterionResult("coherence", passed, tuple(reasons))

    def _certification_reason(self, candidate: MetricCandidate) -> dict:
        from skifer.semantic.dependencies import SemanticDependency
        from skifer.semantic.access_policy import SemanticAccessDenied

        try:
            self.engine._enforce_certification_gate_with_evaluation(
                f"candidate.{candidate.metric_name}", {}, None,
                deps=tuple(SemanticDependency(dataset=t) for t in candidate.tables_used),
            )
        except SemanticAccessDenied as exc:
            return _reason("source_not_certified", exc.decision.value, "ALLOW", False)
        return _reason("source_not_certified", None, "ALLOW", True)

    # --- frequency ---------------------------------------------------------------------

    def frequency(self, candidate: MetricCandidate) -> CriterionResult:
        t = self.thresholds
        reasons = [_reason("min_requests", candidate.request_count, t.min_requests,
                           candidate.request_count >= t.min_requests)]
        requesters = len(candidate.distinct_requesters)
        if requesters == 0:
            # Pseudonymisation unavailable: no requester was ever counted. Never a
            # default success, whatever the threshold says.
            reasons.append(_reason("requesters_unavailable", 0, t.min_distinct_requesters, False))
        else:
            reasons.append(_reason("min_distinct_requesters", requesters,
                                   t.min_distinct_requesters,
                                   requesters >= t.min_distinct_requesters))
        days = len({
            run.executed_at.astimezone(timezone.utc).date()
            for run in self.store.list_runs(candidate.candidate_id, purpose="answer")
            if run.error_type is None
        })
        reasons.append(_reason("min_days", days, t.min_days, days >= t.min_days))
        passed = all(reason["verdict"] == "pass" for reason in reasons)
        return CriterionResult("frequency", passed, tuple(reasons))

    # --- stability ---------------------------------------------------------------------

    def stability(self, candidate: MetricCandidate, *, execute: bool = True) -> CriterionResult:
        """Share of identical result hashes over ``stability_runs`` runs of the published SQL.

        Without ``execute`` (dry run), nothing runs: the score is read from the last
        stability runs already recorded, and is not measured if there are too few.
        """
        t = self.thresholds
        if execute:
            sql = level1_sql(candidate)
            hashes = [self._stability_run(candidate, sql) for _ in range(t.stability_runs)]
        else:
            runs = self.store.list_runs(candidate.candidate_id, purpose="stability")
            if len(runs) < t.stability_runs:
                reason = _reason("stability_not_measured", len(runs), t.stability_runs, False)
                return CriterionResult("stability", False, (reason,))
            runs = sorted(runs, key=lambda run: (run.executed_at, run.run_id))[-t.stability_runs:]
            hashes = [run.result_hash for run in runs]
        counted = Counter(h for h in hashes if h is not None)
        score = (counted.most_common(1)[0][1] if counted else 0) / len(hashes)
        passed = score >= t.min_stability
        return CriterionResult(
            "stability", passed, (_reason("min_stability", round(score, 6), t.min_stability, passed),)
        )

    def _stability_run(self, candidate: MetricCandidate, sql: str) -> str | None:
        from skifer.agentic.candidate_agent import result_hash

        error_type = digest = row_count = None
        try:
            _, rows = self._checker._execute(sql)
            digest, row_count = result_hash(rows), len(rows)
        except Exception as exc:
            error_type = type(exc).__name__
        self.store.append_run(CandidateRun(
            run_id=uuid.uuid4().hex, candidate_id=candidate.candidate_id,
            executed_at=self.clock(), purpose="stability", result_hash=digest,
            row_count=row_count, error_type=error_type,
        ))
        return digest

    # --- report ------------------------------------------------------------------------

    def validate(self, candidate_id: str, *, execute: bool = True) -> ValidationReport:
        candidate = self.store.get_candidate(candidate_id)
        if candidate is None:
            raise KeyError(f"Unknown candidate {candidate_id!r}.")
        version = semantic_layer_version(getattr(self.engine, "models_dir", None))
        coherence = self.coherence(candidate)
        frequency = self.frequency(candidate)
        if not coherence.passed:
            outcome = "needs_review"
        elif not frequency.passed:
            outcome = "pending"
        if coherence.passed and frequency.passed:
            stability = self.stability(candidate, execute=execute)
            outcome = "validated" if stability.passed else "needs_review"
        else:
            # A candidate that cannot be published is never run for stability.
            stability = CriterionResult("stability", False, (
                {"code": "not_evaluated", "observed": None, "threshold": None,
                 "verdict": "skipped"},
            ))
        return ValidationReport(candidate_id, (coherence, frequency, stability), outcome, version)
