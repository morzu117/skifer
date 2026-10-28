"""Usage drift signals: is the semantic layer covering fewer of the questions asked? (Plan 44, S07)

Observation only. This module reads the candidate store, the ``adaptive`` usage
store and the knowledge index; it changes no state, writes nothing and issues no
DDL — a test checks the source. Thresholds may be configured, they are never
applied in v1 (``thresholds_applied`` is always ``False``).

Same discipline as ``adaptive/evaluator.py``: half-open windows ``[since, until)``,
a minimum sample under which a signal is ``inconclusive``, an injected clock
upstream (the caller passes the window), iteration over sorted keys.

The candidate store carries no environment column: it is partitioned physically
(one SQLite file or one Delta schema per environment's catalog). The usage events
are filtered on ``environment`` here, so two environments never mix in a signal.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable

SIGNALS = ("fallback_rate", "orphan_certified_metrics", "rephrasing")
OPTIONAL_SIGNALS = ("reference_degradation",)   # S09: only with stored evaluations
INCONCLUSIVE_REASONS = ("sample_below_minimum", "evidence_unavailable", "questions_redacted")


@dataclass(frozen=True)
class DriftSignal:
    name: str                      # "fallback_rate" | "orphan_certified_metrics" | "rephrasing"
    status: str                    # "measured" | "inconclusive"
    value: float | None
    sample_size: int
    window: tuple[str, str]        # ISO, [since, until)
    details: tuple[dict, ...]
    reason: str | None

    def to_dict(self) -> dict:
        return {"name": self.name, "status": self.status, "value": self.value,
                "sample_size": self.sample_size, "window": list(self.window),
                "details": [dict(detail) for detail in self.details], "reason": self.reason}


@dataclass(frozen=True)
class DriftReport:
    environment: str
    signals: tuple[DriftSignal, ...]
    thresholds_applied: bool       # always False in v1

    def signal(self, name: str) -> DriftSignal:
        return next(signal for signal in self.signals if signal.name == name)

    def to_dict(self) -> dict:
        return {"environment": self.environment, "thresholds_applied": self.thresholds_applied,
                "signals": [signal.to_dict() for signal in self.signals]}


def _aware_utc(value: Any, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{field_name} must be a datetime.")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware.")
    return value.astimezone(timezone.utc)


def _within(moment: datetime, since: datetime, until: datetime) -> bool:
    return since <= moment.astimezone(timezone.utc) < until


def _signal(name, window, sample, *, value=None, details=(), reason=None) -> DriftSignal:
    status = "inconclusive" if reason is not None else "measured"
    return DriftSignal(name, status, None if reason else round(value, 6), sample, window,
                       tuple(details), reason)


def compute_drift(
    *,
    candidate_store: Any,
    usage_store: Any,
    catalog_models: Iterable[str],
    index: Any,
    environment: str,
    since: datetime,
    until: datetime,
    min_sample: int,
    rephrasing_overlap: float = 0.6,
    evaluations: list[dict] | None = None,
) -> DriftReport:
    """Three signals over ``[since, until)`` for one environment; reads only.

    With ``evaluations`` (stored ``EvaluationReport`` dicts, oldest first — S09), a
    fourth signal, ``reference_degradation``, compares the last report to the one before.
    """
    since, until = _aware_utc(since, "since"), _aware_utc(until, "until")
    if since >= until:
        raise ValueError("since must be before until.")
    if not isinstance(environment, str) or not environment.strip():
        raise ValueError("environment must be a non-empty string.")
    if isinstance(min_sample, bool) or not isinstance(min_sample, int) or min_sample < 0:
        raise ValueError(f"min_sample must be a non-negative integer; got {min_sample!r}.")
    if (isinstance(rephrasing_overlap, bool) or not isinstance(rephrasing_overlap, (int, float))
            or not 0 < rephrasing_overlap <= 1):
        raise ValueError(f"rephrasing_overlap must be within (0, 1]; got {rephrasing_overlap!r}.")
    window = (since.isoformat(), until.isoformat())

    # The store bound is inclusive: the half-open window is enforced here.
    usage = sorted(
        (event for event in usage_store.list_events(environment=environment, since=since,
                                                    until=until)
         if event.environment == environment and _within(event.occurred_at, since, until)),
        key=lambda event: (event.occurred_at, event.event_id),
    )
    candidates = sorted(candidate_store.list_candidates(), key=lambda c: c.candidate_id)
    signals = (
        _fallback_rate(candidate_store, candidates, usage, window, since, until, min_sample),
        _orphans(catalog_models, usage, window, min_sample),
        _rephrasing(candidate_store, candidates, index, window, since, until, min_sample,
                    float(rephrasing_overlap)),
    )
    if evaluations is not None:
        signals += (_reference_degradation(evaluations, window),)
    return DriftReport(environment, signals, thresholds_applied=False)


def _reference_degradation(evaluations: list[dict], window) -> DriftSignal:
    """The A or B metrics lower in the last evaluation than in the previous one."""
    if len(evaluations) < 2:
        return _signal("reference_degradation", window, len(evaluations),
                       reason="evidence_unavailable")
    previous, current = (report.get("metrics") or {} for report in evaluations[-2:])
    declines = [
        {"metric": name, "previous": previous[name], "current": current[name]}
        for name in sorted(set(previous) & set(current))
        if name.startswith(("A.", "B.")) and isinstance(previous[name], (int, float))
        and isinstance(current[name], (int, float)) and current[name] < previous[name]
    ]
    return _signal("reference_degradation", window, 2, value=float(len(declines)),
                   details=declines)


def _fallback_rate(store, candidates, usage, window, since, until, min_sample) -> DriftSignal:
    fallbacks = sum(
        1
        for candidate in candidates
        for run in store.list_runs(candidate.candidate_id, purpose="answer")
        if _within(run.executed_at, since, until)
    )
    sample = fallbacks + len(usage)
    if not usage:
        return _signal("fallback_rate", window, sample, reason="evidence_unavailable")
    if sample < min_sample:
        return _signal("fallback_rate", window, sample, reason="sample_below_minimum")
    return _signal("fallback_rate", window, sample, value=fallbacks / sample)


def _orphans(catalog_models, usage, window, min_sample) -> DriftSignal:
    metrics = sorted(set(catalog_models))
    sample = len(usage)
    if not usage:
        return _signal("orphan_certified_metrics", window, sample, reason="evidence_unavailable")
    if sample < min_sample:
        return _signal("orphan_certified_metrics", window, sample, reason="sample_below_minimum")
    used = {metric for event in usage for metric in event.metric_ids}
    orphans = [metric for metric in metrics if metric not in used]
    return _signal("orphan_certified_metrics", window, sample,
                   value=len(orphans) / len(metrics) if metrics else 0.0,
                   details=[{"metric": metric} for metric in orphans])


def _expand(terms: tuple[str, ...], phrase: tuple[str, ...]) -> set[str]:
    """A question term that spells the initials of a run of phrase terms stands for that run.

    "CA" in a question is "chiffre d'affaires" in a synonym: without this, the
    abbreviation a user types would never meet the words a steward wrote.
    """
    expanded = set()
    for term in terms:
        run = None
        for start in range(len(phrase)):
            for end in range(start + 2, len(phrase) + 1):
                if "".join(word[0] for word in phrase[start:end]) == term:
                    run = phrase[start:end]
                    break
            if run:
                break
        expanded.update(run or (term,))
    return expanded


def _overlap(question_terms: tuple[str, ...], node) -> float:
    """Best Jaccard between the question and one phrase of the node (its label or a synonym)."""
    from skifer.knowledge.index import tokenize

    best = 0.0
    for phrase in (node.label, *node.synonyms):
        words = tokenize(phrase)
        if not words:
            continue
        question = _expand(question_terms, words)
        target = set(words)
        best = max(best, len(question & target) / len(question | target))
    return best


def _rephrasing(store, candidates, index, window, since, until, min_sample,
                threshold) -> DriftSignal:
    from skifer.knowledge.index import tokenize

    created = [c for c in candidates if _within(c.created_at, since, until)]
    if getattr(store, "store_questions", "full") == "redacted":
        return _signal("rephrasing", window, len(created), reason="questions_redacted")
    asked = [(c, tokenize(c.origin_question)) for c in created if c.origin_question]
    if len(asked) < min_sample:
        return _signal("rephrasing", window, len(asked), reason="sample_below_minimum")
    certified = [node for node in index.nodes(kinds=["metric"]) if node.status == "certified"]
    scored = ((candidate, node, _overlap(terms, node))
              for candidate, terms in asked for node in certified)
    pairs = [{"candidate_id": candidate.candidate_id, "node_id": node.node_id,
              "overlap": round(overlap, 6)}
             for candidate, node, overlap in scored if overlap >= threshold]
    rephrased = {pair["candidate_id"] for pair in pairs}
    return _signal("rephrasing", window, len(asked),
                   value=len(rephrased) / len(asked) if asked else 0.0, details=pairs)
