"""Human arbitration of metric candidates (Plan 44, S06).

``CandidateService`` is the single implementation of a human decision. The CLI
and the Databricks arbitration notebook are thin adapters over it; an API route
or an App would plug into the same methods.

Certification is not a registry state: a metric is certified once its model is
in ``semantic_catalog.yaml`` after ``skifer semantic sync --promote``. The
service stops at the managed drafts, and a human opens the pull request.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
import os
from typing import Any, Callable
import uuid

from .models import (
    ALLOWED_TRANSITIONS,
    CANDIDATE_STATES,
    CertificationEvent,
    MetricCandidate,
)

DECIDER_IDENTITIES = ("hmac", "plain")


class CandidateError(Exception):
    """A decision that cannot be applied; the message names the next action."""


class UnknownCandidateError(CandidateError):
    """No candidate has this id."""


class StaleCandidateError(CandidateError):
    """The semantic layer changed since the candidate was last validated."""


class StateConflictError(CandidateError):
    """The decision is not an allowed transition from the current state."""


class OutputExistsError(CandidateError):
    """A draft file already exists; nothing was written."""


class ApprovalRefusedError(CandidateError):
    """Coherence fails on a reason no human can waive."""


class DeciderIdentityError(CandidateError):
    """No traceable decider identity: the decision is not recorded."""


@dataclass(frozen=True)
class CandidateView:
    """Allowlisted view of a candidate: the same rendering for ``display()`` and ``--json``."""

    candidate_id: str
    metric_name: str
    state: str
    question: str | None
    tables_used: tuple[str, ...]
    request_count: int
    distinct_requesters: int
    stability_score: float | None
    criteria: tuple[dict, ...]
    published_view: str | None
    published_metric_view: str | None
    updated_at: str

    def to_dict(self) -> dict:
        return {
            "candidate_id": self.candidate_id,
            "metric_name": self.metric_name,
            "state": self.state,
            "question": self.question,
            "tables_used": list(self.tables_used),
            "request_count": self.request_count,
            "distinct_requesters": self.distinct_requesters,
            "stability_score": self.stability_score,
            "criteria": [dict(criterion) for criterion in self.criteria],
            "published_view": self.published_view,
            "published_metric_view": self.published_metric_view,
            "updated_at": self.updated_at,
        }


def _report_of(event: CertificationEvent) -> dict | None:
    """The serialized ``ValidationReport`` an event carries, if any."""
    evidence = event.evidence or {}
    if isinstance(evidence.get("report"), dict):
        evidence = evidence["report"]
    return evidence if isinstance(evidence.get("results"), list) else None


def _chronological(events: list[CertificationEvent]) -> list[CertificationEvent]:
    """Oldest first; events sharing a timestamp follow their state chain, not their random id."""
    ordered: list[CertificationEvent] = []
    pending = sorted(events, key=lambda event: (event.event_at, event.event_id))
    while pending:
        first_at = pending[0].event_at
        group = [event for event in pending if event.event_at == first_at]
        pending = pending[len(group):]
        state = ordered[-1].new_state if ordered else None
        while group:
            moves = [e for e in group if e.previous_state != e.new_state]
            follows = [e for e in moves if e.previous_state == state]
            # Otherwise a chain head: a move no other event of the group leads to.
            heads = [e for e in moves
                     if not any(o.new_state == e.previous_state for o in moves if o is not e)]
            event = (follows or heads or group)[0]
            group.remove(event)
            ordered.append(event)
            if event.previous_state != event.new_state:
                state = event.new_state
    return ordered


class CandidateService:
    """List, show and decide on candidates; every decision is an audited event."""

    def __init__(
        self,
        store: Any,
        *,
        engine: Any = None,
        backend: Any = None,
        index_store: Any = None,
        decider_identity: str = "hmac",
        policy: Any = None,
        clock: Callable[[], datetime] | None = None,
    ):
        if decider_identity not in DECIDER_IDENTITIES:
            raise ValueError(
                f"decider_identity must be one of {list(DECIDER_IDENTITIES)}; "
                f"got {decider_identity!r}."
            )
        self.store = store
        self.engine = engine
        self.backend = backend
        self.index_store = index_store
        self.decider_identity = decider_identity
        self.policy = policy
        self._clock = clock

    # --- reading ------------------------------------------------------------------------

    def list(self, *, state: str | None = None, limit: int = 50) -> list[CandidateView]:
        if state is not None and state not in CANDIDATE_STATES:
            raise CandidateError(f"Unknown state {state!r}; expected one of {list(CANDIDATE_STATES)}.")
        return [self._view(candidate)
                for candidate in self.store.list_candidates(state=state, limit=limit)]

    def show(self, candidate_id: str) -> CandidateView:
        return self._view(self._get(candidate_id))

    def history(self, candidate_id: str) -> list[dict]:
        """Every event of the metric, chronological — including those it is the replacement in."""
        return [event.to_dict()
                for event in _chronological(self.store.list_events(metric_id=candidate_id))]

    # --- simple decisions ---------------------------------------------------------------

    def reject(self, candidate_id: str, *, decider: str, reason: str) -> CandidateView:
        """``candidate``/``validated``/``needs_review`` → ``rejected``. Never stale: one can always refuse."""
        identity = self._decider(decider)
        candidate = self._get(candidate_id)
        self._check_state(candidate, "rejected")
        return self._view(self._transition(candidate, "rejected", identity, reason))

    def review(
        self, candidate_id: str, *, decider: str, reason: str, signal: str | None = None
    ) -> CandidateView:
        """``promoted`` → ``under_review``: the only way in, S07 detects without applying."""
        identity = self._decider(decider)
        candidate = self._get(candidate_id)
        self._check_state(candidate, "under_review")
        version = self._check_fresh(candidate)
        evidence = {"signal": signal} if signal else {}
        return self._view(self._transition(candidate, "under_review", identity, reason,
                                           evidence=evidence, version=version))

    # --- helpers ------------------------------------------------------------------------

    def _now(self) -> datetime:
        return self._clock() if self._clock is not None else datetime.now(timezone.utc)

    def _get(self, candidate_id: str) -> MetricCandidate:
        candidate = self.store.get_candidate(candidate_id)
        if candidate is None:
            raise UnknownCandidateError(
                f"Unknown candidate {candidate_id!r} — 'skifer candidates list' shows them."
            )
        return candidate

    def _decider(self, decider: str) -> str:
        """``human:<pseudonym>`` (hmac) or ``human:<name>`` (plain); never an untraceable decision."""
        if not isinstance(decider, str) or not decider.strip():
            raise DeciderIdentityError("A decision needs a decider.")
        if self.decider_identity == "plain":
            return f"human:{decider.strip()}"
        from skifer.observability.tracing import TRACING_HMAC_SECRET_ENV, TraceAttributePolicy

        policy = self.policy or TraceAttributePolicy(
            user_identity="hmac", hmac_secret=os.environ.get(TRACING_HMAC_SECRET_ENV)
        )
        pseudonym = policy.pseudonymize(decider.strip())
        if not pseudonym:
            raise DeciderIdentityError(
                f"No decider pseudonym: set {TRACING_HMAC_SECRET_ENV}, or use "
                "candidates_decider_identity: plain (local and examples only). "
                "A decision without a traceable author is not recorded."
            )
        return f"human:{pseudonym}"

    def _check_state(self, candidate: MetricCandidate, target: str) -> None:
        if target not in ALLOWED_TRANSITIONS[candidate.state]:
            allowed = sorted(ALLOWED_TRANSITIONS[candidate.state]) or ["none"]
            raise StateConflictError(
                f"Candidate {candidate.candidate_id} is {candidate.state}; it cannot become "
                f"{target} (allowed: {', '.join(allowed)})."
            )

    def _last_report_event(self, candidate_id: str) -> CertificationEvent | None:
        events = [event for event in self.store.list_events(metric_id=candidate_id)
                  if event.semantic_layer_version is not None and _report_of(event) is not None]
        return _chronological(events)[-1] if events else None

    def _current_version(self) -> str:
        from .validator import semantic_layer_version

        return semantic_layer_version(getattr(self.engine, "models_dir", None))

    def _check_fresh(self, candidate: MetricCandidate) -> str:
        """The current semantic layer version, if the last validation was made against it."""
        current = self._current_version()
        last = self._last_report_event(candidate.candidate_id)
        if last is None or last.semantic_layer_version != current:
            why = ("never validated" if last is None
                   else "semantic layer changed since validation")
            raise StaleCandidateError(
                f"Candidate {candidate.candidate_id} is stale ({why}) — run "
                f"'skifer candidates validate --candidate {candidate.candidate_id}'."
            )
        return current

    def _transition(
        self, candidate: MetricCandidate, new_state: str, identity: str, reason: str, *,
        evidence: dict | None = None, version: str | None = None, **changes: Any,
    ) -> MetricCandidate:
        now = self._now()
        self.store.append_event(CertificationEvent(
            event_id=uuid.uuid4().hex, metric_id=candidate.candidate_id,
            metric_name=candidate.metric_name, event_type=new_state,
            previous_state=candidate.state, new_state=new_state, decided_by=identity,
            justification=reason, evidence=evidence or {},
            replaced_by_metric=changes.pop("replaced_by_metric", None), replaces_metric=None,
            semantic_layer_version=version, trace_id=None, event_at=now,
        ))
        updated = replace(candidate, state=new_state, reviewer=identity, updated_at=now, **changes)
        self.store.upsert_candidate(updated)
        return updated

    def _view(self, candidate: MetricCandidate) -> CandidateView:
        last = self._last_report_event(candidate.candidate_id)
        report = _report_of(last) if last is not None else None
        return CandidateView(
            candidate_id=candidate.candidate_id,
            metric_name=candidate.metric_name,
            state=candidate.state,
            question=candidate.origin_question,
            tables_used=tuple(candidate.tables_used),
            request_count=candidate.request_count,
            distinct_requesters=len(candidate.distinct_requesters),
            stability_score=candidate.stability_score,
            criteria=tuple(report["results"]) if report else (),
            published_view=candidate.published_view,
            published_metric_view=candidate.published_metric_view,
            updated_at=candidate.updated_at.astimezone(timezone.utc).isoformat(),
        )
