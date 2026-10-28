"""Human arbitration of metric candidates (Plan 44, S06).

``CandidateService`` is the single implementation of a human decision. The CLI
and the Databricks arbitration notebook are thin adapters over it; an API route
or an App would plug into the same methods.

Certification is not a registry state: a metric is certified once its model is
in ``semantic_catalog.yaml`` after ``skifer semantic sync --promote``. The
service stops at the managed drafts, and a human opens the pull request.
"""

from __future__ import annotations

import contextlib
from dataclasses import dataclass, replace
from datetime import datetime, timezone
import os
from pathlib import Path
import sys
import tempfile
from typing import Any, Callable
import uuid

import yaml

from .models import (
    ALLOWED_TRANSITIONS,
    CANDIDATE_STATES,
    CertificationEvent,
    MetricCandidate,
)

DECIDER_IDENTITIES = ("hmac", "plain")
# Coherence reasons a human may waive by approving; every other failure stands.
WAIVABLE_REASONS = frozenset({"residue_present", "undeclared_join"})
GENERATED_BY = "metric_candidates"
DRAFTS_SEGMENT = ".drafts"
DEFAULT_PIPELINE_DRAFTS = "schemas/.drafts/candidates"


class CandidateError(Exception):
    """A decision that cannot be applied; the message names the next action."""


class CandidateUsageError(CandidateError):
    """The request itself is wrong: an unknown id, a path outside ``.drafts``."""


class UnknownCandidateError(CandidateUsageError):
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

    def approve(self, candidate_id: str, *, decider: str, reason: str) -> CandidateView:
        """Publish at level 1 by human decision, after revalidating coherence.

        A human may waive ``residue_present`` and ``undeclared_join``; a missing
        table, an uncertified source, an unsafe fanout or an invalid schema can
        never be waived (``ApprovalRefusedError``).
        """
        from .promotion import level1_index_entries, promote_level1

        identity = self._decider(decider)
        candidate = self._get(candidate_id)
        self._check_state(candidate, "promoted")
        version = self._check_fresh(candidate)
        coherence = self._validator().coherence(candidate)
        failed = [r["code"] for r in coherence.reasons if r["verdict"] == "fail"]
        refused = sorted(set(failed) - WAIVABLE_REASONS)
        if refused:
            raise ApprovalRefusedError(
                f"Candidate {candidate.candidate_id} cannot be approved: "
                f"{', '.join(refused)} can never be waived — reject it, or fix the source "
                "and run 'skifer candidates validate' again."
            )
        catalog, schema, backend = self._level1_target()
        promote_level1(candidate, backend=backend, catalog=catalog, schema=schema)
        view, metric_view = self._published_names(candidate, catalog, schema)
        updated = self._transition(
            candidate, "promoted", identity, reason, version=version,
            evidence={"waived": sorted(set(failed)), "coherence": coherence.to_dict()},
            published_view=view, published_metric_view=metric_view,
        )
        if self.index_store is not None:
            node, edges = level1_index_entries(candidate)
            self.index_store.upsert_nodes([node])
            if edges:
                self.index_store.upsert_edges(edges)
        return self._view(updated)

    def promote(
        self, candidate_id: str, *, decider: str, reason: str, pipeline_output: str | None = None
    ) -> dict:
        """Level 2: write the pipeline and semantic drafts a human turns into a PR.

        The state does not change — certification is the model in the catalogue
        after ``semantic sync --promote``. Both files are created with ``O_EXCL``
        after the pipeline loaded and the model validated; if either exists,
        neither is written (``OutputExistsError``).
        """
        identity = self._decider(decider)
        candidate = self._get(candidate_id)
        if candidate.state not in ("promoted", "validated"):
            raise StateConflictError(
                f"Candidate {candidate.candidate_id} is {candidate.state}; only a validated or "
                "promoted candidate can be drafted for certification."
            )
        version = self._check_fresh(candidate)
        pipeline_path = Path(pipeline_output or
                             f"{DEFAULT_PIPELINE_DRAFTS}/{candidate.metric_name}.yaml")
        if DRAFTS_SEGMENT not in pipeline_path.parts:
            raise CandidateUsageError(
                f"--pipeline-output must be under a '{DRAFTS_SEGMENT}' directory; "
                f"got {str(pipeline_path)!r}."
            )
        pipeline, converted = self._pipeline_draft(candidate)
        pipeline_text = (
            f"# Generated by {GENERATED_BY} from {candidate.candidate_id} — review, then open a PR.\n"
            f"# filters_to_dimensions: {', '.join(converted) or '(none)'}\n"
            + yaml.safe_dump(pipeline, sort_keys=False, allow_unicode=True)
        )
        models_dir = Path(getattr(self.engine, "models_dir", None) or "semantic_models")
        semantic_path = models_dir / DRAFTS_SEGMENT / f"candidates.{candidate.metric_name}.yaml"
        for path in (pipeline_path, semantic_path):
            if path.exists():
                raise OutputExistsError(f"{path} already exists; nothing was written.")
        semantic_text = yaml.safe_dump(self._staged_semantic_draft(pipeline_text, models_dir),
                                       sort_keys=False, allow_unicode=True)
        _create_all([(pipeline_path, pipeline_text), (semantic_path, semantic_text)])
        result = {"pipeline_draft": str(pipeline_path), "semantic_draft": str(semantic_path),
                  "filters_to_dimensions": list(converted)}
        now = self._now()
        self.store.append_event(CertificationEvent(
            event_id=uuid.uuid4().hex, metric_id=candidate.candidate_id,
            metric_name=candidate.metric_name, event_type="certification_requested",
            previous_state=candidate.state, new_state=candidate.state, decided_by=identity,
            justification=reason, evidence=result, semantic_layer_version=version, event_at=now,
        ))
        self.store.upsert_candidate(replace(candidate, reviewer=identity, updated_at=now))
        return result

    def demote(
        self, candidate_id: str, *, decider: str, reason: str, replaced_by: str | None = None
    ) -> CandidateView:
        """``promoted``/``under_review`` → ``demoted``: drop the level 1 objects, record the replacement."""
        from .models import CANDIDATE_ID_PREFIX, MODEL_METRIC_PREFIX
        from .promotion import level1_index_entries

        identity = self._decider(decider)
        candidate = self._get(candidate_id)
        self._check_state(candidate, "demoted")
        version = self._check_fresh(candidate)
        replacement = None
        if replaced_by is not None:
            if replaced_by.startswith(CANDIDATE_ID_PREFIX):
                replacement = self._get(replaced_by)
            elif not replaced_by.startswith(MODEL_METRIC_PREFIX):
                raise CandidateUsageError(
                    f"--replaced-by must be a candidate id or '{MODEL_METRIC_PREFIX}<model>.<metric>'"
                    f"; got {replaced_by!r}."
                )
        for fqn in (candidate.published_metric_view, candidate.published_view):
            if fqn:
                quoted = ".".join(f"`{part}`" for part in fqn.split("."))
                self._backend().execute_sql(f"DROP VIEW IF EXISTS {quoted}")
        updated = self._transition(
            candidate, "demoted", identity, reason, version=version,
            replaced_by_metric=replaced_by, published_view=None, published_metric_view=None,
        )
        if replaced_by is not None:
            self.store.append_event(CertificationEvent(
                event_id=uuid.uuid4().hex, metric_id=replaced_by,
                metric_name=(replacement.metric_name if replacement is not None
                             else replaced_by[len(MODEL_METRIC_PREFIX):]),
                event_type="replaced",
                previous_state=replacement.state if replacement is not None else "promoted",
                new_state=replacement.state if replacement is not None else "promoted",
                decided_by=identity, justification=reason, evidence={},
                replaces_metric=candidate.candidate_id, semantic_layer_version=version,
                event_at=self._now(),
            ))
        if self.index_store is not None:
            # A demoted metric must not be offered to the agent again.
            node, edges = level1_index_entries(candidate)
            self.index_store.mark_removed([node.node_id])
            if edges:
                self.index_store.mark_edges_removed([edge.edge_id for edge in edges])
        return self._view(updated)

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
        self.store.append_event(event := CertificationEvent(
            event_id=uuid.uuid4().hex, metric_id=candidate.candidate_id,
            metric_name=candidate.metric_name, event_type=new_state,
            previous_state=candidate.state, new_state=new_state, decided_by=identity,
            justification=reason, evidence=evidence or {},
            replaced_by_metric=changes.pop("replaced_by_metric", None), replaces_metric=None,
            semantic_layer_version=version, trace_id=None, event_at=now,
        ))
        updated = replace(candidate, state=new_state, reviewer=identity, updated_at=now, **changes)
        self.store.upsert_candidate(updated)
        if new_state == "rejected":  # Plan 44, S09: an incoherent rejection feeds family C
            from .reference import on_rejected, run_hook

            run_hook(on_rejected, event, self.store)
        return updated

    def _validator(self) -> Any:
        from skifer.knowledge.index import KnowledgeIndex

        from .validator import CandidateValidator, ValidationThresholds

        if self.engine is None or self.index_store is None:
            raise CandidateError("Approving revalidates coherence: it needs an engine and an index.")
        return CandidateValidator(self.store, self.engine,
                                  KnowledgeIndex.from_store(self.index_store),
                                  thresholds=ValidationThresholds(), clock=self._now)

    def _backend(self) -> Any:
        if self.backend is not None:
            return self.backend
        if self.engine is None:
            raise CandidateError("Publishing or dropping a view needs a backend.")
        return self.engine.core._get_backend()

    def _level1_target(self) -> tuple[str | None, str, Any]:
        if self.engine is None:
            raise CandidateError("Publishing at level 1 needs an engine.")
        with contextlib.redirect_stdout(sys.stderr):
            schema = self.engine._resolve_metric_views_schema()
        return getattr(self.engine.core, "db", None), schema, self._backend()

    @staticmethod
    def _published_names(candidate: MetricCandidate, catalog: str | None, schema: str):
        from . import promotion

        view = promotion.level1_view_fqn(candidate, catalog, schema).replace("`", "")
        metric_view = None
        if catalog and promotion.LEVEL1_PUBLISHES_METRIC_VIEW:
            metric_view = promotion.level1_metric_view_fqn(candidate, catalog, schema).replace("`", "")
        return view, metric_view

    @staticmethod
    def _pipeline_draft(candidate: MetricCandidate) -> tuple[dict, tuple[str, ...]]:
        """The level 2 pipeline: ``metric_definition`` — the level 1 SQL — plus its governance seed."""
        from .promotion import metric_definition

        definition, converted = metric_definition(candidate.schema_dict)
        group_by = list(definition["aggregate"]["group_by"])
        pipeline = {"_generated_by": GENERATED_BY}
        pipeline["data_product"] = {"id": f"candidates.{candidate.metric_name}",
                                    "version": "0.1.0"}
        if candidate.metric_description:
            pipeline["data_product"]["description"] = candidate.metric_description
        measures = [m["target"] if isinstance(m, dict) else m[1]
                    for m in definition["aggregate"]["measures"]]
        pipeline["contract"] = {
            "grain": group_by,
            "output": {name: {"description": name} for name in [*group_by, *measures]},
        }
        pipeline["semantic"] = {"model_key": f"candidates.{candidate.metric_name}",
                                "dimensions": group_by,
                                "source_candidate_id": candidate.candidate_id}
        pipeline.update(definition)
        # The projection needs a deterministic target; a reviewer moves it in the PR.
        pipeline["sink"] = {"type": "delta", "schema": "candidates",
                            "table": candidate.metric_name}
        return pipeline, converted

    @staticmethod
    def _staged_semantic_draft(pipeline_text: str, models_dir: Path) -> dict:
        """Load the staged pipeline, project it, validate the model — before anything is visible."""
        from skifer.core.ir import parse_to_ir
        from skifer.semantic.draft_builder import SemanticDraftBuilder
        from skifer.semantic.output_projection import OutputProjector
        from skifer.semantic.validator import SemanticValidator

        with tempfile.TemporaryDirectory(prefix="skifer_candidate_") as staging:
            staged = Path(staging) / "pipeline.yaml"
            staged.write_text(pipeline_text, encoding="utf-8")
            parsed = load_schema(str(staged))
        schema = parse_to_ir(parsed)
        projected = OutputProjector().project(schema)
        payload = SemanticDraftBuilder(str(models_dir)).build_draft(projected, schema)
        result = SemanticValidator().validate_against_projection(
            payload, projected, contract_output=[f.name for f in schema.contract_output])
        if not result.ok:
            raise CandidateError("The semantic draft does not validate: " + "; ".join(result.errors))
        return payload

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


def build_candidate_service(
    semantic: Any, *, store: Any = None, index_store: Any = None,
    clock: Callable[[], datetime] | None = None,
) -> CandidateService:
    """The service the CLI and the arbitration notebook share, built from a ``SemanticEngine``.

    Stores default to where the engine runs; the decider identity mode is the
    environment's ``candidates_decider_identity`` (``hmac`` unless set).
    """
    from skifer.knowledge.store import default_knowledge_store

    from .store import default_candidate_store

    core = semantic.core
    env_config = core._context.env_config()
    return CandidateService(
        store if store is not None else default_candidate_store(core),
        engine=semantic,
        index_store=index_store if index_store is not None else default_knowledge_store(core),
        decider_identity=env_config.get("candidates_decider_identity", "hmac"),
        clock=clock,
    )


def load_schema(path: str) -> dict:
    """``skifer.load_schema``, named here so a test can act between check and write."""
    from skifer.core.schema_loader import load_schema as _load_schema

    return _load_schema(path)


def _create_all(files: list[tuple[Path, str]]) -> None:
    """Create every file with ``O_EXCL``, or none: a file that appears meanwhile is never overwritten."""
    created: list[Path] = []
    try:
        for path, _ in files:
            path.parent.mkdir(parents=True, exist_ok=True)
            try:
                descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
            except FileExistsError:
                raise OutputExistsError(f"{path} already exists; nothing was written.") from None
            os.close(descriptor)
            created.append(path)
        for path, text in files:
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(text)
                handle.flush()
                os.fsync(handle.fileno())
    except BaseException:
        for path in created:
            try:
                path.unlink()
            except OSError:
                pass
        raise
