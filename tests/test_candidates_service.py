"""Human arbitration through ``CandidateService`` (Plan 44, S06)."""

from __future__ import annotations

from datetime import timedelta
import uuid

import pytest

from skifer.candidates import CertificationEvent, SqliteCandidateStore
from skifer.candidates.service import (
    CandidateService,
    DeciderIdentityError,
    StaleCandidateError,
    StateConflictError,
    UnknownCandidateError,
)
from skifer.candidates.validator import semantic_layer_version
from skifer.observability.tracing import TRACING_HMAC_SECRET_ENV
from tests.test_candidates_validator import NOW, make_candidate


class _Engine:
    def __init__(self, models_dir):
        self.models_dir = models_dir


REPORT = {"candidate_id": "x", "outcome": "needs_review", "semantic_layer_version": None,
          "results": [{"criterion": "coherence", "passed": False, "reasons": [
              {"code": "residue_present", "observed": 1, "threshold": 0, "verdict": "fail"}]}]}


@pytest.fixture
def store(tmp_path):
    candidate_store = SqliteCandidateStore(str(tmp_path / "candidates.db"))
    yield candidate_store
    candidate_store.close()


@pytest.fixture
def models_dir(tmp_path):
    (tmp_path / "semantic_catalog.yaml").write_text("models: []\n")
    return str(tmp_path)


def service(store, models_dir, **kwargs):
    kwargs.setdefault("decider_identity", "plain")
    return CandidateService(store, engine=_Engine(models_dir), clock=lambda: NOW, **kwargs)


def validated_event(candidate, previous, new_state, version, *, at=NOW):
    return CertificationEvent(
        event_id=uuid.uuid4().hex, metric_id=candidate.candidate_id,
        metric_name=candidate.metric_name, event_type=new_state, previous_state=previous,
        new_state=new_state, decided_by="agent:validator_v1", evidence=REPORT,
        semantic_layer_version=version, event_at=at,
    )


def seeded(store, models_dir, state="needs_review", *, version=None, **overrides):
    """A candidate in ``state`` whose last validation is recorded against ``version``."""
    candidate = make_candidate(state=state, **overrides)
    store.upsert_candidate(candidate)
    version = version or semantic_layer_version(models_dir)
    previous = {"needs_review": "candidate", "validated": "candidate", "promoted": "validated",
                "under_review": "promoted"}.get(state)
    if previous is not None:
        store.append_event(validated_event(candidate, previous, state, version))
    return candidate


# --- reading ---------------------------------------------------------------------------


def test_list_and_show_render_the_allowlisted_view(store, models_dir):
    candidate = seeded(store, models_dir, origin_question="Revenue in ? per country")
    seeded(store, models_dir, state="candidate", **_distinct("other"))
    views = service(store, models_dir).list(state="needs_review")
    assert [view.candidate_id for view in views] == [candidate.candidate_id]
    view = service(store, models_dir).show(candidate.candidate_id)
    assert view.to_dict() == {
        "candidate_id": candidate.candidate_id, "metric_name": "revenue_by_country",
        "state": "needs_review", "question": "Revenue in ? per country",
        "tables_used": ["main.sales.orders"], "request_count": 6, "distinct_requesters": 2,
        "stability_score": None, "criteria": REPORT["results"], "published_view": None,
        "published_metric_view": None, "updated_at": NOW.isoformat(),
    }
    assert "schema_dict" not in view.to_dict() and "distinct_requesters" in view.to_dict()


def test_unknown_candidate_and_state_are_named_errors(store, models_dir):
    with pytest.raises(UnknownCandidateError, match="candidates list"):
        service(store, models_dir).show("candidate:v1:" + "0" * 64)
    with pytest.raises(Exception, match="Unknown state"):
        service(store, models_dir).list(state="certified")


def test_history_is_chronological_even_on_equal_timestamps(store, models_dir):
    candidate = make_candidate(state="promoted")
    store.upsert_candidate(candidate)
    version = semantic_layer_version(models_dir)
    # Same timestamp, stored in an order the random event ids decide.
    for previous, new_state in [("validated", "promoted"), ("candidate", "validated")]:
        store.append_event(validated_event(candidate, previous, new_state, version))
    store.append_event(validated_event(candidate, "promoted", "under_review", version,
                                       at=NOW + timedelta(hours=1)))
    history = service(store, models_dir).history(candidate.candidate_id)
    assert [(e["previous_state"], e["new_state"]) for e in history] == [
        ("candidate", "validated"), ("validated", "promoted"), ("promoted", "under_review")]


# --- decisions -------------------------------------------------------------------------


def test_reject_and_review_write_one_event_each_with_the_reviewer(store, models_dir):
    rejected = seeded(store, models_dir)
    view = service(store, models_dir).reject(rejected.candidate_id, decider="alice",
                                             reason="duplicate of total_revenue")
    assert view.state == "rejected"
    stored = store.get_candidate(rejected.candidate_id)
    assert stored.reviewer == "human:alice"
    last = service(store, models_dir).history(rejected.candidate_id)[-1]
    assert (last["previous_state"], last["new_state"], last["decided_by"],
            last["justification"]) == (
        "needs_review", "rejected", "human:alice", "duplicate of total_revenue")

    promoted = seeded(store, models_dir, state="promoted", metric_name="promoted_one",
                      **_distinct("promoted_one"))
    view = service(store, models_dir).review(promoted.candidate_id, decider="bob",
                                             reason="usage dropped", signal="usage_drop")
    assert view.state == "under_review"
    last = service(store, models_dir).history(promoted.candidate_id)[-1]
    assert last["evidence"] == {"signal": "usage_drop"}
    assert last["semantic_layer_version"] == semantic_layer_version(models_dir)


def _distinct(target):
    from skifer.candidates import candidate_id_for, fingerprint_candidate
    from tests.test_candidates_validator import country_schema

    schema = country_schema(aggregate={"group_by": ["country"],
                                       "measures": [["amount", target, "sum"]]})
    fingerprint = fingerprint_candidate(schema)
    return {"schema_dict": schema, "fingerprint": fingerprint,
            "candidate_id": candidate_id_for(fingerprint)}


def test_forbidden_transition_is_a_state_conflict(store, models_dir):
    candidate = seeded(store, models_dir, state="candidate")
    with pytest.raises(StateConflictError, match="is candidate; it cannot become under_review"):
        service(store, models_dir).review(candidate.candidate_id, decider="a", reason="r")
    assert store.list_events(metric_id=candidate.candidate_id) == []


# --- S06-R8 ----------------------------------------------------------------------------


def test_stale_candidate_refused_except_reject(store, models_dir, tmp_path):
    candidate = seeded(store, models_dir, state="promoted")
    (tmp_path / "semantic_catalog.yaml").write_text("models: [orders]\n")
    with pytest.raises(StaleCandidateError, match="skifer candidates validate --candidate"):
        service(store, models_dir).review(candidate.candidate_id, decider="a", reason="r")
    assert len(store.list_events(metric_id=candidate.candidate_id)) == 1

    other = seeded(store, models_dir, version="sha256:v1:" + "f" * 64, **_distinct("other"))
    assert service(store, models_dir).reject(other.candidate_id, decider="a",
                                             reason="stale but refused").state == "rejected"


# --- S06-R10 ---------------------------------------------------------------------------


def test_hmac_without_secret_refuses_and_writes_nothing(store, models_dir, monkeypatch):
    monkeypatch.delenv(TRACING_HMAC_SECRET_ENV, raising=False)
    candidate = seeded(store, models_dir)
    hmac_service = service(store, models_dir, decider_identity="hmac")
    with pytest.raises(DeciderIdentityError, match=TRACING_HMAC_SECRET_ENV) as raised:
        hmac_service.reject(candidate.candidate_id, decider="alice", reason="no")
    assert "candidates_decider_identity: plain" in str(raised.value)
    assert len(store.list_events(metric_id=candidate.candidate_id)) == 1
    assert store.get_candidate(candidate.candidate_id) == candidate

    monkeypatch.setenv(TRACING_HMAC_SECRET_ENV, "s3cret")
    hmac_service.reject(candidate.candidate_id, decider="alice", reason="no")
    decided_by = hmac_service.history(candidate.candidate_id)[-1]["decided_by"]
    assert decided_by.startswith("human:") and "alice" not in decided_by


def test_decider_identity_is_validated():
    with pytest.raises(ValueError, match="decider_identity"):
        CandidateService(None, decider_identity="email")
