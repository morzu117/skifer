"""Plan 44 S01: candidate models, closed states and allowed transitions."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone

import pytest

from skifer.candidates.models import (
    ALLOWED_TRANSITIONS,
    CANDIDATE_STATES,
    CandidateRun,
    CertificationEvent,
    MetricCandidate,
    ReferenceQuestion,
    candidate_from_row,
    candidate_id_for,
    check_transition,
    event_from_row,
    fingerprint_candidate,
    reference_from_row,
    run_from_row,
)

NOW = datetime(2026, 10, 28, 12, tzinfo=timezone.utc)
SCHEMA = {"tables": [{"name": "gold.orders", "alias": "o"}]}
FINGERPRINT = fingerprint_candidate(SCHEMA)
CANDIDATE_ID = candidate_id_for(FINGERPRINT)
HASH = "sha256:v1:" + "b" * 64


def make_candidate(**overrides) -> MetricCandidate:
    values = dict(
        candidate_id=CANDIDATE_ID,
        metric_name="revenue_by_region",
        metric_description="Revenue per region",
        origin_question="What is the revenue per region?",
        origin_question_hash=HASH,
        schema_dict=SCHEMA,
        sql_hash=HASH,
        fingerprint=FINGERPRINT,
        tables_used=("gold.orders",),
        columns_used=("gold.orders.amount", "gold.orders.region"),
        joins_used=(("o", "c", "customer_id"),),
        business_rules_refs=(),
        residues=(),
        state="candidate",
        created_at=NOW,
        updated_at=NOW,
    )
    values.update(overrides)
    return MetricCandidate(**values)


def make_run(**overrides) -> CandidateRun:
    values = dict(run_id="run-1", candidate_id=CANDIDATE_ID, executed_at=NOW, purpose="answer")
    values.update(overrides)
    return CandidateRun(**values)


def make_event(**overrides) -> CertificationEvent:
    values = dict(
        event_id="0" * 32,
        metric_id=CANDIDATE_ID,
        metric_name="revenue_by_region",
        event_type="created",
        previous_state=None,
        new_state="candidate",
        decided_by="agent:candidate_agent_v1",
        evidence={},
        event_at=NOW,
    )
    values.update(overrides)
    return CertificationEvent(**values)


def make_reference(**overrides) -> ReferenceQuestion:
    values = dict(
        question_id="q-1",
        question="Revenue per region?",
        family="A",
        expected={"model_key": "sales.orders", "metric": "revenue"},
        source="yaml_bootstrap",
        created_at=NOW,
    )
    values.update(overrides)
    return ReferenceQuestion(**values)


_PAIRS = [(previous, new) for previous in CANDIDATE_STATES for new in CANDIDATE_STATES]


@pytest.mark.parametrize(("previous", "new"), _PAIRS)
def test_transition_matrix(previous, new):
    if new in ALLOWED_TRANSITIONS[previous]:
        check_transition(previous, new)
    else:
        with pytest.raises(ValueError, match="is not allowed"):
            check_transition(previous, new)


@pytest.mark.parametrize("new", CANDIDATE_STATES)
def test_creation_is_only_none_to_candidate(new):
    if new == "candidate":
        check_transition(None, new)
    else:
        with pytest.raises(ValueError, match="is not allowed"):
            check_transition(None, new)


def test_transition_error_message():
    with pytest.raises(ValueError, match="Transition 'promoted' → 'candidate' is not allowed."):
        check_transition("promoted", "candidate")


@pytest.mark.parametrize(
    ("factory", "overrides", "field_name"),
    [
        (make_candidate, {"created_at": datetime(2026, 10, 28)}, "created_at"),
        (make_candidate, {"updated_at": datetime(2026, 10, 28)}, "updated_at"),
        (make_candidate, {"last_requested_at": datetime(2026, 10, 28)}, "last_requested_at"),
        (make_candidate, {"state": "certified"}, "state"),
        (make_candidate, {"request_count": 0}, "request_count"),
        (make_candidate, {"stability_score": 1.5}, "stability_score"),
        (make_candidate, {"stability_score": -0.1}, "stability_score"),
        (make_candidate, {"metric_name": "1revenue"}, "metric_name"),
        (make_candidate, {"metric_name": "revenue; DROP"}, "metric_name"),
        (make_candidate, {"candidate_id": "sha256:v1:x"}, "candidate_id"),
        (make_candidate, {"residues": ({"code": "x"},)}, "residues"),
        (make_candidate, {"reviewer": "alice"}, "reviewer"),
        (make_run, {"executed_at": datetime(2026, 10, 28)}, "executed_at"),
        (make_run, {"purpose": "debug"}, "purpose"),
        (make_run, {"error_type": "ValueError: amount = 42"}, "error_type"),
        (make_run, {"candidate_id": "other:1"}, "candidate_id"),
        (make_event, {"event_at": datetime(2026, 10, 28)}, "event_at"),
        (make_event, {"event_type": "archived"}, "event_type"),
        (make_event, {"decided_by": "alice"}, "decided_by"),
        (make_event, {"event_id": "not-a-uuid"}, "event_id"),
        (make_reference, {"created_at": datetime(2026, 10, 28)}, "created_at"),
        (make_reference, {"family": "D"}, "family"),
        (make_reference, {"source": "llm"}, "source"),
    ],
)
def test_models_reject_invalid_fields(factory, overrides, field_name):
    with pytest.raises(ValueError, match=field_name):
        factory(**overrides)


def test_event_refuses_a_forbidden_state_change():
    with pytest.raises(ValueError, match="is not allowed"):
        make_event(event_type="promoted", previous_state="rejected", new_state="promoted")
    # an event that does not move the state is a fact, not a transition
    make_event(event_type="certified", previous_state="promoted", new_state="promoted")


def test_request_count_defaults_to_one():
    assert make_candidate().request_count == 1


def test_every_model_round_trips_through_its_dict():
    candidate = make_candidate(
        request_count=3,
        distinct_requesters=("p1", "p2"),
        stability_score=0.5,
        coherence={"verdict": True, "reasons": ["ok"]},
        residues=({"code": "expr", "message": "expr: kept", "blocking": True},),
        reviewer="human:abc",
        last_requested_at=NOW,
    )
    assert candidate_from_row(candidate.to_dict()) == candidate
    run = make_run(result_hash=HASH, row_count=0, duration_ms=12, error_type="TimeoutError")
    assert run_from_row(run.to_dict()) == run
    event = make_event(evidence={"request_count": 3}, semantic_layer_version=HASH)
    assert event_from_row(event.to_dict()) == event
    reference = make_reference()
    assert reference_from_row(reference.to_dict()) == reference


def test_to_dict_is_an_allowlist():
    candidate = make_candidate()
    payload = candidate.to_dict()
    assert set(payload) == {
        "candidate_id", "metric_name", "metric_description", "origin_question",
        "origin_question_hash", "schema_dict", "sql_hash", "fingerprint", "tables_used",
        "columns_used", "joins_used", "business_rules_refs", "residues", "state",
        "request_count", "distinct_requesters", "stability_score", "coherence", "trace_id",
        "reviewer", "published_view", "published_metric_view", "created_at", "updated_at",
        "last_requested_at",
    }
    assert replace(candidate, state="validated").to_dict()["state"] == "validated"
