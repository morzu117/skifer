"""Metric candidates registry and certification history (Plan 44).

The registry stores the structure of an out-of-scope question — never its filter
values — so that a recurring, coherent and stable answer can climb towards the
certified semantic layer under human control.
"""

from .models import (
    ALLOWED_TRANSITIONS,
    CANDIDATE_STATES,
    EVENT_TYPES,
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
from .redaction import parameterize_filters, question_hash, requester_pseudonym

__all__ = [
    "ALLOWED_TRANSITIONS",
    "CANDIDATE_STATES",
    "EVENT_TYPES",
    "CandidateRun",
    "CertificationEvent",
    "MetricCandidate",
    "ReferenceQuestion",
    "candidate_from_row",
    "candidate_id_for",
    "check_transition",
    "event_from_row",
    "fingerprint_candidate",
    "parameterize_filters",
    "question_hash",
    "reference_from_row",
    "requester_pseudonym",
    "run_from_row",
]
