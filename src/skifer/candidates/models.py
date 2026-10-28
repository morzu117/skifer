"""Metric candidates, their runs, certification history and reference questions.

The registry stores the *structure* of a question, never its values (Plan 44, D2):
filter values are parameterized before a candidate is built, and no field of these
dataclasses is meant to receive one. Every dataclass validates itself at
construction and serializes through an explicit field-by-field allowlist.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import math
import re
from typing import Any

from .redaction import fingerprint_normal_form


CANDIDATE_STATES = (
    "candidate", "validated", "needs_review", "promoted",
    "under_review", "demoted", "rejected",
)
ALLOWED_TRANSITIONS: dict[str, frozenset[str]] = {
    "candidate":    frozenset({"validated", "needs_review", "rejected"}),
    "validated":    frozenset({"promoted", "needs_review", "rejected"}),
    "needs_review": frozenset({"promoted", "rejected"}),
    "promoted":     frozenset({"under_review", "demoted"}),
    "under_review": frozenset({"promoted", "demoted"}),
    "demoted":      frozenset({"candidate"}),
    "rejected":     frozenset(),
}
EVENT_TYPES = (
    "created", "validated", "needs_review", "promoted", "under_review", "demoted",
    "rejected", "replaced", "reactivated", "certification_requested", "certified",
)
RUN_PURPOSES = ("answer", "stability", "evaluation", "promotion")
REFERENCE_FAMILIES = ("A", "B", "C")
REFERENCE_SOURCES = ("yaml_bootstrap", "promoted", "rejected", "human")
DECIDER_PREFIXES = ("human:", "agent:", "system:")

CANDIDATE_ID_PREFIX = "candidate:v1:"
REFERENCE_SET_PREFIX = "reference_set:"
MODEL_METRIC_PREFIX = "model:"
HASH_PREFIX = "sha256:v1:"

_METRIC_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
# An error is recorded by class name only: a backend message very often quotes
# the offending value, which is exactly what this registry must never hold.
_ERROR_TYPE = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]*$")
_EVENT_ID = re.compile(r"^[0-9a-f]{32}$")


def check_transition(previous: str | None, new: str) -> None:
    """Refuse any state change absent from ``ALLOWED_TRANSITIONS``.

    ``None -> "candidate"`` is the only creation: a candidate never appears
    directly validated, promoted or rejected.
    """
    if new not in CANDIDATE_STATES:
        raise ValueError(f"Unknown candidate state {new!r}.")
    if previous is None:
        if new != "candidate":
            raise ValueError(f"Transition None → {new!r} is not allowed.")
        return
    if previous not in CANDIDATE_STATES:
        raise ValueError(f"Unknown candidate state {previous!r}.")
    if new not in ALLOWED_TRANSITIONS[previous]:
        raise ValueError(f"Transition {previous!r} → {new!r} is not allowed.")


# --- validation helpers -----------------------------------------------------------


def _fail(model: str, field_name: str, message: str) -> ValueError:
    return ValueError(f"{model}.{field_name} {message}")


def _non_empty(model: str, field_name: str, value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise _fail(model, field_name, f"must be a non-empty string; got {value!r}.")
    return value


def _optional_str(model: str, field_name: str, value: Any) -> str | None:
    if value is None:
        return None
    return _non_empty(model, field_name, value)


def _prefixed(model: str, field_name: str, value: Any, prefixes: tuple[str, ...]) -> str:
    _non_empty(model, field_name, value)
    if not value.startswith(prefixes) or value in prefixes:
        raise _fail(
            model, field_name, f"must start with one of {list(prefixes)}; got {value!r}."
        )
    return value


def _aware(model: str, field_name: str, value: Any) -> datetime:
    if not isinstance(value, datetime):
        raise _fail(model, field_name, f"must be a datetime; got {type(value).__name__}.")
    if value.tzinfo is None or value.utcoffset() is None:
        raise _fail(model, field_name, "must be timezone-aware.")
    return value


def _optional_aware(model: str, field_name: str, value: Any) -> datetime | None:
    return None if value is None else _aware(model, field_name, value)


def _string_tuple(model: str, field_name: str, value: Any) -> tuple[str, ...]:
    if not isinstance(value, tuple):
        raise _fail(model, field_name, "must be a tuple of strings.")
    for index, item in enumerate(value):
        _non_empty(model, f"{field_name}[{index}]", item)
    return value


def _optional_non_negative_int(model: str, field_name: str, value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise _fail(model, field_name, f"must be a non-negative integer or None; got {value!r}.")
    return value


def _json_value(model: str, field_name: str, value: Any) -> Any:
    """Refuse anything that would not round-trip through canonical JSON."""
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise _fail(model, field_name, "must contain only finite numbers.")
        return value
    if isinstance(value, (list, tuple)):
        return [
            _json_value(model, f"{field_name}[{index}]", item)
            for index, item in enumerate(value)
        ]
    if isinstance(value, dict):
        copied: dict[str, Any] = {}
        for key in sorted(value, key=str):
            if not isinstance(key, str):
                raise _fail(model, field_name, "must have string keys.")
            copied[key] = _json_value(model, f"{field_name}.{key}", value[key])
        return copied
    raise _fail(
        model, field_name, f"contains a non-JSON value of type {type(value).__name__}."
    )


def _json_dict(model: str, field_name: str, value: Any) -> dict:
    if not isinstance(value, dict):
        raise _fail(model, field_name, f"must be a dict; got {type(value).__name__}.")
    return _json_value(model, field_name, value)


def _iso(value: datetime | None) -> str | None:
    return None if value is None else value.astimezone(timezone.utc).isoformat()


def canonical_json(value: Any) -> str:
    """Canonical JSON: sorted keys, compact separators, UTF-8 preserved."""
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def fingerprint_candidate(schema_dict: dict) -> str:
    """``sha256:v1:`` fingerprint of a candidate's structure.

    Order that carries no meaning (tables by alias, a table's filters, groups,
    ``group_by``, measures by target) is sorted, every filter value becomes
    ``"?"`` and presentation keys (``dev_limit``, ``description``) are dropped:
    two questions differing only by a filter value are the same metric.
    """
    form = fingerprint_normal_form(schema_dict, canonical_json)
    digest = hashlib.sha256(canonical_json(form).encode("utf-8")).hexdigest()
    return f"{HASH_PREFIX}{digest}"


def candidate_id_for(fingerprint: str) -> str:
    """Content-derived candidate identifier: ``candidate:v1:<sha256(fingerprint)>``."""
    if not isinstance(fingerprint, str) or not fingerprint.startswith(HASH_PREFIX):
        raise ValueError(f"fingerprint must start with {HASH_PREFIX!r}; got {fingerprint!r}.")
    digest = hashlib.sha256(fingerprint.encode("utf-8")).hexdigest()
    return f"{CANDIDATE_ID_PREFIX}{digest}"


# --- row decoding helpers -----------------------------------------------------------


def _row_mapping(row: Any, model: str) -> dict:
    if isinstance(row, dict):
        return row
    if hasattr(row, "asDict"):
        return row.asDict(recursive=True)
    raise TypeError(f"Stored {model} row must be a mapping.")


def _decoded(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


def _row_tuple(value: Any) -> tuple:
    decoded = _decoded(value)
    if decoded is None:
        return ()
    return tuple(decoded)


def _row_datetime(value: Any) -> datetime | None:
    if value is None or isinstance(value, datetime):
        return value
    return datetime.fromisoformat(value)


def _row_optional_json(value: Any) -> Any:
    return None if value is None else _decoded(value)


# --- dataclasses ----------------------------------------------------------------------


@dataclass(frozen=True, kw_only=True)
class MetricCandidate:
    """One candidate metric: the structure of a question, never its values."""

    candidate_id: str
    metric_name: str
    metric_description: str | None
    origin_question: str | None
    origin_question_hash: str
    schema_dict: dict
    sql_hash: str
    fingerprint: str
    tables_used: tuple[str, ...]
    columns_used: tuple[str, ...]
    joins_used: tuple[tuple[str, str, str], ...]
    business_rules_refs: tuple[str, ...]
    residues: tuple[dict, ...]
    state: str
    # Defaulted here, not by a DDL DEFAULT: that would need the Delta table
    # feature allowColumnDefaults.
    request_count: int = 1
    distinct_requesters: tuple[str, ...] = ()
    stability_score: float | None = None
    coherence: dict | None = None
    trace_id: str | None = None
    reviewer: str | None = None
    published_view: str | None = None
    published_metric_view: str | None = None
    created_at: datetime
    updated_at: datetime
    last_requested_at: datetime | None = None

    def __post_init__(self) -> None:
        m = "MetricCandidate"
        _prefixed(m, "candidate_id", self.candidate_id, (CANDIDATE_ID_PREFIX,))
        if not isinstance(self.metric_name, str) or not _METRIC_NAME.match(self.metric_name):
            raise _fail(
                m, "metric_name",
                f"must match ^[A-Za-z_][A-Za-z0-9_]*$; got {self.metric_name!r}.",
            )
        _optional_str(m, "metric_description", self.metric_description)
        _optional_str(m, "origin_question", self.origin_question)
        _prefixed(m, "origin_question_hash", self.origin_question_hash, (HASH_PREFIX,))
        _json_dict(m, "schema_dict", self.schema_dict)
        _prefixed(m, "sql_hash", self.sql_hash, (HASH_PREFIX,))
        _prefixed(m, "fingerprint", self.fingerprint, (HASH_PREFIX,))
        if self.fingerprint != fingerprint_candidate(self.schema_dict):
            raise _fail(m, "fingerprint", "does not match the structure of schema_dict.")
        if self.candidate_id != candidate_id_for(self.fingerprint):
            raise _fail(m, "candidate_id", "is not derived from the fingerprint.")
        _string_tuple(m, "tables_used", self.tables_used)
        _string_tuple(m, "columns_used", self.columns_used)
        if not isinstance(self.joins_used, tuple):
            raise _fail(m, "joins_used", "must be a tuple of (left, right, key) tuples.")
        for index, join in enumerate(self.joins_used):
            if not isinstance(join, tuple) or len(join) != 3:
                raise _fail(m, f"joins_used[{index}]", "must be a (left, right, key) tuple.")
            _string_tuple(m, f"joins_used[{index}]", join)
        _string_tuple(m, "business_rules_refs", self.business_rules_refs)
        if not isinstance(self.residues, tuple):
            raise _fail(m, "residues", "must be a tuple of dicts.")
        for index, residue in enumerate(self.residues):
            _json_dict(m, f"residues[{index}]", residue)
            if set(residue) != {"code", "message", "blocking"}:
                raise _fail(
                    m, f"residues[{index}]",
                    f"must have exactly the keys code, message, blocking; got {sorted(residue)}.",
                )
            if not isinstance(residue["blocking"], bool):
                raise _fail(m, f"residues[{index}].blocking", "must be a boolean.")
        if self.state not in CANDIDATE_STATES:
            raise _fail(m, "state", f"must be one of {list(CANDIDATE_STATES)}; got {self.state!r}.")
        if (
            isinstance(self.request_count, bool)
            or not isinstance(self.request_count, int)
            or self.request_count < 1
        ):
            raise _fail(m, "request_count", f"must be an integer >= 1; got {self.request_count!r}.")
        _string_tuple(m, "distinct_requesters", self.distinct_requesters)
        if self.stability_score is not None and (
            isinstance(self.stability_score, bool)
            or not isinstance(self.stability_score, (int, float))
            or not math.isfinite(self.stability_score)
            or not 0 <= self.stability_score <= 1
        ):
            raise _fail(
                m, "stability_score", f"must be within [0, 1] or None; got {self.stability_score!r}."
            )
        if self.coherence is not None:
            _json_dict(m, "coherence", self.coherence)
        _optional_str(m, "trace_id", self.trace_id)
        if self.reviewer is not None:
            _prefixed(m, "reviewer", self.reviewer, ("human:",))
        _optional_str(m, "published_view", self.published_view)
        _optional_str(m, "published_metric_view", self.published_metric_view)
        _aware(m, "created_at", self.created_at)
        _aware(m, "updated_at", self.updated_at)
        _optional_aware(m, "last_requested_at", self.last_requested_at)

    @property
    def auto_promotable(self) -> bool:
        """An empty residue list is the only structural pass to automatic promotion."""
        return not self.residues

    def to_dict(self) -> dict[str, Any]:
        """Serialize field by field; a future field stays private until listed here."""
        m = "MetricCandidate"
        return {
            "candidate_id": self.candidate_id,
            "metric_name": self.metric_name,
            "metric_description": self.metric_description,
            "origin_question": self.origin_question,
            "origin_question_hash": self.origin_question_hash,
            "schema_dict": _json_value(m, "schema_dict", self.schema_dict),
            "sql_hash": self.sql_hash,
            "fingerprint": self.fingerprint,
            "tables_used": list(self.tables_used),
            "columns_used": list(self.columns_used),
            "joins_used": [list(join) for join in self.joins_used],
            "business_rules_refs": list(self.business_rules_refs),
            "residues": [_json_value(m, "residues", residue) for residue in self.residues],
            "state": self.state,
            "request_count": self.request_count,
            "distinct_requesters": list(self.distinct_requesters),
            "stability_score": self.stability_score,
            "coherence": None if self.coherence is None else _json_value(
                m, "coherence", self.coherence
            ),
            "trace_id": self.trace_id,
            "reviewer": self.reviewer,
            "published_view": self.published_view,
            "published_metric_view": self.published_metric_view,
            "created_at": _iso(self.created_at),
            "updated_at": _iso(self.updated_at),
            "last_requested_at": _iso(self.last_requested_at),
        }


def candidate_from_row(row: Any) -> MetricCandidate:
    """Rebuild a candidate from ``to_dict()`` output or a stored row."""
    data = _row_mapping(row, "candidate")
    try:
        request_count = data["request_count"]
        return MetricCandidate(
            candidate_id=data["candidate_id"],
            metric_name=data["metric_name"],
            metric_description=data["metric_description"],
            origin_question=data["origin_question"],
            origin_question_hash=data["origin_question_hash"],
            schema_dict=_decoded(data["schema_dict"]),
            sql_hash=data["sql_hash"],
            fingerprint=data["fingerprint"],
            tables_used=_row_tuple(data["tables_used"]),
            columns_used=_row_tuple(data["columns_used"]),
            joins_used=tuple(tuple(join) for join in _row_tuple(data["joins_used"])),
            business_rules_refs=_row_tuple(data["business_rules_refs"]),
            residues=_row_tuple(data["residues"]),
            state=data["state"],
            request_count=1 if request_count is None else int(request_count),
            distinct_requesters=_row_tuple(data["distinct_requesters"]),
            stability_score=data["stability_score"],
            coherence=_row_optional_json(data["coherence"]),
            trace_id=data["trace_id"],
            reviewer=data["reviewer"],
            published_view=data["published_view"],
            published_metric_view=data["published_metric_view"],
            created_at=_row_datetime(data["created_at"]),
            updated_at=_row_datetime(data["updated_at"]),
            last_requested_at=_row_datetime(data["last_requested_at"]),
        )
    except KeyError as exc:
        raise ValueError(f"Stored candidate is missing field {exc.args[0]!r}.") from exc


@dataclass(frozen=True, kw_only=True)
class CandidateRun:
    """One execution of a candidate, append-only."""

    run_id: str
    candidate_id: str
    executed_at: datetime
    purpose: str
    result_hash: str | None = None
    row_count: int | None = None
    duration_ms: int | None = None
    trace_id: str | None = None
    error_type: str | None = None

    def __post_init__(self) -> None:
        m = "CandidateRun"
        _non_empty(m, "run_id", self.run_id)
        _prefixed(m, "candidate_id", self.candidate_id, (CANDIDATE_ID_PREFIX, REFERENCE_SET_PREFIX))
        _aware(m, "executed_at", self.executed_at)
        if self.purpose not in RUN_PURPOSES:
            raise _fail(m, "purpose", f"must be one of {list(RUN_PURPOSES)}; got {self.purpose!r}.")
        _optional_str(m, "result_hash", self.result_hash)
        _optional_non_negative_int(m, "row_count", self.row_count)
        _optional_non_negative_int(m, "duration_ms", self.duration_ms)
        _optional_str(m, "trace_id", self.trace_id)
        if self.error_type is not None and (
            not isinstance(self.error_type, str) or not _ERROR_TYPE.match(self.error_type)
        ):
            raise _fail(m, "error_type", "must be an exception class name, never a message.")

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "candidate_id": self.candidate_id,
            "executed_at": _iso(self.executed_at),
            "purpose": self.purpose,
            "result_hash": self.result_hash,
            "row_count": self.row_count,
            "duration_ms": self.duration_ms,
            "trace_id": self.trace_id,
            "error_type": self.error_type,
        }


def run_from_row(row: Any) -> CandidateRun:
    data = _row_mapping(row, "run")
    try:
        return CandidateRun(
            run_id=data["run_id"],
            candidate_id=data["candidate_id"],
            executed_at=_row_datetime(data["executed_at"]),
            purpose=data["purpose"],
            result_hash=data["result_hash"],
            row_count=None if data["row_count"] is None else int(data["row_count"]),
            duration_ms=None if data["duration_ms"] is None else int(data["duration_ms"]),
            trace_id=data["trace_id"],
            error_type=data["error_type"],
        )
    except KeyError as exc:
        raise ValueError(f"Stored run is missing field {exc.args[0]!r}.") from exc


@dataclass(frozen=True, kw_only=True)
class CertificationEvent:
    """One append-only change in a metric's certification life."""

    event_id: str
    metric_id: str
    metric_name: str | None
    event_type: str
    previous_state: str | None
    new_state: str
    decided_by: str
    justification: str | None = None
    evidence: dict
    replaced_by_metric: str | None = None
    replaces_metric: str | None = None
    semantic_layer_version: str | None = None
    trace_id: str | None = None
    event_at: datetime

    def __post_init__(self) -> None:
        m = "CertificationEvent"
        if not isinstance(self.event_id, str) or not _EVENT_ID.match(self.event_id):
            raise _fail(m, "event_id", f"must be a uuid4 hex string; got {self.event_id!r}.")
        _prefixed(m, "metric_id", self.metric_id, (CANDIDATE_ID_PREFIX, MODEL_METRIC_PREFIX))
        _optional_str(m, "metric_name", self.metric_name)
        if self.event_type not in EVENT_TYPES:
            raise _fail(
                m, "event_type", f"must be one of {list(EVENT_TYPES)}; got {self.event_type!r}."
            )
        if self.previous_state is not None and self.previous_state not in CANDIDATE_STATES:
            raise _fail(m, "previous_state", f"is not a known state; got {self.previous_state!r}.")
        if self.new_state not in CANDIDATE_STATES:
            raise _fail(m, "new_state", f"is not a known state; got {self.new_state!r}.")
        # An event that records a state change records an allowed one; an event
        # that leaves the state unchanged (replaced, certified…) is a fact, not a move.
        if self.previous_state != self.new_state:
            check_transition(self.previous_state, self.new_state)
        _prefixed(m, "decided_by", self.decided_by, DECIDER_PREFIXES)
        _optional_str(m, "justification", self.justification)
        _json_dict(m, "evidence", self.evidence)
        _optional_str(m, "replaced_by_metric", self.replaced_by_metric)
        _optional_str(m, "replaces_metric", self.replaces_metric)
        if self.semantic_layer_version is not None:
            _prefixed(m, "semantic_layer_version", self.semantic_layer_version, (HASH_PREFIX,))
        _optional_str(m, "trace_id", self.trace_id)
        _aware(m, "event_at", self.event_at)

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "metric_id": self.metric_id,
            "metric_name": self.metric_name,
            "event_type": self.event_type,
            "previous_state": self.previous_state,
            "new_state": self.new_state,
            "decided_by": self.decided_by,
            "justification": self.justification,
            "evidence": _json_value("CertificationEvent", "evidence", self.evidence),
            "replaced_by_metric": self.replaced_by_metric,
            "replaces_metric": self.replaces_metric,
            "semantic_layer_version": self.semantic_layer_version,
            "trace_id": self.trace_id,
            "event_at": _iso(self.event_at),
        }


def event_from_row(row: Any) -> CertificationEvent:
    data = _row_mapping(row, "certification event")
    try:
        return CertificationEvent(
            event_id=data["event_id"],
            metric_id=data["metric_id"],
            metric_name=data["metric_name"],
            event_type=data["event_type"],
            previous_state=data["previous_state"],
            new_state=data["new_state"],
            decided_by=data["decided_by"],
            justification=data["justification"],
            evidence=_decoded(data["evidence"]),
            replaced_by_metric=data["replaced_by_metric"],
            replaces_metric=data["replaces_metric"],
            semantic_layer_version=data["semantic_layer_version"],
            trace_id=data["trace_id"],
            event_at=_row_datetime(data["event_at"]),
        )
    except KeyError as exc:
        raise ValueError(f"Stored certification event is missing field {exc.args[0]!r}.") from exc


@dataclass(frozen=True, kw_only=True)
class ReferenceQuestion:
    """A question of the reference set; ``expected`` depends on the family (S09)."""

    question_id: str
    question: str
    family: str
    expected: dict
    source: str
    semantic_layer_version: str | None = None
    created_at: datetime

    def __post_init__(self) -> None:
        m = "ReferenceQuestion"
        _non_empty(m, "question_id", self.question_id)
        _non_empty(m, "question", self.question)
        if self.family not in REFERENCE_FAMILIES:
            raise _fail(
                m, "family", f"must be one of {list(REFERENCE_FAMILIES)}; got {self.family!r}."
            )
        _json_dict(m, "expected", self.expected)
        if self.source not in REFERENCE_SOURCES:
            raise _fail(
                m, "source", f"must be one of {list(REFERENCE_SOURCES)}; got {self.source!r}."
            )
        if self.semantic_layer_version is not None:
            _prefixed(m, "semantic_layer_version", self.semantic_layer_version, (HASH_PREFIX,))
        _aware(m, "created_at", self.created_at)

    def to_dict(self) -> dict[str, Any]:
        return {
            "question_id": self.question_id,
            "question": self.question,
            "family": self.family,
            "expected": _json_value("ReferenceQuestion", "expected", self.expected),
            "source": self.source,
            "semantic_layer_version": self.semantic_layer_version,
            "created_at": _iso(self.created_at),
        }


def reference_from_row(row: Any) -> ReferenceQuestion:
    data = _row_mapping(row, "reference question")
    try:
        return ReferenceQuestion(
            question_id=data["question_id"],
            question=data["question"],
            family=data["family"],
            expected=_decoded(data["expected"]),
            source=data["source"],
            semantic_layer_version=data["semantic_layer_version"],
            created_at=_row_datetime(data["created_at"]),
        )
    except KeyError as exc:
        raise ValueError(f"Stored reference question is missing field {exc.args[0]!r}.") from exc
