"""Reference question set: deterministic bootstrap, hooks and human import (Plan 44, S09).

Three families live in ``reference_questions`` (S01):

- **A** — the semantic layer answers; ``expected = {model_key, metric}``;
- **B** — outside the semantic layer, with a verified answer;
  ``expected = {schema_dict, result_hash}``;
- **C** — the right outcome is to refuse or ask; ``expected = {outcome: refuse|clarify}``.

The bootstrap is template-based and never calls an LLM: two runs on the same
semantic layer produce the same ``question_id`` values, which is what makes two
evaluations comparable.
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Any, Callable

from .models import HASH_PREFIX, REFERENCE_FAMILIES, ReferenceQuestion

TEMPLATES_FR = ("Quel est le {label} ?", "{label} par {dimension}")   # frozen, versioned
REFERENCE_SET_VERSION = "v1"
EXPECTED_KEYS = {"A": ("model_key", "metric"), "B": ("schema_dict", "result_hash"),
                 "C": ("outcome",)}
C_OUTCOMES = ("refuse", "clarify")

_log = logging.getLogger("skifer.candidates")


def question_id(question: str, expected: dict) -> str:
    """``sha256:v1:`` of the question and its expected answer, canonical JSON."""
    payload = json.dumps({"question": question, "expected": expected}, sort_keys=True,
                         ensure_ascii=False, separators=(",", ":"))
    return HASH_PREFIX + hashlib.sha256(payload.encode("utf-8")).hexdigest()


def validate_expected(family: str, expected: Any) -> dict:
    """The expected answer of a family, with exactly its keys; ``ValueError`` names them."""
    if family not in REFERENCE_FAMILIES:
        raise ValueError(f"family must be one of {list(REFERENCE_FAMILIES)}; got {family!r}.")
    keys = EXPECTED_KEYS[family]
    if not isinstance(expected, dict) or set(expected) != set(keys):
        found = sorted(expected) if isinstance(expected, dict) else type(expected).__name__
        raise ValueError(f"Family {family} expects exactly the keys {list(keys)}; got {found}.")
    if family == "A" and not all(isinstance(expected[k], str) and expected[k] for k in keys):
        raise ValueError("Family A expects 'model_key' and 'metric' as non-empty strings.")
    if family == "B":
        if not isinstance(expected["schema_dict"], dict):
            raise ValueError("Family B expects 'schema_dict' as a mapping.")
        if not str(expected["result_hash"]).startswith(HASH_PREFIX):
            raise ValueError(f"Family B expects 'result_hash' starting with {HASH_PREFIX!r}.")
    if family == "C" and expected["outcome"] not in C_OUTCOMES:
        raise ValueError(f"Family C expects 'outcome' in {list(C_OUTCOMES)}.")
    return dict(expected)


def _question(question: str, family: str, expected: dict, source: str, *,
              version: str | None, now: datetime) -> ReferenceQuestion:
    return ReferenceQuestion(
        question_id=question_id(question, expected), question=question, family=family,
        expected=expected, source=source, semantic_layer_version=version, created_at=now,
    )


def _label(member: dict) -> str:
    return str(member.get("label") or member["name"]).replace("_", " ")


def bootstrap_from_models(
    engine: Any, *, clock: Callable[[], datetime] | None = None
) -> list[ReferenceQuestion]:
    """Family A from the catalogue: one template per (label | synonym), sorted by id.

    ``engine`` is a ``SemanticEngine``: every model of its catalogue is read.
    The dimension is the model's first declared one; without one, only the
    first template applies.
    """
    from .validator import semantic_layer_version

    now = (clock or (lambda: datetime.now(timezone.utc)))()
    version = semantic_layer_version(getattr(engine, "models_dir", None))
    questions: dict[str, ReferenceQuestion] = {}
    for key in sorted(entry["key"] for entry in engine.list_models()):
        model = engine._get_model(key)
        dimensions = [d for d in model.get("dimensions") or [] if d.get("name")]
        dimension = _label(dimensions[0]) if dimensions else None
        for metric in model.get("metrics") or []:
            if not metric.get("name"):
                continue
            expected = {"model_key": key, "metric": metric["name"]}
            phrases = [_label(metric), *[str(s) for s in metric.get("synonyms") or []]]
            for phrase in dict.fromkeys(phrases):
                templates = TEMPLATES_FR if dimension else TEMPLATES_FR[:1]
                for template in templates:
                    text = template.format(label=phrase, dimension=dimension)
                    item = _question(text, "A", expected, "yaml_bootstrap",
                                     version=version, now=now)
                    questions[item.question_id] = item
    return [questions[qid] for qid in sorted(questions)]


def append_new(store: Any, questions: list[ReferenceQuestion]) -> int:
    """Append the questions whose id is not stored yet; returns how many were written."""
    existing = {question.question_id for question in store.list_references()}
    fresh = [question for question in questions if question.question_id not in existing]
    for question in fresh:
        store.append_reference(question)
    return len(fresh)


def add_reference(
    store: Any, *, question: str, family: str, expected: dict,
    clock: Callable[[], datetime] | None = None,
) -> ReferenceQuestion:
    """Human import (family B above all); idempotent on the question id."""
    if not isinstance(question, str) or not question.strip():
        raise ValueError("question must be a non-empty string.")
    expected = validate_expected(family, expected)
    now = (clock or (lambda: datetime.now(timezone.utc)))()
    item = _question(question.strip(), family, expected, "human", version=None, now=now)
    append_new(store, [item])
    return item


def on_certified(event: Any, store: Any) -> ReferenceQuestion | None:
    """A ``certified`` event becomes a family A question on the certified metric."""
    if getattr(event, "event_type", None) != "certified":
        return None
    model_key = (event.evidence or {}).get("model_key")
    if not model_key or not event.metric_name:
        return None
    candidate = store.get_candidate(event.metric_id)
    text = (candidate.origin_question if candidate is not None else None) or \
        TEMPLATES_FR[0].format(label=event.metric_name.replace("_", " "))
    item = _question(text, "A", {"model_key": model_key, "metric": event.metric_name},
                     "promoted", version=event.semantic_layer_version, now=event.event_at)
    append_new(store, [item])
    return item


def _incoherent(event: Any, store: Any) -> bool:
    """A failed coherence in the rejection itself or in the candidate's last validation."""
    for recorded in [event, *reversed(store.list_events(metric_id=event.metric_id))]:
        for result in (recorded.evidence or {}).get("results") or []:
            if isinstance(result, dict) and result.get("criterion") == "coherence":
                return result.get("passed") is False
        coherence = (recorded.evidence or {}).get("coherence")
        if isinstance(coherence, dict) and "passed" in coherence:
            return coherence["passed"] is False
    return False


def on_rejected(event: Any, store: Any) -> ReferenceQuestion | None:
    """A rejection for incoherence becomes a family C question (``refuse``).

    Without a stored question (``candidates_store_questions: redacted``) there is
    nothing to ask again, so nothing is added.
    """
    if getattr(event, "event_type", None) != "rejected" or not _incoherent(event, store):
        return None
    candidate = store.get_candidate(event.metric_id)
    if candidate is None or not candidate.origin_question:
        return None
    item = _question(candidate.origin_question, "C", {"outcome": "refuse"}, "rejected",
                     version=event.semantic_layer_version, now=event.event_at)
    append_new(store, [item])
    return item


def run_hook(hook: Callable[[Any, Any], Any], event: Any, store: Any) -> None:
    """Best-effort: a reference hook never fails the decision that triggered it."""
    try:
        hook(event, store)
    except Exception as exc:
        _log.warning("Reference hook %s failed (%s).", hook.__name__, type(exc).__name__)
