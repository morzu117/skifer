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
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Callable

from .models import (
    HASH_PREFIX,
    REFERENCE_FAMILIES,
    REFERENCE_SET_PREFIX,
    CandidateRun,
    ReferenceQuestion,
)

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


# --- S09.2: evaluation -------------------------------------------------------------------

ANSWERED_MODES = ("query", "view", "candidate")
REFUSED_MODES = ("candidate_refused", "no_model", "needs_clarification", "ambiguous")
EVALUATIONS_DIR = ".skifer_evaluations"


@dataclass(frozen=True)
class EvaluationReport:
    reference_set_version: str
    semantic_layer_version: str | None
    question_count: dict[str, int]                  # per family
    metrics: dict[str, float | None]                # None = empty family
    failures: tuple[dict, ...]                      # never a question text nor a filter value

    def to_dict(self) -> dict:
        return {"reference_set_version": self.reference_set_version,
                "semantic_layer_version": self.semantic_layer_version,
                "question_count": dict(self.question_count), "metrics": dict(self.metrics),
                "failures": [dict(failure) for failure in self.failures]}


def _ratio(numerator: int, denominator: int) -> float | None:
    return None if denominator == 0 else round(numerator / denominator, 6)


def _outcome(response: Any) -> str:
    """``answered`` | ``refused`` | ``error`` — a technical error is neither of the first two."""
    if getattr(response, "access_denied", False):
        return "refused"
    mode = getattr(response, "mode", None)
    if mode in ANSWERED_MODES:
        return "answered"
    return "refused" if mode in REFUSED_MODES else "error"


def _ask(hub: Any, question: str) -> Any:
    try:
        return hub.ask(question)
    except Exception as exc:  # recorded by class name only, like every registry error
        return SimpleNamespace(mode="error", error_type=type(exc).__name__)


def _answer_hash(store: Any, response: Any) -> str | None:
    """The ``result_hash`` the fallback recorded for this answer (its last ``answer`` run)."""
    candidate_id = getattr(response, "candidate_id", None)
    if getattr(response, "mode", None) != "candidate" or not candidate_id:
        return None
    runs = [run for run in store.list_runs(candidate_id, purpose="answer") if run.result_hash]
    return runs[-1].result_hash if runs else None


def _layer_version(hub: Any) -> str | None:
    from .validator import semantic_layer_version

    engine = getattr(hub, "_semantic_engine", None)
    models_dir = getattr(engine, "models_dir", None)
    return semantic_layer_version(models_dir) if models_dir else None


def evaluate(
    hub: Any, index: Any, store: Any, *, k_values: tuple[int, ...] = (5, 10),
    search_mode: str = "bm25", semantic_layer_version: str | None = None,
) -> EvaluationReport:
    """Ask every reference question, sorted by id, and compute the report's metrics.

    - ``A.recall@k``: the expected metric is among the first ``k`` ``metric`` hits;
    - ``A.routing_accuracy``: GenBI answered with the expected ``model_used`` and metric;
    - ``B.exact_match``: the fallback's ``result_hash`` equals the expected one;
    - ``C.correct_refusal`` / ``C.false_answer``: refused / answered where refusing was right;
    - ``C.false_refusal``: refused where an answer was expected, over A and B.
    """
    if (not k_values or any(isinstance(k, bool) or not isinstance(k, int) or k < 1
                            for k in k_values)):
        raise ValueError(f"k_values must be positive integers; got {k_values!r}.")
    ks = tuple(sorted(set(k_values)))
    questions = sorted(store.list_references(), key=lambda q: q.question_id)
    families = {family: [q for q in questions if q.family == family]
                for family in REFERENCE_FAMILIES}
    failures: list[dict] = []
    recalled = {k: 0 for k in ks}
    routed = exact = correct_refusals = false_answers = false_refusals = 0

    for question in families["A"]:
        expected = {"model_key": question.expected["model_key"],
                    "metric": question.expected["metric"]}
        target = f"metric:{expected['model_key']}.{expected['metric']}"
        ids = [hit.node.node_id for hit in index.search(question.question, k=ks[-1],
                                                       kinds=["metric"], mode=search_mode)]
        rank = ids.index(target) + 1 if target in ids else None
        for k in ks:
            recalled[k] += rank is not None and rank <= k
        response = _ask(hub, question.question)
        metrics = list((getattr(response, "query_params", None) or {}).get("metrics") or [])
        ok = (getattr(response, "mode", None) == "query"
              and getattr(response, "model_used", None) == expected["model_key"]
              and expected["metric"] in metrics)
        routed += ok
        false_refusals += _outcome(response) == "refused"
        if not ok or rank is None:
            failures.append({"question_id": question.question_id, "family": "A",
                             "expected": expected,
                             "obtained": {"mode": getattr(response, "mode", None),
                                          "model_used": getattr(response, "model_used", None),
                                          "metrics": [str(m) for m in metrics], "rank": rank}})

    for question in families["B"]:
        response = _ask(hub, question.question)
        obtained_hash = _answer_hash(store, response)
        ok = obtained_hash is not None and obtained_hash == question.expected["result_hash"]
        exact += ok
        false_refusals += _outcome(response) == "refused"
        if not ok:
            # The expected schema_dict is left out: it holds the question's filter values.
            failures.append({"question_id": question.question_id, "family": "B",
                             "expected": {"result_hash": question.expected["result_hash"]},
                             "obtained": {"mode": getattr(response, "mode", None),
                                          "candidate_id": getattr(response, "candidate_id", None),
                                          "result_hash": obtained_hash}})

    for question in families["C"]:
        response = _ask(hub, question.question)
        outcome = _outcome(response)
        correct_refusals += outcome == "refused"
        false_answers += outcome == "answered"
        if outcome != "refused":
            failures.append({"question_id": question.question_id, "family": "C",
                             "expected": {"outcome": question.expected["outcome"]},
                             "obtained": {"mode": getattr(response, "mode", None)}})

    count_a, count_b, count_c = (len(families[f]) for f in REFERENCE_FAMILIES)
    metrics_out: dict[str, float | None] = {
        f"A.recall@{k}": _ratio(recalled[k], count_a) for k in ks}
    metrics_out.update({
        "A.routing_accuracy": _ratio(routed, count_a),
        "B.exact_match": _ratio(exact, count_b),
        "C.correct_refusal": _ratio(correct_refusals, count_c),
        "C.false_answer": _ratio(false_answers, count_c),
        "C.false_refusal": _ratio(false_refusals, count_a + count_b),
    })
    return EvaluationReport(
        reference_set_version=REFERENCE_SET_VERSION,
        semantic_layer_version=(semantic_layer_version if semantic_layer_version is not None
                                else _layer_version(hub)),
        question_count={family: len(families[family]) for family in REFERENCE_FAMILIES},
        metrics=metrics_out, failures=tuple(failures),
    )


def report_hash(report: dict) -> str:
    payload = json.dumps(report, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return HASH_PREFIX + hashlib.sha256(payload.encode("utf-8")).hexdigest()


def evaluation_run_id() -> str:
    return f"{REFERENCE_SET_PREFIX}{REFERENCE_SET_VERSION}"


def store_evaluation(
    report: EvaluationReport, store: Any, *, directory: str = EVALUATIONS_DIR,
    clock: Callable[[], datetime] | None = None,
) -> str:
    """Write ``<directory>/<timestamp>.json`` (never overwriting) and its ``evaluation`` run."""
    now = (clock or (lambda: datetime.now(timezone.utc)))()
    data = report.to_dict()
    os.makedirs(directory, exist_ok=True)
    path = os.path.join(directory, now.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
                        + ".json")
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        json.dump(data, handle, ensure_ascii=False, sort_keys=True, indent=2)
    store.append_run(CandidateRun(
        run_id=uuid.uuid4().hex, candidate_id=evaluation_run_id(), executed_at=now,
        purpose="evaluation", result_hash=report_hash(data),
        row_count=sum(report.question_count.values()),
    ))
    return path


def load_evaluations(store: Any, *, directory: str = EVALUATIONS_DIR) -> list[dict]:
    """Stored reports, oldest first, found through the ``evaluation`` runs' ``result_hash``.

    A run whose report file is gone (cleaned, another machine) is skipped.
    """
    by_hash: dict[str, dict] = {}
    if os.path.isdir(directory):
        for name in sorted(os.listdir(directory)):
            if not name.endswith(".json"):
                continue
            try:
                with open(os.path.join(directory, name), encoding="utf-8") as handle:
                    data = json.load(handle)
            except (OSError, ValueError):
                continue
            if isinstance(data, dict):
                by_hash[report_hash(data)] = data
    runs = store.list_runs(evaluation_run_id(), purpose="evaluation")
    return [by_hash[run.result_hash] for run in runs if run.result_hash in by_hash]
