"""Reference question set: bootstrap, hooks and CLI (Plan 44, S09.1)."""

from __future__ import annotations

import argparse
from dataclasses import replace
from datetime import timedelta
import json
from pathlib import Path
import shutil
import uuid

import pytest

from skifer import cli
from skifer.candidates import CertificationEvent, SqliteCandidateStore
from skifer.candidates.reference import (
    TEMPLATES_FR,
    add_reference,
    append_new,
    bootstrap_from_models,
    on_certified,
    on_rejected,
    question_id,
)
from skifer.semantic.semantic import SemanticEngine
from tests.test_candidates_service import seeded, service
from tests.test_candidates_validator import NOW

EXAMPLES = Path(__file__).resolve().parents[1] / "examples"
B_EXPECTED = {"schema_dict": {"tables": [{"name": "gold.fact_orders"}]},
              "result_hash": "sha256:v1:" + "a" * 64}


@pytest.fixture
def store(tmp_path):
    candidate_store = SqliteCandidateStore(str(tmp_path / "candidates.db"))
    yield candidate_store
    candidate_store.close()


@pytest.fixture
def semantic(tmp_path):
    target = tmp_path / "semantic_models"
    shutil.copytree(EXAMPLES / "03_semantic_and_question" / "semantic_models", target)
    return SemanticEngine(None, models_dir=str(target))


# --- S09-R1 ----------------------------------------------------------------------------


def test_bootstrap_is_idempotent_and_deterministic(semantic, store):
    first = bootstrap_from_models(semantic, clock=lambda: NOW)
    second = bootstrap_from_models(semantic, clock=lambda: NOW + timedelta(days=1))
    assert [q.question_id for q in first] == [q.question_id for q in second]
    assert [q.question_id for q in first] == sorted(q.question_id for q in first)
    questions = {q.question for q in first}
    # total_revenue: label + 3 synonyms, two templates each; order_count: label only.
    assert len(first) == 10
    assert {"Quel est le total revenue ?", "chiffre d'affaires par region", "CA par region",
            "Quel est le order count ?"} <= questions
    revenue = next(q for q in first if q.question == "CA par region")
    assert revenue.family == "A" and revenue.source == "yaml_bootstrap"
    assert revenue.expected == {"model_key": "sales.orders", "metric": "total_revenue"}
    assert revenue.question_id == question_id("CA par region", revenue.expected)
    assert revenue.semantic_layer_version.startswith("sha256:v1:")

    assert append_new(store, first) == 10
    assert append_new(store, second) == 0               # the second bootstrap writes nothing
    assert len(store.list_references(family="A")) == 10
    assert TEMPLATES_FR == ("Quel est le {label} ?", "{label} par {dimension}")


# --- S09-R2 ----------------------------------------------------------------------------


def test_bootstrap_never_calls_llm(semantic, monkeypatch):
    calls = []

    class SpyProvider:
        def __getattr__(self, name):
            calls.append(name)
            raise AssertionError(f"LLM provider used: {name}")

    def spy(*args, **kwargs):
        calls.append("get_llm_provider")
        return SpyProvider()

    import skifer.semantic.llm_provider as llm_provider

    monkeypatch.setattr(llm_provider, "get_llm_provider", spy)
    assert bootstrap_from_models(semantic, clock=lambda: NOW)
    assert calls == []


# --- S09-R8 ----------------------------------------------------------------------------


def _certified(candidate, model_key="candidates.revenue"):
    return CertificationEvent(
        event_id=uuid.uuid4().hex, metric_id=candidate.candidate_id,
        metric_name=candidate.metric_name, event_type="certified",
        previous_state=candidate.state, new_state=candidate.state,
        decided_by="system:semantic_sync_promote", evidence={"model_key": model_key},
        event_at=NOW,
    )


def test_certified_adds_family_a(store, tmp_path):
    candidate = seeded(store, str(tmp_path), state="promoted",
                       origin_question="Quel est le CA net par pays ?")
    added = on_certified(_certified(candidate), store)
    assert added.family == "A" and added.source == "promoted"
    assert added.question == "Quel est le CA net par pays ?"
    assert added.expected == {"model_key": "candidates.revenue", "metric": candidate.metric_name}
    assert store.list_references(family="A") == [added]
    assert on_certified(_certified(candidate), store) == added    # idempotent
    assert len(store.list_references()) == 1

    redacted = seeded(store, str(tmp_path), state="promoted", origin_question=None,
                      **_other("margin"))
    fallback = on_certified(_certified(redacted, "candidates.margin"), store)
    assert fallback.question == TEMPLATES_FR[0].format(label=redacted.metric_name.replace("_", " "))
    assert on_certified(replace(_certified(candidate), event_type="certification_requested"),
                        store) is None


def _other(name):
    from tests.test_candidates_service import _distinct

    return _distinct(name)


def test_incoherent_reject_adds_family_c(store, tmp_path):
    incoherent = seeded(store, str(tmp_path), state="needs_review",
                        origin_question="Revenue in ? per country")
    service(store, str(tmp_path)).reject(incoherent.candidate_id, decider="human:alice",
                                         reason="incohérent")
    [added] = store.list_references(family="C")
    assert added.question == "Revenue in ? per country"
    assert added.expected == {"outcome": "refuse"} and added.source == "rejected"

    # Rejected for another reason (no failed coherence): no family C question.
    plain = seeded(store, str(tmp_path), state="candidate", origin_question="Marge ?",
                   **_other("margin"))
    service(store, str(tmp_path)).reject(plain.candidate_id, decider="human:alice",
                                         reason="doublon")
    assert len(store.list_references(family="C")) == 1
    event = store.list_events(metric_id=plain.candidate_id)[-1]
    assert on_rejected(event, store) is None


def test_reference_hook_failure_never_fails_the_rejection(store, tmp_path, monkeypatch, caplog):
    import skifer.candidates.reference as reference

    def broken(event, store):
        raise RuntimeError("secret value 42")

    monkeypatch.setattr(reference, "on_rejected", broken)
    candidate = seeded(store, str(tmp_path), state="needs_review", origin_question="Q ?")
    view = service(store, str(tmp_path)).reject(candidate.candidate_id, decider="human:alice",
                                                reason="incohérent")
    assert view.state == "rejected"
    assert "RuntimeError" in caplog.text and "secret value" not in caplog.text


def test_add_reference_validates_the_family_keys(store):
    added = add_reference(store, question="CA des clients B2B en 2025", family="B",
                          expected=B_EXPECTED, clock=lambda: NOW)
    assert added.source == "human" and store.list_references(family="B") == [added]
    for family, expected, key in (("B", {"schema_dict": {}}, "result_hash"),
                                  ("C", {"outcome": "maybe"}, "outcome"),
                                  ("A", {"metric": "x"}, "model_key")):
        with pytest.raises(ValueError, match=key):
            add_reference(store, question="q", family=family, expected=expected)


# --- CLI -------------------------------------------------------------------------------


def _args(command, **values):
    values.setdefault("config", None)
    values.setdefault("models_dir", "semantic_models")
    return argparse.Namespace(candidates_command="reference", reference_command=command, **values)


def test_cli_reference_bootstrap_add_list(store, semantic, tmp_path, capsys):
    run = cli.run_candidates_reference
    assert run(_args("bootstrap"), store=store, semantic=semantic, clock=lambda: NOW) == 0
    assert json.loads(capsys.readouterr().out) == {
        "questions": 10, "reference_set_version": "v1", "written": 10}
    assert run(_args("bootstrap"), store=store, semantic=semantic, clock=lambda: NOW) == 0
    assert json.loads(capsys.readouterr().out)["written"] == 0

    expected = tmp_path / "expected.json"
    expected.write_text(json.dumps(B_EXPECTED))
    assert run(_args("add", family="B", question="CA B2B", expected_file=str(expected)),
               store=store, clock=lambda: NOW) == 0
    assert json.loads(capsys.readouterr().out)["family"] == "B"

    wrong = tmp_path / "wrong.yaml"
    wrong.write_text("outcome: refuse\n")
    assert run(_args("add", family="B", question="x", expected_file=str(wrong)),
               store=store) == 2
    assert "schema_dict" in capsys.readouterr().err
    assert run(_args("add", family="B", question="x", expected_file=str(tmp_path / "none")),
               store=store) == 2
    capsys.readouterr()

    assert run(_args("list", family="B", json=True), store=store) == 0
    assert [q["question"] for q in json.loads(capsys.readouterr().out)] == ["CA B2B"]
    assert run(_args("list", family=None, json=False), store=store) == 0
    lines = capsys.readouterr().out.splitlines()
    assert len(lines) == 11 and lines[0].split("\t")[0] in {"A", "B"}
    assert run(_args(None), store=store) == 2


def test_promote_hook_adds_family_a(tmp_path, monkeypatch):
    from skifer.candidates.store import DEFAULT_CANDIDATES_DB

    monkeypatch.chdir(tmp_path)
    store = SqliteCandidateStore(DEFAULT_CANDIDATES_DB)
    candidate = seeded(store, str(tmp_path), state="promoted", origin_question="CA net ?")
    store.close()
    (tmp_path / "semantic_catalog.yaml").write_text("models: []\n")
    cli._record_candidate_certified(candidate.candidate_id, "candidates.revenue", str(tmp_path))
    store = SqliteCandidateStore(DEFAULT_CANDIDATES_DB)
    [added] = store.list_references(family="A")
    store.close()
    assert added.question == "CA net ?" and added.expected["model_key"] == "candidates.revenue"
