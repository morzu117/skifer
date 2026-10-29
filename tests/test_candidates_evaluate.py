"""Evaluation of the reference set (Plan 44, S09.2)."""

from __future__ import annotations

import argparse
import builtins
from datetime import timedelta
import json
import sys
from types import SimpleNamespace

import pytest

from skifer import cli
from skifer.agentic.models import AgentResponse
from skifer.candidates import CandidateRun, SqliteCandidateStore
from skifer.candidates.reference import (
    add_reference,
    evaluate,
    load_evaluations,
    store_evaluation,
)
from tests.test_candidates_validator import NOW

HASH = "sha256:v1:" + "1" * 64
OTHER_HASH = "sha256:v1:" + "2" * 64
CANDIDATE = "candidate:v1:" + "b" * 64
CANARY = "CANARY_7731"


def _a(metric="total_revenue", model_key="sales.orders"):
    return {"model_key": model_key, "metric": metric}


def _b(result_hash=HASH):
    return {"schema_dict": {"tables": [{"name": "gold.fact_orders",
                                        "filter": [f"customer:equals:{CANARY}"]}]},
            "result_hash": result_hash}


# question -> (expected, metric ranking returned by the index, hub response)
CASES = {
    "A": [
        ("a1 revenue", _a(), ["metric:sales.orders.total_revenue"],
         AgentResponse(question="", mode="query", model_used="sales.orders",
                       query_params={"metrics": ["total_revenue"]})),
        (f"a2 revenue {CANARY}", _a(), [f"metric:x.m{i}" for i in range(6)]
         + ["metric:sales.orders.total_revenue"],
         AgentResponse(question="", mode="query", model_used="sales.orders",
                       query_params={"metrics": ["order_count"],
                                     "filters": [{"column": "c", "value": CANARY}]})),
        ("a3 margin", _a("margin"), ["metric:x.m0"], AgentResponse(question="", mode="no_model")),
        ("a4 orders", _a("order_count"), ["metric:x.m0", "metric:x.m1",
                                          "metric:sales.orders.order_count"],
         AgentResponse(question="", mode="query", model_used="sales.orders",
                       query_params={"metrics": ["order_count", "total_revenue"]})),
    ],
    "B": [
        (f"b1 CA des clients {CANARY}", _b(), [],
         AgentResponse(question="", mode="candidate", candidate_id=CANDIDATE)),
        (f"b2 CA B2B {CANARY}", _b(OTHER_HASH), [],
         AgentResponse(question="", mode="candidate_refused", error=f"value {CANARY}")),
    ],
    "C": [
        ("c1 météo", {"outcome": "refuse"}, [], AgentResponse(question="", mode="candidate_refused")),
        ("c2 le truc", {"outcome": "clarify"}, [],
         AgentResponse(question="", mode="needs_clarification")),
        (f"c3 salaire de {CANARY}", {"outcome": "refuse"}, [],
         AgentResponse(question="", mode="query", model_used="hr.people")),
    ],
}


class FakeHub:
    def __init__(self, responses, failing=()):
        self.responses, self.failing, self.asked = responses, set(failing), []

    def ask(self, question, **kwargs):
        self.asked.append(question)
        if question in self.failing:
            raise RuntimeError(f"backend said {CANARY}")
        return self.responses[question]


class FakeIndex:
    def __init__(self, rankings):
        self.rankings, self.calls = rankings, []

    def search(self, question, *, k, kinds, mode):
        self.calls.append((k, tuple(kinds), mode))
        return [SimpleNamespace(node=SimpleNamespace(node_id=node_id))
                for node_id in self.rankings.get(question, [])[:k]]


@pytest.fixture
def store(tmp_path):
    candidate_store = SqliteCandidateStore(str(tmp_path / "candidates.db"))
    yield candidate_store
    candidate_store.close()


def _seed(store, families="ABC"):
    responses, rankings = {}, {}
    for family in families:
        for question, expected, ranking, response in CASES[family]:
            add_reference(store, question=question, family=family, expected=expected,
                          clock=lambda: NOW)
            responses[question], rankings[question] = response, ranking
    store.append_run(CandidateRun(run_id="r1", candidate_id=CANDIDATE, executed_at=NOW,
                                  purpose="answer", result_hash=HASH, row_count=3))
    return FakeHub(responses), FakeIndex(rankings)


@pytest.fixture
def report(store):
    hub, index = _seed(store)
    return evaluate(hub, index, store, semantic_layer_version="sha256:v1:" + "0" * 64)


# --- S09-R3 ----------------------------------------------------------------------------


@pytest.mark.parametrize("metric, expected", [
    ("A.recall@5", 0.5),               # a1 (rank 1) and a4 (rank 3)
    ("A.recall@10", 0.75),             # + a2 (rank 7)
    ("A.routing_accuracy", 0.5),       # a1 and a4; a2 has the wrong metric, a3 no model
    ("B.exact_match", 0.5),            # b1's answer run carries the expected hash
    ("C.correct_refusal", 0.666667),   # c1 refused, c2 asked for a precision
    ("C.false_answer", 0.333333),      # c3 answered
    ("C.false_refusal", 0.333333),     # a3 and b2 refused, out of 6 answerable questions
])
def test_metric_formulas(report, metric, expected):
    assert report.metrics[metric] == expected


def test_report_shape_and_determinism(report, store):
    assert report.question_count == {"A": 4, "B": 2, "C": 3}
    assert report.reference_set_version == "v1"
    # a2, a3 (A) · b2 (B) · c3 (C): in family order, then by question id.
    assert [f["family"] for f in report.failures] == ["A", "A", "B", "C"]
    again = evaluate(*_seed_for(store), store, semantic_layer_version=report.semantic_layer_version)
    assert again.to_dict() == report.to_dict()


def _seed_for(store):
    responses = {q: r for family in "ABC" for q, _, _, r in CASES[family]}
    rankings = {q: ranking for family in "ABC" for q, _, ranking, _ in CASES[family]}
    return FakeHub(responses), FakeIndex(rankings)


def test_search_mode_and_k_are_passed_to_the_index(store):
    hub, index = _seed(store, "A")
    evaluate(hub, index, store, k_values=(3, 20), search_mode="hybrid",
             semantic_layer_version=None)
    assert set(index.calls) == {(20, ("metric",), "hybrid")}
    with pytest.raises(ValueError, match="k_values"):
        evaluate(hub, index, store, k_values=(0,))


# --- S09-R4 ----------------------------------------------------------------------------


def test_empty_family_is_none(store):
    hub, index = _seed(store, "A")
    report = evaluate(hub, index, store, semantic_layer_version=None)
    assert report.metrics["B.exact_match"] is None
    assert report.metrics["C.correct_refusal"] is None
    assert report.metrics["C.false_answer"] is None
    assert report.metrics["C.false_refusal"] == 0.25        # A alone is not empty
    empty = evaluate(FakeHub({}), FakeIndex({}), SqliteCandidateStore(":memory:"),
                     semantic_layer_version=None)
    assert set(empty.metrics.values()) == {None}
    assert empty.question_count == {"A": 0, "B": 0, "C": 0} and empty.failures == ()


# --- S09-R5 ----------------------------------------------------------------------------


def test_failures_never_carry_filter_values(store):
    hub, index = _seed(store)
    hub.failing.add(f"b2 CA B2B {CANARY}")
    report = evaluate(hub, index, store, semantic_layer_version=None)
    serialized = json.dumps(report.to_dict())
    assert CANARY not in serialized
    assert "schema_dict" not in serialized and "a2 revenue" not in serialized
    b2 = next(f for f in report.failures if f["expected"].get("result_hash") == OTHER_HASH)
    assert b2["obtained"]["mode"] == "error"


# --- persistence ------------------------------------------------------------------------


def test_store_and_load_evaluations(report, store, tmp_path):
    directory = str(tmp_path / ".skifer_evaluations")
    first = store_evaluation(report, store, directory=directory, clock=lambda: NOW)
    second = store_evaluation(report, store, directory=directory,
                              clock=lambda: NOW + timedelta(hours=1))
    assert first != second and first.endswith(".json")
    [run, _] = store.list_runs("reference_set:v1", purpose="evaluation")
    assert run.result_hash.startswith("sha256:v1:") and run.row_count == 9
    assert load_evaluations(store, directory=directory) == [report.to_dict()] * 2
    with pytest.raises(FileExistsError):
        store_evaluation(report, store, directory=directory, clock=lambda: NOW)
    assert load_evaluations(store, directory=str(tmp_path / "missing")) == []


# --- S09-R6 + CLI -----------------------------------------------------------------------


def _args(**values):
    values.setdefault("json", True)
    values.setdefault("mlflow", False)
    values.setdefault("config", None)
    values.setdefault("models_dir", "semantic_models")
    return argparse.Namespace(candidates_command="evaluate", **values)


def test_mlflow_flag_requires_extra(store, tmp_path, monkeypatch, capsys):
    monkeypatch.setitem(sys.modules, "mlflow", None)
    hub, index = _seed(store)
    code = cli.run_candidates_evaluate(
        _args(mlflow=True, evaluations_dir=str(tmp_path / "ev")), hub=hub, index=index,
        store=store)
    assert code == 1
    assert 'pip install "skifer[tracing]"' in capsys.readouterr().err
    assert hub.asked == []                      # fails before asking anything


def test_mlflow_logs_metrics_and_report(store, tmp_path, monkeypatch, capsys):
    logged = {}

    class FakeRun:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    fake = SimpleNamespace(
        start_run=lambda run_name: FakeRun(),
        log_params=lambda params: logged.setdefault("params", params),
        log_metrics=lambda metrics: logged.setdefault("metrics", metrics),
        log_dict=lambda data, name: logged.setdefault("artifact", (name, data)),
    )
    monkeypatch.setitem(sys.modules, "mlflow", fake)
    hub, index = _seed(store)
    assert cli.run_candidates_evaluate(
        _args(mlflow=True, evaluations_dir=str(tmp_path / "ev")), hub=hub, index=index,
        store=store, clock=lambda: NOW) == 0
    capsys.readouterr()
    assert logged["params"]["reference_set_version"] == "v1"
    assert logged["metrics"]["A.recall_at_5"] == 0.5
    assert logged["artifact"][0] == "evaluation_report.json"
    assert CANARY not in json.dumps(logged["artifact"][1])


def test_no_mlflow_import_without_flag(store, tmp_path, monkeypatch, capsys):
    imported = []
    real_import = builtins.__import__

    def spy(name, *args, **kwargs):
        if name.split(".")[0] == "mlflow":
            imported.append(name)
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", spy)
    hub, index = _seed(store)
    directory = tmp_path / "ev"
    assert cli.run_candidates_evaluate(_args(evaluations_dir=str(directory)), hub=hub,
                                       index=index, store=store, clock=lambda: NOW) == 0
    assert imported == []
    report = json.loads(capsys.readouterr().out)
    assert report["question_count"] == {"A": 4, "B": 2, "C": 3}
    assert len(list(directory.glob("*.json"))) == 1


def test_cli_empty_set_is_a_valid_report(tmp_path, capsys):
    store = SqliteCandidateStore(":memory:")
    assert cli.run_candidates_evaluate(
        _args(json=False, evaluations_dir=str(tmp_path / "ev")), hub=FakeHub({}),
        index=FakeIndex({}), store=store) == 0
    out = capsys.readouterr().out
    assert "reference set is empty — run 'skifer candidates reference bootstrap'" in out
    assert "B.exact_match\tn/a" in out
    assert not (tmp_path / "ev").exists()


def test_cli_technical_error_is_one(store, tmp_path, capsys):
    class Broken:
        def list_references(self):
            raise RuntimeError(CANARY)

    assert cli.run_candidates_evaluate(_args(evaluations_dir=str(tmp_path)), hub=FakeHub({}),
                                       index=FakeIndex({}), store=Broken()) == 1
    assert CANARY not in capsys.readouterr().err


def test_cli_json_stays_parseable_when_the_hub_prints(store, tmp_path, capsys):
    hub, index = _seed(store)

    class PrintingHub:
        def ask(self, question, **kwargs):
            print("SELECT region AS region FROM gold.orders")   # what SemanticEngine does
            return hub.ask(question, **kwargs)

    assert cli.run_candidates_evaluate(_args(evaluations_dir=str(tmp_path / "ev")),
                                       hub=PrintingHub(), index=index, store=store,
                                       clock=lambda: NOW) == 0
    captured = capsys.readouterr()
    assert json.loads(captured.out)["question_count"]["A"] == 4
    assert "SELECT region" in captured.err
