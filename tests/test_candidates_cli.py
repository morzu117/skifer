"""``skifer candidates`` (Plan 44, S05)."""

from __future__ import annotations

import argparse
import json

import pytest

from skifer.cli import (
    CANDIDATES_EXIT_ERROR,
    CANDIDATES_EXIT_NEEDS_REVIEW,
    CANDIDATES_EXIT_OK,
    run_candidates_command,
)
from skifer.candidates import SqliteCandidateStore
from skifer.knowledge.store import SqliteKnowledgeStore
from tests.test_candidate_agent import SpyBackend, SpySemantic, sales_graph
from tests.test_candidates_validator import NOW, answered_on, country_schema, make_candidate

RESIDUE = ({"code": "not_in_nullable_pitfall", "message": "?", "blocking": False},)


def other_schema(target):
    return country_schema(aggregate={"group_by": ["country"],
                                     "measures": [["amount", target, "sum"]]})

LOOSE = {"candidates_min_requests": 1, "candidates_min_distinct_requesters": 1,
         "candidates_min_days": 1, "candidates_stability_runs": 2}


def args(**overrides):
    values = dict(candidates_command="validate", candidate=None, dry_run=False, json=True,
                  config=None, models_dir="semantic_models")
    values.update(overrides)
    return argparse.Namespace(**values)


class SpyStore:
    """Counts every write while delegating to a real store."""

    WRITES = ("upsert_candidate", "append_run", "append_event", "upsert_nodes", "upsert_edges",
              "mark_removed", "mark_edges_removed")

    def __init__(self, inner):
        self.inner, self.writes = inner, []

    def __getattr__(self, name):
        attribute = getattr(self.inner, name)
        if name in self.WRITES:
            def spy(*a, **kw):
                self.writes.append(name)
                return attribute(*a, **kw)
            return spy
        return attribute


@pytest.fixture
def world(tmp_path):
    store = SqliteCandidateStore(str(tmp_path / "candidates.db"))
    knowledge = SqliteKnowledgeStore(str(tmp_path / "knowledge.db"))
    graph = sales_graph()
    knowledge.upsert_nodes([graph.get(node_id) for node_id in sorted(graph._nodes)])
    knowledge.upsert_edges(graph.edges())
    (tmp_path / "semantic_catalog.yaml").write_text("models: []\n")
    yield store, knowledge, tmp_path
    store.close()
    knowledge.close()


def run(world, *, env=None, backend=None, catalog=None, capsys=None, **overrides):
    store, knowledge, tmp_path = world
    backend = backend or SpyBackend()
    semantic = SpySemantic(backend, env={**LOOSE, **(env or {})})
    semantic.models_dir = str(tmp_path)
    semantic.core.db = catalog
    code = run_candidates_command(args(**overrides), store=store, semantic=semantic,
                                  knowledge_store=knowledge, clock=lambda: NOW)
    return code, backend


def seed(store, schema=None, **overrides):
    candidate = make_candidate(schema, **overrides)
    store.upsert_candidate(candidate)
    answered_on(store, candidate, 0)
    return candidate


def test_validate_promotes_and_prints_json(world, capsys):
    store, _, _ = world
    candidate = seed(store)
    code, backend = run(world)
    assert code == CANDIDATES_EXIT_OK
    out = json.loads(capsys.readouterr().out)
    (row,) = out["candidates"]
    assert (row["outcome"], row["previous_state"], row["new_state"]) == (
        "validated", "candidate", "promoted")
    assert store.get_candidate(candidate.candidate_id).state == "promoted"
    assert store.get_candidate(candidate.candidate_id).stability_score == 1.0
    assert any(sql.startswith("CREATE OR REPLACE VIEW `semantic_candidates`")
               for sql in backend.executed)
    assert row["notes"] == ["metric view skipped: metric views only exist in Unity Catalog "
                            "(catalog: null)"]


def test_validate_dry_run_has_no_side_effect(world, capsys):
    store, knowledge, tmp_path = world
    seed(store)
    spy_store, spy_knowledge = SpyStore(store), SpyStore(knowledge)
    backend = SpyBackend()
    semantic = SpySemantic(backend, env=LOOSE)
    semantic.models_dir = str(tmp_path)
    semantic.core.db = "main"
    before = [c.to_dict() for c in store.list_candidates()]

    code = run_candidates_command(args(dry_run=True), store=spy_store, semantic=semantic,
                                  knowledge_store=spy_knowledge, clock=lambda: NOW)

    assert code == CANDIDATES_EXIT_OK
    assert backend.executed == []
    assert spy_store.writes == [] and spy_knowledge.writes == []
    assert [c.to_dict() for c in store.list_candidates()] == before
    out = json.loads(capsys.readouterr().out)
    assert out["dry_run"] is True
    assert out["candidates"][0]["outcome"] == "pending"  # stability not measured, not run


def test_validate_exit_code_two_on_needs_review(world, capsys):
    store, _, _ = world
    seed(store)
    assert run(world)[0] == CANDIDATES_EXIT_OK  # promoted: no needs_review

    residual = seed(store, other_schema("residual"), metric_name="residual", residues=RESIDUE)
    assert run(world)[0] == CANDIDATES_EXIT_NEEDS_REVIEW
    assert store.get_candidate(residual.candidate_id).state == "needs_review"
    # Already in needs_review: nothing went there during this run.
    assert run(world, candidate=[residual.candidate_id])[0] == CANDIDATES_EXIT_OK
    # A dry run writes no transition, so it never signals one.
    other = seed(store, other_schema("other"), metric_name="other", residues=RESIDUE)
    assert run(world, dry_run=True)[0] == CANDIDATES_EXIT_OK
    assert store.get_candidate(other.candidate_id).state == "candidate"


class _DeniedDDL(SpyBackend):
    def execute_sql(self, sql):
        if sql.startswith("CREATE"):
            raise RuntimeError("PERMISSION_DENIED")
        return super().execute_sql(sql)


def test_validate_exit_code_one_on_ddl_failure(world, capsys):
    store, _, _ = world
    candidate = seed(store)
    code, _ = run(world, backend=_DeniedDDL())
    assert code == CANDIDATES_EXIT_ERROR
    assert store.get_candidate(candidate.candidate_id).state == "candidate"


def test_semantic_layer_version_recomputed(world, capsys):
    store, _, tmp_path = world
    first = seed(store, residues=RESIDUE)
    run(world)
    (tmp_path / "semantic_catalog.yaml").write_text("models: [orders]\n")
    second = seed(store, other_schema("second"), metric_name="second", residues=RESIDUE)
    run(world)
    (event_1,) = store.list_events(metric_id=first.candidate_id)
    (event_2,) = store.list_events(metric_id=second.candidate_id)
    assert event_1.semantic_layer_version.startswith("sha256:v1:")
    assert event_1.semantic_layer_version != event_2.semantic_layer_version


def test_missing_subcommand_is_an_error(capsys):
    assert run_candidates_command(argparse.Namespace()) == CANDIDATES_EXIT_ERROR


# --- S06.4: arbitration subcommands ------------------------------------------------------

from skifer.cli import (  # noqa: E402
    CANDIDATES_EXIT_CONFLICT,
    CANDIDATES_EXIT_STALE,
    CANDIDATES_EXIT_USAGE,
    run_candidates_arbitration,
)
from skifer.observability.tracing import TRACING_HMAC_SECRET_ENV  # noqa: E402
from tests.test_candidates_service import seeded  # noqa: E402
from tests.test_candidates_service import service as make_service  # noqa: E402


def decide(argv, service_):
    """Parse ``skifer candidates <argv>`` the way the CLI does, then run it on ``service_``."""
    command, *rest = argv
    values = dict(candidates_command=command, json=False, state=None, reason=None,
                  pipeline_output=None, signal=None, replaced_by=None)
    if rest and not rest[0].startswith("--"):
        values["candidate_id"] = rest.pop(0)
    while rest:
        flag = rest.pop(0)[2:].replace("-", "_")
        values[flag] = True if flag == "json" else rest.pop(0)
    return run_candidates_arbitration(argparse.Namespace(**values), service=service_,
                                      decider="alice")


@pytest.fixture
def arbitration(tmp_path):
    store = SqliteCandidateStore(str(tmp_path / "candidates.db"))
    (tmp_path / "semantic_catalog.yaml").write_text("models: []\n")
    yield store, str(tmp_path)
    store.close()


def _ok(store, models_dir):
    candidate = seeded(store, models_dir)
    return ["reject", candidate.candidate_id, "--reason", "duplicate"], make_service(
        store, models_dir)


def _no_secret(store, models_dir):
    candidate = seeded(store, models_dir)
    return ["reject", candidate.candidate_id, "--reason", "r"], make_service(
        store, models_dir, decider_identity="hmac")


def _unknown(store, models_dir):
    return ["show", "candidate:v1:" + "0" * 64], make_service(store, models_dir)


def _outside_drafts(store, models_dir):
    candidate = seeded(store, models_dir, state="promoted")
    return ["promote", candidate.candidate_id, "--reason", "r",
            "--pipeline-output", "schemas/gold/x.yaml"], make_service(store, models_dir)


def _stale(store, models_dir):
    candidate = seeded(store, models_dir, state="promoted")
    with open(f"{models_dir}/semantic_catalog.yaml", "w") as handle:
        handle.write("models: [orders]\n")
    return ["review", candidate.candidate_id, "--reason", "r"], make_service(store, models_dir)


def _conflict(store, models_dir):
    candidate = seeded(store, models_dir, state="promoted")
    return ["reject", candidate.candidate_id, "--reason", "r"], make_service(store, models_dir)


@pytest.mark.parametrize("scenario, expected", [
    (_ok, CANDIDATES_EXIT_OK),
    (_no_secret, CANDIDATES_EXIT_ERROR),
    (_unknown, CANDIDATES_EXIT_USAGE),
    (_outside_drafts, CANDIDATES_EXIT_USAGE),
    (_stale, CANDIDATES_EXIT_STALE),
    (_conflict, CANDIDATES_EXIT_CONFLICT),
])
def test_exit_codes(arbitration, scenario, expected, monkeypatch, capsys):
    monkeypatch.delenv(TRACING_HMAC_SECRET_ENV, raising=False)
    store, models_dir = arbitration
    argv, service_ = scenario(store, models_dir)
    assert decide(argv, service_) == expected
    if expected == CANDIDATES_EXIT_OK:
        assert json.loads(capsys.readouterr().out)["state"] == "rejected"


def test_read_commands_print_json(arbitration, capsys):
    store, models_dir = arbitration
    candidate = seeded(store, models_dir)
    service_ = make_service(store, models_dir)

    assert decide(["list", "--state", "needs_review", "--json"], service_) == CANDIDATES_EXIT_OK
    assert [row["candidate_id"] for row in json.loads(capsys.readouterr().out)] == [
        candidate.candidate_id]
    assert decide(["show", candidate.candidate_id, "--json"], service_) == CANDIDATES_EXIT_OK
    assert json.loads(capsys.readouterr().out)["state"] == "needs_review"
    assert decide(["history", candidate.candidate_id], service_) == CANDIDATES_EXIT_OK
    assert "candidate -> needs_review" in capsys.readouterr().out
    assert decide(["list", "--state", "nope"], service_) == CANDIDATES_EXIT_ERROR


def test_json_output_stays_parseable_when_the_engine_prints(arbitration, monkeypatch, capsys):
    """Spark and the semantic catalogue print on start-up; ``--json`` must still be pure JSON."""
    import skifer.candidates.service as service_module
    import skifer.core.core as core_module
    import skifer.observability.certification_store as certification_module
    import skifer.semantic.semantic as semantic_module

    store, models_dir = arbitration
    candidate = seeded(store, models_dir)

    class Chatty:
        def __init__(self, *args, **kwargs):
            print("[SparkFactory] session initialized")

    monkeypatch.setattr(core_module, "SkiferEngine", Chatty)
    monkeypatch.setattr(semantic_module, "SemanticEngine", Chatty)
    monkeypatch.setattr(certification_module, "default_certification_store", lambda e: None)
    monkeypatch.setattr(service_module, "build_candidate_service",
                        lambda semantic: make_service(store, models_dir))
    parsed = argparse.Namespace(candidates_command="show", candidate_id=candidate.candidate_id,
                                json=True, config=None, models_dir=models_dir)
    assert run_candidates_arbitration(parsed) == CANDIDATES_EXIT_OK
    out = capsys.readouterr()
    assert json.loads(out.out)["candidate_id"] == candidate.candidate_id
    assert "session initialized" in out.err
