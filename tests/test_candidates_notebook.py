"""The arbitration notebook runs end to end and goes only through the service (Plan 44, S06)."""

from __future__ import annotations

import ast
from pathlib import Path
import runpy

import pytest
import yaml

from skifer import parse_schema
from skifer.candidates import SqliteCandidateStore, parameterize_filters
from skifer.candidates.service import CandidateService
from skifer.candidates.store import DEFAULT_CANDIDATES_DB
from skifer.knowledge.models import Node
from skifer.knowledge.store import SqliteKnowledgeStore
from tests.fakes.fake_dbutils import FakeDbutils
from tests.test_candidates_service import seeded

NOTEBOOK = (Path(__file__).resolve().parents[1]
            / "examples/25_metric_candidates/notebooks/arbitrage.py")
SOURCE_DB = "plan44_arbitrage"
TABLE = f"{SOURCE_DB}.orders"
CONFIG = """priority_check: [local]
environments:
  local:
    catalog: null
    semantic_metric_views_schema: plan44nb
    candidates_decider_identity: plain
"""


@pytest.fixture
def workspace(tmp_path, monkeypatch, spark):
    """``catalog: null`` config, SQLite stores in a temp dir, one local Delta source table."""
    import skifer.core.environment as environment

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(environment, "get_workspace_client", lambda: None)
    (tmp_path / "config.yaml").write_text(CONFIG)
    (tmp_path / "semantic_models").mkdir()
    (tmp_path / "semantic_models" / "semantic_catalog.yaml").write_text("models: []\n")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {SOURCE_DB}")
    spark.createDataFrame([("FR", 10.0), ("FR", 5.0), ("BE", 7.5)], ["country", "amount"]) \
        .write.mode("overwrite").saveAsTable(TABLE)
    knowledge = SqliteKnowledgeStore()
    knowledge.upsert_nodes([Node.build(kind="table", fqn=TABLE, label="orders",
                                       source="pipeline")])
    knowledge.close()
    store = SqliteCandidateStore(DEFAULT_CANDIDATES_DB)
    raw = {"tables": [{"name": TABLE, "alias": "o", "filter": ["country:equals:FR"]}],
           "aggregate": {"group_by": ["country"], "measures": [["amount", "revenue", "sum"]]}}
    schema, _ = parameterize_filters(parse_schema(yaml.safe_dump(raw)))
    from skifer.candidates import candidate_id_for, fingerprint_candidate

    candidate = seeded(store, str(tmp_path / "semantic_models"), schema_dict=schema,
                       tables_used=(TABLE,), fingerprint=fingerprint_candidate(schema),
                       candidate_id=candidate_id_for(fingerprint_candidate(schema)))
    try:
        yield tmp_path, store, candidate
    finally:
        store.close()
        for (name,) in spark.sql("SHOW DATABASES LIKE 'plan44nb*'").collect():
            spark.sql(f"DROP SCHEMA IF EXISTS {name} CASCADE")
        spark.sql(f"DROP SCHEMA IF EXISTS {SOURCE_DB} CASCADE")


def run_notebook(tmp_path, **widgets):
    displayed = []
    values = {"config_path": str(tmp_path / "config.yaml"), **widgets}
    runpy.run_path(str(NOTEBOOK), init_globals={
        "dbutils": FakeDbutils(values), "display": displayed.append, "spark": None})
    return displayed


def test_notebook_runs_without_decision(workspace):
    tmp_path, store, candidate = workspace
    displayed = run_notebook(tmp_path)
    needs_review, under_review = displayed
    assert [row["candidate_id"] for row in needs_review] == [candidate.candidate_id]
    assert under_review == []
    assert store.get_candidate(candidate.candidate_id).state == "needs_review"
    assert len(store.list_events(metric_id=candidate.candidate_id)) == 1


def test_notebook_runs_approve(workspace, spark, capsys):
    tmp_path, store, candidate = workspace
    displayed = run_notebook(tmp_path, candidate_id=candidate.candidate_id, decision="approve",
                             reason="checked with finance")
    decided = displayed[2][0]
    assert decided["state"] == "promoted"
    history = displayed[3]
    assert history[-1]["decided_by"] == "human:alice"
    assert history[-1]["justification"] == "checked with finance"
    promoted = store.get_candidate(candidate.candidate_id)
    assert promoted.state == "promoted" and promoted.reviewer == "human:alice"
    rows = spark.sql(f"SELECT country, revenue FROM {promoted.published_view} "
                     "ORDER BY country").collect()
    assert [tuple(row) for row in rows] == [("BE", 7.5), ("FR", 15.0)]

    # A refused decision is shown, never raised: the notebook still finishes.
    run_notebook(tmp_path, candidate_id=candidate.candidate_id, decision="reject", reason="r")
    assert "StateConflictError" in capsys.readouterr().out


def test_notebook_uses_only_public_service_methods():
    tree = ast.parse(NOTEBOOK.read_text(encoding="utf-8"))
    used = {node.attr for node in ast.walk(tree)
            if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name)
            and node.value.id == "service"}
    public = {name for name in vars(CandidateService)
              if not name.startswith("_") and callable(getattr(CandidateService, name))}
    assert used and used <= public
    assert {"approve", "reject", "promote", "review", "demote", "list", "history"} <= used


def test_notebook_shows_drift_only_when_asked(workspace):
    tmp_path, store, candidate = workspace
    displayed = run_notebook(tmp_path, show_drift="yes")
    signals = displayed[-1]
    assert {row["name"] for row in signals} >= {"fallback_rate", "rephrasing"}
    assert all(row["status"] == "inconclusive" for row in signals)  # no usage recorded
    assert store.get_candidate(candidate.candidate_id).state == "needs_review"
    assert len(run_notebook(tmp_path)) == 2
