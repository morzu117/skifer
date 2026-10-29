"""Plan 44 S02: `skifer knowledge sync|search` — Spark-free, exit codes 0/1/2."""

from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

import pytest

from skifer import cli

EXAMPLES = Path(__file__).resolve().parents[1] / "examples"


def _run(monkeypatch, capsys, *argv):
    monkeypatch.setattr(sys, "argv", ["skifer", *argv])
    with pytest.raises(SystemExit) as exit_info:
        cli.main()
    captured = capsys.readouterr()
    return exit_info.value.code, captured.out, captured.err


@pytest.fixture
def project(tmp_path, monkeypatch):
    shutil.copytree(EXAMPLES / "03_semantic_and_question" / "semantic_models",
                    tmp_path / "semantic_models")
    monkeypatch.chdir(tmp_path)
    return tmp_path


def test_sync_then_search_finds_the_revenue_metric_on_examples(project, monkeypatch, capsys):
    code, out, _ = _run(monkeypatch, capsys, "knowledge", "sync", "--json")
    assert code == 0
    report = json.loads(out)
    assert report["added"] > 0 and report["unavailable"] == []
    assert (project / ".skifer_knowledge.db").exists()

    code, out, _ = _run(monkeypatch, capsys, "knowledge", "search", "chiffre d'affaires par pays",
                        "--json")
    assert code == 0
    top3 = [hit["node"]["node_id"] for hit in json.loads(out)[:3]]
    assert "metric:sales.orders.total_revenue" in top3

    code, out, _ = _run(monkeypatch, capsys, "knowledge", "sync")
    assert code == 0
    assert "added=0 updated=0" in out and "removed=0" in out

    code, out, _ = _run(monkeypatch, capsys, "knowledge", "search", "revenue", "--kind", "metric",
                        "-k", "1")
    assert code == 0
    assert out.splitlines()[0].split("\t")[1:4] == ["metric", "sales.orders.total_revenue", "certified"]


def test_dry_run_writes_nothing(project, monkeypatch, capsys):
    code, out, _ = _run(monkeypatch, capsys, "knowledge", "sync", "--dry-run")
    assert code == 0 and "(dry run)" in out
    code, out, _ = _run(monkeypatch, capsys, "knowledge", "search", "revenue")
    assert code == 0
    assert out.strip() == "index is empty — run 'skifer knowledge sync'"


def test_search_before_sync_reports_empty_index(project, monkeypatch, capsys):
    code, out, _ = _run(monkeypatch, capsys, "knowledge", "search", "revenue")
    assert (code, out.strip()) == (0, "index is empty — run 'skifer knowledge sync'")
    assert not (project / ".skifer_knowledge.db").exists()


@pytest.mark.parametrize("schema", ["main", "a.b.c", "a b.c"])
def test_malformed_schema_is_a_usage_error(project, monkeypatch, capsys, schema):
    code, _, err = _run(monkeypatch, capsys, "knowledge", "sync", "--schema", schema)
    assert code == 2
    assert "--schema expects CATALOG.SCHEMA" in err


def test_unreachable_store_is_a_technical_error(project, monkeypatch, capsys):
    db = str(project / "missing" / "dir" / "k.db")
    code, _, err = _run(monkeypatch, capsys, "knowledge", "sync", "--db", db)
    assert code == 1
    assert "SqliteKnowledgeStore" in err and "k.db" in err


def test_missing_subcommand_and_bad_kind_are_usage_errors(project, monkeypatch, capsys):
    assert _run(monkeypatch, capsys, "knowledge")[0] == 2
    _run(monkeypatch, capsys, "knowledge", "sync")
    code, _, err = _run(monkeypatch, capsys, "knowledge", "search", "x", "--kind", "view")
    assert code == 2 and "Unknown kinds" in err
