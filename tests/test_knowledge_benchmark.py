"""Search benchmark on the reference set (Plan 44, S09.3)."""

from __future__ import annotations

import argparse
import itertools
import json
import sys

import pytest

from skifer import cli
from skifer.candidates import SqliteCandidateStore
from skifer.candidates.reference import add_reference
from skifer.knowledge.benchmark import benchmark, parse_config
from skifer.knowledge.embeddings import EmbeddingsUnavailableError
from skifer.knowledge.store import SqliteKnowledgeStore
from tests.test_candidates_validator import NOW
from tests.test_knowledge_embeddings import NODES, KeywordEmbedder


@pytest.fixture
def store(tmp_path):
    knowledge = SqliteKnowledgeStore(str(tmp_path / "knowledge.db"))
    knowledge.upsert_nodes(NODES)
    yield knowledge
    knowledge.close()


@pytest.fixture
def questions(tmp_path):
    candidates = SqliteCandidateStore(str(tmp_path / "candidates.db"))
    for text, metric in (("revenue_net", "revenue_net"), ("CA du mois", "revenue_net"),
                         ("orders", "orders"), ("inconnu", "absent")):
        add_reference(candidates, question=text, family="A", clock=lambda: NOW,
                      expected={"model_key": "sales", "metric": metric})
    add_reference(candidates, question="météo", family="C", expected={"outcome": "refuse"},
                  clock=lambda: NOW)
    yield candidates.list_references()
    candidates.close()


def ticking(step=0.001):
    counter = itertools.count()
    return lambda: next(counter) * step


# --- S09-R7 ----------------------------------------------------------------------------


def test_bm25_without_extra(store, questions, monkeypatch):
    monkeypatch.setitem(sys.modules, "sentence_transformers", None)
    result = benchmark(store, questions, configs=["bm25"], monotonic=ticking())["bm25"]
    # revenue_net and orders rank 1; "CA du mois" has no common term; "absent" is not indexed.
    assert result["questions"] == 4                      # family C is not benchmarked
    assert result["recall@5"] == 0.5 and result["recall@10"] == 0.5
    assert result["mean_rank"] == 1 and result["not_found"] == 2
    assert result["latency_ms_p50"] == 1.0 and result["index_build_s"] == 0.001


def test_dense_config_requires_extra(store, questions, monkeypatch, tmp_path, capsys):
    monkeypatch.setitem(sys.modules, "sentence_transformers", None)
    with pytest.raises(EmbeddingsUnavailableError,
                       match=r'pip install "skifer\[knowledge-embeddings\]"'):
        benchmark(store, questions, configs=["bm25", "hybrid:bge-m3"])
    args = argparse.Namespace(configs=["dense:multilingual-e5-small"], json=True,
                              model_cache=None, db=None, candidates_db=None)
    assert cli._knowledge_benchmark(args, store=store, questions=questions) == 1
    assert "skifer[knowledge-embeddings]" in capsys.readouterr().err


def test_dense_and_hybrid_configs_with_injected_embedder(store, questions):
    results = benchmark(store, questions, configs=["bm25", "dense:bge-m3", "hybrid:bge-m3"],
                        load_embedder=lambda model, cache_dir=None: KeywordEmbedder(),
                        monotonic=ticking())
    assert list(results) == ["bm25", "dense:bge-m3", "hybrid:bge-m3"]
    assert results["dense:bge-m3"]["recall@5"] == 0.75     # "CA du mois" found by vectors
    assert results["hybrid:bge-m3"]["recall@5"] == 0.75
    assert results["dense:bge-m3"]["not_found"] == 1
    assert store.list_embeddings("bge-m3") == {}           # the benchmark writes nothing


def test_unknown_config_is_usage_error(store, questions, capsys):
    for config in ("vector", "dense:gpt", "dense"):
        with pytest.raises(ValueError, match="Unknown benchmark config"):
            parse_config(config)
    args = argparse.Namespace(configs=["bm25", "sparse"], json=False, model_cache=None,
                              db=None, candidates_db=None)
    assert cli._knowledge_benchmark(args, store=store, questions=questions) == 2


def test_cli_benchmark_reads_the_sqlite_stores(store, questions, tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["skifer", "knowledge", "benchmark", "--config", "bm25",
                                      "--json", "--db", str(tmp_path / "knowledge.db"),
                                      "--candidates-db", str(tmp_path / "candidates.db")])
    with pytest.raises(SystemExit) as exit_info:
        cli.main()
    assert exit_info.value.code == 0
    assert json.loads(capsys.readouterr().out)["bm25"]["questions"] == 4

    args = argparse.Namespace(configs=["bm25"], json=False, model_cache=None, db=None,
                              candidates_db=str(tmp_path / "missing.db"))
    assert cli._knowledge_benchmark(args, store=store) == 0
    out = capsys.readouterr().out.splitlines()
    assert out[0].startswith("config\trecall@5") and out[1].startswith("bm25\tn/a")
