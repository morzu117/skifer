"""Optional dense embeddings for the knowledge index (Plan 44, S08)."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import types

import pytest

from skifer.knowledge import embeddings
from skifer.knowledge.embeddings import (
    EmbeddingsUnavailableError,
    PrefixedEmbedder,
    embed_nodes,
    load_embedder,
    reciprocal_rank_fusion,
)
from skifer.knowledge.models import Node
from skifer.knowledge.store import NODES_TABLE, DeltaKnowledgeStore, SqliteKnowledgeStore
from tests.fakes.fake_backend import FakeBackend

REPO = Path(__file__).resolve().parents[1]
S02_NODE_COLUMNS = ("node_id", "kind", "fqn", "label", "text", "synonyms", "status", "domain",
                    "source", "fingerprint", "removed")


class FakeEncoder:
    """Deterministic vectors from a hash of the text; records every input."""

    def __init__(self):
        self.inputs: list[str] = []

    def encode(self, texts, normalize_embeddings=True):
        self.inputs.extend(texts)
        return [[byte / 255 for byte in hashlib.sha256(text.encode()).digest()[:8]]
                for text in texts]


def fake_embedder(model_name="multilingual-e5-small"):
    encoder = FakeEncoder()
    return PrefixedEmbedder(model_name, encoder), encoder


def node(fqn, label=None, **overrides):
    values = dict(kind="metric", fqn=fqn, label=label or fqn.rsplit(".", 1)[-1],
                  text="", status="certified", source="semantic")
    values.update(overrides)
    return Node.build(**values)


NODES = [node("sales.revenue_net", synonyms=("chiffre d'affaires net",)),
         node("sales.orders", text="Number of orders"),
         node("sales.margin")]


@pytest.fixture
def store(tmp_path):
    knowledge = SqliteKnowledgeStore(str(tmp_path / "knowledge.db"))
    knowledge.upsert_nodes(NODES)
    yield knowledge
    knowledge.close()


# --- S08-R1 ----------------------------------------------------------------------------


def test_core_never_imports_sentence_transformers():
    code = ("import sys, skifer, skifer.knowledge, skifer.knowledge.embeddings, "
            "skifer.knowledge.index, skifer.cli; "
            "print(sorted(m for m in ('sentence_transformers', 'torch') if m in sys.modules))")
    result = subprocess.run([sys.executable, "-c", code], cwd=REPO, capture_output=True,
                            text=True, check=True)
    assert result.stdout.strip() == "[]"


# --- S08-R2 (the --embed half; dense search is S08.2) -----------------------------------


def test_embed_without_extra_names_the_extra(monkeypatch, tmp_path, capsys):
    from skifer import cli

    monkeypatch.setitem(sys.modules, "sentence_transformers", None)  # import fails
    with pytest.raises(EmbeddingsUnavailableError,
                       match=r'pip install "skifer\[knowledge-embeddings\]"'):
        load_embedder("bge-m3")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["skifer", "knowledge", "sync", "--embed", "bge-m3"])
    with pytest.raises(SystemExit) as exit_info:
        cli.main()
    assert exit_info.value.code == 1
    assert 'skifer[knowledge-embeddings]' in capsys.readouterr().err

    monkeypatch.setattr(sys, "argv", ["skifer", "knowledge", "sync", "--embed", "gpt"])
    with pytest.raises(SystemExit) as exit_info:
        cli.main()
    assert exit_info.value.code == 2


# --- S08-R3 ----------------------------------------------------------------------------


def _fake_sentence_transformers(monkeypatch, *, error=None):
    calls = []

    class SentenceTransformer:
        def __init__(self, model_id, **kwargs):
            calls.append((model_id, kwargs))
            if error is not None:
                raise error
            self.encoder = FakeEncoder()

        def encode(self, texts, normalize_embeddings=True):
            return self.encoder.encode(texts, normalize_embeddings)

    monkeypatch.setitem(sys.modules, "sentence_transformers",
                        types.SimpleNamespace(SentenceTransformer=SentenceTransformer))
    return calls


def test_missing_model_names_cache_and_never_downloads(monkeypatch, tmp_path):
    calls = _fake_sentence_transformers(monkeypatch, error=OSError("not in cache"))
    cache = str(tmp_path / "models")
    with pytest.raises(EmbeddingsUnavailableError) as raised:
        load_embedder("multilingual-e5-small", cache_dir=cache)
    message = str(raised.value)
    assert cache in message and "local_files_only=True" in message
    assert "not in cache" not in message
    assert calls == [("intfloat/multilingual-e5-small",
                      {"cache_folder": cache, "local_files_only": True})]

    monkeypatch.setenv("HF_HOME", str(tmp_path / "hf"))
    with pytest.raises(EmbeddingsUnavailableError, match=str(tmp_path / "hf")):
        load_embedder("bge-m3")

    _fake_sentence_transformers(monkeypatch)
    loaded = load_embedder("bge-m3", cache_dir=cache)
    assert loaded.model_name == "bge-m3" and len(loaded.embed_query("x")) == 8
    with pytest.raises(ValueError, match="Unknown embedding model"):
        load_embedder("gpt")


# --- S08-R4 ----------------------------------------------------------------------------


def test_e5_prefixes_only_for_e5():
    e5, recorded = fake_embedder("multilingual-e5-small")
    e5.embed_query("CA net")
    e5.embed_passages(["revenue_net", "orders"])
    assert recorded.inputs == ["query: CA net", "passage: revenue_net", "passage: orders"]

    bge, recorded = fake_embedder("bge-m3")
    bge.embed_query("CA net")
    bge.embed_passages(["revenue_net"])
    assert recorded.inputs == ["CA net", "revenue_net"]


# --- S08-R5 ----------------------------------------------------------------------------


def test_rrf_expected_ranking():
    fused = reciprocal_rank_fusion([["a", "b", "c"], ["c", "b", "d"]], k=60)
    assert [item for item, _ in fused] == ["c", "b", "a", "d"]  # 1/61 + 1/63 > 2/62
    assert fused[0][1] == pytest.approx(1 / 61 + 1 / 63)
    assert fused[1][1] == pytest.approx(2 / 62)
    assert fused[2][1] == pytest.approx(1 / 61)
    # a and c tie on a swapped pair: broken by id, whatever the input order.
    assert reciprocal_rank_fusion([["c", "a"], ["a", "c"]]) == reciprocal_rank_fusion(
        [["a", "c"], ["c", "a"]])
    assert [item for item, _ in reciprocal_rank_fusion([["c", "a"], ["a", "c"]])] == ["a", "c"]
    with pytest.raises(ValueError):
        reciprocal_rank_fusion([["a"]], k=0)


# --- S08-R6 ----------------------------------------------------------------------------


def test_embed_only_changed_nodes(store):
    embedder, encoder = fake_embedder()
    assert embed_nodes(store, embedder) == 3
    assert embed_nodes(store, embedder) == 0           # second run: nothing to embed
    assert len(encoder.inputs) == 3
    assert encoder.inputs[0] == "passage: margin"

    store.upsert_nodes([node("sales.orders", text="Count of distinct orders")])
    assert [n.node_id for n in store.nodes_to_embed("multilingual-e5-small")] == [
        "metric:sales.orders"]
    assert embed_nodes(store, embedder) == 1
    assert set(store.list_embeddings("multilingual-e5-small")) == {n.node_id for n in NODES}


def test_model_change_reembeds_all(store):
    e5, _ = fake_embedder("multilingual-e5-small")
    bge, encoder = fake_embedder("bge-m3")
    embed_nodes(store, e5)
    assert embed_nodes(store, bge) == 3
    assert sorted(encoder.inputs) == ["margin", "orders Number of orders",
                                      "revenue_net chiffre d'affaires net"]
    assert store.list_embeddings("multilingual-e5-small") == {}
    assert len(store.list_embeddings("bge-m3")) == 3


def test_cli_sync_embed_reports_and_skips_unchanged(monkeypatch, tmp_path, capsys):
    from skifer import cli

    shutil_copy = __import__("shutil").copytree
    shutil_copy(REPO / "examples" / "03_semantic_and_question" / "semantic_models",
                tmp_path / "semantic_models")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(embeddings, "load_embedder",
                        lambda model, cache_dir=None: fake_embedder(model)[0])
    counts = []
    for _ in range(2):
        monkeypatch.setattr(sys, "argv", ["skifer", "knowledge", "sync", "--json",
                                          "--embed", "multilingual-e5-small"])
        with pytest.raises(SystemExit) as exit_info:
            cli.main()
        assert exit_info.value.code == 0
        report = json.loads(capsys.readouterr().out)
        assert report["embedding_model"] == "multilingual-e5-small"
        counts.append(report["embedded"])
    assert counts[0] > 0 and counts[1] == 0


# --- S08-R7 ----------------------------------------------------------------------------


@pytest.mark.parametrize("engine", ["sqlite", "delta"])
def test_store_migration_keeps_rows(engine, tmp_path):
    old = node("sales.orders")
    row = {**old.to_dict(), "synonyms": "[]", "removed": False}
    if engine == "sqlite":
        path = str(tmp_path / "old.db")
        connection = sqlite3.connect(path)
        connection.execute(f"CREATE TABLE {NODES_TABLE} ({', '.join(S02_NODE_COLUMNS)})")
        connection.execute(f"INSERT INTO {NODES_TABLE} VALUES ({', '.join('?' * 11)})",
                           tuple(int(row[c]) if c == "removed" else row[c]
                                 for c in S02_NODE_COLUMNS))
        connection.commit()
        connection.close()
        migrated = SqliteKnowledgeStore(path)
        columns = {r[1] for r in migrated._conn.execute(f"PRAGMA table_info({NODES_TABLE})")}
        assert {"embedding", "embedding_model"} <= columns
    else:
        backend = FakeBackend()
        backend._knowledge_tables[("cat._skifer_knowledge", NODES_TABLE)] = {
            old.node_id: {c: row[c] for c in S02_NODE_COLUMNS}}
        migrated = DeltaKnowledgeStore(backend, schema="cat._skifer_knowledge")
    assert migrated.list_nodes() == [old]
    assert [n.node_id for n in migrated.nodes_to_embed("bge-m3")] == [old.node_id]
    assert migrated.upsert_nodes([old, node("sales.margin")]) == (1, 0, 1)
    embedder, _ = fake_embedder("bge-m3")
    assert embed_nodes(migrated, embedder) == 2
    assert {n.node_id for n in migrated.list_nodes()} == {old.node_id, "metric:sales.margin"}
    if engine == "delta":
        assert backend._knowledge_alters == [(NODES_TABLE, ("embedding", "embedding_model"))]
    else:
        migrated.close()


def test_spark_migration_adds_columns_in_place(spark):
    from skifer.core.spark_backend import SparkBackend

    schema = "plan44_knowledge_migration"
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    try:
        spark.sql(f"CREATE SCHEMA {schema}")
        spark.sql(f"CREATE TABLE {schema}.{NODES_TABLE} ("
                  + ", ".join(f"`{c}` {'BOOLEAN' if c == 'removed' else 'STRING'}"
                              for c in S02_NODE_COLUMNS) + ") USING DELTA")
        old = node("sales.orders")
        values = {**old.to_dict(), "synonyms": "[]"}
        spark.sql(f"INSERT INTO {schema}.{NODES_TABLE} VALUES ("
                  + ", ".join("false" if c == "removed" else
                              ("NULL" if values[c] is None else f"'{values[c]}'")
                              for c in S02_NODE_COLUMNS) + ")")
        store = DeltaKnowledgeStore(SparkBackend(spark=spark), schema=schema)
        assert store.list_nodes() == [old]
        store.upsert_nodes([node("sales.margin")])
        embedder, _ = fake_embedder("bge-m3")
        assert embed_nodes(store, embedder) == 2
        assert set(store.list_embeddings("bge-m3")) == {old.node_id, "metric:sales.margin"}
        assert embed_nodes(store, embedder) == 0
        assert {"embedding", "embedding_model"} <= set(spark.table(f"{schema}.{NODES_TABLE}").columns)
    finally:
        spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


# --- S08.2: search modes ---------------------------------------------------------------


def _index(store, embedder=None):
    from skifer.knowledge.index import KnowledgeIndex

    return KnowledgeIndex.from_store(store, embedder=embedder)


def test_dense_without_extra_names_the_extra(store, monkeypatch):
    index = _index(store)
    for mode in ("dense", "hybrid"):
        with pytest.raises(EmbeddingsUnavailableError,
                           match=r'pip install "skifer\[knowledge-embeddings\]"'):
            index.search("chiffre d'affaires", mode=mode)
    assert index.search("orders")[0].node.node_id == "metric:sales.orders"   # bm25 still works
    with pytest.raises(ValueError, match="mode must be one of"):
        index.search("orders", mode="vector")


class KeywordEmbedder:
    """Two-dimensional vectors: (talks about money, talks about orders)."""

    model_name = "bge-m3"

    @staticmethod
    def _vector(text):
        text = text.lower()
        return [float(any(w in text for w in ("chiffre", "revenue", "ca "))),
                float(any(w in text for w in ("order", "commande")))]

    def embed_passages(self, texts):
        return [self._vector(text) for text in texts]

    def embed_query(self, text):
        return self._vector(text + " ")


def test_dense_and_hybrid_rank_by_vectors(store):
    embedder = KeywordEmbedder()
    embed_nodes(store, embedder)
    index = _index(store, embedder)

    dense = index.search("CA du mois", mode="dense", k=2)
    assert [hit.node.node_id for hit in dense] == ["metric:sales.revenue_net",
                                                   "metric:sales.margin"]
    assert dense[0].score == pytest.approx(1.0) and dense[0].matched_terms == ()
    assert index.search("CA du mois", mode="dense", kinds=["table"]) == []

    hybrid = index.search("orders count", mode="hybrid", k=3)
    assert hybrid[0].node.node_id == "metric:sales.orders"
    assert hybrid[0].score == pytest.approx(2 / 61)
    assert "orders" in hybrid[0].matched_terms
    # deterministic: same query, same ranking.
    assert index.search("orders count", mode="hybrid", k=3) == hybrid


def test_search_default_stays_bm25(store):
    embedder = KeywordEmbedder()
    embed_nodes(store, embedder)
    index = _index(store, embedder)
    from skifer.knowledge.index import KnowledgeIndex

    assert index.search("CA du mois") == KnowledgeIndex.from_store(store).search("CA du mois")
    other = _index(store, fake_embedder("multilingual-e5-small")[0])
    assert other.search("CA du mois", mode="dense") == []   # no vector for that model yet


# --- D7 (validated 29 October 2026): hybrid when a model is configured, bm25 otherwise ----


def test_d7_default_mode_resolution():
    from skifer.core.config import knowledge_embedding_settings

    assert knowledge_embedding_settings({})[2] == "bm25"
    assert knowledge_embedding_settings({"knowledge_embedding_model": "bge-m3"})[2] == "hybrid"
    assert knowledge_embedding_settings({"knowledge_embedding_model": "bge-m3",
                                         "knowledge_search_mode": "bm25"})[2] == "bm25"


def test_d7_index_from_config_searches_hybrid_by_default(store):
    from skifer.knowledge.index import KnowledgeIndex

    embedder = KeywordEmbedder()
    embed_nodes(store, embedder)
    index = KnowledgeIndex.from_config(store, {"knowledge_embedding_model": "bge-m3"},
                                       load_embedder=lambda model, cache_dir=None: embedder)
    assert index.default_mode == "hybrid"
    assert index.search("CA du mois") == index.search("CA du mois", mode="hybrid")
    assert KnowledgeIndex.from_config(store, {}).default_mode == "bm25"


def test_d7_default_hybrid_falls_back_but_explicit_mode_fails(store, monkeypatch, caplog):
    from skifer.knowledge.index import KnowledgeIndex

    monkeypatch.setitem(sys.modules, "sentence_transformers", None)
    with caplog.at_level("WARNING", logger="skifer.knowledge.index"):
        index = KnowledgeIndex.from_config(store, {"knowledge_embedding_model": "bge-m3"})
    assert index.default_mode == "bm25"
    assert "EmbeddingsUnavailableError" in caplog.text
    with pytest.raises(EmbeddingsUnavailableError, match="knowledge-embeddings"):
        KnowledgeIndex.from_config(store, {"knowledge_embedding_model": "bge-m3",
                                           "knowledge_search_mode": "hybrid"})


def test_d7_invalid_default_mode_is_rejected():
    from skifer.knowledge.index import KnowledgeIndex

    with pytest.raises(ValueError, match="default_mode"):
        KnowledgeIndex([], default_mode="vector")
