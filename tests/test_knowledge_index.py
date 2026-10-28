"""Plan 44 S02: tokenizer, field-weighted BM25 and bounded neighborhood."""

from __future__ import annotations

import json
import os
import subprocess
import sys

import pytest

from skifer.knowledge.index import KnowledgeIndex, tokenize
from skifer.knowledge.models import Edge, Node
from skifer.knowledge.store import SqliteKnowledgeStore


def _node(fqn, kind="column", label=None, text="", synonyms=(), domain="main.sales"):
    return Node.build(kind=kind, fqn=fqn, label=label or fqn.rsplit(".", 1)[-1], text=text,
                      synonyms=tuple(synonyms), domain=domain, source="pipeline")


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("totalAmountEur", ("total", "amount", "eur")),
        ("total_amount_eur", ("total", "amount", "eur")),
        ("Chiffre d'affaires", ("chiffre", "affaires")),
        ("Chiffre d’affaires", ("chiffre", "affaires")),
        ("Région / Pays", ("region", "pays")),
        ("HTTPServer2024", ("http", "server", "2024")),
        ("revenue per region in 2026", ("revenue", "region", "2026")),
        ("", ()),
    ],
)
def test_tokenize_cases(text, expected):
    assert tokenize(text) == expected


def _graph() -> list[Node]:
    return [
        _node("main.sales.orders.amount", text="Order amount in EUR", synonyms=("revenue",)),
        _node("main.sales.orders.country", text="Country of the customer", synonyms=("pays",)),
        _node("sales.revenue", kind="metric", label="revenue", text="Total revenue"),
        _node("main.sales.orders", kind="table", text="All orders, one row per order"),
        _node("main.hr.people", kind="table", text="Employees", domain="main.hr"),
    ]


def test_search_ranks_and_filters():
    index = KnowledgeIndex(_graph())
    hits = index.search("revenue")
    assert hits[0].node.node_id == "metric:sales.revenue"
    assert hits[0].matched_terms == ("revenue",)
    assert [h.node.node_id for h in index.search("revenue", kinds=["column"])] == [
        "column:main.sales.orders.amount"]
    assert index.search("employees", domain="main.sales") == []
    assert index.search("employees", domain="main.hr")[0].node.fqn == "main.hr.people"
    assert index.search("the of") == []
    assert len(index.search("order revenue country", k=2)) == 2
    with pytest.raises(ValueError, match="Unknown kinds"):
        index.search("x", kinds=["view"])


def test_label_match_outranks_text_match():
    in_label = _node("a.b.t.margin", label="margin", text="computed value")
    in_text = _node("a.b.t.value", label="value", text="the margin computed")
    hits = KnowledgeIndex([in_text, in_label]).search("margin")
    assert [hit.node.node_id for hit in hits] == [in_label.node_id, in_text.node_id]
    assert hits[0].score > hits[1].score
    in_synonyms = _node("a.b.t.gm", label="gm", text="computed value", synonyms=("margin",))
    hits = KnowledgeIndex([in_text, in_synonyms]).search("margin")
    assert hits[0].node.node_id == in_synonyms.node_id


def test_equal_scores_sort_by_node_id():
    twins = [_node(f"a.b.t.c{i}", label="amount") for i in (3, 1, 2)]
    hits = KnowledgeIndex(twins).search("amount")
    assert [h.node.node_id for h in hits] == sorted(n.node_id for n in twins)
    assert len({h.score for h in hits}) == 1


def test_search_is_deterministic_across_hash_seeds(tmp_path):
    db = tmp_path / "k.db"
    store = SqliteKnowledgeStore(str(db))
    nodes = _graph() + [_node(f"a.b.t.c{i}", label="amount", text=f"amount {i % 3}") for i in range(30)]
    store.upsert_nodes(nodes)
    store.close()
    code = (
        "import json, sys\n"
        "from skifer.knowledge.index import KnowledgeIndex\n"
        "from skifer.knowledge.store import SqliteKnowledgeStore\n"
        "index = KnowledgeIndex.from_store(SqliteKnowledgeStore(sys.argv[1]))\n"
        "print(json.dumps([[h.node.node_id, h.score, h.matched_terms] "
        "for h in index.search('order amount revenue pays', k=50)]))\n"
    )
    outputs = set()
    for seed in ("0", "1", "4242"):
        result = subprocess.run([sys.executable, "-c", code, str(db)], capture_output=True,
                                text=True, env={**os.environ, "PYTHONHASHSEED": seed}, check=True)
        outputs.add(result.stdout.strip())
    assert len(outputs) == 1
    assert len(json.loads(outputs.pop())) > 30


def test_neighbors_bounded_and_cycle_safe():
    names = ["a", "b", "c", "d", "e", "w"]
    nodes = [_node(f"x.y.t.{n}") for n in names]
    ids = {n: f"column:x.y.t.{n}" for n in names}

    def edge(a, b, weight=1.0, kind="derives"):
        return Edge.build(kind=kind, from_id=ids[a], to_id=ids[b], weight=weight, source="pipeline")

    edges = [edge("a", "b"), edge("b", "c"), edge("c", "a"), edge("c", "d"), edge("d", "e"),
             edge("a", "w", weight=0.2), edge("b", "d")]
    index = KnowledgeIndex(nodes, edges)
    result = [(node.label, hops) for node, hops in index.neighbors(ids["a"], max_depth=2)]
    assert result == [("b", 1), ("c", 1), ("d", 2)]
    assert [(n.label, h) for n, h in index.neighbors(ids["a"], max_depth=5)] == [
        ("b", 1), ("c", 1), ("d", 2), ("e", 3)]
    assert ("w", 1) in [(n.label, h) for n, h in index.neighbors(ids["a"], min_weight=0.1)]
    assert index.neighbors(ids["a"], max_depth=0) == []
    assert index.neighbors("column:unknown") == []
    assert index.neighbors(ids["a"], kinds=["table"]) == []


def test_neighbors_skip_removed_nodes(tmp_path):
    store = SqliteKnowledgeStore(str(tmp_path / "k.db"))
    a, b, c = (_node(f"x.y.t.{n}") for n in "abc")
    store.upsert_nodes([a, b, c])
    store.upsert_edges([Edge.build(kind="derives", from_id=a.node_id, to_id=b.node_id, source="pipeline"),
                        Edge.build(kind="derives", from_id=b.node_id, to_id=c.node_id, source="pipeline")])
    store.mark_removed([b.node_id])
    index = KnowledgeIndex.from_store(store)
    assert index.neighbors(a.node_id) == []
    assert [h.node.node_id for h in index.search("b")] == []
