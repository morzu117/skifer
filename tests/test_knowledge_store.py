"""Plan 44 S02: knowledge graph stores (SQLite, Delta over the fake backend, real Spark)."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from skifer.knowledge.models import Edge, Node
from skifer.knowledge.store import (
    EDGES_TABLE,
    NODES_TABLE,
    DeltaKnowledgeStore,
    KnowledgeStore,
    SqliteKnowledgeStore,
    default_knowledge_store,
)
from tests.fakes.fake_backend import FakeBackend

SCHEMA = "prod_cat._skifer_knowledge"


def _node(fqn="main.sales.orders", kind="table", **overrides) -> Node:
    values = dict(kind=kind, fqn=fqn, label=fqn.rsplit(".", 1)[-1], text="Orders",
                  synonyms=("commandes",), status="reference", domain="main.sales",
                  source="pipeline")
    values.update(overrides)
    return Node.build(**values)


def _edge(**overrides) -> Edge:
    values = dict(kind="joins", from_id="table:main.sales.orders",
                  to_id="table:main.sales.customers", keys=("customer_id", "id"),
                  source="pipeline")
    values.update(overrides)
    return Edge.build(**values)


@pytest.fixture(params=["sqlite", "delta"])
def store(request, tmp_path):
    if request.param == "sqlite":
        sqlite = SqliteKnowledgeStore(str(tmp_path / "knowledge.db"))
        yield sqlite
        sqlite.close()
    else:
        yield DeltaKnowledgeStore(FakeBackend(), schema=SCHEMA)


def test_stores_satisfy_the_protocol(store):
    assert isinstance(store, KnowledgeStore)


def test_upsert_counts_added_updated_unchanged(store):
    orders, customers = _node(), _node("main.sales.customers")
    assert store.upsert_nodes([orders, customers]) == (2, 0, 0)
    assert store.upsert_nodes([orders, customers]) == (0, 0, 2)
    changed = _node(text="All orders")
    assert store.upsert_nodes([changed, customers]) == (0, 1, 1)
    assert store.list_nodes() == [customers, changed]
    edge = _edge()
    assert store.upsert_edges([edge]) == (1, 0, 0)
    assert store.upsert_edges([edge]) == (0, 0, 1)
    assert store.upsert_edges([_edge(weight=0.5)]) == (0, 1, 0)


def test_soft_delete_keeps_the_row_and_revival_counts_as_update(store):
    orders, customers = _node(), _node("main.sales.customers")
    store.upsert_nodes([orders, customers])
    assert store.mark_removed([orders.node_id, "table:unknown"]) == 1
    assert store.mark_removed([orders.node_id]) == 0
    assert store.list_nodes() == [customers]
    assert store.list_nodes(include_removed=True) == [customers, orders]
    assert store.upsert_nodes([orders]) == (0, 1, 0)
    assert store.list_nodes() == [customers, orders]


def test_list_filters(store):
    table, column = _node(), _node("main.sales.orders.amount", kind="column")
    other = _node("main.hr.people", domain="main.hr")
    store.upsert_nodes([table, column, other])
    assert store.list_nodes(kinds=["column"]) == [column]
    assert store.list_nodes(domain="main.hr") == [other]
    with pytest.raises(ValueError, match="Unknown kinds"):
        store.list_nodes(kinds=["view"])
    joins = _edge()
    has = _edge(kind="has_column", to_id=column.node_id, keys=())
    store.upsert_edges([joins, has])
    assert store.list_edges(node_id=column.node_id) == [has]
    assert store.list_edges(kinds=["joins"]) == [joins]
    assert store.mark_edges_removed([joins.edge_id]) == 1
    assert store.list_edges() == [has]


def test_round_trip_preserves_every_field(store):
    node = _node(text="Montant « TTC »", synonyms=("chiffre d'affaires", "CA"), domain=None,
                 status="certified", source="semantic", kind="metric")
    edge = _edge(kind="shortcut", weight=0.25, source="semantic")
    store.upsert_nodes([node])
    store.upsert_edges([edge])
    assert store.list_nodes() == [node]
    assert store.list_edges() == [edge]


def test_unchanged_upsert_sends_no_write_to_the_backend():
    backend = FakeBackend()
    store = DeltaKnowledgeStore(backend, schema=SCHEMA)
    store.upsert_nodes([_node()])
    store.upsert_edges([_edge()])
    writes = list(backend._knowledge_writes)
    assert writes == [(NODES_TABLE, ["table:main.sales.orders"]), (EDGES_TABLE, [_edge().edge_id])]
    store.upsert_nodes([_node()])
    store.upsert_edges([_edge()])
    store.mark_removed(["table:absent"])
    assert backend._knowledge_writes == writes


def test_sqlite_store_unreachable_names_the_store_and_database(tmp_path):
    missing = tmp_path / "no" / "such" / "dir" / "k.db"
    with pytest.raises(RuntimeError, match="SqliteKnowledgeStore.*k.db"):
        SqliteKnowledgeStore(str(missing))


def test_default_knowledge_store_selection(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    local = default_knowledge_store(SimpleNamespace(is_local=True, db="dev_cat"))
    assert isinstance(local, SqliteKnowledgeStore)
    assert (tmp_path / ".skifer_knowledge.db").exists()
    assert isinstance(default_knowledge_store(SimpleNamespace(is_local=False, db=None)),
                      SqliteKnowledgeStore)
    backend = object()
    delta = default_knowledge_store(SimpleNamespace(is_local=False, db="prod_cat", backend=backend))
    assert isinstance(delta, DeltaKnowledgeStore)
    assert delta.backend is backend
    assert delta.schema == "prod_cat._skifer_knowledge"


def test_delta_merge_only_rewrites_changed_rows():
    from unittest.mock import MagicMock

    from skifer.core.spark_backend import SparkBackend

    spark = MagicMock()
    SparkBackend(spark=spark).upsert_knowledge_rows(
        "cat.sch", NODES_TABLE, [{"node_id": "x"}], "node_id"
    )
    statements = [call.args[0] for call in spark.sql.call_args_list]
    assert "delta.enableChangeDataFeed = true" in statements[1]
    assert "DEFAULT" not in statements[1]
    merge = statements[-1]
    assert "ON t.`node_id` = s.`node_id`" in merge
    assert "WHEN MATCHED AND (t.fingerprint <> s.fingerprint OR t.removed <> s.removed)" in merge
    with pytest.raises(ValueError, match="Unknown knowledge table"):
        SparkBackend(spark=spark).upsert_knowledge_rows("cat.sch", "x; DROP", [{}], "id")


def test_delta_round_trip_on_local_spark(spark):
    from skifer.core.spark_backend import SparkBackend

    schema = "plan44_knowledge_store_test"
    spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    try:
        store = DeltaKnowledgeStore(SparkBackend(spark=spark), schema=schema)
        assert store.list_nodes() == []
        node = _node(synonyms=("chiffre d'affaires",), domain=None)
        assert store.upsert_nodes([node, _node("main.sales.customers")]) == (2, 0, 0)
        assert store.upsert_nodes([node]) == (0, 0, 1)
        assert store.mark_removed([node.node_id]) == 1
        assert store.list_nodes() == [_node("main.sales.customers")]
        assert store.list_nodes(include_removed=True)[1] == node
        edge = _edge(weight=0.5)
        store.upsert_edges([edge])
        store.upsert_edges([edge])
        assert store.list_edges() == [edge]
        assert spark.sql(f"SELECT count(*) AS n FROM {schema}.{EDGES_TABLE}").first().n == 1
    finally:
        spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
