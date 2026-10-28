"""Plan 44 S02: knowledge graph synchronization from pipelines, semantic models, rules and UC."""

from __future__ import annotations

import shutil
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from skifer.knowledge.store import DeltaKnowledgeStore, SqliteKnowledgeStore
from skifer.knowledge.sync import TABLE_LINEAGE, KnowledgeSynchronizer, validate_domain
from skifer.observability.metadata_index import index_from_path
from skifer.observability.metadata_store import SqliteMetadataStore
from tests.fakes.fake_backend import FakeBackend

EXAMPLES = Path(__file__).resolve().parents[1] / "examples"
PIPELINES = [
    EXAMPLES / "05_rules_join_aggregate" / "joined_orders.yaml",
    EXAMPLES / "13_semantic_projection" / "pipeline.yaml",
]


class FakeInspector:
    """Unity Catalog seen through CatalogInspector's surface, plus an optional lineage reader."""

    def __init__(self, tables=None, *, denied=(), lineage=None, lineage_error=None):
        self.tables = tables if tables is not None else {
            "main.sales": {"erp_invoices": {"invoice_id": "string", "amount_ttc": "decimal(18,2)"}},
        }
        self.denied = set(denied)
        self.lineage = lineage or []
        self.lineage_error = lineage_error

    def list_schemas(self):
        return sorted(domain.split(".")[1] for domain in self.tables)

    def list_tables(self, schema):
        domain = next(d for d in [*self.tables, *self.denied] if d.split(".")[1] == schema)
        if domain in self.denied:
            raise PermissionError("PERMISSION_DENIED on " + domain)
        return sorted(self.tables[domain])

    def describe_table(self, fqn):
        domain, table = fqn.rsplit(".", 1)
        return dict(self.tables[domain][table])

    def list_columns(self, fqn):
        return list(self.describe_table(fqn))

    def table_lineage(self, domains):
        if self.lineage_error is not None:
            raise self.lineage_error
        return self.lineage


class FakeRules:
    def __init__(self, rules):
        self.rules = rules

    def list_rules(self):
        return list(self.rules)

    def get_rule(self, name):
        return SimpleNamespace(func=self.rules[name])


def classify_order(df):
    """Classify an order as small, medium or large by amount."""
    return df


@pytest.fixture
def workspace(tmp_path):
    models = tmp_path / "semantic_models"
    shutil.copytree(EXAMPLES / "14_domain_graph" / "semantic_models", models)
    metadata = SqliteMetadataStore(str(tmp_path / "metadata.db"))
    for path in PIPELINES:
        index_from_path(str(path), metadata)
    return SimpleNamespace(models=models, metadata=metadata, tmp=tmp_path)


def _sync(workspace, store, **overrides):
    values = dict(metadata_store=workspace.metadata, catalog_inspector=FakeInspector(),
                  rule_registry=FakeRules({"classify_order": classify_order}),
                  domains=["main.sales"], models_dir=str(workspace.models))
    values.update(overrides)
    return KnowledgeSynchronizer(store, **values)


def _edit_model(workspace, name, edit):
    path = workspace.models / name
    document = yaml.safe_load(path.read_text())
    edit(document["models"][0])
    path.write_text(yaml.safe_dump(document, sort_keys=False))


def test_first_sync_reads_the_four_sources(workspace):
    store = DeltaKnowledgeStore(FakeBackend(), schema="cat._skifer_knowledge")
    report = _sync(workspace, store).sync()
    assert report.added > 0 and report.updated == report.removed == 0
    assert report.unavailable == ()
    sources = {node.source for node in store.list_nodes()}
    assert sources == {"pipeline", "semantic", "rule", "unity_catalog"}
    ids = {node.node_id for node in store.list_nodes()}
    assert {"table:gold.orders_summary", "column:gold.orders_summary.country",
            "metric:orders.revenue", "dimension:customers.segment", "rule:classify_order",
            "table:main.sales.erp_invoices", "domain:main.sales"} <= ids
    rule = next(node for node in store.list_nodes(kinds=["rule"]))
    assert rule.text == "Classify an order as small, medium or large by amount."
    shortcuts = {(e.from_id, e.to_id, e.keys) for e in store.list_edges(kinds=["shortcut"])}
    assert ("table:gold.orders", "table:gold.customers", ("customer_id", "customer_id")) in shortcuts
    assert ("table:raw_orders", "table:raw_customers", ("customer_id", "customer_id")) in shortcuts
    assert all(e.weight == 1.0 for e in store.list_edges(kinds=["shortcut"]))


def test_second_sync_writes_nothing(workspace):
    backend = FakeBackend()
    store = DeltaKnowledgeStore(backend, schema="cat._skifer_knowledge")
    first = _sync(workspace, store).sync()
    writes = list(backend._knowledge_writes)
    second = _sync(workspace, store).sync()
    assert (second.added, second.updated, second.removed) == (0, 0, 0)
    assert second.unchanged == first.added
    assert backend._knowledge_writes == writes


@pytest.mark.parametrize(
    ("edit", "node_id"),
    [
        (lambda m: m["metrics"][0].update(description="Gross revenue"), "metric:orders.revenue"),
        (lambda m: m["metrics"][0].update(synonyms=["chiffre d'affaires"]), "metric:orders.revenue"),
        (lambda m: m["dimensions"][1].update(description="Order status"), "dimension:orders.status"),
    ],
)
def test_single_change_updates_single_node(workspace, tmp_path, edit, node_id):
    store = SqliteKnowledgeStore(str(tmp_path / "k.db"))
    _sync(workspace, store).sync()
    before = {node.node_id: node.fingerprint for node in store.list_nodes()}
    _edit_model(workspace, "orders.yaml", edit)
    report = _sync(workspace, store).sync()
    assert (report.added, report.updated, report.removed) == (0, 1, 0)
    after = {node.node_id: node.fingerprint for node in store.list_nodes()}
    assert [key for key in after if after[key] != before[key]] == [node_id]


def test_single_column_description_change_updates_single_node(workspace, tmp_path):
    store = SqliteKnowledgeStore(str(tmp_path / "k.db"))
    _sync(workspace, store).sync()
    inspector = FakeInspector()
    inspector.tables["main.sales"]["erp_invoices"]["amount_ttc"] = "decimal(20,2)"
    report = _sync(workspace, store, catalog_inspector=inspector).sync()
    assert (report.added, report.updated, report.removed) == (0, 1, 0)


def test_removed_object_is_soft_deleted_and_unsearchable(workspace, tmp_path):
    store = SqliteKnowledgeStore(str(tmp_path / "k.db"))
    _sync(workspace, store).sync()
    _edit_model(workspace, "order_lines.yaml", lambda m: m.update(dimensions=[]))
    report = _sync(workspace, store).sync()
    assert report.removed == 1
    assert "dimension:order_lines.line_sku" not in {n.node_id for n in store.list_nodes()}
    removed = [n for n in store.list_nodes(include_removed=True)
               if n.node_id == "dimension:order_lines.line_sku"]
    assert len(removed) == 1, "soft delete keeps the row"


def test_unity_catalog_only_table_becomes_reference_node(workspace, tmp_path):
    store = SqliteKnowledgeStore(str(tmp_path / "k.db"))
    _sync(workspace, store).sync()
    node = next(n for n in store.list_nodes() if n.node_id == "table:main.sales.erp_invoices")
    assert (node.status, node.source, node.domain) == ("reference", "unity_catalog", "main.sales")
    column = next(n for n in store.list_nodes() if n.node_id == "column:main.sales.erp_invoices.amount_ttc")
    assert column.status == "reference" and "decimal" in column.text


def test_catalog_metric_is_certified_node(workspace, tmp_path):
    store = SqliteKnowledgeStore(str(tmp_path / "k.db"))
    _sync(workspace, store).sync()
    metric = next(n for n in store.list_nodes(kinds=["metric"]) if n.fqn == "orders.revenue")
    assert (metric.status, metric.source) == ("certified", "semantic")


def test_semantic_node_is_never_overwritten_by_unity_catalog(workspace, tmp_path):
    store = SqliteKnowledgeStore(str(tmp_path / "k.db"))
    _edit_model(workspace, "customers.yaml", lambda m: m.update(table="main.sales.erp_invoices"))
    _sync(workspace, store).sync()
    table = next(n for n in store.list_nodes() if n.node_id == "table:main.sales.erp_invoices")
    assert (table.source, table.text) == ("semantic", "Customer attributes")
    column = next(n for n in store.list_nodes() if n.node_id == "column:main.sales.erp_invoices.invoice_id")
    assert column.source == "unity_catalog", "UC still adds what the higher source did not name"


def test_unreadable_system_lineage_is_reported_not_raised(workspace, tmp_path):
    store = SqliteKnowledgeStore(str(tmp_path / "k.db"))
    inspector = FakeInspector(
        {"main.sales": {"erp_invoices": {"invoice_id": "string"}}},
        denied=["main.secret"],
        lineage_error=PermissionError("system.access.table_lineage: PERMISSION_DENIED"),
    )
    report = _sync(workspace, store, catalog_inspector=inspector,
                   domains=["main.sales", "main.secret"]).sync()
    assert report.unavailable == ("main.secret", TABLE_LINEAGE)
    assert "table:main.sales.erp_invoices" in {n.node_id for n in store.list_nodes()}


def test_unreadable_domain_removes_nothing(workspace, tmp_path):
    store = SqliteKnowledgeStore(str(tmp_path / "k.db"))
    _sync(workspace, store).sync()
    denied = FakeInspector({}, denied=["main.sales"])
    report = _sync(workspace, store, catalog_inspector=denied).sync()
    assert report.removed == 0
    assert report.unavailable == ("main.sales", )
    assert "table:main.sales.erp_invoices" in {n.node_id for n in store.list_nodes()}


def test_table_lineage_becomes_derives_edges(workspace, tmp_path):
    store = SqliteKnowledgeStore(str(tmp_path / "k.db"))
    inspector = FakeInspector(
        {"main.sales": {"a": {"x": "int"}, "b": {"x": "int"}}},
        lineage=[("main.sales.a", "main.sales.b"), ("main.other.z", "main.sales.b")],
    )
    _sync(workspace, store, catalog_inspector=inspector).sync()
    derives = {(e.from_id, e.to_id) for e in store.list_edges(kinds=["derives"])
               if e.source == "unity_catalog"}
    assert derives == {("table:main.sales.a", "table:main.sales.b")}


def test_domains_bound_unity_catalog_sync(workspace, tmp_path):
    store = SqliteKnowledgeStore(str(tmp_path / "k.db"))
    inspector = FakeInspector({"main.sales": {"a": {"x": "int"}}, "main.hr": {"people": {"n": "int"}}})
    _sync(workspace, store, catalog_inspector=inspector, domains=["main.sales"]).sync()
    ids = {n.node_id for n in store.list_nodes()}
    assert "table:main.sales.a" in ids and "table:main.hr.people" not in ids


def test_knowledge_domains_from_config_and_validation(workspace, tmp_path):
    store = SqliteKnowledgeStore(str(tmp_path / "k.db"))
    engine = SimpleNamespace(is_local=True, db=None,
                             context=SimpleNamespace(env_config=lambda: {"knowledge_domains": ["main.hr"]}))
    inspector = FakeInspector({"main.sales": {"a": {"x": "int"}}, "main.hr": {"people": {"n": "int"}}})
    _sync(workspace, store, engine=engine, catalog_inspector=inspector, domains=None).sync()
    ids = {n.node_id for n in store.list_nodes()}
    assert "table:main.hr.people" in ids and "table:main.sales.a" not in ids
    for bad in ("main", "a.b.c", "a b.c", "a.`b`"):
        with pytest.raises(ValueError, match="--schema expects CATALOG.SCHEMA"):
            validate_domain(bad)
    with pytest.raises(ValueError, match="knowledge_domains"):
        KnowledgeSynchronizer(store, domains="main.sales")


def test_dry_run_reports_without_writing(workspace):
    backend = FakeBackend()
    store = DeltaKnowledgeStore(backend, schema="cat._skifer_knowledge")
    report = _sync(workspace, store).sync(dry_run=True)
    assert report.added > 0
    assert backend._knowledge_writes == []
    assert store.list_nodes() == []


def test_unconsulted_sources_remove_nothing(workspace, tmp_path):
    store = SqliteKnowledgeStore(str(tmp_path / "k.db"))
    _sync(workspace, store).sync()
    report = KnowledgeSynchronizer(store, rule_registry=FakeRules({}),
                                   models_dir=str(tmp_path / "absent")).sync()
    assert report.removed == 0
    assert {n.source for n in store.list_nodes()} == {"pipeline", "semantic", "rule", "unity_catalog"}


def test_semantic_engine_start_is_not_touched(workspace):
    from skifer.semantic.semantic import SemanticEngine

    assert "knowledge" not in Path(__import__("inspect").getsourcefile(SemanticEngine)).read_text()
