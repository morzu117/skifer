from __future__ import annotations

import argparse
from datetime import datetime, timezone

from skifer.cli import (
    INDEX_EXIT_ERROR,
    INDEX_EXIT_OK,
    INDEX_EXIT_USAGE,
    run_index_command,
)
from skifer.core.schema_loader import parse_schema
from skifer.observability.metadata_index import index_from_path, index_schema
from skifer.observability.metadata_store import SqliteMetadataStore


def _schema(yaml_text: str) -> dict:
    return parse_schema(yaml_text)


BASE_YAML = """
data_product:
  id: sales.orders
  version: 1.0.0
  owner: data-platform
contract:
  grain: [order_id]
  output:
    order_id:
      logical_type: identifier
      classification: internal
      description: Order identifier
    net_amount:
      logical_type: currency
      classification: confidential
      description: Net amount
tables:
  - name: silver.orders
    alias: ord
select_final:
  - [id, order_id]
  - [amount, net_amount, [cast:double]]
sink:
  type: delta
  schema: gold
  table: fact_orders
"""


def test_index_schema_is_pure_and_deterministic():
    schema = _schema(BASE_YAML)
    now = datetime(2026, 9, 11, 12, 0, tzinfo=timezone.utc)

    first = index_schema(schema, "schemas/orders.yaml", now=now)
    second = index_schema(schema, "schemas/orders.yaml", now=now)

    assert first == second
    assert first.definition_hash == second.definition_hash
    assert first.columns == second.columns
    assert first.lineage == second.lineage


def test_index_schema_resolves_fqn_from_sink_hint():
    record = index_schema(_schema(BASE_YAML), "schemas/orders.yaml")

    assert record.target_fqn == "gold.fact_orders"


def test_index_schema_resolves_fqn_from_data_product_id():
    yaml_text = """
data_product: {id: sales.orders, version: 1.0.0}
tables: [{name: silver.orders, alias: ord}]
select_final:
  - [id, order_id]
"""

    record = index_schema(_schema(yaml_text), "schemas/orders.yaml")

    assert record.target_fqn == "sales.orders"


def test_index_schema_resolves_fqn_from_explicit_override():
    record = index_schema(
        _schema(BASE_YAML),
        "schemas/orders.yaml",
        target_fqn="main.gold.fact_orders",
    )

    assert record.target_fqn == "main.gold.fact_orders"


def test_index_schema_carries_classification_from_contract():
    record = index_schema(_schema(BASE_YAML), "schemas/orders.yaml")

    columns = {column.name: column for column in record.columns}
    assert columns["order_id"].classification == "internal"
    assert columns["order_id"].description == "Order identifier"
    assert columns["net_amount"].classification == "confidential"


def test_index_schema_columns_sources_from_projection():
    record = index_schema(_schema(BASE_YAML), "schemas/orders.yaml")

    columns = {column.name: column for column in record.columns}
    assert columns["order_id"].sources == ("id",)
    assert columns["net_amount"].sources == ("amount",)


def test_index_schema_lineage_graph_populated():
    record = index_schema(_schema(BASE_YAML), "schemas/orders.yaml")

    assert record.lineage["edges"]
    assert record.lineage["summary"]["total_edges"] == 2


def test_index_from_path_upsert_and_noop(tmp_path):
    path = tmp_path / "orders.yaml"
    path.write_text(BASE_YAML, encoding="utf-8")
    store = SqliteMetadataStore(":memory:")

    assert index_from_path(str(path), store) is True
    assert index_from_path(str(path), store) is False


def test_run_index_command_exit_codes(tmp_path, capsys):
    path = tmp_path / "orders.yaml"
    path.write_text(BASE_YAML, encoding="utf-8")
    store = SqliteMetadataStore(":memory:")

    ok = run_index_command(
        argparse.Namespace(paths=[str(path)], db="unused.db", target_fqn=None),
        store=store,
    )
    missing = run_index_command(
        argparse.Namespace(paths=[str(tmp_path / "missing.yaml")], db="unused.db", target_fqn=None),
        store=store,
    )
    usage = run_index_command(
        argparse.Namespace(paths=[str(path), str(path)], db="unused.db", target_fqn="x.y"),
        store=store,
    )

    assert ok == INDEX_EXIT_OK
    assert missing == INDEX_EXIT_ERROR
    assert usage == INDEX_EXIT_USAGE
    captured = capsys.readouterr()
    assert "updated" in captured.out
    assert "--target-fqn" in captured.err


def test_index_schema_with_business_rule_uses_rule_analyzer_lineage():
    from skifer.core.registry import RuleRegistry

    @RuleRegistry.register_rule(name="metadata_index_rule")
    def metadata_index_rule(df):
        return df.withColumn("rule_amount", df["amount"])

    yaml_text = """
data_product: {id: sales.orders, version: 1.0.0}
tables: [{name: silver.orders, alias: ord}]
business_rules: [metadata_index_rule]
select_final:
  - [rule_amount, normalized_amount, [cast:double]]
"""
    try:
        record = index_schema(_schema(yaml_text), "schemas/orders.yaml")
    finally:
        RuleRegistry._rules.pop("metadata_index_rule", None)

    rule_edges = [
        edge
        for edge in record.lineage["edges"]
        if edge["edge_type"] == "rule"
    ]
    assert [column.name for column in record.columns] == ["normalized_amount"]
    assert rule_edges
    assert rule_edges[0]["source_column"] == "amount"
    assert rule_edges[0]["target_column"] == "rule_amount"
