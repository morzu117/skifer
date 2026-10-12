from __future__ import annotations

import argparse
from datetime import datetime, timezone

from skifer.cli import (
    INDEX_EXIT_CLASSIFICATION,
    INDEX_EXIT_ERROR,
    INDEX_EXIT_OK,
    INDEX_EXIT_USAGE,
    run_index_command,
)
from skifer.core.schema_loader import parse_schema
from skifer.observability.certification import ContractDefinition
from skifer.observability.metadata_index import (
    dataset_record_from_definition,
    index_from_path,
    index_schema,
)
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


def test_dataset_record_from_definition_uses_contract_metadata():
    definition = ContractDefinition(
        contract_id="sales.orders",
        contract_version="1.2.0",
        definition_hash="definition-hash",
        canonical_json='{"contract":{"output":[{"classification":"pii",'
        '"description":"Customer email","logical_type":"string",'
        '"name":"email"}]}}',
        data_product_id="sales.orders",
        owner="data-platform",
    )

    record = dataset_record_from_definition(definition, "gold.orders", "run-1")

    assert record.target_fqn == "gold.orders"
    assert record.pipeline_path == "sales.orders"
    assert record.data_product_id == "sales.orders"
    assert record.contract_version == "1.2.0"
    assert record.definition_hash == "definition-hash"
    assert record.owner == "data-platform"
    assert record.last_run_id == "run-1"
    assert record.lineage == {}
    assert record.columns[0].classification == "pii"
    assert record.columns[0].description == "Customer email"


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


def test_run_index_command_strict_violation_writes_nothing_for_path(tmp_path, capsys):
    from skifer.observability.metadata_store import ColumnRecord, DatasetRecord

    path = tmp_path / "orders.yaml"
    path.write_text(
        """
data_product: {id: sales.strict_orders, version: 1.0.0}
contract:
  output:
    email_hash: {logical_type: string}
tables: [{name: silver.orders, alias: ord}]
select_final:
  - [email, email_hash, [upper]]
""",
        encoding="utf-8",
    )
    store = SqliteMetadataStore(":memory:")
    store.upsert(DatasetRecord(
        target_fqn="silver.orders",
        pipeline_path="upstream.yaml",
        data_product_id="sales.raw_orders",
        contract_version="1.0.0",
        definition_hash="upstream-hash",
        owner=None,
        columns=(ColumnRecord("email", classification="pii"),),
        indexed_at=datetime(2026, 9, 11, 12, 0, tzinfo=timezone.utc),
    ))

    result = run_index_command(
        argparse.Namespace(
            paths=[str(path)],
            db="unused.db",
            target_fqn=None,
            strict=True,
        ),
        store=store,
    )

    assert result == INDEX_EXIT_CLASSIFICATION
    assert store.get("sales.strict_orders") is None
    assert len(store.list_all()) == 1
    captured = capsys.readouterr()
    assert f"[index] Classification violation in '{path}':" in captured.err
    assert "email_hash" in captured.err


def test_run_index_command_strict_catches_pii_crossing_a_renaming_rule(tmp_path, capsys):
    """Same violation as above, but the derivation goes through a business rule.

    The rule reads `email` and writes `email_masked`; `select_final` publishes it
    as `hashed_contact`. Until the rule edge carried the published name, this
    pipeline indexed clean: the edge pointed at `email_masked`, which is not a
    column of the target, so the `pii` never reached `hashed_contact` and strict
    mode had nothing to object to.
    """
    from skifer.core.registry import RuleRegistry
    from skifer.observability.metadata_store import ColumnRecord, DatasetRecord

    @RuleRegistry.register_rule(name="strict_index_mask_email")
    def strict_index_mask_email(df):
        return df.withColumn("email_masked", df["email"])

    path = tmp_path / "report.yaml"
    path.write_text(
        """
data_product: {id: sales.masked_report, version: 1.0.0}
tables: [{name: silver.contacts, alias: c}]
business_rules: [strict_index_mask_email]
select_final:
  - [email_masked, hashed_contact]
""",
        encoding="utf-8",
    )
    store = SqliteMetadataStore(":memory:")
    store.upsert(DatasetRecord(
        target_fqn="silver.contacts",
        pipeline_path="upstream.yaml",
        data_product_id="sales.contacts",
        contract_version="1.0.0",
        definition_hash="upstream-hash",
        owner=None,
        columns=(ColumnRecord("email", classification="pii"),),
        indexed_at=datetime(2026, 9, 11, 12, 0, tzinfo=timezone.utc),
    ))

    try:
        result = run_index_command(
            argparse.Namespace(
                paths=[str(path)], db="unused.db", target_fqn=None, strict=True
            ),
            store=store,
        )
    finally:
        RuleRegistry._rules.pop("strict_index_mask_email", None)

    assert result == INDEX_EXIT_CLASSIFICATION
    assert store.get("sales.masked_report") is None
    captured = capsys.readouterr()
    assert "hashed_contact" in captured.err


def test_run_index_command_rules_flag_makes_rule_lineage_appear(tmp_path, monkeypatch, capsys):
    """Without `--rules`, a rule is unresolvable and its columns get no provenance.

    The stored lineage is built by RuleAnalyzer, which can only read a rule the
    process imported. Project rules live in the project, so nothing imports them
    on this path unless asked — and an unresolved rule is skipped silently, so
    the truncated lineage looks exactly like a pipeline that has no rules.
    """
    from skifer.core.registry import RuleRegistry

    (tmp_path / "index_rules_mod.py").write_text(
        "from skifer.core.registry import RuleRegistry\n"
        "@RuleRegistry.register_rule(name='index_flag_rule')\n"
        "def index_flag_rule(df):\n"
        "    return df.withColumn('band', df['amount'])\n",
        encoding="utf-8",
    )
    path = tmp_path / "orders.yaml"
    path.write_text(
        """
data_product: {id: sales.banded, version: 1.0.0}
tables: [{name: silver.orders, alias: ord}]
business_rules: [index_flag_rule]
select_final:
  - [band, amount_band]
""",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    def _index(rules):
        store = SqliteMetadataStore(":memory:")
        assert run_index_command(
            argparse.Namespace(
                paths=[str(path)], db="unused.db", target_fqn=None,
                strict=False, rules=rules,
            ),
            store=store,
        ) == INDEX_EXIT_OK
        record = store.get("sales.banded")
        return [e for e in record.lineage["edges"] if e["edge_type"] == "rule"]

    try:
        assert _index([]) == []
        assert [
            (e["source_column"], e["target_column"]) for e in _index(["index_rules_mod"])
        ] == [("amount", "amount_band")]
    finally:
        RuleRegistry._rules.pop("index_flag_rule", None)


def test_run_index_command_rejects_an_unimportable_rules_module(tmp_path, capsys):
    path = tmp_path / "orders.yaml"
    path.write_text(
        "data_product: {id: sales.plain, version: 1.0.0}\n"
        "tables: [{name: silver.orders, alias: ord}]\n"
        "select_final: [[amount, amount]]\n",
        encoding="utf-8",
    )
    store = SqliteMetadataStore(":memory:")

    result = run_index_command(
        argparse.Namespace(
            paths=[str(path)], db="unused.db", target_fqn=None,
            strict=False, rules=["no_such_rules_module"],
        ),
        store=store,
    )

    # Blaming the pipeline for a rule the caller failed to import would send the
    # reader to the wrong file.
    assert result == INDEX_EXIT_USAGE
    assert store.list_all() == []
    assert "no_such_rules_module" in capsys.readouterr().err


def test_run_index_command_strict_compliant_pipeline_writes_record(tmp_path, capsys):
    path = tmp_path / "orders.yaml"
    path.write_text(
        """
data_product: {id: sales.strict_orders, version: 1.0.0}
contract:
  output:
    email_hash: {logical_type: string, classification: pii}
tables: [{name: silver.orders, alias: ord}]
select_final:
  - [email, email_hash, [upper]]
""",
        encoding="utf-8",
    )
    store = SqliteMetadataStore(":memory:")

    result = run_index_command(
        argparse.Namespace(
            paths=[str(path)],
            db="unused.db",
            target_fqn=None,
            strict=True,
        ),
        store=store,
    )

    assert result == INDEX_EXIT_OK
    assert store.get("sales.strict_orders") is not None
    assert "updated" in capsys.readouterr().out


def test_run_index_command_strict_malformed_yaml_is_index_error(tmp_path, capsys):
    path = tmp_path / "broken.yaml"
    path.write_text("tables: [", encoding="utf-8")

    result = run_index_command(
        argparse.Namespace(
            paths=[str(path)],
            db="unused.db",
            target_fqn=None,
            strict=True,
        ),
        store=SqliteMetadataStore(":memory:"),
    )

    assert result == INDEX_EXIT_ERROR
    captured = capsys.readouterr()
    assert f"[index] Failed to index '{path}':" in captured.err
    assert "Classification violation" not in captured.err


def test_run_index_command_strict_non_classification_value_error_is_index_error(
    tmp_path, capsys,
):
    from unittest.mock import patch

    path = tmp_path / "orders.yaml"
    path.write_text(BASE_YAML, encoding="utf-8")

    with patch(
        "skifer.observability.metadata_index.index_from_path",
        side_effect=ValueError("invalid dataset record"),
    ):
        result = run_index_command(
            argparse.Namespace(
                paths=[str(path)],
                db="unused.db",
                target_fqn=None,
                strict=True,
            ),
            store=SqliteMetadataStore(":memory:"),
        )

    assert result == INDEX_EXIT_ERROR
    captured = capsys.readouterr()
    assert f"[index] Failed to index '{path}': invalid dataset record" in captured.err
    assert "Classification violation" not in captured.err


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
    # `rule_amount` is the rule's internal name; `select_final` publishes it as
    # `normalized_amount`. The edge must carry the published name: pointing at
    # the internal one names a column this record does not have, and leaves the
    # one it does have with no provenance — which is how a `pii` source used to
    # cross a renaming rule without `--strict` ever objecting.
    assert rule_edges[0]["target_column"] == "normalized_amount"
