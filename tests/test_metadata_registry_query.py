from __future__ import annotations

from datetime import datetime, timezone

import pytest
import yaml

from skifer.core.schema_loader import parse_schema
from skifer.lineage.tracker import LineageEdge, LineageGraph
from skifer.observability.metadata_index import index_schema
from skifer.observability.metadata_store import (
    ColumnRecord,
    DatasetRecord,
    MetadataRegistryQuery,
    SqliteMetadataStore,
)


def _pipeline_record(
    source_fqn: str,
    target_fqn: str,
    *,
    source_column: str = "amount",
    target_column: str = "amount",
) -> DatasetRecord:
    schema = parse_schema(
        yaml.safe_dump(
            {
                "data_product": {"id": target_fqn, "version": "1.0.0"},
                "tables": [{"name": source_fqn, "alias": "src"}],
                "select_final": [[source_column, target_column]],
            },
            sort_keys=False,
        )
    )
    return index_schema(
        schema,
        f"schemas/{target_fqn.replace('.', '_')}.yaml",
        target_fqn=target_fqn,
        now=datetime(2026, 9, 11, 12, 0, tzinfo=timezone.utc),
    )


def _record(
    target_fqn: str,
    *,
    columns: tuple[str, ...] = ("c",),
    edges: tuple[LineageEdge, ...] = (),
) -> DatasetRecord:
    graph = LineageGraph()
    for edge in edges:
        graph.add_edge(edge)
    return DatasetRecord(
        target_fqn=target_fqn,
        pipeline_path=f"schemas/{target_fqn}.yaml",
        data_product_id=target_fqn,
        contract_version="1.0.0",
        definition_hash=f"sha256:{target_fqn}",
        owner=None,
        columns=tuple(ColumnRecord(name=column) for column in columns),
        indexed_at=datetime(2026, 9, 11, 12, 0, tzinfo=timezone.utc),
        lineage=graph.to_dict() if edges else {},
    )


def _store(*records: DatasetRecord) -> SqliteMetadataStore:
    store = SqliteMetadataStore(":memory:")
    for record in records:
        store.upsert(record)
    return store


def test_three_chained_pipelines_impact():
    store = _store(
        _pipeline_record("raw.orders", "bronze.orders"),
        _pipeline_record("bronze.orders", "silver.orders"),
        _pipeline_record("silver.orders", "gold.kpi", target_column="amount_eur"),
    )

    report = MetadataRegistryQuery(store).impact("silver.orders")

    assert "gold.kpi" in report.impacted_datasets
    assert ("gold.kpi", "amount_eur") in report.impacted_columns
    assert report.truncated is False


def test_upstream_transitive():
    store = _store(
        _pipeline_record("raw.orders", "bronze.orders"),
        _pipeline_record("bronze.orders", "silver.orders"),
        _pipeline_record("silver.orders", "gold.kpi", target_column="amount_eur"),
    )

    edges = MetadataRegistryQuery(store).upstream("gold.kpi", "amount_eur")

    assert any(
        edge.source_table == "bronze.orders" and edge.target_table == "silver.orders"
        for edge in edges
    )
    assert any(
        edge.source_table == "raw.orders" and edge.target_table == "bronze.orders"
        for edge in edges
    )


def test_downstream_transitive():
    store = _store(
        _pipeline_record("raw.orders", "bronze.orders"),
        _pipeline_record("bronze.orders", "silver.orders"),
        _pipeline_record("silver.orders", "gold.kpi", target_column="amount_eur"),
    )

    edges = MetadataRegistryQuery(store).downstream("silver.orders", "amount")

    assert any(
        edge.target_table == "gold.kpi" and edge.target_column == "amount_eur"
        for edge in edges
    )


def test_cycle_is_refused():
    store = _store(
        _record(
            "a.table",
            columns=("id",),
            edges=(
                LineageEdge("b.table", "id", "a.table", "id"),
            ),
        ),
        _record(
            "b.table",
            columns=("id",),
            edges=(
                LineageEdge("a.table", "id", "b.table", "id"),
            ),
        ),
    )

    query = MetadataRegistryQuery(store)
    with pytest.raises(ValueError, match="contains a cycle"):
        query.merged_graph()
    with pytest.raises(ValueError, match="contains a cycle"):
        query.impact("a.table")


def test_bounded_depth_truncates():
    records = [_record("table_0", columns=("c",))]
    records.extend(
        _record(
            f"table_{index}",
            columns=("c",),
            edges=(
                LineageEdge(f"table_{index - 1}", "c", f"table_{index}", "c"),
            ),
        )
        for index in range(1, 5)
    )
    store = _store(*records)
    query = MetadataRegistryQuery(store, max_depth=2)

    edges = query.merged_graph().downstream_closure("table_0", "c", max_depth=2)
    report = query.impact("table_0")

    assert [(edge.target_table, edge.target_column) for edge in edges] == [
        ("table_1", "c"),
        ("table_2", "c"),
    ]
    assert ("table_2", "c") in report.impacted_columns
    assert ("table_3", "c") not in report.impacted_columns
    assert report.truncated is True


def test_empty_registry_returns_empty_impact():
    report = MetadataRegistryQuery(SqliteMetadataStore(":memory:")).impact("missing.table")

    assert report.to_dict() == {
        "root_fqn": "missing.table",
        "impacted_datasets": [],
        "impacted_columns": [],
        "edges": [],
        "truncated": False,
    }


def test_merge_dedups_shared_edges():
    shared = LineageEdge("bronze.orders", "amount", "silver.orders", "amount")
    store = _store(
        _record("silver.orders", edges=(shared,)),
        _record("audit.orders", edges=(shared,)),
    )

    graph = MetadataRegistryQuery(store).merged_graph()

    assert graph.edges == [shared]
