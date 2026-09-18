from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
import inspect
from uuid import uuid4

import pytest

from skifer.core.adapters.duckdb import DuckDBAdapter
from skifer.core.spark_backend import SparkBackend
from skifer.observability.metadata_store import (
    ColumnRecord,
    DatasetRecord,
    DeltaMetadataStore,
    MetadataStore,
    SqlMetadataStore,
    SqliteMetadataStore,
)


def _record(
    *,
    definition_hash: str = "sha256:definition-a",
    indexed_at: datetime | None = None,
    last_run_id: str | None = "run-1",
    columns: tuple[ColumnRecord, ...] | None = None,
) -> DatasetRecord:
    return DatasetRecord(
        target_fqn="silver.orders",
        pipeline_path="schemas/orders.yaml",
        data_product_id="orders",
        contract_version="1.0.0",
        definition_hash=definition_hash,
        owner="data-platform",
        columns=columns
        or (
            ColumnRecord(
                name="order_id",
                logical_type="string",
                classification=None,
                description="Order identifier",
                sources=("raw_order_id",),
            ),
            ColumnRecord(
                name="amount",
                logical_type="currency",
                classification="internal",
                description="Net order amount",
                sources=("gross_amount", "discount_amount"),
            ),
            ColumnRecord(
                name="customer_email",
                logical_type="string",
                classification="pii",
                description="Customer email",
                sources=("email",),
            ),
        ),
        indexed_at=indexed_at or datetime(2026, 9, 11, 8, 0, tzinfo=timezone.utc),
        last_run_id=last_run_id,
        lineage={
            "edges": [
                {
                    "source_table": "bronze.orders",
                    "source_column": "gross_amount",
                    "target_table": "silver.orders",
                    "target_column": "amount",
                    "transformations": ["cast:decimal"],
                    "edge_type": "select",
                }
            ],
            "tables": ["bronze.orders", "silver.orders"],
            "summary": {"edge_count": 1},
        },
    )


def _row_count(store: SqliteMetadataStore) -> int:
    return store._conn.execute("SELECT COUNT(*) FROM metadata_registry").fetchone()[0]


def _raw_row(store: SqliteMetadataStore) -> tuple[str, str, str]:
    return store._conn.execute(
        "SELECT content_hash, record, indexed_at FROM metadata_registry"
    ).fetchone()


@pytest.fixture(params=("spark", "duckdb"))
def portable_metadata_store(request):
    schema = f"metadata_store_{uuid4().hex}"
    table_fqn = f"{schema}.datasets"
    if request.param == "spark":
        spark = request.getfixturevalue("spark")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{schema}`")
        try:
            yield DeltaMetadataStore(
                SparkBackend(spark=spark, is_local=True),
                table_fqn=table_fqn,
            )
        finally:
            spark.sql(f"DROP SCHEMA IF EXISTS `{schema}` CASCADE")
        return

    duckdb = pytest.importorskip("duckdb")
    connection = duckdb.connect()
    try:
        yield SqlMetadataStore(DuckDBAdapter(connection), table_fqn=table_fqn)
    finally:
        connection.close()


def test_sql_metadata_store_has_exactly_the_delta_store_method_signatures():
    public_methods = {
        name
        for name, member in inspect.getmembers(SqlMetadataStore, inspect.isfunction)
        if not name.startswith("_")
    }

    assert public_methods == {
        name
        for name, member in inspect.getmembers(DeltaMetadataStore, inspect.isfunction)
        if not name.startswith("_")
    }
    for name in public_methods:
        assert inspect.signature(getattr(SqlMetadataStore, name)) == inspect.signature(
            getattr(DeltaMetadataStore, name)
        )


def test_metadata_store_behaves_identically_on_delta_and_sql(portable_metadata_store):
    store = portable_metadata_store
    record = _record()

    assert store.get(record.target_fqn) is None
    assert store.upsert(record) is True
    assert store.get(record.target_fqn) == record

    assert store.upsert(record) is False
    assert store.get(record.target_fqn) == record

    rerun = replace(record, last_run_id="run-2")
    assert store.upsert(rerun) is False
    assert store.get(record.target_fqn) == record

    assert store.attach_run_id(record.target_fqn, record.definition_hash, "run-2") is True
    assert store.attach_run_id(record.target_fqn, record.definition_hash, "run-2") is False
    assert store.get(record.target_fqn) == replace(record, last_run_id="run-2")
    assert store.attach_run_id("missing.table", record.definition_hash, "run-3") is False

    changed = replace(
        record,
        columns=record.columns
        + (
            ColumnRecord(
                name="net_amount",
                logical_type="currency",
                classification="internal",
                description="Net amount after adjustments",
                sources=("amount",),
            ),
        ),
        indexed_at=datetime(2026, 9, 11, 9, 0, tzinfo=timezone.utc),
    )
    assert store.upsert(changed) is True
    assert store.get(record.target_fqn) == changed

    other = _record(
        definition_hash="sha256:definition-b",
        indexed_at=datetime(2026, 9, 11, 10, 0, tzinfo=timezone.utc),
    )
    assert store.upsert(other) is True
    assert store.get(record.target_fqn) == other
    assert store.list_all() == [other, changed]


def test_metadata_search_treats_percent_and_underscore_as_literals(
    portable_metadata_store,
):
    store = portable_metadata_store
    percent = ColumnRecord(name="percent_col", description="Gross% margin")
    underscore = ColumnRecord(name="underscore_col", description="Gross_ margin")
    plain = ColumnRecord(name="plain_col", description="Gross margin")
    record = _record(columns=(percent, underscore, plain))

    assert store.upsert(record) is True

    assert store.search_columns("GROSS%") == [(record.target_fqn, percent)]
    assert store.search_columns("GROSS_") == [(record.target_fqn, underscore)]


def test_sqlite_roundtrip_preserves_all_fields():
    store = SqliteMetadataStore(":memory:")
    record = _record()

    assert store.upsert(record) is True

    assert store.get(record.target_fqn) == record


def test_upsert_returns_true_on_first_insert():
    store = SqliteMetadataStore(":memory:")

    assert store.upsert(_record()) is True


def test_upsert_idempotent_returns_false_and_writes_nothing():
    store = SqliteMetadataStore(":memory:")
    record = _record()
    assert store.upsert(record) is True
    before_count = _row_count(store)
    before_row = _raw_row(store)

    assert store.upsert(record) is False

    assert _row_count(store) == before_count
    assert _raw_row(store) == before_row


def test_upsert_new_run_id_only_is_still_noop():
    store = SqliteMetadataStore(":memory:")
    record = _record(last_run_id="run-1")
    assert store.upsert(record) is True

    rerun = replace(record, last_run_id="run-2")

    assert store.upsert(rerun) is False
    assert store.get(record.target_fqn) == record


def test_attach_run_id_updates_record_without_changing_content_hash_or_indexed_at():
    store = SqliteMetadataStore(":memory:")
    record = _record(last_run_id="run-1")
    assert store.upsert(record) is True
    before_content_hash, _, before_indexed_at = _raw_row(store)

    assert store.attach_run_id(
        record.target_fqn,
        record.definition_hash,
        "run-2",
    ) is True

    after_content_hash, _, after_indexed_at = _raw_row(store)
    assert after_content_hash == before_content_hash
    assert after_indexed_at == before_indexed_at
    assert store.get(record.target_fqn).last_run_id == "run-2"


def test_upsert_changed_columns_replaces_row():
    store = SqliteMetadataStore(":memory:")
    record = _record()
    assert store.upsert(record) is True
    changed = replace(
        record,
        columns=record.columns
        + (
            ColumnRecord(
                name="net_amount",
                logical_type="currency",
                classification="internal",
                description="Net amount after adjustments",
                sources=("amount",),
            ),
        ),
        indexed_at=datetime(2026, 9, 11, 9, 0, tzinfo=timezone.utc),
    )

    assert store.upsert(changed) is True

    assert _row_count(store) == 1
    assert store.get(record.target_fqn) == changed


def test_two_versions_same_fqn_distinct_hash_coexist():
    store = SqliteMetadataStore(":memory:")
    first = _record(
        definition_hash="sha256:first",
        indexed_at=datetime(2026, 9, 11, 8, 0, tzinfo=timezone.utc),
    )
    second = _record(
        definition_hash="sha256:second",
        indexed_at=datetime(2026, 9, 11, 9, 0, tzinfo=timezone.utc),
    )

    assert store.upsert(first) is True
    assert store.upsert(second) is True

    assert _row_count(store) == 2
    assert store.get("silver.orders") == second


def test_search_columns_case_insensitive():
    store = SqliteMetadataStore(":memory:")
    record = _record()
    store.upsert(record)

    assert store.search_columns("AMOUNT") == [("silver.orders", record.columns[1])]


def test_protocol_isinstance():
    store = SqliteMetadataStore(":memory:")

    assert isinstance(store, MetadataStore)


def test_delta_store_uses_backend_spark():
    class FakeResult:
        def collect(self):
            return []

    class FakeSpark:
        def __init__(self):
            self.statements: list[str] = []

        def sql(self, statement: str):
            self.statements.append(statement)
            return FakeResult()

    class FakeBackend:
        def __init__(self):
            self.spark = FakeSpark()

    backend = FakeBackend()

    DeltaMetadataStore(backend, table_fqn="_skifer_metadata.datasets")

    assert len(backend.spark.statements) == 1
    assert "CREATE TABLE IF NOT EXISTS _skifer_metadata.datasets" in backend.spark.statements[0]
    assert not hasattr(backend, "append_certification_contract")


def test_delta_store_escapes_backslashes_in_keys_and_record_json():
    class FakeResult:
        def collect(self):
            return []

    class FakeSpark:
        def __init__(self):
            self.statements: list[str] = []

        def sql(self, statement: str):
            self.statements.append(statement)
            return FakeResult()

    class FakeBackend:
        def __init__(self):
            self.spark = FakeSpark()

    backend = FakeBackend()
    store = DeltaMetadataStore(backend)
    record = replace(_record(), target_fqn="silver.orders\\", owner="café")

    assert store.upsert(record) is True

    lookup_sql, merge_sql = backend.spark.statements[1:]
    assert "target_fqn = 'silver.orders\\\\'" in lookup_sql
    assert "'silver.orders\\\\' AS target_fqn" in merge_sql
    assert r'"owner": "caf\\u00e9"' in merge_sql
