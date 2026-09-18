import inspect

import pytest

from skifer.core.adapters import Adapter
from skifer.core.adapters.duckdb import DuckDBAdapter, DuckDBAdapterError
from skifer.core.capabilities_matrix import (
    CAP_FILE_SOURCES,
    CAP_STREAMING,
    UnsupportedCapabilityError,
    assert_supported,
)
from skifer.core.ir import parse_to_ir
from skifer.core.spark_backend import SparkBackend


class FakeAdapter:
    name = "limited"
    capabilities = frozenset()


def test_spark_backend_satisfies_adapter_protocol_and_signatures():
    backend = SparkBackend(spark=None, is_local=True)
    assert isinstance(backend, Adapter)

    protocol_members = {
        name: member
        for name, member in inspect.getmembers(Adapter)
        if not name.startswith("_")
    }
    for name, protocol_member in protocol_members.items():
        assert hasattr(SparkBackend, name), name
        backend_member = inspect.getattr_static(SparkBackend, name)
        if isinstance(protocol_member, property):
            assert isinstance(backend_member, property), name
            assert inspect.signature(backend_member.fget) == inspect.signature(
                protocol_member.fget
            )
        else:
            assert inspect.signature(backend_member) == inspect.signature(
                protocol_member
            )


def test_duckdb_adapter_satisfies_adapter_protocol_and_signatures():
    duckdb = pytest.importorskip("duckdb")
    connection = duckdb.connect()
    try:
        adapter = DuckDBAdapter(connection=connection)
        assert isinstance(adapter, Adapter)

        protocol_members = {
            name: member
            for name, member in inspect.getmembers(Adapter)
            if not name.startswith("_")
        }
        for name, protocol_member in protocol_members.items():
            assert hasattr(DuckDBAdapter, name), name
            adapter_member = inspect.getattr_static(DuckDBAdapter, name)
            if isinstance(protocol_member, property):
                assert isinstance(adapter_member, property), name
                assert inspect.signature(adapter_member.fget) == inspect.signature(
                    protocol_member.fget
                )
            else:
                assert inspect.signature(adapter_member) == inspect.signature(
                    protocol_member
                )
    finally:
        connection.close()


def test_duckdb_fetch_returns_named_rows_and_fqn_rejects_catalog():
    duckdb = pytest.importorskip("duckdb")
    connection = duckdb.connect()
    try:
        adapter = DuckDBAdapter(connection=connection)
        assert adapter.fetch("SELECT 1 AS id, 'ready' AS status") == [
            {"id": 1, "status": "ready"}
        ]
        assert adapter.build_fqn(None, "silver", "orders") == "silver.orders"
        with pytest.raises(ValueError, match="duckdb.*catalog.*three_part"):
            adapter.build_fqn("three_part", "silver", "orders")
    finally:
        connection.close()


def _source_table(source_type, path, options=None):
    return parse_to_ir(
        {
            "tables": [
                {
                    "name": "file_rows",
                    "source": {
                        "type": source_type,
                        "path": str(path),
                        "options": options or {},
                    },
                }
            ]
        }
    ).tables[0]


def test_duckdb_declares_file_source_capability():
    duckdb = pytest.importorskip("duckdb")
    connection = duckdb.connect()
    try:
        assert CAP_FILE_SOURCES in DuckDBAdapter(connection).capabilities
    finally:
        connection.close()


def test_duckdb_reads_csv_values_with_header_and_nonstandard_separator(tmp_path):
    duckdb = pytest.importorskip("duckdb")
    path = tmp_path / "orders.csv"
    path.write_text("id;label\n1;alpha\n2;beta\n", encoding="utf-8")
    connection = duckdb.connect()
    adapter = DuckDBAdapter(connection)
    try:
        relation = adapter.resolve_source(
            _source_table(
                "csv",
                path,
                {"header": "true", "inferSchema": "true", "sep": ";"},
            )
        )
        assert adapter.fetch(f"SELECT * FROM {relation} ORDER BY id") == [
            {"id": 1, "label": "alpha"},
            {"id": 2, "label": "beta"},
        ]
    finally:
        connection.close()


def test_duckdb_emits_spark_file_option_defaults(tmp_path):
    duckdb = pytest.importorskip("duckdb")
    csv_path = tmp_path / "rows.csv"
    csv_path.write_text("1,alpha\n", encoding="utf-8")
    connection = duckdb.connect()
    adapter = DuckDBAdapter(connection)
    try:
        csv_relation = adapter.resolve_source(_source_table("csv", csv_path))
        assert "header = FALSE" in csv_relation
        assert "all_varchar = TRUE" in csv_relation
        assert "delim = ','" in csv_relation
        assert "names = ['_c0', '_c1']" in csv_relation

        parquet_relation = adapter.resolve_source(
            _source_table("parquet", tmp_path / "rows.parquet")
        )
        assert "union_by_name = FALSE" in parquet_relation

        json_relation = adapter.resolve_source(
            _source_table("json", tmp_path / "rows.json")
        )
        assert "format = 'newline_delimited'" in json_relation
    finally:
        connection.close()


def test_duckdb_reads_parquet_values(tmp_path):
    duckdb = pytest.importorskip("duckdb")
    path = tmp_path / "orders.parquet"
    connection = duckdb.connect()
    adapter = DuckDBAdapter(connection)
    try:
        connection.execute(
            "COPY (SELECT * FROM VALUES (1, 'alpha'), (2, 'beta') "
            "AS rows(id, label)) TO ? (FORMAT PARQUET)",
            [str(path)],
        )
        relation = adapter.resolve_source(_source_table("parquet", path))
        assert adapter.fetch(f"SELECT * FROM {relation} ORDER BY id") == [
            {"id": 1, "label": "alpha"},
            {"id": 2, "label": "beta"},
        ]
    finally:
        connection.close()


def test_duckdb_reads_json_values(tmp_path):
    duckdb = pytest.importorskip("duckdb")
    path = tmp_path / "orders.json"
    path.write_text('{"id": 1, "label": "alpha"}\n{"id": 2, "label": "beta"}\n')
    connection = duckdb.connect()
    adapter = DuckDBAdapter(connection)
    try:
        relation = adapter.resolve_source(
            _source_table("json", path, {"multiLine": "false"})
        )
        assert adapter.fetch(f"SELECT * FROM {relation} ORDER BY id") == [
            {"id": 1, "label": "alpha"},
            {"id": 2, "label": "beta"},
        ]
    finally:
        connection.close()


def test_duckdb_multiline_json_fails_loudly_where_spark_would_drop_records(tmp_path):
    """Several JSON objects concatenated in one file must raise, never diverge.

    Measured against Spark on this exact file: with ``multiLine: true`` Spark parses
    a single JSON value and returns **one** row, silently dropping the second object.
    DuckDB's ``format = 'auto'`` returns **two**. Rather than let the same YAML yield
    a different row count with no error, the adapter emits ``format = 'array'``, which
    is exact for a JSON array and raises for every other shape.
    """
    duckdb = pytest.importorskip("duckdb")
    path = tmp_path / "concatenated.json"
    path.write_text('{"id": 1, "label": "alpha"}\n{"id": 2, "label": "beta"}\n')
    connection = duckdb.connect()
    adapter = DuckDBAdapter(connection)
    try:
        relation = adapter.resolve_source(
            _source_table("json", path, {"multiLine": "true"})
        )
        assert "format = 'array'" in relation
        with pytest.raises(duckdb.Error):
            adapter.fetch(f"SELECT * FROM {relation}")
    finally:
        connection.close()


def test_duckdb_reads_csv_whose_path_contains_an_apostrophe(tmp_path):
    duckdb = pytest.importorskip("duckdb")
    path = tmp_path / "owner's orders.csv"
    path.write_text("id,label\n7,safe\n", encoding="utf-8")
    connection = duckdb.connect()
    adapter = DuckDBAdapter(connection)
    try:
        relation = adapter.resolve_source(
            _source_table("csv", path, {"header": "true"})
        )
        assert "''" in relation
        assert adapter.fetch(f"SELECT * FROM {relation}") == [
            {"id": "7", "label": "safe"}
        ]
    finally:
        connection.close()


@pytest.mark.parametrize("source_type", ["delta", "avro", "orc", "text"])
def test_duckdb_refuses_unsupported_file_format_by_adapter_and_name(
    tmp_path, source_type
):
    duckdb = pytest.importorskip("duckdb")
    connection = duckdb.connect()
    try:
        adapter = DuckDBAdapter(connection)
        with pytest.raises(DuckDBAdapterError) as exc_info:
            adapter.resolve_source(_source_table(source_type, tmp_path / "data"))
        assert "duckdb" in str(exc_info.value)
        assert source_type in str(exc_info.value)
    finally:
        connection.close()


def test_duckdb_refuses_unknown_source_option_by_name(tmp_path):
    duckdb = pytest.importorskip("duckdb")
    connection = duckdb.connect()
    try:
        adapter = DuckDBAdapter(connection)
        with pytest.raises(DuckDBAdapterError, match="multiLine"):
            adapter.resolve_source(
                _source_table("csv", tmp_path / "data.csv", {"multiLine": "true"})
            )
    finally:
        connection.close()


def test_reduced_fake_adapter_is_refused_by_name_and_capability():
    parsed = parse_to_ir({"tables": [{"name": "orders", "streaming": True}]})
    adapter = FakeAdapter()

    with pytest.raises(UnsupportedCapabilityError) as exc_info:
        assert_supported(
            parsed,
            adapter_name=adapter.name,
            supported=adapter.capabilities,
        )

    message = str(exc_info.value)
    assert adapter.name in message
    assert CAP_STREAMING in message
