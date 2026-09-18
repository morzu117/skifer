import inspect

import pytest

from skifer.core.adapters import Adapter
from skifer.core.adapters.duckdb import DuckDBAdapter
from skifer.core.capabilities_matrix import (
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
