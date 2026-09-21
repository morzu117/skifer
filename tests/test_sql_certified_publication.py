"""Certified-publication integration coverage for the real DuckDB SQL path."""

from __future__ import annotations

from uuid import uuid4

import pytest
import yaml

from skifer.core.adapters.duckdb import DuckDBAdapter, DuckDBAdapterError
from skifer.core.core import SkiferEngine
from skifer.core.ir import parse_to_ir
from skifer.core.sql_runner import compile_sql_pipeline_relation
from skifer.observability.certification import canonicalize_contract
from skifer.observability.certification_store import DeltaCertificationStore
from skifer.observability.checks import DataQualityError
from skifer.observability.monitor import DataMonitor
from skifer.observability.publication import (
    PublicationCoordinator,
    RunState,
    stage_dataframe,
    start_publication_run,
)
from skifer.observability.sql_registry import SqlRegistryBackend


@pytest.fixture
def duckdb_runtime(tmp_path):
    duckdb = pytest.importorskip("duckdb")
    connection = duckdb.connect()
    adapter = DuckDBAdapter(connection)
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "priority_check": ["LOCAL"],
                "default_env": "LOCAL",
                "environments": {
                    "LOCAL": {
                        "catalog": None,
                        "engine": "sql",
                        "adapter": "duckdb",
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    try:
        yield connection, adapter, config_path
    finally:
        connection.close()


def _store(adapter, name):
    backend = SqlRegistryBackend(adapter)
    return DeltaCertificationStore(backend, schema=name)


def _engine(connection, adapter, config_path, *, governed=True, registry="certification"):
    return SkiferEngine(
        config_path=str(config_path),
        force_env="LOCAL",
        connection=connection,
        monitor=DataMonitor(adapter) if governed else None,
        certification_store=_store(adapter, registry) if governed else None,
    )


def _certified_join_schema(*, include_reserved=False):
    output = {
        "order_id": {"logical_type": "integer", "required": True},
        "customer_name": {"logical_type": "string", "required": False},
    }
    select_final = [
        ["order_id", "order_id"],
        ["customer_name", "customer_name"],
    ]
    if include_reserved:
        output["_violations"] = {"logical_type": "string", "required": False}
        select_final.append(["_violations", "_violations"])
    return {
        "data_product": {"id": "sales.orders", "version": "1.0.0"},
        "contract": {"output": output},
        "tables": [
            {"name": "source.orders", "alias": "orders"},
            {
                "name": "source.customers",
                "alias": "customers",
                "quality_checks": {"drop_nulls_in": ["customer_name"]},
            },
        ],
        "join": [
            {
                "table_from": ["orders", "customer_id"],
                "table_to": ["customers", "customer_id"],
                "type": "left",
            }
        ],
        "select_final": select_final,
    }


def _create_sources(connection, *, missing_customer=False, include_reserved=False):
    connection.execute("CREATE SCHEMA source")
    customer_id = 987654321 if missing_customer else 10
    reserved = ", 'source-owned' AS _violations" if include_reserved else ""
    connection.execute(
        "CREATE TABLE source.orders AS "
        f"SELECT 1 AS order_id, {customer_id} AS customer_id{reserved}"
    )
    connection.execute(
        "CREATE TABLE source.customers AS "
        "SELECT 10 AS customer_id, 'Ada' AS customer_name"
    )


def test_sql_certified_publication_promotes_and_records_certification(duckdb_runtime):
    connection, adapter, config_path = duckdb_runtime
    _create_sources(connection)
    engine = _engine(
        connection,
        adapter,
        config_path,
        registry=f"cert_pass_{uuid4().hex}",
    )

    run_id = engine.run_process_to_table(
        _certified_join_schema(), "gold", "orders"
    )

    assert adapter.fetch('SELECT * FROM "gold"."orders"') == [
        {"order_id": 1, "customer_name": "Ada"}
    ]
    event = engine.certification_store.get_run(run_id)
    assert event is not None
    assert event.state == "PROMOTED"
    assert engine.certification_store.get_certification("gold.orders").status == "CERTIFIED"
    assert adapter.list_tables("_skifer_staging") == []


def test_duckdb_publication_writes_only_typed_relation_handles(duckdb_runtime):
    _, adapter, _ = duckdb_runtime
    adapter.ensure_schema_exists("gold")

    with pytest.raises(DuckDBAdapterError, match="requires a DuckDBRelation"):
        adapter.write_table(object(), "gold.orders")

    assert not adapter.table_exists(None, "gold", "orders")


def test_sql_critical_failure_quarantines_without_changing_existing_target(
    duckdb_runtime, tmp_path
):
    connection, adapter, config_path = duckdb_runtime
    _create_sources(connection, missing_customer=True)
    connection.execute("CREATE SCHEMA gold")
    connection.execute(
        "CREATE TABLE gold.orders AS "
        "SELECT 700 AS order_id, 'untouched' AS customer_name"
    )
    before_path = tmp_path / "target-before.csv"
    after_path = tmp_path / "target-after.csv"
    connection.execute(
        "COPY (SELECT * FROM gold.orders ORDER BY order_id) TO ? (HEADER)",
        [str(before_path)],
    )
    engine = _engine(
        connection,
        adapter,
        config_path,
        registry=f"cert_fail_{uuid4().hex}",
    )

    with pytest.raises(DataQualityError) as exc_info:
        engine.run_process_to_table(_certified_join_schema(), "gold", "orders")

    connection.execute(
        "COPY (SELECT * FROM gold.orders ORDER BY order_id) TO ? (HEADER)",
        [str(after_path)],
    )
    assert after_path.read_bytes() == before_path.read_bytes()
    assert "987654321" not in str(exc_info.value)
    history = engine.certification_store.list_history("gold.orders")
    assert history[0].state == "QUARANTINED"
    assert all(event.state != "PROMOTED" for event in history)
    assert adapter.list_tables("_skifer_staging") == []

    quarantine_fqn = history[0].quarantine_fqn
    assert quarantine_fqn is not None
    rows = adapter.fetch(
        f"SELECT * FROM {adapter._quoted_fqn(quarantine_fqn)} ORDER BY order_id"
    )
    assert rows == [
        {
            "order_id": 1,
            "customer_name": None,
            "_violations": "NullCheck:0",
            "_run_id": history[0].run_id,
            "_contract_version": "1.0.0",
        }
    ]


def test_sql_quarantine_refuses_reserved_source_column(duckdb_runtime):
    connection, adapter, config_path = duckdb_runtime
    _create_sources(connection, missing_customer=True, include_reserved=True)
    engine = _engine(
        connection,
        adapter,
        config_path,
        registry=f"cert_reserved_{uuid4().hex}",
    )

    with pytest.raises(ValueError, match="Reserved quarantine column collision.*_violations"):
        engine.run_process_to_table(
            _certified_join_schema(include_reserved=True), "gold", "orders"
        )

    assert not adapter.table_exists(None, "gold", "orders")


def test_sql_publication_resume_promotes_staged_relation_once(duckdb_runtime):
    connection, adapter, config_path = duckdb_runtime
    _create_sources(connection)
    registry = f"cert_resume_{uuid4().hex}"
    engine = _engine(connection, adapter, config_path, registry=registry)
    schema = _certified_join_schema()
    definition = canonicalize_contract(parse_to_ir(schema))
    handle = compile_sql_pipeline_relation(
        engine.backend,
        schema,
        context=engine.context,
    )
    adapter.ensure_schema_exists("gold")
    run = start_publication_run(
        "gold.orders", definition, engine.certification_store, str(uuid4())
    )
    staged = stage_dataframe(
        engine.backend,
        run,
        definition,
        engine.certification_store,
        handle,
    )
    coordinator = PublicationCoordinator(
        engine.backend,
        engine.monitor,
        engine.certification_store,
    )

    first = coordinator.resume(staged, definition)
    second = coordinator.resume(staged, definition)

    assert first.state == second.state == "PROMOTED"
    assert first.run.state is second.run.state is RunState.PROMOTED
    assert adapter.fetch('SELECT * FROM "gold"."orders"') == [
        {"order_id": 1, "customer_name": "Ada"}
    ]
    assert adapter.list_tables("_skifer_staging") == []


def test_sql_certified_publication_fails_before_compilation_without_governance(
    duckdb_runtime,
):
    connection, adapter, config_path = duckdb_runtime
    engine = _engine(connection, adapter, config_path, governed=False)

    with pytest.raises(ValueError, match="certification_store and/or monitor"):
        engine.run_process_to_table(_certified_join_schema(), "gold", "orders")

    assert not adapter.schema_exists(None, "gold")
    assert not adapter.schema_exists(None, "_skifer_staging")
