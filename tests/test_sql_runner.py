"""Tests for DuckDB's end-to-end compiled SQL execution path."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import subprocess
import sys
from uuid import uuid4

import pytest
import yaml

from skifer.core.adapters.duckdb import DuckDBAdapter, DuckDBAdapterError
from skifer.core.capabilities_matrix import UnsupportedCapabilityError
from skifer.core.context import ExecutionContext
from skifer.core.core import SkiferEngine
from skifer.core.dialect import quote_fqn
from skifer.core.ir import parse_to_ir
from skifer.core.merge_sql import build_snapshot_apply_sql
from skifer.core.registry import RuleRegistry
from skifer.core.schema_loader import parse_schema
from skifer.core.sql_compiler import SqlCompilationError, compile_select
from skifer.core.sql_runner import _target_parts, run_sql_pipeline


def _context(*, is_job=False, is_production=False):
    return ExecutionContext(
        env="LOCAL",
        config={"environments": {"LOCAL": {"is_production": is_production}}},
        is_job_execution=is_job,
        is_local=True,
    )


@pytest.fixture
def duck_adapter():
    duckdb = pytest.importorskip("duckdb")
    connection = duckdb.connect()
    adapter = DuckDBAdapter(connection=connection)
    yield adapter
    connection.close()


@pytest.fixture
def registered_rules():
    names = []
    yield names
    for name in names:
        RuleRegistry._rules.pop(name, None)


def _register_rule(names, *, kind, result):
    name = f"sql_runner_{kind}_{uuid4().hex}"
    if kind == "sql":
        @RuleRegistry.register_rule(name=name, kind=kind)
        def rule():
            return result
    else:
        @RuleRegistry.register_rule(name=name, kind=kind)
        def rule(df):
            return df
    names.append(name)
    return name


RUN_1 = datetime(2026, 1, 1, 9, 0, tzinfo=timezone.utc)
RUN_2 = datetime(2026, 1, 2, 9, 0, tzinfo=timezone.utc)
RUN_3 = datetime(2026, 1, 3, 9, 0, tzinfo=timezone.utc)
T1 = datetime(2026, 1, 1, 0, 0)
T2 = datetime(2026, 1, 2, 0, 0)


def _snapshot_schema(strategy, *, on_missing="ignore"):
    if strategy == "timestamp":
        mat = {
            "type": "snapshot",
            "strategy": "timestamp",
            "unique_key": ["order_id"],
            "updated_at": "modified_at",
            "on_missing": on_missing,
        }
    else:
        mat = {
            "type": "snapshot",
            "strategy": "check",
            "unique_key": ["order_id"],
            "check_columns": ["status", "amount"],
            "on_missing": on_missing,
        }
    if on_missing == "close":
        mat["max_closed_ratio"] = 1.0
    return {
        "materialization": mat,
        "tables": [{"name": "source.orders", "alias": "orders"}],
    }


def _prepare_snapshot_tables(adapter, strategy):
    adapter.execute_sql("CREATE SCHEMA IF NOT EXISTS source")
    adapter.execute_sql("CREATE SCHEMA IF NOT EXISTS gold")
    adapter.execute_sql("DROP TABLE IF EXISTS source.orders")
    adapter.execute_sql("DROP TABLE IF EXISTS gold.orders")
    if strategy == "timestamp":
        adapter.execute_sql(
            "CREATE TABLE source.orders("
            "order_id INTEGER, status VARCHAR, amount INTEGER, modified_at TIMESTAMP)"
        )
    else:
        adapter.execute_sql(
            "CREATE TABLE source.orders("
            "order_id INTEGER, status VARCHAR, amount INTEGER)"
        )


def _replace_snapshot_source(adapter, strategy, rows):
    adapter.execute_sql("DELETE FROM source.orders")
    if not rows:
        return
    if strategy == "timestamp":
        values = ", ".join(
            f"({order_id}, '{status}', {amount}, TIMESTAMP '{modified_at}')"
            for order_id, status, amount, modified_at in rows
        )
    else:
        values = ", ".join(
            f"({order_id}, '{status}', {amount})"
            for order_id, status, amount in rows
        )
    adapter.execute_sql(f"INSERT INTO source.orders VALUES {values}")


def _snapshot_rows(adapter):
    return adapter.fetch(
        'SELECT "order_id", "status", "amount", "valid_from", "valid_to" '
        'FROM "gold"."orders" ORDER BY "order_id", "valid_from"'
    )


def test_duckdb_pipeline_filter_join_projection_and_sql_rule(
    tmp_path, registered_rules
):
    duckdb = pytest.importorskip("duckdb")
    connection = duckdb.connect()
    connection.execute("CREATE SCHEMA source")
    connection.execute(
        "CREATE TABLE source.orders AS SELECT * FROM VALUES "
        "(1, 10, 'complete'), (2, 20, 'pending'), (3, 30, 'complete') "
        "AS rows(order_id, customer_id, status)"
    )
    connection.execute(
        "CREATE TABLE source.customers AS SELECT * FROM VALUES "
        "(10, 'Ada'), (20, 'Bob'), (30, 'Cy') AS rows(customer_id, name)"
    )
    rule_name = _register_rule(
        registered_rules,
        kind="sql",
        result={"status": "UPPER(status)"},
    )
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
    engine = SkiferEngine(
        config_path=str(config_path),
        force_env="LOCAL",
        connection=connection,
    )
    schema = {
        "tables": [
            {
                "name": "source.orders",
                "alias": "orders",
                "filter": ["status:equals:complete"],
            },
            {"name": "source.customers", "alias": "customers"},
        ],
        "join": [
            {
                "table_from": ["orders", "customer_id"],
                "table_to": ["customers", "customer_id"],
                "type": "inner",
            }
        ],
        "business_rules": [rule_name],
        "select_final": [
            ["order_id", "order_id"],
            ["name", "customer_name", ["upper"]],
            ["status", "status"],
        ],
    }

    pipeline_path = tmp_path / "completed_orders.yaml"
    pipeline_path.write_text(yaml.safe_dump(schema), encoding="utf-8")

    try:
        engine.run_from_yaml(
            str(pipeline_path), "gold", target_table_name="completed_orders"
        )
        assert engine.backend.fetch(
            'SELECT * FROM "gold"."completed_orders" ORDER BY "order_id"'
        ) == [
            {"order_id": 1, "customer_name": "ADA", "status": "COMPLETE"},
            {"order_id": 3, "customer_name": "CY", "status": "COMPLETE"},
        ]
    finally:
        connection.close()


def test_python_rule_is_refused_by_adapter_capability(duck_adapter, registered_rules):
    rule_name = _register_rule(registered_rules, kind="transform", result=None)
    schema = {
        "tables": [{"name": "source.orders", "alias": "orders"}],
        "business_rules": [rule_name],
    }

    with pytest.raises(UnsupportedCapabilityError, match="duckdb.*python_rules"):
        run_sql_pipeline(
            duck_adapter,
            schema,
            "gold.orders",
            context=_context(),
        )


def test_snapshot_strategy_unknown_is_still_rejected():
    with pytest.raises(ValueError, match=r"Unknown 'strategy'.*snapshot"):
        parse_schema("""
materialization:
  type: snapshot
  strategy: hash
  unique_key: [order_id]
  updated_at: modified_at
  on_missing: ignore
tables:
  - name: source.orders
""")


def test_duckdb_view_tracks_source_changes(duck_adapter):
    duck_adapter.execute_sql("CREATE SCHEMA source")
    duck_adapter.execute_sql("CREATE SCHEMA gold")
    duck_adapter.execute_sql(
        "CREATE TABLE source.orders(order_id INTEGER, status VARCHAR)"
    )
    duck_adapter.execute_sql("INSERT INTO source.orders VALUES (1, 'ready')")
    schema = {
        "materialization": {"type": "view"},
        "tables": [{"name": "source.orders", "alias": "orders"}],
        "select_final": [["order_id", "order_id"], ["status", "status"]],
    }

    statement = run_sql_pipeline(
        duck_adapter,
        schema,
        "gold.orders_v",
        context=_context(),
    )

    assert statement.startswith('CREATE OR REPLACE VIEW "gold"."orders_v" AS ')
    assert duck_adapter.fetch(
        'SELECT * FROM "gold"."orders_v" ORDER BY "order_id"'
    ) == [{"order_id": 1, "status": "ready"}]

    duck_adapter.execute_sql("INSERT INTO source.orders VALUES (2, 'done')")

    assert duck_adapter.fetch(
        'SELECT * FROM "gold"."orders_v" ORDER BY "order_id"'
    ) == [
        {"order_id": 1, "status": "ready"},
        {"order_id": 2, "status": "done"},
    ]


@pytest.mark.parametrize(
    ("schema_extra", "message"),
    [
        ({"dev_limit": 1}, "dev_limit"),
        (
            {
                "tables": [
                    {
                        "name": "source.orders",
                        "quality_checks": {"drop_duplicates_on": ["order_id"]},
                    }
                ]
            },
            "drop_duplicates_on",
        ),
    ],
)
def test_duckdb_view_rejects_unstable_persisted_definition_constructs(
    duck_adapter, schema_extra, message
):
    schema = {
        "materialization": {"type": "view"},
        "tables": [{"name": "source.orders"}],
        **schema_extra,
    }

    with pytest.raises(SqlCompilationError, match=message):
        run_sql_pipeline(
            duck_adapter,
            schema,
            "gold.orders_v",
            context=_context(is_job=True),
        )


def test_duckdb_incremental_append_accumulates_across_runs(duck_adapter):
    duck_adapter.execute_sql("CREATE SCHEMA source")
    duck_adapter.execute_sql("CREATE SCHEMA gold")
    duck_adapter.execute_sql("CREATE TABLE source.orders(order_id INTEGER)")
    duck_adapter.execute_sql("INSERT INTO source.orders VALUES (1), (2)")
    schema = {
        "materialization": {"type": "incremental", "strategy": "append"},
        "tables": [{"name": "source.orders", "alias": "orders"}],
    }

    first = run_sql_pipeline(duck_adapter, schema, "gold.orders", context=_context())
    duck_adapter.execute_sql("DELETE FROM source.orders")
    duck_adapter.execute_sql("INSERT INTO source.orders VALUES (3), (4)")
    second = run_sql_pipeline(duck_adapter, schema, "gold.orders", context=_context())

    assert first.startswith('CREATE TABLE "gold"."orders" AS ')
    assert second.startswith('INSERT INTO "gold"."orders" ')
    assert duck_adapter.fetch('SELECT * FROM "gold"."orders" ORDER BY "order_id"') == [
        {"order_id": 1},
        {"order_id": 2},
        {"order_id": 3},
        {"order_id": 4},
    ]


def test_duckdb_incremental_append_watermark_inserts_only_new_rows(duck_adapter):
    duck_adapter.execute_sql("CREATE SCHEMA source")
    duck_adapter.execute_sql("CREATE SCHEMA gold")
    duck_adapter.execute_sql(
        "CREATE TABLE source.orders(order_id INTEGER, updated_at INTEGER)"
    )
    duck_adapter.execute_sql("INSERT INTO source.orders VALUES (1, 1), (2, 2)")
    schema = {
        "materialization": {
            "type": "incremental",
            "strategy": "append",
            "watermark_column": "updated_at",
        },
        "tables": [{"name": "source.orders", "alias": "orders"}],
    }

    run_sql_pipeline(duck_adapter, schema, "gold.orders", context=_context())
    duck_adapter.execute_sql("DELETE FROM source.orders")
    duck_adapter.execute_sql(
        "INSERT INTO source.orders VALUES (1, 1), (2, 2), (3, 3), (4, 4)"
    )
    run_sql_pipeline(duck_adapter, schema, "gold.orders", context=_context())

    assert duck_adapter.fetch(
        'SELECT * FROM "gold"."orders" ORDER BY "order_id"'
    ) == [
        {"order_id": 1, "updated_at": 1},
        {"order_id": 2, "updated_at": 2},
        {"order_id": 3, "updated_at": 3},
        {"order_id": 4, "updated_at": 4},
    ]


def test_duckdb_incremental_append_watermark_unchanged_source_noops(duck_adapter):
    duck_adapter.execute_sql("CREATE SCHEMA source")
    duck_adapter.execute_sql("CREATE SCHEMA gold")
    duck_adapter.execute_sql(
        "CREATE TABLE source.orders(order_id INTEGER, updated_at INTEGER)"
    )
    duck_adapter.execute_sql("INSERT INTO source.orders VALUES (1, 1), (2, 2)")
    schema = {
        "materialization": {
            "type": "incremental",
            "strategy": "append",
            "watermark_column": "updated_at",
        },
        "tables": [{"name": "source.orders", "alias": "orders"}],
    }

    run_sql_pipeline(duck_adapter, schema, "gold.orders", context=_context())
    run_sql_pipeline(duck_adapter, schema, "gold.orders", context=_context())

    assert duck_adapter.fetch(
        'SELECT COUNT(*) AS "row_count" FROM "gold"."orders"'
    ) == [{"row_count": 2}]


def test_duckdb_incremental_merge_updates_inserts_and_keeps_keys_unique(duck_adapter):
    duck_adapter.execute_sql("CREATE SCHEMA source")
    duck_adapter.execute_sql("CREATE SCHEMA gold")
    duck_adapter.execute_sql(
        "CREATE TABLE source.orders(order_id INTEGER, status VARCHAR, amount INTEGER)"
    )
    duck_adapter.execute_sql(
        "INSERT INTO source.orders VALUES (1, 'steady', 10), (2, 'old', 20)"
    )
    schema = {
        "materialization": {
            "type": "incremental",
            "strategy": "merge",
            "unique_key": ["order_id"],
        },
        "tables": [{"name": "source.orders", "alias": "orders"}],
    }

    first = run_sql_pipeline(duck_adapter, schema, "gold.orders", context=_context())
    duck_adapter.execute_sql("DELETE FROM source.orders")
    duck_adapter.execute_sql(
        "INSERT INTO source.orders VALUES "
        "(1, 'steady', 10), (2, 'updated', 25), (3, 'new', 30)"
    )
    second = run_sql_pipeline(duck_adapter, schema, "gold.orders", context=_context())

    assert first.startswith('CREATE TABLE "gold"."orders" AS ')
    assert second.startswith('MERGE INTO "gold"."orders" AS t')
    assert "SET *" not in second
    assert "INSERT *" not in second
    assert 't."order_id" = s."order_id"' in second
    assert 't."order_id" = s."order_id"' not in second.split(
        "WHEN MATCHED THEN UPDATE SET", 1
    )[1].split("WHEN NOT MATCHED", 1)[0]
    assert duck_adapter.fetch(
        'SELECT * FROM "gold"."orders" ORDER BY "order_id"'
    ) == [
        {"order_id": 1, "status": "steady", "amount": 10},
        {"order_id": 2, "status": "updated", "amount": 25},
        {"order_id": 3, "status": "new", "amount": 30},
    ]
    assert duck_adapter.fetch(
        'SELECT "order_id", COUNT(*) AS "row_count" '
        'FROM "gold"."orders" GROUP BY "order_id" ORDER BY "order_id"'
    ) == [
        {"order_id": 1, "row_count": 1},
        {"order_id": 2, "row_count": 1},
        {"order_id": 3, "row_count": 1},
    ]


def test_duckdb_incremental_merge_composite_key_uses_every_key(duck_adapter):
    duck_adapter.execute_sql("CREATE SCHEMA source")
    duck_adapter.execute_sql("CREATE SCHEMA gold")
    duck_adapter.execute_sql(
        "CREATE TABLE source.orders(order_id INTEGER, src VARCHAR, status VARCHAR)"
    )
    duck_adapter.execute_sql(
        "INSERT INTO source.orders VALUES (1, 'web', 'old'), (1, 'pos', 'steady')"
    )
    schema = {
        "materialization": {
            "type": "incremental",
            "strategy": "merge",
            "unique_key": ["order_id", "src"],
        },
        "tables": [{"name": "source.orders", "alias": "orders"}],
    }

    run_sql_pipeline(duck_adapter, schema, "gold.orders", context=_context())
    duck_adapter.execute_sql("DELETE FROM source.orders")
    duck_adapter.execute_sql(
        "INSERT INTO source.orders VALUES "
        "(1, 'web', 'updated'), (1, 'pos', 'steady'), (2, 'web', 'new')"
    )
    statement = run_sql_pipeline(
        duck_adapter, schema, "gold.orders", context=_context()
    )

    assert 't."order_id" = s."order_id" AND t."src" = s."src"' in statement
    assert duck_adapter.fetch(
        'SELECT "order_id", "src", "status" FROM "gold"."orders" '
        'ORDER BY "order_id", "src"'
    ) == [
        {"order_id": 1, "src": "pos", "status": "steady"},
        {"order_id": 1, "src": "web", "status": "updated"},
        {"order_id": 2, "src": "web", "status": "new"},
    ]
    assert duck_adapter.fetch(
        'SELECT "order_id", "src", COUNT(*) AS "row_count" '
        'FROM "gold"."orders" GROUP BY "order_id", "src" '
        'ORDER BY "order_id", "src"'
    ) == [
        {"order_id": 1, "src": "pos", "row_count": 1},
        {"order_id": 1, "src": "web", "row_count": 1},
        {"order_id": 2, "src": "web", "row_count": 1},
    ]


def test_duckdb_incremental_merge_refuses_source_target_column_divergence(
    duck_adapter,
):
    duck_adapter.execute_sql("CREATE SCHEMA source")
    duck_adapter.execute_sql("CREATE SCHEMA gold")
    duck_adapter.execute_sql(
        "CREATE TABLE source.orders(order_id INTEGER, status VARCHAR)"
    )
    duck_adapter.execute_sql("INSERT INTO source.orders VALUES (1, 'old')")
    schema = {
        "materialization": {
            "type": "incremental",
            "strategy": "merge",
            "unique_key": ["order_id"],
        },
        "tables": [{"name": "source.orders", "alias": "orders"}],
    }

    run_sql_pipeline(duck_adapter, schema, "gold.orders", context=_context())
    duck_adapter.execute_sql("DROP TABLE source.orders")
    duck_adapter.execute_sql("CREATE TABLE source.orders(order_id INTEGER, note VARCHAR)")
    duck_adapter.execute_sql("INSERT INTO source.orders VALUES (1, 'new')")

    with pytest.raises(ValueError) as exc_info:
        run_sql_pipeline(duck_adapter, schema, "gold.orders", context=_context())

    message = str(exc_info.value)
    assert "source-only columns: ['note']" in message
    assert "target-only columns: ['status']" in message


@pytest.mark.parametrize("strategy", ["timestamp", "check"])
def test_duckdb_snapshot_first_run_and_unchanged_second_run_noop(duck_adapter, strategy):
    _prepare_snapshot_tables(duck_adapter, strategy)
    if strategy == "timestamp":
        rows = [(1, "steady", 10, "2026-01-01 00:00:00"), (2, "old", 20, "2026-01-02 00:00:00")]
        expected_from = {1: T1, 2: T2}
    else:
        rows = [(1, "steady", 10), (2, "old", 20)]
        expected_from = {1: RUN_1.replace(tzinfo=None), 2: RUN_1.replace(tzinfo=None)}
    _replace_snapshot_source(duck_adapter, strategy, rows)
    schema = _snapshot_schema(strategy)

    first = run_sql_pipeline(
        duck_adapter, schema, "gold.orders", context=_context(), clock=lambda: RUN_1
    )
    second = run_sql_pipeline(
        duck_adapter, schema, "gold.orders", context=_context(), clock=lambda: RUN_2
    )

    assert first.startswith('CREATE TABLE "gold"."orders" AS SELECT ')
    assert "UPDATE " in second
    assert "INSERT INTO" in second
    assert "SET *" not in second
    assert "INSERT *" not in second
    assert _snapshot_rows(duck_adapter) == [
        {
            "order_id": order_id,
            "status": status,
            "amount": amount,
            "valid_from": expected_from[order_id],
            "valid_to": None,
        }
        for order_id, status, amount, *_ in rows
    ]


@pytest.mark.parametrize("strategy", ["timestamp", "check"])
def test_duckdb_snapshot_changed_key_closes_and_inserts_current(duck_adapter, strategy):
    _prepare_snapshot_tables(duck_adapter, strategy)
    if strategy == "timestamp":
        _replace_snapshot_source(duck_adapter, strategy, [(1, "old", 10, "2026-01-01 00:00:00")])
        run_sql_pipeline(
            duck_adapter, _snapshot_schema(strategy), "gold.orders", context=_context(), clock=lambda: RUN_1
        )
        _replace_snapshot_source(duck_adapter, strategy, [(1, "new", 15, "2026-01-02 00:00:00")])
        new_from = T2
    else:
        _replace_snapshot_source(duck_adapter, strategy, [(1, "old", 10)])
        run_sql_pipeline(
            duck_adapter, _snapshot_schema(strategy), "gold.orders", context=_context(), clock=lambda: RUN_1
        )
        _replace_snapshot_source(duck_adapter, strategy, [(1, "new", 15)])
        new_from = RUN_2.replace(tzinfo=None)

    statement = run_sql_pipeline(
        duck_adapter, _snapshot_schema(strategy), "gold.orders", context=_context(), clock=lambda: RUN_2
    )

    assert "SET *" not in statement
    assert "INSERT *" not in statement
    assert _snapshot_rows(duck_adapter) == [
        {
            "order_id": 1,
            "status": "old",
            "amount": 10,
            "valid_from": T1 if strategy == "timestamp" else RUN_1.replace(tzinfo=None),
            "valid_to": new_from,
        },
        {
            "order_id": 1,
            "status": "new",
            "amount": 15,
            "valid_from": new_from,
            "valid_to": None,
        },
    ]


@pytest.mark.parametrize("strategy", ["timestamp", "check"])
def test_duckdb_snapshot_new_key_inserts_current_version(duck_adapter, strategy):
    _prepare_snapshot_tables(duck_adapter, strategy)
    first_rows = [(1, "old", 10, "2026-01-01 00:00:00")] if strategy == "timestamp" else [(1, "old", 10)]
    second_rows = (
        [(1, "old", 10, "2026-01-01 00:00:00"), (2, "new", 20, "2026-01-02 00:00:00")]
        if strategy == "timestamp"
        else [(1, "old", 10), (2, "new", 20)]
    )
    _replace_snapshot_source(duck_adapter, strategy, first_rows)
    run_sql_pipeline(
        duck_adapter, _snapshot_schema(strategy), "gold.orders", context=_context(), clock=lambda: RUN_1
    )
    _replace_snapshot_source(duck_adapter, strategy, second_rows)

    run_sql_pipeline(
        duck_adapter, _snapshot_schema(strategy), "gold.orders", context=_context(), clock=lambda: RUN_2
    )

    assert _snapshot_rows(duck_adapter) == [
        {
            "order_id": 1,
            "status": "old",
            "amount": 10,
            "valid_from": T1 if strategy == "timestamp" else RUN_1.replace(tzinfo=None),
            "valid_to": None,
        },
        {
            "order_id": 2,
            "status": "new",
            "amount": 20,
            "valid_from": T2 if strategy == "timestamp" else RUN_2.replace(tzinfo=None),
            "valid_to": None,
        },
    ]


@pytest.mark.parametrize("strategy", ["timestamp", "check"])
@pytest.mark.parametrize("on_missing", ["close", "ignore"])
def test_duckdb_snapshot_missing_key_policy(duck_adapter, strategy, on_missing):
    _prepare_snapshot_tables(duck_adapter, strategy)
    first_rows = (
        [(1, "kept", 10, "2026-01-01 00:00:00"), (2, "missing", 20, "2026-01-01 00:00:00")]
        if strategy == "timestamp"
        else [(1, "kept", 10), (2, "missing", 20)]
    )
    second_rows = [(1, "kept", 10, "2026-01-01 00:00:00")] if strategy == "timestamp" else [(1, "kept", 10)]
    _replace_snapshot_source(duck_adapter, strategy, first_rows)
    schema = _snapshot_schema(strategy, on_missing=on_missing)
    run_sql_pipeline(
        duck_adapter, schema, "gold.orders", context=_context(), clock=lambda: RUN_1
    )
    _replace_snapshot_source(duck_adapter, strategy, second_rows)

    run_sql_pipeline(
        duck_adapter, schema, "gold.orders", context=_context(), clock=lambda: RUN_2
    )

    missing = [row for row in _snapshot_rows(duck_adapter) if row["order_id"] == 2][0]
    assert missing["valid_to"] == (
        RUN_2.replace(tzinfo=None) if on_missing == "close" else None
    )


@pytest.mark.parametrize("strategy", ["timestamp", "check"])
def test_duckdb_snapshot_bounds_are_continuous_across_three_versions(duck_adapter, strategy):
    _prepare_snapshot_tables(duck_adapter, strategy)
    schema = _snapshot_schema(strategy)
    runs = [
        (RUN_1, [(1, "v1", 10, "2026-01-01 00:00:00")] if strategy == "timestamp" else [(1, "v1", 10)]),
        (RUN_2, [(1, "v2", 20, "2026-01-02 00:00:00")] if strategy == "timestamp" else [(1, "v2", 20)]),
        (RUN_3, [(1, "v3", 30, "2026-01-03 00:00:00")] if strategy == "timestamp" else [(1, "v3", 30)]),
    ]
    for run_at, rows in runs:
        _replace_snapshot_source(duck_adapter, strategy, rows)
        run_sql_pipeline(
            duck_adapter, schema, "gold.orders", context=_context(), clock=lambda run_at=run_at: run_at
        )

    rows = _snapshot_rows(duck_adapter)
    assert rows[0]["valid_to"] == rows[1]["valid_from"]
    assert rows[1]["valid_to"] == rows[2]["valid_from"]
    assert rows[2]["valid_to"] is None


@pytest.mark.parametrize("strategy", ["timestamp", "check"])
def test_duckdb_snapshot_preflight_refusal_leaves_target_unchanged(duck_adapter, strategy):
    _prepare_snapshot_tables(duck_adapter, strategy)
    clean_rows = [(1, "old", 10, "2026-01-01 00:00:00")] if strategy == "timestamp" else [(1, "old", 10)]
    _replace_snapshot_source(duck_adapter, strategy, clean_rows)
    schema = _snapshot_schema(strategy)
    run_sql_pipeline(
        duck_adapter, schema, "gold.orders", context=_context(), clock=lambda: RUN_1
    )
    before = _snapshot_rows(duck_adapter)
    duplicate_rows = (
        [(1, "old", 10, "2026-01-01 00:00:00"), (1, "new", 20, "2026-01-02 00:00:00")]
        if strategy == "timestamp"
        else [(1, "old", 10), (1, "new", 20)]
    )
    _replace_snapshot_source(duck_adapter, strategy, duplicate_rows)

    with pytest.raises(ValueError, match="unique_key.*not unique"):
        run_sql_pipeline(
            duck_adapter, schema, "gold.orders", context=_context(), clock=lambda: RUN_2
        )

    assert _snapshot_rows(duck_adapter) == before


@pytest.mark.parametrize("strategy", ["timestamp", "check"])
def test_duckdb_snapshot_rerun_after_interrupted_close_converges(duck_adapter, strategy):
    _prepare_snapshot_tables(duck_adapter, strategy)
    schema = _snapshot_schema(strategy)
    first_rows = [(1, "old", 10, "2026-01-01 00:00:00")] if strategy == "timestamp" else [(1, "old", 10)]
    second_rows = [(1, "new", 20, "2026-01-02 00:00:00")] if strategy == "timestamp" else [(1, "new", 20)]
    _replace_snapshot_source(duck_adapter, strategy, first_rows)
    run_sql_pipeline(
        duck_adapter, schema, "gold.orders", context=_context(), clock=lambda: RUN_1
    )
    _replace_snapshot_source(duck_adapter, strategy, second_rows)
    materialization = schema["materialization"]
    statements = build_snapshot_apply_sql(
        target_relation=quote_fqn("gold.orders", target="duckdb"),
        source_relation=quote_fqn("source.orders", target="duckdb"),
        source_columns=duck_adapter.list_relation_columns(quote_fqn("source.orders", target="duckdb")),
        materialization=materialization,
        target="duckdb",
        run_at=RUN_2,
    )
    duck_adapter.execute_sql(statements[0])

    run_sql_pipeline(
        duck_adapter, schema, "gold.orders", context=_context(), clock=lambda: RUN_3
    )

    rows = _snapshot_rows(duck_adapter)
    assert len(rows) == 2
    assert rows[0]["valid_to"] == rows[1]["valid_from"]
    assert rows[1]["status"] == "new"
    assert rows[1]["valid_to"] is None


def test_sql_rule_rewrite_uses_resolved_source_columns(
    duck_adapter, registered_rules
):
    duck_adapter.execute_sql("CREATE SCHEMA source")
    duck_adapter.ensure_schema_exists("gold")
    duck_adapter.execute_sql(
        "CREATE TABLE source.orders(order_id INTEGER, status VARCHAR)"
    )
    duck_adapter.execute_sql("INSERT INTO source.orders VALUES (1, 'ready')")
    rule_name = _register_rule(
        registered_rules,
        kind="sql",
        result={"status": "UPPER(status)"},
    )
    schema = {
        "tables": [{"name": "source.orders", "alias": "orders"}],
        "business_rules": [rule_name],
        "select_final": [["order_id", "order_id"], ["status", "status"]],
    }

    with pytest.raises(SqlCompilationError, match="resolve_columns"):
        compile_select(parse_to_ir(schema), persisted_definition=False)

    run_sql_pipeline(
        duck_adapter,
        schema,
        "gold.orders",
        context=_context(),
    )
    assert duck_adapter.fetch('SELECT * FROM "gold"."orders"') == [
        {"order_id": 1, "status": "READY"}
    ]


def test_missing_source_for_column_resolution_names_the_table(
    duck_adapter, registered_rules
):
    rule_name = _register_rule(
        registered_rules,
        kind="sql",
        result={"status": "UPPER(status)"},
    )
    schema = {
        "tables": [{"name": "source.missing", "alias": "orders"}],
        "business_rules": [rule_name],
    }

    with pytest.raises(DuckDBAdapterError, match="source.missing"):
        run_sql_pipeline(
            duck_adapter,
            schema,
            "gold.orders",
            context=_context(),
        )


def _csv_rule_schema(path, rule_name):
    return {
        "tables": [
            {
                "name": "raw_orders",
                "alias": "orders",
                "source": {
                    "type": "csv",
                    "path": str(path),
                    "options": {"header": "true", "inferSchema": "true"},
                },
            }
        ],
        "business_rules": [rule_name],
        "keep_all_columns": True,
    }


def test_sql_rule_adds_column_to_csv_source(
    duck_adapter, tmp_path, registered_rules
):
    path = tmp_path / "orders.csv"
    path.write_text("order_id,status\n1,ready\n", encoding="utf-8")
    duck_adapter.ensure_schema_exists("gold")
    rule_name = _register_rule(
        registered_rules,
        kind="sql",
        result={"status_upper": "UPPER(status)"},
    )

    run_sql_pipeline(
        duck_adapter,
        _csv_rule_schema(path, rule_name),
        "gold.orders_with_status",
        context=_context(),
    )

    assert duck_adapter.fetch('SELECT * FROM "gold"."orders_with_status"') == [
        {"order_id": 1, "status": "ready", "status_upper": "READY"}
    ]


def test_sql_rule_rewrites_one_existing_column_from_csv_source(
    duck_adapter, tmp_path, registered_rules
):
    path = tmp_path / "orders.csv"
    path.write_text("order_id,status\n1,ready\n", encoding="utf-8")
    duck_adapter.ensure_schema_exists("gold")
    rule_name = _register_rule(
        registered_rules,
        kind="sql",
        result={"status": "UPPER(status)"},
    )

    run_sql_pipeline(
        duck_adapter,
        _csv_rule_schema(path, rule_name),
        "gold.rewritten_orders",
        context=_context(),
    )

    cursor = duck_adapter.read_table("gold.rewritten_orders")
    assert [column[0] for column in cursor.description] == ["order_id", "status"]
    assert cursor.fetchall() == [(1, "READY")]


def test_unreadable_csv_rule_source_raises_before_materialization(
    duck_adapter, tmp_path, registered_rules
):
    missing_path = tmp_path / "missing.csv"
    duck_adapter.ensure_schema_exists("gold")
    rule_name = _register_rule(
        registered_rules,
        kind="sql",
        result={"status": "UPPER(status)"},
    )

    with pytest.raises(DuckDBAdapterError, match="relation.*missing.csv"):
        run_sql_pipeline(
            duck_adapter,
            _csv_rule_schema(missing_path, rule_name),
            "gold.unreadable_orders",
            context=_context(),
        )

    assert not duck_adapter.table_exists(None, "gold", "unreadable_orders")


def test_run_sql_pipeline_reads_filters_and_writes_csv_source(
    duck_adapter, tmp_path
):
    path = tmp_path / "orders.csv"
    path.write_text(
        "order_id;status;amount\n1;complete;10\n2;pending;20\n3;complete;30\n",
        encoding="utf-8",
    )
    duck_adapter.ensure_schema_exists("gold")
    schema = {
        "tables": [
            {
                "name": "raw_orders",
                "alias": "orders",
                "source": {
                    "type": "csv",
                    "path": str(path),
                    "options": {
                        "header": "true",
                        "inferSchema": "true",
                        "sep": ";",
                    },
                },
                "filter": [
                    {"column": "status", "operator": "equals", "value": "complete"}
                ],
            }
        ],
        "select_final": [
            ["order_id", "id"],
            ["amount", "amount", ["cast:double"]],
        ],
    }

    run_sql_pipeline(
        duck_adapter,
        schema,
        "gold.completed_orders",
        context=_context(),
    )

    assert duck_adapter.fetch(
        'SELECT * FROM "gold"."completed_orders" ORDER BY "id"'
    ) == [{"id": 1, "amount": 10.0}, {"id": 3, "amount": 30.0}]


def test_run_sql_pipeline_still_refuses_delta_source(duck_adapter, tmp_path):
    schema = {
        "tables": [
            {
                "name": "delta_rows",
                "source": {"type": "delta", "path": str(tmp_path / "delta")},
            }
        ]
    }

    with pytest.raises(DuckDBAdapterError, match="duckdb.*delta"):
        run_sql_pipeline(
            duck_adapter,
            schema,
            "gold.delta_rows",
            context=_context(),
        )


def test_run_sql_pipeline_does_not_access_file_hook_without_capability(duck_adapter):
    class AdapterWithoutFileSources:
        name = "duckdb"
        capabilities = frozenset()

        def list_columns(self, fqn):
            return duck_adapter.list_columns(fqn)

        def execute_sql(self, sql):
            return duck_adapter.execute_sql(sql)

    duck_adapter.execute_sql("CREATE SCHEMA source")
    duck_adapter.execute_sql("CREATE TABLE source.rows(id INTEGER)")
    duck_adapter.execute_sql("INSERT INTO source.rows VALUES (1)")
    duck_adapter.ensure_schema_exists("gold")

    run_sql_pipeline(
        AdapterWithoutFileSources(),
        {"tables": [{"name": "source.rows"}], "select_final": [["id", "id"]]},
        "gold.rows",
        context=_context(),
    )

    assert duck_adapter.fetch('SELECT * FROM "gold"."rows"') == [{"id": 1}]


@pytest.mark.parametrize(
    ("context", "expected_count"),
    [
        (_context(), 2),
        (_context(is_job=True), 4),
        (_context(is_production=True), 4),
    ],
    ids=["interactive", "job", "production"],
)
def test_dev_limit_obeys_execution_context(duck_adapter, context, expected_count):
    duck_adapter.execute_sql("CREATE SCHEMA source")
    duck_adapter.ensure_schema_exists("gold")
    duck_adapter.execute_sql(
        "CREATE TABLE source.rows AS SELECT * FROM VALUES (1), (2), (3), (4) AS rows(id)"
    )
    schema = {
        "tables": [
            {"name": "source.rows", "alias": "rows", "dev_limit": 2}
        ],
        "dev_limit": 1,
        "select_final": [["id", "id"]],
    }

    executed = run_sql_pipeline(
        duck_adapter,
        schema,
        "gold.rows",
        context=context,
    )

    assert len(duck_adapter.fetch('SELECT * FROM "gold"."rows"')) == expected_count
    assert ("LIMIT" in executed) is (expected_count == 2)
    assert schema["dev_limit"] == 1
    assert schema["tables"][0]["dev_limit"] == 2


@pytest.mark.parametrize(
    "context",
    [_context(is_job=True), _context(is_production=True)],
    ids=["job", "production"],
)
def test_nested_partial_dev_limit_is_ignored_outside_interactive_mode(
    duck_adapter, context
):
    duck_adapter.execute_sql("CREATE SCHEMA source")
    duck_adapter.ensure_schema_exists("gold")
    duck_adapter.execute_sql(
        "CREATE TABLE source.rows AS SELECT * FROM VALUES (1), (2), (3), (4) "
        "AS rows(id)"
    )
    schema = {
        "partials": [
            {
                "alias": "middle_rows",
                "schema": {
                    "partials": [
                        {
                            "alias": "leaf_rows",
                            "schema": {
                                "tables": [
                                    {
                                        "name": "source.rows",
                                        "alias": "rows",
                                        "dev_limit": 1,
                                    }
                                ],
                                "dev_limit": 1,
                                "select_final": [["id", "id"]],
                            },
                        }
                    ],
                    "select_final": [["id", "id"]],
                },
            }
        ],
        "select_final": [["id", "id"]],
    }

    executed = run_sql_pipeline(
        duck_adapter,
        schema,
        "gold.nested_rows",
        context=context,
    )

    assert duck_adapter.fetch(
        'SELECT * FROM "gold"."nested_rows" ORDER BY "id"'
    ) == [{"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}]
    assert "LIMIT" not in executed


def test_sql_engine_initializes_when_pyspark_is_unimportable(tmp_path):
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
    project_root = Path(__file__).resolve().parents[1]
    script = r'''
import importlib.abc
import sys

class BlockPyspark(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "pyspark" or fullname.startswith("pyspark."):
            raise ModuleNotFoundError("pyspark blocked", name=fullname)
        return None

sys.meta_path.insert(0, BlockPyspark())
from skifer import SkiferEngine

engine = SkiferEngine(config_path=sys.argv[1], force_env="LOCAL")
assert engine.spark is None
assert engine.backend.name == "duckdb"
assert "pyspark" not in sys.modules
engine.backend.connection.close()
'''
    completed = subprocess.run(
        [sys.executable, "-c", script, str(config_path)],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stdout + completed.stderr


def test_sql_rule_column_resolution_applies_table_resolution(
    duck_adapter, registered_rules
):
    """The resolver must resolve the declared name, as the compiler used to.

    Column resolution moved from the compiler to the runner in 39.3.3, taking
    ``resolve_table`` with it. Nothing else exercises that call, so dropping it
    would leave the sandbox name unresolved and be caught by no test.
    """
    duck_adapter.execute_sql("CREATE SCHEMA sandbox")
    duck_adapter.ensure_schema_exists("gold")
    duck_adapter.execute_sql(
        "CREATE TABLE sandbox.orders AS SELECT * FROM VALUES "
        "(1, 'ready') AS rows(order_id, status)"
    )
    rule_name = _register_rule(
        registered_rules,
        kind="sql",
        result={"status": "UPPER(status)"},
    )

    run_sql_pipeline(
        duck_adapter,
        {
            "tables": [{"name": "orders", "alias": "orders"}],
            "business_rules": [rule_name],
            "keep_all_columns": True,
        },
        "gold.resolved_orders",
        context=_context(),
        resolve_table=lambda name: f"sandbox.{name}",
    )

    cursor = duck_adapter.read_table("gold.resolved_orders")
    assert [column[0] for column in cursor.description] == ["order_id", "status"]
    assert cursor.fetchall() == [(1, "READY")]


def test_incremental_target_name_containing_a_dot_is_not_split_into_a_catalog():
    """A quoted table name that contains a dot must stay one part.

    Stripping quotes and splitting on every dot turned `gold`.`my.table` into a
    three-part name, inventing catalog 'gold' — and DuckDB then refused it for
    carrying a catalog it never had. The failure surfaced only for names with a
    dot, so it would have reached whoever has one and nobody else.
    """
    assert _target_parts("gold.orders") == (None, "gold", "orders")
    assert _target_parts("`gold`.`my.table`") == (None, "gold", "my.table")
    assert _target_parts("cat.gold.orders") == ("cat", "gold", "orders")
