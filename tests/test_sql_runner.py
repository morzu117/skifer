"""Tests for DuckDB's end-to-end compiled SQL execution path."""

from __future__ import annotations

import importlib.abc
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
from skifer.core.ir import parse_to_ir
from skifer.core.registry import RuleRegistry
from skifer.core.sql_compiler import SqlCompilationError, compile_select
from skifer.core.sql_runner import run_sql_pipeline


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
