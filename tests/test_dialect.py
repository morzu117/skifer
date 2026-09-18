"""Tests for the optional SQL dialect boundary."""

from pathlib import Path
import subprocess
import sys

import pytest

from skifer.core.dialect import (
    DialectError,
    SUPPORTED_DIALECTS,
    quote_fqn,
    quote_ident,
    transpile,
)
from skifer.core.ir import parse_to_ir
from skifer.core.schema_loader import parse_schema
from skifer.core.sql_compiler import (
    compile_select,
    quote_fqn as quote_pivot_fqn,
    quote_ident as quote_pivot_ident,
)


def test_core_import_and_databricks_path_do_not_require_sqlglot():
    project_root = Path(__file__).resolve().parents[1]
    script = r'''
import importlib.abc
import sys

class BlockSqlglot(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "sqlglot" or fullname.startswith("sqlglot."):
            raise ModuleNotFoundError("optional sqlglot dependency blocked", name=fullname)
        return None

sys.meta_path.insert(0, BlockSqlglot())
from skifer.core.dialect import DialectError, transpile

sql = "SELECT  1\n-- preserve bytes"
assert transpile(sql, target="databricks") == sql
assert "sqlglot" not in sys.modules
assert not any(name.startswith("sqlglot.") for name in sys.modules)

try:
    transpile(sql, target="duckdb")
except DialectError as exc:
    assert 'pip install -e ".[sql]"' in str(exc)
else:
    raise AssertionError("duckdb transpilation unexpectedly succeeded without sqlglot")
'''

    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=project_root,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stdout + completed.stderr


@pytest.mark.parametrize("target", ["duckdb", "snowflake", "bigquery"])
def test_compile_select_output_transpiles_and_reparses(target):
    sqlglot = pytest.importorskip("sqlglot")
    schema = parse_schema(
        """
tables:
  - name: silver.orders
    alias: ord
    filter:
      - "status:equals:COMPLETE"
select_final:
  - [customer_id, customer_id]
  - [amount, amount_rounded, [cast:double, round:2]]
"""
    )
    pivot_sql = compile_select(parse_to_ir(schema))

    translated = transpile(pivot_sql, target=target)

    assert len(sqlglot.parse(translated, read=target)) == 1


def test_unknown_target_lists_supported_values():
    with pytest.raises(DialectError) as raised:
        transpile("SELECT 1", target="postgres")

    message = str(raised.value)
    assert "postgres" in message
    assert ", ".join(sorted(SUPPORTED_DIALECTS)) in message


@pytest.mark.parametrize("sql", ["", "SELECT 1; SELECT 2"])
def test_zero_or_multiple_statements_are_rejected(sql):
    pytest.importorskip("sqlglot")

    with pytest.raises(DialectError, match="exactly one"):
        transpile(sql, target="duckdb")


@pytest.mark.parametrize("name", ["plain", "we`ird"])
def test_databricks_identifier_quoting_matches_pivot(name):
    assert quote_ident(name, target="databricks") == quote_pivot_ident(name)


@pytest.mark.parametrize(
    "fqn",
    ["schema.table", "catalog.schema.table", "already.`quoted`"],
)
def test_databricks_fqn_quoting_matches_pivot(fqn):
    assert quote_fqn(fqn, target="databricks") == quote_pivot_fqn(fqn)


@pytest.mark.parametrize(
    ("target", "name", "expected"),
    [
        ("duckdb", 'odd"name', '"odd""name"'),
        ("snowflake", 'odd"name', '"odd""name"'),
        ("bigquery", "odd`name", "`odd``name`"),
    ],
)
def test_non_databricks_identifier_quoting_escapes_dialect_quote(
    target, name, expected
):
    pytest.importorskip("sqlglot")

    assert quote_ident(name, target=target) == expected


@pytest.mark.parametrize(
    ("target", "fqn", "expected"),
    [
        ("duckdb", 'sales.odd"table', '"sales"."odd""table"'),
        ("duckdb", 'catalog.sales.odd"table', '"catalog"."sales"."odd""table"'),
        ("snowflake", 'sales.odd"table', '"sales"."odd""table"'),
        (
            "snowflake",
            'catalog.sales.odd"table',
            '"catalog"."sales"."odd""table"',
        ),
        ("bigquery", "sales.odd`table", "`sales`.`odd``table`"),
        (
            "bigquery",
            "catalog.sales.odd`table",
            "`catalog`.`sales`.`odd``table`",
        ),
    ],
)
def test_non_databricks_fqn_quoting_handles_two_and_three_parts(
    target, fqn, expected
):
    pytest.importorskip("sqlglot")

    assert quote_fqn(fqn, target=target) == expected
