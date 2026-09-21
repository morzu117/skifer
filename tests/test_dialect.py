"""Tests for the optional SQL dialect boundary."""

from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest

from skifer.core.dialect import (
    DialectError,
    SUPPORTED_DIALECTS,
    quote_fqn,
    quote_ident,
    transpile,
)
from skifer.core import dialect
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
    [
        "schema.table",
        "catalog.schema.table",
        "already.`quoted`",
        "`my.catalog`.`sales`",
        "`odd``catalog`.`sales`",
        "`unbalanced",
    ],
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
def test_sqlglot_identifier_quote_character_behavior_is_preserved(
    target, name, expected
):
    pytest.importorskip("sqlglot")

    # This freezes sqlglot's rendering, not syntax verified against each target
    # engine; quote characters inside identifiers require a live warehouse test.
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
def test_sqlglot_fqn_quote_character_behavior_is_preserved(
    target, fqn, expected
):
    pytest.importorskip("sqlglot")

    # This freezes sqlglot's rendering, not syntax verified against each target
    # engine; quote characters inside identifiers require a live warehouse test.
    assert quote_fqn(fqn, target=target) == expected


@pytest.mark.parametrize(
    ("target", "fqn", "expected"),
    [
        ("duckdb", "`sales`.`orders`", '"sales"."orders"'),
        ("duckdb", "`catalog`.`sales`.`orders`", '"catalog"."sales"."orders"'),
        ("snowflake", "`sales`.`orders`", '"sales"."orders"'),
        ("snowflake", "`catalog`.`sales`.`orders`", '"catalog"."sales"."orders"'),
        ("bigquery", "`sales`.`orders`", "`sales`.`orders`"),
        ("bigquery", "`catalog`.`sales`.`orders`", "`catalog`.`sales`.`orders`"),
    ],
)
def test_pivot_quoted_fqn_is_requoted_without_embedded_backticks(
    target, fqn, expected
):
    pytest.importorskip("sqlglot")

    assert quote_fqn(fqn, target=target) == expected


@pytest.mark.parametrize(
    ("target", "expected"),
    [
        ("duckdb", '"my.catalog"."sales"'),
        ("snowflake", '"my.catalog"."sales"'),
        ("bigquery", "`my.catalog`.`sales`"),
    ],
)
def test_pivot_quoted_literal_dot_stays_in_one_fqn_part(target, expected):
    pytest.importorskip("sqlglot")

    assert quote_fqn("`my.catalog`.`sales`", target=target) == expected


@pytest.mark.parametrize(
    ("target", "expected"),
    [
        ("duckdb", '"odd`catalog"."sales"'),
        ("snowflake", '"odd`catalog"."sales"'),
        ("bigquery", "`odd``catalog`.`sales`"),
    ],
)
def test_pivot_doubled_backtick_is_unescaped_once_then_requoted(target, expected):
    pytest.importorskip("sqlglot")

    assert quote_fqn("`odd``catalog`.`sales`", target=target) == expected


@pytest.mark.parametrize("target", ["duckdb", "snowflake", "bigquery"])
def test_unbalanced_pivot_fqn_quoting_is_rejected(target):
    pytest.importorskip("sqlglot")

    with pytest.raises(DialectError, match="Unbalanced backtick quoting"):
        quote_fqn("`catalog.sales", target=target)


@pytest.mark.parametrize("operation", ["transpile", "quote_ident", "quote_fqn"])
def test_non_sqlglot_errors_are_not_wrapped(monkeypatch, operation):
    class FakeSqlglotError(Exception):
        pass

    class BrokenIdentifier:
        def __init__(self, **kwargs):
            raise AttributeError("programming error")

    def broken_transpile(*args, **kwargs):
        raise AttributeError("programming error")

    fake_sqlglot = SimpleNamespace(transpile=broken_transpile)
    fake_exp = SimpleNamespace(Identifier=BrokenIdentifier)
    monkeypatch.setattr(
        dialect,
        "_import_sqlglot",
        lambda: (
            fake_sqlglot,
            fake_exp,
            SimpleNamespace(RAISE="raise"),
            FakeSqlglotError,
        ),
    )

    with pytest.raises(AttributeError, match="programming error"):
        if operation == "transpile":
            transpile("SELECT 1", target="duckdb")
        elif operation == "quote_ident":
            quote_ident("orders", target="duckdb")
        else:
            quote_fqn("sales.orders", target="duckdb")
