"""`skifer compile` — the SQL a pipeline would run, without executing anything.

The command opens no connection. Everything here therefore also guards what it
must *refuse*: a compiler that cannot read a catalog and emits plausible SQL
anyway is worse than one that stops, because plausible SQL gets pasted.
"""

from __future__ import annotations

import builtins
from pathlib import Path

import pytest

from skifer.cli import (
    COMPILE_EXIT_ERROR,
    COMPILE_EXIT_OK,
    COMPILE_EXIT_REFUSAL,
    run_compile,
)
from skifer.core.ir import parse_to_ir
from skifer.core.schema_loader import parse_schema
from skifer.core.sql_compiler import compile_select


SIMPLE_PIPELINE = """
tables:
  - name: silver.orders
    alias: ord
    filter:
      - "status:equals:DONE"
select_final:
  - [order_id, order_id]
  - [amount, amount_eur, [cast:double, round:2]]
"""


def _write(tmp_path: Path, text: str, name: str = "pipeline.yaml") -> str:
    path = tmp_path / name
    path.write_text(text, encoding="utf-8")
    return str(path)


@pytest.mark.parametrize("target", ["databricks", "duckdb", "snowflake", "bigquery"])
def test_compile_emits_sql_for_every_target(tmp_path, capsys, target):
    code = run_compile(_write(tmp_path, SIMPLE_PIPELINE), target)

    captured = capsys.readouterr()
    assert code == COMPILE_EXIT_OK
    assert "SELECT" in captured.out
    assert captured.err == ""


def test_databricks_target_is_the_pivot_verbatim(tmp_path, capsys):
    """The pivot must never traverse sqlglot: a materialized view's definition
    hash is computed on this exact text, so a re-rendering would change it."""
    path = _write(tmp_path, SIMPLE_PIPELINE)
    pivot = compile_select(
        parse_to_ir(parse_schema(SIMPLE_PIPELINE)), persisted_definition=False
    )

    run_compile(path, "databricks")
    databricks = capsys.readouterr().out.rstrip("\n")

    run_compile(path, "snowflake")
    snowflake = capsys.readouterr().out.rstrip("\n")

    assert databricks == pivot
    assert snowflake != pivot


def test_sql_goes_to_stdout_and_diagnostics_never_do(tmp_path, capsys):
    """Redirecting stdout to a file must yield a usable .sql, so no diagnostic
    may share that stream — including the sentinel warning."""
    pipeline = """
tables:
  - name: "{{ catalog }}.silver.orders"
    alias: ord
select_final:
  - [order_id, order_id]
"""
    code = run_compile(_write(tmp_path, pipeline), "duckdb")

    captured = capsys.readouterr()
    assert code == COMPILE_EXIT_OK
    assert "[compile]" in captured.err
    assert "[compile]" not in captured.out
    assert captured.out.lstrip().startswith(("WITH", "SELECT"))


def test_sentinel_warning_is_silent_when_the_yaml_has_no_placeholder(tmp_path, capsys):
    """A warning that fires on every file is the one nobody reads on the file
    that needed it."""
    run_compile(_write(tmp_path, SIMPLE_PIPELINE), "duckdb")

    assert capsys.readouterr().err == ""


def test_unregistered_rule_is_refused_without_the_connection_hint(tmp_path, capsys):
    """The refusal must name its own cause.

    An unimported rule has nothing to do with reading a catalog, so the hint about
    opening a connection must not be appended to it — that is how an error message
    sends someone looking in the wrong place.
    """
    pipeline = """
tables:
  - name: silver.orders
    alias: ord
business_rules:
  - not_imported_anywhere
keep_all_columns: true
"""
    code = run_compile(_write(tmp_path, pipeline), "duckdb")

    captured = capsys.readouterr()
    assert code == COMPILE_EXIT_REFUSAL
    assert "not_imported_anywhere" in captured.err
    assert "opens no connection" not in captured.err
    assert captured.out == ""


def test_file_source_is_refused_by_name(tmp_path, capsys):
    pipeline = """
tables:
  - name: raw_orders
    alias: raw
    source:
      type: csv
      path: /tmp/orders.csv
select_final:
  - [order_id, order_id]
"""
    code = run_compile(_write(tmp_path, pipeline), "duckdb")

    captured = capsys.readouterr()
    assert code == COMPILE_EXIT_REFUSAL
    assert "csv" in captured.err
    assert captured.out == ""


def test_view_compiles_as_a_persisted_definition(tmp_path, capsys):
    """A view's definition is re-evaluated on every read, so a development limit
    frozen into it would truncate every future read, not one run."""
    pipeline = """
materialization:
  type: view
dev_limit: 10
tables:
  - name: silver.orders
    alias: ord
select_final:
  - [order_id, order_id]
"""
    code = run_compile(_write(tmp_path, pipeline), "duckdb")

    captured = capsys.readouterr()
    assert code == COMPILE_EXIT_REFUSAL
    assert "dev_limit" in captured.err


def test_missing_file_is_a_technical_error_not_a_refusal(tmp_path, capsys):
    code = run_compile(str(tmp_path / "absent.yaml"), "duckdb")

    assert code == COMPILE_EXIT_ERROR
    assert capsys.readouterr().out == ""


def test_unknown_environment_names_the_declared_ones(tmp_path, capsys):
    code = run_compile(_write(tmp_path, SIMPLE_PIPELINE), "duckdb", env="NOPE")

    captured = capsys.readouterr()
    assert code == COMPILE_EXIT_ERROR
    assert "NOPE" in captured.err


def test_compile_never_imports_pyspark(tmp_path, capsys, monkeypatch):
    """The command must stay usable where Spark is not installed at all."""
    real_import = builtins.__import__

    def guarded(name, *args, **kwargs):
        if name == "pyspark" or name.startswith("pyspark."):
            raise AssertionError(f"'skifer compile' imported {name!r}")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded)
    code = run_compile(_write(tmp_path, SIMPLE_PIPELINE), "snowflake")

    assert code == COMPILE_EXIT_OK
    assert "SELECT" in capsys.readouterr().out
