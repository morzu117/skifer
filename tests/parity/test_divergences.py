"""Reproducible SQL-only evidence for the conditional DuckDB capabilities."""

from decimal import Decimal
import logging

import pytest

from skifer.core.ir import parse_to_ir
from skifer.core.schema_loader import parse_schema
from skifer.core.sql_compiler import compile_select


def test_cast_integer_rounding_diverges(parity_spark, duck):
    """op:cast: Spark truncates 2.7 to 2 while DuckDB rounds it to 3."""
    parsed = parse_to_ir(parse_schema('''
 tables:
   - name: orders
     alias: ord
     filter: ["unit_price:equals:2.7"]
 select_final:
   - [unit_price, integer_price, ["cast:int"]]
'''))
    spark_values = {row[0] for row in parity_spark.sql(compile_select(parsed)).collect()}
    duck_values = {row[0] for row in duck.execute(compile_select(parsed, dialect="duckdb")).fetchall()}
    assert spark_values == {2}
    assert duck_values == {3}


def test_decimal_average_types_diverge(parity_spark, duck):
    """agg:avg: DECIMAL(12,2) averages return Spark Decimal(s=6), DuckDB float."""
    parsed = parse_to_ir(parse_schema('''
 tables:
   - name: orders
     alias: ord
 aggregate:
   group_by: [status]
   measures:
     - [amount, mean_amount, avg]
'''))
    spark_values = dict(parity_spark.sql(compile_select(parsed)).collect())
    duck_values = dict(duck.execute(compile_select(parsed, dialect="duckdb")).fetchall())
    assert spark_values.keys() == duck_values.keys()
    for status, value in spark_values.items():
        assert isinstance(value, Decimal)
        assert value.as_tuple().exponent == -6
        assert isinstance(duck_values[status], float)


def test_cast_failure_semantics_diverge(parity_spark, duck, monkeypatch):
    """op:cast: invalid text raises on Spark ANSI but becomes NULL via DuckDB TRY_CAST."""
    from pyspark.logger import PySparkLogger

    parsed = parse_to_ir(parse_schema('''
 tables:
   - name: orders
     alias: ord
     filter: ["note:is_not_null"]
 select_final:
   - [note, note_int, ["cast:int"]]
'''))
    sql_spark = compile_select(parsed)
    sql_duckdb = compile_select(parsed, dialect="duckdb")
    assert "CAST(" in sql_spark
    assert "TRY_CAST" in sql_duckdb
    # PySpark's non-propagating error logger reformats handlers attached by pytest.
    # Restore the registry before teardown so later tests keep their capture format.
    with monkeypatch.context() as loggers:
        loggers.setitem(
            logging.Logger.manager.loggerDict,
            "SQLQueryContextLogger",
            PySparkLogger("SQLQueryContextLogger"),
        )
        with pytest.raises(Exception):
            parity_spark.sql(sql_spark).collect()
    duck_rows = duck.execute(sql_duckdb).fetchall()
    assert duck_rows
    assert all(row == (None,) for row in duck_rows)
