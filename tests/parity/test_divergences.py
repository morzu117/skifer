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


# --- Plan 47.5: value-conditioned divergences (R5) -------------------------------------

def _isolated_spark_loggers(monkeypatch):
    """PySpark's error loggers reformat pytest's handlers; restore them at teardown."""
    from pyspark.logger import PySparkLogger

    for name in ("SQLQueryContextLogger", "DataFrameQueryContextLogger"):
        monkeypatch.setitem(logging.Logger.manager.loggerDict, name, PySparkLogger(name))


@pytest.fixture
def labels(parity_spark, duck):
    """A text column holding the LIKE metacharacters the canonical dataset lacks."""
    rows = [(1, "100%"), (2, "100x"), (3, "a\\b"), (4, "ab")]
    parity_spark.createDataFrame(rows, "label_id INT, label STRING").createOrReplaceTempView(
        "labels"
    )
    duck.execute('CREATE TABLE "labels" ("label_id" INTEGER, "label" VARCHAR)')
    duck.executemany('INSERT INTO "labels" VALUES (?, ?)', rows)
    try:
        yield
    finally:
        parity_spark.catalog.dropTempView("labels")
        duck.execute('DROP TABLE "labels"')


def _label_filter(flt):
    return parse_to_ir(parse_schema(f'''
 tables:
   - name: labels
     alias: lbl
     filter: ['{flt}']
 select_final:
   - [label_id, label_id]
'''))


def _duckdb_sql_bypassing_the_refusal(parsed):
    """The refused key never compiles for DuckDB; render the Spark SQL to observe it."""
    from skifer.core.dialect import render
    from skifer.core.sql_compiler import SqlCompilationError

    with pytest.raises(SqlCompilationError, match="'literal:like_backslash'"):
        compile_select(parsed, dialect="duckdb")
    return render(compile_select(parsed), "duckdb")


def test_decimal_text_against_an_integer_column_diverges(parity_spark, duck, monkeypatch):
    """literal:decimal_text: Spark (ANSI) raises on '6.5' vs INT; DuckDB rounds it to 7."""
    parsed = parse_to_ir(parse_schema('''
 tables:
   - name: orders
     alias: ord
     filter: ["customer_id:greater_than:6.5"]
 select_final:
   - [customer_id, customer_id]
'''))
    duck_values = {row[0] for row in duck.execute(compile_select(parsed, dialect="duckdb")).fetchall()}
    assert duck_values == {8, 99}  # 7 > 6.5, yet DuckDB compares with 7 and drops it
    with monkeypatch.context() as loggers:
        _isolated_spark_loggers(loggers)
        with pytest.raises(Exception, match="CAST_INVALID_INPUT|cannot be cast"):
            parity_spark.sql(compile_select(parsed)).collect()


def test_like_escaped_percent_diverges(parity_spark, duck, labels):
    r"""literal:like_backslash: '100\%' matches '100%' on Spark, nothing on DuckDB."""
    parsed = _label_filter("label:like:100\\%")
    spark_ids = {row[0] for row in parity_spark.sql(compile_select(parsed)).collect()}
    duck_ids = {row[0] for row in duck.execute(_duckdb_sql_bypassing_the_refusal(parsed)).fetchall()}
    assert spark_ids == {1}
    assert duck_ids == set()


def test_contains_backslash_diverges(parity_spark, duck, labels, monkeypatch):
    r"""literal:like_backslash: 'a\b' raises on Spark (bad escape), matches one row on DuckDB."""
    parsed = _label_filter("label:contains:a\\b")
    duck_ids = {row[0] for row in duck.execute(_duckdb_sql_bypassing_the_refusal(parsed)).fetchall()}
    assert duck_ids == {3}
    with monkeypatch.context() as loggers:
        _isolated_spark_loggers(loggers)
        with pytest.raises(Exception, match="(?i)escape"):
            parity_spark.sql(compile_select(parsed)).collect()


def test_selecting_the_right_join_key_diverges(parity_spark, duck, monkeypatch):
    """Plan 47.4: with differently named keys the SQL path keeps the right key; the
    DataFrame path drops it. SELECT * over such a join is refused; an explicit pick of
    the right key still works in SQL (Spark SQL and DuckDB) and fails in DataFrame."""
    from pyspark.logger import PySparkLogger

    from skifer.core.spark_backend import SparkBackend
    from tests.test_sql_compiler import _make_engine

    yaml_text = '''
 tables:
   - name: orders
     alias: ord
   - name: customers
     alias: cust
 join:
   - table_from: [ord, country]
     table_to: [cust, cust_country]
     type: inner
 select_final:
   - [order_id, order_id]
   - [cust_country, cust_country]
'''
    schema = parse_schema(yaml_text)
    parsed = parse_to_ir(schema)
    spark_rows = parity_spark.sql(compile_select(parsed)).collect()
    duck_rows = duck.execute(compile_select(parsed, dialect="duckdb")).fetchall()
    assert spark_rows and {row[1] for row in spark_rows} <= {"FR", "DE", "IT", "ES"}
    assert len(duck_rows) == len(spark_rows)
    # A DataFrame AnalysisException goes through PySpark's DataFrameQueryContextLogger,
    # which reformats pytest's handlers like SQLQueryContextLogger does: isolate both.
    with monkeypatch.context() as loggers:
        for name in ("SQLQueryContextLogger", "DataFrameQueryContextLogger"):
            loggers.setitem(logging.Logger.manager.loggerDict, name, PySparkLogger(name))
        with pytest.raises(Exception, match="cust_country"):
            _make_engine(SparkBackend(spark=parity_spark, is_local=True)).process_schema(
                schema
            ).collect()
