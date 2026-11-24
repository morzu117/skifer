"""Compare executed values (not SQL types) across DataFrame, Spark SQL and DuckDB."""

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from itertools import zip_longest

from skifer.core.ir import parse_to_ir
from skifer.core.schema_loader import parse_schema
from skifer.core.sql_compiler import compile_select


@dataclass(frozen=True)
class Outcome:
    columns: tuple[str, ...]
    rows_dataframe: list[tuple]
    rows_spark_sql: list[tuple]
    rows_duckdb: list[tuple]
    sql_spark: str
    sql_duckdb: str


def _normalize(value):
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, (int, float, Decimal)):
        return Decimal(str(value)).quantize(Decimal("1e-9"))
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, str):
        return value
    raise AssertionError(f"Unsupported parity value type: {type(value).__name__}")


def _rows(rows, columns, ordered):
    indices = [columns.index(column) for column in ordered]
    normalized = [tuple(_normalize(row[i]) for i in indices) for row in rows]
    return sorted(normalized, key=lambda row: tuple((v is None, str(v)) for v in row))


def run_three_way(spark, duck, yaml_text) -> Outcome:
    from skifer.core.spark_backend import SparkBackend
    from tests.test_sql_compiler import _make_engine

    schema = parse_schema(yaml_text)
    parsed = parse_to_ir(schema)
    sql_spark = compile_select(parsed)
    sql_duckdb = compile_select(parsed, dialect="duckdb")
    sqls = f"Spark SQL:\n{sql_spark}\nDuckDB SQL:\n{sql_duckdb}"
    dataframe = _make_engine(SparkBackend(spark=spark, is_local=True)).process_schema(schema)
    spark_sql = spark.sql(sql_spark)
    duck_result = duck.execute(sql_duckdb)
    duck_columns = [column[0] for column in duck_result.description]
    columns = tuple(sorted(dataframe.columns))
    for route, actual in (("Spark SQL", spark_sql.columns), ("DuckDB SQL", duck_columns)):
        assert sorted(actual) == list(columns), (
            f"{route} columns differ from Spark DataFrame: {actual!r} != {columns!r}\n{sqls}"
        )
    return Outcome(
        columns,
        _rows(dataframe.collect(), dataframe.columns, columns),
        _rows(spark_sql.collect(), spark_sql.columns, columns),
        _rows(duck_result.fetchall(), duck_columns, columns),
        sql_spark,
        sql_duckdb,
    )


def assert_three_way(spark, duck, yaml_text, *, approx_columns=()):
    outcome = run_three_way(spark, duck, yaml_text)
    assert set(approx_columns) <= set(outcome.columns)
    for route, rows in (("Spark SQL", outcome.rows_spark_sql), ("DuckDB SQL", outcome.rows_duckdb)):
        if rows == outcome.rows_dataframe:
            continue
        differences = []
        for expected, actual in zip_longest(outcome.rows_dataframe, rows):
            if expected is None or actual is None:
                differences.append((expected, actual))
                continue
            for column, a, b in zip(outcome.columns, actual, expected):
                if a == b:
                    continue
                if (column in approx_columns and isinstance(a, Decimal) and isinstance(b, Decimal)
                        and abs(a - b) <= max(Decimal(1), Decimal("0.05") * abs(b))):
                    continue
                differences.append((expected, actual))
                break
        assert not differences, (
            f"{route} differs from Spark DataFrame; columns={outcome.columns!r}\n"
            f"First differing rows (DataFrame, {route}): {differences[:5]!r}\n"
            f"Spark SQL:\n{outcome.sql_spark}\nDuckDB SQL:\n{outcome.sql_duckdb}"
        )
    return outcome
