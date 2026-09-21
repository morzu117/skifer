"""Construction-level result equivalence between Spark and compiled DuckDB SQL.

Both paths consume the same physical input: shared file paths for file-source cases,
and matching managed tables otherwise.  The comparison is deliberately shared by
every deterministic case in this module.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from decimal import Decimal
import math
from numbers import Number
from uuid import uuid4

import pytest
import yaml

# `duckdb` comes with the optional `[sql]` extra. `pip install -e ".[dev]"` — the
# command CLAUDE.md documents — does not pull it, and a module-level import then
# stopped pytest at collection: a contributor got a broken suite rather than a
# smaller one. The two modules that import helpers from here skip with it.
duckdb = pytest.importorskip("duckdb")
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from skifer.core.context import ExecutionContext
from skifer.core.adapters.duckdb import DuckDBAdapter
from skifer.core.interpreter import SchemaInterpreter
from skifer.core.registry import RuleRegistry
from skifer.core.schema_loader import parse_schema
from skifer.core.spark_backend import SparkBackend
from skifer.core.sql_runner import run_sql_pipeline


NUMERIC_REL_TOLERANCE = Decimal("1e-9")
NUMERIC_ABS_TOLERANCE = Decimal("1e-9")


@dataclass(frozen=True)
class EquivalenceRuntime:
    spark: object
    duck: duckdb.DuckDBPyConnection
    namespace: str

    def table(self, name: str) -> str:
        return f"{self.namespace}.{name}"


def _spark_type(name: str):
    return {
        "DOUBLE": DoubleType(),
        "INTEGER": IntegerType(),
        "VARCHAR": StringType(),
    }[name]


def _create_table(runtime, name, columns, rows):
    spark_schema = StructType(
        [StructField(column, _spark_type(dtype), True) for column, dtype in columns]
    )
    runtime.spark.createDataFrame(rows, spark_schema).write.format("delta").mode(
        "overwrite"
    ).saveAsTable(runtime.table(name))

    definitions = ", ".join(f'"{column}" {dtype}' for column, dtype in columns)
    placeholders = ", ".join("?" for _ in columns)
    runtime.duck.execute(f'CREATE TABLE "{runtime.namespace}"."{name}" ({definitions})')
    runtime.duck.executemany(
        f'INSERT INTO "{runtime.namespace}"."{name}" VALUES ({placeholders})', rows
    )


@pytest.fixture(scope="module")
def equivalence_runtime(spark):
    """Share exactly one Spark session and one DuckDB connection for the module."""
    connection = duckdb.connect()
    namespace = f"sql_spark_equivalence_{uuid4().hex}"
    runtime = EquivalenceRuntime(spark=spark, duck=connection, namespace=namespace)
    spark.sql(f"CREATE DATABASE `{namespace}`")
    connection.execute(f'CREATE SCHEMA "{namespace}"')

    _create_table(
        runtime,
        "filter_values",
        [
            ("id", "INTEGER"),
            ("text_value", "VARCHAR"),
            ("amount", "DOUBLE"),
            ("nullable_text", "VARCHAR"),
        ],
        [
            (1, "alpha", 10.0, None),
            (2, "alphabet", 20.0, "x"),
            (3, "beta", 30.0, ""),
            (4, "ALPHA", 40.0, None),
            (5, "a%b", 50.0, " y "),
            (6, None, None, "z"),
        ],
    )
    _create_table(
        runtime,
        "operation_values",
        [
            ("id", "INTEGER"),
            ("num_text", "VARCHAR"),
            ("number_value", "DOUBLE"),
            ("padded", "VARCHAR"),
            ("maybe_null", "VARCHAR"),
            ("token", "VARCHAR"),
            ("date_text", "VARCHAR"),
            ("status", "VARCHAR"),
        ],
        [
            (1, "2.65", 2.65, "  MiXeD  ", None, "aa-bb-cc", "2026-02-28", "DONE"),
            (2, "-1.25", -1.25, " spaced ", "kept", "x--z", "2024-02-29", "PENDING"),
            (3, None, None, None, "", None, None, None),
        ],
    )
    _create_table(
        runtime,
        "aggregate_values",
        [
            ("group_key", "VARCHAR"),
            ("value", "DOUBLE"),
            ("stable_value", "VARCHAR"),
        ],
        [
            ("A", 1.0, "first-last-A"),
            ("A", 1.0, "first-last-A"),
            ("A", 3.0, "first-last-A"),
            ("B", None, "first-last-B"),
            ("B", 2.0, "first-last-B"),
            ("C", None, "first-last-C"),
        ],
    )
    _create_table(
        runtime,
        "join_left_same",
        [("id", "INTEGER"), ("left_value", "VARCHAR")],
        [(1, "left-one"), (2, "left-two"), (None, "left-null")],
    )
    _create_table(
        runtime,
        "join_right_same",
        [("id", "INTEGER"), ("right_value", "VARCHAR")],
        [(2, "right-two"), (3, "right-three"), (None, "right-null")],
    )
    _create_table(
        runtime,
        "join_left_different",
        [("left_id", "INTEGER"), ("left_value", "VARCHAR")],
        [(1, "left-one"), (2, "left-two"), (None, "left-null")],
    )
    _create_table(
        runtime,
        "join_right_different",
        [("right_id", "INTEGER"), ("right_value", "VARCHAR")],
        [(2, "right-two"), (3, "right-three"), (None, "right-null")],
    )
    # Its `right_id` column deliberately collides with the *right* table's join
    # key name. Dropping the joined key by bare name would remove this one too,
    # which the DataFrame path keeps — the case that separates a qualified
    # `EXCEPT` from a bare one.
    _create_table(
        runtime,
        "join_left_colliding",
        [("outer_key", "INTEGER"), ("right_id", "VARCHAR"), ("left_value", "VARCHAR")],
        [(2, "kept-two", "left-two"), (3, "kept-three", "left-three")],
    )
    _create_table(
        runtime,
        "duplicate_values",
        [("event_key", "INTEGER"), ("value", "VARCHAR")],
        [(1, "first"), (1, "second"), (2, "only"), (None, "null-a"), (None, "null-b")],
    )

    try:
        yield runtime
    finally:
        spark.sql(f"DROP DATABASE IF EXISTS `{namespace}` CASCADE")
        connection.close()


def _normalized_schema(raw_schema):
    """Run ordinary cases through the same YAML normalization as production."""
    return parse_schema(yaml.safe_dump(raw_schema, sort_keys=False))


def _context(*, is_job=False, is_production=False):
    return ExecutionContext(
        env="local",
        config={"environments": {"local": {"is_production": is_production}}},
        is_job_execution=is_job,
        is_local=True,
    )


def _spark_result(runtime, schema, *, context=None):
    backend = SparkBackend(spark=runtime.spark, is_local=True)
    interpreter = SchemaInterpreter(backend=backend, context=context or _context())
    return interpreter.process_schema(schema)


def _duck_result(runtime, schema, *, context=None):
    adapter = DuckDBAdapter(connection=runtime.duck)
    target = runtime.table(f"result_{uuid4().hex}")
    run_sql_pipeline(
        adapter,
        schema,
        target,
        context=context or _context(),
    )
    try:
        cursor = adapter.read_table(target)
        return [description[0] for description in cursor.description], cursor.fetchall()
    finally:
        adapter.drop_table(target)


def _is_nan(value):
    try:
        return bool(value.is_nan()) if isinstance(value, Decimal) else math.isnan(value)
    except (TypeError, ValueError):
        return False


def _sort_cell(value):
    if value is None:
        return (0, "")
    if _is_nan(value):
        return (1, "NaN")
    if isinstance(value, Number) and not isinstance(value, bool):
        return (2, float(value))
    return (3, type(value).__name__, repr(value))


def _assert_cell_equal(spark_value, duck_value):
    # SQL NULL and numeric NaN are different states.  Each is compared explicitly;
    # in particular, this never turns a NULL into a NaN or treats the two as equal.
    if spark_value is None or duck_value is None:
        assert spark_value is None and duck_value is None
        return
    if _is_nan(spark_value) or _is_nan(duck_value):
        assert _is_nan(spark_value) and _is_nan(duck_value)
        return

    if (
        isinstance(spark_value, Number)
        and not isinstance(spark_value, bool)
        and isinstance(duck_value, Number)
        and not isinstance(duck_value, bool)
    ):
        left = Decimal(str(spark_value))
        right = Decimal(str(duck_value))
        difference = abs(left - right)
        allowed = max(
            NUMERIC_ABS_TOLERANCE,
            NUMERIC_REL_TOLERANCE * max(abs(left), abs(right)),
        )
        # 1e-9 absorbs only representation noise from Decimal/float conversion and
        # sample statistics.  It is far below the 1e-2 boundaries used by these
        # pipelines, so it cannot hide a different rounded value or filter decision.
        assert difference <= allowed, f"{spark_value!r} != {duck_value!r} (tolerance {allowed})"
        return

    # Strings are intentionally exact: no case folding and no whitespace stripping.
    assert spark_value == duck_value


def assert_spark_duckdb_equivalent(spark_df, duck_columns, duck_rows):
    """Compare deterministic results by column name after a common full-row sort.

    Column order is irrelevant but names (including multiplicity) must match.  Both
    sides are reordered by sorted column name and their rows are then sorted by all
    output columns with one explicit NULL/NaN ordering before cell comparison.
    Numeric cells share the named Decimal/float tolerance documented above; text is
    never case- or whitespace-normalized.
    """
    spark_columns = list(spark_df.columns)
    assert Counter(spark_columns) == Counter(duck_columns)
    assert len(set(spark_columns)) == len(spark_columns), "ambiguous duplicate output columns"

    names = sorted(spark_columns)
    spark_rows = [tuple(row[name] for name in names) for row in spark_df.select(*names).collect()]
    duck_indexes = [duck_columns.index(name) for name in names]
    reordered_duck_rows = [tuple(row[index] for index in duck_indexes) for row in duck_rows]

    spark_rows.sort(key=lambda row: tuple(_sort_cell(value) for value in row))
    reordered_duck_rows.sort(key=lambda row: tuple(_sort_cell(value) for value in row))
    assert len(spark_rows) == len(reordered_duck_rows)
    for spark_row, duck_row in zip(spark_rows, reordered_duck_rows):
        for spark_value, duck_value in zip(spark_row, duck_row):
            _assert_cell_equal(spark_value, duck_value)


def _assert_schema_equivalent(runtime, schema, *, context=None):
    spark_df = _spark_result(runtime, schema, context=context)
    duck_columns, duck_rows = _duck_result(runtime, schema)
    assert_spark_duckdb_equivalent(spark_df, duck_columns, duck_rows)


def _file_source_schema(source_type, path, options=None):
    return _normalized_schema(
        {
            "tables": [
                {
                    "name": "physical_file",
                    "alias": "source",
                    "source": {
                        "type": source_type,
                        "path": str(path),
                        "options": options or {},
                    },
                }
            ],
            "keep_all_columns": True,
        }
    )


def test_csv_with_header_file_source_equivalence(equivalence_runtime, tmp_path):
    path = tmp_path / "with_header.csv"
    path.write_text("id,label\n1,alpha\n2,beta\n", encoding="utf-8")

    _assert_schema_equivalent(
        equivalence_runtime,
        _file_source_schema("csv", path, {"header": "true"}),
    )


def test_csv_without_header_file_source_equivalence(equivalence_runtime, tmp_path):
    path = tmp_path / "without_header.csv"
    path.write_text("id;label\n1;alpha\n2;beta\n", encoding="utf-8")

    _assert_schema_equivalent(
        equivalence_runtime,
        _file_source_schema("csv", path),
    )


def test_csv_nonstandard_separator_file_source_equivalence(
    equivalence_runtime, tmp_path
):
    path = tmp_path / "semicolon.csv"
    path.write_text("id;amount\n1;10.5\n2;20.25\n", encoding="utf-8")

    _assert_schema_equivalent(
        equivalence_runtime,
        _file_source_schema(
            "csv",
            path,
            {"header": "true", "inferSchema": "true", "sep": ";"},
        ),
    )


def test_csv_explicit_infer_schema_false_file_source_equivalence(
    equivalence_runtime, tmp_path
):
    path = tmp_path / "text_types.csv"
    path.write_text("id,active\n007,true\n", encoding="utf-8")

    _assert_schema_equivalent(
        equivalence_runtime,
        _file_source_schema(
            "csv", path, {"header": "true", "inferSchema": "false"}
        ),
    )


def test_multiline_json_file_source_equivalence(equivalence_runtime, tmp_path):
    path = tmp_path / "multiline.json"
    path.write_text(
        '[\n  {"id": 1, "label": "alpha"},\n  {"id": 2, "label": "beta"}\n]\n',
        encoding="utf-8",
    )

    _assert_schema_equivalent(
        equivalence_runtime,
        _file_source_schema("json", path, {"multiLine": "true"}),
    )


def test_newline_delimited_json_file_source_equivalence(
    equivalence_runtime, tmp_path
):
    path = tmp_path / "newline_delimited.json"
    path.write_text(
        '{"id": 1, "label": "alpha"}\n{"id": 2, "label": "beta"}\n',
        encoding="utf-8",
    )

    _assert_schema_equivalent(
        equivalence_runtime,
        _file_source_schema("json", path),
    )


def test_parquet_file_source_equivalence(equivalence_runtime, tmp_path):
    directory = tmp_path / "parquet_source"
    equivalence_runtime.spark.createDataFrame(
        [(1, "alpha"), (2, "beta")], ["id", "label"]
    ).coalesce(1).write.mode("overwrite").parquet(str(directory))
    path = next(directory.glob("part-*.parquet"))

    _assert_schema_equivalent(
        equivalence_runtime,
        _file_source_schema("parquet", path),
    )


def test_parquet_merge_schema_file_source_equivalence(
    equivalence_runtime, tmp_path
):
    directory = tmp_path / "merged_parquet_source"
    equivalence_runtime.spark.createDataFrame(
        [(1, "alpha")], ["id", "label"]
    ).coalesce(1).write.mode("overwrite").parquet(str(directory))
    equivalence_runtime.spark.createDataFrame(
        [(2, 20.5)], ["id", "amount"]
    ).coalesce(1).write.mode("append").parquet(str(directory))
    path = directory / "*.parquet"

    _assert_schema_equivalent(
        equivalence_runtime,
        _file_source_schema("parquet", path, {"mergeSchema": "true"}),
    )


FILTER_CASES = [
    ("equals", "text_value:equals:alpha"),
    ("not_equals", "text_value:not_equals:alpha"),
    ("greater_than", "amount:greater_than:20"),
    ("less_than", "amount:less_than:20"),
    ("greater_than_equal", "amount:greater_than_equal:20"),
    ("less_than_equal", "amount:less_than_equal:20"),
    ("in", "text_value:in:alpha,beta"),
    ("not_in", "text_value:not_in:alpha,beta"),
    ("between", "amount:between:20,40"),
    ("not_between", "amount:not_between:20,40"),
    ("is_null", "nullable_text:is_null"),
    ("is_not_null", "nullable_text:is_not_null"),
    ("contains", "text_value:contains:pha"),
    ("not_contains", "text_value:not_contains:pha"),
    ("starts_with", "text_value:starts_with:al"),
    ("ends_with", "text_value:ends_with:ta"),
    ("like", "text_value:like:a%a"),
    ("not_like", "text_value:not_like:a%a"),
    ("sql", "ignored:sql:amount >= 40"),
]


@pytest.mark.parametrize(("operator", "filter_expression"), FILTER_CASES, ids=lambda value: value)
def test_filter_operator_equivalence(equivalence_runtime, operator, filter_expression):
    schema = _normalized_schema(
        {
            "tables": [
                {
                    "name": equivalence_runtime.table("filter_values"),
                    "alias": "source",
                    "filter": [filter_expression],
                }
            ],
            "select_final": [["id", "id"], ["text_value", "text_value"]],
        }
    )
    _assert_schema_equivalent(equivalence_runtime, schema)


def test_filter_groups_equivalence(equivalence_runtime):
    schema = _normalized_schema(
        {
            "tables": [
                {
                    "name": equivalence_runtime.table("filter_values"),
                    "alias": "source",
                    "filter_groups": [
                        ["amount:greater_than_equal:40", "nullable_text:is_null"],
                        ["text_value:equals:beta", "nullable_text:is_not_null"],
                    ],
                }
            ],
            "select_final": [["id", "id"], ["text_value", "text_value"]],
        }
    )
    _assert_schema_equivalent(equivalence_runtime, schema)


def _join_schema(runtime, join_type, *, same_keys):
    if same_keys:
        tables = [
            {"name": runtime.table("join_left_same"), "alias": "left_side"},
            {"name": runtime.table("join_right_same"), "alias": "right_side"},
        ]
        left_key = right_key = "id"
        outputs = [["id", "id"], ["left_value", "left_value"], ["right_value", "right_value"]]
    else:
        tables = [
            {"name": runtime.table("join_left_different"), "alias": "left_side"},
            {"name": runtime.table("join_right_different"), "alias": "right_side"},
        ]
        left_key, right_key = "left_id", "right_id"
        outputs = [
            ["left_id", "left_id"],
            ["left_value", "left_value"],
            ["right_value", "right_value"],
        ]
    return _normalized_schema(
        {
            "tables": tables,
            "join": [
                {
                    "table_from": ["left_side", left_key],
                    "table_to": ["right_side", right_key],
                    "type": join_type,
                }
            ],
            "select_final": outputs,
        }
    )


@pytest.mark.parametrize(
    ("join_type", "same_keys"),
    [("inner", True), ("inner", False), ("left", False), ("right", False), ("full", True)],
)
def test_join_equivalence(equivalence_runtime, join_type, same_keys):
    _assert_schema_equivalent(
        equivalence_runtime,
        _join_schema(equivalence_runtime, join_type, same_keys=same_keys),
    )


def test_cross_join_equivalence(equivalence_runtime):
    """A declared key filters on both paths.

    Spark builds the equality condition and calls ``join(other, cond, "cross")``,
    which filters. Emitting a bare ``CROSS JOIN`` used to turn that into a cartesian
    product, so the same YAML returned a handful of rows on Spark and every pair of
    rows in SQL.
    """
    _assert_schema_equivalent(
        equivalence_runtime,
        _join_schema(equivalence_runtime, "cross", same_keys=False),
    )


def _aggregate_measures():
    """One measure per catalogued aggregate function, checked by a guard below."""
    return [
        ["value", "sum_value", "sum"],
        ["value", "avg_value", "avg"],
        ["value", "min_value", "min"],
        ["value", "max_value", "max"],
        ["*", "row_count", "count"],
        ["value", "distinct_count", "count_distinct"],
        ["value", "distinct_sum", "sum_distinct"],
        ["value", "approx_distinct_count", "approx_count_distinct"],
        ["value", "sample_stddev", "stddev"],
        ["value", "sample_variance", "variance"],
        ["stable_value", "first_value", "first"],
        ["stable_value", "last_value", "last"],
    ]


def test_every_aggregate_function_has_an_equivalence_case():
    """Same guard as for column operations, on the other hand-kept list."""
    from skifer.core.op_catalog import AGGREGATE_FUNCTIONS

    covered = {measure[2] for measure in _aggregate_measures()}
    missing = sorted(set(AGGREGATE_FUNCTIONS) - covered)
    assert not missing, (
        "Aggregate functions with no cross-engine equivalence case: " + ", ".join(missing)
    )


def test_every_filter_operator_has_an_equivalence_case():
    """FILTER_CASES is hand-kept too; an operator added to the catalog and not
    here would simply never be compared across engines."""
    from skifer.core.op_catalog import FILTER_OPERATORS

    covered = {operator for operator, _ in FILTER_CASES}
    missing = sorted(set(FILTER_OPERATORS) - covered)
    assert not missing, (
        "Filter operators with no cross-engine equivalence case: " + ", ".join(missing)
    )


def test_every_aggregate_and_having_equivalence(equivalence_runtime):
    schema = _normalized_schema(
        {
            "tables": [
                {"name": equivalence_runtime.table("aggregate_values"), "alias": "source"}
            ],
            "aggregate": {
                "group_by": ["group_key"],
                "measures": _aggregate_measures(),
                "having": ["row_count:greater_than_equal:2"],
            },
        }
    )
    _assert_schema_equivalent(equivalence_runtime, schema)


def _operation_specs():
    return [
        ["num_text", "cast_value", ["cast:double"]],
        ["number_value", "rounded_value", ["round:1"]],
        ["padded", "upper_value", ["upper"]],
        ["padded", "lower_value", ["lower"]],
        ["padded", "trimmed_value", ["trim"]],
        ["maybe_null", "coalesced_value", ["coalesce:fallback"]],
        ["maybe_null", "nvl_value", ["nvl:fallback"]],
        ["status", "literal_value", ["lit:fixed"]],
        ["padded", "substring_value", ["substring:2,3"]],
        ["token", "split_value", ["split:-,1"]],
        ["date_text", "date_value", ["to_date:yyyy-MM-dd"]],
        # `number_value` carries 2.65, -1.25 and NULL on purpose: CEIL of a
        # negative is where engines most often disagree, and NULL is where an
        # arithmetic op can quietly become 0.
        ["number_value", "abs_value", ["abs"]],
        ["number_value", "ceil_value", ["ceil"]],
        # `padded` keeps its surrounding spaces, which LENGTH must count.
        ["padded", "length_value", ["length"]],
        ["status", "aliased_value", ["col:padded"]],
        ["number_value", "expr_value", ["expr:number_value * 2"]],
        {
            "source": "status",
            "target": "status_label",
            "ops": [
                {"when": "equals:DONE", "then": "lit:Paid"},
                {"when": "equals:PENDING", "then": "lit:Waiting"},
                {"else": "lit:Unknown"},
            ],
        },
    ]


def test_every_column_operation_has_an_equivalence_case():
    """The specs list is maintained by hand, so it must be checked against the catalog.

    Four operations — `abs`, `ceil`, `length` and `col` — had been added to
    `COLUMN_OPS` and never to this list, so no test compared them across engines.
    Nothing failed, because nothing asked. `when`/`then`/`else` are structural
    keywords carried by the conditional entry rather than by an op string.

    This test needs no Spark session: it reads the specs, not the engines.
    """
    from skifer.core.op_catalog import COLUMN_OPS

    structural = {"when", "then", "else"}
    covered = set()
    for spec in _operation_specs():
        ops = spec[2] if isinstance(spec, list) else spec.get("ops", [])
        for op in ops:
            if isinstance(op, str):
                covered.add(op.split(":", 1)[0])
            elif isinstance(op, dict):
                covered.update(op)

    missing = sorted(set(COLUMN_OPS) - structural - covered)
    assert not missing, (
        "Column operations with no cross-engine equivalence case: " + ", ".join(missing)
    )


@pytest.mark.parametrize("construction", ["add_columns", "select_final"])
def test_column_operation_equivalence(equivalence_runtime, construction):
    raw_schema = {
        "tables": [
            {"name": equivalence_runtime.table("operation_values"), "alias": "source"}
        ],
        construction: _operation_specs(),
    }
    if construction == "add_columns":
        raw_schema["keep_all_columns"] = True
    schema = _normalized_schema(raw_schema)
    _assert_schema_equivalent(equivalence_runtime, schema)


def test_one_level_partial_equivalence(equivalence_runtime):
    child = {
        "tables": [
            {
                "name": equivalence_runtime.table("filter_values"),
                "alias": "source",
                "filter": [
                    {"column": "amount", "operator": "greater_than_equal", "value": "30"}
                ],
            }
        ],
        "select_final": [["id", "id"], ["text_value", "text_value"]],
    }
    schema = {
        "partials": [{"alias": "filtered", "schema": child}],
        "select_final": [["id", "id"], ["text_value", "text_value"]],
    }
    _assert_schema_equivalent(equivalence_runtime, schema)


def test_two_level_partial_is_executed_on_both_paths(equivalence_runtime):
    leaf = {
        "tables": [
            {"name": equivalence_runtime.table("operation_values"), "alias": "source"}
        ],
        "select_final": [["id", "id"], ["status", "status"]],
    }
    middle = {
        "partials": [{"alias": "leaf_rows", "schema": leaf}],
        "add_columns": [["status", "upper_status", ["upper"]]],
        "keep_all_columns": True,
    }
    schema = {
        "partials": [{"alias": "middle_rows", "schema": middle}],
        "select_final": [["id", "id"], ["upper_status", "upper_status"]],
    }
    _assert_schema_equivalent(equivalence_runtime, schema)


@pytest.fixture
def sql_rule_names():
    names = []
    yield names
    for name in names:
        RuleRegistry._rules.pop(name, None)


def _register_sql_rule(names, suffix, result):
    name = f"equivalence_{suffix}_{uuid4().hex}"

    @RuleRegistry.register_rule(name=name, kind="sql")
    def rule():
        return result

    names.append(name)
    return name


def test_sql_rule_adds_column_equivalently(equivalence_runtime, sql_rule_names):
    rule_name = _register_sql_rule(sql_rule_names, "add", {"doubled": "amount * 2"})
    schema = _normalized_schema(
        {
            "tables": [
                {
                    "name": equivalence_runtime.table("filter_values"),
                    "alias": "source",
                    "fields": [["id", "id"], ["amount", "amount"]],
                }
            ],
            "business_rules": [rule_name],
            "select_final": [["id", "id"], ["doubled", "doubled"]],
        }
    )
    _assert_schema_equivalent(equivalence_runtime, schema)


def test_sql_rule_rewrites_column_equivalently(equivalence_runtime, sql_rule_names):
    rule_name = _register_sql_rule(sql_rule_names, "rewrite", {"amount": "amount * -1"})
    schema = _normalized_schema(
        {
            "tables": [
                {
                    "name": equivalence_runtime.table("filter_values"),
                    "alias": "source",
                    "fields": [["id", "id"], ["amount", "amount"]],
                }
            ],
            "business_rules": [rule_name],
            "select_final": [["id", "id"], ["amount", "amount"]],
        }
    )
    _assert_schema_equivalent(equivalence_runtime, schema)


def test_csv_existing_column_rewrite_is_equivalent(
    equivalence_runtime, tmp_path, sql_rule_names
):
    from pyspark.sql import functions as F

    path = tmp_path / "rule_rewrite.csv"
    path.write_text("order_id,status\n1,ready\n2,pending\n", encoding="utf-8")
    spark_rule_name = f"equivalence_projection_rewrite_{uuid4().hex}"

    @RuleRegistry.register_rule(name=spark_rule_name, kind="projection")
    def spark_rule(_df):
        return {"status": F.upper(F.col("status"))}

    sql_rule_name = _register_sql_rule(
        sql_rule_names,
        "csv_rewrite",
        {"status": "UPPER(status)"},
    )
    sql_rule_names.append(spark_rule_name)
    table = {
        "name": "raw_orders",
        "alias": "source",
        "source": {
            "type": "csv",
            "path": str(path),
            "options": {"header": "true", "inferSchema": "false"},
        },
    }
    spark_schema = _normalized_schema(
        {
            "tables": [table],
            "business_rules": [spark_rule_name],
            "keep_all_columns": True,
        }
    )
    duck_schema = _normalized_schema(
        {
            "tables": [table],
            "business_rules": [sql_rule_name],
            "keep_all_columns": True,
        }
    )

    spark_df = _spark_result(equivalence_runtime, spark_schema)
    duck_columns, duck_rows = _duck_result(equivalence_runtime, duck_schema)
    assert_spark_duckdb_equivalent(spark_df, duck_columns, duck_rows)


def test_drop_duplicates_keeps_one_original_row_per_key_on_each_path(equivalence_runtime):
    schema = _normalized_schema(
        {
            "tables": [
                {
                    "name": equivalence_runtime.table("duplicate_values"),
                    "alias": "source",
                    "quality_checks": {"drop_duplicates_on": ["event_key"]},
                }
            ],
            "keep_all_columns": True,
        }
    )
    originals = {(1, "first"), (1, "second"), (2, "only"), (None, "null-a"), (None, "null-b")}
    spark_rows = [tuple(row) for row in _spark_result(equivalence_runtime, schema).collect()]
    _, duck_rows = _duck_result(equivalence_runtime, schema)

    for rows in (spark_rows, duck_rows):
        assert len(rows) == 3
        assert Counter(row[0] for row in rows) == Counter({1: 1, 2: 1, None: 1})
        assert all(tuple(row) in originals for row in rows)


def test_dev_limit_interactive_checks_only_count_and_membership(equivalence_runtime):
    schema = _normalized_schema(
        {
            "tables": [
                {
                    "name": equivalence_runtime.table("filter_values"),
                    "alias": "source",
                    "dev_limit": 3,
                }
            ],
            "select_final": [["id", "id"], ["text_value", "text_value"]],
        }
    )
    originals = {(1, "alpha"), (2, "alphabet"), (3, "beta"), (4, "ALPHA"), (5, "a%b"), (6, None)}
    spark_rows = [tuple(row) for row in _spark_result(equivalence_runtime, schema).collect()]
    _, duck_rows = _duck_result(equivalence_runtime, schema)

    for rows in (spark_rows, duck_rows):
        assert len(rows) == 3
        assert all(tuple(row) in originals for row in rows)


@pytest.mark.parametrize(
    "context",
    [_context(is_job=True), _context(is_production=True)],
    ids=["job", "production"],
)
def test_dev_limit_job_and_production_equivalence(equivalence_runtime, context):
    schema = _normalized_schema(
        {
            "tables": [
                {
                    "name": equivalence_runtime.table("filter_values"),
                    "alias": "source",
                    "dev_limit": 3,
                }
            ],
            "select_final": [["id", "id"]],
        }
    )
    spark_count = _spark_result(equivalence_runtime, schema, context=context).count()
    _, duck_rows = _duck_result(equivalence_runtime, schema, context=context)
    assert spark_count == len(duck_rows)


@pytest.mark.parametrize(
    "qualifier",
    ["alias", "table_fqn", "table_short"],
)
def test_a_qualified_select_source_is_refused_by_both_engines(
    equivalence_runtime, qualifier
):
    """Not a feature test — a portability guard on a contradiction we measured.

    Qualifying a select source is what lets lineage attribute a joined column to
    the table it really comes from, and `examples/23_openlineage` ships a schema
    that does it. No qualified form executes: Spark raises UNRESOLVED_COLUMN and
    the compiled SQL a binder error, because the compiler quotes the whole
    string as one identifier — ``SELECT `o.amount` `` rather than
    `` `o`.`amount` ``.

    Whether execution should learn the form or the loader should refuse it is an
    open product decision (Plan 38, point 6). What must not happen meanwhile is
    one engine learning it alone: this pins them together, and fails the day
    either side moves.
    """
    left = equivalence_runtime.table("join_left_same")
    right = equivalence_runtime.table("join_right_same")
    prefixes = {
        "alias": ("left_side", "right_side"),
        "table_fqn": (left, right),
        "table_short": (left.rsplit(".", 1)[-1], right.rsplit(".", 1)[-1]),
    }[qualifier]

    schema = _normalized_schema(
        {
            "tables": [
                {"name": left, "alias": "left_side"},
                {"name": right, "alias": "right_side"},
            ],
            "join": [
                {
                    "table_from": ["left_side", "id"],
                    "table_to": ["right_side", "id"],
                    "type": "left",
                }
            ],
            "select_final": [
                [f"{prefixes[0]}.left_value", "lv"],
                [f"{prefixes[1]}.right_value", "rv"],
            ],
        }
    )

    with pytest.raises(Exception):
        _spark_result(equivalence_runtime, schema)
    with pytest.raises(Exception):
        _duck_result(equivalence_runtime, schema)


@pytest.mark.parametrize(
    "join_type", ["left", "inner", "right", "full", "left_anti", "left_semi"]
)
def test_keep_all_columns_over_a_join_returns_the_same_schema(
    equivalence_runtime, join_type
):
    """A join on differently-named keys used to return two schemas.

    The DataFrame path drops the right-hand key after joining, so a join on
    `left_id = right_id` yields one key column — the same shape the equal-name
    case gets from Spark's own merge. The compiled SQL emitted `ON` and kept
    both, so the same YAML wrote `right_id` into the SQL-mode table and not into
    the Spark one. `select_final` hid it, which is why the existing join tests
    never saw it: they list their outputs.

    `left_anti` and `left_semi` keep only the left side, so there is nothing to
    drop and the parametrisation covers that too.
    """
    schema = _normalized_schema(
        {
            "tables": [
                {"name": equivalence_runtime.table("join_left_different"), "alias": "l"},
                {"name": equivalence_runtime.table("join_right_different"), "alias": "r"},
            ],
            "join": [
                {
                    "table_from": ["l", "left_id"],
                    "table_to": ["r", "right_id"],
                    "type": join_type,
                }
            ],
            "keep_all_columns": True,
        }
    )

    _assert_schema_equivalent(equivalence_runtime, schema)


def test_a_dropped_join_key_does_not_take_a_homonymous_column_with_it(
    equivalence_runtime,
):
    """The left table carries its own `right_id`, which the join must not remove.

    The DataFrame path drops the *object* `df_right["right_id"]`, so the left
    column of the same name survives. Removing it by bare name in SQL would
    delete both, and no other case in this module distinguishes the two: a
    mutation replacing the qualified name with the bare one passed everything.
    """
    schema = _normalized_schema(
        {
            "tables": [
                {"name": equivalence_runtime.table("join_left_colliding"), "alias": "l"},
                {"name": equivalence_runtime.table("join_right_different"), "alias": "r"},
            ],
            "join": [
                {
                    "table_from": ["l", "outer_key"],
                    "table_to": ["r", "right_id"],
                    "type": "left",
                }
            ],
            "keep_all_columns": True,
        }
    )

    _assert_schema_equivalent(equivalence_runtime, schema)
