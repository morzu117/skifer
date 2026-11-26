"""Execute rendered SQL in DuckDB; value parity is reserved for Plan 46.3."""

from pathlib import Path

import duckdb
import pytest
import yaml

from skifer.core.capability_matrix import capability
from skifer.core.ir import VALID_JOIN_TYPES, parse_to_ir
from skifer.core.op_catalog import AGGREGATE_FUNCTIONS, COLUMN_OPS, FILTER_OPERATORS
from skifer.core.schema_loader import parse_schema
from skifer.core.sql_compiler import compile_select


# Fixture types follow the columns referenced by each test YAML: identifiers,
# text fields, numeric measures and date text to parse. Only referenced columns
# are created, so a spurious compiler dependency fails at bind time.
COLUMN_TYPES = {
    "id": ("INTEGER", 1), "order_id": ("INTEGER", 1),
    "country": ("VARCHAR", "FR"), "status": ("VARCHAR", "COMPLETED"),
    "amount": ("DOUBLE", 12.5), "text": ("VARCHAR", "ab-cd"),
    "date_text": ("VARCHAR", "2026-01-02"),
}
FILTER_CASES = {
    "is_null": "amount:is_null", "is_not_null": "amount:is_not_null",
    "equals": "amount:equals:12.5", "not_equals": "amount:not_equals:0",
    "greater_than": "amount:greater_than:0", "less_than": "amount:less_than:100",
    "greater_than_equal": "amount:greater_than_equal:0",
    "less_than_equal": "amount:less_than_equal:100",
    "in": "amount:in:12.5,20", "not_in": "amount:not_in:0,1",
    "between": "amount:between:0,100", "not_between": "amount:not_between:0,1",
    "contains": "text:contains:b", "not_contains": "text:not_contains:z",
    "starts_with": "text:starts_with:a", "ends_with": "text:ends_with:d",
    "like": "text:like:a%", "not_like": "text:not_like:z%",
}
OP_CASES = {
    "cast": ("amount", "cast:double"), "upper": ("text", "upper"),
    "lower": ("text", "lower"), "trim": ("text", "trim"),
    "length": ("text", "length"), "split": ("text", "split:-,0"),
    "substring": ("text", "substring:1,2"), "round": ("amount", "round:1"),
    "abs": ("amount", "abs"), "ceil": ("amount", "ceil"),
    "to_date": ("date_text", "to_date:yyyy-MM-dd"),
    "nvl": ("amount", "nvl:0"), "coalesce": ("amount", "coalesce:0"),
    "lit": ("amount", "lit:7"), "col": ("id", "col:id"),
}


def _schema(**parts):
    return {"tables": [{"name": "orders", "alias": "ord"}], **parts}


def _execute(schema, expected_columns):
    parsed = parse_to_ir(parse_schema(yaml.safe_dump(schema)))
    with duckdb.connect() as connection:
        for table in parsed.tables:
            names = {f.column for f in table.filters}
            names.update(spec.source for spec in table.fields if spec.source)
            if table is parsed.tables[0]:
                names.update(spec.source for spec in parsed.select_final + parsed.add_columns if spec.source)
            for join in parsed.joins:
                if join.alias_left == table.alias:
                    names.update(join.keys_left)
                if join.alias_right == table.alias:
                    names.update(join.keys_right)
            names.discard(None)  # Keyless cross joins have no key column to create.
            if parsed.aggregate:
                names.update(parsed.aggregate.group_by)
                names.update(m.source for m in parsed.aggregate.measures if m.source != "*")
            if not names:
                names.add("id")
            ordered = sorted(names)
            parts = table.name.split(".")
            if len(parts) == 2:
                connection.execute(f'CREATE SCHEMA IF NOT EXISTS "{parts[0]}"')
            fqn = ".".join(f'"{part}"' for part in parts)
            columns = ", ".join(f'"{name}" {COLUMN_TYPES[name][0]}' for name in ordered)
            connection.execute(f"CREATE TABLE {fqn} ({columns})")
            placeholders = ", ".join("?" for _ in ordered)
            connection.execute(
                f"INSERT INTO {fqn} VALUES ({placeholders})",
                [COLUMN_TYPES[name][1] for name in ordered],
            )
        result = connection.execute(compile_select(parsed, dialect="duckdb"))
        assert [column[0] for column in result.description] == expected_columns
        result.fetchall()


def test_family_coverage_matches_catalog():
    assert set(FILTER_CASES) == set(FILTER_OPERATORS) - {"sql"}
    assert set(OP_CASES) | {"when", "then", "else"} == set(COLUMN_OPS) - {"expr"}


def test_materialized_view_example_executes():
    path = Path(__file__).resolve().parents[1] / "examples/08_materialized_view/revenue_by_country.yaml"
    _execute(yaml.safe_load(path.read_text()), ["country", "total_revenue", "order_count"])


@pytest.mark.parametrize("operator", FILTER_CASES)
def test_filter_executes(operator):
    schema = _schema(select_final=[["id", "out"]])
    schema["tables"][0]["filter"] = [FILTER_CASES[operator]]
    _execute(schema, ["out"])


@pytest.mark.parametrize("operation", OP_CASES)
def test_column_operation_executes(operation):
    source, op = OP_CASES[operation]
    _execute(_schema(select_final=[[source, "out", [op]]]), ["out"])


@pytest.mark.parametrize("compact", [False, True])
def test_when_then_else_executes(compact):
    spec = ["amount", "out", ["when:greater_than:0", "then:round:1", "else:lit:0"]] if compact else {
        "source": "amount", "target": "out",
        "ops": [{"when": "greater_than:0", "then": "round:1"}, {"else": "lit:0"}],
    }
    _execute(_schema(select_final=[spec]), ["out"])


@pytest.mark.parametrize("function", AGGREGATE_FUNCTIONS)
def test_aggregate_executes(function):
    _execute(_schema(aggregate={
        "group_by": ["country"], "measures": [["amount", "out", function]],
    }), ["country", "out"])


def test_count_star_executes():
    _execute(_schema(aggregate={
        "group_by": ["country"], "measures": [["*", "out", "count"]],
    }), ["country", "out"])


@pytest.mark.parametrize("join_type", sorted(VALID_JOIN_TYPES))
def test_join_executes(join_type):
    assert capability("duckdb", f"join:{join_type}").kind in {"supported", "conditional"}
    schema = _schema(
        join=[{"table_from": ["ord", "id"], "table_to": ["cust", "id"], "type": join_type}],
        select_final=[["id", "out"]],
    )
    # A cross join has no USING clause; select a uniquely named left column.
    if join_type == "cross":
        schema["join"] = [{"table_from": "ord", "table_to": "cust", "type": "cross"}]
        schema["select_final"] = [["order_id", "out"]]
    schema["tables"].append({"name": "customers", "alias": "cust"})
    _execute(schema, ["out"])
