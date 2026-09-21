"""Every YAML construct must transpile to every target dialect, offline.

This is groundwork for the Snowflake and BigQuery adapters (plan 39.7/39.8),
which need a real account to test properly. Transpilation does not: the compiled
pivot SELECT can be emitted for each dialect and parsed back in it, here, for
free. The test iterates the operator catalogs rather than a fixed list, so an
operator added later is covered the day it is added — which is the point, since
the alternative is discovering it while writing a paid adapter.

What this proves is a **syntactic floor**, not semantics. sqlglot can emit valid
SQL whose behaviour differs across engines (NULL ordering, collation, rounding,
time zones). Nothing here replaces running against a real warehouse.
"""

from __future__ import annotations

import pytest

from skifer.core.dialect import transpile
from skifer.core.ir import parse_to_ir
from skifer.core.op_catalog import AGGREGATE_FUNCTIONS, COLUMN_OPS, FILTER_OPERATORS
from skifer.core.schema_loader import parse_schema
from skifer.core.sql_compiler import compile_select

TARGETS = ("databricks", "duckdb", "snowflake", "bigquery")

#: One representative argument per operator, so each actually compiles. Ops whose
#: payload is structural rather than a value are exercised through the chains
#: that own them, not here.
_FILTER_VALUE = {
    "in": "A,B",
    "not_in": "A,B",
    "between": "1,9",
    "not_between": "1,9",
    "is_null": None,
    "is_not_null": None,
}
_OP_ARG = {
    "cast": "double",
    "round": "2",
    "to_date": "yyyy-MM-dd",
    "lit": "X",
    "expr": "YEAR(order_date)",
    "split": "-,0",
    "substring": "1,4",
    "coalesce": "0",
    "nvl": "0",
    "col": "amount",
}
#: Structural keywords that only exist inside a when/then/else chain.
_CHAIN_ONLY = frozenset({"when", "then", "else"})


def _pivot(body: str) -> str:
    schema = f"""
tables:
  - name: silver.orders
    alias: ord
{body}
"""
    return compile_select(parse_to_ir(parse_schema(schema)), persisted_definition=False)


def _assert_every_target_accepts(pivot: str) -> None:
    sqlglot = pytest.importorskip("sqlglot")
    for target in TARGETS:
        sql = transpile(pivot, target=target)
        # Parsing the emitted SQL back in its own dialect is the floor: it proves
        # the output is at least well-formed there, which a transpile that merely
        # returns a string does not.
        sqlglot.parse_one(sql, read=target)


@pytest.mark.parametrize("operator", sorted(FILTER_OPERATORS))
def test_every_filter_operator_transpiles_to_every_target(operator):
    if FILTER_OPERATORS[operator].requires_raw_sql:
        pytest.skip("raw SQL is passed through verbatim; the user owns its dialect")
    value = _FILTER_VALUE.get(operator, "10")
    clause = f"status:{operator}" if value is None else f"status:{operator}:{value}"
    _assert_every_target_accepts(
        _pivot(f'    filter:\n      - "{clause}"\nkeep_all_columns: true\n')
    )


@pytest.mark.parametrize("operation", sorted(COLUMN_OPS))
def test_every_column_operation_transpiles_to_every_target(operation):
    if operation in _CHAIN_ONLY:
        pytest.skip("structural keyword, covered by the when/then/else chain test")
    argument = _OP_ARG.get(operation)
    op = operation if argument is None else f"{operation}:{argument}"
    _assert_every_target_accepts(
        _pivot(f'select_final:\n  - [amount, shaped, ["{op}"]]\n')
    )


@pytest.mark.parametrize("function", sorted(AGGREGATE_FUNCTIONS))
def test_every_aggregate_function_transpiles_to_every_target(function):
    source = '"*"' if function == "count" else "amount"
    _assert_every_target_accepts(
        _pivot(
            "aggregate:\n  group_by: [country]\n"
            f"  measures:\n    - [{source}, m, {function}]\n"
        )
    )


@pytest.mark.parametrize(
    "join_type",
    ["inner", "left", "right", "full", "cross", "left_anti", "left_semi"],
)
def test_every_join_type_transpiles_to_every_target(join_type):
    _assert_every_target_accepts(
        _pivot(
            f"""  - name: silver.customers
    alias: cust
join:
  - table_from: [ord, customer_id]
    table_to: [cust, id]
    type: {join_type}
select_final:
  - [amount, amount]
"""
        )
    )


def test_a_when_then_else_chain_transpiles_to_every_target():
    _assert_every_target_accepts(
        _pivot(
            """select_final:
  - source: status
    target: label
    ops:
      - when: "equals:DONE"
        then: "lit:Paid"
      - else: "lit:Unknown"
"""
        )
    )
