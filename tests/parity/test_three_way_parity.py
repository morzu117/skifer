"""Executed cases are the only promotion evidence for the DuckDB capability matrix."""

from dataclasses import dataclass, field

import pytest
import yaml

from skifer.core.capability_matrix import MATRIX, Conditional, Refused, Supported, capability
from skifer.core.ir import parse_to_ir
from skifer.core.schema_loader import parse_schema
from skifer.core.sql_compiler import SqlCompilationError, compile_select, used_capabilities
from tests.parity.harness import assert_three_way


CAST = (
    "TRY_CAST on DuckDB: a double->int cast truncates on Spark but rounds on DuckDB, "
    "and an unconvertible value raises on Spark (ANSI) but yields NULL on DuckDB; "
    "parity proven for lossless casts only (numeric text->double, number->string, decimal->double)"
)
SPLIT = (
    "both engines split on a regex (Spark split, DuckDB str_split_regex) and sqlglot shifts "
    "the 0-based index; parity proven for a literal separator only, Java and RE2 regex syntax "
    "diverge beyond that"
)
TO_DATE = "sqlglot translates Java date tokens to strptime; parity proven for yyyy-MM-dd and dd/MM/yyyy only"
AVG = (
    "AVG over DECIMAL yields DECIMAL(p+4,s+4) on Spark and DOUBLE on DuckDB; "
    "parity proven over DOUBLE inputs only"
)
FIRST_LAST = (
    "order-dependent without ORDER BY; parity proven only when every group holds "
    "a single non-null distinct value"
)
APPROX = (
    "approximate algorithms differ by engine; parity proven exact on the canonical dataset "
    "(cardinalities below 50)"
)


@dataclass(frozen=True)
class ParityCase:
    id: str
    yaml: str
    condition: dict[str, str] = field(default_factory=dict)
    approx_columns: tuple = ()


def _yaml(*, table=None, **parts):
    return yaml.safe_dump({"tables": [table or {"name": "orders", "alias": "ord"}], **parts})


CASES = [
    ParityCase("comparison_filters", _yaml(
        table={"name": "orders", "alias": "ord", "filter": [
            "status:equals:DONE", "country:not_equals:IT", "order_id:greater_than:1",
            "order_id:less_than:29", "amount:greater_than_equal:0", "amount:less_than_equal:100",
        ]}, select_final=[["order_id", "order_id"], ["amount", "amount"]],
    )),
    ParityCase("membership_ranges_groups", _yaml(
        table={"name": "orders", "alias": "ord", "filter_groups": [
            ["country:in:FR,DE", "amount:between:0,100"],
            ["country:not_in:FR,DE", "amount:not_between:0,10"],
        ]}, select_final=[["order_id", "order_id"], ["amount", "amount"]],
    )),
    ParityCase("text_filters", _yaml(
        table={"name": "orders", "alias": "ord", "filter": [
            "sku:contains:123", "sku:not_contains:456", "sku:starts_with:AB",
            "sku:ends_with:X", "sku:like:AB%", "sku:not_like:CD%",
        ]}, select_final=[["order_id", "order_id"], ["sku", "sku"]],
    )),
    ParityCase("nulls_where_groups", _yaml(
        table={"name": "orders", "alias": "ord", "quality_checks": {"drop_nulls_in": ["amount"]}},
        where=["order_id:greater_than:2"],
        where_groups=[["note:is_null"], ["note:is_not_null", "country:equals:FR"]],
        select_final=[["order_id", "order_id"], ["note", "note"]],
    )),
    ParityCase("text_ops_fields", _yaml(
        table={"name": "orders", "alias": "ord", "fields": [
            ["order_id", "order_id"], ["note", "note", ["trim"]],
        ]},
        select_final=[["order_id", "order_id"], ["note", "upper_note", ["upper"]],
                      ["note", "lower_note", ["lower"]], ["note", "size", ["length"]],
                      ["note", "prefix", ["substring:1,2"]],
                      ["note", "middle", ["substring:4,3"]]],
    )),
    ParityCase("numeric_ops", _yaml(select_final=[
        ["order_id", "order_id"], ["unit_price", "rounded_2", ["round:2"]],
        ["unit_price", "rounded_0", ["round:0"]], ["amount", "absolute", ["abs"]],
        ["amount", "ceiling", ["ceil"]], ["amount", "filled", ["nvl:0"]],
        ["amount", "coalesced", ["coalesce:0"]],
    ])),
    ParityCase("lossless_casts", _yaml(
        table={"name": "orders", "alias": "ord", "fields": [
            ["order_id", "order_id"], ["order_id", "numeric_text", ["cast:string"]],
            ["amount", "amount"],
        ]}, select_final=[["numeric_text", "number", ["cast:double"]],
                         ["order_id", "text", ["cast:string"]],
                         ["amount", "double_amount", ["cast:double"]]],
    ), {"op:cast": CAST}),
    ParityCase("split_literal", _yaml(select_final=[
        ["order_id", "order_id"], ["sku", "prefix", ["split:-,0"]],
        ["sku", "number", ["split:-,1"]],
    ]), {"op:split": SPLIT}),
    ParityCase("date_formats", _yaml(select_final=[
        ["order_id", "order_id"], ["order_date", "original"],
        ["order_date_text", "iso_date", ["to_date:yyyy-MM-dd"]],
        ["order_date_fr", "fr_date", ["to_date:dd/MM/yyyy"]],
    ]), {"op:to_date": TO_DATE}),
    ParityCase("conditional_compact", _yaml(select_final=[
        ["order_id", "order_id"],
        ["amount", "magnitude", ["when:less_than:0", "then:abs", "else:col:amount"]],
        ["note", "label", ["when:is_not_null", "then:upper", "else:lit:missing"]],
    ])),
    ParityCase("conditional_mapping_add_columns", _yaml(add_columns=[
        {"source": "unit_price", "target": "rounded", "ops": [
            {"when": "greater_than:0", "then": "round:2"}, {"else": "lit:0"},
        ]},
        {"source": "note", "target": "label", "ops": [
            {"when": "is_not_null", "then": "lower"}, {"else": "lit:missing"},
        ]},
    ], keep_all_columns=True)),
    ParityCase("aggregates_having", _yaml(
        add_columns=[["country", "country_code", ["upper"]]],
        aggregate={"group_by": ["country_code"], "measures": [
            ["amount", "total", "sum"], ["amount", "minimum", "min"],
            ["amount", "maximum", "max"], ["order_id", "nb_orders", "count"],
            ["*", "nb_rows", "count"], ["customer_id", "customers", "count_distinct"],
            ["amount", "distinct_total", "sum_distinct"],
        ], "having": ["nb_orders:greater_than:1"]},
    )),
    ParityCase("double_statistics", _yaml(aggregate={
        "group_by": ["country"], "measures": [
            ["unit_price", "mean", "avg"], ["unit_price", "stdev", "stddev"],
            ["unit_price", "variance", "variance"],
        ],
    }), {"agg:avg": AVG}),
    ParityCase("functional_dependency_first_last", _yaml(
        table={"name": "countries", "alias": "country_source"},
        aggregate={"group_by": ["country"], "measures": [
            ["country_name", "first_name", "first"], ["country_name", "last_name", "last"],
        ]},
    ), {"agg:first": FIRST_LAST, "agg:last": FIRST_LAST}),
    ParityCase("approx_distinct_small_cardinality", _yaml(aggregate={
        "group_by": ["country"], "measures": [["customer_id", "customers", "approx_count_distinct"]],
    }), {"agg:approx_count_distinct": APPROX}),
]

for _join_type in ("inner", "left", "right", "full", "cross", "left_semi", "left_anti"):
    _schema = {
        "tables": [{"name": "orders", "alias": "ord"}, {
            "name": "customers", "alias": "cust", "fields": [
                ["customer_id", "cust_id"], ["name", "name"], ["segment", "segment"],
            ],
        }],
        "join": [{"table_from": ["ord", "customer_id"], "table_to": ["cust", "cust_id"],
                  "type": _join_type}],
        "select_final": [["order_id", "order_id"], ["customer_id", "customer_id"]],
    }
    if _join_type not in {"left_semi", "left_anti"}:
        _schema["select_final"].extend([["name", "customer_name"], ["segment", "segment"]])
    CASES.append(ParityCase(f"join_{_join_type}", yaml.safe_dump(_schema)))


REFUSED_CASES = [
    ("filter:sql", _yaml(table={"name": "orders", "alias": "ord", "filter": ["amount:sql:amount > 0"]},
                         select_final=[["order_id", "order_id"]])),
    ("op:expr", _yaml(select_final=[["amount", "doubled", ["expr:amount * 2"]]])),
]


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.id)
def test_three_way_parity(parity_spark, duck, case):
    assert_three_way(parity_spark, duck, case.yaml, approx_columns=case.approx_columns)


@pytest.mark.parametrize("key,yaml_text", REFUSED_CASES, ids=[key for key, _ in REFUSED_CASES])
def test_refused_capability_raises(key, yaml_text):
    parsed = parse_to_ir(parse_schema(yaml_text))
    assert key in used_capabilities(parsed)
    with pytest.raises(SqlCompilationError, match=key):
        compile_select(parsed, dialect="duckdb")


def test_duckdb_matrix_is_backed_by_executed_parity():
    """No Spark session: derive coverage from the same cases the execution test runs."""
    exercised, conditioned = set(), set()
    for case in CASES:
        keys = used_capabilities(parse_to_ir(parse_schema(case.yaml)))
        assert set(case.condition) <= keys, case.id
        for key in keys:
            status = capability("duckdb", key)
            assert not isinstance(status, Refused), (case.id, key)
            if isinstance(status, Conditional):
                assert case.condition.get(key) == status.reason, (case.id, key)
            else:
                assert key not in case.condition, (case.id, key)
        if case.approx_columns:
            assert "agg:approx_count_distinct" in keys
            measures = parse_to_ir(parse_schema(case.yaml)).aggregate.measures
            assert set(case.approx_columns) <= {
                measure.target for measure in measures if measure.func == "approx_count_distinct"
            }
        exercised.update(keys)
        conditioned.update(case.condition)
    refused = {key for key, _ in REFUSED_CASES}
    supported = {key for key, status in MATRIX["duckdb"].items() if isinstance(status, Supported)}
    conditional = {key for key, status in MATRIX["duckdb"].items() if isinstance(status, Conditional)}
    assert supported <= exercised
    assert supported.isdisjoint(conditioned)
    assert conditional == conditioned
    assert refused == {key for key, status in MATRIX["duckdb"].items() if isinstance(status, Refused)}
    assert supported | conditioned | refused == set(MATRIX["duckdb"])
    assert supported.isdisjoint(refused) and conditioned.isdisjoint(refused)
    assert len(MATRIX["duckdb"]) == 57
