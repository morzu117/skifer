"""
Tests for core/ir.py — IR dataclasses and parse_to_ir().

Verifies round-trip parsing of all YAML forms documented in CLAUDE.md:
compact ops, when/then/else compact, dict-form ops, filter forms, joins.
"""
import pytest

from skifer.core.ir import (
    ParsedColumnSpec,
    ParsedDataProduct,
    ParsedFilter,
    ParsedJoin,
    ParsedOp,
    ParsedOutputField,
    ParsedSchema,
    ParsedSemanticSeed,
    ParsedTable,
    WhenClause,
    _parse_op,
    parse_to_ir,
)


# ---------------------------------------------------------------------------
# _parse_op
# ---------------------------------------------------------------------------

class TestParseOp:
    def test_bare_name(self):
        op = _parse_op("upper")
        assert op == ParsedOp("upper", ())

    def test_name_with_single_arg(self):
        op = _parse_op("cast:double")
        assert op.name == "cast"
        assert op.args == ("double",)

    def test_name_with_two_args(self):
        op = _parse_op("split:_,1")
        assert op.name == "split"
        assert op.args == ("_", "1")

    def test_lit_op(self):
        op = _parse_op("lit:ERP")
        assert op.name == "lit"
        assert op.args == ("ERP",)

    def test_when_op(self):
        op = _parse_op("when:equals:ACTIVE")
        assert op.name == "when"
        assert op.args == ("equals:ACTIVE",)

    def test_then_op(self):
        op = _parse_op("then:lit:Active")
        assert op.name == "then"
        assert op.args == ("lit:Active",)

    def test_else_op(self):
        op = _parse_op("else:lit:Unknown")
        assert op.name == "else"
        assert op.args == ("lit:Unknown",)


# ---------------------------------------------------------------------------
# ParsedOp frozen / equality
# ---------------------------------------------------------------------------

class TestParsedOpEquality:
    def test_same_ops_equal(self):
        assert ParsedOp("cast", ("double",)) == ParsedOp("cast", ("double",))

    def test_list_coerced_to_tuple(self):
        op = ParsedOp("round", ["2"])
        assert isinstance(op.args, tuple)

    def test_different_ops_not_equal(self):
        assert ParsedOp("cast", ("double",)) != ParsedOp("cast", ("string",))


# ---------------------------------------------------------------------------
# parse_to_ir — simple schema
# ---------------------------------------------------------------------------

class TestParseToIrSimple:
    def test_empty_schema(self):
        ir = parse_to_ir({})
        assert isinstance(ir, ParsedSchema)
        assert ir.tables == []
        assert ir.joins == []
        assert ir.select_final == []

    def test_single_table_no_filter(self):
        schema = {"tables": [{"name": "silver.orders", "alias": "ord"}]}
        ir = parse_to_ir(schema)
        assert len(ir.tables) == 1
        t = ir.tables[0]
        assert t.name == "silver.orders"
        assert t.alias == "ord"
        assert t.filters == []

    def test_table_alias_defaults_to_name(self):
        schema = {"tables": [{"name": "silver.orders"}]}
        ir = parse_to_ir(schema)
        assert ir.tables[0].alias == "silver.orders"

    def test_business_rules(self):
        schema = {"tables": [], "business_rules": ["flag_high_value", "compute_discount"]}
        ir = parse_to_ir(schema)
        assert ir.business_rules == ["flag_high_value", "compute_discount"]

    def test_dev_limit(self):
        schema = {"tables": [], "dev_limit": 5000}
        ir = parse_to_ir(schema)
        assert ir.dev_limit == 5000

    def test_keep_all_columns(self):
        schema = {"tables": [], "keep_all_columns": True}
        ir = parse_to_ir(schema)
        assert ir.keep_all_columns is True

    def test_sink_preserved(self):
        sink = {"type": "postgres", "table": "fact_orders"}
        schema = {"tables": [], "sink": sink}
        ir = parse_to_ir(schema)
        assert ir.sink == sink

    def test_agent_ready_metadata_is_typed_and_immutable(self):
        schema = {
            "tables": [],
            "data_product": {
                "id": "sales.orders",
                "version": "1.0.0",
                "owner": "sales-data",
            },
            "contract": {
                "grain": ["order_id"],
                "output": {
                    "order_id": {"logical_type": "string", "required": True},
                    "net_revenue": {"logical_type": "number", "description": "Net amount"},
                },
            },
            "semantic": {
                "model_key": "orders",
                "entity": "order",
                "default_time_dimension": "order_id",
                "dimensions": ["order_id"],
            },
        }

        ir = parse_to_ir(schema)

        assert ir.data_product == ParsedDataProduct(
            id="sales.orders", version="1.0.0", owner="sales-data"
        )
        assert ir.contract_output == [
            ParsedOutputField(name="order_id", logical_type="string", required=True),
            ParsedOutputField(name="net_revenue", logical_type="number", description="Net amount"),
        ]
        assert ir.contract_grain == ["order_id"]
        assert ir.semantic == ParsedSemanticSeed(
            model_key="orders",
            entity="order",
            default_time_dimension="order_id",
            dimensions=("order_id",),
        )
        assert isinstance(ir.semantic.dimensions, tuple)


# ---------------------------------------------------------------------------
# Filter parsing
# ---------------------------------------------------------------------------

class TestFilterParsing:
    def test_simple_equals_filter(self):
        schema = {
            "tables": [{
                "name": "t",
                "filter": [{"column": "status", "operator": "equals", "value": "ACTIVE"}],
            }]
        }
        ir = parse_to_ir(schema)
        f = ir.tables[0].filters[0]
        assert isinstance(f, ParsedFilter)
        assert f.column == "status"
        assert f.operator == "equals"
        assert f.value == "ACTIVE"

    def test_nullary_filter(self):
        schema = {
            "tables": [{
                "name": "t",
                "filter": [{"column": "amount", "operator": "is_not_null"}],
            }]
        }
        ir = parse_to_ir(schema)
        f = ir.tables[0].filters[0]
        assert f.operator == "is_not_null"
        assert f.value is None

    def test_in_filter_with_list(self):
        schema = {
            "tables": [{
                "name": "t",
                "filter": [{"column": "region", "operator": "in", "value": ["EMEA", "APAC"]}],
            }]
        }
        ir = parse_to_ir(schema)
        f = ir.tables[0].filters[0]
        assert f.value == ["EMEA", "APAC"]

    def test_filter_groups(self):
        schema = {
            "tables": [{
                "name": "t",
                "filter_groups": [
                    [{"column": "region", "operator": "equals", "value": "EMEA"}],
                    [{"column": "region", "operator": "equals", "value": "APAC"}],
                ],
            }]
        }
        ir = parse_to_ir(schema)
        assert len(ir.tables[0].filter_groups) == 2
        assert ir.tables[0].filter_groups[0][0].value == "EMEA"
        assert ir.tables[0].filter_groups[1][0].value == "APAC"


# ---------------------------------------------------------------------------
# Column spec parsing — linear ops
# ---------------------------------------------------------------------------

class TestColumnSpecLinear:
    def test_simple_column_no_ops(self):
        schema = {"tables": [], "select_final": [["amount", "amount_eur", []]]}
        ir = parse_to_ir(schema)
        cs = ir.select_final[0]
        assert cs.source == "amount"
        assert cs.target == "amount_eur"
        assert cs.ops == []
        assert not cs.is_conditional

    def test_column_with_ops(self):
        schema = {
            "tables": [],
            "select_final": [["amount", "amount_rounded", ["round:2"]]],
        }
        ir = parse_to_ir(schema)
        cs = ir.select_final[0]
        assert len(cs.ops) == 1
        assert cs.ops[0] == ParsedOp("round", ("2",))

    def test_column_multiple_ops(self):
        schema = {
            "tables": [],
            "select_final": [["name", "name_clean", ["trim", "upper"]]],
        }
        ir = parse_to_ir(schema)
        cs = ir.select_final[0]
        assert cs.ops == [ParsedOp("trim", ()), ParsedOp("upper", ())]

    def test_literal_column(self):
        schema = {
            "tables": [],
            "select_final": [["literal:ERP", "source_system", []]],
        }
        ir = parse_to_ir(schema)
        cs = ir.select_final[0]
        assert cs.source == "literal:ERP"
        assert cs.target == "source_system"


# ---------------------------------------------------------------------------
# Column spec parsing — compact when/then/else
# ---------------------------------------------------------------------------

class TestColumnSpecCompactWhen:
    def test_compact_when_chain(self):
        schema = {
            "tables": [],
            "select_final": [
                ["status", "label", ["when:equals:DONE", "then:lit:Paid", "else:lit:Unknown"]]
            ],
        }
        ir = parse_to_ir(schema)
        cs = ir.select_final[0]
        assert cs.is_conditional
        assert len(cs.when_chain) == 1
        assert cs.when_chain[0].condition == ParsedOp("when", ("equals:DONE",))
        assert cs.when_chain[0].then == ParsedOp("then", ("lit:Paid",))
        assert cs.otherwise == ParsedOp("else", ("lit:Unknown",))


# ---------------------------------------------------------------------------
# Column spec parsing — dict form with when/else
# ---------------------------------------------------------------------------

class TestColumnSpecDictForm:
    def test_dict_form_single_when(self):
        schema = {
            "tables": [],
            "select_final": [{
                "source": "status",
                "target": "status_label",
                "ops": [
                    {"when": "equals:DONE", "then": "lit:Paid"},
                    {"else": "lit:Unknown"},
                ],
            }],
        }
        ir = parse_to_ir(schema)
        cs = ir.select_final[0]
        assert cs.is_conditional
        assert len(cs.when_chain) == 1
        assert cs.otherwise is not None

    def test_dict_form_multiple_when(self):
        schema = {
            "tables": [],
            "select_final": [{
                "source": "status",
                "target": "label",
                "ops": [
                    {"when": "equals:DONE", "then": "lit:Paid"},
                    {"when": "equals:PENDING", "then": "lit:Waiting"},
                    {"else": "lit:Unknown"},
                ],
            }],
        }
        ir = parse_to_ir(schema)
        cs = ir.select_final[0]
        assert len(cs.when_chain) == 2

    def test_dict_form_no_source(self):
        """Dict form without source is valid (for constant expressions)."""
        schema = {
            "tables": [],
            "select_final": [{
                "target": "constant_col",
                "ops": [{"when": "sql:1=1", "then": "lit:yes"}, {"else": "lit:no"}],
            }],
        }
        ir = parse_to_ir(schema)
        cs = ir.select_final[0]
        assert cs.source is None


# ---------------------------------------------------------------------------
# Join parsing
# ---------------------------------------------------------------------------

class TestJoinParsing:
    def test_compact_join_same_keys(self):
        schema = {
            "tables": [],
            "join": [{
                "table_from": ["ord", "customer_id"],
                "table_to": ["cust", "id"],
                "type": "left",
            }],
        }
        ir = parse_to_ir(schema)
        j = ir.joins[0]
        assert isinstance(j, ParsedJoin)
        assert j.alias_left == "ord"
        assert j.keys_left == ["customer_id"]
        assert j.alias_right == "cust"
        assert j.keys_right == ["id"]
        assert j.join_type == "left"

    def test_dict_join_form(self):
        schema = {
            "tables": [],
            "join": [{
                "table_from": "ord",
                "table_to": "cust",
                "on_from": "cid",
                "on_to": "id",
                "type": "inner",
            }],
        }
        ir = parse_to_ir(schema)
        j = ir.joins[0]
        assert j.alias_left == "ord"
        assert j.alias_right == "cust"
        assert j.join_type == "inner"

    def test_default_join_type_is_left(self):
        schema = {
            "tables": [],
            "join": [{"table_from": ["a", "id"], "table_to": ["b", "id"]}],
        }
        ir = parse_to_ir(schema)
        assert ir.joins[0].join_type == "left"

    def test_full_outer_join_parsed(self):
        schema = {
            "tables": [],
            "join": [{"table_from": ["a", "id"], "table_to": ["b", "id"], "type": "full outer"}],
        }
        ir = parse_to_ir(schema)
        assert ir.joins[0].join_type == "full"

    @pytest.mark.parametrize("alias", ["outer", "full_outer", "FULL OUTER", "Full Outer"])
    def test_full_join_aliases_normalized(self, alias):
        schema = {
            "tables": [],
            "join": [{"table_from": ["a", "id"], "table_to": ["b", "id"], "type": alias}],
        }
        ir = parse_to_ir(schema)
        assert ir.joins[0].join_type == "full"

    @pytest.mark.parametrize(
        ("alias", "expected"),
        [
            ("left_anti", "left_anti"),
            ("anti", "left_anti"),
            ("left anti", "left_anti"),
            ("leftanti", "left_anti"),
            ("LEFT ANTI", "left_anti"),
            ("left_semi", "left_semi"),
            ("semi", "left_semi"),
            ("left semi", "left_semi"),
            ("leftsemi", "left_semi"),
            ("LEFT SEMI", "left_semi"),
        ],
    )
    def test_anti_semi_join_aliases_normalized(self, alias, expected):
        schema = {
            "tables": [],
            "join": [{"table_from": ["a", "id"], "table_to": ["b", "id"], "type": alias}],
        }
        ir = parse_to_ir(schema)
        assert ir.joins[0].join_type == expected

    def test_invalid_join_type_raises(self):
        schema = {
            "tables": [],
            "join": [{"table_from": ["a", "id"], "table_to": ["b", "id"], "type": "banana"}],
        }
        with pytest.raises(ValueError, match="Invalid join type"):
            parse_to_ir(schema)


# ---------------------------------------------------------------------------
# Table source variants
# ---------------------------------------------------------------------------

class TestTableSourceVariants:
    def test_external_csv_source(self):
        schema = {
            "tables": [{
                "name": "raw_orders",
                "alias": "orders",
                "source": {
                    "type": "csv",
                    "path": "/data/orders/*.csv",
                    "options": {"header": "true"},
                },
            }]
        }
        ir = parse_to_ir(schema)
        t = ir.tables[0]
        assert t.source_type == "csv"
        assert t.source_path == "/data/orders/*.csv"
        assert t.source_options == {"header": "true"}

    def test_quality_checks(self):
        schema = {
            "tables": [{
                "name": "t",
                "quality_checks": {
                    "drop_nulls_in": ["amount"],
                    "drop_duplicates_on": ["order_id"],
                },
            }]
        }
        ir = parse_to_ir(schema)
        t = ir.tables[0]
        assert t.drop_nulls_in == ["amount"]
        assert t.drop_duplicates_on == ["order_id"]

    def test_table_dev_limit(self):
        schema = {"tables": [{"name": "t", "dev_limit": 1000}]}
        ir = parse_to_ir(schema)
        assert ir.tables[0].dev_limit == 1000


# ---------------------------------------------------------------------------
# Raw dict preserved
# ---------------------------------------------------------------------------

class TestRawPreserved:
    def test_raw_dict_attached(self):
        schema = {"tables": [], "business_rules": ["rule_a"]}
        ir = parse_to_ir(schema)
        assert ir.raw is schema


# ---------------------------------------------------------------------------
# Streaming / materialization (Plan 27) — descriptive IR fields
# ---------------------------------------------------------------------------

class TestStreamingIrFields:
    def test_streaming_flag_default_false(self):
        ir = parse_to_ir({"tables": [{"name": "silver.orders", "alias": "ord"}]})
        assert ir.tables[0].streaming is False
        assert ir.materialization is None

    def test_streaming_flag_and_materialization_carried(self):
        schema = {
            "tables": [
                {"name": "bronze.events", "alias": "ev", "streaming": True},
                {"name": "silver.dim", "alias": "dim"},
            ],
            "materialization": {
                "type": "streaming_table",
                "trigger": "available_now",
                "checkpoint": "auto",
                "write_mode": "upsert",
                "keys": ["event_id"],
            },
        }
        ir = parse_to_ir(schema)
        assert ir.tables[0].streaming is True
        assert ir.tables[1].streaming is False
        assert ir.materialization["write_mode"] == "upsert"
        assert ir.materialization["keys"] == ["event_id"]
