"""
Tests for SchemaInterpreter (plan17-2.3).

Verifies that process_schema, get_select_expressions, and _apply_business_rules
in SchemaInterpreter produce the same results as the delegating methods on SkiferEngine.
"""
from unittest.mock import MagicMock

import pytest

from skifer.core.context import ExecutionContext
from skifer.core.interpreter import SchemaInterpreter
from skifer.core.spark_backend import SparkBackend


@pytest.fixture
def mock_backend():
    b = MagicMock()
    b.col.side_effect = lambda name: MagicMock(name=f"col({name})")
    b.lit.side_effect = lambda v: MagicMock(name=f"lit({v})")
    return b


@pytest.fixture
def ctx():
    c = ExecutionContext()
    c.env = "test"
    c.config = {}
    c.is_job_execution = True
    c.schema_suffix = ""
    return c


@pytest.fixture
def interpreter(mock_backend, ctx):
    return SchemaInterpreter(backend=mock_backend, context=ctx)


# ---------------------------------------------------------------------------
# get_select_expressions
# ---------------------------------------------------------------------------

class TestGetSelectExpressions:
    def test_empty_field_list_returns_empty(self, interpreter):
        result = interpreter.get_select_expressions([])
        assert result == []

    def test_simple_field_no_ops(self, interpreter, mock_backend):
        col_mock = MagicMock()
        col_mock.alias.return_value = col_mock
        mock_backend.col.side_effect = None
        mock_backend.col.return_value = col_mock

        result = interpreter.get_select_expressions([["amount", "amount_out", []]])
        assert len(result) == 1
        col_mock.alias.assert_called_once_with("amount_out")

    def test_compact_when_chain_valid(self, interpreter, mock_backend):
        """Compact [when:..., then:..., else:...] should produce a when expression."""
        cond = MagicMock()
        then_val = MagicMock()
        else_val = MagicMock()
        chained = MagicMock()
        chained.alias.return_value = chained
        mock_backend.apply_op.side_effect = [cond, then_val, else_val]
        mock_backend.otherwise.return_value = chained
        mock_backend.when.return_value = MagicMock()

        result = interpreter.get_select_expressions([
            ["status", "label", ["when:equals:A", "then:lit:Active", "else:lit:Unknown"]]
        ])
        assert len(result) == 1

    def test_compact_when_chain_missing_else_raises(self, interpreter):
        with pytest.raises(ValueError, match="else:"):
            interpreter.get_select_expressions([
                ["status", "label", ["when:equals:A", "then:lit:Active"]]
            ])

    def test_compact_when_chain_extra_op_raises(self, interpreter):
        with pytest.raises(ValueError, match="3 elements"):
            interpreter.get_select_expressions([
                ["status", "label", ["when:equals:A", "then:lit:Active", "else:lit:X", "extra"]]
            ])

    def test_dict_form_when_conditions(self, interpreter, mock_backend):
        """Dict-form {source, target, ops:[{when,then},{else}]} should delegate to when_chain."""
        cond = MagicMock()
        then_val = MagicMock()
        else_val = MagicMock()
        chained = MagicMock()
        chained.alias.return_value = chained
        mock_backend.apply_op.side_effect = [cond, then_val, else_val]
        mock_backend.when_chain.return_value = chained

        config = {
            "source": "status",
            "target": "status_label",
            "ops": [
                {"when": "equals:DONE", "then": "lit:Paid"},
                {"else": "lit:Unknown"},
            ],
        }
        result = interpreter.get_select_expressions([config])
        assert len(result) == 1
        mock_backend.when_chain.assert_called_once()


# ---------------------------------------------------------------------------
# _apply_business_rules
# ---------------------------------------------------------------------------

class TestApplyBusinessRules:
    def test_empty_rules_returns_df_unchanged(self, interpreter):
        df = MagicMock()
        result = interpreter._apply_business_rules(df, [])
        assert result is df

    def test_unfused_raises_on_projection_returning_non_dict(self, interpreter):
        from skifer.core.registry import RuleRegistry

        @RuleRegistry.register_rule()
        def _bad_proj(df):
            return "not a dict"  # wrong return type

        df = MagicMock()
        with pytest.raises(TypeError, match="kind='projection'"):
            interpreter._apply_business_rules(df, ["_bad_proj"], fuse_rules=False)


# ---------------------------------------------------------------------------
# process_schema — delegation smoke tests
# ---------------------------------------------------------------------------

class TestProcessSchema:
    def test_no_tables_raises(self, interpreter):
        with pytest.raises(ValueError, match="No tables loaded"):
            interpreter.process_schema({"tables": []})

    def test_dataframes_in_used_directly(self, interpreter, mock_backend):
        """When dataframes_in is supplied the loader path is skipped."""
        df = MagicMock()
        mock_backend.select.return_value = df
        result = interpreter.process_schema(
            {"tables": [{"name": "orders", "alias": "orders"}]},
            dataframes_in={"orders": df},
        )
        mock_backend.read_table.assert_not_called()
        assert result is df

    def test_keep_all_and_select_final_mutually_exclusive(self, interpreter, mock_backend):
        df = MagicMock()
        with pytest.raises(ValueError, match="mutually exclusive"):
            interpreter.process_schema(
                {
                    "tables": [{"name": "t", "alias": "t"}],
                    "keep_all_columns": True,
                    "select_final": [["x", "x", []]],
                },
                dataframes_in={"t": df},
            )


# ---------------------------------------------------------------------------
# Plan 43 — post-join 'where:' is NOT equivalent to a table-level 'filter:'
# across an outer join. Run on a real Spark session (not a mock/fake) because
# this is exactly the kind of NULL/outer-join semantics a simulated backend
# can get subtly wrong — the whole reason the block exists has to be proven,
# not assumed.
# ---------------------------------------------------------------------------

class TestWherePostJoinFilterOnRealSpark:
    @pytest.fixture
    def spark_interpreter(self, spark):
        backend = SparkBackend(spark=spark, is_local=True)
        ctx = ExecutionContext()
        ctx.env = "test"
        ctx.config = {}
        ctx.is_job_execution = True
        ctx.schema_suffix = ""
        return SchemaInterpreter(backend=backend, context=ctx)

    @pytest.fixture
    def orders_and_refunds(self, spark):
        # order 1 has a REJECTED refund, order 2 an APPROVED refund,
        # order 3 has no refund row at all.
        orders = spark.createDataFrame(
            [(1,), (2,), (3,)], ["order_id"]
        )
        refunds = spark.createDataFrame(
            [(1, "REJECTED"), (2, "APPROVED")], ["order_id", "status"]
        )
        return orders, refunds

    def _schema(self, extra: dict) -> dict:
        base = {
            "tables": [
                {"name": "orders", "alias": "o"},
                {"name": "refunds", "alias": "r"},
            ],
            "join": [
                {"table_from": "o", "table_to": "r", "on_from": "order_id", "on_to": "order_id", "type": "left"}
            ],
            "keep_all_columns": True,
        }
        base.update(extra)
        return base

    def test_table_level_filter_pushed_before_join_keeps_every_order(
        self, spark_interpreter, orders_and_refunds
    ):
        """Filtering 'refunds' to APPROVED BEFORE the join only narrows what can
        match — every order is still an unmatched left row otherwise kept."""
        orders, refunds = orders_and_refunds
        schema = self._schema(
            {"tables": [
                {"name": "orders", "alias": "o"},
                {"name": "refunds", "alias": "r", "filter": [{"column": "status", "operator": "equals", "value": "APPROVED"}]},
            ]}
        )
        result = spark_interpreter.process_schema(
            schema, dataframes_in={"orders": orders, "refunds": refunds}
        )
        assert {row["order_id"] for row in result.collect()} == {1, 2, 3}

    def test_where_after_join_drops_every_order_without_an_approved_refund(
        self, spark_interpreter, orders_and_refunds
    ):
        """The SAME predicate, applied post-join via 'where:', collapses the left
        join back to an inner join on it: order 1 (REJECTED) and order 3 (no
        refund, so 'status' is NULL) are both dropped — only order 2 survives."""
        orders, refunds = orders_and_refunds
        schema = self._schema(
            {"where": [{"column": "status", "operator": "equals", "value": "APPROVED"}]}
        )
        result = spark_interpreter.process_schema(
            schema, dataframes_in={"orders": orders, "refunds": refunds}
        )
        assert {row["order_id"] for row in result.collect()} == {2}

    def test_where_groups_or_of_ands_after_join(self, spark_interpreter, orders_and_refunds):
        orders, refunds = orders_and_refunds
        schema = self._schema(
            {
                "where_groups": [
                    [{"column": "status", "operator": "equals", "value": "APPROVED"}],
                    [{"column": "order_id", "operator": "equals", "value": "3"}],
                ]
            }
        )
        result = spark_interpreter.process_schema(
            schema, dataframes_in={"orders": orders, "refunds": refunds}
        )
        assert {row["order_id"] for row in result.collect()} == {2, 3}
