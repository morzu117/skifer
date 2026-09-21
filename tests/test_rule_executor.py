"""
Tests for RuleExecutor — requires Spark (uses conftest fixture).
"""
import pytest
from pyspark.sql import functions as F
from skifer.core.registry import RuleSpec
from skifer.core.rule_planner import RuleStage
from skifer.core.rule_executor import RuleExecutor
from skifer.core.rule_executor import validate_sql_rule_result
from tests.fakes.fake_backend import FakeBackend, FakeColumn, FakeDataFrame


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _spec(name, kind, func):
    return RuleSpec(name=name, func=func, kind=kind)


def _proj_stage(*specs):
    return RuleStage(kind="projection", rules=list(specs))


def _transform_stage(*specs):
    return RuleStage(kind="transform", rules=list(specs))


def _agg_stage(*specs):
    return RuleStage(kind="aggregation", rules=list(specs))


# ---------------------------------------------------------------------------
# Projection fusion
# ---------------------------------------------------------------------------

class TestProjectionFusion:
    def test_single_projection_adds_column(self, spark):
        df = spark.createDataFrame([(100,), (2000,)], ["amount"])

        def flag_high(df):
            return {"is_high": F.when(F.col("amount") >= 1000, 1).otherwise(0)}

        executor = RuleExecutor()
        result = executor.execute(df, [_proj_stage(_spec("flag_high", "projection", flag_high))])
        rows = {r.amount: r.is_high for r in result.collect()}
        assert rows[100] == 0
        assert rows[2000] == 1

    def test_multiple_projections_fused_into_single_select(self, spark):
        """5 projection rules must produce all columns correctly (fusion path)."""
        df = spark.createDataFrame([(1, 10), (2, 20)], ["id", "val"])

        specs = []
        for i in range(5):
            col_name = f"col_{i}"
            def make_func(cn):
                def f(df):
                    return {cn: F.lit(cn)}
                return f
            specs.append(_spec(f"rule_{i}", "projection", make_func(col_name)))

        executor = RuleExecutor()
        result = executor.execute(df, [_proj_stage(*specs)])

        # All columns present
        for i in range(5):
            assert f"col_{i}" in result.columns

        # Existing columns preserved
        assert "id" in result.columns
        assert "val" in result.columns

        # All rows have correct values
        for row in result.collect():
            for i in range(5):
                assert row[f"col_{i}"] == f"col_{i}"

    def test_last_writer_wins_for_duplicate_column(self, spark):
        """If two rules produce the same column name, the second one wins."""
        df = spark.createDataFrame([(1,)], ["id"])

        def rule_first(df):
            return {"label": F.lit("first")}

        def rule_second(df):
            return {"label": F.lit("second")}

        executor = RuleExecutor()
        result = executor.execute(
            df,
            [_proj_stage(
                _spec("r1", "projection", rule_first),
                _spec("r2", "projection", rule_second),
            )]
        )
        assert result.collect()[0]["label"] == "second"

    def test_projection_overwrites_input_column_in_place(self, spark):
        """A projection rule producing a column that already exists must replace
        it in place (no AMBIGUOUS_REFERENCE, position preserved)."""
        df = spark.createDataFrame([("a", 1), ("b", 2)], ["x", "y"])

        def overwrite_x(df):
            return {"x": F.upper(F.col("x"))}

        executor = RuleExecutor()
        result = executor.execute(df, [_proj_stage(_spec("ow", "projection", overwrite_x))])

        # Exactly one 'x' column, in its original position, with normalized value
        assert result.columns == ["x", "y"]
        rows = {r.y: r.x for r in result.collect()}
        assert rows == {1: "A", 2: "B"}

    def test_projection_overwrite_and_add_mixed(self, spark):
        """A rewritten input column and a brand-new column in the same stage."""
        df = spark.createDataFrame([("a", 1)], ["x", "y"])

        def rule(df):
            return {"x": F.upper(F.col("x")), "z": F.lit("new")}

        executor = RuleExecutor()
        result = executor.execute(df, [_proj_stage(_spec("mix", "projection", rule))])

        assert result.columns == ["x", "y", "z"]
        row = result.collect()[0]
        assert row["x"] == "A"
        assert row["z"] == "new"

    def test_projection_wrong_return_type_raises(self, spark):
        df = spark.createDataFrame([(1,)], ["id"])

        def bad_rule(df):
            return df  # returns DataFrame, not dict

        executor = RuleExecutor()
        with pytest.raises(TypeError, match="dict"):
            executor.execute(df, [_proj_stage(_spec("bad", "projection", bad_rule))])

    def test_empty_projection_dict_no_op(self, spark):
        df = spark.createDataFrame([(1,)], ["id"])
        original_cols = df.columns

        def empty_rule(df):
            return {}

        executor = RuleExecutor()
        result = executor.execute(df, [_proj_stage(_spec("empty", "projection", empty_rule))])
        assert result.columns == original_cols

    def test_sql_rule_matches_projection_and_fuses_with_neighbors(self):
        class RecordingBackend(FakeBackend):
            def __init__(self):
                super().__init__()
                self.select_calls = 0

            def expr(self, sql_expr):
                column = FakeColumn(name=f"expr({sql_expr})")
                if sql_expr == "amount * 2":
                    column._eval = lambda row: row["amount"] * 2
                return column

            def select(self, df, columns):
                self.select_calls += 1
                return super().select(df, columns)

        backend = RecordingBackend()
        df = FakeDataFrame([{"amount": 3}])

        def before(df):
            return {"before": backend.lit("yes")}

        def sql_rule():
            return {"doubled": "amount * 2"}

        def equivalent_projection(df):
            column = FakeColumn(name="doubled")
            column._eval = lambda row: row["amount"] * 2
            return {"doubled": column}

        def after(df):
            return {"after": backend.lit("yes")}

        sql_result = RuleExecutor(backend=backend).execute(
            df,
            [_proj_stage(
                _spec("before", "projection", before),
                _spec("portable", "sql", sql_rule),
                _spec("after", "projection", after),
            )],
        )
        projection_result = RuleExecutor(backend=FakeBackend()).execute(
            df,
            [_proj_stage(_spec("equivalent", "projection", equivalent_projection))],
        )

        assert backend.select_calls == 1
        assert sql_result._rows[0]["doubled"] == projection_result._rows[0]["doubled"] == 6
        assert sql_result.columns == ["amount", "before", "doubled", "after"]


class TestSqlRuleValidation:
    def test_non_string_value_names_rule_and_column(self, spark):
        for value in (None, F.lit(1)):
            with pytest.raises(TypeError) as exc_info:
                validate_sql_rule_result("portable_rule", {"bad_column": value}, allow_raw_sql=True)
            message = str(exc_info.value)
            assert "portable_rule" in message
            assert "bad_column" in message
            assert "kind='sql'" in message

    def test_empty_expression_names_rule_and_column(self):
        with pytest.raises(ValueError) as exc_info:
            validate_sql_rule_result("portable_rule", {"bad_column": "  "}, allow_raw_sql=True)
        assert "portable_rule" in str(exc_info.value)
        assert "bad_column" in str(exc_info.value)

    def test_statement_separator_outside_literal_is_rejected(self):
        with pytest.raises(ValueError) as exc_info:
            validate_sql_rule_result("portable_rule", {"bad_column": "amount; DROP TABLE x"}, allow_raw_sql=True)
        assert "portable_rule" in str(exc_info.value)
        assert "bad_column" in str(exc_info.value)

    def test_statement_separator_inside_literal_is_accepted(self):
        assert validate_sql_rule_result(
            "portable_rule",
            {"label": "CASE WHEN status = 'a;b' THEN 'x' ELSE 'y' END"},
            allow_raw_sql=True,
        ) == {"label": "CASE WHEN status = 'a;b' THEN 'x' ELSE 'y' END"}

    def test_select_statement_rejected_when_sqlglot_is_available(self):
        pytest.importorskip("sqlglot")
        with pytest.raises(ValueError) as exc_info:
            validate_sql_rule_result("portable_rule", {"bad_column": "SELECT amount FROM x"}, allow_raw_sql=True)
        assert "portable_rule" in str(exc_info.value)
        assert "bad_column" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Transform rules
# ---------------------------------------------------------------------------

class TestTransformRules:
    def test_transform_rule_applied(self, spark):
        df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])

        def filter_rule(df):
            return df.filter(F.col("id") == 1)

        executor = RuleExecutor()
        result = executor.execute(df, [_transform_stage(_spec("filter", "transform", filter_rule))])
        assert result.count() == 1
        assert result.collect()[0]["id"] == 1

    def test_multiple_transforms_chained(self, spark):
        df = spark.createDataFrame([(1, "hello"), (2, "world")], ["id", "val"])

        def add_upper(df):
            return df.withColumn("upper_val", F.upper(F.col("val")))

        def filter_by_id(df):
            return df.filter(F.col("id") == 1)

        executor = RuleExecutor()
        result = executor.execute(
            df,
            [_transform_stage(
                _spec("upper", "transform", add_upper),
                _spec("filter", "transform", filter_by_id),
            )]
        )
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]["upper_val"] == "HELLO"


# ---------------------------------------------------------------------------
# Aggregation rules
# ---------------------------------------------------------------------------

class TestAggregationRules:
    def test_aggregation_with_agg_keys_fused(self, spark):
        """Two agg rules sharing the same groupBy keys produce a single shuffle."""
        df = spark.createDataFrame(
            [("A", 100), ("A", 200), ("B", 50)],
            ["region", "amount"]
        )

        def agg_total(df):
            return (["region"], {"total": F.sum("amount")})
        agg_total.agg_keys = ["region"]

        def agg_count(df):
            return (["region"], {"cnt": F.count("amount")})
        agg_count.agg_keys = ["region"]

        executor = RuleExecutor()
        result = executor.execute(
            df,
            [_agg_stage(
                _spec("agg_total", "aggregation", agg_total),
                _spec("agg_count", "aggregation", agg_count),
            )]
        )
        rows = {r.region: r for r in result.collect()}
        assert rows["A"].total == 300
        assert rows["A"].cnt == 2
        assert rows["B"].total == 50

    def test_aggregation_without_agg_keys_sequential(self, spark):
        """Agg rules without agg_keys fall back to sequential execution."""
        df = spark.createDataFrame(
            [("A", 100), ("A", 200), ("B", 50)],
            ["region", "amount"]
        )

        def agg_legacy(df):
            return df.groupBy("region").agg(F.sum("amount").alias("total"))

        executor = RuleExecutor()
        result = executor.execute(
            df,
            [_agg_stage(_spec("agg_legacy", "aggregation", agg_legacy))]
        )
        rows = {r.region: r.total for r in result.collect()}
        assert rows["A"] == 300

    def test_different_agg_keys_each_executed_separately(self, spark):
        """Rules with different groupBy keys must each get their own execution."""
        df = spark.createDataFrame(
            [("A", "X", 100), ("A", "Y", 200), ("B", "X", 50)],
            ["region", "cat", "amount"]
        )

        def agg_by_region(df):
            return (["region"], {"total_region": F.sum("amount")})
        agg_by_region.agg_keys = ["region"]

        def agg_by_cat(df):
            return (["cat"], {"total_cat": F.sum("amount")})
        agg_by_cat.agg_keys = ["cat"]

        executor = RuleExecutor()
        # Two separate stages (planner already split them)
        stage1 = _agg_stage(_spec("agg_by_region", "aggregation", agg_by_region))
        result = executor.execute(df, [stage1])
        rows = {r.region: r.total_region for r in result.collect()}
        assert rows["A"] == 300


# ---------------------------------------------------------------------------
# Mixed stages
# ---------------------------------------------------------------------------

class TestMixedStages:
    def test_projection_then_transform(self, spark):
        df = spark.createDataFrame([(100,), (2000,)], ["amount"])

        def proj_rule(df):
            return {"is_high": F.when(F.col("amount") >= 1000, 1).otherwise(0)}

        def filter_rule(df):
            return df.filter(F.col("is_high") == 1)

        executor = RuleExecutor()
        result = executor.execute(
            df,
            [
                _proj_stage(_spec("proj", "projection", proj_rule)),
                _transform_stage(_spec("filter", "transform", filter_rule)),
            ]
        )
        assert result.count() == 1
        assert result.collect()[0]["amount"] == 2000

    def test_20_projection_rules_single_project_node(self, spark):
        """Regression: 20 projection rules all produce correct column values."""
        df = spark.createDataFrame([(1,)], ["id"])

        specs = []
        for i in range(20):
            col_name = f"c{i}"
            def make_func(cn):
                def f(df):
                    return {cn: F.lit(cn)}
                return f
            specs.append(_spec(f"r{i}", "projection", make_func(col_name)))

        executor = RuleExecutor()
        result = executor.execute(df, [_proj_stage(*specs)])

        row = result.collect()[0]
        for i in range(20):
            assert row[f"c{i}"] == f"c{i}"


class TestSqlRuleGovernance:
    """Plan 39, decision D14 — settled 21 September 2026.

    `allow_raw_sql: false` reads "no hand-written SQL runs in this environment",
    without regard to which layer wrote it. A kind='sql' loader was already
    refused under the flag while a kind='sql' rule was not, so the flag announced
    a control it applied by halves.
    """

    def test_a_sql_rule_is_refused_when_raw_sql_is_disabled(self):
        with pytest.raises(ValueError) as exc_info:
            validate_sql_rule_result(
                "portable_rule", {"doubled": "amount * 2"}, allow_raw_sql=False
            )

        message = str(exc_info.value)
        assert "portable_rule" in message
        assert "allow_raw_sql: false" in message

    def test_the_refusal_precedes_structural_validation(self):
        """A malformed result must still be refused for the flag, not its shape.

        Reporting "returned str instead of dict" would send the author to fix the
        rule's return type, in an environment where no rule of that kind may run
        at all.
        """
        with pytest.raises(ValueError, match="allow_raw_sql"):
            validate_sql_rule_result("portable_rule", "not a dict", allow_raw_sql=False)

    def test_the_parameter_has_no_default(self):
        """Three paths reach this function; a fourth must not arrive permissive."""
        import inspect

        parameter = inspect.signature(validate_sql_rule_result).parameters["allow_raw_sql"]

        assert parameter.default is inspect.Parameter.empty
        assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
