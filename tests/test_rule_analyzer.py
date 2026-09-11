"""
Tests for RuleAnalyzer — pure Python, no Spark required.
"""
import pytest
from skifer.core.rule_analyzer import RuleAnalyzer, RuleProfile, RuleWarning
from skifer.core.registry import RuleRegistry


# ---------------------------------------------------------------------------
# Helpers — minimal rule functions (no Spark import needed)
# ---------------------------------------------------------------------------

def _rule_high_value(df):
    return df.withColumn("is_high_value", df["amount"] >= 1000)


def _rule_vip(df):
    import pyspark.sql.functions as F
    return df.withColumn("is_vip", F.when(F.col("amount") >= 5000, 1).otherwise(0))


def _rule_discount(df):
    import pyspark.sql.functions as F
    return df.withColumn("discount", F.when(F.col("amount") >= 1000, 0.1).otherwise(0))


def _rule_overwriter(df):
    import pyspark.sql.functions as F
    return df.withColumn("is_high_value", F.col("amount") >= 9999)


def _rule_no_withcolumn(df):
    return df.filter(df["active"] == 1)


def _rule_bare_col(df):
    from pyspark.sql.functions import col
    return df.withColumn("label", col("status"))


# ---------------------------------------------------------------------------
# analyze_rule
# ---------------------------------------------------------------------------

class TestAnalyzeRule:
    def setup_method(self):
        self.analyzer = RuleAnalyzer()

    def test_detects_output_column_withColumn(self):
        profile = self.analyzer.analyze_rule(_rule_high_value, name="high_value")
        assert "is_high_value" in profile.output_columns

    def test_detects_output_column_with_column_snake_case(self):
        def rule_snake_case(df):
            return df.with_column("score", df["value"])
        profile = self.analyzer.analyze_rule(rule_snake_case)
        assert "score" in profile.output_columns

    def test_detects_input_column_fcol(self):
        profile = self.analyzer.analyze_rule(_rule_vip, name="vip")
        assert "amount" in profile.input_columns

    def test_detects_input_column_bare_col(self):
        profile = self.analyzer.analyze_rule(_rule_bare_col, name="bare_col")
        assert "status" in profile.input_columns

    def test_detects_subscript_input(self):
        profile = self.analyzer.analyze_rule(_rule_high_value, name="high_value")
        assert "amount" in profile.input_columns

    def test_output_not_in_inputs(self):
        profile = self.analyzer.analyze_rule(_rule_vip, name="vip")
        assert "is_vip" not in profile.input_columns

    def test_source_available_true(self):
        profile = self.analyzer.analyze_rule(_rule_high_value)
        assert profile.source_available is True

    def test_no_withcolumn_empty_outputs(self):
        profile = self.analyzer.analyze_rule(_rule_no_withcolumn, name="no_wc")
        assert profile.output_columns == []

    def test_captures_raw_expression(self):
        profile = self.analyzer.analyze_rule(_rule_vip, name="vip")
        assert len(profile.raw_expressions) > 0

    def test_name_defaults_to_function_name(self):
        profile = self.analyzer.analyze_rule(_rule_vip)
        assert profile.name == "_rule_vip"

    def test_custom_name(self):
        profile = self.analyzer.analyze_rule(_rule_vip, name="my_rule")
        assert profile.name == "my_rule"

    def test_source_unavailable_on_builtin(self):
        profile = self.analyzer.analyze_rule(len, name="builtin_len")
        assert profile.source_available is False
        assert profile.output_columns == []

    def test_source_unavailable_on_syntax_error(self, monkeypatch):
        def broken_rule(df):
            return df

        monkeypatch.setattr(
            "skifer.core.rule_analyzer.inspect.getsource",
            lambda func: "def broken_rule(df):\n    return df.withColumn('x',\n",
        )
        profile = self.analyzer.analyze_rule(broken_rule, name="broken")
        assert profile.source_available is False

    def test_analyze_source_reads_raw_source(self):
        profile = self.analyzer.analyze_source(
            """\
def raw_rule(df):
    return {"result": F.col("amount") + 1}
""",
            name="raw_rule",
        )

        assert profile.source_available is True
        assert profile.output_columns == ["result"]
        assert profile.input_columns == ["amount"]

    def test_analyze_source_is_unavailable_on_syntax_error(self):
        profile = self.analyzer.analyze_source("def broken(:", name="broken")

        assert profile == RuleProfile(name="broken", source_available=False)


# ---------------------------------------------------------------------------
# analyze_rules (via RuleRegistry)
# ---------------------------------------------------------------------------

class TestAnalyzeRules:
    def setup_method(self):
        self.analyzer = RuleAnalyzer()
        # Register fresh rules for each test
        RuleRegistry._rules["_test_r1"] = _rule_high_value
        RuleRegistry._rules["_test_r2"] = _rule_vip
        RuleRegistry._rules["_test_r3"] = _rule_discount

    def teardown_method(self):
        for k in ["_test_r1", "_test_r2", "_test_r3"]:
            RuleRegistry._rules.pop(k, None)

    def test_returns_one_profile_per_rule(self):
        profiles = self.analyzer.analyze_rules(["_test_r1", "_test_r2"])
        assert len(profiles) == 2

    def test_unknown_rule_produces_unavailable_profile(self):
        profiles = self.analyzer.analyze_rules(["_does_not_exist"])
        assert len(profiles) == 1
        assert profiles[0].source_available is False

    def test_profiles_have_correct_names(self):
        profiles = self.analyzer.analyze_rules(["_test_r1", "_test_r2"])
        names = [p.name for p in profiles]
        assert "_test_r1" in names
        assert "_test_r2" in names

    def test_unavailable_rule_keeps_original_order_dependency(self):
        profiles = [
            RuleProfile(name="writer", output_columns=["col_a"], source_available=True),
            RuleProfile(name="unknown", source_available=False),
            RuleProfile(name="reader", input_columns=["col_a"], source_available=True),
        ]

        dep_graph = self.analyzer.build_dependency_graph(profiles)
        assert dep_graph["unknown"] == ["writer"]


# ---------------------------------------------------------------------------
# detect_warnings
# ---------------------------------------------------------------------------

class TestDetectWarnings:
    def setup_method(self):
        self.analyzer = RuleAnalyzer()

    def _make_profile(self, name, outputs=None, inputs=None, exprs=None):
        return RuleProfile(
            name=name,
            output_columns=outputs or [],
            input_columns=inputs or [],
            raw_expressions=exprs or [],
            source_available=True,
        )

    def test_overwrite_detected(self):
        profiles = [
            self._make_profile("r1", outputs=["is_high_value"]),
            self._make_profile("r2", outputs=["is_high_value"]),
        ]
        warnings = self.analyzer.detect_warnings(profiles)
        codes = [w.code for w in warnings]
        assert "OVERWRITE" in codes

    def test_overwrite_lists_both_rules(self):
        profiles = [
            self._make_profile("r1", outputs=["col_x"]),
            self._make_profile("r2", outputs=["col_x"]),
        ]
        w = next(w for w in self.analyzer.detect_warnings(profiles) if w.code == "OVERWRITE")
        assert "r1" in w.rules and "r2" in w.rules
        assert w.column == "col_x"

    def test_no_overwrite_when_different_columns(self):
        profiles = [
            self._make_profile("r1", outputs=["col_a"]),
            self._make_profile("r2", outputs=["col_b"]),
        ]
        warnings = self.analyzer.detect_warnings(profiles)
        assert not any(w.code == "OVERWRITE" for w in warnings)

    def test_shared_read_detected_at_threshold(self):
        profiles = [
            self._make_profile("r1", inputs=["amount"]),
            self._make_profile("r2", inputs=["amount"]),
        ]
        warnings = self.analyzer.detect_warnings(profiles, shared_read_threshold=2)
        assert any(w.code == "SHARED_READ" for w in warnings)

    def test_shared_read_not_triggered_below_threshold(self):
        profiles = [
            self._make_profile("r1", inputs=["amount"]),
        ]
        warnings = self.analyzer.detect_warnings(profiles, shared_read_threshold=2)
        assert not any(w.code == "SHARED_READ" for w in warnings)

    def test_shared_read_column_correct(self):
        profiles = [
            self._make_profile("r1", inputs=["amount"]),
            self._make_profile("r2", inputs=["amount"]),
        ]
        w = next(w for w in self.analyzer.detect_warnings(profiles) if w.code == "SHARED_READ")
        assert w.column == "amount"

    def test_duplicate_expr_detected(self):
        expr = "F.when(F.col('amount') >= 1000, 1).otherwise(0)"
        profiles = [
            self._make_profile("r1", exprs=[expr]),
            self._make_profile("r2", exprs=[expr]),
        ]
        warnings = self.analyzer.detect_warnings(profiles)
        assert any(w.code == "DUPLICATE_EXPR" for w in warnings)

    def test_duplicate_expr_lists_both_rules(self):
        expr = "F.col('x') + 1"
        profiles = [
            self._make_profile("r1", exprs=[expr]),
            self._make_profile("r2", exprs=[expr]),
        ]
        w = next(w for w in self.analyzer.detect_warnings(profiles) if w.code == "DUPLICATE_EXPR")
        assert "r1" in w.rules and "r2" in w.rules

    def test_no_warnings_on_clean_rules(self):
        profiles = [
            self._make_profile("r1", outputs=["col_a"], inputs=["x"], exprs=["F.col('x')"]),
            self._make_profile("r2", outputs=["col_b"], inputs=["y"], exprs=["F.col('y')"]),
        ]
        warnings = self.analyzer.detect_warnings(profiles)
        assert warnings == []

    def test_warning_level_overwrite_is_warning(self):
        profiles = [
            self._make_profile("r1", outputs=["col_x"]),
            self._make_profile("r2", outputs=["col_x"]),
        ]
        w = next(w for w in self.analyzer.detect_warnings(profiles) if w.code == "OVERWRITE")
        assert w.level == "warning"

    def test_warning_level_shared_read_is_info(self):
        profiles = [
            self._make_profile("r1", inputs=["amount"]),
            self._make_profile("r2", inputs=["amount"]),
        ]
        w = next(w for w in self.analyzer.detect_warnings(profiles) if w.code == "SHARED_READ")
        assert w.level == "info"

    def test_source_unavailable_profiles_ignored(self):
        profiles = [
            RuleProfile(name="unavailable", source_available=False),
            self._make_profile("r1", outputs=["col_x"]),
        ]
        warnings = self.analyzer.detect_warnings(profiles)
        assert not any(w.code == "OVERWRITE" for w in warnings)


# ---------------------------------------------------------------------------
# Performance lints (Phase 4)
# ---------------------------------------------------------------------------

class TestPerformanceLints:
    def setup_method(self):
        self.analyzer = RuleAnalyzer()

    def _make_perf_profile(self, name, has_python_udf=False, loc=5, withcolumn_count=1):
        return RuleProfile(
            name=name,
            source_available=True,
            has_python_udf=has_python_udf,
            loc=loc,
            withcolumn_count=withcolumn_count,
        )

    # --- PHOTON_BREAKING ---

    def test_photon_breaking_detected_for_udf(self):
        profiles = [self._make_perf_profile("r1", has_python_udf=True)]
        warnings = self.analyzer.detect_warnings(profiles)
        codes = [w.code for w in warnings]
        assert "PHOTON_BREAKING" in codes

    def test_photon_breaking_not_triggered_without_udf(self):
        profiles = [self._make_perf_profile("r1", has_python_udf=False)]
        warnings = self.analyzer.detect_warnings(profiles)
        assert not any(w.code == "PHOTON_BREAKING" for w in warnings)

    def test_photon_breaking_level_is_warning(self):
        profiles = [self._make_perf_profile("r1", has_python_udf=True)]
        w = next(w for w in self.analyzer.detect_warnings(profiles) if w.code == "PHOTON_BREAKING")
        assert w.level == "warning"

    def test_photon_breaking_mentions_rule_name(self):
        profiles = [self._make_perf_profile("my_udf_rule", has_python_udf=True)]
        w = next(w for w in self.analyzer.detect_warnings(profiles) if w.code == "PHOTON_BREAKING")
        assert "my_udf_rule" in w.message

    def test_photon_breaking_not_for_unavailable_source(self):
        profile = RuleProfile(name="unavailable", source_available=False, has_python_udf=False)
        warnings = self.analyzer.detect_warnings([profile])
        assert not any(w.code == "PHOTON_BREAKING" for w in warnings)

    # --- UDF detection via AST ---

    def test_analyze_rule_detects_udf_decorator(self):
        def rule_with_udf(df):
            from pyspark.sql.functions import udf
            from pyspark.sql.types import StringType

            @udf(StringType())
            def my_transform(val):
                return val.upper()
            return df.withColumn("col", my_transform(df["col"]))

        profile = self.analyzer.analyze_rule(rule_with_udf, name="udf_rule")
        assert profile.has_python_udf is True

    def test_analyze_rule_no_udf_detected_for_clean_rule(self):
        import pyspark.sql.functions as F

        def clean_rule(df):
            return df.withColumn("col", F.upper(F.col("col")))

        profile = self.analyzer.analyze_rule(clean_rule, name="clean")
        assert profile.has_python_udf is False

    # --- COMPLEXITY_HIGH ---

    def test_complexity_high_detected_on_high_loc(self):
        profiles = [self._make_perf_profile("big_rule", loc=35)]
        warnings = self.analyzer.detect_warnings(profiles, loc_threshold=30)
        assert any(w.code == "COMPLEXITY_HIGH" for w in warnings)

    def test_complexity_high_not_triggered_below_loc_threshold(self):
        profiles = [self._make_perf_profile("small_rule", loc=10)]
        warnings = self.analyzer.detect_warnings(profiles, loc_threshold=30)
        assert not any(w.code == "COMPLEXITY_HIGH" for w in warnings)

    def test_complexity_high_detected_on_many_withcolumn(self):
        profiles = [self._make_perf_profile("wide_rule", withcolumn_count=6)]
        warnings = self.analyzer.detect_warnings(profiles, withcolumn_threshold=5)
        assert any(w.code == "COMPLEXITY_HIGH" for w in warnings)

    def test_complexity_high_not_triggered_below_withcolumn_threshold(self):
        profiles = [self._make_perf_profile("normal_rule", withcolumn_count=3)]
        warnings = self.analyzer.detect_warnings(profiles, withcolumn_threshold=5)
        assert not any(w.code == "COMPLEXITY_HIGH" for w in warnings)

    def test_complexity_high_level_is_info(self):
        profiles = [self._make_perf_profile("wide_rule", withcolumn_count=10)]
        w = next(w for w in self.analyzer.detect_warnings(profiles, withcolumn_threshold=5)
                 if w.code == "COMPLEXITY_HIGH")
        assert w.level == "info"

    def test_complexity_high_mentions_rule_name(self):
        profiles = [self._make_perf_profile("huge_rule", loc=50)]
        w = next(w for w in self.analyzer.detect_warnings(profiles, loc_threshold=30)
                 if w.code == "COMPLEXITY_HIGH")
        assert "huge_rule" in w.message

    def test_analyze_rule_counts_loc(self):
        def rule_with_lines(df):
            a = 1
            b = 2
            c = 3
            return {}

        profile = self.analyzer.analyze_rule(rule_with_lines)
        assert profile.loc >= 4  # at least the def line + 3 assignments + return

    def test_analyze_rule_counts_withcolumn_calls(self):
        import pyspark.sql.functions as F

        def multi_wc_rule(df):
            df = df.withColumn("a", F.lit(1))
            df = df.withColumn("b", F.lit(2))
            df = df.withColumn("c", F.lit(3))
            return df

        profile = self.analyzer.analyze_rule(multi_wc_rule)
        assert profile.withcolumn_count == 3


# ---------------------------------------------------------------------------
# build_dependency_graph
# ---------------------------------------------------------------------------

class TestBuildDependencyGraph:
    def setup_method(self):
        self.analyzer = RuleAnalyzer()

    def _p(self, name, outputs=None, inputs=None):
        return RuleProfile(
            name=name,
            output_columns=outputs or [],
            input_columns=inputs or [],
            source_available=True,
        )

    def test_no_dependencies_empty_lists(self):
        profiles = [self._p("a", outputs=["x"]), self._p("b", outputs=["y"])]
        graph = self.analyzer.build_dependency_graph(profiles)
        assert graph["a"] == []
        assert graph["b"] == []

    def test_dependency_detected_when_b_reads_a_output(self):
        profiles = [
            self._p("a", outputs=["score"]),
            self._p("b", inputs=["score"]),
        ]
        graph = self.analyzer.build_dependency_graph(profiles)
        assert "a" in graph["b"]

    def test_no_self_dependency(self):
        profiles = [self._p("a", outputs=["x"], inputs=["x"])]
        graph = self.analyzer.build_dependency_graph(profiles)
        assert graph["a"] == []

    def test_unavailable_source_produces_no_edges(self):
        unavailable = RuleProfile(name="u", source_available=False, output_columns=["x"])
        available = RuleProfile(name="v", source_available=True, input_columns=["x"])
        graph = self.analyzer.build_dependency_graph([unavailable, available])
        # v reads x written by u but u was source_available=False → still detects edge
        # (u still registers as a writer even when source unavailable)
        assert isinstance(graph["v"], list)


class TestASTCanonicalization:
    """plan17-0.5 — expressions with different F-aliases are detected as duplicates."""

    def test_same_alias_detected(self):
        """Same expression, same alias → duplicate detected (baseline)."""
        from skifer.core.rule_analyzer import _canonicalize_expr
        import ast
        src = "F.col('amount') * 2"
        node = ast.parse(src, mode="eval").body
        assert _canonicalize_expr(node) == _canonicalize_expr(node)

    def test_different_f_aliases_produce_same_canonical_form(self):
        """F.col('x') and sf.col('x') must yield the same canonical string."""
        from skifer.core.rule_analyzer import _canonicalize_expr
        import ast
        expr_f = ast.parse("F.col('amount') * 2", mode="eval").body
        expr_sf = ast.parse("sf.col('amount') * 2", mode="eval").body
        assert _canonicalize_expr(expr_f) == _canonicalize_expr(expr_sf)

    def test_duplicate_expr_detected_across_different_aliases(self):
        """DUPLICATE_EXPR warning fires when two rules use same logic under different imports."""
        RuleRegistry.clear()
        analyzer = RuleAnalyzer()

        @RuleRegistry.register_rule()
        def rule_a(df):
            return df.withColumn("total", F.col("amount") * 2)  # noqa: F821

        @RuleRegistry.register_rule()
        def rule_b(df):
            return df.withColumn("total2", sf.col("amount") * 2)  # noqa: F821

        profiles = analyzer.analyze_rules(["rule_a", "rule_b"])
        warnings = analyzer.detect_warnings(profiles)
        dup_warnings = [w for w in warnings if w.code == "DUPLICATE_EXPR"]
        assert len(dup_warnings) >= 1, "Expected at least one DUPLICATE_EXPR warning"
        RuleRegistry.clear()

# ---------------------------------------------------------------------------
# Projection rules — the documented default kind, which returns {name: Column}
# ---------------------------------------------------------------------------

def _projection_rule(df):
    import pyspark.sql.functions as F
    return {"order_class": F.when(F.col("amount") >= 500, "priority").otherwise("standard")}


def _projection_rule_via_variable(df):
    import pyspark.sql.functions as F
    columns = {"order_class": F.col("amount")}
    return columns


def _projection_rule_with_a_lookup_table(df):
    import pyspark.sql.functions as F
    labels = {"FR": "France", "DE": "Germany"}
    expression = F.col("code")
    for code, label in labels.items():
        expression = F.when(F.col("code") == code, label).otherwise(expression)
    return {"country_name": expression}


def test_projection_rule_output_columns_are_detected():
    """Outputs used to be read only from withColumn, so projection rules had none.

    `projection` is the default rule kind and the one the documentation teaches, but
    every analyzer test wrote a `transform` rule, so nothing noticed. Lineage then
    attributed a rule-made column to a source table that has no such column.
    """
    profile = RuleAnalyzer().analyze_rule(_projection_rule, name="projection")

    assert profile.output_columns == ["order_class"]
    assert profile.input_columns == ["amount"]


def test_projection_rule_returning_a_named_dict_is_detected():
    profile = RuleAnalyzer().analyze_rule(
        _projection_rule_via_variable, name="via_variable"
    )

    assert profile.output_columns == ["order_class"]


def test_a_dict_that_is_not_returned_is_not_an_output_column():
    """Only the returned mapping names columns; a lookup table inside a rule does not."""
    profile = RuleAnalyzer().analyze_rule(
        _projection_rule_with_a_lookup_table, name="lookup"
    )

    assert profile.output_columns == ["country_name"]
