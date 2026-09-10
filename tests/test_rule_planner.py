"""
Tests for RulePlanner — pure Python, no Spark required.
"""
import pytest
from skifer.core.registry import RuleRegistry, RuleSpec
from skifer.core.rule_planner import RulePlanner, RuleStage, RuleCycleError, _topological_sort


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_spec(name, kind="projection", func=None):
    if func is None:
        def func(df):
            return {}
    return RuleSpec(name=name, func=func, kind=kind)


def _register(name, kind="projection", func=None):
    if func is None:
        def func(df):
            return {}
        func.__name__ = name
    spec = RuleSpec(name=name, func=func, kind=kind)
    RuleRegistry._rules[name] = spec
    return spec


def _cleanup(*names):
    for n in names:
        RuleRegistry._rules.pop(n, None)


# ---------------------------------------------------------------------------
# Stage grouping
# ---------------------------------------------------------------------------

class TestStageGrouping:
    def setup_method(self):
        self.planner = RulePlanner()

    def teardown_method(self):
        _cleanup("p1", "p2", "a1", "p3", "t1", "p4")

    def test_single_projection_rule_one_stage(self):
        _register("p1", kind="projection")
        stages = self.planner.plan(["p1"])
        assert len(stages) == 1
        assert stages[0].kind == "projection"

    def test_two_consecutive_projection_rules_one_stage(self):
        _register("p1", kind="projection")
        _register("p2", kind="projection")
        stages = self.planner.plan(["p1", "p2"])
        assert len(stages) == 1
        assert stages[0].kind == "projection"
        assert len(stages[0].rules) == 2

    def test_mixed_kinds_produce_multiple_stages(self):
        _register("p1", kind="projection")
        _register("a1", kind="aggregation")
        _register("p3", kind="projection")
        stages = self.planner.plan(["p1", "a1", "p3"])
        assert len(stages) == 3
        kinds = [s.kind for s in stages]
        assert kinds == ["projection", "aggregation", "projection"]

    def test_transform_breaks_projection_stage(self):
        _register("p1", kind="projection")
        _register("t1", kind="transform")
        _register("p4", kind="projection")
        stages = self.planner.plan(["p1", "t1", "p4"])
        assert len(stages) == 3

    def test_empty_rules_list(self):
        stages = self.planner.plan([])
        assert stages == []

    def test_single_rule_stage_contains_that_rule(self):
        _register("p1", kind="projection")
        stages = self.planner.plan(["p1"])
        assert stages[0].rules[0].name == "p1"


# ---------------------------------------------------------------------------
# Topological sort
# ---------------------------------------------------------------------------

class TestTopologicalSort:
    def test_no_dependencies_preserves_order(self):
        names = ["a", "b", "c"]
        dep_graph = {"a": [], "b": [], "c": []}
        result = _topological_sort(dep_graph, names)
        assert result == names

    def test_simple_dependency_reorders(self):
        # b depends on a → a must come before b
        names = ["b", "a"]
        dep_graph = {"b": ["a"], "a": []}
        result = _topological_sort(dep_graph, names)
        assert result.index("a") < result.index("b")

    def test_chain_dependency(self):
        # c depends on b, b depends on a
        names = ["c", "b", "a"]
        dep_graph = {"c": ["b"], "b": ["a"], "a": []}
        result = _topological_sort(dep_graph, names)
        assert result == ["a", "b", "c"]

    def test_cycle_raises_error(self):
        names = ["a", "b"]
        dep_graph = {"a": ["b"], "b": ["a"]}
        with pytest.raises(RuleCycleError, match="cycle"):
            _topological_sort(dep_graph, names)

    def test_independent_rules_stable_order(self):
        names = ["x", "y", "z"]
        dep_graph = {"x": [], "y": [], "z": []}
        result = _topological_sort(dep_graph, names)
        assert result == ["x", "y", "z"]

    def test_diamond_dependency(self):
        # d depends on b and c, both depend on a
        names = ["d", "b", "c", "a"]
        dep_graph = {"d": ["b", "c"], "b": ["a"], "c": ["a"], "a": []}
        result = _topological_sort(dep_graph, names)
        assert result.index("a") < result.index("b")
        assert result.index("a") < result.index("c")
        assert result.index("b") < result.index("d")
        assert result.index("c") < result.index("d")


# ---------------------------------------------------------------------------
# Projection stage sorting
# ---------------------------------------------------------------------------

class TestProjectionStageSorting:
    def setup_method(self):
        self.planner = RulePlanner()

    def teardown_method(self):
        _cleanup("rule_a", "rule_b", "rule_c")

    def test_independent_rules_no_reorder(self):
        """Rules with no dependencies should keep original YAML order."""
        def rule_a(df):
            return {"col_a": None}
        def rule_b(df):
            return {"col_b": None}

        _register("rule_a", kind="projection", func=rule_a)
        _register("rule_b", kind="projection", func=rule_b)

        stages = self.planner.plan(["rule_a", "rule_b"])
        assert len(stages) == 1
        names = [r.name for r in stages[0].rules]
        assert names == ["rule_a", "rule_b"]

    def test_dependency_preserves_correct_order(self, capsys):
        """rule_a writes 'score', rule_b reads 'score': original order already correct (no reorder)."""
        import pyspark.sql.functions as F

        def rule_a(df):
            return {"score": F.lit(1)}

        def rule_b(df):
            return {"label": F.col("score")}

        _register("rule_a", kind="projection", func=rule_a)
        _register("rule_b", kind="projection", func=rule_b)

        stages = self.planner.plan(["rule_a", "rule_b"])
        assert len(stages) == 1
        names = [r.name for r in stages[0].rules]
        # rule_b depends on rule_a → rule_a stays first (already correct)
        assert names.index("rule_a") < names.index("rule_b")

    def test_cycle_raises_rule_cycle_error(self):
        """A genuine cycle raises RuleCycleError."""
        # Manually inject profiles with a cycle by mocking the analyzer
        import unittest.mock as mock
        from skifer.core.rule_analyzer import RuleAnalyzer, RuleProfile

        def rule_a(df):
            return {}
        def rule_c(df):
            return {}

        _register("rule_a", kind="projection", func=rule_a)
        _register("rule_c", kind="projection", func=rule_c)

        cycle_graph = {"rule_a": ["rule_c"], "rule_c": ["rule_a"]}

        with mock.patch.object(RuleAnalyzer, "build_dependency_graph", return_value=cycle_graph):
            with pytest.raises(RuleCycleError):
                self.planner.plan(["rule_a", "rule_c"])


# ---------------------------------------------------------------------------
# Aggregation stage splitting
# ---------------------------------------------------------------------------

class TestAggregationStageSplitting:
    def setup_method(self):
        self.planner = RulePlanner()

    def teardown_method(self):
        _cleanup("agg_same1", "agg_same2", "agg_diff")

    def test_same_agg_keys_kept_in_one_stage(self):
        def agg_same1(df):
            return (["region"], {"total": None})
        agg_same1.agg_keys = ["region"]

        def agg_same2(df):
            return (["region"], {"avg": None})
        agg_same2.agg_keys = ["region"]

        _register("agg_same1", kind="aggregation", func=agg_same1)
        _register("agg_same2", kind="aggregation", func=agg_same2)

        stages = self.planner.plan(["agg_same1", "agg_same2"])
        assert len(stages) == 1
        assert stages[0].kind == "aggregation"
        assert len(stages[0].rules) == 2

    def test_different_agg_keys_split_into_separate_stages(self):
        def agg_same1(df):
            return (["region"], {"total": None})
        agg_same1.agg_keys = ["region"]

        def agg_diff(df):
            return (["country"], {"cnt": None})
        agg_diff.agg_keys = ["country"]

        _register("agg_same1", kind="aggregation", func=agg_same1)
        _register("agg_diff", kind="aggregation", func=agg_diff)

        stages = self.planner.plan(["agg_same1", "agg_diff"])
        assert len(stages) == 2

    def test_no_agg_keys_each_rule_its_own_stage(self):
        def agg_same1(df):
            return df
        def agg_diff(df):
            return df

        _register("agg_same1", kind="aggregation", func=agg_same1)
        _register("agg_diff", kind="aggregation", func=agg_diff)

        stages = self.planner.plan(["agg_same1", "agg_diff"])
        assert len(stages) == 2
