"""
Tests for core/op_catalog.py — operator catalog completeness, alias resolution, suggestions.
"""
import pytest

from skifer.core.op_catalog import (
    FILTER_OPERATORS,
    COLUMN_OPS,
    resolve_filter_operator,
    resolve_column_op,
    suggest,
)


# ---------------------------------------------------------------------------
# FILTER_OPERATORS
# ---------------------------------------------------------------------------

class TestFilterOperatorsCatalog:
    """Every operator handled by operations.py must appear in the catalog."""

    # Canonical names from CLAUDE.md / operations.py
    CANONICAL_NAMES = [
        "equals", "not_equals",
        "greater_than", "less_than", "greater_than_equal", "less_than_equal",
        "in", "between", "not_between", "not_in",
        "contains", "not_contains",
        "starts_with", "ends_with",
        "is_null", "is_not_null",
        "like", "not_like",
        "sql",
    ]

    def test_all_canonical_operators_present(self):
        for op in self.CANONICAL_NAMES:
            assert op in FILTER_OPERATORS, f"Operator '{op}' missing from FILTER_OPERATORS"

    def test_arity_none_operators(self):
        assert FILTER_OPERATORS["is_null"].arity == "none"
        assert FILTER_OPERATORS["is_not_null"].arity == "none"

    def test_arity_list_operators(self):
        assert FILTER_OPERATORS["in"].arity == "list"
        assert FILTER_OPERATORS["between"].arity == "list"
        assert FILTER_OPERATORS["not_between"].arity == "list"
        assert FILTER_OPERATORS["not_in"].arity == "list"

    def test_sql_requires_raw_sql(self):
        assert FILTER_OPERATORS["sql"].requires_raw_sql is True

    def test_standard_operators_do_not_require_raw_sql(self):
        for name, spec in FILTER_OPERATORS.items():
            if name != "sql":
                assert spec.requires_raw_sql is False, f"'{name}' unexpectedly requires_raw_sql"

    def test_canonical_field_matches_key(self):
        for key, spec in FILTER_OPERATORS.items():
            assert spec.canonical == key, f"Mismatch: key={key}, spec.canonical={spec.canonical}"


class TestFilterAliasResolution:
    """Aliases map to canonical names."""

    def test_canonical_resolves_to_itself(self):
        assert resolve_filter_operator("equals") == "equals"
        assert resolve_filter_operator("in") == "in"
        assert resolve_filter_operator("is_null") == "is_null"

    def test_eq_alias(self):
        assert resolve_filter_operator("eq") == "equals"

    def test_ne_alias(self):
        assert resolve_filter_operator("ne") == "not_equals"

    def test_gt_alias(self):
        assert resolve_filter_operator("gt") == "greater_than"

    def test_lt_alias(self):
        assert resolve_filter_operator("lt") == "less_than"

    def test_gte_alias(self):
        assert resolve_filter_operator("gte") == "greater_than_equal"

    def test_lte_alias(self):
        assert resolve_filter_operator("lte") == "less_than_equal"

    def test_symbol_aliases(self):
        assert resolve_filter_operator("=") == "equals"
        assert resolve_filter_operator("!=") == "not_equals"
        assert resolve_filter_operator(">") == "greater_than"
        assert resolve_filter_operator("<") == "less_than"
        assert resolve_filter_operator(">=") == "greater_than_equal"
        assert resolve_filter_operator("<=") == "less_than_equal"

    def test_notcontains_alias(self):
        assert resolve_filter_operator("notcontains") == "not_contains"

    def test_notlike_alias(self):
        assert resolve_filter_operator("notlike") == "not_like"

    def test_unknown_returns_none(self):
        assert resolve_filter_operator("foobar") is None
        assert resolve_filter_operator("") is None


# ---------------------------------------------------------------------------
# COLUMN_OPS
# ---------------------------------------------------------------------------

class TestColumnOpsCatalog:
    """Every op handled by operations.py must appear in COLUMN_OPS."""

    CANONICAL_OPS = [
        "cast", "upper", "lower", "trim", "round", "abs", "length",
        "to_date", "nvl", "coalesce", "lit", "expr",
        "split", "substring", "col", "when", "then", "else",
    ]

    def test_all_canonical_ops_present(self):
        for op in self.CANONICAL_OPS:
            assert op in COLUMN_OPS, f"Op '{op}' missing from COLUMN_OPS"

    def test_ceil_present(self):
        assert "ceil" in COLUMN_OPS

    def test_expr_requires_raw_sql(self):
        assert COLUMN_OPS["expr"].requires_raw_sql is True

    def test_standard_ops_do_not_require_raw_sql(self):
        for name, spec in COLUMN_OPS.items():
            if name != "expr":
                assert spec.requires_raw_sql is False, f"'{name}' unexpectedly requires_raw_sql"

    def test_arity_none_ops(self):
        for op in ("upper", "lower", "trim", "abs", "length"):
            assert COLUMN_OPS[op].arity == "none", f"Expected arity 'none' for '{op}'"

    def test_arity_none_ceil(self):
        assert COLUMN_OPS["ceil"].arity == "none"

    def test_arity_single_ops(self):
        for op in ("cast", "round", "to_date", "nvl", "coalesce", "lit"):
            assert COLUMN_OPS[op].arity == "single", f"Expected arity 'single' for '{op}'"

    def test_arity_two_ops(self):
        assert COLUMN_OPS["split"].arity == "two"
        assert COLUMN_OPS["substring"].arity == "two"

    def test_arity_expression_ops(self):
        assert COLUMN_OPS["expr"].arity == "expression"

    def test_arity_condition(self):
        assert COLUMN_OPS["when"].arity == "condition"

    def test_arity_passthrough(self):
        assert COLUMN_OPS["col"].arity == "passthrough"
        assert COLUMN_OPS["then"].arity == "passthrough"
        assert COLUMN_OPS["else"].arity == "passthrough"


class TestColumnOpResolution:
    def test_canonical_resolves_to_itself(self):
        for name in COLUMN_OPS:
            assert resolve_column_op(name) == name

    def test_unknown_returns_none(self):
        assert resolve_column_op("foobar") is None
        assert resolve_column_op("") is None


# ---------------------------------------------------------------------------
# suggest()
# ---------------------------------------------------------------------------

class TestSuggest:
    def test_filter_typo_equls(self):
        suggestions = suggest("equls", FILTER_OPERATORS)
        assert "equals" in suggestions

    def test_filter_typo_grater_than(self):
        suggestions = suggest("grater_than", FILTER_OPERATORS)
        assert "greater_than" in suggestions

    def test_filter_alias_still_suggests_canonical(self):
        # "eq" is a valid alias — suggest on "eqs" should give "equals"
        suggestions = suggest("eqs", FILTER_OPERATORS)
        assert "equals" in suggestions

    def test_op_typo_uppr(self):
        suggestions = suggest("uppr", COLUMN_OPS)
        assert "upper" in suggestions

    def test_op_typo_csat(self):
        suggestions = suggest("csat", COLUMN_OPS)
        assert "cast" in suggestions

    def test_garbage_returns_empty_or_list(self):
        suggestions = suggest("xyzxyzxyz", FILTER_OPERATORS)
        assert isinstance(suggestions, list)

    def test_no_duplicate_suggestions(self):
        suggestions = suggest("equals", FILTER_OPERATORS)
        assert len(suggestions) == len(set(suggestions))
