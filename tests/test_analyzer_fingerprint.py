"""Tests for structural fingerprinting (Plan 43 — 43.4): order/alias/hashseed
invariance, qualifier stripping, and literal abstraction.
"""
from __future__ import annotations

import subprocess
import sys

from skifer.analyzer.fingerprint import expression_fingerprint, subquery_fingerprint


def _expr(sql: str):
    import sqlglot

    return sqlglot.parse_one(sql, dialect="databricks")


def _select(sql: str):
    import sqlglot

    return sqlglot.parse_one(sql, dialect="databricks")


class TestExpressionFingerprint:
    def test_identical_expressions_fingerprint_the_same(self):
        a = expression_fingerprint(_expr("UPPER(TRIM(country))"), abstract_literals=False)
        b = expression_fingerprint(_expr("UPPER(TRIM(country))"), abstract_literals=False)

        assert a == b
        assert a.startswith("sha256:v1:")

    def test_table_qualifier_is_stripped(self):
        a = expression_fingerprint(_expr("UPPER(TRIM(c.country))"), abstract_literals=False)
        b = expression_fingerprint(_expr("UPPER(TRIM(country))"), abstract_literals=False)

        assert a == b

    def test_different_literal_differs_when_not_abstracted(self):
        a = expression_fingerprint(_expr("amount * 1.2"), abstract_literals=False)
        b = expression_fingerprint(_expr("amount * 1.21"), abstract_literals=False)

        assert a != b

    def test_different_literal_matches_when_abstracted(self):
        a = expression_fingerprint(_expr("amount * 1.2"), abstract_literals=True)
        b = expression_fingerprint(_expr("amount * 1.21"), abstract_literals=True)

        assert a == b

    def test_abstracting_a_string_literal_does_not_collide_with_a_numeric_one(self):
        a = expression_fingerprint(_expr("COALESCE(x, 0)"), abstract_literals=True)
        b = expression_fingerprint(_expr("COALESCE(x, 'n/a')"), abstract_literals=True)

        assert a != b

    def test_commutative_add_is_order_invariant(self):
        a = expression_fingerprint(_expr("a + b"), abstract_literals=False)
        b = expression_fingerprint(_expr("b + a"), abstract_literals=False)

        assert a == b

    def test_non_commutative_sub_is_order_sensitive(self):
        a = expression_fingerprint(_expr("a - b"), abstract_literals=False)
        b = expression_fingerprint(_expr("b - a"), abstract_literals=False)

        assert a != b

    def test_structurally_different_expressions_fingerprint_differently(self):
        a = expression_fingerprint(_expr("amount - cost"), abstract_literals=True)
        b = expression_fingerprint(_expr("amount * (1 - margin_rate)"), abstract_literals=True)

        assert a != b

    def test_fingerprint_is_insensitive_to_pythonhashseed(self):
        code = (
            "from skifer.analyzer.fingerprint import expression_fingerprint\n"
            "import sqlglot\n"
            "ast = sqlglot.parse_one('UPPER(TRIM(c.country))', dialect='databricks')\n"
            "print(expression_fingerprint(ast, abstract_literals=False))\n"
        )
        results = set()
        for seed in ("0", "1", "42"):
            result = subprocess.run(
                [sys.executable, "-I", "-c", code],
                capture_output=True, text=True,
                env={"PYTHONHASHSEED": seed, "PATH": "/usr/bin:/bin"},
                cwd="src",
            )
            assert result.returncode == 0, result.stderr
            results.add(result.stdout.strip())
        assert len(results) == 1


class TestSubqueryFingerprint:
    def test_identical_subqueries_fingerprint_the_same(self):
        a = subquery_fingerprint(_select("SELECT SUM(amount) FROM silver.orders_enriched"))
        b = subquery_fingerprint(_select("SELECT SUM(amount) FROM silver.orders_enriched"))

        assert a == b

    def test_different_table_differs(self):
        a = subquery_fingerprint(_select("SELECT SUM(amount) FROM silver.orders_enriched"))
        b = subquery_fingerprint(_select("SELECT SUM(amount) FROM silver.other_table"))

        assert a != b

    def test_filter_order_does_not_matter(self):
        a = subquery_fingerprint(_select("SELECT a FROM t WHERE x = 1 AND y = 2"))
        b = subquery_fingerprint(_select("SELECT a FROM t WHERE y = 2 AND x = 1"))

        assert a == b

    def test_different_projection_differs(self):
        a = subquery_fingerprint(_select("SELECT a FROM t"))
        b = subquery_fingerprint(_select("SELECT b FROM t"))

        assert a != b
