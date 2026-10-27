"""Tests for the project analyzer's AST node-type inventory (Plan 43 — 43.0)."""
from __future__ import annotations

import glob

import pytest

from skifer.analyzer.inventory import inventory_corpus, inventory_statement
from skifer.analyzer.parser import parse_script

CORPUS_DIR = "examples/24_project_analyzer/legacy_sql"


def _parse(sql: str, tmp_path, name: str = "a.sql"):
    path = tmp_path / name
    path.write_text(sql)
    return parse_script(path)[0]


class TestInventoryStatement:
    def test_counts_left_join(self, tmp_path):
        stmt = _parse(
            "SELECT o.id FROM silver.orders o LEFT JOIN silver.refunds r ON o.id = r.order_id;",
            tmp_path,
        )

        inv = inventory_statement(stmt)

        assert inv.join_types["left"] == 1

    def test_counts_inner_join_as_default_label(self, tmp_path):
        stmt = _parse(
            "SELECT o.id FROM silver.orders o JOIN bronze.products p ON o.product_id = p.id;",
            tmp_path,
        )

        inv = inventory_statement(stmt)

        assert inv.join_types["inner"] == 1

    def test_counts_scalar_functions_by_lowercase_name(self, tmp_path):
        stmt = _parse("SELECT UPPER(TRIM(country)) AS c FROM silver.customers;", tmp_path)

        inv = inventory_statement(stmt)

        assert inv.function_names["upper"] == 1
        assert inv.function_names["trim"] == 1

    def test_counts_aggregate_separately_from_scalar_functions(self, tmp_path):
        stmt = _parse(
            "SELECT country, SUM(amount) AS total FROM silver.orders GROUP BY country;",
            tmp_path,
        )

        inv = inventory_statement(stmt)

        assert inv.aggregate_names["sum"] == 1
        assert "sum" not in inv.function_names

    def test_counts_window_function(self, tmp_path):
        stmt = _parse(
            "SELECT ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts DESC) AS rn FROM silver.snapshots;",
            tmp_path,
        )

        inv = inventory_statement(stmt)

        assert inv.window_count == 1

    def test_counts_scalar_subquery(self, tmp_path):
        stmt = _parse(
            "SELECT (SELECT SUM(amount) FROM silver.orders) AS total FROM silver.customers;",
            tmp_path,
        )

        inv = inventory_statement(stmt)

        assert inv.subquery_count == 1

    def test_counts_cte(self, tmp_path):
        stmt = _parse(
            "WITH recent AS (SELECT * FROM silver.orders) SELECT * FROM recent;",
            tmp_path,
        )

        inv = inventory_statement(stmt)

        assert inv.cte_count == 1

    def test_counts_union(self, tmp_path):
        stmt = _parse(
            "SELECT id FROM silver.orders UNION ALL SELECT id FROM silver.refunds;",
            tmp_path,
        )

        inv = inventory_statement(stmt)

        assert inv.union_count == 1

    def test_counts_case_expression(self, tmp_path):
        stmt = _parse(
            "SELECT CASE WHEN amount > 100 THEN 1 ELSE 0 END AS flag FROM silver.orders;",
            tmp_path,
        )

        inv = inventory_statement(stmt)

        assert inv.case_count == 1

    def test_counts_group_by_and_having(self, tmp_path):
        stmt = _parse(
            "SELECT country, SUM(amount) FROM silver.orders GROUP BY country HAVING SUM(amount) > 10;",
            tmp_path,
        )

        inv = inventory_statement(stmt)

        assert inv.group_by_count == 1
        assert inv.having_count == 1

    def test_carries_source_path_and_line(self, tmp_path):
        stmt = _parse("SELECT 1;", tmp_path, name="b.sql")

        inv = inventory_statement(stmt)

        assert inv.source_path == stmt.source_path
        assert inv.line == stmt.line


class TestInventoryCorpus:
    def test_aggregates_counts_across_statements(self, tmp_path):
        path = tmp_path / "a.sql"
        path.write_text(
            "SELECT SUM(amount) FROM silver.orders;\n"
            "SELECT SUM(amount) FROM silver.refunds;\n"
        )
        statements = parse_script(path)

        corpus = inventory_corpus(statements)

        assert corpus.aggregate_names["sum"] == 2
        assert len(corpus.statements) == 2

    def test_empty_corpus_has_zeroed_counters(self):
        corpus = inventory_corpus([])

        assert corpus.statements == []
        assert corpus.window_count == 0
        assert corpus.subquery_count == 0


def test_legacy_sql_corpus_matches_the_documented_pattern_coverage():
    """End-to-end against examples/24_project_analyzer/legacy_sql/ — the
    baseline the mapper (43.2) will be measured against. Every count here was
    produced by running inventory_corpus() over the real corpus, not guessed."""
    paths = sorted(glob.glob(f"{CORPUS_DIR}/*.sql"))
    assert len(paths) == 8, "corpus file count changed — update this test's expectations"

    all_statements = []
    for path in paths:
        all_statements.extend(parse_script(path))

    corpus = inventory_corpus(all_statements)

    assert len(corpus.statements) == 8
    assert dict(corpus.join_types) == {"left": 2, "inner": 3}
    assert corpus.function_names["upper"] == 3
    assert corpus.function_names["trim"] == 3
    assert corpus.function_names["coalesce"] == 3
    assert dict(corpus.aggregate_names) == {"sum": 9, "lag": 1}
    assert corpus.window_count == 2
    assert corpus.subquery_count == 3
    assert corpus.cte_count == 3
    assert corpus.union_count == 2
    assert corpus.case_count == 2
    assert corpus.group_by_count == 1
    assert corpus.having_count == 1
