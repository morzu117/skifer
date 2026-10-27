"""Tests for the round-trip oracle (Plan 43 — 43.2): compile a mapped schema
draft back to SQL and compare it structurally against the source statement.
"""
from __future__ import annotations

import glob
from dataclasses import replace

import pytest

from skifer.analyzer.mapper import map_statement
from skifer.analyzer.parser import parse_script
from skifer.analyzer.roundtrip import DIVERGENT, SKIPPED, UNVERIFIED, VERIFIED, verify

CORPUS_DIR = "examples/24_project_analyzer/legacy_sql"

EXPECTED_STATUS = {
    "01_stg_orders.sql": VERIFIED,
    "02_stg_customers.sql": VERIFIED,
    "03_stg_refunds.sql": VERIFIED,
    "04_silver_orders_enriched.sql": VERIFIED,
    "05_silver_customers_geo.sql": SKIPPED,
    "06_gold_margin_v1.sql": VERIFIED,
    "07_gold_margin_v2.sql": SKIPPED,
    "08_kpi_monthly.sql": SKIPPED,
}


def _map_and_verify(sql: str, tmp_path, name: str = "a.sql"):
    path = tmp_path / name
    path.write_text(sql)
    stmt = parse_script(path)[0]
    mapped = map_statement(stmt)
    return mapped, verify(mapped, stmt)


class TestCorpusProperty:
    """The 43.0 synthetic corpus is the oracle's own regression baseline: every
    statement's mapping outcome and round-trip verdict is pinned here so a
    change to the mapper or the compiler that silently alters either is caught.
    """

    @pytest.mark.parametrize("filename", sorted(EXPECTED_STATUS))
    def test_expected_status(self, filename):
        path = f"{CORPUS_DIR}/{filename}"
        stmt = parse_script(path)[0]
        mapped = map_statement(stmt)

        result = verify(mapped, stmt)

        assert result.status == EXPECTED_STATUS[filename]

    def test_every_corpus_file_is_covered(self):
        paths = sorted(glob.glob(f"{CORPUS_DIR}/*.sql"))
        filenames = {p.split("/")[-1] for p in paths}
        assert filenames == set(EXPECTED_STATUS)


class TestVerifiedCases:
    def test_simple_filtered_projection_verifies(self, tmp_path):
        _, result = _map_and_verify(
            "SELECT id, amount FROM bronze.orders WHERE status = 'ACTIVE';",
            tmp_path,
        )

        assert result.status == VERIFIED

    def test_join_with_pushdown_filter_verifies(self, tmp_path):
        _, result = _map_and_verify(
            "SELECT o.id, r.amount FROM bronze.orders o "
            "JOIN bronze.refunds r ON o.id = r.order_id "
            "WHERE o.status = 'ACTIVE';",
            tmp_path,
        )

        assert result.status == VERIFIED

    def test_aggregate_with_pushdown_filter_and_having_verifies(self, tmp_path):
        _, result = _map_and_verify(
            "SELECT country, SUM(amount) AS total FROM t WHERE amount > 0 "
            "GROUP BY country HAVING SUM(amount) > 10;",
            tmp_path,
        )

        assert result.status == VERIFIED

    def test_or_group_with_parenthesized_branch_verifies(self, tmp_path):
        _, result = _map_and_verify(
            "SELECT x FROM t WHERE (region = 'EU' AND flag = 1) OR region = 'US';",
            tmp_path,
        )

        assert result.status == VERIFIED

    def test_apostrophe_literal_verifies(self, tmp_path):
        _, result = _map_and_verify(
            "SELECT id FROM t WHERE name = 'O\\'Brien';",
            tmp_path,
        )

        assert result.status == VERIFIED


class TestUnverifiedCases:
    """Constructs the compiler itself refuses are reported honestly as
    ``unverified`` with the compiler's own reason, never a crash."""

    def test_qualify_dedup_is_unverified_not_a_crash(self, tmp_path):
        mapped, result = _map_and_verify(
            "SELECT * FROM ("
            "  SELECT order_id, "
            "         ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_id DESC) AS rn "
            "  FROM t"
            ") WHERE rn = 1;",
            tmp_path,
        )

        assert not mapped.needs_curation
        assert result.status == UNVERIFIED
        assert "qualify" in result.detail

    def test_distinct_is_unverified_not_a_crash(self, tmp_path):
        mapped, result = _map_and_verify("SELECT DISTINCT a, b FROM t;", tmp_path)

        assert not mapped.needs_curation
        assert result.status == UNVERIFIED


class TestSkippedCases:
    def test_needs_curation_statement_is_skipped(self, tmp_path):
        mapped, result = _map_and_verify(
            "SELECT id FROM a UNION SELECT id FROM b;", tmp_path,
        )

        assert mapped.needs_curation
        assert result.status == SKIPPED


class TestCatchesRealDivergence:
    """The oracle must actually detect a wrong mapping, not just rubber-stamp
    everything the mapper produces."""

    def test_corrupted_join_key_is_reported_divergent(self, tmp_path):
        path = tmp_path / "a.sql"
        path.write_text(
            "SELECT o.id FROM bronze.orders o JOIN bronze.refunds r ON o.id = r.order_id;"
        )
        stmt = parse_script(path)[0]
        mapped = map_statement(stmt)

        corrupted_join = dict(mapped.schema_dict["join"][0])
        corrupted_join["table_to"] = ["r", "wrong_column"]
        corrupted_schema = dict(mapped.schema_dict)
        corrupted_schema["join"] = [corrupted_join]
        corrupted = replace(mapped, schema_dict=corrupted_schema)

        result = verify(corrupted, stmt)

        assert result.status == DIVERGENT
