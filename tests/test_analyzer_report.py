"""Tests for the analysis report assembly (Plan 43 — 43.2)."""
from __future__ import annotations

import dataclasses
import json

from skifer.analyzer.mapper import Residue
from skifer.analyzer.parser import parse_script
from skifer.analyzer.report import (
    EXIT_CLEAN,
    EXIT_NEEDS_REVIEW,
    AnalysisReport,
    StatementReport,
    build_report,
)
from skifer.analyzer.roundtrip import VERIFIED


def _parse(sql: str, tmp_path, name: str = "a.sql"):
    path = tmp_path / name
    path.write_text(sql)
    return parse_script(path)


class TestBuildReport:
    def test_clean_statement_is_clean(self, tmp_path):
        statements = _parse("SELECT id FROM bronze.orders WHERE status = 'ACTIVE';", tmp_path)

        report = build_report(statements)

        assert len(report.statements) == 1
        assert report.statements[0].is_clean
        assert report.exit_code == EXIT_CLEAN

    def test_needs_curation_statement_is_not_clean(self, tmp_path):
        statements = _parse("SELECT id FROM a UNION SELECT id FROM b;", tmp_path)

        report = build_report(statements)

        assert not report.statements[0].is_clean
        assert report.statements[0].mapped is False
        assert report.statements[0].roundtrip_status is None
        assert report.exit_code == EXIT_NEEDS_REVIEW

    def test_mapped_but_unverified_statement_is_not_clean(self, tmp_path):
        statements = _parse("SELECT DISTINCT a, b FROM t;", tmp_path)

        report = build_report(statements)

        assert report.statements[0].mapped is True
        assert report.statements[0].roundtrip_status != VERIFIED
        assert not report.statements[0].is_clean
        assert report.exit_code == EXIT_NEEDS_REVIEW

    def test_mapped_and_verified_but_with_a_residue_is_not_clean(self, tmp_path):
        statements = _parse("SELECT amount * 1.2 AS amount_vat FROM t;", tmp_path)

        report = build_report(statements)

        assert report.statements[0].mapped is True
        assert report.statements[0].roundtrip_status == VERIFIED
        assert report.statements[0].residues  # expr_fallback residue
        assert not report.statements[0].is_clean

    def test_exit_code_is_clean_only_when_every_statement_is_clean(self, tmp_path):
        statements = _parse(
            "SELECT id FROM bronze.orders WHERE status = 'ACTIVE';\n"
            "SELECT id FROM a UNION SELECT id FROM b;\n",
            tmp_path,
        )

        report = build_report(statements)

        assert len(report.statements) == 2
        assert report.exit_code == EXIT_NEEDS_REVIEW


class TestMarkdownAndJson:
    def test_markdown_mentions_target_and_status(self, tmp_path):
        statements = _parse(
            "CREATE TABLE silver.orders AS SELECT id FROM bronze.orders WHERE status = 'ACTIVE';",
            tmp_path,
        )

        md = build_report(statements).to_markdown()

        assert "silver.orders" in md
        assert VERIFIED in md

    def test_markdown_lists_residues_with_blocking_marker(self, tmp_path):
        statements = _parse("SELECT id FROM a UNION SELECT id FROM b;", tmp_path)

        md = build_report(statements).to_markdown()

        assert "needs_curation" in md
        assert "[blocking]" in md
        assert "union_requires_multi_schema" in md

    def test_json_round_trips_through_json_loads(self, tmp_path):
        statements = _parse("SELECT id FROM bronze.orders WHERE status = 'ACTIVE';", tmp_path)

        parsed = json.loads(build_report(statements).to_json())

        assert parsed["statement_count"] == 1
        assert parsed["clean_count"] == 1
        assert parsed["statements"][0]["roundtrip_status"] == VERIFIED


class TestAllowlistSerialization:
    """A field added to these dataclasses later must stay out of the
    serialized output until ``to_dict`` is deliberately updated to expose it —
    the same discipline ``semantic/evidence.py``'s DTOs use.
    """

    def test_statement_report_to_dict_is_a_closed_allowlist(self):
        report = StatementReport(
            source_path="a.sql",
            line=1,
            target=None,
            mapped=True,
            roundtrip_status=VERIFIED,
            roundtrip_detail=None,
            residues=(Residue(code="x", message="m"),),
        )

        assert set(report.to_dict()) == {
            "source_path", "line", "target", "mapped",
            "roundtrip_status", "roundtrip_detail", "residues",
        }

    def test_a_field_added_later_does_not_leak_into_to_dict_until_admitted(self):
        @dataclasses.dataclass(frozen=True)
        class _FutureStatementReport(StatementReport):
            surprise_field: str = "leaked"

        report = _FutureStatementReport(
            source_path="a.sql",
            line=1,
            target=None,
            mapped=True,
            roundtrip_status=VERIFIED,
            roundtrip_detail=None,
        )

        assert "surprise_field" not in report.to_dict()

    def test_analysis_report_to_dict_is_a_closed_allowlist(self):
        report = AnalysisReport(statements=())

        assert set(report.to_dict()) == {"statement_count", "clean_count", "statements"}
