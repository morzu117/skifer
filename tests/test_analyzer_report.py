"""Tests for the analysis report assembly (Plan 43 — 43.2/43.3)."""
from __future__ import annotations

import dataclasses
import glob
import json

from skifer.analyzer.mapper import Residue
from skifer.analyzer.parser import parse_script
from skifer.analyzer.report import (
    EXIT_CLEAN,
    EXIT_NEEDS_REVIEW,
    AnalysisReport,
    InlinedCteReport,
    SplitReport,
    SplitUnitReport,
    StatementReport,
    build_report,
    build_split_report,
)
from skifer.analyzer.roundtrip import VERIFIED

CORPUS_DIR = "examples/24_project_analyzer/legacy_sql"


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

    def test_split_unit_report_to_dict_is_a_closed_allowlist(self):
        report = SplitUnitReport(
            kind="partial", name="c", path="_partials/c.yaml", source_path="a.sql",
            readers=("a.sql",), mapped=True, roundtrip_status=VERIFIED,
        )

        assert set(report.to_dict()) == {
            "kind", "name", "path", "source_path", "readers", "mapped", "roundtrip_status",
        }

    def test_split_report_to_dict_is_a_closed_allowlist(self):
        report = SplitReport(dag_mermaid="graph LR")

        assert set(report.to_dict()) == {"cycle_error", "dag_mermaid", "units", "inlined"}


def _corpus_statements():
    statements = []
    for path in sorted(glob.glob(f"{CORPUS_DIR}/*.sql")):
        statements.extend(parse_script(path))
    return statements


def _parse_split(sql: str, tmp_path, name: str = "a.sql"):
    path = tmp_path / name
    path.write_text(sql)
    return parse_script(path)


class TestBuildSplitReport:
    def test_corpus_monthly_partial_is_reported_with_its_readers_and_blocked_status(self):
        report = build_split_report(_corpus_statements())

        monthly = next(u for u in report.units if u.kind == "partial" and u.name == "monthly")
        assert any("08_kpi_monthly.sql" in r for r in monthly.readers)
        assert monthly.mapped is False
        assert monthly.roundtrip_status is None

    def test_corpus_intermediate_table_is_reported_as_mapped_with_its_readers(self):
        """This intermediate table is a synthetic grand-total rewrite (no
        backing SQL statement — see splitter.py's ``_grand_total_schema_dict``),
        so it is reported as mapped (its schema_dict compiles) but carries no
        round-trip status: there is nothing to diff it against."""
        report = build_split_report(_corpus_statements())

        intermediate = next(u for u in report.units if u.kind == "intermediate_table")
        assert intermediate.mapped is True
        assert intermediate.roundtrip_status is None
        assert len(intermediate.readers) >= 2

    def test_corpus_dag_mermaid_contains_gold_and_silver_edges(self):
        report = build_split_report(_corpus_statements())

        assert "graph LR" in report.dag_mermaid
        assert "margin_v1" in report.dag_mermaid
        assert "orders_enriched" in report.dag_mermaid

    def test_corpus_inlined_cte_is_reported(self):
        report = build_split_report(_corpus_statements())

        assert any(i.cte_name == "latest_snapshot" for i in report.inlined)

    def test_cycle_is_reported_without_raising(self, tmp_path):
        statements = _parse_split(
            "CREATE TABLE a.t1 AS SELECT x FROM a.t2;\n"
            "CREATE TABLE a.t2 AS SELECT x FROM a.t1;\n",
            tmp_path,
        )

        report = build_split_report(statements)

        assert report.cycle_error is not None
        assert "a.t1" in report.cycle_error
        assert report.units == ()

    def test_markdown_includes_decoupage_section_and_mermaid_fence(self):
        report = build_split_report(_corpus_statements())

        md = report.to_markdown()

        assert "## Découpage" in md
        assert "```mermaid" in md
