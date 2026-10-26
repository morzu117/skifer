"""Tests for draft/rule-stub emission (Plan 43 — 43.4): staging-then-validation,
overwrite refusal, reproducible re-emission, and the country-rule wiring on
the real corpus.
"""
from __future__ import annotations

import glob

import yaml

from skifer.analyzer.emitter import emit_corpus
from skifer.analyzer.parser import parse_script

CORPUS_DIR = "examples/24_project_analyzer/legacy_sql"


def _corpus_statements():
    statements = []
    for path in sorted(glob.glob(f"{CORPUS_DIR}/*.sql")):
        statements.extend(parse_script(path))
    return statements


class TestCorpusEmission:
    def test_six_mains_and_one_intermediate_table_are_drafted(self, tmp_path):
        report = emit_corpus(_corpus_statements(), out_dir=tmp_path)

        drafts = {a.path for a in report.artifacts if a.kind == "draft"}
        assert drafts == {
            "schemas/.drafts/silver/stg_orders.yaml",
            "schemas/.drafts/silver/stg_customers.yaml",
            "schemas/.drafts/silver/stg_refunds.yaml",
            "schemas/.drafts/silver/orders_enriched.yaml",
            "schemas/.drafts/silver/orders_enriched_agg_sum_amount.yaml",
            "schemas/.drafts/gold/margin_v1.yaml",
            "schemas/.drafts/gold/margin_v2.yaml",
        }
        assert all(a.status == "written" for a in report.artifacts if a.kind == "draft")

    def test_bare_select_scripts_are_skipped_with_a_reason(self, tmp_path):
        report = emit_corpus(_corpus_statements(), out_dir=tmp_path)

        skipped_names = {s.name for s in report.skipped}
        assert f"{CORPUS_DIR}/05_silver_customers_geo.sql" in skipped_names
        assert f"{CORPUS_DIR}/08_kpi_monthly.sql" in skipped_names
        assert all("no CREATE/INSERT target" in s.reason for s in report.skipped
                    if s.name and s.name.endswith(".sql"))

    def test_partials_are_skipped_not_silently_attached(self, tmp_path):
        report = emit_corpus(_corpus_statements(), out_dir=tmp_path)

        partial_names = {s.name for s in report.skipped if s.name in (
            "cleaned_customers_branch1", "cleaned_customers_branch2", "monthly",
        )}
        assert partial_names == {"cleaned_customers_branch1", "cleaned_customers_branch2", "monthly"}

    def test_country_rule_stub_is_generated_and_wired_into_its_users(self, tmp_path):
        report = emit_corpus(_corpus_statements(), out_dir=tmp_path)

        stub = next(a for a in report.artifacts if a.kind == "rule_stub")
        assert stub.path == "rules/_generated/country.py"
        assert stub.status == "written"
        source = (tmp_path / stub.path).read_text()
        assert "def country(df):" in source
        assert 'F.expr("UPPER(TRIM(country))")' in source
        assert "# project_analyzer:marker" in source

        for draft_name in ("stg_customers", "orders_enriched"):
            draft = yaml.safe_load((tmp_path / f"schemas/.drafts/silver/{draft_name}.yaml").read_text())
            assert "country" in draft.get("business_rules", [])
            assert all(
                (entry[1] if isinstance(entry, list) else entry.get("target")) != "country"
                for entry in draft.get("select_final", [])
            )

    def test_every_draft_carries_the_marker_and_metadata(self, tmp_path):
        report = emit_corpus(_corpus_statements(), out_dir=tmp_path)

        for artifact in report.artifacts:
            if artifact.kind != "draft":
                continue
            draft = yaml.safe_load((tmp_path / artifact.path).read_text())
            assert draft["_generated_by"] == "project_analyzer"
            assert "sql_sha256" in draft["metadata"]["source"]
            assert isinstance(draft["metadata"]["needs_curation"], list)

    def test_reemitting_from_a_fresh_parse_is_a_byte_for_byte_noop(self, tmp_path):
        emit_corpus(_corpus_statements(), out_dir=tmp_path)
        report2 = emit_corpus(_corpus_statements(), out_dir=tmp_path)

        assert all(a.status == "unchanged" for a in report2.artifacts)


class TestStagingAndValidation:
    def test_overwrite_is_refused_without_the_marker(self, tmp_path):
        existing = tmp_path / "schemas/.drafts" / "silver" / "stg_orders.yaml"
        existing.parent.mkdir(parents=True)
        existing.write_text("tables: []\n# hand-edited, no marker\n")

        report = emit_corpus(_corpus_statements(), out_dir=tmp_path)

        target = next(a for a in report.artifacts if a.path == "schemas/.drafts/silver/stg_orders.yaml")
        assert target.status == "refused_exists"
        assert existing.read_text() == "tables: []\n# hand-edited, no marker\n"

    def test_a_rule_stub_file_without_the_marker_is_not_overwritten(self, tmp_path):
        stub_path = tmp_path / "rules" / "_generated" / "country.py"
        stub_path.parent.mkdir(parents=True)
        stub_path.write_text("# hand-written\ndef country(df):\n    return {}\n")

        report = emit_corpus(_corpus_statements(), out_dir=tmp_path)

        stub = next(a for a in report.artifacts if a.kind == "rule_stub")
        assert stub.status == "refused_exists"
        assert "hand-written" in stub_path.read_text()

    def test_changed_source_sql_rewrites_a_marked_draft(self, tmp_path):
        emit_corpus(_corpus_statements(), out_dir=tmp_path)

        draft_path = tmp_path / "schemas/.drafts" / "silver" / "stg_orders.yaml"
        draft = yaml.safe_load(draft_path.read_text())
        draft["metadata"]["source"]["sql_sha256"] = "stale"
        draft_path.write_text(yaml.safe_dump(draft, sort_keys=False))

        report = emit_corpus(_corpus_statements(), out_dir=tmp_path)

        target = next(a for a in report.artifacts if a.path == "schemas/.drafts/silver/stg_orders.yaml")
        assert target.status == "written"

    def test_has_residue_is_true_when_anything_is_skipped(self, tmp_path):
        report = emit_corpus(_corpus_statements(), out_dir=tmp_path)

        assert report.has_residue  # the corpus always has skipped partials/non-materialized mains.

    def test_dry_run_validates_without_touching_disk(self, tmp_path):
        report = emit_corpus(_corpus_statements(), out_dir=tmp_path, dry_run=True)

        assert report.artifacts
        assert all(a.status == "would_write" for a in report.artifacts)
        assert not tmp_path.exists() or list(tmp_path.iterdir()) == []


class TestGenericBehavior:
    def _write(self, tmp_path, name: str, sql: str):
        (tmp_path / name).write_text(sql)
        return parse_script(str(tmp_path / name))

    def test_a_clean_single_statement_script_is_drafted(self, tmp_path):
        src_dir = tmp_path / "src"
        src_dir.mkdir()
        statements = self._write(src_dir, "a.sql", "CREATE TABLE silver.t AS SELECT id FROM bronze.t_raw;")

        out_dir = tmp_path / "out"
        report = emit_corpus(statements, out_dir=out_dir)

        assert [a.path for a in report.artifacts] == ["schemas/.drafts/silver/t.yaml"]
        assert report.artifacts[0].status == "written"

    def test_an_unmappable_statement_is_skipped_not_drafted(self, tmp_path):
        src_dir = tmp_path / "src"
        src_dir.mkdir()
        statements = self._write(
            src_dir, "a.sql",
            "CREATE TABLE silver.t AS SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM bronze.t_raw;",
        )

        out_dir = tmp_path / "out"
        report = emit_corpus(statements, out_dir=out_dir)

        assert report.artifacts == ()
        assert len(report.skipped) == 1
        assert "needs_curation" in report.skipped[0].reason
