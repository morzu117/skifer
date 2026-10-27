"""Tests for recurring-expression detection (Plan 43 — 43.4): the corpus
produces exactly the shared-rule candidate, divergence and below-threshold
entries its own design comments describe, plus synthetic coverage for the
parameterized (literal-variant) promotion path.
"""
from __future__ import annotations

import glob

from skifer.analyzer.parser import parse_script
from skifer.analyzer.patterns import PatternThresholds, find_patterns

CORPUS_DIR = "examples/24_project_analyzer/legacy_sql"


def _corpus_statements():
    statements = []
    for path in sorted(glob.glob(f"{CORPUS_DIR}/*.sql")):
        statements.extend(parse_script(path))
    return statements


def _parse(sql: str, tmp_path, name: str = "a.sql"):
    path = tmp_path / name
    path.write_text(sql)
    return parse_script(path)


class TestCorpusPatterns:
    def test_country_expression_becomes_a_shared_rule_candidate(self):
        report = find_patterns(_corpus_statements())

        names = {c.name for c in report.shared_rule_candidates}
        assert "country" in names
        country = next(c for c in report.shared_rule_candidates if c.name == "country")
        assert len(country.occurrences) == 3
        assert {o.source_path for o in country.occurrences} == {
            f"{CORPUS_DIR}/02_stg_customers.sql",
            f"{CORPUS_DIR}/04_silver_orders_enriched.sql",
            f"{CORPUS_DIR}/05_silver_customers_geo.sql",
        }
        assert country.kind == "projection"
        assert not country.is_parameterized

    def test_net_margin_is_a_divergence_not_a_shared_candidate(self):
        report = find_patterns(_corpus_statements())

        divergence = next(d for d in report.divergences if d.output_alias == "net_margin")
        assert len(divergence.occurrences) == 2
        assert {o.source_path for o in divergence.occurrences} == {
            f"{CORPUS_DIR}/06_gold_margin_v1.sql",
            f"{CORPUS_DIR}/07_gold_margin_v2.sql",
        }
        assert not any(c.name == "net_margin" for c in report.shared_rule_candidates)

    def test_vat_markup_on_different_columns_is_not_merged_or_reported(self):
        """04's ``amount * 1.2`` and 08's ``total_amount * 1.21`` share an
        operator shape but not a column — grouping abstracts literal values,
        never column identity (that would merge unrelated business concepts
        that merely happen to share an operator), so each stays its own
        singleton occurrence and is reported nowhere (below_threshold only
        surfaces groups of >= 2)."""
        report = find_patterns(_corpus_statements())

        all_occurrences = [
            o for c in report.shared_rule_candidates for o in c.occurrences
        ] + [
            o for b in report.below_threshold for o in b.occurrences
        ] + [
            o for d in report.divergences for o in d.occurrences
        ]
        vat_aliases = {o.output_alias for o in all_occurrences if "vat" in o.output_alias}
        assert vat_aliases == set()

    def test_discount_coalesce_stays_below_threshold(self):
        report = find_patterns(_corpus_statements())

        discount = [b for b in report.below_threshold if any(o.output_alias == "discount" for o in b.occurrences)]
        assert len(discount) == 1
        assert discount[0].occurrence_count == 2

    def test_a_one_off_case_when_is_not_reported_anywhere(self):
        report = find_patterns(_corpus_statements())

        all_aliases = {o.output_alias for c in report.shared_rule_candidates for o in c.occurrences}
        all_aliases |= {o.output_alias for b in report.below_threshold for o in b.occurrences}
        assert "status_label" not in all_aliases
        assert "is_vip" not in all_aliases


class TestGenericBehavior:
    def test_three_identical_occurrences_across_three_scripts_is_promoted(self, tmp_path):
        for i in range(3):
            (tmp_path / f"s{i}.sql").write_text(
                f"CREATE TABLE a.t{i} AS SELECT UPPER(TRIM(x)) AS cleaned_x FROM a.src;"
            )
        statements = []
        for i in range(3):
            statements.extend(parse_script(str(tmp_path / f"s{i}.sql")))

        report = find_patterns(statements)

        assert len(report.shared_rule_candidates) == 1
        assert report.shared_rule_candidates[0].name == "cleaned_x"

    def test_literal_variant_above_threshold_is_parameterized(self, tmp_path):
        for i, literal in enumerate((1.1, 1.2, 1.3)):
            (tmp_path / f"s{i}.sql").write_text(
                f"CREATE TABLE a.t{i} AS SELECT amount * {literal} AS amount_vat FROM a.src;"
            )
        statements = []
        for i in range(3):
            statements.extend(parse_script(str(tmp_path / f"s{i}.sql")))

        report = find_patterns(statements)

        assert len(report.shared_rule_candidates) == 1
        candidate = report.shared_rule_candidates[0]
        assert candidate.is_parameterized
        assert {o.literal_values for o in candidate.occurrences} == {("1.1",), ("1.2",), ("1.3",)}

    def test_two_occurrences_below_default_threshold_of_three(self, tmp_path):
        (tmp_path / "t1.sql").write_text("CREATE TABLE a.t1 AS SELECT COALESCE(x, 0) AS cleaned FROM a.src;")
        (tmp_path / "t2.sql").write_text("CREATE TABLE a.t2 AS SELECT COALESCE(x, 0) AS cleaned FROM a.other;")
        statements = parse_script(str(tmp_path / "t1.sql")) + parse_script(str(tmp_path / "t2.sql"))

        report = find_patterns(statements)

        assert report.shared_rule_candidates == ()
        assert len(report.below_threshold) == 1
        assert report.below_threshold[0].occurrence_count == 2

    def test_custom_thresholds_promote_a_two_occurrence_pattern(self, tmp_path):
        (tmp_path / "t1.sql").write_text("CREATE TABLE a.t1 AS SELECT COALESCE(x, 0) AS cleaned FROM a.src;")
        (tmp_path / "t2.sql").write_text("CREATE TABLE a.t2 AS SELECT COALESCE(x, 0) AS cleaned FROM a.other;")
        statements = parse_script(str(tmp_path / "t1.sql")) + parse_script(str(tmp_path / "t2.sql"))

        report = find_patterns(statements, thresholds=PatternThresholds(min_occurrences=2, min_scripts=2))

        assert len(report.shared_rule_candidates) == 1

    def test_same_alias_different_formula_is_a_divergence(self, tmp_path):
        (tmp_path / "t1.sql").write_text("CREATE TABLE a.t1 AS SELECT amount - cost AS net_margin FROM a.src;")
        (tmp_path / "t2.sql").write_text("CREATE TABLE a.t2 AS SELECT amount * (1 - rate) AS net_margin FROM a.other;")
        statements = parse_script(str(tmp_path / "t1.sql")) + parse_script(str(tmp_path / "t2.sql"))

        report = find_patterns(statements)

        assert len(report.divergences) == 1
        assert report.divergences[0].output_alias == "net_margin"

    def test_bare_column_passthrough_is_not_a_candidate(self, tmp_path):
        statements = _parse("SELECT id FROM a.t;", tmp_path)

        report = find_patterns(statements)

        assert report.shared_rule_candidates == ()
        assert report.below_threshold == ()
        assert report.divergences == ()
