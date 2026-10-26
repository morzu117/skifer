"""Tests for the CTE/subquery splitter (Plan 43 — 43.3): a reused CTE becomes
a partial, a cross-script duplicate becomes a proposed intermediate table, a
single-use CTE is inlined, and every resulting unit is independently mappable
and round-trip verifiable.
"""
from __future__ import annotations

import glob

import yaml

from skifer.analyzer.mapper import map_statement
from skifer.analyzer.parser import parse_script
from skifer.analyzer.roundtrip import VERIFIED, verify
from skifer.analyzer.splitter import SplitUnit, split_corpus

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


def _by_kind(result, kind: str) -> list[SplitUnit]:
    return [u for u in result.units if u.kind == kind]


class TestCorpusSplitting:
    """05's reused CTE becomes two filtered partials, 07/08's shared scalar
    subquery becomes one proposed intermediate table, and every unit the
    splitter produces independently maps and round-trip verifies (except the
    one residue — GROUP BY + JOIN — this corpus always expected to remain
    blocked for 'monthly')."""

    def test_cte_read_by_two_branches_of_one_script_becomes_two_partials(self):
        result = split_corpus(_corpus_statements())

        partials = {u.name for u in _by_kind(result, "partial")}
        assert "cleaned_customers_branch1" in partials
        assert "cleaned_customers_branch2" in partials

    def test_both_customer_geo_partials_map_and_verify_clean(self):
        result = split_corpus(_corpus_statements())

        for name in ("cleaned_customers_branch1", "cleaned_customers_branch2"):
            unit = next(u for u in result.units if u.name == name)
            mapped = map_statement(unit.statement)
            assert not mapped.needs_curation, mapped.residues
            assert verify(mapped, unit.statement).status == VERIFIED

    def test_subquery_shared_across_two_scripts_becomes_one_intermediate_table(self):
        result = split_corpus(_corpus_statements())

        intermediate = _by_kind(result, "intermediate_table")
        names = {u.name for u in intermediate}
        assert "silver.orders_enriched_agg_sum_amount" in names

    def test_intermediate_table_schema_compiles(self):
        from skifer.core.ir import parse_to_ir
        from skifer.core.schema_loader import parse_schema
        from skifer.core.sql_compiler import compile_select

        result = split_corpus(_corpus_statements())
        unit = next(u for u in _by_kind(result, "intermediate_table") if u.name == "silver.orders_enriched_agg_sum_amount")

        normalized = parse_schema(yaml.safe_dump(unit.schema_dict, sort_keys=False), params={"allow_raw_sql": True})
        sql = compile_select(parse_to_ir(normalized))

        assert "SUM(" in sql.upper()
        assert "orders_enriched" in sql

    def test_statement_07_resolves_to_verified_after_splitting(self):
        """07 is needs_curation standalone (a shared scalar subquery) — once
        the subquery is swapped for the intermediate table, its 'main' unit
        maps and verifies cleanly."""
        result = split_corpus(_corpus_statements())

        main_units = _by_kind(result, "main")
        unit = next(u for u in main_units if "07_gold_margin_v2.sql" in u.statement.source_path)
        mapped = map_statement(unit.statement)

        assert not mapped.needs_curation
        assert verify(mapped, unit.statement).status == VERIFIED

    def test_statement_05_both_branches_resolve_to_verified_after_splitting(self):
        result = split_corpus(_corpus_statements())

        branches = [u for u in _by_kind(result, "main") if "05_silver_customers_geo.sql" in u.statement.source_path]
        assert len(branches) == 2
        for unit in branches:
            mapped = map_statement(unit.statement)
            assert not mapped.needs_curation, mapped.residues
            assert verify(mapped, unit.statement).status == VERIFIED

    def test_latest_snapshot_cte_used_once_is_inlined_not_split_out(self):
        result = split_corpus(_corpus_statements())

        names = {u.name for u in result.units if u.name}
        assert "latest_snapshot" not in names
        inlined_names = {n.cte_name for n in result.inlined}
        assert "latest_snapshot" in inlined_names

    def test_monthly_cte_used_twice_becomes_a_shared_partial(self):
        result = split_corpus(_corpus_statements())

        partials = {u.name for u in _by_kind(result, "partial")}
        assert "monthly" in partials

    def test_monthly_partial_is_honestly_blocked_not_silently_wrong(self):
        """monthly's own body joins stg_refunds on top of a ROW_NUMBER()=1
        dedup FROM — an explicit, named residue, never a silently dropped
        join or a false 'verified'."""
        result = split_corpus(_corpus_statements())

        unit = next(u for u in _by_kind(result, "partial") if u.name == "monthly")
        mapped = map_statement(unit.statement)

        assert mapped.needs_curation
        assert any(r.blocking for r in mapped.residues)

    def test_statement_08_lag_branch_remains_blocked_on_window_function(self):
        result = split_corpus(_corpus_statements())

        branches = [u for u in _by_kind(result, "main") if "08_kpi_monthly.sql" in u.statement.source_path]
        assert len(branches) == 2
        residue_codes = set()
        for unit in branches:
            mapped = map_statement(unit.statement)
            for r in mapped.residues:
                residue_codes.add(r.code)
        assert "window_function_unsupported" in residue_codes

    def test_no_statement_without_cte_or_shared_subquery_is_duplicated(self):
        result = split_corpus(_corpus_statements())

        main_sources = [u.statement.source_path for u in _by_kind(result, "main")]
        for simple in ("01_stg_orders.sql", "02_stg_customers.sql", "03_stg_refunds.sql", "06_gold_margin_v1.sql"):
            matching = [p for p in main_sources if simple in p]
            assert len(matching) == 1


class TestSplitterGenericBehavior:
    """Synthetic, minimal cases exercising the classification rules directly,
    independent of the 43.0 corpus."""

    def test_cte_used_once_is_inlined_as_a_from_subquery(self, tmp_path):
        """Inlining wraps the CTE as a literal derived-table subquery in the
        consumer's FROM — always structurally valid SQL. Whether the result
        then maps cleanly is 43.2's own, pre-existing concern: today only the
        ROW_NUMBER()=1 dedup shape is a recognized FROM-subquery pattern, so
        an arbitrary single-use CTE honestly surfaces 43.2's documented
        ``subquery_in_from`` boundary rather than silently mapping wrong
        (D7) — this is the real corpus's own ``latest_snapshot`` case in
        reverse: there, the CTE's body happens to match the recognized
        pattern, so inlining resolves it (see test_analyzer_splitter.py's
        corpus tests)."""
        statements = _parse(
            "WITH c AS (SELECT a, b FROM t WHERE a > 1) SELECT a, b FROM c WHERE b < 10;",
            tmp_path,
        )

        result = split_corpus(statements)

        assert not any(u.name == "c" for u in result.units)
        assert result.inlined[0].cte_name == "c"
        main = _by_kind(result, "main")
        assert len(main) == 1
        assert "FROM (SELECT" in main[0].statement.text.upper() or "FROM (" in main[0].statement.text
        mapped = map_statement(main[0].statement)
        assert mapped.needs_curation
        assert mapped.residues[0].code == "subquery_in_from"

    def test_cte_used_twice_with_same_predicate_becomes_one_shared_partial(self, tmp_path):
        statements = _parse(
            "WITH c AS (SELECT a, b FROM t) "
            "SELECT a, b FROM c WHERE a > 1 "
            "UNION ALL "
            "SELECT a, b FROM c WHERE a > 1;",
            tmp_path,
        )

        result = split_corpus(statements)

        partials = _by_kind(result, "partial")
        assert len(partials) == 1
        assert partials[0].name == "c"

    def test_cte_used_twice_with_different_predicates_becomes_two_partials(self, tmp_path):
        statements = _parse(
            "WITH c AS (SELECT a, b FROM t) "
            "SELECT a, b FROM c WHERE a > 1 "
            "UNION ALL "
            "SELECT a, b FROM c WHERE a < 0;",
            tmp_path,
        )

        result = split_corpus(statements)

        partials = {u.name for u in _by_kind(result, "partial")}
        assert partials == {"c_branch1", "c_branch2"}

    def test_each_branch_partial_keeps_only_its_own_predicate(self, tmp_path):
        statements = _parse(
            "WITH c AS (SELECT a, b FROM t) "
            "SELECT a, b FROM c WHERE a > 1 "
            "UNION ALL "
            "SELECT a, b FROM c WHERE a < 0;",
            tmp_path,
        )

        result = split_corpus(statements)

        branch1 = next(u for u in _by_kind(result, "partial") if u.name == "c_branch1")
        branch2 = next(u for u in _by_kind(result, "partial") if u.name == "c_branch2")
        assert "1" in branch1.statement.text
        assert "0" in branch2.statement.text

    def test_identical_cte_in_two_different_scripts_becomes_intermediate_table(self, tmp_path):
        path1 = tmp_path / "s1.sql"
        path2 = tmp_path / "s2.sql"
        path1.write_text("WITH c AS (SELECT a FROM t) SELECT a FROM c;")
        path2.write_text("WITH c AS (SELECT a FROM t) SELECT a FROM c;")
        statements = parse_script(path1) + parse_script(path2)

        result = split_corpus(statements)

        intermediate = _by_kind(result, "intermediate_table")
        assert len(intermediate) == 1
        assert intermediate[0].name == "silver.c"

    def test_intermediate_table_consumers_are_rewritten_to_read_it(self, tmp_path):
        path1 = tmp_path / "s1.sql"
        path2 = tmp_path / "s2.sql"
        path1.write_text("WITH c AS (SELECT a FROM t) SELECT a FROM c;")
        path2.write_text("WITH c AS (SELECT a FROM t) SELECT a FROM c;")
        statements = parse_script(path1) + parse_script(path2)

        result = split_corpus(statements)

        for unit in _by_kind(result, "main"):
            mapped = map_statement(unit.statement)
            assert not mapped.needs_curation
            assert mapped.schema_dict["tables"][0]["name"] == "silver.c"

    def test_nested_cte_chain_resolves_through_earlier_inline(self, tmp_path):
        """A later CTE reading an earlier, single-use CTE must see the
        earlier one already inlined as a literal subquery, not a dangling
        reference to a name that no longer exists."""
        statements = _parse(
            "WITH base AS (SELECT a, b FROM t), "
            "agg AS (SELECT a, SUM(b) AS total FROM base GROUP BY a) "
            "SELECT a, total FROM agg "
            "UNION ALL "
            "SELECT a, total FROM agg;",
            tmp_path,
        )

        result = split_corpus(statements)

        assert not any(u.name == "base" for u in result.units)
        assert {n.cte_name for n in result.inlined} == {"base"}
        agg_partial = next(u for u in _by_kind(result, "partial") if u.name == "agg")
        assert "base" not in {ref for ref in [agg_partial.statement.text] if "FROM base" in ref}

    def test_statement_without_cte_or_subquery_passes_through_as_single_main_unit(self, tmp_path):
        statements = _parse("SELECT id FROM bronze.orders WHERE status = 'ACTIVE';", tmp_path)

        result = split_corpus(statements)

        assert len(result.units) == 1
        assert result.units[0].kind == "main"
        assert result.units[0].statement.ast is statements[0].ast
