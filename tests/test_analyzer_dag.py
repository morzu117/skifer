"""Tests for the corpus-wide materialized-table dependency graph (Plan 43 — 43.3)."""
from __future__ import annotations

import glob

import pytest

from skifer.analyzer.dag import DagCycleError, build_dag, infer_layer
from skifer.analyzer.parser import parse_script

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


class TestBuildDag:
    def test_corpus_has_one_node_per_materialized_target(self):
        dag = build_dag(_corpus_statements())

        targets = {n.target for n in dag.nodes}
        assert targets == {
            "silver.stg_orders",
            "silver.stg_customers",
            "silver.stg_refunds",
            "silver.orders_enriched",
            "gold.margin_v1",
            "gold.margin_v2",
        }

    def test_bare_select_has_no_materialized_node(self, tmp_path):
        # 05 and 08 in the real corpus are bare SELECTs (no CREATE/INSERT) —
        # they contribute no node to the materialized-table graph.
        statements = _parse("WITH c AS (SELECT a FROM t) SELECT a FROM c;", tmp_path)

        dag = build_dag(statements)

        assert dag.nodes == ()

    def test_node_reads_resolve_through_ctes_to_base_tables(self):
        dag = build_dag(_corpus_statements())

        enriched = dag.node("silver.orders_enriched")
        assert enriched is not None
        assert set(enriched.reads) == {"silver.stg_orders", "silver.stg_customers", "silver.stg_refunds"}

    def test_node_excludes_self_reference(self, tmp_path):
        statements = _parse("CREATE TABLE a.t AS SELECT x FROM a.t;", tmp_path)

        dag = build_dag(statements)

        assert dag.node("a.t").reads == ()

    def test_cycle_raises_with_the_cycle_named(self, tmp_path):
        statements = _parse(
            "CREATE TABLE a.t1 AS SELECT x FROM a.t2;\n"
            "CREATE TABLE a.t2 AS SELECT x FROM a.t1;\n",
            tmp_path,
        )

        with pytest.raises(DagCycleError) as exc_info:
            build_dag(statements)

        assert exc_info.value.cycle == ("a.t1", "a.t2", "a.t1")

    def test_three_way_cycle_is_detected(self, tmp_path):
        statements = _parse(
            "CREATE TABLE a.t1 AS SELECT x FROM a.t2;\n"
            "CREATE TABLE a.t2 AS SELECT x FROM a.t3;\n"
            "CREATE TABLE a.t3 AS SELECT x FROM a.t1;\n",
            tmp_path,
        )

        with pytest.raises(DagCycleError):
            build_dag(statements)

    def test_has_aggregate_is_detected_from_group_by(self, tmp_path):
        statements = _parse(
            "CREATE TABLE a.t AS SELECT region, SUM(amount) AS total FROM a.src GROUP BY region;",
            tmp_path,
        )

        dag = build_dag(statements)

        assert dag.node("a.t").has_aggregate is True

    def test_has_aggregate_is_false_without_group_by(self):
        dag = build_dag(_corpus_statements())

        assert dag.node("silver.stg_orders").has_aggregate is False


class TestInferLayer:
    def test_schema_name_hint_wins_over_heuristic(self):
        layer, reason = infer_layer("gold.whatever", ("bronze.x",), has_aggregate=False, all_consumers_external=False)

        assert layer == "gold"
        assert "gold" in reason

    def test_staging_hint_maps_to_silver(self):
        layer, _ = infer_layer("stg.orders", (), has_aggregate=False, all_consumers_external=False)

        assert layer == "silver"

    def test_dwh_hint_maps_to_gold(self):
        layer, _ = infer_layer("dwh.kpis", (), has_aggregate=False, all_consumers_external=False)

        assert layer == "gold"

    def test_zero_in_degree_without_hint_is_bronze(self):
        layer, reason = infer_layer("raw.orders", (), has_aggregate=False, all_consumers_external=False)

        assert layer == "bronze"
        assert "zero in-degree" in reason

    def test_aggregate_without_hint_is_gold(self):
        layer, reason = infer_layer("raw.kpis", ("raw.orders",), has_aggregate=True, all_consumers_external=False)

        assert layer == "gold"
        assert "aggregate" in reason

    def test_externally_consumed_leaf_without_hint_is_gold(self):
        layer, reason = infer_layer("raw.report", ("raw.orders",), has_aggregate=False, all_consumers_external=True)

        assert layer == "gold"
        assert "consumer" in reason

    def test_default_reader_without_hint_is_silver(self):
        layer, reason = infer_layer("raw.mid", ("raw.orders",), has_aggregate=False, all_consumers_external=False)

        assert layer == "silver"
        assert "default" in reason

    def test_hint_matches_whole_schema_segment_not_substring(self):
        # 'goldfinch' must not match the 'gold' hint via naive substring search.
        layer, reason = infer_layer("goldfinch.orders", (), has_aggregate=False, all_consumers_external=False)

        assert layer == "bronze"
        assert "goldfinch" not in reason or "zero in-degree" in reason
