"""Tests for the deterministic SQL -> Skifer schema mapper (Plan 43 — 43.2)."""
from __future__ import annotations

from skifer.analyzer.mapper import map_statement
from skifer.analyzer.parser import parse_script


def _map(sql: str, tmp_path, name: str = "a.sql"):
    path = tmp_path / name
    path.write_text(sql)
    stmt = parse_script(path)[0]
    return map_statement(stmt)


class TestTargetExtraction:
    def test_create_table_target_is_the_fqn(self, tmp_path):
        mapped = _map("CREATE TABLE silver.orders AS SELECT id FROM bronze.orders;", tmp_path)

        assert mapped.target == "silver.orders"

    def test_insert_into_records_a_target(self, tmp_path):
        mapped = _map("INSERT INTO silver.orders SELECT id FROM bronze.orders;", tmp_path)

        assert mapped.target is not None

    def test_bare_select_has_no_target(self, tmp_path):
        mapped = _map("SELECT id FROM bronze.orders;", tmp_path)

        assert mapped.target is None


class TestFromAndJoin:
    def test_single_table_from(self, tmp_path):
        mapped = _map("SELECT id FROM bronze.orders;", tmp_path)

        assert mapped.schema_dict["tables"] == [{"name": "bronze.orders", "alias": "orders"}]

    def test_explicit_alias_is_kept(self, tmp_path):
        mapped = _map("SELECT o.id FROM bronze.orders o;", tmp_path)

        assert mapped.schema_dict["tables"][0]["alias"] == "o"

    def test_compact_join_key_on_matching_names(self, tmp_path):
        mapped = _map(
            "SELECT o.id FROM bronze.orders o JOIN bronze.refunds r ON o.id = r.id;",
            tmp_path,
        )

        join = mapped.schema_dict["join"][0]
        assert join["table_from"] == ["o", "id"]
        assert join["table_to"] == ["r", "id"]
        assert join["type"] == "inner"

    def test_using_clause_maps_to_compact_join_key(self, tmp_path):
        mapped = _map(
            "SELECT o.id FROM bronze.orders o JOIN bronze.refunds r USING (id);",
            tmp_path,
        )

        join = mapped.schema_dict["join"][0]
        assert join["table_from"] == ["o", "id"]
        assert join["table_to"] == ["r", "id"]

    def test_composite_equi_join_maps_to_list_keys(self, tmp_path):
        mapped = _map(
            "SELECT a.x FROM t a JOIN u b ON a.k1 = b.k1 AND a.k2 = b.k2;",
            tmp_path,
        )

        join = mapped.schema_dict["join"][0]
        assert join["table_from"] == ["a", ["k1", "k2"]]
        assert join["table_to"] == ["b", ["k1", "k2"]]

    def test_left_join_type(self, tmp_path):
        mapped = _map(
            "SELECT o.id FROM bronze.orders o LEFT JOIN bronze.refunds r ON o.id = r.id;",
            tmp_path,
        )

        assert mapped.schema_dict["join"][0]["type"] == "left"

    def test_non_equi_join_is_blocking_curation(self, tmp_path):
        mapped = _map("SELECT a.x FROM t a JOIN u b ON a.k > b.k;", tmp_path)

        assert mapped.needs_curation
        codes = [r.code for r in mapped.blocking_residues]
        assert "non_equi_join_unsupported" in codes

    def test_join_right_side_not_a_plain_table_is_blocking_curation(self, tmp_path):
        mapped = _map(
            "SELECT a.x FROM t a JOIN (SELECT 1 AS k) b ON a.k = b.k;",
            tmp_path,
        )

        assert mapped.needs_curation


class TestWherePushDown:
    def test_single_table_unqualified_filter_pushes_down(self, tmp_path):
        mapped = _map("SELECT id FROM bronze.orders WHERE status = 'ACTIVE';", tmp_path)

        assert mapped.schema_dict["tables"][0]["filter"] == ["status:equals:ACTIVE"]
        assert "where" not in mapped.schema_dict

    def test_multi_table_predicate_goes_to_where(self, tmp_path):
        mapped = _map(
            "SELECT o.id FROM bronze.orders o JOIN bronze.refunds r ON o.id = r.id "
            "WHERE o.amount > r.amount;",
            tmp_path,
        )

        assert mapped.needs_curation  # comparison between two columns has no condition-string form

    def test_qualified_filter_on_nullable_outer_side_stays_in_where(self, tmp_path):
        mapped = _map(
            "SELECT o.id FROM bronze.orders o LEFT JOIN bronze.refunds r ON o.id = r.id "
            "WHERE r.status = 'OPEN';",
            tmp_path,
        )

        assert mapped.schema_dict["where"] == ["status:equals:OPEN"]
        assert "filter" not in mapped.schema_dict["tables"][1]

    def test_qualified_filter_on_non_nullable_side_pushes_down(self, tmp_path):
        mapped = _map(
            "SELECT o.id FROM bronze.orders o LEFT JOIN bronze.refunds r ON o.id = r.id "
            "WHERE o.status = 'ACTIVE';",
            tmp_path,
        )

        assert mapped.schema_dict["tables"][0]["filter"] == ["status:equals:ACTIVE"]

    def test_is_null_and_is_not_null(self, tmp_path):
        mapped = _map(
            "SELECT id FROM t WHERE a IS NULL AND b IS NOT NULL;",
            tmp_path,
        )

        assert mapped.schema_dict["tables"][0]["filter"] == ["a:is_null", "b:is_not_null"]

    def test_in_and_not_in(self, tmp_path):
        mapped = _map(
            "SELECT id FROM t WHERE a IN ('X', 'Y') AND b NOT IN ('Z');",
            tmp_path,
        )

        filt = mapped.schema_dict["tables"][0]["filter"]
        assert filt == ["a:in:X,Y", "b:not_in:Z"]

    def test_not_in_flags_nullable_pitfall_non_blocking(self, tmp_path):
        mapped = _map("SELECT id FROM t WHERE a NOT IN ('Z');", tmp_path)

        assert not mapped.needs_curation
        codes = [r.code for r in mapped.residues]
        assert "not_in_nullable_pitfall" in codes
        assert all(not r.blocking for r in mapped.residues if r.code == "not_in_nullable_pitfall")

    def test_between_and_not_between(self, tmp_path):
        mapped = _map(
            "SELECT id FROM t WHERE a BETWEEN 1 AND 2 AND b NOT BETWEEN 3 AND 4;",
            tmp_path,
        )

        filt = mapped.schema_dict["tables"][0]["filter"]
        assert filt == ["a:between:1,2", "b:not_between:3,4"]

    def test_like_and_not_like(self, tmp_path):
        mapped = _map(
            "SELECT id FROM t WHERE a LIKE 'X%' AND b NOT LIKE 'Y%';",
            tmp_path,
        )

        filt = mapped.schema_dict["tables"][0]["filter"]
        assert filt == ["a:like:X%", "b:not_like:Y%"]

    def test_single_or_group_single_table_pushes_to_filter_groups(self, tmp_path):
        mapped = _map(
            "SELECT id FROM t WHERE region = 'EU' OR region = 'US';",
            tmp_path,
        )

        assert mapped.schema_dict["tables"][0]["filter_groups"] == [
            ["region:equals:EU"], ["region:equals:US"],
        ]
        assert "where_groups" not in mapped.schema_dict

    def test_or_group_with_parenthesized_and_branch(self, tmp_path):
        mapped = _map(
            "SELECT x FROM t WHERE (region = 'EU' AND flag = 1) OR region = 'US';",
            tmp_path,
        )

        assert mapped.schema_dict["tables"][0]["filter_groups"] == [
            ["region:equals:EU", "flag:equals:1"],
            ["region:equals:US"],
        ]

    def test_two_top_level_or_groups_is_blocking_curation(self, tmp_path):
        mapped = _map(
            "SELECT x FROM t WHERE (a = 1 OR a = 2) AND (b = 1 OR b = 2);",
            tmp_path,
        )

        assert mapped.needs_curation
        assert any(r.code == "multiple_or_groups_unsupported" for r in mapped.blocking_residues)

    def test_or_group_over_the_branch_cap_is_blocking_curation(self, tmp_path):
        branches = " OR ".join(f"a = {i}" for i in range(9))
        mapped = _map(f"SELECT x FROM t WHERE {branches};", tmp_path)

        assert mapped.needs_curation
        assert any(r.code == "or_group_limit_exceeded" for r in mapped.blocking_residues)

    def test_unsupported_predicate_is_blocking_curation(self, tmp_path):
        mapped = _map("SELECT id FROM t WHERE a = b;", tmp_path)

        assert mapped.needs_curation
        assert any(r.code == "unsupported_predicate" for r in mapped.blocking_residues)


class TestSelectFinal:
    def test_bare_column_passthrough(self, tmp_path):
        mapped = _map("SELECT id FROM t;", tmp_path)

        assert mapped.schema_dict["select_final"] == [["id", "id", []]]

    def test_aliased_column(self, tmp_path):
        mapped = _map("SELECT id AS order_id FROM t;", tmp_path)

        assert mapped.schema_dict["select_final"] == [["id", "order_id", []]]

    def test_star_only_sets_keep_all_columns(self, tmp_path):
        mapped = _map("SELECT * FROM t;", tmp_path)

        assert mapped.schema_dict["keep_all_columns"] is True
        assert "select_final" not in mapped.schema_dict

    def test_scalar_op_chain(self, tmp_path):
        mapped = _map("SELECT ROUND(CAST(amount AS DOUBLE), 2) AS amount_eur FROM t;", tmp_path)

        source, target, ops = mapped.schema_dict["select_final"][0]
        assert source == "amount"
        assert target == "amount_eur"
        assert ops == ["cast:double", "round:2"]

    def test_literal_shorthand(self, tmp_path):
        mapped = _map("SELECT 'ERP' AS source_system FROM t;", tmp_path)

        assert mapped.schema_dict["select_final"][0] == [None, "source_system", ["lit:ERP"]]

    def test_case_when_else_chain(self, tmp_path):
        mapped = _map(
            "SELECT CASE WHEN status = 'DONE' THEN 'Paid' ELSE 'Unknown' END AS status_label FROM t;",
            tmp_path,
        )

        entry = mapped.schema_dict["select_final"][0]
        assert entry["source"] == "status"
        assert entry["target"] == "status_label"
        assert entry["ops"] == [
            {"when": "equals:DONE", "then": "lit:Paid"},
            {"else": "lit:Unknown"},
        ]

    def test_unmapped_expression_falls_back_to_expr_non_blocking(self, tmp_path):
        mapped = _map("SELECT amount * 1.2 AS amount_vat FROM t;", tmp_path)

        assert not mapped.needs_curation
        entry = mapped.schema_dict["select_final"][0]
        assert entry[0] is None
        assert entry[2][0].startswith("expr:")
        assert any(r.code == "expr_fallback" for r in mapped.residues)


class TestDistinct:
    def test_distinct_pushes_drop_duplicates_on_the_single_table(self, tmp_path):
        mapped = _map("SELECT DISTINCT a, b FROM t;", tmp_path)

        assert mapped.schema_dict["tables"][0]["quality_checks"] == {
            "drop_duplicates_on": ["a", "b"],
        }

    def test_distinct_star_is_blocking_curation(self, tmp_path):
        mapped = _map("SELECT DISTINCT * FROM t;", tmp_path)

        assert mapped.needs_curation
        assert any(r.code == "distinct_star_unsupported" for r in mapped.blocking_residues)

    def test_distinct_with_join_is_blocking_curation(self, tmp_path):
        mapped = _map(
            "SELECT DISTINCT a.x FROM t a JOIN u b ON a.k = b.k;",
            tmp_path,
        )

        assert mapped.needs_curation
        assert any(r.code == "distinct_with_join_unsupported" for r in mapped.blocking_residues)


class TestAggregate:
    def test_group_by_sum_count_distinct_having(self, tmp_path):
        mapped = _map(
            "SELECT country, SUM(amount) AS total, COUNT(DISTINCT id) AS n FROM t "
            "GROUP BY country HAVING SUM(amount) > 10;",
            tmp_path,
        )

        aggregate = mapped.schema_dict["aggregate"]
        assert aggregate["group_by"] == ["country"]
        assert aggregate["measures"] == [
            {"source": "amount", "target": "total", "func": "sum"},
            {"source": "id", "target": "n", "func": "count_distinct"},
        ]
        assert aggregate["having"] == ["total:greater_than:10"]

    def test_count_star(self, tmp_path):
        mapped = _map("SELECT country, COUNT(*) AS n FROM t GROUP BY country;", tmp_path)

        assert mapped.schema_dict["aggregate"]["measures"] == [
            {"source": "*", "target": "n", "func": "count"},
        ]

    def test_where_predicate_still_pushes_down_alongside_aggregate(self, tmp_path):
        mapped = _map(
            "SELECT country, SUM(amount) AS total FROM t WHERE amount > 0 GROUP BY country;",
            tmp_path,
        )

        assert mapped.schema_dict["tables"][0]["filter"] == ["amount:greater_than:0"]
        assert mapped.schema_dict["aggregate"]["group_by"] == ["country"]

    def test_group_by_all_is_blocking_curation(self, tmp_path):
        mapped = _map("SELECT country, SUM(amount) AS total FROM t GROUP BY ALL;", tmp_path)

        assert mapped.needs_curation
        assert any(r.code == "group_by_all_unsupported" for r in mapped.blocking_residues)

    def test_group_by_with_join_is_blocking_curation(self, tmp_path):
        mapped = _map(
            "SELECT o.country, SUM(o.amount) AS total FROM t o JOIN u r ON o.k = r.k "
            "GROUP BY o.country;",
            tmp_path,
        )

        assert mapped.needs_curation
        assert any(r.code == "aggregate_with_join_unsupported" for r in mapped.blocking_residues)

    def test_unsupported_aggregate_function_is_blocking_curation(self, tmp_path):
        mapped = _map(
            "SELECT country, LAG(amount) OVER (ORDER BY amount) AS prev FROM t GROUP BY country;",
            tmp_path,
        )

        assert mapped.needs_curation


class TestQualifyDedup:
    def test_row_number_equals_1_maps_to_preprocess_qualify(self, tmp_path):
        mapped = _map(
            "SELECT * FROM ("
            "  SELECT order_id, amount, "
            "         ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY snapshot_date DESC) AS rn "
            "  FROM silver.order_snapshots"
            ") WHERE rn = 1;",
            tmp_path,
        )

        table = mapped.schema_dict["tables"][0]
        assert table["name"] == "silver.order_snapshots"
        assert table["preprocess"]["qualify"] == {
            "partition_by": ["order_id"],
            "order_by": "snapshot_date",
            "descending": True,
        }
        assert mapped.schema_dict["keep_all_columns"] is True
        assert "filter" not in table

    def test_qualify_dedup_does_not_reprocess_the_rn_filter(self, tmp_path):
        mapped = _map(
            "SELECT * FROM ("
            "  SELECT order_id, "
            "         ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_id DESC) AS rn "
            "  FROM t"
            ") WHERE rn = 1;",
            tmp_path,
        )

        assert "where" not in mapped.schema_dict
        assert "filter" not in mapped.schema_dict["tables"][0]


class TestUnsupportedConstructs:
    def test_cte_is_blocking_curation(self, tmp_path):
        mapped = _map(
            "WITH base AS (SELECT order_id, amount FROM orders WHERE amount > 0) "
            "SELECT order_id, amount FROM base;",
            tmp_path,
        )

        assert mapped.needs_curation
        assert any(r.code == "cte" for r in mapped.blocking_residues)

    def test_union_is_blocking_curation(self, tmp_path):
        mapped = _map("SELECT id FROM a UNION SELECT id FROM b;", tmp_path)

        assert mapped.needs_curation
        assert any(r.code == "union_requires_multi_schema" for r in mapped.blocking_residues)

    def test_subquery_in_from_is_blocking_curation(self, tmp_path):
        mapped = _map("SELECT x FROM (SELECT 1 AS x) sub;", tmp_path)

        assert mapped.needs_curation
        assert any(r.code == "subquery_in_from" for r in mapped.blocking_residues)

    def test_subquery_in_select_is_blocking_curation(self, tmp_path):
        mapped = _map(
            "SELECT id, (SELECT MAX(amount) FROM t2) AS max_amount FROM t1;",
            tmp_path,
        )

        assert mapped.needs_curation
        assert any(r.code == "subquery_in_select" for r in mapped.blocking_residues)

    def test_subquery_in_where_is_blocking_curation(self, tmp_path):
        mapped = _map(
            "SELECT id FROM t1 WHERE id IN (SELECT id FROM t2);",
            tmp_path,
        )

        assert mapped.needs_curation
        assert any(r.code == "subquery_in_where" for r in mapped.blocking_residues)

    def test_window_function_other_than_dedup_is_blocking_curation(self, tmp_path):
        mapped = _map(
            "SELECT id, LAG(amount) OVER (ORDER BY id) AS prev_amount FROM t;",
            tmp_path,
        )

        assert mapped.needs_curation
        assert any(r.code == "window_function_unsupported" for r in mapped.blocking_residues)

    def test_empty_statement_is_blocking_curation(self, tmp_path):
        mapped = _map("CREATE TABLE t (id INT);", tmp_path)

        assert mapped.needs_curation
        assert any(r.code == "empty_statement" for r in mapped.blocking_residues)
