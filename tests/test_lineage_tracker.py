"""
Tests for LineageTracker and LineageGraph — pure Python, no Spark required.
"""
import pytest
from skifer.lineage.tracker import (
    LineageEdge, LineageGraph, LineageTracker,
    _extract_sql_identifiers, _extract_select_entry,
)
from skifer.core.registry import RuleRegistry


# ---------------------------------------------------------------------------
# Minimal rule functions (no Spark import needed at module level)
# ---------------------------------------------------------------------------

def _rule_high_value(df):
    return df.withColumn("is_high_value", df["amount"] >= 1000)


def _rule_flag_vip(df):
    import pyspark.sql.functions as F
    return df.withColumn("is_vip", F.when(F.col("amount") >= 5000, 1).otherwise(0))


# ---------------------------------------------------------------------------
# Minimal schema dicts (already normalised — as if from parse_schema())
# ---------------------------------------------------------------------------

_SCHEMA_SELECT = {
    "tables": [{"name": "bronze.raw_orders", "alias": "ord"}],
    "select_final": [
        ["amount", "amount_eur", ["cast:double", "round:2"]],
        ["customer_id", "customer_id", []],
    ],
}

_SCHEMA_LITERAL = {
    "tables": [{"name": "bronze.raw_orders", "alias": "ord"}],
    "select_final": [
        [None, "source_system", ["lit:ERP"]],   # literal column
    ],
}

_SCHEMA_JOIN = {
    "tables": [
        {"name": "bronze.raw_orders", "alias": "ord"},
        {"name": "silver.customers", "alias": "cust"},
    ],
    "join": [
        {"table_from": "ord", "on_from": "customer_id", "table_to": "cust", "on_to": "id", "type": "left"},
    ],
}

_SCHEMA_ADD_COLUMNS = {
    "tables": [{"name": "bronze.raw_orders", "alias": "ord"}],
    "add_columns": [
        ["amount", "amount_rounded", ["round:2"]],
    ],
}

_SCHEMA_DICT_FORM = {
    "tables": [{"name": "bronze.raw_orders"}],
    "select_final": [
        {"source": "status", "target": "status_label", "ops": [{"when": "equals:DONE", "then": "lit:Paid"}]},
    ],
}


# ---------------------------------------------------------------------------
# LineageEdge
# ---------------------------------------------------------------------------

class TestLineageEdge:
    def test_equality_same_fields(self):
        e1 = LineageEdge("t1", "col_a", "t2", "col_b", [], "select")
        e2 = LineageEdge("t1", "col_a", "t2", "col_b", ["cast:double"], "select")
        assert e1 == e2  # transformations not part of equality

    def test_inequality_different_edge_type(self):
        e1 = LineageEdge("t1", "col_a", "t2", "col_b", [], "select")
        e2 = LineageEdge("t1", "col_a", "t2", "col_b", [], "join")
        assert e1 != e2

    def test_hashable(self):
        e = LineageEdge("t1", "col_a", "t2", "col_b", [], "select")
        assert hash(e) is not None


# ---------------------------------------------------------------------------
# LineageGraph
# ---------------------------------------------------------------------------

class TestLineageGraph:
    def _make_edge(self, src_col="col_a", tgt_col="col_b", edge_type="select"):
        return LineageEdge("src_table", src_col, "tgt_table", tgt_col, [], edge_type)

    def test_add_edge_increases_len(self):
        g = LineageGraph()
        g.add_edge(self._make_edge())
        assert len(g) == 1

    def test_duplicate_edge_not_added(self):
        g = LineageGraph()
        e = self._make_edge()
        g.add_edge(e)
        g.add_edge(e)
        assert len(g) == 1

    def test_tables_returns_all_unique(self):
        g = LineageGraph()
        g.add_edge(self._make_edge("col_a", "col_b"))
        assert "src_table" in g.tables()
        assert "tgt_table" in g.tables()

    def test_upstream_finds_matching_edge(self):
        g = LineageGraph()
        g.add_edge(self._make_edge("col_a", "col_b"))
        edges = g.upstream("tgt_table", "col_b")
        assert len(edges) == 1
        assert edges[0].source_column == "col_a"

    def test_upstream_no_match_returns_empty(self):
        g = LineageGraph()
        g.add_edge(self._make_edge())
        assert g.upstream("tgt_table", "unknown") == []

    def test_downstream_finds_matching_edge(self):
        g = LineageGraph()
        g.add_edge(self._make_edge("col_a", "col_b"))
        edges = g.downstream("src_table", "col_a")
        assert len(edges) == 1
        assert edges[0].target_column == "col_b"

    def test_merge_combines_edges(self):
        g1 = LineageGraph()
        g1.add_edge(self._make_edge("col_a", "col_x"))
        g2 = LineageGraph()
        g2.add_edge(self._make_edge("col_b", "col_y"))
        g1.merge(g2)
        assert len(g1) == 2

    def test_merge_skips_duplicates(self):
        g1 = LineageGraph()
        e = self._make_edge()
        g1.add_edge(e)
        g2 = LineageGraph()
        g2.add_edge(e)
        g1.merge(g2)
        assert len(g1) == 1

    def test_bool_false_when_empty(self):
        assert not LineageGraph()

    def test_bool_true_when_has_edges(self):
        g = LineageGraph()
        g.add_edge(self._make_edge())
        assert g

    def test_to_dict_structure(self):
        g = LineageGraph()
        g.add_edge(LineageEdge("t1", "c1", "t2", "c2", ["cast:double"], "select"))
        d = g.to_dict()
        assert "edges" in d
        assert "tables" in d
        assert "summary" in d
        assert d["summary"]["total_edges"] == 1
        assert d["summary"]["edge_types"]["select"] == 1


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class TestExtractSqlIdentifiers:
    def test_simple_column(self):
        assert "amount_ttc" in _extract_sql_identifiers("amount_ttc")

    def test_sum_expression(self):
        result = _extract_sql_identifiers("SUM(amount_ttc)")
        assert "amount_ttc" in result
        assert "sum" not in result  # keyword filtered

    def test_keywords_excluded(self):
        result = _extract_sql_identifiers("SELECT amount FROM orders WHERE status = 'ok'")
        assert "select" not in result
        assert "from" not in result
        assert "where" not in result

    def test_single_char_excluded(self):
        result = _extract_sql_identifiers("a + b")
        assert "a" not in result
        assert "b" not in result

    def test_complex_expression(self):
        result = _extract_sql_identifiers("COALESCE(unit_price, list_price) * quantity")
        assert "unit_price" in result
        assert "list_price" in result
        assert "quantity" in result


class TestExtractSelectEntry:
    def test_list_form_full(self):
        src, tgt, ops = _extract_select_entry(["amount", "amount_eur", ["cast:double", "round:2"]])
        assert src == "amount"
        assert tgt == "amount_eur"
        assert ops == ["cast:double", "round:2"]

    def test_list_form_no_ops(self):
        src, tgt, ops = _extract_select_entry(["amount", "amount_eur", []])
        assert ops == []

    def test_list_form_literal(self):
        src, tgt, ops = _extract_select_entry([None, "source_system", ["lit:ERP"]])
        assert src is None
        assert tgt == "source_system"

    def test_dict_form(self):
        entry = {"source": "status", "target": "status_label", "ops": [{"when": "equals:DONE"}]}
        src, tgt, ops = _extract_select_entry(entry)
        assert src == "status"
        assert tgt == "status_label"
        assert ops == ["conditional"]

    def test_dict_form_no_ops(self):
        src, tgt, ops = _extract_select_entry({"source": "status", "target": "status_label"})
        assert ops == []


# ---------------------------------------------------------------------------
# LineageTracker.from_schema — Core schema path
# ---------------------------------------------------------------------------

class TestFromSchema:
    def test_select_final_creates_edges(self):
        graph = LineageTracker.from_schema(_SCHEMA_SELECT, target_name="silver.fact_orders")
        assert len(graph) == 2

    def test_select_final_edge_source_table(self):
        graph = LineageTracker.from_schema(_SCHEMA_SELECT, target_name="silver.fact_orders")
        edges = graph.upstream("silver.fact_orders", "amount_eur")
        assert len(edges) == 1
        assert edges[0].source_table == "bronze.raw_orders"
        assert edges[0].source_column == "amount"

    def test_select_final_edge_transformations(self):
        graph = LineageTracker.from_schema(_SCHEMA_SELECT, target_name="silver.fact_orders")
        edges = graph.upstream("silver.fact_orders", "amount_eur")
        assert "cast:double" in edges[0].transformations
        assert "round:2" in edges[0].transformations

    def test_select_final_edge_type_is_select(self):
        graph = LineageTracker.from_schema(_SCHEMA_SELECT, target_name="silver.fact_orders")
        for edge in graph.edges:
            assert edge.edge_type == "select"

    def test_literal_column_uses_placeholder(self):
        graph = LineageTracker.from_schema(_SCHEMA_LITERAL, target_name="output")
        edges = graph.upstream("output", "source_system")
        assert len(edges) == 1
        assert edges[0].source_column == "<literal>"

    def test_add_columns_creates_edges(self):
        graph = LineageTracker.from_schema(_SCHEMA_ADD_COLUMNS, target_name="output")
        edges = graph.upstream("output", "amount_rounded")
        assert len(edges) == 1
        assert edges[0].source_column == "amount"

    def test_join_creates_edge(self):
        graph = LineageTracker.from_schema(_SCHEMA_JOIN, target_name="output")
        join_edges = [e for e in graph.edges if e.edge_type == "join"]
        assert len(join_edges) == 1
        assert join_edges[0].source_table == "bronze.raw_orders"
        assert join_edges[0].source_column == "customer_id"
        assert join_edges[0].target_table == "silver.customers"
        assert join_edges[0].target_column == "id"

    def test_join_resolves_alias_to_fqn(self):
        graph = LineageTracker.from_schema(_SCHEMA_JOIN, target_name="output")
        join_edges = [e for e in graph.edges if e.edge_type == "join"]
        assert join_edges[0].source_table == "bronze.raw_orders"  # alias "ord" resolved
        assert join_edges[0].target_table == "silver.customers"   # alias "cust" resolved

    def test_default_target_name_uses_first_table(self):
        graph = LineageTracker.from_schema(_SCHEMA_SELECT)
        target_tables = {e.target_table for e in graph.edges}
        assert any("output" in t for t in target_tables)

    def test_dict_form_select_final(self):
        graph = LineageTracker.from_schema(_SCHEMA_DICT_FORM, target_name="output")
        edges = graph.upstream("output", "status_label")
        assert len(edges) == 1
        assert edges[0].source_column == "status"
        assert edges[0].transformations == ["conditional"]

    def test_empty_schema_returns_empty_graph(self):
        graph = LineageTracker.from_schema({})
        assert not graph

    def test_tables_populated(self):
        graph = LineageTracker.from_schema(_SCHEMA_SELECT, target_name="silver.fact_orders")
        assert "bronze.raw_orders" in graph.tables()
        assert "silver.fact_orders" in graph.tables()


class TestFromSchemaBusinessRules:
    def setup_method(self):
        RuleRegistry._rules["_test_high_value"] = _rule_high_value
        RuleRegistry._rules["_test_flag_vip"] = _rule_flag_vip

    def teardown_method(self):
        RuleRegistry._rules.pop("_test_high_value", None)
        RuleRegistry._rules.pop("_test_flag_vip", None)

    def _schema(self, rules):
        return {
            "tables": [{"name": "bronze.raw_orders", "alias": "ord"}],
            "business_rules": rules,
        }

    def test_rule_creates_edge(self):
        graph = LineageTracker.from_schema(self._schema(["_test_high_value"]), target_name="output")
        rule_edges = [e for e in graph.edges if e.edge_type == "rule"]
        assert len(rule_edges) >= 1

    def test_rule_edge_output_column(self):
        graph = LineageTracker.from_schema(self._schema(["_test_high_value"]), target_name="output")
        rule_edges = [e for e in graph.edges if e.edge_type == "rule"]
        output_cols = {e.target_column for e in rule_edges}
        assert "is_high_value" in output_cols

    def test_rule_edge_input_column(self):
        graph = LineageTracker.from_schema(self._schema(["_test_high_value"]), target_name="output")
        rule_edges = [e for e in graph.edges if e.edge_type == "rule"]
        input_cols = {e.source_column for e in rule_edges}
        assert "amount" in input_cols

    def test_rule_transformation_label(self):
        graph = LineageTracker.from_schema(self._schema(["_test_high_value"]), target_name="output")
        rule_edges = [e for e in graph.edges if e.edge_type == "rule"]
        assert all("rule:_test_high_value" in e.transformations for e in rule_edges)

    def test_unknown_rule_silently_skipped(self):
        graph = LineageTracker.from_schema(
            self._schema(["_does_not_exist"]), target_name="output"
        )
        rule_edges = [e for e in graph.edges if e.edge_type == "rule"]
        assert rule_edges == []


# ---------------------------------------------------------------------------
# LineageTracker.from_semantic_model — Semantic path
# ---------------------------------------------------------------------------

class TestFromSemanticModel:
    def _model(self, dims=None, metrics=None):
        return {
            "key": "kpi_orders",
            "table": "gold.fact_orders",
            "dimensions": dims or [],
            "metrics": metrics or [],
        }

    def test_dimension_simple_column(self):
        model = self._model(dims=[{"name": "channel", "sql": "channel"}])
        graph = LineageTracker.from_semantic_model(model)
        edges = graph.upstream("kpi_orders", "channel")
        assert len(edges) == 1
        assert edges[0].source_column == "channel"
        assert edges[0].source_table == "gold.fact_orders"

    def test_dimension_edge_type_is_metric(self):
        model = self._model(dims=[{"name": "channel", "sql": "channel"}])
        graph = LineageTracker.from_semantic_model(model)
        for edge in graph.edges:
            assert edge.edge_type == "metric"

    def test_metric_sum_expression(self):
        model = self._model(metrics=[{"name": "revenue", "sql": "SUM(amount_ttc)", "type": "sum"}])
        graph = LineageTracker.from_semantic_model(model)
        edges = graph.upstream("kpi_orders", "revenue")
        assert len(edges) >= 1
        assert any(e.source_column == "amount_ttc" for e in edges)

    def test_metric_aggregation_type_in_transformations(self):
        model = self._model(metrics=[{"name": "revenue", "sql": "SUM(amount_ttc)", "type": "sum"}])
        graph = LineageTracker.from_semantic_model(model)
        edges = graph.upstream("kpi_orders", "revenue")
        assert any("sum" in e.transformations for e in edges)

    def test_metric_no_aggregation_type(self):
        model = self._model(metrics=[{"name": "revenue", "sql": "amount_ttc", "type": ""}])
        graph = LineageTracker.from_semantic_model(model)
        edges = graph.upstream("kpi_orders", "revenue")
        assert len(edges) == 1
        assert edges[0].transformations == []

    def test_uses_model_key_as_target_table(self):
        model = self._model(dims=[{"name": "channel", "sql": "channel"}])
        graph = LineageTracker.from_semantic_model(model)
        assert "kpi_orders" in graph.tables()

    def test_uses_table_as_source_table(self):
        model = self._model(dims=[{"name": "channel", "sql": "channel"}])
        graph = LineageTracker.from_semantic_model(model)
        assert "gold.fact_orders" in graph.tables()

    def test_empty_model_returns_empty_graph(self):
        model = {"key": "empty", "table": "gold.t"}
        graph = LineageTracker.from_semantic_model(model)
        assert not graph

    def test_multi_column_expression(self):
        model = self._model(dims=[{"name": "full_name", "sql": "first_name || ' ' || last_name"}])
        graph = LineageTracker.from_semantic_model(model)
        src_cols = {e.source_column for e in graph.upstream("kpi_orders", "full_name")}
        assert "first_name" in src_cols
        assert "last_name" in src_cols

# ---------------------------------------------------------------------------
# Columns created by a business rule (Plan 34)
# ---------------------------------------------------------------------------

def _rule_classify(df):
    import pyspark.sql.functions as F
    return {"order_class": F.when(F.col("amount") >= 500, "priority").otherwise("standard")}


def test_a_rule_made_column_is_not_attributed_to_a_source_table():
    """`raw_orders.order_class` names a column that does not exist in raw_orders.

    The column is created inside the pipeline by a projection rule, so the select
    that carries it must not claim a source table. Its real origin is the rule edge.
    """
    RuleRegistry.register_rule(name="classify_order_lineage")(_rule_classify)
    schema = {
        "tables": [{"name": "raw_orders", "alias": "ord"}],
        "business_rules": ["classify_order_lineage"],
        "select_final": [["order_class", "order_class"]],
    }

    graph = LineageTracker.from_schema(schema, target_name="gold.orders")
    edges = graph.upstream("gold.orders", "order_class")

    select_edges = [e for e in edges if e.edge_type == "select"]
    rule_edges = [e for e in edges if e.edge_type == "rule"]

    assert [e.source_table for e in select_edges] == ["<rule>"]
    assert [(e.source_table, e.source_column) for e in rule_edges] == [
        ("raw_orders", "amount")
    ]


def test_a_plain_source_column_keeps_its_source_table():
    schema = {
        "tables": [{"name": "raw_orders", "alias": "ord"}],
        "select_final": [["amount", "amount"]],
    }

    graph = LineageTracker.from_schema(schema, target_name="gold.orders")

    assert [e.source_table for e in graph.upstream("gold.orders", "amount")] == [
        "raw_orders"
    ]
