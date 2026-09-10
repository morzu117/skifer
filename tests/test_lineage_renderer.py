"""
Tests for LineageRenderer — pure Python, no Spark required.
"""
import pytest
from skifer.lineage.tracker import LineageEdge, LineageGraph
from skifer.lineage.renderer import LineageRenderer


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_graph(*edges: LineageEdge) -> LineageGraph:
    g = LineageGraph()
    for e in edges:
        g.add_edge(e)
    return g


def _edge(src_table, src_col, tgt_table, tgt_col, ops=None, edge_type="select"):
    return LineageEdge(src_table, src_col, tgt_table, tgt_col, ops or [], edge_type)


# ---------------------------------------------------------------------------
# to_mermaid
# ---------------------------------------------------------------------------

class TestToMermaid:
    def setup_method(self):
        self.renderer = LineageRenderer()

    def test_starts_with_graph_lr(self):
        g = _make_graph(_edge("t1", "col_a", "t2", "col_b"))
        result = self.renderer.to_mermaid(g)
        assert result.startswith("graph LR")

    def test_custom_direction(self):
        g = _make_graph(_edge("t1", "col_a", "t2", "col_b"))
        result = self.renderer.to_mermaid(g, direction="TD")
        assert result.startswith("graph TD")

    def test_contains_source_label(self):
        g = _make_graph(_edge("bronze.orders", "amount", "silver.fact", "amount_eur"))
        result = self.renderer.to_mermaid(g)
        assert "bronze.orders.amount" in result

    def test_contains_target_label(self):
        g = _make_graph(_edge("bronze.orders", "amount", "silver.fact", "amount_eur"))
        result = self.renderer.to_mermaid(g)
        assert "silver.fact.amount_eur" in result

    def test_select_edge_uses_arrow(self):
        g = _make_graph(_edge("t1", "col_a", "t2", "col_b", edge_type="select"))
        result = self.renderer.to_mermaid(g)
        assert "-->" in result

    def test_join_edge_uses_dotted_arrow(self):
        g = _make_graph(_edge("t1", "col_a", "t2", "col_b", edge_type="join"))
        result = self.renderer.to_mermaid(g)
        assert "-.->" in result

    def test_metric_edge_uses_thick_arrow(self):
        g = _make_graph(_edge("t1", "col_a", "t2", "col_b", edge_type="metric"))
        result = self.renderer.to_mermaid(g)
        assert "==>" in result

    def test_transformations_appear_as_label(self):
        g = _make_graph(_edge("t1", "col_a", "t2", "col_b", ops=["cast:double", "round:2"]))
        result = self.renderer.to_mermaid(g)
        assert "cast:double" in result
        assert "round:2" in result

    def test_join_without_ops_shows_join_label(self):
        g = _make_graph(_edge("t1", "col_a", "t2", "col_b", ops=[], edge_type="join"))
        result = self.renderer.to_mermaid(g)
        assert "join" in result

    def test_empty_graph_returns_only_header(self):
        g = LineageGraph()
        result = self.renderer.to_mermaid(g)
        assert result.strip() == "graph LR"

    def test_node_ids_sanitized(self):
        # Dots in table/column names must be sanitised to underscores
        g = _make_graph(_edge("bronze.orders", "amount", "silver.fact", "amount_eur"))
        result = self.renderer.to_mermaid(g)
        # Node ID should not contain dots (Mermaid syntax)
        lines = result.splitlines()
        # Find lines defining node IDs (not the label lines which use quotes)
        for line in lines:
            if "[" not in line:
                # Edge lines: node IDs must not have dots outside labels
                parts = line.split("|")
                if parts:
                    node_part = parts[0].strip()
                    assert "." not in node_part


# ---------------------------------------------------------------------------
# to_json
# ---------------------------------------------------------------------------

class TestToJson:
    def setup_method(self):
        self.renderer = LineageRenderer()

    def test_returns_dict(self):
        g = _make_graph(_edge("t1", "col_a", "t2", "col_b"))
        result = self.renderer.to_json(g)
        assert isinstance(result, dict)

    def test_has_edges_key(self):
        g = _make_graph(_edge("t1", "col_a", "t2", "col_b"))
        result = self.renderer.to_json(g)
        assert "edges" in result

    def test_has_tables_key(self):
        g = _make_graph(_edge("t1", "col_a", "t2", "col_b"))
        result = self.renderer.to_json(g)
        assert "tables" in result
        assert "t1" in result["tables"]
        assert "t2" in result["tables"]

    def test_has_summary_key(self):
        g = _make_graph(_edge("t1", "col_a", "t2", "col_b"))
        result = self.renderer.to_json(g)
        assert "summary" in result
        assert result["summary"]["total_edges"] == 1

    def test_edge_fields_present(self):
        g = _make_graph(_edge("t1", "col_a", "t2", "col_b", ops=["cast:double"], edge_type="select"))
        result = self.renderer.to_json(g)
        edge = result["edges"][0]
        assert edge["source_table"] == "t1"
        assert edge["source_column"] == "col_a"
        assert edge["target_table"] == "t2"
        assert edge["target_column"] == "col_b"
        assert edge["transformations"] == ["cast:double"]
        assert edge["edge_type"] == "select"

    def test_multiple_edge_types_counted(self):
        g = _make_graph(
            _edge("t1", "a", "t2", "b", edge_type="select"),
            _edge("t1", "c", "t2", "d", edge_type="join"),
        )
        result = self.renderer.to_json(g)
        assert result["summary"]["edge_types"]["select"] == 1
        assert result["summary"]["edge_types"]["join"] == 1

    def test_empty_graph(self):
        result = self.renderer.to_json(LineageGraph())
        assert result["edges"] == []
        assert result["summary"]["total_edges"] == 0


# ---------------------------------------------------------------------------
# to_html
# ---------------------------------------------------------------------------

class TestToHtml:
    def setup_method(self):
        self.renderer = LineageRenderer()

    def test_returns_string(self):
        g = _make_graph(_edge("t1", "col_a", "t2", "col_b"))
        result = self.renderer.to_html(g)
        assert isinstance(result, str)

    def test_contains_mermaid_script(self):
        g = _make_graph(_edge("t1", "col_a", "t2", "col_b"))
        result = self.renderer.to_html(g)
        assert "mermaid" in result.lower()

    def test_contains_doctype(self):
        g = _make_graph(_edge("t1", "col_a", "t2", "col_b"))
        result = self.renderer.to_html(g)
        assert "<!DOCTYPE html>" in result

    def test_contains_graph_content(self):
        g = _make_graph(_edge("bronze.orders", "amount", "silver.fact", "amount_eur"))
        result = self.renderer.to_html(g)
        assert "bronze.orders.amount" in result
