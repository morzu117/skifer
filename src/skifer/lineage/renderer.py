"""
LineageRenderer — exports a LineageGraph to Mermaid, JSON, or standalone HTML.
"""

from __future__ import annotations

import re

from .tracker import LineageGraph


def _mermaid_node_id(table: str, column: str) -> str:
    """Sanitize (table, column) into a valid Mermaid node ID."""
    raw = f"{table}.{column}"
    return re.sub(r"[^a-zA-Z0-9_]", "_", raw)


def _mermaid_label(table: str, column: str) -> str:
    """Human-readable label for a Mermaid node."""
    return f"{table}.{column}"


class LineageRenderer:
    """
    Renders a LineageGraph to various output formats.

    Usage::

        renderer = LineageRenderer()
        print(renderer.to_mermaid(graph))
        data = renderer.to_json(graph)
        html = renderer.to_html(graph)
    """

    # Edge style per type
    _EDGE_STYLES = {
        "select": "-->",
        "join": "-.->",
        "rule": "-->",
        "metric": "==>",
    }

    def to_mermaid(self, graph: LineageGraph, direction: str = "LR") -> str:
        """
        Render the graph as a Mermaid flowchart string.

        Args:
            graph:      The LineageGraph to render.
            direction:  Mermaid direction ('LR', 'TD', 'RL', 'BT'). Default: 'LR'.

        Returns:
            Mermaid diagram string, ready to embed in Markdown or HTML.
        """
        lines = [f"graph {direction}"]

        for edge in graph.edges:
            src_id = _mermaid_node_id(edge.source_table, edge.source_column)
            tgt_id = _mermaid_node_id(edge.target_table, edge.target_column)
            src_label = _mermaid_label(edge.source_table, edge.source_column)
            tgt_label = _mermaid_label(edge.target_table, edge.target_column)

            # Node definitions with labels
            lines.append(f'    {src_id}["{src_label}"]')
            lines.append(f'    {tgt_id}["{tgt_label}"]')

            # Edge with label
            arrow = self._EDGE_STYLES.get(edge.edge_type, "-->")
            if edge.transformations:
                ops_str = ", ".join(edge.transformations)
                edge_line = f"    {src_id} {arrow}|\"{ops_str}\"| {tgt_id}"
            elif edge.edge_type == "join":
                edge_line = f'    {src_id} {arrow}|"join"| {tgt_id}'
            else:
                edge_line = f"    {src_id} {arrow} {tgt_id}"
            lines.append(edge_line)

        return "\n".join(lines)

    def to_json(self, graph: LineageGraph) -> dict:
        """
        Render the graph as a plain dict (JSON-serialisable).

        Returns the same structure as LineageGraph.to_dict().
        """
        return graph.to_dict()

    def to_html(self, graph: LineageGraph, direction: str = "LR") -> str:
        """
        Render the graph as a standalone HTML page embedding Mermaid.js.

        Args:
            graph:      The LineageGraph to render.
            direction:  Mermaid direction. Default: 'LR'.

        Returns:
            Self-contained HTML string viewable in a browser.
        """
        mermaid_src = self.to_mermaid(graph, direction=direction)
        summary = graph.to_dict()["summary"]
        subtitle = (
            f"{summary['total_edges']} edges — "
            + ", ".join(f"{v} {k}" for k, v in summary["edge_types"].items())
        )
        return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Skifer — Lineage</title>
  <script src="https://cdn.jsdelivr.net/npm/mermaid/dist/mermaid.min.js"></script>
  <style>
    body {{ font-family: sans-serif; margin: 2rem; background: #fafafa; }}
    h1 {{ font-size: 1.4rem; color: #333; }}
    p  {{ font-size: 0.9rem; color: #666; margin-top: 0; }}
    .mermaid {{ background: white; padding: 1rem; border-radius: 8px;
                box-shadow: 0 1px 4px rgba(0,0,0,.12); }}
  </style>
</head>
<body>
  <h1>Column-level Lineage</h1>
  <p>{subtitle}</p>
  <div class="mermaid">
{mermaid_src}
  </div>
  <script>mermaid.initialize({{ startOnLoad: true, theme: 'default' }});</script>
</body>
</html>"""
