"""
LineageTracker — static column-level lineage derived from YAML schemas and semantic models.

Builds a LineageGraph (DAG) from two sources:
  1. Core schemas (select_final, join, add_columns, business_rules) via parse_schema()
  2. Semantic models (dimensions/metrics SQL) via SemanticEngine YAML format

No Spark execution required — analysis is purely static.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Literal

from ..core.rule_analyzer import RuleAnalyzer
from ..core.registry import RuleRegistry


# ---------------------------------------------------------------------------
# SQL keywords to exclude when parsing identifiers from SQL expressions
# ---------------------------------------------------------------------------

_SQL_KEYWORDS = frozenset({
    "select", "from", "where", "and", "or", "not", "in", "is", "null",
    "true", "false", "as", "on", "join", "left", "right", "inner", "outer",
    "full", "cross", "group", "by", "order", "having", "limit", "distinct",
    "case", "when", "then", "else", "end", "sum", "count", "avg", "min",
    "max", "coalesce", "if", "ifnull", "isnull", "cast", "over", "partition",
    "row_number", "rank", "dense_rank", "between", "like", "ilike",
})

_SQL_IDENTIFIER_RE = re.compile(r'\b([a-z_][a-z0-9_]*)\b', re.IGNORECASE)


RULE_ORIGIN = "<rule>"
"""Source table for a column a business rule created inside the pipeline."""


def _business_rule_outputs(rule_names) -> set:
    """The columns the named rules create, as far as static analysis can tell."""
    from ..core.registry import RuleRegistry

    analyzer = RuleAnalyzer()
    outputs: set = set()
    for rule_name in rule_names:
        try:
            rule_spec = RuleRegistry.get_rule(rule_name)
        except ValueError:
            continue
        profile = analyzer.analyze_rule(rule_spec.func, name=rule_name)
        if profile.source_available:
            outputs.update(profile.output_columns)
    return outputs

def _extract_sql_identifiers(sql_expr: str) -> list[str]:
    """
    Best-effort extraction of column identifiers from a SQL expression.
    Filters out SQL keywords, numeric tokens, and single-char names.
    """
    matches = _SQL_IDENTIFIER_RE.findall(sql_expr)
    return [
        m for m in matches
        if m.lower() not in _SQL_KEYWORDS and len(m) > 1
    ]


def _extract_select_entry(entry) -> tuple[str | None, str, list[str]]:
    """
    Parse one row from select_final or add_columns into (src_col, tgt_col, ops).

    Handles both list form [src, tgt, ops] and dict form {source, target, ops}.
    """
    if isinstance(entry, list):
        src_col = entry[0]          # None for literals (already normalised by schema_loader)
        tgt_col = entry[1]
        ops = entry[2] if len(entry) > 2 else []
        if not isinstance(ops, list):
            ops = [ops]
        return src_col, tgt_col, ops
    if isinstance(entry, dict):
        src_col = entry.get("source")
        tgt_col = entry.get("target", "")
        ops_raw = entry.get("ops", [])
        # Dict-form ops (when/then/else) are represented as "conditional"
        ops = ["conditional"] if ops_raw else []
        return src_col, tgt_col, ops
    return None, "", []


# ---------------------------------------------------------------------------
# Core dataclasses
# ---------------------------------------------------------------------------

@dataclass
class LineageEdge:
    """A directed column-level lineage edge in the pipeline DAG."""
    source_table: str
    source_column: str
    target_table: str
    target_column: str
    transformations: list[str] = field(default_factory=list)
    edge_type: Literal["select", "join", "rule", "metric"] = "select"

    def __eq__(self, other):
        if not isinstance(other, LineageEdge):
            return False
        return (
            self.source_table == other.source_table
            and self.source_column == other.source_column
            and self.target_table == other.target_table
            and self.target_column == other.target_column
            and self.edge_type == other.edge_type
        )

    def __hash__(self):
        return hash((
            self.source_table, self.source_column,
            self.target_table, self.target_column,
            self.edge_type,
        ))


class LineageGraph:
    """
    A directed acyclic graph of column-level lineage edges.

    Supports forward traversal (impact analysis) and backward traversal
    (provenance), as well as Mermaid/JSON export via LineageRenderer.
    """

    def __init__(self):
        self._edges: list[LineageEdge] = []

    # ------------------------------------------------------------------
    # Mutation
    # ------------------------------------------------------------------

    def add_edge(self, edge: LineageEdge) -> None:
        """Add an edge to the graph (duplicates silently ignored)."""
        if edge not in self._edges:
            self._edges.append(edge)

    def merge(self, other: "LineageGraph") -> None:
        """Merge all edges from another graph into this one."""
        for edge in other._edges:
            self.add_edge(edge)

    # ------------------------------------------------------------------
    # Traversal
    # ------------------------------------------------------------------

    def upstream(self, table: str, column: str) -> list[LineageEdge]:
        """Return edges whose target is (table, column) — provenance."""
        return [
            e for e in self._edges
            if e.target_table == table and e.target_column == column
        ]

    def downstream(self, table: str, column: str) -> list[LineageEdge]:
        """Return edges whose source is (table, column) — impact analysis."""
        return [
            e for e in self._edges
            if e.source_table == table and e.source_column == column
        ]

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    @property
    def edges(self) -> list[LineageEdge]:
        return list(self._edges)

    def tables(self) -> set[str]:
        """Return all unique table names referenced in the graph."""
        names: set[str] = set()
        for e in self._edges:
            names.add(e.source_table)
            names.add(e.target_table)
        return names

    def __len__(self) -> int:
        return len(self._edges)

    def __bool__(self) -> bool:
        return bool(self._edges)

    # ------------------------------------------------------------------
    # Export
    # ------------------------------------------------------------------

    def to_dict(self) -> dict:
        """Serialize the graph to a plain dict (for JSON export)."""
        edge_type_counts: dict[str, int] = {}
        edges_list = []
        for e in self._edges:
            edge_type_counts[e.edge_type] = edge_type_counts.get(e.edge_type, 0) + 1
            edges_list.append({
                "source_table": e.source_table,
                "source_column": e.source_column,
                "target_table": e.target_table,
                "target_column": e.target_column,
                "transformations": e.transformations,
                "edge_type": e.edge_type,
            })
        return {
            "edges": edges_list,
            "tables": sorted(self.tables()),
            "summary": {
                "total_edges": len(self._edges),
                "edge_types": edge_type_counts,
            },
        }


# ---------------------------------------------------------------------------
# LineageTracker
# ---------------------------------------------------------------------------

class LineageTracker:
    """
    Builds a LineageGraph from a core schema dict or a semantic model dict.

    Usage::

        # From a core pipeline schema
        schema = parse_schema(yaml_str)
        graph = LineageTracker.from_schema(schema, target_name="silver.fact_orders")

        # From a semantic YAML model
        graph = LineageTracker.from_semantic_model(model_dict)

        # Merge both into a unified graph
        graph.merge(LineageTracker.from_semantic_model(model_dict))
    """

    @staticmethod
    def from_schema(
        schema_dict: dict,
        target_name: str | None = None,
    ) -> LineageGraph:
        """
        Build a LineageGraph from a normalised core schema dict.

        Args:
            schema_dict:  Normalised schema dict (output of parse_schema()).
            target_name:  FQN or label for the pipeline output table.
                          Defaults to the first table's name with an "_output" suffix.

        Returns:
            LineageGraph with edges derived from select_final, add_columns,
            join, and business_rules sections.
        """
        from skifer.core.ir import parse_to_ir
        ps = parse_to_ir(schema_dict)
        graph = LineageGraph()

        primary_table = ps.tables[0].name if ps.tables else "unknown"
        target = target_name or f"{primary_table}_output"

        # alias → FQN mapping (for join resolution)
        alias_to_table: dict[str, str] = {pt.alias: pt.name for pt in ps.tables}
        for pt in ps.tables:
            alias_to_table[pt.name] = pt.name  # also allow FQN as key

        def _op_str(op):
            return f"{op.name}:{','.join(op.args)}" if op.args else op.name

        # Columns a business rule creates do not exist in any source table, so a
        # select on one must not be attributed to the primary table: that names a
        # column the reader will not find. Their real origin is the rule edge added
        # in step 4, which carries the rule's own inputs.
        rule_outputs = _business_rule_outputs(ps.business_rules)

        def _source_table_for(column: str | None) -> str:
            return RULE_ORIGIN if column in rule_outputs else primary_table

        # 1. select_final
        for cs in ps.select_final:
            transformations = ["conditional"] if cs.is_conditional else [_op_str(op) for op in cs.ops]
            graph.add_edge(LineageEdge(
                source_table=_source_table_for(cs.source),
                source_column=cs.source if cs.source is not None else "<literal>",
                target_table=target,
                target_column=cs.target,
                transformations=transformations,
                edge_type="select",
            ))

        # 2. add_columns (same structure as select_final)
        for cs in ps.add_columns:
            transformations = ["conditional"] if cs.is_conditional else [_op_str(op) for op in cs.ops]
            graph.add_edge(LineageEdge(
                source_table=_source_table_for(cs.source),
                source_column=cs.source if cs.source is not None else "<literal>",
                target_table=target,
                target_column=cs.target,
                transformations=transformations,
                edge_type="select",
            ))

        # 3. join — edges between join key columns across source tables
        for pj in ps.joins:
            from_table = alias_to_table.get(pj.alias_left, pj.alias_left)
            to_table = alias_to_table.get(pj.alias_right, pj.alias_right)
            for k_left, k_right in zip(pj.keys_left, pj.keys_right):
                graph.add_edge(LineageEdge(
                    source_table=from_table,
                    source_column=k_left,
                    target_table=to_table,
                    target_column=k_right,
                    transformations=[],
                    edge_type="join",
                ))

        # 4. business_rules — via RuleAnalyzer AST introspection
        analyzer = RuleAnalyzer()
        for rule_name in ps.business_rules:
            try:
                rule_spec = RuleRegistry.get_rule(rule_name)
            except ValueError:
                continue  # unknown rule — skip silently

            profile = analyzer.analyze_rule(rule_spec.func, name=rule_name)
            if not profile.source_available:
                continue

            for out_col in profile.output_columns:
                if profile.input_columns:
                    for in_col in profile.input_columns:
                        graph.add_edge(LineageEdge(
                            source_table=primary_table,
                            source_column=in_col,
                            target_table=target,
                            target_column=out_col,
                            transformations=[f"rule:{rule_name}"],
                            edge_type="rule",
                        ))
                else:
                    # Rule writes a column but no inputs detected
                    graph.add_edge(LineageEdge(
                        source_table=primary_table,
                        source_column="<unknown>",
                        target_table=target,
                        target_column=out_col,
                        transformations=[f"rule:{rule_name}"],
                        edge_type="rule",
                    ))

        return graph

    @staticmethod
    @staticmethod
    def selected_subgraph(
        model_dict: dict,
        selected_columns: set[str] | frozenset[str],
    ) -> LineageGraph:
        """Lineage restricted to the members a query actually selected.

        Building the full model graph and handing it to the evidence would carry
        every unselected metric along with it; only the selected targets are
        kept here.
        """
        full = LineageTracker.from_semantic_model(model_dict)
        subgraph = LineageGraph()
        for edge in full.edges:
            if edge.target_column in selected_columns:
                subgraph.add_edge(edge)
        return subgraph

    def from_semantic_model(model_dict: dict) -> LineageGraph:
        """
        Build a LineageGraph from a semantic model YAML dict.

        Args:
            model_dict:  Semantic model dict as loaded by SemanticEngine.
                         Expected keys: "table", "key", "dimensions", "metrics".

        Returns:
            LineageGraph with edges derived from dimension and metric SQL expressions.
            Edge type is "metric" for both dimensions and metrics.
        """
        graph = LineageGraph()
        source_table = model_dict.get("table", "unknown")
        model_key = model_dict.get("key", model_dict.get("name", "unknown_model"))

        for dim in model_dict.get("dimensions", []):
            dim_name = dim.get("name", "")
            sql_expr = dim.get("sql", "")
            if not dim_name:
                continue
            identifiers = _extract_sql_identifiers(sql_expr)
            if not identifiers:
                # sql is a bare column reference (e.g. sql: "channel")
                identifiers = [sql_expr.strip()] if sql_expr.strip() else []
            for src_col in identifiers:
                graph.add_edge(LineageEdge(
                    source_table=source_table,
                    source_column=src_col,
                    target_table=model_key,
                    target_column=dim_name,
                    transformations=[],
                    edge_type="metric",
                ))

        for metric in model_dict.get("metrics", []):
            metric_name = metric.get("name", "")
            sql_expr = metric.get("sql", "")
            agg_type = metric.get("type", "")
            if not metric_name:
                continue
            identifiers = _extract_sql_identifiers(sql_expr)
            if not identifiers:
                identifiers = [sql_expr.strip()] if sql_expr.strip() else []
            ops = [agg_type] if agg_type else []
            for src_col in identifiers:
                graph.add_edge(LineageEdge(
                    source_table=source_table,
                    source_column=src_col,
                    target_table=model_key,
                    target_column=metric_name,
                    transformations=ops,
                    edge_type="metric",
                ))

        return graph
