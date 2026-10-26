"""Corpus-wide dependency graph over materialized tables (Plan 43 — 43.3).

Only materialized nodes (``CREATE``/``INSERT`` targets) participate in this
graph — CTEs, derived subqueries and temporary views are resolved away to the
real base tables they ultimately read, which is what matters for cycle
detection and layer inference. Splitting non-materialized nodes into
``partials:``/intermediate tables is :mod:`skifer.analyzer.splitter`'s job,
not this module's.
"""
from __future__ import annotations

from dataclasses import dataclass

from skifer.analyzer.parser import Statement

_LAYER_SCHEMA_HINTS: dict[str, str] = {
    "bronze": "bronze",
    "silver": "silver",
    "gold": "gold",
    "stg": "silver",
    "staging": "silver",
    "dwh": "gold",
    "mart": "gold",
}


class DagCycleError(ValueError):
    """A cycle was found among materialized tables; names the cycle in order."""

    def __init__(self, cycle: tuple[str, ...]):
        self.cycle = cycle
        super().__init__("Cycle detected among materialized tables: " + " -> ".join(cycle))


@dataclass(frozen=True)
class Node:
    """One materialized table: what it reads, resolved through any CTEs."""

    target: str
    source_path: str
    reads: tuple[str, ...]
    has_aggregate: bool


@dataclass(frozen=True)
class DagResult:
    nodes: tuple[Node, ...]

    def node(self, target: str) -> Node | None:
        for n in self.nodes:
            if n.target == target:
                return n
        return None


def _cte_names(body) -> set[str]:
    names: set[str] = set()
    for node in (body, getattr(body, "this", None), getattr(body, "expression", None)):
        if node is None:
            continue
        with_ = node.args.get("with_") if hasattr(node, "args") else None
        if with_ is not None:
            names |= {cte.alias_or_name for cte in with_.expressions}
    return names


def _base_tables_read(ast) -> set[str]:
    """Real table names *ast* reads — CTE/temp-view names are excluded."""
    from sqlglot import exp

    from skifer.analyzer.mapper import _extract_target_and_body, _table_fqn

    _, body = _extract_target_and_body(ast)
    if body is None:
        return set()

    cte_names = _cte_names(body)

    tables: set[str] = set()
    for node in ast.walk():
        node = node[0] if isinstance(node, tuple) else node
        if isinstance(node, exp.Table):
            if node.name in cte_names and node.args.get("db") is None and node.args.get("catalog") is None:
                continue
            tables.add(_table_fqn(node))
    return tables


def _statement_has_aggregate(ast) -> bool:
    from skifer.analyzer.mapper import _extract_target_and_body

    _, body = _extract_target_and_body(ast)
    if body is None:
        return False
    bodies = [body]
    if hasattr(body, "this") and hasattr(body, "expression"):
        bodies = [body.this, body.expression] if body.this is not None and body.expression is not None else [body]
    return any(getattr(b, "args", {}).get("group") is not None for b in bodies if b is not None)


def build_dag(statements: list[Statement]) -> DagResult:
    """Build the materialized-table dependency graph for *statements*.

    Raises:
        DagCycleError: a materialized table (transitively) reads itself.
    """
    from skifer.analyzer.mapper import _extract_target_and_body

    nodes: list[Node] = []
    for stmt in statements:
        target, _ = _extract_target_and_body(stmt.ast)
        if target is None:
            continue  # a bare SELECT has no materialized identity at corpus level.
        reads = tuple(sorted(_base_tables_read(stmt.ast) - {target}))
        nodes.append(Node(
            target=target,
            source_path=stmt.source_path,
            reads=reads,
            has_aggregate=_statement_has_aggregate(stmt.ast),
        ))

    by_target = {n.target: n for n in nodes}

    WHITE, GRAY, BLACK = 0, 1, 2
    color = {n.target: WHITE for n in nodes}
    path: list[str] = []

    def visit(t: str) -> None:
        if color.get(t) == BLACK:
            return
        if color.get(t) == GRAY:
            start = path.index(t)
            raise DagCycleError(tuple(path[start:]) + (t,))
        color[t] = GRAY
        path.append(t)
        node = by_target.get(t)
        if node is not None:
            for r in node.reads:
                if r in by_target:
                    visit(r)
        path.pop()
        color[t] = BLACK

    for n in nodes:
        if color[n.target] == WHITE:
            visit(n.target)

    return DagResult(nodes=tuple(nodes))


def infer_layer(target: str, reads: tuple[str, ...], *, has_aggregate: bool, all_consumers_external: bool) -> tuple[str, str]:
    """Heuristic layer assignment for *target*. Returns ``(layer, reason)``.

    The target's own schema segment wins over the heuristic when it names a
    known layer; otherwise: zero in-degree -> bronze, an aggregate or a node
    with no corpus-internal reader -> gold, else silver.
    """
    parts = [p for p in target.lower().split(".")]
    for part in parts:
        if part in _LAYER_SCHEMA_HINTS:
            return _LAYER_SCHEMA_HINTS[part], f"target name contains schema segment '{part}'"

    if not reads:
        return "bronze", "zero in-degree: reads no table produced by this corpus"

    if has_aggregate or all_consumers_external:
        reason = "has an aggregate block" if has_aggregate else "every consumer is outside the corpus"
        return "gold", reason

    return "silver", "default: reads a corpus-produced table, not a terminal aggregate"
