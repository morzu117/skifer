"""AST node-type inventory over parsed statements (Plan 43 — 43.0).

Pure counting: nothing here maps a construct to the Skifer YAML grammar. This
is the raw coverage baseline the mapper (43.2) will be measured against,
independent of what the mapper can or cannot translate yet.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field

from skifer.analyzer.parser import Statement


@dataclass
class StatementInventory:
    """AST shape of a single statement."""

    source_path: str
    line: int
    node_counts: Counter = field(default_factory=Counter)
    join_types: Counter = field(default_factory=Counter)
    function_names: Counter = field(default_factory=Counter)
    aggregate_names: Counter = field(default_factory=Counter)
    window_count: int = 0
    subquery_count: int = 0
    cte_count: int = 0
    union_count: int = 0
    case_count: int = 0
    group_by_count: int = 0
    having_count: int = 0


@dataclass
class CorpusInventory:
    """AST shape summed across every statement of a corpus."""

    statements: list[StatementInventory] = field(default_factory=list)
    node_counts: Counter = field(default_factory=Counter)
    join_types: Counter = field(default_factory=Counter)
    function_names: Counter = field(default_factory=Counter)
    aggregate_names: Counter = field(default_factory=Counter)
    window_count: int = 0
    subquery_count: int = 0
    cte_count: int = 0
    union_count: int = 0
    case_count: int = 0
    group_by_count: int = 0
    having_count: int = 0

    def add(self, s: StatementInventory) -> None:
        self.statements.append(s)
        self.node_counts.update(s.node_counts)
        self.join_types.update(s.join_types)
        self.function_names.update(s.function_names)
        self.aggregate_names.update(s.aggregate_names)
        self.window_count += s.window_count
        self.subquery_count += s.subquery_count
        self.cte_count += s.cte_count
        self.union_count += s.union_count
        self.case_count += s.case_count
        self.group_by_count += s.group_by_count
        self.having_count += s.having_count


def _join_label(node: object) -> str:
    """Build a join label like 'left', 'left semi' or 'inner' from a Join node."""
    side = str(node.args.get("side") or "").strip().lower()
    kind = str(node.args.get("kind") or "").strip().lower()
    label = " ".join(part for part in (side, kind) if part)
    return label or "inner"


def inventory_statement(stmt: Statement) -> StatementInventory:
    """Walk *stmt*'s AST once, counting node shapes."""
    from sqlglot import exp

    inv = StatementInventory(source_path=stmt.source_path, line=stmt.line)

    for node in stmt.ast.walk():
        inv.node_counts[type(node).__name__] += 1

        if isinstance(node, exp.Join):
            inv.join_types[_join_label(node)] += 1

        if isinstance(node, exp.AggFunc):
            inv.aggregate_names[node.sql_name().lower()] += 1
        elif isinstance(node, exp.Func):
            name = node.name if isinstance(node, exp.Anonymous) else node.sql_name()
            inv.function_names[(name or "anonymous").lower()] += 1

        if isinstance(node, exp.Window):
            inv.window_count += 1
        if isinstance(node, exp.Subquery):
            inv.subquery_count += 1
        if isinstance(node, exp.CTE):
            inv.cte_count += 1
        if isinstance(node, exp.Union):
            inv.union_count += 1
        if isinstance(node, exp.Case):
            inv.case_count += 1
        if isinstance(node, exp.Group):
            inv.group_by_count += 1
        if isinstance(node, exp.Having):
            inv.having_count += 1

    return inv


def inventory_corpus(statements: list[Statement]) -> CorpusInventory:
    """Inventory every statement and sum the result into one corpus-level view."""
    corpus = CorpusInventory()
    for stmt in statements:
        corpus.add(inventory_statement(stmt))
    return corpus
