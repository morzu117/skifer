"""
Project analyzer — turns existing SQL into first-draft Skifer pipelines (Plan 43).

Deterministic-first: pattern detection over a parsed AST, no LLM call for any
structural decision (table names, join conditions, filter values). The
optional ``sqlglot`` dependency (extra ``analyzer``) is imported lazily —
this package, like the rest of the core, stays importable without it.
"""
from skifer.analyzer.frontends import RawStatement, split_sql_file
from skifer.analyzer.parser import (
    ANALYZER_EXTRA,
    AnalyzerDependencyError,
    Statement,
    StatementParseError,
    parse_script,
)
from skifer.analyzer.inventory import (
    CorpusInventory,
    StatementInventory,
    inventory_corpus,
    inventory_statement,
)

__all__ = [
    "RawStatement",
    "split_sql_file",
    "ANALYZER_EXTRA",
    "AnalyzerDependencyError",
    "Statement",
    "StatementParseError",
    "parse_script",
    "CorpusInventory",
    "StatementInventory",
    "inventory_corpus",
    "inventory_statement",
]
