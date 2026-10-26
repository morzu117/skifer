"""Lazy sqlglot-backed SQL parsing (Plan 43 — 43.0).

The analyzer package must stay importable without the optional ``sqlglot``
dependency — only calling :func:`parse_script` needs it, and only then does a
missing-dependency error surface, with an install hint.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from skifer.analyzer.frontends import RawStatement, split_sql_file

ANALYZER_EXTRA = 'pip install -e ".[analyzer]"'

DEFAULT_DIALECT = "databricks"


class AnalyzerDependencyError(RuntimeError):
    """sqlglot is required to parse SQL and is not installed."""


class StatementParseError(ValueError):
    """A single SQL statement failed to parse — names its file, line and the
    underlying sqlglot error, rather than failing the whole script opaquely."""

    def __init__(self, raw: RawStatement, cause: Exception):
        super().__init__(f"{raw.source_path}:{raw.line}: failed to parse statement: {cause}")
        self.source_path = raw.source_path
        self.line = raw.line


@dataclass(frozen=True)
class Statement:
    """One parsed SQL statement: its sqlglot AST plus its file/line origin."""

    source_path: str
    line: int
    text: str
    ast: Any  # sqlglot.Expression — typed Any so sqlglot need not be importable


def _sqlglot():
    try:
        import sqlglot
    except ImportError as exc:
        raise AnalyzerDependencyError(
            f"SQL parsing requires the optional dependency: `{ANALYZER_EXTRA}`."
        ) from exc
    return sqlglot


def parse_script(path: str | Path, *, dialect: str = DEFAULT_DIALECT) -> list[Statement]:
    """Split *path* into statements and parse each with sqlglot.

    Raises:
        AnalyzerDependencyError: sqlglot is not installed.
        StatementParseError: a statement fails to parse; names the file, line
            and underlying sqlglot error.
    """
    sqlglot = _sqlglot()
    statements: list[Statement] = []
    for raw in split_sql_file(path):
        try:
            ast = sqlglot.parse_one(raw.text, dialect=dialect)
        except Exception as exc:  # sqlglot's own ParseError/TokenizeError
            raise StatementParseError(raw, exc) from exc
        statements.append(Statement(source_path=raw.source_path, line=raw.line, text=raw.text, ast=ast))
    return statements
