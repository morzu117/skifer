"""Render common Spark SQL to a closed set of target dialects."""

import re

TARGETS = ("databricks", "duckdb", "snowflake")


class DialectError(ValueError):
    """SQL cannot be rendered for the requested target."""


def render(sql: str, target: str) -> str:
    """Render one statement; Databricks preserves the source byte for byte."""
    if target not in TARGETS:
        raise DialectError(f"Unknown SQL dialect {target!r}; expected one of {TARGETS}.")
    if target == "databricks":
        return sql
    try:
        import sqlglot
    except ImportError as exc:
        raise DialectError(
            f"SQL rendering for '{target}' requires sqlglot: pip install skifer[dialects]"
        ) from exc

    # Spark's reader interprets doubled quotes as adjacent literals. Normalize
    # only complete string literals; the target restores SQL-standard escaping.
    literal = re.compile(r"'(?:[^']|'')*'")
    source = literal.sub(lambda match: match.group(0).replace("''", "\\'"), sql)
    try:
        statements = sqlglot.transpile(source, read="spark", write=target)
    except sqlglot.SqlglotError as exc:
        raise DialectError(f"SQL cannot be rendered for '{target}'.") from exc
    if len(statements) != 1 or not statements[0]:
        raise DialectError("SQL must render to exactly one statement.")
    return statements[0]
