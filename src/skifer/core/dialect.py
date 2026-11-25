"""Render common Spark SQL to a closed set of target dialects.

Snowflake always quotes identifiers and preserves their authored case. An unquoted
Snowflake table created as orders is named ORDERS: models must spell it ORDERS.
"""

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
        from sqlglot.errors import ErrorLevel, SqlglotError
    except ImportError as exc:
        raise DialectError(
            f"SQL rendering for '{target}' requires sqlglot: pip install skifer[dialects]"
        ) from exc

    # Spark's reader interprets doubled quotes as adjacent literals. Normalize
    # only literal interiors, leaving backticked identifiers untouched.
    token = re.compile(r"`(?:[^`]|``)*`|'(?:[^']|'')*'")
    source = token.sub(
        lambda m: m.group(0) if m.group(0).startswith("`")
        else "'" + m.group(0)[1:-1].replace("''", "\\'") + "'",
        sql,
    )
    try:
        statements = sqlglot.transpile(
            source, read="spark", write=target, unsupported_level=ErrorLevel.RAISE,
            identify=target == "snowflake",
        )
    except SqlglotError as exc:
        raise DialectError(f"SQL cannot be rendered for '{target}'.") from exc
    if len(statements) != 1 or not statements[0]:
        raise DialectError("SQL must render to exactly one statement.")
    return statements[0]
