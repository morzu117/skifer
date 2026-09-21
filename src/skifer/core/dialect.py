"""SQL dialect translation and identifier quoting.

Spark SQL is Skifer's pivot dialect. Optional ``sqlglot`` support is imported
only when a non-Databricks target requires translation or dialect-aware
quoting.
"""

from __future__ import annotations

from typing import Any


SUPPORTED_DIALECTS: frozenset[str] = frozenset(
    {"databricks", "duckdb", "snowflake", "bigquery"}
)

_SQLGLOT_DIALECT: dict[str, str] = {
    "databricks": "databricks",
    "duckdb": "duckdb",
    "snowflake": "snowflake",
    "bigquery": "bigquery",
}

_SQL_EXTRA_INSTALL = 'pip install -e ".[sql]"'


class DialectError(ValueError):
    """Raised when SQL cannot be represented in the requested dialect."""


def _validate_target(target: str) -> None:
    if target not in SUPPORTED_DIALECTS:
        allowed = ", ".join(sorted(SUPPORTED_DIALECTS))
        raise DialectError(
            f"Unsupported SQL target {target!r}; supported targets: {allowed}."
        )


def _import_sqlglot() -> tuple[Any, Any, Any, Any]:
    try:
        import sqlglot
        from sqlglot import exp
        from sqlglot.errors import ErrorLevel, SqlglotError
    except ImportError as exc:
        raise DialectError(
            "SQL dialect support requires the optional dependencies; "
            f"install them with `{_SQL_EXTRA_INSTALL}`."
        ) from exc
    return sqlglot, exp, ErrorLevel, SqlglotError


def split_fqn(fqn: str) -> list[str]:
    """Split a qualified name on its dots, honouring backtick quoting.

    Public because every caller that needs the parts of an FQN needs this exact
    rule. A naive ``fqn.split(".")`` splits a backtick-quoted table name that
    itself contains a dot into two parts, inventing a catalog out of the schema
    name.
    """
    parts: list[str] = []
    part: list[str] = []
    in_quotes = False
    index = 0
    while index < len(fqn):
        char = fqn[index]
        if char == "`":
            if in_quotes and index + 1 < len(fqn) and fqn[index + 1] == "`":
                part.append("`")
                index += 2
                continue
            if in_quotes:
                in_quotes = False
            elif not part:
                in_quotes = True
            else:
                part.append(char)
        elif char == "." and not in_quotes:
            parts.append("".join(part))
            part = []
        else:
            part.append(char)
        index += 1

    if in_quotes:
        raise DialectError(f"Unbalanced backtick quoting in qualified name {fqn!r}.")
    parts.append("".join(part))
    return parts


def transpile(sql: str, *, target: str) -> str:
    """Translate one Spark SQL statement to a supported adapter dialect.

    Databricks is the pivot target and therefore returns the original string
    byte-for-byte without importing ``sqlglot``.
    """
    _validate_target(target)
    if target == "databricks":
        return sql

    sqlglot, _, error_level, sqlglot_error = _import_sqlglot()
    try:
        statements = sqlglot.transpile(
            sql,
            read=_SQLGLOT_DIALECT["databricks"],
            write=_SQLGLOT_DIALECT[target],
            error_level=error_level.RAISE,
            unsupported_level=error_level.RAISE,
        )
    except sqlglot_error as exc:
        raise DialectError(
            f"Could not transpile SQL to target {target!r}: "
            f"{type(exc).__name__}: {exc}"
        ) from exc

    if len(statements) != 1 or not statements[0].strip():
        raise DialectError(
            "SQL dialect translation requires exactly one non-empty statement; "
            f"received {len([statement for statement in statements if statement.strip()])}."
        )
    return statements[0]


def quote_ident(name: str, *, target: str) -> str:
    """Quote one identifier according to the requested adapter dialect."""
    _validate_target(target)
    if target == "databricks":
        from skifer.core.sql_compiler import quote_ident as quote_pivot_ident

        return quote_pivot_ident(name)

    _, exp, _, sqlglot_error = _import_sqlglot()
    try:
        return exp.Identifier(this=str(name), quoted=True).sql(
            dialect=_SQLGLOT_DIALECT[target]
        )
    except sqlglot_error as exc:
        raise DialectError(
            f"Could not quote identifier for target {target!r}: "
            f"{type(exc).__name__}: {exc}"
        ) from exc


def quote_fqn(fqn: str, *, target: str) -> str:
    """Quote every component of a two- or three-part qualified name."""
    _validate_target(target)
    if target == "databricks":
        from skifer.core.sql_compiler import quote_fqn as quote_pivot_fqn

        return quote_pivot_fqn(fqn)

    return ".".join(
        quote_ident(part, target=target) for part in split_fqn(fqn)
    )
