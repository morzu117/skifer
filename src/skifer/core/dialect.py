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


def _import_sqlglot() -> tuple[Any, Any, Any]:
    try:
        import sqlglot
        from sqlglot import exp
        from sqlglot.errors import ErrorLevel
    except ImportError as exc:
        raise DialectError(
            "SQL dialect support requires the optional dependencies; "
            f"install them with `{_SQL_EXTRA_INSTALL}`."
        ) from exc
    return sqlglot, exp, ErrorLevel


def transpile(sql: str, *, target: str) -> str:
    """Translate one Spark SQL statement to a supported adapter dialect.

    Databricks is the pivot target and therefore returns the original string
    byte-for-byte without importing ``sqlglot``.
    """
    _validate_target(target)
    if target == "databricks":
        return sql

    sqlglot, _, error_level = _import_sqlglot()
    try:
        statements = sqlglot.transpile(
            sql,
            read=_SQLGLOT_DIALECT["databricks"],
            write=_SQLGLOT_DIALECT[target],
            error_level=error_level.RAISE,
            unsupported_level=error_level.RAISE,
        )
    except Exception as exc:
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

    _, exp, _ = _import_sqlglot()
    try:
        return exp.Identifier(this=str(name), quoted=True).sql(
            dialect=_SQLGLOT_DIALECT[target]
        )
    except Exception as exc:
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

    return ".".join(quote_ident(part, target=target) for part in fqn.split("."))
