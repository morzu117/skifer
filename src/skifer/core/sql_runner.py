"""End-to-end execution of one compiled pipeline through an SQL adapter."""

from __future__ import annotations

from dataclasses import replace
from typing import Any, Callable

from skifer.core.capabilities_matrix import assert_supported
from skifer.core.dialect import quote_fqn, transpile
from skifer.core.ir import ParsedSchema, parse_to_ir
from skifer.core.sql_compiler import compile_select


def _schema_without_dev_limits(schema: dict) -> dict:
    """Copy a nested schema mapping while removing only execution-time limits."""
    copied = {key: value for key, value in schema.items() if key != "dev_limit"}
    copied["tables"] = [
        {key: value for key, value in table.items() if key != "dev_limit"}
        for table in schema.get("tables", [])
    ]
    copied["partials"] = [
        {
            **partial,
            "schema": _schema_without_dev_limits(partial.get("schema", {})),
        }
        for partial in schema.get("partials", [])
    ]
    return copied


def _without_dev_limits(parsed: ParsedSchema) -> ParsedSchema:
    """Return an IR copy whose root, table and nested-partial limits are absent."""
    tables = [replace(table, dev_limit=None) for table in parsed.tables]
    partials = [
        replace(partial, schema=_schema_without_dev_limits(partial.schema))
        for partial in parsed.partials
    ]
    return replace(parsed, dev_limit=None, tables=tables, partials=partials)


def run_sql_pipeline(
    adapter: Any,
    schema_dict: dict,
    target_fqn: str,
    *,
    context: Any,
    resolve_table: Callable[[str], str] | None = None,
    allow_raw_sql: bool = True,
) -> str:
    """Compile, transpile and materialize one YAML pipeline as an SQL table."""
    parsed = parse_to_ir(schema_dict)
    assert_supported(
        parsed,
        adapter_name=adapter.name,
        supported=adapter.capabilities,
    )
    if context.is_job_execution or context.is_production:
        parsed = _without_dev_limits(parsed)

    select_sql = compile_select(
        parsed,
        resolve_table=resolve_table,
        allow_raw_sql=allow_raw_sql,
        resolve_source=adapter.resolve_source,
        resolve_columns=adapter.list_columns,
        persisted_definition=False,
    )
    translated = transpile(select_sql, target=adapter.name)
    statement = (
        f"CREATE OR REPLACE TABLE {quote_fqn(target_fqn, target=adapter.name)} AS "
        f"{translated}"
    )
    adapter.execute_sql(statement)
    return statement
