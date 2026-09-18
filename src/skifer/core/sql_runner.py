"""End-to-end execution of one compiled pipeline through an SQL adapter."""

from __future__ import annotations

from dataclasses import replace
from typing import Any, Callable

from skifer.core.capabilities_matrix import CAP_FILE_SOURCES, assert_supported
from skifer.core.dialect import quote_fqn, quote_ident, split_fqn, transpile
from skifer.core.ir import ParsedSchema, ParsedTable, parse_to_ir
from skifer.core.merge_sql import assert_merge_columns_match, build_merge_sql
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


def _target_parts(target_fqn: str) -> tuple[str | None, str, str]:
    """Split the target FQN into (catalog, schema, table), honouring quoting.

    Stripping the quotes first and splitting on every dot turned a table whose
    name legitimately contains one into three parts: ```gold`.`my.table``` became
    catalog ``gold``, schema ``my``, table ``table`` — and DuckDB then refused it
    for carrying a catalog it never had. ``split_fqn`` is the rule the dialect
    already applies everywhere else.
    """
    parts = [part for part in split_fqn(target_fqn) if part]
    if len(parts) == 2:
        return None, parts[0], parts[1]
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    raise ValueError(
        f"Incremental materialization requires a two- or three-part target FQN, got {target_fqn!r}."
    )


def _append_watermark_bound(
    select_sql: str,
    *,
    target: str,
    watermark_column: str | None,
    adapter_name: str,
) -> str:
    if not watermark_column:
        return select_sql
    alias = quote_ident("_skifer_incremental_src", target=adapter_name)
    column = quote_ident(watermark_column, target=adapter_name)
    # The bound is strictly greater than the previous maximum. Using >= would
    # reinsert every row on the last processed boundary on each run.
    return (
        f"SELECT * FROM (\n{select_sql}\n) AS {alias}\n"
        f"WHERE NOT EXISTS (SELECT 1 FROM {target})\n"
        f"  OR {alias}.{column} > (SELECT MAX({column}) FROM {target})"
    )


def run_sql_pipeline(
    adapter: Any,
    schema_dict: dict,
    target_fqn: str,
    *,
    context: Any,
    resolve_table: Callable[[str], str] | None = None,
    allow_raw_sql: bool = True,
) -> str:
    """Compile, transpile and materialize one YAML pipeline through SQL DDL."""
    parsed = parse_to_ir(schema_dict)
    assert_supported(
        parsed,
        adapter_name=adapter.name,
        supported=adapter.capabilities,
    )
    materialization = parsed.materialization or {}
    is_view = materialization.get("type") == "view"
    is_incremental_append = (
        materialization.get("type") == "incremental"
        and materialization.get("strategy") == "append"
    )
    is_incremental_merge = (
        materialization.get("type") == "incremental"
        and materialization.get("strategy") == "merge"
    )
    if (context.is_job_execution or context.is_production) and not is_view:
        parsed = _without_dev_limits(parsed)

    resolve = resolve_table or (lambda name: name)

    def resolve_columns(table: ParsedTable) -> list[str]:
        if table.source_type and CAP_FILE_SOURCES in adapter.capabilities:
            relation = adapter.resolve_source(table)
        else:
            relation = quote_fqn(resolve(table.name), target=adapter.name)
        return adapter.list_relation_columns(relation)

    select_sql = compile_select(
        parsed,
        resolve_table=resolve_table,
        allow_raw_sql=allow_raw_sql,
        resolve_source=(
            adapter.resolve_source
            if CAP_FILE_SOURCES in adapter.capabilities
            else None
        ),
        resolve_columns=resolve_columns,
        persisted_definition=is_view,
    )
    translated = transpile(select_sql, target=adapter.name)
    target = quote_fqn(target_fqn, target=adapter.name)
    if is_view:
        statement = f"CREATE OR REPLACE VIEW {target} AS {translated}"
    elif is_incremental_append:
        catalog, schema, table = _target_parts(target_fqn)
        target_exists = adapter.table_exists(catalog, schema, table)
        if target_exists:
            bounded = _append_watermark_bound(
                translated,
                target=target,
                watermark_column=materialization.get("watermark_column"),
                adapter_name=adapter.name,
            )
            statement = f"INSERT INTO {target} {bounded}"
        else:
            statement = f"CREATE TABLE {target} AS {translated}"
    elif is_incremental_merge:
        catalog, schema, table = _target_parts(target_fqn)
        target_exists = adapter.table_exists(catalog, schema, table)
        if target_exists:
            target_columns = adapter.list_relation_columns(target)
            source_columns = adapter.list_relation_columns(f"(\n{translated}\n)")
            assert_merge_columns_match(
                source_columns=source_columns,
                target_columns=target_columns,
                unique_key=materialization["unique_key"],
            )
            statement = build_merge_sql(
                target_relation=target,
                source_relation=f"(\n{translated}\n)",
                unique_key=materialization["unique_key"],
                target_columns=target_columns,
                target=adapter.name,
            )
        else:
            statement = f"CREATE TABLE {target} AS {translated}"
    else:
        statement = f"CREATE OR REPLACE TABLE {target} AS {translated}"
    adapter.execute_sql(statement)
    return statement
