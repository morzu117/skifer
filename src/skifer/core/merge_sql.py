"""Shared SQL construction for cumulative write materializations."""

from __future__ import annotations

from datetime import datetime, timezone

from skifer.core.dialect import quote_ident


def assert_merge_columns_match(
    *,
    source_columns: list[str],
    target_columns: list[str],
    unique_key: list[str],
    label: str = "[incremental merge]",
) -> None:
    """Refuse a merge whose compiled source and existing target schemas differ."""
    source_set = set(source_columns)
    target_set = set(target_columns)
    source_only = sorted(source_set - target_set)
    target_only = sorted(target_set - source_set)
    missing_keys = [key for key in unique_key if key not in target_set or key not in source_set]

    details = []
    if source_only:
        details.append(f"source-only columns: {source_only}")
    if target_only:
        details.append(f"target-only columns: {target_only}")
    if missing_keys:
        details.append(f"unique_key columns not present in both source and target: {missing_keys}")

    if details:
        raise ValueError(
            f"{label} Source and target columns must match exactly; "
            + "; ".join(details)
            + "."
        )


def build_merge_sql(
    *,
    target_relation: str,
    source_relation: str,
    unique_key: list[str],
    target_columns: list[str],
    target: str,
) -> str:
    """Build a dialect-quoted MERGE statement with explicit column lists."""
    key_set = set(unique_key)
    on_clause = " AND ".join(
        f"t.{quote_ident(key, target=target)} = s.{quote_ident(key, target=target)}"
        for key in unique_key
    )
    update_columns = [column for column in target_columns if column not in key_set]
    update_assignments = ", ".join(
        f"{_set_target(column, target=target)} = s.{quote_ident(column, target=target)}"
        for column in update_columns
    )
    insert_columns = ", ".join(
        quote_ident(column, target=target) for column in target_columns
    )
    insert_values = ", ".join(
        f"s.{quote_ident(column, target=target)}" for column in target_columns
    )

    lines = [
        f"MERGE INTO {target_relation} AS t",
        f"USING {source_relation} AS s",
        f"ON {on_clause}",
    ]
    if update_assignments:
        lines.append(f"WHEN MATCHED THEN UPDATE SET {update_assignments}")
    lines.append(
        f"WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})"
    )
    return "\n".join(lines)


def build_snapshot_initial_select_sql(
    *,
    source_relation: str,
    source_columns: list[str],
    materialization: dict,
    target: str,
    run_at: datetime,
) -> str:
    """Build the SELECT body for a first SCD2 snapshot materialization."""
    value_columns = ", ".join(
        f"s.{quote_ident(column, target=target)}" for column in source_columns
    )
    valid_from = _snapshot_valid_from_sql(
        materialization=materialization,
        target=target,
        run_at=run_at,
        source_alias="s",
        fallback_to_closed_boundary=False,
    )
    valid_to = _snapshot_null_valid_to_sql(
        materialization=materialization,
        target=target,
        source_alias="s",
    )
    return (
        f"SELECT {value_columns}, {valid_from} AS {quote_ident('valid_from', target=target)}, "
        f"{valid_to} AS {quote_ident('valid_to', target=target)} "
        f"FROM {source_relation} AS s"
    )


def build_snapshot_apply_sql(
    *,
    target_relation: str,
    source_relation: str,
    source_columns: list[str],
    materialization: dict,
    target: str,
    run_at: datetime,
) -> list[str]:
    """Build idempotent SCD2 close/insert statements with explicit columns."""
    strategy = materialization["strategy"]
    statements = [
        _snapshot_close_changed_sql(
            target_relation=target_relation,
            source_relation=source_relation,
            materialization=materialization,
            target=target,
            run_at=run_at,
        )
    ]
    if materialization.get("on_missing") == "close":
        statements.append(
            _snapshot_close_missing_sql(
                target_relation=target_relation,
                source_relation=source_relation,
                materialization=materialization,
                target=target,
                run_at=run_at,
            )
        )
    statements.append(
        _snapshot_insert_current_sql(
            target_relation=target_relation,
            source_relation=source_relation,
            source_columns=source_columns,
            materialization=materialization,
            target=target,
            run_at=run_at,
            use_closed_boundary_for_check=strategy == "check",
        )
    )
    return statements


def sql_timestamp_literal(value: datetime) -> str:
    """Render a timezone-aware clock value as a portable SQL timestamp literal."""
    if not isinstance(value, datetime):
        raise TypeError("Snapshot clock must return datetime values.")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Snapshot clock must return timezone-aware datetimes.")
    utc_value = value.astimezone(timezone.utc).replace(tzinfo=None)
    return f"TIMESTAMP '{utc_value.isoformat(sep=' ', timespec='microseconds')}'"


def _set_target(column: str, *, target: str) -> str:
    quoted = quote_ident(column, target=target)
    if target == "duckdb":
        return quoted
    return f"t.{quoted}"


def _snapshot_close_changed_sql(
    *,
    target_relation: str,
    source_relation: str,
    materialization: dict,
    target: str,
    run_at: datetime,
) -> str:
    close_at = _snapshot_valid_from_sql(
        materialization=materialization,
        target=target,
        run_at=run_at,
        source_alias="s",
        fallback_to_closed_boundary=False,
    )
    if target == "databricks":
        return (
            f"MERGE INTO {target_relation} AS t\n"
            f"USING {source_relation} AS s\n"
            f"ON {_current_condition('t', target=target)}\n"
            f"  AND {_key_match('t', 's', materialization['unique_key'], target=target)}\n"
            f"WHEN MATCHED AND ({_changed_condition(materialization, target=target)}) "
            f"THEN UPDATE SET {_set_target('valid_to', target=target)} = {close_at}"
        )
    return (
        f"UPDATE {target_relation} AS t\n"
        f"SET {quote_ident('valid_to', target=target)} = {close_at}\n"
        f"FROM {source_relation} AS s\n"
        f"WHERE {_current_condition('t', target=target)}\n"
        f"  AND {_key_match('t', 's', materialization['unique_key'], target=target)}\n"
        f"  AND ({_changed_condition(materialization, target=target)})"
    )


def _snapshot_close_missing_sql(
    *,
    target_relation: str,
    source_relation: str,
    materialization: dict,
    target: str,
    run_at: datetime,
) -> str:
    return (
        f"UPDATE {target_relation} AS t\n"
        f"SET {quote_ident('valid_to', target=target)} = {sql_timestamp_literal(run_at)}\n"
        f"WHERE {_current_condition('t', target=target)}\n"
        f"  AND NOT EXISTS (\n"
        f"    SELECT 1 FROM {source_relation} AS s\n"
        f"    WHERE {_key_match('t', 's', materialization['unique_key'], target=target)}\n"
        f"  )"
    )


def _snapshot_insert_current_sql(
    *,
    target_relation: str,
    source_relation: str,
    source_columns: list[str],
    materialization: dict,
    target: str,
    run_at: datetime,
    use_closed_boundary_for_check: bool,
) -> str:
    insert_columns = [*source_columns, "valid_from", "valid_to"]
    rendered_columns = ", ".join(quote_ident(column, target=target) for column in insert_columns)
    source_values = ", ".join(
        f"s.{quote_ident(column, target=target)}" for column in source_columns
    )
    valid_from = _snapshot_valid_from_sql(
        materialization=materialization,
        target=target,
        run_at=run_at,
        source_alias="s",
        target_relation=target_relation,
        fallback_to_closed_boundary=use_closed_boundary_for_check,
    )
    valid_to = _snapshot_null_valid_to_sql(
        materialization=materialization,
        target=target,
        source_alias="s",
    )
    return (
        f"INSERT INTO {target_relation} ({rendered_columns})\n"
        f"SELECT {source_values}, {valid_from}, {valid_to}\n"
        f"FROM {source_relation} AS s\n"
        f"WHERE NOT EXISTS (\n"
        f"  SELECT 1 FROM {target_relation} AS t\n"
        f"  WHERE {_current_condition('t', target=target)}\n"
        f"    AND {_key_match('t', 's', materialization['unique_key'], target=target)}\n"
        f")"
    )


def _snapshot_valid_from_sql(
    *,
    materialization: dict,
    target: str,
    run_at: datetime,
    source_alias: str,
    target_relation: str | None = None,
    fallback_to_closed_boundary: bool,
) -> str:
    if materialization["strategy"] == "timestamp":
        return f"{source_alias}.{quote_ident(materialization['updated_at'], target=target)}"
    run_literal = sql_timestamp_literal(run_at)
    if not fallback_to_closed_boundary:
        return run_literal
    return (
        "COALESCE(("
        f"SELECT MAX(t_prev.{quote_ident('valid_to', target=target)}) "
        f"FROM {target_relation} AS t_prev "
        f"WHERE t_prev.{quote_ident('valid_to', target=target)} IS NOT NULL "
        f"AND {_key_match('t_prev', source_alias, materialization['unique_key'], target=target)}"
        f"), {run_literal})"
    )


def _snapshot_null_valid_to_sql(
    *,
    materialization: dict,
    target: str,
    source_alias: str,
) -> str:
    if materialization["strategy"] == "timestamp":
        updated_at = f"{source_alias}.{quote_ident(materialization['updated_at'], target=target)}"
        return f"CASE WHEN FALSE THEN {updated_at} ELSE NULL END"
    return "CAST(NULL AS TIMESTAMP)"


def _changed_condition(materialization: dict, *, target: str) -> str:
    if materialization["strategy"] == "timestamp":
        updated_at = quote_ident(materialization["updated_at"], target=target)
        valid_from = quote_ident("valid_from", target=target)
        return f"s.{updated_at} > t.{valid_from}"
    return " OR ".join(
        f"t.{quote_ident(column, target=target)} IS DISTINCT FROM "
        f"s.{quote_ident(column, target=target)}"
        for column in materialization["check_columns"]
    )


def _current_condition(alias: str, *, target: str) -> str:
    return f"{alias}.{quote_ident('valid_to', target=target)} IS NULL"


def _key_match(left_alias: str, right_alias: str, unique_key: list[str], *, target: str) -> str:
    return " AND ".join(
        f"{left_alias}.{quote_ident(key, target=target)} = "
        f"{right_alias}.{quote_ident(key, target=target)}"
        for key in unique_key
    )
