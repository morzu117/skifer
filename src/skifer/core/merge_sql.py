"""Shared SQL construction for incremental merge materializations."""

from __future__ import annotations

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


def _set_target(column: str, *, target: str) -> str:
    quoted = quote_ident(column, target=target)
    if target == "duckdb":
        return quoted
    return f"t.{quoted}"
