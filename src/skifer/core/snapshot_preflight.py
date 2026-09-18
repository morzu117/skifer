"""Fail-closed SCD2 snapshot preflight checks."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from skifer.core.dialect import quote_ident
from skifer.core.merge_sql import assert_merge_columns_match


SCD2_SYSTEM_COLUMNS: tuple[str, str] = ("valid_from", "valid_to")


@dataclass(frozen=True)
class SnapshotFinding:
    """One blocking SCD2 preflight finding with a pasteable YAML suggestion."""

    kind: str
    message: str
    suggested_yaml: str
    details: dict[str, Any] = field(default_factory=dict)

    def render(self) -> str:
        lines = [self.message, "  Suggested YAML:"]
        lines.extend(f"    {line}" if line else "" for line in self.suggested_yaml.splitlines())
        return "\n".join(lines)


@dataclass(frozen=True)
class SnapshotPreflightReport:
    """Report-first snapshot result. Applying is allowed only when it is clean."""

    findings: tuple[SnapshotFinding, ...] = field(default_factory=tuple)

    @property
    def safe_to_apply(self) -> bool:
        return not self.findings


@dataclass(frozen=True)
class SnapshotPreflightInputs:
    """Precomputed counts and column lists consumed by pure snapshot checks."""

    unique_key_null_count: int = 0
    unique_key_duplicate_count: int = 0
    target_current_count: int = 0
    missing_current_count: int = 0
    updated_at_null_count: int = 0
    late_arrival_count: int = 0
    source_columns: tuple[str, ...] = field(default_factory=tuple)
    target_columns: tuple[str, ...] = field(default_factory=tuple)
    target_exists: bool = True


def build_snapshot_preflight_report(
    materialization: dict[str, Any],
    inputs: SnapshotPreflightInputs,
) -> SnapshotPreflightReport:
    """Run every SCD2 preflight judgment over already-collected inputs."""
    checks = (
        check_unique_key_nulls(materialization, inputs),
        check_unique_key_duplicates(materialization, inputs),
        check_missing_current_rows(materialization, inputs),
        check_updated_at_nulls(materialization, inputs),
        check_late_arrivals(materialization, inputs),
        check_column_drift(materialization, inputs),
    )
    return SnapshotPreflightReport(
        findings=tuple(finding for finding in checks if finding is not None)
    )


def check_unique_key_nulls(
    materialization: dict[str, Any],
    inputs: SnapshotPreflightInputs,
) -> SnapshotFinding | None:
    """Refuse a NULL in the key, which no equality can ever match.

    Measured on DuckDB: a single NULL-keyed row passes the duplicate check
    (``GROUP BY`` yields one group of one) and then never joins to its own row in
    the target, because ``t.key = s.key`` is never true for NULL. Every run would
    insert it again. That accumulates in silence, which is why it is refused here
    rather than left to the write.
    """
    count = inputs.unique_key_null_count
    if count <= 0:
        return None
    unique_key = _columns(materialization.get("unique_key"))
    key_fragment = _yaml_list(unique_key)
    return SnapshotFinding(
        kind="unique_key_null",
        message=(
            f"[snapshot] REFUSED - 'unique_key' {key_fragment} contains NULL:\n"
            f"  {count} rows carry a NULL key. A NULL never equals a NULL, so such a "
            "row can never match its own version and would be inserted again on "
            "every run."
        ),
        suggested_yaml=(
            "quality_checks:\n"
            f"  drop_nulls_in: {key_fragment}\n"
            "# or filter the rows out explicitly, or pick a key that is never NULL."
        ),
        details={"null_key_count": count, "unique_key": tuple(unique_key)},
    )


def check_unique_key_duplicates(
    materialization: dict[str, Any],
    inputs: SnapshotPreflightInputs,
) -> SnapshotFinding | None:
    count = inputs.unique_key_duplicate_count
    if count <= 0:
        return None
    unique_key = _columns(materialization.get("unique_key"))
    order_column = materialization.get("updated_at") or "<deterministic_column>"
    key_fragment = _yaml_list(unique_key)
    return SnapshotFinding(
        kind="unique_key_not_unique",
        message=(
            f"[snapshot] REFUSED - 'unique_key' {key_fragment} is not unique in this batch:\n"
            f"  {count} keys carry more than one row. SCD2 cannot decide which is current."
        ),
        suggested_yaml=(
            "quality_checks:\n"
            f"  drop_duplicates_on: {key_fragment}\n"
            "preprocess:\n"
            f"  qualify: {{order_by: \"{order_column} DESC\"}}\n"
            "or extend the key so it identifies one row."
        ),
        details={"duplicate_key_count": count, "unique_key": tuple(unique_key)},
    )


def check_missing_current_rows(
    materialization: dict[str, Any],
    inputs: SnapshotPreflightInputs,
) -> SnapshotFinding | None:
    if materialization.get("on_missing") != "close":
        return None
    missing = inputs.missing_current_count
    current = inputs.target_current_count
    threshold = float(materialization.get("max_closed_ratio", 0.2))
    ratio = missing / current if current else 0.0
    if missing <= 0 or ratio <= threshold:
        return None
    return SnapshotFinding(
        kind="missing_rows_exceed_max_closed_ratio",
        message=(
            "[snapshot] REFUSED - current target rows absent from this batch exceed "
            "'max_closed_ratio':\n"
            f"  {missing} of {current} current target rows would be closed "
            f"({ratio:.6g} > {threshold:.6g}).\n"
            "  Skifer cannot tell a complete snapshot from a partial extract here."
        ),
        suggested_yaml=(
            "materialization:\n"
            "  on_missing: ignore\n"
            "# If this pipeline really reads a complete snapshot, raise the threshold explicitly:\n"
            f"#   max_closed_ratio: {min(ratio, 1.0):.6g}"
        ),
        details={
            "missing_current_count": missing,
            "target_current_count": current,
            "max_closed_ratio": threshold,
        },
    )


def check_updated_at_nulls(
    materialization: dict[str, Any],
    inputs: SnapshotPreflightInputs,
) -> SnapshotFinding | None:
    if materialization.get("strategy") != "timestamp":
        return None
    count = inputs.updated_at_null_count
    if count <= 0:
        return None
    updated_at = str(materialization.get("updated_at", "updated_at"))
    return SnapshotFinding(
        kind="updated_at_null",
        message=(
            f"[snapshot] REFUSED - 'updated_at' column '{updated_at}' contains NULL:\n"
            f"  {count} rows cannot be ordered into an SCD2 timeline."
        ),
        suggested_yaml=(
            "quality_checks:\n"
            f"  drop_nulls_in: [{updated_at}]"
        ),
        details={"updated_at": updated_at, "null_count": count},
    )


def check_late_arrivals(
    materialization: dict[str, Any],
    inputs: SnapshotPreflightInputs,
) -> SnapshotFinding | None:
    if materialization.get("strategy") != "timestamp":
        return None
    if materialization.get("on_late_arrival", "refuse") != "refuse":
        return None
    count = inputs.late_arrival_count
    if count <= 0:
        return None
    updated_at = str(materialization.get("updated_at", "updated_at"))
    unique_key = _columns(materialization.get("unique_key"))
    return SnapshotFinding(
        kind="late_arrival",
        message=(
            f"[snapshot] REFUSED - 'updated_at' column '{updated_at}' is older than "
            "the current version for the same key:\n"
            f"  {count} rows arrive before the current SCD2 version for "
            f"'unique_key' {_yaml_list(unique_key)}."
        ),
        suggested_yaml=(
            "materialization:\n"
            "  on_late_arrival: ignore"
        ),
        details={
            "updated_at": updated_at,
            "late_arrival_count": count,
            "unique_key": tuple(unique_key),
        },
    )


def check_column_drift(
    materialization: dict[str, Any],
    inputs: SnapshotPreflightInputs,
) -> SnapshotFinding | None:
    if not inputs.target_exists:
        return None
    expected_target_columns = list(inputs.source_columns) + list(SCD2_SYSTEM_COLUMNS)
    try:
        assert_merge_columns_match(
            source_columns=expected_target_columns,
            target_columns=list(inputs.target_columns),
            unique_key=_columns(materialization.get("unique_key")),
            label="[snapshot]",
        )
    except ValueError as exc:
        return SnapshotFinding(
            kind="column_drift",
            message=(
                "[snapshot] REFUSED - compiled source columns and target columns diverge:\n"
                f"  {exc}"
            ),
            suggested_yaml=(
                "select_final:\n"
                "  - [<source_column>, <target_column>]\n"
                "# Align the compiled source output with the existing snapshot target,\n"
                "# excluding only Skifer's fixed SCD2 columns: valid_from, valid_to."
            ),
            details={
                "source_columns": tuple(inputs.source_columns),
                "target_columns": tuple(inputs.target_columns),
                "scd2_system_columns": SCD2_SYSTEM_COLUMNS,
            },
        )
    return None


def collect_snapshot_preflight_inputs(
    adapter: Any,
    *,
    source_relation: str,
    target_relation: str,
    materialization: dict[str, Any],
    target_exists: bool,
) -> SnapshotPreflightInputs:
    """Collect SQL counts for the pure SCD2 preflight checks."""
    unique_key = _columns(materialization.get("unique_key"))
    source_columns = tuple(adapter.list_relation_columns(source_relation))
    duplicate_count = _scalar_count(
        adapter,
        _duplicate_key_count_sql(
            source_relation,
            unique_key=unique_key,
            adapter_name=adapter.name,
        ),
    )
    null_key_count = _scalar_count(
        adapter,
        _null_key_count_sql(
            source_relation,
            unique_key=unique_key,
            adapter_name=adapter.name,
        ),
    )
    updated_at_null_count = 0
    late_arrival_count = 0
    target_current_count = 0
    missing_current_count = 0
    target_columns: tuple[str, ...] = ()

    if materialization.get("strategy") == "timestamp":
        updated_at = str(materialization["updated_at"])
        updated_at_null_count = _scalar_count(
            adapter,
            _updated_at_null_count_sql(
                source_relation,
                updated_at=updated_at,
                adapter_name=adapter.name,
            ),
        )

    if target_exists:
        target_columns = tuple(adapter.list_relation_columns(target_relation))
        target_current_count = _scalar_count(
            adapter,
            _target_current_count_sql(
                target_relation,
                adapter_name=adapter.name,
            ),
        )
        if materialization.get("on_missing") == "close":
            missing_current_count = _scalar_count(
                adapter,
                _missing_current_count_sql(
                    source_relation,
                    target_relation,
                    unique_key=unique_key,
                    adapter_name=adapter.name,
                ),
            )
        if (
            materialization.get("strategy") == "timestamp"
            and materialization.get("on_late_arrival", "refuse") == "refuse"
        ):
            late_arrival_count = _scalar_count(
                adapter,
                _late_arrival_count_sql(
                    source_relation,
                    target_relation,
                    unique_key=unique_key,
                    updated_at=str(materialization["updated_at"]),
                    adapter_name=adapter.name,
                ),
            )

    return SnapshotPreflightInputs(
        unique_key_null_count=null_key_count,
        unique_key_duplicate_count=duplicate_count,
        target_current_count=target_current_count,
        missing_current_count=missing_current_count,
        updated_at_null_count=updated_at_null_count,
        late_arrival_count=late_arrival_count,
        source_columns=source_columns,
        target_columns=target_columns,
        target_exists=target_exists,
    )


def _duplicate_key_count_sql(
    source_relation: str,
    *,
    unique_key: list[str],
    adapter_name: str,
) -> str:
    keys = ", ".join(_qualified("s", key, adapter_name=adapter_name) for key in unique_key)
    return (
        f"SELECT COUNT(*) AS {_alias(adapter_name)} FROM ("
        f"SELECT {keys} FROM {source_relation} AS s "
        f"GROUP BY {keys} HAVING COUNT(*) > 1"
        ") AS d"
    )


def _null_key_count_sql(
    source_relation: str,
    *,
    unique_key: list[str],
    adapter_name: str,
) -> str:
    condition = " OR ".join(
        f"{_qualified('s', key, adapter_name=adapter_name)} IS NULL"
        for key in unique_key
    )
    return (
        f"SELECT COUNT(*) AS {_alias(adapter_name)} FROM {source_relation} AS s "
        f"WHERE {condition}"
    )


def _updated_at_null_count_sql(
    source_relation: str,
    *,
    updated_at: str,
    adapter_name: str,
) -> str:
    return (
        f"SELECT COUNT(*) AS {_alias(adapter_name)} FROM {source_relation} AS s "
        f"WHERE {_qualified('s', updated_at, adapter_name=adapter_name)} IS NULL"
    )


def _target_current_count_sql(target_relation: str, *, adapter_name: str) -> str:
    return (
        f"SELECT COUNT(*) AS {_alias(adapter_name)} FROM {target_relation} AS t "
        f"WHERE {_qualified('t', 'valid_to', adapter_name=adapter_name)} IS NULL"
    )


def _missing_current_count_sql(
    source_relation: str,
    target_relation: str,
    *,
    unique_key: list[str],
    adapter_name: str,
) -> str:
    key_match = _key_match("t", "s", unique_key, adapter_name=adapter_name)
    return (
        f"SELECT COUNT(*) AS {_alias(adapter_name)} FROM {target_relation} AS t "
        f"WHERE {_qualified('t', 'valid_to', adapter_name=adapter_name)} IS NULL "
        f"AND NOT EXISTS (SELECT 1 FROM {source_relation} AS s WHERE {key_match})"
    )


def _late_arrival_count_sql(
    source_relation: str,
    target_relation: str,
    *,
    unique_key: list[str],
    updated_at: str,
    adapter_name: str,
) -> str:
    key_match = _key_match("s", "t", unique_key, adapter_name=adapter_name)
    return (
        f"SELECT COUNT(*) AS {_alias(adapter_name)} FROM {source_relation} AS s "
        f"JOIN {target_relation} AS t ON {key_match} "
        f"AND {_qualified('t', 'valid_to', adapter_name=adapter_name)} IS NULL "
        f"WHERE {_qualified('s', updated_at, adapter_name=adapter_name)} "
        f"< {_qualified('t', 'valid_from', adapter_name=adapter_name)}"
    )


def _scalar_count(adapter: Any, sql: str) -> int:
    rows = adapter.fetch(sql)
    if not rows:
        return 0
    value = next(iter(rows[0].values()))
    return int(value or 0)


def _key_match(left_alias: str, right_alias: str, keys: list[str], *, adapter_name: str) -> str:
    return " AND ".join(
        f"{_qualified(left_alias, key, adapter_name=adapter_name)} = "
        f"{_qualified(right_alias, key, adapter_name=adapter_name)}"
        for key in keys
    )


def _qualified(alias: str, column: str, *, adapter_name: str) -> str:
    return f"{alias}.{quote_ident(column, target=adapter_name)}"


def _alias(adapter_name: str) -> str:
    return quote_ident("_skifer_count", target=adapter_name)


def _columns(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(column) for column in value]
    return []


def _yaml_list(columns: list[str]) -> str:
    return "[" + ", ".join(columns) + "]"
