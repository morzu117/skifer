"""Shared constants for the Skifer core."""
from __future__ import annotations

# Ordered from least to most sensitive — index = severity rank (Plan 31.3.1).
CLASSIFICATION_LEVELS: tuple[str, ...] = (
    "public",
    "internal",
    "confidential",
    "restricted",
    "pii",
)
CLASSIFICATION_RANK: dict[str, int] = {
    name: index for index, name in enumerate(CLASSIFICATION_LEVELS)
}

# Restricted and PII filter values remain visible in governed audit evidence.
SENSITIVE_CLASSIFICATIONS: frozenset[str] = frozenset({"restricted", "pii"})

# Contract lifecycle statuses accepted in the ``contract:`` block (Plan 31.3.3).
VALID_CONTRACT_STATUSES: frozenset[str] = frozenset(
    {"draft", "active", "deprecated"}
)
DEFAULT_CONTRACT_STATUS: str = "active"

#: How hard ``contract.output`` is enforced on an environment (Plan 40).
#:
#: ``off`` (the default) keeps the historical behaviour exactly: the block stays
#: metadata. ``warn`` runs the derived checks at warning severity so a team can
#: measure before imposing. ``strict`` runs them as critical, which quarantines a
#: violation — and additionally requires a pipeline to declare ``data_product:``,
#: since staging is the only way a failed contract leaves the target untouched.
CONTRACT_ENFORCEMENT_LEVELS: tuple[str, ...] = ("off", "warn", "strict")
DEFAULT_CONTRACT_ENFORCEMENT: str = "off"

#: Spark file formats supported by the declarative ``source:`` block (Plan 14).
VALID_SOURCE_TYPES: frozenset[str] = frozenset(
    {"csv", "parquet", "json", "avro", "orc", "delta", "text"}
)

#: File formats readable via ``readStream`` without an explicit schema (Plan 27).
#: csv/json require a user schema; parquet/orc/avro require the global
#: ``spark.sql.streaming.schemaInference`` session conf — both deferred.
VALID_STREAMING_SOURCE_TYPES: frozenset[str] = frozenset({"delta", "text"})

#: Materialization types accepted by the top-level ``materialization:`` block
#: (Plans 27, 28 and 39.4.1).
VALID_MATERIALIZATION_TYPES: frozenset[str] = frozenset(
    {
        "table",
        "view",
        "incremental",
        "snapshot",
        "streaming_table",
        "materialized_view",
    }
)

#: Keys allowed in the ``materialization:`` block, per type. The allowlist is
#: type-dependent so streaming options cannot leak onto a materialized view
#: (and vice versa) — every other key is rejected at load time.
MATERIALIZATION_ALLOWED_KEYS: dict[str, frozenset[str]] = {
    "table": frozenset({"type"}),
    "view": frozenset({"type"}),
    "incremental": frozenset(
        {"type", "strategy", "unique_key", "watermark_column"}
    ),
    "snapshot": frozenset(
        {
            "type",
            "strategy",
            "unique_key",
            "updated_at",
            "check_columns",
            "on_missing",
            "max_closed_ratio",
            "on_late_arrival",
        }
    ),
    "streaming_table": frozenset({"type", "trigger", "checkpoint", "write_mode", "keys"}),
    "materialized_view": frozenset(
        {"type", "schedule", "comment", "cluster_by", "partition_by", "refresh"}
    ),
}

#: Strategies accepted by cumulative batch materializations (Plan 39.4.1).
VALID_INCREMENTAL_STRATEGIES: frozenset[str] = frozenset({"append", "merge"})
VALID_SNAPSHOT_STRATEGIES: frozenset[str] = frozenset({"timestamp", "check"})
VALID_SNAPSHOT_ON_MISSING: frozenset[str] = frozenset({"close", "ignore"})
VALID_SNAPSHOT_ON_LATE_ARRIVAL: frozenset[str] = frozenset({"refuse", "ignore"})
DEFAULT_SNAPSHOT_MAX_CLOSED_RATIO: float = 0.2
DEFAULT_SNAPSHOT_ON_LATE_ARRIVAL: str = "refuse"

#: Default trigger for ``materialization: streaming_table``.
DEFAULT_STREAMING_TRIGGER: str = "available_now"

#: Refresh modes for ``materialization: materialized_view`` (Plan 28).
#: ``auto`` refreshes an already up-to-date view on each run; ``never`` leaves
#: refreshing to the declared ``schedule``.
VALID_MV_REFRESH_MODES: frozenset[str] = frozenset({"auto", "never"})

#: Accepted prefixes for the materialized-view ``schedule`` clause. The rest of
#: the string is passed through to Databricks, which validates it precisely.
VALID_MV_SCHEDULE_PREFIXES: tuple[str, ...] = ("EVERY ", "CRON ")

#: Table property carrying the hash of the compiled SELECT, so a changed YAML
#: triggers CREATE OR REPLACE instead of refreshing a stale definition (Plan 28).
MV_DEFINITION_HASH_PROPERTY: str = "skifer.definition_hash"
