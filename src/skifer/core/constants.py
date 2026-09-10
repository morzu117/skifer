"""Shared constants for the Skifer core."""
from __future__ import annotations

#: Spark file formats supported by the declarative ``source:`` block (Plan 14).
VALID_SOURCE_TYPES: frozenset[str] = frozenset(
    {"csv", "parquet", "json", "avro", "orc", "delta", "text"}
)

#: File formats readable via ``readStream`` without an explicit schema (Plan 27).
#: csv/json require a user schema; parquet/orc/avro require the global
#: ``spark.sql.streaming.schemaInference`` session conf — both deferred.
VALID_STREAMING_SOURCE_TYPES: frozenset[str] = frozenset({"delta", "text"})

#: Materialization types accepted by the top-level ``materialization:`` block
#: (Plan 27 for ``streaming_table``, Plan 28 for ``materialized_view``).
VALID_MATERIALIZATION_TYPES: frozenset[str] = frozenset(
    {"table", "streaming_table", "materialized_view"}
)

#: Keys allowed in the ``materialization:`` block, per type. The allowlist is
#: type-dependent so streaming options cannot leak onto a materialized view
#: (and vice versa) — every other key is rejected at load time.
MATERIALIZATION_ALLOWED_KEYS: dict[str, frozenset[str]] = {
    "table": frozenset({"type"}),
    "streaming_table": frozenset({"type", "trigger", "checkpoint", "write_mode", "keys"}),
    "materialized_view": frozenset(
        {"type", "schedule", "comment", "cluster_by", "partition_by", "refresh"}
    ),
}

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
