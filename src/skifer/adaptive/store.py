"""Append-only SQLite and Delta stores for semantic usage events."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import sqlite3
from typing import Any, Protocol, runtime_checkable

from .models import SemanticUsageEvent


DEFAULT_RETENTION_DAYS = 90


@runtime_checkable
class UsageEventStore(Protocol):
    retention_days: int

    def append(self, event: SemanticUsageEvent) -> None: ...

    def list_events(
        self,
        *,
        environment: str | None = None,
        consumer_class: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        limit: int | None = None,
    ) -> list[SemanticUsageEvent]: ...


def _validate_retention_days(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError("retention_days must be a positive integer.")
    return value


def _validate_bound(value: datetime | None, field_name: str) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, datetime):
        raise TypeError(f"{field_name} must be a datetime or None.")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware.")
    return value.astimezone(timezone.utc)


def _validate_limit(value: int | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError("limit must be a positive integer or None.")
    return value


def _validate_partition(value: str | None, field_name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string or None.")
    return value


def _storage_row(event: SemanticUsageEvent) -> dict[str, Any]:
    """Build the persistence row field by field, never from dataclass internals."""
    payload = event.to_dict()
    return {
        "event_id": payload["event_id"],
        "occurred_at": payload["occurred_at"],
        "environment": payload["environment"],
        "consumer_class": payload["consumer_class"],
        "model_hashes": json.dumps(payload["model_hashes"], separators=(",", ":")),
        "metric_ids": json.dumps(payload["metric_ids"], separators=(",", ":")),
        "dimension_ids": json.dumps(payload["dimension_ids"], separators=(",", ":")),
        "normalized_filter_shape": json.dumps(
            payload["normalized_filter_shape"], separators=(",", ":")
        ),
        "query_fingerprint": payload["query_fingerprint"],
        "duration_ms": payload["duration_ms"],
        "rows_returned": payload["rows_returned"],
        "bytes_scanned": payload["bytes_scanned"],
        "status": payload["status"],
    }


def _string_tuple(value: Any, field_name: str) -> tuple[str, ...]:
    try:
        decoded = json.loads(value) if isinstance(value, str) else value
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Invalid stored usage event field '{field_name}'.") from exc
    if not isinstance(decoded, (list, tuple)) or not all(
        isinstance(item, str) for item in decoded
    ):
        raise ValueError(f"Invalid stored usage event field '{field_name}'.")
    return tuple(decoded)


def _event_from_row(row: Any) -> SemanticUsageEvent:
    if not isinstance(row, dict):
        if hasattr(row, "asDict"):
            row = row.asDict(recursive=True)
        else:
            raise TypeError("Stored usage event row must be a mapping.")
    try:
        occurred_at = row["occurred_at"]
        if isinstance(occurred_at, str):
            occurred_at = datetime.fromisoformat(occurred_at)
        return SemanticUsageEvent(
            event_id=row["event_id"],
            occurred_at=occurred_at,
            environment=row["environment"],
            consumer_class=row["consumer_class"],
            model_hashes=_string_tuple(row["model_hashes"], "model_hashes"),
            metric_ids=_string_tuple(row["metric_ids"], "metric_ids"),
            dimension_ids=_string_tuple(row["dimension_ids"], "dimension_ids"),
            normalized_filter_shape=_string_tuple(
                row["normalized_filter_shape"], "normalized_filter_shape"
            ),
            query_fingerprint=row["query_fingerprint"],
            duration_ms=row["duration_ms"],
            rows_returned=row["rows_returned"],
            bytes_scanned=row["bytes_scanned"],
            status=row["status"],
        )
    except KeyError as exc:
        raise ValueError(f"Stored usage event is missing field {exc.args[0]!r}.") from exc


class SqliteUsageEventStore:
    """Local append-only usage store; retention is configuration metadata only."""

    def __init__(
        self,
        db_path: str = ".skifer_adaptive.db",
        *,
        retention_days: int = DEFAULT_RETENTION_DAYS,
    ):
        self.retention_days = _validate_retention_days(retention_days)
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._migrate()

    def _migrate(self) -> None:
        self._conn.executescript("""
        CREATE TABLE IF NOT EXISTS adaptive_schema_migrations (
          version INTEGER PRIMARY KEY
        );
        CREATE TABLE IF NOT EXISTS semantic_usage_events (
          event_id TEXT PRIMARY KEY,
          occurred_at TEXT NOT NULL,
          environment TEXT NOT NULL,
          consumer_class TEXT NOT NULL,
          model_hashes TEXT NOT NULL,
          metric_ids TEXT NOT NULL,
          dimension_ids TEXT NOT NULL,
          normalized_filter_shape TEXT NOT NULL,
          query_fingerprint TEXT NOT NULL,
          duration_ms INTEGER,
          rows_returned INTEGER,
          bytes_scanned INTEGER,
          status TEXT NOT NULL
        );
        INSERT OR IGNORE INTO adaptive_schema_migrations(version) VALUES (1);
        """)
        self._conn.commit()

    def append(self, event: SemanticUsageEvent) -> None:
        if not isinstance(event, SemanticUsageEvent):
            raise TypeError("event must be a SemanticUsageEvent instance.")
        row = _storage_row(event)
        self._conn.execute(
            "INSERT OR IGNORE INTO semantic_usage_events VALUES "
            "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            tuple(row.values()),
        )
        self._conn.commit()

    def list_events(
        self,
        *,
        environment: str | None = None,
        consumer_class: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        limit: int | None = None,
    ) -> list[SemanticUsageEvent]:
        environment = _validate_partition(environment, "environment")
        consumer_class = _validate_partition(consumer_class, "consumer_class")
        since = _validate_bound(since, "since")
        until = _validate_bound(until, "until")
        limit = _validate_limit(limit)
        clauses: list[str] = []
        params: list[Any] = []
        if environment is not None:
            clauses.append("environment = ?")
            params.append(environment)
        if consumer_class is not None:
            clauses.append("consumer_class = ?")
            params.append(consumer_class)
        if since is not None:
            clauses.append("occurred_at >= ?")
            params.append(since.isoformat())
        if until is not None:
            clauses.append("occurred_at <= ?")
            params.append(until.isoformat())
        query = "SELECT * FROM semantic_usage_events"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY occurred_at DESC, event_id DESC"
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)
        rows = self._conn.execute(query, params).fetchall()
        columns = [item[0] for item in self._conn.execute(
            "SELECT * FROM semantic_usage_events LIMIT 0"
        ).description]
        return [_event_from_row(dict(zip(columns, row))) for row in rows]

    def close(self) -> None:
        self._conn.close()


class DeltaUsageEventStore:
    """Delta adapter over usage-specific SparkBackend primitives."""

    def __init__(
        self,
        backend: Any,
        schema: str = "_skifer_adaptive",
        *,
        retention_days: int = DEFAULT_RETENTION_DAYS,
    ):
        self.backend = backend
        self.schema = schema
        self.retention_days = _validate_retention_days(retention_days)

    def append(self, event: SemanticUsageEvent) -> None:
        if not isinstance(event, SemanticUsageEvent):
            raise TypeError("event must be a SemanticUsageEvent instance.")
        self.backend.append_semantic_usage_event(self.schema, _storage_row(event))

    def list_events(
        self,
        *,
        environment: str | None = None,
        consumer_class: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        limit: int | None = None,
    ) -> list[SemanticUsageEvent]:
        environment = _validate_partition(environment, "environment")
        consumer_class = _validate_partition(consumer_class, "consumer_class")
        since = _validate_bound(since, "since")
        until = _validate_bound(until, "until")
        limit = _validate_limit(limit)
        rows = self.backend.list_semantic_usage_events(
            self.schema,
            environment=environment,
            consumer_class=consumer_class,
            since=since.isoformat() if since else None,
            until=until.isoformat() if until else None,
            limit=limit,
        )
        return [_event_from_row(row) for row in rows]
