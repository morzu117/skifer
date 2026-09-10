"""Bounded, append-only state history for governed capability execution."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import math
import re
import sqlite3
from threading import Lock
from types import MappingProxyType
from typing import Any, Mapping, Protocol

from .autonomy import ExecutionState
from .credentials import CredentialLease
from .validator import _is_secret_key


MAX_EVENT_DETAIL_BYTES = 4_096
MAX_HISTORY_PAGE_SIZE = 1_000
_MAX_DETAIL_DEPTH = 16
_MAX_DETAIL_NODES = 1_024
_REQUEST_HASH = re.compile(r"^sha256:v1:[0-9a-f]{64}$")


class CapabilityHistoryError(RuntimeError):
    """A capability event cannot be safely stored or queried."""


def _copy_bounded_detail(
    detail: Mapping[str, Any], *, max_bytes: int = MAX_EVENT_DETAIL_BYTES
) -> dict[str, Any]:
    """Copy bounded JSON data while refusing secret-shaped keys and leases."""
    if not isinstance(detail, Mapping):
        raise CapabilityHistoryError("Capability event detail must be a mapping.")
    if (
        not isinstance(max_bytes, int)
        or isinstance(max_bytes, bool)
        or max_bytes <= 0
        or max_bytes > MAX_EVENT_DETAIL_BYTES
    ):
        raise CapabilityHistoryError(
            f"Capability event detail limit must be between 1 and {MAX_EVENT_DETAIL_BYTES} bytes."
        )

    nodes = 0
    ancestors: set[int] = set()

    def copy(value: Any, path: str, depth: int) -> Any:
        nonlocal nodes
        if isinstance(value, CredentialLease):
            raise CapabilityHistoryError(
                f"Capability event detail '{path}' must never contain a CredentialLease."
            )
        if depth > _MAX_DETAIL_DEPTH:
            raise CapabilityHistoryError("Capability event detail exceeds its nesting bound.")
        nodes += 1
        if nodes > _MAX_DETAIL_NODES:
            raise CapabilityHistoryError("Capability event detail exceeds its node bound.")
        if value is None or isinstance(value, (str, bool, int)):
            return value
        if isinstance(value, float):
            if math.isfinite(value):
                return value
            raise CapabilityHistoryError(
                f"Capability event detail '{path}' contains a non-finite number."
            )
        if isinstance(value, list):
            if id(value) in ancestors:
                raise CapabilityHistoryError("Capability event detail contains a cycle.")
            ancestors.add(id(value))
            try:
                return [copy(item, f"{path}[{index}]", depth + 1) for index, item in enumerate(value)]
            finally:
                ancestors.remove(id(value))
        if isinstance(value, Mapping):
            if id(value) in ancestors:
                raise CapabilityHistoryError("Capability event detail contains a cycle.")
            ancestors.add(id(value))
            try:
                copied: dict[str, Any] = {}
                for key, item in value.items():
                    if not isinstance(key, str):
                        raise CapabilityHistoryError(
                            f"Capability event detail '{path}' contains a non-string key."
                        )
                    if _is_secret_key(key):
                        raise CapabilityHistoryError(
                            f"Capability event detail '{path}.{key}' contains a forbidden secret key."
                        )
                    copied[key] = copy(item, f"{path}.{key}", depth + 1)
                return copied
            finally:
                ancestors.remove(id(value))
        raise CapabilityHistoryError(
            f"Capability event detail '{path}' contains unsupported type '{type(value).__name__}'."
        )

    copied = copy(detail, "detail", 0)
    encoded = json.dumps(
        copied, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")
    if len(encoded) > max_bytes:
        raise CapabilityHistoryError(
            f"Capability event detail exceeds the enforced {max_bytes}-byte limit."
        )
    return copied


@dataclass(frozen=True)
class CapabilityStateEvent:
    """One immutable, allowlisted transition record for a governed request."""

    event_id: str
    request_hash: str
    capability_id: str
    capability_version: str
    subject: str
    state: ExecutionState
    occurred_at: datetime
    detail: Mapping[str, Any]

    def __post_init__(self) -> None:
        for field_name, maximum in (
            ("event_id", 128),
            ("capability_id", 128),
            ("capability_version", 64),
            ("subject", 512),
        ):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value or len(value) > maximum:
                raise CapabilityHistoryError(
                    f"Capability event {field_name} must contain 1 to {maximum} characters."
                )
        if _REQUEST_HASH.fullmatch(self.request_hash) is None:
            raise CapabilityHistoryError("Capability event request_hash must use sha256:v1.")
        if not isinstance(self.state, ExecutionState):
            raise CapabilityHistoryError("Capability event state must be an ExecutionState.")
        if (
            not isinstance(self.occurred_at, datetime)
            or self.occurred_at.tzinfo is None
            or self.occurred_at.utcoffset() != timedelta(0)
        ):
            raise CapabilityHistoryError(
                "Capability event occurred_at must be timezone-aware UTC."
            )
        copied = _copy_bounded_detail(self.detail)
        object.__setattr__(self, "detail", MappingProxyType(copied))

    def to_dict(self) -> dict[str, Any]:
        """Serialize only intentionally persisted event fields."""
        return {
            "event_id": self.event_id,
            "request_hash": self.request_hash,
            "capability_id": self.capability_id,
            "capability_version": self.capability_version,
            "subject": self.subject,
            "state": self.state.value,
            "occurred_at": self.occurred_at.astimezone(timezone.utc).isoformat(),
            "detail": _copy_bounded_detail(self.detail),
        }


class CapabilityHistoryStore(Protocol):
    def append(self, event: CapabilityStateEvent) -> None: ...

    def events_for(
        self, request_hash: str, *, limit: int = 100
    ) -> list[CapabilityStateEvent]: ...

    def latest_state(self, request_hash: str) -> CapabilityStateEvent | None: ...


class SqliteCapabilityHistoryStore:
    """Local immutable event journal keyed by caller-supplied event IDs."""

    def __init__(self, db_path: str = ".skifer_capability_history.db") -> None:
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._lock = Lock()
        self._migrate()

    def _migrate(self) -> None:
        with self._lock:
            self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS schema_migrations (
              version INTEGER PRIMARY KEY
            );
            CREATE TABLE IF NOT EXISTS capability_state_events (
              event_id TEXT PRIMARY KEY,
              request_hash TEXT NOT NULL,
              capability_id TEXT NOT NULL,
              capability_version TEXT NOT NULL,
              subject TEXT NOT NULL,
              state TEXT NOT NULL,
              occurred_at TEXT NOT NULL,
              detail TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS capability_events_request_time
              ON capability_state_events(request_hash, occurred_at, event_id);
            INSERT OR IGNORE INTO schema_migrations(version) VALUES (1);
            """)
            self._conn.commit()

    def append(self, event: CapabilityStateEvent) -> None:
        if not isinstance(event, CapabilityStateEvent):
            raise CapabilityHistoryError("Capability history accepts only CapabilityStateEvent.")
        payload = event.to_dict()
        detail_json = json.dumps(
            payload["detail"], ensure_ascii=False, separators=(",", ":"), sort_keys=True
        )
        with self._lock:
            self._conn.execute(
                "INSERT OR IGNORE INTO capability_state_events "
                "(event_id, request_hash, capability_id, capability_version, subject, state, occurred_at, detail) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    payload["event_id"],
                    payload["request_hash"],
                    payload["capability_id"],
                    payload["capability_version"],
                    payload["subject"],
                    payload["state"],
                    payload["occurred_at"],
                    detail_json,
                ),
            )
            self._conn.commit()

    def events_for(
        self, request_hash: str, *, limit: int = 100
    ) -> list[CapabilityStateEvent]:
        self._validate_query(request_hash, limit)
        with self._lock:
            rows = self._conn.execute(
                "SELECT event_id, request_hash, capability_id, capability_version, subject, state, occurred_at, detail "
                "FROM capability_state_events WHERE request_hash = ? "
                "ORDER BY occurred_at ASC, rowid ASC LIMIT ?",
                (request_hash, limit),
            ).fetchall()
        return [self._event_from_row(row) for row in rows]

    def latest_state(self, request_hash: str) -> CapabilityStateEvent | None:
        self._validate_request_hash(request_hash)
        with self._lock:
            row = self._conn.execute(
                "SELECT event_id, request_hash, capability_id, capability_version, subject, state, occurred_at, detail "
                "FROM capability_state_events WHERE request_hash = ? "
                "ORDER BY occurred_at DESC, rowid DESC LIMIT ?",
                (request_hash, 1),
            ).fetchone()
        return self._event_from_row(row) if row is not None else None

    @staticmethod
    def _validate_query(request_hash: str, limit: int) -> None:
        SqliteCapabilityHistoryStore._validate_request_hash(request_hash)
        if (
            not isinstance(limit, int)
            or isinstance(limit, bool)
            or limit <= 0
            or limit > MAX_HISTORY_PAGE_SIZE
        ):
            raise CapabilityHistoryError(
                f"Capability history limit must be between 1 and {MAX_HISTORY_PAGE_SIZE}."
            )

    @staticmethod
    def _validate_request_hash(request_hash: str) -> None:
        if not isinstance(request_hash, str) or _REQUEST_HASH.fullmatch(request_hash) is None:
            raise CapabilityHistoryError("Capability history request_hash must use sha256:v1.")

    @staticmethod
    def _event_from_row(row: tuple[Any, ...]) -> CapabilityStateEvent:
        try:
            detail = json.loads(row[7])
            occurred_at = datetime.fromisoformat(row[6])
            return CapabilityStateEvent(
                event_id=row[0],
                request_hash=row[1],
                capability_id=row[2],
                capability_version=row[3],
                subject=row[4],
                state=ExecutionState(row[5]),
                occurred_at=occurred_at,
                detail=detail,
            )
        except (ValueError, TypeError, json.JSONDecodeError) as exc:
            raise CapabilityHistoryError("Stored capability event is invalid.") from exc

    def close(self) -> None:
        with self._lock:
            self._conn.close()
