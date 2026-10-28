"""SQLite and Delta stores for metric candidates and their certification history.

Classification ``internal`` (Plan 44, D2): this registry holds the question and the
structure of a query — never a filter value. ``upsert_candidate`` refuses a
``schema_dict`` that still carries one, so a caller that forgot to parameterize
fails loudly instead of leaking a value into a durable table.

``metric_certification_history`` is append-only: neither the Protocol, nor the
adapters, nor the backend primitives offer a way to change or remove an event.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
import re
import sqlite3
from typing import Any, Protocol, runtime_checkable

from .models import (
    CANDIDATE_STATES,
    REFERENCE_FAMILIES,
    RUN_PURPOSES,
    CandidateRun,
    CertificationEvent,
    MetricCandidate,
    ReferenceQuestion,
    candidate_from_row,
    canonical_json,
    event_from_row,
    reference_from_row,
    run_from_row,
)
from .redaction import parameterize_filters


DEFAULT_RETENTION_DAYS = 365
DEFAULT_CANDIDATES_DB = ".skifer_candidates.db"
QUESTION_STORAGE_MODES = ("full", "redacted")

CANDIDATES_TABLE = "metric_candidates"
RUNS_TABLE = "metric_candidate_runs"
HISTORY_TABLE = "metric_certification_history"
REFERENCES_TABLE = "reference_questions"

# Column name -> logical type. The order is the persisted column order; JSON
# columns hold canonical JSON text. Shared by the SQLite and Delta adapters.
TABLE_COLUMNS: dict[str, tuple[tuple[str, str], ...]] = {
    CANDIDATES_TABLE: (
        ("candidate_id", "string"), ("metric_name", "string"),
        ("metric_description", "string"), ("origin_question", "string"),
        ("origin_question_hash", "string"), ("schema_dict", "json"),
        ("sql_hash", "string"), ("fingerprint", "string"), ("tables_used", "json"),
        ("columns_used", "json"), ("joins_used", "json"), ("business_rules_refs", "json"),
        ("residues", "json"), ("state", "string"), ("request_count", "int"),
        ("distinct_requesters", "json"), ("stability_score", "float"),
        ("coherence", "json"), ("trace_id", "string"), ("reviewer", "string"),
        ("published_view", "string"), ("published_metric_view", "string"),
        ("created_at", "string"), ("updated_at", "string"),
        ("last_requested_at", "string"),
    ),
    RUNS_TABLE: (
        ("run_id", "string"), ("candidate_id", "string"), ("executed_at", "string"),
        ("purpose", "string"), ("result_hash", "string"), ("row_count", "int"),
        ("duration_ms", "int"), ("trace_id", "string"), ("error_type", "string"),
    ),
    HISTORY_TABLE: (
        ("event_id", "string"), ("metric_id", "string"), ("metric_name", "string"),
        ("event_type", "string"), ("previous_state", "string"), ("new_state", "string"),
        ("decided_by", "string"), ("justification", "string"), ("evidence", "json"),
        ("replaced_by_metric", "string"), ("replaces_metric", "string"),
        ("semantic_layer_version", "string"), ("trace_id", "string"),
        ("event_at", "string"),
    ),
    REFERENCES_TABLE: (
        ("question_id", "string"), ("question", "string"), ("family", "string"),
        ("expected", "json"), ("source", "string"),
        ("semantic_layer_version", "string"), ("created_at", "string"),
    ),
}
ID_COLUMNS = {
    CANDIDATES_TABLE: "candidate_id",
    RUNS_TABLE: "run_id",
    HISTORY_TABLE: "event_id",
    REFERENCES_TABLE: "question_id",
}
_SQLITE_TYPES = {"string": "TEXT", "json": "TEXT", "int": "INTEGER", "float": "REAL"}
_PARAMETER = re.compile(r"^\{\{ p\d+ \}\}$")


@runtime_checkable
class CandidateStore(Protocol):
    retention_days: int
    store_questions: str

    def upsert_candidate(self, candidate: MetricCandidate) -> None: ...

    def get_candidate(self, candidate_id: str) -> MetricCandidate | None: ...

    def find_by_fingerprint(self, fingerprint: str) -> MetricCandidate | None: ...

    def list_candidates(
        self, *, state: str | None = None, limit: int | None = None
    ) -> list[MetricCandidate]: ...

    def append_run(self, run: CandidateRun) -> None: ...

    def list_runs(
        self, candidate_id: str, *, purpose: str | None = None
    ) -> list[CandidateRun]: ...

    def append_event(self, event: CertificationEvent) -> None: ...

    def list_events(
        self,
        *,
        metric_id: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
    ) -> list[CertificationEvent]: ...

    def append_reference(self, question: ReferenceQuestion) -> None: ...

    def list_references(self, *, family: str | None = None) -> list[ReferenceQuestion]: ...


# --- validation ------------------------------------------------------------------------


def validate_retention_days(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"candidates_retention_days must be a positive integer; got {value!r}.")
    return value


def validate_store_questions(value: Any) -> str:
    if value not in QUESTION_STORAGE_MODES:
        raise ValueError(
            f"candidates_store_questions must be one of {list(QUESTION_STORAGE_MODES)}; "
            f"got {value!r}."
        )
    return value


def candidate_settings(env_config: dict | None) -> tuple[str, int]:
    """(``candidates_store_questions``, ``candidates_retention_days``) of an environment."""
    env_config = env_config or {}
    return (
        validate_store_questions(env_config.get("candidates_store_questions", "full")),
        validate_retention_days(
            env_config.get("candidates_retention_days", DEFAULT_RETENTION_DAYS)
        ),
    )


def _bound(value: datetime | None, field_name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, datetime):
        raise TypeError(f"{field_name} must be a datetime or None.")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware.")
    return value.astimezone(timezone.utc).isoformat()


def _limit(value: int | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError("limit must be a positive integer or None.")
    return value


def _choice(value: str | None, allowed: tuple[str, ...], field_name: str) -> str | None:
    if value is not None and value not in allowed:
        raise ValueError(f"{field_name} must be one of {list(allowed)}; got {value!r}.")
    return value


def _require(value: Any, expected: type, name: str) -> None:
    if not isinstance(value, expected):
        raise TypeError(f"{name} must be a {expected.__name__} instance.")


# --- persistence rows ------------------------------------------------------------------


def assert_value_free(schema_dict: dict) -> None:
    """Refuse a ``schema_dict`` whose filters still hold a value instead of ``{{ pN }}``."""
    _, values = parameterize_filters(schema_dict)
    for value in values.values():
        if not (isinstance(value, str) and _PARAMETER.match(value)):
            raise ValueError(
                "MetricCandidate.schema_dict holds a filter value; parameterize it with "
                "parameterize_filters() before storing (values are never persisted)."
            )


def storage_row(table: str, payload: dict) -> dict[str, Any]:
    """Build a persistence row column by column from a ``to_dict()`` payload."""
    row: dict[str, Any] = {}
    for column, kind in TABLE_COLUMNS[table]:
        value = payload[column]
        row[column] = canonical_json(value) if kind == "json" and value is not None else value
    return row


def candidate_row(candidate: MetricCandidate, store_questions: str) -> dict[str, Any]:
    _require(candidate, MetricCandidate, "candidate")
    assert_value_free(candidate.schema_dict)
    if store_questions == "redacted":
        candidate = replace(candidate, origin_question=None)
    return storage_row(CANDIDATES_TABLE, candidate.to_dict())


def _sqlite_ddl(table: str) -> str:
    columns = ", ".join(
        f"{column} {_SQLITE_TYPES[kind]}"
        + (" PRIMARY KEY" if column == ID_COLUMNS[table] else "")
        for column, kind in TABLE_COLUMNS[table]
    )
    return f"CREATE TABLE IF NOT EXISTS {table} ({columns});"


class SqliteCandidateStore:
    """Local candidate registry; retention is read and validated, nothing is purged."""

    def __init__(
        self,
        db_path: str = DEFAULT_CANDIDATES_DB,
        *,
        store_questions: str = "full",
        retention_days: int = DEFAULT_RETENTION_DAYS,
    ):
        self.store_questions = validate_store_questions(store_questions)
        self.retention_days = validate_retention_days(retention_days)
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.executescript("\n".join(_sqlite_ddl(table) for table in TABLE_COLUMNS))
        self._conn.commit()

    def _insert(self, table: str, row: dict[str, Any], *, upsert: bool = False) -> None:
        columns = list(row)
        placeholders = ", ".join("?" for _ in columns)
        statement = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
        if upsert:
            key = ID_COLUMNS[table]
            assignments = ", ".join(
                f"{column} = excluded.{column}" for column in columns if column != key
            )
            statement += f" ON CONFLICT({key}) DO UPDATE SET {assignments}"
        else:
            statement = statement.replace("INSERT INTO", "INSERT OR IGNORE INTO", 1)
        self._conn.execute(statement, tuple(row.values()))
        self._conn.commit()

    def _select(self, table: str, clauses: list[str], params: list[Any], order: str,
                limit: int | None = None) -> list[dict[str, Any]]:
        query = f"SELECT * FROM {table}"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += f" ORDER BY {order}"
        if limit is not None:
            query += " LIMIT ?"
            params = [*params, limit]
        cursor = self._conn.execute(query, params)
        columns = [item[0] for item in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def upsert_candidate(self, candidate: MetricCandidate) -> None:
        self._insert(CANDIDATES_TABLE, candidate_row(candidate, self.store_questions), upsert=True)

    def get_candidate(self, candidate_id: str) -> MetricCandidate | None:
        rows = self._select(CANDIDATES_TABLE, ["candidate_id = ?"], [candidate_id], "candidate_id")
        return candidate_from_row(rows[0]) if rows else None

    def find_by_fingerprint(self, fingerprint: str) -> MetricCandidate | None:
        rows = self._select(CANDIDATES_TABLE, ["fingerprint = ?"], [fingerprint], "candidate_id")
        return candidate_from_row(rows[0]) if rows else None

    def list_candidates(
        self, *, state: str | None = None, limit: int | None = None
    ) -> list[MetricCandidate]:
        state = _choice(state, CANDIDATE_STATES, "state")
        clauses, params = (["state = ?"], [state]) if state is not None else ([], [])
        rows = self._select(
            CANDIDATES_TABLE, clauses, params, "updated_at DESC, candidate_id", _limit(limit)
        )
        return [candidate_from_row(row) for row in rows]

    def append_run(self, run: CandidateRun) -> None:
        _require(run, CandidateRun, "run")
        self._insert(RUNS_TABLE, storage_row(RUNS_TABLE, run.to_dict()))

    def list_runs(self, candidate_id: str, *, purpose: str | None = None) -> list[CandidateRun]:
        purpose = _choice(purpose, RUN_PURPOSES, "purpose")
        clauses, params = ["candidate_id = ?"], [candidate_id]
        if purpose is not None:
            clauses.append("purpose = ?")
            params.append(purpose)
        rows = self._select(RUNS_TABLE, clauses, params, "executed_at, run_id")
        return [run_from_row(row) for row in rows]

    def append_event(self, event: CertificationEvent) -> None:
        _require(event, CertificationEvent, "event")
        self._insert(HISTORY_TABLE, storage_row(HISTORY_TABLE, event.to_dict()))

    def list_events(
        self,
        *,
        metric_id: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
    ) -> list[CertificationEvent]:
        """Events in ``[since, until)``, oldest first: a half-open window counts nothing twice."""
        clauses: list[str] = []
        params: list[Any] = []
        for clause, value in (
            ("metric_id = ?", metric_id),
            ("event_at >= ?", _bound(since, "since")),
            ("event_at < ?", _bound(until, "until")),
        ):
            if value is not None:
                clauses.append(clause)
                params.append(value)
        rows = self._select(HISTORY_TABLE, clauses, params, "event_at, event_id")
        return [event_from_row(row) for row in rows]

    def append_reference(self, question: ReferenceQuestion) -> None:
        _require(question, ReferenceQuestion, "question")
        self._insert(REFERENCES_TABLE, storage_row(REFERENCES_TABLE, question.to_dict()))

    def list_references(self, *, family: str | None = None) -> list[ReferenceQuestion]:
        family = _choice(family, REFERENCE_FAMILIES, "family")
        clauses, params = (["family = ?"], [family]) if family is not None else ([], [])
        rows = self._select(REFERENCES_TABLE, clauses, params, "created_at, question_id")
        return [reference_from_row(row) for row in rows]

    def close(self) -> None:
        self._conn.close()


# --- Delta --------------------------------------------------------------------------------

DEFAULT_CANDIDATES_SCHEMA = "_skifer_candidates"
_DELTA_TYPES = {"string": "STRING", "json": "STRING", "int": "BIGINT", "float": "DOUBLE"}
# Read order of each table; the time column bounds a [since, until) window.
ORDER_COLUMNS = {
    CANDIDATES_TABLE: (("updated_at", "DESC"), ("candidate_id", "ASC")),
    RUNS_TABLE: (("executed_at", "ASC"), ("run_id", "ASC")),
    HISTORY_TABLE: (("event_at", "ASC"), ("event_id", "ASC")),
    REFERENCES_TABLE: (("created_at", "ASC"), ("question_id", "ASC")),
}
TIME_COLUMNS = {HISTORY_TABLE: "event_at"}


def delta_columns_ddl(table: str) -> str:
    """Spark schema string of a registry table (also used by ``createDataFrame``)."""
    return ", ".join(f"{column} {_DELTA_TYPES[kind]}" for column, kind in TABLE_COLUMNS[table])


def delta_create_table_sql(qualified_table: str, table: str) -> str:
    """Explicit Delta DDL with Change Data Feed on, and no column ``DEFAULT``.

    ``request_count``'s default of 1 is set by ``MetricCandidate``: a DDL
    ``DEFAULT`` would require the ``allowColumnDefaults`` table feature.
    """
    return (
        f"CREATE TABLE IF NOT EXISTS {qualified_table} ({delta_columns_ddl(table)}) "
        "USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true)"
    )


class DeltaCandidateStore:
    """Delta adapter over named ``SparkBackend`` candidate primitives; never counts."""

    def __init__(
        self,
        backend: Any,
        schema: str = DEFAULT_CANDIDATES_SCHEMA,
        *,
        store_questions: str = "full",
        retention_days: int = DEFAULT_RETENTION_DAYS,
    ):
        self.backend = backend
        self.schema = schema
        self.store_questions = validate_store_questions(store_questions)
        self.retention_days = validate_retention_days(retention_days)

    def upsert_candidate(self, candidate: MetricCandidate) -> None:
        self.backend.upsert_metric_candidate(
            self.schema, candidate_row(candidate, self.store_questions)
        )

    def get_candidate(self, candidate_id: str) -> MetricCandidate | None:
        row = self.backend.get_metric_candidate(self.schema, candidate_id=candidate_id)
        return candidate_from_row(row) if row else None

    def find_by_fingerprint(self, fingerprint: str) -> MetricCandidate | None:
        row = self.backend.get_metric_candidate(self.schema, fingerprint=fingerprint)
        return candidate_from_row(row) if row else None

    def list_candidates(
        self, *, state: str | None = None, limit: int | None = None
    ) -> list[MetricCandidate]:
        rows = self.backend.list_metric_candidates(
            self.schema,
            state=_choice(state, CANDIDATE_STATES, "state"),
            limit=_limit(limit),
        )
        return [candidate_from_row(row) for row in rows]

    def append_run(self, run: CandidateRun) -> None:
        _require(run, CandidateRun, "run")
        self.backend.append_candidate_row(
            self.schema, RUNS_TABLE, storage_row(RUNS_TABLE, run.to_dict()), ID_COLUMNS[RUNS_TABLE]
        )

    def list_runs(self, candidate_id: str, *, purpose: str | None = None) -> list[CandidateRun]:
        filters = {"candidate_id": candidate_id}
        if _choice(purpose, RUN_PURPOSES, "purpose") is not None:
            filters["purpose"] = purpose
        rows = self.backend.list_candidate_rows(self.schema, RUNS_TABLE, **filters)
        return [run_from_row(row) for row in rows]

    def append_event(self, event: CertificationEvent) -> None:
        _require(event, CertificationEvent, "event")
        self.backend.append_candidate_row(
            self.schema, HISTORY_TABLE, storage_row(HISTORY_TABLE, event.to_dict()),
            ID_COLUMNS[HISTORY_TABLE],
        )

    def list_events(
        self,
        *,
        metric_id: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
    ) -> list[CertificationEvent]:
        filters = {} if metric_id is None else {"metric_id": metric_id}
        rows = self.backend.list_candidate_rows(
            self.schema, HISTORY_TABLE,
            since=_bound(since, "since"), until=_bound(until, "until"), **filters,
        )
        return [event_from_row(row) for row in rows]

    def append_reference(self, question: ReferenceQuestion) -> None:
        _require(question, ReferenceQuestion, "question")
        self.backend.append_candidate_row(
            self.schema, REFERENCES_TABLE,
            storage_row(REFERENCES_TABLE, question.to_dict()), ID_COLUMNS[REFERENCES_TABLE],
        )

    def list_references(self, *, family: str | None = None) -> list[ReferenceQuestion]:
        filters = {}
        if _choice(family, REFERENCE_FAMILIES, "family") is not None:
            filters["family"] = family
        rows = self.backend.list_candidate_rows(self.schema, REFERENCES_TABLE, **filters)
        return [reference_from_row(row) for row in rows]


def default_candidate_store(engine: Any) -> CandidateStore:
    """Return the candidate store matching where ``engine`` runs.

    Same rule as ``default_certification_store``: SQLite locally (``catalog:
    null`` or a session the engine started in ``local[*]``), otherwise Delta
    qualified with the environment's catalog — a bare schema would resolve against
    whatever catalog the session happens to use.
    """
    context = getattr(engine, "context", None)
    env_config = context.env_config() if context is not None else {}
    store_questions, retention_days = candidate_settings(env_config)
    catalog = getattr(engine, "db", None)
    if engine.is_local or not catalog:
        return SqliteCandidateStore(
            store_questions=store_questions, retention_days=retention_days
        )
    return DeltaCandidateStore(
        engine.backend,
        schema=f"{catalog}.{DEFAULT_CANDIDATES_SCHEMA}",
        store_questions=store_questions,
        retention_days=retention_days,
    )
