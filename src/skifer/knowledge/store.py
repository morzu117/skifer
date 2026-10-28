"""Knowledge graph persistence: SQLite locally, Delta on Databricks (Plan 44, S02).

Writes are diffed against the stored fingerprints before anything is sent: an
unchanged node or edge is never rewritten, so a sync over unchanged sources emits
no write at all. A node that disappears from its source is soft-deleted
(``removed``), never physically deleted.
"""

from __future__ import annotations

import json
import sqlite3
from typing import Any, Iterable, Protocol, runtime_checkable

from .models import EDGE_KINDS, NODE_KINDS, Edge, Node, edge_from_row, node_from_row


DEFAULT_KNOWLEDGE_DB = ".skifer_knowledge.db"
DEFAULT_KNOWLEDGE_SCHEMA = "_skifer_knowledge"
NODES_TABLE = "knowledge_nodes"
EDGES_TABLE = "knowledge_edges"

# column -> storage kind; tuples are stored as JSON strings on both engines.
TABLE_COLUMNS: dict[str, tuple[tuple[str, str], ...]] = {
    NODES_TABLE: (
        ("node_id", "string"), ("kind", "string"), ("fqn", "string"), ("label", "string"),
        ("text", "string"), ("synonyms", "json"), ("status", "string"), ("domain", "string"),
        ("source", "string"), ("fingerprint", "string"), ("removed", "bool"),
    ),
    EDGES_TABLE: (
        ("edge_id", "string"), ("kind", "string"), ("from_id", "string"), ("to_id", "string"),
        ("join_keys", "json"), ("weight", "float"), ("source", "string"),
        ("fingerprint", "string"), ("removed", "bool"),
    ),
}
ID_COLUMNS = {NODES_TABLE: "node_id", EDGES_TABLE: "edge_id"}

_SQLITE_TYPES = {"string": "TEXT", "json": "TEXT", "float": "REAL", "bool": "INTEGER"}
_DELTA_TYPES = {"string": "STRING", "json": "STRING", "float": "DOUBLE", "bool": "BOOLEAN"}


@runtime_checkable
class KnowledgeStore(Protocol):
    def upsert_nodes(self, nodes: Iterable[Node]) -> tuple[int, int, int]: ...

    def upsert_edges(self, edges: Iterable[Edge]) -> tuple[int, int, int]: ...

    def mark_removed(self, node_ids: Iterable[str]) -> int: ...

    def mark_edges_removed(self, edge_ids: Iterable[str]) -> int: ...

    def list_nodes(
        self, *, kinds: Iterable[str] | None = None, domain: str | None = None,
        include_removed: bool = False,
    ) -> list[Node]: ...

    def list_edges(
        self, *, node_id: str | None = None, kinds: Iterable[str] | None = None
    ) -> list[Edge]: ...


def node_row(node: Node, *, removed: bool = False) -> dict[str, Any]:
    row = node.to_dict()
    row["synonyms"] = json.dumps(row["synonyms"], ensure_ascii=False)
    row["removed"] = removed
    return row


def edge_row(edge: Edge, *, removed: bool = False) -> dict[str, Any]:
    row = edge.to_dict()
    row["join_keys"] = json.dumps(row.pop("keys"), ensure_ascii=False)
    row["fingerprint"] = edge.fingerprint
    row["removed"] = removed
    return row


def _edge_from_storage(row: dict[str, Any]) -> Edge:
    return edge_from_row({**row, "keys": row["join_keys"]})


def _kinds(kinds: Iterable[str] | None, allowed: tuple[str, ...]) -> set[str] | None:
    if kinds is None:
        return None
    selected = set(kinds)
    unknown = sorted(selected - set(allowed))
    if unknown:
        raise ValueError(f"Unknown kinds {unknown}; expected a subset of {list(allowed)}.")
    return selected


def _diff(
    incoming: dict[str, dict[str, Any]], existing: dict[str, dict[str, Any]]
) -> tuple[list[dict[str, Any]], tuple[int, int, int]]:
    """Rows to write, and (added, updated, unchanged) — a revived row counts as updated."""
    writes: list[dict[str, Any]] = []
    added = updated = unchanged = 0
    for key in sorted(incoming):
        row = incoming[key]
        current = existing.get(key)
        if current is None:
            added += 1
        elif current["fingerprint"] != row["fingerprint"] or bool(current["removed"]):
            updated += 1
        else:
            unchanged += 1
            continue
        writes.append(row)
    return writes, (added, updated, unchanged)


class _DiffingStore:
    """Shared diff logic; subclasses provide ``_rows`` and ``_write``."""

    def _rows(self, table: str) -> dict[str, dict[str, Any]]:
        raise NotImplementedError

    def _write(self, table: str, rows: list[dict[str, Any]]) -> None:
        raise NotImplementedError

    def upsert_nodes(self, nodes: Iterable[Node]) -> tuple[int, int, int]:
        incoming = {node.node_id: node_row(node) for node in nodes}
        writes, counts = _diff(incoming, self._rows(NODES_TABLE))
        if writes:
            self._write(NODES_TABLE, writes)
        return counts

    def upsert_edges(self, edges: Iterable[Edge]) -> tuple[int, int, int]:
        incoming = {edge.edge_id: edge_row(edge) for edge in edges}
        writes, counts = _diff(incoming, self._rows(EDGES_TABLE))
        if writes:
            self._write(EDGES_TABLE, writes)
        return counts

    def _mark(self, table: str, ids: Iterable[str]) -> int:
        existing = self._rows(table)
        writes = [
            {**existing[key], "removed": True}
            for key in sorted(set(ids))
            if key in existing and not existing[key]["removed"]
        ]
        if writes:
            self._write(table, writes)
        return len(writes)

    def mark_removed(self, node_ids: Iterable[str]) -> int:
        """Soft delete: the row stays, flagged ``removed``; returns how many changed."""
        return self._mark(NODES_TABLE, node_ids)

    def mark_edges_removed(self, edge_ids: Iterable[str]) -> int:
        return self._mark(EDGES_TABLE, edge_ids)

    def list_nodes(
        self, *, kinds: Iterable[str] | None = None, domain: str | None = None,
        include_removed: bool = False,
    ) -> list[Node]:
        selected = _kinds(kinds, NODE_KINDS)
        return [
            node_from_row(row)
            for _, row in sorted(self._rows(NODES_TABLE).items())
            if (include_removed or not row["removed"])
            and (selected is None or row["kind"] in selected)
            and (domain is None or row["domain"] == domain)
        ]

    def list_edges(
        self, *, node_id: str | None = None, kinds: Iterable[str] | None = None
    ) -> list[Edge]:
        selected = _kinds(kinds, EDGE_KINDS)
        return [
            _edge_from_storage(row)
            for _, row in sorted(self._rows(EDGES_TABLE).items())
            if not row["removed"]
            and (selected is None or row["kind"] in selected)
            and (node_id is None or node_id in (row["from_id"], row["to_id"]))
        ]


def _sqlite_ddl(table: str) -> str:
    columns = ", ".join(
        f"{column} {_SQLITE_TYPES[kind]}" + (" PRIMARY KEY" if column == ID_COLUMNS[table] else "")
        for column, kind in TABLE_COLUMNS[table]
    )
    return f"CREATE TABLE IF NOT EXISTS {table} ({columns});"


class SqliteKnowledgeStore(_DiffingStore):
    """Local knowledge graph in ``.skifer_knowledge.db``."""

    def __init__(self, db_path: str = DEFAULT_KNOWLEDGE_DB):
        self.db_path = db_path
        try:
            self._conn = sqlite3.connect(db_path, check_same_thread=False)
            self._conn.executescript("\n".join(_sqlite_ddl(table) for table in TABLE_COLUMNS))
            self._conn.commit()
        except sqlite3.Error as exc:
            raise RuntimeError(
                f"Knowledge store SqliteKnowledgeStore cannot open database {db_path!r}: "
                f"{type(exc).__name__}."
            ) from exc

    def _rows(self, table: str) -> dict[str, dict[str, Any]]:
        cursor = self._conn.execute(f"SELECT * FROM {table}")
        columns = [item[0] for item in cursor.description]
        key = ID_COLUMNS[table]
        rows = {}
        for values in cursor.fetchall():
            row = dict(zip(columns, values))
            row["removed"] = bool(row["removed"])
            rows[row[key]] = row
        return rows

    def _write(self, table: str, rows: list[dict[str, Any]]) -> None:
        columns = [column for column, _ in TABLE_COLUMNS[table]]
        placeholders = ", ".join("?" for _ in columns)
        self._conn.executemany(
            f"INSERT OR REPLACE INTO {table} ({', '.join(columns)}) VALUES ({placeholders})",
            [tuple(int(row[c]) if c == "removed" else row[c] for c in columns) for row in rows],
        )
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()


def delta_columns_ddl(table: str) -> str:
    """Spark schema string of a knowledge table (also used by ``createDataFrame``)."""
    return ", ".join(f"`{column}` {_DELTA_TYPES[kind]}" for column, kind in TABLE_COLUMNS[table])


def delta_create_table_sql(qualified_table: str, table: str) -> str:
    return (
        f"CREATE TABLE IF NOT EXISTS {qualified_table} ({delta_columns_ddl(table)}) "
        "USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true)"
    )


class DeltaKnowledgeStore(_DiffingStore):
    """Delta adapter over the named ``upsert_knowledge_rows`` / ``list_knowledge_rows``."""

    def __init__(self, backend: Any, *, schema: str):
        self.backend = backend
        self.schema = schema

    def _rows(self, table: str) -> dict[str, dict[str, Any]]:
        key = ID_COLUMNS[table]
        try:
            rows = self.backend.list_knowledge_rows(self.schema, table)
        except Exception as exc:
            raise RuntimeError(
                f"Knowledge store DeltaKnowledgeStore cannot read {self.schema}.{table}: "
                f"{type(exc).__name__}."
            ) from exc
        return {row[key]: {**row, "removed": bool(row["removed"])} for row in rows}

    def _write(self, table: str, rows: list[dict[str, Any]]) -> None:
        self.backend.upsert_knowledge_rows(self.schema, table, rows, ID_COLUMNS[table])


def default_knowledge_store(engine: Any) -> KnowledgeStore:
    """SQLite locally or without a catalog; Delta in ``<catalog>._skifer_knowledge``."""
    catalog = getattr(engine, "db", None)
    if engine.is_local or not catalog:
        return SqliteKnowledgeStore()
    return DeltaKnowledgeStore(engine.backend, schema=f"{catalog}.{DEFAULT_KNOWLEDGE_SCHEMA}")
