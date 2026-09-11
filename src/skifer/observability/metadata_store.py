"""metadata_store.py - MetadataStore Protocol + SQLite/Delta backends (Plan 31.2)."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field, replace
from datetime import datetime
import hashlib
import json
import sqlite3
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from skifer.core.constants import CLASSIFICATION_LEVELS

if TYPE_CHECKING:
    from skifer.lineage.tracker import LineageEdge, LineageGraph


@dataclass(frozen=True)
class ColumnRecord:
    name: str
    logical_type: str | None = None
    classification: str | None = None
    description: str | None = None
    sources: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.classification is not None and self.classification not in CLASSIFICATION_LEVELS:
            raise ValueError(
                f"classification '{self.classification}' is invalid. "
                f"Allowed: {list(CLASSIFICATION_LEVELS)}"
            )
        object.__setattr__(self, "sources", tuple(self.sources))


@dataclass(frozen=True)
class DatasetRecord:
    target_fqn: str
    pipeline_path: str
    data_product_id: str | None
    contract_version: str | None
    definition_hash: str
    owner: str | None
    columns: tuple[ColumnRecord, ...]
    indexed_at: datetime
    last_run_id: str | None = None
    lineage: dict = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "columns", tuple(self.columns))


@runtime_checkable
class MetadataStore(Protocol):
    """Persistence protocol for the metadata registry.

    Upsert is idempotent by ``(target_fqn, definition_hash)``: re-indexing an
    unchanged definition writes nothing new.
    """

    def upsert(self, record: DatasetRecord) -> bool:
        """Persist a record, returning True when the stored content changed."""
        ...

    def get(self, target_fqn: str) -> DatasetRecord | None:
        """Return the latest known record for a physical FQN."""
        ...

    def list_all(self) -> list[DatasetRecord]:
        """Return every persisted dataset record."""
        ...

    def search_columns(self, text: str) -> list[tuple[str, ColumnRecord]]:
        """Return columns whose name or description contains ``text``."""
        ...


@dataclass(frozen=True)
class ImpactReport:
    root_fqn: str
    impacted_datasets: tuple[str, ...]
    impacted_columns: tuple[tuple[str, str], ...]
    edges: tuple[dict, ...]
    truncated: bool

    def to_dict(self) -> dict:
        return {
            "root_fqn": self.root_fqn,
            "impacted_datasets": list(self.impacted_datasets),
            "impacted_columns": [
                {"target_fqn": fqn, "column": column}
                for fqn, column in self.impacted_columns
            ],
            "edges": [dict(edge) for edge in self.edges],
            "truncated": self.truncated,
        }


class MetadataRegistryQuery:
    """Merged lineage queries across every record in the metadata registry."""

    def __init__(self, store: MetadataStore, *, max_depth: int = 20):
        self._store = store
        self._max_depth = max_depth

    def merged_graph(self) -> "LineageGraph":
        from skifer.lineage.tracker import LineageGraph

        graph = LineageGraph()
        for record in self._store.list_all():
            if record.lineage:
                graph.merge(LineageGraph.from_dict(record.lineage))
        if graph.has_cycle():
            raise ValueError("Metadata lineage graph contains a cycle; refusing to traverse.")
        return graph

    def upstream(self, fqn: str, column: str) -> list["LineageEdge"]:
        return self.merged_graph().upstream_closure(
            fqn, column, max_depth=self._max_depth
        )

    def downstream(self, fqn: str, column: str) -> list["LineageEdge"]:
        return self.merged_graph().downstream_closure(
            fqn, column, max_depth=self._max_depth
        )

    def impact(self, fqn: str) -> ImpactReport:
        graph = self.merged_graph()
        record = self._store.get(fqn)
        if record is None:
            return ImpactReport(
                root_fqn=fqn,
                impacted_datasets=(),
                impacted_columns=(),
                edges=(),
                truncated=False,
            )

        edges: list["LineageEdge"] = []
        seen_edges: set["LineageEdge"] = set()
        impacted_columns: set[tuple[str, str]] = set()
        truncated = False
        for column in record.columns:
            column_edges, column_truncated = graph._closure(
                fqn,
                column.name,
                direction="downstream",
                max_depth=self._max_depth,
            )
            truncated = truncated or column_truncated
            for edge in column_edges:
                if edge not in seen_edges:
                    seen_edges.add(edge)
                    edges.append(edge)
                impacted_columns.add((edge.target_table, edge.target_column))

        impacted_datasets = tuple(
            sorted({table for table, _ in impacted_columns if table != fqn})
        )
        return ImpactReport(
            root_fqn=fqn,
            impacted_datasets=impacted_datasets,
            impacted_columns=tuple(sorted(impacted_columns)),
            edges=tuple(_edge_to_dict(edge) for edge in edges),
            truncated=truncated,
        )

    def search_columns(self, text: str) -> list[tuple[str, "ColumnRecord"]]:
        return self._store.search_columns(text)


def _record_to_json(record: DatasetRecord) -> str:
    """Serialize a DatasetRecord as deterministic JSON."""
    return json.dumps(asdict(record), default=_json_default, sort_keys=True)


def _record_from_json(payload: str) -> DatasetRecord:
    """Rebuild a DatasetRecord from the store JSON representation."""
    raw = json.loads(payload)
    columns = tuple(ColumnRecord(**column) for column in raw.get("columns", []))
    return DatasetRecord(
        target_fqn=raw["target_fqn"],
        pipeline_path=raw["pipeline_path"],
        data_product_id=raw.get("data_product_id"),
        contract_version=raw.get("contract_version"),
        definition_hash=raw["definition_hash"],
        owner=raw.get("owner"),
        columns=columns,
        indexed_at=_coerce_datetime(raw["indexed_at"]),
        last_run_id=raw.get("last_run_id"),
        lineage=raw.get("lineage") or {},
    )


def _content_fingerprint(record: DatasetRecord) -> str:
    """Hash record content excluding run-specific indexing metadata."""
    payload = asdict(record)
    payload.pop("indexed_at", None)
    payload.pop("last_run_id", None)
    encoded = json.dumps(payload, default=_json_default, sort_keys=True).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _json_default(value: object) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _coerce_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        return value
    if hasattr(value, "to_pydatetime"):
        return value.to_pydatetime()
    if isinstance(value, str):
        return datetime.fromisoformat(value)
    raise TypeError(f"Cannot deserialize datetime from {type(value).__name__}")


def _search_records(
    records: list[DatasetRecord], text: str
) -> list[tuple[str, ColumnRecord]]:
    needle = text.casefold()
    matches: list[tuple[str, ColumnRecord]] = []
    for record in records:
        for column in record.columns:
            haystack = f"{column.name}\n{column.description or ''}".casefold()
            if needle in haystack:
                matches.append((record.target_fqn, column))
    return matches


def _edge_to_dict(edge: "LineageEdge") -> dict:
    return {
        "source_table": edge.source_table,
        "source_column": edge.source_column,
        "target_table": edge.target_table,
        "target_column": edge.target_column,
        "transformations": list(edge.transformations),
        "edge_type": edge.edge_type,
    }


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _row_value(row: object, key: str, index: int = 0):
    if isinstance(row, dict):
        return row[key]
    try:
        return row[key]  # type: ignore[index]
    except (KeyError, TypeError):
        return row[index]  # type: ignore[index]


class SqliteMetadataStore:
    _CREATE_TABLE = """
    CREATE TABLE IF NOT EXISTS metadata_registry (
        target_fqn      TEXT NOT NULL,
        definition_hash TEXT NOT NULL,
        content_hash    TEXT NOT NULL,
        record          TEXT NOT NULL,
        indexed_at      TEXT NOT NULL,
        PRIMARY KEY (target_fqn, definition_hash)
    )
    """

    def __init__(self, db_path: str = ".skifer_metadata.db"):
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute(self._CREATE_TABLE)
        self._conn.commit()

    def upsert(self, record: DatasetRecord) -> bool:
        content_hash = _content_fingerprint(record)
        row = self._conn.execute(
            "SELECT content_hash FROM metadata_registry "
            "WHERE target_fqn = ? AND definition_hash = ?",
            (record.target_fqn, record.definition_hash),
        ).fetchone()
        if row is not None and row[0] == content_hash:
            return False

        self._conn.execute(
            "INSERT OR REPLACE INTO metadata_registry "
            "(target_fqn, definition_hash, content_hash, record, indexed_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                record.target_fqn,
                record.definition_hash,
                content_hash,
                _record_to_json(record),
                record.indexed_at.isoformat(),
            ),
        )
        self._conn.commit()
        return True

    def attach_run_id(
        self,
        target_fqn: str,
        definition_hash: str,
        last_run_id: str,
    ) -> bool:
        """Attach the latest certified run id without changing content idempotence."""
        row = self._conn.execute(
            "SELECT record FROM metadata_registry "
            "WHERE target_fqn = ? AND definition_hash = ?",
            (target_fqn, definition_hash),
        ).fetchone()
        if row is None:
            return False
        record = _record_from_json(row[0])
        if record.last_run_id == last_run_id:
            return False
        updated = replace(record, last_run_id=last_run_id)
        self._conn.execute(
            "UPDATE metadata_registry SET record = ? "
            "WHERE target_fqn = ? AND definition_hash = ?",
            (_record_to_json(updated), target_fqn, definition_hash),
        )
        self._conn.commit()
        return True

    def get(self, target_fqn: str) -> DatasetRecord | None:
        row = self._conn.execute(
            "SELECT record FROM metadata_registry WHERE target_fqn = ? "
            "ORDER BY indexed_at DESC LIMIT 1",
            (target_fqn,),
        ).fetchone()
        return _record_from_json(row[0]) if row else None

    def list_all(self) -> list[DatasetRecord]:
        rows = self._conn.execute(
            "SELECT record FROM metadata_registry "
            "ORDER BY target_fqn ASC, indexed_at DESC, definition_hash ASC"
        ).fetchall()
        return [_record_from_json(row[0]) for row in rows]

    def search_columns(self, text: str) -> list[tuple[str, ColumnRecord]]:
        return _search_records(self.list_all(), text)

    def close(self) -> None:
        self._conn.close()


class DeltaMetadataStore:
    """Delta metadata store using only ``backend.spark``."""

    def __init__(self, backend, table_fqn: str = "_skifer_metadata.datasets"):
        self._backend = backend
        self._table_fqn = table_fqn
        self._ensure_table()

    def _spark(self):
        spark = getattr(self._backend, "spark", None)
        if spark is None:
            raise RuntimeError("DeltaMetadataStore requires a backend with a .spark attribute.")
        return spark

    def _ensure_table(self) -> None:
        try:
            spark = self._spark()
            spark.sql(
                f"""
                CREATE TABLE IF NOT EXISTS {self._table_fqn} (
                    target_fqn      STRING,
                    definition_hash STRING,
                    content_hash    STRING,
                    `record`        STRING,
                    indexed_at      TIMESTAMP
                ) USING DELTA
                """
            )
        except Exception:
            pass

    def upsert(self, record: DatasetRecord) -> bool:
        spark = self._spark()
        content_hash = _content_fingerprint(record)
        rows = spark.sql(
            f"""
            SELECT content_hash
            FROM {self._table_fqn}
            WHERE target_fqn = {_sql_literal(record.target_fqn)}
              AND definition_hash = {_sql_literal(record.definition_hash)}
            LIMIT 1
            """
        ).collect()
        if rows and _row_value(rows[0], "content_hash") == content_hash:
            return False

        record_json = _record_to_json(record)
        spark.sql(
            f"""
            MERGE INTO {self._table_fqn} AS target
            USING (
                SELECT
                    {_sql_literal(record.target_fqn)} AS target_fqn,
                    {_sql_literal(record.definition_hash)} AS definition_hash,
                    {_sql_literal(content_hash)} AS content_hash,
                    {_sql_literal(record_json)} AS `record`,
                    CAST({_sql_literal(record.indexed_at.isoformat())} AS TIMESTAMP) AS indexed_at
            ) AS source
            ON target.target_fqn = source.target_fqn
               AND target.definition_hash = source.definition_hash
            WHEN MATCHED THEN UPDATE SET
                content_hash = source.content_hash,
                `record` = source.`record`,
                indexed_at = source.indexed_at
            WHEN NOT MATCHED THEN INSERT (
                target_fqn, definition_hash, content_hash, `record`, indexed_at
            ) VALUES (
                source.target_fqn,
                source.definition_hash,
                source.content_hash,
                source.`record`,
                source.indexed_at
            )
            """
        )
        return True

    def attach_run_id(
        self,
        target_fqn: str,
        definition_hash: str,
        last_run_id: str,
    ) -> bool:
        """Attach the latest certified run id without changing content idempotence."""
        spark = self._spark()
        rows = spark.sql(
            f"""
            SELECT `record`
            FROM {self._table_fqn}
            WHERE target_fqn = {_sql_literal(target_fqn)}
              AND definition_hash = {_sql_literal(definition_hash)}
            LIMIT 1
            """
        ).collect()
        if not rows:
            return False
        record = _record_from_json(_row_value(rows[0], "record"))
        if record.last_run_id == last_run_id:
            return False
        updated = replace(record, last_run_id=last_run_id)
        spark.sql(
            f"""
            UPDATE {self._table_fqn}
            SET `record` = {_sql_literal(_record_to_json(updated))}
            WHERE target_fqn = {_sql_literal(target_fqn)}
              AND definition_hash = {_sql_literal(definition_hash)}
            """
        )
        return True

    def get(self, target_fqn: str) -> DatasetRecord | None:
        spark = self._spark()
        rows = spark.sql(
            f"""
            SELECT `record`
            FROM {self._table_fqn}
            WHERE target_fqn = {_sql_literal(target_fqn)}
            ORDER BY indexed_at DESC
            LIMIT 1
            """
        ).collect()
        return _record_from_json(_row_value(rows[0], "record")) if rows else None

    def list_all(self) -> list[DatasetRecord]:
        spark = self._spark()
        rows = spark.sql(
            f"""
            SELECT `record`
            FROM {self._table_fqn}
            ORDER BY target_fqn ASC, indexed_at DESC, definition_hash ASC
            """
        ).collect()
        return [_record_from_json(_row_value(row, "record")) for row in rows]

    def search_columns(self, text: str) -> list[tuple[str, ColumnRecord]]:
        return _search_records(self.list_all(), text)
