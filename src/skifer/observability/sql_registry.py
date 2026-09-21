"""Portable SQL persistence for Skifer's governance registries.

The registry deliberately depends only on the thin runtime adapter boundary.  It
owns the SQL needed to create, append, query, and upsert registry rows so each
warehouse adapter does not have to grow a parallel persistence API.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from skifer.core.adapters.base import Adapter
from skifer.core.dialect import quote_fqn, quote_ident
from skifer.core.sql_compiler import escape_sql_string


class SqlRegistryError(ValueError):
    """Raised when a registry definition or operation is invalid."""


@dataclass(frozen=True)
class SqlColumn:
    """One named column in a registry table."""

    name: str
    sql_type: str


@dataclass(frozen=True)
class TableDefinition:
    """Declarative shape and logical key of one registry table."""

    name: str
    columns: tuple[SqlColumn, ...]
    key: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.name:
            raise SqlRegistryError("A registry table name must not be empty.")
        names = tuple(column.name for column in self.columns)
        if not names or any(not name for name in names):
            raise SqlRegistryError(
                f"Registry table {self.name!r} must declare named columns."
            )
        if len(set(names)) != len(names):
            raise SqlRegistryError(
                f"Registry table {self.name!r} declares duplicate columns."
            )
        if not self.key or any(column not in names for column in self.key):
            raise SqlRegistryError(
                f"Registry table {self.name!r} must declare a valid non-empty key."
            )

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(column.name for column in self.columns)


def _columns(*items: tuple[str, str]) -> tuple[SqlColumn, ...]:
    return tuple(SqlColumn(name, sql_type) for name, sql_type in items)


CONTRACT_DEFINITIONS = TableDefinition(
    name="contract_definitions",
    columns=_columns(
        ("contract_id", "STRING"),
        ("contract_version", "STRING"),
        ("definition_hash", "STRING"),
        ("canonical_json", "STRING"),
        ("data_product_id", "STRING"),
        ("owner", "STRING"),
        ("hash_algorithm", "STRING"),
        ("canonicalization_version", "BIGINT"),
        ("status", "STRING"),
        ("created_at", "STRING"),
    ),
    key=("definition_hash",),
)

MATERIALIZATION_RUNS = TableDefinition(
    name="materialization_runs",
    columns=_columns(
        ("event_id", "STRING"),
        ("run_id", "STRING"),
        ("dataset", "STRING"),
        ("state", "STRING"),
        ("contract_id", "STRING"),
        ("contract_version", "STRING"),
        ("definition_hash", "STRING"),
        ("occurred_at", "STRING"),
        ("target_fqn", "STRING"),
        ("staging_fqn", "STRING"),
        ("quarantine_fqn", "STRING"),
    ),
    key=("event_id",),
)

CHECK_RESULTS = TableDefinition(
    name="check_results",
    columns=_columns(
        ("event_id", "STRING"),
        ("run_id", "STRING"),
        ("check_type", "STRING"),
        ("scope", "STRING"),
        ("severity", "STRING"),
        ("status", "STRING"),
        ("actual_value", "STRING"),
        ("expected_value", "STRING"),
        ("message", "STRING"),
    ),
    key=("event_id",),
)

INCIDENTS = TableDefinition(
    name="incidents",
    columns=_columns(
        ("id", "STRING"),
        ("target_fqn", "STRING"),
        ("run_id", "STRING"),
        ("check_name", "STRING"),
        ("severity", "STRING"),
        ("status", "STRING"),
        ("opened_at", "STRING"),
        ("assignee", "STRING"),
        ("root_cause", "STRING"),
        ("resolved_at", "STRING"),
    ),
    key=("id",),
)

SEMANTIC_USAGE_EVENTS = TableDefinition(
    name="semantic_usage_events",
    columns=_columns(
        ("event_id", "STRING"),
        ("occurred_at", "STRING"),
        ("environment", "STRING"),
        ("consumer_class", "STRING"),
        ("model_hashes", "STRING"),
        ("metric_ids", "STRING"),
        ("dimension_ids", "STRING"),
        ("normalized_filter_shape", "STRING"),
        ("query_fingerprint", "STRING"),
        ("duration_ms", "BIGINT"),
        ("rows_returned", "BIGINT"),
        ("bytes_scanned", "BIGINT"),
        ("status", "STRING"),
    ),
    key=("event_id",),
)

CHECK_HISTORY = TableDefinition(
    name="check_history",
    columns=_columns(
        ("table_fqn", "STRING"),
        ("ts", "TIMESTAMP"),
        ("report_json", "STRING"),
    ),
    key=("table_fqn", "ts", "report_json"),
)

METADATA_DATASETS = TableDefinition(
    name="datasets",
    columns=_columns(
        ("target_fqn", "STRING"),
        ("definition_hash", "STRING"),
        ("content_hash", "STRING"),
        ("record", "STRING"),
        ("indexed_at", "TIMESTAMP"),
    ),
    key=("target_fqn", "definition_hash"),
)

TABLE_DEFINITIONS: tuple[TableDefinition, ...] = (
    CONTRACT_DEFINITIONS,
    MATERIALIZATION_RUNS,
    CHECK_RESULTS,
    INCIDENTS,
    SEMANTIC_USAGE_EVENTS,
    CHECK_HISTORY,
    METADATA_DATASETS,
)


def _sql_literal(value: Any) -> str:
    """Render one bounded registry value through the sole escaping path."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, datetime):
        return "CAST(" + _sql_literal(value.isoformat()) + " AS TIMESTAMP)"
    if isinstance(value, str):
        return "'" + escape_sql_string(value) + "'"
    raise TypeError(
        "Registry values must be strings, numbers, booleans, or None; "
        f"received {type(value).__name__}."
    )


class SqlRegistry:
    """Store registry rows using only the existing :class:`Adapter` Protocol."""

    def __init__(self, adapter: Adapter, schema: str):
        if not schema:
            raise SqlRegistryError("A registry schema must not be empty.")
        self.adapter = adapter
        self.schema = schema

    def ensure_table(self, definition: TableDefinition) -> None:
        """Create a definition once and reject an incompatible existing table."""
        self.adapter.ensure_schema_exists(self.schema)
        if not self.adapter.table_exists(None, self.schema, definition.name):
            columns = ", ".join(
                f"{self._ident(column.name)} {column.sql_type}"
                for column in definition.columns
            )
            self.adapter.execute_sql(
                f"CREATE TABLE IF NOT EXISTS {self._table(definition)} ({columns})"
            )

        actual = tuple(
            self.adapter.list_relation_columns(self._table(definition))
        )
        if actual != definition.column_names:
            raise SqlRegistryError(
                f"Registry table {self.schema}.{definition.name} has columns "
                f"{actual!r}; expected {definition.column_names!r}."
            )

    def append(self, definition: TableDefinition, row: Mapping[str, Any]) -> None:
        """Append one row once, using the definition key for idempotency."""
        values = self._row(definition, row)
        self.ensure_table(definition)
        columns = ", ".join(self._ident(name) for name in definition.column_names)
        literals = ", ".join(_sql_literal(values[name]) for name in definition.column_names)
        key_match = self._key_match(definition, values)
        table = self._table(definition)
        self.adapter.execute_sql(
            f"INSERT INTO {table} ({columns}) SELECT {literals} "
            f"WHERE NOT EXISTS (SELECT 1 FROM {table} WHERE {key_match})"
        )

    def insert(self, definition: TableDefinition, row: Mapping[str, Any]) -> None:
        """Insert one row without applying key idempotency."""
        values = self._row(definition, row)
        self.ensure_table(definition)
        columns = ", ".join(self._ident(name) for name in definition.column_names)
        literals = ", ".join(_sql_literal(values[name]) for name in definition.column_names)
        self.adapter.execute_sql(
            f"INSERT INTO {self._table(definition)} ({columns}) VALUES ({literals})"
        )

    def find(
        self,
        definition: TableDefinition,
        *,
        where: Mapping[str, Any] | None = None,
        comparisons: Sequence[tuple[str, str, Any]] | None = None,
        order_by: Sequence[tuple[str, str]] | None = None,
        limit: int | None = None,
    ) -> list[dict]:
        """Read rows using bounded filters, deterministic ordering, and a limit."""
        self.ensure_table(definition)
        predicates = self._predicates(definition, where or {})
        predicates.extend(self._comparisons(definition, comparisons or ()))
        ordering = self._ordering(definition, order_by)
        query = f"SELECT * FROM {self._table(definition)}"
        if predicates:
            query += " WHERE " + " AND ".join(predicates)
        query += " ORDER BY " + ", ".join(ordering)
        if limit is not None:
            if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
                raise SqlRegistryError("Registry query limit must be a positive integer.")
            query += f" LIMIT {limit}"
        return self.adapter.fetch(query)

    def upsert(self, definition: TableDefinition, row: Mapping[str, Any]) -> None:
        """Insert a key once or replace every stored value for that key."""
        values = self._row(definition, row)
        self.ensure_table(definition)
        target = self._ident("target")
        source = self._ident("source")
        source_values = ", ".join(
            f"{_sql_literal(values[name])} AS {self._ident(name)}"
            for name in definition.column_names
        )
        key_match = " AND ".join(
            f"{target}.{self._ident(name)} = {source}.{self._ident(name)}"
            for name in definition.key
        )
        assignments = ", ".join(
            f"{self._ident(name)} = {source}.{self._ident(name)}"
            for name in definition.column_names
        )
        columns = ", ".join(self._ident(name) for name in definition.column_names)
        inserts = ", ".join(
            f"{source}.{self._ident(name)}" for name in definition.column_names
        )
        self.adapter.execute_sql(
            f"MERGE INTO {self._table(definition)} AS {target} "
            f"USING (SELECT {source_values}) AS {source} ON {key_match} "
            f"WHEN MATCHED THEN UPDATE SET {assignments} "
            f"WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({inserts})"
        )

    def _ident(self, name: str) -> str:
        return quote_ident(name, target=self.adapter.name)

    def _table(self, definition: TableDefinition) -> str:
        return quote_fqn(
            f"{self.schema}.{definition.name}", target=self.adapter.name
        )

    def _row(
        self, definition: TableDefinition, row: Mapping[str, Any]
    ) -> dict[str, Any]:
        if not isinstance(row, Mapping):
            raise TypeError("Registry row must be a mapping.")
        missing = set(definition.column_names) - set(row)
        extra = set(row) - set(definition.column_names)
        if missing or extra:
            raise SqlRegistryError(
                f"Registry row for {definition.name!r} does not match its definition; "
                f"missing={sorted(missing)!r}, extra={sorted(extra)!r}."
            )
        values = dict(row)
        null_keys = [name for name in definition.key if values[name] is None]
        if null_keys:
            raise SqlRegistryError(
                f"Registry key columns cannot be NULL: {null_keys!r}."
            )
        return values

    def _key_match(
        self, definition: TableDefinition, values: Mapping[str, Any]
    ) -> str:
        return " AND ".join(
            f"{self._ident(name)} = {_sql_literal(values[name])}"
            for name in definition.key
        )

    def _predicates(
        self, definition: TableDefinition, where: Mapping[str, Any]
    ) -> list[str]:
        unknown = set(where) - set(definition.column_names)
        if unknown:
            raise SqlRegistryError(
                f"Unknown filter columns for {definition.name!r}: {sorted(unknown)!r}."
            )
        predicates = []
        for name, value in where.items():
            column = self._ident(name)
            predicates.append(
                f"{column} IS NULL"
                if value is None
                else f"{column} = {_sql_literal(value)}"
            )
        return predicates

    def _comparisons(
        self,
        definition: TableDefinition,
        comparisons: Sequence[tuple[str, str, Any]],
    ) -> list[str]:
        names = set(definition.column_names)
        allowed_operators = {"=", "!=", ">=", "<="}
        predicates = []
        for item in comparisons:
            if not isinstance(item, (tuple, list)) or len(item) != 3:
                raise SqlRegistryError(
                    "Registry comparisons must be (column, operator, value) triples."
                )
            name, operator, value = item
            if name not in names or operator not in allowed_operators:
                raise SqlRegistryError(
                    f"Invalid registry comparison {item!r} for {definition.name!r}."
                )
            column = self._ident(name)
            if value is None:
                if operator not in {"=", "!="}:
                    raise SqlRegistryError(
                        "Registry NULL comparisons only support '=' and '!='."
                    )
                predicates.append(
                    f"{column} IS {'NOT ' if operator == '!=' else ''}NULL"
                )
            else:
                predicates.append(f"{column} {operator} {_sql_literal(value)}")
        return predicates

    def _ordering(
        self,
        definition: TableDefinition,
        order_by: Sequence[tuple[str, str]] | None,
    ) -> list[str]:
        requested = list(order_by or ())
        names = set(definition.column_names)
        seen: set[str] = set()
        ordering = []
        for item in requested:
            if not isinstance(item, (tuple, list)) or len(item) != 2:
                raise SqlRegistryError(
                    "Registry ordering entries must be (column, direction) pairs."
                )
            name, direction = item
            normalized = str(direction).upper()
            if name not in names or normalized not in {"ASC", "DESC"}:
                raise SqlRegistryError(
                    f"Invalid registry ordering {item!r} for {definition.name!r}."
                )
            if name not in seen:
                ordering.append(f"{self._ident(name)} {normalized}")
                seen.add(name)
        for name in definition.key:
            if name not in seen:
                ordering.append(f"{self._ident(name)} ASC")
        return ordering


class SqlRegistryBackend:
    """Speak the stores' existing vocabulary on top of the generic registry."""

    def __init__(self, adapter):
        self.adapter = adapter

    def _registry(self, schema: str) -> SqlRegistry:
        return SqlRegistry(self.adapter, schema)

    def append_certification_check(self, schema: str, row: dict) -> None:
        self._registry(schema).append(CHECK_RESULTS, row)

    def append_certification_contract(self, schema: str, row: dict) -> None:
        self._registry(schema).append(CONTRACT_DEFINITIONS, row)

    def append_certification_run(self, schema: str, row: dict) -> None:
        self._registry(schema).append(MATERIALIZATION_RUNS, row)

    def append_semantic_usage_event(self, schema: str, row: dict) -> None:
        self._registry(schema).append(SEMANTIC_USAGE_EVENTS, row)

    def get_certification_check_results(
        self, schema: str, run_id: str
    ) -> list[dict]:
        return self._registry(schema).find(
            CHECK_RESULTS,
            where={"run_id": run_id},
        )

    def get_certification_contract(
        self, schema: str, contract_id: str, version: str
    ) -> dict | None:
        rows = self._registry(schema).find(
            CONTRACT_DEFINITIONS,
            where={"contract_id": contract_id, "contract_version": version},
            limit=2,
        )
        if len(rows) > 1:
            raise ValueError(
                f"Contract '{contract_id}' version '{version}' has ambiguous definitions."
            )
        return rows[0] if rows else None

    def get_certification_contract_by_hash(
        self, schema: str, contract_id: str, definition_hash: str
    ) -> dict | None:
        rows = self._registry(schema).find(
            CONTRACT_DEFINITIONS,
            where={
                "contract_id": contract_id,
                "definition_hash": definition_hash,
            },
            limit=1,
        )
        return rows[0] if rows else None

    def get_certification_run(self, schema: str, run_id: str) -> dict | None:
        rows = self._registry(schema).find(
            MATERIALIZATION_RUNS,
            where={"run_id": run_id},
            order_by=(("occurred_at", "DESC"),),
            limit=1,
        )
        return rows[0] if rows else None

    def get_incident(self, schema: str, incident_id: str) -> dict | None:
        rows = self._registry(schema).find(
            INCIDENTS,
            where={"id": incident_id},
            limit=1,
        )
        return rows[0] if rows else None

    def get_latest_certification_promotion(
        self, schema: str, dataset: str
    ) -> dict | None:
        rows = self._registry(schema).find(
            MATERIALIZATION_RUNS,
            where={"dataset": dataset, "state": "PROMOTED"},
            order_by=(("occurred_at", "DESC"),),
            limit=1,
        )
        return rows[0] if rows else None

    def get_open_incident(
        self, schema: str, target_fqn: str, check_name: str
    ) -> dict | None:
        rows = self._registry(schema).find(
            INCIDENTS,
            where={"target_fqn": target_fqn, "check_name": check_name},
            comparisons=(("status", "!=", "RESOLVED"),),
            order_by=(("opened_at", "DESC"),),
            limit=1,
        )
        return rows[0] if rows else None

    def list_certification_history(
        self, schema: str, dataset: str, limit: int = 50
    ) -> list[dict]:
        return self._registry(schema).find(
            MATERIALIZATION_RUNS,
            where={"dataset": dataset},
            order_by=(("occurred_at", "DESC"),),
            limit=limit,
        )

    def list_incidents(
        self,
        schema: str,
        *,
        status: str | None = None,
        target_fqn: str | None = None,
        limit: int = 50,
    ) -> list[dict]:
        where = {}
        if status is not None:
            where["status"] = status
        if target_fqn is not None:
            where["target_fqn"] = target_fqn
        return self._registry(schema).find(
            INCIDENTS,
            where=where,
            order_by=(("opened_at", "DESC"),),
            limit=limit,
        )

    def list_open_incidents(
        self, schema: str, target_fqn: str
    ) -> list[dict]:
        return self._registry(schema).find(
            INCIDENTS,
            where={"target_fqn": target_fqn},
            comparisons=(("status", "!=", "RESOLVED"),),
            order_by=(("opened_at", "DESC"),),
        )

    def list_semantic_usage_events(
        self,
        schema: str,
        *,
        environment: str | None = None,
        consumer_class: str | None = None,
        since: str | None = None,
        until: str | None = None,
        limit: int | None = None,
    ) -> list[dict]:
        where = {}
        if environment is not None:
            where["environment"] = environment
        if consumer_class is not None:
            where["consumer_class"] = consumer_class
        comparisons = []
        if since is not None:
            comparisons.append(("occurred_at", ">=", since))
        if until is not None:
            comparisons.append(("occurred_at", "<=", until))
        return self._registry(schema).find(
            SEMANTIC_USAGE_EVENTS,
            where=where,
            comparisons=comparisons,
            order_by=(("occurred_at", "DESC"), ("event_id", "DESC")),
            limit=limit,
        )

    def upsert_incident(self, schema: str, row: dict) -> None:
        self._registry(schema).upsert(INCIDENTS, row)


__all__ = [
    "CHECK_HISTORY",
    "CHECK_RESULTS",
    "CONTRACT_DEFINITIONS",
    "INCIDENTS",
    "MATERIALIZATION_RUNS",
    "METADATA_DATASETS",
    "SEMANTIC_USAGE_EVENTS",
    "TABLE_DEFINITIONS",
    "SqlColumn",
    "SqlRegistry",
    "SqlRegistryBackend",
    "SqlRegistryError",
    "TableDefinition",
]
