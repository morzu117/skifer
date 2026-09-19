"""DuckDB implementation of the thin runtime adapter boundary."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from skifer.core.capabilities_matrix import (
    CAP_CERTIFIED_PUBLICATION,
    CAP_DEV_LIMIT,
    CAP_DROP_DUPLICATES,
    CAP_FILE_SOURCES,
    CAP_INCREMENTAL,
    CAP_SNAPSHOT,
    CAP_VIEW,
)
from skifer.core.dialect import quote_fqn, quote_ident, transpile
from skifer.core.ir import ParsedTable
from skifer.core.sql_compiler import escape_sql_string


class DuckDBAdapterError(RuntimeError):
    """Raised when a DuckDB adapter operation cannot be completed."""


@dataclass(frozen=True, slots=True)
class DuckDBRelation:
    """Opaque DuckDB relation handle backed by one complete SELECT query."""

    sql: str


class DuckDBAdapter:
    """Execute compiled Skifer SQL through one caller-owned DuckDB connection."""

    def __init__(self, connection: Any):
        if connection is None:
            raise TypeError("DuckDBAdapter requires an injected connection.")
        self._connection = connection

    @property
    def name(self) -> str:
        return "duckdb"

    @property
    def capabilities(self) -> frozenset[str]:
        return frozenset(
            {
                CAP_DEV_LIMIT,
                CAP_CERTIFIED_PUBLICATION,
                CAP_DROP_DUPLICATES,
                CAP_FILE_SOURCES,
                CAP_INCREMENTAL,
                CAP_VIEW,
                CAP_SNAPSHOT,
            }
        )

    @property
    def connection(self) -> Any:
        """Expose the injected connection for explicit lifecycle management."""
        return self._connection

    def resolve_source(self, table: ParsedTable) -> str:
        """Return DuckDB's relation expression for one file-backed table.

        Supported Spark options are deliberately allowlisted and translated:
        CSV ``header`` -> ``header``, ``sep`` -> ``delim``, and
        ``inferSchema`` -> ``auto_detect`` when true or ``all_varchar`` when
        false; Parquet ``mergeSchema`` -> ``union_by_name``; JSON
        ``multiLine`` -> ``format``. Spark defaults are emitted explicitly so
        DuckDB auto-detection cannot silently change the input. Every other
        option is refused instead of being ignored.
        """
        source_type = table.source_type
        readers = {
            "csv": "read_csv",
            "parquet": "read_parquet",
            "json": "read_json_auto",
        }
        if source_type not in readers:
            raise DuckDBAdapterError(
                f"Adapter 'duckdb' does not support file source format {source_type!r}."
            )
        if not isinstance(table.source_path, str) or not table.source_path:
            raise DuckDBAdapterError(
                f"Adapter 'duckdb' requires a non-empty path for file source "
                f"format {source_type!r}."
            )
        if not isinstance(table.source_options, Mapping):
            raise DuckDBAdapterError(
                f"Adapter 'duckdb' requires source options for format "
                f"{source_type!r} to be a mapping."
            )

        allowed = {
            "csv": frozenset({"header", "inferSchema", "sep"}),
            "parquet": frozenset({"mergeSchema"}),
            "json": frozenset({"multiLine"}),
        }[source_type]
        unknown = sorted(set(table.source_options) - allowed, key=str)
        if unknown:
            raise DuckDBAdapterError(
                f"Adapter 'duckdb' does not support source option {unknown[0]!r} "
                f"for file source format {source_type!r}."
            )

        arguments = [self._string_literal(table.source_path)]
        if source_type == "csv":
            header = self._boolean_option(table, "header", default=False)
            infer = self._boolean_option(table, "inferSchema", default=False)
            separator = (
                self._string_option(table, "sep", allow_empty=False)
                if "sep" in table.source_options
                else self._string_literal(",")
            )
            arguments.extend(
                [
                    "header = " + header,
                    "auto_detect = TRUE" if infer == "TRUE" else "all_varchar = TRUE",
                    "delim = " + separator,
                ]
            )
            if header == "FALSE":
                arguments.append("names = " + self._spark_csv_column_names(arguments))
        elif source_type == "parquet":
            arguments.append(
                "union_by_name = "
                + self._boolean_option(table, "mergeSchema", default=False)
            )
        elif source_type == "json":
            # ``multiLine: true`` maps to ``array``, never to ``auto``. Measured
            # against Spark on the same physical files: for a JSON array both read
            # the same rows, but for several objects concatenated in one file Spark
            # parses a single JSON value and keeps only the first, where ``auto``
            # reads them all — the same YAML returning a different row count with no
            # error. ``array`` is exact where it applies and raises everywhere else,
            # which is the trade this project makes: a loud failure over a silent
            # divergence. A single JSON object spanning several lines is therefore
            # unsupported here; wrap it in an array.
            multiline = self._boolean_option(table, "multiLine", default=False)
            json_format = "array" if multiline == "TRUE" else "newline_delimited"
            arguments.append("format = " + self._string_literal(json_format))

        return f"{readers[source_type]}({', '.join(arguments)})"

    @staticmethod
    def _string_literal(value: str) -> str:
        return "'" + escape_sql_string(value) + "'"

    @classmethod
    def _string_option(
        cls, table: ParsedTable, name: str, *, allow_empty: bool
    ) -> str:
        value = table.source_options[name]
        if not isinstance(value, str) or (not allow_empty and not value):
            raise DuckDBAdapterError(
                f"Adapter 'duckdb' source option {name!r} for file source format "
                f"{table.source_type!r} must be a non-empty string."
            )
        return cls._string_literal(value)

    def _spark_csv_column_names(self, arguments: list[str]) -> str:
        relation = f"read_csv({', '.join(arguments)})"
        try:
            cursor = self._connection.execute(f"SELECT * FROM {relation} LIMIT 0")
        except Exception as exc:
            raise DuckDBAdapterError(
                f"Adapter 'duckdb' could not inspect CSV source {arguments[0]} "
                "to reproduce Spark header=false column names."
            ) from exc
        names = [f"_c{index}" for index, _ in enumerate(cursor.description or [])]
        return "[" + ", ".join(self._string_literal(name) for name in names) + "]"

    @staticmethod
    def _boolean_option(
        table: ParsedTable, name: str, *, default: bool | None = None
    ) -> str:
        if name not in table.source_options:
            if default is None:
                raise DuckDBAdapterError(
                    f"Adapter 'duckdb' requires source option {name!r} for file "
                    f"source format {table.source_type!r}."
                )
            return "TRUE" if default else "FALSE"
        value = table.source_options[name]
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, str) and value.lower() in {"true", "false"}:
            return value.upper()
        raise DuckDBAdapterError(
            f"Adapter 'duckdb' source option {name!r} for file source format "
            f"{table.source_type!r} must be true or false."
        )

    @staticmethod
    def _reject_catalog(catalog: str | None, operation: str) -> None:
        if catalog is not None:
            raise ValueError(
                f"Adapter 'duckdb' operation '{operation}' does not support a catalog; "
                f"DuckDB table names use schema.table, received catalog={catalog!r}."
            )

    @staticmethod
    def _quoted_fqn(fqn: str) -> str:
        return quote_fqn(fqn, target="duckdb")

    @staticmethod
    def _quoted_ident(name: str) -> str:
        return quote_ident(name, target="duckdb")

    def _fetch_with_params(self, query: str, params: list[Any]) -> list[dict]:
        cursor = self._connection.execute(query, params)
        columns = [column[0] for column in (cursor.description or [])]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def fetch(self, query: str) -> list[dict]:
        cursor = self._connection.execute(query)
        columns = [column[0] for column in (cursor.description or [])]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def execute_sql(self, sql: str) -> Any:
        return self._connection.execute(sql)

    def sql(self, query: str) -> Any:
        return self._connection.execute(query)

    def read_table(self, fqn: str) -> Any:
        return self.sql(f"SELECT * FROM {self._quoted_fqn(fqn)}")

    def write_table(self, df: Any, fqn: str, mode: str = "overwrite") -> None:
        relation_sql = self._relation_sql(df, operation="write_table")
        if mode not in ("overwrite", "append"):
            raise ValueError(
                f"Adapter 'duckdb' does not support write mode {mode!r}; "
                "valid modes are 'overwrite' and 'append'."
            )
        target = self._quoted_fqn(fqn)
        statement = (
            f"CREATE OR REPLACE TABLE {target} AS {relation_sql}"
            if mode == "overwrite"
            else f"INSERT INTO {target} {relation_sql}"
        )
        transaction_started = False
        try:
            self._connection.execute("BEGIN TRANSACTION")
            transaction_started = True
            self._connection.execute(statement)
            self._connection.execute("COMMIT")
        except Exception:
            if transaction_started:
                self._connection.execute("ROLLBACK")
            raise

    def relation(self, sql: str) -> DuckDBRelation:
        """Wrap compiled SQL without executing or exposing a DuckDB cursor."""
        if not isinstance(sql, str) or not sql.strip():
            raise TypeError("DuckDB relation SQL must be a non-empty string.")
        return DuckDBRelation(sql=sql)

    @staticmethod
    def _relation_sql(handle: Any, *, operation: str) -> str:
        if not isinstance(handle, DuckDBRelation):
            raise DuckDBAdapterError(
                f"Adapter 'duckdb' operation {operation!r} requires a DuckDBRelation "
                f"handle, got {type(handle).__name__}; DataFrame objects are unsupported."
            )
        return handle.sql

    def write_staging(self, handle: Any, fqn: str) -> None:
        """Materialize one relation handle into an isolated staging table."""
        relation_sql = self._relation_sql(handle, operation="write_staging")
        schema = fqn.replace("`", "").replace('"', "").split(".")[-2]
        self.ensure_schema_exists(schema)
        self.execute_sql(
            f"CREATE OR REPLACE TABLE {self._quoted_fqn(fqn)} AS {relation_sql}"
        )

    def read_staging(self, fqn: str) -> DuckDBRelation:
        return self.relation(f"SELECT * FROM {self._quoted_fqn(fqn)}")

    def tag_row_violations(
        self,
        handle: Any,
        predicates: dict[str, str],
        run_id: str,
        contract_version: str,
    ) -> DuckDBRelation:
        """Return a relation that adds bounded, value-free quarantine metadata."""
        relation_sql = self._relation_sql(handle, operation="tag_row_violations")
        cases = []
        for label, predicate in predicates.items():
            escaped_label = escape_sql_string(label)
            translated = transpile(predicate, target=self.name)
            cases.append(f"CASE WHEN ({translated}) THEN '{escaped_label}' END")
        violations = f"concat_ws(',', {', '.join(cases)})" if cases else "''"
        escaped_run_id = escape_sql_string(run_id)
        escaped_contract_version = escape_sql_string(contract_version)
        return self.relation(
            "SELECT *, "
            f"{violations} AS {self._quoted_ident('_violations')}, "
            f"'{escaped_run_id}' AS {self._quoted_ident('_run_id')}, "
            f"'{escaped_contract_version}' AS {self._quoted_ident('_contract_version')} "
            f"FROM (\n{relation_sql}\n) AS {self._quoted_ident('_skifer_staged')}"
        )

    def drop_staging(self, fqn: str) -> None:
        self.drop_table(fqn)

    def table_exists(self, catalog: str | None, schema: str, table: str) -> bool:
        self._reject_catalog(catalog, "table_exists")
        fqn = self.build_fqn(None, schema, table)
        try:
            self._connection.execute(f"SELECT 1 FROM {self._quoted_fqn(fqn)} LIMIT 0")
            return True
        except Exception:
            return False

    def schema_exists(self, catalog: str | None, schema: str) -> bool:
        self._reject_catalog(catalog, "schema_exists")
        rows = self._fetch_with_params(
            f"SELECT {self._quoted_ident('schema_name')} "
            f"FROM {self._quoted_fqn('information_schema.schemata')} "
            f"WHERE {self._quoted_ident('schema_name')} = ?",
            [schema],
        )
        return bool(rows)

    def ensure_schema_exists(self, schema: str) -> None:
        self.execute_sql(f"CREATE SCHEMA IF NOT EXISTS {self._quoted_ident(schema)}")

    def list_tables(self, schema: str, catalog: str | None = None) -> list[str]:
        self._reject_catalog(catalog, "list_tables")
        rows = self._fetch_with_params(
            f"SELECT {self._quoted_ident('table_name')} "
            f"FROM {self._quoted_fqn('information_schema.tables')} "
            f"WHERE {self._quoted_ident('table_schema')} = ? "
            f"ORDER BY {self._quoted_ident('table_name')}",
            [schema],
        )
        return [row["table_name"] for row in rows]

    def list_columns(self, fqn: str) -> list[str]:
        try:
            cursor = self._connection.execute(
                f"SELECT * FROM {self._quoted_fqn(fqn)} LIMIT 0"
            )
        except Exception as exc:
            raise DuckDBAdapterError(
                f"Adapter 'duckdb' cannot resolve columns for table {fqn!r}; "
                "the table does not exist or is not readable."
            ) from exc
        return [column[0] for column in (cursor.description or [])]

    def list_relation_columns(self, relation: str) -> list[str]:
        try:
            cursor = self._connection.execute(f"SELECT * FROM {relation} LIMIT 0")
        except Exception as exc:
            # A quoted FQN reads badly under repr ('"gold"."orders"'), so the
            # unquoted form is appended — but only when it actually differs.
            # A file relation carries no double quotes, and printing it twice
            # made the operator hunt for a difference that was never there.
            unquoted = relation.replace('"', "")
            alias = "" if unquoted == relation else f" ({unquoted})"
            raise DuckDBAdapterError(
                f"Adapter 'duckdb' cannot resolve columns for relation "
                f"{relation!r}{alias}; "
                "the relation does not exist or is not readable."
            ) from exc
        return [column[0] for column in (cursor.description or [])]

    def list_schemas(self, catalog: str | None = None) -> list[str]:
        self._reject_catalog(catalog, "list_schemas")
        rows = self.fetch(
            f"SELECT {self._quoted_ident('schema_name')} "
            f"FROM {self._quoted_fqn('information_schema.schemata')} "
            f"ORDER BY {self._quoted_ident('schema_name')}"
        )
        return [row["schema_name"] for row in rows]

    def list_column_types(self, fqn: str) -> dict[str, str]:
        try:
            cursor = self._connection.execute(
                f"SELECT * FROM {self._quoted_fqn(fqn)} LIMIT 0"
            )
        except Exception as exc:
            raise DuckDBAdapterError(
                f"Adapter 'duckdb' cannot resolve column types for table {fqn!r}; "
                "the table does not exist or is not readable."
            ) from exc
        return {column[0]: str(column[1]) for column in (cursor.description or [])}

    def build_fqn(self, catalog: str | None, schema: str, table: str) -> str:
        self._reject_catalog(catalog, "build_fqn")
        # Return the logical two-part name. Every statement that consumes it
        # quotes both components immediately before execution.
        return f"{schema}.{table}"

    def drop_table(self, fqn: str) -> None:
        self.execute_sql(f"DROP TABLE IF EXISTS {self._quoted_fqn(fqn)}")

    def clone_table(
        self,
        src_catalog: str | None,
        src_schema: str,
        src_table: str,
        tgt_catalog: str | None,
        tgt_schema: str,
        tgt_table: str,
    ) -> None:
        source = self.build_fqn(src_catalog, src_schema, src_table)
        target = self.build_fqn(tgt_catalog, tgt_schema, tgt_table)
        self.execute_sql(
            f"CREATE OR REPLACE TABLE {self._quoted_fqn(target)} AS "
            f"SELECT * FROM {self._quoted_fqn(source)}"
        )

    def register_temp_view(self, df: Any, name: str) -> None:
        try:
            self._connection.register(name, df)
        except Exception as exc:
            raise DuckDBAdapterError(
                f"Adapter 'duckdb' could not register temporary view {name!r}."
            ) from exc

    def get_current_user(self) -> str | None:
        rows = self.fetch(f"SELECT current_user AS {self._quoted_ident('user_name')}")
        return rows[0]["user_name"] if rows else None
