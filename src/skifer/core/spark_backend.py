"""
SparkBackend — the PySpark/Databricks runtime backend of Skifer.
Encapsulates all PySpark-specific code for use with SkiferEngine.
"""
from __future__ import annotations
import logging
import time
from functools import reduce
from typing import Any
from uuid import uuid4

from pyspark.sql import functions as F
from pyspark.sql import Window

from skifer.core.writer import (
    write_dataframe,
    write_dataframe_local,
    write_stream_dataframe_local,
    drop_table_if_exists,
    ensure_schema_exists,
)
from skifer.core.environment import (
    DATABRICKS_SDK_INSTALL_HINT,
    get_workspace_client,
    is_databricks_sdk_available,
)
from skifer.core.constants import VALID_SOURCE_TYPES, VALID_STREAMING_SOURCE_TYPES

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Dispatch tables (plan17-3.2)
# ---------------------------------------------------------------------------

def _schema_part(fqn: str) -> str:
    """Return the schema of a 2- or 3-part FQN, quoted or not."""
    parts = [part.strip("`") for part in fqn.replace("`.`", ".").strip("`").split(".")]
    if len(parts) not in (2, 3):
        raise ValueError(f"Cannot determine the schema of '{fqn}'.")
    return parts[-2]


def _smart_lit_spark(val_str: str):
    try:
        if "." in str(val_str):
            return F.lit(float(val_str))
        return F.lit(int(val_str))
    except (ValueError, TypeError):
        return F.lit(val_str)


def _isin_list(val):
    if isinstance(val, list):
        return val
    out = []
    for v in str(val).replace(";", ",").split(","):
        stripped = v.strip()
        if stripped != v:
            logger.warning(
                "Filter value '%s' has leading/trailing space. "
                "If the value contains a comma, use the dict form instead.", v,
            )
        out.append(stripped)
    return out


def _between_cond(c, f, negate: bool):
    op_name = "not_between" if negate else "between"
    vals = list(f.value) if isinstance(f.value, (list, tuple)) else _isin_list(f.value)
    if len(vals) != 2:
        raise ValueError(
            f"Filter on column '{f.column}': {op_name} operator expects exactly 2 values "
            f"(lo,hi); got {len(vals)}."
        )
    lo, hi = vals
    return ((c < lo) | (c > hi)) if negate else ((c >= lo) & (c <= hi))


# Filter dispatch: (c: Column, f: ParsedFilter) -> Column (boolean)
_SPARK_FILTER_DISPATCH: dict = {
    "is_not_null": lambda c, f: c.isNotNull(),
    "is_null":     lambda c, f: c.isNull(),
    "equals":      lambda c, f: c == f.value,
    "not_equals":  lambda c, f: c != f.value,
    "greater_than": lambda c, f: c > f.value,
    "less_than":    lambda c, f: c < f.value,
    "greater_than_equal": lambda c, f: c >= f.value,
    "less_than_equal":    lambda c, f: c <= f.value,
    "contains":    lambda c, f: c.like(f"%{f.value}%"),
    "not_contains": lambda c, f: ~c.like(f"%{f.value}%"),
    "starts_with": lambda c, f: c.like(f"{f.value}%"),
    "ends_with":   lambda c, f: c.like(f"%{f.value}"),
    "like":        lambda c, f: c.like(f.value),
    "not_like":    lambda c, f: ~c.like(f.value),
    "in":          lambda c, f: c.isin(_isin_list(f.value)),
    "not_in":      lambda c, f: ~c.isin(_isin_list(f.value)),
    "between":     lambda c, f: _between_cond(c, f, negate=False),
    "not_between": lambda c, f: _between_cond(c, f, negate=True),
}

# Declared arity -> the number of arguments the handler actually indexes.
_REQUIRED_ARGS: dict = {"single": 1, "two": 2}


def _check_op_arity(name: str, op: Any) -> None:
    """Enforce the arity the op catalog declares, before the handler indexes args.

    The catalog has always carried an `arity` for every column operation, and nothing
    read it: a missing argument surfaced as `IndexError: tuple index out of range`
    raised inside a lambda, naming neither the operation nor the column. The most
    common way to get here is a YAML flow sequence left unquoted, where
    `[substring:1,7]` is two items rather than one operation.
    """
    from skifer.core.op_catalog import COLUMN_OPS

    spec = COLUMN_OPS.get(name)
    required = _REQUIRED_ARGS.get(spec.arity, 0) if spec else 0
    if len(op.args) >= required:
        return

    raise ValueError(
        f"Operation '{name}' needs {required} argument(s) but received "
        f"{len(op.args)}. Expected: {spec.description} "
        "In YAML, quote the whole operation — [\"substring:1,7\"] is one operation, "
        "[substring:1,7] is two items."
    )

# Op dispatch: (c: Column, op: ParsedOp) -> Column (value)
_SPARK_OP_DISPATCH: dict = {
    "cast":      lambda c, op: c.cast("timestamp") if op.args[0].lower() in ("datetime", "timestamp") else c.cast(op.args[0]),
    "upper":     lambda c, op: F.upper(c),
    "lower":     lambda c, op: F.lower(c),
    "trim":      lambda c, op: F.trim(c),
    "round":     lambda c, op: F.round(c, int(op.args[0])),
    "abs":       lambda c, op: F.abs(c),
    "ceil":      lambda c, op: F.ceil(c),
    "length":    lambda c, op: F.length(c),
    "to_date":   lambda c, op: F.to_date(c, op.args[0]),
    "split":     lambda c, op: F.split(c, op.args[0]).getItem(int(op.args[1])),
    "substring": lambda c, op: F.substring(c, int(op.args[0]), int(op.args[1])),
    # Single free-form value ops: rejoin comma-split args so values containing
    # commas survive _parse_op (which splits for split:/substring:).
    "coalesce":  lambda c, op: F.coalesce(c, _smart_lit_spark(",".join(map(str, op.args)))),
    "nvl":       lambda c, op: F.coalesce(c, _smart_lit_spark(",".join(map(str, op.args)))),
    "col":       lambda c, op: F.col(f"`{op.args[0]}`") if op.args else c,
    "lit":       lambda c, op: _smart_lit_spark(",".join(map(str, op.args))),
}

# Aggregate dispatch (Plan 28): (c: Column) -> Column. Keys must stay in sync
# with op_catalog.AGGREGATE_FUNCTIONS — enforced by a drift-guard test.
_SPARK_AGG_DISPATCH: dict = {
    "sum":                   F.sum,
    "avg":                   F.avg,
    "min":                   F.min,
    "max":                   F.max,
    "count":                 F.count,
    "count_distinct":        F.countDistinct,
    "sum_distinct":          F.sum_distinct,
    "approx_count_distinct": F.approx_count_distinct,
    "stddev":                F.stddev,
    "variance":              F.variance,
    "first":                 F.first,
    "last":                  F.last,
}


class SparkBackend:
    """
    Spark/Databricks runtime backend — the single execution backend of the engine.

    Wraps a SparkSession and delegates all platform operations to PySpark APIs.
    This is the default backend used by SkiferEngine when no backend is specified.
    """

    def __init__(self, spark=None, is_local: bool = False):
        """
        Args:
            spark: An existing SparkSession instance.
            is_local (bool): Whether the session is running in local mode.
        """
        self._spark = spark
        self._is_local = is_local
        self._workspace_client_cache = None

        if spark is not None:
            self._apply_connect_patches()

    def _apply_connect_patches(self) -> None:
        """
        Applies Databricks Connect v2 workarounds when needed.

        Two patches are attempted:
        1. ``_patch_debugging`` — pre-populates PySpark's internal
           ``_enable_debugging_cache`` to avoid a failing gRPC call on
           some Windows + PyCharm Connect v2 configurations.
        2. ``_patch_user_context`` — injects the authenticated user's
           email into ``SparkConnectClient._user_id`` when it is absent,
           preventing 'Missing required field UserContext' errors on every
           gRPC execution call (saveAsTable, collect, DDL).

        Both patches are no-ops on standard local PySpark sessions and on
        Databricks notebook sessions.
        """
        self._patch_debugging()
        self._patch_user_context()

    def _patch_debugging(self) -> None:
        """Pre-populate PySpark's debugging cache to avoid a gRPC UserContext error."""
        try:
            self._spark.conf.get("spark.python.sql.dataFrameDebugging.enabled", "false")
        except Exception:
            try:
                import pyspark.errors.utils as _pu
                if getattr(_pu, "_enable_debugging_cache", None) is None:
                    _pu._enable_debugging_cache = False
                    print("   [SparkBackend] ⚙️ Applied PySpark Connect debugging cache patch.")
            except Exception:
                pass

    def _patch_user_context(self) -> None:
        """Inject user_id into SparkConnectClient when it is empty (Connect v2 local bug)."""
        try:
            client = getattr(self._spark, 'client', None)
            if client is None or getattr(client, '_user_id', None):
                return
            w = self._get_workspace_client()
            if w:
                email = w.current_user.me().user_name
                if email:
                    client._user_id = email
                    print(f"   [SparkBackend] ⚙️ Applied UserContext patch: user_id='{email}'.")
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Session / Environment
    # ------------------------------------------------------------------

    @property
    def spark(self):
        """The underlying SparkSession."""
        return self._spark

    @property
    def is_local(self) -> bool:
        return self._is_local

    def _get_workspace_client(self):
        """Return a cached WorkspaceClient, or None if credentials are unavailable."""
        if self._workspace_client_cache is not None:
            return self._workspace_client_cache
        client = get_workspace_client()
        self._workspace_client_cache = client
        return client

    def execute_sql(self, sql: str) -> Any:
        return self._spark.sql(sql)

    def sql(self, query: str) -> Any:
        return self._spark.sql(query)

    def _append_certification(self, schema: str, table: str, row: dict, key: str) -> None:
        from skifer.core.sql_compiler import escape_sql_string, quote_ident
        qualified = f"{quote_ident(schema)}.{quote_ident(table)}"
        plain = f"{schema}.{table}"
        self._spark.sql(f"CREATE SCHEMA IF NOT EXISTS {quote_ident(schema)}")
        if self._spark.catalog.tableExists(plain):
            escaped = escape_sql_string(row[key])
            if self._spark.sql(f"SELECT 1 FROM {qualified} WHERE {quote_ident(key)} = '{escaped}' LIMIT 1").collect():
                return
        self._spark.createDataFrame([row]).write.format("delta").mode("append").saveAsTable(plain)

    def append_certification_contract(self, schema: str, row: dict) -> None:
        self._append_certification(schema, "contract_definitions", row, "definition_hash")

    def get_certification_contract(
        self, schema: str, contract_id: str, version: str
    ) -> dict | None:
        from skifer.core.sql_compiler import escape_sql_string, quote_ident
        plain = f"{schema}.contract_definitions"
        if not self._spark.catalog.tableExists(plain):
            return None
        escaped_id = escape_sql_string(contract_id)
        escaped_version = escape_sql_string(version)
        rows = self._spark.sql(
            f"SELECT * FROM {quote_ident(schema)}.{quote_ident('contract_definitions')} "
            f"WHERE {quote_ident('contract_id')} = '{escaped_id}' "
            f"AND {quote_ident('contract_version')} = '{escaped_version}' LIMIT 2"
        ).collect()
        if len(rows) > 1:
            raise ValueError(
                f"Contract '{contract_id}' version '{version}' has ambiguous definitions."
            )
        return rows[0].asDict(recursive=True) if rows else None

    def append_certification_run(self, schema: str, row: dict) -> None:
        self._append_certification(schema, "materialization_runs", row, "event_id")

    def append_certification_check(self, schema: str, row: dict) -> None:
        self._append_certification(schema, "check_results", row, "event_id")

    def get_certification_run(self, schema: str, run_id: str) -> dict | None:
        from skifer.core.sql_compiler import escape_sql_string, quote_ident
        plain = f"{schema}.materialization_runs"
        if not self._spark.catalog.tableExists(plain):
            return None
        escaped = escape_sql_string(run_id)
        row = self._spark.sql(
            f"SELECT * FROM {quote_ident(schema)}.{quote_ident('materialization_runs')} "
            f"WHERE {quote_ident('run_id')} = '{escaped}' ORDER BY {quote_ident('occurred_at')} DESC LIMIT 1"
        ).first()
        return row.asDict(recursive=True) if row else None

    def get_latest_certification_promotion(self, schema: str, dataset: str) -> dict | None:
        from skifer.core.sql_compiler import escape_sql_string, quote_ident
        plain = f"{schema}.materialization_runs"
        if not self._spark.catalog.tableExists(plain):
            return None
        escaped = escape_sql_string(dataset)
        row = self._spark.sql(
            f"SELECT * FROM {quote_ident(schema)}.{quote_ident('materialization_runs')} "
            f"WHERE {quote_ident('dataset')} = '{escaped}' AND {quote_ident('state')} = 'PROMOTED' "
            f"ORDER BY {quote_ident('occurred_at')} DESC LIMIT 1"
        ).first()
        return row.asDict(recursive=True) if row else None

    def list_certification_history(self, schema: str, dataset: str, limit: int = 50) -> list[dict]:
        from skifer.core.sql_compiler import escape_sql_string, quote_ident
        plain = f"{schema}.materialization_runs"
        if not self._spark.catalog.tableExists(plain):
            return []
        escaped = escape_sql_string(dataset)
        rows = self._spark.sql(
            f"SELECT * FROM {quote_ident(schema)}.{quote_ident('materialization_runs')} "
            f"WHERE {quote_ident('dataset')} = '{escaped}' "
            f"ORDER BY {quote_ident('occurred_at')} DESC LIMIT {int(limit)}"
        ).collect()
        return [row.asDict(recursive=True) for row in rows]

    def get_certification_check_results(self, schema: str, run_id: str) -> list[dict]:
        from skifer.core.sql_compiler import escape_sql_string, quote_ident
        plain = f"{schema}.check_results"
        if not self._spark.catalog.tableExists(plain):
            return []
        escaped = escape_sql_string(run_id)
        rows = self._spark.sql(
            f"SELECT * FROM {quote_ident(schema)}.{quote_ident('check_results')} "
            f"WHERE {quote_ident('run_id')} = '{escaped}'"
        ).collect()
        return [row.asDict(recursive=True) for row in rows]

    def upsert_incident(self, schema: str, row: dict) -> None:
        from skifer.core.sql_compiler import quote_ident

        plain = f"{schema}.incidents"
        qualified = f"{quote_ident(schema)}.{quote_ident('incidents')}"
        self._spark.sql(f"CREATE SCHEMA IF NOT EXISTS {quote_ident(schema)}")
        schema_ddl = (
            "id STRING, target_fqn STRING, run_id STRING, check_name STRING, "
            "severity STRING, status STRING, opened_at STRING, assignee STRING, "
            "root_cause STRING, resolved_at STRING"
        )
        if not self._spark.catalog.tableExists(plain):
            self._spark.createDataFrame([row], schema=schema_ddl).write.format(
                "delta"
            ).saveAsTable(plain)
            return
        view_name = "_skifer_incident_src"
        self._spark.createDataFrame([row], schema=schema_ddl).createOrReplaceTempView(
            view_name
        )
        assignments = ", ".join(
            f"t.{quote_ident(key)} = s.{quote_ident(key)}" for key in row
        )
        self._spark.sql(
            f"MERGE INTO {qualified} t USING {view_name} s ON t.id = s.id "
            f"WHEN MATCHED THEN UPDATE SET {assignments} "
            "WHEN NOT MATCHED THEN INSERT *"
        )

    def get_open_incident(
        self, schema: str, target_fqn: str, check_name: str
    ) -> dict | None:
        from skifer.core.sql_compiler import escape_sql_string, quote_ident

        plain = f"{schema}.incidents"
        if not self._spark.catalog.tableExists(plain):
            return None
        escaped_target = escape_sql_string(target_fqn)
        escaped_check = escape_sql_string(check_name)
        row = self._spark.sql(
            f"SELECT * FROM {quote_ident(schema)}.{quote_ident('incidents')} "
            f"WHERE {quote_ident('target_fqn')} = '{escaped_target}' "
            f"AND {quote_ident('check_name')} = '{escaped_check}' "
            f"AND {quote_ident('status')} != 'RESOLVED' "
            f"ORDER BY {quote_ident('opened_at')} DESC LIMIT 1"
        ).first()
        return row.asDict(recursive=True) if row else None

    def list_open_incidents(self, schema: str, target_fqn: str) -> list[dict]:
        from skifer.core.sql_compiler import escape_sql_string, quote_ident

        plain = f"{schema}.incidents"
        if not self._spark.catalog.tableExists(plain):
            return []
        escaped = escape_sql_string(target_fqn)
        rows = self._spark.sql(
            f"SELECT * FROM {quote_ident(schema)}.{quote_ident('incidents')} "
            f"WHERE {quote_ident('target_fqn')} = '{escaped}' "
            f"AND {quote_ident('status')} != 'RESOLVED' "
            f"ORDER BY {quote_ident('opened_at')} DESC"
        ).collect()
        return [row.asDict(recursive=True) for row in rows]

    def get_incident(self, schema: str, incident_id: str) -> dict | None:
        from skifer.core.sql_compiler import escape_sql_string, quote_ident

        plain = f"{schema}.incidents"
        if not self._spark.catalog.tableExists(plain):
            return None
        escaped = escape_sql_string(incident_id)
        row = self._spark.sql(
            f"SELECT * FROM {quote_ident(schema)}.{quote_ident('incidents')} "
            f"WHERE {quote_ident('id')} = '{escaped}' LIMIT 1"
        ).first()
        return row.asDict(recursive=True) if row else None

    def list_incidents(
        self,
        schema: str,
        *,
        status: str | None = None,
        target_fqn: str | None = None,
        limit: int = 50,
    ) -> list[dict]:
        from skifer.core.sql_compiler import escape_sql_string, quote_ident

        plain = f"{schema}.incidents"
        if not self._spark.catalog.tableExists(plain):
            return []
        clauses = []
        if status is not None:
            clauses.append(
                f"{quote_ident('status')} = '{escape_sql_string(status)}'"
            )
        if target_fqn is not None:
            clauses.append(
                f"{quote_ident('target_fqn')} = '{escape_sql_string(target_fqn)}'"
            )
        query = f"SELECT * FROM {quote_ident(schema)}.{quote_ident('incidents')}"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += f" ORDER BY {quote_ident('opened_at')} DESC LIMIT {int(limit)}"
        rows = self._spark.sql(query).collect()
        return [row.asDict(recursive=True) for row in rows]

    def append_semantic_usage_event(self, schema: str, row: dict) -> None:
        """Append one idempotent, value-free adaptive usage event to Delta."""
        from skifer.core.sql_compiler import escape_sql_string, quote_ident

        table = "semantic_usage_events"
        plain = f"{schema}.{table}"
        qualified = f"{quote_ident(schema)}.{quote_ident(table)}"
        self._spark.sql(f"CREATE SCHEMA IF NOT EXISTS {quote_ident(schema)}")
        if self._spark.catalog.tableExists(plain):
            event_id = escape_sql_string(row["event_id"])
            existing = self._spark.sql(
                f"SELECT 1 FROM {qualified} WHERE {quote_ident('event_id')} = "
                f"'{event_id}' LIMIT 1"
            ).collect()
            if existing:
                return
        schema_ddl = (
            "event_id STRING, occurred_at STRING, environment STRING, "
            "consumer_class STRING, model_hashes STRING, metric_ids STRING, "
            "dimension_ids STRING, normalized_filter_shape STRING, "
            "query_fingerprint STRING, duration_ms BIGINT, rows_returned BIGINT, "
            "bytes_scanned BIGINT, status STRING"
        )
        self._spark.createDataFrame([row], schema=schema_ddl).write.format(
            "delta"
        ).mode("append").saveAsTable(plain)

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
        """Read adaptive events without triggering any aggregate or Spark count."""
        from skifer.core.sql_compiler import escape_sql_string, quote_ident

        table = "semantic_usage_events"
        plain = f"{schema}.{table}"
        if not self._spark.catalog.tableExists(plain):
            return []
        clauses = []
        for column, value, operator in (
            ("environment", environment, "="),
            ("consumer_class", consumer_class, "="),
            ("occurred_at", since, ">="),
            ("occurred_at", until, "<="),
        ):
            if value is not None:
                clauses.append(
                    f"{quote_ident(column)} {operator} '{escape_sql_string(value)}'"
                )
        qualified = f"{quote_ident(schema)}.{quote_ident(table)}"
        query = f"SELECT * FROM {qualified}"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += f" ORDER BY {quote_ident('occurred_at')} DESC, {quote_ident('event_id')} DESC"
        if limit is not None:
            query += f" LIMIT {int(limit)}"
        rows = self._spark.sql(query).collect()
        return [row.asDict(recursive=True) for row in rows]

    def check_catalog_access(self, catalog: str, timeout: int = 30) -> bool:
        if not catalog:
            return True
        # Run Spark call in a thread with a timeout to avoid blocking init indefinitely
        import threading
        result = [False]
        exc = [None]

        def _check():
            try:
                self._spark.sql(f"SHOW SCHEMAS IN `{catalog}`").limit(1).collect()
                result[0] = True
            except Exception as e:
                exc[0] = e

        t = threading.Thread(target=_check, daemon=True)
        t.start()
        t.join(timeout)
        if result[0]:
            return True
        if t.is_alive():
            print(f"      ⚠️  Catalog check timed out after {timeout}s — falling back to SDK check.")
        # Fallback: SDK catalog check (no Spark, pure REST)
        try:
            w = self._get_workspace_client()
            if w:
                w.catalogs.get(catalog)
                return True
        except Exception:
            pass
        return False

    def get_current_user(self) -> str | None:
        try:
            rows = self._spark.sql("SELECT current_user()").collect()
            if rows and rows[0][0]:
                return rows[0][0]
        except Exception:
            pass
        try:
            w = self._get_workspace_client()
            if w:
                return w.current_user.me().user_name
        except Exception:
            pass
        return None

    # ------------------------------------------------------------------
    # Table I/O
    # ------------------------------------------------------------------

    def read_table(self, fqn: str) -> Any:
        return self._spark.table(fqn)

    def read_source(self, source_type: str, path: str, options: dict | None = None) -> Any:
        """
        Read an external file source via spark.read.format(source_type).load(path).

        Args:
            source_type (str): Spark format — one of csv, parquet, json, avro, orc, delta, text.
            path (str): File path or glob supported by the active Spark session.
            options (dict | None): spark.read options (e.g. {"header": "true", "inferSchema": "true"}).

        Returns:
            DataFrame

        Raises:
            ValueError: If source_type is not in the supported list.
        """
        if source_type not in VALID_SOURCE_TYPES:
            raise ValueError(
                f"[read_source] Unknown source type '{source_type}'. "
                f"Valid types: {sorted(VALID_SOURCE_TYPES)}"
            )
        reader = self._spark.read.format(source_type)
        if options:
            reader = reader.options(**options)
        return reader.load(path)

    def write_table(self, df: Any, fqn: str, mode: str = "overwrite") -> None:
        if getattr(df, "isStreaming", False):
            raise ValueError(
                f"[write_table] Cannot batch-write a streaming DataFrame to '{fqn}'. "
                "Declare 'materialization: streaming_table' in the schema so the "
                "engine uses the streaming write path."
            )
        label = fqn.replace("`", "").split(".")[-1]
        write_dataframe(df, fqn, label, self._is_local, self._spark)

    def write_staging(self, df: Any, fqn: str) -> None:
        """Write one exact certified-publication staging table.

        The staging schema is created on demand. Only the *target* schema is
        ensured by the engine, so the very first certified publication in a fresh
        environment failed on a missing ``_skifer_staging`` — the staging area
        is an implementation detail of publication, and nobody else creates it.
        """
        self.ensure_schema_exists(_schema_part(fqn))
        self.write_table(df, fqn, mode="overwrite")

    def read_staging(self, fqn: str) -> Any:
        return self.read_table(fqn)

    def tag_row_violations(
        self,
        df: Any,
        predicates: dict[str, str],
        run_id: str,
        contract_version: str,
    ) -> Any:
        """Annotate a staged DataFrame with row-level quarantine metadata."""
        from skifer.core.sql_compiler import escape_sql_string, quote_ident

        view_name = f"_skifer_quarantine_{uuid4().hex}"
        self.register_temp_view(df, view_name)
        cases = []
        for label, predicate in predicates.items():
            escaped_label = escape_sql_string(label)
            cases.append(f"CASE WHEN ({predicate}) THEN '{escaped_label}' END")
        violations = f"concat_ws(',', {', '.join(cases)})" if cases else "''"
        escaped_run_id = escape_sql_string(run_id)
        escaped_contract_version = escape_sql_string(contract_version)
        return self._spark.sql(
            "SELECT *, "
            f"{violations} AS {quote_ident('_violations')}, "
            f"'{escaped_run_id}' AS {quote_ident('_run_id')}, "
            f"'{escaped_contract_version}' AS {quote_ident('_contract_version')} "
            f"FROM {quote_ident(view_name)}"
        )

    def drop_staging(self, fqn: str) -> None:
        self.drop_table(fqn)

    # ------------------------------------------------------------------
    # Streaming I/O (Plan 27)
    # ------------------------------------------------------------------

    def read_table_stream(self, fqn: str) -> Any:
        """Read a catalog table as a streaming DataFrame (spark.readStream.table)."""
        return self._spark.readStream.table(fqn)

    def read_source_stream(self, source_type: str, path: str, options: dict | None = None) -> Any:
        """
        Read an external file source as a stream via spark.readStream.

        Only self-describing formats are supported (delta: schema in the log,
        text: fixed ``value`` column) — csv/json/parquet/orc/avro need an
        explicit schema, which the YAML surface does not model yet.
        """
        if source_type not in VALID_STREAMING_SOURCE_TYPES:
            raise ValueError(
                f"[read_source_stream] Source type '{source_type}' cannot be read as a "
                f"stream without an explicit schema. Valid streaming source types: "
                f"{sorted(VALID_STREAMING_SOURCE_TYPES)}"
            )
        reader = self._spark.readStream.format(source_type)
        if options:
            reader = reader.options(**options)
        return reader.load(path)

    def default_checkpoint_root(self) -> str | None:
        """
        Root directory for ``checkpoint: auto`` resolution.

        Local mode: ``{spark.sql.warehouse.dir}/_checkpoints``.
        Databricks: None — the engine requires an explicit checkpoint or the
        ``checkpoint_base`` environment param (fail-fast upstream).
        """
        if not self._is_local:
            return None
        warehouse = self._spark.conf.get("spark.sql.warehouse.dir")
        return f"{warehouse.rstrip('/')}/_checkpoints"

    def write_stream_table(
        self,
        df: Any,
        fqn: str,
        checkpoint_location: str,
        trigger: str = "available_now",
        write_mode: str = "append",
        keys: list[str] | None = None,
    ) -> None:
        """
        Start a streaming write to a Delta table and wait for completion.

        write_mode ``append``: plain incremental append.
        write_mode ``upsert`` (CDC Type 1): ``foreachBatch`` — each micro-batch
        is deduplicated on ``keys`` then MERGE'd into the target (matched →
        UPDATE, not matched → INSERT). Uniqueness across batches is guaranteed
        by the target itself (no streaming state) and replayed micro-batches
        are idempotent.

        trigger: ``available_now`` (incremental batch — returns after the
        backfill) or ``interval:<duration>`` (permanent streaming query —
        blocks; monitor hooks are never reached, by design).
        """
        if not getattr(df, "isStreaming", False):
            raise ValueError(
                f"[write_stream_table] DataFrame for '{fqn}' is not streaming — "
                "use write_table for batch writes."
            )
        if write_mode not in ("append", "upsert"):
            raise ValueError(
                f"[write_stream_table] Invalid write_mode '{write_mode}'. "
                "Valid write modes: ['append', 'upsert']."
            )
        if write_mode == "upsert" and not keys:
            raise ValueError(
                f"[write_stream_table] write_mode 'upsert' for '{fqn}' requires "
                "non-empty merge keys."
            )
        try:
            if write_mode == "upsert":
                writer = (
                    df.writeStream.foreachBatch(self._make_upsert_batch_fn(fqn, keys))
                    .option("checkpointLocation", checkpoint_location)
                )
                writer = self._apply_trigger(writer, trigger)
                writer.start().awaitTermination()
            else:
                writer = (
                    df.writeStream.format("delta")
                    .outputMode("append")
                    .option("checkpointLocation", checkpoint_location)
                )
                writer = self._apply_trigger(writer, trigger)
                if self._is_local:
                    # Starts at the warehouse path, awaits termination, then registers
                    # the table in the metastore (needs the _delta_log to exist).
                    write_stream_dataframe_local(writer, fqn, self._spark)
                else:
                    writer.toTable(fqn).awaitTermination()
        except Exception as exc:  # noqa: BLE001 — enrich, never swallow (data-loss risk)
            if "writeStream" in str(exc) or "not supported" in str(exc).lower():
                raise RuntimeError(
                    f"[write_stream_table] Streaming write to '{fqn}' failed — this "
                    "databricks-connect version may not support writeStream. Run as a "
                    "Databricks job or upgrade databricks-connect. Original error: "
                    f"{exc}"
                ) from exc
            raise

    def _make_upsert_batch_fn(self, fqn: str, keys: list[str]):
        """Build the foreachBatch callback for CDC Type 1 upsert into ``fqn``."""
        is_local = self._is_local
        clean_fqn = fqn.replace("`", "")
        view_name = "_skifer_upsert_src_" + clean_fqn.replace(".", "_")

        def _merge_batch(batch_df: Any, batch_id: int) -> None:
            spark = batch_df.sparkSession
            # Dedup WITHIN the micro-batch (batch op — legal, bounded state).
            deduped = batch_df.dropDuplicates(keys)

            if not spark.catalog.tableExists(clean_fqn):
                # First micro-batch: create the target.
                if is_local:
                    write_dataframe_local(deduped, fqn, spark)
                else:
                    deduped.write.format("delta").saveAsTable(fqn)
                return

            deduped.createOrReplaceTempView(view_name)
            on_clause = " AND ".join(f"t.`{k}` = s.`{k}`" for k in keys)
            spark.sql(
                f"MERGE INTO {fqn} AS t USING {view_name} AS s ON {on_clause} "
                "WHEN MATCHED THEN UPDATE SET * "
                "WHEN NOT MATCHED THEN INSERT *"
            )

        return _merge_batch

    @staticmethod
    def _apply_trigger(writer: Any, trigger: str) -> Any:
        """Map the YAML trigger grammar onto writeStream.trigger()."""
        if trigger == "available_now":
            return writer.trigger(availableNow=True)
        if trigger.startswith("interval:"):
            return writer.trigger(processingTime=trigger[len("interval:"):].strip())
        raise ValueError(
            f"[write_stream_table] Invalid trigger '{trigger}'. "
            "Valid triggers: 'available_now' or 'interval:<duration>'."
        )

    # ------------------------------------------------------------------
    # Materialized views (Plan 28)
    # ------------------------------------------------------------------

    #: Poll cadence and ceiling for a warehouse statement. Creating a materialized
    #: view starts a serverless pipeline, which routinely takes several minutes.
    MV_POLL_INTERVAL_SECONDS: float = 5.0
    MV_POLL_TIMEOUT_SECONDS: float = 1800.0

    def execute_sql_on_warehouse(self, sql: str, warehouse_id: str) -> list[list]:
        """
        Run a statement on a SQL warehouse via the Statement Execution API.

        ``CREATE MATERIALIZED VIEW`` is rejected by all-purpose clusters and by
        Databricks Connect, so materialized-view DDL never goes through
        ``spark.sql`` on Databricks — it goes here.

        Returns:
            The result rows as a list of lists (empty for pure DDL).

        Raises:
            RuntimeError: On any SDK/statement failure, enriched with the target
                          warehouse. Never swallowed: a materialized view that is
                          silently not created is a data-loss bug (Plan 27 #7).
        """
        w = self._get_workspace_client()
        if w is None:
            reason = (
                DATABRICKS_SDK_INSTALL_HINT
                if not is_databricks_sdk_available()
                else "Configure workspace credentials (DATABRICKS_HOST / DATABRICKS_TOKEN)."
            )
            raise RuntimeError(
                f"[materialized_view] No Databricks workspace client available to reach "
                f"warehouse '{warehouse_id}'. {reason}"
            )
        try:
            response = w.statement_execution.execute_statement(
                statement=sql, warehouse_id=warehouse_id, wait_timeout="30s",
            )
            return self._await_statement(w, response, warehouse_id)
        except RuntimeError:
            raise
        except Exception as exc:  # noqa: BLE001 — enrich, never swallow
            raise RuntimeError(
                f"[materialized_view] Statement execution failed on warehouse "
                f"'{warehouse_id}': {exc}\nStatement:\n{sql}"
            ) from exc

    def _await_statement(self, w: Any, response: Any, warehouse_id: str) -> list[list]:
        """Poll a statement until a terminal state; raise on anything but SUCCEEDED."""
        deadline = time.monotonic() + self.MV_POLL_TIMEOUT_SECONDS
        statement_id = getattr(response, "statement_id", None)

        while True:
            status = getattr(response, "status", None)
            state = getattr(status, "state", None)
            name = str(getattr(state, "value", state) or "").upper()

            if name in ("PENDING", "RUNNING"):
                if time.monotonic() > deadline:
                    raise RuntimeError(
                        f"[materialized_view] Statement {statement_id} still {name} after "
                        f"{self.MV_POLL_TIMEOUT_SECONDS:.0f}s on warehouse '{warehouse_id}'. "
                        "Creating a materialized view starts a serverless pipeline — check "
                        "its progress in the Databricks UI before re-running."
                    )
                logger.info("   -> [MV] Statement %s is %s — waiting...", statement_id, name)
                time.sleep(self.MV_POLL_INTERVAL_SECONDS)
                response = w.statement_execution.get_statement(statement_id)
                continue

            if name in ("SUCCEEDED", ""):
                result = getattr(response, "result", None)
                return list(getattr(result, "data_array", None) or [])

            error = getattr(status, "error", None)
            message = getattr(error, "message", None) or str(error or "no error detail")
            raise RuntimeError(
                f"[materialized_view] Statement {statement_id} ended in state {name} on "
                f"warehouse '{warehouse_id}': {message}"
            )

    def _run_ddl(self, sql: str, warehouse_id: str | None) -> list[list]:
        """Route a statement to the warehouse when configured, else to the session."""
        if warehouse_id:
            return self.execute_sql_on_warehouse(sql, warehouse_id)
        rows = self._spark.sql(sql).collect()
        return [list(r) for r in rows]

    def create_materialized_view(self, ddl: str, warehouse_id: str | None = None) -> None:
        """Execute a ``CREATE [OR REPLACE] MATERIALIZED VIEW`` statement."""
        self._run_ddl(ddl, warehouse_id)

    def refresh_materialized_view(self, fqn: str, warehouse_id: str | None = None) -> None:
        """Trigger a refresh of an existing materialized view."""
        self._run_ddl(f"REFRESH MATERIALIZED VIEW {fqn}", warehouse_id)

    def drop_materialized_view(self, fqn: str, warehouse_id: str | None = None) -> None:
        """Drop a materialized view if it exists."""
        self._run_ddl(f"DROP MATERIALIZED VIEW IF EXISTS {fqn}", warehouse_id)

    def get_table_property(
        self, fqn: str, key: str, warehouse_id: str | None = None
    ) -> str | None:
        """
        Read one TBLPROPERTIES entry, or None when unreadable.

        Returning None on failure is deliberate: the caller then falls back to
        ``CREATE OR REPLACE``, which is more expensive but never leaves a stale
        definition in place.
        """
        try:
            rows = self._run_ddl(f"SHOW TBLPROPERTIES {fqn}", warehouse_id)
        except Exception as exc:  # noqa: BLE001 — absence is a normal outcome here
            logger.debug("[MV] Could not read properties of %s: %s", fqn, exc)
            return None
        for row in rows:
            values = list(row)
            if len(values) >= 2 and str(values[0]) == key:
                return str(values[1])
        return None

    def register_temp_view(self, df: Any, name: str) -> None:
        df.createOrReplaceTempView(name)

    def drop_table(self, fqn: str) -> None:
        drop_table_if_exists(fqn, self._spark, self._get_workspace_client)

    def ensure_schema_exists(self, schema: str) -> None:
        ensure_schema_exists(schema, self._is_local, self._spark)

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    def schema_exists(self, catalog: str | None, schema: str) -> bool:
        try:
            # Local spark_catalog only supports single-part namespaces
            if catalog and not self._is_local:
                self._spark.sql(f"SHOW TABLES IN `{catalog}`.`{schema}`").limit(1).collect()
            else:
                self._spark.sql(f"SHOW TABLES IN `{schema}`").limit(1).collect()
            return True
        except Exception:
            return False

    def table_exists(self, catalog: str | None, schema: str, table: str) -> bool:
        try:
            if catalog and not self._is_local:
                schema_fqn = f"`{catalog}`.`{schema}`"
            else:
                schema_fqn = f"`{schema}`"
            pattern = table.replace("\\", "\\\\").replace("'", "\\'")
            rows = self._spark.sql(f"SHOW TABLES IN {schema_fqn} LIKE '{pattern}'").collect()
            return any(row["tableName"].casefold() == table.casefold() for row in rows)
        except Exception:
            # Catalog lookup failed — also check session-scoped temp views
            try:
                return self._spark.catalog.tableExists(table)
            except Exception:
                return False

    def list_tables(self, schema: str, catalog: str | None = None) -> list[str]:
        """List tables in a schema using SHOW TABLES IN."""
        try:
            if catalog:
                rows = self._spark.sql(f"SHOW TABLES IN `{catalog}`.`{schema}`").collect()
            else:
                rows = self._spark.sql(f"SHOW TABLES IN `{schema}`").collect()
            return [r["tableName"] for r in rows]
        except Exception:
            return []

    def list_columns(self, fqn: str) -> list[str]:
        """List column names for a fully-qualified table using SHOW COLUMNS IN."""
        try:
            rows = self._spark.sql(f"SHOW COLUMNS IN {fqn}").collect()
            return [r["col_name"] for r in rows]
        except Exception:
            return []

    def list_schemas(self, catalog: str | None = None) -> list[str]:
        """List schemas using SHOW SCHEMAS (best-effort)."""
        try:
            rows = self._spark.sql("SHOW SCHEMAS").collect()
            return [r[0] for r in rows]
        except Exception:
            return []

    def list_column_types(self, fqn: str) -> dict[str, str]:
        """Return {col_name: data_type} for a table using DESCRIBE TABLE (best-effort)."""
        try:
            rows = self._spark.sql(f"DESCRIBE TABLE {fqn}").collect()
            return {
                r["col_name"]: r["data_type"]
                for r in rows
                if r["col_name"] and not r["col_name"].startswith("#")
            }
        except Exception:
            return {}

    # ------------------------------------------------------------------
    # FQN Building
    # ------------------------------------------------------------------

    def build_fqn(self, catalog: str | None, schema: str, table: str) -> str:
        if catalog:
            return f"`{catalog}`.`{schema}`.`{table}`"
        return f"`{schema}`.`{table}`"

    # ------------------------------------------------------------------
    # Column Factory
    # ------------------------------------------------------------------

    def col(self, name: str) -> Any:
        return F.col(f"`{name}`")

    def lit(self, value: Any) -> Any:
        return F.lit(value)

    def expr(self, sql_expr: str) -> Any:
        return F.expr(sql_expr)

    def lit_true(self) -> Any:
        return F.lit(True)

    # ------------------------------------------------------------------
    # Operations on Columns
    # ------------------------------------------------------------------

    def apply_op(self, c: Any, op: Any, allow_raw_sql: bool = True) -> Any:
        """Apply a ParsedOp to a column via dispatch table (plan17-3.2)."""
        from skifer.core.ir import ParsedOp, _parse_op

        name = op.name
        # _parse_op splits args on commas (for split:/substring:); ops whose
        # argument is a single free-form value need the raw remainder back.
        raw_arg = ",".join(str(a) for a in op.args)

        # when: — evaluate inner condition string against column c
        if name == "when":
            from skifer.core.op_catalog import resolve_filter_operator
            from skifer.core.ir import ParsedFilter as _PF
            inner_str = raw_arg if op.args else "is_not_null"
            parts = inner_str.split(":", 1)
            raw_op_name = parts[0]
            inner_op_name = resolve_filter_operator(raw_op_name) or raw_op_name
            inner_val = parts[1] if len(parts) > 1 else None
            # Evaluate condition against c directly (not via build_filter column lookup)
            handler = _SPARK_FILTER_DISPATCH.get(inner_op_name)
            if handler is None:
                from skifer.core.op_catalog import FILTER_OPERATORS, suggest as _s
                hints = _s(raw_op_name, FILTER_OPERATORS)
                raise ValueError(
                    f"Unknown when: condition '{raw_op_name}'."
                    + (f" Did you mean: {hints}?" if hints else "")
                )
            return handler(c, _PF(column="__", operator=inner_op_name, value=inner_val))

        # then:/else: — unwrap and evaluate inner value op
        if name in ("then", "else"):
            inner = _parse_op(raw_arg) if op.args else ParsedOp("col", ())
            return self.apply_op(c, inner, allow_raw_sql)

        # expr: — governance check
        if name == "expr":
            if not allow_raw_sql:
                raise ValueError(
                    "[Governance] expr: operation is disabled in the current environment "
                    "(allow_raw_sql: false)."
                )
            return F.expr(raw_arg)

        handler = _SPARK_OP_DISPATCH.get(name)
        if handler is None:
            from skifer.core.op_catalog import COLUMN_OPS, suggest as _s
            hints = _s(name, COLUMN_OPS)
            raise ValueError(
                f"Unknown column operation '{name}'."
                + (f" Did you mean: {hints}?" if hints else "")
            )
        _check_op_arity(name, op)
        return handler(c, op)

    def build_filter(self, f: Any, allow_raw_sql: bool = True) -> Any:
        """Build a boolean column expression from a ParsedFilter (plan17-3.2)."""
        from skifer.core.op_catalog import resolve_filter_operator
        c = F.col(f"`{f.column}`")
        canonical_op = resolve_filter_operator(f.operator) or f.operator

        if canonical_op == "sql":
            if not allow_raw_sql:
                raise ValueError(
                    "[Governance] sql: filter operator is disabled in the current environment "
                    "(allow_raw_sql: false)."
                )
            return F.expr(f.value)

        handler = _SPARK_FILTER_DISPATCH.get(canonical_op)
        if handler is None:
            from skifer.core.op_catalog import FILTER_OPERATORS, suggest as _s
            hints = _s(f.operator, FILTER_OPERATORS)
            raise ValueError(
                f"Unknown filter operator '{f.operator}' on column '{f.column}'."
                + (f" Did you mean: {hints}?" if hints else "")
            )
        from skifer.core.ir import ParsedFilter as _PF
        return handler(c, _PF(column=f.column, operator=canonical_op, value=f.value))

    # ------------------------------------------------------------------
    # DataFrame Operations
    # ------------------------------------------------------------------

    def select(self, df: Any, columns: list) -> Any:
        return df.select(columns)

    def filter(self, df: Any, condition: Any) -> Any:
        return df.filter(condition)

    def join(self, left: Any, right: Any, on: Any, how: str = "left") -> Any:
        return left.join(right, on=on, how=how)

    def drop_columns(self, df: Any, columns: list) -> Any:
        return df.drop(*columns)

    def with_column(self, df: Any, name: str, col: Any) -> Any:
        return df.withColumn(name, col)

    def limit(self, df: Any, n: int) -> Any:
        return df.limit(n)

    def union_by_name(self, dfs: list, allow_missing: bool = True) -> Any:
        return reduce(
            lambda x, y: x.unionByName(y, allowMissingColumns=allow_missing),
            dfs,
        )

    def drop_duplicates(self, df: Any, cols: list[str] | None = None) -> Any:
        if cols:
            return df.dropDuplicates(cols)
        return df.dropDuplicates()

    def drop_nulls(self, df: Any, subset: list[str]) -> Any:
        return df.dropna(subset=subset)

    def agg_expr(self, func: str, source: str) -> Any:
        """Build an aggregate Column for a canonical function name (Plan 28).

        ``source`` may be ``"*"`` for ``count`` only (validated at load time).
        """
        handler = _SPARK_AGG_DISPATCH.get(func)
        if handler is None:
            raise ValueError(
                f"[aggregate] Unknown aggregate function '{func}'. "
                f"Valid functions: {sorted(_SPARK_AGG_DISPATCH)}"
            )
        col = F.lit(1) if source == "*" else F.col(f"`{source}`")
        return handler(col)

    def group_by_agg(self, df: Any, keys: list[str], exprs: dict) -> Any:
        """Group by *keys* and apply the ``{target: Column}`` aggregate expressions."""
        agg_cols = [expr.alias(target) for target, expr in exprs.items()]
        return df.groupBy(*[F.col(f"`{k}`") for k in keys]).agg(*agg_cols)

    def cache(self, df: Any) -> Any:
        try:
            return df.cache()
        except Exception:
            print("   [SparkBackend] cache(): best-effort failed, continuing without caching.")
            return df

    def unpersist(self, df: Any) -> None:
        try:
            df.unpersist()
        except Exception:
            pass

    def is_empty(self, df: Any) -> bool:
        try:
            return df.isEmpty()
        except Exception:
            return False

    def count(self, df: Any) -> int:
        return df.count()

    # ------------------------------------------------------------------
    # Window
    # ------------------------------------------------------------------

    def row_number_over(
        self, df: Any, partition_by: list[str], order_by: list[dict]
    ) -> Any:
        """
        Adds a '_rn' column with row_number over the specified window.

        Args:
            df: Input DataFrame.
            partition_by (list[str]): Column names to partition by.
            order_by (list[dict]): List of {"field": col_name, "order": "asc"|"desc"}.

        Returns:
            DataFrame with '_rn' column appended.
        """
        partition_cols = [F.col(c) for c in partition_by]
        order_cols = [
            F.col(o["field"]).desc() if o["order"] == "desc" else F.col(o["field"]).asc()
            for o in order_by
        ]
        window_spec = Window.partitionBy(*partition_cols).orderBy(*order_cols)
        return df.withColumn("_rn", F.row_number().over(window_spec))

    # ------------------------------------------------------------------
    # When / Otherwise
    # ------------------------------------------------------------------

    def when(self, condition: Any, value: Any) -> Any:
        return F.when(condition, value)

    def otherwise(self, col: Any, value: Any) -> Any:
        return col.otherwise(value)

    def when_chain(self, conditions: list, otherwise_val: Any) -> Any:
        expr = None
        for cond, val in conditions:
            expr = F.when(cond, val) if expr is None else expr.when(cond, val)
        return expr.otherwise(otherwise_val) if expr is not None else F.lit(otherwise_val)

    # ------------------------------------------------------------------
    # Platform-specific (best-effort)
    # ------------------------------------------------------------------

    def optimize_table(self, fqn: str, zorder_cols: list[str] | None = None) -> None:
        """
        Runs OPTIMIZE on a Delta table with optional ZORDER BY.
        [SparkBackend] optimize_table: native (Databricks OPTIMIZE).
        Skips gracefully in local mode or when gRPC UserContext fails.
        """
        try:
            if zorder_cols:
                zorder_str = ", ".join(zorder_cols)
                print(f"     -> [SparkBackend] optimize_table: native — OPTIMIZE {fqn} ZORDER BY ({zorder_str})")
                self._spark.sql(f"OPTIMIZE {fqn} ZORDER BY ({zorder_str})")
            else:
                print(f"     -> [SparkBackend] optimize_table: native — OPTIMIZE {fqn}")
                self._spark.sql(f"OPTIMIZE {fqn}")
        except Exception as e:
            if "UserContext" in str(e) or "user_context" in str(e).lower():
                print("     -> [SparkBackend] optimize_table: skipped (not supported via Connect v2 locally).")
            else:
                raise

    def clone_table(
        self,
        src_catalog: str | None,
        src_schema: str,
        src_table: str,
        tgt_catalog: str | None,
        tgt_schema: str,
        tgt_table: str,
    ) -> None:
        """
        Clone a table to the sandbox schema.
        Databricks (non-local): SHALLOW CLONE (zero-copy, metadata only).
        Local (Delta + Derby):  CREATE TABLE AS SELECT * (full copy fallback).
        [SparkBackend] clone_table: native on Databricks, best-effort locally.
        """
        src_fqn = self.build_fqn(src_catalog, src_schema, src_table)
        tgt_fqn = self.build_fqn(tgt_catalog, tgt_schema, tgt_table)

        if self._is_local:
            # Local spark_catalog only supports single-part namespaces — drop catalog prefix
            local_src_fqn = f"`{src_schema}`.`{src_table}`"
            local_tgt_fqn = f"`{tgt_schema}`.`{tgt_table}`"
            print(f"     -> [SparkBackend] clone_table: best-effort — CTAS {local_tgt_fqn} AS SELECT * FROM {local_src_fqn}")
            self._spark.sql(f"CREATE TABLE IF NOT EXISTS {local_tgt_fqn} AS SELECT * FROM {local_src_fqn}")
        else:
            try:
                print(f"     -> [SparkBackend] clone_table: native — SHALLOW CLONE {src_fqn} → {tgt_fqn}")
                self._spark.sql(f"CREATE TABLE IF NOT EXISTS {tgt_fqn} SHALLOW CLONE {src_fqn}")
            except Exception:
                print("     -> [SparkBackend] clone_table: best-effort — SHALLOW CLONE failed, using CTAS")
                self._spark.sql(f"CREATE TABLE IF NOT EXISTS {tgt_fqn} AS SELECT * FROM {src_fqn}")
