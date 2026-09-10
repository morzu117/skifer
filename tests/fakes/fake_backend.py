"""
FakeBackend — Pure Python backend for testing SkiferEngine without PySpark.

Operates on plain Python lists-of-dicts as DataFrames.
Proves that the engine is truly decoupled from Spark.
"""
from __future__ import annotations
from typing import Any


class FakeDataFrame:
    """Minimal DataFrame abstraction backed by a list of dicts."""

    #: Mirrors pyspark's DataFrame.isStreaming (set by read_table_stream).
    is_streaming: bool = False

    def __init__(self, rows: list[dict], name: str = "df"):
        self._rows = list(rows)
        self._name = name

    def __repr__(self):
        return f"FakeDataFrame({self._name}, {len(self._rows)} rows)"

    def __len__(self):
        return len(self._rows)

    @property
    def columns(self) -> list[str]:
        if not self._rows:
            return []
        return list(self._rows[0].keys())


class FakeColumn:
    """A lazy column expression for FakeDataFrame."""

    def __init__(self, name: str | None = None, value=None, is_literal: bool = False):
        self._name = name
        self._value = value
        self._is_literal = is_literal

    def __eq__(self, other):
        return FakeCondition(lambda row: self._eval(row) == other)

    def __ne__(self, other):
        return FakeCondition(lambda row: self._eval(row) != other)

    def _eval(self, row):
        if self._is_literal:
            return self._value
        name = self._name.strip("`")
        return row.get(name)

    def alias(self, name):
        original_eval = self._eval
        col = FakeColumn(name)
        col._eval = original_eval
        col._name = name
        return col

    def isNotNull(self):
        return FakeCondition(lambda row: self._eval(row) is not None)

    def isNull(self):
        return FakeCondition(lambda row: self._eval(row) is None)

    def cast(self, t):
        """No-op cast — returns self for simplicity."""
        return self

    def like(self, pattern):
        import fnmatch
        return FakeCondition(lambda row, p=pattern: fnmatch.fnmatch(str(self._eval(row) or ""), p.replace("%", "*")))

    def isin(self, values):
        return FakeCondition(lambda row, v=values: self._eval(row) in v)

    def desc(self):
        return (self, "desc")

    def asc(self):
        return (self, "asc")


class FakeCondition:
    """A lazy boolean condition for FakeDataFrame."""

    def __init__(self, fn):
        self._fn = fn

    def __call__(self, row):
        return self._fn(row)

    def __and__(self, other):
        return FakeCondition(lambda row: self._fn(row) and other._fn(row))

    def __or__(self, other):
        return FakeCondition(lambda row: self._fn(row) or other._fn(row))

    def __invert__(self):
        return FakeCondition(lambda row: not self._fn(row))


class FakeBackend:
    """
    Pure-Python test double duck-typing the SparkBackend surface.

    Designed exclusively for unit tests — no Spark, no Delta, no network.
    DataFrames are FakeDataFrame instances (lists of dicts).
    """

    def __init__(self, tables: dict[str, list[dict]] | None = None):
        """
        Args:
            tables: Pre-loaded tables as {fqn: list_of_dicts}.
        """
        self._tables = tables or {}
        self._written = {}   # Records write calls: {fqn: rows}
        self._dropped = []   # Records drop calls
        self._schemas_created = []  # Records ensure_schema_exists calls
        self._temp_views = {}  # Records register_temp_view calls: {name: FakeDataFrame}
        self._streams = {}   # Records write_stream_table calls: {fqn: {rows, checkpoint, trigger}}
        self._materialized_views = {}  # Records MV DDL: {fqn: ddl}
        self._mv_calls = []  # Ordered log of ("create"|"refresh"|"drop", fqn) tuples
        self._table_properties = {}  # {fqn: {key: value}} — TBLPROPERTIES store
        self._missing_tables = set()  # FQNs table_exists() must report as absent
        self._certification = {"contracts": [], "runs": [], "checks": []}
        self._adaptive_usage_events = []

    def _append_certification(self, kind: str, row: dict) -> None:
        if not any(existing.get("event_id", existing.get("definition_hash")) == row.get("event_id", row.get("definition_hash")) for existing in self._certification[kind]):
            self._certification[kind].append(dict(row))

    def append_certification_contract(self, schema: str, row: dict) -> None:
        self._append_certification("contracts", row)

    def get_certification_contract(
        self, schema: str, contract_id: str, version: str
    ) -> dict | None:
        rows = [
            row
            for row in self._certification["contracts"]
            if row["contract_id"] == contract_id
            and row["contract_version"] == version
        ]
        if len(rows) > 1:
            raise ValueError(
                f"Contract '{contract_id}' version '{version}' has ambiguous definitions."
            )
        return rows[0] if rows else None

    def append_certification_run(self, schema: str, row: dict) -> None:
        self._append_certification("runs", row)

    def append_certification_check(self, schema: str, row: dict) -> None:
        self._append_certification("checks", row)

    def get_certification_run(self, schema: str, run_id: str) -> dict | None:
        rows = [row for row in self._certification["runs"] if row["run_id"] == run_id]
        return rows[-1] if rows else None

    def append_semantic_usage_event(self, schema: str, row: dict) -> None:
        if not any(
            existing["event_id"] == row["event_id"]
            for existing in self._adaptive_usage_events
        ):
            self._adaptive_usage_events.append(dict(row))

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
        rows = [
            row
            for row in self._adaptive_usage_events
            if (environment is None or row["environment"] == environment)
            and (consumer_class is None or row["consumer_class"] == consumer_class)
            and (since is None or row["occurred_at"] >= since)
            and (until is None or row["occurred_at"] <= until)
        ]
        rows.sort(key=lambda row: (row["occurred_at"], row["event_id"]), reverse=True)
        return [dict(row) for row in (rows[:limit] if limit is not None else rows)]

    def register_temp_view(self, df: Any, name: str) -> None:
        self._temp_views[name] = df

    # ------------------------------------------------------------------
    # Session / Environment
    # ------------------------------------------------------------------

    @property
    def spark(self):
        return None

    @property
    def is_local(self) -> bool:
        return True

    def execute_sql(self, sql: str) -> Any:
        return None

    def check_catalog_access(self, catalog: str) -> bool:
        return True

    def get_current_user(self) -> str | None:
        return "fake_user"

    # ------------------------------------------------------------------
    # Table I/O
    # ------------------------------------------------------------------

    def read_table(self, fqn: str) -> FakeDataFrame:
        clean = fqn.replace("`", "")
        if clean not in self._tables:
            raise ValueError(f"[FakeBackend] Table '{clean}' not found. Available: {list(self._tables.keys())}")
        return FakeDataFrame(self._tables[clean], name=clean)

    def read_source(self, source_type: str, path: str, options: dict | None = None) -> FakeDataFrame:
        raise NotImplementedError(
            "[read_source] FakeBackend does not support external file sources."
        )

    def write_table(self, df: FakeDataFrame, fqn: str, mode: str = "overwrite") -> None:
        if getattr(df, "is_streaming", False):
            raise ValueError(
                f"[write_table] Cannot batch-write a streaming DataFrame to '{fqn}'."
            )
        self._written[fqn] = list(df._rows)

    def write_staging(self, df: FakeDataFrame, fqn: str) -> None:
        self.write_table(df, fqn)
        self._tables[fqn.replace("`", "")] = list(df._rows)

    def read_staging(self, fqn: str) -> FakeDataFrame:
        return self.read_table(fqn)

    def tag_row_violations(
        self,
        df: FakeDataFrame,
        predicates: dict[str, str],
        run_id: str,
        contract_version: str,
    ) -> FakeDataFrame:
        import re

        parsed = []
        for label, predicate in predicates.items():
            match = re.fullmatch(r"`(\w+)`\s+IS\s+(NOT\s+)?NULL", predicate, re.IGNORECASE)
            parsed.append((label, match.group(1), bool(match.group(2))) if match else None)

        rows = []
        for row in df._rows:
            labels = []
            for item in parsed:
                # Test double scope: unsupported SQL predicates intentionally match no rows.
                if item is None:
                    continue
                label, column, negate = item
                is_null = row.get(column) is None
                if (is_null and not negate) or (not is_null and negate):
                    labels.append(label)
            new_row = dict(row)
            new_row["_violations"] = ",".join(labels)
            new_row["_run_id"] = run_id
            new_row["_contract_version"] = contract_version
            rows.append(new_row)
        return self._carry_streaming(df, FakeDataFrame(rows, df._name))

    def drop_staging(self, fqn: str) -> None:
        self.drop_table(fqn)

    # ------------------------------------------------------------------
    # Streaming I/O (Plan 27)
    # ------------------------------------------------------------------

    def read_table_stream(self, fqn: str) -> FakeDataFrame:
        df = self.read_table(fqn)
        df.is_streaming = True
        return df

    def read_source_stream(self, source_type: str, path: str, options: dict | None = None) -> FakeDataFrame:
        raise NotImplementedError(
            "[read_source_stream] FakeBackend does not support external file sources."
        )

    def default_checkpoint_root(self) -> str | None:
        return "/fake/_checkpoints"

    def write_stream_table(
        self,
        df: FakeDataFrame,
        fqn: str,
        checkpoint_location: str,
        trigger: str = "available_now",
        write_mode: str = "append",
        keys: list[str] | None = None,
    ) -> None:
        if not getattr(df, "is_streaming", False):
            raise ValueError(
                f"[write_stream_table] DataFrame for '{fqn}' is not streaming."
            )
        if write_mode == "upsert" and not keys:
            raise ValueError(
                f"[write_stream_table] write_mode 'upsert' for '{fqn}' requires "
                "non-empty merge keys."
            )
        rows = list(df._rows)
        if write_mode == "upsert":
            # Simulate CDC Type 1: last row wins per key tuple.
            merged: dict = {}
            for row in rows:
                merged[tuple(row.get(k) for k in keys)] = row
            rows = list(merged.values())
        self._streams[fqn] = {
            "rows": rows,
            "checkpoint": checkpoint_location,
            "trigger": trigger,
            "write_mode": write_mode,
            "keys": list(keys) if keys else None,
        }

    def drop_table(self, fqn: str) -> None:
        self._dropped.append(fqn)

    def ensure_schema_exists(self, schema: str) -> None:
        self._schemas_created.append(schema)

    # ------------------------------------------------------------------
    # Materialized views (Plan 28)
    # ------------------------------------------------------------------

    def execute_sql_on_warehouse(self, sql: str, warehouse_id: str) -> list[list]:
        self._mv_calls.append(("warehouse", sql))
        return []

    def create_materialized_view(self, ddl: str, warehouse_id: str | None = None) -> None:
        fqn = self._parse_mv_target(ddl)
        self._materialized_views[fqn] = ddl
        self._mv_calls.append(("create", fqn))
        # The view now exists, carrying the hash its own DDL declared.
        self._missing_tables.discard(fqn)
        hash_value = self._parse_mv_hash(ddl)
        if hash_value:
            self._table_properties.setdefault(fqn, {})["skifer.definition_hash"] = hash_value

    def refresh_materialized_view(self, fqn: str, warehouse_id: str | None = None) -> None:
        self._mv_calls.append(("refresh", fqn.replace("`", "")))

    def drop_materialized_view(self, fqn: str, warehouse_id: str | None = None) -> None:
        clean = fqn.replace("`", "")
        self._materialized_views.pop(clean, None)
        self._table_properties.pop(clean, None)
        self._missing_tables.add(clean)
        self._mv_calls.append(("drop", clean))

    def get_table_property(
        self, fqn: str, key: str, warehouse_id: str | None = None
    ) -> str | None:
        return self._table_properties.get(fqn.replace("`", ""), {}).get(key)

    @staticmethod
    def _parse_mv_target(ddl: str) -> str:
        """Extract the target FQN from a CREATE [OR REPLACE] MATERIALIZED VIEW statement."""
        head = ddl.splitlines()[0]
        return head.split("MATERIALIZED VIEW", 1)[1].strip().replace("`", "")

    @staticmethod
    def _parse_mv_hash(ddl: str) -> str | None:
        """Extract the definition hash the DDL persists in TBLPROPERTIES."""
        import re
        match = re.search(r"'skifer\.definition_hash'\s*=\s*'([^']+)'", ddl)
        return match.group(1) if match else None

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    def schema_exists(self, catalog: str | None, schema: str) -> bool:
        return True

    def table_exists(self, catalog: str | None, schema: str, table: str) -> bool:
        """True unless the FQN was explicitly registered in ``_missing_tables``."""
        fqn = ".".join(p for p in (catalog, schema, table) if p)
        return fqn not in self._missing_tables

    def list_tables(self, schema: str, catalog: str | None = None) -> list[str]:
        """Return tables whose FQN ends with schema.* from pre-loaded tables."""
        result = []
        for fqn in self._tables:
            clean = fqn.replace("`", "")
            parts = clean.split(".")
            fqn_schema = parts[-2] if len(parts) >= 2 else ""
            if fqn_schema == schema:
                result.append(parts[-1])
        return result

    def list_columns(self, fqn: str) -> list[str]:
        """Return column names from the first row of the pre-loaded table."""
        clean = fqn.replace("`", "")
        rows = self._tables.get(clean)
        if rows:
            return list(rows[0].keys())
        return []

    def list_schemas(self, catalog: str | None = None) -> list[str]:
        return []

    def list_column_types(self, fqn: str) -> dict[str, str]:
        clean = fqn.replace("`", "")
        rows = self._tables.get(clean)
        if rows:
            return {k: "string" for k in rows[0].keys()}
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

    def col(self, name: str) -> FakeColumn:
        return FakeColumn(name=name)

    def lit(self, value: Any) -> FakeColumn:
        c = FakeColumn(is_literal=True, value=value)
        return c

    def expr(self, sql_expr: str) -> FakeColumn:
        return FakeColumn(name=f"expr({sql_expr})")

    def lit_true(self) -> FakeColumn:
        return self.lit(True)

    # ------------------------------------------------------------------
    # Operations on Columns
    # ------------------------------------------------------------------

    def apply_op(self, c: Any, op: Any, allow_raw_sql: bool = True) -> Any:
        """No-op for FakeBackend — returns column unchanged (plan17-3.2)."""
        return c

    def build_filter(self, f: Any, allow_raw_sql: bool = True) -> Any:
        """Build a FakeCondition from a ParsedFilter (plan17-3.2)."""
        col_name = f.column
        op = f.operator
        val = f.value
        c = FakeColumn(name=col_name)
        if op == "is_not_null":
            return c.isNotNull()
        if op == "is_null":
            return c.isNull()
        if op == "equals":
            return FakeCondition(lambda row, cn=col_name, v=val: row.get(cn) == v)
        if op == "not_equals":
            return FakeCondition(lambda row, cn=col_name, v=val: row.get(cn) != v)
        if op == "in":
            return FakeCondition(
                lambda row, cn=col_name, v=val: row.get(cn)
                in (v.split(",") if isinstance(v, str) else v)
            )
        if op == "not_in":
            return FakeCondition(
                lambda row, cn=col_name, v=val: row.get(cn)
                not in (v.split(",") if isinstance(v, str) else v)
            )
        if op == "contains":
            return FakeCondition(
                lambda row, cn=col_name, v=val: v in str(row.get(cn, ""))
            )
        return FakeCondition(lambda row: True)

    # ------------------------------------------------------------------
    # DataFrame Operations
    # ------------------------------------------------------------------

    @staticmethod
    def _carry_streaming(src: FakeDataFrame, out: FakeDataFrame) -> FakeDataFrame:
        """Propagate the is_streaming flag through transformations (like PySpark)."""
        out.is_streaming = getattr(src, "is_streaming", False)
        return out

    def select(self, df: FakeDataFrame, columns: list) -> FakeDataFrame:
        """Select columns — if columns are strings, project rows."""
        if not columns:
            return self._carry_streaming(df, FakeDataFrame(df._rows, df._name))
        # columns may be FakeColumn with alias, or strings
        new_rows = []
        for row in df._rows:
            new_row = {}
            for col in columns:
                if isinstance(col, str):
                    new_row[col] = row.get(col)
                elif isinstance(col, FakeColumn) and col._name:
                    new_row[col._name] = col._eval(row)
                else:
                    # Unknown expression — keep row as-is
                    new_row.update(row)
                    break
            new_rows.append(new_row)
        return self._carry_streaming(df, FakeDataFrame(new_rows, df._name))

    def filter(self, df: FakeDataFrame, condition: Any) -> FakeDataFrame:
        if callable(condition):
            rows = [r for r in df._rows if condition(r)]
        else:
            rows = list(df._rows)
        return self._carry_streaming(df, FakeDataFrame(rows, df._name))

    def join(self, left: FakeDataFrame, right: FakeDataFrame, on: Any, how: str = "left") -> FakeDataFrame:
        """Simple nested-loop join for testing."""
        result = []
        if isinstance(on, list):
            # list join: same key names
            for l_row in left._rows:
                matched = False
                for r_row in right._rows:
                    if all(l_row.get(k) == r_row.get(k) for k in on):
                        merged = {**l_row, **{k: v for k, v in r_row.items() if k not in on}}
                        result.append(merged)
                        matched = True
                if not matched and how in ("left", "left_outer"):
                    result.append(dict(l_row))
        else:
            # expression join — no-op, just return left rows for testing
            result = list(left._rows)
        return self._carry_streaming(left, FakeDataFrame(result, f"{left._name}_joined"))

    def drop_columns(self, df: FakeDataFrame, columns: list) -> FakeDataFrame:
        col_names = set()
        for c in columns:
            if isinstance(c, str):
                col_names.add(c)
            elif isinstance(c, FakeColumn) and c._name:
                col_names.add(c._name.strip("`"))
        rows = [{k: v for k, v in row.items() if k not in col_names} for row in df._rows]
        return self._carry_streaming(df, FakeDataFrame(rows, df._name))

    def with_column(self, df: FakeDataFrame, name: str, col: Any) -> FakeDataFrame:
        rows = []
        for row in df._rows:
            new_row = dict(row)
            if isinstance(col, FakeColumn):
                new_row[name] = col._eval(row)
            else:
                new_row[name] = col
            rows.append(new_row)
        return self._carry_streaming(df, FakeDataFrame(rows, df._name))

    def limit(self, df: FakeDataFrame, n: int) -> FakeDataFrame:
        return FakeDataFrame(df._rows[:n], df._name)

    def union_by_name(self, dfs: list, allow_missing: bool = True) -> FakeDataFrame:
        all_rows = []
        for df in dfs:
            all_rows.extend(df._rows)
        return FakeDataFrame(all_rows, "union")

    def drop_duplicates(self, df: FakeDataFrame, cols: list[str] | None = None) -> FakeDataFrame:
        if cols:
            seen = set()
            rows = []
            for row in df._rows:
                key = tuple(row.get(c) for c in cols)
                if key not in seen:
                    seen.add(key)
                    rows.append(row)
        else:
            seen_keys = set()
            rows = []
            for row in df._rows:
                key = tuple(sorted(row.items()))
                if key not in seen_keys:
                    seen_keys.add(key)
                    rows.append(row)
        return FakeDataFrame(rows, df._name)

    def drop_nulls(self, df: FakeDataFrame, subset: list[str]) -> FakeDataFrame:
        rows = [row for row in df._rows if all(row.get(c) is not None for c in subset)]
        return FakeDataFrame(rows, df._name)

    #: Pure-Python twins of the Spark aggregate functions (Plan 28).
    _AGG_IMPL = {
        "sum": lambda vals: sum(vals) if vals else None,
        "avg": lambda vals: (sum(vals) / len(vals)) if vals else None,
        "min": lambda vals: min(vals) if vals else None,
        "max": lambda vals: max(vals) if vals else None,
        "count": len,
        "count_distinct": lambda vals: len(set(vals)),
        "sum_distinct": lambda vals: sum(set(vals)) if vals else None,
        "approx_count_distinct": lambda vals: len(set(vals)),
        "stddev": lambda vals: None,
        "variance": lambda vals: None,
        "first": lambda vals: vals[0] if vals else None,
        "last": lambda vals: vals[-1] if vals else None,
    }

    def agg_expr(self, func: str, source: str) -> Any:
        """Return a ``(func, source)`` marker consumed by group_by_agg (Plan 28)."""
        if func not in self._AGG_IMPL:
            raise ValueError(f"[aggregate] Unknown aggregate function '{func}'.")
        return (func, source)

    def group_by_agg(self, df: FakeDataFrame, keys: list[str], exprs: dict) -> FakeDataFrame:
        """Group rows by *keys* and evaluate the aggregate markers from agg_expr."""
        groups: dict = {}
        for row in df._rows:
            key = tuple(row.get(k) for k in keys)
            groups.setdefault(key, []).append(row)

        rows = []
        for key, group_rows in groups.items():
            new_row = dict(zip(keys, key))
            for target, (func, source) in exprs.items():
                if source == "*":
                    values = [1] * len(group_rows)
                else:
                    values = [r.get(source) for r in group_rows if r.get(source) is not None]
                new_row[target] = self._AGG_IMPL[func](values)
            rows.append(new_row)
        return FakeDataFrame(rows, df._name)

    def cache(self, df: FakeDataFrame) -> FakeDataFrame:
        return df  # No-op for fake

    def unpersist(self, df: FakeDataFrame) -> None:
        pass  # No-op for fake

    def is_empty(self, df: FakeDataFrame) -> bool:
        return len(df._rows) == 0

    def count(self, df: FakeDataFrame) -> int:
        return len(df._rows)

    # ------------------------------------------------------------------
    # Window
    # ------------------------------------------------------------------

    def row_number_over(self, df: FakeDataFrame, partition_by: list[str], order_by: list[dict]) -> FakeDataFrame:
        """Add _rn column with row_number grouped by partition_by."""
        from itertools import groupby
        rows = sorted(df._rows, key=lambda r: tuple(r.get(p, "") for p in partition_by))
        result = []
        for key, group in groupby(rows, key=lambda r: tuple(r.get(p) for p in partition_by)):
            for i, row in enumerate(group, start=1):
                new_row = dict(row)
                new_row["_rn"] = i
                result.append(new_row)
        return FakeDataFrame(result, df._name)

    # ------------------------------------------------------------------
    # When / Otherwise
    # ------------------------------------------------------------------

    def when(self, condition: Any, value: Any) -> Any:
        return (condition, value, None)  # (cond, then, else)

    def otherwise(self, col: Any, value: Any) -> Any:
        cond, then, _ = col
        return (cond, then, value)

    def when_chain(self, conditions: list, otherwise_val: Any) -> Any:
        return (conditions, otherwise_val)

    # ------------------------------------------------------------------
    # Platform-specific
    # ------------------------------------------------------------------

    def optimize_table(self, fqn: str, zorder_cols: list[str] | None = None) -> None:
        print(f"[FakeBackend] optimize_table: no-op for {fqn}")

    def clone_table(
        self,
        src_catalog,
        src_schema,
        src_table,
        tgt_catalog,
        tgt_schema,
        tgt_table,
    ) -> None:
        print(f"[FakeBackend] clone_table: no-op ({src_schema}.{src_table} → {tgt_schema}.{tgt_table})")
