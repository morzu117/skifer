"""Write-strategy equivalence between Spark and the compiled DuckDB SQL path.

Every test here runs the **same pipeline YAML at least twice**, changing the source
between runs, and compares the final state of the two targets. One run proves
nothing about a cumulative strategy: the first run fills an empty table, where
`append`, `merge`, `snapshot` and a plain `CREATE TABLE AS` are indistinguishable.
The divergences this module exists to catch all appear on the second run.

Both engines are driven through their public entry point (`run_from_yaml`), not
through internals, so what is compared is what a user would actually get.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest
import yaml

from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from skifer.core.core import SkiferEngine
from tests.test_sql_spark_equivalence import assert_spark_duckdb_equivalent


REPO_ROOT = Path(__file__).resolve().parent.parent

_SPARK_TYPES = {
    "INT": IntegerType(),
    "STRING": StringType(),
    "TIMESTAMP": TimestampType(),
}
_DUCK_TYPES = {"INT": "INTEGER", "STRING": "VARCHAR", "TIMESTAMP": "TIMESTAMP"}


class WriteRuntime:
    """One Spark engine and one DuckDB engine sharing an injected clock."""

    def __init__(self, spark, tmp_path):
        self.namespace = f"write_equiv_{uuid4().hex[:12]}"
        self.tmp_path = tmp_path
        self._now = datetime(2026, 1, 1, 9, tzinfo=timezone.utc)
        clock = lambda: self._now  # noqa: E731 - injected on purpose, never wall clock
        self.sql_engine = SkiferEngine(force_env="LOCAL_SQL", clock=clock)
        self.spark_engine = SkiferEngine(spark=spark, force_env="LOCAL", clock=clock)
        # No sandbox on either engine. Smart Sandbox clones a source table into the
        # user schema on first use and never refreshes it, so the second run would
        # read the FIRST batch again and diverge from DuckDB — a harness artefact
        # that would read as a product defect. An empty suffix resolves every table
        # to itself, which is what job and production mode already do.
        self.spark_engine.schema_suffix = ""
        self.spark = spark
        self.duck = self.sql_engine.backend
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS `{self.namespace}`")
        self.duck.execute_sql(f'CREATE SCHEMA IF NOT EXISTS "{self.namespace}"')

    def tick(self, moment: datetime) -> None:
        """Move the injected clock; both engines read the same value."""
        self._now = moment

    def source(self, name: str) -> str:
        return f"{self.namespace}.{name}"

    def write_source(self, name, columns, rows):
        """(Re)create the same source rows in both engines, from one definition.

        Types are declared once, logically, and rendered per engine: Spark rejects
        a bare ``VARCHAR`` without a length, DuckDB has no ``STRING``. Declaring the
        columns twice by hand is how the two sources start to differ.

        Spark is written through a DataFrame overwrite rather than DROP + CREATE:
        recreating the table under the same name leaves Spark holding a stale file
        listing, and the next write fails reading a part file that no longer exists.
        """
        spark_schema = StructType(
            [StructField(column, _SPARK_TYPES[dtype], True) for column, dtype in columns]
        )
        self.spark.createDataFrame(rows, spark_schema).write.format("delta").mode(
            "overwrite"
        ).option("overwriteSchema", "true").saveAsTable(
            f"`{self.namespace}`.`{name}`"
        )

        duck_ddl = ", ".join(
            f'"{column}" {_DUCK_TYPES[dtype]}' for column, dtype in columns
        )
        self.duck.execute_sql(f'DROP TABLE IF EXISTS "{self.namespace}"."{name}"')
        self.duck.execute_sql(f'CREATE TABLE "{self.namespace}"."{name}" ({duck_ddl})')
        if rows:
            values = ", ".join(_sql_row(row) for row in rows)
            self.duck.execute_sql(
                f'INSERT INTO "{self.namespace}"."{name}" VALUES {values}'
            )
        return self.source(name)

    def run(self, schema: dict, layer: str, table: str) -> None:
        """Run one YAML through both engines, in the order the product supports."""
        path = self.tmp_path / f"{uuid4().hex}.yaml"
        path.write_text(yaml.safe_dump(schema, sort_keys=False), encoding="utf-8")
        self.sql_engine.run_from_yaml(str(path), layer, table)
        self.spark_engine.run_from_yaml(str(path), layer, table)

    def assert_targets_equivalent(self, layer: str, table: str) -> None:
        """Compare the two targets with the module-wide strict comparator."""
        duck_fqn = f"{self.sql_engine.get_target_schema(layer)}.{table}"
        spark_fqn = f"{self.spark_engine.get_target_schema(layer)}.{table}"
        cursor = self.duck.read_table(duck_fqn)
        duck_columns = [description[0] for description in cursor.description]
        duck_rows = cursor.fetchall()
        spark_df = self.spark.table(spark_fqn)
        assert_spark_duckdb_equivalent(spark_df, duck_columns, duck_rows)
        return duck_rows, duck_columns

    def close(self):
        self.duck.connection.close()


def _sql_row(row) -> str:
    return "(" + ", ".join(_sql_literal(value) for value in row) + ")"


def _sql_literal(value) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return repr(value)
    if isinstance(value, datetime):
        return "TIMESTAMP '" + value.strftime("%Y-%m-%d %H:%M:%S") + "'"
    return "'" + str(value).replace("'", "''") + "'"


@pytest.fixture
def write_runtime(spark, tmp_path):
    runtime = WriteRuntime(spark, tmp_path)
    yield runtime
    runtime.close()


def _target(name: str) -> str:
    """Unique target name per run.

    The local Delta warehouse is shared across runs; a directory left behind by an
    interrupted run makes the next CREATE fail on a non-empty location, which looks
    like a product defect and is not one.
    """
    return f"{name}_{uuid4().hex[:8]}"


def _orders_schema(runtime, materialization):
    return {
        "materialization": materialization,
        "tables": [{"name": runtime.source("orders"), "alias": "orders"}],
        "select_final": [
            ["order_id", "order_id"],
            ["status", "status"],
            ["amount", "amount"],
        ],
    }


ORDER_COLUMNS = (("order_id", "INT"), ("status", "STRING"), ("amount", "INT"))


def test_incremental_append_accumulates_identically_on_both_engines(write_runtime):
    """Two runs on different rows must leave four rows on both engines.

    A single run would leave two rows whatever the strategy, including a plain
    overwrite — which is exactly the implementation this test exists to reject.
    """
    target = _target("appended")
    schema = _orders_schema(
        write_runtime, {"type": "incremental", "strategy": "append"}
    )
    write_runtime.write_source(
        "orders", ORDER_COLUMNS, [(1, "new", 10), (2, "new", 20)]
    )
    write_runtime.run(schema, "gold", target)

    write_runtime.write_source(
        "orders", ORDER_COLUMNS, [(3, "new", 30), (4, "new", 40)]
    )
    write_runtime.run(schema, "gold", target)

    rows, _ = write_runtime.assert_targets_equivalent("gold", target)
    assert len(rows) == 4


def test_incremental_append_watermark_bounds_identically_on_both_engines(write_runtime):
    """Rows at or below the stored maximum must not be appended twice.

    The second source deliberately repeats the first batch. An unbounded append
    would double it on both engines and still look self-consistent, so the row
    count is asserted as well as the equivalence.
    """
    target = _target("watermarked")
    schema = _orders_schema(
        write_runtime,
        {"type": "incremental", "strategy": "append", "watermark_column": "order_id"},
    )
    write_runtime.write_source(
        "orders", ORDER_COLUMNS, [(1, "new", 10), (2, "new", 20)]
    )
    write_runtime.run(schema, "gold", target)

    write_runtime.write_source(
        "orders",
        ORDER_COLUMNS,
        [(1, "new", 10), (2, "new", 20), (3, "new", 30)],
    )
    write_runtime.run(schema, "gold", target)

    rows, _ = write_runtime.assert_targets_equivalent("gold", target)
    assert len(rows) == 3


def test_incremental_merge_updates_in_place_identically_on_both_engines(write_runtime):
    """A changed key is updated, an untouched key is kept, a new key is inserted.

    The assertion that separates a real merge from an append is the absence of a
    duplicate on the key — an append would leave two rows for key 1 and still
    contain every expected value.
    """
    target = _target("merged")
    schema = _orders_schema(
        write_runtime,
        {"type": "incremental", "strategy": "merge", "unique_key": ["order_id"]},
    )
    write_runtime.write_source(
        "orders", ORDER_COLUMNS, [(1, "old", 10), (2, "steady", 20)]
    )
    write_runtime.run(schema, "gold", target)

    write_runtime.write_source(
        "orders", ORDER_COLUMNS, [(1, "new", 15), (3, "new", 30)]
    )
    write_runtime.run(schema, "gold", target)

    rows, columns = write_runtime.assert_targets_equivalent("gold", target)
    assert len(rows) == 3
    key = columns.index("order_id")
    status = columns.index("status")
    by_key = {row[key]: row[status] for row in rows}
    assert by_key == {1: "new", 2: "steady", 3: "new"}


def test_view_follows_the_source_identically_on_both_engines(write_runtime):
    """A view must show the new rows after the source changes, without rerunning.

    Reading it once would pass on a `CREATE TABLE AS`. The source is changed after
    the single run, and both engines are read again.
    """
    target = _target("orders_view")
    schema = _orders_schema(write_runtime, {"type": "view"})
    write_runtime.write_source("orders", ORDER_COLUMNS, [(1, "first", 10)])
    write_runtime.run(schema, "gold", target)
    first, _ = write_runtime.assert_targets_equivalent("gold", target)
    assert len(first) == 1

    write_runtime.write_source(
        "orders", ORDER_COLUMNS, [(1, "first", 10), (2, "second", 20)]
    )
    second, _ = write_runtime.assert_targets_equivalent("gold", target)
    assert len(second) == 2


SNAPSHOT_COLUMNS = (
    ("order_id", "INT"),
    ("status", "STRING"),
    ("amount", "INT"),
    ("modified_at", "TIMESTAMP"),
)

T1 = datetime(2026, 1, 1)
T2 = datetime(2026, 1, 2)


def _snapshot_schema(runtime, materialization, *, with_modified_at):
    columns = [
        ["order_id", "order_id"],
        ["status", "status"],
        ["amount", "amount"],
    ]
    if with_modified_at:
        columns.append(["modified_at", "modified_at"])
    return {
        "materialization": materialization,
        "tables": [{"name": runtime.source("orders"), "alias": "orders"}],
        "select_final": columns,
    }


def test_snapshot_timestamp_history_is_identical_on_both_engines(write_runtime):
    """One changed key, one untouched key, one new key — and matching SCD2 bounds.

    The untouched key is what separates a real SCD2 write from one that closes and
    reinserts everything: that implementation produces the right rows for keys 1
    and 3 and a spurious second version for key 2.
    """
    target = _target("snap_ts")
    schema = _snapshot_schema(
        write_runtime,
        {
            "type": "snapshot",
            "strategy": "timestamp",
            "unique_key": ["order_id"],
            "updated_at": "modified_at",
            "on_missing": "ignore",
        },
        with_modified_at=True,
    )
    write_runtime.write_source(
        "orders", SNAPSHOT_COLUMNS, [(1, "old", 10, T1), (2, "steady", 20, T1)]
    )
    write_runtime.run(schema, "gold", target)

    write_runtime.tick(datetime(2026, 1, 2, 9, tzinfo=timezone.utc))
    write_runtime.write_source(
        "orders",
        SNAPSHOT_COLUMNS,
        [(1, "new", 15, T2), (2, "steady", 20, T1), (3, "new", 30, T2)],
    )
    write_runtime.run(schema, "gold", target)

    rows, columns = write_runtime.assert_targets_equivalent("gold", target)
    assert len(rows) == 4
    key, valid_from, valid_to = (
        columns.index("order_id"),
        columns.index("valid_from"),
        columns.index("valid_to"),
    )
    open_versions = [row for row in rows if row[valid_to] is None]
    assert sorted(row[key] for row in open_versions) == [1, 2, 3]
    steady = [row for row in rows if row[key] == 2]
    assert len(steady) == 1
    assert steady[0][valid_from] == T1


def test_snapshot_check_history_is_identical_on_both_engines(write_runtime):
    """Without a source timestamp, `valid_from` comes from the injected clock.

    Both engines must read the same clock, otherwise the two histories are stamped
    differently and nothing downstream lines up.
    """
    target = _target("snap_check")
    schema = _snapshot_schema(
        write_runtime,
        {
            "type": "snapshot",
            "strategy": "check",
            "unique_key": ["order_id"],
            "check_columns": ["status", "amount"],
            "on_missing": "close",
            "max_closed_ratio": 1.0,
        },
        with_modified_at=False,
    )
    columns = SNAPSHOT_COLUMNS[:3]
    write_runtime.write_source("orders", columns, [(1, "old", 10), (2, "gone", 20)])
    write_runtime.run(schema, "gold", target)

    write_runtime.tick(datetime(2026, 1, 2, 9, tzinfo=timezone.utc))
    write_runtime.write_source("orders", columns, [(1, "new", 15), (3, "new", 30)])
    write_runtime.run(schema, "gold", target)

    rows, names = write_runtime.assert_targets_equivalent("gold", target)
    key, valid_to = names.index("order_id"), names.index("valid_to")
    open_keys = sorted(row[key] for row in rows if row[valid_to] is None)
    # Key 2 vanished from the batch and `on_missing: close` closed it, so it is
    # present in history but no longer current.
    assert open_keys == [1, 3]
    assert sorted(row[key] for row in rows) == [1, 1, 2, 3]


def test_a_duplicated_merge_key_is_refused_by_both_engines(write_runtime, tmp_path):
    """The same batch must not be accepted by one engine and refused by the other.

    Measured before the guard existed: Delta refuses with
    `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE`, while DuckDB accepted
    the statement and kept one of the two rows, arbitrarily. Nothing in the result
    said a row had been dropped — which makes the permissive side the dangerous one,
    and makes this a divergence in the very feature this module exists to prove
    equivalent.

    The first run is deliberately clean: it creates the table on both engines, as a
    `CREATE TABLE AS` would, and only the second run performs a MERGE. Guarding the
    creation too would refuse a batch Spark accepts.
    """
    target = _target("dup_key")
    schema = _orders_schema(
        write_runtime,
        {"type": "incremental", "strategy": "merge", "unique_key": ["order_id"]},
    )
    write_runtime.write_source(
        "orders", ORDER_COLUMNS, [(1, "first", 10), (2, "first", 20)]
    )
    write_runtime.run(schema, "gold", target)

    # Order 1 now appears twice, with conflicting values.
    write_runtime.write_source(
        "orders", ORDER_COLUMNS, [(1, "A", 11), (1, "B", 12), (2, "second", 21)]
    )
    path = tmp_path / "dup.yaml"
    path.write_text(yaml.safe_dump(schema, sort_keys=False), encoding="utf-8")

    with pytest.raises(Exception) as sql_error:
        write_runtime.sql_engine.run_from_yaml(str(path), "gold", target)
    with pytest.raises(Exception) as spark_error:
        write_runtime.spark_engine.run_from_yaml(str(path), "gold", target)

    # The SQL path names the key and the count, and suggests a YAML fix; Delta
    # raises its own message. What must match is the verdict, not the wording.
    assert "unique_key" in str(sql_error.value)
    assert "order_id" in str(sql_error.value)
    assert "MULTIPLE_SOURCE_ROW" in str(spark_error.value)

    # And neither engine changed the target.
    rows, columns = write_runtime.assert_targets_equivalent("gold", target)
    assert sorted(rows) == [(1, "first", 10), (2, "first", 20)]


def test_the_refusal_names_no_data_value(write_runtime, tmp_path):
    """A refusal reports columns and counts, never the offending values (Plan 31)."""
    target = _target("dup_quiet")
    schema = _orders_schema(
        write_runtime,
        {"type": "incremental", "strategy": "merge", "unique_key": ["order_id"]},
    )
    write_runtime.write_source("orders", ORDER_COLUMNS, [(1, "first", 10)])
    write_runtime.run(schema, "gold", target)

    write_runtime.write_source(
        "orders", ORDER_COLUMNS, [(1, "secret-status", 999), (1, "other", 998)]
    )
    path = tmp_path / "quiet.yaml"
    path.write_text(yaml.safe_dump(schema, sort_keys=False), encoding="utf-8")

    with pytest.raises(Exception) as error:
        write_runtime.sql_engine.run_from_yaml(str(path), "gold", target)

    message = str(error.value)
    assert "secret-status" not in message
    assert "999" not in message
