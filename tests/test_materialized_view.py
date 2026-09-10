"""
Plan 28 — engine wiring tests for materialized views.

Part 1: pure functions — warehouse resolution, definition hash, DDL assembly.
Part 2: FakeBackend (no Spark) — create/refresh/replace dispatch, SQL artifact
        generation, pattern refusals, defensive write guard, full_refresh.
Part 3: real local Spark session — end-to-end materialization of the compiled
        SELECT into a Delta table (decision #20 also proves the SQL is valid).
"""
import os
import shutil

import pytest

from skifer.core.context import ExecutionContext
from skifer.core.core import SkiferEngine
from skifer.core.interpreter import SchemaInterpreter
from skifer.core.patterns import PipelinePatterns
from skifer.core.schema_loader import parse_schema
from skifer.core.sql_compiler import (
    compile_materialized_view_ddl,
    definition_hash,
)
from tests.fakes.fake_backend import FakeBackend


def _make_engine(backend, *, is_local=False, params=None, is_job=False, is_prod=False):
    """Minimal engine with an injected backend (model: tests/test_streaming.py)."""
    engine = object.__new__(SkiferEngine)
    ctx = ExecutionContext()
    object.__setattr__(engine, "_context", ctx)
    engine.spark = getattr(backend, "spark", None)
    engine.is_local = is_local
    engine.db = None
    engine.env = "local"
    env_block = {}
    if params:
        env_block["params"] = params
    if is_prod:
        env_block["is_production"] = True
    engine.config = {"environments": {"local": env_block}}
    engine.schema_suffix = ""
    engine.is_job_execution = is_job
    engine._backend = backend
    engine._interpreter = SchemaInterpreter(backend=backend, context=ctx)
    engine._patterns = PipelinePatterns(engine=engine)
    engine.monitor = None
    return engine


def _mv_schema(**mat_overrides):
    mat = {"type": "materialized_view", **mat_overrides}
    return {
        "materialization": mat,
        "tables": [
            {"name": "silver.orders", "alias": "ord",
             "filter": [{"column": "status", "operator": "equals", "value": "DONE"}]},
        ],
        "aggregate": {
            "group_by": ["country"],
            "measures": [{"source": "amount", "target": "total", "func": "sum"}],
        },
    }


def _backend_with_orders():
    return FakeBackend(tables={
        "silver.orders": [
            {"country": "FR", "amount": 10, "status": "DONE"},
            {"country": "FR", "amount": 5, "status": "DONE"},
        ],
    })


# ==============================================================================
# Part 1 — pure functions
# ==============================================================================

class TestWarehouseResolution:
    def test_configured_warehouse_is_returned(self):
        engine = _make_engine(FakeBackend(), params={"sql_warehouse_id": "wh-123"})
        assert engine.resolve_sql_warehouse_id() == "wh-123"

    def test_local_mode_needs_no_warehouse(self):
        engine = _make_engine(FakeBackend(), is_local=True)
        assert engine.resolve_sql_warehouse_id() is None

    def test_interactive_without_warehouse_returns_none(self):
        engine = _make_engine(FakeBackend())
        assert engine.resolve_sql_warehouse_id() is None

    def test_job_without_warehouse_fails_fast(self):
        engine = _make_engine(FakeBackend(), is_job=True)
        with pytest.raises(ValueError) as exc:
            engine.resolve_sql_warehouse_id()
        assert "sql_warehouse_id" in str(exc.value)

    def test_production_without_warehouse_fails_fast(self):
        engine = _make_engine(FakeBackend(), is_prod=True)
        with pytest.raises(ValueError) as exc:
            engine.resolve_sql_warehouse_id()
        assert "sql_warehouse_id" in str(exc.value)

    def test_job_failure_happens_before_any_compilation(self):
        """The fail-fast must fire without reading a single source table."""
        b = _backend_with_orders()
        engine = _make_engine(b, is_job=True)
        with pytest.raises(ValueError):
            engine._create_materialized_view(_mv_schema(), "gold", "fact_orders")
        assert b._schemas_created == []


class TestDefinitionHash:
    def test_same_definition_same_hash(self):
        assert definition_hash("SELECT 1", {}) == definition_hash("SELECT 1", {})

    def test_different_select_different_hash(self):
        assert definition_hash("SELECT 1", {}) != definition_hash("SELECT 2", {})

    def test_schedule_is_part_of_the_definition(self):
        """Changing only the schedule must still trigger a replace."""
        base = definition_hash("SELECT 1", {"schedule": "EVERY 6 HOURS"})
        assert base != definition_hash("SELECT 1", {"schedule": "EVERY 12 HOURS"})

    def test_comment_and_clustering_are_part_of_the_definition(self):
        assert definition_hash("SELECT 1", {"comment": "a"}) != definition_hash(
            "SELECT 1", {"comment": "b"}
        )
        assert definition_hash("SELECT 1", {"cluster_by": ["a"]}) != definition_hash(
            "SELECT 1", {"cluster_by": ["b"]}
        )

    def test_refresh_mode_is_not_part_of_the_definition(self):
        """refresh: drives run-time behaviour, not what the view *is*."""
        assert definition_hash("SELECT 1", {"refresh": "auto"}) == definition_hash(
            "SELECT 1", {"refresh": "never"}
        )


class TestDdlCompilation:
    def test_minimal_create(self):
        ddl = compile_materialized_view_ddl("`gold`.`mv`", "SELECT 1")
        assert ddl.startswith("CREATE MATERIALIZED VIEW `gold`.`mv`")
        assert ddl.endswith("AS\nSELECT 1")

    def test_or_replace(self):
        ddl = compile_materialized_view_ddl("`gold`.`mv`", "SELECT 1", or_replace=True)
        assert ddl.startswith("CREATE OR REPLACE MATERIALIZED VIEW")

    def test_unquoted_fqn_is_quoted(self):
        ddl = compile_materialized_view_ddl("cat.gold.mv", "SELECT 1")
        assert "`cat`.`gold`.`mv`" in ddl

    def test_all_clauses_in_databricks_order(self):
        ddl = compile_materialized_view_ddl(
            "`gold`.`mv`", "SELECT 1",
            {"cluster_by": ["country"], "comment": "CA", "schedule": "EVERY 6 HOURS"},
            "abc123",
        )
        positions = [
            ddl.index("CLUSTER BY"), ddl.index("COMMENT"),
            ddl.index("TBLPROPERTIES"), ddl.index("SCHEDULE"), ddl.index("\nAS\n"),
        ]
        assert positions == sorted(positions)
        assert "CLUSTER BY (`country`)" in ddl
        assert "SCHEDULE EVERY 6 HOURS" in ddl
        assert "'skifer.definition_hash' = 'abc123'" in ddl

    def test_partition_by_when_no_clustering(self):
        ddl = compile_materialized_view_ddl(
            "`gold`.`mv`", "SELECT 1", {"partition_by": ["country", "year"]}
        )
        assert "PARTITIONED BY (`country`, `year`)" in ddl
        assert "CLUSTER BY" not in ddl

    def test_comment_quote_is_escaped(self):
        ddl = compile_materialized_view_ddl(
            "`gold`.`mv`", "SELECT 1", {"comment": "CA d'affaires"}
        )
        assert "COMMENT 'CA d''affaires'" in ddl

    def test_no_tblproperties_without_hash(self):
        assert "TBLPROPERTIES" not in compile_materialized_view_ddl("`g`.`m`", "SELECT 1")


# ==============================================================================
# Part 2 — FakeBackend wiring
# ==============================================================================

class TestMaterializedViewDispatch:
    def test_absent_view_is_created(self):
        b = _backend_with_orders()
        b._missing_tables.add("gold.fact_orders")
        engine = _make_engine(b, params={"sql_warehouse_id": "wh-1"})

        executed = engine._create_materialized_view(_mv_schema(), "gold", "fact_orders")

        assert executed is True
        assert [c[0] for c in b._mv_calls] == ["create"]
        ddl = b._materialized_views["gold.fact_orders"]
        assert ddl.startswith("CREATE MATERIALIZED VIEW")
        assert "SUM(`amount`) AS `total`" in ddl
        assert b._written == {}  # no DataFrame was ever built or written

    def test_unchanged_definition_only_refreshes(self):
        b = _backend_with_orders()
        b._missing_tables.add("gold.fact_orders")
        engine = _make_engine(b, params={"sql_warehouse_id": "wh-1"})

        engine._create_materialized_view(_mv_schema(), "gold", "fact_orders")
        engine._create_materialized_view(_mv_schema(), "gold", "fact_orders")

        assert [c[0] for c in b._mv_calls] == ["create", "refresh"]

    def test_changed_definition_replaces(self):
        b = _backend_with_orders()
        b._missing_tables.add("gold.fact_orders")
        engine = _make_engine(b, params={"sql_warehouse_id": "wh-1"})

        engine._create_materialized_view(_mv_schema(), "gold", "fact_orders")
        changed = _mv_schema()
        changed["aggregate"]["measures"].append(
            {"source": "amount", "target": "avg_amount", "func": "avg"}
        )
        engine._create_materialized_view(changed, "gold", "fact_orders")

        assert [c[0] for c in b._mv_calls] == ["create", "create"]
        ddl = b._materialized_views["gold.fact_orders"]
        assert ddl.startswith("CREATE OR REPLACE MATERIALIZED VIEW")
        assert "AVG(`amount`) AS `avg_amount`" in ddl

    def test_schedule_change_alone_triggers_replace(self):
        b = _backend_with_orders()
        b._missing_tables.add("gold.fact_orders")
        engine = _make_engine(b, params={"sql_warehouse_id": "wh-1"})

        engine._create_materialized_view(
            _mv_schema(schedule="EVERY 6 HOURS"), "gold", "fact_orders"
        )
        engine._create_materialized_view(
            _mv_schema(schedule="EVERY 12 HOURS"), "gold", "fact_orders"
        )

        assert [c[0] for c in b._mv_calls] == ["create", "create"]
        assert "SCHEDULE EVERY 12 HOURS" in b._materialized_views["gold.fact_orders"]

    def test_refresh_never_skips_the_refresh(self):
        b = _backend_with_orders()
        b._missing_tables.add("gold.fact_orders")
        engine = _make_engine(b, params={"sql_warehouse_id": "wh-1"})
        schema = _mv_schema(refresh="never", schedule="EVERY 6 HOURS")

        engine._create_materialized_view(schema, "gold", "fact_orders")
        executed = engine._create_materialized_view(schema, "gold", "fact_orders")

        assert [c[0] for c in b._mv_calls] == ["create"]  # no second call at all
        assert executed is True

    def test_unreadable_hash_falls_back_to_replace(self):
        """An existing view whose property cannot be read is replaced, never left stale."""
        b = _backend_with_orders()  # table_exists → True, no stored property
        engine = _make_engine(b, params={"sql_warehouse_id": "wh-1"})

        engine._create_materialized_view(_mv_schema(), "gold", "fact_orders")

        assert [c[0] for c in b._mv_calls] == ["create"]
        assert b._materialized_views["gold.fact_orders"].startswith("CREATE OR REPLACE")

    def test_uncompilable_schema_raises(self):
        """business_rules reach the compiler only via hand-built dicts — still refused."""
        from skifer.core.sql_compiler import SqlCompilationError

        b = _backend_with_orders()
        engine = _make_engine(b, params={"sql_warehouse_id": "wh-1"})
        schema = _mv_schema()
        schema["business_rules"] = ["flag_high_value"]

        with pytest.raises(SqlCompilationError):
            engine._create_materialized_view(schema, "gold", "fact_orders")
        assert b._mv_calls == []


class TestSqlArtifactGeneration:
    def test_without_warehouse_the_sql_is_written_not_executed(self, tmp_path):
        b = _backend_with_orders()
        engine = _make_engine(b, params={"sql_output_dir": str(tmp_path)})

        executed = engine._create_materialized_view(_mv_schema(), "gold", "fact_orders")

        assert executed is False
        assert b._mv_calls == []
        artifact = tmp_path / "gold_fact_orders.sql"
        assert artifact.exists()
        content = artifact.read_text()
        assert content.startswith("CREATE OR REPLACE MATERIALIZED VIEW")
        assert "SUM(`amount`) AS `total`" in content

    def test_warning_names_the_missing_config_key(self, tmp_path, caplog):
        engine = _make_engine(_backend_with_orders(), params={"sql_output_dir": str(tmp_path)})
        with caplog.at_level("WARNING"):
            engine._create_materialized_view(_mv_schema(), "gold", "fact_orders")
        assert "sql_warehouse_id" in caplog.text
        assert "NOT CREATED" in caplog.text


class TestPatternWiring:
    def test_run_process_to_table_short_circuits(self):
        b = _backend_with_orders()
        b._missing_tables.add("gold.fact_orders")
        engine = _make_engine(b, params={"sql_warehouse_id": "wh-1"})

        engine.run_process_to_table(_mv_schema(), "gold", "fact_orders")

        assert [c[0] for c in b._mv_calls] == ["create"]
        assert b._written == {}
        assert b._streams == {}

    def test_monitor_runs_only_when_the_ddl_executed(self, tmp_path):
        class _RecordingMonitor:
            def __init__(self):
                self.calls = []

            def check_from_schema(self, fqn, schema, raise_on_critical=True):
                self.calls.append(fqn)
                raise AssertionError("monitor must not run without a materialized view")

        engine = _make_engine(_backend_with_orders(), params={"sql_output_dir": str(tmp_path)})
        engine.monitor = _RecordingMonitor()

        engine.run_process_to_table(_mv_schema(), "gold", "fact_orders")

        assert engine.monitor.calls == []

    def test_process_and_split_refuses(self):
        engine = _make_engine(_backend_with_orders(), params={"sql_warehouse_id": "wh-1"})
        with pytest.raises(NotImplementedError) as exc:
            engine.run_process_and_split(_mv_schema(), [{"label": "fr", "value": "FR"}],
                                         "gold", "fact_orders", "country")
        assert "materialized view" in str(exc.value)

    def test_union_sources_refuses(self):
        engine = _make_engine(_backend_with_orders(), params={"sql_warehouse_id": "wh-1"})
        with pytest.raises(NotImplementedError) as exc:
            engine.run_union_sources_to_table(
                _mv_schema(), [{"label": "fr"}], "silver", "gold", "fact_orders",
                ["orders"], "ord",
            )
        assert "materialized view" in str(exc.value)

    def test_write_dataframe_guard(self):
        """Third barrier: a DataFrame can never be written under an MV materialization."""
        b = _backend_with_orders()
        engine = _make_engine(b, params={"sql_warehouse_id": "wh-1"})
        with pytest.raises(ValueError) as exc:
            engine._write_dataframe(
                b.read_table("silver.orders"), "`gold`.`fact_orders`", "fact_orders",
                materialization={"type": "materialized_view"},
            )
        assert "defined by SQL" in str(exc.value)
        assert b._written == {}


class TestFullRefresh:
    def test_drops_the_view_on_databricks(self):
        b = _backend_with_orders()
        engine = _make_engine(b, params={"sql_warehouse_id": "wh-1"})
        engine.full_refresh("gold", "fact_orders", materialization="materialized_view")
        assert b._mv_calls == [("drop", "gold.fact_orders")]
        assert b._dropped == []

    def test_drops_the_table_in_local_mode(self):
        b = _backend_with_orders()
        engine = _make_engine(b, is_local=True)
        engine.full_refresh("gold", "fact_orders", materialization="materialized_view")
        assert b._dropped == ["`gold`.`fact_orders`"]
        assert b._mv_calls == []

    def test_streaming_remains_the_default(self, tmp_path):
        b = _backend_with_orders()
        engine = _make_engine(b, is_local=True)
        engine.full_refresh("gold", "events", checkpoint=str(tmp_path / "ckpt"))
        assert b._dropped == ["`gold`.`events`"]
        assert b._mv_calls == []


class TestDatabricksSdkPackaging:
    """Plan 28.5 — the SDK is an optional extra; its absence must be diagnosable."""

    @staticmethod
    def _block_sdk(monkeypatch):
        import sys
        monkeypatch.setitem(sys.modules, "databricks", None)
        monkeypatch.setitem(sys.modules, "databricks.sdk", None)

    @staticmethod
    def _fake_sdk(monkeypatch):
        import sys
        import types
        pkg = types.ModuleType("databricks")
        sdk = types.ModuleType("databricks.sdk")
        sdk.WorkspaceClient = lambda **kwargs: object()
        pkg.sdk = sdk
        monkeypatch.setitem(sys.modules, "databricks", pkg)
        monkeypatch.setitem(sys.modules, "databricks.sdk", sdk)

    def test_availability_probe(self, monkeypatch):
        from skifer.core import environment

        self._block_sdk(monkeypatch)
        assert environment.is_databricks_sdk_available() is False
        self._fake_sdk(monkeypatch)
        assert environment.is_databricks_sdk_available() is True

    def test_missing_sdk_is_logged_not_silent(self, monkeypatch, caplog):
        """A missing SDK used to look exactly like missing credentials."""
        from skifer.core import environment

        self._block_sdk(monkeypatch)
        monkeypatch.setenv("DATABRICKS_HOST", "https://example.cloud.databricks.com")
        monkeypatch.setenv("DATABRICKS_TOKEN", "dapi-fake")
        with caplog.at_level("WARNING"):
            assert environment.get_workspace_client() is None
        assert "skifer[databricks]" in caplog.text

    def test_warehouse_error_points_at_the_extra(self, monkeypatch):
        from skifer.core.spark_backend import SparkBackend

        self._block_sdk(monkeypatch)
        backend = object.__new__(SparkBackend)
        backend._workspace_client_cache = None
        monkeypatch.setattr(backend, "_get_workspace_client", lambda: None)
        with pytest.raises(RuntimeError) as exc:
            backend.execute_sql_on_warehouse("SELECT 1", "wh-1")
        assert "skifer[databricks]" in str(exc.value)

    def test_warehouse_error_points_at_credentials_when_sdk_present(self, monkeypatch):
        from skifer.core.spark_backend import SparkBackend

        self._fake_sdk(monkeypatch)
        backend = object.__new__(SparkBackend)
        backend._workspace_client_cache = None
        monkeypatch.setattr(backend, "_get_workspace_client", lambda: None)
        with pytest.raises(RuntimeError) as exc:
            backend.execute_sql_on_warehouse("SELECT 1", "wh-1")
        assert "DATABRICKS_TOKEN" in str(exc.value)
        assert "skifer[databricks]" not in str(exc.value)

    def test_extra_is_declared_in_pyproject(self):
        import pathlib
        content = (pathlib.Path(__file__).parents[1] / "pyproject.toml").read_text()
        assert "databricks = [" in content
        assert "databricks-sdk" in content


# ==============================================================================
# Part 3 — real Spark E2E (decision #20: local mode materializes the SELECT)
# ==============================================================================

def _make_real_engine(spark):
    from skifer.core.spark_backend import SparkBackend
    return _make_engine(SparkBackend(spark=spark, is_local=True), is_local=True)


def _cleanup(spark, db, table):
    warehouse = spark.conf.get("spark.sql.warehouse.dir").replace("file:", "")
    spark.sql(f"DROP TABLE IF EXISTS `{db}`.`{table}`")
    spark.sql(f"DROP DATABASE IF EXISTS {db}")
    shutil.rmtree(os.path.join(warehouse, db), ignore_errors=True)


def test_e2e_materialized_view_local(spark):
    """Full engine path: YAML → compile → spark.sql → Delta table with the right rows."""
    spark.sql("CREATE DATABASE IF NOT EXISTS mv_e2e_src")
    spark.createDataFrame(
        [("FR", 100, "DONE"), ("FR", 50, "DONE"), ("DE", 30, "DONE"), ("FR", 999, "DRAFT")],
        ["country", "amount", "status"],
    ).write.format("delta").mode("overwrite").saveAsTable("`mv_e2e_src`.`orders`")

    yaml_schema = """
materialization:
  type: materialized_view
  comment: "Revenue per country"
  schedule: "EVERY 6 HOURS"

tables:
  - name: mv_e2e_src.orders
    alias: ord
    filter:
      - "status:equals:DONE"

aggregate:
  group_by: [country]
  measures:
    - [amount, total_amount, sum]
    - [amount, nb_orders, count]
  having:
    - "total_amount:greater_than:50"
"""
    engine = _make_real_engine(spark)
    _cleanup(spark, "mv_e2e", "revenue_by_country")
    try:
        engine.run_process_to_table(parse_schema(yaml_schema), "mv_e2e", "revenue_by_country")

        rows = {r["country"]: r for r in spark.table("`mv_e2e`.`revenue_by_country`").collect()}
        assert set(rows) == {"FR"}  # DE totals 30, filtered out by HAVING
        assert rows["FR"]["total_amount"] == 150  # DRAFT row excluded by the table filter
        assert rows["FR"]["nb_orders"] == 2
    finally:
        _cleanup(spark, "mv_e2e", "revenue_by_country")
        spark.sql("DROP TABLE IF EXISTS `mv_e2e_src`.`orders`")
        spark.sql("DROP DATABASE IF EXISTS mv_e2e_src")


def test_e2e_materialized_view_local_rerun_is_idempotent(spark):
    """Re-running an MV locally overwrites the table instead of appending."""
    spark.sql("CREATE DATABASE IF NOT EXISTS mv_e2e_src2")
    spark.createDataFrame([("FR", 10), ("DE", 20)], ["country", "amount"]) \
        .write.format("delta").mode("overwrite").saveAsTable("`mv_e2e_src2`.`orders`")

    yaml_schema = """
materialization: materialized_view
tables:
  - name: mv_e2e_src2.orders
    alias: ord
aggregate:
  group_by: [country]
  measures:
    - [amount, total_amount, sum]
"""
    engine = _make_real_engine(spark)
    _cleanup(spark, "mv_e2e2", "revenue")
    try:
        engine.run_process_to_table(parse_schema(yaml_schema), "mv_e2e2", "revenue")
        engine.run_process_to_table(parse_schema(yaml_schema), "mv_e2e2", "revenue")
        assert spark.table("`mv_e2e2`.`revenue`").count() == 2
    finally:
        _cleanup(spark, "mv_e2e2", "revenue")
        spark.sql("DROP TABLE IF EXISTS `mv_e2e_src2`.`orders`")
        spark.sql("DROP DATABASE IF EXISTS mv_e2e_src2")
