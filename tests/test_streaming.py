"""
Plan 27 — engine wiring tests for streaming tables.

Part 1: FakeBackend (no Spark) — dispatch, checkpoint resolution, preflight,
        pattern refusals, full_refresh.
Part 2: real local Spark session (marker ``streaming``) — end-to-end
        incrementality and CDC Type 1 upsert through the full engine path.
"""
import os
import shutil

import pytest

from skifer.core.core import SkiferEngine
from skifer.core.context import ExecutionContext
from skifer.core.interpreter import SchemaInterpreter
from skifer.core.patterns import PipelinePatterns
from skifer.core.registry import RuleRegistry
from skifer.core.schema_loader import parse_schema
from tests.fakes.fake_backend import FakeBackend


def _make_engine(backend):
    """Minimal engine with an injected backend (model: tests/test_partials.py)."""
    engine = object.__new__(SkiferEngine)
    ctx = ExecutionContext()
    object.__setattr__(engine, "_context", ctx)
    engine.spark = getattr(backend, "spark", None)
    engine.is_local = backend.is_local
    engine.db = None
    engine.env = "local"
    engine.config = {"environments": {"local": {}}}
    engine.schema_suffix = ""
    engine.is_job_execution = False
    engine._backend = backend
    engine._interpreter = SchemaInterpreter(backend=backend, context=ctx)
    engine._patterns = PipelinePatterns(engine=engine)
    engine.monitor = None
    return engine


def _streaming_schema(**mat_overrides):
    mat = {"type": "streaming_table", "trigger": "available_now", "checkpoint": "auto",
           "write_mode": "append", **mat_overrides}
    return {
        "materialization": mat,
        "tables": [
            {"name": "bronze.events", "alias": "ev", "streaming": True},
            {"name": "silver.dim", "alias": "dim"},
        ],
        "join": [{"table_from": "ev", "on_from": "country_id",
                  "table_to": "dim", "on_to": "country_id", "type": "left"}],
        "keep_all_columns": True,
    }


# ==============================================================================
# Part 1 — FakeBackend wiring
# ==============================================================================

class TestStreamingDispatch:
    def test_streaming_table_read_via_read_table_stream(self):
        b = FakeBackend(tables={
            "bronze.events": [{"id": 1, "country_id": 10}],
            "silver.dim": [{"country_id": 10, "country": "FR"}],
        })
        engine = _make_engine(b)
        df = engine.process_schema(_streaming_schema())
        assert df.is_streaming is True

    def test_run_process_to_table_records_stream_write(self):
        b = FakeBackend(tables={
            "bronze.events": [{"id": 1, "country_id": 10}],
            "silver.dim": [{"country_id": 10, "country": "FR"}],
        })
        engine = _make_engine(b)
        engine.run_process_to_table(_streaming_schema(), "silver", "events_clean")

        assert b._written == {}  # no batch write happened
        fqn = "`silver`.`events_clean`"
        assert fqn in b._streams
        stream = b._streams[fqn]
        assert stream["trigger"] == "available_now"
        assert stream["write_mode"] == "append"
        assert stream["checkpoint"] == "/fake/_checkpoints/silver/events_clean"

    def test_checkpoint_auto_includes_sandbox_suffix(self):
        b = FakeBackend(tables={
            "bronze.events": [{"id": 1, "country_id": 10}],
            "silver.dim": [{"country_id": 10, "country": "FR"}],
        })
        engine = _make_engine(b)
        engine.schema_suffix = "_jdoe"
        # Source sandbox resolution would try to clone; keep sources pre-loaded instead.
        engine.is_job_execution = True  # skip source sandbox resolve, keep target suffix
        engine.run_process_to_table(_streaming_schema(), "silver", "events_clean")
        fqn = "`silver_jdoe`.`events_clean`"
        assert fqn in b._streams
        assert b._streams[fqn]["checkpoint"] == "/fake/_checkpoints/silver_jdoe/events_clean"

    def test_explicit_checkpoint_used_verbatim(self):
        b = FakeBackend(tables={
            "bronze.events": [{"id": 1, "country_id": 10}],
            "silver.dim": [{"country_id": 10, "country": "FR"}],
        })
        engine = _make_engine(b)
        schema = _streaming_schema(checkpoint="/explicit/ckpt")
        engine.run_process_to_table(schema, "silver", "events_clean")
        assert b._streams["`silver`.`events_clean`"]["checkpoint"] == "/explicit/ckpt"

    def test_checkpoint_auto_without_root_fails_fast(self):
        class NoRootBackend(FakeBackend):
            def default_checkpoint_root(self):
                return None  # Databricks behavior

        b = NoRootBackend(tables={"bronze.events": [{"id": 1}]})
        engine = _make_engine(b)
        with pytest.raises(ValueError, match="checkpoint_base"):
            engine.resolve_checkpoint_location("silver", "events_clean", {"checkpoint": "auto"})

    def test_checkpoint_base_param_used_on_databricks(self):
        class NoRootBackend(FakeBackend):
            def default_checkpoint_root(self):
                return None

        b = NoRootBackend(tables={"bronze.events": [{"id": 1}]})
        engine = _make_engine(b)
        engine.config = {"environments": {"local": {"params": {"checkpoint_base": "/Volumes/ckpt"}}}}
        path = engine.resolve_checkpoint_location("silver", "events_clean", {"checkpoint": "auto"})
        assert path == "/Volumes/ckpt/silver/events_clean"

    def test_upsert_write_mode_and_keys_forwarded(self):
        b = FakeBackend(tables={
            "bronze.events": [
                {"id": 1, "country_id": 10},
                {"id": 1, "country_id": 10},  # duplicate key
                {"id": 2, "country_id": 10},
            ],
            "silver.dim": [{"country_id": 10, "country": "FR"}],
        })
        engine = _make_engine(b)
        schema = _streaming_schema(write_mode="upsert", keys=["id"])
        engine.run_process_to_table(schema, "silver", "events_clean")
        stream = b._streams["`silver`.`events_clean`"]
        assert stream["write_mode"] == "upsert"
        assert stream["keys"] == ["id"]
        assert len(stream["rows"]) == 2  # last-write-wins per key


class TestStreamingPreflight:
    def test_intermediate_mode_table_rejected(self):
        b = FakeBackend(tables={"bronze.events": [{"id": 1}]})
        engine = _make_engine(b)
        schema = {
            "materialization": {"type": "streaming_table", "checkpoint": "/c",
                                "trigger": "available_now", "write_mode": "append"},
            "tables": [{"name": "bronze.events", "streaming": True}],
            "keep_all_columns": True,
        }
        with pytest.raises(ValueError, match="intermediate_mode='table'"):
            engine.process_schema(schema, intermediate_mode="table")

    def test_aggregation_rule_rejected_at_run(self):
        @RuleRegistry.register_rule(name="_test_stream_agg", kind="aggregation")
        def _agg_rule(df):
            return df
        try:
            b = FakeBackend(tables={"bronze.events": [{"id": 1}]})
            engine = _make_engine(b)
            schema = {
                "materialization": {"type": "streaming_table", "checkpoint": "/c",
                                    "trigger": "available_now", "write_mode": "append"},
                "tables": [{"name": "bronze.events", "streaming": True}],
                "business_rules": ["_test_stream_agg"],
                "keep_all_columns": True,
            }
            with pytest.raises(ValueError, match="aggregation rule"):
                engine.process_schema(schema)
        finally:
            RuleRegistry._rules.pop("_test_stream_agg", None)

    def test_raw_dict_dev_limit_rejected_at_run(self):
        """Hand-built dicts bypass load_schema — the interpreter re-checks."""
        b = FakeBackend(tables={"bronze.events": [{"id": 1}]})
        engine = _make_engine(b)
        schema = {
            "tables": [{"name": "bronze.events", "streaming": True, "dev_limit": 10}],
            "keep_all_columns": True,
        }
        with pytest.raises(ValueError, match="dev_limit"):
            engine.process_schema(schema)


class TestStreamingPatternRefusals:
    def test_run_process_and_split_refuses_streaming(self):
        b = FakeBackend(tables={"bronze.events": [{"id": 1, "r": "A"}]})
        engine = _make_engine(b)
        schema = {"materialization": {"type": "streaming_table"},
                  "tables": [{"name": "bronze.events", "streaming": True}]}
        with pytest.raises(NotImplementedError, match="streaming pivot table"):
            engine.run_process_and_split(schema, [{"label": "a", "value": "A"}], "silver", "t", "r")

    def test_run_union_sources_refuses_streaming(self):
        b = FakeBackend(tables={})
        engine = _make_engine(b)
        schema = {"materialization": {"type": "streaming_table"}, "tables": []}
        with pytest.raises(NotImplementedError, match="materialized-table use case"):
            engine.run_union_sources_to_table(schema, [], "bronze", "silver", "t", [], "s")


class TestFullRefresh:
    def test_full_refresh_purges_checkpoint_and_drops_table(self, tmp_path):
        ckpt_dir = tmp_path / "silver" / "events_clean"
        ckpt_dir.mkdir(parents=True)
        (ckpt_dir / "offsets").write_text("0")

        class TmpRootBackend(FakeBackend):
            def default_checkpoint_root(self):
                return str(tmp_path)

        b = TmpRootBackend(tables={})
        engine = _make_engine(b)
        engine.full_refresh("silver", "events_clean")

        assert not ckpt_dir.exists()
        assert "`silver`.`events_clean`" in b._dropped


# ==============================================================================
# Part 2 — real Spark E2E (marker: streaming)
# ==============================================================================

def _make_real_engine(spark):
    from skifer.core.spark_backend import SparkBackend
    return _make_engine(SparkBackend(spark=spark, is_local=True))


def _cleanup(spark, db, table):
    warehouse = spark.conf.get("spark.sql.warehouse.dir").replace("file:", "")
    spark.sql(f"DROP TABLE IF EXISTS `{db}`.`{table}`")
    spark.sql(f"DROP DATABASE IF EXISTS {db}")
    shutil.rmtree(os.path.join(warehouse, db), ignore_errors=True)


@pytest.mark.streaming
def test_e2e_streaming_incremental_two_runs(spark, tmp_path):
    """Full engine path: YAML → parse → run 1 backfill → append → run 2 delta only."""
    src = str(tmp_path / "src")
    ckpt = str(tmp_path / "ckpt")
    spark.createDataFrame(
        [(1, "EMEA"), (2, "APAC")], ["id", "region"]
    ).write.format("delta").save(src)

    yaml_schema = f"""
materialization:
  type: streaming_table
  checkpoint: "{ckpt}"
tables:
  - name: raw_events
    alias: ev
    streaming: true
    source:
      type: delta
      path: "{src}"
keep_all_columns: true
"""
    engine = _make_real_engine(spark)
    _cleanup(spark, "stream_e2e", "events_clean")
    try:
        schema = parse_schema(yaml_schema)
        engine.run_process_to_table(schema, "stream_e2e", "events_clean")
        rows = spark.table("`stream_e2e`.`events_clean`").collect()
        assert sorted(r["id"] for r in rows) == [1, 2]

        spark.createDataFrame([(3, "AMER")], ["id", "region"]) \
            .write.format("delta").mode("append").save(src)
        engine.run_process_to_table(parse_schema(yaml_schema), "stream_e2e", "events_clean")
        rows2 = spark.table("`stream_e2e`.`events_clean`").collect()
        assert sorted(r["id"] for r in rows2) == [1, 2, 3]  # append, no reprocessing
    finally:
        _cleanup(spark, "stream_e2e", "events_clean")


@pytest.mark.streaming
def test_e2e_streaming_upsert_two_runs(spark, tmp_path):
    """Full engine path with CDC Type 1: duplicate keys update instead of duplicating."""
    src = str(tmp_path / "src")
    ckpt = str(tmp_path / "ckpt")
    spark.createDataFrame(
        [(1, "pending"), (2, "pending")], ["order_id", "status"]
    ).write.format("delta").save(src)

    yaml_schema = f"""
materialization:
  type: streaming_table
  checkpoint: "{ckpt}"
  write_mode: upsert
  keys: [order_id]
tables:
  - name: raw_orders
    alias: ord
    streaming: true
    source:
      type: delta
      path: "{src}"
keep_all_columns: true
"""
    engine = _make_real_engine(spark)
    _cleanup(spark, "stream_e2e_up", "orders_current")
    try:
        engine.run_process_to_table(parse_schema(yaml_schema), "stream_e2e_up", "orders_current")
        rows = {r["order_id"]: r["status"] for r in spark.table("`stream_e2e_up`.`orders_current`").collect()}
        assert rows == {1: "pending", 2: "pending"}

        # Order 1 changes status — same key must update, not duplicate.
        spark.createDataFrame([(1, "shipped"), (3, "pending")], ["order_id", "status"]) \
            .write.format("delta").mode("append").save(src)
        engine.run_process_to_table(parse_schema(yaml_schema), "stream_e2e_up", "orders_current")
        rows2 = {r["order_id"]: r["status"] for r in spark.table("`stream_e2e_up`.`orders_current`").collect()}
        assert rows2 == {1: "shipped", 2: "pending", 3: "pending"}
    finally:
        _cleanup(spark, "stream_e2e_up", "orders_current")
