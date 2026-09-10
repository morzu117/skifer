"""
Tests for SparkBackend.
"""
import os

import pytest
from unittest.mock import MagicMock
from skifer.core.spark_backend import SparkBackend
from skifer.core.constants import VALID_SOURCE_TYPES


@pytest.fixture
def spark(request):
    """Try to use the shared local Spark session from conftest."""
    try:
        return request.getfixturevalue("spark")
    except Exception:
        pytest.skip("No active SparkContext available")


@pytest.fixture
def mock_spark():
    return MagicMock()


@pytest.fixture
def backend(mock_spark):
    return SparkBackend(spark=mock_spark, is_local=True)


def test_spark_backend_is_local(backend):
    assert backend.is_local is True


def test_spark_backend_build_fqn_with_catalog(backend):
    fqn = backend.build_fqn("my_catalog", "silver", "orders")
    assert fqn == "`my_catalog`.`silver`.`orders`"


def test_spark_backend_build_fqn_without_catalog(backend):
    fqn = backend.build_fqn(None, "silver", "orders")
    assert fqn == "`silver`.`orders`"


def test_spark_backend_col(spark):
    """col() should call F.col with backtick-quoted name (requires active SparkContext)."""
    b = SparkBackend(spark=spark, is_local=True)
    result = b.col("my_column")
    assert result is not None


def test_spark_backend_lit(spark):
    """lit() should call F.lit (requires active SparkContext)."""
    b = SparkBackend(spark=spark, is_local=True)
    result = b.lit(42)
    assert result is not None


def test_spark_backend_execute_sql(backend):
    backend.execute_sql("SELECT 1")
    backend.spark.sql.assert_called_once_with("SELECT 1")


def test_spark_backend_sql_alias(backend):
    backend.sql("SELECT 1")
    backend.spark.sql.assert_called_once_with("SELECT 1")


def test_get_certification_contract_is_bounded_and_escapes_lookup_values(backend):
    row = MagicMock()
    row.asDict.return_value = {"contract_id": "sales'orders"}
    backend.spark.catalog.tableExists.return_value = True
    backend.spark.sql.return_value.collect.return_value = [row]

    result = backend.get_certification_contract(
        "certification", "sales'orders", "1.0.0'rc"
    )

    assert result == {"contract_id": "sales'orders"}
    statement = backend.spark.sql.call_args.args[0]
    assert "sales''orders" in statement
    assert "1.0.0''rc" in statement
    assert statement.endswith("LIMIT 2")


def test_spark_backend_read_table(backend):
    backend.read_table("`silver`.`orders`")
    backend.spark.table.assert_called_once_with("`silver`.`orders`")


def test_spark_backend_drop_table_calls_sql(backend):
    backend.drop_table("`silver`.`orders`")
    backend.spark.sql.assert_called()


def test_spark_backend_table_exists_uses_show_tables_not_show_columns(backend):
    backend.spark.sql.return_value.collect.return_value = [{"tableName": "orders"}]

    assert backend.table_exists(None, "silver", "orders") is True

    sql = backend.spark.sql.call_args[0][0]
    assert sql == "SHOW TABLES IN `silver` LIKE 'orders'"
    assert "SHOW COLUMNS" not in sql


def test_spark_backend_table_exists_false_when_show_tables_returns_no_match(backend):
    backend.spark.sql.return_value.collect.return_value = []

    assert backend.table_exists(None, "silver_jdoe", "orders") is False

    sql = backend.spark.sql.call_args[0][0]
    assert sql == "SHOW TABLES IN `silver_jdoe` LIKE 'orders'"
    backend.spark.catalog.tableExists.assert_not_called()


def test_spark_backend_table_exists_remote_preserves_catalog():
    mock_spark = MagicMock()
    mock_spark.sql.return_value.collect.return_value = [{"tableName": "dim_job"}]
    b = SparkBackend(spark=mock_spark, is_local=False)

    assert b.table_exists("demo_catalog", "silver_jdoe", "dim_job") is True

    mock_spark.sql.assert_called_once_with(
        "SHOW TABLES IN `demo_catalog`.`silver_jdoe` LIKE 'dim_job'"
    )


def test_spark_backend_table_exists_falls_back_to_temp_view_on_catalog_error(backend):
    backend.spark.sql.side_effect = Exception("schema not found")
    backend.spark.catalog.tableExists.return_value = True

    assert backend.table_exists(None, "silver", "orders") is True
    backend.spark.catalog.tableExists.assert_called_once_with("orders")


def test_spark_backend_ensure_schema_exists_local(backend):
    """In local mode, ensure_schema_exists should run CREATE DATABASE."""
    backend.ensure_schema_exists("silver_jdoe")
    backend.spark.sql.assert_called_with("CREATE DATABASE IF NOT EXISTS `silver_jdoe`")


def test_spark_backend_ensure_schema_exists_local_with_catalog(backend):
    """In local mode, catalog prefix must be stripped — spark_catalog requires single-part namespace."""
    backend.ensure_schema_exists("default.silver_jdoe")
    backend.spark.sql.assert_called_with(
        "CREATE DATABASE IF NOT EXISTS `silver_jdoe`"
    )


def test_spark_backend_ensure_schema_exists_remote():
    """In remote mode, ensure_schema_exists should run CREATE SCHEMA with full FQN."""
    mock_spark = MagicMock()
    b = SparkBackend(spark=mock_spark, is_local=False)
    b.ensure_schema_exists("demo_catalog.silver_jdoe")
    mock_spark.sql.assert_called_with(
        "CREATE SCHEMA IF NOT EXISTS `demo_catalog`.`silver_jdoe`"
    )


def test_spark_backend_filter(backend):
    df = MagicMock()
    cond = MagicMock()
    backend.filter(df, cond)
    df.filter.assert_called_once_with(cond)


def test_spark_backend_limit(backend):
    df = MagicMock()
    backend.limit(df, 100)
    df.limit.assert_called_once_with(100)


def test_spark_backend_drop_duplicates_with_cols(backend):
    df = MagicMock()
    backend.drop_duplicates(df, ["id", "name"])
    df.dropDuplicates.assert_called_once_with(["id", "name"])


def test_spark_backend_drop_duplicates_without_cols(backend):
    df = MagicMock()
    backend.drop_duplicates(df)
    df.dropDuplicates.assert_called_once_with()


def test_spark_backend_drop_nulls(backend):
    df = MagicMock()
    backend.drop_nulls(df, ["amount", "customer_id"])
    df.dropna.assert_called_once_with(subset=["amount", "customer_id"])


def test_spark_backend_with_column(backend):
    df = MagicMock()
    col = MagicMock()
    backend.with_column(df, "new_col", col)
    df.withColumn.assert_called_once_with("new_col", col)


def test_spark_backend_count(backend):
    df = MagicMock()
    df.count.return_value = 42
    assert backend.count(df) == 42


def test_spark_backend_is_empty(backend):
    df = MagicMock()
    df.isEmpty.return_value = False
    assert backend.is_empty(df) is False


def test_spark_backend_check_catalog_access_null(backend):
    """Null catalog should always return True."""
    assert backend.check_catalog_access(None) is True
    assert backend.check_catalog_access("") is True


def test_spark_backend_check_catalog_access_success(backend):
    backend.spark.sql.return_value.limit.return_value.collect.return_value = [1]
    result = backend.check_catalog_access("my_catalog")
    assert result is True


def test_spark_backend_optimize_table_native(backend):
    """optimize_table should run OPTIMIZE SQL."""
    backend.optimize_table("`silver`.`orders`")
    backend.spark.sql.assert_called_with("OPTIMIZE `silver`.`orders`")


def test_spark_backend_optimize_table_zorder(backend):
    backend.optimize_table("`silver`.`orders`", zorder_cols=["date", "country"])
    backend.spark.sql.assert_called_with("OPTIMIZE `silver`.`orders` ZORDER BY (date, country)")


def test_spark_backend_clone_table_local_uses_ctas():
    """In local mode, clone_table uses CREATE TABLE AS SELECT."""
    mock_spark = MagicMock()
    b = SparkBackend(spark=mock_spark, is_local=True)
    b.clone_table(None, "silver", "orders", None, "silver_sandbox", "orders")
    sql = mock_spark.sql.call_args[0][0]
    assert "CREATE TABLE IF NOT EXISTS" in sql
    assert "SELECT * FROM" in sql
    assert "SHALLOW CLONE" not in sql


def test_spark_backend_clone_table_databricks_uses_shallow_clone():
    """In Databricks mode (non-local), clone_table uses SHALLOW CLONE."""
    mock_spark = MagicMock()
    b = SparkBackend(spark=mock_spark, is_local=False)
    b.clone_table("cat", "silver", "orders", "cat", "silver_sandbox", "orders")
    sql = mock_spark.sql.call_args[0][0]
    assert "SHALLOW CLONE" in sql


def test_spark_backend_clone_table_databricks_fallback_to_ctas():
    """When SHALLOW CLONE raises, falls back to CTAS."""
    mock_spark = MagicMock()
    mock_spark.sql.side_effect = [Exception("SHALLOW CLONE not supported"), MagicMock()]
    b = SparkBackend(spark=mock_spark, is_local=False)
    b.clone_table(None, "silver", "orders", None, "silver_sandbox", "orders")
    calls = [c[0][0] for c in mock_spark.sql.call_args_list]
    assert any("SELECT * FROM" in s for s in calls)


# ------------------------------------------------------------------
# read_source tests
# ------------------------------------------------------------------

def test_read_source_csv_calls_spark_read(backend):
    """read_source csv should call spark.read.format('csv').options(...).load(path)."""
    mock_reader = MagicMock()
    backend.spark.read.format.return_value = mock_reader
    mock_reader.options.return_value = mock_reader

    backend.read_source("csv", "/data/orders/*.csv", {"header": "true"})

    backend.spark.read.format.assert_called_once_with("csv")
    mock_reader.options.assert_called_once_with(header="true")
    mock_reader.load.assert_called_once_with("/data/orders/*.csv")


def test_read_source_parquet_no_options(backend):
    """read_source with no options should not call .options()."""
    mock_reader = MagicMock()
    backend.spark.read.format.return_value = mock_reader

    backend.read_source("parquet", "/data/orders/")

    backend.spark.read.format.assert_called_once_with("parquet")
    mock_reader.options.assert_not_called()
    mock_reader.load.assert_called_once_with("/data/orders/")


def test_read_source_empty_options(backend):
    """read_source with empty dict options should not call .options()."""
    mock_reader = MagicMock()
    backend.spark.read.format.return_value = mock_reader

    backend.read_source("json", "/data/events.json", {})

    mock_reader.options.assert_not_called()
    mock_reader.load.assert_called_once_with("/data/events.json")


def test_read_source_all_valid_types(backend):
    """All declared valid types should not raise ValueError."""
    mock_reader = MagicMock()
    backend.spark.read.format.return_value = mock_reader
    for t in sorted(VALID_SOURCE_TYPES):
        backend.read_source(t, "/some/path")


def test_read_source_invalid_type_raises(backend):
    """Unknown source type should raise ValueError with helpful message."""
    with pytest.raises(ValueError, match="Unknown source type 'xlsx'"):
        backend.read_source("xlsx", "/data/file.xlsx")


def test_read_source_invalid_type_jdbc_raises(backend):
    """JDBC is explicitly not supported in this plan."""
    with pytest.raises(ValueError, match="Unknown source type 'jdbc'"):
        backend.read_source("jdbc", "jdbc://host/db")


def test_when_chain_single(backend, spark):
    """when_chain with one condition produces a PySpark conditional column."""
    from pyspark.sql import functions as F
    df = spark.createDataFrame([(1,), (2,)], ["x"])
    cond = F.col("x") == 1
    val = F.lit("one")
    default = F.lit("other")
    result_col = backend.when_chain([(cond, val)], default)
    rows = {r["x"]: r["out"] for r in df.withColumn("out", result_col).collect()}
    assert rows[1] == "one"
    assert rows[2] == "other"


def test_when_chain_multiple(backend, spark):
    """when_chain with two conditions produces correct multi-branch output."""
    from pyspark.sql import functions as F
    df = spark.createDataFrame([(1,), (2,), (3,)], ["x"])
    result_col = backend.when_chain(
        [(F.col("x") == 1, F.lit("one")), (F.col("x") == 2, F.lit("two"))],
        F.lit("other"),
    )
    rows = {r["x"]: r["out"] for r in df.withColumn("out", result_col).collect()}
    assert rows[1] == "one"
    assert rows[2] == "two"
    assert rows[3] == "other"


# ---------------------------------------------------------------------------
# Operators wired by Plan 26 (previously only in the removed core/operations.py)
# ---------------------------------------------------------------------------

def test_build_filter_between(backend, spark):
    """between keeps rows inside the inclusive [lo, hi] range."""
    from skifer.core.ir import ParsedFilter
    df = spark.createDataFrame([(5,), (10,), (15,), (20,), (25,)], ["amount"])
    cond = backend.build_filter(ParsedFilter("amount", "between", "10,20"))
    assert sorted(r["amount"] for r in df.filter(cond).collect()) == [10, 15, 20]


def test_build_filter_not_between(backend, spark):
    """not_between keeps rows strictly outside [lo, hi]."""
    from skifer.core.ir import ParsedFilter
    df = spark.createDataFrame([(5,), (10,), (15,), (20,), (25,)], ["amount"])
    cond = backend.build_filter(ParsedFilter("amount", "not_between", "10,20"))
    assert sorted(r["amount"] for r in df.filter(cond).collect()) == [5, 25]


@pytest.mark.parametrize("operator", ["between", "not_between"])
@pytest.mark.parametrize("value", ["10", "10,20,30"])
# `spark` is unused here on purpose: build_filter() calls F.col(), which needs a live
# JVM context even for the arity error this test asserts.
def test_build_filter_between_invalid_arity_raises(backend, spark, operator, value):
    """between/not_between require exactly 2 values (lo,hi)."""
    from skifer.core.ir import ParsedFilter
    with pytest.raises(ValueError, match="exactly 2 values"):
        backend.build_filter(ParsedFilter("amount", operator, value))


def test_apply_op_ceil(backend, spark):
    """ceil rounds up to the next integer."""
    from pyspark.sql import functions as F
    from skifer.core.ir import _parse_op
    df = spark.createDataFrame([(1.2,), (3.0,)], ["x"])
    col = backend.apply_op(F.col("x"), _parse_op("ceil"))
    rows = [r["out"] for r in df.withColumn("out", col).collect()]
    assert rows == [2, 3]


def test_apply_op_expr_with_comma_survives_ir_parsing(backend, spark):
    """expr: arguments containing commas are passed whole to F.expr."""
    from pyspark.sql import functions as F
    from skifer.core.ir import _parse_op
    df = spark.createDataFrame([("hello",)], ["col1"])
    col = backend.apply_op(F.col("col1"), _parse_op("expr:concat(col1, '_x')"))
    assert df.withColumn("out", col).collect()[0]["out"] == "hello_x"


def test_apply_op_lit_with_comma_survives_ir_parsing(backend, spark):
    """lit: values containing commas are kept whole."""
    from pyspark.sql import functions as F
    from skifer.core.ir import _parse_op
    df = spark.createDataFrame([("a",)], ["col1"])
    col = backend.apply_op(F.col("col1"), _parse_op("lit:x,y"))
    assert df.withColumn("out", col).collect()[0]["out"] == "x,y"


# ---------------------------------------------------------------------------
# Streaming primitives (Plan 27)
# ---------------------------------------------------------------------------

def test_read_table_stream_calls_readstream_table(backend):
    backend.read_table_stream("`bronze`.`events`")
    backend.spark.readStream.table.assert_called_once_with("`bronze`.`events`")


def test_read_source_stream_delta(backend):
    backend.read_source_stream("delta", "/data/events")
    backend.spark.readStream.format.assert_called_once_with("delta")
    backend.spark.readStream.format.return_value.load.assert_called_once_with("/data/events")


def test_read_source_stream_with_options(backend):
    backend.read_source_stream("text", "/data/logs", options={"wholetext": "false"})
    fmt = backend.spark.readStream.format.return_value
    fmt.options.assert_called_once_with(wholetext="false")


@pytest.mark.parametrize("source_type", ["csv", "json", "parquet", "orc", "avro"])
def test_read_source_stream_schemaless_types_rejected(backend, source_type):
    with pytest.raises(ValueError, match="without an explicit schema"):
        backend.read_source_stream(source_type, "/data/x")


def test_write_table_rejects_streaming_dataframe(backend):
    df = MagicMock()
    df.isStreaming = True
    with pytest.raises(ValueError, match="Cannot batch-write a streaming DataFrame"):
        backend.write_table(df, "`silver`.`events`")


def test_write_stream_table_rejects_batch_dataframe(backend):
    df = MagicMock()
    df.isStreaming = False
    with pytest.raises(ValueError, match="is not streaming"):
        backend.write_stream_table(df, "`silver`.`events`", "/ckpt")


def test_write_stream_table_remote_chain(mock_spark):
    """Remote (non-local) path: writeStream...toTable(fqn).awaitTermination()."""
    b = SparkBackend(spark=mock_spark, is_local=False)
    df = MagicMock()
    df.isStreaming = True
    b.write_stream_table(df, "`silver`.`events`", "/ckpt/events", trigger="available_now")

    df.writeStream.format.assert_called_once_with("delta")
    chain = df.writeStream.format.return_value
    chain.outputMode.assert_called_once_with("append")
    chain.outputMode.return_value.option.assert_called_once_with(
        "checkpointLocation", "/ckpt/events"
    )
    trigger_mock = chain.outputMode.return_value.option.return_value.trigger
    trigger_mock.assert_called_once_with(availableNow=True)
    trigger_mock.return_value.toTable.assert_called_once_with("`silver`.`events`")
    trigger_mock.return_value.toTable.return_value.awaitTermination.assert_called_once()


def test_write_stream_table_interval_trigger(mock_spark):
    b = SparkBackend(spark=mock_spark, is_local=False)
    df = MagicMock()
    df.isStreaming = True
    b.write_stream_table(df, "`s`.`t`", "/ckpt", trigger="interval:30 seconds")
    trigger_mock = (
        df.writeStream.format.return_value.outputMode.return_value.option.return_value.trigger
    )
    trigger_mock.assert_called_once_with(processingTime="30 seconds")


def test_write_stream_table_invalid_trigger_raises(mock_spark):
    b = SparkBackend(spark=mock_spark, is_local=False)
    df = MagicMock()
    df.isStreaming = True
    with pytest.raises(ValueError, match="Invalid trigger"):
        b.write_stream_table(df, "`s`.`t`", "/ckpt", trigger="every_night")


def test_default_checkpoint_root_local(backend):
    backend.spark.conf.get.return_value = "/tmp/warehouse/"
    assert backend.default_checkpoint_root() == "/tmp/warehouse/_checkpoints"


def test_default_checkpoint_root_remote_is_none(mock_spark):
    b = SparkBackend(spark=mock_spark, is_local=False)
    assert b.default_checkpoint_root() is None


@pytest.mark.streaming
def test_streaming_roundtrip_local(spark, tmp_path):
    """Real availableNow round-trip: Delta source → read stream → write stream → rows land.

    Hermetic: DROP TABLE on a LOCATION-registered table is metadata-only, so the
    physical warehouse dir must be purged too — otherwise reruns accumulate rows.
    """
    import shutil

    src_path = str(tmp_path / "src")
    ckpt = str(tmp_path / "ckpt")
    warehouse = spark.conf.get("spark.sql.warehouse.dir").replace("file:", "")
    table_dir = os.path.join(warehouse, "stream_test", "target")

    spark.sql("DROP TABLE IF EXISTS `stream_test`.`target`")
    shutil.rmtree(table_dir, ignore_errors=True)
    spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"]).write.format("delta").save(src_path)

    b = SparkBackend(spark=spark, is_local=True)
    stream_df = b.read_source_stream("delta", src_path)
    assert stream_df.isStreaming

    spark.sql("CREATE DATABASE IF NOT EXISTS stream_test")
    try:
        b.write_stream_table(stream_df, "`stream_test`.`target`", ckpt)

        rows = spark.table("`stream_test`.`target`").collect()
        assert sorted(r["id"] for r in rows) == [1, 2]

        # Incremental proof at backend level: append to the source, re-run with
        # the same checkpoint — only the new row lands (no duplicates of 1/2).
        spark.createDataFrame([(3, "c")], ["id", "val"]).write.format("delta").mode("append").save(src_path)
        stream_df2 = b.read_source_stream("delta", src_path)
        b.write_stream_table(stream_df2, "`stream_test`.`target`", ckpt)
        rows2 = spark.table("`stream_test`.`target`").collect()
        assert sorted(r["id"] for r in rows2) == [1, 2, 3]
    finally:
        spark.sql("DROP TABLE IF EXISTS `stream_test`.`target`")
        spark.sql("DROP DATABASE IF EXISTS stream_test")
        shutil.rmtree(table_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# CDC Type 1 upsert — foreachBatch + MERGE INTO (Plan 27)
# ---------------------------------------------------------------------------

def test_write_stream_table_upsert_requires_keys(mock_spark):
    b = SparkBackend(spark=mock_spark, is_local=False)
    df = MagicMock()
    df.isStreaming = True
    with pytest.raises(ValueError, match="requires\\s+non-empty merge keys"):
        b.write_stream_table(df, "`s`.`t`", "/ckpt", write_mode="upsert")


def test_write_stream_table_invalid_write_mode(mock_spark):
    b = SparkBackend(spark=mock_spark, is_local=False)
    df = MagicMock()
    df.isStreaming = True
    with pytest.raises(ValueError, match="Invalid write_mode"):
        b.write_stream_table(df, "`s`.`t`", "/ckpt", write_mode="merge")


def test_write_stream_table_upsert_uses_foreach_batch(mock_spark):
    """Upsert path: writeStream.foreachBatch(...).option(ckpt).trigger().start()."""
    b = SparkBackend(spark=mock_spark, is_local=False)
    df = MagicMock()
    df.isStreaming = True
    b.write_stream_table(df, "`s`.`t`", "/ckpt", write_mode="upsert", keys=["id"])

    df.writeStream.foreachBatch.assert_called_once()
    chain = df.writeStream.foreachBatch.return_value
    chain.option.assert_called_once_with("checkpointLocation", "/ckpt")
    chain.option.return_value.trigger.assert_called_once_with(availableNow=True)
    chain.option.return_value.trigger.return_value.start.assert_called_once()
    chain.option.return_value.trigger.return_value.start.return_value.awaitTermination.assert_called_once()
    # No direct Delta sink on the upsert path — foreachBatch owns the write.
    df.writeStream.format.assert_not_called()


def test_upsert_batch_fn_merges_on_keys(mock_spark):
    """The foreachBatch callback dedups the batch then issues a MERGE INTO."""
    b = SparkBackend(spark=mock_spark, is_local=False)
    fn = b._make_upsert_batch_fn("`silver`.`orders`", ["order_id", "src"])

    batch_df = MagicMock()
    session = batch_df.sparkSession
    session.catalog.tableExists.return_value = True

    fn(batch_df, batch_id=7)

    batch_df.dropDuplicates.assert_called_once_with(["order_id", "src"])
    deduped = batch_df.dropDuplicates.return_value
    deduped.createOrReplaceTempView.assert_called_once_with("_skifer_upsert_src_silver_orders")
    merge_sql = session.sql.call_args[0][0]
    assert "MERGE INTO `silver`.`orders` AS t" in merge_sql
    assert "t.`order_id` = s.`order_id` AND t.`src` = s.`src`" in merge_sql
    assert "WHEN MATCHED THEN UPDATE SET *" in merge_sql
    assert "WHEN NOT MATCHED THEN INSERT *" in merge_sql


def test_upsert_batch_fn_creates_target_when_absent(mock_spark):
    """First micro-batch: target does not exist → created, no MERGE issued."""
    b = SparkBackend(spark=mock_spark, is_local=False)
    fn = b._make_upsert_batch_fn("`silver`.`orders`", ["order_id"])

    batch_df = MagicMock()
    session = batch_df.sparkSession
    session.catalog.tableExists.return_value = False

    fn(batch_df, batch_id=0)

    deduped = batch_df.dropDuplicates.return_value
    deduped.write.format.assert_called_once_with("delta")
    deduped.write.format.return_value.saveAsTable.assert_called_once_with("`silver`.`orders`")
    session.sql.assert_not_called()


@pytest.mark.streaming
def test_streaming_upsert_roundtrip_local(spark, tmp_path):
    """Real CDC Type 1 round-trip: in-batch dedup, cross-run update, no duplicates."""
    import shutil

    src_path = str(tmp_path / "src")
    ckpt = str(tmp_path / "ckpt")
    warehouse = spark.conf.get("spark.sql.warehouse.dir").replace("file:", "")
    table_dir = os.path.join(warehouse, "stream_upsert", "target")

    spark.sql("DROP TABLE IF EXISTS `stream_upsert`.`target`")
    shutil.rmtree(table_dir, ignore_errors=True)
    spark.sql("CREATE DATABASE IF NOT EXISTS stream_upsert")

    # Batch 1 contains a duplicate key (1) — in-batch dedup must keep one row.
    spark.createDataFrame(
        [(1, "a"), (1, "a2"), (2, "b")], ["id", "val"]
    ).write.format("delta").save(src_path)

    b = SparkBackend(spark=spark, is_local=True)
    try:
        stream_df = b.read_source_stream("delta", src_path)
        b.write_stream_table(
            stream_df, "`stream_upsert`.`target`", ckpt, write_mode="upsert", keys=["id"]
        )
        rows = {r["id"]: r["val"] for r in spark.table("`stream_upsert`.`target`").collect()}
        assert set(rows) == {1, 2}

        # Run 2: same key with a new value (update) + a new key (insert).
        spark.createDataFrame(
            [(2, "b_updated"), (3, "c")], ["id", "val"]
        ).write.format("delta").mode("append").save(src_path)
        stream_df2 = b.read_source_stream("delta", src_path)
        b.write_stream_table(
            stream_df2, "`stream_upsert`.`target`", ckpt, write_mode="upsert", keys=["id"]
        )
        rows2 = {r["id"]: r["val"] for r in spark.table("`stream_upsert`.`target`").collect()}
        assert rows2[2] == "b_updated"      # updated, not duplicated
        assert rows2[3] == "c"              # inserted
        assert len(rows2) == 3              # exactly one row per key

        # Run 3: no new source data — nothing changes (idempotent no-op).
        stream_df3 = b.read_source_stream("delta", src_path)
        b.write_stream_table(
            stream_df3, "`stream_upsert`.`target`", ckpt, write_mode="upsert", keys=["id"]
        )
        rows3 = {r["id"]: r["val"] for r in spark.table("`stream_upsert`.`target`").collect()}
        assert rows3 == rows2
    finally:
        spark.sql("DROP TABLE IF EXISTS `stream_upsert`.`target`")
        spark.sql("DROP DATABASE IF EXISTS stream_upsert")
        shutil.rmtree(table_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Plan 29 — SQL string literals must survive a backslash-terminated value
# ---------------------------------------------------------------------------

def test_escape_sql_string_doubles_backslashes_before_quotes():
    """Doubling the quote alone is not enough in Spark.

    With ``spark.sql.parser.escapedStringLiterals=false`` (the default) a
    backslash escapes the next character, so a value ending in one turns the
    closing quote into an escaped quote and everything after it is parsed as
    SQL. Reproduced against a real local session before this was fixed: the
    payload below returned every row of the table instead of none.
    """
    from skifer.core.sql_compiler import escape_sql_string, sql_literal

    assert escape_sql_string("x\\") == "x\\\\"
    assert escape_sql_string("a\\b") == "a\\\\b"
    # Backslashes are doubled first, or doubling the quotes would be escaped.
    assert escape_sql_string("x\\'") == "x\\\\''"
    assert sql_literal("O'Brien") == "'O''Brien'"
    assert sql_literal("x\\") == "'x\\\\'"


def test_get_certification_contract_neutralises_a_backslash_payload():
    """The agent-reachable contract read must not let a value escape its quotes."""
    captured = []

    class Spark:
        class catalog:
            @staticmethod
            def tableExists(name):
                return True

        def sql(self, text):
            captured.append(text)
            class R:
                @staticmethod
                def collect():
                    return []
            return R()

    backend = SparkBackend.__new__(SparkBackend)
    backend._spark = Spark()
    backend.get_certification_contract("_c", "x\\", " OR 1=1 --")

    sql = captured[0]
    # The closing quote of the injected value stays a closing quote.
    assert "'x\\\\'" in sql
    assert sql.count("'") % 2 == 0


def test_write_staging_creates_the_staging_schema_on_demand():
    """Nobody else creates the certified-publication staging area.

    The engine ensures the *target* schema exists, but `_skifer_staging` is an
    implementation detail of publication, so the very first certified publication
    in a fresh environment failed on a missing schema.
    """
    from skifer.core.spark_backend import SparkBackend, _schema_part

    assert _schema_part("_skifer_staging.fact_orders_abc") == "_skifer_staging"
    assert _schema_part("`cat`.`silver`.`orders`") == "silver"

    created: list[str] = []
    written: list[str] = []

    backend = SparkBackend.__new__(SparkBackend)
    backend.ensure_schema_exists = created.append
    backend.write_table = lambda df, fqn, mode="overwrite": written.append(fqn)

    backend.write_staging(object(), "_skifer_staging.fact_orders_abc")

    assert created == ["_skifer_staging"]
    assert written == ["_skifer_staging.fact_orders_abc"]


# `spark` is unused here on purpose: apply_op() builds Spark columns, which need a live
# JVM context even for the arity errors these tests assert.
@pytest.mark.parametrize(
    "op_str, operation, required, received",
    [
        ("substring:1", "substring", 2, 1),
        ("split:sep", "split", 2, 1),
        ("cast", "cast", 1, 0),
        ("round", "round", 1, 0),
        ("to_date", "to_date", 1, 0),
    ],
)
def test_apply_op_missing_argument_names_the_operation(
    backend, spark, op_str, operation, required, received
):
    """A missing argument used to surface as IndexError from inside a lambda.

    The op catalog has always declared an arity for every column operation and nothing
    read it, so `substring:1` crashed with `tuple index out of range`, naming neither
    the operation nor the column. Found by writing an example whose YAML left a flow
    sequence unquoted, which is how a reader actually hits this.
    """
    from pyspark.sql import functions as F
    from skifer.core.ir import _parse_op

    with pytest.raises(ValueError) as raised:
        backend.apply_op(F.col("x"), _parse_op(op_str))

    message = str(raised.value)
    assert operation in message
    assert f"needs {required} argument(s) but received {received}" in message


def test_apply_op_accepts_the_declared_arity(backend, spark):
    """The guard must not reject the documented syntax."""
    from pyspark.sql import functions as F
    from skifer.core.ir import _parse_op

    df = spark.createDataFrame([("abcdefgh",)], ["x"])
    column = backend.apply_op(F.col("x"), _parse_op("substring:1,3"))
    assert df.select(column.alias("out")).collect()[0]["out"] == "abc"
