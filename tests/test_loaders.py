import pytest
from unittest.mock import MagicMock
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DoubleType

from skifer.core.loaders import load_and_union_tables, load_generic_explode_union
from skifer.core.spark_backend import SparkBackend
from tests.fakes.fake_backend import FakeBackend, FakeDataFrame

# ==============================================================================
# load_and_union_tables
# ==============================================================================

def test_load_and_union_tables_with_backend():
    """load_and_union_tables uses backend.read_table and backend.union_by_name."""
    backend = FakeBackend(tables={
        "silver.a": [{"id": 1, "val": "x"}],
        "silver.b": [{"id": 2, "val": "y"}],
    })
    result = load_and_union_tables(None, ["silver.a", "silver.b"], backend=backend)
    assert isinstance(result, FakeDataFrame)
    assert len(result) == 2


def test_load_and_union_tables_with_spark_backend(spark):
    """load_and_union_tables works with a real SparkBackend."""
    data = [("A", 1)]
    df = spark.createDataFrame(data, ["col1", "col2"])
    df.createOrReplaceTempView("test_loader_view")
    backend = SparkBackend(spark=spark, is_local=True)
    result = load_and_union_tables(None, ["test_loader_view"], backend=backend)
    assert result.count() == 1
    spark.catalog.dropTempView("test_loader_view")


def test_load_and_union_tables_no_backend_raises():
    """Without backend=, loader raises RuntimeError."""
    with pytest.raises(RuntimeError, match="A backend is required"):
        load_and_union_tables(None, ["some.table"])


def test_load_and_union_tables_no_tables_found():
    """All tables fail to load → ValueError."""
    backend = FakeBackend(tables={})  # empty — all reads will raise
    with pytest.raises(ValueError, match="No tables loaded successfully"):
        load_and_union_tables(None, ["non_existent_table"], backend=backend)


def test_load_and_union_tables_missing_table_skipped():
    """One table missing → skipped, rest loaded."""
    backend = FakeBackend(tables={"silver.a": [{"id": 1}]})
    result = load_and_union_tables(None, ["silver.a", "silver.b"], backend=backend)
    assert len(result) == 1


# ==============================================================================
# load_generic_explode_union
# ==============================================================================

def test_load_generic_explode_union(spark):
    """Full integration test via SparkBackend."""
    source_schema = StructType([
        StructField("order_id", StringType()),
        StructField("cart", StructType([
            StructField("items", ArrayType(StructType([
                StructField("sku", StringType()),
                StructField("price", DoubleType())
            ]))),
            StructField("rewards", ArrayType(StructType([
                StructField("reward_sku", StringType()),
                StructField("discount", DoubleType())
            ])))
        ]))
    ])
    data = [
        ("ORDER1", ([{"sku": "SKU100", "price": 99.9}], [{"reward_sku": "REWARD1", "discount": 5.0}])),
        ("ORDER2", ([{"sku": "SKU200", "price": 10.0}, {"sku": "SKU300", "price": 20.0}], []))
    ]
    df = spark.createDataFrame(data, schema=source_schema)
    df.createOrReplaceTempView("raw_orders")
    backend = SparkBackend(spark=spark, is_local=True)

    union_config = {
        "common_cols": ["order_id"],
        "branches": [
            {"explode_col": "cart.items", "item_alias": "item",
             "mappings": {"product_sku": "item.sku", "line_amount": "item.price"},
             "defaults": {"line_type": "SALE"}},
            {"explode_col": "cart.rewards", "item_alias": "reward",
             "mappings": {"product_sku": "reward.reward_sku", "line_amount": "reward.discount"},
             "defaults": {"line_type": "REWARD"}},
        ]
    }
    result_df = load_generic_explode_union(None, source_fqn="raw_orders",
                                           union_config=union_config, backend=backend)
    assert result_df.count() == 4
    expected_cols = ["order_id", "product_sku", "line_amount", "line_type"]
    assert all(col in result_df.columns for col in expected_cols)
    spark.catalog.dropTempView("raw_orders")


def test_load_generic_explode_union_no_backend_raises():
    """Without backend=, loader raises RuntimeError."""
    with pytest.raises(RuntimeError, match="A backend is required"):
        load_generic_explode_union(None, source_fqn="t", union_config={})


def test_load_generic_explode_union_non_spark_backend_raises():
    """Non-SparkBackend (no .spark attr) raises RuntimeError."""
    backend = FakeBackend(tables={"t": []})
    with pytest.raises(RuntimeError, match="SparkBackend"):
        load_generic_explode_union(None, source_fqn="t", union_config={}, backend=backend)

