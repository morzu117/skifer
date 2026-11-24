"""Identical canonical data in Spark temporary views and in-memory DuckDB tables."""

import duckdb
import pytest
from pyspark.sql.types import StructField, StructType

from tests.parity.dataset import TABLES


@pytest.fixture(scope="session")
def parity_spark(spark):
    for name, (columns, rows) in TABLES.items():
        schema = StructType([StructField(column, spark_type) for column, spark_type, _ in columns])
        spark.createDataFrame(rows, schema).createOrReplaceTempView(name)
    return spark


@pytest.fixture(scope="session")
def duck():
    connection = duckdb.connect()
    try:
        for name, (columns, rows) in TABLES.items():
            ddl = ", ".join(f'"{column}" {duck_type}' for column, _, duck_type in columns)
            connection.execute(f'CREATE TABLE "{name}" ({ddl})')
            placeholders = ", ".join("?" for _ in columns)
            connection.executemany(f'INSERT INTO "{name}" VALUES ({placeholders})', rows)
        yield connection
    finally:
        connection.close()
