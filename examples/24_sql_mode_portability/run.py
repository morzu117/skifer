"""Run one YAML pipeline on DuckDB SQL and Spark, then compare exact rows.

    python examples/24_sql_mode_portability/run.py
"""

from pathlib import Path

from pyspark.sql import functions as F

from skifer import RuleRegistry, SkiferEngine

EXAMPLE_DIR = Path(__file__).parent
PIPELINE_PATH = EXAMPLE_DIR / "pipeline.yaml"
TARGET_LAYER = "portable"
TARGET_TABLE = "orders"


@RuleRegistry.register_rule(name="portable_order_class_spark")
def portable_order_class_spark(_df):
    """PySpark implementation of the portable business concept."""
    return {
        "order_class": F.when(F.col("amount") >= 500, F.lit("priority")).otherwise(
            F.lit("standard")
        )
    }


@RuleRegistry.register_rule(name="portable_order_class_sql", kind="sql")
def portable_order_class_sql():
    """SQL implementation of the same portable business concept."""
    return {
        "order_class": (
            "CASE WHEN amount >= 500 THEN 'priority' ELSE 'standard' END"
        )
    }


def _params(engine: SkiferEngine, rule_name: str) -> dict:
    return {
        **engine.default_params,
        "example_dir": EXAMPLE_DIR.as_posix(),
        "business_rule": rule_name,
    }


def main() -> None:
    # SQL must be selected before Spark initialization so the engine takes its
    # Spark-free startup branch. This is the order exercised by this example.
    sql_engine = SkiferEngine(force_env="LOCAL_SQL")
    spark_engine = SkiferEngine(force_env="LOCAL")

    try:
        sql_engine.run_from_yaml(
            str(PIPELINE_PATH),
            TARGET_LAYER,
            TARGET_TABLE,
            params=_params(sql_engine, "portable_order_class_sql"),
        )
        sql_rows = sql_engine.backend.fetch(
            'SELECT * FROM "portable"."orders" ORDER BY "order_id"'
        )
        print(
            "Executed pipeline.yaml on DuckDB SQL "
            f"with rule `portable_order_class_sql`: {len(sql_rows)} rows"
        )

        spark_engine.run_from_yaml(
            str(PIPELINE_PATH),
            TARGET_LAYER,
            TARGET_TABLE,
            params=_params(spark_engine, "portable_order_class_spark"),
        )
        spark_target = (
            f"{spark_engine.get_target_schema(TARGET_LAYER)}.{TARGET_TABLE}"
        )
        spark_rows = [
            row.asDict(recursive=True)
            for row in spark_engine.spark.table(spark_target)
            .orderBy("order_id")
            .collect()
        ]
        print(
            "Executed pipeline.yaml on Spark "
            f"with rule `portable_order_class_spark`: {len(spark_rows)} rows"
        )

        if spark_rows != sql_rows:
            raise AssertionError(
                "Spark and DuckDB returned different rows:\n"
                f"Spark: {spark_rows!r}\nDuckDB: {sql_rows!r}"
            )

        print(f"Equality check: PASS — {len(spark_rows)} identical rows from one YAML.")
        for row in spark_rows:
            print(
                f"  {row['order_id']} | {row['amount']} | {row['order_class']}"
            )
    finally:
        sql_engine.backend.connection.close()


if __name__ == "__main__":
    main()
