"""Repository example parity between Spark and the compiled DuckDB SQL path."""

from __future__ import annotations

from pathlib import Path

import pytest
from pyspark.sql import functions as F

from skifer import RuleRegistry, SkiferEngine
from tests.test_sql_spark_equivalence import assert_spark_duckdb_equivalent


REPO_ROOT = Path(__file__).resolve().parent.parent


EXAMPLES_DIR = REPO_ROOT / "examples"


PIPELINES = [
    pytest.param(
        "01_first_pipeline/silver_orders.yaml",
        None,
        4,
        id="01_first_pipeline",
    ),
    pytest.param(
        "05_rules_join_aggregate/joined_orders.yaml",
        "classify_order",
        4,
        id="05_joined_orders",
    ),
    pytest.param(
        "05_rules_join_aggregate/orders_by_region.yaml",
        "classify_order",
        2,
        id="05_orders_by_region",
    ),
    pytest.param(
        "06_nested_partials/customer_orders.yaml",
        "normalize_customer_key",
        3,
        id="06_customer_orders",
    ),
]


def _register_sql_rule(rule_name: str) -> None:
    if rule_name == "classify_order":
        result = {
            "order_class": (
                "CASE WHEN amount >= 500 THEN 'priority' ELSE 'standard' END"
            )
        }
    elif rule_name == "normalize_customer_key":
        result = {"join_customer_id": "UPPER(customer_code)"}
    else:  # pragma: no cover - the parametrization is closed above
        raise AssertionError(f"No SQL example rule registered for {rule_name!r}")

    @RuleRegistry.register_rule(name=rule_name, kind="sql")
    def sql_rule():
        return result


def _register_spark_rule(rule_name: str) -> None:
    if rule_name == "classify_order":

        @RuleRegistry.register_rule(name=rule_name)
        def spark_rule(_df):
            return {
                "order_class": F.when(
                    F.col("amount") >= 500, F.lit("priority")
                ).otherwise(F.lit("standard"))
            }

    elif rule_name == "normalize_customer_key":

        @RuleRegistry.register_rule(name=rule_name)
        def spark_rule(_df):
            return {"join_customer_id": F.upper(F.col("customer_code"))}

    else:  # pragma: no cover - the parametrization is closed above
        raise AssertionError(f"No Spark example rule registered for {rule_name!r}")


@pytest.mark.parametrize(("relative_yaml", "rule_name", "expected_rows"), PIPELINES)
def test_repository_example_is_equivalent_on_spark_and_duckdb(
    spark, relative_yaml, rule_name, expected_rows
):
    yaml_path = EXAMPLES_DIR / relative_yaml
    example_dir = yaml_path.parent
    target_layer = "sql_mode_examples"
    target_table = relative_yaml.replace("/", "_").replace(".yaml", "")
    previous_rule = RuleRegistry._rules.get(rule_name) if rule_name else None

    # Instantiate SQL first: this is the measured order used by the portability
    # example and ensures runtime selection happens before a new Spark engine is built.
    sql_engine = SkiferEngine(force_env="LOCAL_SQL")
    spark_engine = SkiferEngine(spark=spark, force_env="LOCAL")
    params = {"example_dir": example_dir.as_posix()}

    try:
        if rule_name:
            _register_sql_rule(rule_name)
        sql_engine.run_from_yaml(
            str(yaml_path), target_layer, target_table, params=params
        )
        duck_cursor = sql_engine.backend.read_table(
            f"{target_layer}.{target_table}"
        )
        duck_columns = [column[0] for column in duck_cursor.description]
        duck_rows = duck_cursor.fetchall()

        if rule_name:
            _register_spark_rule(rule_name)
        spark_engine.run_from_yaml(
            str(yaml_path), target_layer, target_table, params=params
        )
        spark_result = spark.table(
            f"{spark_engine.get_target_schema(target_layer)}.{target_table}"
        )

        assert len(duck_rows) == expected_rows
        assert_spark_duckdb_equivalent(
            spark_result, duck_columns, duck_rows
        )
    finally:
        sql_engine.backend.connection.close()
        if rule_name:
            if previous_rule is None:
                RuleRegistry._rules.pop(rule_name, None)
            else:
                RuleRegistry._rules[rule_name] = previous_rule


def test_certified_publication_example_is_equivalent_on_spark_and_duckdb(spark, tmp_path):
    """Example 02 publishes under contract on both engines, with the same rows.

    This was the exit criterion moved out of phase 39.3 when certified publication
    existed on Spark only. It is not enough that each engine publishes something:
    the rows must match, and both must record a CERTIFIED verdict — a pipeline that
    published different data under the same contract would be worse than one that
    refused.
    """
    from skifer.observability.certification_store import SqliteCertificationStore
    from skifer.observability.monitor import DataMonitor

    example_dir = EXAMPLES_DIR / "02_quality_and_contract"
    yaml_path = example_dir / "gold_orders.yaml"
    layer, table = "sql_mode_examples", "fact_orders_certified"
    params = {"example_dir": example_dir.as_posix()}

    sql_engine = SkiferEngine(force_env="LOCAL_SQL")
    sql_engine.monitor = DataMonitor(sql_engine.backend)
    sql_engine.certification_store = SqliteCertificationStore(str(tmp_path / "sql.db"))

    spark_engine = SkiferEngine(spark=spark, force_env="LOCAL")
    spark_engine.schema_suffix = ""
    spark_engine.monitor = DataMonitor(spark_engine.backend)
    spark_engine.certification_store = SqliteCertificationStore(
        str(tmp_path / "spark.db")
    )

    try:
        sql_engine.run_from_yaml(str(yaml_path), layer, table, params=params)
        spark_engine.run_from_yaml(str(yaml_path), layer, table, params=params)

        duck_fqn = f"{sql_engine.get_target_schema(layer)}.{table}"
        cursor = sql_engine.backend.read_table(duck_fqn)
        duck_columns = [description[0] for description in cursor.description]
        duck_rows = cursor.fetchall()
        spark_df = spark.table(f"{spark_engine.get_target_schema(layer)}.{table}")

        assert len(duck_rows) == 4
        assert_spark_duckdb_equivalent(spark_df, duck_columns, duck_rows)

        for engine in (sql_engine, spark_engine):
            certification = engine.certification_store.get_certification(
                f"{engine.get_target_schema(layer)}.{table}"
            )
            assert certification.status == "CERTIFIED"
            assert certification.checks_passed is True
    finally:
        sql_engine.backend.connection.close()
