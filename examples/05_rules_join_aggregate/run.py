"""Join two sources, apply Python business logic, and aggregate in YAML.

    python examples/05_rules_join_aggregate/run.py

The YAML says what the pipeline does. The registered projection rule below is
the deliberately small piece of Python that says how one business term works.
"""

from pathlib import Path

from pyspark.sql import functions as F

from skifer import RuleRegistry, SkiferEngine, load_schema

EXAMPLE_DIR = Path(__file__).parent


@RuleRegistry.register_rule()
def classify_order(df):
    """Define the Python half: one named business concept, returning columns."""
    return {
        "order_class": F.when(F.col("amount") >= 500, F.lit("priority")).otherwise(
            F.lit("standard")
        )
    }


def main() -> None:
    engine = SkiferEngine(force_env="LOCAL")
    params = {**engine.default_params, "example_dir": str(EXAMPLE_DIR)}

    # `aggregate` is terminal, so the stage before it is a pipeline of its own.
    engine.run_from_yaml(
        str(EXAMPLE_DIR / "joined_orders.yaml"), "silver", "joined_orders", params=params
    )
    joined = f"{engine.get_target_schema('silver')}.joined_orders"
    print("\nJoined rows after Python rule `classify_order`:")
    engine.spark.table(joined).orderBy("order_id").show(truncate=False)

    engine.run_from_yaml(
        str(EXAMPLE_DIR / "orders_by_region.yaml"),
        "gold",
        "orders_by_region",
        params=params,
    )
    target = f"{engine.get_target_schema('gold')}.orders_by_region"
    print("Aggregated rows after `having total_amount > 500`:")
    engine.spark.table(target).orderBy("region").show(truncate=False)

    try:
        load_schema(
            str(EXAMPLE_DIR / "refused_aggregate_and_select.yaml"), params=params
        )
    except ValueError as exc:
        print(f"Refused schema: {exc}")


if __name__ == "__main__":
    main()
