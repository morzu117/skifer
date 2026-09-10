"""Use a nested pipeline inline, then expose it as a temporary debug view.

    python examples/06_nested_partials/run.py

The child creates a join key with a Python rule. The parent consumes that key
directly, without making the child into a table between the two transformations.
"""

from pathlib import Path

from pyspark.sql import functions as F

from skifer import RuleRegistry, SkiferEngine, load_schema

EXAMPLE_DIR = Path(__file__).parent


@RuleRegistry.register_rule()
def normalize_customer_key(df):
    return {"join_customer_id": F.upper(F.col("customer_code"))}


def _print_rows(engine: SkiferEngine, table_name: str) -> None:
    target = f"{engine.get_target_schema('silver')}.{table_name}"
    for row in engine.spark.table(target).orderBy("order_id").collect():
        print(
            f"{row.order_id} | {row.customer_key} | "
            f"{row.customer_name} | {row.amount}"
        )


def main() -> None:
    engine = SkiferEngine(force_env="LOCAL")
    base_params = {**engine.default_params, "example_dir": str(EXAMPLE_DIR)}
    yaml_path = str(EXAMPLE_DIR / "customer_orders.yaml")
    suffix = engine.schema_suffix or ""
    view_name = f"prepared_orders_{suffix}" if suffix else "prepared_orders"

    engine.run_from_yaml(yaml_path, "silver", "nested_inline", params=base_params)
    print("\nMode: inline")
    _print_rows(engine, "nested_inline")
    print(f"Intermediate temp view exists: {engine.spark.catalog.tableExists(view_name)}")

    engine.run_from_yaml(
        yaml_path,
        "silver",
        "nested_temp_view",
        params={**base_params, "intermediate_mode": "temp_view"},
    )
    print("\nMode: temp_view (set in run params, absent from YAML)")
    _print_rows(engine, "nested_temp_view")
    print(f"Intermediate temp view exists: {engine.spark.catalog.tableExists(view_name)}")

    try:
        load_schema(
            str(EXAMPLE_DIR / "refused_alias_collision.yaml"), params=base_params
        )
    except ValueError as exc:
        print(f"Refused partial: {exc}")


if __name__ == "__main__":
    main()
