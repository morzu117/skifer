"""Join on a rule-produced key from a nested partial, inline or as a debug view.

    python examples/06_nested_partials/run.py
"""

from pathlib import Path

from pyspark.sql import functions as F

from skifer import RuleRegistry, SkiferEngine, load_schema

EXAMPLE_DIR = Path(__file__).parent


@RuleRegistry.register_rule()
def normalize_customer_key(df):
    return {"join_customer_id": F.upper(F.col("customer_code"))}


engine = SkiferEngine(force_env="LOCAL")
params = {**engine.default_params, "example_dir": EXAMPLE_DIR.as_posix()}
yaml_path = str(EXAMPLE_DIR / "customer_orders.yaml")
view_name = f"prepared_orders_{engine.schema_suffix}" if engine.schema_suffix else "prepared_orders"

# The materialization mode is a run parameter: the YAML is identical in both runs.
for mode in ("inline", "temp_view"):
    table = f"nested_{mode}"
    engine.run_from_yaml(yaml_path, "silver", table, params={**params, "intermediate_mode": mode})
    print(f"\nMode: {mode}")
    for row in engine.spark.table(f"{engine.get_target_schema('silver')}.{table}").orderBy("order_id").collect():
        print(f"{row.order_id} | {row.customer_key} | {row.customer_name} | {row.amount}")
    print(f"Intermediate temp view exists: {engine.spark.catalog.tableExists(view_name)}")

try:
    load_schema(str(EXAMPLE_DIR / "refused_alias_collision.yaml"), params=params)
except ValueError as exc:
    print(f"Refused partial: {exc}")
