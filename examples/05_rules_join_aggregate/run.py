"""Join two sources, apply one Python business rule, and aggregate in YAML.

    python examples/05_rules_join_aggregate/run.py
"""

from pathlib import Path

from skifer import SkiferEngine, load_schema

import rules  # noqa: F401 — registers classify_order, used by joined_orders.yaml

EXAMPLE_DIR = Path(__file__).parent

engine = SkiferEngine(force_env="LOCAL")
params = {**engine.default_params, "example_dir": EXAMPLE_DIR.as_posix()}

engine.run_from_yaml(str(EXAMPLE_DIR / "joined_orders.yaml"), "silver", "joined_orders", params=params)
print("\nJoined rows after Python rule `classify_order`:")
engine.spark.table(f"{engine.get_target_schema('silver')}.joined_orders").orderBy("order_id").show(truncate=False)

engine.run_from_yaml(str(EXAMPLE_DIR / "orders_by_region.yaml"), "gold", "orders_by_region", params=params)
print("Aggregated rows after `having total_amount > 500`:")
engine.spark.table(f"{engine.get_target_schema('gold')}.orders_by_region").orderBy("region").show(truncate=False)

try:
    load_schema(str(EXAMPLE_DIR / "refused_aggregate_and_select.yaml"), params=params)
except ValueError as exc:
    print(f"Refused schema: {exc}")
