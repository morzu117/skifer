"""A first Bronze -> Silver pipeline, run locally with no cluster.

    python examples/01_first_pipeline/run.py
"""

from pathlib import Path

from skifer import SkiferEngine

EXAMPLE_DIR = Path(__file__).parent

engine = SkiferEngine(force_env="LOCAL")
engine.run_from_yaml(
    str(EXAMPLE_DIR / "silver_orders.yaml"),
    "silver",
    "orders",
    params={**engine.default_params, "example_dir": EXAMPLE_DIR.as_posix()},
)

# Interactively the table lands in your sandbox schema (silver_<user>), not in `silver`.
table = f"{engine.get_target_schema('silver')}.orders"
print(f"\nWritten to: {table}")
engine.spark.table(table).orderBy("order_id").show(truncate=False)
