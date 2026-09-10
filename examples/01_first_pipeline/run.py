"""Run the first pipeline locally: no cluster, no Databricks account.

    python examples/01_first_pipeline/run.py

Spark starts in local[*] mode with Delta Lake, the table is written under
`.spark-warehouse/`, and the resulting rows are printed back.
"""

from pathlib import Path

from skifer import SkiferEngine

EXAMPLE_DIR = Path(__file__).parent


def main() -> None:
    engine = SkiferEngine(force_env="LOCAL")

    # run_from_yaml WRITES the table; it does not hand back a DataFrame. The
    # pipeline's product is the table, and reading it back is a separate step —
    # the same one a downstream job or a dashboard would do.
    engine.run_from_yaml(
        str(EXAMPLE_DIR / "silver_orders.yaml"),
        "silver",
        "orders",
        params={**engine.default_params, "example_dir": str(EXAMPLE_DIR)},
    )

    # Interactively, the engine writes to your personal sandbox schema
    # (silver_<user>) rather than to the shared one. In a job or in production
    # the suffix disappears and this resolves to plain `silver`.
    schema = engine.get_target_schema("silver")
    print(f"\nWritten to: {schema}.orders")

    engine.spark.table(f"{schema}.orders").orderBy("order_id").show(truncate=False)


if __name__ == "__main__":
    main()
