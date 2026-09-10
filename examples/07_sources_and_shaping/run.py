"""Read JSON, use grouped filters and a loader, then shape before grouping.

    python examples/07_sources_and_shaping/run.py

Everything is local: the JSON is checked in and the registered loader constructs
a two-row lookup DataFrame in the local Spark session.
"""

from pathlib import Path

from skifer import RuleRegistry, SkiferEngine, load_schema

EXAMPLE_DIR = Path(__file__).parent


@RuleRegistry.register_loader()
def load_segment_labels(config, *, backend, **kwargs):
    print("Registered loader supplied segment labels from Python.")
    return backend.spark.createDataFrame(
        [("A", "Retail"), ("B", "Business")],
        ["segment_code", "segment_name"],
    )


def main() -> None:
    engine = SkiferEngine(force_env="LOCAL")
    params = {**engine.default_params, "example_dir": str(EXAMPLE_DIR)}
    engine.run_from_yaml(
        str(EXAMPLE_DIR / "monthly_events.yaml"),
        "gold",
        "monthly_events",
        params=params,
    )

    target = f"{engine.get_target_schema('gold')}.monthly_events"
    print("\nAggregated after grouped filters and dev_limit=3:")
    for row in engine.spark.table(target).orderBy("event_month", "segment_name").collect():
        print(
            f"{row.event_month} | {row.segment_name} | "
            f"total={row.total_amount} | events={row.event_count}"
        )

    try:
        load_schema(str(EXAMPLE_DIR / "refused_filter.yaml"), params=params)
    except ValueError as exc:
        print(f"Refused filter: {exc}")


if __name__ == "__main__":
    main()
