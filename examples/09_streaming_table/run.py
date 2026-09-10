"""Run two finite streaming upserts against one automatic checkpoint.

    python examples/09_streaming_table/run.py

The source is a local Delta table. ``available_now`` drains the rows currently
available and terminates, making two consecutive incremental runs visible.
"""

from pathlib import Path
from tempfile import TemporaryDirectory

from skifer import SkiferEngine, load_schema
import yaml

EXAMPLE_DIR = Path(__file__).parent
TARGET_LAYER = "streaming_example"
TARGET_TABLE = "orders_current"


def _commits_recorded(engine: SkiferEngine, schema_path: str) -> int:
    """How many micro-batches the checkpoint has committed, read off disk.

    This is the evidence that the second run was incremental. The final table
    cannot show it: with an upsert, replaying the whole source from scratch
    would produce exactly the same rows. The checkpoint is what makes the
    second run read only the appended records, so the checkpoint is what the
    example has to show.
    """
    materialization = yaml.safe_load(Path(schema_path).read_text())["materialization"]
    resolved = engine.resolve_checkpoint_location(
        engine.get_target_schema(TARGET_LAYER), TARGET_TABLE, dict(materialization)
    )
    # Locally the engine returns a `file:` URI, which Path would read as a
    # relative name rather than as the absolute location it is.
    commits = Path(resolved.removeprefix("file:")) / "commits"
    return len(list(commits.glob("[0-9]*"))) if commits.is_dir() else 0


def _rows(engine: SkiferEngine, target: str) -> list[tuple[int, str]]:
    return [
        (row.order_id, row.status)
        for row in engine.spark.table(target).orderBy("order_id").collect()
    ]


def _print_rows(rows: list[tuple[int, str]]) -> None:
    for order_id, status in rows:
        print(f"order_id={order_id} | status={status}")


def main() -> None:
    engine = SkiferEngine(force_env="LOCAL")

    # Make repeated script runs deterministic: the target and its automatic
    # warehouse checkpoint are one stateful unit and are reset together.
    engine.full_refresh(TARGET_LAYER, TARGET_TABLE)
    print("Startup reset: target table and automatic checkpoint cleared.")

    with TemporaryDirectory(prefix="skifer-streaming-example-") as runtime_dir:
        source_path = str(Path(runtime_dir) / "raw_orders")
        engine.spark.createDataFrame(
            [(1, "pending"), (2, "pending")],
            ["order_id", "status"],
        ).write.format("delta").save(source_path)

        params = {**engine.default_params, "source_path": source_path}
        schema_path = str(EXAMPLE_DIR / "orders_current.yaml")
        target = f"{engine.get_target_schema(TARGET_LAYER)}.{TARGET_TABLE}"

        engine.run_from_yaml(
            schema_path,
            TARGET_LAYER,
            TARGET_TABLE,
            params=params,
        )
        first_rows = _rows(engine, target)
        print(f"\nRun 1 added {len(first_rows)} target rows:")
        _print_rows(first_rows)
        print(f"Checkpoint commits after run 1: {_commits_recorded(engine, schema_path)}")

        # Two new source records: order 1 changes and order 3 is new. The second
        # available-now run shares the first run's checkpoint, so it consumes
        # this Delta append rather than replaying the original snapshot.
        engine.spark.createDataFrame(
            [(1, "shipped"), (3, "pending")],
            ["order_id", "status"],
        ).write.format("delta").mode("append").save(source_path)

        engine.run_from_yaml(
            schema_path,
            TARGET_LAYER,
            TARGET_TABLE,
            params=params,
        )
        second_rows = _rows(engine, target)
        added = len(second_rows) - len(first_rows)
        source_rows = engine.spark.read.format("delta").load(source_path).count()
        print(f"\nRun 2 added {added} target row, from {source_rows} source rows:")
        _print_rows(second_rows)
        print(f"Checkpoint commits after run 2: {_commits_recorded(engine, schema_path)}")
        print("Order 1 was updated in place; it was not duplicated.")

        try:
            load_schema(
                str(EXAMPLE_DIR / "refused_dev_limit.yaml"),
                params=params,
            )
        except ValueError as exc:
            print(f"Refused schema: {exc}")


if __name__ == "__main__":
    main()
