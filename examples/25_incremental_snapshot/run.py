"""Run the same two days through `incremental merge` and `snapshot` SCD2.

    python examples/25_incremental_snapshot/run.py

The point of both strategies is the *second* run. One run leaves the same three
rows whichever you choose — including a plain overwrite, which is neither.
"""

from pathlib import Path

from skifer import SkiferEngine

EXAMPLE_DIR = Path(__file__).parent
TARGET_LAYER = "history"


def _params(engine: SkiferEngine, day: str) -> dict:
    return {
        **engine.default_params,
        "example_dir": EXAMPLE_DIR.as_posix(),
        "day": day,
    }


def _run_both_days(engine: SkiferEngine, pipeline: str, table: str) -> list[dict]:
    """Run day 1, then day 2, and read the target back."""
    for day in ("day1", "day2"):
        engine.run_from_yaml(
            str(EXAMPLE_DIR / pipeline),
            TARGET_LAYER,
            table,
            params=_params(engine, day),
        )
    return engine.backend.fetch(
        f'SELECT * FROM "{TARGET_LAYER}"."{table}" ORDER BY "order_id", "valid_from"'
        if table == "orders_scd2"
        else f'SELECT * FROM "{TARGET_LAYER}"."{table}" ORDER BY "order_id"'
    )


def main() -> None:
    engine = SkiferEngine(force_env="LOCAL_SQL")
    try:
        merged = _run_both_days(engine, "incremental.yaml", "orders_current")
        print(f"incremental merge — {len(merged)} row(s), one per order:")
        for row in merged:
            print(
                f"  {row['order_id']} | {row['customer']:<10} | "
                f"{row['status']:<8} | {row['amount']}"
            )

        history = _run_both_days(engine, "snapshot.yaml", "orders_scd2")
        print(f"\nsnapshot SCD2 — {len(history)} row(s), versions included:")
        for row in history:
            closed = row["valid_to"]
            window = "open" if closed is None else f"closed {closed}"
            print(
                f"  {row['order_id']} | {row['customer']:<10} | "
                f"{row['status']:<8} | {row['amount']:>3} | {window}"
            )

        # Order 1 changed status between the two days. Merge keeps the latest
        # value and forgets the old one; SCD2 keeps both, which is the entire
        # difference between the two strategies.
        merged_order_1 = [row for row in merged if row["order_id"] == 1]
        history_order_1 = [row for row in history if row["order_id"] == 1]
        if len(merged_order_1) != 1:
            raise AssertionError(f"merge should keep one row per key: {merged_order_1!r}")
        if len(history_order_1) != 2:
            raise AssertionError(
                f"SCD2 should keep both versions of order 1: {history_order_1!r}"
            )
        if merged_order_1[0]["status"] != "shipped":
            raise AssertionError("merge should carry the day-2 status")
        if {row["status"] for row in history_order_1} != {"pending", "shipped"}:
            raise AssertionError("SCD2 should keep the day-1 status alongside day 2")

        # Order 2 is byte-identical on both days. It must still have exactly one
        # version: an SCD2 write that closes and reinserts every key produces
        # the right answer for orders 1 and 4 and a spurious second version
        # here, which is the failure mode worth checking for.
        history_order_2 = [row for row in history if row["order_id"] == 2]
        if len(history_order_2) != 1:
            raise AssertionError(
                f"an unchanged order must not gain a version: {history_order_2!r}"
            )

        # Order 3 is absent from day 2. `on_missing: ignore` reads that as a
        # partial extract rather than a deletion, so its row stays open.
        order_3 = [row for row in history if row["order_id"] == 3]
        if len(order_3) != 1 or order_3[0]["valid_to"] is not None:
            raise AssertionError(
                f"on_missing: ignore should leave order 3 open: {order_3!r}"
            )

        print(
            "\nChecks: PASS — merge keeps 1 version of order 1, SCD2 keeps 2, "
            "an unchanged order gains none, and the row missing on day 2 stays open."
        )
    finally:
        engine.backend.connection.close()


if __name__ == "__main__":
    main()
