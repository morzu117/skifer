"""Publish one good batch, then quarantine a batch that breaks its contract.

    python examples/10_monitor_and_quarantine/run.py
"""

from pathlib import Path
import tempfile

from pyspark.sql import functions as F

from skifer import RuleRegistry, SkiferEngine
from skifer.observability.certification_store import (
    SqliteCertificationStore,
)
from skifer.observability.checks import DataQualityError
from skifer.observability.monitor import DataMonitor

EXAMPLE_DIR = Path(__file__).parent
TARGET_TABLE = "monitored_orders"


@RuleRegistry.register_rule()
def invalidate_rejected_order(df):
    """Introduce the bad batch's contract violation after source cleanup."""
    return {
        "order_id": F.when(
            F.col("status") == "rejected", F.lit(None).cast("integer")
        ).otherwise(F.col("order_id"))
    }


def _rows(engine, fqn, *columns):
    return [
        row.asDict()
        for row in engine.spark.table(fqn).select(*columns).orderBy("amount").collect()
    ]


def main() -> None:
    registry_path = Path(tempfile.mkdtemp()) / "certification.db"
    store = SqliteCertificationStore(str(registry_path))
    engine = SkiferEngine(force_env="LOCAL")
    engine.monitor = DataMonitor(engine.backend)
    engine.certification_store = store

    schema_path = str(EXAMPLE_DIR / "monitored_orders.yaml")
    params = {**engine.default_params}

    engine.run_from_yaml(
        schema_path,
        "gold",
        TARGET_TABLE,
        params={**params, "batch_file": str(EXAMPLE_DIR / "data/good_orders.csv")},
    )
    target_fqn = f"{engine.get_target_schema('gold')}.{TARGET_TABLE}"
    good_rows = _rows(engine, target_fqn, "order_id", "amount")
    print("Good batch promoted to the consumer table:")
    for row in good_rows:
        print(f"  order_id={row['order_id']} | amount={row['amount']}")

    try:
        engine.run_from_yaml(
            schema_path,
            "gold",
            TARGET_TABLE,
            params={**params, "batch_file": str(EXAMPLE_DIR / "data/bad_orders.csv")},
        )
    except DataQualityError as exc:
        failure = exc.report.failures()[0]
        print(f"Framework refusal: {type(exc).__name__}")
        print(f"Failed check: {type(failure.contract).__name__} — {failure.message}")
    else:  # pragma: no cover - this is a runnable safety assertion
        raise AssertionError("The bad batch was unexpectedly promoted.")

    consumer_rows = _rows(engine, target_fqn, "order_id", "amount")
    print(f"Consumer table unchanged after refusal: {consumer_rows == good_rows}")
    for row in consumer_rows:
        print(f"  order_id={row['order_id']} | amount={row['amount']}")

    quarantined = next(
        event
        for event in store.list_history(target_fqn)
        if event.state == "QUARANTINED"
    )
    print(f"Recorded publication state: {quarantined.state}")
    print(f"Quarantine run id: {quarantined.run_id}")
    print("Quarantined snapshot (the complete rejected batch):")
    quarantine_rows = _rows(
        engine,
        quarantined.quarantine_fqn,
        "order_id",
        "amount",
        "_violations",
    )
    for row in quarantine_rows:
        print(
            f"  order_id={row['order_id']} | amount={row['amount']} "
            f"| _violations={row['_violations'] or '<none>'}"
        )


if __name__ == "__main__":
    main()
