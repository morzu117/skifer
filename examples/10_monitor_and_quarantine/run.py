"""Publish one good batch, then quarantine a batch that breaks its contract.

    python examples/10_monitor_and_quarantine/run.py
"""

import atexit
import shutil
from pathlib import Path
import tempfile

from pyspark.sql import functions as F

from skifer import RuleRegistry, SkiferEngine
from skifer.observability.certification_store import SqliteCertificationStore
from skifer.observability.checks import DataQualityError
from skifer.observability.monitor import DataMonitor

EXAMPLE_DIR = Path(__file__).parent
SCHEMA = str(EXAMPLE_DIR / "monitored_orders.yaml")


@RuleRegistry.register_rule()
def invalidate_rejected_order(df):
    """Plants the bad batch's contract violation: a NULL order_id."""
    return {"order_id": F.when(F.col("status") == "rejected", F.lit(None).cast("integer")).otherwise(F.col("order_id"))}


workdir = Path(tempfile.mkdtemp())
atexit.register(shutil.rmtree, workdir, ignore_errors=True)

store = SqliteCertificationStore(str(workdir / "certification.db"))
engine = SkiferEngine(force_env="LOCAL")
engine.monitor = DataMonitor(engine.backend)
engine.certification_store = store
target = f"{engine.get_target_schema('gold')}.monitored_orders"


def run(batch):
    params = {**engine.default_params, "batch_file": (EXAMPLE_DIR / "data" / batch).as_posix()}
    engine.run_from_yaml(SCHEMA, "gold", "monitored_orders", params=params)


def rows(table, *extra):
    return engine.spark.table(table).select("order_id", "amount", *extra).orderBy("amount").collect()


run("good_orders.csv")
good = rows(target)
print("Good batch promoted to the consumer table:")
for row in good:
    print(f"  order_id={row.order_id} | amount={row.amount}")

try:
    run("bad_orders.csv")
except DataQualityError as exc:
    failure = exc.report.failures()[0]
    print(f"Framework refusal: {type(exc).__name__}")
    print(f"Failed check: {type(failure.contract).__name__} — {failure.message}")

after = rows(target)
print(f"Consumer table unchanged after refusal: {after == good}")
for row in after:
    print(f"  order_id={row.order_id} | amount={row.amount}")

quarantined = next(event for event in store.list_history(target) if event.state == "QUARANTINED")
print(f"Recorded publication state: {quarantined.state}")
print(f"Quarantine run id: {quarantined.run_id}")
print("Quarantined snapshot (the complete rejected batch):")
for row in rows(quarantined.quarantine_fqn, "_violations"):
    print(f"  order_id={row.order_id} | amount={row.amount} | _violations={row._violations or '<none>'}")
