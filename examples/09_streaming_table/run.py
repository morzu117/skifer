"""Two incremental streaming upserts sharing one automatic checkpoint.

    python examples/09_streaming_table/run.py
"""

import atexit
import shutil
from pathlib import Path
from tempfile import mkdtemp

import yaml

from skifer import SkiferEngine, load_schema

EXAMPLE_DIR = Path(__file__).parent
SCHEMA = str(EXAMPLE_DIR / "orders_current.yaml")

engine = SkiferEngine(force_env="LOCAL")
engine.full_refresh("streaming_example", "orders_current")  # drops table and checkpoint together
print("Startup reset: target table and automatic checkpoint cleared.")

workdir = Path(mkdtemp())
atexit.register(shutil.rmtree, workdir, ignore_errors=True)

source_path = str(workdir / "raw_orders")
params = {**engine.default_params, "source_path": source_path}
target = f"{engine.get_target_schema('streaming_example')}.orders_current"
checkpoint = engine.resolve_checkpoint_location(
    engine.get_target_schema("streaming_example"),
    "orders_current",
    yaml.safe_load(Path(SCHEMA).read_text())["materialization"],
)


def append_source(rows):
    engine.spark.createDataFrame(rows, ["order_id", "status"]).write.format("delta").mode("append").save(source_path)


def show_target(run):
    for row in engine.spark.table(target).orderBy("order_id").collect():
        print(f"order_id={row.order_id} | status={row.status}")
    # An upsert replay would give the same rows: the checkpoint is what proves the run was incremental.
    commits = list((Path(checkpoint.removeprefix("file:")) / "commits").glob("[0-9]*"))
    print(f"Checkpoint commits after run {run}: {len(commits)}")


append_source([(1, "pending"), (2, "pending")])
engine.run_from_yaml(SCHEMA, "streaming_example", "orders_current", params=params)
first = engine.spark.table(target).count()
print(f"\nRun 1 added {first} target rows:")
show_target(1)

append_source([(1, "shipped"), (3, "pending")])  # order 1 changes, order 3 arrives
engine.run_from_yaml(SCHEMA, "streaming_example", "orders_current", params=params)
added = engine.spark.table(target).count() - first
sources = engine.spark.read.format("delta").load(source_path).count()
print(f"\nRun 2 added {added} target row, from {sources} source rows:")
show_target(2)
print("Order 1 was updated in place; it was not duplicated.")

try:
    load_schema(str(EXAMPLE_DIR / "refused_dev_limit.yaml"), params=params)
except ValueError as exc:
    print(f"Refused schema: {exc}")
