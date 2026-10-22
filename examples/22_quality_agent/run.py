"""Run QualityAgent checks against deliberately bad local data.

    python examples/22_quality_agent/run.py
"""

import atexit
import shutil
from pathlib import Path
import tempfile

from skifer import SkiferEngine
from skifer.agentic.quality_agent import QualityAgent
from skifer.observability.checks import NullCheck, UniqueCheck
from skifer.observability.history import SqliteHistoryStore

engine = SkiferEngine(force_env="LOCAL")
schema = engine.get_target_schema("gold")
table = f"{schema}.quality_agent_orders"
engine.backend.ensure_schema_exists(schema)
engine.spark.createDataFrame([(1, 100.0), (1, None), (2, 50.0)], "order_id INT, amount DOUBLE").write.format(
    "delta"
).mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)

workdir = Path(tempfile.mkdtemp())
atexit.register(shutil.rmtree, workdir, ignore_errors=True)

store = SqliteHistoryStore(str(workdir / "quality_history.db"))
agent = QualityAgent(backend=engine.backend, history_store=store)

failed = agent.check(
    table,
    contracts=[
        NullCheck(table, column="amount", severity="critical"),
        UniqueCheck(table, columns=["order_id"], severity="critical"),
    ],
)
print("Two critical checks on the real table:")
for result in failed.report.results:
    print(f"  {type(result.contract).__name__}: {result.status.value} — {result.message.splitlines()[0]}")
print(f"  QualityResponse.passed: {failed.passed}")

# A check that cannot run is ERROR, never confused with a pass.
errored = agent.check(table, contracts=[NullCheck(table, column="missing_amount", severity="warning")])
result = errored.report.results[0]
print("\nA check that could not execute:")
print(f"  NullCheck status: {result.status.value}")
print(f"  Framework refusal: {result.message.splitlines()[0]}")
print(f"  QualityResponse.passed: {errored.passed}")

status = next(line.strip() for line in agent.report(table, format="text").splitlines() if "Status    :" in line)
print(f"\nLatest text report: {status}")

history = agent.get_history(table, n=10)
print("\nHistory after two runs:")
print(f"  mode: {history.mode}")
print(f"  reports recorded: {len(history.history_reports)}")
print(f"  checks per run (newest first): {', '.join(str(len(r.results)) for r in history.history_reports)}")
