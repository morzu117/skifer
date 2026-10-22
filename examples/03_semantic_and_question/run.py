"""Query a certified table through semantic names and get the evidence back.

    python examples/03_semantic_and_question/run.py
"""

import atexit
import shutil
from pathlib import Path
import tempfile

from skifer import SkiferEngine
from skifer.agentic.resolver import SemanticQuery, SemanticQueryError
from skifer.observability.certification_store import SqliteCertificationStore
from skifer.observability.monitor import DataMonitor
from skifer.semantic.semantic import SemanticEngine

EXAMPLE_DIR = Path(__file__).parent
GOLD_SCHEMA = EXAMPLE_DIR.parent / "02_quality_and_contract" / "gold_orders.yaml"

workdir = Path(tempfile.mkdtemp())
atexit.register(shutil.rmtree, workdir, ignore_errors=True)

store = SqliteCertificationStore(str(workdir / "certification.db"))
engine = SkiferEngine(force_env="LOCAL")
engine.schema_suffix = ""  # the shared governed table, not a personal sandbox copy
engine.monitor = DataMonitor(engine.backend)
engine.certification_store = store
engine.config["environments"]["LOCAL"]["semantic_certification_policy"] = "enforce"

engine.run_from_yaml(
    str(GOLD_SCHEMA),
    "gold",
    "fact_orders",
    params={**engine.default_params, "example_dir": GOLD_SCHEMA.parent.as_posix()},
)

semantic = SemanticEngine(
    engine, models_dir=str(EXAMPLE_DIR / "semantic_models"), certification_store=store
)
result = semantic.query_with_evidence(
    SemanticQuery(
        model_name="sales.orders",
        metrics=["total_revenue", "order_count"],
        group_by=["region"],
        date_from="2026-01-01",
        date_to="2026-01-31",
    )
)

print("\nQuestion: What were revenue and order count by region in January 2026?")
result.dataframe.orderBy("region").show(truncate=False)

evidence = result.evidence.to_dict()
print("Evidence:")
print(f"  SQL hash : {evidence['sql_hash']}")
print(f"  datasets : {[source['dataset'] for source in evidence['sources']]}")
print("  certification snapshot:")
for source in evidence["sources"]:
    print(f"    {source['dataset']}: {source['certification_status']} (contract {source['contract_version']})")

try:
    semantic.query(SemanticQuery(model_name="sales.orders", metrics=["total_revenues"]))
except SemanticQueryError as exc:
    print(f"\nUnknown name refused: {exc}")
