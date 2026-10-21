"""Publish a table under a contract and read back its certification.

    python examples/02_quality_and_contract/run.py
"""

from pathlib import Path
import tempfile

from skifer import SkiferEngine
from skifer.observability.certification_store import SqliteCertificationStore
from skifer.observability.monitor import DataMonitor

EXAMPLE_DIR = Path(__file__).parent

store = SqliteCertificationStore(str(Path(tempfile.mkdtemp()) / "certification.db"))

# A pipeline declaring `data_product` needs both a monitor and a certification store.
engine = SkiferEngine(force_env="LOCAL")
engine.monitor = DataMonitor(engine.backend)
engine.certification_store = store

engine.run_from_yaml(
    str(EXAMPLE_DIR / "gold_orders.yaml"),
    "gold",
    "fact_orders",
    params={**engine.default_params, "example_dir": EXAMPLE_DIR.as_posix()},
)

fqn = f"{engine.get_target_schema('gold')}.fact_orders"
print(f"\nPublished: {fqn}")
engine.spark.table(fqn).orderBy("order_id").show(truncate=False)

certification = store.get_certification(fqn)
print("Certification for this dataset:")
print(f"  status           : {certification.status}")
print(f"  checks passed    : {certification.checks_passed}")
print(f"  contract version : {certification.contract_version}")
print(f"  certified at     : {certification.certified_at}")
