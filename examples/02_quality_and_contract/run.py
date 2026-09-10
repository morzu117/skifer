"""Publish a table under a contract, and see what certification records.

    python examples/02_quality_and_contract/run.py

Nothing here talks to Databricks. The certification registry is a local SQLite
file, and the table is written to the local Delta warehouse.
"""

from pathlib import Path
import tempfile

from skifer import SkiferEngine
from skifer.observability.certification_store import SqliteCertificationStore
from skifer.observability.monitor import DataMonitor

EXAMPLE_DIR = Path(__file__).parent


def main() -> None:
    registry_path = Path(tempfile.mkdtemp()) / "certification.db"
    store = SqliteCertificationStore(str(registry_path))

    # A pipeline that declares `data_product` MUST be given both a monitor and a
    # certification store. Publishing under a contract with nothing to check the
    # contract against, or nowhere to record the verdict, fails fast rather than
    # quietly publishing an uncertified table.
    engine = SkiferEngine(force_env="LOCAL")
    engine.monitor = DataMonitor(engine.backend)
    engine.certification_store = store

    engine.run_from_yaml(
        str(EXAMPLE_DIR / "gold_orders.yaml"),
        "gold",
        "fact_orders",
        params={**engine.default_params, "example_dir": str(EXAMPLE_DIR)},
    )

    schema = engine.get_target_schema("gold")
    fqn = f"{schema}.fact_orders"
    print(f"\nPublished: {fqn}")
    engine.spark.table(fqn).orderBy("order_id").show(truncate=False)

    # This is what the semantic layer and any agent consult before answering a
    # question about this table. An uncertified dataset is simply not queryable.
    certification = store.get_certification(fqn)
    print("Certification for this dataset:")
    print(f"  status           : {certification.status}")
    print(f"  checks passed    : {certification.checks_passed}")
    print(f"  contract version : {certification.contract_version}")
    print(f"  certified at     : {certification.certified_at}")


if __name__ == "__main__":
    main()
