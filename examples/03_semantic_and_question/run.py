"""Query the certified orders table through deterministic semantic names."""

from pathlib import Path
import tempfile

from skifer import SkiferEngine
from skifer.agentic.resolver import SemanticQuery, SemanticQueryError
from skifer.observability.certification_store import SqliteCertificationStore
from skifer.observability.monitor import DataMonitor
from skifer.semantic.semantic import SemanticEngine


EXAMPLE_DIR = Path(__file__).parent
GOLD_SCHEMA = EXAMPLE_DIR.parent / "02_quality_and_contract" / "gold_orders.yaml"


def main() -> None:
    registry_path = Path(tempfile.mkdtemp()) / "certification.db"
    store = SqliteCertificationStore(str(registry_path))
    engine = SkiferEngine(force_env="LOCAL")

    # This example represents the stable governed table, rather than an
    # interactive personal sandbox copy.
    engine.schema_suffix = ""
    engine.monitor = DataMonitor(engine.backend)
    engine.certification_store = store
    engine.config["environments"]["LOCAL"][
        "semantic_certification_policy"
    ] = "enforce"

    engine.run_from_yaml(
        str(GOLD_SCHEMA),
        "gold",
        "fact_orders",
        params={**engine.default_params, "example_dir": str(GOLD_SCHEMA.parent)},
    )

    semantic = SemanticEngine(
        engine,
        models_dir=str(EXAMPLE_DIR / "semantic_models"),
        certification_store=store,
    )
    query = SemanticQuery(
        model_name="sales.orders",
        metrics=["total_revenue", "order_count"],
        group_by=["region"],
        date_from="2026-01-01",
        date_to="2026-01-31",
        explanation="Revenue and order count by region in January 2026",
    )
    result = semantic.query_with_evidence(query)

    print("\nQuestion: What were revenue and order count by region in January 2026?")
    result.dataframe.orderBy("region").show(truncate=False)

    evidence = result.evidence.to_dict()
    print("Evidence:")
    print(f"  SQL hash : {evidence['sql_hash']}")
    print(f"  datasets : {[source['dataset'] for source in evidence['sources']]}")
    print("  certification snapshot:")
    for source in evidence["sources"]:
        print(
            f"    {source['dataset']}: {source['certification_status']} "
            f"(contract {source['contract_version']})"
        )

    print("\nWhat GenBIAgent adds (not called here):")
    print("  Natural language -> model, metric, dimension, and filter names")
    print("  QueryResolver -> deterministic SQL from those declared names")

    try:
        semantic.query(
            SemanticQuery(model_name="sales.orders", metrics=["total_revenues"])
        )
    except SemanticQueryError as exc:
        print(f"  Unknown name refused: {exc}")


if __name__ == "__main__":
    main()
