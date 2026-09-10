"""Plan semantic joins and calendar dates without starting Spark.

    python examples/14_domain_graph/run.py

The planner accepts semantic names. Relationship conditions, stable aliases,
and calendar bounds come only from the curated YAML definitions.
"""

from pathlib import Path
from types import SimpleNamespace

from skifer.agentic.resolver import (
    QueryResolver,
    SemanticQuery,
    SemanticQueryError,
)
from skifer.semantic.planner import SemanticPlanner
from skifer.semantic.semantic import SemanticEngine


EXAMPLE_DIR = Path(__file__).parent


def main() -> None:
    semantic = SemanticEngine(
        SimpleNamespace(), models_dir=str(EXAMPLE_DIR / "semantic_models")
    )
    planner = SemanticPlanner(semantic)
    resolver = QueryResolver(planner)

    query = SemanticQuery(
        model_name="orders",
        metrics=["revenue"],
        group_by=["customers.segment"],
    )
    plan = planner.plan(query)
    resolved = resolver.resolve(query, semantic._get_model("orders"), None)
    join = plan.joins[0]

    print("Names supplied: metric=revenue, dimension=customers.segment")
    print("Resolved join path:")
    print(
        f"  {join.source_model} --{join.relationship_name} "
        f"({join.cardinality})--> {join.target_model}"
    )
    print("Planner supplied the aliases and join condition; no LLM supplied SQL.")
    print("Compiled SQL:")
    print(resolved.full_sql)

    period_plan = planner.plan(
        SemanticQuery(model_name="orders", metrics=["revenue"], period="FY2025_H1")
    )
    print("\nVersioned calendar:")
    print(
        f"  {period_plan.period_name} -> {period_plan.date_from} through "
        f"{period_plan.date_to} ({period_plan.calendar_key} "
        f"v{period_plan.calendar_version})"
    )

    try:
        planner.plan(
            SemanticQuery(
                model_name="orders",
                metrics=["revenue"],
                group_by=["order_lines.line_sku"],
            )
        )
    except SemanticQueryError as exc:
        print(f"Refused unsafe fanout: {exc}")

    try:
        planner.plan(
            SemanticQuery(
                model_name="order_lines",
                metrics=["line_revenue"],
                group_by=["customers.segment"],
            )
        )
    except SemanticQueryError as exc:
        print(f"Refused many-to-many path: {exc}")

    try:
        planner.plan(
            SemanticQuery(
                model_name="orders",
                metrics=["revenue"],
                period="FY2025_H1",
                date_from="2025-02-01",
            )
        )
    except SemanticQueryError as exc:
        print(f"Refused ambiguous dates: {exc}")


if __name__ == "__main__":
    main()
