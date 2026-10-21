"""Plan semantic joins, fanout refusals and calendar periods from names only, without Spark.

    python examples/14_domain_graph/run.py
"""

from pathlib import Path
from types import SimpleNamespace

from skifer.agentic.resolver import QueryResolver, SemanticQuery, SemanticQueryError
from skifer.semantic.planner import SemanticPlanner
from skifer.semantic.semantic import SemanticEngine

semantic = SemanticEngine(SimpleNamespace(), models_dir=str(Path(__file__).parent / "semantic_models"))
planner = SemanticPlanner(semantic)

query = SemanticQuery(model_name="orders", metrics=["revenue"], group_by=["customers.segment"])
join = planner.plan(query).joins[0]
print("Names supplied: metric=revenue, dimension=customers.segment")
print("Resolved join path:")
print(f"  {join.source_model} --{join.relationship_name} ({join.cardinality})--> {join.target_model}")
print("Planner supplied the aliases and join condition; no LLM supplied SQL.")
print("Compiled SQL:")
print(QueryResolver(planner).resolve(query, semantic._get_model("orders"), None).full_sql)

# A period is a name; its bounds come from the versioned calendars/retail_445.yaml.
plan = planner.plan(SemanticQuery(model_name="orders", metrics=["revenue"], period="FY2025_H1"))
print("\nVersioned calendar:")
print(f"  {plan.period_name} -> {plan.date_from} through {plan.date_to} ({plan.calendar_key} v{plan.calendar_version})")

refused = {
    "unsafe fanout": SemanticQuery(model_name="orders", metrics=["revenue"], group_by=["order_lines.line_sku"]),
    "many-to-many path": SemanticQuery(model_name="order_lines", metrics=["line_revenue"], group_by=["customers.segment"]),
    "ambiguous dates": SemanticQuery(model_name="orders", metrics=["revenue"], period="FY2025_H1", date_from="2025-02-01"),
}
for label, bad_query in refused.items():
    try:
        planner.plan(bad_query)
    except SemanticQueryError as exc:
        print(f"Refused {label}: {exc}")
