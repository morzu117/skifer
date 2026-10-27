"""Read column-level lineage out of a pipeline without running it.

    python examples/11_lineage_and_dictionary/run.py
"""

from pathlib import Path
import runpy

from pyspark.sql import functions as F

from skifer import load_schema
from skifer.core.rule_analyzer import RuleAnalyzer
from skifer.lineage.dictionary import DataDictionary
from skifer.lineage.renderer import LineageRenderer
from skifer.lineage.tracker import LineageTracker

EXAMPLE_05 = Path(__file__).parent.parent / "05_rules_join_aggregate"
TARGET = "gold.joined_orders"

runpy.run_path(str(EXAMPLE_05 / "rules.py"))  # registers classify_order; starts no Spark
schema = load_schema(str(EXAMPLE_05 / "joined_orders.yaml"), params={"example_dir": EXAMPLE_05.as_posix()})
graph = LineageTracker.from_schema(schema, target_name=TARGET)
print("Static analysis only: no Spark session was created.")

for title, column in [
    ("A column read straight from a source table:", "amount"),
    ("A column no source table contains, made by a Python rule:", "order_class"),
]:
    print(f"\n{title}\n{column}:")
    for edge in graph.upstream(TARGET, column):
        via = f" via {', '.join(edge.transformations)}" if edge.transformations else ""
        print(f"  from {edge.source_table}.{edge.source_column} [{edge.edge_type}]{via}")

entry = DataDictionary(graph).get(TARGET, "order_class")
print(f"\nDictionary entry:\n  field: {entry.table}.{entry.name}\n  sources: {', '.join(entry.source_fields)}")

print("\nMermaid, the rule-made column only:")
for line in LineageRenderer().to_mermaid(graph).splitlines():
    if line == "graph LR" or "order_class" in line:
        print(line)


def opaque_rule(df):
    name = "_".join(["order", "bucket"])  # built at runtime: invisible to static analysis
    return {name: F.col("amount")}


profile = RuleAnalyzer().analyze_rule(opaque_rule, name="opaque_rule")
print("\nWhat static analysis cannot see:")
print(f"  opaque_rule declares outputs: {profile.output_columns or '<none detected>'}")
print("  A name built at runtime is invisible; lineage omits it silently.")
