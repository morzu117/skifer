"""Read column-level lineage out of a pipeline, without running it.

    python examples/11_lineage_and_dictionary/run.py

Lineage here is static analysis: the YAML is parsed and the Python rules are
read as source. No Spark session is created and no data is touched.
"""

from pathlib import Path
import runpy

from skifer import load_schema
from skifer.core.rule_analyzer import RuleAnalyzer
from skifer.lineage.dictionary import DataDictionary
from skifer.lineage.renderer import LineageRenderer
from skifer.lineage.tracker import LineageTracker

EXAMPLE_DIR = Path(__file__).parent
EXAMPLE_05 = EXAMPLE_DIR.parent / "05_rules_join_aggregate"
TARGET = "gold.joined_orders"


def _print_upstream(graph, column: str) -> None:
    print(f"{column}:")
    for edge in graph.upstream(TARGET, column):
        origin = f"{edge.source_table}.{edge.source_column}"
        detail = f" via {', '.join(edge.transformations)}" if edge.transformations else ""
        print(f"  from {origin} [{edge.edge_type}]{detail}")


def opaque_rule(df):
    """A rule static analysis cannot read: the column name is built at runtime."""
    from pyspark.sql import functions as F

    name = "_".join(["order", "bucket"])
    return {name: F.col("amount")}


def main() -> None:
    # Example 05 registers `classify_order`. Importing its module runs the
    # decorator; it does not run its pipeline, and it starts no Spark session.
    runpy.run_path(str(EXAMPLE_05 / "run.py"))
    schema = load_schema(
        str(EXAMPLE_05 / "joined_orders.yaml"),
        params={"example_dir": str(EXAMPLE_05)},
    )
    graph = LineageTracker.from_schema(schema, target_name=TARGET)

    print("Static analysis only: no Spark session was created.")
    print("\nA column read straight from a source table:")
    _print_upstream(graph, "amount")

    print("\nA column no source table contains, made by a Python rule:")
    _print_upstream(graph, "order_class")

    entry = DataDictionary(graph).get(TARGET, "order_class")
    print("\nDictionary entry:")
    print(f"  field: {entry.table}.{entry.name}")
    print(f"  sources: {', '.join(entry.source_fields)}")

    print("\nMermaid, the rule-made column only:")
    for line in LineageRenderer().to_mermaid(graph).splitlines():
        if line == "graph LR" or "order_class" in line:
            print(line)

    # The honest limit. A column name assembled at runtime is invisible to an
    # AST reader, and the tracker says nothing about it rather than guessing.
    profile = RuleAnalyzer().analyze_rule(opaque_rule, name="opaque_rule")
    print("\nWhat static analysis cannot see:")
    print(f"  opaque_rule declares outputs: {profile.output_columns or '<none detected>'}")
    print("  A name built at runtime is invisible; lineage omits it silently.")


if __name__ == "__main__":
    main()
