"""Route agent questions and inspect metadata without Spark or an LLM.

    python examples/21_hub_routing/run.py
"""

from pathlib import Path
import runpy

from skifer import load_schema
from skifer.agentic.dictionary_agent import DictionaryAgent
from skifer.agentic.hub import AgenticHub
from skifer.agentic.lineage_agent import LineageAgent

EXAMPLE_DIR = Path(__file__).parent
EXAMPLE_05 = EXAMPLE_DIR.parent / "05_rules_join_aggregate"
TARGET = "gold.joined_orders"


def main() -> None:
    # Importing example 05 registers `classify_order`; its main() is not called,
    # so this performs static analysis without creating a Spark session.
    runpy.run_path(str(EXAMPLE_05 / "run.py"))
    schema = load_schema(
        str(EXAMPLE_05 / "joined_orders.yaml"),
        params={"example_dir": str(EXAMPLE_05)},
    )
    dictionary = DictionaryAgent(schema_dict=schema, target_name=TARGET)
    lineage = LineageAgent(schema_dict=schema, target_name=TARGET)

    found = dictionary.lookup(TARGET, "order_class")
    print("Dictionary lookup:")
    print(f"  mode: {found.mode}")
    print(f"  field: {found.entry.table}.{found.entry.name}")
    print(f"  source fields: {', '.join(found.entry.source_fields)}")
    print(f"  transformations: {', '.join(found.entry.transformations)}")

    missing = dictionary.lookup(TARGET, "order_clas")
    print("\nMisspelt lookup:")
    print(f"  mode: {missing.mode}")
    print(f"  error: {missing.error}")
    print(f"  suggestions: {', '.join(missing.suggestions)}")

    hub = AgenticHub(lineage_agent=lineage, dictionary_agent=dictionary)
    questions = [
        "Where does gold.joined_orders.order_class come from?",
        "Run checks on gold.joined_orders",
        "Revenue by region",
    ]
    print("\nHub routing without an LLM or Spark:")
    for question in questions:
        response = hub.ask(question)
        print(f"  {question} -> mode={response.mode}")
        if response.error:
            print(f"    Framework refusal: {response.error}")

    undetermined = dictionary.ask("Tell me about order_class")
    print("\nDictionaryAgent refuses to guess an undetermined intent:")
    print(f"  mode: {undetermined.mode}")
    print(f"  {undetermined.error}")


if __name__ == "__main__":
    main()
