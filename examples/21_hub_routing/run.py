"""The hub routes questions to static lineage and dictionary agents, without Spark or an LLM.

    python examples/21_hub_routing/run.py
"""

from pathlib import Path
import runpy

from skifer import load_schema
from skifer.agentic.dictionary_agent import DictionaryAgent
from skifer.agentic.hub import AgenticHub
from skifer.agentic.lineage_agent import LineageAgent

EXAMPLE_05 = Path(__file__).parent.parent / "05_rules_join_aggregate"
TARGET = "gold.joined_orders"

# Registers example 05's `classify_order` rule: the agents analyse it statically.
runpy.run_path(str(EXAMPLE_05 / "rules.py"))
schema = load_schema(str(EXAMPLE_05 / "joined_orders.yaml"), params={"example_dir": EXAMPLE_05.as_posix()})
dictionary = DictionaryAgent(schema_dict=schema, target_name=TARGET)
hub = AgenticHub(lineage_agent=LineageAgent(schema_dict=schema, target_name=TARGET), dictionary_agent=dictionary)

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

print("\nHub routing without an LLM or Spark:")
for question in [
    "Where does gold.joined_orders.order_class come from?",
    "Run checks on gold.joined_orders",
    "Revenue by region",
]:
    response = hub.ask(question)
    print(f"  {question} -> mode={response.mode}")
    if response.error:
        print(f"    Framework refusal: {response.error}")

undetermined = dictionary.ask("Tell me about order_class")
print("\nDictionaryAgent refuses to guess an undetermined intent:")
print(f"  mode: {undetermined.mode}")
print(f"  {undetermined.error}")
