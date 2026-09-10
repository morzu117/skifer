# 21 — Route questions through the agentic hub

**What it shows:** `DictionaryAgent` and `LineageAgent` build useful answers from a pipeline's
static metadata, while `AgenticHub` routes questions to the injected specialist. No Spark session,
LLM, API key, cluster, or network is involved.

## Run it

```bash
python examples/21_hub_routing/run.py
```

The example registers the Python rule from [example 05](../05_rules_join_aggregate/) and loads its
YAML exactly as the static lineage example does. Importing the rule module does not run its
pipeline. The dictionary therefore sees both the real source column and the honest `<rule>` origin
of `order_class` without reading a table.

## What you should see

```text
Dictionary lookup:
  mode: lookup
  field: gold.joined_orders.order_class
  source fields: <rule>.order_class, raw_orders.amount
  transformations: rule:classify_order

Misspelt lookup:
  mode: error
  error: Column 'order_clas' not found in dictionary.
  suggestions: order_class, order_id

Hub routing without an LLM or Spark:
  Where does gold.joined_orders.order_class come from? -> mode=trace
  Run checks on gold.joined_orders -> mode=error
    Framework refusal: Agent 'quality' non configure dans ce hub.
  Revenue by region -> mode=error
    Framework refusal: Agent 'genbi' non configure dans ce hub.

DictionaryAgent refuses to guess an undetermined intent:
  mode: error
  Could not determine intent from the question. Use lookup(), list_fields(), or export() directly, or provide an llm_provider for natural language support.
```

The lineage question reaches the injected `LineageAgent`. Quality and business-query questions
name their missing target agents instead of falling through to some unrelated specialist. A direct
dictionary lookup remains available when natural-language intent is genuinely ambiguous.

## Remember

Deterministic routing should expose a missing agent or unknown intent, never invent an answer.

## Next

[22 — Quality agent](../22_quality_agent/) runs contracts against a real local table and preserves
both failures and execution errors in history.
