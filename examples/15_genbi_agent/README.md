# 15 — GenBI agent: natural language to deterministic SQL

**What it shows:** `GenBIAgent` performs two LLM-assisted interpretation steps: it selects a
semantic model, then fills a `SemanticQuery`. The example's `StubLLMProvider` implements the real
provider interface but returns fixed JSON locally, with no API key or network.

## Run it

```bash
python examples/15_genbi_agent/run.py
```

The SQL is shown rather than run, so the example stays Spark-free. The two private agent steps are
called separately here only to stop at the learning boundary before `SemanticEngine` executes.

## What you should see

```text
🧠 [SemanticEngine] Catalogue chargé — 2 modèles indexés.
Question: Show revenue by region
Step A — stub LLM returned: {"selected":"orders","candidates":[],"reason":"The question asks about sales revenue."}
Step A — GenBIAgent selected model: orders
Step B — stub LLM returned names only: {"model_name":"orders","metrics":["revenue"],"group_by":["region"]}
Step B — SemanticQuery: model=orders, metrics=['revenue'], group_by=['region']
QueryResolver built from those names:
SELECT
    sales_region AS region,
    SUM(amount) AS revenue
FROM `gold`.`orders`
GROUP BY sales_region
SQL shown, not executed: this example stays Spark-free.
Bad stub LLM returned names only: {"model_name":"orders","metrics":["revenue"],"group_by":["regions"]}
Resolver refusal: Dimension 'regions' is not declared in root model 'orders' or any catalog model. Did you mean ['region']? Available: ['channel', 'region', 'warehouse']
```

Compare the Step B JSON with the SQL. The LLM returned `revenue` and `region`; it never saw or
returned `SUM(amount)`, `sales_region`, the physical table, or a `GROUP BY`. The deterministic
resolver read those definitions from `orders.yaml`.

The last proposal invents `regions`. The resolver refuses it before execution and reports the
nearest declared name, `region`. This is the safety boundary: plausible model output does not
become plausible SQL.

## Remember

The LLM proposes names only; the deterministic resolver either compiles declared names or refuses them.

## Next

[16 — Read-only MCP](../16_mcp_readonly/) exposes the governed semantic boundary to external agents
without exposing SQL or write operations.
