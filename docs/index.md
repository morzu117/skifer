# Skifer

**Declarative Data Engineering · Semantic BI · Agentic NL Query · Governance — Spark and Databricks.**

Skifer is an end-to-end framework for Spark and Databricks Lakehouse: from raw ingestion
pipelines to natural language analytics — without writing SQL.

What makes it usable by agents rather than only by people is the governance running through it: a
dataset that is not certified is not queryable, every answer carries the evidence it relied on, and
the only way to act on a system outside the lakehouse is a declared capability a human approved.
The stabilized runtime target is Databricks plus local PySpark/Delta for development.
Eighteen runnable examples cover the feature surface, and the test suite executes every one so readers can trust that they stay current.

---

## Four layers, and governance across all of them

```
                                                    ┌──────────────────────────┐
┌─────────────────────────────────────────────────┐ │  G O V E R N A N C E     │
│  AGENTIC LAYER                                  │ │                          │
│  "Quel est le CA par pays pour le Q4 ?" → DF    │ │  Contract & certified    │
│  GenBIAgent · QueryResolver · SessionHistory    │◀┤  publication             │
├─────────────────────────────────────────────────┤ │  Certification gate      │
│  SEMANTIC LAYER                                 │ │  Evidence for every      │
│  YAML models · metrics · dimensions · catalog   │◀┤  answer                  │
│  SemanticEngine · SemanticBuilder · Planner     │ │  Runtime tracing         │
├─────────────────────────────────────────────────┤ │  Read-only MCP surface   │
│  OBSERVABILITY & LINEAGE LAYER                  │ │  Governed capabilities   │
│  YAML-as-contract · column lineage · checks     │◀┤  (write-back)            │
│  DataMonitor · ContractExtractor · Lineage      │ │                          │
├─────────────────────────────────────────────────┤ │  Fail-closed, always:    │
│  CORE LAYER                                     │ │  an unresolved question  │
│  Declarative YAML pipelines · Bronze→Silver→Gold│◀┤  is refused, never       │
│  SkiferEngine · RuleRegistry · SparkBackend │ │  answered anyway         │
└─────────────────────────────────────────────────┘ └──────────────────────────┘
                    Spark runtime (Databricks + local PySpark)
```

Governance is **not a fifth layer stacked on top** — it cuts through all four. A contract declared
on a pipeline in the core layer decides whether the semantic layer may serve a question about it,
and whether an agent gets an answer at all. The same certification that gates a dashboard gates an
autonomous agent.

| What governance does | Where it applies |
|---|---|
| **Contract & certified publication** — stage, validate, then promote or quarantine | Core layer, opt-in per pipeline |
| **Certification gate** — an uncertified dataset is not queryable | Semantic + agentic layers |
| **Evidence** — every answer carries the SQL hash, sources and freshness it relied on | Semantic + agentic layers |
| **Runtime tracing** — off by default, and business behaviour never depends on it | All layers |
| **Read-only MCP surface** — an external agent reaches only the governed layer | Semantic layer |
| **Governed capabilities** — the only path to an action outside the lakehouse | Above the agentic layer |


---

## Core Layer — No more PySpark spaghetti

Replace hundreds of lines of imperative PySpark with a declarative YAML schema.
Business logic lives in registered, testable Python functions — never in notebooks.

**Step 1: Define a business rule**

```python
from skifer import RuleRegistry
from pyspark.sql import functions as F

@RuleRegistry.register_rule(name="flag_vip_orders")
def flag_vip_orders(df):
    """Tags orders above 1000€ as VIP."""
    return df.withColumn("is_vip", F.col("amount_ttc") > 1000)
```

**Step 2: Define a declarative schema (YAML or Python dict)**

`schemas/gold/fact_orders.yaml`:

```yaml
tables:
  - name: "{{ catalog }}.bronze.raw_orders"
    alias: orders
  - name: "{{ catalog }}.bronze.raw_customers"
    alias: customers
    fields:
      - [customer_id, customer_id]
      - [segment, customer_segment]

join:
  - table_from: [orders, customer_id]
    table_to: [customers, customer_id]
    type: left

business_rules:
  - flag_vip_orders

select_final:
  - [order_id, order_id]
  - [order_date, order_date]
  - [channel, channel]
  - [customer_segment, customer_segment]
  - [amount_ttc, chiffre_affaires]
  - [is_vip, is_vip]
```

**Step 3: Execute**

```python
from skifer import SkiferEngine, load_schema

engine = SkiferEngine()
schema = load_schema("schemas/gold/fact_orders.yaml", params=engine.default_params)
engine.run_process_to_table(schema, target_layer="gold", target_table_name="fact_orders")
```

---

## Semantic Layer — LLM-generated BI models

`SemanticBuilder` analyses your transformation notebooks and business glossaries to generate
YAML semantic models — defining metrics, dimensions and filters in business terms.

```python
from skifer import SkiferEngine
from skifer.semantic.builder import SemanticBuilder
from skifer.semantic.llm_provider import get_llm_provider

engine = SkiferEngine()
builder = SemanticBuilder(llm_provider=get_llm_provider(), output_dir="semantic_models")

builder.build(
    model_key="kpi_orders",
    table="gold.fact_orders",
    layer="gold",
    description="Sales KPIs: revenue, orders, basket size by channel and country",
    base_filter="status = 'completed'",
    source_notebook="notebooks/transform_orders.ipynb",
)
```

The output is a validated YAML model + an updated `semantic_catalog.yaml` index.

---

## Agentic Layer — Natural language to analytics

Ask business questions in plain language. `GenBIAgent` selects the right semantic model,
translates the question to a validated `SemanticQuery`, and executes it on Spark/Databricks.
The LLM never writes SQL and never sees your data.

```python
from skifer import SkiferEngine

engine = SkiferEngine()
agent = engine.get_agent()  # Shorthand factory method (auto-init SemanticEngine + GenBIAgent)

resp = agent.ask("Quel est le chiffre d'affaires par pays pour le Q4 2024 ?")

if resp.success:
    resp.result.data.show()

agent.session.to_pdf("weekly_report.pdf")
```

Or initialize manually for more control:

```python
from skifer.agentic.agent import GenBIAgent

agent = GenBIAgent(semantic, get_llm_provider(), history=True)
```

---

## Key design principles

| Principle | Implementation |
|---|---|
| **No SQL in notebooks** | `process_schema()` + `@RuleRegistry.register_rule` |
| **LLM never sees data** | QueryResolver validates names before any execution |
| **LLM never writes SQL** | Produces only metric/dimension names; SQL built deterministically |
| **Catalog-first** | `semantic_catalog.yaml` loaded once at startup (~500 tokens for LLM) |
| **Environment-aware** | Auto-detects Dev/QA/Prod; sandbox mode isolates interactive work |
| **Provider-agnostic LLM** | OpenAI · Anthropic · Google — auto-detected from env vars |
| **Spark-first runtime** | Databricks Lakehouse in production, local PySpark/Delta for development |
| **YAML-as-contract** | Quality checks and lineage derived automatically from existing YAML — zero extra config |
| **Fail-closed everywhere** | A missing store, an unresolved dependency, an ambiguous join: refused, never silently allowed |
| **An LLM informs, never authorizes** | A model's output can shape a decision; it can never grant access or trigger an action |
| **Every answer carries its evidence** | SQL hash, source datasets, certification snapshot and freshness — readable after the session ends |
| **Nothing acts without approval** | A capability runs in shadow by default; an approval binds one exact request and expires |

---

## Installation

```bash
# Core (no platform dependencies)
pip install skifer

# Spark / Databricks runtime
pip install "skifer[spark]"              # Databricks + local PySpark/Delta
pip install "skifer[databricks]"         # workspace API: SQL warehouses, materialized views

# With LLM support
pip install "skifer[llm-openai]"        # OpenAI
pip install "skifer[llm-anthropic]"     # Anthropic Claude
pip install "skifer[llm-google]"        # Google Gemini

# Everything used by the stabilized Spark/Databricks path
pip install "skifer[spark,semantic-full]"
```

→ [Getting Started](getting_started.md) · [Core Engine](core.md) · [Lineage & Dictionary](lineage.md) · [Data Observability](observability.md) · [Semantic Layer](semantic.md) · [Agentic Layer](agentic.md)
