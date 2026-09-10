# Skifer Framework

> **An Enterprise-Ready, Declarative Data Engineering Framework for Spark and Databricks Lakehouse.**

Skifer transforms complex PySpark pipelines into standardized, readable, and maintainable
declarative contracts. By strictly decoupling the **What** (YAML schemas) from the **How** (Python
business rules), it enables robust Bronze → Silver → Gold pipelines on Databricks and local Spark,
optimized for Power BI Direct Query.

It then goes one step further: the same YAML that builds a table declares the contract that governs
it. A dataset that is not certified is not queryable, every answer carries the evidence it relied on,
and the only way an agent can act on a system outside the lakehouse is a declared capability that a
human approved. **Governance is not a layer on top — it runs through all of them.**

---

## Key Capabilities

### Build pipelines without PySpark spaghetti

- **YAML Pipeline Schemas** — sources, joins, transformations and quality checks in readable YAML. No more 1,000-line notebooks.
- **External Declarative Sources** — CSV, Parquet, JSON, Avro, ORC, Delta or Text from ADLS Gen2, DBFS, S3-compatible mounts or local storage, straight from YAML.
- **Self-documenting operators** — `region:equals:EMEA`, `amount:greater_than_equal:100`, `status:in:ACTIVE,PENDING`. Nothing to memorize.
- **Business Logic Isolation** — register pure Python/PySpark rules with `@RuleRegistry.register_rule()`.
- **Streaming tables & materialized views** — incremental writes and native compiled SQL views, declared the same way.
- **Direct Query Optimized** — pre-calculate OBT, YoY shifts and distinct counts in Gold to keep Power BI DAX light.

### Ask questions in natural language

- **Semantic Layer** — generate semantic YAML models from your Gold tables, then query them in plain language with `GenBIAgent`. The LLM proposes names; the SQL is built deterministically.
- **Domain graph & joins** — declare grain, entities and relationships; join paths are resolved from names, and an unsafe fanout is refused rather than silently wrong.
- **Evidence for every answer** — the SQL hash actually executed, the source columns, a certification snapshot and the freshness behind the number.

### Govern what people and agents may do

- **Contracts & certified publication** — stage, validate, then promote or quarantine. Opt-in per pipeline.
- **Certification gate** — an uncertified or stale dataset is not queryable; the policy is per environment, from `off` to `enforce`.
- **Read-only MCP surface** — an external agent discovers and queries only the governed layer, filtered by its scopes.
- **Governed Capabilities** — the single path to an action on an external system: a closed schema, deterministic preconditions, an explicit autonomy mode, an approval bound to that exact request, a just-in-time credential the agent never sees, and an append-only journal that makes a duplicate impossible. Shadow by default: nothing executes.
- **Supervised Adaptive Gold** — real usage produces explainable Gold/materialized-view *proposals* with their evidence. Nothing is ever deployed: a human accepts or rejects, and a measured regression only ever asks for a review.

### Run it anywhere Spark runs

- **Local Spark Development Mode** — the whole framework on `local[*]` PySpark + Delta. No cluster required.
- **Smart Sandbox** — in interactive mode, source tables resolve to your personal sandbox schema; missing tables are cloned transparently.
- **Environment Aware** — auto-detects Dev / QA / Prod Databricks catalogs at runtime, with per-user isolation.


---

## Architecture

```
Bronze (Raw)  →  Silver (Standardized)  →  Gold (Semantic / OBT)
                          │
        ┌─────────────────┴─────────────────────┐
        │          Skifer             │
        │  1. Declarative schema (YAML / dict)  │
        │  2. Rule registry (Python logic)      │
        │  3. Delta I/O, streaming, MVs         │
        │  4. Semantic layer + agents           │
        └─────────────────┬─────────────────────┘
                          │
   ┌──────────────────────┴──────────────────────┐
   │  GOVERNANCE — across all four, not above    │
   │  contract · certification · evidence        │
   │  tracing · MCP surface · capabilities       │
   └──────────────────────┬──────────────────────┘
                          │
        ┌─────────────────┼─────────────────┐
        │                 │                 │
   Power BI          A person           An agent
 (Direct Query)   (natural language)  (MCP, scoped)
```

Every consumer on the bottom row goes through the same gate. The certification that decides whether
a dashboard may read a table is the certification that decides whether an agent may — there is no
second, looser path for automation.

---

## Installation

```bash
pip install skifer
pip install "skifer[spark]"        # Databricks + local PySpark/Delta
pip install "skifer[databricks]"   # workspace API: SQL warehouses, materialized views
pip install "skifer[mcp]"           # optional read-only MCP server (`mcp>=2.1,<3`)

# Optional — LLM providers for the Semantic Layer
pip install skifer[llm-anthropic]   # Claude (Anthropic)
pip install skifer[llm-openai]      # GPT (OpenAI)
pip install skifer[llm-google]      # Gemini (Google)

# Optional — PDF export for session history
pip install skifer[semantic-pdf]
```

---

## Local Development Setup

Run the same Spark-oriented framework on your laptop without a Databricks cluster. PySpark runs in `local[*]` mode, Delta Lake is enabled, and Apache Derby serves as the embedded metastore (no installation required).

### 1. Create `config.yaml` at your project root

```yaml
default_env: LOCAL
priority_check: [DEV, QA, PROD, LOCAL]

environments:
  LOCAL:
    catalog: null          # null = no Unity Catalog, triggers local mode
    is_production: false

  DEV:
    catalog: "my_dev_catalog"
    is_production: false

  PROD:
    catalog: "my_prod_catalog"
    is_production: true
```

When `catalog` is `null`, the engine skips all Databricks catalog checks and boots in local mode. When Databricks credentials are present (`DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_CLUSTER_ID`), the engine tries Databricks Connect first and falls back to local automatically.

### 2. (Optional) `.env` for Databricks credentials

```ini
# Only needed for Databricks Connect (remote cluster from IDE)
DATABRICKS_HOST=https://your-workspace.azuredatabricks.net
DATABRICKS_TOKEN=dapiXXXXXX
DATABRICKS_CLUSTER_ID=0123-456789-abcdef

# LLM provider for the Semantic Layer
ANTHROPIC_API_KEY=sk-ant-...
```

### 3. Boot the engine

```python
from skifer import SkiferEngine

engine = SkiferEngine()  # auto-discovers config.yaml upwards from cwd
# → boots local[*] Spark + Delta Lake + Derby metastore
# → env=LOCAL, no catalog prefix, sandbox suffix = _<your_os_user>
```

Session detection priority:
1. **Active Databricks session** — running inside a notebook or cluster
2. **Databricks Connect v2** — IDE + remote cluster via `.env` credentials
3. **Local PySpark + Delta Lake** — fully offline, zero configuration

---

## Quick Start: Building a Pipeline

### Option A — YAML file (recommended)

Store schemas as YAML files in your project for reuse and version control.

```yaml
# schemas/gold/fact_transactions.yaml
tables:
  - name: "{{ catalog }}.silver.transactions"
    alias: tx
    filter:
      - "region:equals:EMEA"
      - "customer_id:is_not_null"
    quality_checks:
      drop_duplicates_on: [transaction_id]
      drop_nulls_in: [amount, transaction_date]

business_rules:
  - flag_high_value

select_final:
  - [transaction_id, id]
  - [transaction_date, date, [cast:date]]
  - [amount, amount_eur, [cast:double, round:2]]
  - [is_high_value, is_high_value]
```

```python
from skifer import SkiferEngine, RuleRegistry, load_schema
from pyspark.sql import functions as F

engine = SkiferEngine()

@RuleRegistry.register_rule()
def flag_high_value(df):
    return df.withColumn("is_high_value", F.when(F.col("amount") >= 1000, 1).otherwise(0))

# Load schema from file — {{ catalog }} is replaced with engine's active catalog
schema = load_schema("schemas/gold/fact_transactions.yaml", params=engine.default_params)

# Preview before running (no Spark execution)
engine.describe_schema(schema)

# Run and write to Delta
engine.run_process_to_table(schema_dict=schema, target_layer="gold", target_table_name="fact_transactions")
```

### Option B — Inline YAML string

For quick iterations or notebook-local schemas.

```python
from skifer import SkiferEngine, parse_schema

engine = SkiferEngine()

schema = parse_schema("""
tables:
  - name: "{{ catalog }}.silver.transactions"
    alias: tx
    filter:
      - "region:equals:EMEA"
select_final:
  - [transaction_id, id]
  - [amount, amount_eur, [cast:double]]
""", params=engine.default_params)

engine.run_process_to_table(schema_dict=schema, target_layer="gold", target_table_name="fact_transactions")
```

### Execution patterns

| Method | Use case |
|---|---|
| `run_process_to_table(schema, layer, table)` | Process a schema and write to a single Delta table |
| `run_process_and_split(schema, split_values, layer, base_name, col)` | Split result into one table per value (e.g. one table per region) |
| `run_union_sources_to_table(schema, partitions, src_layer, tgt_layer, table, bases, alias, dedup_after_union=True)` | Union partitioned source tables, process, write |
| `optimize_table(layer, table, zorder_cols)` | Run Delta OPTIMIZE with optional ZORDER BY |
| `describe_schema(schema)` | Dry-run summary: sources, joins, columns — no Spark execution |

---

## Semantic Layer

The Semantic Layer lets you auto-generate structured YAML models from your Gold tables using an LLM, then query them in natural language.

### Step 1 — Build a semantic model from a Gold table

`SemanticBuilder` inspects the table schema, optionally reads a Jupyter notebook and a business glossary, calls the LLM, validates the output (up to 3 attempts), and registers the model in `semantic_catalog.yaml`.

```python
from skifer import SkiferEngine
from skifer.semantic.builder import SemanticBuilder
from skifer.semantic.llm_provider import get_llm_provider

engine = SkiferEngine()
builder = SemanticBuilder(
    llm_provider=get_llm_provider(),   # auto-detects from env vars
    output_dir="semantic_models",
)

builder.build(
    model_name="kpi_orders",
    split_value="erp",
    table="gold.fact_orders",
    layer="gold",
    source_notebook="notebooks/fact_orders.ipynb",  # optional — adds business context
    glossary_path="glossaries/orders.json",          # optional — injects domain terms
    description="Order KPIs from ERP (SAP source)",
    tags=["orders", "revenue", "erp"],
)
# → writes semantic_models/kpi_orders.erp.yaml
# → updates semantic_catalog.yaml
```

The generated YAML describes dimensions (with SQL expressions + types) and metrics (with SQL + aggregation type). It is fully human-readable and editable after generation.

### Step 2 — Load the Semantic Engine

```python
from skifer.semantic.semantic import SemanticEngine

sem = SemanticEngine(engine, models_dir="semantic_models")
# → loads semantic_catalog.yaml only at startup (lightweight)

# Browse available models
sem.list_models()
sem.list_models(tags=["revenue"], summary=True)

# Inspect a specific model
sem.get_model_summary("kpi_orders.erp")
```

### Step 3 — Query in natural language with GenBIAgent

```python
from skifer.agentic.agent import GenBIAgent

agent = GenBIAgent(semantic_engine=sem, llm_provider=get_llm_provider())

# Ask a business question
response = agent.ask("What is the total revenue by region for last quarter?")

if response.success:
    response.result.data.show()       # PySpark DataFrame
elif response.needs_clarification:
    print(response.clarification_message)

# Export the session as PDF
agent.history.to_pdf("session_export.pdf")
```

### LLM provider auto-detection

`get_llm_provider()` selects the provider based on environment variables:

| Variable | Provider |
|---|---|
| `ANTHROPIC_API_KEY` | Claude (Anthropic) |
| `OPENAI_API_KEY` | GPT (OpenAI) |
| `GOOGLE_API_KEY` | Gemini (Google) |
| `LLM_PROVIDER=anthropic\|openai\|google` | Force a specific provider |

---

## External Declarative Sources

Read external files (CSV, Parquet, JSON, Avro, ORC, Delta, Text) from any blob storage or local filesystem directly from your YAML schema — no Python required. All standard pipeline features (filter, quality checks, dev_limit, joins) apply identically to external sources.

```yaml
tables:
  - name: raw_orders
    alias: orders
    source:
      type: csv
      path: "abfss://container@account.dfs.core.windows.net/bronze/orders/*.csv"
      options:
        header: "true"
        inferSchema: "true"
    filter:
      - "status:is_not_null"
    quality_checks:
      drop_nulls_in: [order_id, amount]
    dev_limit: 5000

  - name: "{{ catalog }}.silver.products"   # regular Delta table — mix freely
    alias: products
```

Supported `type` values: `csv`, `parquet`, `json`, `avro`, `orc`, `delta`, `text`.  
Path supports `{{ param }}` injection — use `load_schema(..., params={"base_path": "..."})` to parameterize.

> **Note:** External sources are a Spark/Databricks feature.

---

## Adding Business Rules

Rules are decoupled from execution notebooks. Define them in a centralized `rules.py` and import it before running the engine.

```python
from pyspark.sql import functions as F
from skifer import RuleRegistry

@RuleRegistry.register_rule()
def enrich_transaction_data(df):
    return (
        df
        .withColumn(
            "is_high_value",
            F.when(F.col("amount") >= 1000, 1).otherwise(0)
        )
        .withColumn(
            "clean_status",
            F.when(F.lower(F.col("status")).isin(["completed", "done"]), "Paid")
             .otherwise("Pending")
        )
        .fillna({"amount": 0.0})
    )
```

---

## YAML Schema Reference

### Filter operators

All operators use full English names. SQL abbreviations (`eq`, `gte`, etc.) are accepted as aliases.

| Operator | Example | Notes |
|---|---|---|
| `equals` | `"status:equals:ACTIVE"` | alias: `eq` |
| `not_equals` | `"status:not_equals:CANCELLED"` | alias: `ne` |
| `greater_than` | `"amount:greater_than:100"` | alias: `gt` |
| `less_than` | `"age:less_than:18"` | alias: `lt` |
| `greater_than_equal` | `"score:greater_than_equal:90"` | alias: `gte` |
| `less_than_equal` | `"qty:less_than_equal:5"` | alias: `lte` |
| `in` | `"status:in:ACTIVE,PENDING"` | comma or `;` separated |
| `not_in` | `"region:not_in:FR,DE"` | |
| `contains` | `"label:contains:promo"` | |
| `not_contains` | `"label:not_contains:test"` | |
| `starts_with` | `"ref:starts_with:ORD"` | |
| `ends_with` | `"email:ends_with:@corp.com"` | |
| `is_null` | `"discount:is_null"` | no value |
| `is_not_null` | `"customer_id:is_not_null"` | no value |
| `like` | `"name:like:J%"` | SQL LIKE pattern |
| `not_like` | `"name:not_like:test%"` | |
| `sql` | `"sql:amount > threshold"` | raw SQL escape hatch |

For values containing commas, use the dict form: `{column: city, operator: in, value: ["New York, NY", "Paris"]}`.

### `select_final` operations

Operations are applied left-to-right on the source column.

| Operation | Example | Result |
|---|---|---|
| `cast:type` | `cast:date`, `cast:double` | Type casting |
| `upper` / `lower` | `upper` | String case |
| `trim` | `trim` | Strip whitespace |
| `round:N` | `round:2` | Round to N decimals |
| `abs` | `abs` | Absolute value |
| `length` | `length` | String length |
| `to_date:fmt` | `to_date:yyyy-MM-dd` | Parse string to date |
| `nvl:val` | `nvl:0` | Replace null with value |
| `coalesce:val` | `coalesce:0` | Same as `nvl:` |
| `lit:val` | `lit:ERP` | Constant value |
| `expr:sql` | `expr:year(order_date)` | Arbitrary SQL expression |
| `split:sep,idx` | `split:-,1` | Split string, take index |
| `substring:start,len` | `substring:1,4` | Substring |
| `when:op:val` | `when:equals:DONE` | Condition (use with `then:` / `else:`) |

**Shorthand for constant columns:**
```yaml
select_final:
  - [literal:ERP, source_system]          # adds source_system = 'ERP'
  - [literal:0.0, discount, [cast:double]]
```

**OR filter groups:**
```yaml
filter_groups:
  - ["region:equals:EMEA", "status:is_not_null"]   # EMEA AND not null
  - ["region:equals:APAC"]                          # OR APAC
```

**Keep all columns + add computed ones:**
```yaml
keep_all_columns: true
add_columns:
  - [amount, amount_rounded, [round:2]]
  - [literal:ERP, source_system]
```

**Quality checks:**
```yaml
quality_checks:
  drop_nulls_in: [customer_id, order_date]
  drop_duplicates_on: [order_id, sku_id]
```

**Compact join syntax:**
```yaml
join:
  - table_from: [orders, customer_id]
    table_to: [customers, id]
    type: left
```

**Parameter injection:**
```yaml
tables:
  - name: "{{ catalog }}.silver.orders"
    filter:
      - "region:equals:{{ region }}"
```
```python
schema = load_schema("schemas/fact_orders.yaml", params={**engine.default_params, "region": "FR"})
```

**Dev sampling (ignored in job/prod):**
```yaml
dev_limit: 10000    # schema-level
tables:
  - name: silver.orders
    dev_limit: 5000  # table-level override
```

**Streaming tables (native Structured Streaming, no DLT — Plan 27):**
```yaml
materialization:
  type: streaming_table       # incremental writes with a checkpoint
  write_mode: upsert          # append (default) | upsert (CDC type 1 — MERGE on keys)
  keys: [order_id]            # required iff upsert
tables:
  - name: bronze.raw_orders
    alias: ord
    streaming: true           # read via readStream — exactly one per schema
  - name: silver.dim_customer
    alias: cust               # batch dimension → static side of the join
```
Run 1 backfills, run 2 only processes new rows. Works locally and on Databricks
jobs with the same YAML. Full refresh: `engine.full_refresh(layer, table)`
(atomic checkpoint purge + table drop). See `docs/core.md` for the guardrails
(join topology, incompatible features, checkpoint resolution).

**Declarative aggregations (Plan 28):**
```yaml
aggregate:
  group_by: [country, order_month]
  measures:
    - [amount, total_amount, sum]        # [source, target, func]
    - [order_id, nb_orders, count_distinct]
  having:
    - "total_amount:greater_than:1000"   # filter grammar, on measure aliases
```
No Python rule needed for an ordinary GROUP BY. Works in any batch pipeline.

**Semantic projection seed (Plan 29):** a pipeline may declare its data product,
its intended output contract and a semantic-model hint while its physical model
is still being built:
```yaml
data_product:
  id: sales.orders
  version: 1.0.0
  owner: analytics@company.example
  description: Orders ready for analysis
contract:
  grain: [order_id]
  output:
    order_id: {logical_type: identifier, required: true, unique: true}
    order_date: {logical_type: date}
    net_revenue: {logical_type: currency, classification: internal}
semantic:
  model_key: orders
  entity: order
  default_time_dimension: order_date
  dimensions: [order_id, order_date]
```
All references in `grain`, `dimensions` and `default_time_dimension` must name a
declared final output. These blocks are optional and do not execute a semantic
model or make an LLM call.

**Materialized views (Unity Catalog, Plan 28):**
```yaml
materialization:
  type: materialized_view     # compiled to SQL — Databricks manages the refresh
  schedule: "EVERY 6 HOURS"   # optional
  cluster_by: [country]       # optional (exclusive with partition_by)
tables:
  - name: "{{ catalog }}.silver.orders"
    alias: ord
    filter: ["status:equals:DONE"]
aggregate:
  group_by: [country]
  measures: [[amount, total_amount, sum]]
```
The whole DataFrame pipeline is short-circuited: the YAML is compiled to SQL and
issued as `CREATE [OR REPLACE] MATERIALIZED VIEW` on a Pro/Serverless SQL
warehouse (`params.sql_warehouse_id`, needs `pip install
'skifer[databricks]'` outside Databricks). A definition hash stored in
`TBLPROPERTIES` means an unchanged YAML only triggers a `REFRESH`, while a
changed one is replaced — never left stale. Locally the compiled SELECT is
materialized as a Delta table. Python `business_rules` are not compilable to SQL
and are refused at load: materialize them upstream in silver, then aggregate.

---

## Smart Sandbox

In interactive (non-job, non-prod) mode, source tables are transparently resolved to your personal sandbox schema (`schema_XXXX` where `XXXX` is derived from your username).

| Situation | Behavior |
|---|---|
| Table exists in `silver_XXXX` | Loaded directly — transparent |
| Table missing in `silver_XXXX` | Logged warning + shallow clone from `silver` + load |
| Schema `silver_XXXX` doesn't exist | Schema created + table cloned + load |
| Table missing in main schema | `ValueError` raised |

Configure behavior in `config.yaml`:
```yaml
sandbox:
  missing_table: copy   # copy (default) | error
```

> **Note:** Add `.skifer_user` to your `.gitignore`. This file is auto-generated to cache your sandbox suffix.

---

## Developer Tools

```python
# Force a specific environment (bypass auto-detection)
engine = SkiferEngine(force_env="LOCAL")

# Preview a schema without executing Spark
engine.describe_schema(schema)

# List registered rules and loaders
RuleRegistry.list_rules()
RuleRegistry.list_loaders()
```

---

## Governed Capabilities (write-back)

A **capability** is the only way an agent can act on a system outside the lakehouse.
Every step is declared, deterministic and refusable:

```yaml
capabilities:
  - id: support.create_ticket
    version: 1.0.0
    owner: support-platform
    description: Create a support ticket after deterministic duplicate checks
    mode: write
    executor: support_ticket_v1        # a registered name, never an import path
    acting_as: delegated_user
    required_scopes: [tickets:create]
    input_schema:                      # closed: additionalProperties false, bounded sizes
      type: object
      additionalProperties: false
      required: [request_id, category]
      properties:
        request_id: {type: string, maxLength: 128}
        category:   {type: string, maxLength: 32, enum: [payment, network, access]}
    preconditions:
      - rule: no_open_duplicate        # a registered rule name, never code
    reversibility: compensatable
    compensation: support.close_ticket
    approval: supervised
    idempotency_key: request_id        # must be a required input property
    provenance:
      policy_uri: policies/support-ticket-v3.md
      policy_hash: sha256:<64 hex>
```

```yaml
environments:
  prod:
    capability_autonomy: supervised    # shadow (default) | supervised
```

The LLM chooses a capability and fills the schema. It never decides the policy, the
preconditions, the approval, the credentials or the idempotency. `UNKNOWN` never
becomes `ALLOW`, an approval is void the moment one argument character changes, a
credential is issued just in time by an injected provider and never stored, and a
duplicate request replays the recorded outcome instead of acting twice.

Nothing in this layer can roll back or delete. Compensation, when declared, is a new
audited action with its own identity.

See [Agentic Layer](docs/agentic.md) for the full contract.

---

## Supervised Adaptive Gold

Usage of the semantic layer is recorded as privacy-safe events (logical IDs and
value-free filter shapes — never a question, a SQL string or a filter value),
aggregated into strictly partitioned patterns, and turned into **proposals** by a
static registry of versioned rules. Each proposal cites its evidence, its rule
version and its thresholds, and is written — validated — under
`.skifer_proposals/`. The engine never edits `schemas/`, never runs a
pipeline, never creates a materialized view and never runs Git.

```bash
skifer adaptive list
skifer adaptive show PROPOSAL
skifer adaptive diff PROPOSAL
skifer adaptive accept PROPOSAL --output schemas/gold/orders_summary.yaml
skifer adaptive reject PROPOSAL --reason "Not enough benefit"
skifer adaptive evaluate PROPOSAL --store .skifer_adaptive.db
```

`accept` refuses to overwrite an existing file and refuses a proposal whose source
definitions have drifted. `evaluate` compares usage before and after delivery and
returns `improved`, `regressed` or `inconclusive` — and reports `inconclusive`
rather than attributing causality to too little data. **A regression produces a
human review recommendation and nothing else**: there is no rollback and no drop
in this layer.

See [Agentic Layer](docs/agentic.md) for the full contract, including exit codes.

---

## Configuration Reference (`config.yaml`)

```yaml
default_env: LOCAL                      # Fallback if no Databricks catalog is reachable
priority_check: [DEV, QA, PROD, LOCAL]  # Detection order

environments:
  LOCAL:
    catalog: null                       # null = local mode (no Unity Catalog)
    is_production: false

  DEV:
    catalog: "my_dev_catalog"
    is_production: false

  QA:
    catalog: "my_qa_catalog"
    is_production: false

  PROD:
    catalog: "my_prod_catalog"
    is_production: true

sandbox:
  missing_table: copy                   # copy (default) | error
```

The engine also reads the `semantic_views_schema` key (optional) to resolve the target schema for semantic views.
