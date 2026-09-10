# Core Engine

The Core Engine is the foundation of Skifer.
It replaces imperative data pipeline notebooks with a declarative dictionary schema, keeping all business logic in registered, testable Python functions.

**Spark-only** (Plan 26): the runtime target is Databricks Lakehouse in production and local PySpark/Delta for development. `SparkBackend` (`core/spark_backend.py`) is the single execution backend; tests inject duck-typed doubles via `engine.backend`.

---

## Architecture

```
schema dict  +  @RuleRegistry.register_rule
       │
       ▼
SkiferEngine.run_process_to_table()
       │
       ├── process_schema()          ← join + transform + select
       │       ├── load tables (via backend)
       │       ├── apply joins (via backend)
       │       ├── apply operations (via backend)
       │       ├── apply business_rules (registered functions)
       │       └── apply select_final (via backend)
       │
       └── SparkBackend.write_table()  ← Databricks | Local PySpark
```

### Spark Runtime Boundary

The engine delegates data operations to `SparkBackend` — the only runtime backend. In tests, a duck-typed double can be injected via `engine.backend`.

| Backend | Implementation | Use case |
|---|---|---|
| **SparkBackend** | PySpark + Delta Lake | Databricks, local development |

---

## SkiferEngine

```python
from skifer import SkiferEngine

engine = SkiferEngine()
```

The engine auto-detects:

- **Spark runtime** — active Databricks notebook, Databricks Connect v2, or local PySpark
- **Spark session** — active Databricks notebook > Databricks Connect v2 > local PySpark
- **Config file** — walks up the directory tree looking for `config.yaml`
- **Environment** — tests catalog access at startup, selects first reachable env (PROD → QA → DEV)
- **User identity** — enables sandbox mode (per-user schema suffix) in non-production

### Constructor

```python
SkiferEngine(spark=None, config_path=None, force_env=None, monitor=None)
```

| Parameter | Description |
|---|---|
| `spark` | Existing `SparkSession` (optional — auto-created if absent). |
| `config_path` | Path to `config.yaml` (optional — auto-discovered) |
| `force_env` | Force a specific environment (e.g. `"LOCAL"`), bypass auto-detection. Raises `ValueError` if env not in config. |
| `monitor` | Optional `DataMonitor` — quality checks run after each `run_process_to_table()`. |

### New factory method: `get_agent()`

Simplified access to the Agentic layer (SemanticEngine + GenBIAgent in one call):

```python
agent = engine.get_agent(
    llm_provider=None,        # auto-detect from env if None
    models_dir="semantic_models",
    history=True,
    session_title="My Session"
)

resp = agent.ask("What is the Q4 revenue?")
```

---

## Core methods

### `run_process_to_table`

The primary entry point. Executes the pipeline and writes the result to a Delta table.

```python
engine.run_process_to_table(
    schema_dict=schema,
    target_layer="gold",
    target_table_name="fact_orders",
)
```

| Parameter | Type | Description |
|---|---|---|
| `schema_dict` | `dict` | Declarative pipeline schema |
| `target_layer` | `str` | Target layer (`gold`, `silver`, etc.) |
| `target_table_name` | `str` | Target table name (without schema prefix) |
| `dataframes_in` | `dict` | Optional pre-loaded DataFrames keyed by alias |

When the schema declares `materialization: streaming_table`, the same call runs
the pipeline as Structured Streaming: the checkpoint is resolved before any
read, the write goes through `writeStream` (append or upsert) and the call
returns after the backfill with the default `available_now` trigger
(`interval:` triggers block — permanent streaming job). See
[Streaming tables](#streaming-tables-streaming-materialization-plan-27).

When it declares `materialization: materialized_view`, the DataFrame pipeline is
short-circuited entirely — no source is read. The schema is compiled to SQL and
the `CREATE [OR REPLACE] MATERIALIZED VIEW` statement is executed on a SQL
warehouse. See
[Materialized views](#materialized-views-materialization-materialized_view-plan-28).

### `process_schema`

Executes the pipeline and returns the resulting DataFrame **without writing**.

```python
df = engine.process_schema(schema_dict, dataframes_in=None)
```

With streaming sources the returned DataFrame is a **non-started** streaming
DataFrame (`df.isStreaming == True`) — a direct caller owns its own
`writeStream`; only the `run_*` patterns write.

### `run_process_and_split`

Executes the pipeline and writes one Delta table per distinct value of a split column.

```python
engine.run_process_and_split(
    schema_dict=schema,
    target_layer="gold",
    target_table_name="fact_orders",
    split_column="channel",
)
```

---

## Schema dictionary reference

The schema dict drives the entire pipeline. All keys are optional except `tables`.

```python
schema = {
    # ── Sources ───────────────────────────────────────────────────────────
    "tables": [
        # Minimal: load all columns from Delta table
        {"name": "bronze.raw_orders", "alias": "orders"},

        # External file source (CSV, Parquet, JSON, Avro, ORC, Delta, Text)
        {
            "name": "raw_orders_ext",
            "alias": "orders",
            "source": {
                "type": "csv",
                "path": "abfss://container@account.dfs.core.windows.net/bronze/orders/*.csv",
                "options": {"header": "true", "inferSchema": "true"},
            },
        },

        # With column selection + rename
        {
            "name": "bronze.raw_products",
            "alias": "products",
            "fields": [
                ["product_id", "product_id"],
                ["category",   "category"],
                ["brand",      "brand"],
            ],
        },

        # With pre-filter
        {
            "name": "bronze.raw_customers",
            "alias": "customers",
            "filter": [{"column": "active", "operator": "equals", "value": "true"}],
        },
    ],

    # ── Joins ─────────────────────────────────────────────────────────────
    "join": [
        {
            "table_from": "orders",
            "table_to":   "products",
            "on_from":    "product_id",
            "on_to":      "product_id",
            "type":       "left",          # inner | left | right | full | cross | left_anti | left_semi
        },
    ],

    # ── Column operations ─────────────────────────────────────────────────
    "operations": [
        {
            "column": "amount_ttc",
            "alias":  "revenue_eur",
            "ops":    ["cast:double"],
        },
        {
            "column": "order_date",
            "alias":  "order_month",
            "ops":    ["cast:date"],
        },
    ],

    # ── Business rules ────────────────────────────────────────────────────
    "business_rules": ["flag_vip_orders", "add_month"],

    # ── Post-join filter ──────────────────────────────────────────────────
    "filter": [
        {"column": "status", "operator": "equals", "value": "completed"}
    ],

    # ── Final column selection ────────────────────────────────────────────
    "select_final": [
        ["order_id",   "order_id"],
        ["order_date", "order_date"],
        ["order_month","order_month"],
        ["category",   "category"],
        ["amount_ttc", "chiffre_affaires"],
        ["is_vip",     "is_vip"],
    ],
}
```

The runnable walkthrough in `examples/07_sources_and_shaping/` reads checked-in
JSON, applies an OR-of-ANDs filter, joins labels supplied by a registered loader,
limits the development input, and prints the aggregate built from an added month
column; it also prints the load-time refusal for a misspelled filter operator.

For the complete rule-to-result path, `examples/05_rules_join_aggregate/` joins
two CSV sources, runs the registered `classify_order` Python rule, and prints both
the classified rows and the grouped totals left by `having total_amount > 500`.

---

## Nested sub-transformations (`partials`)

A schema can declare **nested YAML sub-transformations** via a top-level
`partials:` block. Each partial is executed as a full pipeline and its output is
exposed under an `alias`, usable in `join` and `business_rules` exactly like a
table. This removes the need to materialize intermediate tables just because a
join depends on a column produced by a business rule.

```yaml
# schemas/silver/customer_orders.yaml
partials:
  - alias: dly
    path: _partials/prepared_orders.yaml   # relative to THIS YAML's directory
  - alias: qtr
    path: _partials/qtr_calendar.yaml

tables:
  - name: "{{ catalog }}.silver.date_dim"
    alias: cal

join:
  - table_from: [dly, work_day]     # work_day is produced by a rule INSIDE the dly partial
    table_to: [cal, cal_date]
    type: left
  - table_from: [dly, fiscal_qtr]
    table_to: [qtr, qtr_key]
    type: left

business_rules:
  - enrich_with_date_dim
```

**Semantics**

- Child schemas run through the normal path (sources → joins → rules → select)
  and **never write a final table** — only the parent's `run_process_to_table`
  writes.
- Relative `path` resolves from the **parent YAML directory**. `parse_schema`
  (string form) only accepts absolute partial paths.
- Child schemas **inherit the parent's params** (`{{ key }}` injection).
- Partial `alias` must be **unique** across `partials` and `tables`.
- **Cyclic** references (direct or indirect) fail fast at load time.

**Materialization — `intermediate_mode` (run-global debug knob)**

Materialization is *not* declared in the YAML; it is a run parameter so the prod
schema stays pure. Default is `inline`.

| Mode | Behavior | Use case |
|---|---|---|
| `inline` (default) | Child output kept as an in-memory DataFrame. No storage. | Production. |
| `temp_view` | Registers a session view named after the `alias` (sandbox-suffixed). | Interactive debugging. |
| `table` | Writes a physical table `catalog.skifer_partials_<suffix>.<alias>`. | Large-volume debug / restartability. |

```python
# Production — inline, zero intermediate tables
engine.run_from_yaml("schemas/silver/customer_orders.yaml",
                     target_layer="silver", target_table_name="fct_customer_orders")

# Debug — materialize each partial as a temp view
engine.run_from_yaml("schemas/silver/customer_orders.yaml",
                     target_layer="silver", target_table_name="fct_customer_orders",
                     params={**engine.default_params, "intermediate_mode": "temp_view"})
```

In every mode the parent keeps using the DataFrame directly; `temp_view` / `table`
only add an inspectable side artifact.

`examples/06_nested_partials/` runs the same parent pipeline in `inline` and
`temp_view` modes, prints whether the intermediate view exists, and shows the
load-time refusal when a partial alias collides with a table alias.

---

## Streaming tables (`streaming` / `materialization`) — Plan 27

A pipeline can run as **native Structured Streaming** — incremental reads with a
checkpoint, no DLT dependency, identical behavior locally and on Databricks jobs:

```yaml
materialization:
  type: streaming_table        # shorthand also works: materialization: streaming_table
  trigger: available_now       # default — incremental batch, returns after the backfill
                               # or "interval:30 seconds" — permanent query (blocks)
  checkpoint: auto             # default — see resolution below; or an explicit path
  write_mode: upsert           # append (default) | upsert (CDC type 1)
  keys: [event_id]             # required iff write_mode: upsert — MERGE key

tables:
  - name: "{{ catalog }}.bronze.raw_events"
    alias: ev
    streaming: true            # read via spark.readStream.table(...)
  - name: "{{ catalog }}.silver.dim_country"
    alias: dim                 # batch → static side of the join

join:
  - table_from: [ev, country_id]   # the stream MUST be the join base
    table_to: [dim, id]
    type: left                     # inner | left only with a streaming base
```

Run it exactly like a batch pipeline — run 1 backfills, run 2 only processes new
rows (checkpoint):

```python
engine.run_from_yaml("schemas/silver/events_clean.yaml",
                     target_layer="silver", target_table_name="events_clean")
```

**Write modes.** `append` writes new rows incrementally. `upsert` implements
**CDC type 1** via `foreachBatch`: each micro-batch is deduplicated on `keys`,
then `MERGE INTO` the target (matched → update, not matched → insert). The
target guarantees key uniqueness across batches — no streaming state, and
replayed micro-batches are idempotent. Prefer `upsert` whenever a business key
exists.

**Checkpoint resolution** (`checkpoint: auto`): locally,
`{warehouse}/_checkpoints/{schema}{sandbox_suffix}/{table}`; on Databricks, the
`checkpoint_base` environment param is required (`environments.<ENV>.params.checkpoint_base`)
— the run fails fast **before reading anything** if it is missing. Checkpoints
are sandbox-suffixed in interactive mode, so two developers never share offsets.
In job/prod mode there is no suffix: the production checkpoint is intentionally
shared across scheduled runs (that is the incrementality). Promoting a pipeline
to prod starts a fresh checkpoint → the first prod run is a full backfill.

**Full refresh.** Dropping the target without purging its checkpoint (or the
reverse) causes duplicates or an incomplete backfill. Use the atomic helper:

```python
engine.full_refresh("silver", "events_clean")   # purge checkpoint + drop table
```

**Load-time guardrails** — every streaming incompatibility fails fast with an
actionable message when the YAML is loaded: strict bijection
(`streaming: true` ⇔ `materialization: streaming_table`), exactly one streaming
table per schema, stream as join base with `inner`/`left` only, file sources
restricted to `delta`/`text`, and rejections for `dev_limit`,
`preprocess.qualify`, `drop_duplicates_on` (use `write_mode: upsert` instead),
loaders, JDBC sinks, `materialized_view` and streaming inside
`partials:`. `aggregation`-kind rules and the `aggregate:` block are rejected at
run start — aggregate in a downstream **batch** pipeline or
[materialized view](#materialized-views-materialization-materialized_view-plan-28)
reading the streaming table.

**Composing flows: sub-layers.** Chain streaming tables instead of nesting
transformations — each stage owns its table and its checkpoint, and a table
written by streaming is a first-class streaming source for the next stage
(this is NOT a stream-stream join):

```
bronze.raw_events ──stream──► silver_landing.events ──stream──► silver_enrich.events ──batch──► gold
```

The Gold aggregation stage stays batch (see guardrails above). Fan-out
(one pivot → N slices) follows the same idea: N downstream streaming pipelines,
each with a `filter:` and its own checkpoint.

`examples/09_streaming_table/` performs two finite `available_now` runs against
a local Delta source, printing checkpoint progress and the CDC type 1 update that
leaves three target rows from four source records; it also prints the refusal of
`dev_limit` on a streaming source.

---

## Declarative aggregations (`aggregate`) — Plan 28

A top-level `aggregate:` block expresses a GROUP BY without writing a Python
rule. It works in any batch pipeline — it is not tied to materialized views:

```yaml
add_columns:                                # applied BEFORE the aggregation,
  - [order_date, order_month, [to_date:yyyy-MM]]   # so it can produce a group key

aggregate:
  group_by: [country, order_month]
  measures:
    - [amount, total_amount, sum]           # compact: [source, target, func]
    - [order_id, nb_orders, count_distinct]
    - source: amount                        # or the mapping form
      target: avg_basket
      func: avg
  having:                                   # optional — filter grammar on measure aliases
    - "total_amount:greater_than:1000"
```

**Functions**: `sum`, `avg` (`mean`, `average`), `min`, `max`, `count`,
`count_distinct`, `sum_distinct`, `approx_count_distinct`, `stddev` (`std`),
`variance` (`var`), `first`, `last`. Only `count` accepts `source: "*"`.

`aggregate:` is mutually exclusive with `select_final` and `keep_all_columns`,
and is rejected on streaming pipelines (streaming aggregations need watermark
support). `having:` is validated at load against the group keys and the measure
aliases, so a typo fails before any Spark work.

---

## Semantic projection metadata (`data_product`, `contract`, `semantic`) — Plan 29

Pipeline YAML can describe a data product and its planned semantic surface before
the target table exists. The metadata is optional and remains separate from
Python business rules:

```yaml
data_product:
  id: sales.orders
  version: 1.0.0
  owner: analytics@company.example
  description: Curated orders for analytics
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

`id` and `model_key` accept letters, numbers, `.`, `_` and `-`; `version` must
be a semantic version. `contract.output` is a mapping keyed by final output
name. `contract.grain`, `semantic.dimensions`, and
`semantic.default_time_dimension` must reference declared outputs. No SQL is
accepted in `semantic`. This metadata is projected into a draft semantic model
without starting Spark or calling an LLM — see
[Semantic Layer → Projected semantic models](semantic.md#projected-semantic-models-plan-29).

The pure `OutputProjector` consumes the parsed schema and produces the planned
output fields plus a stable definition hash. It infers only deterministic cases
such as casts, literals, string operations, `length`, and counting aggregates.
Source-dependent types, raw `expr:` SQL and Python business rules remain
explicitly `unknown`; it never opens Spark merely to guess them.

When a batch schema declares a valid `data_product`/`contract.output`, `run_process_to_table`
uses certified publication if the engine was constructed with both `monitor=`
and `certification_store=`. The DataFrame is written to staging, validated
there, and only promoted to the target when no critical check blocks publication.
Schemas without `data_product` still use the direct legacy write path; certified
publication currently refuses streaming tables, JDBC sinks and materialized
views.

---

## Materialized views (`materialization: materialized_view`) — Plan 28

A materialized view is a Unity Catalog object **defined by a SQL query**, whose
incremental refresh Databricks manages for you. The engine compiles the YAML to
SQL and issues the DDL — no DataFrame is ever built:

```yaml
materialization:
  type: materialized_view      # shorthand also works: materialization: materialized_view
  schedule: "EVERY 6 HOURS"    # optional — or "CRON '0 0 6 * * ?' AT TIME ZONE 'Europe/Paris'"
  comment: "Revenue per country and month"   # optional
  cluster_by: [country]        # optional — mutually exclusive with partition_by
  refresh: auto                # auto (default) | never (leave it to the SCHEDULE)

tables:
  - name: "{{ catalog }}.silver.orders"
    alias: ord
    filter:
      - "status:equals:DONE"
  - name: "{{ catalog }}.silver.customers"
    alias: cust

join:
  - table_from: [ord, customer_id]
    table_to: [cust, id]
    type: left

aggregate:
  group_by: [country]
  measures:
    - [amount, total_amount, sum]
```

Run it exactly like any other pipeline:

```python
engine.run_from_yaml("schemas/gold/fact_orders.yaml", target_layer="gold")
```

**Where the DDL runs.** `CREATE MATERIALIZED VIEW` is refused by all-purpose
clusters and by Databricks Connect, so the statement never goes through
`spark.sql`: it is executed on a **Pro or Serverless SQL warehouse** through the
Statement Execution API. Declare the warehouse once per environment:

```yaml
environments:
  PROD:
    catalog: prod_catalog
    is_production: true
    params:
      sql_warehouse_id: 1234abcd5678efgh
```

| Situation | Behavior |
|---|---|
| `sql_warehouse_id` configured | DDL executed on the warehouse, progress polled until the statement reaches a terminal state |
| Missing, interactive mode | The DDL is written to `generated_sql/<schema>_<table>.sql` (override with the `sql_output_dir` param) + a **`MATERIALIZED VIEW NOT CREATED`** warning naming the config key. The monitor is skipped — there is nothing to check yet |
| Missing, job or production mode | `ValueError` **before any compilation** — a scheduled run that silently creates nothing is worse than one that fails |
| Local mode (`catalog: null`) | Delta OSS has no materialized views: the compiled SELECT is executed and persisted as a regular Delta table. This also proves at every run that the generated SQL is valid |

**Definition drift.** A SHA-256 of the compiled SELECT *and* of the
definition-bearing options (`schedule`, `comment`, `cluster_by`, `partition_by`)
is stored in `TBLPROPERTIES ('skifer.definition_hash')`:

| State | Action |
|---|---|
| View absent | `CREATE MATERIALIZED VIEW` |
| Hash identical | `REFRESH MATERIALIZED VIEW` (skipped entirely under `refresh: never`) |
| Hash different or unreadable | `CREATE OR REPLACE MATERIALIZED VIEW` + a log line showing the change |

So editing the YAML can never leave a stale view running the old definition —
and an unchanged YAML never pays for a full rebuild.

```python
engine.full_refresh("gold", "fact_orders", materialization="materialized_view")  # DROP, next run recreates
```

**Load-time guardrails.** Everything a persisted SQL definition cannot express
fails fast, with every blocker reported in a single error: Python
`business_rules` (materialize them upstream in a silver table, then aggregate
here), `partials:`, file sources and loaders (ingest to a catalog table first),
`dev_limit` (a frozen `LIMIT` in a persisted definition silently truncates),
JDBC sinks, `streaming: true`, `drop_duplicates_on` and `preprocess.qualify`,
and `keep_all_columns` combined with a join or several tables (duplicate column
names are illegal in a view). `run_process_and_split` and
`run_union_sources_to_table` refuse materialized views — declare one view per
slice, or list the sources explicitly.

`examples/08_materialized_view/` compiles a grouped view without starting Spark
and prints its `SELECT`, deterministic definition hashes, and complete `CREATE
MATERIALIZED VIEW` DDL; it also shows why a Python business rule is refused in a
persisted SQL definition.

> The `databricks-sdk` package is needed to reach a SQL warehouse from outside
> Databricks: `pip install 'skifer[databricks]'` (already present on a
> Databricks runtime).

---

## Filter operators

Filters are supported in `tables[].filter` and at the top-level `filter` key.

All operators support both **canonical English names** (recommended) and **SQL abbreviations** (backward compatible).

| Canonical | Abbrev | SQL equivalent | Example value |
|---|---|---|---|
| `equals` | `eq` | `=` | `"completed"` |
| `not_equals` | `ne` | `!=` | `"cancelled"` |
| `greater_than` | `gt` | `>` | `"100"` |
| `less_than` | `lt` | `<` | `"100"` |
| `greater_than_equal` | `gte` | `>=` | `"2024-01-01"` |
| `less_than_equal` | `lte` | `<=` | `"2024-12-31"` |
| `in` | — | `IN (...)` | `"EMEA;APAC"` (semicolon-separated) |
| `not_in` | — | `NOT IN (...)` | `"cancelled;returned"` |
| `contains` | — | `LIKE %val%` | `"order"` |
| `not_contains` | — | `NOT LIKE %val%` | `"test"` |
| `starts_with` | — | `LIKE val%` | `"ORD"` |
| `ends_with` | — | `LIKE %val` | `"2024"` |
| `is_null` | — | `IS NULL` | — |
| `is_not_null` | — | `IS NOT NULL` | — |
| `like` | — | `LIKE (case-sensitive)` | `"ORD%"` |
| `not_like` | — | `NOT LIKE` | `"%test%"` |
| `sql` | — | raw expression | `"year(order_date) = 2024"` |

---

## Column operations

Operations are applied sequentially to a column before the final select.

| Operation | Effect | Example |
|---|---|---|
| `cast:type` | Cast to PySpark type | `cast:double`, `cast:date` |
| `upper` | Uppercase string | `upper` |
| `lower` | Lowercase string | `lower` |
| `trim` | Strip whitespace | `trim` |
| `round:N` | Round to N decimals | `round:2` |
| `abs` | Absolute value | `abs` |
| `length` | String length | `length` |
| `to_date:fmt` | Parse string to date | `to_date:yyyy-MM-dd` |
| `lit:val` | Replace with literal | `lit:0` |
| `col:name` | Replace with another column | `col:amount_ttc` |
| `expr:sql` | Arbitrary SQL expression | `expr:year(order_date)` |
| `coalesce:val` | Fill nulls | `coalesce:0` |
| `nvl:val` | Alias for coalesce | `nvl:0` |
| `split:sep,idx` | Split string, take index | `split:-,0` |
| `substring:start,len` | Substring | `substring:1,4` |
| `when:op:val` | CASE WHEN condition | Part of chained `when/then/else` |
| `then:val` | Then branch value | Part of chained `when/then/else` |
| `else:val` | Else fallback | Part of chained `when/then/else` |

---

## RuleRegistry

Business rules are registered globally and referenced by name in `schema["business_rules"]`.

```python
from skifer import RuleRegistry
from pyspark.sql import functions as F

@RuleRegistry.register_rule(name="flag_vip_orders")
def flag_vip_orders(df):
    """Tags orders above 1000€ as VIP."""
    return df.withColumn("is_vip", F.col("amount_ttc") > 1000)

@RuleRegistry.register_rule(name="add_month")
def add_month(df):
    """Extracts the order month from order_date."""
    return df.withColumn("order_month", F.date_format("order_date", "yyyy-MM"))
```

Rules are plain PySpark functions — they can be unit-tested independently of the engine.

`examples/05_rules_join_aggregate/` is the runnable end-to-end example: its YAML
names `classify_order`, its Python registers that projection rule, and the script
prints the joined rows with `priority`/`standard` labels before printing the
declarative aggregate and its `having` result.

```python
# List registered rules
RuleRegistry.list_rules()   # ["flag_vip_orders", "add_month"]

# Retrieve and call directly
fn = RuleRegistry.get_rule("flag_vip_orders")
df_out = fn(df_in)
```

---

## Configuration file

Skifer discovers `config.yaml` by walking up from the current working directory.

```yaml
default_env: DEV

priority_check:
  - PROD
  - QA
  - DEV

environments:
  DEV:
    catalog: my_catalog_dev
    db: my_catalog_dev
    is_production: false

  QA:
    catalog: my_catalog_qa
    db: my_catalog_qa
    is_production: false

  PROD:
    catalog: my_catalog
    db: my_catalog
    is_production: true
    semantic_views_schema: semantic_views
```

The engine tests catalog access at startup and selects the first accessible environment from `priority_check`.

### Sandbox mode

In non-production environments, each user gets an isolated schema suffix:

```
user: john.doe@company.com  →  schema suffix: _johndoe
writes to: silver_johndoe.fact_orders (instead of silver.fact_orders)
```

This prevents concurrent notebook runs from overwriting each other.

---

## Local development (no cluster needed)

Skifer works on your local machine without a Databricks cluster:

```bash
# 1. Install with local Spark backend
pip install "skifer[spark]"

# 2. Set local mode in config.yaml
cat > config.yaml << EOF
default_env: LOCAL

environments:
  LOCAL:
    catalog: null          # null = local mode, no Unity Catalog
    is_production: false
EOF

# 3. Run your pipeline locally
python my_pipeline.py
```

When `catalog: null`, the engine:
- Skips all catalog access checks
- Creates a local PySpark session with `local[*]`
- Stores data in a local Delta Lake (with Apache Derby metastore)
- Uses 2-part FQN (`schema.table`) instead of 3-part

No remote credentials needed — perfect for notebook development.
