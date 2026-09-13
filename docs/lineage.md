# Lineage & Data Dictionary

**Column-level lineage and data dictionary derived statically from YAML schemas — no Spark execution required.**

The lineage module answers the most frequent question in data engineering:  
*"Where does this field come from? What table? What transformation was applied?"*

Since Skifer declares everything in YAML, lineage is derivable at parse time — without running any pipeline.

---

## Quickstart

```python
from skifer import parse_schema, LineageTracker, DataDictionary, LineageRenderer

schema = parse_schema("""
tables:
  - name: bronze.raw_orders
    alias: ord
  - name: bronze.raw_customers
    alias: cust
join:
  - table_from: [ord, customer_id]
    table_to: [cust, id]
    type: left
select_final:
  - [amount, amount_eur, [cast:double, round:2]]
  - [channel, channel, [upper]]
""")

graph = LineageTracker.from_schema(schema, target_name="silver.fact_orders")

# Provenance — where does amount_eur come from?
for edge in graph.upstream("silver.fact_orders", "amount_eur"):
    print(f"← {edge.source_table}.{edge.source_column}  ops={edge.transformations}")

# Export Mermaid diagram
renderer = LineageRenderer()
print(renderer.to_mermaid(graph))
```

---

## Architecture

```
lineage/
  tracker.py     # LineageTracker — builds the graph from YAML schemas
  dictionary.py  # DataDictionary — per-column metadata and glossary enrichment
  renderer.py    # LineageRenderer — Mermaid, JSON, HTML export
  classification.py # ordered sensitivity propagation over column lineage
```

---

## LineageTracker

### `from_schema(schema_dict, target_name)`

Derives column-level lineage from a core pipeline YAML schema (output of `parse_schema`).

**Sources of lineage edges:**

| YAML section | Edge type | Description |
|---|---|---|
| `select_final` | `select` | Source column → target column with transformations |
| `add_columns` | `select` | Same as select_final |
| `join` | `join` | Join key relationships between tables |
| `business_rules` | `rule` | Detected via AST analysis of the registered rule function |

```python
graph = LineageTracker.from_schema(schema, target_name="silver.fact_orders")
print(f"{len(graph)} edges, tables: {graph.tables()}")
```

### `from_semantic_model(model_dict)`

Derives lineage from a semantic YAML model (dimensions and metrics SQL expressions).

```python
model = {
    "key": "kpi_orders",
    "table": "gold.fact_orders",
    "metrics": [{"name": "revenue", "sql": "SUM(amount_eur)", "type": "sum"}],
}
semantic_graph = LineageTracker.from_semantic_model(model)
```

### Merging graphs

`LineageGraph.merge()` combines two graphs into a single end-to-end DAG:

```python
core_graph = LineageTracker.from_schema(schema, target_name="silver.fact_orders")
core_graph.merge(semantic_graph)
# → full lineage from bronze.raw_orders to semantic KPIs
```

---

## LineageGraph — navigation

```python
# Provenance — "where does this column come from?"
upstream = graph.upstream("silver.fact_orders", "amount_eur")

# Impact analysis — "what breaks if I change this?"
downstream = graph.downstream("bronze.raw_orders", "amount")

# All tables referenced
tables = graph.tables()

# Export to dict (JSON-serialisable)
data = graph.to_dict()  # {"edges": [...], "tables": [...], "summary": {...}}
```

## Persisted metadata registry — Plan 31

`DatasetRecord` persists a pipeline's physical target, definition hash, product
and contract identity, owner, output columns, latest run ID, and serialized
lineage. Local projects use `SqliteMetadataStore`; Databricks integrations can
use `DeltaMetadataStore`. Re-indexing an unchanged definition is idempotent.

```bash
skifer index schemas/silver/fact_orders.yaml --db .skifer_metadata.db
skifer dictionary silver.fact_orders --format json
skifer lineage silver.fact_orders.amount_eur --direction up --format mermaid
skifer lineage silver.fact_orders --direction down --format json
```

`MetadataRegistryQuery` merges every stored graph, refuses a cycle, and computes
upstream and downstream closures across pipeline boundaries. Its impact report
contains impacted datasets and columns, traversed edges, and a `truncated` flag
when the depth limit is reached. Certified publication also updates the registry
with its run ID; this hook is non-blocking and cannot reverse a promotion.

### Classification propagation

The ordered levels are `public`, `internal`, `confidential`, `restricted`, and
`pii`. `resolve_field_classifications()` selects the strongest upstream level
for each target column. The default `warn` mode warns when a non-public inferred
level was not declared; `strict` raises. An explicitly lower declaration is
retained and logged rather than silently rewritten.

See [Services and local API](services_and_api.md#metadata-registry-and-cross-pipeline-lineage)
for the service boundary and HTTP access.

---

## LineageEdge

Each edge in the graph is a `LineageEdge` dataclass:

```python
@dataclass
class LineageEdge:
    source_table: str
    source_column: str
    target_table: str
    target_column: str
    transformations: list[str]   # e.g. ["cast:double", "round:2"]
    edge_type: str               # "select" | "join" | "rule" | "metric"
```

---

## LineageRenderer

```python
renderer = LineageRenderer()

# Mermaid — readable in GitHub, Notion, any Markdown tool
mermaid_str = renderer.to_mermaid(graph, direction="LR")

# JSON — consumable by a front-end or data catalog
data = renderer.to_json(graph)

# HTML — standalone page with embedded Mermaid.js
renderer_html = renderer.to_html(graph)
with open("lineage.html", "w") as f:
    f.write(renderer_html)
```

**Mermaid edge styles by type:**

| Type | Style | Meaning |
|---|---|---|
| `select` | `-->` | Projection / transformation |
| `join` | `-.->` | Join key |
| `rule` | `-->` | Business rule (label prefixed `rule:`) |
| `metric` | `==>` | Semantic dimension or metric |

---

## DataDictionary

Built from a `LineageGraph`, `DataDictionary` indexes every column with its source fields, transformations, and an optional business description.

```python
dd = DataDictionary(graph)

# Print full report
dd.print_report()

# Lookup a specific field
entry = dd.get("silver.fact_orders", "amount_eur")
print(entry.source_fields)     # ['bronze.raw_orders.amount']
print(entry.transformations)   # ['cast:double', 'round:2']
print(entry.description)       # '' (until enriched)

# List all fields for a table
fields = dd.list_fields("silver.fact_orders")
```

### Glossary enrichment

`enrich_from_glossary()` reads a glossary file (TXT, JSON, YAML, PDF, PPTX) and auto-assigns descriptions to matching columns via fuzzy matching (`difflib`, cutoff 0.8):

```python
dd.enrich_from_glossary("glossary.yaml")

entry = dd.get("silver.fact_orders", "amount_eur")
print(entry.description)  # "Order amount converted to euros"
```

---

## Integration with SkiferEngine

```python
engine = SkiferEngine()
schema = load_schema("schemas/silver/fact_orders.yaml", params=engine.default_params)

graph = engine.build_lineage(
    schema,
    semantic_models=[semantic_model],
    target_name="silver.fact_orders",
)
```

---

## Business rules — AST analysis

`business_rules` edges are derived via `RuleAnalyzer` — static AST inspection of the registered Python function. The analysis detects `withColumn` / `with_column` calls (outputs) and `F.col` / `col` / `df["col"]` references (inputs).

**Known limitation:** dynamic column names (e.g. `withColumn(variable, ...)`) are not detected. Only string literals in `withColumn` calls are tracked.

`examples/11_lineage_and_dictionary/` statically analyses the joined pipeline
from example 05 and prints source provenance, the `<rule>` origin and dictionary
entry for `order_class`, plus the important lower-bound case: an opaque rule
whose output name is built at runtime is not detected rather than guessed.

To declare inputs/outputs explicitly:

```python
@RuleRegistry.register_rule(name="flag_high_value", inputs=["amount"], outputs=["is_high_value"])
def flag_high_value(df):
    return df.withColumn("is_high_value", F.when(F.col("amount") >= 1000, 1).otherwise(0))
```

---

## Limitations

| Limitation | Details |
|---|---|
| Multi-table `select_final` | Source column attributed to the primary table (best-effort — join source cannot be determined statically) |
| Complex SQL in semantic metrics | Regex-based identifier extraction — SQL keywords filtered, but string literals may be captured |
| Dynamic rule column names | `withColumn(variable, ...)` not detected by AST analysis |
| Sampling | Only `DataDictionary.distinct_sample` requires a live backend; all other lineage is static |

---

## Runnable walkthrough

→ `examples/11_lineage_and_dictionary/` — column provenance, the `<rule>` origin, the Mermaid
rendering, a dictionary entry, and the boundary static analysis cannot cross. It is executed by the
test suite, so it says what the code does today.
