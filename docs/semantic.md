# Semantic Layer

The Semantic Layer sits between the Gold tables and the natural-language agent.
It defines **what your data means** — in business terms — independently of SQL.

```
Gold tables (Delta)
      │
      ▼
 YAML semantic models   ←  SemanticBuilder (LLM-generated)
      │
      ▼
 SemanticEngine         ←  catalog-first, lazy-loading
      │
      ▼
 SemanticQuery  →  QueryResolver  →  Spark DataFrame
```

---

## Concepts

| Concept | Description |
|---|---|
| **Semantic model** | A YAML file declaring metrics and dimensions for one table |
| **Dimension** | A grouping axis (country, channel, date…) — maps to a SQL expression |
| **Metric** | A KPI (revenue, orders…) — maps to an aggregation expression |
| **base_filter** | A WHERE clause applied to every query on this model |
| **Catalog** | `semantic_catalog.yaml` — lightweight index loaded once at startup |
| **model_key** | Unique identifier for a model (e.g. `"kpi_orders"`, `"kpi_orders_erp"`) |

---

## SemanticEngine

`SemanticEngine` loads the catalog once and lazy-loads full YAML models on demand.

```python
from skifer.semantic.semantic import SemanticEngine

semantic = SemanticEngine(engine, models_dir="semantic_models")
```

| Parameter | Description |
|---|---|
| `engine` | A `SkiferEngine` instance (used for Spark access) |
| `models_dir` | Directory containing `semantic_catalog.yaml` and model YAML files |

### Methods

#### `list_models()`

Returns a summary of all registered semantic models.

```python
semantic.list_models()
# [{'key': 'kpi_orders', 'description': '...', 'tags': [...], ...},
#  {'key': 'kpi_orders_erp', 'description': '...', 'tags': [...], ...}]
```

#### `get_model_summary(model_key)`

Returns a compact dict for a single model (dimensions + metrics names only).

```python
semantic.get_model_summary("kpi_orders")
```

#### `query(semantic_query)`

Executes a `SemanticQuery` on Spark/Databricks and returns a Spark DataFrame.

```python
from skifer.agentic.resolver import SemanticQuery

q = SemanticQuery(
    model_name="kpi_orders",              # model key (unique identifier)
    metrics=["chiffre_affaires", "nb_commandes"],
    group_by=["channel", "country"],
    filters=[{"column": "status", "operator": "equals", "value": "completed"}],
    date_from="2024-01-01",
    date_to="2024-12-31",
)
df = semantic.query(q)
df.show()
```

`examples/03_semantic_and_question/` runs an explicit semantic query over a
certified local table and prints revenue and order count by region together with
the SQL hash, datasets read, and frozen certification snapshot produced by that
same execution.

For cross-model planning, `examples/14_domain_graph/` resolves a declared
orders-to-customers join and a versioned fiscal period, prints the compiled SQL,
and then prints separate pre-SQL refusals for unsafe fanout, many-to-many metric
paths, and ambiguous period/date bounds.

#### `create_view(semantic_query)`

Executes the query and registers the result as a Spark temp view.

```python
q.mode = "query"
q.view_name = "my_view"
semantic.create_view(q)
```

#### `get_llm_context()`

Returns the compact catalog string sent to the LLM (~500 tokens).
Useful for debugging what the agent sees.

```python
print(semantic.get_llm_context())
```

#### `update_catalog` (class method)

Called automatically by `SemanticBuilder` after generating a new model.

```python
SemanticEngine.update_catalog(models_dir, catalog_entry_dict)
```

---

## Projected semantic models (Plan 29)

A pipeline that declares `data_product:`, `contract:` and `semantic:`
(see [Core Engine](core.md#semantic-projection-metadata-data_product-contract-semantic-plan-29))
is the deterministic source of the semantic model. The model is prepared in the
same PR as the table, without Spark and without an LLM.

```text
pipeline YAML → OutputProjector → SemanticDraftBuilder → .drafts/<model>.yaml
                                          │
                                          └→ SemanticSynchronizer → promote → semantic_models/<model>.yaml
```

### Managed drafts

`SemanticDraftBuilder` turns a `ProjectedSchema` into a **managed draft** under
`semantic_models/.drafts/<model_key>.yaml`. The draft is a pure function of its
input: identical input yields a byte-identical file. It is written atomically
(temp file + `os.replace`) and carries a `_generated_by` marker plus a
`metadata` block with the source contract, its version, the projection
definition hash, the generated field names, and the declared grain. The builder
refuses to overwrite a file that does not carry that marker.

It is deliberately conservative:

- dimensions are emitted only when **declared** (in the semantic seed or
  `contract.output`) or structurally safe — a field whose type could not be
  inferred is never guessed into a dimension;
- a dimension whose type cannot be mapped onto a validator-supported semantic
  type keeps no `type` key and is flagged `needs_curation: [type]` rather than
  being coerced to `string`;
- the pipeline supports more aggregates than the semantic layer
  (`stddev`, `variance`, `sum_distinct`, `approx_count_distinct`, `first`,
  `last`); those measures are listed under `metadata.unmapped_measures` for
  curation instead of failing the draft;
- when no measure maps at all, a `row_count` `COUNT(*)` placeholder keeps the
  draft valid and is flagged `needs_curation` so it is replaced before promotion.

Drafts live in a hidden `.drafts/` directory and are **never** preloaded:
`SemanticEngine` still only reads `semantic_catalog.yaml` at startup.

### Synchronization

`SemanticSynchronizer` is a **three-way merge against the last generation**, not
a diff between two files. It reconciles the last managed draft (the base), the
curated model (the human's current state) and a freshly regenerated candidate,
so curated edits survive regeneration.

`SyncReport` exposes `.changes`, `.conflicts`, `.suggestions` and
`.safe_to_apply`. **Any ambiguity yields a non-empty report and writes
nothing** — including a rename suggestion, which is reported for a human to
decide rather than auto-applied. Conflicts include a removed column still used
by a curated metric, a narrowing type change, and a change to the declared
grain.

### CLI workflow

```bash
skifer semantic sync PIPELINE --check         # report only, writes nothing
skifer semantic sync PIPELINE --write-draft   # apply when the report is clean
skifer semantic sync PIPELINE --promote       # draft → curated model + catalog
skifer semantic validate PIPELINE MODEL       # cross-validate a model against a pipeline
```

Exit codes are the CI contract:

| Code | Meaning |
|---|---|
| `0` | already current / valid / promoted |
| `1` | technical or validation failure |
| `2` | drift detected (or applied by `--write-draft`) |
| `3` | conflict or unresolved rename suggestion |

`--check` is strictly side-effect free. `--promote` refuses on conflicts or
validation failure, is a no-op when the curated model is already current, and
refuses outright rather than writing whenever promotion would drop content a
human authored. It updates the catalog through the existing
`SemanticEngine.update_catalog()` path.

`examples/13_semantic_projection/` projects the managed draft twice to show
byte-identical output, then prints `semantic sync --check` exit codes `0`, `2`,
and `3` for current, drift, and conflict states, along with the refusal to
overwrite an unmanaged human-owned draft.

---

## SemanticBuilder

`SemanticBuilder` uses an LLM to generate YAML semantic models from business context.

```python
from skifer.semantic.builder import SemanticBuilder
from skifer.semantic.llm_provider import get_llm_provider

builder = SemanticBuilder(
    llm_provider=get_llm_provider(),
    output_dir="semantic_models",
)
```

### `build()`

```python
yaml_path = builder.build(
    model_key="kpi_orders",              # Changed: single identifier (no split_value)
    table="gold.fact_orders",
    layer="gold",
    description="Sales KPIs by channel, country and category",
    base_filter="status = 'completed'",
    tags=["orders", "revenue"],
    source_notebook="notebooks/transform_orders.ipynb",
    glossary_path="glossaries/business_terms.json",
    extra_context="Revenue is always in euros (EUR).",
)
```

| Parameter | Type | Description |
|---|---|---|
| `model_key` | `str` | Unique model identifier (e.g. `"kpi_orders"`, `"kpi_orders_erp"`) — no longer separate `model_name` + `split_value` |
| `table` | `str` | Source table (e.g. `"gold.fact_orders"`) |
| `layer` | `str` | Layer (`"gold"`, `"silver"`) |
| `description` | `str` | Human-readable description |
| `base_filter` | `str` | SQL WHERE clause applied to all metrics |
| `tags` | `list[str]` | Tags for catalog search |
| `source_notebook` | `str` | Path to a `.ipynb` transformation notebook |
| `glossary_path` | `str` | Path to a business glossary (JSON/YAML/TXT/PDF/PPTX) |
| `extra_context` | `str` | Free-text business context |

**Returns:** absolute path to the generated YAML file.

The builder:

1. Extracts context from the notebook and glossary
2. Calls the LLM with a strict structural prompt
3. Validates the output (up to 3 attempts — on failure, asks for correction)
4. Writes `output_dir/<model_key>.yaml` (flat structure)
5. Updates `semantic_catalog.yaml`

If all 3 attempts fail, the context is saved to `output_dir/.errors/` and a `RuntimeError` is raised.

### `build_from_projection()` — constrained enrichment (Plan 29)

When a managed draft already exists, the LLM must not author structure. This
entry point keeps the draft authoritative and treats the model's response as
**untrusted input**:

```python
yaml_path = builder.build_from_projection(
    projected=projected,          # ProjectedSchema from OutputProjector
    schema=parsed_schema,         # ParsedSchema from parse_to_ir
    managed_draft=draft_payload,  # from SemanticDraftBuilder
)
```

Accepted from the LLM: `description` and `synonyms` at model, dimension and
metric level, plus new metrics whose `sql` is either `"*"` with `count` or the
exact name of a projected output.

Rejected: any new dimension, any rename/retype/redefinition of a managed field,
any metric whose dependency is not a projected output, any unrecognised key,
any aggregate type outside the supported set, and any name that is not a plain
SQL identifier. The last one matters because `QueryResolver` interpolates
dimension and metric names directly as SQL aliases (`<expr> AS <name>`) — a name
like `n FROM secrets; DROP TABLE audit--` would otherwise reach the emitted SQL.
`SemanticValidator` enforces the same identifier rule, so it also covers the
legacy `build()` path and `--promote`.

A rejected suggestion is a normal outcome: it is dropped and the draft stands.
The merged result is then re-checked by both `SemanticValidator.validate_yaml()`
and `validate_against_projection()` before anything is written.

---

## NotebookExtractor

`NotebookExtractor` extracts business context from Jupyter notebooks for use by `SemanticBuilder`.

```python
from skifer.semantic.extractor import NotebookExtractor

extractor = NotebookExtractor()
context = extractor.extract("notebooks/transform_orders.ipynb")
print(context)
```

The extractor keeps:

- **Markdown cells** — business descriptions and documentation
- **Code cells containing SQL** — transformation logic
- **`[DOC]`** tagged cells — explicit documentation blocks
- **`[SQL]`** tagged cells — explicit SQL blocks
- **`[CODE]`** tagged cells — explicit code blocks

It discards boilerplate (imports, `print`, `display`, `show`) to minimize token usage.

---

## RuleInspector

`RuleInspector` extracts docstrings from registered `RuleRegistry` functions to provide business context to `SemanticBuilder`.

```python
from skifer.semantic.extractor import RuleInspector

inspector = RuleInspector()
context = inspector.extract_all()
```

---

## GlossaryReader

`GlossaryReader` reads business glossaries in multiple formats and converts them to plain text for LLM injection.

```python
from skifer.semantic.glossary import GlossaryReader

reader = GlossaryReader()
text = reader.read("glossaries/business_terms.json")
```

Supported formats:

| Format | Notes |
|---|---|
| `.json` | Flattens nested structures |
| `.yaml` / `.yml` | Flattens nested structures |
| `.txt` | Read as-is |
| `.pdf` | Extracted via `pypdf` or `pdfminer` |
| `.pptx` | Extracted via `python-pptx` |

---

## LLM Provider

`get_llm_provider()` returns a provider-agnostic `LLMProvider` instance.

```python
from skifer.semantic.llm_provider import get_llm_provider

llm = get_llm_provider()              # auto-detect from env vars
llm = get_llm_provider("anthropic")   # force provider
llm = get_llm_provider("openai", model="gpt-4o")  # force model
```

### Auto-detection priority

The provider is detected from environment variables in this order:

1. `OPENAI_API_KEY` → OpenAI (GPT-4o)
2. `ANTHROPIC_API_KEY` → Anthropic (Claude Sonnet)
3. `GOOGLE_API_KEY` → Google (Gemini 1.5)

Override with `LLM_PROVIDER=openai|anthropic|google`.

### Supported providers

| Provider | Default model | Env var |
|---|---|---|
| `openai` | `gpt-4o` | `OPENAI_API_KEY` |
| `anthropic` | `claude-sonnet-4-5` | `ANTHROPIC_API_KEY` |
| `google` | `gemini-1.5-flash` | `GOOGLE_API_KEY` |

### Methods

```python
# Single-turn completion
response = llm.complete(
    system_prompt="You are a BI expert.",
    user_message="What are common sales KPIs?",
    temperature=0.1,
)

# Multi-turn with history
response = llm.complete_with_history(
    system_prompt="...",
    history=[
        {"role": "user",      "content": "What is the revenue?"},
        {"role": "assistant", "content": "..."},
    ],
    temperature=0.0,
)
```

---

## semantic_catalog.yaml

The catalog is a lightweight index automatically maintained by `SemanticBuilder`.
The LLM only ever sees this file — never the full model YAML.

```yaml
models:
  - key: kpi_orders
    file: kpi_orders.yaml              # Flat structure (no subdirectory)
    layer: gold
    table: gold.fact_orders
    description: Sales KPIs by channel, country and category
    tags: [gold, kpi_orders, orders, revenue]
    dimensions: [channel, country, category, brand, order_date]
    metrics: [chiffre_affaires, nb_commandes, panier_moyen]
    base_filter: "status = 'completed'"
    generated_at: "2024-01-15"
  - key: kpi_orders_erp
    file: kpi_orders_erp.yaml
    layer: gold
    table: gold.fact_orders_erp
    description: ERP sales metrics
    tags: [gold, kpi_orders, erp, revenue]
    dimensions: [region, sales_org]
    metrics: [total_revenue, order_count]
    base_filter: "source = 'erp'"
    generated_at: "2024-01-15"
```

This keeps the LLM context window small (~500 tokens regardless of model count) while giving the agent enough information to select the right model.
