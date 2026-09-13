# API Reference

Complete reference for all public classes and functions.

---

## Core Layer

### `SkiferEngine`

**`skifer.core.core.SkiferEngine`**

Main orchestration engine. Handles environment setup, Spark runtime initialization, and pipeline execution.

```python
from skifer import SkiferEngine

engine = SkiferEngine(spark=None, config_path=None, force_env=None, monitor=None)
```

| Method | Signature | Description |
|---|---|---|
| `__init__` | `(spark=None, config_path=None, force_env=None, monitor=None)` | Initialize engine. Auto-detects Spark/Databricks/local mode, config.yaml, and environment. |
| `process_schema` | `(schema_dict, dataframes_in=None) → DataFrame` | Execute pipeline, return DataFrame (no write). |
| `run_process_to_table` | `(schema_dict, target_layer, target_table_name, dataframes_in=None)` | Execute pipeline and write to Delta table. |
| `run_process_and_split` | `(schema_dict, target_layer, target_table_name, split_column, dataframes_in=None)` | Execute pipeline and write one table per distinct split value. |
| `get_agent` | `(llm_provider=None, models_dir="semantic_models", history=False, session_title="Session") → GenBIAgent` | Factory method: create SemanticEngine + GenBIAgent in one call. |

---

### `RuleRegistry`

**`skifer.core.registry.RuleRegistry`**

Singleton registry for business rule functions and data loaders.

```python
from skifer import RuleRegistry
```

| Method / Decorator | Signature | Description |
|---|---|---|
| `register_rule` | `(name) → decorator` | Register a DataFrame transformation function. |
| `register_loader` | `(name) → decorator` | Register a data loader function. |
| `get_rule` | `(name) → Callable` | Retrieve a registered rule by name. |
| `get_loader` | `(name) → Callable` | Retrieve a registered loader by name. |
| `list_rules` | `() → list[str]` | List all registered rule names. |

---

## Application Services — Plan 31

The classes in `skifer.services` are transport-neutral and require a
`RequestContext` on every governed operation. See [Services and local API](services_and_api.md)
for scopes, HTTP routes, and CLI examples.

### `RequestContext` and `LocalIdentity`

**`skifer.services.context.RequestContext`** carries `subject`, a frozen set of
named `scopes`, `consumer_class`, and `TraceContext`. `require_scope(ctx, name)`
refuses an absent exact scope.

**`skifer.services.identity.LocalIdentity`** resolves the local OS identity and
creates a request context from a static local scope set. It rejects
`certification_override`; authority is never read from a request body or header.

### Service container

```python
from skifer.services import local_request_context
from skifer.services.container import build_services

services = build_services(".")  # does not open Spark
ctx = local_request_context()
project = services.project.open(ctx)
```

`ServiceContainer` exposes `project`, `rules`, `semantic`, `governance`,
`quality`, `agents`, `execution`, and the shared governed `data_service`.

| Class | Selected operations |
|---|---|
| `ProjectService` | `open`, `get_pipeline`, `describe`, `project_output`, `lineage`, `explain_rules`, `json_schema`, `op_catalog`, `write_pipeline`, `audit` |
| `RuleService` | `scan`, `list`, `dependency_graph`, `generate_snippet`, `write_rule` |
| `SemanticService` | `list_models`, `get_model`, `check`, `write_draft`, `promote` |
| `GovernanceService` | contracts, products, certification/history, run/quarantine reads, registry dictionary/upstream/downstream/impact/search, `diff_contracts` |
| `QualityService` | `checks_from_schema`, `history`, `last_report`, incident list/acknowledge/assign/resolve |
| `AgentService` | `ask`, `build` |
| `ExecutionService` | `connect`, `session`, `close`, `submit`, `status`, `cancel`, `logs`, `result` |

### `ExecutionService` result types

`SessionView` describes the current lazy runtime session. `JobStatusView` and
`LogsView` support polling. `ResultView` is the bounded JSON-native terminal
result and contains `job_id`, `run_id`, kind/state, rows, total, schema, monitor
report, publication decision, optional quarantine summary, and error.
Only one job may be active per service instance, and its `job_id` is the engine
`run_id`.

### Metadata registry

**`skifer.observability.metadata_store.DatasetRecord`** stores a target FQN,
pipeline and contract identity, definition hash, owner, `ColumnRecord` entries,
index time, latest run ID, and serialized lineage.

`SqliteMetadataStore` and `DeltaMetadataStore` implement `MetadataStore`:

| Method | Description |
|---|---|
| `upsert(record) → bool` | Idempotently store a changed definition. |
| `attach_run_id(target_fqn, definition_hash, run_id) → bool` | Attach the latest publication identity without changing definition content. |
| `get(target_fqn)` / `list_all()` | Read the latest registry records. |
| `search_columns(text)` | Search column names and descriptions. |

`MetadataRegistryQuery.upstream()`, `.downstream()`, and `.impact()` traverse the
merged cross-pipeline graph with cycle and depth protection.

---

## Semantic Layer

### `SemanticEngine`

**`skifer.semantic.semantic.SemanticEngine`**

Catalog-first semantic engine. Lazy-loads YAML models on demand.

```python
from skifer.semantic.semantic import SemanticEngine

semantic = SemanticEngine(engine, models_dir="semantic_models")
```

| Method | Signature | Description |
|---|---|---|
| `__init__` | `(engine, models_dir)` | Load catalog from `models_dir/semantic_catalog.yaml`. |
| `list_models` | `() → list[dict]` | Return compact summary of all registered models. |
| `get_model_summary` | `(model_key) → dict` | Return summary for a single model. |
| `query` | `(semantic_query) → DataFrame` | Execute a `SemanticQuery` and return a Spark DataFrame. |
| `query_with_evidence` | `(semantic_query, …) → SemanticResult` | Same execution, returning the DataFrame together with its `SemanticEvidence`. `query()` delegates to it. |
| `create_view` | `(semantic_query) → None` | Execute query and register result as Spark temp view. |
| `get_llm_context` | `() → str` | Return compact catalog string for LLM injection (~500 tokens). |
| `update_catalog` | `(models_dir, entry) → None` *(classmethod)* | Add or update a model entry in `semantic_catalog.yaml`. |

---

### `SemanticBuilder`

**`skifer.semantic.builder.SemanticBuilder`**

LLM-powered YAML semantic model generator.

```python
from skifer.semantic.builder import SemanticBuilder

builder = SemanticBuilder(llm_provider=llm, output_dir="semantic_models")
```

| Method | Signature | Description |
|---|---|---|
| `__init__` | `(llm_provider, output_dir, glossary_path=None)` | Initialize builder with LLM provider and output directory. |
| `build` | `(model_key, table, layer, description, tags, source_notebook, glossary_path, extra_context, base_filter) → str` | Generate YAML model and update catalog. Returns path to generated file. |

---

### `get_llm_provider`

**`skifer.semantic.llm_provider.get_llm_provider`**

Factory function for provider-agnostic LLM access.

```python
from skifer.semantic.llm_provider import get_llm_provider

llm = get_llm_provider(provider=None, model=None)
```

| Parameter | Type | Description |
|---|---|---|
| `provider` | `str \| None` | `"openai"`, `"anthropic"`, `"google"`, or `None` (auto-detect) |
| `model` | `str \| None` | Model name override (e.g. `"gpt-4o"`, `"claude-sonnet-4-5"`) |

---

### `LLMProvider`

**`skifer.semantic.llm_provider.LLMProvider`** (abstract base class)

| Method | Signature | Description |
|---|---|---|
| `complete` | `(system_prompt, user_message, temperature=0.1) → str` | Single-turn LLM completion. |
| `complete_with_history` | `(system_prompt, history, temperature=0.0) → str` | Multi-turn completion with message history. |

---

### `NotebookExtractor`

**`skifer.semantic.extractor.NotebookExtractor`**

Extracts business context from Jupyter notebooks.

```python
from skifer.semantic.extractor import NotebookExtractor

context = NotebookExtractor().extract(notebook_path)
```

| Method | Signature | Description |
|---|---|---|
| `extract` | `(notebook_path) → str` | Extract text context from `.ipynb` file. |

---

### `RuleInspector`

**`skifer.semantic.extractor.RuleInspector`**

Extracts docstrings from registered rule functions.

```python
from skifer.semantic.extractor import RuleInspector

context = RuleInspector().extract_all()
```

| Method | Signature | Description |
|---|---|---|
| `extract_all` | `() → str` | Return concatenated docstrings of all registered rules. |

---

### `GlossaryReader`

**`skifer.semantic.glossary.GlossaryReader`**

Reads business glossaries from multiple file formats.

```python
from skifer.semantic.glossary import GlossaryReader

text = GlossaryReader().read(glossary_path)
```

| Method | Signature | Description |
|---|---|---|
| `read` | `(path) → str` | Read and flatten glossary. Supports `.json`, `.yaml`, `.txt`, `.pdf`, `.pptx`. |

---

## Agentic Layer

### `GenBIAgent`

**`skifer.agentic.agent.GenBIAgent`**

Natural language BI agent. Two-step LLM pipeline + deterministic SQL execution.

```python
from skifer.agentic.agent import GenBIAgent

agent = GenBIAgent(semantic, llm, history=False, session_title="Session")
```

| Method | Signature | Description |
|---|---|---|
| `__init__` | `(semantic, llm, history=False, session_title="Session")` | Initialize agent. |
| `ask` | `(question) → AgentResponse` | Ask a natural language question. Returns `AgentResponse`. |

**`AgentResponse`** fields:

| Field | Type | Description |
|---|---|---|
| `success` | `bool` | True if a result was produced |
| `needs_clarification` | `bool` | True if a follow-up question was asked |
| `clarification_question` | `str` | Follow-up question text |
| `result` | `QueryResult \| None` | Query result (when `success=True`) |
| `error` | `str \| None` | Error message (when `success=False`) |

**`QueryResult`** fields:

| Field | Type | Description |
|---|---|---|
| `data` | `DataFrame` | Result Spark DataFrame |
| `explanation` | `str` | LLM-generated explanation |
| `model_used` | `str` | Model key that was selected |
| `resolved_sql` | `str` | Full SQL executed |
| `response_format` | `str` | `"kpi"`, `"table"`, `"chart"`, `"text_analysis"` |

---

### `QueryResolver`

**`skifer.agentic.resolver.QueryResolver`**

Deterministic, LLM-free SQL builder. Validates all names against the YAML model before execution.

```python
from skifer.agentic.resolver import QueryResolver

resolver = QueryResolver()
resolved = resolver.resolve(semantic_query, model_yaml)
```

| Method | Signature | Description |
|---|---|---|
| `resolve` | `(semantic_query, model_yaml) → ResolvedQuery` | Validate names and build full SQL. Raises `SemanticQueryError` on unknown names. |

---

### `SemanticQuery`

**`skifer.agentic.resolver.SemanticQuery`** (dataclass)

The boundary object between the LLM and the execution engine.

```python
from skifer.agentic.resolver import SemanticQuery

q = SemanticQuery(
    model_name="kpi_orders",
    metrics=["chiffre_affaires"],
    group_by=["country"],
    filters=[{"column": "status", "operator": "equals", "value": "completed"}],
    date_from="2024-01-01",
    date_to="2024-12-31",
)
```

| Field | Type | Default | Description |
|---|---|---|---|
| `model_name` | `str` | required | Model key (e.g. `"kpi_orders"`, `"kpi_orders_erp"`) |
| `metrics` | `list[str]` | `[]` | Metric names |
| `group_by` | `list[str]` | `[]` | Dimension names for GROUP BY |
| `filters` | `list[dict]` | `[]` | Declarative filters |
| `date_from` | `str \| None` | `None` | Start date ISO 8601 |
| `date_to` | `str \| None` | `None` | End date ISO 8601 |
| `mode` | `str` | `"query"` | `"query"` or `"view"` |
| `view_name` | `str \| None` | `None` | View name when `mode="view"` |
| `response_format` | `str` | `"table"` | `"kpi"`, `"table"`, `"chart"`, `"text_analysis"` |
| `explanation` | `str` | `""` | LLM-generated explanation |

| Class method | Signature | Description |
|---|---|---|
| `from_dict` | `(data: dict) → SemanticQuery` | Construct from a JSON dict (LLM output). |

---

### `ResolvedQuery`

**`skifer.agentic.resolver.ResolvedQuery`** (dataclass)

Full SQL query ready for execution, built by `QueryResolver`.

| Field | Type | Description |
|---|---|---|
| `select_exprs` | `list[str]` | SELECT expressions (e.g. `"SUM(amount_ttc) AS chiffre_affaires"`) |
| `from_fqn` | `str` | Fully-qualified table name |
| `where_clauses` | `list[str]` | WHERE conditions |
| `group_by_exprs` | `list[str]` | GROUP BY expressions |
| `full_sql` | `str` | Complete assembled SQL string |
| `sources` | `tuple[str, ...]` | Physical datasets the plan reads |
| `model_keys` | `tuple[str, ...]` | Semantic models the plan uses |
| `selected_expressions` | `tuple[SelectedExpression, ...]` | Definitions of the selected members only |

---

### `SemanticEvidence`

**`skifer.semantic.evidence.SemanticEvidence`** (frozen dataclass, Plan 29)

Self-contained, JSON-serializable record of what an answer was built from. It holds
no DataFrame and no live Spark reference, so it survives the session that produced it.

| Field | Type | Description |
|---|---|---|
| `schema_version` | `str` | Evidence format version (`"1"`) |
| `evidence_id` | `str` | Per-request UUID — never derived from the question text |
| `model_keys` / `dimensions` | `tuple[str, ...]` | Semantic objects the answer used |
| `metrics` | `tuple[MetricEvidence, ...]` | Per-metric definition hash, source columns and `lineage_status` |
| `sources` | `tuple[SourceEvidence, ...]` | Per-dataset certification snapshot, frozen at query time |
| `normalized_filters` | `tuple[dict, ...]` | Filter structure; values redacted unless policy allows |
| `policy` | `PolicyEvaluation` | The certification decision that authorised execution |
| `sql_hash` / `sql_text` | `str \| None` | `sha256:v1:<hex>` of the executed SQL; raw text only under policy |
| `execution_status` | `str` | `"pending"`, `"succeeded"` or `"failed"` |
| `execution_duration_seconds` | `float \| None` | Measured on a monotonic clock |
| `execution_error_type` | `str \| None` | Error class name — never the backend message |

`to_dict(include_sql=False, include_filter_values=False)` is a deliberate
field-by-field allowlist, not `asdict`: a field added later stays out of the payload
until it is explicitly admitted. Requesting SQL that policy withheld raises
`EvidenceRedactionError` rather than returning `None`, which a consumer could not tell
apart from "there was no SQL".

**`EvidencePolicy`** is a pure decision object (`include_sql`, `include_filter_values`),
fail-closed by default. **`SemanticExecutionError`** carries a partial evidence marked
`execution_status="failed"`.

---

### `SemanticQueryError`

**`skifer.agentic.resolver.SemanticQueryError`**

Raised by `QueryResolver` when a metric or dimension name is not found in the YAML model.

```python
from skifer.agentic.resolver import SemanticQueryError

try:
    resolved = resolver.resolve(q, model_yaml)
except SemanticQueryError as e:
    print(e)
    # Unknown metric 'revenue'. Did you mean: 'chiffre_affaires'?
```

---

### `SessionHistory`

**`skifer.agentic.history.SessionHistory`**

Container for all interactions in a `GenBIAgent` session.

```python
session = agent.session
```

| Method | Signature | Description |
|---|---|---|
| `entries` | `() → list[HistoryEntry]` | Return all logged entries. |
| `__len__` | `() → int` | Number of entries. |
| `to_pdf` | `(output_path) → None` | Export session to PDF (requires `fpdf2`). |
| `to_json` | `(output_path) → None` | Export session to JSON. |
| `from_json` | `(path) → SessionHistory` *(classmethod)* | Load session from JSON. |

**`HistoryEntry`** fields:

| Field | Type | Description |
|---|---|---|
| `timestamp` | `str` | ISO 8601 timestamp |
| `question` | `str` | Original user question |
| `model_used` | `str \| None` | Model key selected |
| `explanation` | `str \| None` | LLM explanation |
| `response_format` | `str \| None` | Output format |
| `kpi_value` | `float \| int \| None` | KPI scalar value |
| `kpi_label` | `str \| None` | KPI label |
| `table_markdown` | `str \| None` | Markdown table string |
| `chart_config` | `dict \| None` | Chart configuration |
| `chart_image_b64` | `str \| None` | Base64-encoded PNG |
| `text_summary` | `str \| None` | Narrative text |
| `error` | `str \| None` | Error message |

---

### `HistoryExporter`

**`skifer.agentic.exporter.HistoryExporter`**

Generates a PDF from a `SessionHistory`. Used internally by `session.to_pdf()`.

```python
from skifer.agentic.exporter import HistoryExporter

HistoryExporter().to_pdf(session, "report.pdf")
```

| Method | Signature | Description |
|---|---|---|
| `to_pdf` | `(session, output_path) → None` | Generate PDF. Requires `fpdf2`. |
