# Agentic Layer

The Agentic Layer exposes the Semantic Layer through a natural language interface.
Ask a business question in plain French or English — get back a Spark DataFrame from Databricks or local PySpark.

The LLM **never writes SQL** and **never sees your data**.

## Read-only MCP surface

The MCP documentation moved to [Read-only MCP](mcp.md). This pointer preserves the original section anchor for existing links.

---

## Governed capabilities (write-back)

The write-back documentation moved to [Governed capabilities](capabilities.md). This pointer preserves the original section anchor for existing links.

---

## Supervised adaptive Gold

The adaptive optimization documentation moved to [Supervised adaptive Gold](adaptive.md). This pointer preserves the original section anchor for existing links.

---

## Architecture

```
User question (natural language)
        │
        ▼
   GenBIAgent.ask()
        │
        ├── Step A — Model selection (LLM)
        │     Input: compact catalog (~500 tokens)
        │     Output: model key  →  full YAML loaded
        │
        ├── Step B — SemanticQuery generation (LLM)
        │     Input: question + model YAML
        │     Output: SemanticQuery (metric/dimension names only, no SQL)
        │
        ├── QueryResolver.resolve()  [no LLM]
        │     Input: SemanticQuery + YAML model
        │     Output: ResolvedQuery (full SQL)
        │     Raises: SemanticQueryError if any name is unknown
        │
        └── SparkBackend.execute_sql()  [Databricks/local PySpark]
              Output: Spark DataFrame
```

## Semantic certification migration

Semantic certification is controlled per environment with
`semantic_certification_policy`. Deploy first with `off` (the default) to keep
existing agent behavior unchanged. Move pre-production to `warn` next: queries
continue to run, and operators can observe
`SemanticEngine.certification_warning_count` during a representative traffic
window. This counter is in-memory, scoped to one `SemanticEngine` instance, reset
on each restart, and not aggregated across processes or pods; it is useful for an
interactive session or an isolated job, not as a production traffic-window metric.

Enable `enforce` only after warnings have stayed at zero for that window.
Use `supervised` instead when uncertified access should require a human review
rather than a hard denial. Certification storage and contract identity are
covered in [Data Observability](observability.md); keep that registry healthy
before switching production to a strict policy.
In serving, `consumer_id`, `user_id` and `trace_id` currently come from
client-declared request metadata, not from an authenticated Databricks Model
Serving identity. `scopes` is therefore deliberately never populated from
request params there, so callers cannot self-assign `certification_override`.

---

## GenBIAgent

```python
from skifer.agentic.agent import GenBIAgent

agent = GenBIAgent(
    semantic=semantic,
    llm=llm,
    history=True,
    session_title="Weekly KPIs",
)
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `semantic` | `SemanticEngine` | required | Semantic engine instance |
| `llm` | `LLMProvider` | required | LLM provider instance |
| `history` | `bool` | `False` | Enable session history logging |
| `session_title` | `str` | `"Session"` | Title for the PDF/JSON export |

### `ask(question)`

The main entry point.

`examples/15_genbi_agent/` uses a local stub provider to print the two names-only
LLM steps and the SQL that `QueryResolver` deterministically builds from them;
it then proposes the unknown dimension `regions` and prints the refusal with the
declared `region` suggestion.

```python
resp = agent.ask("Quel est le CA total par pays pour le Q4 2024 ?")
```

Returns an `AgentResponse` object:

```python
if resp.success:
    resp.result.data.show()          # Spark DataFrame
    print(resp.result.explanation)   # what the LLM understood

elif resp.needs_clarification:
    print(resp.clarification_question)  # follow-up question to ask the user

else:
    print(resp.error)  # error message
```

### `AgentResponse` fields

| Field | Type | Description |
|---|---|---|
| `success` | `bool` | True if a DataFrame was produced |
| `needs_clarification` | `bool` | True if the agent asked a follow-up question |
| `clarification_question` | `str` | The follow-up question (when `needs_clarification`) |
| `result` | `QueryResult` | Contains `data` (DataFrame) and `explanation` |
| `error` | `str` | Error message on failure |

### `QueryResult` fields

| Field | Type | Description |
|---|---|---|
| `data` | `DataFrame` | The result as a Spark DataFrame |
| `explanation` | `str` | LLM-generated explanation of what was computed |
| `model_used` | `str` | Model key that was selected |
| `resolved_sql` | `str` | The full SQL that was executed |
| `response_format` | `str` | `"kpi"`, `"table"`, `"chart"`, `"text_analysis"` |

---

## Response formats

The LLM detects the appropriate output format from the question phrasing:

| Format | When used | What is stored |
|---|---|---|
| `kpi` | Single scalar value ("total revenue") | `kpi_value`, `kpi_label` |
| `table` | Grouped data ("by country") | `table_markdown` |
| `chart` | Trend or comparison ("over time", "compare") | `chart_config`, `chart_image_b64` (PNG) |
| `text_analysis` | Narrative ("analyze", "explain") | `text_summary` |

---

## QueryResolver

`QueryResolver` is deterministic — no LLM, no surprises.
It translates a `SemanticQuery` (containing only names from the YAML) into full SQL.

```python
from skifer.agentic.resolver import QueryResolver, SemanticQuery

resolver = QueryResolver()

q = SemanticQuery(
    model_name="kpi_orders",
    metrics=["chiffre_affaires", "nb_commandes"],
    group_by=["channel", "country"],
    filters=[{"column": "status", "operator": "equals", "value": "completed"}],
    date_from="2024-01-01",
    date_to="2024-12-31",
)

resolved = resolver.resolve(q, model_yaml)
print(resolved.full_sql)
```

If a metric or dimension name is not found in the YAML, `SemanticQueryError` is raised **before any Spark execution**, with a suggestion:

```
SemanticQueryError: Unknown metric 'revenue'. Did you mean: 'chiffre_affaires'?
```

### Multi-model queries (Plan 29, feature 6)

Given a `SemanticPlanner`, the resolver compiles queries that span several
curated models. Names still come from the YAML only — the LLM never emits a join
condition, a table alias or a date bound.

```python
from skifer.semantic.planner import SemanticPlanner

resolver = QueryResolver(SemanticPlanner(semantic_engine))
resolved = resolver.resolve(
    SemanticQuery(model_name="orders", metrics=["revenue"], group_by=["customers.segment"]),
    semantic_engine._get_model("orders"),
    "`main`",
)
```

```sql
SELECT m1.`segment` AS segment, SUM(m0.`amount`) AS revenue
FROM `main`.`gold`.`orders` AS m0
LEFT JOIN `main`.`gold`.`customers` AS m1 ON m0.`cid` = m1.`id`
GROUP BY m1.`segment`
```

Aliases are positional and therefore stable across runs and hash seeds. A name
that exists in the root model resolves there, preserving every pre-existing
single-model query byte for byte; a name that exists in several non-root models
must be qualified as `model.name`. Grain safety, fanout and calendar rules are
documented in the [YAML reference](yaml_spec.md#semantic-domain-fields).

Filter values are literals, not SQL: strings are checked for injection
characters, numbers and booleans render as-is, and any other type — including a
list or a mapping smuggled in as a scalar value — is refused by name.

### `SemanticQuery` fields

| Field | Type | Description |
|---|---|---|
| `model_name` | `str` | Model key (e.g. `"kpi_orders.ecommerce"`) |
| `metrics` | `list[str]` | Metric names to aggregate |
| `group_by` | `list[str]` | Dimension names for GROUP BY |
| `filters` | `list[dict]` | Declarative filters (see below) |
| `date_from` | `str` | Start date ISO 8601 (`"2024-01-01"`) |
| `date_to` | `str` | End date ISO 8601 (`"2024-12-31"`) |
| `period` | `str` | Name of a period declared in the model's calendar; exclusive with `date_from`/`date_to` |
| `mode` | `str` | `"query"` (DataFrame) or `"view"` (temp view) |
| `view_name` | `str` | View name when `mode="view"` |
| `response_format` | `str` | `"kpi"`, `"table"`, `"chart"`, `"text_analysis"` |
| `explanation` | `str` | LLM explanation of the query |

### Filter operators

| Operator | SQL | Notes |
|---|---|---|
| `equals` | `= 'value'` | (also: `eq`) |
| `not_equals` | `!= 'value'` | (also: `neq`) |
| `greater_than` | `> 'value'` | (also: `gt`) |
| `less_than` | `< 'value'` | (also: `lt`) |
| `greater_than_equal` | `>= 'value'` | (also: `gte`) |
| `less_than_equal` | `<= 'value'` | (also: `lte`) |
| `in` | `IN ('a', 'b')` | Values as list or semicolon-separated string |
| `not_in` | `NOT IN (...)` | Values as list or semicolon-separated string |
| `contains` | `LIKE %val%` | |
| `not_contains` | `NOT LIKE %val%` | |
| `starts_with` | `LIKE val%` | |
| `ends_with` | `LIKE %val` | |
| `like` | `LIKE 'pattern'` | |
| `not_like` | `NOT LIKE 'pattern'` | |
| `is_null` | `IS NULL` | No `value` needed |
| `is_not_null` | `IS NOT NULL` | No `value` needed |

```python
filters = [
    {"column": "channel",  "operator": "in",  "value": ["web", "mobile"]},
    {"column": "country",  "operator": "equals",  "value": "France"},
    {"column": "brand",    "operator": "not_equals", "value": "test"},
]
```

---

## SessionHistory

When `history=True`, every `ask()` call is logged in `agent.session`.

```python
# Inspect entries
for entry in agent.session.entries():
    print(entry.question, "→", entry.model_used)

# Export to PDF
agent.session.to_pdf("weekly_report.pdf")

# Export to JSON
agent.session.to_json("session_backup.json")

# Reload from JSON
from skifer.agentic.history import SessionHistory
session = SessionHistory.from_json("session_backup.json")
```

### `HistoryEntry` fields

| Field | Description |
|---|---|
| `timestamp` | ISO 8601 timestamp |
| `question` | Original user question |
| `model_used` | Semantic model key |
| `explanation` | LLM explanation |
| `response_format` | Format detected (`kpi`, `table`, `chart`, `text_analysis`) |
| `kpi_value` | Scalar value for `kpi` format |
| `kpi_label` | Label for `kpi` format |
| `table_markdown` | Markdown table for `table` format |
| `chart_config` | Chart configuration dict for `chart` format |
| `chart_image_b64` | Base64-encoded PNG for `chart` format |
| `text_summary` | Narrative text for `text_analysis` format |
| `error` | Error message if the query failed |

---

## PDF export

The `HistoryExporter` generates a structured PDF from a `SessionHistory`.

```python
agent.session.to_pdf("analysis_report.pdf")
```

PDF structure:

- **Cover page** — session title, generation date, number of analyses
- **One page per interaction** — question, model used, LLM explanation, formatted result

Requires `fpdf2`:

```bash
pip install fpdf2
# or
pip install "skifer[semantic-full]"
```

---

## Clarification loop

When the agent cannot confidently match the question to a model or metric, it returns a clarification question instead of guessing:

```python
resp = agent.ask("Montre-moi les chiffres")

if resp.needs_clarification:
    print(resp.clarification_question)
    # "Souhaitez-vous voir les chiffres de ventes (kpi_orders.ecommerce)
    #  ou les chiffres de stocks (kpi_inventory) ?"

# Follow up with the clarification
resp2 = agent.ask("Les chiffres de ventes")
```

---

## Design guarantees

| Guarantee | How it is enforced |
|---|---|
| LLM never writes SQL | Step B output is only metric/dimension names; SQL is built by `QueryResolver` |
| LLM never sees data | Only catalog metadata and YAML structure go to the LLM |
| Invalid names fail fast | `QueryResolver` raises `SemanticQueryError` before any Spark call |
| Suggestions on error | Levenshtein distance used to suggest the closest valid name |
| Reproducible SQL | Same `SemanticQuery` always produces the same SQL |

---

## AgenticHub — Unified conversational router

`AgenticHub` is the single entry point for all agentic interactions. It routes natural language
questions to the right specialist agent using deterministic keyword matching (no LLM required for routing).

`examples/21_hub_routing/` runs this router with no LLM and no Spark session, which is what the
class docstring promises. It prints the mode each question resolves to, including the two that
refuse: a hub answers `Agent '<name>' non configuré dans ce hub` for an agent nobody injected,
rather than improvising. It also shows `DictionaryAgent` declining an intent it cannot determine
without a provider, which is better than guessing one.

```python
from skifer.agentic.hub import AgenticHub
from skifer.semantic.llm_provider import get_llm_provider

hub = AgenticHub(
    genbi_agent=agent,           # optional
    lineage_agent=lineage_agent, # optional
    quality_agent=quality_agent, # optional
    dictionary_agent=dict_agent, # optional
    builder_agent=builder_agent, # optional
    llm=get_llm_provider(),      # optional — enables LLM intent fallback
)

resp = hub.ask("D'où vient la colonne amount_eur dans fact_orders ?")
# → routed to LineageAgent.ask()

resp = hub.ask("Quel est le CA par pays pour le Q4 ?")
# → routed to GenBIAgent.ask()
```

### Routing logic

| Keyword trigger | Agent |
|---|---|
| `lineage`, `lignage`, `provient`, `upstream`, `downstream`, `d'où vient` | `LineageAgent` |
| `quality`, `qualité`, `check`, `null`, `doublon`, `contrat` | `QualityAgent` |
| `dictionnaire`, `dictionary`, `définition`, `description`, `glossaire` | `DictionaryAgent` |
| `pipeline`, `yaml`, `builder`, `génère`, `créer un pipeline` | `BuilderAgent` |
| Everything else | `GenBIAgent` (or LLM intent classification if `llm` provided) |

If the target agent is not configured, `hub.ask()` returns `mode="error"` with an informative message.

### UserProfile — Alias pre-resolution

`AgenticHub` integrates `UserProfile` to normalize user-specific vocabulary before routing.

```python
from skifer.agentic.user_profile import UserProfile

profile = UserProfile()
profile.add_alias("CA", "chiffre_affaires")
profile.add_alias("clients VIP", "segment:premium")

# Persists in ~/.skifer_profile.yaml
```

When the hub receives a question, known aliases are substituted before routing —
so `"CA par pays"` is treated as `"chiffre_affaires par pays"`.

---

## LineageAgent — Conversational column lineage

`LineageAgent` wraps `LineageTracker`, `DataDictionary`, and `LineageRenderer` behind a
natural language interface — no Spark execution required.

```python
from skifer.agentic.lineage_agent import LineageAgent
from skifer import parse_schema

schema = parse_schema(open("schemas/gold/fact_orders.yaml").read())
agent = LineageAgent(schema_dict=schema)

# Trace where a column comes from
resp = agent.trace("fact_orders", "amount_eur")

# Trace downstream impact
resp = agent.impact("raw_orders", "amount")

# Look up a field definition
resp = agent.lookup("fact_orders", "amount_eur")

# Render the full lineage graph
resp = agent.render(format="mermaid")   # or "html", "json"

# Natural language routing
resp = agent.ask("D'où vient la colonne amount_eur ?")
```

### `LineageResponse` fields

| Field | Description |
|---|---|
| `mode` | `"trace"`, `"impact"`, `"lookup"`, `"render"`, `"error"` |
| `entries` | List of `FieldEntry` (upstream/downstream columns) |
| `graph` | Rendered graph string (mermaid / HTML / JSON) |
| `narrative` | LLM-generated prose explanation (when `narrative=True`) |
| `error` | Error message on failure |

### `narrative=True` — LLM prose explanations

```python
agent = LineageAgent(schema_dict=schema, llm=get_llm_provider())
resp = agent.trace("fact_orders", "amount_eur", narrative=True)
print(resp.narrative)
# "The column amount_eur originates from raw_orders.amount,
#  cast to double and rounded to 2 decimal places."
```

---

## QualityAgent — Conversational data quality

`QualityAgent` wraps `DataMonitor`, `ContractExtractor`, `HistoryStore`, `MonitorReporter`,
and `AlertDispatcher` behind a natural language interface.

`examples/22_quality_agent/` runs it against a real local table holding one NULL and one duplicate,
and prints both failures. It also runs a check on a column that does not exist: the status is
`ERROR`, never a silent pass. Watch `QualityResponse.passed` in that output — it means *no critical
failure*, so a non-critical check that could not execute at all leaves it `True`.

```python
from skifer.agentic.quality_agent import QualityAgent
from skifer.observability.history import SqliteHistoryStore

agent = QualityAgent(
    backend=engine.backend,
    schema_dict=schema,           # default schema used by ask()
    history_store=SqliteHistoryStore(),
    alert_config={"slack_webhook": "https://hooks.slack.com/..."},  # optional
    llm=get_llm_provider(),       # optional — enables narrative + NL parsing
)

# Run quality checks
resp = agent.check("catalog.silver.fact_orders")

# Get last 5 reports
resp = agent.get_history("catalog.silver.fact_orders", n=5)

# Render latest report
resp = agent.report("catalog.silver.fact_orders", format="text")

# Natural language routing
resp = agent.ask("Lance les checks qualité sur fact_orders")
```

### `QualityResponse` fields

| Field | Description |
|---|---|
| `mode` | `"check"`, `"history"`, `"report"`, `"error"` |
| `report` | `MonitorReport` from the latest check |
| `history` | List of past `MonitorReport` objects |
| `narrative` | LLM-generated quality summary (when `narrative=True`) |
| `error` | Error message on failure |

---

## DictionaryAgent — Conversational data dictionary

`DictionaryAgent` wraps `DataDictionary` and `GlossaryReader` behind a natural language interface.

```python
from skifer.agentic.dictionary_agent import DictionaryAgent

agent = DictionaryAgent(schema_dict=schema)

# Look up a column
resp = agent.lookup("fact_orders", "amount_eur")

# List all fields for a table
resp = agent.list_fields("fact_orders")

# Export dictionary
resp = agent.export(format="json")   # or "text"

# Enrich from a glossary file
agent.enrich("glossary/business_terms.xlsx")

# Natural language
resp = agent.ask("Qu'est-ce que la colonne amount_eur ?")
```

### `DictionaryResponse` fields

| Field | Description |
|---|---|
| `mode` | `"lookup"`, `"list"`, `"export"`, `"error"` |
| `entry` | Single `FieldEntry` (for lookup) |
| `entries` | List of `FieldEntry` (for list) |
| `export_text` | Serialized dictionary (for export) |
| `narrative` | LLM-generated explanation (when `narrative=True`) |
| `error` | Error message on failure |

---

## BuilderAgent — ETL pipeline generation

`BuilderAgent` generates `SkiferEngine`-ready YAML pipeline schemas from natural language
descriptions or via an interactive wizard. All table and column names are validated against the
live catalog before output.

`examples/20_builder_and_orchestration/` shows that validation refusing a draft: a stub provider
proposes a plausible filter operator that does not exist, catalog checks pass, and the schema loader
refuses it before anything is written. It then exports the accepted pipeline to Airflow and prints
the generated task body, which is a TODO scaffold rather than a working call.

```python
from skifer.agentic.builder_agent import BuilderAgent

builder = BuilderAgent(
    core=engine,
    llm=get_llm_provider(),  # required for ask() mode
)

# Interactive wizard (no LLM required)
yaml_path = builder.wizard()

# Natural language generation (LLM required)
resp = builder.ask(
    "Créer un pipeline Gold qui joint orders et customers, "
    "filtre sur region=EMEA et calcule le CA total par channel."
)
print(resp.yaml_content)
resp.save("schemas/gold/fact_orders_emea.yaml")
```

### `BuilderResponse` fields

| Field | Description |
|---|---|
| `success` | `True` if a valid YAML was produced |
| `yaml_content` | Generated YAML string |
| `needs_clarification` | `True` if catalog validation raised a question |
| `clarification_question` | Follow-up question for the user |
| `error` | Error message on failure |

### CatalogInspector — Catalog validation

`CatalogInspector` powers all catalog validation inside `BuilderAgent`.
It can also be used standalone:

```python
from skifer.core.catalog_inspector import CatalogInspector

inspector = CatalogInspector(backend=engine.backend, catalog=engine.db)

inspector.list_tables("silver")
inspector.list_columns("catalog.silver.fact_orders")
inspector.validate_table("catalog.silver.fact_orders")   # raises CatalogError if missing
inspector.validate_columns("catalog.silver.fact_orders", ["amount", "channel"])
```

### OrchestratorExporter — DAG generation

Generate orchestration artefacts for your pipelines:

```python
from skifer.agentic.orchestrator import OrchestratorExporter

exporter = OrchestratorExporter(backend=engine.backend)
result = exporter.export(
    yaml_paths=["schemas/gold/fact_orders.yaml", "schemas/gold/fact_customers.yaml"],
    format="auto",   # "airflow", "databricks", "python", or "auto"
)

print(result.airflow_dag)       # Airflow DAG Python file
print(result.databricks_bundle) # Databricks Asset Bundle YAML
print(result.python_script)     # Standalone Python script (always generated)
```

---

## Semantic evidence (Plan 29, feature 4)

Every answer can carry a record of what it was built from. The API is additive:
`SemanticEngine.query()` still returns a DataFrame, and `query_with_evidence()`
returns both.

```python
from skifer.semantic.evidence import EvidencePolicy

result = semantic.query_with_evidence(query)
result.dataframe                       # unchanged Spark DataFrame
result.evidence.to_dict()              # redacted, JSON-serializable

# Disclosure is opt-in and fail-closed by default.
audit = semantic.query_with_evidence(query, evidence_policy=EvidencePolicy(include_sql=True))
```

The evidence records the models and metrics used, a `sha256:v1:` hash of the SQL
actually executed, per-metric definition hashes and source columns, a frozen
certification snapshot per dataset, the policy decision that authorised execution,
and execution status and duration. It holds no DataFrame and no live Spark handle,
so it stays readable after the session ends.

Redaction is the default everywhere, including the text channel: filter values are
replaced by `<redacted>`, raw SQL is withheld, and asking for SQL the policy did not
release raises `EvidenceRedactionError` instead of returning `None`. When execution
fails, `SemanticExecutionError` carries a partial evidence marked
`execution_status="failed"`; only the error's class name is recorded, never the
backend message, which routinely quotes offending data values.

### Provenance in replies

Agentic responses expose an optional `evidence` field, and the text rendering adds
exactly one provenance line:

```text
Provenance : 2 sources : gold.orders, gold.customers · certification incomplète : gold.customers missing · fraîcheur 2 h · décision warn
```

The weakest link wins on every axis — all datasets are named, one uncertified source
flips the line away from "certifié", the reported date is the oldest certification and
the freshness the stalest, and a non-`ALLOW` decision is stated. A line that summarised
only the first source would reassure the reader about exactly the source that is not
certified.

### Serving

`SkiferChatModel.predict()` leaves `choices` byte-for-byte unchanged, so a client
reading only `choices[0].message.content` is unaffected. The evidence is added beside
it in a versioned block:

```json
{"choices": [...], "skifer": {"schema_version": "1", "evidence": {...}}}
```

MLflow is an optional extra and is not installed in the test environment, so
acceptance of this top-level field by `mlflow.pyfunc.ChatModel` has not been verified
empirically — see the note in `serving/chat_model.py`.

---

## CLI — `skifer hub`

A fully interactive terminal REPL powered by `rich`:

```bash
skifer hub
```

```
╔══════════════════════════════╗
║   Skifer Hub       ║
╚══════════════════════════════╝

[1] Nouvelle session
[2] Reprendre une session existante

> Quel est le CA par pays pour le Q4 2024 ?

  ┌─────────────────────────────────────────────────────┐
  │  kpi_orders.ecommerce — Chiffre d'affaires Q4 2024  │
  │  France: 1 245 870 €   Germany: 987 340 €  ...      │
  └─────────────────────────────────────────────────────┘

Special commands:
  help       — show help
  history    — display session history
  export pdf — export session to PDF
  exit       — quit
```

Install:

```bash
pip install "skifer[semantic-full]"
skifer hub
```
