# AGENTS.md — Skifer

## Architecture in one sentence
Declarative YAML schemas ("what") are strictly decoupled from Python business rules ("how"). The `SkiferEngine` bridges them at runtime: loads a YAML schema, resolves sandbox tables, applies `RuleRegistry` rules, and writes a Delta table.

## Package layout
```
src/skifer/
  core/         # SkiferEngine, ConfigurationManager, RuleRegistry, SandboxResolver, schema_loader
  semantic/     # SemanticEngine (catalog-first, lazy), SemanticBuilder (LLM), SemanticValidator
                # llm_provider.py — LLMProvider ABC + DatabricksProvider / OpenAI / Anthropic / Google
                # access_policy.py (pure cert policy), dependencies.py, output_projection.py — Plan 29
                # draft_builder.py (managed drafts), sync.py (three-way merge),
                # persistence.py (shared atomic YAML write + catalog entry) — Plan 29
  agentic/      # GenBIAgent, QueryResolver (deterministic SQL — no LLM SQL gen), SessionHistory
                # core/spark_backend.py — SparkBackend, the single runtime backend (Plan 26)
  observability/ # checks, monitor, contracts, alerts, history, reporter
                # certification{,_store}.py, publication.py, quarantine.py, odcs.py, uc_mirror.py — Plan 29
                # metadata_store.py, metadata_index.py, incidents.py, audit.py — Plan 31
  lineage/      # tracker, dictionary, renderer, classification.py — Plan 31 propagation
  services/     # transport-neutral application layer (Plan 31):
                # context.py, serialization.py, project.py, rules.py, governance.py, quality.py,
                # semantic.py, agents.py, identity.py, execution.py, container.py
  api/          # optional local FastAPI: app.py, security.py, errors.py, routes/ — Plan 31
  mcp/          # Read-only MCP server: server/resources/tools/auth/config/runtime — Plan 29 feature 7
  adaptive/     # Usage events -> patterns -> explainable proposals -> human review — Plan 29 feature 8
  capabilities/ # Governed write-back: validator, registry, preconditions, autonomy,
                #   credentials, idempotent executor, append-only history, harness — Plan 29 feature 9
  serving/      # SkiferChatModel (mlflow.pyfunc.ChatModel), hub_response_to_text()
  spark_factory.py   # get_spark_session(): Databricks notebook > Connect v2 > local[*]
  utils.py           # safe_columns and helpers
```
Public API exported from `__init__.py`: `SkiferEngine`, `RuleRegistry`, `ConfigurationManager`, `load_schema`, `parse_schema`, `safe_columns`.

## Essential commands
```bash
pip install -e ".[dev]"            # dev install
pip install -e ".[api]"            # FastAPI + Uvicorn local API
pytest                             # full test suite (no live cluster or API key needed)
pytest tests/test_core.py          # single module
uv run --extra api --extra spark --extra dev pytest  # API + Spark + dev test environment
ruff check src/                    # lint
pip install -e ".[docs]"           # mkdocs + material theme
mkdocs build --strict              # build the site; must stay warning-free
```

## Mandatory workflow for every `src/` change
1. Modify source in `src/skifer/`
2. Add/update the corresponding test in `tests/test_<module>.py`
3. Add an entry to `CHANGELOG.md` under `## [Unreleased]`
> Never bump the version in `pyproject.toml`.

When implementing a development plan from `docs/roadmap/`: **one plan point = one separate commit**, message referencing the plan point (e.g. `feat(plan17-1.2): ...`). Run `pytest tests/ -x --tb=short` and `ruff check src/` before each commit.

## Release checklist (every version bump — done by the user)
1. No new feature without associated, passing tests
2. Existing tests green — or updated with a documented reason if broken
3. `CHANGELOG.md` up to date
4. `CLAUDE.md` / `AGENTS.md` updated
5. mkdocs documentation (`docs/`) updated

## Key conventions

### Local vs Databricks mode
- `catalog: null` in `config.yaml` → local PySpark `local[*]` + Delta + Derby metastore (no cluster needed).
- FQN is `schema.table` locally, `catalog.schema.table` on Databricks.
- Env vars `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_CLUSTER_ID` trigger Connect v2.

### YAML `{{ key }}` param injection
Parameters are injected with a simple `{{ key }}` syntax (no Jinja2 dependency) before YAML parse:
```yaml
tables:
  - name: "{{ catalog }}.silver.orders"
    alias: ord
    filter:
      - "region:equals:EMEA"
      - "status:in:ACTIVE,PENDING"
      - "customer_id:is_not_null"
```

### Filter operator canonical names
`equals`, `not_equals`, `greater_than`, `less_than`, `greater_than_equal`, `less_than_equal`, `in`, `between`, `not_between`, `not_in`, `contains`, `not_contains`, `starts_with`, `ends_with`, `is_null`, `is_not_null`, `like`, `not_like`, `sql`

### `select_final` operations
`cast:type`, `upper`, `lower`, `trim`, `round:N`, `abs`, `length`, `to_date:fmt`, `nvl:val`, `coalesce:val`, `lit:val`, `expr:sql`, `split:sep,idx`, `substring:start,len`, `when:op:val`, `then:...`, `else:...`
`ceil`,

### Join types
Canonical join types: `left`, `right`, `inner`, `full`, `cross`, `left_anti`, `left_semi`.
Accepted aliases include `full outer`/`full_outer`/`outer` → `full`, `anti`/`left anti` → `left_anti`,
and `semi`/`left semi` → `left_semi`. `left_anti` and `left_semi` are Spark DataFrame-only.

### Nested partials (Plan 25)
A top-level `partials:` block (`{alias, path}` entries) runs nested YAML schemas as sub-transformations
and exposes each output under its `alias` (usable in `join`/`business_rules`) — a join can therefore depend
on a column produced by a business rule with no intermediate table. Child schemas inherit the parent's params,
resolve relative paths from the parent YAML dir, never write a final table, and are cycle-checked at load
(alias must be unique across `partials`/`tables`). Materialization is a **run-global** param, not YAML:
`run_from_yaml(..., params={..., "intermediate_mode": "inline|temp_view|table"})` (default `inline`).
`temp_view`/`table` name the artifact after the partial `alias` (sandbox-suffixed); the parent always keeps
using the DataFrame directly. `SparkBackend.register_temp_view` is the supported path.

### Streaming tables (Plan 27)
A table with `streaming: true` is read via `readStream`; the schema must declare a top-level
`materialization: streaming_table` block (strict bijection) — shorthand string or dict
`{type, trigger, checkpoint, write_mode, keys}`. Writes are incremental: `append` (default) or
CDC type 1 `upsert` (`foreachBatch` + `MERGE INTO` on `keys` — idempotent on replay). Load-time
guardrails reject every streaming incompatibility (one stream per schema, stream = join base,
`inner`/`left` only, sources `delta`/`text` only, no `dev_limit`/`qualify`/`drop_duplicates_on`/
loaders/JDBC sinks/partials children); `aggregation` rules are rejected at run start. Checkpoints:
`auto` → local `{warehouse}/_checkpoints/{schema+suffix}/{table}`, Databricks → requires the
`checkpoint_base` env param (fail-fast). `engine.full_refresh(layer, table)` purges checkpoint +
drops the table atomically. Compose flows with sub-layers (one streaming table + checkpoint per
stage), never with partials.

### Declarative aggregations (Plan 28)
A top-level `aggregate: {group_by, measures, having}` block replaces writing a Python `kind="aggregation"`
rule for an ordinary GROUP BY — usable in **any batch pipeline**, not only in materialized views. Measures
take `[source, target, func]` or `{source, target, func}`; functions live in `AGGREGATE_FUNCTIONS`
(`core/op_catalog.py`, single source of truth: `sum`, `avg`, `min`, `max`, `count`, `count_distinct`,
`sum_distinct`, `approx_count_distinct`, `stddev`, `variance`, `first`, `last` + aliases), and only `count`
accepts `source: "*"`. `having:` reuses the filter grammar and is validated at load against
`group_by ∪ measure targets`. Mutually exclusive with `select_final`/`keep_all_columns`, rejected on
streaming pipelines; `add_columns` runs **before** the aggregation so it can produce a group key.

### Materialized views (Plan 28)
`materialization: materialized_view` **short-circuits the DataFrame pipeline entirely** — no source is read.
The schema is compiled to SQL (`core/sql_compiler.py`) and issued as `CREATE [OR REPLACE] MATERIALIZED VIEW`
on a **Pro/Serverless SQL warehouse** via the Statement Execution API (`params.sql_warehouse_id`) — all-purpose
clusters and Databricks Connect refuse that DDL. No warehouse → fail-fast in job/prod **before compilation**,
otherwise the DDL is written to `{sql_output_dir}` (default `generated_sql/`) with a "NOT CREATED" warning and
the monitor is skipped. Local mode executes the compiled SELECT and writes a Delta table. Definition drift is
caught by a SHA-256 of the SELECT + definition options stored in `TBLPROPERTIES ('skifer.definition_hash')`:
absent → CREATE, same → REFRESH (skipped under `refresh: never`), different/unreadable → CREATE OR REPLACE.
Load-time refusals: Python `business_rules` (materialize upstream), `partials`, file sources, loaders,
`dev_limit`, JDBC sinks, `streaming: true`, `drop_duplicates_on`, `preprocess.qualify`, and `keep_all_columns`
with a join or >1 table. `run_process_and_split`/`run_union_sources_to_table` refuse MVs;
`engine.full_refresh(layer, table, materialization="materialized_view")` drops the view. SDK: `[databricks]` extra.

### Semantic projection from the pipeline (Plan 29, feature 0)
A pipeline declaring `data_product:` / `contract:` / `semantic:` is the deterministic source of its
semantic model — prepared in the same PR as the table, with no Spark and no LLM:
`OutputProjector` → `SemanticDraftBuilder` → `semantic_models/.drafts/<model>.yaml` →
`SemanticSynchronizer` → `--promote` → `semantic_models/<model>.yaml`.

**Drafts are pure functions of their input** (byte-for-byte reproducible), written atomically, and
marked `_generated_by` + `metadata` (source contract, version, `source_definition_hash`,
`generated_fields`, `grain`). The builder refuses to overwrite an unmarked file. It stays
conservative: dimensions only when declared or structurally safe; an unmappable type gets
`needs_curation` instead of being coerced to `string`; aggregates the pipeline supports but the
semantic layer does not (`stddev`, `variance`, `sum_distinct`, `approx_count_distinct`, `first`,
`last`) go to `metadata.unmapped_measures` rather than failing the draft.

`SemanticSynchronizer` is a **three-way merge against the last generation**, not a file diff.
**Any ambiguity produces a non-empty report and writes nothing** — a rename is suggested, never
auto-applied.

CLI exit codes are the CI contract — `0` current/valid/promoted, `1` technical or validation
failure, `2` drift, `3` conflict:
`skifer semantic sync PIPELINE --check|--write-draft|--promote` and
`skifer semantic validate PIPELINE MODEL`. `--check` never writes. `--promote` no-ops when the
curated model is current and refuses rather than writing when it would drop human-authored content.

`SemanticBuilder.build_from_projection()` treats LLM output as **untrusted input**: only
`description`/`synonyms` plus metrics bounded to `COUNT(*)` or a projected output are admitted;
everything else is dropped. Dimension and metric names must match `^[A-Za-z_][A-Za-z0-9_]*$` —
`QueryResolver` interpolates them straight into SQL as aliases, so a non-identifier name is an
injection vector. `SemanticValidator` enforces the same rule, covering the legacy `build()` path too.

Never make `SemanticEngine._load_catalog()` preload drafts — `.drafts/` stays invisible at startup.

### Certified publication & semantic certification gate (Plan 29)
**Opt-in via `data_product:`.** A schema declaring `data_product: {id, version}` routes `run_process_to_table()`
through `PublicationCoordinator` (`observability/publication.py`): stage into `_skifer_staging` → monitor
checks → persist check results → **promote** or **quarantine** (snapshot in `_skifer_quarantine`, offending
rows tagged `_violations`/`_run_id`/`_contract_version`). A schema **without** `data_product` keeps the legacy
write path byte-for-byte. The engine then requires both `certification_store=` and `monitor=` or it fails fast.
Explicit refusals: streaming, JDBC sinks, materialized views. Crash recovery: `coordinator.resume(run, definition)`.

**Registry.** `SqliteCertificationStore` (local) / `DeltaCertificationStore` (Databricks) in
`observability/certification_store.py`. Run events key on the **physical `target_fqn`**, not the logical
`data_product_id`. `Certification.checks_passed` derives from recorded critical check results. The persisted
`run_id` is the one minted by the pipeline (`run_process_to_table` / `run_from_yaml`) — **one end-to-end audit
identity**, minted on the business path and therefore identical whether or not tracing is on.

**Gate.** Configured per environment at the environment root (like `allow_raw_sql`):
`semantic_certification_policy: off|warn|enforce|supervised` (default `off`), plus optional
`semantic_certification_max_age: 24h` and `semantic_consumer_class`. `SemanticEngine.query()`/`create_view()`
and `GenBIAgent` run a **double gate** — preflight before compiling (no SQL generated on a known DENY) and a
recheck before executing. Decisions: `ALLOW`/`WARN`/`DENY`/`REQUIRE_HUMAN`; reasons: `MISSING`, `FAILED_CHECK`,
`EXPIRED`, `OVERRIDDEN`. The policy itself lives in `semantic/access_policy.py` as a **pure function** —
keep it dependency-free. **Fail-closed everywhere**: missing store, unresolved dependency, naive datetime →
refuse, never a silent ALLOW. Break-glass `CertificationOverride` requires the `certification_override` scope
in `ConsumerContext.scopes`, `actor == consumer_id`, and a tz-aware `expires_at` within 4h; serving never
populates `scopes` from client params.

### Semantic domain graph & joins (Plan 29, feature 6)
A model may declare `grain:`, `entities:` and `relationships:` (see `docs/yaml_spec.md`). A legacy
single-table model without them stays valid and produces **the same SQL as before**.

`SemanticPlanner` (`semantic/planner.py`) resolves **names** into a join path **before any SQL
exists** — the LLM never emits a join condition, an alias or a date bound. A name found in the root
model resolves there (strict backwards compatibility); a name found in several non-root models must be
qualified `model.name`. Aliases are positional, hence stable across runs and hash seeds. Explicit
refusals: no path; several minimal paths (both are named, never arbitrated); `many_to_many` and
`unknown` — **each with its own cause**, so a declared `many_to_many` is not reported as undeclared.

**Grain safety (fanout).** A relationship duplicates the rows of whichever side it is crossed *from*
when that side is the "one" side — this depends on the **direction of travel**, not on the declared
cardinality alone. The planner walks outward from the model owning the metric and refuses the first
edge that fans: a metric upstream of a `one_to_many` (mandated message `Unsafe fanout: metric grain
'order' crosses one_to_many relationship 'order_lines'.`), a metric reached **beyond** a fanout, and a
metric on the one-side of a `many_to_one`. Still allowed: a metric owned by the direct many-side
(computed at its own grain) and any query with no metric at all.

**Additivity.** `additive` (default, legacy behaviour) · `semi_additive` (requires
`non_additive_dimensions`, each pinned by `group_by` or an `eq` filter) · `non_additive` (refused as
soon as the plan contains a join).

**Versioned calendars.** A fiscal quarter resolves from a **declared definition**, never from Python
and never from the LLM, which only emits a period *name*. `calendars/<key>.yaml` carries
`key`/`version`/`periods` with ISO bounds; a model opts in with `calendar: <key>`. Loaded lazily and
cached — no calendar is opened at startup. `period` combined with `date_from`/`date_to` is refused as
ambiguous; unknown period, model without a calendar, and `period` without a planner are explicit
errors. Every date bound must parse as `YYYY-MM-DD` before reaching the SQL.

Filter values are literals, not SQL: strings are injection-checked, numbers and booleans render as-is,
and any other type — including a list or mapping smuggled in as a scalar — is refused by name.

### Semantic evidence (Plan 29, feature 4)
`query()` still returns a DataFrame; `query_with_evidence()` returns `SemanticResult`
(DataFrame + `SemanticEvidence`). **One execution source**: `query()` delegates — two parallel paths
would drift, and this is the layer where a gap between what runs and what is attested is unacceptable.

Evidence is **standalone** (no DataFrame, no live Spark handle, readable after the session) and
serialized through a **field-by-field allowlist**, never `asdict`: a field added later stays out until
deliberately admitted. It carries a `sha256:v1:` hash of the SQL *actually executed*, metric definition
hashes, source columns from a lineage subgraph restricted to the selected members (with an explicit
`lineage_status`), a frozen per-dataset certification snapshot, the policy decision, and execution
status/duration (duration on a **monotonic** clock, never a wall-clock subtraction).

**SQL normalization before hashing** collapses whitespace **outside string literals only**. A blanket
`\s+` would give `x = 'a  b'` and `x = 'a b'` the same hash — two different queries attested as one.

**Fail-closed disclosure.** `EvidencePolicy` is a pure object governing `include_sql` /
`include_filter_values`, both off by default. Requesting withheld SQL raises `EvidenceRedactionError`
rather than returning `None`, which a consumer cannot tell apart from "there was no SQL". On failure,
`SemanticExecutionError` carries partial evidence marked `execution_status="failed"` holding only the
error's **class name** — never the backend message, which routinely quotes offending data values.

**Provenance line** (exactly one, serving included): every dataset named, weakest link winning on each
axis — one uncertified source flips it away from "certifié", the date shown is the oldest certification
and the freshness the stalest, a non-ALLOW decision is stated. The text channel is not an escape hatch
from redaction.

**Serving**: `choices` unchanged byte-for-byte; evidence added beside it under a versioned `skifer`
block. MLflow is an optional extra and absent here, so acceptance of that top-level field by
`mlflow.pyfunc.ChatModel` is **not empirically verified** (noted in the code).

**Multi-model certification coverage.** The preflight only sees the root model — the join path does not
exist yet — so the **recheck evaluates every dataset in the compiled plan**. Without it, a multi-model
query obtained an ALLOW decided on the root table alone and read the joined datasets uncertified.
`off` mode resolves no dependencies at all.

### Runtime tracing (Plan 29, feature 5)
Off by default, **no mandatory dependency**. The core never imports `mlflow` or `opentelemetry` at
module level; adapters import them lazily, and both `observability/tracing.py` and
`tracing_exporters.py` import cleanly where neither is installed (proven via `sys.modules`). Optional
extra: `pip install -e ".[tracing]"`. Config lives under `observability.tracing`
(`exporter: none|otlp|mlflow|dual`, `required`, `capture_prompts`, `capture_sql`, `user_identity`,
`trace_location`).

**The rule above all others: business behaviour never depends on tracing.** An exporter failing at
init, span open, attribute set or close changes no return value, no persisted data and no exception —
it logs once, rate-limited. `required: true` is the only exception and is not the default.
**Never duplicate a business flow on tracing state**: that is precisely what made slice 5.2 derive the
persisted `run_id` from the trace context and fail an otherwise valid publication.

`NoOpTracer` (default, allocation-free) and `InMemoryTracer` (tests) implement the `Tracer`/`Span`
Protocols. The active span lives in a `ContextVar`, never a global — isolation proven across threads
and async tasks. `trace_id` always comes from the parent; correlation ids take the innermost context
explicitly set. A span is never parented to one that has already ended.

**Redaction.** `TraceAttributePolicy` allowlists attribute keys and bounds count (32), value length
(256) and events (64) per span. Everything else is dropped — `question`, `prompt`, `sql_text`, filter
values — and a rejection records the key and type, never the value. The policy applies on **every**
path leaving the process, `set_attribute` and the OTLP/MLflow adapters included: redaction that held
only in tests would keep the canaries green while production leaked. Errors record the exception's
**class name** only.

Span names come from a fixed taxonomy, never built from a variable (cardinality). Durations use a
monotonic clock. No Spark `count()` is ever triggered to fill an attribute.

**Identity.** `omit` by default; `hmac` reads `SKIFER_TRACING_HMAC_SECRET`, never `config.yaml`.
**With `hmac` and no secret the identity is omitted, not hashed** — an unkeyed digest of an email or
employee id is reversible by anyone who can guess the input.

**Unity Catalog.** `trace_location` must pre-exist; the exporter never creates it, and fails with an
actionable message when it is missing. Under `dual`, one exporter failing does not stop the other.

## Read-only MCP exposure (Plan 29 — feature 7)

An external agent discovers and queries **only** the governed layer. Optional extra:
`pip install -e ".[mcp]"` (official `mcp>=2.1,<3` SDK); the core imports without it.

```text
MCP transport (stdio/http) -> auth.py -> RequestContext
        v thin handlers, no direct engine access
  AgentReadyDataService   <- THE security boundary
        v scopes, budgets, certification gate, evidence
  SemanticEngine.query_with_evidence / catalog / lineage
```

**`AgentReadyDataService` (`agentic/data_service.py`) is the boundary**, testable without a transport.
Fail-closed scopes through `require_scope`: `models:read`, `contracts:read`, `lineage:read`,
`query:execute`. DTOs are built by **field-by-field allowlist** — never a raw YAML dict, never
`asdict`. `ServiceLimits` carries **hard** ceilings (100/page, 1000 rows, 50 filters, 1024-character
values); exceeding one is an explicit error, **never a silent clamp**. The pagination cursor is
opaque and validated; a stale one is refused, never reinterpreted as "start over". `query()` never
returns a DataFrame, and **no client scope reaches `ConsumerContext`** (the allowlist is empty), so
`certification_override` cannot cross the MCP boundary.

**Resources** (`mcp/resources.py`): `skifer://semantic/catalog`, `.../semantic/models/{key}`,
`.../contracts/{id}/{version}`, `.../certification/{dataset}`, `.../lineage/{dataset}/{column}`.
**Discovery is scope-filtered** — advertising an endpoint and the scope that opens it is
reconnaissance. URIs are parsed strictly: percent-decoding happens **before** validation, and `..`,
`/` and `\` are refused in a decoded segment. A deterministic ETag rides in the metadata.

**Tool** (`mcp/tools.py`): `query_semantic_model` only, with a **closed** JSON Schema
(`additionalProperties: false` at every level, closed operator enum). `mode`/`view_name` are
**absent** from the schema, not merely rejected. Enforcement is **server-side**
(`set(arguments) - allowed`), never the read-only annotation, which a client may ignore. The
advertised schema derives from the instance's limits, so it never promises more than the service
grants.

**Auth** (`mcp/auth.py`): stdio uses **static** identity and scopes from local config, ignoring
whatever the request claims. HTTP validates a bearer through an **injected verifier** — no token is
minted or signed here, and this is not an Authorization Server. Audience, issuer, resource and expiry
are all checked; expiry has **no leeway** and is timezone-aware UTC. Credentials are **per request**,
never in a singleton: isolation is proven under concurrency. No verifier detail crosses the boundary.
Trace identity is `omit` by default; `hmac` **with no secret omits** rather than hashing unkeyed.

**CLI**: `skifer mcp serve --transport stdio|http --config <file>`. A **non-loopback bind is a
startup error**, not a warning, and HTTP demands a complete auth configuration. Health is a frozen
constant with no business data. No secret appears in startup logs or configuration errors, YAML
parser diagnostics included.

### RuleRegistry pattern
```python
@RuleRegistry.register_rule()
def flag_high_value(df):
    return df.withColumn("is_high_value", F.when(F.col("amount") >= 1000, 1).otherwise(0))
```
Rules must be imported before calling any `engine.run_*` method.

### SemanticEngine: catalog-first, lazy loading
Only `semantic_catalog.yaml` loads at startup. Individual model YAMLs load on-demand and cache. **Never pre-load all models.**

### QueryResolver: zero LLM SQL
`QueryResolver` builds SQL exclusively from names defined in semantic YAML models. Unknown names raise `SemanticQueryError` with Levenshtein suggestions. Never generate raw SQL directly.

### Smart Sandbox
In interactive (non-prod, non-job) mode, source tables auto-resolve to `schema_XXXX.table`. Missing tables are cloned transparently. Cache file `.skifer_user` must be in `.gitignore`.

### Environment default params
An environment in `config.yaml` may declare an optional `params:` mapping; these merge into `engine.default_params`, so minimal notebooks resolve `{{ key }}` placeholders without hardcoding storage paths. Priority: explicit `run_from_yaml(params=…)` > config environment `params` > built-in `catalog`/`env` (built-ins win on collision). Non-mapping `params:` fails fast. Environment matching is case-insensitive via `ExecutionContext.env_config()`.

### Databricks Deployment (serving/)
`SkiferChatModel` (`serving/chat_model.py`) wraps `AgenticHub` as an `mlflow.pyfunc.ChatModel`:
- **`load_context()`** — called once at pod startup: initialises `SparkSession`, `DatabricksLLMProvider`, and `AgenticHub`.
- **`predict(messages)`** — extracts last user message, calls `hub.ask()`, returns OpenAI-compatible dict.
- **Deploy**: `python scripts/deploy_to_databricks.py` logs the model and registers it in UC Model Registry.
- **LLM provider**: `DatabricksProvider` in `llm_provider.py` — OpenAI-compatible client pointing at `{DATABRICKS_HOST}/serving-endpoints`. Auto-detected when `DATABRICKS_HOST` + `DATABRICKS_TOKEN` are set.
- **Install**: `pip install -e ".[serving]"` (adds `mlflow>=2.12,<3` and `openai>=1.0`).

### Tests
- LLM calls are always mocked; no live cluster, API key or network is needed.
- Spark is **not** mocked everywhere: 182 tests run a real local Delta session (the `spark`
  fixture in `tests/conftest.py`, plus the examples), the rest use doubles. A test that
  builds a `F.col(...)` needs the fixture even when it only asserts a raised error.
- One test file per module: `tests/test_<module>.py`.
- Fakes live in `tests/fakes/`.
- `examples/` is a deliverable: `tests/test_examples.py` runs every `run.py` in a
  subprocess. Changing behaviour an example shows means updating the example.

## Supervised adaptive Gold (Plan 29 — feature 8)

Real usage feeds **proposals**; nothing is deployed. The engine never edits `schemas/`, never calls
`engine.run_*`, never creates a materialized view, and never runs Git.

```text
SemanticEvidence -> UsageEventStore -> PatternAggregator -> RecommendationEngine
                                                                  v
                                              OptimizationProposal (evidence + rule + version)
                                                                  v ProposalGenerator
                        .skifer_proposals/<id>/{pipeline.yaml, draft, proposal.json}
                                                                  v adaptive accept
                                          human-owned schema -> PR -> deployment
                                                                  v adaptive evaluate
                                          improved | regressed | inconclusive
```

**No sensitive data in events.** `usage_event_from_evidence()` never copies the evidence: it
serializes it only to validate the allowlist, then rebuilds field by field. Filter shapes are
`column:operator`, never with a value. Fingerprint versioned (`sha256:v1:`) and insensitive to
non-semantic ordering.

**Deterministic aggregation.** Strict partition by environment, consumer class and model definition
hash; `failed` counted apart from succeeded; clock injected and mandatory; iteration over sorted
keys, so results are stable under any `PYTHONHASHSEED`.

**Explainable rules, never a model.** Static versioned registry (`frequent_aggregate`,
`repeated_join_path`, `missing_dimension`, `unused_generated_asset`) — no arbitrary plugins, no ML in
v1. Every reason names the threshold, the observed value and the verdict; a rule that does not
conclude emits a `RecommendationRefusal` with its contraindications. Fanout safety is **shared** with
`SemanticPlanner` via `find_unsafe_fanout()`, not reimplemented.

**Artifacts validated before display.** `ProposalGenerator` stages, runs `load_schema` **and**
`SemanticValidator`, then publishes: a visible proposal is one that loads. `proposal_id` is
content-derived (`proposal:v1:<sha256>`); artifact paths are recorded **relative** to the proposals
root — an absolute path pins the artifact to one checkout and leaks the local username.

**Human decision.** `skifer adaptive list|show|diff|accept|reject|evaluate`. `accept` refuses to
overwrite an existing output at **syscall level** (`os.O_CREAT | os.O_EXCL`, no check-then-write
race), revalidates source hashes (drift -> `stale`, not acceptable), and records `delivery.json`.

**Outcome evaluation.** Before/after windows on the partition recovered from the proposal's own
evidence events, never guessed; half-open windows, so no event is counted twice. Below the minimum
thresholds -> `inconclusive` naming the window and threshold; evidence purged -> `inconclusive:
evidence_unavailable`, not an exception. A regression on **either** axis (duration percentile, failure
rate) outweighs an improvement on the other. A zero baseline is compared against zero directly — the
multiplicative formula called two identical 0 ms windows regressed, i.e. exactly the queries an
optimization made too fast to measure. **A regression yields a human review recommendation and
nothing else**: no rollback, no drop, no subprocess, asserted at source level.

Exit codes: `0` success · `1` technical error · `2` usage · `3` stale · `4` state conflict or existing
output · `5` measured regression.

## Governed capabilities / write-back (Plan 29 — feature 9)

A capability lets an agent trigger an action on an **external** system: the only non-read-only
surface. The LLM picks a `capability_id` and fills a closed input schema; it never decides policy,
preconditions, approval, credentials or idempotency.

```text
lazy catalog -> CapabilityRegistry -> PreconditionEvaluator -> AutonomyStateMachine
                                                            -> CredentialBroker (injected, JIT)
                                                            -> GovernedExecutor -> append-only history
```

**The document can never name Python.** `executor` and every precondition `rule` are plain lowercase
names resolved against an explicit in-process registry; anything resembling an import path is
refused, and the modules contain no importlib/eval/dynamic lookup — asserted at source level.

**Closed, bounded input schema.** `additionalProperties: false` everywhere, mandatory maxLength /
maxItems / minimum / maximum, and refusal of `$ref`/`allOf`/`anyOf`/`oneOf`/`not`/`patternProperties`.
Depth is bounded (8): a document that crashes the validator is a document that was never validated.

**Fail-closed decisions.** A rule returns a typed `PreconditionOutcome` whose `reason_code` is a
closed identifier, so a sentence or a model answer cannot become a decision — it becomes UNKNOWN. A
raising rule becomes UNKNOWN with the class name only. **UNKNOWN never becomes ALLOW.** The state
hash is computed by the evaluator, never by the rule: the attestation cannot be forged by what it
attests. Preconditions are re-evaluated immediately before execution.

**Autonomy resolves to the stricter side** of the declared `approval` and the environment's
`capability_autonomy` (`shadow` default); `irreversible` never reaches `guarded`. Shadow executes
nothing, takes no credential, and calls only executors that declared dry-run support at registration.

**An approval binds one exact request**: actor, reason, an enforced 4-hour ceiling, plus the request
and precondition hashes. One changed argument character invalidates it. The precondition hash covers
rules, verdicts and observed state but **never wall-clock time** — hashing time made every approval
impossible to validate, and a fixed test clock hid it.

**Credentials**: the provider is injected — Skifer mints/signs/renews nothing and is not an
authorization server. Least scopes, TTL above the ceiling **refused rather than clamped**, and the
provider's own answer verified (extra scopes, different subject or longer life are refused). The
secret is reachable only via `reveal()`; repr/str/logging/json redact it and pickling raises.

**Idempotent execution.** Identity = hash(version, normalized input, subject). The journal is
consulted first, the pre-call EXECUTING event is written **before** the external call — the only
thing making a lost response recoverable — and a duplicate replays the recorded outcome. A lost
response is never retried blindly. Compensation is a first-class audited run, never a rollback; a
failed compensation stays COMPENSATING. A credential echoed back by an executor is redacted from the
stored outcome and the returned result identically, so a replay stays byte-for-byte identical.

**Over MCP an agent cannot approve itself.** Discovery is scope- and mode-filtered; an invisible tool
returns the SAME error as an unknown one. `approval`, `subject`, `mode`, `scopes`, `lease` are absent
from the schema, not merely rejected. No tool grants an approval. Annotations are advisory;
enforcement is server-side. Envelope status: proposed | pending | executed | refused, reason CODES only.

**Harness**: replays an adversarial dataset against the real components and reports decision
precision, escalation rate and duplicate side effects — counted from the external system's own call
log, never from what the framework believes it did.

## Governed local application layer (Plan 31 — features 1–7)

`services/` is the transport-neutral application boundary. Project, rule, governance, quality,
semantic, agent, and execution operations receive a `RequestContext` and require an exact scope from
`NAMED_SCOPES`. `LocalIdentity` derives its subject locally and uses a static authority set; request
payloads cannot grant scopes and `certification_override` is excluded. DTOs are serialized field by
field. The `DatasetRecord` registry has SQLite and Delta stores; `skifer index`, `lineage`, and
`dictionary` provide Spark-free cross-pipeline upstream/downstream/impact metadata. Publication
metadata and incident hooks are **non-blocking**.

Pipeline governance uses the ordered `public|internal|confidential|restricted|pii` taxonomy with
lineage propagation, structured owners (`team`, `steward`, `domain`, `contact`), and contract
`status`, `reviewers`, effective dates, `sla`, and `security`. Canonicalization v2 hashes `sla` and
`security`, but excludes status, reviewers, and effective dates. ODCS 3.1 import and
`diff_contracts()` breaking-change detection are implemented. Quarantine opens critical incidents;
recovery resolves them; routing includes the dataset owner and downstream owners. Alert payloads
for incidents contain no data values. Filter values named in `EvidencePolicy.sensitive_columns`
(the `pii`/`restricted` surface) remain redacted even when ordinary values are disclosed.

`ExecutionService` keeps one lazy session and **one active job per project**; `job_id == run_id`, and
`ResultView` is bounded and JSON-native. The optional `[api]` FastAPI surface maps routes 1:1 to
services on loopback. Every business route declares one named scope, and `api/routes/` must never
import the engine. CLI surfaces: `skifer api serve|openapi`, incident management, and the Spark-free
`skifer audit` coverage report.

**Active plans:**

| Plan | Branch | Status |
|---|---|---|
| [Library development scenario (Plan 31)](docs/roadmap/31_lib_development_scenario.md) | `feat/plan31-f6-api`, stacked on f1..f5/f7 | **Implemented.** All 7 features are complete. |

## Key files
| File | Purpose |
|---|---|
| `config.yaml` | Runtime env config (catalog names, env priority, sandbox, default `params`) |
| `semantic_catalog.yaml` | Index of semantic models (auto-updated by SemanticBuilder) |
| `CHANGELOG.md` | Always update `[Unreleased]` section after every change |
| `docs/roadmap/*_plan.md` | Development plans — source of truth before implementing |
| `.skifer_user` | Auto-generated user suffix cache — add to `.gitignore` |

## Multi-agent workflow — your role (Codex = Dev / Build)

This repo uses a 3-agent workflow (see `docs/roadmap/20_multi_agent_workflow_plan.md`):
**Claude** = architecture/plan & triage · **you (Codex)** = development · **Antigravity** (`agy`, sur compte Google) = review.
Agents do not call each other — they coordinate through shared context: **GBrain** + git +
the plan doc. The supervisor triggers each phase transition manually.

**Current product focus.** The product is Spark/Databricks-only (Plan 26): Databricks Lakehouse in
production, Databricks Connect or local PySpark/Delta in development. Do not reintroduce non-Spark
backends unless the user explicitly reopens that direction.

**GBrain (shared semantic memory).** This repo is registered as the isolated
`skifer` source in the local shared brain. Prefer the `gbrain` CLI over
plain grep for semantic/documentation lookups:
- `gbrain search "<terms>"` — find code/notes by meaning when you don't know the exact name
- `gbrain code-def <symbol>` / `gbrain code-refs <symbol>` — use once a code-aware sync pack is active
Read `plan:<task>` (Claude's decisions) before implementing.

**Your handoff contract before review (Definition of Done — objective, not vibes):**
1. branch committed (one plan point = one commit),
2. `pytest tests/ -x` green (no `src/` change without tests — see mandatory workflow above),
3. `CHANGELOG.md` `[Unreleased]` updated,
4. write a `dev-handoff:<task>` GBrain page (`echo "<note>" | gbrain put dev-handoff:<task>`)
   with: scope delivered, files touched, **deviations from the plan + why**, tests added,
   self-flagged risks. Then tell the supervisor "ready for review".

**gstack skills** (optional, structured procedures) live in
`~/.claude/skills/gstack/.agents/skills/` — read a `SKILL.md` if you want its workflow.
