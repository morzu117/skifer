# CLAUDE.md — Skifer

## Project

Declarative data engineering framework for Spark/Databricks lakehouse pipelines (Bronze → Silver → Gold). The product is Spark/Databricks-only (Plan 26): Databricks in production, local PySpark/Delta for development. `SparkBackend` is the single runtime backend; there is no multi-backend abstraction.  
Core design principle: **What** (YAML schema file or dict) is strictly decoupled from **How** (Python rules in `RuleRegistry`).

## Package structure

```
src/skifer/
  core/
    core.py            # SkiferEngine — main entry point (run_process_to_table, etc.)
    backend.py         # Internal runtime boundary + VALID_SOURCE_TYPES (Spark-first)
    config.py          # ConfigurationManager — reads config.yaml, detects LOCAL/DEV/QA/PROD
    environment.py     # Env helpers: is_running_as_job(), get_clean_username(), get_workspace_client()
    schema_loader.py   # load_schema(), parse_schema() — YAML → normalized dict
    registry.py        # RuleRegistry — @register_rule(kind=...) / @register_loader() decorators
    rule_analyzer.py   # RuleAnalyzer — AST static analysis of rules (reads/writes, warnings)
    rule_planner.py    # RulePlanner — groups rules into stages, topological sort, cycle detection
    rule_executor.py   # RuleExecutor — fuses projection/aggregation stages (single select/groupBy)
    sql_compiler.py    # compile_select / compile_materialized_view_ddl — YAML → SQL (pure Python)
    sandbox.py         # SandboxResolver — transparent sandbox schema/table resolution
    catalog_inspector.py # CatalogInspector — catalog/schema/table introspection + suggestions
    loaders.py         # Built-in Delta table loaders
    writer.py          # Write helpers used by SparkBackend
    spark_backend.py   # SparkBackend — Databricks / local PySpark (single runtime backend)
    constants.py       # VALID_SOURCE_TYPES and shared constants
  sinks/
    jdbc.py            # JDBCSink — postgres/jdbc sink: block (run_process_to_table only)
  observability/
    checks.py          # DataContract checks (Null, Unique, Type, Freshness, Volume, SchemaDrift…)
    monitor.py         # DataMonitor + MonitorReport — pass monitor= to SkiferEngine
    contracts.py       # ContractExtractor — derives checks from schema quality_checks
    alerts.py          # AlertDispatcher
    history.py         # SqliteHistoryStore / DeltaHistoryStore
    reporter.py        # MonitorReporter
    certification.py   # ContractDefinition + canonicalize_contract() — canonical contract identity (Plan 29)
    certification_store.py # Certification/RunEvent + Sqlite/Delta certification stores (Plan 29)
    publication.py     # PublicationCoordinator — stage → validate → promote/quarantine (Plan 29)
    quarantine.py      # quarantine_staging() + row-level violation tagging (Plan 29)
    odcs.py            # ODCS 3.1 export of a ContractDefinition (Plan 29)
    uc_mirror.py       # Non-blocking Unity Catalog tag mirror for certifications (Plan 29)
    metadata_store.py  # DatasetRecord registry; SQLite + Delta persistence (Plan 31)
    metadata_index.py  # Spark-free pipeline indexing and cross-pipeline lineage (Plan 31)
    incidents.py       # Incident state machine, recovery, owner/downstream routing (Plan 31)
    audit.py           # Spark-free governance coverage audit (Plan 31)
  lineage/
    tracker.py         # LineageTracker/LineageGraph — static column-level lineage (no Spark)
    dictionary.py      # DataDictionary + FieldEntry
    renderer.py        # LineageRenderer — Mermaid/JSON export
    classification.py  # public→pii classification propagation over lineage (Plan 31)
  semantic/
    builder.py         # SemanticBuilder — LLM-based YAML model generation
    semantic.py        # SemanticEngine — catalog-first, lazy-load, query/create_view
    validator.py       # SemanticValidator — validates YAML model structure
    access_policy.py   # Pure certification policy: evaluate(), ConsumerContext, overrides (Plan 29)
    dependencies.py    # resolve_dependencies() — datasets a semantic model reads (Plan 29)
    output_projection.py # OutputProjector — predicts final schema without Spark (Plan 29)
    draft_builder.py   # SemanticDraftBuilder — deterministic managed drafts in .drafts/ (Plan 29)
    sync.py            # SemanticSynchronizer — three-way merge, SyncReport, conflicts (Plan 29)
    persistence.py     # write_yaml_atomic() + build_catalog_entry() — shared by CLI and builder (Plan 29)
    extractor.py       # NotebookExtractor + RuleInspector
    glossary.py        # GlossaryReader (JSON/YAML/TXT/PDF/PPTX)
    llm_provider.py    # LLMProvider ABC + get_llm_provider() factory
  agentic/
    agent.py           # GenBIAgent — 2-step LLM pipeline (model selection → SemanticQuery)
    resolver.py        # QueryResolver — deterministic SQL builder, zero LLM, no hallucinated SQL
    orchestrator.py    # Multi-agent orchestration + OrchestratorExporter
    hub.py             # AgenticHub — entry point for the `skifer hub` CLI
    builder_agent.py   # BuilderAgent — generates pipeline YAML schemas
    lineage_agent.py   # LineageAgent | quality_agent.py — QualityAgent | dictionary_agent.py — DictionaryAgent
    models.py          # AgentResponse, FormattedResult, ResponseFormat
    history.py         # SessionHistory + HistoryEntry
    exporter.py        # HistoryExporter → PDF via fpdf2
    user_profile.py    # UserProfile
  mcp/
    server.py          # Read-only MCP server — thin handlers, no direct engine access (Plan 29)
    resources.py       # Governed resources, discovery filtered by scopes (Plan 29)
    tools.py           # query_semantic_model — closed JSON Schema, server-side enforcement (Plan 29)
    auth.py            # stdio static identity / HTTP injected bearer verifier (Plan 29)
    config.py          # Transport config; a non-loopback bind is a startup error (Plan 29)
    runtime.py         # Frozen health constant, no business data (Plan 29)
  adaptive/
    models.py          # SemanticUsageEvent + OptimizationProposal, versioned fingerprint (Plan 29)
    store.py           # Sqlite/Delta usage event stores — append-only (Plan 29)
    aggregator.py      # PatternAggregator — strict partitions, injected clock (Plan 29)
    recommender.py     # Static versioned rule registry, explainable thresholds (Plan 29)
    generator.py       # ProposalGenerator — staged, validated, content-derived ids (Plan 29)
    workflow.py        # AdaptiveWorkflow — human accept/reject, O_EXCL non-destruction (Plan 29)
    evaluator.py       # OutcomeEvaluator — improved/regressed/inconclusive, review only (Plan 29)
  capabilities/
    models.py          # CapabilityDefinition/Summary + closed enums (Plan 29)
    validator.py       # Closed schema, bounded depth, no importable names (Plan 29)
    registry.py        # Catalog-first lazy capability catalog (Plan 29)
    executors.py       # Explicit executor registry — no dynamic resolution (Plan 29)
    invoker.py         # Scope + argument boundary for read capabilities (Plan 29)
    preconditions.py   # ALLOW/DENY/UNKNOWN, evaluator-computed state hash (Plan 29)
    autonomy.py        # shadow/supervised/guarded + approval binding (Plan 29)
    credentials.py     # Injected JIT provider, never-serialised lease (Plan 29)
    executor.py        # GovernedExecutor — idempotent writes + compensation (Plan 29)
    history.py         # Append-only capability state journal (Plan 29)
    harness.py         # Adversarial scenario runner + metrics (Plan 29)
  services/
    context.py         # RequestContext, named scopes, limits, transport-neutral errors (Plan 31)
    serialization.py   # Allowlisted JSON-native serialization
    project.py         # Project, pipeline, metadata index and coverage-audit application service
    rules.py           # Rule discovery, snippets and guarded persistence
    governance.py      # Contracts, certification, registry lineage/dictionary/impact
    quality.py         # Quality history and incident operations
    semantic.py        # Semantic query/catalog and managed-draft operations
    agents.py          # Agent façades with allowlisted results
    identity.py        # LocalIdentity; authority never comes from a request
    execution.py       # Session, async jobs and bounded ResultView
    container.py       # Spark-lazy local service composition root
  api/
    app.py             # Lazy FastAPI factory; loopback local API over services/
    security.py        # One named scope dependency per business route
    errors.py          # Stable service-error to HTTP mapping
    routes/            # Thin 1:1 adapters; no engine imports
  serving/
    chat_model.py      # SkiferChatModel — mlflow.pyfunc.ChatModel wrapping AgenticHub
    _response_serializer.py  # hub_response_to_text() — HubResponse → str (shared CLI + serving)
  cli.py               # `skifer` CLI, including Plan 31 index/lineage/dictionary/incidents/api/audit
  spark_factory.py     # get_spark_session() — auto-detects: Databricks notebook > Connect v2 > local[*]
  utils.py             # safe_columns and other helpers
  __init__.py          # Public API: SkiferEngine, RuleRegistry, ConfigurationManager,
                       #             load_schema, parse_schema, safe_columns
tests/                 # pytest, one file per module (tests/fakes/ for fake backends)
```

## Commands

```bash
pytest                             # run full test suite
pytest tests/test_core.py          # single file
ruff check src/                    # lint
pip install -e ".[dev]"            # dev mode
pip install -e ".[llm-anthropic]"  # with LLM support (also: llm-openai, llm-google, semantic-full)
pip install -e ".[serving]"        # MLflow + OpenAI SDK for Databricks Model Serving deployment
pip install -e ".[api]"            # FastAPI + Uvicorn for the loopback local API
pip install -e ".[docs]"           # mkdocs + material theme
mkdocs build --strict              # build the site; must stay warning-free
uv run --extra api --extra spark --extra dev pytest  # API + Spark + dev test environment
```

## Mandatory workflow for every change to `src/`

For **every** modification to `src/skifer/`:

1. Modify the source in `src/skifer/`
2. Write or update the corresponding test in `tests/`
3. Add an entry to `CHANGELOG.md` under `## [Unreleased]`

**One plan point = one separate commit.** When implementing a development plan, each numbered point gets its own commit referencing the plan (e.g. `feat(plan17-1.2): ...`). Never bundle two points in one commit.

**Never bump the version number** — the user manages versioning. Do not modify `pyproject.toml` version.

## Release checklist (every version bump)

A version bump (done by the user) requires all 5 points — they keep `CLAUDE.md`, `AGENTS.md` and the mkdocs site in sync:

1. No new feature without associated, passing tests
2. Existing tests green — or updated with a documented reason if broken
3. `CHANGELOG.md` up to date
4. `CLAUDE.md` / `AGENTS.md` updated
5. mkdocs documentation (`docs/`) updated

## Key conventions

### Local mode vs Databricks
- `catalog: null` in `config.yaml` → skips Unity Catalog, starts `local[*]` PySpark + Delta + Derby.
- FQN is 2-part (`schema.table`) locally, 3-part (`catalog.schema.table`) on Databricks.
- Session detection order: active Databricks notebook → Databricks Connect v2 (`.env`) → local PySpark.

### YAML schema patterns
```yaml
data_product:                              # optional (Plan 29) — opt-in to certified publication
  id: sales.fact_orders                    # required — stable product identifier
  version: 1.2.0                           # required — semantic version X.Y.Z
  owner: data-platform                     # optional
  description: "Commandes consolidées"     # optional

contract:                                  # optional (Plan 29) — explicit output contract
  grain: [order_id]
  output:
    order_id: {logical_type: string, required: true, unique: true}
    amount:   {logical_type: decimal, classification: internal}

semantic:                                  # optional (Plan 29) — seed for the semantic model
  model_key: sales.orders                  # required — generated alongside the pipeline, not after it
  entity: order                            # optional
  default_time_dimension: order_date       # optional
  dimensions: [region, status]             # optional

partials:                                  # nested YAML sub-transformations (Plan 25)
  - alias: dly                             # output exposed under this alias (join/rules)
    path: _partials/base.yaml              # relative to THIS YAML's dir; inherits parent params
                                           # child never writes a final table; alias unique vs tables
                                           # cycles fail fast; materialization via run param below

tables:
  - name: raw_orders                       # optionally load from external source:
    alias: orders
    source:
      type: csv                            # csv | parquet | json | avro | orc | delta | text
      path: "{{ base_path }}/orders/*.csv" # {{ param }} injection supported
      options:
        header: "true"
        inferSchema: "true"

  - name: "{{ catalog }}.silver.orders"   # {{ key }} param injection (pre-parse, no Jinja2 dep)
    alias: ord
    filter:
      - "region:equals:EMEA"              # string form: "col:operator[:value]"
      - "status:in:ACTIVE,PENDING"        # comma-separated for in/not_in
      - "customer_id:is_not_null"         # no-value operators
    filter_groups:                        # OR-of-ANDs (groups are OR-combined)
      - ["region:equals:EMEA", "status:is_not_null"]
      - ["region:equals:APAC"]
    quality_checks:
      drop_duplicates_on: [order_id]
      drop_nulls_in: [amount, customer_id]
    dev_limit: 5000                       # ignored in job/prod mode

join:
  - table_from: [ord, customer_id]        # compact: [alias, key]
    table_to: [cust, id]
    type: left                            # left (defaut) | right | inner | full | cross | left_anti | left_semi
                                          # aliases acceptes : full outer/full_outer/outer -> full,
                                          # anti/left anti -> left_anti, semi/left semi -> left_semi
                                          # left_anti et left_semi : Spark DataFrame only for now

business_rules:
  - flag_high_value

select_final:
  - [amount, amount_eur, [cast:double, round:2]]  # [source, alias, [ops...]]
  - [literal:ERP, source_system]                  # constant column shorthand
  - source: status                                # chained when/else (dict form)
    target: status_label
    ops:
      - when: "equals:DONE"
        then: "lit:Paid"
      - else: "lit:Unknown"

keep_all_columns: true   # mutually exclusive with select_final
add_columns:
  - [amount, amount_rounded, [round:2]]   # applied BEFORE aggregate (can build a group key)

aggregate:                              # optional (Plan 28) — exclusive with select_final/keep_all_columns
  group_by: [country, order_month]
  measures:
    - [amount, total_amount, sum]       # [source, target, func] — or {source, target, func}
    - [order_id, nb_orders, count_distinct]
  having:                               # optional — filter grammar on measure aliases
    - "total_amount:greater_than:1000"

dev_limit: 10000         # schema-level (table-level overrides this)

sink:                    # optional — default is Delta table write
  type: postgres         # delta | postgres | jdbc (needs POSTGRES_HOST/DB/USER/PASSWORD env vars)
  schema: analytics      # optional overrides for target schema/table
  table: fact_orders

materialization:              # optional (Plan 27) — shorthand: materialization: streaming_table
  type: streaming_table       # table (default, batch overwrite) | streaming_table (incremental)
  trigger: available_now      # default; or "interval:30 seconds" (permanent query, blocks)
  checkpoint: auto            # default; local → {warehouse}/_checkpoints/... ;
                              # Databricks → requires params.checkpoint_base ; or explicit path
  write_mode: upsert          # append (default) | upsert (CDC type 1: foreachBatch + MERGE INTO)
  keys: [order_id]            # required iff write_mode: upsert — merge key
                              # streaming: true required on exactly ONE table (bijection strict)

materialization:              # optional (Plan 28) — shorthand: materialization: materialized_view
  type: materialized_view     # compiled to SQL, created as a Unity Catalog MV
  schedule: "EVERY 6 HOURS"   # optional; or "CRON '0 0 6 * * ?' AT TIME ZONE 'Europe/Paris'"
  comment: "CA par pays"      # optional
  cluster_by: [country]       # optional — mutually exclusive with partition_by
  refresh: auto               # auto (default) | never (leave it to the SCHEDULE)
```

### Loading a schema
```python
from skifer import load_schema, parse_schema

schema = load_schema("schemas/gold/fact_orders.yaml", params=engine.default_params)
schema = load_schema("path/to/file.yaml", params={**engine.default_params, "region": "FR"})
schema = parse_schema(yaml_string, params={"catalog": engine.db})

engine.default_params  # → {"catalog": "my_dev_catalog", "env": "DEV", **env_params}
```

An environment may declare an optional `params:` mapping in `config.yaml`; those keys are merged into `engine.default_params` (so minimal notebooks resolve `{{ key }}` placeholders without passing storage paths). Priority: explicit `run_from_yaml(params=…)` > config environment `params` > built-in `catalog`/`env` (built-ins win on collision). A non-mapping `params:` fails fast. Environment matching against `config.yaml` keys is case-insensitive (`ExecutionContext.env_config()`).

### Nested partials (Plan 25)
A top-level `partials:` block runs nested YAML schemas as sub-transformations and exposes each output under its `alias` (usable in `join`/`business_rules`) — so a join can depend on a column produced by a business rule without an intermediate table. Child schemas inherit the parent's params, resolve relative paths from the parent YAML dir, never write a final table, and are cycle-checked at load. Materialization is a **run-global** param, not YAML: `run_from_yaml(..., params={..., "intermediate_mode": "inline|temp_view|table"})` (default `inline`). `temp_view`/`table` name the artifact after the partial `alias` (sandbox-suffixed); the parent always keeps using the DataFrame directly. See `docs/core.md`.

### Streaming tables (Plan 27)
A table with `streaming: true` is read via `readStream` (catalog table, or `source:` restricted to `delta`/`text`); the schema must declare `materialization: streaming_table` (strict bijection) and is written incrementally (append, or CDC type 1 upsert via `write_mode: upsert` + `keys:`). Constraints enforced at load: exactly one streaming table per schema, it must be the join base, join types `inner`/`left` only; incompatible with `dev_limit`, `preprocess.qualify`, `drop_duplicates_on` (→ use upsert), loaders, JDBC sinks, partials children, `aggregation` rules (rejected at run — aggregate downstream in batch). Checkpoints are sandbox-suffixed in interactive mode; `engine.full_refresh(layer, table)` purges checkpoint + drops the table atomically. Flow composition uses **sub-layers** (each stage = its own streaming table + checkpoint, the next stage reads it with `streaming: true` — chaining streaming tables is NOT stream-stream). See `docs/core.md`.

### Agrégations déclaratives (Plan 28)
Le bloc `aggregate: {group_by, measures, having}` remplace l'écriture d'une règle Python `kind="aggregation"` pour un GROUP BY ordinaire — il sert **aussi en batch**, indépendamment des MV. Fonctions dans `AGGREGATE_FUNCTIONS` (`core/op_catalog.py`, source de vérité unique) : `sum`, `avg` (`mean`/`average`), `min`, `max`, `count`, `count_distinct`, `sum_distinct`, `approx_count_distinct`, `stddev`, `variance`, `first`, `last` ; seul `count` accepte `source: "*"`. Exclusif avec `select_final`/`keep_all_columns`, refusé en streaming (watermarks requis), `having:` validé au load contre `group_by ∪ alias de mesures`. Primitives backend : `agg_expr` / `group_by_agg`.

### Materialized views (Plan 28)
`materialization: materialized_view` **court-circuite tout le pipeline DataFrame** : le schéma est compilé en SQL (`core/sql_compiler.py` → `compile_select`) puis émis en `CREATE [OR REPLACE] MATERIALIZED VIEW`. Le DDL passe par un **SQL warehouse Pro/Serverless** (Statement Execution API, `params.sql_warehouse_id`) — un cluster all-purpose ou Databricks Connect le refuse. Sans warehouse : fail-fast en job/prod **avant compilation**, sinon génération d'un `.sql` dans `{sql_output_dir}` (défaut `generated_sql/`) + warning « NOT CREATED » (monitor sauté). En local, le SELECT compilé est exécuté et écrit en table Delta (ce qui vérifie le SQL à chaque run). **Dérive de définition** : hash SHA-256 du SELECT + des options définissantes (`schedule`, `comment`, `cluster_by`, `partition_by` — pas `refresh`) stocké dans `TBLPROPERTIES ('skifer.definition_hash')` → absente = CREATE, hash identique = REFRESH (sauté si `refresh: never`), hash différent/illisible = CREATE OR REPLACE. Refus au load : `business_rules` Python (matérialiser en amont), `partials`, sources fichier, loaders, `dev_limit`, sinks JDBC, `streaming: true`, `drop_duplicates_on`, `preprocess.qualify`, `keep_all_columns` + join/>1 table. `run_process_and_split` / `run_union_sources_to_table` refusent les MV. `engine.full_refresh(layer, table, materialization="materialized_view")` droppe la vue. SDK : extra `[databricks]`. Voir `docs/core.md`.

### Projection sémantique depuis le pipeline (Plan 29 — feature 0)

Un pipeline qui déclare `data_product:` / `contract:` / `semantic:` est la **source déterministe**
du modèle sémantique : celui-ci est préparé dans la même PR que la table, sans Spark et sans LLM.

```text
pipeline YAML → OutputProjector → SemanticDraftBuilder → semantic_models/.drafts/<model>.yaml
                                        └→ SemanticSynchronizer → --promote → semantic_models/<model>.yaml
```

**Drafts gérés.** Fonction pure de l'entrée (reproductible byte-for-byte), écrits atomiquement,
marqués `_generated_by` + `metadata` (contrat source, version, `source_definition_hash`,
`generated_fields`, `grain`). Le builder refuse d'écraser un fichier sans ce marqueur. Conservateur
par construction : dimensions uniquement si déclarées ou sûres, type non mappable → `needs_curation`
plutôt que coercion en `string`, agrégats supportés par le pipeline mais pas par la couche sémantique
(`stddev`, `variance`, `sum_distinct`, `approx_count_distinct`, `first`, `last`) → listés dans
`metadata.unmapped_measures` au lieu de faire échouer le draft.

**Synchronisation.** `SemanticSynchronizer` fait un **merge trois voies contre la dernière
génération** — pas un diff entre deux fichiers. `SyncReport` expose `.changes` / `.conflicts` /
`.suggestions` / `.safe_to_apply`. **Toute ambiguïté produit un rapport non vide et n'écrit rien**
(une suggestion de renommage est reportée, jamais appliquée d'office).

**CLI / CI** — les codes de sortie sont le contrat :

```bash
skifer semantic sync PIPELINE --check         # 0 à jour | 2 dérive | 3 conflit — n'écrit jamais rien
skifer semantic sync PIPELINE --write-draft   # applique si le rapport est propre
skifer semantic sync PIPELINE --promote       # draft → modèle curé + catalogue
skifer semantic validate PIPELINE MODEL       # cross-validation modèle ↔ projection
```

`0` à jour/valide/promu · `1` erreur technique ou de validation · `2` dérive · `3` conflit.
`--promote` est un no-op si le modèle curé est déjà à jour, et **refuse** plutôt que d'écrire dès
qu'une promotion ferait disparaître du contenu écrit par un humain.

**Enrichissement LLM contraint.** `SemanticBuilder.build_from_projection()` traite la sortie du LLM
comme une **entrée non fiable** : seuls `description` et `synonyms` sont acceptés, plus des métriques
bornées à `COUNT(*)` ou à une sortie projetée. Tout le reste est rejeté — nouvelle dimension,
rename/retype d'un champ géré, clé inconnue, type d'agrégat non supporté. Les noms de dimensions et
de métriques doivent matcher `^[A-Za-z_][A-Za-z0-9_]*$` : `QueryResolver` les interpole directement
comme alias SQL, donc un nom non conforme est un vecteur d'injection. `SemanticValidator` applique la
même règle, ce qui couvre aussi le chemin `build()` historique et `--promote`.

`SemanticEngine._load_catalog()` ne précharge **jamais** les drafts — `.drafts/` reste invisible au
démarrage.

### Publication certifiée & gate sémantique (Plan 29 — features 0 partielle, 1, 2, 3)

**Publication certifiée (opt-in).** Un schéma qui déclare `data_product:` route `run_process_to_table()`
à travers `PublicationCoordinator` : stage dans `_skifer_staging` → checks du monitor → persistance des
résultats → **promotion** ou **quarantaine** (snapshot dans `_skifer_quarantine`, lignes fautives taguées
`_violations`/`_run_id`/`_contract_version`). Un schéma **sans** `data_product` garde le chemin d'écriture
historique inchangé. Le moteur doit alors recevoir `certification_store=` **et** `monitor=`, sinon fail-fast.
Refus explicites : streaming, sinks JDBC, materialized views. Reprise après crash : `coordinator.resume(run, definition)`.

**Registre de certification.** `SqliteCertificationStore` (local, `.skifer_certification.db`) ou
`DeltaCertificationStore` (Databricks). `get_certification(dataset, consumer_class)` renvoie un `Certification`
dont `checks_passed` est calculé depuis les check results critiques enregistrés. Les événements de run sont
indexés sur le **`target_fqn` physique**, pas sur le `data_product_id` logique. Le `run_id` persisté est
celui frappé par le pipeline (`run_process_to_table` / `run_from_yaml`) : **une identité d'audit de bout en
bout**, frappée sur le chemin métier et donc identique que le tracing soit actif ou non.

**Gate de certification sémantique.** Piloté par environnement dans `config.yaml` (à la racine de
l'environnement, comme `allow_raw_sql`) :

```yaml
environments:
  prod:
    semantic_certification_policy: enforce   # off (défaut) | warn | enforce | supervised
    semantic_certification_max_age: 24h      # optionnel — au-delà → EXPIRED
    semantic_consumer_class: agent_read      # optionnel — défaut "dashboard"
```

`SemanticEngine.query()`/`create_view()` et `GenBIAgent` appliquent un **double gate** : préflight avant
compilation (zéro SQL généré sur un DENY connu) puis recheck avant exécution (protection contre les courses).
Décisions : `ALLOW` / `WARN` (compteur `certification_warning_count`) / `DENY` / `REQUIRE_HUMAN`. Raisons :
`MISSING`, `FAILED_CHECK`, `EXPIRED`, `OVERRIDDEN`. **Fail-closed partout** — store absent, dépendance non
résolue, datetime naïf → refus, jamais un ALLOW silencieux. Migration recommandée : `off` → `warn` (observer
`certification_warning_count`) → `enforce`.

**Override break-glass.** `CertificationOverride(reason, actor, trace_id, expires_at)` exige le scope
`certification_override` dans `ConsumerContext.scopes`, `actor == consumer_id`, un `expires_at` timezone-aware
sous 4h ; chaque usage est journalisé (WARNING). Le serving ne peuple **jamais** `scopes` depuis les `params`
client — un client ne peut pas s'auto-attribuer le scope.

### Graphe de domaine & jointures sémantiques (Plan 29 — feature 6)

Un modèle peut déclarer `grain:`, `entities:` et `relationships:` (voir `docs/yaml_spec.md`).
Un modèle mono-table historique sans ces blocs reste valide et produit **le même SQL qu'avant**.

```text
semantic YAMLs lazy ──▶ DomainGraph ──▶ SemanticPlanner ──▶ SemanticPlan ──▶ QueryResolver SQL
SemanticQuery (noms uniquement) ──────────┘
```

`SemanticPlanner` résout des **noms** en chemin de jointure **avant** qu'aucun SQL n'existe : le LLM
n'émet jamais de condition de jointure, d'alias ni de borne de date. Un nom présent dans le modèle
racine y est résolu (rétrocompat stricte) ; un nom présent dans plusieurs modèles non-racine doit être
qualifié `modele.nom`. Alias positionnels, donc stables d'un run à l'autre et sous tout `PYTHONHASHSEED`.
Refus explicites : aucun chemin, plusieurs chemins minimaux (les deux sont nommés, jamais arbitrés),
`many_to_many` et `unknown` — chacun avec **sa cause propre**, un `many_to_many` déclaré n'étant pas
rapporté comme non déclaré.

**Sécurité de grain (fanout).** Une relation duplique les lignes du côté **depuis lequel** elle est
traversée quand ce côté est le côté « one » — cela dépend du **sens de parcours**, pas de la seule
cardinalité déclarée. Le planner part du modèle porteur de la métrique et refuse la première arête qui
duplique : métrique en amont d'un `one_to_many` (message imposé `Unsafe fanout: metric grain 'order'
crosses one_to_many relationship 'order_lines'.`), métrique atteinte **au-delà** d'un fanout, métrique
sur le côté « one » d'un `many_to_one`. Restent autorisés : une métrique portée par le côté « many »
direct (calculée à son propre grain) et toute requête sans métrique.

**Additivité.** `additive` (défaut, comportement historique) · `semi_additive` (exige
`non_additive_dimensions`, chacune devant être épinglée par `group_by` ou un filtre `eq`) ·
`non_additive` (refusée dès qu'il y a un join).

**Calendriers versionnés.** Un trimestre fiscal se résout par **définition déclarée**, jamais en Python
ni par le LLM — qui n'émet qu'un *nom* de période. `calendars/<key>.yaml` porte `key`/`version`/`periods`
(bornes ISO) ; le modèle s'y abonne par `calendar: <key>`. Chargement paresseux et mis en cache : aucun
calendrier n'est ouvert au démarrage. `period` combiné à `date_from`/`date_to` est refusé pour ambiguïté ;
période inconnue, modèle sans calendrier, `period` sans planner sont des erreurs explicites. Toute borne
de date doit parser en `YYYY-MM-DD` avant d'atteindre le SQL.

### Réponses sémantiques avec preuves (Plan 29 — feature 4)

`SemanticEngine.query()` retourne toujours un DataFrame ; `query_with_evidence()` retourne un
`SemanticResult` (DataFrame + `SemanticEvidence`). **Une seule source d'exécution** : `query()` délègue
à `query_with_evidence()` — deux chemins parallèles divergeraient, et c'est la couche où un écart entre
ce qui est exécuté et ce qui est prouvé serait inacceptable.

La preuve est **autonome** (aucun DataFrame, aucune session Spark vivante — lisible après la session) et
sérialisée par une **allowlist champ par champ**, jamais `asdict` : un champ ajouté plus tard reste
absent tant qu'il n'est pas explicitement admis. Elle porte le hash `sha256:v1:` du SQL **réellement
exécuté**, les hashs de définition des métriques, les colonnes sources issues d'un sous-graphe de
lineage restreint aux membres sélectionnés (avec `lineage_status` explicite), un snapshot figé de
certification par dataset, la décision de policy et le statut/durée d'exécution (durée sur horloge
monotone, jamais une soustraction d'horloges murales).

**Normalisation du SQL avant hachage** : les espaces sont réduits **hors des littéraux de chaîne
uniquement**. Un `\s+` global rendrait `x = 'a  b'` et `x = 'a b'` identiques — deux requêtes attestées
comme la même, le seul défaut qu'une couche de preuve ne peut pas se permettre.

**Divulgation fail-closed.** `EvidencePolicy` (fonction pure) gouverne `include_sql` et
`include_filter_values` ; par défaut ni l'un ni l'autre. Demander un SQL retenu lève
`EvidenceRedactionError` plutôt que de renvoyer `None` — un consommateur ne distinguerait pas « pas de
SQL » de « SQL refusé ». Sur échec, `SemanticExecutionError` porte une preuve partielle marquée
`execution_status="failed"` ne contenant que le **nom de classe** de l'erreur, jamais le message backend
qui cite très souvent les valeurs fautives.

**Ligne de provenance** (une seule, y compris en serving) : tous les datasets nommés, le maillon le plus
faible l'emportant sur chaque axe — une source non certifiée bascule la ligne hors de « certifié », la
date affichée est la plus ancienne certification et la fraîcheur la plus périmée, une décision non-ALLOW
est annoncée. Le canal texte applique la même redaction que le JSON.

**Serving** : `choices` reste inchangé à l'octet près ; la preuve est ajoutée à côté dans un bloc
versionné `skifer`. mlflow étant un extra optionnel non installé, l'acceptation de ce champ
top-level par `mlflow.pyfunc.ChatModel` **n'est pas vérifiée empiriquement** (note dans le code).

**Couverture de certification multi-modèle.** Le préflight ne voit que le modèle racine (le chemin de
jointure n'existe pas encore) ; le **recheck évalue chaque dataset du plan compilé**. Sans cela, une
requête multi-modèle obtenait un ALLOW décidé sur la seule table racine puis lisait les datasets joints
sans qu'aucun soit certifié. En mode `off`, aucune dépendance n'est résolue.

### Tracing runtime (Plan 29 — feature 5)

Désactivé par défaut, **aucune dépendance obligatoire** ajoutée au cœur. Le cœur n'importe jamais
`mlflow` ni `opentelemetry` au niveau module : les adaptateurs les importent paresseusement, et
`observability/tracing.py` comme `tracing_exporters.py` s'importent dans une installation qui n'a ni
l'un ni l'autre (prouvé par inspection de `sys.modules`). Extra optionnel : `pip install -e ".[tracing]"`.

```yaml
observability:
  tracing:
    exporter: none        # none | otlp | mlflow | dual
    required: false       # true → un échec d'export devient une erreur
    capture_prompts: false
    capture_sql: false
    user_identity: omit   # omit | hmac
    trace_location: null  # destination MLflow / Unity Catalog
```

**La règle qui prime sur tout : le comportement métier ne dépend jamais du tracing.** Un exporter qui
échoue à l'init, à l'ouverture d'un span, à la pose d'un attribut ou à la fermeture n'affecte ni une
valeur de retour, ni une donnée persistée, ni une exception — il journalise une seule fois
(rate-limité). `required: true` est la seule exception, et ce n'est pas le défaut.
**Ne duplique JAMAIS un flux métier selon l'état du tracing** : c'est exactement ce qui, en slice 5.2,
faisait dériver le `run_id` persisté et échouer une publication valide.

`InMemoryTracer` (tests) et `NoOpTracer` (défaut, sans état ni allocation) implémentent les Protocols
`Tracer`/`Span`. Le span courant vit dans un `ContextVar`, jamais dans une globale : isolation prouvée
sur threads et tâches async. Le `trace_id` vient toujours du parent — une trace reste une trace — mais
les IDs de corrélation prennent le contexte le plus interne explicitement posé. Un span n'est jamais
rattaché à un parent **déjà terminé**.

**Redaction.** `TraceAttributePolicy` allowliste les clés d'attributs (`ALLOWED_ATTRIBUTE_KEYS`) et
borne leur nombre (32), la taille des valeurs (256) et les événements (64) par span. Tout le reste est
écarté — `question`, `prompt`, `sql_text`, valeur de filtre — et le rejet ne conserve que la clé et le
type, jamais la valeur. La policy s'applique sur **tous** les chemins qui sortent du processus, y
compris `set_attribute` et les adaptateurs OTLP/MLflow : une redaction qui ne vaudrait qu'en test
laisserait les canaris au vert pendant que la production fuit. Une erreur n'enregistre que le **nom de
classe** de l'exception, jamais son message, qui cite souvent la valeur fautive.

Noms de spans issus d'une taxonomie fixe, jamais construits depuis une valeur variable (cardinalité).
Durées sur horloge monotone. Aucun `count()` Spark déclenché pour tracer.

**Identité utilisateur.** `omit` par défaut. `hmac` lit le secret dans
`SKIFER_TRACING_HMAC_SECRET`, jamais dans `config.yaml`. **Sans secret, l'identité est omise, pas
hachée** — un digest sans clé d'un e-mail ou d'un matricule est réversible par qui devine l'entrée.

**Unity Catalog.** `trace_location` doit préexister : l'exporter ne la crée **jamais**. Absente, il
échoue avec un message disant quoi créer et où. En mode `dual`, si un exporter tombe, l'autre continue.

### Exposition MCP read-only (Plan 29 — feature 7)

Un agent externe découvre et interroge **uniquement** la couche gouvernée. Extra optionnel :
`pip install -e ".[mcp]"` (SDK officiel `mcp>=2.1,<3`) — le cœur s'importe sans lui.

```text
transport MCP (stdio/http) → auth.py → RequestContext
          ↓ handlers minces, aucun accès direct aux moteurs
   AgentReadyDataService   ← LA frontière de sécurité
          ↓ scopes, budgets, gate de certification, preuve
   SemanticEngine.query_with_evidence / catalogue / lineage
```

**`AgentReadyDataService` (`agentic/data_service.py`) est la frontière**, testable sans transport.
Scopes fail-closed via `require_scope` : `models:read`, `contracts:read`, `lineage:read`,
`query:execute`. DTO construits par **allowlist champ par champ** — jamais un dict YAML brut, jamais
`asdict`. Plafonds **durs** dans `ServiceLimits` (100/page, 1000 lignes, 50 filtres, 1024 caractères
par valeur) : un dépassement est une erreur explicite, **jamais un clamp silencieux**. Curseur de
pagination opaque, validé, périmé = refusé — jamais réinterprété comme « recommence au début ».
`query()` ne rend jamais un DataFrame et **aucun scope client n'entre dans `ConsumerContext`**
(l'allowlist est vide) : `certification_override` ne peut pas franchir la frontière MCP.

**Resources** (`mcp/resources.py`) : `skifer://semantic/catalog`, `.../semantic/models/{key}`,
`.../contracts/{id}/{version}`, `.../certification/{dataset}`, `.../lineage/{dataset}/{column}`.
La **découverte est filtrée par scopes** — lister un endpoint et le scope qui l'ouvre est de la
reconnaissance. URI parsées strictement : percent-decoding **avant** validation, `..`/`/`/`\` refusés.
ETag déterministe dans les metadata.

**Tool** (`mcp/tools.py`) : `query_semantic_model` seul, JSON Schema **fermé**
(`additionalProperties: false` à tous les niveaux, enum d'opérateurs close). `mode`/`view_name` sont
**absents** du schéma, pas seulement rejetés. L'enforcement est **serveur** (`set(arguments) - allowed`),
jamais l'annotation read-only, qu'un client peut ignorer. Le schéma annoncé dérive des limites de
l'instance : il ne promet jamais plus que ce que le service accorde.

**Auth** (`mcp/auth.py`) : stdio = identité et scopes **statiques** issus de la config locale, ce que
prétend la requête est ignoré. HTTP = bearer validé par un **vérificateur injecté** — on ne fabrique ni
ne signe de token, il n'y a pas de serveur d'autorisation ici. Audience, issuer, resource et expiry
vérifiés, expiry **sans tolérance** et en UTC timezone-aware. Credentials **par requête**, jamais dans
un singleton : isolation prouvée sous threads. Aucun détail du vérificateur ne franchit la frontière.
Identité de trace `omit` par défaut ; `hmac` **sans secret omet** plutôt que de hacher sans clé.

**CLI** : `skifer mcp serve --transport stdio|http --config <fichier>`. Un bind **non-loopback**
est une **erreur au démarrage**, pas un warning, et HTTP exige une configuration d'auth complète.
Health = constante figée, sans donnée métier. Aucun secret dans les logs ni dans les erreurs de config,
diagnostics YAML compris.

### Adaptive Gold supervisé (Plan 29 — feature 8)

L'usage réel de la couche sémantique alimente des **propositions** d'optimisation Gold/MV.
Rien n'est jamais déployé : le moteur ne touche pas `schemas/`, n'appelle jamais `engine.run_*`,
ne crée aucune MV et n'exécute aucune commande git.

```text
SemanticEvidence ──▶ UsageEventStore ──▶ PatternAggregator ──▶ RecommendationEngine
                                                                      │
                                                       OptimizationProposal (preuves + règle + version)
                                                                      │ ProposalGenerator
                                            .skifer_proposals/<id>/{pipeline.yaml, draft, proposal.json}
                                                                      │ skifer adaptive accept
                                                        schéma possédé par un humain → PR → déploiement
                                                                      │ skifer adaptive evaluate
                                                        improved | regressed | inconclusive
```

**Événements sans donnée sensible.** `usage_event_from_evidence()` ne recopie jamais la preuve :
il la sérialise pour valider l'allowlist puis reconstruit un événement champ par champ. Aucune
question, aucun SQL, aucune valeur de filtre — la forme d'un filtre est `colonne:opérateur`, jamais
`colonne:opérateur:valeur`. Le fingerprint est versionné (`sha256:v1:…`) et insensible à l'ordre non
sémantique : deux requêtes identiques aux permutations près partagent un fingerprint.

**Agrégation déterministe.** Partition **stricte** par environnement, classe de consommateur et hash
de définition de modèle — deux environnements ne se mélangent jamais dans un même agrégat. Les
événements `failed` sont comptés séparément des succès. Horloge injectée et obligatoire, itération
sur clés triées : le même jeu d'événements produit le même résultat sous n'importe quel
`PYTHONHASHSEED`.

**Règles explicables, jamais un modèle.** Registry **statique** et versionné (`frequent_aggregate`,
`repeated_join_path`, `missing_dimension`, `unused_generated_asset`) — pas de plugin arbitraire, pas
de ML en v1. Le score est une composition de seuils nommés : chaque raison cite le seuil, la valeur
observée et le verdict. Une règle qui ne conclut pas produit un `RecommendationRefusal` portant ses
**contre-indications**, pas un silence. Garde-fous : sources certifiées, cardinalités sûres (la
détection de fanout est **partagée** avec `SemanticPlanner` via `find_unsafe_fanout()`, pas
réimplémentée), pas de SQL brut ni de règle Python non compilable.

**Artefacts validés avant d'être montrés.** `ProposalGenerator` écrit dans un répertoire de staging,
fait passer le pipeline par `load_schema` **et** le draft sémantique par `SemanticValidator`, et ne
publie qu'ensuite — une proposition visible est une proposition qui charge. `proposal_id` est dérivé
du contenu (`proposal:v1:<sha256>`), et les chemins sont enregistrés **relatifs** à la racine des
propositions : un chemin absolu épinglerait l'artefact à un checkout, y écrirait le nom d'utilisateur
et ferait diverger deux générations identiques.

**Décision humaine.** `skifer adaptive list|show|diff|accept|reject|evaluate`. `accept` refuse
d'écraser une sortie existante — la garantie est au niveau syscall (`os.O_CREAT | os.O_EXCL`), sans
course entre le test et l'écriture — et revalide les hashes de source : une définition qui a bougé
passe `stale` et n'est pas acceptable. Codes de sortie : `0` succès · `1` erreur technique ·
`2` usage · `3` proposition périmée · `4` conflit d'état ou sortie existante · `5` régression mesurée.

**Évaluation des résultats.** `skifer adaptive evaluate` compare la fenêtre avant livraison à la
fenêtre après, sur la partition exacte dont la proposition est issue — **reconstruite depuis ses
propres événements de preuve, jamais devinée**. Fenêtres semi-ouvertes : aucun événement compté deux
fois. Sous les seuils de données minimales, le résultat est `inconclusive` avec la fenêtre et le
seuil fautifs ; preuves purgées par la rétention → `inconclusive: evidence_unavailable`, pas une
exception. Une régression sur **l'un** des deux axes (percentile de durée, taux d'échec) l'emporte
sur une amélioration de l'autre. Un référentiel à zéro se compare à zéro directement — la formule
multiplicative déclarait « régressées » deux fenêtres identiques à 0 ms, soit exactement les requêtes
qu'une optimisation a rendues trop rapides pour être mesurées. **Une régression ne produit qu'une
demande de revue humaine** : le module ne contient ni rollback, ni drop, ni subprocess, et un test le
vérifie au niveau source — la garantie est que la capacité est absente, pas seulement inutilisée.


### Capacités gouvernées & write-back (Plan 29 — feature 9)

Une capacité permet à un agent de déclencher une action sur un système **externe** — la seule surface
non-read-only du framework. Le LLM choisit un `capability_id` et remplit un schéma d'entrée fermé ;
**il ne décide jamais** de la policy, des préconditions, de l'approbation, des credentials ni de
l'idempotence.

```text
catalogue YAML lazy → CapabilityRegistry → PreconditionEvaluator → AutonomyStateMachine
                                                                   → CredentialBroker (JIT injecté)
                                                                   → GovernedExecutor → historique append-only
```

**Le document ne peut jamais nommer du Python.** `executor` et chaque `rule` de précondition sont des
noms plats résolus contre un registre explicite en processus ; tout ce qui ressemble à un chemin
d'import est refusé, et les modules ne contiennent ni `importlib`, ni `eval`, ni résolution dynamique
— vérifié au niveau source.

**Schéma d'entrée fermé et borné.** `additionalProperties: false` à tous les niveaux, `maxLength` /
`maxItems` / `minimum` / `maximum` obligatoires, refus de `$ref`, `allOf`, `anyOf`, `oneOf`, `not`,
`patternProperties`. La profondeur est bornée (8 niveaux) : une profondeur non bornée fait planter le
validateur au lieu de répondre, et **un document qui fait planter le validateur n'a jamais été validé**.

**Décisions fail-closed.** Une règle retourne un `PreconditionOutcome` typé dont le `reason_code` suit
une grammaire fermée : une phrase, un document récupéré ou une réponse de modèle ne peut pas devenir
une décision — elle devient `UNKNOWN`. Une règle qui lève devient `UNKNOWN` avec le seul nom de classe.
**`UNKNOWN` ne devient jamais `ALLOW`.** Le hash d'état est calculé par l'évaluateur, jamais par la
règle : l'attestation n'est pas falsifiable par l'attesté. Recheck systématique juste avant exécution.

**Autonomie : toujours le côté le plus strict.** Le mode effectif est le plus restrictif entre
l'`approval` déclaré et `capability_autonomy` de l'environnement (`shadow` par défaut) ; une capacité
`irreversible` n'atteint jamais `guarded`. En `shadow`, rien ne s'exécute, aucun credential n'est pris,
et un executor ne tourne que s'il a **déclaré** son support du dry-run à l'enregistrement.

**Une approbation lie une requête exacte.** `ApprovalRecord` porte l'acteur, la raison, une fenêtre de
validité plafonnée à 4h (appliquée) et deux hashes — requête et rapport de préconditions. Changer un
caractère d'un argument l'invalide. Le hash de préconditions couvre les règles, leurs verdicts et
l'état observé, **jamais l'horloge murale** : le rapport est réévalué avant exécution, donc y hacher le
temps rendait toute approbation invalidable — défaut qu'une horloge de test fixe avait masqué.

**Credentials JIT, jamais stockés.** Le provider est **injecté** : Skifer ne fabrique, ne
signe et ne renouvelle rien, et n'est pas un serveur d'autorisation. Le courtier demande exactement les
scopes déclarés, **refuse** un TTL au-dessus du plafond au lieu de le rogner, et vérifie la réponse du
provider — scopes en trop, sujet différent ou durée plus longue sont rejetés. Le secret n'est
accessible que par `reveal()` ; `repr`, `str`, `logging` et `json` le rédigent, et `pickle` **lève**.

**Exécution idempotente, compensation auditée.** Identité = hash (version, entrée normalisée, sujet).
`GovernedExecutor` consulte le journal d'abord, écrit l'événement `EXECUTING` **avant** l'appel externe
— seule chose qui rend une réponse perdue récupérable — et rejoue le résultat enregistré pour un
doublon. Une réponse perdue n'est **jamais** rejouée en aveugle. La compensation est un run auditée à
part entière, jamais un rollback ; une compensation échouée reste `COMPENSATING`. Un credential
renvoyé par un executor est rédigé du résultat stocké **et** du résultat retourné à l'identique, pour
qu'un rejeu reste identique au premier appel.

**Sur MCP, un agent ne peut pas s'auto-approuver.** Découverte filtrée par scope et mode ; un tool
invisible rend la **même** erreur qu'un tool inconnu. `approval`, `subject`, `mode`, `scopes`, `lease`
sont **absents du schéma**, pas seulement rejetés. Aucun tool n'accorde d'approbation. Les annotations
MCP sont indicatives ; l'enforcement est serveur. Enveloppe versionnée : `proposed` | `pending` |
`executed` | `refused`, avec des **codes** de raison seulement.

**Harness d'évaluation.** `CapabilityHarness` rejoue un jeu adverse (allow, deny, unknown, approbation
périmée, état modifié, doublon, timeout, réponse perdue, échec de compensation, injection de prompt)
contre les vrais composants et mesure précision, taux d'escalade et effets de bord dupliqués — comptés
sur le **journal du système externe**, jamais sur ce que le framework croit avoir fait.

### Couche applicative locale gouvernée (Plan 31 — features 1–7)

`services/` est la couche applicative indépendante du transport : projet, règles, gouvernance,
qualité, sémantique, agents et exécution reçoivent tous un `RequestContext` et exigent l'un des scopes
nommés de `NAMED_SCOPES`. `LocalIdentity` dérive le sujet de l'OS et accorde l'ensemble local statique,
jamais une autorité fournie par la requête ; `certification_override` en reste exclu. Les DTO sont
sérialisés champ par champ. L'index de métadonnées (`DatasetRecord`, SQLite/Delta) est alimenté sans
Spark par `skifer index` et expose dictionnaire, lineage amont/aval et impact entre pipelines. Les
hooks d'indexation et d'incident branchés sur la publication restent **non bloquants**.

La gouvernance YAML porte la taxonomie ordonnée `public|internal|confidential|restricted|pii`, sa
propagation par lineage, un owner structuré (`team`, `steward`, `domain`, `contact`) et le cycle de vie
du contrat (`status`, `reviewers`, dates d'effet, `sla`, `security`). La canonicalisation est en v2 :
`sla` et `security` participent au hash ; `status`, `reviewers`, `effective_from` et `effective_until`
n'y participent pas. ODCS 3.1 est importable par `skifer contract import`; `diff_contracts()` marque
les suppressions, retypages, durcissements de champ, baisses de classification et relâchements SLA.

Une quarantaine ouvre les incidents critiques ; une publication rétablie résout les incidents ouverts.
Le routage couvre le propriétaire du dataset et les propriétaires aval, avec webhook générique,
Slack, e-mail, Microsoft Teams et Google Chat. Les alertes d'incident ne contiennent **aucune valeur de
donnée**. La divulgation de preuves reste fail-closed : toute colonne placée dans
`EvidencePolicy.sensitive_columns` (la surface prévue pour `pii`/`restricted`) garde sa valeur de
filtre rédigée, même quand les autres valeurs ont été explicitement demandées.

`ExecutionService` gère une session lazy et un unique job actif par projet ; `job_id == run_id`, et
`ResultView` borne et sérialise le résultat, le rapport de qualité et la décision de publication.
L'extra `[api]` ajoute une API FastAPI loopback dont les routes correspondent aux services : chaque
route métier déclare exactement son scope et `api/routes/` n'importe jamais le moteur. CLI :
`skifer api serve|openapi`, `skifer incidents`, et `skifer audit` (audit de couverture Spark-free,
avec seuil CI optionnel).

### Filter operators (canonical names — SQL abbreviations are aliases)
`equals`, `not_equals`, `greater_than`, `less_than`, `greater_than_equal`, `less_than_equal`,  
`in`, `between`, `not_between`, `not_in`, `contains`, `not_contains`, `starts_with`, `ends_with`,  
`is_null`, `is_not_null`, `like`, `not_like`, `sql`

### `select_final` operations
`cast:type`, `upper`, `lower`, `trim`, `round:N`, `abs`, `length`, `to_date:fmt`,  
`ceil`,  
`nvl:val`, `coalesce:val`, `lit:val`, `expr:sql`, `split:sep,idx`, `substring:start,len`,  
`when:op:val`, `then:...`, `else:...`

### Smart Sandbox
In interactive (non-prod, non-job) mode, source tables auto-resolve to `schema_XXXX.table`.  
Missing tables are shallow-cloned from the main schema (Databricks) or CTAS'd (local).  
Cache file `.skifer_user` must be in `.gitignore`.

```yaml
sandbox:
  missing_table: copy   # copy (default) | error
```

### RuleRegistry & rule kinds
```python
@RuleRegistry.register_rule()                      # kind="projection" (default)
def flag_high_value(df):
    return {"is_high_value": F.when(F.col("amount") >= 1000, 1).otherwise(0)}

@RuleRegistry.register_rule(kind="transform")      # escape hatch: df -> df
def custom_logic(df):
    return df.withColumn(...)

RuleRegistry.list_rules()    # list registered rules
RuleRegistry.list_loaders()  # list registered loaders
```
- `projection` rules return `dict[str, Column]`; consecutive ones are **fused into a single `select()`** by RulePlanner/RuleExecutor (avoids O(N²) Catalyst plan growth).
- `aggregation` rules sharing the same `groupBy` keys (via `agg_keys` attribute) are fused into one `groupBy().agg()`.
- `transform` rules run sequentially, never fused (opaque to the planner).
- Rules must be imported before calling any `engine.run_*` method.
- `engine.explain_rules(schema_dict)` prints a static redundancy report (OVERWRITE, SHARED_READ, DUPLICATE_EXPR, PHOTON_BREAKING, COMPLEXITY_HIGH) without executing anything.

### Governance
`allow_raw_sql: false` on an environment in `config.yaml` disables `expr:` operations and the `sql` filter operator in that environment.

### QueryResolver (agentic layer)
Builds SQL **only from names defined in semantic YAML models** — never raw SQL. Unknown names raise `SemanticQueryError` with Levenshtein suggestions.

### SemanticEngine
Catalog-first, lazy-loading: only `semantic_catalog.yaml` loads at startup; model YAMLs load on-demand and cache. Never pre-load all models.

### Tests
- LLM calls are always mocked; no live cluster, API key or network is needed.
- Spark, however, is **not** mocked everywhere: 182 tests run a real local Delta session
  (the `spark` fixture in `tests/conftest.py`, plus the examples), the other ~2200 use
  doubles. A test that builds a `F.col(...)` needs the fixture even when it only asserts
  a raised error — omitting it makes the test pass only next to its neighbours.
- The Spark-free tests run in ~27s, the whole suite in ~158s.
- One test file per module: `tests/test_<module>.py`.
- `examples/` is a deliverable, not illustration: `tests/test_examples.py` executes every
  `run.py` in a subprocess. Changing behaviour an example shows means updating the example.

## Development plans

When working in **plan mode** (designing an implementation before coding), always follow this workflow:

1. **Explore** the codebase to build a complete inventory of files and coupling points affected.
2. **Design** a phased plan with clear dependency ordering between phases.
3. **Write the plan** to `docs/roadmap/<feature>_plan.md` — this is the source of truth for implementation.
4. **Commit the plan** on the working branch before any code change.
5. **Wait for explicit user validation** before starting implementation.
6. **Implement phase by phase**, running `pytest tests/ -x --tb=short` after each phase.
7. **Each phase = one commit** with a descriptive message referencing the plan.

Plan documents live in `docs/roadmap/` and follow the naming convention `<NN>_<feature>_plan.md`.
The plan must include: context, phased steps with files to create/modify, risk assessment, verification strategy.

**Active plans:**

| Plan | Branch | Status |
|---|---|---|
| [Multi-plateforme](docs/roadmap/01_multi_plateforme_plan.md) | `multi_plateforme` | Abandonné — remplacé par le Plan 26 (produit Spark/Databricks uniquement) |
| [Prérequis GUI (Plan 11)](docs/roadmap/11_gui_framework_prerequisites_plan.md) | `feat/gui-client-planning` | Implémenté — PR en cours |
| [Stabilisation pré-prod (Plan 12)](docs/roadmap/12_stabilisation_preproduction_plan.md) | `main` | En attente d'implémentation |
| [Rule Engine Optimization (Plan 13)](docs/roadmap/13_rule_engine_optimization_plan.md) | `stabilize-and-fix` (mergé via PR #18) | Implémenté |
| [Sources externes déclaratives (Plan 14)](docs/roadmap/14_external_sources_plan.md) | `feat/external-sources` (mergé via PR #21) | Implémenté |
| [Sanitize modules restants (Plan 15)](docs/roadmap/15_sanitize_remaining_modules_plan.md) | `sanitize_code` (mergé via PRs #22/#23) | Implémenté |
| [JDBC sink (Plan 16)](docs/roadmap/16_jdbc_sink_plan.md) | mergé via PRs #25–#28 | Implémenté |
| [Optimisation core : fail-fast, IR, refactor engine (Plan 17)](docs/roadmap/17_core_optimization_plan.md) | mergé via PR #29 | Partiellement implémenté (lots 0, 1, 2.1–2.2) — reste repris dans le Plan 18 |
| [Suite optimisation core : reste plan 17 + correctifs revue (Plan 18)](docs/roadmap/18_core_optimization_followup_plan.md) | mergé via PR #30 | Implémenté |
| [MLflow Serving & Databricks deployment (Plan 19)](docs/roadmap/19_mlflow_serving_plan.md) | `feat/mlflow-serving` | En cours d'implémentation |
| [Workflow multi-agents Claude/Codex/Antigravity (Plan 20)](docs/roadmap/20_multi_agent_workflow_plan.md) | `chore/multi-agent-workflow` | Protocole v1 validé (1er rodage OK ; review = Antigravity) |
| [Orchestration autonome + sous-tâches/routage modèle (Plan 22)](docs/roadmap/22_autonomous_orchestration_plan.md) | mergé `main` | Implémenté — Modèle B validé sur 3 cycles (between/not_between/ceil) |
| [Opérateur de filtre `between` (Plan 23)](docs/roadmap/23_filter_between_plan.md) | mergé via PR #44 | Implémenté |
| [Opérateur de filtre `not_between` (Plan 24)](docs/roadmap/24_filter_not_between_plan.md) | mergé via PR #45 | Implémenté |
| [Sous-transformations YAML `partials:` (Plan 25)](docs/roadmap/25_yaml_schema_partials_plan.md) | `feat/yaml-schema-partials` | Implémenté (phases 25.1–25.5) |
| [Recentrage Databricks : aplatissement Spark-only (Plan 26)](docs/roadmap/26_databricks_refocus_plan.md) | mergé via PR #52 | Implémenté |
| [Tables streaming — Structured Streaming natif (Plan 27)](docs/roadmap/27_streaming_tables_plan.md) | mergé via PR #53 | Implémenté (phases 27.0–27.6) |
| [Materialized views — SQL natif compilé + agrégations déclaratives (Plan 28)](docs/roadmap/28_materialized_views_plan.md) | mergé via PR #54 | Implémenté (phases 28.0–28.6) |
| [Agent-ready semantic layer — programme 10 features / 53 slices (Plan 29)](docs/roadmap/29_agent_ready_semantic_layer_program.md) | Toutes mergées : features 1/2/3 (PR #56), 0 (#57), 6 (#58), 4 (#59), 5 (#60), 7 (#62), 8 (#63), 9 (#64) | **53/53 slices — programme complet.** Les 10 features sont implémentées. Plans par feature : [`docs/roadmap/29_agent_ready_semantic_layer/`](docs/roadmap/29_agent_ready_semantic_layer/README.md) |
| [Scénario de développement de la bibliothèque (Plan 31)](docs/roadmap/31_lib_development_scenario.md) | `feat/plan31-f6-api`, empilée sur f1..f5/f7 | **Implémenté.** Les 7 features sont livrées : services, registre de métadonnées, gouvernance YAML, incidents/alertes, exécution async, API locale et audit de couverture. |
| [Refonte doc d'appropriation (Plan 33)](docs/roadmap/33_documentation_onboarding_plan.md) | mergé via PR #65 | Implémenté (phases A–D) — parcours d'onboarding exécutable, `examples/` testés, gouvernance transverse, site sans artefact interne |
| [Un exemple parlant par feature (Plan 34)](docs/roadmap/34_examples_per_feature_plan.md) | mergé via PR #68 | Implémenté (phases 0–6) — 18 exemples sous `examples/`, tous exécutés par la suite, chacun atteignable depuis la doc ; 3 défauts du code livré trouvés en les écrivant |

## Key files

| File | Purpose |
|---|---|
| `config.yaml` | Runtime env config (catalog names, env priority, sandbox options, default `params`) |
| `semantic_catalog.yaml` | Index of all semantic models (auto-updated by SemanticBuilder) |
| `CHANGELOG.md` | Release notes — always update `[Unreleased]` section |
| `pyproject.toml` | Build config + optional dep groups |
| `.skifer_user` | Auto-generated user suffix cache — add to `.gitignore` |
| `docs/roadmap/*_plan.md` | Development plans — source of truth for implementation |

## gstack

Use /browse from gstack for all web browsing. Never use mcp__claude-in-chrome__* tools.
Available skills: /office-hours, /plan-ceo-review, /plan-eng-review, /plan-design-review,
/design-consultation, /design-shotgun, /design-html, /review, /ship, /land-and-deploy,
/canary, /benchmark, /browse, /open-gstack-browser, /qa, /qa-only, /design-review,
/setup-browser-cookies, /setup-deploy, /setup-gbrain, /sync-gbrain, /retro, /investigate,
/document-release, /document-generate, /codex, /cso, /autoplan, /pair-agent, /careful, /freeze,
/guard, /unfreeze, /gstack-upgrade, /learn.

**GBrain (persistent context).** This repo is registered as the isolated
`skifer` source in the local shared brain. Run GBrain sync after
significant workflow/documentation changes. Prefer `gbrain search` over grep for
semantic documentation and workflow lookups; use `gbrain code-def` /
`gbrain code-refs` once a code-aware sync pack is active for this source.
The `## GBrain Search Guidance` block below is written/refreshed automatically by /sync-gbrain.

## GBrain Search Guidance (configured by /sync-gbrain)
<!-- gstack-gbrain-search-guidance:start -->

GBrain is set up and synced on this machine. The agent should prefer gbrain
over Grep when the question is semantic or when you don't know the exact
identifier yet.

**This worktree is pinned to a worktree-scoped code source** via the
`.gbrain-source` file in the repo root (kubectl-style context).
`gbrain code-def`, `code-refs`, `code-callers`, `code-callees`, `search`, and
`query` from anywhere under this worktree route to that source by default —
no `--source` flag needed (gbrain >= 0.41.38.0; on older gbrain the call-graph
commands need `--source "$(cat .gbrain-source)"`). Conductor sibling worktrees
of the same repo each have their own pin and their own indexed pages, so
semantic results match the code on disk here.

Call-graph queries (`code-callers`/`code-callees`) also need the graph to be
built first — run `/sync-gbrain --dream` (or `--full`) if they return
`count: 0`. This only works if this source's gbrain schema pack extracts code
symbols; on a non-code-aware pack `--dream` completes but the graph stays empty
and reports a WARN. `code-def`/`code-refs` need the same extraction.

Two indexed corpora available via the `gbrain` CLI:
- This worktree's code (auto-pinned via `.gbrain-source`).
- `~/.gstack/` curated memory (registered as `gstack-brain-<user>` source via
  the existing federation pipeline).

Prefer gbrain when:
- "Where is X handled?" / semantic intent, no exact string yet:
    `gbrain search "<terms>"` or `gbrain query "<question>"`
- "Where is symbol Y defined?" / symbol-based code questions:
    `gbrain code-def <symbol>` or `gbrain code-refs <symbol>`
- "What calls Y?" / "What does Y depend on?":
    `gbrain code-callers <symbol>` / `gbrain code-callees <symbol>`
- "What did we decide last time?" / past plans, retros, learnings:
    `gbrain search "<terms>" --source gstack-brain-<user>`

Grep is still right for known exact strings, regex, multiline patterns, and
file globs. Run `/sync-gbrain` after meaningful code changes; for ongoing
auto-sync across all worktrees, run `gbrain autopilot --install` once per
machine — gbrain's daemon handles incremental refresh on a schedule.

Safety: don't run `/sync-gbrain` while `gbrain autopilot` is active — the
orchestrator refuses destructive source ops when it detects a running autopilot
to avoid racing it (#1734). Prefer registering user repos with `gbrain sources
add --path <dir>` (no `--url`): URL-managed sources can auto-reclone, and the
sync code walk for them requires an explicit `--allow-reclone` opt-in.

<!-- gstack-gbrain-search-guidance:end -->
