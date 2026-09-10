# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [2.1.0] - 2026-09-10

### Added

- `skifer.__version__`, read back from the installed distribution metadata rather than
  restated in the source. `pyproject.toml` stays the single source of truth, so the two
  can never drift. A source checkout that is not installed reports `"unknown"` instead of
  raising, which keeps `PYTHONPATH=src` imports working.
- A `Tests` workflow running the suite on every push and pull request, across Python 3.10
  to 3.13, with a JVM installed so the Spark-backed tests actually run rather than error
  out. Until now the only workflows published; nothing verified anything.

### Changed

- Both publishing workflows now authenticate to PyPI and TestPyPI through **trusted
  publishing** (OIDC): `id-token: write` at the job level, no API token secret. The
  token-based configuration failed on its first run, because the action falls back to
  OIDC when no password is supplied and then lacks the permission to mint an identity.

### Fixed

- **The read-only MCP server rejected every resource URI on Python 3.10 and below.**
  `_parse_uri` called `parse_qs(..., strict_parsing=True)` unconditionally, and before
  Python 3.11 that raises on the empty string. A URI carrying no parameters — nearly all of
  them — was therefore reported as `invalid_request`, so the whole MCP surface was
  unreachable. Strict parsing now applies only to a query that actually exists.
- **Restored `examples/07_sources_and_shaping/data/events.json`, which the repository
  never contained.** `.gitignore` carried a blanket `*.json` with exceptions for `schemas/`
  and `features/` but not for `examples/`, so the dataset the example reads was silently
  dropped — while the example's own docstring says "the JSON is checked in". The exception
  is added, and the example runs again. Its filter now keeps exactly three rows, so
  `dev_limit: 3` no longer trims: the printed result cannot depend on partition ordering.
- **`requires-python` is now `>=3.10`.** The package claimed `>=3.9`, but `import skifer`
  raised `TypeError` on 3.9: `schema_loader.py` uses PEP 604 unions in annotations
  evaluated at definition time, and `evidence.py` uses `dataclass(kw_only=True)`, which is
  3.10 only. Nothing ever ran on 3.9, so the claim went unchallenged. The 3.9 classifier is
  dropped. Python 3.9 reached end of life in October 2025.
- Added `from __future__ import annotations` to the five modules that use PEP 604 unions,
  so annotations are never evaluated at runtime.
- Removed dead imports and f-strings with no placeholders in `loaders.py`, `sandbox.py`
  and `spark_factory.py`.
- `UserProfile.save()` and `.load()` resolve the default profile path **at call time**
  instead of binding it at import. Bound at import, `_PROFILE_PATH` could not be
  substituted, so `add_alias()` — which saves on its own — wrote to the real
  `~/.skifer_profile.yaml` of whoever ran the suite. `test_add_alias_persists` took a
  `tmp_path` and looked isolated, but only its explicit `save()` used it; the implicit one
  it claims to cover was never actually asserted. The test now redirects the default path,
  asserts the implicit save, and a companion test fails outright if `Path.home()` is
  resolved during a test.
- `jsonschema` is now declared in the `dev` extra. It was used only by
  `tests/test_json_schema.py` and declared nowhere, so in a fresh environment its 15 tests
  skipped silently — the only tests that validate the published
  `schemas/skifer-pipeline.schema.json`. A skipped test reads like a passing suite.

### Documentation

- Rewrote the installation instructions, which still described a private repository and a
  hand-built wheel. Skifer is on PyPI: `pip install skifer`. `docs/install_databricks.md`
  now covers cluster and notebook installs, extras, TestPyPI pre-releases, and says why
  `skifer[spark]` is the wrong extra on a Databricks Runtime, whose own PySpark and Delta
  it would override.

## [2.0.0] - 2026-09-10

> Version number set on 2026-08-03; published to PyPI on 2026-09-10. Everything below
> shipped in that one artifact, including the work that had accumulated under
> "Unreleased" in between.

### Added

- (Plan 29, slice 9.8) Added a deterministic governed-capability evaluation
  harness and an adversarial fake support-ticket pilot spanning allow, deny,
  unknown, stale approval, live-state drift, duplicate request, timeout, lost
  response reconciliation, compensation failure, and prompt injection. Reports
  expose allowlisted decision precision and escalation metrics, while duplicate
  side effects are measured independently from the fake external system's own
  append-only call log. Scenario clocks must advance and fail loudly on
  exhaustion, so incomplete time-sensitive flows cannot be reported as passing.
- (Plan 29, slice 9.7) Added an optional governed MCP capability surface with one
  closed-schema tool per caller-visible write capability. Discovery requires every
  declared scope and an MCP-permitted shadow/supervised mode; invisible capabilities
  are indistinguishable from unknown tools. Calls reuse `GovernedExecutor` for
  validation, preconditions, server-side approvals, credentials, idempotency, and
  replay, while protocol envelopes expose only lifecycle status, deterministic
  request identity, reason codes, and JSON-native outcomes. Approval, subject,
  autonomy, scopes, and leases cannot be supplied by an MCP caller, and advisory
  MCP annotations never affect enforcement.
- (Plan 29, slice 9.6) Capabilities now execute idempotently against an append-only
  history. `GovernedExecutor` computes one request identity, consults the journal
  before acting, writes the pre-call `EXECUTING` event **before** the external call —
  the only thing that makes a lost response recoverable — and replays the recorded
  outcome for a duplicate request instead of calling again. A lost response is never
  retried blindly: either the executor declares a reconciliation lookup at
  registration, or a human reconciles. Compensation is a first-class audited run with
  its own identity and events, never a rollback, and a failed compensation stays
  `COMPENSATING` rather than being recorded as compensated. Writes reach an external
  system through this governed path only; `CapabilityInvoker.invoke()` still refuses
  them. `SqliteCapabilityHistoryStore` contains no UPDATE or DELETE, asserted in tests.
  A live credential echoed back by an executor is redacted out of the stored outcome —
  and out of the returned result identically, so a replay stays byte-for-byte identical
  to the first call. Refusing `CredentialLease` instances and secret-looking key names
  does not cover a secret value under an innocuous key, and an append-only journal
  cannot be scrubbed afterwards.
- (Plan 29, slice 9.5) Added injected just-in-time credential providers and an
  in-memory-only `CredentialLease` whose secret is explicitly revealed, redacted
  from rendering, and forbidden from serialization. The broker enforces least
  scopes, a 15-minute TTL ceiling, a bounded live-lease count, timezone-aware
  strict expiry, provider-answer verification, class-name-only failures, and a
  no-provider-call shadow guard. Read executors must explicitly opt into receiving
  a lease as a separate argument.
- (Plan 29, slice 9.4) Added fail-closed capability autonomy modes, bounded
  approval records tied to deterministic request and precondition hashes, and an
  explicit execution state machine whose guarded transitions prevent approval
  reuse after input or live-state changes. Per-environment autonomy accepts only
  `shadow` and `supervised` in v1; shadow records proposals and invokes only
  executors that explicitly declare dry-run support, without entering execution.
- (Plan 29, slice 9.3) Added deterministic governed-capability preconditions:
  bounded explicit rule registration, typed `ALLOW`/`DENY`/`UNKNOWN` outcomes,
  canonical versioned hashes over bounded live-state snapshots, fail-closed
  aggregation, and a mandatory state recheck immediately before execution.
  Capabilities with declared rules cannot run without an injected evaluator and
  state reader; invalid or oversized rule results, rule failures, and unhashable
  state become `UNKNOWN` without disclosing external error messages.
- (Plan 29, slice 9.2) Added an explicit capability executor allowlist and a
  fail-closed read-only invoker with scope and closed-schema argument checks,
  JSON-safe result normalization, and class-name-only failures. `AgenticHub`
  accepts capabilities only through a structured `CapabilityRequest`; neither
  deterministic nor LLM free-text intent routing can authorize execution.
- (Plan 29, slice 9.1) Added the governed capability declaration foundation:
  frozen, allowlisted capability models; fail-closed structural validation for
  identifiers, scopes, rule names, bounded closed JSON Schemas, idempotency,
  reversibility, approval, provenance, and secret-bearing keys; plus a
  catalog-first registry that exposes summaries at startup and validates,
  cross-checks, and caches definitions only when explicitly requested.
- (Plan 29, slice 8.6) Added supervised post-delivery outcome measurement for
  accepted adaptive Gold proposals. Usage is compared across non-overlapping,
  minimum-data-guarded before/after windows with shared nearest-rank duration
  percentiles and failure rates; results are explicitly improved, regressed, or
  inconclusive. Regressions produce only a human-review recommendation. Acceptance
  now records an atomic delivery link, measured outcomes remain re-evaluable, and
  `skifer adaptive evaluate` exposes the result with a dedicated regression
  exit code.
- (Plan 29, slice 8.5) Added the human-only `skifer adaptive list`, `show`,
  `diff`, `accept`, and `reject` workflow with atomic proposal status decisions,
  source-contract hash revalidation, dedicated stale/conflict exit codes, and
  exclusive output creation. Acceptance never overwrites a human schema, invokes
  Git or deploys an asset; an explicit `--if-identical` option only permits a
  byte-identical idempotent no-op.
- (Plan 29, slice 8.4) Added atomic, review-only generation of validated adaptive
  Gold pipeline YAML, allowlisted proposal JSON, and managed semantic drafts under
  `.skifer_proposals/`. Physical proposals are loaded before publication,
  materialized views are compiled through the Plan 28 SQL compiler, and existing
  proposal directories are only reused when every generated byte is identical.
- (Plan 29, slice 8.3) Added the four static, versioned adaptive Gold
  recommendation rules with named threshold decompositions, content-derived stable
  proposal IDs, and explicit refusal records. Certification, compilability, grain,
  directional fanout, and minimum-benefit guardrails now fail closed; recommendations
  remain frozen review objects and never generate files, deploy assets, or drop data.
- (Plan 29, slice 8.2) Added deterministic, clock-injected adaptive pattern
  aggregation over bounded 7-day/30-day windows, with strict environment,
  consumer-class, and model-definition partitions. Success and failure populations
  remain separate, and duration percentiles are withheld until enough successful
  observations exist.
- (Plan 29, slice 8.1) Added privacy-safe adaptive usage events with an
  allowlisted `SemanticEvidence` conversion, canonical versioned query
  fingerprints, and append-only SQLite/Delta stores. Events retain only logical
  model/metric/dimension IDs and value-free filter shapes; raw questions, SQL,
  filter values, and clear user identities have no persistence field. Retention
  is configurable metadata in this slice and never triggers deletion.
- (Plan 29, slice 7.5) Completed the read-only MCP feature with the strict
  `skifer mcp serve --transport stdio|http --config ...` launcher, closed
  startup configuration, injectable governed-service and bearer-verifier
  factories, bounded service limits, stdio and Streamable HTTP runners, and a
  business-data-free `{"status":"ok"}` health endpoint. Public stdio and every
  incompletely authenticated HTTP configuration now fail at startup; startup
  logs and configuration errors use fixed, secret-free messages. Added an
  official-SDK list/read/query smoke test that skips cleanly when the optional
  `mcp>=2.1,<3` extra is absent, plus generic stdio/HTTP integration guidance.
- (Plan 29, slice 7.4) Added fail-closed MCP request authentication factories.
  Local stdio authority comes only from explicit immutable startup configuration;
  HTTP bearer credentials are authenticated per request by an injected verifier and
  rechecked for issuer, audience/resource, timezone-aware expiry, and token scopes.
  No authorization server or JWT implementation is included, credentials never enter
  singleton or connection state, certification overrides are refused, protocol errors
  disclose no verifier details, and traced identities reuse the existing omit/HMAC
  policy with keyed secrets only.
- (Plan 29, slice 7.3) Added the closed, read-only `query_semantic_model` MCP Tool.
  Its schema accepts only bounded semantic names, structured filters, dates, periods,
  and row limits; execution delegates exclusively to `AgentReadyDataService`, returns
  JSON-native rows with redacted evidence and truncation state, and advertises a
  read-only annotation where supported without relying on that client-side hint for
  enforcement.
- (Plan 29, slice 7.2) Added the optional, read-only MCP resource surface for
  governed semantic catalog, model, contract, certification, and column-lineage
  reads. Handlers call only `AgentReadyDataService`, expose canonical JSON with
  SHA-256 ETags, preserve catalog pagination, translate expected and unexpected
  failures to sanitized structured protocol errors, and keep the `mcp>=2.1,<3`
  SDK behind a lazy `[mcp]` extra so core imports remain dependency-free.
- (Plan 29, slice 7.1) Added the transport-independent
  `AgentReadyDataService` as the fail-closed boundary for future MCP reads. It
  centralizes exact scope checks, stable validated catalog pagination, governed
  model/contract/certification/lineage DTO allowlists, hard query and filter
  budgets, redacted evidence, and bounded JSON-native row materialization.
  Semantic queries keep the existing certification gate by using
  `query_with_evidence`; client scopes are deliberately not forwarded into its
  `ConsumerContext`, preventing an MCP caller from injecting a certification
  override. Certification stores now expose only the minimal versioned contract
  lookup needed by this boundary.
- (Plan 29, slice 5.5) Trace redaction, limits and pseudonymisation.
  `TraceAttributePolicy` allowlists span attribute keys, bounds their count,
  length and events per span, and is applied on **every** path that leaves the
  process — `set_attribute` included, and in the OTLP/MLflow adapters, not only
  in the in-memory tracer used by tests. User identity is omitted by default;
  `hmac` uses a secret from `SKIFER_TRACING_HMAC_SECRET` and omits the
  identity when no secret is configured rather than emitting an unkeyed,
  reversible digest. Canary tests cover token, email, PII, prompt and SQL.
- (Plan 29, slice 5.4) Added lazily loaded OTLP/HTTP and MLflow tracing
  exporters with config-driven `none`, `otlp`, `mlflow`, and failure-isolated
  `dual` modes. Optional SDK failures are sanitized and rate-limited without
  affecting business execution unless tracing is explicitly required; exporter
  endpoints and authentication headers are redacted from configuration reprs,
  logs, and errors. MLflow destinations are validated read-only and never
  provisioned implicitly.
- (Plan 29, slice 5.3) Added end-to-end semantic and agentic tracing with stable
  spans for hub routing, model selection, certification policy, deterministic
  compilation, SQL execution, LLM completion, and serving serialization. SQL
  spans reuse the evidence hash and never retain SQL text; LLM spans expose only
  provider/model and monotonic latency, with prompts disabled. Active roots take
  precedence for evidence correlation, while an explicit W3C consumer trace ID
  seeds a new root and no-op tracing preserves legacy evidence behavior.
- (Plan 29, slice 5.2) Tracing no longer reaches into business identity.
  `PublicationCoordinator.publish` had a separate traced code path that derived
  the persisted `run_id` from the ambient trace context: enabling tracing changed
  the run recorded in the certification store, and raised outright when that
  ambient id was not a UUID. Publication now runs one path, owns its run id, and
  correlates with the pipeline through the shared trace id. A monitor exposing
  `tracer` as a property is no longer silently downgraded to `NoOpTracer`.
- (Plan 29, slice 5.2) Added best-effort pipeline and certified-publication
  tracing at stable boundaries (schema load, source resolution, transforms,
  staging, contract evaluation, promotion/quarantine), with one pipeline run ID
  propagated through publication spans. Global `observability.tracing` config
  defaults to the allocation-free no-op tracer, rejects unavailable exporters
  actionably, and supports explicit required mode; exporter failures are
  rate-limited and never mask business failures, and tracing performs no Spark
  counts or captures SQL, filter values, or PII.
- (Plan 29, slice 5.1) Span parenting never points at a closed span. Ending a
  parent before its child restored an already-ended span as the active one, so
  every span started afterwards hung off a span the trace backend had seen end.
  Parent resolution now walks to the nearest ancestor still open.
- (Plan 29, slice 5.1) Added a dependency-free runtime tracing foundation:
  `Tracer`/`Span` protocols, an allocation-free default `NoOpTracer`, a
  deterministic `InMemoryTracer`, W3C-compatible trace/span IDs, monotonic
  timings, explicit unfinished-span visibility, and `contextvars`-isolated
  propagation of trace, run, session, and evidence IDs. Invalid or non-finite
  attributes are omitted, auditable, and warned once without affecting business
  behavior; context managers record failures, mark spans `ERROR`, and re-raise
  the original exception unchanged.
- (Plan 29, slice 4.4) The human provenance line speaks for every source, not
  just the first. A query joining a certified table to an uncertified one used
  to announce "certifié", reassuring the reader about precisely the source that
  was not — reachable under `warn`, the recommended migration step. It now names
  all datasets, reports the weakest certification and the oldest freshness, and
  surfaces a non-ALLOW decision, still on a single line.
- (Plan 29, slice 4.4) Agentic and serving responses now carry optional,
  allowlisted semantic evidence. Human responses append one redacted provenance
  line (source, certification, freshness); OpenAI-compatible serving preserves
  `choices` and adds versioned top-level `skifer.evidence` metadata.
- (Plan 29, features 3/4/6) **Certification bypass closed.** The gate's preflight
  can only see the root model, because the join path does not exist yet, so a
  multi-model query obtained an ALLOW decided on the root table alone and then
  read every joined dataset without any of them being certified. The recheck now
  evaluates each dataset the compiled plan actually reads; an uncertified joined
  dataset raises `SemanticAccessDenied` naming it. `off` mode still resolves no
  dependencies at all.
- (Plan 29, slice 4.3) Semantic query evidence now snapshots the certifications
  used by the final pre-execution gate, records optional backend statement IDs,
  UTC execution instants, monotonic durations, and an explicit succeeded/failed
  status. SQL failures raise `SemanticExecutionError` with a safe partial proof
  containing only the backend error class, never its potentially sensitive
  message. Missing certification snapshots under a non-off ALLOW decision fail
  closed; legacy backends keep their unchanged `execute_sql(sql)` contract.
- (Plan 29, slice 4.2) Compilation evidence. `ResolvedQuery` now carries its
  logical plan (sources, model keys, selected expressions) from both the legacy
  single-model path and the multi-model one, without snapshotting whole model
  definitions. `hash_sql()` hashes the SQL actually executed, prefixed with its
  algorithm and normalization version (`sha256:v1:`); normalization collapses
  formatting **outside string literals only**, so `x = 'a  b'` and `x = 'a b'`
  keep distinct hashes. `MetricEvidence` carries a definition hash covering the
  formula, aggregate type and inline filters, plus source columns taken from a
  lineage subgraph restricted to the selected members — with an explicit
  `lineage_status` so an unresolved lineage is not read as "no sources". A pure
  `EvidencePolicy` governs disclosure, fail-closed by default; requesting raw
  SQL that policy withheld raises `EvidenceRedactionError` instead of returning
  `None`, which a consumer could not tell apart from "there was no SQL".
- (Plan 29, slice 4.1) Evidence serialization refuses non-finite floats. `NaN`
  and `Infinity` are written by `json.dumps` as bare tokens that no strict JSON
  parser accepts, which would have made an evidence artefact unreadable by the
  very consumers it exists for.
- (Plan 29, slice 4.1) Added standalone, allowlisted semantic evidence models
  and `SemanticEngine.query_with_evidence()`. The historical `query()` API now
  delegates to that single execution path while preserving its DataFrame return;
  evidence serialization redacts SQL and filter values by default and fails
  closed for naive datetimes or unsupported types.
- (Plan 29, slice 6.6) Added adversarial and local-Spark end-to-end coverage for
  the semantic domain graph: curated-versus-proposed relationships, ambiguity,
  cardinality, fanout, composite keys, fiscal periods, injection rejection,
  lazy file access/cache behavior, and legacy SQL compatibility are now
  verified against a coherent orders domain.
- (Sécurité) `QueryResolver` refuse désormais toute valeur de filtre non
  scalaire et valide chaque membre d'une liste `in`. Les membres de `in` et les
  valeurs non-`str` étaient interpolés via un `str()` nu, qui transportait leurs
  propres quotes dans le SQL : `value=["A','B') OR 1=1 --"]` produisait
  `region IN ('A','B') OR 1=1 --')`. Ces valeurs proviennent de la sortie du LLM.
  Le rendu des valeurs bien formées est inchangé.
- (Plan 29, slice 6.5) Added fail-closed semantic grain compatibility and
  versioned calendars. Metrics now declare additive, semi-additive, or
  non-additive behavior; the planner rejects unpinned semi-additive dimensions,
  non-additive cross-model queries, and any join path that would duplicate a
  metric's rows before SQL generation — a relationship fans out relative to the
  direction it is crossed in, so a metric reached beyond a `one_to_many` hop or
  sitting on the one-side of a `many_to_one` is refused just like one upstream
  of a `one_to_many`. Models may reference lazily loaded
  `calendars/<key>.yaml` definitions, and named periods resolve deterministically
  to validated ISO bounds with calendar provenance recorded on the semantic
  plan. Raw date bounds are now strictly parsed before interpolation, closing
  the date-filter SQL injection path, while legacy queries retain byte-identical
  SQL. The agent prompt asks for a declared period name when the model exposes a
  calendar and keeps free `date_from`/`date_to` bounds otherwise.
- (Plan 29, slice 6.4) Added deterministic multi-model semantic planning and
  SQL compilation. Named metric/dimension references now resolve through lazy
  catalog summaries, curated `DomainGraph` paths become ordered joins with
  stable aliases and qualified/validated identifiers, composite keys retain
  their declared pairing order, and absent, ambiguous, `many_to_many`, or
  `unknown` relationship paths fail explicitly before execution. Unqualified
  root-model queries retain the legacy byte-for-byte SQL path; ambiguous names
  outside root require explicit `model.name` qualification.
- (Plan 29, slice 6.3) Added a lazy `DomainGraph` for curated semantic
  relationships. It discovers candidates from catalog summaries without
  preloading all YAML models, exposes `related_models`, `get_entity`,
  `neighbors`, and `find_paths`, returns every minimal path in deterministic
  order, refuses silent truncation with explicit depth/model-limit errors, and
  invalidates cached edges when the catalog or a loaded model definition hash
  changes. Proposed relationships under
  `metadata.relationship_candidates` remain non-queryable and are never
  traversed before human curation.
- (Plan 29, slice 6.2) Managed semantic drafts can now carry deterministic
  relationship proposals derived from pipeline joins. The projection preserves
  join key order and field-level entity annotations, the draft builder emits
  `metadata.relationship_candidates` with `status: proposed` only when a join
  is semantically admissible, and records explicit rejection reasons for
  unsupported automatic cases such as `cross`/`left_anti`. Cardinality remains
  conservative: only table-level uniqueness checks and declared contract grain
  can justify a `one` side; otherwise the proposal stays `unknown` and is not
  queryable until a human curates and promotes a real `relationships:` block.
- (Plan 29, slice 6.1) Semantic models can now declare an optional domain
  block with `grain`, typed `entities`, and directed `relationships`. The
  validator parses and validates those fields with the same safe-identifier
  boundary already enforced for dimensions/metrics, rejects unsupported join
  types, rejects self-relations, and fails closed on `unknown` cardinality with
  an explicit action to declare or certify uniqueness before querying. The
  semantic catalog now stores only entity names and related model keys so
  candidate discovery stays catalog-first and lazy without preloading full
  model YAML files.
- (Plan 29, slice 0.6) `SemanticBuilder` can now enrich a managed semantic
  draft from a `ProjectedSchema` without letting the LLM invent or overwrite
  structure. The deterministic draft remains the source of truth: post-LLM
  filtering rejects invented dimensions, rejects any attempt to rename/retype/
  redefine managed fields, rejects new metrics whose dependency is not an
  existing projected output, and only admits descriptions, synonyms, and
  bounded new metrics before running both `SemanticValidator.validate_yaml()`
  and `validate_against_projection()`. The no-Spark end-to-end path is now
  covered from pipeline YAML to draft, constrained fake-LLM enrichment,
  promotion, lazy catalog loading, and final semantic validation.
- (Plan 28) Documentation: `docs/core.md` gains two reference sections — "Declarative aggregations" and "Materialized views" (YAML surface, where the DDL runs and what happens when no warehouse is configured, the definition-hash decision table, full refresh, load-time guardrails) — plus cross-links from `run_process_to_table`; `docs/getting_started.md` documents `sql_warehouse_id` alongside `checkpoint_base`; `CLAUDE.md`/`AGENTS.md`/`README.md`/`docs/index.md` updated with the `aggregate:` block, the materialized-view surface and the `[databricks]` extra. Note: `docs/yaml_spec.md` was deliberately left alone — it documents the **semantic** model YAML (dimensions/metrics), not the pipeline YAML, whose reference is `docs/core.md`.
- (Plan 28) New `[databricks]` optional extra declaring `databricks-sdk` — the SDK was imported by `core/environment.py` and `spark_factory.py` without ever being declared as a dependency (pre-existing gap). The import stays lazy, and a missing SDK is no longer indistinguishable from missing credentials: `get_workspace_client()` logs which of the two is wrong instead of swallowing both, and the materialized-view warehouse error names the install command. The extra is unnecessary on a Databricks runtime, where the SDK is pre-installed.
- (Plan 28) Materialized-view execution — `run_process_to_table`/`run_from_yaml` now create the view end to end. `materialization: materialized_view` **short-circuits the DataFrame pipeline entirely** (no source is read): the schema is compiled to SQL, wrapped in `CREATE [OR REPLACE] MATERIALIZED VIEW … AS <select>` (clause order: clustering, `COMMENT`, `TBLPROPERTIES`, `SCHEDULE`) and executed on a SQL warehouse through the Statement Execution API — `CREATE MATERIALIZED VIEW` is refused by all-purpose clusters and by Databricks Connect, so the DDL never goes through `spark.sql` on Databricks. New `SparkBackend` primitives `execute_sql_on_warehouse` (with polling to a terminal state and a 30-min ceiling; failures re-raised enriched, never swallowed), `create_/refresh_/drop_materialized_view` and `get_table_property`. **Definition drift is detected**: a SHA-256 of the compiled SELECT *and* of the definition-bearing options (`schedule`, `comment`, `cluster_by`, `partition_by` — not `refresh`) is stored in `TBLPROPERTIES ('skifer.definition_hash')`; absent view → `CREATE`, identical hash → `REFRESH` (skipped under `refresh: never`, leaving it to the `SCHEDULE`), different or unreadable hash → `CREATE OR REPLACE`, so a changed YAML can never leave a stale view in place. Warehouse resolution (`engine.resolve_sql_warehouse_id`) reads `params.sql_warehouse_id` and **fails fast in job/production mode before any compilation** when it is missing; in interactive mode the DDL is written to `{sql_output_dir}/…​.sql` (default `generated_sql/`) with an explicit "NOT CREATED" warning naming the config key, and the post-create monitor is skipped since there is nothing to check. In local mode (`catalog: null`) there are no materialized views in Delta OSS, so the compiled SELECT is executed and persisted through the regular batch write — which also proves at every run that the generated SQL is valid. `engine.full_refresh(..., materialization="materialized_view")` drops the view (no checkpoint to purge); `run_process_and_split` and `run_union_sources_to_table` refuse materialized views with actionable messages, and `_write_dataframe` raises if a DataFrame ever reaches it under an MV materialization (third barrier). Sandbox resolution is shared with the DataFrame path via the extracted `SchemaInterpreter.resolve_source_table`, so both read exactly the same tables.
- (Plan 28) Materialized-view YAML surface: `materialization: materialized_view` is now accepted (it was recognized-but-rejected since Plan 27), with the dict form `{schedule, comment, cluster_by, partition_by, refresh}` — `schedule` must start with `EVERY ` or `CRON `, `cluster_by`/`partition_by` are mutually exclusive (Databricks accepts one clustering strategy), and `refresh` is `auto` (default) or `never`. The `materialization` allowlist is now **per type** (`MATERIALIZATION_ALLOWED_KEYS` in `core/constants.py`) instead of a flat set, so streaming options can no longer be silently accepted on a materialized view (previously `checkpoint:` on `type: table` passed the allowlist and was caught only by a follow-up check). Load-time cross-validation aggregates every blocker in one error: `business_rules` (Python is not expressible in SQL — materialize upstream), `partials:`, file sources, loaders, `dev_limit` (table and schema level), JDBC sinks, `streaming: true`, `drop_duplicates_on`, `preprocess.qualify`, and `keep_all_columns` with a join or several tables. JSON Schema and `schemas/skifer-pipeline.schema.json` updated accordingly.
- (Plan 28) `core/sql_compiler.py` (new): compiles a parsed schema (`parse_to_ir`) into a single `WITH … SELECT …` statement — one CTE per table alias (projection + filters + `filter_groups` OR-of-ANDs + `drop_nulls_in`), a join tree using `USING (…)` when key names match and `ON` otherwise, then `select_final` / `keep_all_columns` / `aggregate` as the final projection. Pure Python (no Spark, no catalog access); table names go through a caller-supplied `resolve_table` so sandbox resolution stays in one place. Identifiers are backtick-quoted and every value is emitted as an escaped SQL literal (filter values are never interpolated raw). `_SQL_FILTER_DISPATCH`/`_SQL_OP_DISPATCH` mirror the Spark dispatch tables key-for-key, enforced by drift-guard tests, and `expr:`/`sql:` remain subject to `allow_raw_sql`. Anything without a faithful SQL form raises `SqlCompilationError` instead of emitting approximate SQL: Python `business_rules`, `partials:`, loaders, file sources, streaming reads, `dev_limit`, `quality_checks.drop_duplicates_on` and `preprocess.qualify` (the last two would need a windowed `ROW_NUMBER` with a deterministic `ORDER BY` plus the full column list — neither `QUALIFY` nor `SELECT * EXCEPT` is available). Backed by parity tests that run both paths on a real local Spark session and compare rows.
- (Plan 28) Declarative `aggregate:` block — a top-level `{group_by, measures, having}` mapping replaces the need to write a Python `kind="aggregation"` rule (and to hand-attach `agg_keys`) for ordinary GROUP BY work. Measures accept the compact `[source, target, func]` form or a `{source, target, func}` mapping; functions come from the new `AGGREGATE_FUNCTIONS` catalog in `core/op_catalog.py` (`sum`, `avg`, `min`, `max`, `count`, `count_distinct`, `sum_distinct`, `approx_count_distinct`, `stddev`, `variance`, `first`, `last`, plus aliases such as `mean`), with `source: "*"` reserved to `count`. `having:` reuses the existing filter grammar and is validated against group keys and measure targets. The block is mutually exclusive with `select_final` and `keep_all_columns`, is rejected on streaming pipelines (load-time and run-time preflight — streaming aggregations need watermarks), and `add_columns` now runs *before* the aggregation so derived columns can feed `group_by`. New backend primitives `agg_expr`/`group_by_agg` (SparkBackend + FakeBackend, dispatch table kept in sync with the catalog by a drift-guard test), IR types `ParsedAggregate`/`ParsedMeasure` on `ParsedSchema.aggregate`, and `AggregateDef`/`MeasureEntry` in the JSON Schema (`schemas/skifer-pipeline.schema.json` regenerated).
- (Plan 27) Engine wiring for streaming tables — `run_process_to_table`/`run_from_yaml` execute `materialization: streaming_table` end to end: the interpreter reads `streaming: true` tables via `read_table_stream`/`read_source_stream` (sandbox resolution unchanged), the checkpoint is resolved **before** any read (`engine.resolve_checkpoint_location`: explicit path verbatim; `auto` → local `{warehouse}/_checkpoints/{schema+suffix}/{table}` or the `checkpoint_base` env param on Databricks, with a fail-fast error naming the exact config key), and the write dispatches to `write_stream_table`. Run-time preflight rejects `intermediate_mode='table'`, `aggregation`-kind rules (with guidance toward downstream batch/materialized views) and re-checks limit/window incompatibilities for hand-built schema dicts. `run_process_and_split`/`run_union_sources_to_table` refuse streaming schemas with actionable messages. The post-write monitor still runs after `awaitTermination()` (complete data with `available_now`).
- (Plan 27) `engine.full_refresh(target_layer, table)`: atomically purges the resolved checkpoint AND drops the target table so the next run re-ingests from scratch — prevents the half-purged state (duplicates or incomplete backfill).
- (Plan 27) CDC Type 1 upsert for streaming tables: `write_stream_table(write_mode="upsert", keys=[...])` runs `foreachBatch` — each micro-batch is deduplicated on the merge keys (batch op, bounded state) then `MERGE INTO` the target (`matched → UPDATE SET *`, `not matched → INSERT *`). Cross-batch uniqueness is guaranteed by the target itself (no streaming state) and replayed micro-batches are idempotent (neutralizes foreachBatch's at-least-once semantics). First micro-batch creates the target if absent (local: path write + metastore registration; Databricks: `saveAsTable`). FakeBackend simulates last-write-wins per key.
- (Plan 27) Streaming backend primitives in `SparkBackend`: `read_table_stream` (readStream.table), `read_source_stream` (delta/text only — schema-less formats rejected), `write_stream_table` (Delta append + checkpoint, `available_now`/`interval:` triggers, internal `awaitTermination()`, Connect-v2 failures re-raised with an actionable message — never swallowed) and `default_checkpoint_root()` (local: `{warehouse}/_checkpoints`; Databricks: None → explicit config required). `write_table` now rejects streaming DataFrames (the batch path's `isEmpty` guard would silently skip the write). Local twin `writer.write_stream_dataframe_local` registers the metastore entry only after termination and only if a `_delta_log` exists (zero-row first run → skip + warning). FakeBackend mirrors + `_streams` recorder; new `streaming` pytest marker; `.spark-warehouse/` added to `.gitignore` (pre-existing gap).
- (Plan 27) IR carries the streaming surface: `ParsedTable.streaming` and `ParsedSchema.materialization` (descriptive — feeds `describe_schema`/lineage). JSON Schema gains the top-level `materialization` property (`MaterializationDef`: string shorthand or dict with `trigger`/`checkpoint`/`write_mode`/`keys`) and a `streaming` boolean on `TableDef`; `schemas/skifer-pipeline.schema.json` regenerated.
- (Plan 27) Streaming-tables YAML surface: per-table `streaming: true` flag and top-level `materialization:` block (string shorthand or dict `{type, trigger, checkpoint, write_mode, keys}`; defaults `trigger: available_now`, `checkpoint: auto`, `write_mode: append`; `keys` required with `write_mode: upsert`). Load-time fail-fast validation of every streaming incompatibility (aggregated errors): strict bijection `streaming` ⇔ `materialization: streaming_table`, single streaming table per schema, stream-static joins restricted to `inner`/`left` with the stream as join base, streaming file sources restricted to `delta`/`text`, and rejections for `dev_limit`, `preprocess.qualify`, `drop_duplicates_on` (error guides to `write_mode: upsert`), loaders, JDBC sinks, `materialized_view`, and streaming inside `partials:` children. New constants `VALID_STREAMING_SOURCE_TYPES`, `VALID_MATERIALIZATION_TYPES`, `DEFAULT_STREAMING_TRIGGER` in `core/constants.py`. Execution wiring lands in later Plan 27 phases.
- `scripts/release.py`: added a guarded release helper that bumps the package version, promotes the changelog, creates the release commit and tag, and pushes `main` plus the tag to trigger TestPyPI and PyPI.

### Changed

- (Plan 29) End-to-end run identity. The `run_id` minted by
  `run_process_to_table` / `run_from_yaml` now travels down to the certification
  registry, so one pipeline run is one audit identity instead of two correlated
  only by a shared `trace_id` — a link that disappeared whenever traces were not
  exported. **This changes persisted data:** `run_id` values written by earlier
  versions do not follow this convention, so any external tool correlating on
  them must be checked. The id is minted on the business path, never derived
  from the trace context, and tests pin it identical whether tracing is on or
  off (the slice 5.2 guarantee). `PipelinePatterns.run_process_to_table` and
  `run_from_yaml` accept an optional `run_id` and still mint one when called
  standalone. `SkiferEngine.run_from_yaml` no longer keeps a second,
  traced-only copy of its own flow: the two had already drifted apart, and
  `skifer.schema.load` is now emitted from the single implementation.
- `core/json_schema.py`: the `source.type` enum is now derived from `VALID_SOURCE_TYPES` (single source of truth); `schemas/skifer-pipeline.schema.json` regenerated accordingly.
- `core/interpreter.py`: the select path now goes through the IR (`apply_op` + `_parse_op`) instead of the deprecated string wrappers.
- **BREAKING** (Plan 26): `SparkBackend` moved from `skifer.backends.spark` to `skifer.core.spark_backend`; the `backends/` package is gone. `SparkBackend` is now exported at package top level (`from skifer import SparkBackend`, lazily so Spark-less installs keep working).
- `agentic/orchestrator.py`: the exporter now targets Spark/Databricks only — it emits a Databricks Asset Bundle, an Airflow DAG with the Databricks operator, or a Python script; the Snowflake/BigQuery Airflow branches are gone and `format="auto"` resolves to `script` for non-Spark/local backends.

### Removed

- **BREAKING** (Plan 26): removed the legacy non-Spark backends `skifer.backends.sql_base`, `skifer.backends.snowpark` and `skifer.backends.bigquery`, along with the `snowflake` and `bigquery` optional dependency extras. The product is Spark/Databricks-only; `pip install skifer[snowflake]` / `[bigquery]` are no longer available.
- **BREAKING** (Plan 26): removed the `Backend` Protocol (`skifer.core.backend`), the `backend=` kwarg of `SkiferEngine.__init__`, the `capabilities` mechanism (SparkBackend property + `temp_view` gate on partials materialization) and the deprecated `_patch_connect_debugging`/`_patch_connect_user_context` engine shims. The engine is now Spark-native and always builds a `SparkBackend`; tests inject doubles via `engine._backend` (duck typing). `VALID_SOURCE_TYPES` moved to `skifer.core.constants` (old import path `skifer.core.backend` is gone).
- **BREAKING** (Plan 26): removed the dead string-op module `core/operations.py` and the deprecated `SparkBackend.apply_operation` / `build_filter_expression` wrappers.

### Fixed

- **`<rule>` leaked into the data dictionary as if it were a table.** Attributing a
  rule-made column to `<rule>` fixed a lineage edge that named a source column no table
  contains, but the dictionary indexed that marker like any other source: it listed a
  field nobody can query, and let one column name fill two of the three nearest-name
  suggestion slots. The marker is now excluded from the index while the provenance it
  carries is kept on the real entry. Found by driving `DictionaryAgent` for the first
  time since Plan 26.
- Nearest-name suggestions from `DictionaryAgent.lookup()` are deduplicated. Only three
  are offered, and a column name present in two tables used to take two of the slots with
  the same word, which reads as a bug and costs the reader a real alternative.
- **Column lineage named a source column that does not exist.** `RuleAnalyzer` read output
  columns only from `withColumn`, so a `projection` rule — the default kind, and the one
  the documentation teaches — declared none. The lineage tracker then attributed the
  column the rule created to the primary source table, reporting
  `raw_orders.order_class -> order_class` for a column `raw_orders` has never had. The
  analyzer now reads the returned `{name: Column}` mapping, directly or through a local
  name, while ignoring a dict that is not returned so a lookup table inside a rule is not
  mistaken for output columns. A select on a rule-made column is attributed to `<rule>`
  instead of to a table. Nothing caught this because every analyzer and tracker test wrote
  `transform` rules; found by writing example 11.
- **A column operation missing an argument crashed with `IndexError: tuple index out of
  range`**, raised inside a lambda, naming neither the operation nor the column. The op
  catalog has declared an arity for every column operation since it was written, and
  nothing ever read it. `substring:1`, `split:sep`, and a bare `cast`, `round` or
  `to_date` now say which operation was called, how many arguments it needs, and how many
  it received. The realistic way to hit this is an unquoted YAML flow sequence, where
  `[substring:1,7]` is two items rather than one operation, so the message says that too.
  Found by writing example 07.
- **Local Spark workers now run the interpreter that started them.** `get_spark_session()`
  pins `PYSPARK_PYTHON` in local mode, as the test conftest already did privately. Without
  it, Spark launched whatever `python3` the PATH offered, and a system Python of a
  different minor version killed the job with `PYTHON_VERSION_MISMATCH` — a message that
  says nothing about the virtualenv the user is in. It only bit outside pytest, which is
  exactly where a reader runs an example.
- **Ten tests depended on a Spark session they never requested.** They build a column
  with `F.col(...)`, which needs a live JVM context, but declared no `spark` fixture:
  they passed only because another test in the same process had started the session
  first. Running any of them alone, filtering with `-k`, or distributing the suite would
  have failed them — including both `allow_raw_sql` governance guards, exactly the tests
  one wants to be able to verify in isolation. Measured while sizing a two-tier CI: the
  2201 tests that need no Spark run in 27s, against 158s for the whole suite.
- Removed two stale developer handoffs from `docs/`. One of them describes a fix in
  `src/skifer/backends/spark.py`, a module deleted three plans ago, so it
  now misinforms anyone who finds it; both duplicate what the changelog and the git
  history already record. Their filenames also contain a colon, which Windows cannot
  check out.
- **The documentation site published 50 internal working artifacts.** Forty-eight
  implementation plans, a point-in-time feature review, and two developer handoffs
  each shipped at their own public URL. They were absent from the navigation menu,
  which is not the same thing: MkDocs builds every page under `docs/`, and omitting
  one from `nav` hides the menu entry while still publishing the page. They are now
  excluded from the build itself, and a test asserts the built site contains none of
  them. Found by running `mkdocs build --strict` for the first time.
- The strict documentation build now passes with zero warnings, from eleven. Ten came
  from plan documents linking to source files; the last was an onboarding link to the
  project README, which does not exist on the published site. Links that leave the
  site now point at the repository.
- **Semantic queries now work with `catalog: null`.** A two-part model table such as
  `gold.fact_orders` was always expanded to a three-part name, producing a literal
  `None` catalog in local Spark SQL. The resolver now keeps the valid two-part name
  when no catalog is configured. Found by executing the local semantic onboarding
  example.
- Unknown names rejected by the domain-aware semantic planner now include nearest
  declared-name suggestions, matching the legacy single-model resolver's refusal
  contract instead of returning only the complete available-name list.
- **Certified publication now works in local mode.** It never had. Two distinct faults
  hit the very first staging write: the staging FQN is deliberately built unquoted —
  it is a stored identity, recorded in every run event — while the local writer only
  parsed the backtick-quoted form; and `_skifer_staging` was never created,
  because the engine ensures only the *target* schema and the staging area is an
  implementation detail of publication. Plan 29 feature 1 was therefore unusable on
  the documented development path, and the test suite missed it because those tests
  use fake backends. Found by running a real pipeline as an example.
- `SkiferEngine.backend` is now public. Building a `DataMonitor` is documented,
  and the documentation instructed readers to reach into `engine._backend` in twelve
  places across four pages — which quietly made renaming that attribute a breaking
  change for everyone who followed the docs.
- (Plan 29, slice 9.8) A scenario whose own code raises is now reported as `error`,
  not as a governance `refused`. A crashed scenario has observed nothing — the system
  did not refuse anything — and labelling it a refusal let a broken scenario wear the
  costume of a governance decision, so a future scenario that legitimately expects a
  refusal could have passed on a crash.
- (Plan 29, slice 9.4) `precondition_hash` no longer hashes wall-clock time. It covered
  the whole report, `evaluated_at` and each outcome's `observed_at` included, so the
  hash changed on every re-evaluation even when the state and every verdict were
  strictly identical. Since an approval is bound to that hash and the report is
  re-evaluated immediately before execution, **an approval could never validate against
  a fresh report** and supervised mode was unusable. A fixed test clock hid this
  completely. The hash now covers what an approver actually approves — the rules, their
  verdicts and the observed state — while the separate expiry check remains what bounds
  how long an approval stays good.
- (Plan 29, slice 9.2) Capability argument validation now stops at the bound it
  declares. Reporting a breached `maxItems` and then descending into every element
  bought the caller work proportional to what they sent rather than to the limit the
  capability declared: 200k surplus items produced 200k error messages and an 8 MB
  exception string, and 200k unknown keys did the same. An over-length array is now
  refused without being walked, and the error list is capped at 32 entries plus an
  explicit truncation notice. Arguments reach this boundary from the caller, so the
  work they can buy is part of the attack surface.
- (Plan 29, slice 9.1) The capability validator no longer crashes on a deeply nested
  document. Nesting is a size, and section 8 of the plan requires sizes to be bounded:
  without an explicit ceiling the recursive schema walk and the secret-key scan blew
  the interpreter stack at a nesting depth of 497 under the default recursion limit —
  a few kilobytes of YAML — and raised `RecursionError` instead of returning a verdict,
  breaking the validator's own contract that only a wrong argument type may raise. A
  document that crashes the validator is a document that was never validated, so the
  refusal must be an answer rather than a crash. Input schemas are now bounded to 8
  levels and documents to 16.
- (Plan 29, slice 8.6) An unchanged zero-millisecond baseline is no longer reported
  as a regression. `after >= before * (1 + ratio)` is satisfied by every
  non-negative observation once the baseline is zero, so two identical windows of
  sub-millisecond queries produced a `regressed` verdict and a human review
  request — on exactly the queries an optimization made too fast to measure, and
  while the same result's own reason line reported the ratio as improved. A zero
  baseline is now compared against zero directly, reports `ratio=undefined`
  instead of a misleading number, and can never read as improved.
- (Plan 29, slice 8.6) `skifer adaptive evaluate` refuses a `--store` path that
  does not exist instead of creating it. `sqlite3.connect()` creates the file it is
  given, so a mistyped path produced an empty store, an `evidence_unavailable`
  verdict indistinguishable from a genuine retention purge, exit code 0, and a
  proposal moved to `measured` on a measurement that never ran.
- (Plan 29, slice 8.4) Proposal artifacts now record paths relative to the proposals
  root instead of absolute ones. An absolute path pinned a reviewed artifact to the
  checkout that produced it: it broke as soon as a proposal was read elsewhere, wrote
  the local filesystem layout — username included — into a file a human is meant to
  read, and made two runs of identical content differ byte-for-byte even though the
  `proposal_id` is content-derived. `ProposalGenerator.resolve()` turns a recorded
  path back into a usable one and refuses anything absolute or escaping the root.
- (Plan 29, slice 7.3) The MCP query tool now advertises the bounds its service
  actually enforces. The input schema was built from the hard ceilings, so a service
  configured with a tighter `ServiceLimits` promised agents up to 1000 rows and 50
  filters while refusing anything above its own budget. The refusal was fail-closed,
  but a schema that overstates what may be asked is one an agent cannot plan against.
  `MCPTools` now derives its descriptor and its argument validation from
  `service.limits`, and deliberately has no fallback when that budget is absent.
- (Plan 29, slice 7.2) MCP resource discovery is now filtered by the caller's scopes.
  `list_resources` and `list_resource_templates` took no request context at all, so an
  agent holding no scope enumerated the whole governed surface and learned which scope
  opened each endpoint — reconnaissance the plan's definition of done explicitly rules
  out ("resources filtrées par scopes"). Both now require a `RequestContext`, the server
  resolves the caller on every handler rather than only on read, and a non-context
  argument fails closed instead of listing everything.
- (Plan 29, slice 7.1) **SQL injection in Spark SQL string literals.** Doubling the
  single quote is not sufficient: with Spark's default
  `spark.sql.parser.escapedStringLiterals=false` a backslash escapes the next
  character, so a value ending in `\` turned the closing quote into an escaped
  quote and let the rest of the value be parsed as SQL. Reproduced against a real
  local Spark session: `contract_id="x\\"` with `version=" OR 1=1 --"` returned every
  row of `contract_definitions` instead of none. New `escape_sql_string()` in
  `core/sql_compiler.py` doubles backslashes before quotes and is now used by
  `sql_literal()` and by all ten interpolated literals in `core/spark_backend.py`.
  The agent-reachable contract read added by this slice is what made the flaw
  reachable from outside; the other sites shared the same pattern.
- (Plan 29, slice 7.1) `AgentReadyDataService.get_contract` now shape-checks
  `contract_id` and `version` at the boundary before they reach storage — they are
  the only free text an external caller puts on a path that ends in SQL — and does
  so before touching the certification store.
- (Plan 29, slice 6.6) The planner now emits the exact mandatory fail-closed
  message for an `unknown` relationship cardinality, instead of prefixing it
  with a relationship name.
- (Plan 29, slice 6.4) A declared `many_to_many` relationship is now refused
  with its own cause instead of being reported as an unknown cardinality. The
  shared message told the author to "declare or certify uniqueness" for a
  cardinality they had already declared, pointing them at the wrong thing; it
  now names the fanout and the two real ways out (a bridge model, or restating
  the relationship at a grain where it is many_to_one).
- (Plan 29, slice 6.2) A proposed relationship candidate no longer guesses
  `to_entity`. It named an entity of the *target* model, which the pipeline
  cannot see, and was resolved from this model's projected outputs matched by
  bare column name — so a join onto a column called `id` silently attributed
  the target side to a local entity and emitted a confidently wrong candidate,
  while a non-colliding join was rejected outright. `to_entity` is now left for
  curation, the joined physical table is recorded next to the suggested
  `to_model` so the guess can be checked, and the relationship is named after
  the local foreign entity (matching the plan's own `orders_customer` example).
- (Plan 29, slice 6.1) `SemanticValidator` now rejects a malformed
  `entities:`/`relationships:` block instead of silently ignoring it. The parse
  layer skips any entry that is not a mapping, so `entities: [order, customer]`
  validated green with zero entities — an easy mistake to make since the
  *catalog summary* stores exactly that bare-name shape while a model must
  declare full mappings.
- (Plan 29, correctifs ultrareview 2 — merged_bug_001/bug_003) The
  preflight+recheck double-call to `enforce_certification_gate()` (`SemanticEngine.
  query()`/`create_view()`, `GenBIAgent._process()`/`_execute_query()`) no longer
  duplicates observability side-effects: the stdout warn banner and the override
  audit log (`access_policy.evaluate(..., count_warning=...)`) are now gated by
  the same `count_warning` flag as the counter, and the agent's recheck call now
  passes `count_warning=False` (it previously used the default `True`, doubling
  `certification_warning_count` and the banner on every agent `ask()`).
  `enforce_certification_gate()` also now returns the `certifications` tuple it
  already fetched, so `create_view()` reuses the recheck's tuple for the DDL
  provenance comment instead of re-fetching a 3rd time (store round-trips per
  certified view: 3 → 2).
- (Plan 29, correctifs ultrareview 2 — bug_010) `SqliteCertificationStore`
  now opens its connection with `check_same_thread=False`, matching the
  existing `SqliteHistoryStore` precedent. Databricks Model Serving builds the
  store on the `load_context()` init thread and queries it from `predict()`
  worker threads; without this flag every lookup off the init thread raised
  `sqlite3.ProgrammingError`, which `SemanticEngine._get_certification`
  silently degraded to `MISSING`, denying every request the moment
  `semantic_certification_policy` left `off`.
- (Plan 29, correctifs ultrareview 2 — bug_002) `PublicationCoordinator.publish()`
  now propagates the real state returned by `quarantine_staging()` instead of
  hardcoding `"QUARANTINED"`, so a quarantine-write failure (`CHECK_ERROR`) is
  reported accurately instead of pointing operators at a snapshot that was
  never written.
- (Plan 29, correctifs review Feature 1 — F09) `quarantine_staging()` now
  passes failed row-level check results into real quarantine tagging, rejects
  reserved metadata-column collisions before writing, and records quarantine
  run events against the physical `target_fqn`. The pure-Python test fake only
  evaluates `IS NULL`/`IS NOT NULL` predicates by design; the full SQL predicate
  grammar is validated through the real Spark backend.
- (Plan 29, correctifs review Feature 1 — F08) `promote_staging()` now
  resumes runs already recorded in `PROMOTING`, making crash recovery
  idempotent across the pre-write marker and the target overwrite; the
  `PROMOTED` fast path also performs the orphaned staging cleanup that was
  previously skipped. Strict `resume(run_id)` support without a caller-provided
  `definition` remains future work because it requires a broader store read API
  such as `get_contract(...)`.
- (Plan 29, correctifs review Feature 1 — F07) `run_process_to_table()`
  now wires certified publication into the batch write path when a schema
  declares `data_product`: the engine stages, validates, records check results,
  then promotes or quarantines. Schemas without `data_product` keep the legacy
  direct `_write_dataframe()` path unchanged, and certified publication now
  refuses streaming, JDBC sinks and materialized views explicitly. Crash
  recovery via `.resume()` remains future work.
- (Plan 29, correctifs review Feature 1 — F05) `promote_staging()` now
  refuses promotion when recorded critical checks for the staged run are in
  `FAIL` or `ERROR`, while keeping the current non-blocking behaviour when no
  checks are recorded yet; real check execution/wiring before promotion
  remains future work.
- (Plan 29, correctifs review Feature 1/3 — F06) Certified-publication run
  events now key `RunEvent.dataset` on the physical `target_fqn`, aligning
  publication history with the semantic certification gate lookup on
  `model["table"]`. Preserving `data_product_id` explicitly on `RunEvent`
  remains future work because it requires a persistent schema migration.
- (Plan 29, correctifs review Feature 3 — F13/F16) Serving now serializes
  policy denials with explicit decision/reasons/recommended action in the
  assistant text response, and the serving/agentic docs now state clearly that
  request `consumer_id`/`user_id`/`trace_id` are unauthenticated client
  metadata that must not grant scopes or sensitive authorization.
- (Plan 29, correctifs review Feature 3 — F12) Semantic view provenance
  comments now persist the certification state actually observed at view
  creation time, including the active policy mode plus the certified
  contract version and definition hash when available, instead of echoing
  empty dependency placeholders.
- (Plan 29, correctifs review Feature 3 — F11) Certification overrides now
  require the explicit `certification_override` break-glass scope on the
  requesting consumer context, require the override actor to match that
  context's `consumer_id`, reject naive `expires_at` datetimes with a clear
  validation error, and emit structured WARNING/ERROR audit logs for accepted
  and rejected attempts. A dedicated durable audit store remains future work
  because it would introduce a new persistence subsystem beyond this bounded
  fix.
- (Plan 29, correctifs review Feature 3 — F10) Semantic certification now
  fails closed when a semantic model resolves no physical dependency: empty
  dependency sets are evaluated as a missing certification instead of being
  implicitly allowed.
- (Plan 29, correctifs review Feature 3 — F03) Aligned the agent query path
  with SemanticEngine's certification preflight/recheck sequence so known
  denials happen before query resolution and races are caught before SQL
  execution.
- (Plan 29, correctifs review Feature 3 — F04 partiel) Semantic certification
  decisions now reject expired certifications and promoted runs with critical
  failed/error checks; `HASH_MISMATCH` and `UNAUTHORIZED` remain documented
  future work because the semantic model schema does not yet expose expected
  contract versions or authorized scopes.
- (Plan 29, correctifs review Feature 3 — F02) Completed Delta certification reads for promoted status, history and check results, and wired a default SQLite certification store into agent and serving SemanticEngine entry points.
- (Plan 29, correctifs review Feature 3) Corrected the semantic certification config example to keep governance keys at the environment root, moved `GenBIAgent` SQL printing after a successful certification gate, and fixed `SemanticEngine.certification_warning_count` so warn-mode queries increment once per logical request instead of once per recheck.
- (Plan 29, correctifs ultrareview feature 0) The semantic catalog no longer
  churns between runs: auto-derived tags were built with `list(set(...))`, whose
  order varies with Python hash randomization, so `semantic_catalog.yaml` was
  rewritten with reshuffled tags on every `--promote`. They are sorted now.
- (Plan 29, correctifs ultrareview feature 0) `semantic/persistence.py` (new)
  holds the shared `write_yaml_atomic()` and `build_catalog_entry()` helpers.
  The atomic-write pattern was copy-pasted across `cli.py`, `semantic/sync.py`
  and `semantic/draft_builder.py`, and the catalog-entry builder was duplicated
  verbatim between `cli.py` and `SemanticBuilder` — letting the LLM path and the
  CLI promote path drift into different catalog shapes for the same model.
  `SemanticBuilder._write_yaml()` now writes atomically too, via `safe_dump`
  instead of `yaml.dump`.
- (Plan 29, correctifs ultrareview feature 0) `SemanticSynchronizer` now merges
  every model-level key found in the base draft, the curated model or the
  candidate, instead of a fixed allow-list. The allow-list silently dropped
  `synonyms` — which `SemanticBuilder.build_from_projection()` is explicitly
  allowed to write — as well as hand-authored keys such as `tags` or
  `base_filter`, which made `--promote` refuse permanently after the first
  pipeline drift and broke the documented enrichment loop.
- `tests/test_schema_loader.py`: `test_parse_schema_malformed_yaml_raises` had lost its `def` line, so its body ran at import time and the malformed-YAML case was never actually exercised as a test. Restored.
- `core/json_schema.py`: the top-level `partials:` block (Plan 25) was never registered in the generated JSON Schema — with `additionalProperties: false`, any YAML using partials failed editor-side/`skifer validate` validation. Added `partials` property + `PartialDef`.
- `ceil`, `between` and `not_between` now work on the Spark execution path: they were listed in the operator catalog but missing from the SparkBackend dispatch tables (their only implementations lived in the removed legacy modules), so a YAML `between:` filter raised "Unknown filter operator" at runtime.
- `expr:`, `lit:`, `nvl:`, `coalesce:` and `when:` values containing commas are no longer truncated by IR parsing (e.g. `expr:concat(col1, '_x')` reaches Spark whole).
- `in`/`not_in` filter values with leading/trailing spaces now log the same warning on the Spark path as the legacy string path did.

### Security

- (Plan 29, slice 0.6) `SemanticValidator` now rejects dimension and metric
  names that are not plain SQL identifiers, and the LLM enrichment boundary
  rejects them before they are ever merged. `QueryResolver` interpolates those
  names directly as SQL aliases (`<expr> AS <name>`), so a name such as
  `n FROM secrets; DROP TABLE audit--` previously passed every check and
  reached the emitted SQL — an injection path through LLM-authored metric
  names. New metric aggregate types are validated at the same boundary.
- (Plan 29, slice 0.5) The `skifer` CLI now exposes
  `semantic sync PIPELINE --check|--write-draft|--promote` and
  `semantic validate PIPELINE MODEL` for deterministic semantic-layer CI.
  The sync commands return stable CI exit codes (`0` current/success,
  `1` technical or validation failure, `2` drift detected or draft updated,
  `3` conflict/rename suggestion), keep `--check` strictly side-effect free,
  refuse `--promote` on sync conflicts or semantic validation failures, and
  promote via the existing `SemanticEngine.update_catalog()` catalog path.
  The validation command cross-checks a semantic YAML against the pipeline's
  projected outputs and surfaces explicit contract/projection mismatch errors.
  `--promote` is a no-op on an already-current curated model (re-promoting an
  unchanged pipeline would otherwise have overwritten it with the un-curated
  draft) and refuses outright, rather than writing, whenever promotion would
  drop content a human authored in the curated model.
- (Plan 29, slice 0.4) `semantic.sync` now performs a report-first,
  metadata-driven three-way synchronization between the last managed draft,
  the curated semantic model and a freshly regenerated candidate draft. It
  preserves curated edits, reports deterministic auto-applicable changes,
  flags blocking conflicts such as removed generated dependencies and
  narrowing type changes, emits explicit rename suggestions without applying
  them, and only writes an updated `.drafts/<model>.yaml` when the full report
  is conflict-free and unambiguous. A grain change is detected by comparing the
  `contract.grain` now persisted in the draft's `metadata.grain`, so it is
  caught on every pipeline shape rather than only on aggregate pipelines.
- (Plan 29, slice 0.3) `semantic.SemanticDraftBuilder` now turns a
  `ProjectedSchema` plus the parsed contract/semantic seed into a managed,
  byte-stable semantic-model draft under `semantic_models/.drafts/`. The
  builder is fully deterministic and LLM-free, emits declared or structurally
  safe dimensions only, projects declarative aggregate measures as metrics,
  carries the source contract/version plus projection definition hash, writes
  atomically via same-directory temp files + `os.replace()`, and refuses to
  overwrite an unrecognized curated file without explicit promotion. Aggregates
  the pipeline supports but the semantic layer does not (`stddev`, `variance`,
  `sum_distinct`, `approx_count_distinct`, `first`, `last`) are listed under
  `metadata.unmapped_measures` for curation rather than failing the draft, and
  the `row_count` fallback emitted when no measure maps is flagged
  `needs_curation` so it is replaced before promotion.
- (Plan 29, slice 3.4) Semantic agent query execution now enforces the
  certification gate before backend SQL, supports bounded attributed
  `CertificationOverride` objects for direct SemanticEngine calls, and exposes
  an in-memory warning counter for warn-mode migration windows.
- (Plan 29, slice 3.3) Semantic query and view execution now support an
  optional certification gate with pre-compile and pre-execute checks plus a
  non-sensitive definition identity comment on created views.
- (Plan 29, slice 3.2) Semantic dependency resolution now returns a stable,
  typed tuple of deduplicated physical datasets from the current semantic-model
  schema, preserving a forward-compatible contract/hash shape without
  speculative multi-source handling.
- (Plan 29, slice 3.1) Semantic certification policy now evaluates purely as
  `ALLOW`, `WARN`, `DENY` or `REQUIRE_HUMAN` from an explicit consumer context
  and policy mode, before any SQL or LLM work begins.
- (Plan 29, slice 1.5) A staged certified-publication run can now promote its
  validated snapshot through `PROMOTING` to `PROMOTED`; retries after promotion
  are idempotent and do not rewrite the target.
- (Plan 29, slice 1.4) Row-level quarantine now retains deterministic violation
  predicates only for failed row checks and rejects collisions with reserved
  quarantine metadata columns.
- (Plan 29, slice 1.3) Blocking staging validation can now snapshot an exact
  quarantine copy before cleanup. A quarantine-write error preserves staging
  and records `CHECK_ERROR`, leaving the certified target untouched.
- (Plan 29, slice 1.2) Certified publication runs now allocate a validated UUID
  and an exact `_skifer_staging` target, persist `STARTED`/`STAGING`/`STAGED`
  events, and use dedicated backend operations for staging I/O and cleanup.
- (Plan 29, slice 2.4) Certification stores expose current certification and
  run history. The optional Unity Catalog mirror writes only allowlisted,
  non-sensitive tags and returns `SYNC_ERROR` on permission failure without
  invalidating the local certification.
- (Plan 29, slice 2.3) `LoadFreshnessCheck` now measures time since the latest
  successful promotion, separately from event-time `DataFreshnessCheck`/
  compatible `FreshnessCheck`. Both accept an injected UTC clock; future values
  under a controlled clock produce `ERROR` rather than an ambiguous SLA result.
- (Plan 29, slice 2.2) SQLite certification persistence now records immutable
  contract, run and check events with schema migration and event-ID idempotence.
  A targeted Delta adapter uses the same append-only contract through Spark
  backend primitives; it does not introduce another processing backend.
- (Plan 29, slice 2.1) `canonicalize_contract()` turns the typed pipeline
  contract into a versioned SHA-256 identity with deterministic JSON; human
  descriptions and ownership stay queryable but do not invalidate a certified
  definition. A loss-aware ODCS 3.1 export maps fundamentals, schema, quality,
  owner and reader role, and reports every unmapped source field explicitly.
- (Plan 29, slice 1.1) Data-quality results now carry an explicit `PASS`,
  `FAIL`, `ERROR` or `SKIPPED` status while retaining the `passed` compatibility
  property (`True` only for `PASS`). Row-level contracts expose safe SQL
  violation predicates; unknown operators are `SKIPPED`, and backend exceptions
  are `ERROR`, so neither can silently satisfy a critical contract.
- (Plan 29, slice 0.2) `semantic.OutputProjector` derives a deterministic,
  Spark-free `ProjectedSchema` from the parsed pipeline IR. It records output
  provenance, safe physical-type inference and explicit unknown diagnostics for
  raw SQL, Python rules, heterogeneous coalescing and `keep_all_columns`; its
  canonical hash intentionally excludes source paths and timestamps.
- (Plan 29, slice 0.1) Pipeline schemas can now declare optional
  `data_product`, `contract` and `semantic` metadata. The loader validates the
  output contract and semantic references before Spark starts, and the immutable
  IR exposes typed product, output-field and semantic-seed objects. The pipeline
  JSON Schema is updated at the same time; schemas that do not use these blocks
  retain their existing behaviour.

### Documentation

- Added runnable examples 21–22 for Spark-free `AgenticHub` routing through the
  dictionary and lineage agents, plus live `QualityAgent` checks that distinguish
  failures from execution errors and persist two runs in disposable local history.
- **Removed `example/`, the Jupyter notebook directory that nothing executed.** It held
  the pre-Plan-33 version of the same idea as `examples/`, and having both invited the
  question of which one to trust. Two of its seven notebooks imported
  `skifer.backends.spark`, deleted by Plan 26, so they had stopped working
  without anything noticing; the rest had not been run in months. Two published
  documentation pages still sent readers to it, alongside the tested examples they had
  just gained. Everything it demonstrated that no example covered is now covered.
- Added runnable examples 19–20 so that removal loses nothing: static rule redundancy
  analysis through both `RuleAnalyzer` and `engine.explain_rules()`, and offline
  `BuilderAgent` drafting with its validation refusal and orchestration export. Example 20
  prints the generated Airflow task body, which is a TODO scaffold — a DAG copied without
  reading it schedules nothing and reports success.
- (Plan 34, phase 6) Wired all eighteen runnable examples into their relevant feature
  documentation and routed the getting-started next steps through the demonstrated output.
- (Plan 34, phase 5) Added dependency-free runnable examples 16–18 for the scoped
  read-only MCP boundary, explainable adaptive Gold proposals and review exit codes,
  and exact approvals with idempotent capability execution and audited compensation.
- (Plan 34, phase 4) Added Spark-free runnable examples 13–15 for deterministic
  semantic projection and sync exit codes, domain-graph join planning with fanout and
  calendar guardrails, and names-only GenBI interpretation through an offline stub provider.
- (Plan 34, phase 3) Added locally runnable examples 10–12 for failed certified
  publication with row-tagged quarantine, static lineage and data-dictionary output,
  and dependency-free runtime tracing with redaction and exporter-failure isolation.
- (Plan 34, phase 2) Added locally runnable examples 08–09 for materialized-view SQL
  compilation and definition hashing without Spark, plus finite streaming-table runs that
  reuse an automatic checkpoint and apply CDC type 1 upserts; both print real guardrail refusals.
- (Plan 34, phase 1) Added locally runnable examples 05–07 for Python business rules,
  joins and declarative aggregates, nested partials and their debug materialization modes,
  and JSON sources shaped through grouped filters, a registered loader, development limits,
  and pre-aggregation columns; each example also prints a real framework refusal.
- (Plan 34, phase 0) The example suite now checks that each example still prints what its
  README promises, not merely that it exits zero. An example that keeps running while
  printing something else drifts without failing anything, which is the failure mode
  documentation examples actually have. Every example gained a uniform
  `What you should see` section, and each is still run in its own subprocess: a shared
  process would let one pass because a previous one had started Spark or registered a
  rule for it.
- (Plan 33) Brought `CLAUDE.md` and `AGENTS.md` back in step with the code, as the release
  checklist requires: the `docs` extra and the strict site build are listed among the
  commands, Plan 29 is recorded as fully merged and Plan 33 added, and both files stated
  that "all tests mock Spark" — which is false for 182 of them, and is precisely the
  belief that let five defects ship on the documented local path.
- (Plan 33) Added a `docs` optional dependency group. The repository has shipped an
  MkDocs configuration and a release checklist requiring the site to be up to date,
  while declaring neither MkDocs nor its theme anywhere — so nobody could build or
  preview the documentation without guessing the toolchain. `pip install -e ".[docs]"`
  now makes `mkdocs build --strict` runnable.
- (Plan 33, phases C–D) Split MCP, supervised adaptive Gold, and governed
  capabilities out of the conversational agentic guide; added an end-to-end
  governance narrative; made every reader-facing documentation page reachable
  through grouped MkDocs navigation; added a regression test for orphan pages;
  and moved root-level workflow notes into `docs/contributing/`.

## [1.4.1] - 2026-07-02

### Fixed
- `.github/workflows/publish-pypi.yml`, `.github/workflows/publish-testpypi.yml`: switched package publishing from OIDC trusted publishing to explicit PyPI API tokens (`PYPI_API_TOKEN` and `TEST_PYPI_API_TOKEN`) so the workflows can publish with GitHub environment secrets.
- `backends/spark.py`: `SparkBackend.table_exists()` now probes table existence with `SHOW TABLES ... LIKE` instead of `SHOW COLUMNS IN <table>`, avoiding noisy Databricks `TABLE_OR_VIEW_NOT_FOUND` stacktraces during first-run sandbox clone resolution while preserving temp-view fallback behavior.

### Changed
- `.gitignore`: ignore the local `.pip-cache/` directory and stop tracking the generated macOS `example/.DS_Store` file.
- Repositioned project documentation and package metadata around the stabilized Spark/Databricks product path, with local PySpark/Delta as the development runtime. Legacy non-Spark connector references are now treated as compatibility details rather than the public positioning.
- Initialized the repo-local multi-agent workflow scaffold (`init.md`, `.agents/`, `.gstack/`, `features/_template/`, `adr/`, `tools/brain/`) adapted from a prior internal workflow and aligned to the Spark/Databricks stabilization focus.

## [1.4.0] - 2026-07-01

### Added
- `docs/core.md`, `CLAUDE.md`, `AGENTS.md` (Plan 25, phase 25.5): documented the `partials:` block, its load/execution semantics, the `intermediate_mode` run-global materialization knob, and added Plan 25 to the roadmap index.
- `core/interpreter.py` (Plan 25, phase 25.4): `intermediate_mode: table` — a partial's output is written as a physical intermediate table for restartability / perf inspection. The FQN is deterministic and env-aware: a dedicated `skifer_partials` schema suffixed with the sandbox suffix (`catalog.skifer_partials_<suffix>.<alias>`), so concurrent interactive runs never collide. The schema is auto-created; the parent keeps using the DataFrame directly.
- `core/backend.py`, `backends/spark.py`, `backends/snowpark.py`, `backends/sql_base.py`, `core/interpreter.py` (Plan 25, phase 25.3): `intermediate_mode: temp_view` for nested partials. New optional `Backend.register_temp_view(df, name)` capability (`"temp_view"`) — implemented for Spark (`createOrReplaceTempView`), Snowpark (`create_or_replace_temp_view`) and SQL backends (`CREATE OR REPLACE TEMPORARY VIEW`, not advertised by default). The partial's view/table name is derived from its `alias` (sandbox-suffixed), so no `debug_name` is needed. Backends lacking the capability fall back to inline with a warning; the parent always keeps using the DataFrame directly.
- `core/interpreter.py`, `core/core.py`, `core/patterns.py` (Plan 25, phase 25.2): `partials:` are now executed inline. Each nested schema runs through the full `process_schema` path (recursively) before source loading and is injected under its alias — a `join` can therefore depend on a column produced by a business rule inside the partial, with no intermediate table. A run-global `intermediate_mode` param (`inline` default; `temp_view`/`table` land in later phases) threads from `run_from_yaml(params=…)` → `run_process_to_table` → `process_schema`. A partial alias supplied via `dataframes_in` overrides execution.
- `core/schema_loader.py`, `core/ir.py` (Plan 25, phase 25.1): declarative `partials:` block — a schema can reference nested YAML sub-transformations by `{alias, path}`. Child schemas are recursively loaded (relative paths resolve from the parent YAML directory), inherit the parent's params, and are exposed under their `alias` to `join`/`business_rules`. Alias uniqueness across `partials`/`tables`, and cyclic references (direct or indirect) fail fast at load time. `ParsedSchema` now carries a `partials` list of `ParsedPartial`.
## [1.3.0] - 2026-06-29

### Added
- `core/context.py`: an active environment may declare an optional `params:` mapping in `config.yaml`; these are merged into `ExecutionContext.default_params` (and therefore `engine.default_params`). Minimal notebooks can now run templated YAMLs (e.g. `{{ inbound_base_path }}`) without passing storage paths. Priority: explicit `run_from_yaml(params=…)` > config environment `params` > built-in `catalog`/`env` (built-ins win on key collision so a stray `params.catalog` cannot shadow the resolved catalog). A non-mapping `params` fails fast with a clear config error.

### Fixed
- `core/context.py`, `core/core.py`, `core/interpreter.py`: environment matching against `config.yaml` is now case-insensitive (centralized in `ExecutionContext.env_config()`). Previously `is_production` (and `allow_raw_sql`) were looked up with `self.env.lower()` against upper-cased config keys (`DEV`, `PROD`, …), so the lookup never matched and `is_production` always resolved to `False` — disabling the production-mode sandbox guard.

## [1.2.0] - 2026-06-25

### Added
- `core/op_catalog.py`, `core/operations.py`, `backends/sql_base.py`: added the `ceil` select_final column operation across the catalog, Spark runtime, and SQL runtime.
- `core/op_catalog.py`: registered the `not_between` filter operator with list-style arity for two comma-separated bounds and documented it in the operator catalogs.
- `backends/sql_base.py`: SQL backends now support `not_between` filters with escaped bounds and arity validation.
- Spark join planning now accepts `left_anti` and `left_semi` join types, plus tolerant aliases (`anti`, `left anti`, `semi`, `left semi`) normalized in `core/ir.py`.
- `core/op_catalog.py`: registered the `between` filter operator with list-style arity for two comma-separated bounds (`lo,hi`) and documented it in the operator catalogs.
- `core/operations.py`: Spark filter expressions now support `between` with inclusive bounds and fail fast when the filter does not provide exactly two values.
- `backends/sql_base.py`: SQL backends now support inclusive `between` filters with escaped bounds and arity validation.

### Fixed
- `core/rule_executor.py`: a fused `projection` stage whose rule produces a column name already present in the input DataFrame no longer raises `AMBIGUOUS_REFERENCE`. The rewritten column now replaces the input column in place (position preserved), matching sequential `withColumn` semantics.

### Changed
- `core/interpreter.py` now preserves Spark anti/semi semantics on different-key joins by skipping the post-join drop of right-side key columns; `left_anti` and `left_semi` return only left-side columns as intended.
- `backends/sql_base.py` and `backends/snowpark.py` now fail fast with `NotImplementedError` for anti/semi joins instead of executing unsupported backend paths. Spark DataFrame remains the only supported backend for these join types.

## [1.1.0] - 2026-06-22

### Added
- Full outer join support in YAML schema: `type: full` (aliases acceptés : `outer`, `full outer`, `full_outer`). Validation fail-fast des types de join avec message d'erreur explicite sur les valeurs inconnues. Types canoniques : `left`, `right`, `inner`, `full`, `cross` (plan 20).
- `DatabricksProvider` in `llm_provider.py`: LLM provider for Databricks Foundation Models API (OpenAI-compatible). Reads `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_LLM_MODEL` from environment. Auto-detected by `get_llm_provider()` when both Databricks env vars are present (plan 19 phase 1).
- `skifer.serving` package: `hub_response_to_text()` serializer converts any `HubResponse` type to plain text — shared between CLI and MLflow serving layer. Added `[serving]` optional dependency group (`mlflow>=2.12,<3`, `openai>=1.0`) (plan 19 phase 2).
- `SkiferChatModel` in `serving/chat_model.py`: `mlflow.pyfunc.ChatModel` wrapper around `AgenticHub`. `load_context()` initialises SparkSession + hub once at pod startup; `predict()` converts OpenAI-format messages to `hub.ask()` and returns an OpenAI-compatible response dict (plan 19 phase 3).
- `scripts/deploy_to_databricks.py`: deployment helper that logs `SkiferChatModel` to a Databricks MLflow experiment, registers it in Unity Catalog, and prints instructions for creating the Model Serving endpoint and testing it via curl (plan 19 phase 4).

## [1.0.0] - 2026-06-16

First stable release — promoted from the `1.0.0-beta.*` line (Plans 11 through 18: multi-platform backends, external declarative sources, JDBC sink, rule engine optimization, core IR/fail-fast refactor, automated PyPI/TestPyPI publishing).

### Added

- `utils.py`: new `configure_logging(level=logging.INFO)` — attaches a `StreamHandler` to the `"skifer"` logger so the step-by-step pipeline logs (`[Load]`, `[Join]`, `[Rule]`, `[Write]`, …) emitted via `logger.info()` since Plan 17/18 are visible again in notebooks/scripts without needing `logging.basicConfig()`. Idempotent (no duplicate handlers), scoped to the package logger (not root), `propagate=False` to avoid duplicate lines. Exported from the top-level package.
- `utils.py`: `setup_notebook()` now calls `configure_logging()` by default (new `log_level` kwarg, defaults to `logging.INFO`; pass `log_level=None` to skip).
- Workflows GitHub Actions `.github/workflows/publish-testpypi.yml` et `publish-pypi.yml` pour la publication automatisée du package (TestPyPI sur push `main`/dispatch manuel, PyPI sur tag `v*`/dispatch manuel)
- Métadonnées PyPI complètes dans `pyproject.toml` (`license`, `keywords`, `project.urls`, classifiers Python 3.9–3.12)
- `core/op_catalog.py`: new operator catalog module — single source of truth for all filter operators (`FILTER_OPERATORS`) and column operations (`COLUMN_OPS`), with alias resolution helpers (`resolve_filter_operator`, `resolve_column_op`) and a `suggest()` helper backed by `difflib`. No Spark dependency. (Plan 17-1.1)
- `core/schema_loader.py`: fail-fast operator validation at schema load time — unknown filter operators or column ops raise a `ValueError` listing all problems at once with `Did you mean …?` suggestions; aliases (`eq`, `gt`, `>=` …) are accepted. (Plan 17-1.2)
- `core/schema_loader.py`: join aliases (`table_from`/`table_to`) are now validated at load time against declared table aliases/names; an unknown reference raises `ValueError` with `Did you mean …?` suggestions instead of a bare `KeyError` at runtime. (Plan 17-1.4)
- `core/core.py`: `run_union_sources_to_table` no longer calls `count()` before `dropDuplicates` — the full count triggered an extra Spark job just for a log message. (Plan 17-0.1)
- `core/core.py`: `dev_limit` is now applied **after** `filter`, `preprocess`, `quality_checks`, and `filter_groups` instead of before them, so the limit correctly samples from already-filtered data. (Plan 17-0.2)
- `core/core.py`: `add_columns` now delegates to `get_select_expressions`, enabling the full `when/else` dict form and compact conditional chains. (Plan 17-0.3)
- `core/registry.py`: `RuleSpec` now carries a lazy `profile` property that caches the AST `RuleProfile` on first access, avoiding repeated `inspect.getsource` + AST parsing across `analyze_rules` and `_sort_projection_stage`. (Plan 17-0.4)
- `core/rule_analyzer.py`: AST expressions are now canonicalized before `DUPLICATE_EXPR` comparison — different Spark import aliases (`F`, `sf`, `functions`) are normalized to a common `_F` placeholder so functionally identical expressions in different rules are correctly flagged as duplicates. (Plan 17-0.5)
- `core/core.py`: `_is_running_as_job`, `_get_workspace_client`, and `_get_clean_username` are now thin wrappers that delegate to `core/environment.py`, eliminating duplicated detection logic. (Plan 17-2.1)
- `sinks/jdbc.py`: new `JDBCSink` for PostgreSQL-compatible JDBC writes driven by `.env` (`POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`).
- `core/schema_loader.py`, `core/core.py`: new optional YAML `sink:` block to route `run_process_to_table()` to PostgreSQL/Timescale via Spark JDBC while keeping Delta as the default sink.
- `core/core.py`: new `engine.run_from_yaml(path, target_layer, target_table_name=None)` convenience method — loads the YAML (with `default_params`) and runs the pipeline in one call; `target_table_name` defaults to the YAML filename stem.
- `utils.py`: new `setup_notebook(*extra_paths)` helper — finds the project root (via `config.yaml`), adds extra subdirs to `sys.path`, and returns the root path; replaces the boilerplate setup cell in notebooks.

### Changed

- `core/operations.py`, `backends/snowpark.py`, `backends/sql_base.py`: unknown filter operators, unknown `when:` conditions, and unknown column operations now raise `ValueError` with suggestions instead of silently returning `lit(True)` / `lit(False)` / passing the column through unchanged. `print()` calls replaced by `logging`. (Plan 17-1.3)
- `core/backend.py`, `backends/spark.py`, `backends/snowpark.py`, `backends/sql_base.py`, `core/core.py`: multi-condition `when` chains in the dict form of `select_final` were calling `.when()` directly on the result column — a PySpark-specific pattern not guaranteed on other backends. Introduced `when_chain(conditions, otherwise_val)` on the Backend Protocol and all implementations; `get_select_expressions` now routes all CASE WHEN expressions through the backend. (Plan 18-B.3)
- `core/schema_loader.py`, `core/core.py`: compact `when/then/else` chains in `select_final` / `add_columns` ops lists now require exactly 3 elements `[when:..., then:..., else:...]`. A chain with fewer or more elements previously produced silently wrong results (no column applied, or extra ops ignored); it now raises a descriptive `ValueError` both at schema load time and at runtime as a defense-in-depth guard. (Plan 18-B.2)
- `core/core.py`: `run_process_to_table`, `run_process_and_split`, and `run_union_sources_to_table` no longer drop the target table **before** running `process_schema`. All backends already use `mode("overwrite")` with schema evolution (`overwriteSchema=true` on Delta), so the pre-processing drop was both redundant and dangerous — a failed run would leave the target table permanently destroyed. The write is now fully atomic: the previous table survives intact if processing raises. (Plan 18-B.1)
- `core/interpreter.py` (new), `core/core.py`: `process_schema`, `get_select_expressions`, and `_apply_business_rules` extracted from `SkiferEngine` into a new `SchemaInterpreter` class. `SkiferEngine` now holds a `_interpreter` and delegates to it; the three public methods remain as one-line wrappers. All `print()` calls inside the extracted methods replaced by `logging`. (Plan 17-2.3)
- `core/patterns.py` (new), `core/core.py`: `run_process_to_table`, `run_process_and_split`, `run_union_sources_to_table`, and `run_from_yaml` extracted from `SkiferEngine` into a new `PipelinePatterns` class. All remaining `print()` calls in `core.py` outside `describe_schema` replaced by `logging`. (Plan 17-2.4)
- `core/ir.py` (new): Intermediate Representation for pipeline schemas. Introduces `ParsedOp`, `ParsedFilter`, `WhenClause`, `ParsedColumnSpec`, `ParsedJoin`, `ParsedTable`, `ParsedSchema`, and `parse_to_ir()`. Parses the full schema dict once at load time; consumers (interpreter, lineage, inspection) will use it in subsequent plan points. (Plan 17-3.1)
- `backends/spark.py`, `backends/snowpark.py`, `backends/sql_base.py`, `core/backend.py`: all backends now implement `apply_op(c, ParsedOp)` and `build_filter(ParsedFilter)` via per-backend dispatch tables (`_SPARK_FILTER_DISPATCH`, `_SPARK_OP_DISPATCH` in Spark). String-based `apply_operation` / `build_filter_expression` are thin wrappers that parse their string argument and delegate. `core/interpreter.py` now routes filter processing through `build_filter(ParsedFilter)` directly. Aliases (`eq`, `gt`, `>=`, …) are resolved to canonical names before dispatch lookup via `resolve_filter_operator`. (Plan 18-3.2)
- `core/op_catalog.py`: two new module-level type-inference maps — `OP_INFERRED_TYPE` (op name → SQL type, e.g. `round→double`, `to_date→date`) and `CAST_INFERRED_TYPE` (cast arg → SQL type) — extracted from the inline `_TYPE_FROM_OP` dict that lived inside `infer_output_schema`. (Plan 18-B.7)
- `core/schema_loader.py`: two new YAML forms accepted as alternatives to the existing string mini-language. (1) **Filter mapping form** — `filter:` can now be a mapping (`{region: EMEA, status: {in: [ACTIVE, PENDING]}, customer_id: is_not_null}`); bare string values that match a no-arg operator are treated as that operator, all others as `equals`. (2) **select_final/add_columns mapping form** — entries with `from`/`as` keys (`{from: amount, as: amount_eur, ops: [{cast: double}]}`) are accepted; single-key dict ops like `{cast: double}` are converted to the canonical op-string `"cast:double"`. Both new forms normalize to the same internal representation as the existing string/list forms and go through the same validation. (Plan 18-3.5)
- `lineage/tracker.py` — `LineageTracker.from_schema` now calls `parse_to_ir(schema_dict)` and walks `ParsedTable`, `ParsedJoin`, and `ParsedColumnSpec` IR objects instead of raw dicts. Op strings reconstructed via `op.name:arg` only at the edge boundary; `is_conditional` replaces the `isinstance(entry, dict)` check. Multi-key joins now emit one edge per key pair. Output (graph edges) unchanged. (Plan 18-3.4)
- `core/json_schema.py` (new): `generate_json_schema() -> dict` builds a JSON Schema (draft-07) for Skifer pipeline YAMLs, derived from the operator catalog. Filter operator enums and column op descriptions are generated from `FILTER_OPERATORS` / `COLUMN_OPS` — adding a new operator to the catalog automatically surfaces it in the schema. Covers all three `filter` forms (string, dict, mapping) and all three `select_final`/`add_columns` forms (list, `source`/`target`, `from`/`as`). (Plan 18-4.1)
- `schemas/skifer-pipeline.schema.json` (new): committed schema file generated from `generate_json_schema()`. The non-drift test `tests/test_json_schema.py::TestNonDrift::test_committed_schema_matches_generated` fails if the file is stale — regenerate with `python -c "from skifer.core.json_schema import generate_json_schema; import json; print(json.dumps(generate_json_schema(), indent=2))" > schemas/skifer-pipeline.schema.json`. (Plan 18-4.1)
- `cli.py`: new `skifer validate <paths…>` subcommand — validates one or more pipeline YAML schema files without requiring Spark. Accepts glob patterns; auto-injects sentinel values for `{{ key }}` template placeholders so templated schemas pass structural validation. Aggregates all errors across all files, prints per-file `OK` / `FAIL` lines and a summary, and exits non-zero if any file fails. (Plan 18-4.2)
- `core/core.py`: `_auto_detect_environment` now caches its result in a module-level dict `_ENV_DETECTION_CACHE` keyed by `(absolute_config_path, mtime)`. Subsequent engine instantiations within the same Python process that use the same unmodified config file skip all catalog network checks. Set `env_detection_cache: false` in `config.yaml` to opt out. `SkiferEngine.clear_env_detection_cache()` allows explicit invalidation. (Plan 18-B.5)
- `core/sandbox.py`, `core/interpreter.py`: `SandboxResolver.resolve` now memoizes in `_resolve_cache` (instance dict) so `table_exists`/`schema_exists` are called at most once per table per resolver instance. `SchemaInterpreter` now holds a single `_sandbox_resolver` reused across all `process_schema` calls on the same engine, so cloned tables are never re-checked within a session. (Plan 18-B.6)
- `core/core.py` — `describe_schema` and `infer_output_schema` now consume the `ParsedSchema` IR (via `parse_to_ir`) instead of walking raw schema dicts. Both functions call `parse_to_ir(schema_dict)` once and read `ParsedTable`, `ParsedJoin`, and `ParsedColumnSpec` objects throughout; op strings are reconstructed only at output boundaries. (Plan 18-3.3)

### Fixed

- `SparkBackend.sql()` — ajout de l'alias manquant (`sql` → `execute_sql`) requis par les checks d'observabilité (`NullCheck`, `UniqueCheck`, `TypeCheck`, etc.)
- `core/patterns.py`, `backends/spark.py`: `run_union_sources_to_table` now uses `backend.table_exists()` before reading each source table, replacing the broad `except Exception` that previously swallowed permission and network errors. `SparkBackend.table_exists` also falls back to `spark.catalog.tableExists` to handle session-scoped temp views. Only genuinely absent tables are skipped (logged at WARNING); all other errors propagate. (Plan 18-B.4)
- `backends/spark.py`, `core/writer.py`: sandbox schema creation and table resolution now strip the catalog prefix in local mode — `spark_catalog` only supports single-part namespaces (`REQUIRES_SINGLE_PART_NAMESPACE`).
- `core/writer.py`: `ensure_schema_exists` now also runs `CREATE SCHEMA IF NOT EXISTS` on Databricks (was no-op in non-local mode, causing sandbox schema creation to silently fail).
- `core/core.py`: `process_schema` now normalizes string filters (`"col:op:val"`) at runtime, allowing raw `yaml.safe_load()` dicts to be passed directly without going through `parse_schema`.
- `backends/spark.py`: `check_catalog_access` now runs the Spark SQL check in a thread with a 30s timeout, preventing `SkiferEngine()` init from blocking indefinitely when a Databricks cluster is slow to respond.
- `core/core.py`: `process_schema` now handles compact list form `[alias, key]` for `table_from`/`table_to` in join definitions at runtime (bypasses `parse_schema` normalization).
- `core/core.py`: `_ensure_schema_exists` now prepends the catalog (`self.db`) so target schemas are created in the correct Unity Catalog catalog on Databricks.
- `core/core.py`: `run_process_and_split` and `run_union_sources_to_table` now raise `NotImplementedError` immediately when the schema contains a JDBC sink (`sink: {type: postgres/jdbc}`), preventing silent Delta fallback writes.
- `core/config.py` now rejects empty or malformed top-level YAML configs with a clear error.
- `core/rule_analyzer.py` now treats syntax errors as source-unavailable and keeps unavailable rules ordered conservatively.
- `backends/bigquery.py`, `backends/snowpark.py`, and `backends/sql_base.py` fix schema listing, DDL execution, and clone-table protocol gaps.
- `observability/` now uses UTC-aware timestamps, hydrates volume variation checks from history, supports dict-form `select_final`, and creates HTML report directories automatically.
- `cli.py` exits non-zero when invoked without a subcommand.

## [1.0.0-beta.1] - 2026-05-05

### Added — Hub agentique (piste 4)

**`agentic/hub.py`** — `AgenticHub`
- Routeur unique pour toutes les interactions agentiques
- Routage déterministe par mots-clés (sans LLM) : lineage / dictionary / quality / builder / genbi
- Fallback LLM optionnel (classification d'intent en 1 appel)
- Pré-résolution des alias utilisateur via `UserProfile`
- Tous les agents sont optionnels — réponse `mode="error"` si agent non configuré

**`agentic/history.py`** — Extension `SessionHistory`
- `session_id` (uuid4), `ttl_days`, `is_expired`
- `load_or_create(scope_id)` — charge/crée depuis `~/.skifer_sessions/`
- `save(scope_id)` — persiste dans `~/.skifer_sessions/<scope_id>.json`
- `purge_expired(scope_id)` — supprime les sessions expirées
- Rétrocompatibilité `to_json` / `from_json` conservée

**`agentic/user_profile.py`** — `UserProfile`
- Alias utilisateur normalisés (insensible casse + accents)
- `resolve_alias(term)`, `add_alias(term, canonical)`, `reject_suggestion(term)`
- Persistance dans `~/.skifer_profile.yaml`
- Intégré dans `AgenticHub.ask()` pour pré-résolution avant routage

**`cli.py`** — commande `skifer hub`
- REPL conversationnel terminal avec `rich`
- Choix de session (nouvelle / reprendre)
- Commandes spéciales : `exit`, `quit`, `help`, `history`, `export pdf`
- `pyproject.toml` : déclaration `[project.scripts]` → `skifer = "skifer.cli:main"`

**Tests**
- `tests/test_hub.py` — 15 tests (routage déterministe, agent unavailable, kwargs forwarding)
- `tests/test_history.py` — 9 nouveaux tests Phase B (TTL, persistance, purge, compat)
- `tests/test_user_profile.py` — 13 tests (alias, normalisation, persistance, intégration hub)
- `tests/test_cli.py` — 4 smoke tests CLI

### Added — LineageAgent : agent conversationnel de lignage colonne-niveau

Nouveau composant `LineageAgent` dans la couche agentic — expose `LineageTracker`,
`DataDictionary` et `LineageRenderer` sous une API conversationnelle cohérente avec `GenBIAgent`
et `BuilderAgent`. Aucun Spark ni LLM requis pour les fonctionnalités cœur.

**`agentic/lineage_agent.py`** — `LineageAgent`
- `trace(table, column)` — traversée upstream : d'où vient ce champ ?
- `impact(table, column)` — traversée downstream : quelles colonnes dépendent de ce champ ?
- `render(format)` — rendu du graphe complet (mermaid / html / json)
- `lookup(table, column)` — lookup `DataDictionary` avec suggestions Levenshtein si non trouvé
- `ask(question)` — routing NL déterministe (mots-clés, sans LLM) + fallback LLM optionnel
- `load_schema(schema_dict, schema_type)` — rechargement dynamique (core ou semantic)
- `narrative=True` dans `trace()` / `impact()` — explication en prose via LLM (optionnel)
- `history=True` — log des interactions dans `SessionHistory`

**`agentic/models.py`** — `LineageResponse`

**`agentic/__init__.py`** — exports `LineageAgent`, `LineageResponse`

**`tests/test_lineage_agent.py`** — 32 tests

---

### Added — QualityAgent : agent conversationnel de data quality

Nouveau composant `QualityAgent` dans la couche agentic — expose `DataMonitor`,
`ContractExtractor`, `HistoryStore`, `MonitorReporter` et `AlertDispatcher` sous une API
conversationnelle cohérente avec `GenBIAgent` et `BuilderAgent`. Un backend est requis pour
exécuter les checks SQL ; LLM optionnel pour la narration et le parsing NL.

**`agentic/quality_agent.py`** — `QualityAgent`
- `check(fqn, contracts, schema_dict)` — exécute les checks, stocke en historique, dispatche les alertes
- `get_history(fqn, n)` — récupère les n derniers `MonitorReport` depuis le `HistoryStore`
- `report(fqn, format)` — rendu du dernier report (text / json) via `MonitorReporter`
- `ask(question)` — routing NL déterministe (mots-clés, sans LLM) + fallback LLM optionnel
- `schema_dict` au constructeur : schéma par défaut utilisé par `ask()` pour dériver les contracts
- `narrative=True` dans `check()` : résumé en prose via LLM (optionnel)
- `session_history=True` : log des interactions dans `SessionHistory`
- `alert_config` : dispatch best-effort (webhook / Slack / email) après chaque check

**`agentic/models.py`** — `QualityResponse`

**`agentic/__init__.py`** — exports `QualityAgent`, `QualityResponse`

**`tests/test_quality_agent.py`** — 31 tests

---

### Added — DictionaryAgent : agent conversationnel de data dictionary

Nouveau composant `DictionaryAgent` dans la couche agentic — expose `DataDictionary` et
`GlossaryReader` sous une API conversationnelle cohérente avec `LineageAgent`, `QualityAgent`
et `GenBIAgent`. Aucun Spark ni LLM requis pour les fonctionnalités cœur.

**`agentic/dictionary_agent.py`** — `DictionaryAgent`
- `lookup(table, column)` — recherche d'un FieldEntry avec suggestions Levenshtein si non trouvé
- `list_fields(table)` — liste de tous les champs, optionnellement filtrée par table
- `export(format)` — export du dictionnaire complet en text ou JSON
- `enrich(glossary_path)` — enrichissement des descriptions via `GlossaryReader` (JSON/YAML/TXT/PDF/PPTX)
- `load_schema(schema_dict, schema_type)` — rechargement dynamique (core ou semantic)
- `ask(question)` — routing NL déterministe (mots-clés, sans accents) + fallback LLM optionnel
- `narrative=True` dans `lookup()` : explication en prose via LLM (optionnel)
- `session_history=True` : log des interactions dans `SessionHistory`

**`agentic/models.py`** — `DictionaryResponse`

**`agentic/__init__.py`** — exports `DictionaryAgent`, `DictionaryResponse`

**`tests/test_dictionary_agent.py`** — 34 tests

---

### Added — BuilderAgent : génération de YAML de pipeline ETL

Nouveau composant `BuilderAgent` dans la couche agentic — permet de créer des YAML de pipeline
`SkiferEngine`-ready (tables, joins, filtres, business_rules, select_final) via un wizard
pas-à-pas ou en langage naturel (LLM), avec validation catalog garantie.

**`core/catalog_inspector.py`** — `CatalogInspector` + `CatalogError`
- `list_tables(schema)` / `list_columns(fqn)` — découverte catalog depuis n'importe quel backend
- `validate_table(fqn)` — lève `CatalogError` si la table n'existe pas (+ liste disponible)
- `validate_columns(fqn, cols)` — lève `CatalogError` avec top-3 suggestions Levenshtein
- `fqn(schema, table)` — construit le FQN correct selon catalog présent ou non

**Backend Protocol** — nouvelles méthodes (best-effort)
- `list_tables(schema, catalog)` — `SHOW TABLES IN` (Spark), `INFORMATION_SCHEMA.TABLES` (SQL), `SHOW TABLES IN SCHEMA` (Snowflake), `INFORMATION_SCHEMA.TABLES` (BigQuery)
- `list_columns(fqn)` — `SHOW COLUMNS IN` (Spark), `INFORMATION_SCHEMA.COLUMNS` (SQL/BigQuery), `SHOW COLUMNS IN TABLE` (Snowflake)
- Implémenté dans : `SparkBackend`, `SQLBackend` (base BigQuery), `BigQueryBackend`, `SnowparkBackend`, `FakeBackend`

**`agentic/builder_agent.py`** — `BuilderAgent`
- Mode `wizard()` : assistant pas-à-pas interactif (6 étapes, validation en temps réel, sans LLM)
- Mode `ask(description)` : pipeline 2 étapes LLM → JSON structuré → validation catalog → YAML
- `export_orchestration(yaml_paths, format)` : délègue à `OrchestratorExporter`
- `_validate_etl_struct(etl)` : valide tables et colonnes, retourne une question de clarification si nécessaire

**`agentic/orchestrator.py`** — `OrchestratorExporter`
- Génère 3 niveaux d'artefacts d'orchestration :
  - Airflow DAG Python (universel — spark/snowflake/bigquery)
  - Databricks Asset Bundle YAML (natif Spark — format `databricks`)
  - Python script fallback (toujours généré — aucune dépendance)
- Auto-détection du format selon le backend (`auto`)

**`agentic/models.py`** — nouveaux dataclasses
- `BuilderResponse` — résultat structuré de `BuilderAgent.ask()`
- `OrchestratorExportResult` — résultat de `OrchestratorExporter.export()`

**`__init__.py`** — nouveaux exports publics : `BuilderAgent`, `CatalogInspector`, `CatalogError`

### Fixed — review post-génération
- `builder_agent.py` : suppression du double import `models` + fusion sur une ligne
- `builder_agent.py` : bug `joins: list[dict] = {}` (dict au lieu de list) corrigé en `[]`
- `builder_agent.py` : `dev_limit` retourne `None` si l'user ne saisit rien (au lieu de forcer 10000)
- `builder_agent.py` : `_save_yaml()` appelle désormais `parse_schema()` avant écriture — garantit que le YAML est sémantiquement valide pour `SkiferEngine`
- `builder_agent.py` : prompt `_STEP_A_SYSTEM` clarifie que `select_final[].source` doit être un nom de colonne nu (sans préfixe d'alias comme `o.order_id`)
- `builder_agent.py` : `_build_schema_dict()` strip défensif du préfixe d'alias dans `select_final.source` (`o.order_id` → `order_id`)
- `example/demo_builder_agent.ipynb` : `MockCore` initialise désormais `self._backend = SparkBackend(...)` — corrige `AttributeError: 'NoneType' object has no attribute 'join'` lors de l'exécution d'un pipeline avec jointure

## [0.11.0] - 2026-04-28

### Added — Data Observability

Nouveau module `observability/` — data quality checks dérivés automatiquement des YAML schemas existants.
Le YAML est le contrat : aucune configuration supplémentaire requise pour les checks de base.

**`observability/checks.py`** — 8 types de DataContract
- `NullCheck` — absence de valeurs NULL dans une colonne (`severity: critical`)
- `UniqueCheck` — unicité d'un ensemble de colonnes (`severity: critical`)
- `TypeCheck` — type SQL d'une colonne via `DESCRIBE` (`severity: warning`)
- `FilterInvariantCheck` — l'invariant de filtre tient sur la table cible (`severity: warning`) — 12 opérateurs
- `FreshnessCheck` — fraîcheur d'une colonne timestamp (formats: `2h`, `30m`, `1d`, `3600s`)
- `VolumeCheck` — nombre de lignes dans les bornes `[min_rows, max_rows]`
- `VolumeVariationCheck` — variation de volume ≤ threshold vs run précédent (skip si no history)
- `SchemaDriftCheck` — colonnes ajoutées/supprimées vs colonnes déclarées dans `select_final`
- `CustomSqlCheck` — SQL personnalisé comparé à une valeur `expect` scalaire
- `DataQualityError` — exception levée sur échec critique (`raise_on_critical=True`)
- `CheckResult` — dataclass résultat : `passed`, `actual_value`, `expected_value`, `message`, `severity`, `timestamp`

**`observability/contracts.py`** — `ContractExtractor`
- Dérive automatiquement les DataContracts depuis un schema YAML normalisé (output de `parse_schema`)
- Mapping automatique (Phase 1) : `quality_checks.drop_nulls_in` → `NullCheck`, `drop_duplicates_on` → `UniqueCheck`, `filter[]` → `FilterInvariantCheck`, `select_final[cast:type]` → `TypeCheck`
- Section optionnelle `observability:` dans le YAML (Phase 2) : `freshness` → `FreshnessCheck`, `volume` → `VolumeCheck` + `VolumeVariationCheck`, `schema_drift.enabled: true` → `SchemaDriftCheck`, `custom_checks[]` → `CustomSqlCheck`

**`observability/monitor.py`** — `DataMonitor` + `MonitorReport`
- `DataMonitor(backend, history_store=None)` — accepte tout objet avec une méthode `sql(query)`
- `check_table(fqn, contracts, raise_on_critical=False)` — exécute les checks, retourne `MonitorReport`
- `check_from_schema(fqn, schema_dict, raise_on_critical=False)` — raccourci YAML → contracts → checks
- `MonitorReport.has_critical_failures()`, `.failures()`, `.summary()` (status: PASS / WARN / CRITICAL)
- Intégration `history_store` best-effort (pas de raise si le store échoue)

**`observability/history.py`** — persistence
- `HistoryStore` Protocol (`runtime_checkable`) — `store()`, `get_last_n()`, `get_latest()`
- `SqliteHistoryStore` — SQLite local (développement/tests), support `:memory:`
- `DeltaHistoryStore` — Delta table production Databricks (`_observability.check_history`)
- `.skifer_observability.db` ajouté au `.gitignore`

**`observability/reporter.py`** — `MonitorReporter`
- `to_json(report)` — JSON pretty-print avec `results` + `summary`
- `to_text(report)` — rapport terminal avec icônes ✅ 🚨 ⚠️
- `to_html(report, output_path)` — page HTML standalone, tableau coloré par sévérité

**`observability/alerts.py`** — `AlertDispatcher`
- `dispatch(report, config)` → liste des canaux notifiés
- Canaux : `webhook_url` (HTTP POST JSON), `slack_webhook` (Incoming Webhook), `email` (SMTP + starttls)
- `min_severity` : filtre les failures sous le seuil (défaut : `"warning"`)
- Best-effort sur tous les canaux — pas de raise en cas d'échec d'envoi

**`core/core.py`** — intégration pipeline
- `SkiferEngine.__init__` accepte `monitor: DataMonitor | None = None`
- Après `_write_dataframe()` dans `run_process_to_table()` : exécute les checks automatiquement si monitor configuré
- `DataQualityError` se propage depuis `run_process_to_table()` sur échec critique

**`observability/__init__.py`** — exports publics de tous les types ci-dessus

**Tests** — 75 tests dans `tests/test_observability.py` (aucun Spark requis)
- `TestContractExtractor` (7), `TestNullCheck` (3), `TestUniqueCheck` (3), `TestTypeCheck` (3), `TestFilterInvariantCheck` (4)
- `TestDataMonitor` (8), `TestFreshnessCheck` (4), `TestVolumeCheck` (4), `TestVolumeVariationCheck` (3), `TestSchemaDriftCheck` (3), `TestCustomSqlCheck` (3)
- `TestContractExtractorPhase2` (6), `TestSqliteHistoryStore` (7), `TestEngineMonitorIntegration` (4)
- `TestMonitorReporter` (6), `TestAlertDispatcher` (7)

**Notebook** — `example/demo_observability.ipynb` — scénario 3 runs (clean → régression → corrigé), historique SQLite, export JSON/HTML, impact lineage downstream

## [0.10.0]

### Added — Lineage Tracker & Data Dictionary

**`lineage/tracker.py`** — new module
- `LineageEdge` dataclass: directed column-level edge (`source_table`, `source_column`, `target_table`, `target_column`, `transformations`, `edge_type`)
- `LineageGraph` class: adjacency-list DAG with `add_edge()`, `merge()`, `upstream()`, `downstream()`, `tables()`, `to_dict()`
- `LineageTracker.from_schema(schema_dict, target_name)` — static lineage from a core YAML schema: extracts edges from `select_final`, `add_columns`, `join`, and `business_rules` (via `RuleAnalyzer` AST introspection)
- `LineageTracker.from_semantic_model(model_dict)` — static lineage from a semantic YAML model: extracts edges from `dimensions` and `metrics` SQL expressions (best-effort regex identifier extraction)
- No external dependencies (no networkx), no Spark execution required

**`lineage/renderer.py`** — new module
- `LineageRenderer.to_mermaid(graph, direction)` — Mermaid flowchart string with typed edge styles (select `-->`, join `-.->`, rule `-->`, metric `==>`)
- `LineageRenderer.to_json(graph)` — JSON-serialisable dict (edges, tables, summary)
- `LineageRenderer.to_html(graph)` — standalone HTML page embedding Mermaid.js

**`lineage/dictionary.py`** — new module
- `FieldEntry` dataclass: per-column metadata (`name`, `table`, `description`, `source_fields`, `transformations`)
- `DataDictionary(graph)`: built from `LineageGraph`; `get(table, column)`, `list_fields(table)`, `to_dict()`, `print_report()`
- `DataDictionary.enrich_from_glossary(path)`: fuzzy-matches column names against glossary terms via `difflib.get_close_matches` (cutoff 0.8); reads any format supported by `GlossaryReader`

**`core/core.py`** — new method on `SkiferEngine`
- `build_lineage(schema_dict, semantic_models=None, target_name=None)` — builds and optionally merges a `LineageGraph` from core schema + semantic models; returns the graph for programmatic use

**`__init__.py`** — public exports
- `LineageTracker`, `LineageGraph`, `LineageEdge`, `DataDictionary`, `LineageRenderer` added to top-level package

**Tests** — 91 unit and integration tests (pure Python, no Spark):
- `tests/test_lineage_tracker.py` — 50 tests (LineageEdge, LineageGraph, helpers, from_schema, business_rules, from_semantic_model)
- `tests/test_lineage_renderer.py` — 22 tests (Mermaid, JSON, HTML)
- `tests/test_lineage_dictionary.py` — 19 tests (construction, lookup, list_fields, glossary enrichment)
- `tests/test_engine_fake_backend.py` — 5 integration tests for `build_lineage()`

---

## [0.9.1] - 2026-04-24 - BETA

### Added — Rule Optimizer

**`core/rule_analyzer.py`** — new module
- `RuleProfile` dataclass: captures per-rule static analysis (`output_columns`, `input_columns`, `raw_expressions`, `source_available`)
- `RuleWarning` dataclass: represents a detected redundancy (`OVERWRITE`, `SHARED_READ`, `DUPLICATE_EXPR`)
- `RuleAnalyzer` class:
  - `analyze_rule(func, name)` — best-effort AST analysis via `inspect.getsource()` + `ast.parse()`; detects `withColumn`/`with_column` (outputs) and `F.col`/`col`/`df["col"]` (inputs)
  - `analyze_rules(rule_names)` — analyzes a list of registered rule names via `RuleRegistry`
  - `detect_warnings(profiles, shared_read_threshold=2)` — flags OVERWRITE (same column written by multiple rules), SHARED_READ (same column read by N+ rules), DUPLICATE_EXPR (identical expression in multiple rules)
  - `print_report(profiles, warnings)` — human-readable stdout report

**`core/core.py`** — new method on `SkiferEngine`
- `explain_rules(schema_dict, shared_read_threshold=2)` — dry-run rule analysis; prints report and returns `(profiles, warnings)` for programmatic use

**`__init__.py`** — `RuleAnalyzer` exported from top-level package

**Tests** — 27 unit tests (`test_rule_analyzer.py`, pure Python, no Spark) + 4 integration tests (`test_engine_fake_backend.py`)

## [0.9.0] - 2026-04-21 - BETA

### Breaking (v0.9) — Semantic / Agentic : simplification API

**`semantic/builder.py`** — Option C : suppression de `model_name` + `split_value`
- `build(model_name, split_value, ...)` → `build(model_key, ...)` — un seul identifiant
- Structure de fichiers : `semantic_models/<model_key>.yaml` (plate) au lieu de `<model_name>/<split_value>.yaml`
- `_build_catalog_entry()` simplifié : plus de paramètre `split_value` ni `model_name`
- Le champ `file` dans le catalogue reflète la structure plate : `"kpi_orders_erp.yaml"`

**`core/core.py`** — Option B : factory method `get_agent()`
- Nouvelle méthode `SkiferEngine.get_agent(llm_provider=None, models_dir=..., history=False, session_title=...)`
- Instancie `SemanticEngine` + `GenBIAgent` en un seul appel
- Si `llm_provider=None`, auto-détecte depuis les variables d'environnement
- Usage : `agent = engine.get_agent()` puis `resp = agent.ask("...")`

**`agentic/agent.py`** — correction de bug
- `_execute_query()` utilisait `self.semantic.spark.sql(...)` (attribut supprimé)
- Corrigé en `self.semantic.core._get_backend().execute_sql(...)` — agnostique plateforme

**Tests**
- `test_semantic_builder.py` : mis à jour pour `model_key` + structure plate + nouveau test `test_build_flat_file_no_subdirectory`
- `test_semantic_engine_catalog.py` : fixture mise à jour vers structure plate (`kpi_orders_erp.yaml`), ajout de deux tests `get_agent`

### Breaking (v0.9) — pyproject.toml et core

**`pyproject.toml`**
- Version bumped `0.7` → `0.9.0`
- `pyspark>=3.3.0` et `delta-spark>=2.0.0` déplacés de `dependencies` vers `[spark]` (optionnel)
- Ajout du groupe `[snowflake]` : `snowflake-snowpark-python>=1.0.0`
- Ajout du groupe `[bigquery]` : `google-cloud-bigquery>=3.0.0`, `google-cloud-bigquery-storage>=2.0.0`
- `requires-python` passé de `>=3.8` à `>=3.9`
- `description` et `classifiers` mis à jour (multi-platform, Production/Stable)
- Installation : `pip install skifer[spark]` / `[snowflake]` / `[bigquery]`

**`core/core.py`**
- Suppression de la méthode `_write_dataframe` dupliquée (deux définitions identiques, la seconde écrasait la première)

### Breaking (v1.0) — Core fully platform-agnostic, no backward compatibility

**`core/core.py`**
- Suppression des imports Spark au niveau module : `from pyspark.sql import functions as F`, `from pyspark.sql import Window` (imports morts)
- Import `get_spark_session` (spark_factory) déplacé en lazy à l'intérieur du bloc Spark de `__init__`
- Import `DBUtils` déplacé en lazy avec try/except à l'intérieur du bloc Spark de `__init__` ; `self.dbutils = None` pour les backends non-Spark
- Suppression du bloc de ré-exports backward-compat (`_apply_operation`, `_build_filter_expression`, `_get_workspace_client_fn`, `_write_dataframe_fn`, etc.) — ces symboles ne sont plus accessibles via `skifer.core.core`

**`tests/test_core.py`**
- Import de `_apply_operation` / `_build_filter_expression` déplacé vers `skifer.core.operations` (source canonique)
- Suppression des deux `mocker.patch('skifer.core.core.DBUtils')` devenus invalides (DBUtils n'est plus un attribut module-level)

### Fixed
- `test_core_env_detection.py` : mis à jour pour tester `_check_catalog_access` via backend mock (suppression des tests Spark SQL directs obsolètes suite à la Phase 3)
- `test_core_join.py` : `_make_engine()` injecte désormais un `SparkBackend` pour que `process_schema` fonctionne
- `test_core_username.py` : `_make_engine()` injecte un backend mock ; les tests `test_returns_username_from_spark_sql` remplacés par `test_returns_username_from_backend`
- `test_engine_with_backend.py` : suppression du test `test_engine_get_backend_lazy_creation` (plus de création lazy sans spark) ; remplacement par deux tests clairs
- `test_loaders.py` : suppression des anciens tests orphelins accolés en double (résidu de migration Phase 4)
- `test_sandbox.py` : suppression des anciens tests `test_clone_table_*` orphelins en double (résidu de migration Phase 3.5)

### Added — Multi-plateforme : nettoyage des derniers couplages Spark + core agnostique

**Tâche 3 — `ConfigurationManager` backend-aware**
- `src/skifer/core/config.py` : `ConfigurationManager.__init__` accepte `backend=None`. `_detect_environment` délègue à `backend.check_catalog_access()` quand disponible ; `spark.sql()` conservé en fallback.

**Tâche 4 — `self.spark` résiduel supprimé de `core.py`**
- `_check_catalog_access` : délègue à `self._get_backend().check_catalog_access(catalog)` — le backend encapsule déjà les deux stratégies (Spark SQL + SDK REST).
- `_get_clean_username` Strategy 1 : délègue à `backend.get_current_user()` ; fallback `self.spark.sql` conservé uniquement pour les engines créés via `object.__new__` sans backend.
- `describe_schema` : `SandboxResolver` instancié avec le backend injecté.

**Tâche 5 — `self.spark` mort supprimé de `semantic.py`**
- Suppression de `self.spark = core_engine.spark` — toutes les requêtes SQL transitent par `self.core._get_backend().execute_sql()`.

**Finalisation — core 100 % agnostique sur le chemin d'exécution**
- `src/skifer/core/loaders.py` : `load_generic_explode_union` — cas edge "aucune branche" utilise `backend.limit(df_source, 0)` au lieu de `spark.createDataFrame([], ...)` quand un backend est disponible.

**Résumé de l'état post-migration**
- Le chemin d'exécution complet (`process_schema`, `run_process_to_table`, `run_process_and_split`, `run_union_sources_to_table`) est 100 % agnostique — zéro appel Spark direct.
- Les `self.spark` restants dans `core.py` sont **by design** : initialisation session (bloc legacy SparkBackend), patches gRPC Connect v2, et fallbacks de compatibilité pour engines instanciés via `object.__new__`.
- 509 tests passent, notebook `demo_agent.ipynb` exécuté sans erreur (38 cellules).



**`clone_table` dans le Backend Protocol (best-effort)**
- `src/skifer/core/backend.py` : méthode `clone_table(src_catalog, src_schema, src_table, tgt_catalog, tgt_schema, tgt_table)` ajoutée au Protocol (niveau best-effort — utilisée par le Sandbox).
- `src/skifer/backends/spark.py` : `SparkBackend.clone_table` — SHALLOW CLONE natif sur Databricks, CTAS en fallback local ou si SHALLOW CLONE échoue.
- `src/skifer/backends/snowpark.py` : `SnowparkBackend.clone_table` — `CREATE TABLE ... CLONE` (zero-copy natif Snowflake), fallback CTAS si échec.
- `src/skifer/backends/bigquery.py` : `BigQueryBackend.clone_table` — `CREATE TABLE ... AS SELECT *` (best-effort, BigQuery sans clone natif), erreur silencieuse avec log.
- `tests/fakes/fake_backend.py` : `FakeBackend.clone_table` — no-op pour les tests de découplage.

**SandboxResolver délègue au backend**
- `src/skifer/core/sandbox.py` : `schema_exists`, `table_exists`, `create_schema` et `clone_table` délèguent au `backend` quand il est présent — supprime les appels SQL Spark-spécifiques (`SHOW TABLES IN`, backticks, `SHALLOW CLONE`) du resolver.
- Tests de non-régression : `test_backend_protocol.py`, `test_backend_spark.py`, `test_backend_snowpark.py`, `test_backend_bigquery.py`, `test_backend_sql_base.py`, `test_sandbox.py` mis à jour.



**Phase 0 — Extraction des modules internes**
- `src/skifer/core/operations.py` : `_apply_operation` et `_build_filter_expression` extraits de `core.py` — disponibles comme fonctions standalone réutilisables par tous les backends.
- `src/skifer/core/environment.py` : helpers de détection d'environnement Databricks extraits de `SkiferEngine` (`get_workspace_client`, `patch_connect_debugging`, `patch_connect_user_context`, `is_running_as_job`, `get_clean_username`).
- `src/skifer/core/writer.py` : helpers d'écriture Delta extraits (`write_dataframe`, `write_dataframe_local`, `drop_table_if_exists`, `ensure_schema_exists`).

**Phase 1 — Backend Protocol**
- `src/skifer/core/backend.py` : `Backend` — Protocol `@runtime_checkable` définissant le contrat plateforme complet (lecture, écriture, colonnes, filtres, joins, window, optimize, etc.).

**Phase 2 — SparkBackend**
- `src/skifer/backends/__init__.py` + `backends/spark.py` : `SparkBackend` — implémentation concrète du Protocol pour PySpark/Databricks. Encapsule toutes les opérations Spark. Déclare ses `capabilities` (optimize, zorder, shallow_clone, cache, window).

**Phase 3 — Branchement de l'engine**
- `SkiferEngine.__init__` accepte désormais un paramètre `backend=` optionnel (Backend Protocol). Si absent, un `SparkBackend` est créé automatiquement — **compatibilité ascendante totale**.
- `SkiferEngine._get_backend()` : lazy-initialization du backend (gère les tests qui instancient via `object.__new__`).
- Toutes les méthodes internes (`process_schema`, `run_process_to_table`, `run_process_and_split`, `run_union_sources_to_table`, `optimize_table`) délèguent au backend via `self._get_backend()` — plus aucun appel direct à `self.spark.*` ou `F.*` dans le core.

**Phase 3.3 — `get_select_expressions` migré vers le backend**
- `get_select_expressions()` délègue désormais toutes les opérations colonnes (`col`, `lit`, `when`, `otherwise`, `apply_operation`) au backend via `self._get_backend()`. Plus aucun import `F.*` dans la logique métier.

**Phase 3.5 — SandboxResolver backend-aware**
- `SandboxResolver.__init__` accepte un paramètre `backend=` optionnel. Toutes les opérations SQL (`schema_exists`, `table_exists`, `create_schema`, `clone_table`) passent par `self._sql()` qui délègue au backend si disponible, ou à `spark.sql()` en fallback (compatibilité ascendante).

**Phase 3.6 — SemanticEngine backend-aware**
- `SemanticEngine.query()` et `create_view()` utilisent `self.core._get_backend().execute_sql()` au lieu de `self.spark.sql()`.

**Phase 3.7 — ConfigurationManager backend-aware**
- `ConfigurationManager._detect_environment()` utilise `backend.check_catalog_access()` si un backend est attaché, sinon `spark.sql()` en fallback.

**Phase 4 — Loaders backend-aware**
- `load_and_union_tables` et `load_generic_explode_union` acceptent `**kwargs` et extraient `backend=` si présent. Utilisent `backend.read_table()` et `backend.union_by_name()` quand disponibles ; `spark.table()` sinon (compatibilité totale).
- L'engine injecte désormais `backend=self._get_backend()` dans chaque appel de loader — les loaders utilisateur ignorent ce kwarg via `**kwargs`.
- `tests/test_loaders.py` : 3 nouveaux tests backend-aware.


- `tests/fakes/fake_backend.py` : `FakeBackend` — backend Python pur (listes de dicts) pour tester `SkiferEngine` sans PySpark. Implémente le Protocol complet.
- `tests/fakes/fake_backend.py` : `FakeDataFrame`, `FakeColumn`, `FakeCondition` — abstractions DataFrame légères.
- `tests/test_engine_fake_backend.py` : 16 tests prouvant le découplage réel (zéro import PySpark dans les tests de logique métier).
- `tests/test_engine_with_backend.py` : tests d'intégration Engine + backend injecté.
- `tests/test_backend_protocol.py` : tests de conformité du Protocol.
- `tests/test_backend_spark.py` : tests unitaires SparkBackend (27 cas).

**Phase 6 — SnowparkBackend (Snowflake)**
- `src/skifer/backends/snowpark.py` : `SnowparkBackend` — implémentation concrète du Protocol pour Snowflake/Snowpark. Import lazy (ne casse pas les projets sans `snowflake-snowpark-python`). Différences clés vs SparkBackend :
  - `df.with_column()` (snake_case) au lieu de `df.withColumn()`
  - `df.union_by_name(allow_missing_columns=True)` au lieu de `df.unionByName()`
  - `df.cache_result()` au lieu de `df.cache()`
  - `df.count() == 0` pour `is_empty()` (pas de `isEmpty()` en Snowpark)
  - FQN `"db"."schema"."table"` (double-guillemets) au lieu de backticks
  - `optimize_table` : best-effort via `ALTER TABLE ... CLUSTER BY` (Automatic Clustering Snowflake)
  - `apply_operation` / `build_filter_expression` : logique identique à PySpark mais avec `snowflake.snowpark.functions` et double-guillemets sur les identifiants
  - `expr()` : essaie `SF.expr()` puis `SF.sql_expr()` en fallback (compatibilité multi-versions Snowpark)
  - Capabilities : `clustering`, `zero_copy_clone`, `cache_result`, `window`
- `tests/test_backend_snowpark.py` : 56 tests unitaires, session Snowpark entièrement mockée (aucune connexion Snowflake requise).

**Phase 7 — SQLBackend (base class) + BigQueryBackend**
- `src/skifer/backends/sql_base.py` : infrastructure SQL complète :
  - `SQLColumn` — fragment SQL avec opérateurs Python (`==`, `!=`, `>`, `~`, `.alias()`, `.cast()`, `.isin()`, `.like()`, etc.)
  - `SQLCondition` — condition booléenne SQL avec `&`, `|`, `~`
  - `SQLWhenExpr` — expression `CASE WHEN … END` chaînable (`.when()`, `.otherwise()`)
  - `SQLQuery` — lazy query builder immutable (clone pattern). Accumule `filter → join → select → write` sans exécuter de SQL. Les JOINs wrappent les deux côtés en sous-requêtes. `filter` après `with_column` wrappe automatiquement pour rendre les colonnes calculées requêtables (pattern row_number → filter `_rn = 1`).
  - `SQLBackend` — classe abstraite implémentant le Protocol complet via génération SQL. `DataFrame` = `SQLQuery`. SQL matérialisé uniquement à `write_table` / `count`. `drop_duplicates` utilise `QUALIFY ROW_NUMBER()` (BigQuery/Snowflake) ; `drop_nulls` chaîne des `IS NOT NULL`. `row_number_over` génère la window function et délègue le wrapping à `filter`.
- `src/skifer/backends/bigquery.py` : `BigQueryBackend` étendant `SQLBackend`. Import lazy. Spécificités BigQuery :
  - FQN backtick-quoted : `` `project.dataset.table` `` (chemin doté dans un seul backtick)
  - Schémas = Datasets BigQuery ; Catalogues = GCP Projects
  - `ensure_schema_exists` → `client.create_dataset(exists_ok=True)`
  - `drop_table` → `client.delete_table(not_found_ok=True)`
  - `optimize_table` : best-effort — recrée la table avec `CLUSTER BY` ; sans clés → no-op
  - `get_current_user` : service account email > `SESSION_USER()`
  - Capabilities : `qualify`, `window`, `clustering`, `select_except`
- `tests/test_backend_sql_base.py` : 110 tests purs Python (SQLColumn, SQLCondition, SQLWhenExpr, SQLQuery, SQLBackend — aucune dépendance externe)
- `tests/test_backend_bigquery.py` : 30 tests, client BigQuery entièrement mocké (aucune connexion GCP requise)

## [0.8.0] - 2026-04-21

### Fixed
- **Agentic / GenBIAgent**: `_format_kpi` no longer swallows all exceptions silently. Only `IndexError`/`KeyError` (empty result or missing column) are caught and return `kpi_value=None`; Spark exceptions (e.g. unknown table) now propagate to `_execute_query` and surface as an error response.
- **Demo notebook**: cell 41 now guards against `kpi_value=None` — displays raw data via `.show()` when the scalar value cannot be extracted, instead of raising `TypeError`.

### Added
- **Core / Governance**: `allow_raw_sql` per-environment flag in `config.yaml`. When set to `false`, the `sql:` filter operator and the `expr:` select operation both raise a `ValueError` at schema processing time. Default is `true` — no breaking change. Recommended to set `allow_raw_sql: false` on `PROD` environments.
- **Core / Refactoring**: `SkiferEngine._get_workspace_client()` — centralized cached helper for Databricks SDK access. Replaces four identical `WorkspaceClient(host, token)` instantiations in `_patch_connect_user_context`, `_check_catalog_access`, `_get_clean_username`, and `_drop_table_if_exists`. Credentials are read once; the result is cached for the engine instance lifetime.

### Changed
- **Core / Operators**: Canonical English names for all filter operators and `when:` conditions. Old SQL abbreviations (`eq`, `gte`, `notlike`, etc.) kept as aliases — fully backward compatible.

### Added
- **Core / Operators**: New filter operators: `starts_with`, `ends_with`, `not_contains` (canonical), `not_like` (canonical).
- **Core / Operations**: New `select_final` operations: `lower`, `round:N`, `abs`, `length`, `to_date:fmt`, `nvl:val`.
- **Core / Operations**: Unknown operation now logs a warning instead of silently passing the column through.
- **Registry**: `RuleRegistry.list_loaders()` method for API symmetry with `list_rules()`.
- **Core**: `run_union_sources_to_table` now accepts `dedup_after_union=True` parameter (default: True, existing behavior). Set to False to keep all rows after union. Dedup outcome is now logged explicitly.
- **Core**: `quality_checks` block at table level in schema dict. Supports `drop_nulls_in: [cols]` (`df.dropna`) and `drop_duplicates_on: [cols]` (`df.dropDuplicates`). Applied after `preprocess.qualify`, before joins.
- **Core**: `force_env` parameter on `SkiferEngine.__init__` to bypass auto-detection and force a specific environment (e.g. `force_env="LOCAL"`). Raises `ValueError` if env not in config.
- **Core**: `dev_limit` option at schema or table level. Applies `df.limit(N)` in non-prod interactive mode only — silently ignored in job/prod runs.
- **Schema Loader**: `load_schema(path, params=None)` — loads a pipeline schema from a YAML file. Searches upward from cwd. Supports `{{ key }}` parameter injection.
- **Schema Loader**: `parse_schema(yaml_str, params=None)` — loads a pipeline schema from an inline YAML string. Same normalization and parameter injection as `load_schema`.
- **Schema Loader**: Both functions normalize compact syntax: filter strings (`"col:op:val"`), compact joins (`[alias, key]`), 2-element `select_final` rows, `literal:value` shorthand.
- **Core**: `engine.default_params` property returning `{"catalog": ..., "env": ...}` for use with `load_schema`.
- Both `load_schema` and `parse_schema` exported from `skifer` top-level package.
- **Core**: `filter_groups` key at table level — list of filter lists. Conditions within a group are AND-combined; groups are OR-combined. Coexists with existing `filter` key.
- **Core**: `keep_all_columns: true` + `add_columns` — keep all columns from the joined DataFrame and append computed columns. Mutually exclusive with `select_final`.
- **Core**: Multi-condition `when/elseif/else` in `select_final` using dict form with `when`, `then`, `else` keys. Generates chained `F.when(...).when(...).otherwise(...)`.
- **Schema Loader**: `literal:value` shorthand in `select_final` source position — `[literal:ERP, source_system]` creates a constant column without a null source.
- **Core / Sandbox**: Transparent sandbox resolution in `process_schema`. In interactive non-prod mode, source tables are automatically resolved to their sandboxed equivalents (`schema_XXXX.table`). Missing sandbox tables are cloned from the main schema via `SHALLOW CLONE` (Databricks) or `CREATE TABLE AS SELECT` (local).
- **Core / Sandbox**: New `SandboxResolver` class (`core/sandbox.py`) handling schema/table existence checks, schema creation, and table cloning. Isolated for testability.
- **Core / Sandbox**: `sandbox.missing_table: copy | error` option in `config.yaml` (default: `copy`).
- **Core**: User suffix now persisted to `.skifer_user` file next to `config.yaml` on first derivation. Read on subsequent inits for reliability. Add `.skifer_user` to your `.gitignore`.
- **Core**: `engine.describe_schema(schema_dict)` — dry-run summary printed to stdout. Shows sources with sandbox resolution preview, filters, quality checks, joins, business rules, and output columns. No Spark execution.

## [0.7.0] - 2026-04-20

### Added (previous)

- **Local Development Mode** (`spark_factory.py`, `core/core.py`, `core/config.py`): Full offline development without a Databricks cluster. New `spark_factory.get_spark_session()` factory auto-detects the execution context in priority order: (1) active Databricks notebook session, (2) Databricks Connect v2 via `DATABRICKS_HOST / DATABRICKS_TOKEN / DATABRICKS_CLUSTER_ID`, (3) local PySpark `local[*]` with Delta Lake and Apache Derby embedded metastore. No installation beyond `pip install skifer` required for local mode.
- **Adaptive FQN** (`core/core.py` — `_build_fqn`): Table fully-qualified names are now 3-part (`` `catalog`.`schema`.`table` ``) on Databricks and 2-part (`` `schema`.`table` ``) in local mode, where no Unity Catalog is available.
- **Auto schema creation** (`core/core.py` — `_ensure_schema_exists`): In local mode, `CREATE DATABASE IF NOT EXISTS` is called before each write so the target schema is always ready in Derby.
- **Local Delta write** (`core/core.py` — `_write_dataframe_local`): `saveAsTable` is incompatible with PySpark 4.x + Delta 4.x in local mode (`CANNOT_CREATE_DATA_SOURCE_TABLE.EXTERNAL_METADATA_UNSUPPORTED`). Local writes use `df.write.format("delta").mode("overwrite").save(path)` followed by `CREATE TABLE IF NOT EXISTS … USING DELTA LOCATION path`, which is fully supported.
- **OS user fallback** (`core/core.py` — `_get_clean_username`): Added `getpass.getuser()` as a 4th strategy after Spark SQL, Databricks SDK, and DBUtils. Ensures the sandbox suffix is always resolved correctly in local mode.
- **Null-catalog local env** (`core/config.py`, `core/core.py`): Setting `catalog: null` in `config.yaml` marks an environment as local. `_check_catalog_access` short-circuits to `True` immediately (no `SHOW SCHEMAS IN` attempt). `_detect_environment` logs a clear "Local environment selected" message instead of a generic success.
- **`config.yaml` template**: Added `config.yaml` at project root with a `LOCAL` environment (null catalog) and placeholder `DEV / QA / PROD` Databricks environments. `default_env: LOCAL` acts as a safety net when no remote catalog is reachable.
- **Semantic / LLMProvider** (`semantic/llm_provider.py`): Provider-agnostic `LLMProvider` ABC with `get_llm_provider()` factory. Supports OpenAI, Anthropic, and Google Gemini. Auto-detected from env vars (`ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, `GOOGLE_API_KEY`, `LLM_PROVIDER`). Optional deps: `pip install skifer[llm-openai|llm-anthropic|llm-google]`.
- **Semantic / SemanticEngine** (`semantic/semantic.py`): Catalog-first + lazy-loading architecture. Only `semantic_catalog.yaml` is loaded at startup (O(1)); full YAMLs are loaded on-demand and cached. New methods: `list_models(tags, layer, summary)`, `reload_catalog()`, `get_model_summary()`. `query()` and `create_view()` accept a `SemanticQuery` and execute via `spark.sql()`. Target schema resolved from `semantic_views_schema` in `config.yaml` with sandbox suffix. Static `update_catalog()` to maintain `semantic_catalog.yaml`.
- **Semantic / SemanticBuilder** (`semantic/builder.py`): Generates semantic YAML models via LLM from Jupyter notebooks, rule docstrings, and a Business Glossary. Validation loop (max 3 attempts) — on failure, error context is written to `.errors/`. Automatically updates `semantic_catalog.yaml` after each build.
- **Semantic / SemanticValidator** (`semantic/validator.py`): Validates a semantic YAML model structure. Checks required fields, aggregation types, duplicate names, missing SQL. Returns a `ValidationResult` with separate errors and warnings.
- **Semantic / NotebookExtractor + RuleInspector** (`semantic/extractor.py`): `NotebookExtractor` extracts business context from Jupyter notebooks (Markdown + SQL cells). `RuleInspector` extracts docstrings from rule functions in a Python file or direct reference.
- **Semantic / GlossaryReader** (`semantic/glossary.py`): Reads a Business Glossary from JSON, YAML, TXT, PDF (pypdf/pdfminer), or PPTX (python-pptx). Returns content as text injected into LLM prompts.
- **Agentic / QueryResolver + SemanticQuery** (`agentic/resolver.py`): `SemanticQuery` is the LLM/execution boundary object — contains only names from the YAML, never raw SQL. `QueryResolver` (deterministic, zero LLM) validates each name against the YAML and builds the complete SQL. Raises `SemanticQueryError` on unknown names — no hallucinated SQL. Levenshtein suggestions via `difflib`. Supports: `sum`, `count_distinct`, `count`, `avg`, `min`, `max`, `CASE WHEN` filters, model `base_filter`, operators `eq/neq/gt/lt/gte/lte/in/like/is_null/is_not_null`, automatic date ranges on `type: date` dimensions.
- **Agentic / AgentResponse + FormattedResult + ResponseFormat** (`agentic/models.py`): `ResponseFormat` enum (kpi / table / chart / text_analysis). `FormattedResult` with `data` (source-of-truth DataFrame) + format-specific fields. Structured `AgentResponse` with `success` and `needs_clarification` properties.
- **Agentic / SessionHistory + HistoryEntry** (`agentic/history.py`): `HistoryEntry` logs one interaction (timestamp, question, model, serialized result). `SessionHistory` container with `to_pdf()` (delegated to `HistoryExporter`) and `to_json()` / `from_json()`.
- **Agentic / HistoryExporter** (`agentic/exporter.py`): Exports a `SessionHistory` to PDF via `fpdf2`. Structure: cover page + one section per interaction. Format-aware rendering: KPI value, Markdown table, PNG base64 chart (matplotlib), narrative text. Optional dep: `pip install skifer[semantic-pdf]`.
- **Agentic / GenBIAgent** (`agentic/agent.py`): 2-step LLM pipeline — Step A selects the model from the compact catalog (~500 tokens), Step B translates to `SemanticQuery` (structured JSON output). Deterministic `QueryResolver` in between. Clarification loop: ambiguity or unknown name → `AgentResponse(mode="needs_clarification")` with suggestions. Optional multi-turn conversational history. Auto-formatting by `ResponseFormat`. Step D LLM for narrative summaries. Integrated `SessionHistory` — auto-logs every `ask()`.
- **pyproject.toml**: Optional dependency groups `llm-openai`, `llm-anthropic`, `llm-google`, `semantic-pdf`, `semantic-full`.
- **Tests**: `test_llm_provider.py`, `test_resolver.py`, `test_semantic_engine_catalog.py`, `test_validator.py`, `test_history.py`, `test_semantic_builder.py`.

## [0.6.4] - 2026-03-26

### Added
- **Doc**: Initial version of the documentation

## [0.6.3] - 2026-03-25

### Fixed
- **Core**: Root cause fix for `Missing required field 'UserContext'` on all gRPC execution calls (`saveAsTable`, `collect`, DDL commands) when running locally with Databricks Connect v2. The root cause: `SparkConnectClient._user_id` was empty after session creation, so PySpark's `execute_command` never injected `user_context` into gRPC requests — causing the server to reject every execution call. New `_patch_connect_user_context()` method (called at init after `_patch_connect_debugging()`): checks if `spark.client._user_id` is absent, fetches the current user email via Databricks SDK REST API (not affected by the gRPC issue), and injects it directly into the client. One-time init patch with no functional side effects. This fixes all remaining `saveAsTable`, `isEmpty()`, and DDL failures in a single place rather than patching each call site individually.
- **Core**: `_write_dataframe`: `df.isEmpty()` (which triggers `collect()` via gRPC) is now wrapped in try/except as a secondary guard. If it fails, the write proceeds unconditionally.
- **Core**: `optimize_table`: distinguishes `UserContext` gRPC errors (skipped with a warning — `OPTIMIZE` has no SDK REST equivalent) from real errors (still re-raised).

## [0.6.2] - 2026-03-25



## [0.6.1] - 2026-03-25

### Changed
- **Core**: Renamed and generalized the three main execution patterns:
  - `run_follow_schema` → `run_process_to_table`: processes a schema dict and writes to a single Delta table.
  - `run_split_to_org` → `run_process_and_split`: processes a schema dict then splits the result into multiple tables based on any column value. `org_list` (with `org_code`/`label`) replaced by `split_values` (with `value`/`label`) and `split_column` is now a required positional argument. No longer tied to org/sales_org logic.
  - `run_unify_and_process` → `run_union_sources_to_table`: unions source partition tables, processes the schema, writes to a single table. `org_list` replaced by `source_partitions` (list of `{"label": ...}`), `unified_source_base_names` → `source_base_names`, `unified_temp_view_key` → `source_alias`.

### Fixed
- **Core**: `run_process_and_split` (ex `run_split_to_org`): `df.cache()` triggered a gRPC `AnalyzePlan` call that failed with `Missing required field 'UserContext'` on Databricks Connect v2 from local environments (Windows/PyCharm). The `cache()` and `unpersist()` calls are now wrapped in try/except: if `cache()` fails, execution continues without caching (no functional impact, only a performance optimisation is skipped). A warning is printed to indicate the fallback.
- **Core**: `_drop_table_if_exists` now uses two strategies: (1) `spark.sql("DROP TABLE IF EXISTS ...")` as primary (works natively on Databricks), (2) `WorkspaceClient.tables.delete()` REST API as fallback (works locally when gRPC DDL calls fail with `Missing UserContext`). Mirrors the pattern already used by `_check_catalog_access`.

## [0.5.10] - 2026-03-25

### Added
- **Utils**: New `skifer.utils.safe_columns(df)` helper. On native Databricks it delegates to `df.columns` (AnalyzePlan gRPC). On Databricks Connect v2 where AnalyzePlan fails with `Missing UserContext`, it returns `[]` so that optional-column branches in rules are silently skipped rather than crashing. Exported from the top-level package as `from skifer import safe_columns`.
- **Tests**: Added `tests/test_utils_safe_columns.py` with 5 tests covering the happy path, both gRPC error patterns, unrelated exception re-raise, and top-level import.

### Fixed
- **Rules** (`rules/common.py`, `rules/ch_rules.py`, `rules/kpis.py`, `rules/loader.py`): Replaced all 26 `df.columns` / `df_a.columns` occurrences with `safe_columns(df)`. Each `df.columns` access was an `AnalyzePlan` gRPC call that failed with `UserContext` on Databricks Connect v2 from local environments. Added `from skifer import safe_columns` import to each file.
- **Registry**: Added `skifer/registry.py` compatibility shim so that user rule files using `from skifer.registry import RuleRegistry` continue to work without modification. The canonical location remains `skifer.core.registry`; the shim re-exports the same singleton class. All three import forms now resolve correctly.

### Added
- **Tests**: Added `tests/test_registry_import_paths.py` with 2 tests asserting all import paths resolve to the same `RuleRegistry` singleton.

## [0.5.9] - 2026-03-25

### Fixed
- **Core**: Resolved `SparkConnectGrpcException` (`Missing required field 'UserContext'`) in `process_schema` JOIN logic. `df_to.columns` triggered an `AnalyzePlan` gRPC call (schema inspection) during join planning, which fails with UserContext on Databricks Connect v2 from local environments. The JOIN is now built using two strategies that avoid schema inspection: (1) when `on_from == on_to`, use PySpark's list-based join (`df.join(other, on=['key'])`) which deduplicates automatically without schema fetch; (2) when keys differ, build the condition from DataFrame column references (`df_main[col] == df_to[col]`) and drop right-side keys post-join using `df_to[col]` references — all lazy, no `AnalyzePlan` RPC.

### Added
- **Tests**: Added `tests/test_core_join.py` with 5 unit tests asserting that `df_to.columns` is never called in any join scenario (same keys, different keys, single key, multi-key).

## [0.5.8] - 2026-03-25

### Fixed
- **Core**: Resolved `SparkConnectGrpcException` (`Missing required field 'UserContext'`) that blocked every `F.col()` / DataFrame operation when running locally with Databricks Connect v2. PySpark Connect's internal `is_debugging_enabled()` function calls `spark.conf.get("spark.python.sql.dataFrameDebugging.enabled")` via gRPC before any user code runs; this RPC call fails with UserContext on some Windows + PyCharm configurations. The new `_patch_connect_debugging()` method (called immediately after session creation) tries the conf.get(); if it fails, it directly pre-populates the PySpark module-level cache `_enable_debugging_cache = False`, preventing any further failing gRPC call for the lifetime of the session. No functional impact on pipeline execution.

### Added
- **Tests**: Added `tests/test_core_connect_patch.py` with 4 unit tests covering the happy path, cache patching, idempotency, and resilience against PySpark internal changes.

## [0.5.7] - 2026-03-25

### Fixed
- **Core**: Resolved `unknown_user` detection failure when running locally with Databricks Connect v2. `SELECT current_user()` fails via gRPC for the same `Missing UserContext` reason as catalog checks. `_get_clean_username` now uses three strategies in order: (1) Spark SQL, (2) Databricks SDK REST API (`WorkspaceClient.current_user.me()`), (3) DBUtils notebook context tags. This prevents the sandbox suffix from being set to `_unknown_user`, which caused downstream table loading to target non-existent schemas (e.g. `bronze_unknown_user`).

### Added
- **Tests**: Added `tests/test_core_username.py` with 6 unit tests covering all three strategies and edge cases for `_get_clean_username`.

## [0.5.6] - 2026-03-25

### Fixed
- **Core**: Resolved `Missing required field 'UserContext'` gRPC error that occurred during environment auto-detection when running locally with Databricks Connect v2 (PyCharm / VS Code on Windows). The `SHOW SCHEMAS IN` command issued via Databricks Connect fails for DDL/catalog metadata operations because the gRPC layer does not propagate the user context. The engine now uses a two-strategy approach in the new `_check_catalog_access` method: (1) Spark SQL as primary (works natively on Databricks), (2) Databricks SDK REST API (`WorkspaceClient.catalogs.get()`) as fallback, which uses HTTP and is not affected by the gRPC limitation.

### Added
- **Core**: New `_check_catalog_access(catalog)` method encapsulating the two-strategy catalog verification logic.
- **Core**: `default_env` fallback in `_auto_detect_environment`. If all catalog checks fail (e.g., no network access to the remote cluster during local dev), the engine now uses the `default_env` key from `config.yaml` instead of raising a fatal `ValueError`. No change in behavior if `default_env` is not defined.
- **Tests**: Added `tests/test_core_env_detection.py` with 10 unit tests covering all strategies and fallback scenarios for `_check_catalog_access` and `_auto_detect_environment`.

## [0.5.5] - 2024-XX-XX

### Fixed
- **Core**: Resolved `SparkConnectGrpcException` (`Missing required field 'UserContext'`) for Windows users on Databricks Connect v2. The engine now explicitly extracts `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, and `DATABRICKS_CLUSTER_ID` from the environment variables (loaded via `.env`) and directly injects them into the `Config()` object during `DatabricksSession` initialization. This bypasses the sometimes unreliable implicit profile resolution on Windows.

## [0.5.4] - 2024-XX-XX

### Fixed
- **Core**: Refactored the `config.yaml` and `.env` discovery mechanisms. The engine now correctly searches upwards from the *current working directory* (where the user runs the script) rather than from the library's internal installation directory (`site-packages`). This allows users to place their `.env` file at the root of their own project to securely authenticate Databricks Connect.

## [0.5.3] - 2024-XX-XX

### Fixed
- **Core**: Resolved `SparkConnectGrpcException` (`Missing required field 'UserContext'`) when initializing `DatabricksSession` via Databricks Connect v2. The engine now correctly initializes the session using `DatabricksSession.builder.sdkConfig(Config()).getOrCreate()` to implicitly load the user context from the local Databricks profile or environment variables. Added a specific check to load `DATABRICKS_CLUSTER_ID` from the environment if present.

## [0.5.2] - 2024-XX-XX

### Fixed
- **Core**: Resolved `SparkConnectGrpcException` (`Missing required field 'UserContext'`) when initializing `DatabricksSession` via Databricks Connect. The engine now uses `DatabricksSession.builder.sdkConfig(Config()).getOrCreate()` to properly load local credentials and context.

## [0.5.1] - 2024-XX-XX

### Added
- **Project Structure**: Reorganized the codebase into a modern `src/skifer/` layout for proper packaging and distribution as a Python library.
- **Packaging**: Created `pyproject.toml` with build configurations, dependencies (`pyspark`, `delta-spark`, `pyyaml`, `python-dotenv`), and development tools (`pytest`, `pytest-mock`, `ruff`).
- **Tests**: Implemented a comprehensive test suite in the `tests/` directory:
  - `test_core.py`: Unit tests for helpers (`_apply_operation`, `_build_filter_expression`), schema processing (`get_select_expressions`, `process_schema` including `preprocess/qualify`), and error handling.
  - `test_loaders.py`: Integration tests for data loaders (`load_and_union_tables`, `load_generic_explode_union`).
  - `test_config.py`: Unit tests for environment detection and configuration loading using `tmp_path`.
  - `conftest.py`: Added a robust PySpark session fixture tailored for local testing (handling Delta Lake dependencies via `configure_spark_with_delta_pip` and macOS localhost resolution).
- **Documentation**: Added detailed Google/Sphinx style docstrings to all major classes and methods in `core.py`, `config.py`, `loaders.py`, `registry.py`, `semantic.py`, and `agent.py`.

### Changed
- **Config**: Replaced the state-altering `USE CATALOG` command with a read-only `SHOW SCHEMAS IN` command in `ConfigurationManager._detect_environment` to prevent initialization errors in local/testing environments.
- **Core**: Broadened the exception handling during `DatabricksSession` initialization (Databricks Connect) in `SkiferEngine.__init__` to catch generic `Exception` (rather than just `ImportError`). This prevents crashes caused by transitive missing dependencies like `zstandard` on local machines, allowing a graceful fallback to a standard `SparkSession`.

### Fixed
- **Testing**: Fixed `JAVA_GATEWAY_EXITED` issues by aligning Java versions and utilizing the native `delta-spark` helper (`configure_spark_with_delta_pip`) instead of fragile Maven package downloads.
- **Testing**: Fixed `[SCHEMA_NOT_FOUND]` and `[SCHEMA_NOT_EMPTY]` errors during runner tests by correctly initializing and dropping testing schemas (`spark_catalog.gold_test_user`).
- **Core**: Corrected a brittle assertion in `get_select_expressions` tests that failed due to internal PySpark string representation changes.

## [0.5.0] - Initial Framework Setup
- Initial implementation of the declarative engine.
- Bronze/Silver/Gold pipeline support.
- Rule Registry implementation.
- Basic LLM agent integration for semantic querying.
