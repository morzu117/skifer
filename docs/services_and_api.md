# Services and local API

Plan 31 adds a transport-neutral application layer for local tools and a thin,
optional HTTP adapter. The service classes are the authority boundary: the CLI,
FastAPI routes, and future clients delegate to them instead of reproducing
validation, scope checks, serialization, or execution behavior.

## Install and run

FastAPI and Uvicorn are optional:

```bash
pip install -e ".[api]"

# Bind the project API to 127.0.0.1:8000.
skifer api serve --project . --port 8000

# Print a deterministic OpenAPI document without starting a server.
skifer api openapi --project .
```

The server always binds to `127.0.0.1`; the CLI does not expose a host option.
CORS accepts only `localhost` and `127.0.0.1` origins. `GET /health` returns the
constant `{"status": "ok"}` and contains no project data.

## Application services

`build_services(project_dir)` creates a `ServiceContainer` without opening a
Spark session. Its SQLite stores live in the project directory; semantic catalog
reads remain Spark-free until an execution session is connected.

| Service | Responsibility | Principal scopes |
|---|---|---|
| `ProjectService` | Open a project, inspect or write pipeline YAML, expose the JSON Schema and operation catalog, run coverage audit | `project:read`, `pipelines:write` |
| `RuleService` | Discover and inspect rules, build dependency graphs, generate snippets, persist rule files | `project:read`, `rules:write` |
| `SemanticService` | Browse models and check/write/promote managed semantic drafts | `models:read`, `pipelines:write` |
| `GovernanceService` | Contracts, data products, certifications, runs, dictionary, registry lineage and impact | `contracts:read`, `lineage:read` |
| `QualityService` | Derived checks, report history, incident listing and transitions | `contracts:read`, `incidents:write` |
| `AgentService` | Conversational queries and builder-agent requests with allowlisted results | `query:execute`, `pipelines:write` |
| `ExecutionService` | Lazy runtime session, background jobs, cancellation, logs, bounded results | `execute:run` |

Every method receives a `RequestContext`. `require_scope()` checks an exact name
from `NAMED_SCOPES`; limits are hard refusals rather than silent clamps. DTOs are
converted to JSON-native values through explicit fields, not a generic dump of
internal objects.

For the local API, `LocalIdentity` obtains the subject from the local environment
and supplies a static set of local scopes. Request fields cannot add authority.
The `certification_override` scope is deliberately absent from both
`NAMED_SCOPES` and the local identity.

## HTTP routes and scopes

Routes are thin adapters over the same container. Each business route declares
its scope through `Scope(...)`; the route modules do not import `SkiferEngine` or
the core runtime.

| Route group | Scope |
|---|---|
| `/project`, `/config`, read-only `/pipelines`, read-only `/rules`, `/me` | `project:read` |
| `PUT /pipelines/*`, semantic draft write/promote, `POST /agents/build` | `pipelines:write` |
| `PUT /rules/*` | `rules:write` |
| `/catalog` and `POST /semantic/check` | `models:read` |
| `POST /semantic/query` and `POST /agents/ask` | `query:execute` |
| contracts, data products, certifications, quality history/check definitions, incident reads | `contracts:read` |
| lineage and dictionary | `lineage:read` |
| incident acknowledge/assign/resolve | `incidents:write` |
| `/session` and `/jobs` | `execute:run` |

Service errors have stable HTTP mappings. Unknown resources are not exposed as
raw Python exceptions; the explicit service error hierarchy determines the
public status and response code.

## Async execution

Connect once, submit a job, then poll its status, logs, and result:

```text
POST /session/connect
POST /jobs                 {"kind": "run", "path": "schemas/gold/orders.yaml"}
GET  /jobs/{job_id}
GET  /jobs/{job_id}/logs?after=0
GET  /jobs/{job_id}/result
```

Supported job kinds are `preview`, `run`, `full_refresh`, and `check`. Version 1
allows one active job per project; a second submission is an explicit conflict.
The identifier minted at submission is passed to the engine, so
`job_id == run_id` throughout publication, certification, logs, and the final
`ResultView`.

`ResultView` is JSON-native and bounded. Depending on the job kind, it carries
rows and schema plus the monitor report, publication decision, quarantine
summary, or a failure. Cancellation uses the available Spark job-group
or job-tag mechanism; it does not create a second execution path.

## Metadata registry and cross-pipeline lineage

`DatasetRecord` stores the physical target, pipeline path, data-product and
contract identity, definition hash, owner, columns, latest run ID, and serialized
column-lineage graph. `SqliteMetadataStore` is the local backend and
`DeltaMetadataStore` is the Databricks backend. Upsert is idempotent for an
unchanged definition.

Indexing is static and opens no Spark session:

```bash
skifer index schemas/silver/orders.yaml --db .skifer_metadata.db
skifer dictionary catalog.silver.orders --db .skifer_metadata.db
skifer lineage catalog.silver.orders.customer_id --direction up --format json
skifer lineage catalog.silver.orders --direction down --format mermaid
```

`MetadataRegistryQuery` merges the stored graphs, rejects cycles, and follows
upstream or downstream column closures across pipeline boundaries. Its impact
report lists downstream datasets, columns, edges, and whether traversal hit the
configured depth bound. The same registry operations are available through
`GovernanceService`; dictionary and lineage have local API routes.

After a successful certified publication, metadata indexing attaches the same
run ID. Indexing and incident hooks are non-blocking: a registry failure emits a
warning but cannot change the publication decision.

## Governance coverage audit

The audit reports five Spark-free coverage metrics for pipeline YAML files:
`data_product`, `contract`, `structured_owner`, `field_descriptions`, and
`field_classification`. Invalid or unreadable files stay in the denominator and
are reported as unparsed.

```bash
skifer audit "schemas/**/*.yaml"
skifer audit "schemas/**/*.yaml" --json
skifer audit "schemas/**/*.yaml" --min-coverage 90
```

The command exits `2` when a non-empty audit falls below `--min-coverage`, which
makes the threshold directly usable in CI. See [pipeline governance metadata](yaml_spec.md#pipeline-governance-metadata-plan-31)
for the fields measured by the audit.
