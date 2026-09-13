# Data Observability

**Data quality checks derived automatically from your existing YAML schemas — no additional configuration required.**

Skifer's YAML schemas are already data contracts:

- `quality_checks` → null and uniqueness constraints
- `filter` → business invariants expected on the data
- `select_final` → output schema (columns, types via `cast:`)

Most observability tools (Great Expectations, Soda, Monte Carlo) require you to re-declare these rules separately. Skifer already has them in the YAML.

## Contract identity and ODCS export

`canonicalize_contract(parse_to_ir(schema))` creates the stable identity used by
the certification registry. Canonicalization version 2 hashes the data-product
ID/version, output fields, grain, semantic seed, SLA, and security with SHA-256.
Field order is preserved and JSON object keys are sorted. Ownership,
descriptions, lifecycle status, reviewers, and effective dates are deliberately
excluded, so workflow or documentation edits do not invalidate certification.

`export_odcs_31(schema, definition)` exports the supported surface to an ODCS
3.1-shaped document: fundamentals, output properties, required/unique quality
rules, owner team and a read role. It returns warnings for metadata that has no
portable ODCS mapping rather than dropping it silently.

`import_odcs_31(document)` performs the supported reverse mapping; the CLI
prints the resulting `data_product` and `contract` blocks with explicit warning
comments:

```bash
skifer contract import contract.odcs.yaml
```

`diff_contracts(old, new)` detects additions and breaking removals, retypes,
required hardening, classification downgrades, and SLA relaxation. The complete
YAML surface is in [Pipeline governance metadata](yaml_spec.md#pipeline-governance-metadata-plan-31).

Certification persistence is append-only. `SqliteCertificationStore` is the
local implementation; `DeltaCertificationStore` uses the same event IDs to
deduplicate retries on Databricks. Neither store rewrites historical runs.

Certified publication is opt-in for batch pipelines that declare
`data_product` plus a valid `contract.output` and whose `SkiferEngine`
was built with both `monitor=` and `certification_store=`. The write path is
staging → monitor checks → stored check results → promotion, or quarantine on a
critical failure; schemas without `data_product` keep the direct write and
post-write monitor behaviour. Streaming tables, JDBC sinks and materialized
views are refused for certified publication in this version.

`examples/02_quality_and_contract/` exercises the passing branch and prints the
`CERTIFIED` record after the staged batch is promoted. The complementary
`examples/10_monitor_and_quarantine/` publishes a good batch, rejects a later
batch with a critical `NullCheck`, and prints both the unchanged consumer table
and the row-tagged quarantined snapshot.

Freshness has two distinct meanings: `DataFreshnessCheck` (the compatible
`FreshnessCheck`) evaluates an event-time column, while `LoadFreshnessCheck`
uses the latest `PROMOTED` certification event. They must not be substituted
for one another.

---

## Quickstart

```python
from skifer import load_schema, SkiferEngine
from skifer.observability.monitor import DataMonitor
from skifer.observability.history import SqliteHistoryStore
from skifer.observability.reporter import MonitorReporter

engine = SkiferEngine()
schema = load_schema("schemas/silver/fact_orders.yaml", params=engine.default_params)

store = SqliteHistoryStore()  # or DeltaHistoryStore for production
monitor = DataMonitor(backend=engine.backend, history_store=store)

report = monitor.check_from_schema("catalog.silver.fact_orders", schema)
print(MonitorReporter().to_text(report))
```

---

## Architecture

```
observability/
  checks.py      # DataContract dataclasses — NullCheck, UniqueCheck, TypeCheck, ...
  contracts.py   # ContractExtractor — YAML → DataContract list
  monitor.py     # DataMonitor + MonitorReport
  history.py     # HistoryStore Protocol + SqliteHistoryStore + DeltaHistoryStore
  reporter.py    # MonitorReporter — JSON / text / HTML
  alerts.py      # AlertDispatcher — webhook, Slack, email
  metadata_store.py # DatasetRecord registry — SQLite and Delta
  metadata_index.py # Spark-free definition indexing
  incidents.py   # NEW/ACKNOWLEDGED/ASSIGNED/RESOLVED state machine and routing
  audit.py       # Spark-free governance coverage metrics
```

---

## Automatic checks from YAML

`ContractExtractor` reads a normalized schema dict and derives checks with no extra configuration:

| YAML source | Check generated | Default severity |
|---|---|---|
| `quality_checks.drop_nulls_in: [col_a]` | `NullCheck(column="col_a")` | `critical` |
| `quality_checks.drop_duplicates_on: [id]` | `UniqueCheck(columns=["id"])` | `critical` |
| `filter: "status:in:ACTIVE,PENDING"` | `FilterInvariantCheck(operator="in")` | `warning` |
| `select_final: [..., [cast:double]]` | `TypeCheck(expected_type="double")` | `warning` |

Checks derived from `quality_checks` are `critical` by default — they are already enforced at runtime, so a violation post-write indicates a bug.  
Checks derived from `filter` are `warning` by default — they express a business expectation, not a hard guarantee.

```python
from skifer.observability.contracts import ContractExtractor

contracts = ContractExtractor().extract(schema)
for c in contracts:
    print(f"[{c.severity}] {type(c).__name__}")
```

---

## Configurable checks via `observability:` section

Add an optional `observability:` block to any YAML schema for additional constraints:

```yaml
observability:
  freshness:
    max_delay: "2h"             # alert if MAX(updated_at) is older than 2 hours
    timestamp_column: updated_at
  volume:
    min_rows: 1000              # alert if < 1000 rows
    max_rows: 10000000          # alert if > 10M rows
    variation_threshold: 0.3    # alert if volume changes > 30% vs previous run
  schema_drift:
    enabled: true               # detect columns added or removed vs select_final
  custom_checks:
    - sql: "SELECT COUNT(*) AS result FROM {table} WHERE amount < 0"
      expect: 0
      severity: critical
```

| Config key | Check | Notes |
|---|---|---|
| `freshness` | `FreshnessCheck` | Supported units: `h`, `m`, `d`, `s` |
| `volume.min_rows` / `max_rows` | `VolumeCheck` | Either bound is optional |
| `volume.variation_threshold` | `VolumeVariationCheck` | Skipped silently if no history yet |
| `schema_drift.enabled` | `SchemaDriftCheck` | Expected columns derived from `select_final` |
| `custom_checks` | `CustomSqlCheck` | `{table}` placeholder replaced with actual FQN at runtime |

---

## DataMonitor

`DataMonitor` orchestrates check execution against a live table. It takes the engine's `SparkBackend` (in tests, any object implementing `sql(query) → DataFrame-like` works).

```python
monitor = DataMonitor(backend=engine.backend, history_store=store)

# From a schema dict (recommended)
report = monitor.check_from_schema(
    "catalog.silver.fact_orders",
    schema,
    raise_on_critical=False,   # set True to raise DataQualityError on critical failures
)

# From an explicit contract list
contracts = ContractExtractor().extract(schema)
report = monitor.check_table("catalog.silver.fact_orders", contracts)
```

---

## MonitorReport

```python
report.has_critical_failures()  # bool
report.failures()               # list[CheckResult] — all failed checks
report.summary()                # dict — total/passed/failed/critical/status (PASS|WARN|CRITICAL)
```

Each `CheckResult` carries:

| Field | Type | Description |
|---|---|---|
| `status` | `CheckStatus` | `PASS`, `FAIL`, `ERROR` or `SKIPPED` — distinct execution outcomes |
| `passed` | `bool` | Whether the check passed |
| `actual_value` | `Any` | Measured value (e.g. number of nulls found) |
| `expected_value` | `Any` | Expected value (e.g. `0`) |
| `message` | `str` | Human-readable description |
| `severity` | `str` | `"info"` \| `"warning"` \| `"critical"` |
| `timestamp` | `datetime` | When the check ran |

`passed` is retained for compatibility and is `True` only for `PASS`.
An unsupported check is `SKIPPED`, and an execution exception is `ERROR`; both
therefore block a critical contract when `raise_on_critical=True` rather than
being reported as a successful validation. `NullCheck` and
`FilterInvariantCheck` are row-scoped and expose a safely quoted SQL predicate
for their violating rows; aggregate checks remain dataset-scoped.

---

## Severity levels

| Level | Pipeline behaviour |
|---|---|
| `info` | Logged only |
| `warning` | Logged + alert dispatched (non-blocking) |
| `critical` | Logged + alert + raises `DataQualityError` if `raise_on_critical=True` |

---

## History

Store reports across runs to enable regression detection and volume trend analysis.

```python
from skifer.observability.history import SqliteHistoryStore, DeltaHistoryStore

# Local / tests
store = SqliteHistoryStore()              # default: .skifer_observability.db
store = SqliteHistoryStore(":memory:")    # in-memory for tests

# Production (Databricks)
store = DeltaHistoryStore(backend=engine.backend)  # default table: _observability.check_history

# Usage
store.store(report)
latest   = store.get_latest("silver.fact_orders")
last_10  = store.get_last_n("silver.fact_orders", n=10)
```

Add `.skifer_observability.db` to your `.gitignore`.

---

## Reporter

```python
from skifer.observability.reporter import MonitorReporter

reporter = MonitorReporter()

print(reporter.to_text(report))       # terminal output with ✅ 🚨 ⚠️ icons
json_str = reporter.to_json(report)   # pretty-printed JSON with results + summary
reporter.to_html(report, "report.html")  # standalone HTML with coloured table
```

---

## Alerts

```python
from skifer.observability.alerts import AlertDispatcher

dispatcher = AlertDispatcher()
dispatcher.dispatch(report, {
    "webhook_url":   "https://myserver.com/hooks/skifer",
    "slack_webhook": "https://hooks.slack.com/services/...",
    "msteams_webhook": "https://example.webhook.office.com/...",
    "google_chat_webhook": "https://chat.googleapis.com/v1/spaces/...",
    "email": {
        "host": "smtp.company.com",
        "port": 587,
        "user": "alerts@company.com",
        "password": "...",
        "to": ["data-team@company.com"],
    },
    "min_severity": "warning",   # "info" | "warning" | "critical"
})
```

All channels are best-effort — a send failure logs a warning but does not raise.

## Incidents and governed alert routing — Plan 31

A quarantined run opens one `NEW` incident for each distinct failed critical
check. The allowed state transitions are `NEW → ACKNOWLEDGED|ASSIGNED|RESOLVED`,
`ACKNOWLEDGED → ASSIGNED|RESOLVED`, and `ASSIGNED → ASSIGNED|RESOLVED`.
A later successful promotion automatically resolves open incidents for the
dataset with root cause `recovered`. Incident persistence and recovery hooks are
best-effort and cannot change the publication decision.

```bash
skifer incidents list --status NEW --target catalog.gold.orders
skifer incidents ack RUN_ID:NullCheck:customer_id
skifer incidents assign RUN_ID:NullCheck:customer_id --assignee alice@example.com
skifer incidents resolve RUN_ID:NullCheck:customer_id --root-cause upstream-fixed
```

`AlertRouter` starts with the governed owner of the affected dataset and adds
owners of downstream datasets found through the metadata registry, bounded by
depth and deduplicated by contact/channel. Incident alerts support generic
webhook, Slack, email, Microsoft Teams, and Google Chat.

Incident payloads are built from an explicit redacted shape: dataset, incident
ID, check type/column, severity, contract-version change when relevant, and
routing metadata. They never copy check messages, actual values, or expected
values. Separately, semantic evidence always redacts filter values whose columns
are in `EvidencePolicy.sensitive_columns`, even when disclosure of ordinary
filter values was requested; callers use that set for `restricted` and `pii`
fields.

### Alert routing at publication — Plan 35

Configure publication alerts on each environment in `config.yaml`:

```yaml
environments:
  prod:
    catalog: production
    alerts:
      webhook_url: "https://alerts.example.invalid/skifer"
      slack_webhook: "https://hooks.slack.com/services/..."
      msteams_webhook: "https://example.webhook.office.com/..."
      google_chat_webhook: "https://chat.googleapis.com/v1/spaces/..."
      email: {}
      min_severity: critical
      max_depth: 3
```

The `alerts` mapping accepts only `webhook_url`, `slack_webhook`,
`msteams_webhook`, `google_chat_webhook`, `email`, `min_severity`, and
`max_depth`. Configuration is validated at load time: an unknown key or a value
of the wrong type raises an error naming the environment and key, without
echoing the value.

A quarantine sends an incident alert. After a promotion, Skifer compares the
new definition hash with the last `PROMOTED` definition for the same target and
sends a breaking-change alert only when the hashes differ and the contract diff
is breaking. The first publication of a target, an unchanged hash, and a target
whose earlier publications predate recorded definitions do not produce a
breaking-change alert. `PublicationCoordinator.resume()` never sends an alert,
and a failing channel never fails the publication.

Both incident and breaking-change alerts are always critical, so
`min_severity` has no effect on them. Recipients include the dataset owner and
the owners of downstream datasets found in the metadata registry, with traversal
bounded by `max_depth`. Without a metadata store, configured channels are still
notified, but no owner can be resolved.

!!! warning
    Webhook URLs and SMTP credentials are secrets, while `config.yaml` is
    usually versioned. Do not commit real values. Skifer does not yet provide
    environment-variable interpolation for these settings.

---

## Integration with SkiferEngine

When a `DataMonitor` is provided to `SkiferEngine`, checks run automatically after every `run_process_to_table()` call:

```python
from skifer.observability.monitor import DataMonitor
from skifer.observability.history import DeltaHistoryStore

history = DeltaHistoryStore(backend=engine.backend)
monitor = DataMonitor(backend=engine.backend, history_store=history)

engine = SkiferEngine(monitor=monitor)

# Checks run automatically post-write:
engine.run_process_to_table(schema, target_layer="silver", target_table_name="fact_orders")
# → [Monitor] Running post-write quality checks on 'catalog.silver.fact_orders'...
# → [Monitor] PASS — 5/5 checks passed.
```

A `DataQualityError` is raised and the pipeline stops if any critical check fails.

---

## Standalone observability jobs

The monitor can also run independently of the ETL pipeline — scheduled as a separate Databricks Workflow or cron job:

```python
# Standalone observability job — no ETL pipeline required
engine = SkiferEngine()
schema = load_schema("schemas/silver/fact_orders.yaml", params=engine.default_params)

monitor = DataMonitor(backend=engine.backend, history_store=DeltaHistoryStore(engine.backend))
report = monitor.check_from_schema("catalog.silver.fact_orders", schema)

MonitorReporter().to_html(report, "/dbfs/reports/fact_orders_quality.html")
```

---

## Runtime tracing (Plan 29, feature 5)

Tracing is **off by default** and adds no mandatory dependency. Nothing about
business behaviour changes when it is enabled — that is the property the whole
design is built around.

```yaml
observability:
  tracing:
    exporter: none        # none | otlp | mlflow | dual
    required: false       # true → an export failure is an error, not a warning
    capture_prompts: false
    capture_sql: false
    user_identity: omit   # omit | hmac
    trace_location: null  # MLflow / Unity Catalog destination
```

Install the optional SDKs with `pip install -e ".[tracing]"`. If the extra is
missing, the exporter degrades to no tracing and says so once, naming what to
install; with `required: true` it raises instead.

### What a span may carry

Attributes go through an allowlist (`ALLOWED_ATTRIBUTE_KEYS`) — run and contract
ids, model key, decision, status, durations, counts, `sql_hash`, statement id,
provider and model. **Everything else is dropped**, including anything named
`question`, `prompt`, `sql_text` or a filter value. Rejections are recorded with
the key and the type, never the value.

Bounds apply per span: 32 attributes, 64 events, 256 characters per value. Span
names come from a fixed taxonomy and are never built from a variable — a table
name or a request id belongs in an attribute, or it multiplies cardinality on the
backend.

Errors record the exception's **class name** only. A backend error message
routinely quotes the offending value, which is the data the trace exists to avoid
carrying.

### User identity

`user_identity: omit` (the default) records nothing. `hmac` records a keyed
pseudonym, with the secret read from `SKIFER_TRACING_HMAC_SECRET` — never
from `config.yaml`, since a committed file is the last place a key should live.

**With `hmac` but no secret, the identity is omitted rather than hashed.** An
unkeyed digest of an email or an employee id is reversible by anyone who can
guess the input, and a rainbow table over a company directory is small.

### Unity Catalog permissions and retention

`trace_location` must already exist: the exporter **never creates it**. If it is
missing or inaccessible, initialisation fails with a message saying what to
create and where. Grant the workspace principal write access to that location
before enabling the exporter, and set retention there — the framework does not
manage trace lifetime.

### Dual export

`exporter: dual` sends to OTLP and MLflow together. If one fails, the other keeps
receiving spans; the failure is logged once and never reaches business code.

`examples/12_tracing/` prints a nested in-memory span tree, the keys and types of
rejected question/SQL/filter attributes, omitted identities, and a business
result that still returns after an exporter raises.

---

## OpenLineage

Skifer produces what a catalog displays — output schema, design-time column lineage, check
results, a versioned contract, a computed certification — and can push it out as
[OpenLineage](https://openlineage.io) `RunEvent`s. This is **off by default**, adds no mandatory
dependency, and, like `uc_mirror.py`, never changes what the pipeline it observes does.

```yaml
observability:
  lineage:
    emitter: none               # none (default) | http
    url: "https://lineage.example.internal"   # required when emitter: http
    endpoint: /api/v1/lineage   # default
    job_namespace: skifer       # default
    dataset_namespace: null     # default — resolved automatically, see below
    timeout_seconds: 5.0        # default; must be > 0 and <= 60
```

Any other key under `observability.lineage` (including `api_key`, `apiKey` or `token`) is
rejected at load with a message naming the key — the API key is read **only** from the
`OPENLINEAGE_API_KEY` environment variable, never from `config.yaml`. Validation never echoes the
configured `url` in an error message.

Load-time validation: `url` must be a string starting with `http://` or `https://`, and is
required when `emitter: http`; `endpoint` must be a non-empty string starting with `/`;
`timeout_seconds` must be a number greater than 0 and at most 60. Any violation fails at load,
before any event is ever built.

### When events are emitted

Certified publication (`data_product:` pipelines, `PublicationCoordinator`) and a non-certified
batch write from `run_process_to_table` are the only two sources in v1. Streaming tables,
materialized views, JDBC sinks, `run_process_and_split` and `run_union_sources_to_table` emit
nothing.

- **Certified publication** — `START` is emitted once staging begins, before any check runs.
    - A critical check failure emits `FAIL` carrying the check results (`dataQualityAssertions`)
      and `certification: UNCERTIFIED`. The run is normally quarantined, with a snapshot kept in
      `_skifer_quarantine`; if the quarantine itself cannot be completed (`CHECK_ERROR`), no
      snapshot is guaranteed, but the event is the same `FAIL`.
    - A successful promotion: `COMPLETE` carries the check results and `certification: CERTIFIED`.
    - An exception raised during the publication (a backend or write failure, not a check
      failure): `FAIL` is emitted **without** assertions or a certification value, and the
      original exception still propagates unchanged.
    - `resume()` (crash recovery) emits `COMPLETE`. It is built from the persisted contract, not
      the original pipeline schema, so it carries no `columnLineage`, no `inputs`, and no
      `dataQualityAssertions` — only the output `schema` and the `skifer` facet.

    All of these share the **same `run_id`** as the certification record itself — the identity a
    publication is audited under, whether or not tracing is enabled. A `resume()` after a `FAIL`
    therefore produces two terminal events for one `run_id` (`FAIL` then `COMPLETE`); a consumer
    should keep the latest.

- **Non-certified batch write** (`run_process_to_table` without `data_product:`) — a single
  `COMPLETE` with schema and column lineage, no assertions and no certification. It is emitted
  **only after the post-write monitor has returned** (or did not run at all): if that monitor
  raises `DataQualityError`, no event is sent for that write.

Every emission is best-effort. With a real emitter, the `DatasetRecord` is rebuilt for each
event sent: twice for a certified publication (`START`, then its terminal `FAIL`/`COMPLETE`),
once for a `resume()` and once for a non-certified batch write. With `emitter: none` no record
is built at all. If building the record, building the event or sending it fails, the failure
becomes a single `RuntimeWarning` naming only the exception's class, and is safe even under a
warnings-as-errors filter.

### Dataset namespace resolution

In this priority order:

1. `observability.lineage.dataset_namespace`, if set.
2. Off local mode, `unitycatalog://<hostname>` derived from the `DATABRICKS_HOST` environment
   variable. `DATABRICKS_HOST` may be a bare host or a full workspace URL — port, path, query and
   fragment are ignored, and the host is lower-cased.
3. Otherwise (local mode, or `DATABRICKS_HOST` unset), `skifer://local`.

Only a plain DNS host name reaches step 2's `unitycatalog://` form: the host must fully match
dot-separated `[a-z0-9-]` labels. A `DATABRICKS_HOST` containing `@` (embedded credentials) or
whose host does not match is treated as unparseable — it falls back to `skifer://local` with one
best-effort warning that never echoes the configured value, and never fails the engine. An empty
`DATABRICKS_HOST` (after trimming whitespace), or a value whose host part is itself empty (for
example `https://:443`), falls back to `skifer://local` **without** a warning — there is no host
to report. The warning fires only when a host is actually present and gets rejected: it contains
`@`, fails the allowlist, or cannot be parsed.

!!! warning
    Do not embed credentials in `DATABRICKS_HOST` (`user:secret@host`). Such a value is
    **rejected**, not stripped and cleaned — set `observability.lineage.dataset_namespace`
    explicitly if the host cannot be used as-is.

### What an event contains

A `RunEvent` names its `job` (`job_namespace`, name = the target's physical FQN), the same
`run_id` as the business run it describes, and dataset entries built from the pipeline's own
lineage graph:

- **`inputs`** — every distinct source table reached by a real column edge into the output. A
  table that contributes no output column (for example a `left_semi`/`left_anti` join, or a join
  used only to filter rows) is not listed.
- **`outputs`** — exactly one entry, the target FQN, carrying:
    - **`schema`** facet — every output column's name; `type` is present only when the
      column declares a `logical_type`.
    - **`columnLineage`** facet (on output columns only) — one `inputFields` entry per real
      source column, each carrying a `transformations` entry mapped from the pipeline's own edge
      type:

        | Skifer edge | OpenLineage `type`/`subtype` |
        |---|---|
        | `select`/`add_columns`, no operation | `DIRECT` / `IDENTITY` |
        | `select`/`add_columns`, with operations (`cast:`, `round:`, …) | `DIRECT` / `TRANSFORMATION` |
        | `rule` (business rule) | `DIRECT` / `TRANSFORMATION` |
        | `metric` (declarative `aggregate:`) | `DIRECT` / `AGGREGATION` |
        | `join` | `INDIRECT` / `JOIN` |

      This table documents the full mapping the event builder supports. **In v1 the lineage
      tracker never produces the last two rows through a real pipeline**: a `join` edge always
      targets a source table, and the event builder keeps only edges whose target is the output,
      so no `INDIRECT`/`JOIN` entry is emitted; a declarative `aggregate:` block produces no
      lineage edge at all. Concretely, a joined pipeline lists in `inputs` only the tables that
      contribute an output column, with `IDENTITY`/`TRANSFORMATION` column lineage; a pipeline
      using `aggregate:` has no `columnLineage` and no `inputs`. Its other facets are unaffected:
      `schema` and `skifer` are always present, and a certified `aggregate:` pipeline still
      carries `dataQualityAssertions` on its terminal event.

      `description`, when present, is a sorted, comma-joined list of **operation names only**
      (`cast`, `round`, `conditional`, …) — never a `cast:double` argument or an `expr:` SQL
      expression.
    - **`dataQualityAssertions`** facet (certified publication only) — one entry per check that
      was not `SKIPPED`: `assertion` (the check's class name), `success`, `severity` (`critical`
      → `error`, anything else → `warn`) and `column` whenever the check targets a column
      (a table-scoped check like `SchemaDrift` carries no `column`). **Never**
      `expected`, `actual`, or a check message — those routinely quote the offending data.
    - **`skifer`** custom facet — `definitionHash`, and when known, `contractVersion`,
      `dataProductId`, `certification` (`CERTIFIED` / `UNCERTIFIED`) and a `classifications` map
      of column name to declared classification (`public`, `internal`, `confidential`,
      `restricted`, `pii`).

### Limits of column lineage

Only a source column whose name is a plain ASCII identifier (`[A-Za-z_][A-Za-z0-9_]*`) is
emitted in `columnLineage` or `inputs`. A constant (`literal:`/`lit:` shorthand), a raw `expr:`
result, a column a business rule created, a nested/dotted path, or a real column whose name
carries an accent, a space, or a leading digit is left out — it still appears in the `schema`
facet, since that only names the output. This is a deliberate fail-safe allowlist, not a bug: no
value can leak through it, but lineage for such a column is incomplete rather than approximate.
Widening it would require a canonical name supplied by the tracker itself, not attempted in v1.

- In v1, a `join` edge never reaches the output and a declarative `aggregate:` block produces no
  edge at all, so neither ever appears in `columnLineage` — see the note under
  [What an event contains](#what-an-event-contains).

### Compatibility with catalog consumers

An HTTP consumer that implements the OpenLineage HTTP API (for example
[Marquez](https://marquezproject.ai)) is the intended target. This emitter has been tested only
against a local generic HTTP endpoint, not against any specific catalog product. It speaks only the
OpenLineage HTTP API; it does not adapt to a catalog's preferred transport. OpenMetadata has its
own OpenLineage ingestion path (in some versions, a Kafka-based connector) — check which
transports your OpenMetadata version supports before pointing this emitter at it.

`examples/23_openlineage/` builds a `START` and a `COMPLETE` event from a small pipeline with a
`pii` column, prints the column-lineage transformation for a cast column, the redacted
assertions, and proves a secret carried by a check result never reaches the emitted JSON — then
shows an unreachable HTTP emitter reporting only an exception class name.

---

## Comparison with existing tools

| Tool | Approach | Limitation |
|---|---|---|
| Great Expectations | Declarative config, separate from pipeline | Must re-declare expectations separately |
| Soda | SQL checks + YAML config | Config separate from the pipeline |
| Monte Carlo | SaaS, ML-based anomaly detection | Expensive, black-box, no data contract |
| **Skifer** | Pipeline YAMLs **are** the contracts | Zero extra config for basic checks |

---

## Runnable walkthrough

→ `examples/10_monitor_and_quarantine/` — a batch that violates its contract, the consumer table
left untouched, and the rejected rows tagged in quarantine. It is executed by the test suite, so it
says what the code does today.
