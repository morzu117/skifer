# Data Observability

**Data quality checks derived automatically from your existing YAML schemas — no additional configuration required.**

Skifer's YAML schemas are already data contracts:

- `quality_checks` → null and uniqueness constraints
- `filter` → business invariants expected on the data
- `select_final` → output schema (columns, types via `cast:`)

Most observability tools (Great Expectations, Soda, Monte Carlo) require you to re-declare these rules separately. Skifer already has them in the YAML.

## Contract identity and ODCS export

`canonicalize_contract(parse_to_ir(schema))` creates the stable identity used by
the certification registry: data-product ID/version, output fields, grain and
semantic seed are rendered as canonical JSON and hashed with SHA-256. Field
order is preserved; JSON object keys are sorted. Ownership and descriptions are
kept with the definition but deliberately excluded from the hash, so a
documentation-only edit does not invalidate a certification.

`export_odcs_31(schema, definition)` exports the supported surface to an ODCS
3.1-shaped document: fundamentals, output properties, required/unique quality
rules, owner team and a read role. It returns warnings for metadata that has no
portable ODCS mapping rather than dropping it silently.

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
