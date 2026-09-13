# 23 — OpenLineage events

**What it shows:** the pure `build_run_event` builder turns a pipeline's own
`DatasetRecord` into a `START` and a `COMPLETE` OpenLineage `RunEvent`, an `InMemoryEmitter`
collects them without any network, and a redaction canary proves a secret string carried by a
check result never reaches the emitted JSON. A second emitter shows that an unreachable catalog
never raises.

## Run it

```bash
python examples/23_openlineage/run.py
```

No Spark session and no engine are needed — `build_run_event` is pure Python, and the emitters
here (including `HttpEmitter`) use only the Python standard library, no extra dependency.

## What you should see

```text
Event types emitted, in order:
  START
  COMPLETE
Job name: gold.customer_orders
Input dataset names:
  silver.customers
  silver.orders
Output dataset name: gold.customer_orders
Column lineage for amount_eur: source=silver.orders.amount type=DIRECT subtype=TRANSFORMATION description=cast
Data quality assertions:
  assertion=NullCheck success=False severity=error column=customer_email
  assertion=NullCheck success=True severity=warn column=order_id
Skifer facet classifications:
  amount_eur: internal
  customer_email: pii
  order_id: internal
Secret present in events: False
Unreachable emitter warning category: RuntimeWarning
Unreachable emitter reported exception: ConnectionRefusedError
```

The pipeline declares a `data_product`, a `contract.output` with one `pii` column
(`customer_email`), a join across two source tables, and a `cast:double` on `amount_eur` — no
`business_rules`. `index_schema()` builds the same `DatasetRecord` the metadata registry would
persist, entirely without Spark. `build_run_event()` is called twice on that same record, once
per `eventType`, both carrying the same `run_id` — the same structure `PublicationCoordinator`
emits around a certified publication (`docs/observability.md#openlineage`), except that real
events carry the current time as `eventTime` instead of this example's fixed `EVENT_TIME`.

`amount_eur` shows `DIRECT`/`TRANSFORMATION` because it goes through `cast:double`; a column
copied without a transformation (like `order_id`) would show `DIRECT`/`IDENTITY` instead — the
`description` field only ever carries an allowlisted operation name, never a `cast:double` or
`expr:` expression verbatim.

The `CheckResult` passed to the `COMPLETE` event carries the secret string
`sk-live-SECRET123ABC` in both `actual_value` and `message`. `dataQualityAssertions` never
carries either field — only `assertion` (the check's class name), `success`, `severity`
(`critical` → `error`, anything else → `warn`) and `column` survive. `json.dumps` over every
emitted event confirms the secret is absent.

The last two lines emit the same `COMPLETE` event through an `HttpEmitter` built with an injected
`opener` that raises `ConnectionRefusedError` instead of opening a socket — simulating an
unreachable catalog deterministically, independent of the environment's network or proxy setup.
`emit()` never raises: the failure becomes one `RuntimeWarning` naming only the exception's
class — never the url, the endpoint, or the event body.

## Remember

OpenLineage emission observes a pipeline; it never gates or alters what the pipeline does, and it
never carries a value a check or a column expression could have leaked.

## Next

[12 — Runtime tracing](../12_tracing/) shows the same non-blocking, redacted-by-allowlist shape
applied to spans instead of lineage events.
