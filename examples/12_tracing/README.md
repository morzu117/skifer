# 12 — Runtime tracing and redaction

**What it shows:** the dependency-free `InMemoryTracer` records nested spans and allowlisted
attributes. Attempts to attach a question, SQL text, and a filter value are rejected with only the
attribute key and Python type retained. Identity defaults and exporter failure isolation are visible.

## Run it

```bash
python examples/12_tracing/run.py
```

No Spark session and no `tracing` extra are needed.

## What you should see

```text
Recorded span tree:
- skifer.pipeline.run attributes={'run_id': 'run-12'}
  - skifer.transform.execute attributes={'status': 'complete'}
Rejected attributes (key and type only):
  key=question | type=str
  key=sql_text | type=str
  key=filter_value | type=str
Default user identity: mode=omit | recorded=None
HMAC without secret: recorded=None
Exporter failure type: RuntimeError
Business result despite exporter failure: 42
```

The rejected values themselves never appear in a span or rejection record. The allowlist admits
`run_id` and `status`; it refuses the three sensitive keys even though their values are ordinary
strings. `hmac` without `SKIFER_TRACING_HMAC_SECRET` omits identity because an unkeyed hash of a
guessable identifier would not protect it.

`configured_span_scope()` catches the exporter's real `RuntimeError` in best-effort mode. The
business calculation still returns 42, showing that tracing does not create a second business path.

## Remember

Tracing may observe business work, but redaction and exporter failures must never alter that work.

## Next

[13 — Semantic projection](../13_semantic_projection/) derives a managed semantic draft from the
pipeline and makes drift a CI-visible exit code.
