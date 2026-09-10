# 17 — Supervised adaptive Gold

**What it shows:** ten synthetic, privacy-safe semantic usage events are stored and aggregated into
one pattern. A static explainable rule either produces a content-addressed proposal or returns a
visible `RecommendationRefusal`; the human workflow then rejects unsafe delivery states.

## Run it

```bash
python examples/17_adaptive_gold/run.py
```

No Spark, LLM, API key, network, or persistent workspace is used. The example creates a temporary
SQLite event store and proposal roots, then removes the entire directory.

## What you should see

```text
Aggregated events: 10 in 7d
Rule: frequent_aggregate v1
  duration_percentile_ms: observed=1200.0 >= required=1000.0 -> passed=True
  event_count: observed=10 >= required=10 -> passed=True
Verdict: proposal (materialized_view)
Proposal id: proposal:v1:...
Content-derived id stable across recommendation repeats: True
RecommendationRefusal: frequent_aggregate v1
  reason: threshold:duration_percentile_ms:observed=1200.0:>=1000.0:passed=true
  reason: threshold:event_count:observed=10:>=11:passed=false
  contraindication: benefit_not_proven:event_count
Path independence: same id generated under two proposal roots: True
Existing-output refusal: [adaptive] The requested output already exists; refusing to overwrite it.
Existing-output exit code: 4
Human rejection recorded: status=rejected
Stale refusal: [adaptive] Proposal 'proposal:v1:...
Stale exit code: 3
Deployment performed: no; schemas/ and Git were never touched.
Temporary workspace removed: True
```

Every threshold line is the rule's own `expected_benefit` or refusal reason. A rule below its event
threshold says why it did not conclude and names `benefit_not_proven:event_count`; silence would be
indistinguishable from a rule that never ran.

The proposal ID is produced before any generator path exists. Recommending the same content again
and generating it below two different roots retain the same ID. A clock or checkout path is not its
identity.

Codes `3` and `4` are the adaptive CLI contract: stale source definitions and an existing output are
different refusal categories. The existing human file is never overwritten.

Nothing is ever deployed: the adaptive module writes no human-owned schema, calls no `engine.run_*`,
creates no materialized view, and runs no Git command. Acceptance only delivers a candidate for a
separate human-owned review and deployment process; this example intentionally demonstrates reject
and refusal paths instead.

## Remember

Usage can produce an explainable proposal, never an automatic Gold deployment.

## Next

[18 — Governed capability execution](../18_capability_execution/) crosses the external write
boundary with exact approval binding, idempotence, and audited compensation.
