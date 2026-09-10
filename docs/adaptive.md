# Supervised adaptive Gold

This page documents supervised adaptive Gold for data and platform engineers who turn usage patterns into human-reviewed optimization proposals; for the conversational analytics layer, return to the [Agentic Layer](agentic.md).

Real usage of the semantic layer feeds **proposals** for Gold tables and
materialized views. Nothing is ever deployed. The engine never edits `schemas/`,
never calls `engine.run_*`, never creates a materialized view, and never runs a
Git command — the only thing it produces is a reviewable directory.

```
SemanticEvidence ─▶ UsageEventStore ─▶ PatternAggregator ─▶ RecommendationEngine
                                                                    │
                                                    OptimizationProposal
                                                    (evidence + rule + version)
                                                                    │ ProposalGenerator
                              .skifer_proposals/<id>/{pipeline.yaml, draft, proposal.json}
                                                                    │ adaptive accept
                                              human-owned schema → PR → deployment
                                                                    │ adaptive evaluate
                                              improved | regressed | inconclusive
```

### Usage events carry no sensitive data

`usage_event_from_evidence()` never copies the evidence object: it serializes it
only to validate the allowlist, then rebuilds the event field by field. No
question, no SQL, no filter value. A filter is recorded as `column:operator`,
never `column:operator:value`. The fingerprint is versioned (`sha256:v1:…`) and
insensitive to non-semantic ordering, so two identical queries that differ only
by permutation share one fingerprint.

### Aggregation is strictly partitioned and deterministic

`PatternAggregator` partitions by environment, consumer class and model
definition hash — two environments never blend into one aggregate. `failed`
events are counted apart from successes. The clock is injected and mandatory and
iteration runs over sorted keys, so the same events produce the same result under
any `PYTHONHASHSEED`.

### Rules are explainable, never a model

The rule registry is **static** and versioned (`frequent_aggregate`,
`repeated_join_path`, `missing_dimension`, `unused_generated_asset`) — no
arbitrary plugins, and no ML in v1. The score is a composition of named
thresholds: every reason cites the threshold, the observed value and the verdict.
A rule that does not conclude emits a `RecommendationRefusal` carrying its
**contraindications**, never silence. Guardrails: certified sources, safe
cardinalities — fanout detection is *shared* with `SemanticPlanner` through
`find_unsafe_fanout()` rather than reimplemented — no raw SQL, and no Python rule
that cannot be compiled.

`examples/17_adaptive_gold/` aggregates ten privacy-safe events, prints the
threshold evidence for both a proposal and a `RecommendationRefusal`, and then
shows the human review boundary through stale (`3`) and existing-output (`4`)
exit codes without deploying anything.

### Artifacts are validated before a human sees them

`ProposalGenerator` writes to a staging directory, runs the pipeline through
`load_schema` **and** the semantic draft through `SemanticValidator`, and only
then publishes. A visible proposal is one that loads. `proposal_id` is derived
from the content (`proposal:v1:<sha256>`), and paths are recorded **relative** to
the proposals root: an absolute path would pin the artifact to one checkout,
write the local username into a file meant to be read, and make two identical
generations differ byte-for-byte.

### The decision is human

```bash
skifer adaptive list
skifer adaptive show PROPOSAL
skifer adaptive diff PROPOSAL
skifer adaptive accept PROPOSAL --output schemas/gold/orders_summary.yaml
skifer adaptive reject PROPOSAL --reason "Not enough benefit"
skifer adaptive evaluate PROPOSAL --store .skifer_adaptive.db
```

`accept` refuses to overwrite an existing output — the guarantee is at syscall
level (`os.O_CREAT | os.O_EXCL`), with no race between the check and the write —
and revalidates the source hashes: a definition that has moved makes the proposal
`stale`, and a stale proposal cannot be accepted. It records `delivery.json`, the
link between the proposal and the asset a human actually delivered.

### Outcome evaluation never rolls anything back

`skifer adaptive evaluate` compares the window before delivery with the
window after it, on the exact partition the proposal was built from — recovered
from the proposal's own evidence events, never guessed. Windows are half-open, so
no event is counted twice.

Below the minimum-data thresholds the result is `inconclusive` with a reason
naming the window and the threshold that failed; causality is never attributed to
data that cannot support it. If the evidence events are gone (retention), the
result is `inconclusive: evidence_unavailable` — not an exception, and not a
guess. A regression on **either** axis (duration percentile or failure rate)
outweighs an improvement on the other.

A regression produces a **human review recommendation and nothing else**. The
module has no rollback, no drop, no redeploy and no subprocess — a test asserts
this at source level, because the guarantee is that the capability is absent, not
merely unused.

Exit codes are the contract:

| Code | Meaning |
|---|---|
| `0` | success (`improved` or `inconclusive` for `evaluate`) |
| `1` | technical error |
| `2` | usage error |
| `3` | stale proposal |
| `4` | state conflict or existing output |
| `5` | measured regression — review required |
