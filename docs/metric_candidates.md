# Metric candidates

A certified semantic layer answers what it covers and refuses the rest. That refusal is
honest, but it leaves the question unanswered and teaches the platform nothing. The
metric candidates module (Plan 44) answers **outside** the semantic layer in a governed way.
The answer is flagged « non certifiée ». The metric it computed is kept as a **candidate**,
and the candidate climbs toward certification only on evidence and a human decision.

Nothing here weakens the certified path:

- A question the catalogue covers never reaches the fallback.
- A candidate never becomes certified on its own.
- `candidate_fallback: off` (the default) is byte-for-byte the GenBI behaviour before Plan 44.

Run the whole cycle offline with [`examples/25_metric_candidates/`](https://github.com/morzu117/skifer/tree/main/examples/25_metric_candidates):

```bash
python examples/25_metric_candidates/run.py
```

## The cycle

```text
question ─▶ AgenticHub ─▶ GenBIAgent ──── covered ────▶ certified answer
                              │ no_model / needs_clarification without suggestion
                              ▼
                     MetricCandidateAgent ── knowledge index (BM25 / hybrid)
                              │ schema_dict in the pipeline grammar, validated, compiled
                              ▼
            answer « non certifiée » + candidate (store) + run (result hash)
                              │ asked again: same fingerprint → same candidate
                              ▼
   skifer candidates validate ── coherence · frequency · stability
         │ pass                         │ a doubt
         ▼                              ▼
   level 1: view + metric view     needs_review ─▶ human: approve | reject
   in the candidates schema
         │ skifer candidates promote (human)
         ▼
   level 2: pipeline + semantic drafts under .drafts/ ─▶ pull request ─▶ semantic sync --promote
                                                                         └▶ event `certified`
```

### How the fallback agent answers

- **The model never writes executed SQL.** It proposes a `schema_dict` in the declarative
  pipeline grammar. With the `[analyzer]` extra it may instead propose one `SELECT`, which the
  Plan 43 mapper translates; the SQL text itself never runs.
- **The proposal is validated** before anything runs:
  - closed keys, and only indexed tables;
  - no raw SQL where the environment forbids it;
  - joins declared by the domain graph;
  - fanout safety, using the same `find_unsafe_fanout()` as `SemanticPlanner`.
- **Correctable errors go back to the model**, three attempts at most.
- **Refusals are named**:
  - `no_table_found`
  - `unknown_table`
  - `join_without_key`
  - `unsafe_fanout`
  - `raw_sql_rejected`
  - `blocking_residue`
  - `sql_path_unavailable`
  - `invalid_after_retries`
  - `execution_failed`
- **The certification gate of the sources applies**, exactly as it does for a semantic query.
- **Execution is bounded** to 1,000 rows and 60 s.
- **Filter values are parameterized** before the fingerprint is taken. « panier moyen pour EMEA »
  and « … pour APAC » are the same need, so they share one candidate.

### Routing: `off | shadow | on`

| `candidate_fallback` | What the user gets | What is stored |
|---|---|---|
| `off` (default) | the GenBI answer, unchanged | nothing — no agent, index or store is even built |
| `shadow` | the GenBI answer, plus « Une réponse non certifiée a été préparée (candidate …) » | the candidate and its run |
| `on` | the candidate answer, starting with `> Réponse non certifiée — calculée hors du semantic layer, à valider.` | the candidate and its run |

The fallback only triggers on `no_model`, or on `needs_clarification` without a suggestion.

- It **never** triggers on a policy refusal: a `DENY` stays a `DENY`.
- A promoted candidate is shown to the agent first. A question it already covers reuses it, and
  its `request_count` grows, instead of creating a duplicate.

## Two promotion levels

| Level | Who decides | What exists | Certified? |
|---|---|---|---|
| 1 — `promoted` | `candidates validate` when all criteria pass, or a human `approve` | a view and a Unity Catalog metric view in `<schema>_candidates`, tagged `certification = CANDIDATE` | no |
| 2 — certified | a human, through a pull request | the pipeline YAML and semantic model written by `candidates promote` under `.drafts/`, merged, then `semantic sync --promote` | yes |

**Level 1.**

- The level 1 SQL is `compile_select()` of the candidate's `schema_dict`, the same function the
  level 2 pipeline compiles with. A test pins the two SQL texts as equal.
- DDL only ever targets the candidates schema.
- The tag key is `certification`, not `skifer.certification`: Unity Catalog refuses `.` and `=`
  in a tag key, which was verified live.

**Level 2.**

- `candidates promote` writes with `O_EXCL` and never overwrites.
- It writes only under a `.drafts/` directory.
- It turns parameterized filters into dimensions.
- Certification is still the presence of the model in `semantic_catalog.yaml`. The candidate
  registry records it through a `certified` event, written by `semantic sync --promote` from the
  model's `semantic.source_candidate_id`.

`source_candidate_id` is **provenance**. It is not part of the contract identity hash: a
pipeline promoted from a candidate and the same pipeline written by hand certify as the same
contract.

### Validation criteria

| Criterion | Passes when | Configured by |
|---|---|---|
| coherence | no residue, valid schema, every table present, no undeclared join, no unsafe fanout, sources certified | — (a residue or an undeclared join can be waived by a human `approve`; nothing else can) |
| frequency | enough requests, from enough distinct requesters, over enough distinct days | `candidates_min_requests` (5), `candidates_min_distinct_requesters` (2), `candidates_min_days` (2) |
| stability | the published definition re-run gives the same result hash | `candidates_stability_runs` (3), `candidates_min_stability` (1.0) |

**Distinct requesters are counted through an HMAC pseudonym.**

- The secret comes from `SKIFER_TRACING_HMAC_SECRET`, never from `config.yaml`.
- Without the secret, requesters are not counted at all, and frequency fails with
  `requesters_unavailable`. An unkeyed hash of an identity is reversible, so no identity beats
  a fake one.

**A candidate whose evidence is still short** stays `candidate` (`outcome: pending`). No event is
written for it.

## What is stored, and what never is

| Stored | Never stored |
|---|---|
| the metric name, its description, the `schema_dict` with **filter values parameterized** | a filter value in a fingerprint, an evidence record or a drift report |
| the question, in full by default, or only its `sha256:v1:` hash with `candidates_store_questions: redacted` | the question in an evaluation report, a span or a log line |
| each run: result hash, row count, duration, error **class name** | result rows, an error message (it often quotes the offending value) |
| the requester as an HMAC pseudonym | a requester in clear, or an unkeyed hash of one |
| the decider as `human:<pseudonym>`, `agent:<chain>` or `system:drift` (`candidates_decider_identity: plain` keeps the login, for local use and examples only) | an approval the agent granted itself: no tool, CLI path or MCP surface can |
| an append-only certification history: who, why, with which evidence, against which `semantic_layer_version` | an edited or deleted history event |

**Classification.** The question is business text a user typed, so the store is classified
`internal`, not `public`.

- Questions are kept for `candidates_retention_days` (365 by default).
- Use `redacted` wherever a question may carry a name, an account number or anything personal.
- `adaptive` never stores the question at all, because it only needs the shape of a query.
  `candidates` does store it, because a human arbitrating a metric needs to read what was asked.

## Configuration

Every key is read per environment in `config.yaml`, next to `catalog:`, and validated when the
configuration loads. A wrong value is a startup error, never a silently clamped threshold.

```yaml
environments:
  prod:
    candidate_fallback: shadow            # off (default) | shadow | on
    candidates_store_questions: redacted  # full (default) | redacted
    candidates_retention_days: 365        # positive integer
    candidates_decider_identity: hmac     # hmac (default) | plain (local/examples only)
    # validation thresholds
    candidates_min_requests: 5
    candidates_min_distinct_requesters: 2
    candidates_min_days: 2
    candidates_stability_runs: 3          # at least 1
    candidates_min_stability: 1.0         # within [0, 1]
    # drift (observation only)
    candidates_min_sample: 30             # below it, a signal is inconclusive
    candidates_drift_thresholds:          # validated, never applied in v1
      fallback_rate: 0.2
      orphan_certified_metrics: 0.1
    # knowledge index (D7)
    knowledge_embedding_model: multilingual-e5-small   # null (default) | multilingual-e5-small | bge-m3
    knowledge_model_cache: /Volumes/main/ops/models/hf # where the weights already are
    knowledge_search_mode: null           # null → hybrid with a model, bm25 without | bm25 | dense | hybrid
    knowledge_domains: [main.gold]        # CATALOG.SCHEMA list bounding the Unity Catalog sync
    semantic_metric_views_schema: gold_views  # level 1 writes to <this>_candidates
```

The stores follow the rule of the other Skifer stores:

- **SQLite locally:**
  - `.skifer_candidates.db`
  - `.skifer_knowledge.db`
- **Delta on Databricks:**
  - `<catalog>._skifer_candidates`
  - `<catalog>._skifer_knowledge`

## Arbitration: notebook on Databricks, CLI locally

On Databricks, a human decides in `examples/25_metric_candidates/notebooks/arbitrage.py`:

1. Fill the widgets: `candidate_id`, `decision`, `reason`.
2. Run the notebook.
3. Optionally set `show_drift` to also see the usage signals.

Locally, the same decisions are CLI commands:

```bash
skifer candidates list --state needs_review
skifer candidates show ID
skifer candidates approve ID --reason "checked with finance"
skifer candidates reject  ID --reason "duplicates total_revenue"
skifer candidates promote ID --reason "ready for a PR" [--pipeline-output schemas/.drafts/x.yaml]
skifer candidates review  ID --reason "…" [--signal fallback_rate]
skifer candidates demote  ID --reason "…" [--replaced-by model:sales.orders.revenue]
skifer candidates history ID
```

**Neither holds decision logic.** Every check lives in `CandidateService`:

- the state matrix;
- staleness against the current `semantic_layer_version`;
- which reasons a human may waive;
- `O_EXCL` drafts.

A test pins that the notebook only calls its public methods.

**Why there is no app in v1.** One arbiter and a handful of candidates a week do not need a queue
or a UI. The signal to build the `api/` routes and an app is several arbiters, a need for screen
traceability, or a waiting list. They would sit on the same `CandidateService`, so nothing of
the notebook is thrown away.

## Exit codes

The exit codes are the contract a job or a CI step reads.

### `skifer candidates validate`

| Code | Meaning |
|---|---|
| `0` | every candidate judged; none went to `needs_review` |
| `1` | technical error |
| `2` | at least one candidate **went** to `needs_review` in this run — the signal a job notifies on; it wins over `1`, and a `--dry-run` never returns it |

### `skifer candidates` arbitration (`list`, `show`, `history`, `approve`, `reject`, `promote`, `review`, `demote`)

| Code | Meaning |
|---|---|
| `0` | done |
| `1` | technical error, including `candidates_decider_identity: hmac` without `SKIFER_TRACING_HMAC_SECRET` (nothing is written) |
| `2` | usage error: unknown candidate or state |
| `3` | stale: the candidate was judged against another `semantic_layer_version`; re-validate first (`reject` stays allowed) |
| `4` | refused: a forbidden transition, an `approve` over a reason a human may not waive, or a draft that already exists (never overwritten) |

### `skifer candidates drift`, `reference`, `evaluate`

| Code | Meaning |
|---|---|
| `0` | done, including an empty reference set, which writes no report |
| `1` | technical error, including `--mlflow` without the `[tracing]` extra |
| `2` | usage error: a naive or inverted window, an unreadable `--expected-file`, an invalid expected block |

### `skifer knowledge sync`, `search`, `benchmark`

| Code | Meaning |
|---|---|
| `0` | done |
| `1` | technical error, including a dense or hybrid config without `[knowledge-embeddings]` or without the model in the cache |
| `2` | usage error: `--schema` not `CATALOG.SCHEMA`, an unknown `--embed` model or benchmark `--config` |

## Drift signals, and what they do not do

`skifer candidates drift [--since ISO] [--until ISO] [--json]` reads the usage events and the
candidate store over a half-open window, partitioned by environment.

| Signal | Says |
|---|---|
| `fallback_rate` | the share of questions answered by the fallback agent |
| `orphan_certified_metrics` | certified metrics nobody asked for in the window |
| `rephrasing` | the same need asked in different words, a hint that a synonym is missing |
| `reference_degradation` | a family A or B metric of the reference evaluation that dropped since the previous report |

**Observation only.**

- A signal never changes a candidate's state, never writes an event and never calls a model.
- `candidates_drift_thresholds` is validated and recorded, and every report says
  `thresholds_applied: false`.
- Below `candidates_min_sample` events, a signal is `inconclusive` with its reason. It is never a
  number computed on three events.
- A test pins at the source level that the module cannot transition or write.

## The reference set

A versioned set of questions with their expected outcome, in three families:

| Family | Expected | Comes from |
|---|---|---|
| A | the certified model and metric that should answer | `reference bootstrap` (fixed French templates over every catalogue metric, its label and synonyms, no LLM, idempotent); a `certified` event adds the original question |
| B | the `schema_dict` and result hash of a known candidate | a human, with `reference add --family B` |
| C | `refuse` or `clarify` | a human, or a rejection for incoherence |

```bash
skifer candidates reference bootstrap
skifer candidates reference add --family C --question "météo à Paris" --expected-file c.json
skifer candidates reference list [--family A] [--json]
skifer candidates evaluate [--json] [--mlflow]
```

`evaluate` asks every question through the hub and reports:

- `A.recall@5`, `A.recall@10` and `A.routing_accuracy`;
- `B.exact_match`;
- `C.correct_refusal`, `C.false_answer` and `C.false_refusal`.

A family with no question reports `null`, never `0`.

**The report:**

- is written with `O_EXCL` to `.skifer_evaluations/`;
- is recorded as an `evaluation` run;
- with `--mlflow`, is also logged to an MLflow run, with `@` written `_at_` in metric names.

Failures carry the question id and the observed mode. They never carry the question text, a
`schema_dict` or a filter value.

## The knowledge index and the D7 decision

`skifer knowledge sync [--schema CATALOG.SCHEMA] [--embed MODEL --model-cache PATH]` builds a
graph of tables, columns, metrics and candidates. It reads three sources:

- the metadata registry (`skifer index`);
- the semantic catalogue;
- optionally, Unity Catalog `information_schema`, bounded to the declared schemas.

The fallback agent searches this graph.

**Search modes:**

- **BM25** is pure Python and needs no dependency.
- **Dense and hybrid** need the optional `[knowledge-embeddings]` extra (`sentence-transformers`).
  Models run on the driver, are loaded only from a local cache (`local_files_only=True`) and are
  never downloaded implicitly.
- **Hybrid** is the reciprocal rank fusion of BM25 and dense.

**D7, decided on 29 October 2026 after `skifer knowledge benchmark`.**

- **The setup:** the examples catalogue (8 metrics) and two question sets:
  - the 22 bootstrap questions, which reuse the labels;
  - 17 hand-written French and English paraphrases, which avoid them.
- **The result:**

| Config | Paraphrases found in the top 10 | Mean rank | p50 latency |
|---|---|---|---|
| `bm25` | 5 / 17 | 1.4 | 0.03 ms |
| `hybrid:multilingual-e5-small` | 17 / 17 | 2.3 | 17 ms |
| `hybrid:bge-m3` | 17 / 17 | 1.8 | 90 ms |

- On the bootstrap questions, every config finds every metric.
- **The default that follows:**
  - `knowledge_search_mode: null` means `hybrid` when `knowledge_embedding_model` is set, and
    `bm25` otherwise.
  - `multilingual-e5-small` is the recommended model: about 470 MB, against about 2.3 GB for
    `bge-m3`.
  - A `hybrid` that is only the default falls back to `bm25` with one warning when the model
    cannot load.
  - An explicit `dense` or `hybrid` fails instead.
- **What it was not measured on:** a large Databricks catalogue. Re-run the benchmark on yours:

```bash
skifer knowledge benchmark --config bm25 --config hybrid:multilingual-e5-small \
    --config hybrid:bge-m3 --model-cache /Volumes/main/ops/models/hf
```

## Scheduling on Databricks (Lakeflow Jobs)

Three tasks cover the cycle: sync, then validate, then evaluate. Each runs the `skifer` entry
point of the installed wheel. Only `validate` needs a SQL-capable compute, because level 1 runs
DDL.

```yaml
resources:
  jobs:
    skifer_metric_candidates:
      schedule: {quartz_cron_expression: "0 0 5 * * ?", timezone_id: Europe/Paris}
      tasks:
        - task_key: knowledge_sync
          python_wheel_task:
            package_name: skifer
            entry_point: skifer
            parameters: [knowledge, sync, --schema, main.gold, --config, /Workspace/skifer/config.yaml]
        - task_key: candidates_validate
          depends_on: [{task_key: knowledge_sync}]
          python_wheel_task:
            package_name: skifer
            entry_point: skifer
            parameters: [candidates, validate, --json, --config, /Workspace/skifer/config.yaml]
        - task_key: candidates_evaluate
          depends_on: [{task_key: candidates_validate}]
          python_wheel_task:
            package_name: skifer
            entry_point: skifer
            parameters: [candidates, evaluate, --json, --config, /Workspace/skifer/config.yaml]
```

A `validate` exit code `2` fails the task on purpose, so the job's notification reaches the
arbiter.

Dense search needs one more step: copy the model weights to a Volume once and point
`knowledge_model_cache` at it. Many workspaces block Hugging Face.

## Limits

- **The fallback agent answers one table, or tables joined by declared relationships.** An
  undeclared join is computed but goes to `needs_review`.
- **No windowed metrics.** A `LAG()` or `OVER` has no declarative equivalent and is refused as a
  blocking residue.
- **No automatic action on drift** in v1. Thresholds will be calibrated on real usage first.
- **Level 2 is a pull request.** Skifer never merges or deploys it.
- **D7 was measured on the examples catalogue only.** Re-run `knowledge benchmark` on a real
  catalogue before relying on the default.
- **One arbiter, through a notebook or the CLI.** The app is deferred (see above).
