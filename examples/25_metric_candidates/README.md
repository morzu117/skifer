# 25 — Metric candidates: from an uncovered question to a promotable metric

**What it shows:** the Plan 44 cycle, offline and end to end. A question the certified semantic
layer covers is answered by it; a question it does not cover is answered by the fallback agent
with a **candidate** metric, flagged « non certifiée ». Asked again with other filter values and
by other people, the same need stays one candidate; `candidates validate` judges it on coherence,
frequency and stability and publishes it at level 1; `candidates promote` writes the level 2
drafts a human turns into a pull request. Drift, the reference set and the evaluation close the
loop.

## Run it

```bash
python examples/25_metric_candidates/run.py
```

- Every LLM answer is scripted in `run.py` (`ScriptedLLM`): no API key, no network, no workspace.
- `config.yaml` turns the fallback `on` and lowers the validation thresholds so that four
  questions are enough.
- Requesters are counted through an HMAC pseudonym; the key in `run.py` exists for the example
  only — a real one comes from `SKIFER_TRACING_HMAC_SECRET`.
- The script copies its inputs to a temporary directory and works there: the SQLite stores, the
  Delta tables, the drafts and the evaluation report never land in this folder.
- Each step calls the function its `skifer` command dispatches to, so exit codes and outputs are
  the CLI's.

## What you should see

```text
[01] gold.fact_orders and gold.dim_customers published and certified
[02] knowledge sync: [knowledge] added=30 updated=0 unchanged=0 removed=0
[03] covered question -> query (certified semantic layer)
[03] uncovered question -> candidate
> Réponse non certifiée — calculée hors du semantic layer, à valider.
[04] 4 requests from 3 requesters, one candidate: candidate:v1:...
[05] candidates validate -> exit 0, state promoted
[06] candidates promote -> exit 0, drafts for a pull request:
schemas/.drafts/candidates/average_basket.yaml
semantic_models/.drafts/candidates.average_basket.yaml
[07] candidates drift -> exit 0, observation only: ...
[07] reference bootstrap -> exit 0: {"questions": 8, "reference_set_version": "v1", "written": 8}
[07] candidates evaluate -> exit 0, A.recall@5 = 1.0, A.routing_accuracy = 1.0
[08] candidates history
created  None -> candidate  agent:candidate_agent_v1
validated  candidate -> validated  agent:validator_v1
promoted  validated -> promoted  agent:validator_v1
certification_requested  promoted -> promoted
```

## The CLI directly

```bash
skifer index schemas/*.yaml
skifer knowledge sync --models-dir semantic_models
skifer candidates validate --json
skifer candidates promote <ID> --reason "checked with finance"
skifer candidates drift --json
skifer candidates reference bootstrap
skifer candidates evaluate --json
skifer candidates history <ID>
```

On Databricks, the decisions go through `notebooks/arbitrage.py` (Plan 44, S06) instead of the CLI.
