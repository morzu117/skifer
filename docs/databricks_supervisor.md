# Skifer behind a Databricks supervisor

Genie answers open questions over tables. Skifer answers the questions whose figures must be
official: it only knows the metrics declared in the semantic models, refuses what it cannot
resolve, applies the certification gate, and returns evidence with every answer. This page
deploys the Skifer agent on Model Serving and puts it next to a Genie space behind an Agent
Bricks supervisor, which routes each question to one or the other.

```text
                       ┌── Genie space ─── certified Gold tables only (exploration)
user ── supervisor ────┤
                       └── Skifer endpoint ─ semantic models, certification gate, evidence (KPIs)
```

> **Not yet validated on a workspace**: every step below is tested up to MLflow. Two points
> depend on the Databricks product and have not been checked yet: that the supervisor accepts a
> custom `ResponsesAgent` endpoint as an agent, and how long the endpoint takes to start. Check
> the first one before anything else; without it the endpoint still works on its own, from a
> notebook, an app or the Playground.

## 1. Prerequisites

- **Serverless compute** enabled on the workspace. The endpoint reads tables through
  Databricks Connect serverless: Model Serving gives a container no Spark of its own.
- **A chat endpoint** the agent calls to turn a question into a semantic query, for example a
  Foundation Model API endpoint. Skifer has no default model; the endpoint name is given at
  log time.
- **Certified publication** running for the Gold tables the agent reads (see
  [End-to-end Governance](governance.md)), so that the certification store in
  `<catalog>._skifer_certification` has something to say. The endpoint resolves that schema
  against the environment's catalog, never against the session's current catalog.
- **UC tags** turned on in the production environment, so Genie can be limited to the same
  certified tables:

```yaml
environments:
  prod:
    catalog: main
    semantic_certification_policy: enforce   # or warn while migrating
    uc_certification_tags: true              # tag each table on promotion
```

With `uc_certification_tags: true`, every promotion runs
`ALTER TABLE … SET TAGS ('certification' = 'CERTIFIED', 'contract_version' = …)` on the target.
A tagging failure (usually a missing `APPLY TAG` privilege) is logged as a warning and never fails
the publication. The tags are a snapshot taken at promotion: a certification that later expires
through `semantic_certification_max_age` keeps its tag, and only the Skifer endpoint enforces the
expiry.

- **A Skifer release that contains `SkiferResponsesAgent`.** The endpoint installs
  `skifer[serving]==<installed version>` from PyPI. A development install reports the version of
  its last release, which may predate the agent; until the release is published, pass
  `pip_requirements` pointing at a wheel the endpoint can install.

## 2. Log and register the agent

From a Databricks notebook, install Skifer with MLflow and the OpenAI client, **without** the
`serving` extra: that extra adds `databricks-connect`, which must not sit next to the cluster's
own PySpark. The extra is for the endpoint, which `log_agent` declares for you.

```python
%pip install skifer "mlflow>=3,<4" "openai>=1.0"
dbutils.library.restartPython()
```

```python
import mlflow
from skifer.serving import log_agent

mlflow.set_registry_uri("databricks-uc")

with mlflow.start_run():
    info = log_agent(
        "config.yaml",
        "semantic",
        llm_endpoint="databricks-meta-llama-3-3-70b-instruct",
        registered_model_name="main.agents.skifer",
        example_question="Quel est le CA par région ?",
    )
```

`log_agent` ships `config.yaml` and the semantic models directory as artifacts, writes the chat
endpoint into the model configuration, and declares it as a resource (plus `warehouse_id=` when
the models use a SQL warehouse). **MLflow calls the agent once while logging**: the hub is built
on the notebook's session and the example question is really asked, so a wrong path, a missing
model or an unreachable chat endpoint fails here rather than on the endpoint.

From a laptop, `scripts/deploy_to_databricks.py` does the same through Databricks Connect
serverless; install `skifer[serving]` there, without `[spark]`:

```bash
export DATABRICKS_SERVERLESS_COMPUTE_ID=auto
python scripts/deploy_to_databricks.py \
    --llm-endpoint databricks-meta-llama-3-3-70b-instruct \
    --registered-name main.agents.skifer
```

## 3. Create the serving endpoint

Serve the registered model (*Serving → Create serving endpoint*, or `databricks.agents.deploy`)
with these environment variables:

| Variable | Value |
|---|---|
| `DATABRICKS_SERVERLESS_COMPUTE_ID` | `auto` — Spark through Connect serverless |
| `DATABRICKS_HOST` | the workspace URL |
| `DATABRICKS_TOKEN` | `{{secrets/<scope>/<key>}}`, a token of a principal that can read the Gold tables and the certification schema |

The hub is built once per replica, at start, and reused by every request. Test the endpoint in
the Responses format:

```bash
curl -X POST "$DATABRICKS_HOST/serving-endpoints/<endpoint>/invocations" \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"input": [{"role": "user", "content": "Quel est le CA par région ?"}]}'
```

The answer text is in `output[0].content[0].text`, the evidence in
`custom_outputs.skifer.evidence`. A client may declare who is asking in `custom_inputs`
(`consumer_id`, `user_id`, `trace_id`); those values feed logs and traces, never an
authorization, and `scopes` is ignored.

## 4. Limit the Genie space to certified tables

Add to the Genie space only the Gold tables tagged `CERTIFIED`. This query lists them:

```sql
SELECT catalog_name, schema_name, table_name
FROM main.information_schema.table_tags
WHERE tag_name = 'certification' AND tag_value = 'CERTIFIED'
ORDER BY schema_name, table_name;
```

Genie and Skifer then read the same tables, and a figure Genie shows can be cross-checked against
the Skifer answer.

## 5. Wire the supervisor

In Agent Bricks, create a supervisor with two agents. The supervisor routes on the descriptions,
so they carry the division of labour:

| Agent | Description to give the supervisor |
|---|---|
| Skifer endpoint | Official figures: KPIs, revenue, volumes and every metric defined in the semantic layer. Answers only with certified data and returns the evidence behind each figure. Use it whenever a number will be reported or compared. |
| Genie space | Free exploration of the certified Gold tables: listing, filtering, ad-hoc breakdowns and questions no defined metric covers. |

## 6. Verify

Run these checks after each deployment:

1. A KPI question sent to the endpoint returns `custom_outputs.skifer.evidence` with a
   `sql_hash` and the certification of each source.
2. A question about a metric that no model defines is refused, with suggestions, and no SQL is
   run.
3. With `semantic_certification_policy: enforce`, a question over a table without a `CERTIFIED`
   record is refused.
4. Through the supervisor, a KPI question goes to Skifer and an exploration question goes to
   Genie.
5. The first request after the endpoint scales from zero answers within the client's timeout.

## Limits

- The caller's identity is declared by the client, not authenticated by Model Serving: the
  endpoint reads data as the principal of `DATABRICKS_TOKEN`.
- Answers are not streamed.
- Genie does not see the semantic models: the metric definitions are not exported to it.
- Skifer's [read-only MCP server](mcp.md) is not deployed by this page.
