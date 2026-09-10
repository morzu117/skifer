# 20 — Builder validation and orchestration export

**What it shows:** an offline stub LLM drafts pipeline structure, `BuilderAgent` checks the local
catalog and then validates the generated YAML, and `export_orchestration()` emits Airflow or
runner-script artifacts. No Spark session, API key, cluster, or network is involved.

## Run it

```bash
python examples/20_builder_and_orchestration/run.py
```

The first proposal uses a plausible but nonexistent filter operator. Catalog checks pass, then the
framework's real schema loader refuses the draft before it can be written. A second, valid proposal
is written and exported entirely inside a `TemporaryDirectory`; its generated script can contain the
temporary YAML path, but the whole tree is removed before the example exits, leaving the repository
clean.

## What you should see

```text
Offline stub proposed filter: region:approximately:EMEA
Framework refusal: Erreur de sauvegarde : Schema validation failed with 1 error(s):
  [table 'orders' filter] column 'region': unknown filter operator 'approximately'.
  Valid operators: ['between', 'contains', 'ends_with', 'equals', 'greater_than', 'greater_than_equal', 'in', 'is_not_null', 'is_null', 'less_than', 'less_than_equal', 'like', 'not_between', 'not_contains', 'not_equals', 'not_in', 'not_like', 'sql', 'starts_with']
Invalid pipeline written: False
Validated pipeline written in temporary directory: True

Export format requested: airflow -> produced: airflow
Generated files: airflow/dag_skifer_pipelines.py, scripts/run_pipelines.py
DAG excerpt:
  with DAG("skifer_pipelines", start_date=datetime(2026, 1, 1), schedule="@daily") as dag:
  python_callable=lambda: None,  # TODO: call run_pipeline("pipeline")
The DAG is a scaffold: the task body is a TODO, not a working call.

Export format requested: auto -> produced: script
Generated files: scripts/run_pipelines.py
Runner script excerpt:
  from skifer import SkiferEngine, load_schema
  # Pipeline: pipeline
Temporary directory removed: True
```

## What the export is, and what it is not

The Airflow DAG is a **scaffold**. Its task body is `lambda: None` with a TODO, so a DAG copied
without reading it schedules nothing and reports success. The example prints that line rather than
describing it, because a reader who only skims the file name would not find out otherwise.

Every format also emits `scripts/run_pipelines.py`, so a pipeline is never trapped in an
orchestrator the team turns out not to run. And `auto` is not a promise of Airflow: off Databricks
it resolves to that runner script, which the example shows by asking for both.

`LocalCatalogBackend` is a tiny duck-typed catalog implementation kept in this example rather than
imported from the test suite. `BuilderAgent` only needs its catalog methods, so using it here is the
smallest honest path and proves that schema drafting itself does not require live Spark.

## Remember

The LLM may draft a pipeline, but only framework-validated YAML is written or exported.

## Next

[21 — Hub routing](../21_hub_routing/) routes static agent questions without starting Spark or
calling an LLM.
