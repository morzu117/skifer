# Genie on metric views

Genie does not read `semantic_models/`: it only knows Unity Catalog objects. A *metric view* is a
Unity Catalog view declared in YAML, with named dimensions and measures, that Genie can query
directly. Publishing each Skifer semantic model as a metric view lets Genie answer with Skifer's
own definitions — the same `sql` and the same aggregate for a given metric name — instead of
re-deriving a figure from a raw table on its own.

```text
semantic_models/*.yaml ──▶ SemanticEngine.publish_metric_views() ──▶ CREATE VIEW … WITH METRICS
                                        │ certification gate (per model)        │
                                                                   <catalog>.semantic.<model>
                                                                                 │
                                                                     Genie space : only source
```

## 1. Publish

```python
from skifer import SkiferEngine
from skifer.semantic.semantic import SemanticEngine

engine = SkiferEngine(force_env="DEV")
semantic = SemanticEngine(engine, models_dir="semantic_models", certification_store=store)

report = semantic.publish_metric_views()
print(report.to_dict())
```

`model_keys=["sales.orders"]` publishes a subset instead of the whole catalog. `dry_run=True`
executes nothing and returns the DDL it would have run — use it to review before a first publish.

Each model becomes one view, named after the last segment of its key (`sales.orders` → `orders`),
in the schema `semantic_metric_views_schema` from `config.yaml` (default `semantic`; the sandbox
suffix applies in interactive mode, as it does for `semantic_views_schema`):

```yaml
environments:
  prod:
    semantic_metric_views_schema: semantic   # optional — defaults to "semantic"
```

To inspect the DDL without a Spark session — before it runs on a cluster, or in CI — use the CLI:

```bash
skifer semantic metric-views --catalog <catalog> --schema semantic
skifer semantic metric-views --catalog <catalog> --model sales.orders   # one model only
```

It returns `0` if every model translates, `1` if one is refused; a refused model does not stop the
others from printing. This command does **not** apply the certification gate and does not execute
anything — it is the translation step alone, for review.

## 2. Certification

`publish_metric_views()` applies the same per-model certification gate as `create_view()` (the
environment's `semantic_certification_policy`). A model whose source is not certified is **not**
published, and if a view from an earlier publish still exists under that name, it is dropped:

```text
mode=enforce, source MISSING  →  view not created / existing view dropped
mode=warn,    source MISSING  →  view created, certification_warning_count incremented
mode=off                      →  no certification dependency resolved at all
```

Leaving a stale view in place would let Genie keep answering from a definition whose source has
since lost its certification — dropping it is what makes the certification gate mean something
once the data reaches Genie.

## 3. Point the Genie space here

Add only the views in `<catalog>.<schema>` (the one `publish_metric_views` wrote to) as Genie's
data sources — no raw table. Genie then only has Skifer's own dimensions and measures to answer
with.

## Limits (v1, Plan 42)

- **Mono-table models only.** A model's `relationships` are not translated: joining in a metric
  view would need to re-implement the fanout protection `SemanticPlanner` already does for
  Skifer's own queries. Such a model is refused by the translation, with that reason.
- **`semi_additive` metrics are refused.** Their meaning depends on dimensions pinned at query
  time (`non_additive_dimensions`), which a metric view's own `GROUP BY` does not respect.
- **`calendar` is not translated.** A named period (`"Q1 FY26"`) still only resolves through
  Skifer; Genie sees the raw dimension.
- **What Skifer still adds on top of a metric view:** a question about a metric name that does not
  exist is refused with suggestions rather than answered from an invented column, and every answer
  carries evidence — certification, source freshness, the exact SQL hash. A metric view gives Genie
  the right numbers; it does not give it Skifer's refusal or its proof.

See [Databricks supervisor](databricks_supervisor.md) for how this fits next to the Skifer serving
endpoint, and [YAML Model Reference](yaml_spec.md#metric-fields) for the metric `sql`/`type` shape
that `publish_metric_views` translates.
