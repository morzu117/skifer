# YAML Model Reference

Semantic models are YAML files stored in `semantic_models/<model_key>.yaml` (flat structure).
They define the business vocabulary — dimensions, metrics, filters — for one Gold table.

---

## Pipeline governance metadata — Plan 31

Pipeline schemas may attach ownership and lifecycle governance to the existing
`data_product` and `contract` blocks:

```yaml
data_product:
  id: sales.orders
  version: 2.0.0
  owner:
    team: sales-analytics
    steward: alice@example.com
    domain: commerce
    contact: data-sales@example.com
  description: Certified order facts

contract:
  status: active                 # draft | active | deprecated; default: active
  reviewers: [alice@example.com, bob@example.com]
  effective_from: 2026-01-01    # inclusive ISO date
  effective_until: 2026-12-31   # inclusive ISO date; must not precede effective_from
  grain: [order_id]
  sla:
    refresh_frequency: daily
    max_latency: 2h
  security:
    level: restricted
    access_policy: row_filter:region
  output:
    order_id:
      logical_type: identifier
      required: true
      unique: true
      classification: internal
      description: Stable order identifier
    customer_email:
      logical_type: string
      classification: pii
      description: Customer contact email
```

An owner may still be a legacy string. The structured mapping accepts only
`team`, `steward`, `domain`, and `contact`; `data_product.domain` remains the
product-level domain and takes precedence over `owner.domain` for the effective
domain.

Field classification uses one ordered taxonomy, from least to most sensitive:
`public`, `internal`, `confidential`, `restricted`, `pii`. Static column lineage
propagates the strongest upstream classification. A missing non-public target
declaration warns in the default mode and raises in strict mode; an explicit
lower classification remains explicit but is logged. Regardless of evidence
disclosure settings, filter values for columns supplied in
`EvidencePolicy.sensitive_columns` are always rendered as `<redacted>`; callers
use that set for `restricted` and `pii` fields.

Contract identity uses canonicalization version 2. `sla` and `security` are part
of the SHA-256 definition hash. Lifecycle workflow metadata—`status`,
`reviewers`, `effective_from`, and `effective_until`—is intentionally outside
the hash, as are ownership and descriptions. `diff_contracts(old, new)` marks
field removals, retypes, required hardening, classification downgrades, and SLA
relaxation or an uncomparable SLA change as breaking.

ODCS 3.1 documents can be imported to these two YAML blocks:

```bash
skifer contract import contract.odcs.yaml
```

The importer validates the supported ODCS shape, maps supported ownership,
fields, quality, SLA, and security metadata, and emits comments for information
that has no Skifer mapping instead of dropping it silently.

See [Services and local API](services_and_api.md) for metadata indexing,
incident management, and the governance coverage audit.

---

## File location

```
semantic_models/
├── semantic_catalog.yaml          ← lightweight index (auto-maintained)
├── kpi_orders.yaml                ← model key: kpi_orders
├── kpi_orders_erp.yaml            ← model key: kpi_orders_erp
├── kpi_inventory.yaml             ← model key: kpi_inventory
└── kpi_inventory_warehouse.yaml   ← model key: kpi_inventory_warehouse
```

The **model key** is a unique identifier (e.g. `"kpi_orders"`, `"kpi_orders_erp"`).
No longer uses `<model_name>/<split_value>` structure.

---

## Full YAML structure

```yaml
key: kpi_orders                            # unique model key
description: "Sales KPIs by channel, country and product category"
layer: gold                                # gold | silver | bronze
table: gold.fact_orders                    # fully-qualified table name
base_filter: "status = 'completed'"        # applied to every query
tags: [orders, revenue, ecommerce]
grain: [order]                             # optional semantic grain (entity names)
calendar: fiscal_fr                        # optional — enables period names (see below)

entities:
  - name: order
    type: primary
    key: order_id
  - name: customer
    type: foreign
    key: [customer_id, customer_source]

relationships:
  - name: orders_customer
    from_entity: customer
    to_model: customers
    to_entity: customer
    cardinality: many_to_one
    join_type: left
    verified_by_contract: false

dimensions:
  - name: channel
    sql: channel
    type: string
    description: "Sales channel (web, mobile, store)"

  - name: country
    sql: country
    type: string
    description: "Customer country (ISO code)"

  - name: order_date
    sql: order_date
    type: date
    description: "Order date"

metrics:
  - name: chiffre_affaires
    sql: amount_ttc
    type: sum
    description: "Revenue incl. VAT (EUR)"

  - name: nb_commandes
    sql: order_id
    type: count_distinct
    description: "Number of unique orders"

  - name: panier_moyen
    sql: amount_ttc
    type: avg
    description: "Average basket size (EUR)"

  - name: ca_web
    sql: amount_ttc
    type: sum
    description: "Revenue from web channel only"
    filters:
      - sql: "channel = 'web'"
```

---

## Field reference

### Model-level fields

| Field | Required | Type | Description |
|---|---|---|---|
| `key` | yes | string | Unique model key (e.g. `"kpi_orders"`, `"kpi_orders_erp"`) |
| `description` | yes | string | Human-readable description |
| `layer` | yes | string | Source layer: `gold`, `silver`, `bronze` |
| `table` | yes | string | Fully-qualified table name (e.g. `gold.fact_orders`) |
| `base_filter` | no | string | SQL WHERE clause applied to every query |
| `tags` | no | list | Tags used for catalog search and model selection |
| `grain` | no | list | Ordered list of semantic entity names defining the model grain |
| `entities` | no | list | Local entity definitions and ordered key columns |
| `relationships` | no | list | Declared semantic joins to other models |
| `calendar` | no | string | Key of a declared calendar in `calendars/<key>.yaml`, enabling named periods |
| `dimensions` | yes | list | List of dimension definitions |
| `metrics` | yes | list | List of metric definitions |

---

### Dimension fields

| Field | Required | Type | Description |
|---|---|---|---|
| `name` | yes | string | Dimension name used in `SemanticQuery.group_by` and `filters` |
| `sql` | yes | string | Column name or SQL expression (e.g. `year(order_date)`) |
| `type` | yes | string | Data type: `string`, `date`, `timestamp`, `integer`, `float` |
| `description` | no | string | Business description |

**Dimension `sql` examples:**

```yaml
- name: channel
  sql: channel           # direct column reference

- name: order_year
  sql: year(order_date)  # SQL expression

- name: country_upper
  sql: upper(country)    # function call

- name: revenue_bucket
  sql: "CASE WHEN amount_ttc > 1000 THEN 'premium' ELSE 'standard' END"
```

---

### Metric fields

| Field | Required | Type | Description |
|---|---|---|---|
| `name` | yes | string | Metric name used in `SemanticQuery.metrics` |
| `sql` | yes | string | Column or expression to aggregate |
| `type` | yes | string | Aggregation: `sum`, `count_distinct`, `count`, `avg`, `min`, `max` |
| `description` | no | string | Business description |
| `filters` | no | list | Inline filters (applied as CASE WHEN inside the aggregation) |
| `additivity` | no | string | `additive` (default), `semi_additive`, or `non_additive` |
| `non_additive_dimensions` | required for `semi_additive` | list | Dimensions the metric must not be summed across |

**Metric `type` to SQL mapping:**

| Type | Generated SQL |
|---|---|
| `sum` | `SUM(sql)` |
| `count_distinct` | `COUNT(DISTINCT sql)` |
| `count` | `COUNT(sql)` |
| `avg` | `AVG(sql)` |
| `min` | `MIN(sql)` |
| `max` | `MAX(sql)` |

**Inline metric filters:**

```yaml
- name: ca_completed
  sql: amount_ttc
  type: sum
  description: "Revenue for completed orders only"
  filters:
    - sql: "status = 'completed'"

- name: ca_france
  sql: amount_ttc
  type: sum
  description: "Revenue in France only"
  filters:
    - sql: "country = 'France'"
    - sql: "status = 'completed'"   # multiple filters are ANDed
```

Inline filters are applied as `CASE WHEN` conditions:

```sql
SUM(CASE WHEN status = 'completed' THEN amount_ttc END) AS ca_completed
```

---

### Semantic domain fields

`grain`, `entities`, and `relationships` are optional. A legacy single-table
model without them remains valid.

#### `entities`

| Field | Required | Type | Description |
|---|---|---|---|
| `name` | yes | string | Stable semantic entity identifier |
| `type` | yes | string | `primary`, `foreign`, or `unique` |
| `key` | yes | string or list | One column name or an ordered composite key |

Composite keys preserve order exactly as declared. Every entity name and every
key column must match `^[A-Za-z_][A-Za-z0-9_]*$`.

#### `relationships`

| Field | Required | Type | Description |
|---|---|---|---|
| `name` | yes | string | Stable semantic relationship identifier |
| `from_entity` | yes | string | Local entity name |
| `to_model` | yes | string | Target semantic model key |
| `to_entity` | yes | string | Target entity name in `to_model` |
| `cardinality` | yes | string | `one_to_one`, `many_to_one`, `one_to_many`, `many_to_many`, or `unknown` |
| `join_type` | yes | string | `inner` or `left` only in v1 |
| `verified_by_contract` | no | boolean | Whether the join safety was certified from a contract/check |

`unknown` cardinality is not queryable: validation rejects it with an explicit
actionable error so ambiguous or unsafe joins fail closed.

#### How relationships are planned

`SemanticPlanner` resolves a query of names into a join path before any SQL
exists. It refuses, with a message naming what to fix:

- no declared path between the metric and the dimension;
- more than one minimal path — the ambiguity is reported with both paths, never
  resolved by picking one;
- `many_to_many` or `unknown` cardinality in a metric query, each with its own
  cause so a declared `many_to_many` is not reported as undeclared.

#### Grain safety and fanout

A relationship duplicates rows of whichever side it is crossed **from** when that
side is the "one" side — which depends on the direction of travel, not on the
declared cardinality alone. Starting from the model that owns the metric, the
planner walks the join graph outward and refuses the first edge that fans:

| Situation | Verdict |
|---|---|
| Metric upstream of a `one_to_many` | refused — `Unsafe fanout: metric grain 'order' crosses one_to_many relationship 'order_lines'.` |
| Metric on the direct many-side of a `one_to_many` | allowed — computed at its own grain |
| Metric reached *beyond* a fanout, e.g. `orders -1:N-> lines -N:1-> products` | refused — the product measure repeats once per line |
| Metric on the one-side of a `many_to_one` | refused — its row repeats once per row of the many-side |
| Query with no metric at all | allowed — nothing to duplicate |

#### Metric additivity

- `additive` (default) — no restriction; a legacy metric without the key behaves
  exactly as before.
- `semi_additive` — must declare `non_additive_dimensions`; each listed dimension
  has to be pinned by the query, either in `group_by` or by an `eq` filter.
- `non_additive` — refused as soon as the plan contains a join, since a ratio or
  a distinct count does not survive a change of row multiplicity.

---

## Calendars — `calendars/<key>.yaml`

A fiscal period is resolved from a declared, versioned definition. It is never
computed in Python and never produced by the LLM, which only ever emits a period
*name*.

```yaml
key: fiscal_fr
version: "2024.1"
periods:
  - name: FY2024_Q3
    start: "2024-10-01"
    end: "2024-12-31"
```

A model opts in with `calendar: fiscal_fr`; `SemanticQuery.period` then resolves
to the declared bounds, and the calendar key and version are recorded on the
plan. Calendar files are loaded lazily and cached — none is opened at startup.

Fail-closed rules: `period` combined with `date_from`/`date_to` is refused as
ambiguous rather than silently preferring one; an unknown period name, a model
with no `calendar:`, and a `period` passed to a resolver built without a planner
are all explicit errors. Every date bound, declared or supplied, must parse as
`YYYY-MM-DD` before it can reach the SQL.

---

## semantic_catalog.yaml

The catalog is a lightweight index automatically maintained by `SemanticBuilder`.
The LLM only ever reads this file — never the full YAML.

```yaml
models:
  - key: kpi_orders.ecommerce
    file: kpi_orders/ecommerce.yaml
    layer: gold
    table: gold.fact_orders
    description: "Sales KPIs by channel, country and product category"
    tags: [gold, kpi_orders, ecommerce, orders, revenue]
    dimensions: [channel, country, category, brand, order_date]
    metrics: [chiffre_affaires, nb_commandes, panier_moyen, ca_web]
    entities: [order, customer]
    related_models: [customers]
    base_filter: "status = 'completed'"
    generated_at: "2024-01-15"
```

### Catalog field reference

| Field | Description |
|---|---|
| `key` | Unique model key (e.g. `"kpi_orders"`, `"kpi_orders_erp"`) |
| `file` | Relative path to the YAML file |
| `layer` | Source layer |
| `table` | Fully-qualified table name |
| `description` | Short description for model selection |
| `tags` | Tags for semantic search |
| `dimensions` | List of dimension names (for display only) |
| `metrics` | List of metric names (for display only) |
| `entities` | List of local entity names only (no keys or SQL) |
| `related_models` | Target model keys declared by relationships only |
| `base_filter` | Base filter (for display only) |
| `generated_at` | ISO date of last generation |

---

## Special character handling

YAML values that contain `:`, `#`, `[`, `]`, or quotes **must be double-quoted**:

```yaml
# ✅ Correct
description: "CA : chiffre d'affaires total"
base_filter: "status = 'completed'"
sql: "CASE WHEN amount_ttc > 1000 THEN 'premium' ELSE 'standard' END"

# ❌ Will cause YAML parse error
description: CA : chiffre d'affaires total
base_filter: status = 'completed'
```

`SemanticBuilder` enforces this rule in its LLM system prompt and re-tries up to 3 times on validation failure.

---

## Naming conventions

| Entity | Convention | Example |
|---|---|---|
| Model key | `snake_case` | `kpi_orders`, `kpi_orders_erp` |
| Dimension name | `snake_case` | `order_date`, `customer_segment` |
| Metric name | `snake_case` | `chiffre_affaires`, `nb_commandes` |
| Table | `layer.table_name` | `gold.fact_orders` |

Dimension and metric names are **enforced**, not merely conventional: they must
match `^[A-Za-z_][A-Za-z0-9_]*$` and `SemanticValidator` rejects anything else.
`QueryResolver` interpolates them directly as SQL aliases (`<expr> AS <name>`),
so a name containing spaces, punctuation or SQL syntax would be an injection
vector. Accents and dashes are not allowed in names — put them in `description`
or `synonyms` instead.

The same identifier rule also applies to semantic entity names, relationship
names, model keys referenced by relationships, and declared key columns. Those
values also reach generated SQL (aliases and join conditions), so they are part
of the same injection boundary.

---

## Authoring a model manually

You can write YAML models by hand without using `SemanticBuilder`. After writing, register the model in the catalog:

```python
from skifer.semantic.semantic import SemanticEngine

catalog_entry = {
    "key": "kpi_orders_manual",
    "file": "kpi_orders_manual.yaml",
    "layer": "gold",
    "table": "gold.fact_orders",
    "description": "Manually authored model",
    "tags": ["orders", "manual"],
    "dimensions": ["channel", "country"],
    "metrics": ["chiffre_affaires"],
    "base_filter": None,
    "generated_at": "2024-01-15",
}
SemanticEngine.update_catalog("semantic_models", catalog_entry)
```
