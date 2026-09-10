# Development Plan — YAML Pipeline Templates + Quality Checks DSL

**Branch:** `life-quality`  
**Status:** F0–F7 implemented. F8–F9 pending.

**Legend:** ✅ Implemented · 🔲 To implement · 📋 Backlog

---

## Naming convention — canonical rule

**The YAML is read by anyone (analysts, POs, ops). All operator names must be self-documenting English. SQL abbreviations (`eq`, `gte`) are kept as backward-compatible aliases but are no longer the canonical form.**

Business rules (Python side) are exempt — they are logic, read by developers.

---

## ✅ Feature 0 — Naming convention cleanup + new operators

**Commit:** `866cc17 [F0]`

### Filter operators (`_build_filter_expression`)

| Canonical name | Accepted aliases | Notes |
|---|---|---|
| `equals` | `eq`, `=` | |
| `not_equals` | `ne`, `!=` | |
| `greater_than` | `gt`, `>` | |
| `less_than` | `lt`, `<` | |
| `greater_than_equal` | `gte`, `>=` | |
| `less_than_equal` | `lte`, `<=` | |
| `in` | — | value: comma-separated `"A,B"` or list; `;` still accepted as separator |
| `not_in` | — | same as `in` |
| `contains` | — | |
| `not_contains` | `notcontains` | fixed inconsistency |
| `starts_with` | — | `c.like("val%")` |
| `ends_with` | — | `c.like("%val")` |
| `is_null` | — | no value |
| `is_not_null` | — | no value |
| `like` | — | raw SQL LIKE pattern |
| `not_like` | `notlike` | fixed inconsistency |
| `sql` | — | raw SQL expression via `F.expr(val)` |

**`in` / `not_in` safety contract:** the string format `"status:in:A,B"` splits on `,`.  
If a value contains a comma, use the dict form: `{column: city, operator: in, value: ["New York, NY", "Paris"]}`.  
Loader warns if any split value has leading/trailing space.

### `when:` conditions (`_apply_operation`)

| Canonical form | Accepted aliases |
|---|---|
| `when:equals:val` | `when:eq:val` |
| `when:not_equals:val` | `when:ne:val` |
| `when:not_like:pattern` | `when:notlike:pattern` |
| `when:not_contains:val` | `when:notcontains:val` |
| `when:starts_with:val` | — |
| `when:ends_with:val` | — |

### Tier 1 operations added (`_apply_operation`)

| Operation string | PySpark |
|---|---|
| `lower` | `F.lower(c)` |
| `round:2` | `F.round(c, 2)` |
| `abs` | `F.abs(c)` |
| `length` | `F.length(c)` |
| `to_date:yyyy-MM-dd` | `F.to_date(c, fmt)` |
| `nvl:0` | `F.coalesce(c, F.lit(0))` |

### Also bundled into F0
- Unknown operation now logs a warning instead of silently passing the column through
- `RuleRegistry.list_loaders()` added for API symmetry with `list_rules()`

---

## ✅ Feature 1 — YAML Pipeline Templates

**Commit:** `1e8d6d1 [F1]`

Two loading modes, both normalize compact YAML syntax to the internal dict format.

### Mode A — File (recommended)

```python
from skifer import load_schema
schema = load_schema("schemas/gold/fact_orders.yaml", params=engine.default_params)
schema = load_schema("schemas/gold/fact_orders.yaml", params={**engine.default_params, "split_value": "FR"})
```

### Mode B — Inline YAML string

```python
from skifer import parse_schema
schema = parse_schema("""
tables:
  - name: "{{ catalog }}.silver.orders"
    filter:
      - "region:equals:{{ region }}"
select_final:
  - [order_id, id]
""", params={"catalog": engine.db, "region": "FR"})
```

### Parameter injection: `{{ key }}`
- Substitution before `yaml.safe_load()` — no Jinja2 dependency
- Fail fast: `ValueError` with missing key name if placeholder remains after injection

### `engine.default_params`
```python
engine.default_params  # → {"catalog": "my_dev_catalog", "env": "DEV"}
```
`schema_suffix` intentionally absent — handled automatically by `get_target_schema()` at write time.

### Normalization performed by the loader
- Filter strings `"col:op:val"` → dict
- Compact joins `[alias, key]` → `{table_from, on_from, ...}`
- 2-element `select_final` rows `[src, tgt]` → `[src, tgt, []]`
- `literal:value` shorthand in `select_final` → `[null, target, [lit:value]]`
- `keep_all_columns` + `select_final` mutual exclusion check

---

## ✅ Feature 2 — Quality Checks DSL

**Commit:** `002e970 [F2]`

```yaml
tables:
  - name: silver.orders
    quality_checks:
      drop_nulls_in: [customer_id, global_date]
      drop_duplicates_on: [order_number, sku_id]
```

Execution order: `filter → preprocess.qualify → drop_nulls_in → drop_duplicates_on`

---

## ✅ Feature 3 — Smart Sandbox Resolution

**Commits:** `2bede5c [F3]`, `019875a [F3 fix]`

In interactive non-prod mode, source tables are transparently resolved to `schema_XXXX.table`.

### Resolution algorithm
```
suffixed_schema = schema + schema_suffix

CASE 1 — suffixed schema exists:
  1a — table exists → load transparently (no log)
  1b — table missing → log + SHALLOW CLONE from main + load

CASE 2 — suffixed schema missing:
  → create schema
  → log + SHALLOW CLONE from main + load
  → if table missing in main schema → raise ValueError

Job / prod mode: bypass entirely, load as-is
```

Clone strategy: `SHALLOW CLONE` (Databricks) / `CTAS` (local Derby).  
No auto-refresh — drop sandbox table to force re-clone.

### User ID persistence (dual storage)
Priority on read: `.skifer_user` file → re-derive (4 strategies cascade).  
On first derivation: persist to `.skifer_user` next to `config.yaml` (best-effort).

**`.skifer_user` format:**
```
# Auto-generated by Skifer. Add to .gitignore.
user_email=mon_user@company.com
suffix=mon_user
```

### config.yaml
```yaml
sandbox:
  missing_table: copy   # copy (default) | error
```

### New module: `core/sandbox.py`
`SandboxResolver` — isolated from engine for testability. Handles `parse_fqn`, `schema_exists`, `table_exists`, `create_schema`, `clone_table`, `resolve`.

---

## ✅ Feature 4 — Core Robustness

**Commit:** `61143d0 [F4]`

`dedup_after_union=True` parameter on `run_union_sources_to_table` (default: True — existing behavior).  
Set to False to keep all rows. Outcome logged explicitly in both cases.

---

## ✅ Feature 5 — Schema Expressiveness

**Commit:** `7f797f6 [F5]`

### OR filter groups
```yaml
filter_groups:
  - ["region:equals:EMEA", "status:is_not_null"]   # AND within group
  - ["region:equals:APAC"]                          # OR between groups
```

### `keep_all_columns` + `add_columns`
```yaml
keep_all_columns: true
add_columns:
  - [amount, amount_rounded, [round:2]]
  - [literal:ERP, source_system]
```
Mutually exclusive with `select_final`.

### Chained `when/else` (dict form)
```yaml
select_final:
  - source: status
    target: status_label
    ops:
      - when: "equals:DONE"
        then: "lit:Paid"
      - when: "equals:PENDING"
        then: "lit:In Progress"
      - else: "lit:Unknown"
```

### `literal:` shorthand
```yaml
- [literal:ERP, source_system]
- [literal:0.0, discount, [cast:double]]
```

---

## ✅ Feature 6 — Developer Ergonomics

**Commit:** `5baf623 [F6]`

### `force_env`
```python
engine = SkiferEngine(force_env="LOCAL")  # bypasses auto-detection
```
Raises `ValueError` if env not in `config.yaml`.

### `dev_limit`
```yaml
dev_limit: 10000        # schema-level
tables:
  - name: silver.orders
    dev_limit: 5000     # table-level override
```
Silently ignored in job/prod mode.

---

## ✅ Feature 7 — Observability: `describe_schema()`

**Commit:** `d49a054 [F7]`

```python
engine.describe_schema(schema)
```

Dry-run summary — no Spark execution. Shows sources with sandbox resolution preview, filters, quality checks, joins, business rules, output columns, and mode/env footer.

---

## 🔲 Feature 8 — Governance: `allow_raw_sql`

### Goal

Without this guard, `sql:` and `expr:` in a YAML schema can execute arbitrary SQL in production. A governance flag in `config.yaml` allows platform teams to disable these escape hatches per environment.

### Config

```yaml
environments:
  PROD:
    catalog: "my_prod_catalog"
    is_production: true
    allow_raw_sql: false    # disables sql: operator and expr: operation
```

Default: `true` (no breaking change — current behavior preserved everywhere).

### Behavior when `allow_raw_sql: false`

In `process_schema`, before applying filters and operations, check the current env config:

- **Filter operator `sql:`** → raise `ValueError`:  
  `"[Governance] sql: filter operator is disabled in env PROD (allow_raw_sql: false)."`
- **Operation `expr:sql_string`** → raise `ValueError`:  
  `"[Governance] expr: operation is disabled in env PROD (allow_raw_sql: false)."`

The check happens at schema processing time (not load time), so the error surfaces with env context.

### Files to modify

| File | Action |
|---|---|
| `src/skifer/core/core.py` | Add `allow_raw_sql` check in `_build_filter_expression` and `_apply_operation`; read flag from `self.config` |
| `tests/test_core.py` | `sql:` blocked when flag false; `expr:` blocked; both allowed when flag true (default) |
| `CHANGELOG.md` | Entry under `[Unreleased]` |

---

## 🔲 Feature 9 — Refactoring: centralize `WorkspaceClient`

### Goal

`WorkspaceClient` is instantiated identically in 4 places in `core.py`:
- `_patch_connect_user_context`
- `_check_catalog_access`
- `_drop_table_if_exists`
- `_get_clean_username`

Each duplicates the same `host = os.getenv(...)` / `token = ...` / `WorkspaceClient(host, token)` pattern. Centralize into a cached private helper.

### Implementation

```python
def _get_workspace_client(self):
    """
    Returns a WorkspaceClient if Databricks credentials are available, else None.
    Result is cached for the lifetime of the engine instance.
    """
    if hasattr(self, "_workspace_client_cache"):
        return self._workspace_client_cache

    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    if not (host and token):
        self._workspace_client_cache = None
        return None
    try:
        from databricks.sdk import WorkspaceClient
        self._workspace_client_cache = WorkspaceClient(host=host, token=token)
    except Exception:
        self._workspace_client_cache = None
    return self._workspace_client_cache
```

All 4 call sites replaced with `w = self._get_workspace_client(); if w: ...`.

**Side benefit:** single SDK connection per engine instance instead of 4 separate ones.

### Files to modify

| File | Action |
|---|---|
| `src/skifer/core/core.py` | Add `_get_workspace_client()` method; replace 4 instantiation sites |
| `tests/test_core.py` | Cache hit (second call returns same object), cache miss (no creds → None), used in `_check_catalog_access` |
| `CHANGELOG.md` | Entry under `[Unreleased]` |

---

## Out of scope / backlog 📋

- `assert_no_nulls_in: [cols]` — blocking validation vs silent drop
- `drop_duplicates_on` with explicit tie-breaking order (`preprocess.qualify` covers this)
- Schema-level quality checks (post-join, before `select_final`)
- `assert_row_count_gt: N` — pipeline guard
- Jinja2 conditionals / loops in YAML params
- Tier 2 operations (`replace`, `date_format`, `date_add`, `concat`, etc.)
- Sandbox table refresh command (`engine.refresh_sandbox()`)
- Warning in `to_pdf()` about data confidentiality *(Copilot rec. #3 — minor UX, out of current scope)*

## Not included — rationale

- **Sanitize `.skifer_user` (Copilot rec. #2):** the email is a human-readable annotation never used programmatically. The real protection is the `.gitignore` instruction already embedded in the file and the README. No security gain from removing it.
- **Limit `_find_file_upwards` depth (Copilot rec. #4):** the unlimited search is intentional for monorepo support (`config.yaml` may live several levels up). Limiting to 2–3 levels would break this use case.

---

## Delivery order

| # | Feature | Status | Commit |
|---|---|---|---|
| 0 | Naming convention + new ops + warning + `list_loaders()` | ✅ | `866cc17` |
| 4 | `dedup_after_union` configurable | ✅ | `61143d0` |
| 2 | `quality_checks` DSL | ✅ | `002e970` |
| 6 | `force_env`, `dev_limit` | ✅ | `5baf623` |
| 1 | YAML loader (`load_schema`, `parse_schema`, `default_params`) | ✅ | `1e8d6d1` |
| 5 | Schema expressiveness (OR filters, `keep_all_columns`, chained when, `literal:`) | ✅ | `7f797f6` |
| 3 | Smart sandbox resolution + `SandboxResolver` + user ID persistence | ✅ | `2bede5c` + `019875a` |
| 7 | `describe_schema()` | ✅ | `d49a054` |
| — | README + docs | ✅ | `e0311de` |
| 8 | Governance: `allow_raw_sql` per env | 🔲 | — |
| 9 | Refactoring: centralize `WorkspaceClient` | 🔲 | — |
