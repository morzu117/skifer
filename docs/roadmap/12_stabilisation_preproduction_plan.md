# Plan 12 — Stabilisation pré-production (SQL injection + edge cases)

## Context

Audit de stabilité avant intégration interne sur Databricks. Deux catégories de correctifs :
1. **Sécurité** : injection SQL dans le backend Spark (`operations.py`) — le SQL backend est déjà sécurisé via `_smart_val()` / `_sql_escape()`, pas le chemin Spark.
2. **Couverture de tests** : cas limites non couverts sur les filtres, les paramètres YAML, et les `when/then/else`.

---

## Phase 1 — Fix sécurité : SQL injection dans `operations.py`

### Problème

**Fichier :** `src/skifer/core/operations.py:173`

```python
# AVANT (vulnérable)
return F.expr(f"`{col_name}` {sql_op} '{val}'")
```

Si `val` contient une apostrophe (ex: `"O'Brien"`), la requête est brisée ou exploitable.  
Le SQL backend (`backends/sql_base.py`) utilise déjà `_smart_val()` + `_sql_escape()` — seul le chemin Spark est affecté.

### Fix

Remplacer `F.expr()` par les **opérateurs natifs PySpark Column** — aucune interpolation de chaîne.

```python
# APRÈS (sécurisé)
c = F.col(f"`{col_name}`")
if op in op_map:
    sql_op = op_map[op]
    if sql_op == "=":   return c == val
    if sql_op == "!=":  return c != val
    if sql_op == ">":   return c > val
    if sql_op == "<":   return c < val
    if sql_op == ">=":  return c >= val
    if sql_op == "<=":  return c <= val
```

**Fichiers modifiés :**
- `src/skifer/core/operations.py` : lignes 163–173 (branche `op_map` de `build_condition`)

**Test à ajouter dans `tests/test_core.py`** (après `test_build_filter_expression_canonical_and_aliases` ~ligne 349) :

```python
def test_build_filter_expression_value_with_apostrophe(spark):
    df = spark.createDataFrame([("O'Brien",), ("Smith",)], ["name"])
    conditions = _build_filter_expression([{"column": "name", "operator": "equals", "value": "O'Brien"}])
    result = df.filter(conditions).collect()
    assert len(result) == 1
    assert result[0]["name"] == "O'Brien"
```

---

## Phase 2 — Tests edge cases : filtres Spark

**Fichier cible :** `tests/test_core.py` (après `test_build_filter_expression_in_string_with_spaces_warning` ~ligne 376)

| Test à créer | Scénario |
|---|---|
| `test_build_filter_expression_apostrophe_equals` | valeur avec `'` sur `equals` |
| `test_build_filter_expression_apostrophe_contains` | valeur avec `'` sur `contains` |
| `test_build_filter_expression_numeric_as_string` | valeur numérique passée en string `"42"` |
| `test_build_filter_expression_empty_string_value` | valeur `""` sur `equals` |
| `test_filter_groups_single_group` | `filter_groups` avec un seul groupe |
| `test_filter_groups_empty_list` | `filter_groups: []` → pas de filtre appliqué |

**Fichier cible :** `tests/test_backend_sql_base.py` (après `test_build_filter_equals` ~ligne 645)

| Test à créer | Scénario |
|---|---|
| `test_build_filter_equals_apostrophe` | `equals` avec `"O'Brien"` → SQL correctement échappé avec `''` |

---

## Phase 3 — Tests edge cases : YAML params + when/then/else

### 3a. `schema_loader` — param None

**Fichier :** `tests/test_schema_loader.py` (après `test_parse_schema_missing_param_raises` ~ligne 56)

Comportement actuel : `None` → string vide silencieusement (`schema_loader.py:27`).  
Ajouter un test qui documente ce comportement (ou impose une `ValueError`) :

```python
def test_parse_schema_none_param_becomes_empty_string():
    yaml_str = "tables:\n  - name: '{{ catalog }}.silver.orders'"
    schema = parse_schema(yaml_str, params={"catalog": None})
    assert schema["tables"][0]["name"] == ".silver.orders"  # comportement actuel documenté
```

### 3b. `when/then/else` — cas malformés

**Fichier :** `tests/test_core.py` (après `test_chained_when_else_dict_form` ~ligne 905)

| Test à créer | Scénario |
|---|---|
| `test_when_compact_missing_else` | `ops = ["when:equals:X", "then:lit:Y"]` (2 éléments, pas 3) → pas de crash |
| `test_when_compact_invalid_condition` | `when:unknown_op:X` → warning + pas de crash |

---

## Phase 4 — Exception handling ciblée (`core.py`)

Les `except Exception: pass` des lignes 331–437 sont intentionnels (chaîne de fallbacks user/job). **Ne pas toucher.**

Un seul fix ciblé :

**Fichier :** `src/skifer/core/core.py:185–186`

```python
# AVANT
except Exception:
    self._workspace_client_cache = None

# APRÈS
except Exception as e:
    import warnings
    warnings.warn(f"[Skifer] Databricks WorkspaceClient unavailable: {e}", stacklevel=2)
    self._workspace_client_cache = None
```

Pas de test ajouté pour ce bloc (comportement de fallback).

---

## Récapitulatif des fichiers modifiés

| Fichier | Nature |
|---|---|
| `src/skifer/core/operations.py` | Fix SQL injection lignes 163–173 |
| `src/skifer/core/core.py` | Warning WorkspaceClient ligne 185 |
| `tests/test_core.py` | +8 tests (filtres apostrophe, filter_groups, when/then/else) |
| `tests/test_schema_loader.py` | +1 test (param None) |
| `tests/test_backend_sql_base.py` | +1 test (apostrophe SQL backend) |
| `CHANGELOG.md` | Entrée `[Unreleased]` |

## Stratégie de vérification

```bash
pytest tests/test_core.py -x --tb=short
pytest tests/test_schema_loader.py -x --tb=short
pytest tests/test_backend_sql_base.py -x --tb=short
pytest tests/ -x --tb=short   # suite complète
```

Tous les tests existants doivent rester verts. Les nouveaux tests sur apostrophes doivent passer grâce au fix Phase 1.

## Risques

| Risque | Mitigation |
|---|---|
| Le passage à opérateurs natifs PySpark change le comportement sur les types numériques | Ajouter un test `numeric_as_string` en Phase 2 pour vérifier |
| `filter_groups: []` peut avoir un comportement implicite dans le pipeline | Test explicite en Phase 2 |