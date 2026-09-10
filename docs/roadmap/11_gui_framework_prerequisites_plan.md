# Plan 11 — Prérequis framework pour le client graphique

> Statut : **Implémenté — prêt à merger sur `main`**  
> Branche : `feat/gui-client-planning`  
> Objectif : préparer le framework Skifer pour être exposé via une API FastAPI dans le client Electron.

---

## Contexte

Le client graphique (`skifer-app`) consomme le framework via une couche FastAPI locale. Trois gaps ont été identifiés lors de l'audit :

1. `CatalogInspector` ne permet pas l'énumération des schemas et ne retourne pas les types de colonnes.
2. `describe_schema()` affiche dans stdout mais ne retourne rien — inutilisable depuis une API.
3. Il n'existe pas de méthode retournant le schéma de sortie d'un pipeline (noms + types) sans exécuter Spark.

Ce plan adresse ces trois points de manière indépendante.

---

## Phase 1 — Étendre `CatalogInspector` et le Backend Protocol

### Fichiers à modifier

- `src/skifer/core/backend.py`
- `src/skifer/core/catalog_inspector.py`
- `tests/test_catalog_inspector.py`

### 1.1 — Ajouter `list_schemas()` et `list_column_types()` au Backend Protocol

Dans `backend.py`, ajouter deux nouvelles méthodes à la section **Metadata** (après la ligne 95) :

```python
def list_schemas(self, catalog: str | None = None) -> list[str]:
    """Retourne la liste des schemas accessibles dans le catalog donné."""
    ...

def list_column_types(self, fqn: str) -> dict[str, str]:
    """Retourne un dict {nom_colonne: type_sql} pour la table FQN donnée.
    Retourne {} si non supporté par le backend."""
    ...
```

> `list_column_types()` est déclarée "best-effort" (retourne `{}` si non supportée) pour ne pas casser les backends existants.

### 1.2 — Implémenter dans `DeltaBackend` (backend Spark local)

Fichier : `src/skifer/core/loaders.py` ou le fichier contenant `DeltaBackend`.

```python
def list_schemas(self, catalog: str | None = None) -> list[str]:
    rows = self.spark.sql("SHOW SCHEMAS").collect()
    return [r[0] for r in rows]

def list_column_types(self, fqn: str) -> dict[str, str]:
    rows = self.spark.sql(f"DESCRIBE TABLE {fqn}").collect()
    return {r["col_name"]: r["data_type"] for r in rows if r["col_name"] and not r["col_name"].startswith("#")}
```

### 1.3 — Ajouter les méthodes dans `CatalogInspector`

Dans `catalog_inspector.py`, ajouter après la méthode `list_columns()` (ligne 115) :

```python
def list_schemas(self) -> list[str]:
    """Retourne la liste des schemas du catalog courant."""
    return self._backend.list_schemas(self._catalog)

def describe_table(self, fqn_str: str) -> dict[str, str]:
    """Retourne {nom_colonne: type_sql} pour la table donnée.
    Retourne {} si le backend ne supporte pas list_column_types().
    """
    return self._backend.list_column_types(fqn_str)
```

> Note : `list_catalogs()` n'est pas ajouté ici — le catalog courant est déjà connu via `self._catalog` (fourni par `ConfigurationManager`). La FastAPI lira le catalog depuis la config, pas via énumération.

### 1.4 — Tests à ajouter dans `tests/test_catalog_inspector.py`

```python
def test_list_schemas_returns_list(mock_backend):
    # mock_backend.list_schemas() retourne ["silver", "gold"]
    inspector = CatalogInspector(mock_backend, catalog="dev")
    assert inspector.list_schemas() == ["silver", "gold"]

def test_describe_table_returns_types(mock_backend):
    # mock_backend.list_column_types() retourne {"id": "bigint", "amount": "double"}
    inspector = CatalogInspector(mock_backend, catalog="dev")
    result = inspector.describe_table("dev.gold.orders")
    assert result == {"id": "bigint", "amount": "double"}

def test_describe_table_empty_if_unsupported(mock_backend):
    # mock_backend.list_column_types() retourne {}
    inspector = CatalogInspector(mock_backend, catalog="dev")
    assert inspector.describe_table("dev.gold.orders") == {}
```

---

## Phase 2 — Refactorer `describe_schema()` pour retourner un dict structuré

### Fichier à modifier

- `src/skifer/core/core.py`

### 2.1 — Signature cible

Remplacer le comportement print-only par une méthode qui retourne un dict **et** affiche optionnellement le résumé :

```python
def describe_schema(self, schema_dict: dict, print_summary: bool = True) -> dict:
```

### 2.2 — Structure du dict retourné

```python
{
    "sources": [
        {"name": "catalog.silver.orders", "alias": "ord", "filters": [...], "dev_limit": 5000}
    ],
    "joins": [
        {"from": ["ord", "customer_id"], "to": ["cust", "id"], "type": "left"}
    ],
    "business_rules": ["flag_high_value"],
    "quality_checks": {
        "drop_duplicates_on": ["order_id"],
        "drop_nulls_in": ["amount"]
    },
    "output_columns": [
        {"source": "amount", "target": "amount_eur", "ops": ["cast:double", "round:2"]},
        {"source": "literal:ERP", "target": "source_system", "ops": []}
    ],
    "mode": "DEV",
    "env": "DEV",
    "sandbox_suffix": "_abc1"
}
```

### 2.3 — Stratégie de refactoring

La logique de parsing existe déjà dans `describe_schema()` (lignes 939–1063). Il faut :

1. Extraire les sections actuellement imprimées dans des variables intermédiaires.
2. Construire le dict de retour depuis ces variables.
3. Si `print_summary=True` (défaut), imprimer le résumé existant comme avant — **aucune régression**.
4. Retourner le dict dans tous les cas.

> L'appel existant `engine.describe_schema(schema)` sans argument reste identique en comportement.

### 2.4 — Tests à ajouter dans `tests/test_core.py`

```python
def test_describe_schema_returns_dict(engine, minimal_schema):
    result = engine.describe_schema(minimal_schema, print_summary=False)
    assert isinstance(result, dict)
    assert "sources" in result
    assert "output_columns" in result

def test_describe_schema_sources(engine, schema_with_filter):
    result = engine.describe_schema(schema_with_filter, print_summary=False)
    assert len(result["sources"]) == 1
    assert result["sources"][0]["alias"] == "ord"

def test_describe_schema_print_still_works(engine, minimal_schema, capsys):
    engine.describe_schema(minimal_schema, print_summary=True)
    captured = capsys.readouterr()
    assert "Sources" in captured.out
```

---

## Phase 3 — Ajouter `infer_output_schema()`

### Fichier à modifier

- `src/skifer/core/core.py`

### 3.1 — Signature

```python
def infer_output_schema(self, schema_dict: dict) -> list[dict]:
```

### 3.2 — Valeur de retour

```python
[
    {"name": "amount_eur",    "source": "amount",        "ops": ["cast:double", "round:2"], "type": "double"},
    {"name": "source_system", "source": "literal:ERP",   "ops": [],                         "type": "string"},
    {"name": "status_label",  "source": "status",        "ops": ["when/else"],              "type": "string"},
]
```

### 3.3 — Logique d'inférence de type (sans Spark)

La méthode parcourt `select_final` (ou `add_columns`) et applique des règles statiques :

| Opération présente | Type inféré |
|---|---|
| `cast:double` | `double` |
| `cast:int` / `cast:integer` | `int` |
| `cast:string` | `string` |
| `cast:date` / `to_date:*` | `date` |
| `cast:timestamp` | `timestamp` |
| `round:N` | `double` |
| `lit:*` / `literal:*` | `string` (valeur constante) |
| `when/else` | `string` (par défaut) |
| Aucune op → source connue | type de la source si disponible via `describe_table()` |
| Aucune info | `unknown` |

> Le type est **indicatif** — suffisant pour l'affichage dans le YAML Builder. Pas de validation stricte.

### 3.4 — Intégration avec `describe_schema()`

`infer_output_schema()` peut être appelée indépendamment ou enrichir le dict de `describe_schema()` :

```python
result = engine.describe_schema(schema, print_summary=False)
# result["output_columns"] contiendra les types si infer_output_schema() est appelé en amont
```

### 3.5 — Tests à ajouter dans `tests/test_core.py`

```python
def test_infer_output_schema_cast(engine):
    schema = {"select_final": [["amount", "amount_eur", ["cast:double", "round:2"]]]}
    result = engine.infer_output_schema(schema)
    col = next(c for c in result if c["name"] == "amount_eur")
    assert col["type"] == "double"

def test_infer_output_schema_literal(engine):
    schema = {"select_final": [["literal:ERP", "source_system"]]}
    result = engine.infer_output_schema(schema)
    assert result[0]["type"] == "string"

def test_infer_output_schema_unknown(engine):
    schema = {"select_final": [["some_col", "out_col"]]}
    result = engine.infer_output_schema(schema)
    assert result[0]["type"] == "unknown"
```

---

## Ordre d'implémentation recommandé

```
Phase 1 → Phase 2 → Phase 3
```

Chaque phase est indépendante mais Phase 3 peut réutiliser les types retournés par Phase 1 (`describe_table()`). Implémenter dans cet ordre pour pouvoir enrichir Phase 3 avec les types réels des colonnes sources.

---

## CHANGELOG

Ajouter sous `## [Unreleased]` :

```markdown
### Added
- `CatalogInspector.list_schemas()` — liste les schemas du catalog courant
- `CatalogInspector.describe_table(fqn)` — retourne les types de colonnes `{nom: type}`
- `Backend.list_schemas()` — nouvelle méthode du Protocol (best-effort)
- `Backend.list_column_types()` — nouvelle méthode du Protocol (best-effort)
- `SkiferEngine.infer_output_schema(schema_dict)` — infère les types de sortie sans exécuter Spark

### Changed
- `SkiferEngine.describe_schema()` — retourne maintenant un dict structuré en plus d'afficher le résumé. Paramètre `print_summary=True` ajouté (rétrocompatible).
```

---

## Vérification

```bash
pytest tests/test_catalog_inspector.py -x --tb=short
pytest tests/test_core.py -x --tb=short
ruff check src/
```
