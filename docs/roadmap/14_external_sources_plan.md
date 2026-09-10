# Plan 14 — Sources externes déclaratives (CSV, Parquet, Blob, JSON…)

> **Statut : Implémenté — livré dans `1.0.0-beta.4`**

## Contexte

Aujourd'hui, chaque table d'entrée dans le bloc `tables:` est résolue via
`backend.read_table(fqn)` — c'est-à-dire une table Delta/Unity Catalog identifiée
par un FQN (`catalog.schema.table`).

Il existe déjà un mécanisme d'échappatoire : `source_type: loader` + un Python
`@register_loader` enregistré à la main. Mais ce mécanisme **exige du code Python**,
ce qui viole le principe "What not How" du framework.

### Besoin utilisateur

Initialiser une couche Bronze en lisant un fichier externe (CSV, Parquet, JSON, Avro)
depuis un blob storage (ADLS Gen2, S3, GCS) ou le système de fichiers local, **sans
écrire de Python**. Le YAML suffit :

```yaml
tables:
  - name: raw_orders
    alias: orders
    source:
      type: csv
      path: "abfss://container@account.dfs.core.windows.net/bronze/orders/*.csv"
      options:
        header: "true"
        inferSchema: "true"
```

### Périmètre de ce plan

- Formats supportés : `csv`, `parquet`, `json`, `avro`, `orc`, `delta`, `text`
- Stockages supportés : tout chemin accessible à la session Spark (ADLS, S3, GCS, DBFS, local)
- Backend cible : **SparkBackend uniquement** (les backends SQL/DuckDB/BigQuery n'ont pas d'API
  de lecture de fichiers — reporté à un futur plan)
- Injection de paramètres `{{ key }}` dans `path` (même mécanisme que les FQNs)
- Compatibilité totale avec `filter`, `quality_checks`, `dev_limit`, `fields`, `preprocess`
- Rétrocompatibilité : sans `source:`, comportement inchangé

### Non-objectifs

- Pas d'écriture vers des sources externes (uniquement la lecture)
- Pas de support JDBC dans ce plan (complexité de connexion, secrets — plan futur)
- Pas d'inférence automatique du format depuis l'extension (trop fragile)
- Pas de gestion des secrets/credentials (assumé configuré dans l'environnement Spark)
- Backends non-Spark non couverts

---

## Architecture

### Nouveau bloc `source:` dans la définition de table

Le bloc `source:` est **optionnel** et coexiste avec tous les champs existants.
Quand il est présent, il remplace la résolution FQN (`backend.read_table`).

```yaml
tables:
  - name: raw_orders          # obligatoire — utilisé comme clé dans dataframes_in
    alias: orders             # optionnel — alias pour les jointures
    source:
      type: csv               # obligatoire — format Spark (csv, parquet, json, avro, orc, delta, text)
      path: "abfss://..."     # obligatoire — chemin ou glob (supporte {{ param }})
      options:                # optionnel — spark.read.options()
        header: "true"
        inferSchema: "true"
        delimiter: ";"
    filter:                   # tous les champs existants fonctionnent normalement
      - status:is_not_null
    quality_checks:
      drop_nulls_in: [order_id]
    dev_limit: 5000
```

**Formats Spark valides :** `csv`, `parquet`, `json`, `avro`, `orc`, `delta`, `text`

### Priorité de résolution (dans `process_schema`)

```
1. dataframes_in[name]   → DataFrame injecté manuellement (inchangé — CAS A)
2. source_type: loader   → loader Python enregistré (inchangé — CAS B)
3. source.type + path    → lecture fichier via backend.read_source() (NOUVEAU — CAS C)
4. backend.read_table()  → lecture table Delta/Unity Catalog (inchangé — CAS D)
```

### Extension du protocole `Backend`

Nouvelle méthode abstraite sur `Backend` :

```python
def read_source(self, source_type: str, path: str, options: dict) -> DataFrame:
    """Read an external file source (csv, parquet, json, avro, orc, delta, text)."""
    raise NotImplementedError
```

**`SparkBackend`** : implémentation via `spark.read.format(source_type).options(**options).load(path)`

**Autres backends** (SQL, DuckDB, BigQuery, etc.) : lève `NotImplementedError` avec message clair.

### Normalisation dans `schema_loader.py`

`parse_schema()` / `load_schema()` appliquent déjà l'injection `{{ param }}` sur les noms
de table. La même mécanique doit s'appliquer sur `source.path`.

---

## Phases d'implémentation

### Phase 1 — Extension du Backend

**Fichiers modifiés :**
- `src/skifer/core/backend.py` — méthode `read_source()` sur `Backend` (ABC)
- `src/skifer/core/spark_backend.py` — implémentation Spark

**Détail `SparkBackend.read_source` :**
```python
VALID_SOURCE_TYPES = frozenset({"csv", "parquet", "json", "avro", "orc", "delta", "text"})

def read_source(self, source_type: str, path: str, options: dict | None = None) -> DataFrame:
    if source_type not in VALID_SOURCE_TYPES:
        raise ValueError(
            f"[read_source] Unknown source type '{source_type}'. "
            f"Valid: {sorted(VALID_SOURCE_TYPES)}"
        )
    reader = self.spark.read.format(source_type)
    if options:
        reader = reader.options(**options)
    return reader.load(path)
```

**Tests :** `tests/test_backend.py` — mock `spark.read`, tester types valides/invalides, options vides/remplies.

---

### Phase 2 — Normalisation YAML (`schema_loader.py`)

**Fichiers modifiés :**
- `src/skifer/core/schema_loader.py` — fonction `_normalize_table()` (ou équivalent)

Lors du parsing YAML, si `table.source` est présent :
1. Valider que `type` et `path` sont présents → `ValueError` explicite sinon
2. Valider que `type` est dans la liste des types supportés
3. Appliquer l'injection `{{ param }}` sur `path` (même logique que pour `name`)
4. Normaliser `options` en `dict` (défaut `{}` si absent)

**Tests :** `tests/test_schema_loader.py` — parsing YAML avec source, injection params dans path, erreur si type manquant, erreur si path manquant.

---

### Phase 3 — Logique de chargement dans `core.py`

**Fichiers modifiés :**
- `src/skifer/core/core.py` — méthode `process_schema()`, bloc « CAS B »

Insertion du CAS C entre `source_type: loader` et `read_table()` :

```python
elif "source" in t:
    src_conf = t["source"]
    logger.info("[Load] Reading external source: %s (%s)", t["name"], src_conf["type"])
    df = self._get_backend().read_source(
        source_type=src_conf["type"],
        path=src_conf["path"],
        options=src_conf.get("options", {}),
    )
```

Aucune modification des blocs suivants (dev_limit, filter, quality_checks, fields) — ils
s'appliquent identiquement quelle que soit la source du DataFrame.

**Tests :** `tests/test_core.py`
- `source: csv` avec SparkBackend mocké → read_source appelé avec bons args
- `filter` + `quality_checks` appliqués après chargement externe
- `dev_limit` appliqué après chargement externe
- Backend non-Spark + `source:` → NotImplementedError propagée proprement
- Compatibilité : table sans `source:` → comportement inchangé

---

### Phase 4 — `describe_schema()` — support dry-run

**Fichiers modifiés :**
- `src/skifer/core/core.py` — méthode `describe_schema()`

Afficher la source externe dans le dry-run :
```
[1] raw_orders   alias: orders
    source: csv @ abfss://container@account.dfs.core.windows.net/bronze/orders/*.csv
    filter:  status is_not_null
```

**Tests :** `tests/test_core.py` — describe_schema avec source externe produit le bon affichage.

---

### Phase 5 — Notebook exemple

**Fichiers modifiés :**
- `example/demo_external_sources.ipynb` — **nouveau notebook**

Scénario : ingestion Bronze depuis un CSV local (simulé dans les data de test).

```python
schema = parse_schema("""
tables:
  - name: raw_orders
    alias: orders
    source:
      type: csv
      path: "example/data/orders.json"   # json local pour la démo
      options:
        multiLine: "true"
    quality_checks:
      drop_nulls_in: [order_id, amount_ht]

select_final:
  - [order_id, order_id]
  - [amount_ht, amount_ht]
  - [status, status]
""")

df = core.process_schema(schema)
df.show(5)
```

---

## Risques et points d'attention

| Risque | Mitigation |
|---|---|
| Chemins blob non accessibles (credentials manquants) | L'erreur Spark est explicite — pas de wrapping supplémentaire |
| `inferSchema: "true"` lent sur gros fichiers | Documenter : fournir un schéma Spark explicite via `options: schema: "col1 STRING, col2 INT"` |
| Backend non-Spark + `source:` → crash silencieux | Phase 1 : `NotImplementedError` avec message clair |
| Conflits entre `source:` et `source_type: loader` | Phase 2 : validation YAML → erreur si les deux sont présents |
| `dev_limit` sur CSV distant → lecture complète avant limit | Comportement Spark standard, documenter |

---

## Stratégie de vérification

Après chaque phase : `pytest tests/ -x --tb=short`

Tests d'intégration supplémentaires (phase 3) :
- CSV local réel avec les fichiers `example/data/` existants
- Vérifier que `filter` + `dev_limit` s'appliquent correctement après lecture

---

## CLAUDE.md — mise à jour YAML schema patterns

À ajouter dans la section `YAML schema patterns` :

```yaml
tables:
  - name: raw_orders
    alias: orders
    source:
      type: csv          # csv | parquet | json | avro | orc | delta | text
      path: "{{ base_path }}/orders/*.csv"
      options:
        header: "true"
        inferSchema: "true"
```

---

## Résumé des fichiers impactés

| Fichier | Changement |
|---|---|
| `src/skifer/core/backend.py` | + `read_source()` abstrait |
| `src/skifer/core/spark_backend.py` | + `read_source()` implémenté |
| `src/skifer/core/schema_loader.py` | Validation + normalisation du bloc `source:` |
| `src/skifer/core/core.py` | CAS C dans `process_schema()` + `describe_schema()` |
| `tests/test_backend.py` | Tests `read_source` |
| `tests/test_schema_loader.py` | Tests parsing `source:` |
| `tests/test_core.py` | Tests intégration chargement externe |
| `example/demo_external_sources.ipynb` | Nouveau notebook de démonstration |
| `CHANGELOG.md` | Entrée `[Unreleased]` |
| `CLAUDE.md` | Mise à jour section YAML schema patterns |
