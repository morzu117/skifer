# Plan de migration — Abstraction multi-plateforme

> Branche de travail : `multi_plateforme`
> Statut : Abandonné — remplacé par le [Plan 26 (recentrage Databricks)](26_databricks_refocus_plan.md) ; le produit est Spark/Databricks uniquement (29 juillet 2026)
> Document conservé à titre historique. Ne pas implémenter.

---

## Contexte

Skifer est couple a PySpark/Databricks dans tout son core (`core.py`, `sandbox.py`, `config.py`, `spark_factory.py`, `loaders.py`). L'objectif est de rendre le moteur agnostique de la plateforme en introduisant un `Backend` Protocol, puis d'implementer un `SparkBackend` qui encapsule tout le code Spark existant. Les modules deja agnostiques (schema_loader, registry, semantic/builder, semantic/validator, agentic/resolver, etc.) ne sont pas touches.

Plateformes cibles (TOP 5 marche) : **Databricks**, **Snowflake**, **Google BigQuery**, **AWS Redshift**, **Azure Synapse / Fabric**.

---

## Etat des lieux des plateformes cibles

### SDK Python et paradigmes

| Plateforme | SDK Python | Paradigme | DataFrame API ? | SQL natif ? |
|---|---|---|---|---|
| **Databricks** | `pyspark` + `databricks-sdk` | DataFrame-first | Oui (PySpark DataFrame) | Oui (`spark.sql()`) |
| **Snowflake** | `snowflake-snowpark-python` | DataFrame-first | Oui (Snowpark DataFrame) | Oui (`session.sql()`) |
| **BigQuery** | `google-cloud-bigquery` | SQL-first | Non natif (BigFrames existe mais immature) | Oui (`client.query()`) |
| **Redshift** | `redshift-connector` / `psycopg2` | SQL-only | Non | Oui (via connector SQL) |
| **Synapse / Fabric** | `pyspark` (Fabric) / `pyodbc` (Synapse SQL) | Mixte | Oui sur Fabric (PySpark), Non sur Synapse SQL | Oui |

### Deux familles de backends

**Famille 1 — Plateformes a DataFrame API** (Databricks, Snowflake, Fabric)

Les operations comme `df.filter()`, `df.join()`, `df.select()` existent nativement en Python. Snowpark a ete concu pour etre compatible avec l'API PySpark — les noms de methodes sont quasi-identiques :

```python
# Databricks (PySpark)
from pyspark.sql import functions as F
df = spark.table("catalog.schema.orders")
df = df.filter(F.col("status") == "ACTIVE")
df = df.join(customers, on="customer_id", how="left")

# Snowflake (Snowpark) — quasi-identique
from snowflake.snowpark import functions as F
df = session.table("database.schema.orders")
df = df.filter(F.col("status") == "ACTIVE")
df = df.join(customers, on="customer_id", how="left")
```

**Famille 2 — Plateformes SQL-only** (BigQuery, Redshift, Synapse SQL)

Pas de DataFrame API Python native. Toute interaction passe par des requetes SQL en string :

```python
# BigQuery
from google.cloud import bigquery
client = bigquery.Client()
result = client.query("SELECT * FROM `project.dataset.orders` WHERE status = 'ACTIVE'")

# Redshift
import redshift_connector
conn = redshift_connector.connect(host=..., database=..., user=..., password=...)
cursor = conn.cursor()
cursor.execute("SELECT * FROM schema.orders WHERE status = 'ACTIVE'")
```

### Strategie d'architecture : hybride (DataFrame + SQL)

**Deux classes de base** pour couvrir les deux familles, unifiees derriere le meme Protocol :

```
Backend Protocol
    |
    ├── DataFrameBackend (classe de base)     → delegue aux APIs DataFrame natives
    |       ├── SparkBackend                  → pyspark
    |       ├── SnowparkBackend               → snowflake.snowpark
    |       └── FabricBackend                 → pyspark (Fabric)
    |
    └── SQLBackend (classe de base)           → genere du SQL, retourne des pandas DataFrames
            ├── BigQueryBackend               → google.cloud.bigquery
            ├── RedshiftBackend               → redshift_connector
            └── SynapseBackend                → pyodbc
```

**Le `SQLBackend`** implemente toutes les methodes du Protocol en construisant progressivement une requete SQL. Le SQL n'est materialise qu'au moment de l'execution (write, collect, count) :

```python
class SQLBackend:
    """Classe de base pour les backends SQL-only."""

    def filter(self, df, condition):
        # df est un SQLQuery wrapper, condition est un fragment SQL
        df.add_where(condition)
        return df

    def join(self, left, right, on, how="left"):
        left.add_join(right, on, how)
        return left

    def select(self, df, columns):
        df.set_select(columns)
        return df

    # A l'execution, le SQL complet est materialise et envoye au backend
    def write_table(self, df, fqn, mode="overwrite"):
        sql = df.to_sql()
        self.execute_sql(f"CREATE OR REPLACE TABLE {fqn} AS {sql}")
```

**Pourquoi cette approche et pas tout en SQL ?**

Faire passer Spark et Snowpark par du SQL string perdrait les optimisations natives (Catalyst, Snowpark query planning). Les backends DataFrame gardent leurs performances optimales ; seuls les backends SQL-only passent par la generation SQL.

### Matrice de compatibilite par methode

| Methode Protocol | Spark (DataFrame) | Snowpark (DataFrame) | BigQuery (SQL) | Redshift (SQL) |
|---|---|---|---|---|
| `read_table` | `spark.table(fqn)` | `session.table(fqn)` | `SELECT * FROM fqn` | `SELECT * FROM fqn` |
| `col` | `F.col("x")` | `F.col("x")` | `"x"` (string ref) | `"x"` (string ref) |
| `lit` | `F.lit(val)` | `F.lit(val)` | SQL literal | SQL literal |
| `filter` | `df.filter(cond)` | `df.filter(cond)` | `WHERE clause` | `WHERE clause` |
| `join` | `df.join(...)` | `df.join(...)` | `JOIN ... ON` | `JOIN ... ON` |
| `select` | `df.select(cols)` | `df.select(cols)` | `SELECT cols` | `SELECT cols` |
| `with_column` | `df.withColumn(n, c)` | `df.with_column(n, c)` | `SELECT *, expr AS n` | `SELECT *, expr AS n` |
| `drop_duplicates` | `df.dropDuplicates(cols)` | `df.drop_duplicates(cols)` | `QUALIFY ROW_NUMBER()` | CTE + `ROW_NUMBER()` |
| `drop_nulls` | `df.dropna(subset)` | `df.dropna(subset)` | `WHERE col IS NOT NULL` | `WHERE col IS NOT NULL` |
| `row_number_over` | `Window` + `row_number()` | `Window` + `row_number()` | `QUALIFY ROW_NUMBER() OVER (...)` | CTE + `ROW_NUMBER()` |
| `union_by_name` | `unionByName(allow=True)` | `union_by_name(allow=True)` | `UNION ALL` (colonnes explicites) | `UNION ALL` |
| `write_table` | `saveAsTable` / Delta | `write.save_as_table()` | `CREATE OR REPLACE TABLE AS` | `CREATE TABLE AS` |
| `optimize_table` | `OPTIMIZE ZORDER` | `ALTER TABLE CLUSTER BY` | `CLUSTER BY` a la creation | `VACUUM` + `ANALYZE` |
| `cache` | `df.cache()` | `df.cache_result()` | No-op (cache implicite) | No-op |
| `execute_sql` | `spark.sql(sql)` | `session.sql(sql)` | `client.query(sql)` | `cursor.execute(sql)` |

### Ordre d'implementation des backends

| Ordre | Backend | Justification |
|---|---|---|
| 1 | **SparkBackend** | Code existant — on extrait ce qui marche deja |
| 2 | **SnowparkBackend** | API quasi-identique a PySpark, valide le Protocol avec un effort minimal |
| 3 | **SQLBackend** (base class) | Construit le generateur SQL necessaire a BigQuery et Redshift |
| 4 | **BigQueryBackend** | Plus gros cloud DWH apres Snowflake, forte demande marche |
| 5 | **RedshiftBackend** | Effort marginal une fois le SQLBackend en place |

---

## Architecture du contrat d'interface

### Principe : polymorphisme a l'instanciation

L'engine ne sait jamais quel backend il utilise. Il appelle toujours les memes methodes du Protocol. Le choix se fait **une seule fois**, a l'instanciation — pas de `if backend == "spark"` dans le core.

```
Utilisateur
    |
    v
SkiferEngine(backend=SparkBackend())     # ou SnowflakeBackend(), BigQueryBackend()
    |
    v
self._backend.read_table(fqn)               # appel uniforme
self._backend.filter(df, condition)
self._backend.write_table(df, fqn)
    |                                    |                                |
    v                                    v                                v
SparkBackend                     SnowflakeBackend                 BigQueryBackend
  spark.table(fqn)                 session.table(fqn)              client.query(sql)
  df.filter(cond)                  df.filter(cond)                 WHERE clause
  df.write.saveAsTable()           df.write.save_as_table()        INSERT INTO ...
```

Le point de decision :
```python
# Implicite (defaut) — rien ne change pour l'utilisateur Spark existant
engine = SkiferEngine()
# → cree automatiquement un SparkBackend en interne

# Explicite — l'utilisateur choisit son backend
from skifer_snowflake import SnowflakeBackend
engine = SkiferEngine(backend=SnowflakeBackend(sf_session))
```

### Gestion des methodes non-natives : capabilities + best-effort

Toutes les plateformes ne supportent pas les memes operations. Trois cas de figure :

**Cas 1 — Equivalent natif direct (90% des methodes)**

`read_table`, `filter`, `join`, `select`, `drop_nulls`, etc. ont un equivalent direct sur toutes les plateformes. Chaque backend implemente avec son API native.

**Cas 2 — Pas d'equivalent natif, mais une strategie best-effort existe**

Le backend implemente quand meme la methode avec la meilleure strategie disponible sur la plateforme, et **log clairement que ce n'est pas natif**. L'objectif est que l'utilisateur beneficie toujours d'une optimisation, meme partielle.

Exemple pour `optimize_table` :

| Plateforme | Natif ? | Strategie best-effort |
|---|---|---|
| Databricks | Oui | `OPTIMIZE {fqn} ZORDER BY (cols)` |
| Snowflake | Partiel | `ALTER TABLE {fqn} CLUSTER BY (cols)` — declare les cles, Snowflake optimise en arriere-plan |
| BigQuery | Non | `CREATE OR REPLACE TABLE {fqn} CLUSTER BY (cols) AS SELECT * FROM {fqn} ORDER BY (cols)` — recree la table clusterisee |
| Spark local | Non | `df.orderBy(cols).write.mode("overwrite")` — ameliore le data skipping Delta |

Exemple pour `cache` :

| Plateforme | Natif ? | Strategie best-effort |
|---|---|---|
| Spark | Oui | `df.cache()` — cache en memoire/disque |
| Snowflake | Implicite | No-op — Snowflake a un result cache automatique |
| BigQuery | Non | No-op + log — pas de cache explicite, l'utilisateur peut utiliser des tables materialisees |

Exemple pour `clone_table` (sandbox) :

| Plateforme | Natif ? | Strategie best-effort |
|---|---|---|
| Databricks | Oui | `SHALLOW CLONE` (zero-copy, metadata only) |
| Snowflake | Oui | `CREATE TABLE ... CLONE` (zero-copy natif) |
| BigQuery | Non | `CREATE TABLE ... AS SELECT *` (copie complete) |
| Spark local | Non | `CREATE TABLE ... AS SELECT *` (copie complete) |

**Cas 3 — Operation impossible (aucune strategie raisonnable)**

Le backend leve une exception explicite ou retourne un indicateur.

### Systeme de capabilities

Chaque backend declare ses capacites. L'engine peut adapter son comportement ou informer l'utilisateur.

```python
class Backend(Protocol):
    @property
    def capabilities(self) -> set[str]:
        """Capacites supportees par ce backend."""
        ...
```

```python
class SparkBackend:
    @property
    def capabilities(self) -> set[str]:
        return {"optimize", "zorder", "shallow_clone", "cache", "window"}

class SnowflakeBackend:
    @property
    def capabilities(self) -> set[str]:
        return {"clustering", "zero_copy_clone", "cache_implicit", "window"}
```

L'engine utilise les capabilities pour le logging et les decisions :

```python
def optimize_table(self, target_layer, target_table_name, zorder_cols=None):
    fqn = self._build_fqn(actual_schema, target_table_name)
    result = self._backend.optimize_table(fqn, zorder_cols)
    # Le backend log lui-meme si c'est natif ou best-effort
```

**Regle : le Protocol definit le QUOI (optimise cette table), le Backend decide le COMMENT (natif, best-effort, ou no-op avec log). L'engine n'a jamais besoin de connaitre la plateforme.**

### Niveaux de support par methode

Pour chaque methode du Protocol, on definit le niveau de support attendu :

| Niveau | Signification | Comportement |
|---|---|---|
| **required** | Doit etre implemente par tout backend | Pas de fallback — si absent, le backend n'est pas conforme |
| **best-effort** | Implemente avec la meilleure strategie dispo | Log `[BackendName] method_name: native` ou `[BackendName] method_name: best-effort (strategie utilisee)` |
| **optional** | Peut etre un no-op | Log `[BackendName] method_name: not supported, skipped` |

| Methode | Niveau |
|---|---|
| `read_table`, `write_table`, `execute_sql` | **required** |
| `col`, `lit`, `expr`, `filter`, `select`, `join` | **required** |
| `with_column`, `limit`, `drop_columns`, `union_by_name` | **required** |
| `drop_duplicates`, `drop_nulls`, `count`, `is_empty` | **required** |
| `when`, `otherwise`, `apply_operation`, `build_filter_expression` | **required** |
| `row_number_over`, `build_fqn` | **required** |
| `schema_exists`, `table_exists`, `ensure_schema_exists`, `drop_table` | **required** |
| `check_catalog_access`, `get_current_user` | **required** |
| `optimize_table` | **best-effort** |
| `cache`, `unpersist` | **best-effort** |
| `clone_table` (dans sandbox) | **best-effort** |

---

## Phase 0 — Extraction des modules internes (decouplage preparatoire)

**But** : Reduire `core.py` (1362 lignes) en extrayant les fonctions couplees a Spark dans des modules dedies. Pas de changement de comportement — deplacements mecaniques uniquement.

**Risque** : Tres faible.

### 0.1 — Creer `src/skifer/core/operations.py`

Extraire de `core.py` :
- `_apply_operation(c, op_str, allow_raw_sql)` (lignes 42-153)
- `_build_filter_expression(filter_list, allow_raw_sql)` (lignes 156-232)

`core.py` re-importe ces fonctions pour garder la compatibilite :
```python
from skifer.core.operations import _apply_operation, _build_filter_expression
```

### 0.2 — Creer `src/skifer/core/environment.py`

Extraire de `SkiferEngine` les methodes de detection d'environnement Databricks :
- `_get_workspace_client(host, token)` → fonction standalone
- `_patch_connect_debugging(spark)` → fonction standalone
- `_patch_connect_user_context(spark, workspace_client)` → fonction standalone
- `_is_running_as_job(dbutils, env_vars)` → fonction standalone
- `_get_clean_username(spark, config, dbutils, workspace_client_fn)` → fonction standalone

Ces fonctions prennent leurs dependances en parametres au lieu de `self`.

### 0.3 — Creer `src/skifer/core/writer.py`

Extraire de `SkiferEngine` :
- `_write_dataframe(df, fqn, label, is_local, spark)` → fonction standalone
- `_write_dataframe_local(df, fqn, spark)` → fonction standalone
- `_drop_table_if_exists(fqn, spark, workspace_client_fn)` → fonction standalone
- `_ensure_schema_exists(schema, is_local, spark)` → fonction standalone

### Fichiers

| Action | Fichier |
|---|---|
| Creer | `src/skifer/core/operations.py` |
| Creer | `src/skifer/core/environment.py` |
| Creer | `src/skifer/core/writer.py` |
| Modifier | `src/skifer/core/core.py` — corps remplaces par delegation |
| Creer | `tests/test_operations.py` |
| Creer | `tests/test_environment.py` |
| Creer | `tests/test_writer.py` |

### Verification
```bash
pytest tests/ -x --tb=short
```

---

## Phase 1 — Definir le Backend Protocol

**But** : Creer le contrat d'interface que tous les backends doivent respecter. Aucun consommateur encore — juste le contrat.

**Risque** : Aucun (ajout pur).

### Creer `src/skifer/core/backend.py`

```python
from __future__ import annotations
from typing import Protocol, Any, runtime_checkable

@runtime_checkable
class Backend(Protocol):
    """Contrat plateforme pour Skifer."""

    # --- Capabilities ---
    @property
    def capabilities(self) -> set[str]:
        """Capacites supportees : 'optimize', 'zorder', 'shallow_clone', 'cache', etc."""
        ...

    # --- Session / Environnement ---
    @property
    def is_local(self) -> bool: ...

    def execute_sql(self, sql: str) -> Any: ...
    def check_catalog_access(self, catalog: str) -> bool: ...
    def get_current_user(self) -> str | None: ...

    # --- Table I/O ---
    def read_table(self, fqn: str) -> Any: ...
    def write_table(self, df: Any, fqn: str, mode: str = "overwrite") -> None: ...
    def drop_table(self, fqn: str) -> None: ...
    def ensure_schema_exists(self, schema: str) -> None: ...

    # --- Metadata ---
    def schema_exists(self, catalog: str | None, schema: str) -> bool: ...
    def table_exists(self, catalog: str | None, schema: str, table: str) -> bool: ...

    # --- Column Factory ---
    def col(self, name: str) -> Any: ...
    def lit(self, value: Any) -> Any: ...
    def expr(self, sql_expr: str) -> Any: ...
    def lit_true(self) -> Any: ...

    # --- Operations sur colonnes ---
    def apply_operation(self, c: Any, op_str: str, allow_raw_sql: bool = True) -> Any: ...
    def build_filter_expression(self, filter_list: list[dict], allow_raw_sql: bool = True) -> Any: ...

    # --- DataFrame Operations ---
    def select(self, df: Any, columns: list) -> Any: ...
    def filter(self, df: Any, condition: Any) -> Any: ...
    def join(self, left: Any, right: Any, on: Any, how: str = "left") -> Any: ...
    def drop_columns(self, df: Any, columns: list) -> Any: ...
    def with_column(self, df: Any, name: str, col: Any) -> Any: ...
    def limit(self, df: Any, n: int) -> Any: ...
    def union_by_name(self, dfs: list, allow_missing: bool = True) -> Any: ...
    def drop_duplicates(self, df: Any, cols: list[str] | None = None) -> Any: ...
    def drop_nulls(self, df: Any, subset: list[str]) -> Any: ...
    def cache(self, df: Any) -> Any: ...
    def unpersist(self, df: Any) -> None: ...
    def is_empty(self, df: Any) -> bool: ...
    def count(self, df: Any) -> int: ...

    # --- Window ---
    def row_number_over(self, df: Any, partition_by: list[str], order_by: list[dict]) -> Any: ...

    # --- When/Then/Otherwise ---
    def when(self, condition: Any, value: Any) -> Any: ...
    def otherwise(self, col: Any, value: Any) -> Any: ...

    # --- Plateforme-specifique (optionnel, defaut no-op) ---
    def optimize_table(self, fqn: str, zorder_cols: list[str] | None = None) -> None: ...

    # --- Build FQN ---
    def build_fqn(self, catalog: str | None, schema: str, table: str) -> str: ...
```

### Fichiers

| Action | Fichier |
|---|---|
| Creer | `src/skifer/core/backend.py` |
| Creer | `tests/test_backend_protocol.py` |

### Verification
```bash
pytest tests/test_backend_protocol.py -v
```

---

## Phase 2 �� Implementer SparkBackend

**But** : Encapsuler tout le code Spark existant dans une implementation concrete du Backend Protocol.

**Risque** : Faible (ajout pur, aucun code existant modifie).

### Creer `src/skifer/backends/spark.py`

`SparkBackend` encapsule :
- `operations.py` → `apply_operation()`, `build_filter_expression()`
- `writer.py` → `write_table()`, `drop_table()`, `ensure_schema_exists()`
- `environment.py` → `check_catalog_access()`, `get_current_user()`
- `spark_factory.py` → session creation
- `pyspark.sql.functions` → `col()`, `lit()`, `expr()`, `when()`
- `pyspark.sql.Window` → `row_number_over()`
- Toutes les operations DataFrame (`.filter()`, `.join()`, `.select()`, etc.)

```python
class SparkBackend:
    def __init__(self, spark=None, is_local: bool = False):
        self._spark = spark
        self._is_local = is_local

    @property
    def spark(self):
        return self._spark

    @property
    def is_local(self) -> bool:
        return self._is_local

    def col(self, name: str):
        return F.col(f"`{name}`")

    def read_table(self, fqn: str):
        return self._spark.table(fqn)

    def filter(self, df, condition):
        return df.filter(condition)

    def join(self, left, right, on, how="left"):
        return left.join(right, on=on, how=how)

    # ... chaque methode delegue au code Spark existant
```

### Fichiers

| Action | Fichier |
|---|---|
| Creer | `src/skifer/backends/__init__.py` |
| Creer | `src/skifer/backends/spark.py` |
| Creer | `tests/test_backend_spark.py` |

### Verification
```bash
pytest tests/test_backend_spark.py -v
```

---

## Phase 3 — Brancher l'engine sur le Backend (phase critique)

**But** : Modifier `SkiferEngine` pour utiliser `self._backend` au lieu d'appeler Spark directement.

**Risque** : Eleve — touche `core.py` (1362 lignes). Mitigation : migration methode par methode, commit atomique, tests apres chaque commit.

### Strategie : migration incrementale

Chaque sous-etape est un commit autonome. Les tests existants sont lances apres chaque commit.

### 3.1 — Modifier `__init__` pour accepter `backend=`

```python
def __init__(self, spark=None, config_path=None, force_env=None, backend=None):
    if backend is not None:
        self._backend = backend
        self.spark = backend.spark if hasattr(backend, 'spark') else spark
    else:
        # Legacy : comportement identique a aujourd'hui
        # ... existing spark init ...
        from skifer.backends.spark import SparkBackend
        self._backend = SparkBackend(self.spark, self.is_local)
```

**Compatibilite** : `SkiferEngine()` sans args continue de creer un SparkBackend automatiquement.

### 3.2 — Migrer `process_schema()` (lignes 923-1083)

| Code actuel | Remplacement |
|---|---|
| `self.spark.table(name)` | `self._backend.read_table(name)` |
| `df.limit(n)` | `self._backend.limit(df, n)` |
| `df.filter(_build_filter_expression(...))` | `self._backend.filter(df, self._backend.build_filter_expression(...))` |
| `Window.partitionBy().orderBy()` + `row_number()` | `self._backend.row_number_over(df, ...)` |
| `df.dropna(subset=cols)` | `self._backend.drop_nulls(df, cols)` |
| `df.dropDuplicates(cols)` | `self._backend.drop_duplicates(df, cols)` |
| `dfs[alias].select(...)` | `self._backend.select(dfs[alias], ...)` |
| `df_main.join(...)` | `self._backend.join(...)` |
| `.drop(...)` | `self._backend.drop_columns(...)` |
| `df_main.withColumn(...)` | `self._backend.with_column(...)` |
| `F.col(...)` | `self._backend.col(...)` |

### 3.3 — Migrer `get_select_expressions()` (lignes 851-921)

Remplacer `F.col()`, `F.when()`, `F.lit()`, `_apply_operation()` par les equivalents backend. Partie la plus delicate (when/then/else imbriques). A migrer en dernier dans cette phase.

### 3.4 — Migrer les methodes d'orchestration

- `run_process_to_table()` → `self._backend` pour write/drop/ensure_schema
- `run_process_and_split()` → idem + `cache()` / `unpersist()`
- `run_union_sources_to_table()` → idem + `union_by_name()`, `count()`
- `optimize_table()` → `self._backend.optimize_table()`

### 3.5 — Migrer `SandboxResolver`

Accepter un `backend` en constructeur. Remplacer `self.spark.sql(...)` par `self._backend.execute_sql(...)` dans `schema_exists()`, `table_exists()`, `create_schema()`, `clone_table()`.

### 3.6 — Migrer `SemanticEngine`

- `semantic.py:211` — `self.spark.sql(resolved.full_sql)` → `self.core._backend.execute_sql(...)`
- `semantic.py:255` — `self.spark.sql(ddl)` → `self.core._backend.execute_sql(...)`

### 3.7 — Migrer `ConfigurationManager`

- `config.py:123` — accepter un `backend` optionnel, utiliser `backend.check_catalog_access()` si disponible

### Fichiers

| Action | Fichier |
|---|---|
| Modifier | `src/skifer/core/core.py` |
| Modifier | `src/skifer/core/sandbox.py` |
| Modifier | `src/skifer/semantic/semantic.py` |
| Modifier | `src/skifer/core/config.py` |
| Creer | `tests/test_engine_with_backend.py` |

### Verification apres chaque sous-etape
```bash
pytest tests/ -x --tb=short
```

---

## Phase 4 — Loaders backend-aware

**But** : Rendre les loaders integres compatibles avec le backend tout en preservant la signature des loaders utilisateur.

**Risque** : Faible.

Passer `backend=self._backend` dans les kwargs du loader. Les loaders utilisateur l'ignorent via `**kwargs`, les loaders integres l'utilisent.

### Fichiers

| Action | Fichier |
|---|---|
| Modifier | `src/skifer/core/core.py` (invocation des loaders) |
| Modifier | `src/skifer/core/loaders.py` (loaders integres) |
| Modifier | `tests/test_loaders.py` |

---

## Phase 5 — FakeBackend pour tests sans Spark

**But** : Prouver que l'engine est reellement decouplee en executant les tests avec un backend Python pur (sans PySpark).

**Risque** : Faible (ajout pur).

### Fichiers

| Action | Fichier |
|---|---|
| Creer | `tests/fakes/__init__.py` |
| Creer | `tests/fakes/fake_backend.py` |
| Creer | `tests/test_engine_fake_backend.py` |

---

## Graphe de dependances

```
Phase 0 (Extraction)
    |
    v
Phase 1 (Protocol) -----> Phase 2 (SparkBackend)
                                |
                                v
                           Phase 3 (Branchement Engine) -----> Phase 4 (Loaders)
                                |
                                v
                           Phase 5 (FakeBackend)
```

Phases 0 et 1 peuvent etre faites en parallele (fichiers differents).

---

## Chemin critique et risques

| Phase | Risque | Mitigation |
|---|---|---|
| **Phase 3** | **Eleve** — touche core.py (1362 lignes) | Migration methode par methode, commit atomique, tests apres chaque commit |
| Phase 3.3 | **Moyen** — when/then/else imbriques | Migrer en dernier, comparer outputs avec l'ancien code |
| Phase 3.5 | **Faible** — sandbox deja quasi SQL-pur | Tests mock existants couvrent bien |
| Phase 0 | **Tres faible** — deplacements mecaniques | Re-exports preserves |

---

## Strategie de verification globale

1. **Apres chaque phase** : `pytest tests/ -x --tb=short`
2. **Apres Phase 3** : `SkiferEngine()` (constructeur par defaut) fonctionne identiquement
3. **Apres Phase 5** : tests FakeBackend prouvent le decouplage reel
4. **CHANGELOG** : une entree par phase dans `[Unreleased]`
