# Piste 1 — Abstraction multi-plateforme

> Priorite : abandonnee
> Statut : Abandonné — remplacé par le [Plan 26 (recentrage Databricks)](26_databricks_refocus_plan.md) ; le produit est Spark/Databricks uniquement (29 juillet 2026)

---

## Constat actuel

Le couplage avec Databricks/PySpark est profond et reparti dans toute la codebase :

| Couche | Dependance concrete |
|---|---|
| `spark_factory.py` | `SparkSession`, `DatabricksSession`, `DatabricksConnect` |
| `core.py` | `pyspark.sql.functions`, `pyspark.sql.Window`, `DBUtils`, `Delta` format, `saveAsTable` |
| `loaders.py` | `spark.table()`, `unionByName`, `F.col`, `F.explode` |
| `sandbox.py` | `SHOW SCHEMAS`, `CREATE SCHEMA`, `CTAS`, `spark.catalog` |
| `config.py` | Notion de `catalog` Unity Catalog, detection d'env Databricks |
| `semantic/` | `spark.sql()` pour executer le SQL genere par le resolver |

Le YAML et la logique declarative (`schema_loader.py`, `registry.py`, `resolver.py`) sont deja quasi-independants de la plateforme. C'est un point fort.

---

## Recommandation d'architecture : librairie principale + connecteurs

**Option retenue : monorepo avec packages separes.**

```
skifer/              # core — zero dep plateforme
skifer-spark/        # connecteur PySpark/Databricks (ce qui existe aujourd'hui)
skifer-snowflake/    # connecteur Snowflake (futur)
skifer-bigquery/     # connecteur BigQuery (futur)
```

### Pourquoi pas une seule librairie avec tous les connecteurs ?

- L'utilisateur Snowflake ne veut pas installer `pyspark` + `delta-spark` (>500 MB de JARs).
- Les dependances sont mutuellement exclusives et lourdes.
- Le modele de packaging par extras (`pip install skifer[snowflake]`) reste fragile pour des backends aussi differents — un package separe est plus propre.

### Pourquoi pas des libs completement independantes ?

- Le YAML, le RuleRegistry, le schema_loader, le semantic layer sont 100% partageables.
- Dupliquer ca dans chaque connecteur creerait une dette de maintenance insoutenable.

---

## Interface d'abstraction proposee

Le coeur definirait un **Backend Protocol** (ABC ou Protocol Python) :

```python
# skifer/core/backend.py

from typing import Protocol, Any

class ExecutionBackend(Protocol):
    """Contrat que chaque connecteur doit implementer."""

    def read_table(self, fqn: str) -> Any:
        """Lit une table et retourne un objet tabulaire natif."""
        ...

    def write_table(self, data: Any, fqn: str, mode: str = "overwrite") -> None:
        """Ecrit un objet tabulaire dans une table."""
        ...

    def execute_sql(self, sql: str) -> Any:
        """Execute une requete SQL et retourne le resultat."""
        ...

    def schema_exists(self, catalog: str | None, schema: str) -> bool: ...
    def table_exists(self, catalog: str | None, schema: str, table: str) -> bool: ...
    def create_schema(self, catalog: str | None, schema: str) -> None: ...
    def drop_table(self, fqn: str) -> None: ...
    def build_fqn(self, catalog: str | None, schema: str, table: str) -> str: ...

    # Operations sur colonnes (le plus delicat)
    def col(self, name: str) -> Any: ...
    def lit(self, value: Any) -> Any: ...
    def apply_filter(self, data: Any, condition: Any) -> Any: ...
    def apply_join(self, left: Any, right: Any, on: list, how: str) -> Any: ...
    def apply_select(self, data: Any, expressions: list) -> Any: ...
    def apply_window(self, data: Any, partition_by: list, order_by: list) -> Any: ...
```

### Point critique : les operations sur colonnes

C'est la partie la plus delicate. Aujourd'hui `_apply_operation()` et `_build_filter_expression()` parlent directement en `F.col()`, `F.when()`, `F.expr()`. Deux approches :

1. **IR intermediaire** : le core traduit le YAML en un arbre d'operations abstrait, chaque backend le compile en natif. Plus propre, plus lourd a implementer.
2. **Backend-level helpers** : chaque backend implemente `apply_operation(col, op_str)` avec sa propre logique. Plus rapide a faire, plus de code duplique.

**Recommandation** : commencer par l'approche 2 (copier `_apply_operation` dans le backend Spark), puis refactorer vers un IR une fois qu'on a deux backends fonctionnels et qu'on voit les vrais patterns communs.

---

## Plan de migration (grandes etapes)

1. Extraire tout ce qui est YAML/schema/registry/semantic dans un package `skifer` pur Python.
2. Creer le Protocol `ExecutionBackend`.
3. Migrer `core.py` pour utiliser le backend au lieu de Spark directement — c'est le gros du travail.
4. Le package `skifer-spark` implemente `SparkBackend(ExecutionBackend)`.
5. Les tests existants continuent de passer (la session Spark locale devient un test du backend Spark).
6. Ecrire `SnowflakeBackend` comme preuve de concept (Snowpark Python ou connecteur SQL pur).

---

## Risques

- **Performance** : une couche d'abstraction peut casser les optimisations Catalyst/Photon si mal faite. Il faut que le backend puisse "passer" les operations en natif, pas tout re-materialiser.
- **Parite de fonctionnalites** : tous les backends ne supporteront pas tout (ex: `OPTIMIZE ZORDER` est 100% Databricks). Prevoir un systeme de capabilities declarees par backend.
- **Effort** : c'est un refactoring structurel. Compter plusieurs semaines de travail a temps plein.

---

## Dependances avec les autres pistes

| Piste | Impact |
|---|---|
| Piste 2 (Client graphique) | Le catalogue explorer depend du backend pour `list_schemas()`, `list_tables()`, `describe_table()` |
| Piste 3 (Lineage) | Le sampling necessite un backend. Le reste est statique (YAML parsing) |
| Piste 5 (Observabilite) | L'execution des checks necessite un backend |
