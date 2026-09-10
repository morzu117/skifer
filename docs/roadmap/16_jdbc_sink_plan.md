# Plan 16 — JDBC Sink : écriture Gold vers PostgreSQL / Timescale

> Branche : `feat/jdbc-sink`
> Statut : En attente de validation
> Objectif : permettre à `run_process_to_table` d'écrire vers un sink JDBC (PostgreSQL, Timescale…)
> en plus du sink Delta par défaut, avec un YAML aussi minimal que possible.

---

## Contexte

Skifer écrit aujourd'hui exclusivement en Delta Lake (`df.write.format("delta")`).
Un partenaire souhaite brancher le Gold sur PostgreSQL / TimescaleDB pour alimenter ses moteurs
d'intelligence (cohort, contagion, journey…). L'architecture cible :

```
tenant.yaml  →  Skifer (Bronze→Silver→Gold)  →  Postgres/Timescale  →  engines + dashboard
```

Le choix technique est Spark JDBC — Spark sait écrire en JDBC nativement, le driver PostgreSQL
est disponible, et ça n'ajoute aucune dépendance Python supplémentaire.

---

## Principe de design : YAML minimal, credentials dans .env

Le YAML ne doit pas contenir de credentials ni de URLs de connexion.
Toutes les informations sensibles vivent dans `.env` (déjà chargé au démarrage du moteur).

### YAML cible (côté utilisateur)

**Cas standard — sink Postgres, table déduite du pipeline :**
```yaml
# scripts/gold/kpi_customers.yaml
tables:
  - name: "{{ catalog }}.silver.fact_orders"
    alias: fo
...
sink:
  type: postgres
```
C'est tout. Le nom de table cible est déduit automatiquement (`target_layer.target_table_name`).

**Cas avancé — override du schéma ou de la table cible :**
```yaml
sink:
  type: postgres
  schema: reporting          # optionnel, surcharge le schéma cible
  table: kpi_clients_gold    # optionnel, surcharge le nom de table
```

### .env côté projet

```dotenv
POSTGRES_HOST=prod-db.internal
POSTGRES_PORT=5432
POSTGRES_DB=analytics
POSTGRES_USER=etl_user
POSTGRES_PASSWORD=secret
```

### Valeurs par défaut

| Paramètre     | Défaut                                      |
|---------------|---------------------------------------------|
| `POSTGRES_PORT` | `5432`                                    |
| `schema`      | valeur de `target_layer` (ex: `gold`)       |
| `table`       | `target_table_name` (ex: `kpi_customers`)   |
| mode d'écriture | `overwrite` (DROP + recreate via Spark)  |

---

## Périmètre — ce qui ne change PAS

- Le pipeline de transformation est **inchangé** : load → filter → join → rules → select_final
  produisent un DataFrame Spark exactement comme avant.
- Le sink Delta reste le défaut absolu : aucun YAML sans `sink:` n'est affecté.
- Les autres backends (Snowpark, BigQuery, SQLBase) ne sont pas touchés.
- Sandbox, dev_limit, quality_checks : comportement identique.

---

## Fichiers à créer / modifier

| Fichier | Action |
|---------|--------|
| `src/skifer/sinks/__init__.py` | Nouveau module sinks |
| `src/skifer/sinks/jdbc.py` | `JDBCSink` — lecture .env, construction URL, write Spark JDBC |
| `src/skifer/core/schema_loader.py` | Normalisation + validation du bloc `sink:` |
| `src/skifer/core/core.py` | Routage dans `_write_dataframe()` et `run_process_to_table()` |
| `tests/test_sink_jdbc.py` | Tests unitaires (Spark mocké, pas de PG réel nécessaire) |
| `CHANGELOG.md` | Entrée `[Unreleased]` |

---

## Phases d'implémentation

### Phase 1 — `JDBCSink` (sinks/jdbc.py)

Responsabilités :
- Lire `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`
  depuis `os.environ` (déjà peuplé par `load_dotenv` au démarrage).
- Construire l'URL JDBC : `jdbc:postgresql://{host}:{port}/{db}`.
- Exposer `write(df, schema, table, mode="overwrite")`.
- Lever `EnvironmentError` avec un message clair si une variable obligatoire est absente.

```python
# Interface publique visée
class JDBCSink:
    def __init__(self):  # lit os.environ
        ...
    def write(self, df, schema: str, table: str, mode: str = "overwrite") -> None:
        ...
```

Pas de paramètre de connexion dans le constructeur : tout vient de l'environnement.

### Phase 2 — Normalisation du bloc `sink:` (schema_loader.py)

Dans `_normalize_schema()`, après les validations existantes :

- Si `sink:` absent → ne rien faire (Delta par défaut, comportement inchangé).
- Si `sink:` présent → valider :
  - `type` obligatoire, valeurs acceptées : `["delta", "postgres", "jdbc"]`
  - `postgres` et `jdbc` sont des alias du même backend.
  - `schema` et `table` sont optionnels (string).
  - Toute autre clé → `ValueError` explicite (évite les typos silencieux).

### Phase 3 — Routage dans core.py

Modifier `_write_dataframe()` pour accepter un `sink_config` optionnel :

```python
def _write_dataframe(self, df, fqn, label, sink_config=None):
    if sink_config and sink_config.get("type") in ("postgres", "jdbc"):
        from skifer.sinks.jdbc import JDBCSink
        schema = sink_config.get("schema") or fqn.split(".")[-2]
        table  = sink_config.get("table")  or fqn.split(".")[-1]
        JDBCSink().write(df, schema, table)
    else:
        self._get_backend().write_table(df, fqn)
```

Modifier `run_process_to_table()` pour extraire `sink_config` depuis `schema_dict` et le passer à
`_write_dataframe()`. Les étapes `_ensure_schema_exists` et `_drop_table_if_exists` sont skippées
quand le sink est JDBC (Spark JDBC gère le mode overwrite lui-même).

### Phase 4 — Tests (tests/test_sink_jdbc.py)

Cas à couvrir (tout en offline, Spark mocké) :
- `JDBCSink.write()` appelle bien `df.write.format("jdbc")` avec les bons paramètres.
- `EnvironmentError` si `POSTGRES_HOST` absent.
- Normalisation YAML : `sink: {type: postgres}` → pas d'erreur.
- Normalisation YAML : `sink: {type: unknown}` → `ValueError`.
- `run_process_to_table` avec sink postgres → n'appelle pas `backend.write_table()`.
- `run_process_to_table` sans sink → comportement Delta inchangé.

---

## Risques et points d'attention

| Risque | Mitigation |
|--------|------------|
| Driver JDBC PostgreSQL absent sur le cluster | Documenter : le JAR `postgresql-42.x.jar` doit être installé sur le cluster Databricks (ou inclus dans les libs du job). Spark JDBC en a besoin. |
| Mode `overwrite` avec Spark JDBC ≠ DROP TABLE | Spark JDBC `overwrite` tronque la table mais ne recrée pas les contraintes (PK, index). Acceptable pour un sink analytique (Timescale/PG sans contraintes). À documenter. |
| Sandbox en mode JDBC | Le suffixe `_jdoe` n'a pas de sens pour un sink PG. La sandbox est skippée automatiquement : `get_target_schema()` n'est pas utilisé pour le nom de table JDBC. |
| `_drop_table_if_exists` sur un sink JDBC | Inutile et impossible (c'est une table PG, pas une table Spark). Skip explicite dans `run_process_to_table`. |

---

## Vérification

```bash
pytest tests/test_sink_jdbc.py -v      # nouveaux tests
pytest tests/ -x --tb=short            # non-régression complète
```

---

## Ce plan N'inclut PAS (hors périmètre)

- Mode `append` vers JDBC (possible ultérieurement, même pattern).
- Sink Snowflake, BigQuery, Redshift (backends séparés, plans futurs).
- Configuration de plusieurs sinks nommés dans `config.yaml` (complexité non justifiée aujourd'hui).
- Création automatique de la table PG (Spark JDBC le fait en mode overwrite).
