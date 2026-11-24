# DuckDB — connecteur v0.1

Le paquet séparé [skifer-duckdb](https://github.com/morzu117/skifer-duckdb) implémente les trois
contrats du cœur : exécution SQL, catalogue et publication. Le cœur compile le YAML et rend le
SQL DuckDB ; le connecteur gère une connexion et renvoie une `DuckDBPyRelation` native, paresseuse.

## Installation

Python 3.10 ou supérieur, Skifer 4.2 et DuckDB `>=1.5.6,<2` :

```bash
pip install skifer-duckdb
```

Tant que Skifer 4.2 n'est pas sur PyPI, installer depuis les sources du Plan 46 :

```bash
cd /chemin/vers/skifer-duckdb
python3 -m venv .venv
.venv/bin/pip install -e "/chemin/vers/skifer[dialects]" "duckdb>=1.5.6,<2" "pytest>=7.0" "ruff>=0.1.0"
.venv/bin/pip install --no-deps -e ".[dev]"
```

Le worktree du Plan 46 déclare encore la version `4.1.0` malgré les contrats 4.2 présents.
L'installation locale utilise donc `--no-deps` pour le connecteur après installation explicite
des dépendances ; elle ne change aucune version. `pip check` signale ce décalage de métadonnées
attendu jusqu'au passage du cœur à 4.2. La contrainte distribuée reste `skifer[dialects]>=4.2.0`.

La CI du connecteur installe temporairement Skifer depuis GitHub `main` ; elle ne pourra être
verte qu'après le merge du Plan 46 sur `main`. La borne DuckDB exclut 2.x : la parité exécutée
observe `round(2.675, 2) = 2.67` avec 2.0.1, contre `2.68` dans la référence Spark / DuckDB 1.5.6.
Aucune tolérance supplémentaire ne masque cette différence.

## Pipeline et publication

```python
from skifer.core.ir import parse_to_ir
from skifer.core.schema_loader import parse_schema
from skifer_duckdb import DuckDBConnector

parsed = parse_to_ir(parse_schema("""
tables: [{name: orders, alias: ord}]
aggregate:
  group_by: [country]
  measures: [[amount, revenue, sum]]
"""))

with DuckDBConnector() as connector:
    connector.write_table("orders", """
        SELECT * FROM (VALUES ('FR', 10), ('FR', 20), ('DE', 5)) AS t(country, amount)
    """, "overwrite")
    print(connector.report(parsed))  # statuts et motifs issus du cœur
    print(connector.compile(parsed))
    result = connector.execute_schema(parsed)
    print(result.dataframe.fetchall())
    connector.publish_schema(parsed, "revenue_by_country", "overwrite")
    connector.create_or_replace_view("revenue_view", "SELECT * FROM revenue_by_country")
```

`DuckDBConnector(path=":memory:", default_schema="main", read_only=False)` possède une connexion.
`close()` ou la sortie du `with` la ferme. Consommer les relations avant cette fermeture ; `.df()`
est aussi disponible avec les dépendances pandas de DuckDB. Un `statement_id` UUID est généré
par le connecteur, ce n'est pas un identifiant de requête serveur. Le SQL brut est exécuté tel quel.

Un nom nu devient `main.nom` (ou le `default_schema` choisi). Les noms à deux ou trois parties
restent inchangés. Les noms vides, de plus de trois parties ou contenant des guillemets/backticks
sont refusés. Aucun sandbox ni schéma n'est créé implicitement.

Chaque publication de table ou vue est transactionnelle : `BEGIN`, `COMMIT`, et `ROLLBACK` sur
exception. `overwrite` remplace la table ; `append` exige une table existante. Tout autre mode
est refusé avant exécution. Les erreurs DuckDB sont propagées.

## Couche sémantique

Créer `semantic_models/semantic_catalog.yaml` :

```yaml
models:
  - key: orders
    file: orders.yaml
    dimensions: [country]
    metrics: [revenue]
```

Puis `semantic_models/orders.yaml` :

```yaml
models:
  - key: orders
    name: orders
    table: main.orders
    dimensions:
      - {name: country, sql: country, type: string}
    metrics:
      - {name: revenue, sql: amount, type: sum}
```

```python
from pathlib import Path

from skifer.semantic.evidence import EvidencePolicy
from skifer.semantic.resolver import SemanticQuery
from skifer_duckdb import DuckDBConnector, semantic_engine

with DuckDBConnector() as connector:
    connector.write_table("orders", "SELECT 'FR' AS country, 30 AS amount", "overwrite")
    engine = semantic_engine(str(Path("semantic_models").resolve()), connector)
    result = engine.query_with_evidence(
        SemanticQuery("orders", metrics=["revenue"], group_by=["country"]),
        evidence_policy=EvidencePolicy(include_sql=True),
    )
    print(result.dataframe.fetchall())
    print(result.evidence.to_dict(include_sql=True))
```

`semantic_engine` délègue à `SemanticEngine.from_backend(..., dialect="duckdb")`. Le catalogue
seul est chargé au démarrage. Utiliser un chemin absolu pour `models_dir` : le cœur résout les chemins relatifs depuis sa propre racine.
`catalog=None` conserve `main.orders` ; `catalog="memory"` donne
`memory.main.orders` en mémoire. `environment`, `config` et `certification_store` sont transmis
au cœur, qui conserve ses contrôles de certification et sa politique de divulgation des preuves.
Fournir explicitement le connecteur permet d'en gérer la fermeture ; sans argument, le helper
crée une connexion en mémoire, accessible par `engine.core._get_backend()` pour la fermer.

## Limites issues de la matrice

La seule source des statuts est `skifer.core.capability_matrix.MATRIX["duckdb"]` : 48 `Supported`,
7 `Conditional`, 2 `Refused`. Une capacité conditionnelle n'est pas une promesse générale de
parité : `report(parsed)` expose le motif, et la compilation reste possible sous ces conditions.
Les motifs ci-dessous sont ceux de la matrice, reproduits exactement.

| Capacité conditionnelle | Motif exact |
|---|---|
| `agg:approx_count_distinct` | approximate algorithms differ by engine; parity proven exact on the canonical dataset (at most 2 distinct values per group on the canonical dataset) |
| `agg:avg` | AVG over DECIMAL yields DECIMAL(p+4,s+4) on Spark and DOUBLE on DuckDB; parity proven over DOUBLE inputs only |
| `agg:first` | order-dependent without ORDER BY; parity proven only when every group holds a single non-null distinct value |
| `agg:last` | order-dependent without ORDER BY; parity proven only when every group holds a single non-null distinct value |
| `op:cast` | TRY_CAST on DuckDB: a double->int cast truncates on Spark but rounds on DuckDB, and an unconvertible value raises on Spark (ANSI) but yields NULL on DuckDB; parity proven for casts only (numeric text->double, integer->string, decimal->double) |
| `op:split` | both engines split on a regex (Spark split, DuckDB str_split_regex) and sqlglot shifts the 0-based index; parity proven for a literal separator only, Java and RE2 regex syntax diverge beyond that |
| `op:to_date` | sqlglot translates Java date tokens to strptime; parity proven for yyyy-MM-dd and dd/MM/yyyy only |

| Capacité refusée | Motif exact |
|---|---|
| `filter:sql` | raw SQL fragments are authored in Spark SQL and are not rendered for another target |
| `op:expr` | raw SQL fragments are authored in Spark SQL and are not rendered for another target |

Une capacité refusée lève `SqlCompilationError`. Le connecteur n'implémente ni règles Python,
ni `filter`/`join`/`with_column`, ni DataFrame universel, ni streaming. La publication gouvernée
`PublicationCoordinator` (staging, promotion, upsert) reste hors périmètre de ce contrat de publication.
La matrice décrit la grammaire pipeline ; elle ne constitue pas une preuve générale de parité de
tout le vocabulaire du résolveur sémantique.

## Preuves exécutées

Les tests de `skifer-duckdb` rejouent **23 cas** contre les lignes Spark DataFrame figées dans
`tests/fixtures/parity_reference.json`, avec le SHA du cœur, les données et leurs types.
La normalisation est copiée du harnais du cœur : nombres en `Decimal` quantifiés à `1e-9`, dates
ISO, colonnes et lignes triées, sans tolérance supplémentaire. Le script de régénération tourne
dans le venv Spark du cœur et réutilise `run_three_way` ; les tests du connecteur n'importent pas Spark.

Le test d'intégration reprend aussi le YAML de
[l'exemple 26 — SQL portable](https://github.com/morzu117/skifer/tree/main/examples/26_portable_sql)
et ses résultats via `execute_schema`. L'exemple du cœur reste indépendant du paquet connecteur.
Voir la [parité exécutée du cœur](../core.md#parite-executee-plan-463) pour les conditions et les
écarts mesurés.
