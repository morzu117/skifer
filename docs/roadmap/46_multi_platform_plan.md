# Plan 46 — Skifer multi-plateforme : cœur SQL-first, connecteurs, découpe

**Statut :** validé le 23 novembre 2026 (décisions §5 tranchées) — implémentation en cours.
**Branche :** `feat/plan46-multi-platform` (plan), puis une branche `feat/plan46-46.N-<sujet>` par sous-plan.
**Décideur :** Julien. Un sous-plan = une PR = un commit par point, préfixe `feat(plan46-46.N): …`.

## 1. Contexte

Le Plan 26 a recentré Skifer sur Spark/Databricks en supprimant le `Backend` Protocol à ~40 méthodes du
Plan 01 et ses trois implémentations (Snowpark, BigQuery, `sql_base`). La leçon est documentée dans
`26_databricks_refocus_plan.md` : le dispatch métier dupliqué par plateforme faisait dériver même la
plateforme principale (`between`, `not_between`, `ceil` déclarés dans `op_catalog` et absents du dispatch
Spark réel).

Depuis, Skifer a acquis sans le chercher les briques d'une portabilité **SQL-first** :

- `core/sql_compiler.compile_select` : YAML → SQL pur Python, sans Spark, avec drift-guard contre les
  dispatch Spark (24 appelants, 47 tests).
- `agentic/resolver.QueryResolver(dialect="databricks"|"duckdb")` : transpilation `sqlglot` du SQL
  sémantique.
- `SemanticEngine.from_backend(backend, …)` : runtime sémantique sur n'importe quel objet exposant
  `execute_sql` / `execute_sql_with_metadata`, utilisé par skifer-board (Plan 45).
- `candidates/promotion.py` niveau 1 : `compile_select` → `CREATE OR REPLACE VIEW` → metric view.

Le 9 novembre 2026, l'extension à d'autres plateformes est redevenue un objectif ferme. Deux sessions de
réflexion contradictoire (Claude + Codex, 7 et 9 novembre) ont fixé la cible, résumée dans
[« Skifer multi-plateforme : cible, découpe et première tranche »](https://claude.ai/artifact/FzSaj7Se3KkT1ECAzgbvbd) :

- **Architecture :** YAML → IR → compilation relationnelle commune → rendu de dialecte (`sqlglot`) →
  connecteur mince à trois contrats (exécution, catalogue, publication). Matrice de capacités
  op × cible avec statut explicite `supported` / `conditional` / `refused`. Parité **exécutée** à trois
  voies : Spark DataFrame, Spark SQL compilé, DuckDB SQL compilé.
- **Taxonomie :** le dépôt `skifer` garde le langage L (`core.constants`, `core.op_catalog`, `core.ir`,
  `core.schema_loader`, `core.sql_compiler`, `core.json_schema`, futur `skifer-core`) et la couche
  gouvernée ; les runtimes vivent dans des dépôts publics distincts, créés le 9 novembre 2026 en
  squelette : [skifer-engine](https://github.com/morzu117/skifer-engine) (Spark/Databricks),
  [skifer-duckdb](https://github.com/morzu117/skifer-duckdb), [skifer-snowflake](https://github.com/morzu117/skifer-snowflake).
- **Séquence :** DuckDB d'abord, Snowflake ensuite ; la découpe en wheels (`skifer-core`, migration du
  runtime Spark vers `skifer-engine`) est **déclenchée par le second connecteur**, pas avant.
- **Atlas / Board :** le générateur Atlas vit dans Atlas ; Skifer possède le format sémantique versionné,
  son JSON Schema, `SemanticValidator` et les règles de réagrégation ; une vue SQL nommée fait le pont
  avec les graphes virtuels Atlas.

Au 23 novembre 2026, **rien de cela n'existe dans le dépôt `skifer`** : pas d'ADR, pas de CODEOWNERS, pas
de contrat d'import, pas de contrat de connecteur, pas d'exemple portable. Ce plan borne le sujet :
sept sous-plans, chacun livrable seul, avec un état « terminé » explicite.

### Cinq interdits (hérités du Plan 26)

1. Aucun dispatch métier par plateforme : une op se compile une fois, en SQL commun, puis se **rend**
   dans un dialecte.
2. Aucun DataFrame ou Protocol universel : trois contrats étroits, pas un `Backend` large.
3. Aucune capacité implicite : ce qui n'est pas dans la matrice est `refused`, avec son motif.
4. L'égalité des clés n'est pas l'égalité de sens : chaque op portable est **exécutée** sur chaque cible
   et comparée, pas seulement présente dans un dict.
5. Aucune dégradation silencieuse : un connecteur qui ne sait pas faire lève, il ne renvoie jamais un
   résultat approximatif.

## 2. Inventaire des points de couplage (mesuré le 23 novembre 2026)

| Point | Constat | Sous-plan |
|---|---|---|
| `skifer/__init__.py` | importe `SkiferEngine`, `BuilderAgent`, lineage au chargement | 46.1 |
| `core/__init__.py` | importe le moteur et auto-enregistre les loaders | 46.1 |
| `semantic/__init__.py`, `agentic/__init__.py` | exports eager (10 et 13 imports) | 46.1 |
| `agentic/resolver.py` | importé par `semantic/planner.py`, `semantic/semantic.py`, `semantic/metric_views.py`, `services/semantic.py`, `mcp/tools.py`, `agentic/agent.py`, `agentic/data_service.py` — la couche sémantique dépend de la couche agentique | 46.1 |
| `core/spark_backend.py` | importe `candidates.store` | 46.1 |
| `core/config.py` | valide les options `knowledge_*` | 46.1 |
| `core/patterns.py` → `observability.best_effort` | dépendance core → observability | 46.1 |
| `import skifer.core.op_catalog` | charge 87 modules Skifer (151 ms, sans pyspark) | 46.1 (cible : < 15) |
| `compile_select` | dialecte Spark figé (backticks, `_SPARK_*_DISPATCH` comme référence) | 46.2 |
| `QueryResolver.dialect` | seul point de rendu de dialecte, côté agentique | 46.2 |
| `from_backend` | contrat d'exécution implicite (duck typing sur deux noms) | 46.2 |
| `schemas/` | seul `skifer-pipeline.schema.json` ; pas de schema du modèle sémantique | 46.5 |
| `.github/` | pas de `CODEOWNERS`, pas de contrat d'import en CI | 46.0 |
| `pyproject.toml` | `duckdb>=1,<2` déjà en extra `dev` ; `sqlglot>=25` en `[analyzer]` | 46.2, 46.3 |

## 3. Sous-plans

Les estimations sont **mon temps d'exécution** (exploration, code, tests, doc, gate complet), hors
temps de relecture et de décision côté Julien. Chaque sous-plan se termine par le gate complet :

```bash
pytest tests/ -x --tb=short && ruff check src/      # python
mkdocs build --strict                               # docs
lint-imports                                        # à partir de 46.0
```

### 46.0 — ADR, CODEOWNERS, contrat d'import de référence (S, 1 à 2 h)

**But.** Figer la décision et poser l'outil qui empêchera la couche gouvernée de reglisser dans le cœur.

**Créer.**
- `docs/adr/0001-sql-first-multi-platform.md` : décision, alternatives écartées (Protocol universel,
  dispatch par plateforme, monorepo), conséquences, les cinq interdits.
- `.github/CODEOWNERS` : Julien seul sur `src/skifer/core/`, `src/skifer/semantic/`,
  `src/skifer/agentic/resolver.py` (jusqu'à son déplacement), `schemas/`, `.github/`, `pyproject.toml` ;
  Julien + Pierre sur `docs/integrations/atlas/` et `examples/atlas/` (handle GitHub de Pierre à fournir).
- Section `[tool.importlinter]` dans `pyproject.toml` avec **un seul** contrat `forbidden` qui passe
  déjà : `skifer.core.op_catalog`, `skifer.core.constants`, `skifer.core.ir` n'importent pas
  `skifer.semantic`, `skifer.agentic`, `skifer.observability`, `pyspark`. `import-linter` en extra `dev`,
  étape `lint-imports` dans `.github/workflows/tests.yml`.

**Modifier.** `CLAUDE.md` / `AGENTS.md` (ligne du Plan 46 dans la table des plans), `CHANGELOG.md`.

**Terminé quand.** CI verte avec l'étape `lint-imports` ; ADR commité ; CODEOWNERS actif.

### 46.1 — Frontières d'import : cœur importable à vide (M, 4 à 6 h)

**But.** `import skifer.core.sql_compiler` charge le langage L et rien d'autre. C'est le prérequis
mesurable de la découpe : sans lui, `skifer-core` est un vœu.

**Modifier.**
- `src/skifer/__init__.py`, `core/__init__.py`, `semantic/__init__.py`, `agentic/__init__.py` : exports
  paresseux via `__getattr__` (PEP 562), `__all__` inchangé — l'API publique ne bouge pas.
- Déplacer `agentic/resolver.py` → `semantic/resolver.py` ; `agentic/resolver.py` devient un module de
  ré-export avec `DeprecationWarning`. Mettre à jour les sept importeurs.
- `core/spark_backend.py` : import de `candidates.store` déplacé dans la fonction qui l'utilise.
- `core/config.py` : la validation `knowledge_*` migre vers `knowledge/config.py`, appelée par
  `ConfigurationManager` paresseusement.
- `core/patterns.py` : `best_effort` déplacé sous `core/` (ou inversé), de sorte que `core` ne dépende
  plus d'`observability`.

**Créer.**
- Contrats `importlinter` : `layers` (`core` < `observability` < `semantic` < `agentic` < `services` <
  `api`/`mcp`/`serving`) et `independence` (`candidates`, `knowledge`, `adaptive`, `capabilities`).
- `tests/test_import_boundaries.py` : sous-processus `python -c "import skifer.core.op_catalog"` ;
  assertion sur `sys.modules` (aucun `skifer.semantic*`, `skifer.agentic*`, `pyspark*`) et sur le
  nombre de modules Skifer chargés (plafond 15, valeur mesurée épinglée).

**Risque.** Import cycles masqués par l'ordre d'import actuel. Mitigation : un contrat à la fois, gate
complet entre chaque, `examples/` exécutés par la suite servent de test d'intégration.

**Terminé quand.** Trois contrats `importlinter` verts, test de frontière vert, suite complète verte,
aucun changement d'API publique.

**Réalisé le 24 novembre 2026** (branche `feat/plan46-46.1-import-boundaries`, dev délégué à Codex,
deux passes de review `claude -p` sans outils). Mesure : `import skifer.core.op_catalog` charge 3 modules
Skifer au lieu de 87 ; les 8 modules du langage L chargent entre 3 et 6 modules, épinglés par test en
interpréteur frais. Écarts par rapport au texte ci-dessus, assumés :

- `core/spark_backend.py` et `core/config.py` n'ont pas bougé : leurs imports vers `candidates`,
  `knowledge` et `knowledge.embeddings` étaient déjà locaux aux fonctions. `core/patterns.py` →
  `observability.best_effort` est resté : `core.core`, `patterns`, `spark_backend` sont le **runtime**
  (futur `skifer-engine`), pas le langage L, et un contrat `layers` sur tout `core` était la mauvaise
  forme. Les contrats portent donc sur la liste explicite des 8 modules de L, plus `semantic` <
  `agentic` et la paresse des quatre racines.
- `agentic/resolver.py` n'est pas un module de ré-export mais un **alias `sys.modules`** du module
  réel après `DeprecationWarning` : identité des objets, noms privés, `monkeypatch` et logger
  préservés.
- Décision de conception : `exclude_type_checking_imports = true`. Les blocs `if TYPE_CHECKING:` des
  racines donnent la visibilité statique des ré-exports et créent des arêtes grimp qui casseraient le
  contrat des racines. Les contrats gardent la structure runtime ; la preuve d'exécution est
  `tests/test_import_boundaries.py`. **Mesuré avec l'option désactivée** : seul le contrat des racines
  casse, et uniquement par ces blocs ; aucune arête de typage n'est masquée sur les contrats du
  langage L et de `semantic`/`agentic`. Un test AST interdit toute dérive entre les blocs
  `TYPE_CHECKING` et `_EXPORTS`.
- `RuleRegistry.get_loader()` / `list_loaders()` importent `skifer.core.loaders` au premier appel : un
  `import skifer` nu ne charge plus le moteur sans changer ce que le registre répond.
- Suivi différé (pas dans 46.1) : importer chaque module `skifer.*` dans un interpréteur frais pour
  détecter un cycle masqué par l'ancien chargement eager.

### 46.2 — Contrats de connecteur, matrice de capacités, rendu de dialecte (M, 6 à 8 h)

**But.** Donner au cœur ce que les trois dépôts de runtime implémenteront, sans qu'aucun ne soit encore
écrit. Tout est pur Python, testé sans Spark ni DuckDB.

**Créer.**
- `core/connector.py` : trois `Protocol` courts.
  `ExecutionContract.execute_sql_with_metadata(sql) -> ExecutionResult` (réutilise
  `semantic.evidence.ExecutionResult`, déplacé dans `core/` avec ré-export) ;
  `CatalogContract.table_exists / columns / resolve_fqn` ;
  `PublicationContract.create_or_replace_view / write_table`.
  Plus `ConnectorCapabilities` : dialecte, ops supportées, écritures supportées.
- `core/dialect.py` : `render(sql_spark: str, target: str) -> str` via `sqlglot.transpile(read="spark",
  write=target)` ; cibles fermées `databricks`, `duckdb`, `snowflake` ; `sqlglot` importé paresseusement,
  erreur explicite s'il manque (extra `[dialects]` = `sqlglot>=25`, partagé avec `[analyzer]`).
- `core/capability_matrix.py` : pour chaque op de `op_catalog` (filtres, ops de colonne, agrégats,
  types de jointure) × cible, un statut `supported` / `conditional(reason)` / `refused(reason)`.
  Drift-guard : l'ensemble des clés égale celui d'`op_catalog` — une op ajoutée sans ligne de matrice
  casse le test.
- `tests/test_connector_contracts.py`, `tests/test_dialect.py`, `tests/test_capability_matrix.py`.

**Modifier.**
- `compile_select(parsed, resolve_table=None, allow_raw_sql=True, *, dialect="databricks")` : compile en
  Spark SQL comme aujourd'hui puis rend via `core/dialect.py` ; `dialect="databricks"` est un no-op
  byte-for-byte (épinglé par test sur les 47 cas existants). Avant rendu, `capability_matrix` est
  consultée : une op `refused` pour la cible lève `SqlCompilationError` avec le motif.
- `QueryResolver` : délègue son rendu à `core/dialect.py` (une seule implémentation).
- `SemanticEngine.from_backend` : accepte un `ExecutionContract` ; le duck typing actuel reste accepté
  avec un `DeprecationWarning`.
- `docs/core.md` : section « Portabilité SQL ».

**Terminé quand.** `compile_select(..., dialect="duckdb")` produit du SQL DuckDB valide pour tous les
schémas d'`examples/08_materialized_view` et `examples/25_metric_candidates` (vérifié par
`duckdb.sql(...).explain()` en test, `duckdb` étant déjà en `dev`) ; matrice complète ; résolveur
sans rendu propre.

**Réalisé le 24 novembre 2026** (branche `feat/plan46-46.2-connector-contracts`, six commits Codex +
un correctif de review, gate 4269 tests). Écarts et décisions, assumés :

- Les schémas de l'exemple 25 ne sont **pas** compilables (règles Python, sources fichier) : la preuve
  « SQL valide » porte sur l'exemple 08 et sur des fixtures couvrant **chaque** opérateur de filtre,
  op de colonne, agrégat et type de jointure, exécutées sur DuckDB en mémoire (57 tests, colonnes
  vérifiées, pas de valeurs : c'est 46.3).
- La matrice compte 57 clés (19 filtres, 19 ops, 12 agrégats, 7 jointures), énumérées à la main et
  gardées par drift-guard. Databricks : tout `Supported`. DuckDB et Snowflake : `Conditional("parity
  not executed yet (Plan 46.3)")` par défaut — rien n'est `Supported` avant la parité exécutée.
  `filter:sql` et `op:expr` sont **`Refused` hors Databricks** : un fragment brut est écrit en Spark
  SQL et serait relu comme tel par le rendu — le laisser passer était une dégradation silencieuse.
  Snowflake refuse aussi `first`/`last` et `left_semi`/`left_anti`.
- `compile_select(dialect=)` est keyword-only ; `databricks` est un no-op octet pour octet, épinglé
  sur tous les YAML du fichier de test. `_reject_uncompilable` passe d'abord, puis
  `capability_report(parsed, dialect)` ; tous les `Refused` sont levés en **une** erreur. Les
  `Conditional` ne journalisent rien : le rapport est leur canal.
- `render()` : `unsupported_level=RAISE` (sqlglot n'approxime jamais en silence) ; tokenizer
  identifiants/littéraux pour la normalisation des `''` — la version héritée du résolveur cassait le
  littéral vide et interceptait une exception inexistante dans sqlglot 30.
- `from_backend` accepte `ExecutionContract` **ou** l'ancien `execute_sql`, **sans** avertissement :
  skifer-board l'utilise en production.
- `ExecutionResult` vit dans `core/connector.py`, ré-exporté par `semantic/evidence.py`.
- Reporté, noté ici : `PublicationContract` est trop étroit pour `PublicationCoordinator` (staging,
  promotion, upsert) → à élargir en 46.4 quand un connecteur réel l'implémente ; le chemin sémantique
  du résolveur rend un dialecte sans consulter la matrice (le SQL sémantique n'est pas la grammaire
  pipeline) → 46.3 décide s'il faut une matrice sémantique propre.

### 46.3 — Harnais de parité à trois voies et exemple portable (M, 5 à 7 h)

**But.** Prouver l'interdit n° 4 : une op est portable quand son **résultat exécuté** est identique sur
Spark DataFrame, Spark SQL compilé et DuckDB SQL compilé.

**Créer.**
- `tests/parity/` : `conftest.py` (fixture `spark` existante + connexion DuckDB en mémoire), jeu de
  données canonique (3 tables, ~50 lignes, types date/decimal/string/null), `test_three_way_parity.py`
  paramétré sur chaque op `supported` de la matrice pour `duckdb`. Comparaison après tri et
  normalisation des types (decimal → `Decimal`, date → ISO). Une op `conditional` est testée avec sa
  condition ; une op `refused` est testée comme levant.
- `examples/26_portable_sql/` : `README.md`, `orders_by_country.yaml`, `run.py` qui compile le même
  schéma en Databricks SQL et DuckDB SQL, exécute la version DuckDB sur un parquet local et affiche le
  SQL rendu pour les deux cibles. Exécuté par `tests/test_examples.py`.

**Modifier.** `docs/core.md`, `examples/README.md`, `mkdocs.yml`, `CHANGELOG.md`.

**Risque.** Divergences numériques légitimes (arrondi `round` demi-pair vs demi-supérieur, `split` sur
regex Spark vs littéral DuckDB). Elles ne sont **pas** masquées : chacune devient une ligne
`conditional` de la matrice avec son motif, ou `refused`.

**Terminé quand.** Parité verte sur toutes les ops `supported` ; chaque écart trouvé a sa ligne de
matrice ; exemple exécuté par la suite.

**Réalisé le 24 novembre 2026** (branche `feat/plan46-46.3-parity`, quatre commits Codex + trois
correctifs issus de trois passes de review sans outil, gate 4307 tests). Écarts et décisions, assumés :

- **Le harnais a trouvé un vrai défaut produit avant d'être fini.** Sur `type: cross`, le chemin
  DataFrame appliquait les clés déclarées (24 lignes) là où le SQL compilé rendait le produit
  cartésien (360). Décision en deux temps : le SQL est la référence, le DataFrame est aligné ; puis,
  à la review, des clés sur un `cross` sont **refusées au chargement** (un produit cartésien n'a pas
  de condition d'égalité) — forme sans clé `table_from: ord` / `table_to: cust`. Le mapper de
  l'analyzer émettait des clés factices sur un `CROSS JOIN` : il émet désormais la forme sans clé,
  sans quoi ses propres drafts auraient été refusés.
- **Matrice DuckDB : 48 `Supported`, 7 `Conditional`, 2 `Refused`.** Le défaut reste `Conditional` ;
  une clé n'est `Supported` que si elle figure dans la liste explicite `_DUCKDB_PROVEN`, gardée par un
  test : exercée par au moins un cas vert et jamais citée dans la condition d'un cas. La première
  version promouvait par défaut tout ce qui n'était pas listé — une clé ajoutée plus tard aurait été
  `Supported` sans preuve (interdit n° 3).
- **23 cas à trois voies + 3 pins de divergence**, 246 tests sur session Spark réelle. Les sept
  `Conditional` portent un écart **mesuré**, pas supposé : `cast` (prouvé pour texte numérique →
  double, entier → string, decimal → double ; ailleurs troncature contre arrondi, et un cast
  invalide lève sous ANSI côté Spark là où le pin DuckDB observe `NULL`), `avg` (type de retour sur
  `DECIMAL`), `first`/`last` (non déterministes sans ordre, sur des groupes multi-lignes),
  `split`, `to_date`, et `approx_count_distinct` — exact sur le jeu canonique, mais seulement parce
  qu'un groupe n'y porte jamais plus de deux valeurs distinctes : le motif le dit plutôt que de
  promouvoir.
- La comparaison est **exacte** : la tolérance numérique et les colonnes « approx » de la première
  version ne servaient à aucun cas et ont été supprimées ; un cas dont le résultat est vide échoue.
  La fixture retire ses vues temporaires et vérifie que la session est en mode ANSI.
- L'exemple 26 **asserte** ses lignes, ses totaux et le refus d'`op:expr` sur DuckDB ; son README
  reproduit la sortie réelle.
- Review : retenus — défaut `Supported` implicite, fuite de vues temporaires, exemple non asserté,
  condition de `cast` non épinglée, tolérance morte, clés d'un `cross` seulement averties, motifs
  trop affirmatifs. Déclinés, avec raison : changer la règle `exercised` (une clé citée dans une
  condition ne doit pas être promue par un autre cas), l'import privé du fabricant de moteur et les
  imports `pyspark`/`duckdb` au niveau module, cohérents avec les tests existants (`duckdb` est dans
  `[dev]`).
- Piège appris : une erreur ANSI de Spark crée un `PySparkLogger` non propagé ; pytest y attache son
  handler de capture, dont `addHandler` remplace le formateur par du JSON — un test de tracing sans
  rapport échouait **après** le nouveau pin, selon l'ordre. Corrigé dans le pin seul (registre de
  loggers restauré) ; ni le test de tracing ni le code de production n'ont bougé.
- Reporté, noté ici : la parité du vocabulaire SQL du **résolveur sémantique** (pas la grammaire
  pipeline) → 46.4, où un connecteur réel l'exécute ; les valeurs de filtre rendues en littéraux de
  chaîne (`HAVING nb_orders > '1'`, comportement antérieur du compilateur, toléré par coercition
  implicite sur les deux moteurs) → à trancher avec 46.4 ; Snowflake reste entièrement
  `Conditional`/`Refused` jusqu'à 46.6 ; l'enum de jointure du JSON Schema ignore
  `left_semi`/`left_anti` (antérieur à ce plan).

### 46.4 — Connecteur DuckDB v0.1 dans `skifer-duckdb` (M, 4 à 6 h)

**But.** Premier runtime hors Spark, implémentant les trois contrats de 46.2 contre `skifer>=4.2`.

**Créer (dépôt `skifer-duckdb`).**
- `src/skifer_duckdb/connector.py` : `DuckDBConnector(path=":memory:")` implémentant
  `ExecutionContract`, `CatalogContract`, `PublicationContract` ; `capabilities` dérivées de la matrice
  du cœur, jamais redéclarées localement.
- `src/skifer_duckdb/semantic.py` : `semantic_engine(models_dir, **kw)` = `SemanticEngine.from_backend`
  avec `dialect="duckdb"`.
- Tests : contrats, parité **à deux voies** contre le SQL de référence Spark figé en fixture (le dépôt
  n'embarque pas Spark), publication de vue, modèle sémantique de bout en bout.
- CI : matrice Python existante + installation de `skifer` depuis PyPI à la version minimale déclarée.

**Modifier (dépôt `skifer`).** `docs/integrations/duckdb.md` (installation, limites issues de la
matrice, lien vers l'exemple 26). Aucune dépendance de `skifer` vers `skifer-duckdb`.

**Terminé quand.** `pip install skifer-duckdb` puis `examples/26_portable_sql` tourne en remplaçant
`duckdb.sql` par le connecteur ; skifer-board peut remplacer son backend ad hoc par `DuckDBConnector`.

**Réalisé le 25 novembre 2026** (dépôt `skifer-duckdb`, branche `feat/plan46-46.4-connector`, deux
commits Codex + trois correctifs de review, 79 tests ; côté cœur, branche
`feat/plan46-46.4-duckdb-connector`, page d'intégration seule, gate 4307 tests). Rien n'est poussé.
Écarts et décisions, assumés :

- **DuckDB 2.x n'a pas la parité de 1.5.** Le venv neuf du connecteur a pris 2.0.1 et deux cas sont
  tombés : `round(CAST(2.675 AS DOUBLE), 2)` y rend 2.67, contre 2.68 sur Spark et sur DuckDB 1.5.6
  (reproduit à part). Le connecteur déclare `duckdb>=1.5.6,<2` ; la preuve de `op:round` dans la
  matrice ne vaut donc que pour DuckDB 1.x. La référence figée enregistre les versions qui l'ont
  produite et un test échoue si le majeur installé diffère.
- **La parité est rejouée sans Spark** : les 23 cas du harnais de 46.3 sont figés en JSON par
  `scripts/freeze_reference.py` (après égalité des trois voies) et rejoués par le connecteur. Les
  types et l'ordre des colonnes DuckDB sont épinglés contre la référence, mais **non** comparés aux
  types Spark ; les lignes sont comparées comme multi-ensemble, les nombres après quantification à
  1e-9 — c'est l'oracle du cœur, recopié à l'identique, et la doc le dit au lieu d'annoncer « sans
  tolérance ».
- **L'append était positionnel** (trouvé en review, vérifié : un `2.7` s'insérait comme `3` dans une
  colonne entière). Il est désormais par nom, à schéma exact : colonnes manquantes, en trop, dupliquées
  ou de type différent sont refusées avant toute écriture. Pas de `BY NAME`, qui comblerait en `NULL`.
- Un `select_sql` doit être **un seul SELECT** ; le catalogue est insensible à la casse comme DuckDB ;
  `columns()` d'une relation absente lève au lieu de rendre `[]` ; l'append vise une table de base.
- **Transactions** : une écriture ouvre la sienne, ou **rejoint** celle de l'appelant sans la
  commiter. Un second `BEGIN` ne permet pas de la détecter — il invalide la transaction en cours ;
  le connecteur compare deux lectures de `current_transaction_id()` (stable dans une transaction,
  croissant en autocommit).
- `DuckDBConnector(connection=…)` accepte une connexion existante, jamais fermée par le connecteur :
  c'est ce qui rend le remplacement du backend ad hoc de Board possible. Ce remplacement n'a **pas**
  été essayé dans skifer-board.
- Gate sémantique testé avec le vrai `SqliteCertificationStore` : refus `MISSING`, puis exécution
  une fois le dataset certifié.
- Déclinés, documentés dans le README du connecteur : matérialiser les résultats (une relation
  paresseuse se comporte comme un DataFrame Spark — une erreur d'exécution peut surgir à la lecture),
  verrou multi-thread (un connecteur par thread), `overwrite` qui ne conserve ni contraintes ni index.
- Reporté, noté ici : le cœur est encore estampillé 4.1.0 alors que le connecteur exige
  `skifer>=4.2.0` — installation locale en `--no-deps`, et **la CI du connecteur restera rouge tant
  que le Plan 46 n'est pas sur `main`** (elle installe le cœur depuis `main`, à repasser sur PyPI à
  la 4.2) ; le support de DuckDB 2.x (rendre `op:round` conditionnel ou compiler un arrondi décimal)
  est une décision du cœur ; `PublicationContract` reste étroit (pas de staging ni d'upsert) ; la
  parité du vocabulaire SQL du résolveur sémantique n'est prouvée que sur un cas de bout en bout
  (`SUM`/`GROUP BY`) ; les valeurs de filtre rendues en littéraux de chaîne restent inchangées.

### 46.5 — Contrat Atlas : JSON Schema sémantique, fixture, recette de vue nommée (M, 4 à 6 h)

**But.** Donner à Pierre une frontière stable sans accès en écriture au cœur : Atlas génère, Skifer
valide et possède le format.

**Créer.**
- `schemas/skifer-semantic-model.schema.json` (versionné `$id` … `/v1`) : dérivé de
  `SemanticValidator` ; test d'équivalence : tout YAML accepté par le validateur passe le schema et
  réciproquement sur un corpus de 20 modèles (positifs et négatifs).
- `tests/fixtures/atlas/` : trois modèles sémantiques tels qu'Atlas les produit (fournis par Pierre ou
  reconstruits depuis skifer-atlas), un pipeline YAML de pont, et `test_atlas_contract.py` qui les
  valide, les projette par `OutputProjector`, et exécute la vue nommée sur DuckDB.
- `docs/integrations/atlas/README.md` : recette « graphe virtuel Atlas → vue SQL nommée → modèle
  sémantique Skifer → certification », règles de réagrégation, qui possède quoi, processus de PR.
- `skifer semantic validate --schema` : validation contre le JSON Schema sans Spark.

**Dépend de.** Décision n° 1 (§5) : l'arbitrage est dans Atlas ; la recette le dit.

**Terminé quand.** Atlas peut épingler `skifer>=4.2` au lieu de `v4.0.0` et valider ses modèles en CI
avec `skifer semantic validate --schema` ; la fixture est verte des deux côtés.

**Réalisé le 25 novembre 2026** (branche `feat/plan46-46.5-atlas-contract`, cinq commits Codex + trois
correctifs de review, gate 4476 tests). **M1 est atteint côté cœur seulement** : rien n'a été fait
dans Atlas. Écarts et décisions, assumés :

- **La fixture est reconstruite, pas produite par Atlas.** Atlas n'émet aujourd'hui aucun modèle
  sémantique Skifer, seulement des documents KPI (graphes). `tests/fixtures/atlas/` montre la forme
  que le pont *devrait* produire : une copie gelée du KPI `revenu-net`, deux vues nommées, trois
  modèles (`revenu_net`, référentiel `entites`, `effectifs` semi-additif), un pipeline de pont. Le
  test lie la sortie du nœud KPI aux colonnes réelles de la vue DuckDB, à la projection du pont et
  aux membres du modèle, et asserte des valeurs. « Verte des deux côtés » n'est donc **pas** tenu :
  elle n'est verte que sur le cœur.
- **Le JSON Schema est nécessaire, pas suffisant** — l'« équivalence » ci-dessus est abandonnée.
  Le validateur porte des contrôles croisés (doublons, références de grain et de relation, `sql`
  déjà agrégé) qu'un schéma n'exprime pas. À la place : *aucun faux rejet*, prouvé par un test
  différentiel (2426 mutations des positifs, 1101 acceptées par le validateur, toutes acceptées par
  le schéma — il a trouvé `key: null`, que le schéma refusait à tort) ; et un corpus de 44 documents
  où chaque négatif épingle son erreur et dit si le schéma seul l'attrape (19) ou non (8). Là où le
  validateur n'émet qu'un warning (type de dimension inconnu, clé inconnue), le schéma accepte.
- **`skifer semantic validate --schema MODEL…` rend le verdict complet** : schéma par fichier puis
  validateur sur **l'ensemble** des fichiers — la première version validait fichier par fichier et
  laissait passer une relation cassée vers un autre fichier. Clé dupliquée entre fichiers = erreur ;
  cible non fournie = warning explicite ; warnings du validateur affichés. Extra `[schema]`.
  L'usage `PIPELINE MODEL` est inchangé à l'octet.
- **Le validateur a été durci, hors plan** : il appliquait le motif d'identifiant à `str(name)`,
  donc `name: no` (booléen YAML) devenait la dimension « False ». Noms, `sql`, clés d'entités,
  grain et extrémités de relation doivent être des chaînes ; une forme inattendue est une erreur
  nommée, plus un plantage ; les identifiants passent par `fullmatch` (`"abc\n"` passait).
  **Changement de comportement assumé** : `sql: 1` doit s'écrire `sql: "1"`. `key: null` reste
  accepté. Aucun modèle du dépôt n'a changé de verdict.
- Version du schéma : `v1` tant qu'il ne fait que s'élargir, tout resserrement = `v2`. `$id` est un
  identifiant, rien n'est servi à cette URL.
- Reporté, noté ici : le générateur côté Atlas et sa CI ; `skifer>=4.2` n'est pas publié (la
  recette donne l'installation depuis git) ; **`QueryResolver` refuse tout filtre dont la valeur
  contient un tiret** — un `eq` sur une date ISO est rejeté avant SQL, donc une métrique
  semi-additive ne peut pas être épinglée par égalité sur une dimension date (antérieur à ce plan,
  épinglé par un test, à corriger dans le résolveur) ; trois modèles des exemples 03 et 25 sont
  refusés par le validateur faute de `name` (antérieur, verdict épinglé) ; le handle de Pierre dans
  CODEOWNERS.

### 46.6 — Connecteur Snowflake v0.1 dans `skifer-snowflake` (L, 8 à 12 h)

**But.** Second connecteur : il valide que les contrats de 46.2 ne sont pas « DuckDB-shaped » et
déclenche la découpe de 46.7.

**Créer (dépôt `skifer-snowflake`).** `SnowflakeConnector` sur `snowflake-connector-python`
(authentification par paramètres injectés, jamais lus dans un fichier de config Skifer), rendu
`dialect="snowflake"`, tests sur un **faux curseur** (aucun compte requis en CI), un test d'intégration
optionnel activé par variable d'environnement. Colonne de matrice `snowflake` ajoutée dans le cœur
(46.2) avec parité à deux voies contre le SQL de référence.

**Risque.** Guillemets et casse des identifiants (Snowflake replie en majuscules sans guillemets). La
règle : le rendu cite **toujours** les identifiants ; c'est une ligne `conditional` de la matrice, pas
une option.

**Terminé quand.** Suite verte sur faux curseur ; au moins un run d'intégration réel documenté dans le
README du dépôt.

**Réalisé le 25 novembre 2026 — sur faux curseur uniquement ; le critère « run d'intégration réel »
n'est PAS satisfait** (dépôt `skifer-snowflake`, branche `feat/plan46-46.6-connector`, deux commits
Codex `a7c5dde`, `d52e193` + deux de correction après review `01875d9`, `3b22868` ; cœur, branche
`feat/plan46-46.6-snowflake-connector` : `1d15671`, `5049a31`, `f30c4b8` ; gate cœur 4486 verts,
connecteur 125 verts et 32 sautés ; rien n'est poussé). Écarts et décisions, assumés :

- **Aucun compte Snowflake : rien n'a été exécuté.** La matrice reste à 0 `Supported`, 51
  `Conditional`, 6 `Refused` ; aucune capacité n'est promue. Le test d'intégration (20 cas de parité
  + 12 scénarios dans un schéma jetable) est écrit et **n'a jamais tourné** : chaque scénario nomme
  l'hypothèse du README qu'il lèverait. Ce qui est prouvé se limite au SQL émis et à la séquence
  d'appels DB-API.
- **Connexion injectée**, jamais configurée ni fermée par le connecteur ; identifiants de cible
  toujours cités à casse exacte ; catalogue par `INFORMATION_SCHEMA` à paramètres liés.
- **Changement du cœur non prévu par le plan** : le rendu Snowflake cite désormais tous les
  identifiants (`identify=True`, `1d15671`). Sans cela le chemin sémantique émettait du SQL non cité
  que Snowflake replie en majuscules, vers d'autres objets que ceux cités par le connecteur.
  Conséquence épinglée et documentée, non corrigée : la casse doit être cohérente dans tout le YAML
  (`AS Total … ORDER BY total` donne `"Total"` et `"total"`, deux identifiants distincts) ;
  `localtimestamp` sans parenthèses devient une colonne.
- **SQL figé** : 20 cas compilent, 3 sont refusés (`first`/`last`, `left_semi`, `left_anti`). Le
  golden fige le rendu du cœur ; `REGEXP_SPLIT(...)[0]` et les formats `TRY_TO_DATE` sont ce que
  sqlglot produit, sans preuve que Snowflake les accepte.
- **Review sans outil, deux bloquants corrigés** : la publication ne réécrit plus le SQL de
  l'appelant (elle ajoutait des guillemets, donc changeait l'objet visé) ; les trois chemins
  d'écriture refusent une transaction ouverte par l'appelant au lieu de la commiter par DDL. Aussi :
  plus de `statement_id` fabriqué (`None` sans `sfqid`), faux curseur qui exige la fermeture de
  chaque curseur interne. Réfuté : l'injection par nom de table (le cœur cite, épinglé par un test).
- **Limites connues** : l'append compare les noms de colonnes, pas les types ; après un `COMMIT` en
  erreur réseau l'issue est inconnue et une relance peut dupliquer ; sqlglot 30.35 côté connecteur,
  30.33 côté cœur.
- Reporté, noté ici : run réel et promotion de capacités ; contrôle de
  `QUOTED_IDENTIFIERS_IGNORE_CASE` ; normalisation de `ColumnInfo.type` ; CI du connecteur rouge
  tant que le cœur 4.2 n'est pas publié. **46.7 exige 46.6 « vert » : à l'utilisateur de décider si
  la découpe démarre sans run réel.**

### 46.7 — Découpe : wheel `skifer-core`, migration du runtime Spark vers `skifer-engine` (L, 12 à 16 h)

**But.** Le changement de distribution, exécuté seulement une fois les deux connecteurs livrés.

**Étapes.**
1. `pyproject.toml` du dépôt `skifer` publie **deux** distributions depuis le même dépôt :
   `skifer-core` (langage L + contrats + matrice + dialecte) et `skifer` (couche gouvernée, dépend de
   `skifer-core==X.Y.Z`). Un seul `src/`, deux `[project]` via un `pyproject` par distribution sous
   `packaging/`. `scripts/release.py` et `release.yml` adaptés.
2. `core/spark_backend.py`, `core/writer.py`, `core/loaders.py`, `spark_factory.py`, matérialisations
   streaming/MV migrent vers `skifer-engine` (`skifer_engine.spark`), avec des shims d'import dans
   `skifer.core` émettant un `DeprecationWarning` pendant une version mineure.
3. `SkiferEngine` reste dans `skifer` et reçoit son runtime par injection (`SkiferEngine(connector=…)`),
   `get_spark_session()` devenant le défaut fourni par `skifer-engine` quand il est installé.
4. Les ~182 tests Spark migrent vers `skifer-engine` ; `skifer` garde les ~2200 tests sans Spark plus un
   job CI qui installe `skifer-engine[spark]` et rejoue les exemples.

**Dépend de.** Décision n° 2 (§5) : « non », donc l'étape 3 se limite à l'injection du connecteur et
aucun pipeline à règles Python n'est promis hors Spark.

**Risque.** Le plus élevé du plan : rupture d'import pour les utilisateurs, double publication PyPI,
`examples/` à rejouer dans deux dépôts. Mitigation : shims pendant une mineure, essai à blanc du
workflow de release, et **aucun** début avant 46.6 vert.

**Terminé quand.** `pip install skifer` sans extra n'installe ni pyspark ni delta ; `pip install
skifer-engine[spark]` rejoue les 25 exemples ; CHANGELOG porte la note de migration.

## 4. Séquencement, dépendances, estimation

```text
46.0 ──▶ 46.1 ──▶ 46.2 ──▶ 46.3 ──▶ 46.4 ──┐
                     │                        ├──▶ 46.6 ──▶ 46.7
                     └──▶ 46.5 ───────────────┘
```

| Sous-plan | Taille | Mon temps d'exécution | Cumul | Jalon |
|---|---|---|---|---|
| 46.0 ADR, CODEOWNERS, import-linter | S | 1 à 2 h | 2 h | |
| 46.1 Frontières d'import | M | 4 à 6 h | 8 h | |
| 46.2 Contrats, matrice, dialecte | M | 6 à 8 h | 16 h | |
| 46.3 Parité à trois voies, exemple 26 | M | 5 à 7 h | 23 h | |
| 46.4 Connecteur DuckDB v0.1 | M | 4 à 6 h | 29 h | **M1 — Board et Atlas servis** |
| 46.5 Contrat Atlas, JSON Schema | M | 4 à 6 h | 35 h | **M1** |
| 46.6 Connecteur Snowflake v0.1 | L | 8 à 12 h | 47 h | |
| 46.7 Découpe des wheels | L | 12 à 16 h | 63 h | **M2 — distribution cible** |

Fourchette totale : **44 à 63 heures d'exécution**, en huit PR. En pratique une PR par séance de
travail pour 46.0 à 46.5, deux à trois séances pour 46.6 et 46.7, chacune suivie de ta relecture.

**Le sujet a une fin.** M1 (46.0 à 46.5, 24 à 35 h) suffit à ce que skifer-board et skifer-atlas
consomment une frontière stable et qu'un utilisateur exécute un pipeline SQL-only sur DuckDB. Si M2
est reporté, rien de M1 n'est à défaire : les contrats et la matrice sont les mêmes avec ou sans
découpe en wheels. Hors périmètre de ce plan, explicitement : BigQuery, Fabric, règles Python
portables (`sql_projection`), orchestration multi-cible.

## 5. Décisions tranchées par Julien le 23 novembre 2026

1. **Divergence de sens d'un KPI entre Atlas et Skifer : arbitrée dans Atlas.** Atlas est le lieu de
   la décision métier ; Skifer ne tranche pas, il **valide** (schema, projection, certification) et
   refuse ce qu'il ne peut pas prouver. Conséquence pour 46.5 : la recette dit explicitement qu'un
   désaccord se règle côté Atlas puis se repropage ; Skifer n'offre aucun mécanisme d'override du sens.
2. **Un pipeline à règles Python ne tourne pas hors Spark.** Recommandation suivie. Hors Spark, Skifer
   exécute du SQL compilé uniquement ; les règles Python restent un atout de `skifer-engine`. Le Plan 26
   n'est pas rouvert, aucun kind `sql_projection` dans ce plan ; l'étape 3 de 46.7 se limite à
   l'injection du connecteur.
3. **CODEOWNERS : on avance tel quel.** Julien seul pour l'instant ; la ligne Pierre sur
   `docs/integrations/atlas/` et `examples/atlas/` sera ajoutée quand son handle sera fourni.

## 6. Stratégie de vérification

- Gate complet à chaque commit : `pytest tests/ -x --tb=short && ruff check src/`, `lint-imports` dès
  46.0, `mkdocs build --strict` quand `docs/` bouge.
- Non-régression byte-for-byte : `compile_select(dialect="databricks")` sur les 47 cas existants et le
  SQL des exemples 08 et 25 figé en fixture avant 46.2.
- Parité exécutée (46.3) comme oracle de toute ligne `supported` de la matrice ; une op ne passe à
  `supported` qu'avec son test de parité.
- Frontières d'import vérifiées en sous-processus, jamais dans le processus de test (dont `sys.modules`
  est déjà pollué).
- Dépôts de runtime : CI contre la version minimale de `skifer` déclarée, pas contre `main`.
- `examples/` exécutés par la suite ; l'exemple 26 est le test d'acceptation de M1.

## 7. Risques transverses

| Risque | Effet | Mitigation |
|---|---|---|
| Reglissement vers un dispatch par plateforme sous pression d'une op manquante | Retour au Plan 01 | Interdit n° 1 dans l'ADR ; la matrice n'a que trois statuts et `refused` est acceptable |
| `sqlglot` rend un SQL valide mais sémantiquement différent | Faux `supported` | Parité exécutée obligatoire avant tout `supported` |
| Atlas avance plus vite que la frontière | Pierre contourne et dépend d'internes | 46.5 avant 46.6 ; `skifer semantic validate --schema` publié tôt |
| Découpe anticipée | Double maintenance sans second connecteur | 46.7 strictement après 46.6 |
| Fatigue du sujet | Abandon à mi-chemin | M1 autosuffisant ; chaque sous-plan livre une valeur seule |
