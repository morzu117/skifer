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
