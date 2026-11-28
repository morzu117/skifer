# Plan 47 — Frontières des wheels, contrat de connecteur partagé et durcissement du compilateur

| | |
|---|---|
| **Statut** | Proposé le 26 novembre 2026. Décisions D1 – D8 arbitrées le même jour (recommandations retenues). Aucun code n'est écrit ; le lancement de l'implémentation reste à déclencher par l'utilisateur. |
| **Branche du plan** | `feat/plan47-wheel-boundaries` (depuis `main` @ `e2e36b7`, release 4.2.0) |
| **Dépôts touchés** | `skifer` (cœur, 4 wheels), `skifer-duckdb`, `skifer-snowflake` |
| **Suite de** | [Plan 46](46_multi_platform_plan.md) (46.0 – 46.7 livrés), [ADR 0001](../adr/0001-sql-first-multi-platform.md) |
| **Destinataires** | Agents de dev (un point = un brief = un commit), puis review |

## 1. Contexte

La découpe 46.7 est publiée : un seul arbre `src/skifer`, quatre wheels construits depuis
`packaging/<wheel>/pyproject.toml`, tous dans le même paquet Python `skifer.*`.

| Wheel | Contenu aujourd'hui | Dépendances |
|---|---|---|
| `skifer-core` | 11 modules de langage + `skifer/__init__.py`, `skifer/core/__init__.py` | `pyyaml` ; extra `dialects` (`sqlglot`) |
| `skifer` | couche gouvernée (sémantique, agents, observabilité, MCP, API, CLI…) + `core/{core,config,context,catalog_inspector,environment,patterns,rule_planner,sandbox}.py` | `skifer-core==X` |
| `skifer-engine` | `spark_factory.py`, `core/{spark_backend,writer,loaders,interpreter,rule_executor}.py` | `skifer==X`, `pyspark`, `delta-spark` |
| `skifer-databricks` | `serving/` uniquement | `skifer==X`, `mlflow`, `openai`, `databricks-connect` |
| `skifer-duckdb`, `skifer-snowflake` (dépôts séparés) | un connecteur chacun | `skifer[dialects]>=4.2.0` |

L'audit de la 4.2.0 confirme que l'objectif premier est atteint : une installation
`skifer[dialects]` + connecteur Snowflake n'installe ni ne charge `pyspark`, `delta`, `mlflow`
ou `databricks`. Il a aussi mesuré des défauts de robustesse du compilateur (dont deux
injections SQL), une surface publique qui ment selon le wheel installé, un contrat de
connecteur sous-spécifié et dupliqué, et des frontières de wheel qui ne correspondent pas
encore à « un wheel = un rôle ou une plateforme ».

### 1.1 Constats (ce que le plan corrige)

« Exécuté » = reproduit par un run réel ; « lu » = établi par lecture du code seulement.

| Id | Constat | Preuve | Point |
|---|---|---|---|
| R1 | **Injection SQL par le type de `cast:`** — l'argument est interpolé brut (`sql_compiler.py`, `_SQL_OP_DISPATCH["cast"]`). `cast:int) + (SELECT max(secret) FROM main.vault` compile avec `allow_raw_sql=False` sur les trois cibles ; `MetricCandidateAgent.validate()` l'accepte. | Exécuté (PyPI 4.2.0 ; secret lu sur DuckDB) | 47.1 |
| R2 | **`quote_fqn` laisse passer brut tout nom contenant un backtick.** Avec `SnowflakeConnector` (dont `_parts` n'interdit pas les quotes) un nom de table forgé produit `… UNION ALL SELECT * FROM "SALARIES"`. La note « réfuté » du Plan 46 est fausse. | Exécuté (faux curseur) | 47.2, 47.15 |
| R3 | `publish_schema` d'un connecteur ignore `data_product:`, `contract:`, `sink:` : un YAML à clé `required`+`unique` publie doublons et NULL, sans avertissement. | Exécuté (DuckDB) | 47.11 |
| R4 | La couche gouvernée suppose un DataFrame Spark (`.limit().collect()`) : `AgentReadyDataService.query` lève `AttributeError` sur une relation DuckDB ; même motif dans `agent.py` ×4, `candidate_agent.py`, `cli.py`. `candidate_agent` compile toujours en dialecte `databricks`. | Exécuté (data service) / lu (autres) | 47.10, 47.13 |
| R5 | **Divergences silencieuses sur des clés déclarées `Supported` pour DuckDB** : `qty:greater_than:6.5` sur un INT (Spark lève, DuckDB arrondit et omet une ligne) ; `label:like:100\%` (1 ligne vs 0) ; `label:contains:a\b` (Spark lève, DuckDB 1 ligne) ; `keep_all_columns` sur une jointure à clés de noms différents (5 colonnes en DataFrame, 6 en SQL). | Exécuté (Spark + DuckDB) | 47.4, 47.5 |
| R6 | Deux tables sans jointure : la seconde est ignorée en silence. | Exécuté (SQL) / lu (DataFrame) | 47.3 |
| R7 | `SemanticEngine` écrit sur `stdout` (dont le SQL exécuté) ; le transport MCP stdio utilise `stdout` comme canal de protocole, sans redirection. | Lu (corruption du flux non exécutée de bout en bout) | 47.6 |
| S1 | Avec `skifer-core` seul, `skifer.__all__` annonce 22 noms dont 17 lèvent un `ModuleNotFoundError` nu ; `RuleRegistry.list_loaders()` lève sur `skifer.core.loaders`. | Exécuté (PyPI 4.2.0) | 47.7 |
| S2 | Le wheel `skifer` se décrit « for Spark and Databricks », `skifer[databricks]` installe `databricks-sdk` et non `skifer-databricks`, `sqlglot>=25` n'a pas de borne haute (trois versions installées côte à côte), les connecteurs n'ont pas de borne haute sur le cœur. | Lu (manifestes) | 47.8 |
| S3 | Les frontières ne sont vérifiées que depuis les `pyproject.toml` : aucun job ne construit les wheels ni n'importe un environnement « un seul wheel ». | Lu (`tests/test_packaging.py`, `tests.yml`) | 47.0 |
| S4 | Contrat de connecteur sous-spécifié : type de résultat (relation vs curseur), `statement_id` (fabriqué par DuckDB), arité des FQN, transactions (jointes vs refusées), typage de l'append. `compile/report/execute_schema/publish_schema` et `tests/_normalize.py` sont dupliqués à l'identique dans les deux connecteurs. | Lu | 47.10 – 47.12, 47.14, 47.15 |
| S5 | `SparkBackend` ne satisfait aucun des trois contrats ; il porte 24 méthodes de stockage de gouvernance (certification, incidents, candidats, connaissance, usage) sans port déclaré. | Exécuté (`isinstance` ×3) / lu | 47.16, 47.17 |
| S6 | `skifer` embarque `SkiferEngine` (`core/core.py`), inutilisable sans `skifer-engine` ; `skifer-engine` dépend de `skifer` : cycle. | Lu | 47.18 |
| S7 | `skifer-databricks` ne contient que le serving ; le code propre à Databricks (`uc_mirror`, `metric_views`, `analyzer/history`) reste dans `skifer`, et `observability/__init__.py` importe `uc_mirror` au chargement. | Lu (scan AST des imports) | 47.19, 47.20 |

Ce qui tient et ne doit pas régresser : échappement des littéraux (`escape_sql_string`),
contrats import-linter, discipline de la matrice de capacités, transactions et append typé du
connecteur DuckDB.

## 2. Objectifs et non-objectifs

**Objectifs**

1. Aucune chaîne venant d'un YAML (donc d'un LLM) n'atteint le SQL sans passer par un
   littéral échappé, un identifiant cité ou une grammaire fermée.
2. Toute divergence connue entre moteurs est soit refusée, soit déclarée `Conditional` et
   épinglée par un test de parité — jamais silencieuse.
3. Chaque wheel a un rôle énonçable en une phrase, vérifié par un job qui le construit et
   l'installe seul.
4. Un connecteur est mince **et** vérifiable : orchestration et kit de conformité vivent dans
   `skifer-core`, une fois.
5. Aucun code propre à Databricks ne se charge dans une session d'une autre plateforme.

**Non-objectifs**

- Aucune nouvelle plateforme, aucun run Snowflake réel (critère 46.6 inchangé), pas de M2 GCP.
- Aucune règle Python hors Spark, aucun DataFrame universel (les « cinq interdits » du Plan 46
  restent en vigueur).
- Aucun déplacement de classe d'un fichier à l'autre quand un déplacement de **packaging**
  suffit (tous les wheels partagent l'espace `skifer.*` : changer de wheel = déplacer une ligne
  `force-include`).
- Aucun bump de version, aucune release : la version et le moment restent à l'utilisateur.

## 3. Décisions (arbitrées le 26 novembre 2026)

**Les huit recommandations ci-dessous sont retenues par l'utilisateur.** Elles valent
décision pour tous les briefs : un agent de dev ne les rouvre pas, et s'arrête s'il rencontre
une question de conception qu'elles ne couvrent pas.

| Id | Question | Décision retenue | Concerne |
|---|---|---|---|
| D1 | Les stores `Delta*` changent-ils de wheel ? | **Non.** 4 sur 6 n'appellent que des méthodes du backend : on déclare ce port (47.16) et ils restent dans `skifer`. `DeltaMetadataStore` et `DeltaHistoryStore` (qui font `backend.spark.sql`) restent en place, signalés comme couplés à Spark ; leur passage par le port est un suivi. | 47.16 |
| D2 | `SkiferEngine` : injection d'un runtime, ou déplacement dans `skifer-engine` ? | **Déplacement par packaging** de `core/{core,patterns,context,sandbox,rule_planner}.py`. Le scan des imports montre qu'aucun module gouverné ne les importe au chargement ; le cycle disparaît sans toucher au code. | 47.18 |
| D3 | Comment la couche gouvernée lit-elle des lignes hors Spark ? | **Quatrième contrat étroit `RowsContract.fetch_rows(result, limit)`** (lignes bornées, `list[dict]`), avec addendum à l'ADR 0001. Ce n'est pas un DataFrame universel : une seule opération, bornée. Alternative écartée : laisser agents et MCP « Spark seulement » avec une erreur typée. | 47.10, 47.13 |
| D4 | Le `print` du SQL exécuté dans `SemanticEngine` passe-t-il par `logging` (DEBUG) ? | **Oui.** | partie de 47.6 |
| D5 | Publier via un connecteur un YAML qui déclare `data_product:` ? | **Refus explicite** (« la publication certifiée passe par `SkiferEngine` »). Idem `sink:` non Delta et `materialization:` autre que `table`. | 47.11 |
| D6 | Les connecteurs dépendent-ils de `skifer-core[dialects]` (et de `skifer` seulement via un extra `semantic`) ? | **Oui.** Installation minimale = 3 distributions. Rupture : `skifer_duckdb.semantic` exige alors `pip install skifer-duckdb[semantic]`. | 47.14, 47.15 |
| D7 | `skifer-databricks` devient-il « la plateforme Databricks » (SDK + code UC), le serving passant en extra `skifer-databricks[serving]` ? | **Oui.** `skifer[databricks]` → `skifer-databricks==X` ; `skifer[serving]` → `skifer-databricks[serving]==X`. Nécessaire parce que `databricks-connect` est incompatible avec `pyspark` : la base ne peut pas l'imposer. Rupture : `pip install skifer-databricks` n'apporte plus `mlflow`. | 47.20 |
| D8 | Borne haute de `sqlglot` ? | **`sqlglot>=25,<31`** (versions vues : 30.33 – 30.35). Relever la borne = rejouer SQL figés + parité. | partie de 47.8 |

## 4. Règles communes à tous les briefs

À recopier en tête de chaque brief confié à un agent de dev.

- **Dépôt cœur** : `skifer`, branche de lot créée depuis `main` à jour (`git fetch` d'abord) —
  ou depuis `feat/plan47-wheel-boundaries` tant que ce plan n'est pas mergé dans `main` :
  `feat/plan47-a-hardening`, `feat/plan47-b-connector-contract`, `feat/plan47-c-wheel-boundaries`.
  Un lot = une PR ; un lot ne démarre que lorsque le précédent est mergé.
- **Un point = un commit**, message `feat(plan47-47.N): …` (`fix(...)` pour 47.1 – 47.6,
  `chore(...)`/`docs(...)` selon le cas). Pas de push, pas de PR, pas de merge par l'agent.
- **Toute modification de `src/skifer/`** s'accompagne d'un test dans `tests/` et d'une entrée
  dans `CHANGELOG.md` sous `## [Unreleased]`. Jamais de bump de version.
- **Périmètre insertion-only.** Les suppressions ne sont légitimes que là où le brief les
  demande explicitement (elles sont listées point par point). Les briefs qui touchent un
  manifeste (`pyproject.toml`, `packaging/*/pyproject.toml`), la CI ou `scripts/release.py` le
  disent : sans cette mention, n'y touche pas.
- **Gate complet avant de rendre la main** (jamais un sous-ensemble) :

  ```bash
  export JAVA_HOME=/opt/homebrew/opt/openjdk@17 PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
  pytest tests/ -x --tb=short && ruff check src/ && lint-imports
  mkdocs build --strict        # si docs/ ou mkdocs.yml est touché
  ```

- **Comprendre avant de modifier** : `codegraph explore "<symboles>"`, pas de grep en première
  intention. Le rayon d'impact rendu est un plancher (flux pilotés par YAML).
- **Ne conçois pas.** Si le brief laisse une décision de conception ouverte, arrête-toi et
  écris-la dans le handoff. Les décisions D1 – D8 sont tranchées par l'utilisateur :
  ne les rouvre pas.
- **Échec pré-existant** : le prouver sur la base sans le diff, le signaler, ne jamais le
  contourner (`skip`, seuil abaissé, assertion retirée).
- **Handoff** : commit, sortie du gate, lignes supprimées
  (`git --no-pager show <COMMIT> | grep -E "^-" | grep -vE "^---" | wc -l`), écarts au brief.

## 5. Lot A — Durcissement (aucune décision requise, publiable seul)

Ordre : 47.0 d'abord (il vérifie tout le reste), puis 47.1 – 47.9 dans l'ordre. 47.1/47.2,
47.6, 47.7 et 47.8 sont indépendants entre eux et parallélisables en worktrees séparés.

### 47.0 — Garde-fou : les wheels sont construits, installés et importés seuls

**But.** Fermer S3 : toute la suite du plan déplace des frontières, il faut un test qui les
observe réellement.

**Fichiers.** `scripts/check_wheels.py` (nouveau), `tests/test_packaging.py`,
`.github/workflows/tests.yml` (**CI modifiée à la demande du brief**), `CHANGELOG.md`.

**Comportement attendu.**

1. `tests/test_packaging.py` (sans build, tourne en local) : chaque fichier `src/skifer/**/*.py`
   appartient à **exactement un** wheel d'après les `force-include` (`exclude` compris) — ni
   orphelin, ni doublon. Lister les orphelins dans le message d'échec.
2. `scripts/check_wheels.py` : construit les quatre wheels (`pip wheel --no-deps
   packaging/<wheel> -w <tmp>`), vérifie depuis les `RECORD` que les listes de fichiers sont
   disjointes et identiques à ce que prédit le test (1), puis, dans des venvs jetables :
   - **core seul** (`skifer-core[dialects]`) : `import skifer`, `load_schema`/`parse_schema`,
     `compile_select` sur les trois dialectes ;
   - **gouverné sans moteur** (`skifer` + `skifer-core`, extras `dialects,mcp,api` exclus du
     critère) : chaque module des deux wheels s'importe, ou échoue **uniquement** par un
     `ModuleNotFoundError` dont `.name` est une dépendance tierce optionnelle déclarée ; après
     import, `sys.modules` ne contient ni `pyspark`, ni `delta`, ni `mlflow`, ni `databricks`.
   Sortie : code 0/1 et un rapport lisible (module, wheel, cause).
3. Job CI `wheels` (Python 3.12, sans Java) qui lance le script.

**Hors périmètre.** Installer `pyspark` ou `mlflow` dans ce job ; modifier `publish-*.yml`.

**Terminé quand.** Le test (1) est vert en local ; le script est vert en CI ; un fichier
ajouté sous `src/skifer/` sans ligne `force-include` fait échouer (1).

**Note.** Le script a besoin du réseau (hatchling, dépendances). Sans réseau dans le bac à
sable de l'agent : livrer le script, exécuter (1), et le dire dans le handoff.

### 47.1 — Compilateur : grammaire fermée pour le type de `cast` (R1)

**Fichiers.** `src/skifer/core/sql_compiler.py`, `tests/test_sql_compiler.py`,
`tests/test_candidate_agent.py`, `CHANGELOG.md`.

**Comportement attendu.** Une fonction `validate_cast_type(text) -> str` (descente récursive,
profondeur ≤ 4, aucune regex « classe de caractères » seule — `int) AS a, (SELECT 1` ne
contient que des caractères anodins) accepte exactement :

- scalaire : nom dans une liste fermée — `string, varchar, char, int, integer, bigint, long,
  smallint, short, tinyint, byte, double, float, real, decimal, numeric, dec, boolean, bool,
  date, timestamp, timestamp_ntz, datetime, binary` (insensible à la casse) — avec précision
  optionnelle `(p)` ou `(p,s)` (entiers) uniquement pour `decimal/numeric/dec/varchar/char` ;
- composé : `array<T>`, `map<K,V>`, `struct<nom:T,…>` où `nom` matche
  `^[A-Za-z_][A-Za-z0-9_]*$`.

Tout le reste lève `SqlCompilationError` citant le type reçu (`repr`) et la liste admise. Le
dispatch `cast` passe par cette fonction ; la correspondance `datetime → timestamp` est
conservée. Le chemin DataFrame (`spark_backend`) n'est pas modifié.

**Tests.** Les 9 types utilisés dans le dépôt (`double, string, date, int, long, timestamp,
integer, decimal, datetime`) compilent à l'identique ; `decimal(10,2)`, `array<string>`,
`map<string,int>`, `struct<a:int,b:array<string>>` compilent ; la charge
`int) + (SELECT max(secret) FROM main.vault`, `int) AS a, (SELECT 1`, `int; DROP TABLE x`,
`int -- x`, un type inconnu et une profondeur 5 lèvent, sur les trois dialectes ;
`MetricCandidateAgent.validate()`/`compile()` refusent la charge.

**Terminé quand.** Plus aucune entrée de `_SQL_OP_DISPATCH` / `_SQL_FILTER_DISPATCH`
n'interpole un argument hors `sql_literal`, `quote_ident`, `int(...)` ou
`validate_cast_type` — un test parcourt les deux tables avec un argument-sonde et l'affirme.

### 47.2 — `quote_fqn` ne laisse plus rien passer brut (R2)

**Fichiers.** `src/skifer/core/sql_compiler.py`, `tests/test_sql_compiler.py`,
`docs/roadmap/46_multi_platform_plan.md` (corriger la mention « réfuté », une ligne),
`CHANGELOG.md`.

**Comportement attendu.** Un nom contenant un backtick n'est accepté que s'il est **entièrement
bien formé** : une à trois parties ``` `…` ``` séparées par des points, sans backtick ni `\x00`
à l'intérieur (c'est ce que produit `core/sandbox.py`). Sinon `SqlCompilationError`. Un nom
sans backtick garde le comportement actuel, avec refus d'une partie vide. Les 27 appels
existants (`spark_backend.py`, `uc_mirror.py`, `sql_compiler.py`) ne changent pas.

**Tests.** Noms 1/2/3 parties, cités ou non ; sortie du sandbox ; la charge
``` `ORDERS` WHERE 1=0 UNION ALL SELECT * FROM `SALARIES` ``` lève ; backtick orphelin, partie
vide, `\x00` lèvent ; `compile_select` avec un `resolve_table` renvoyant un nom forgé lève.

### 47.3 — Table déclarée mais jamais jointe (R6)

**Fichiers.** `src/skifer/core/sql_compiler.py`, `src/skifer/core/interpreter.py`,
`tests/test_sql_compiler.py`, `tests/test_interpreter.py`, `CHANGELOG.md`.

**Comportement attendu.**

- Chemin SQL (`compile_select`) : avec plus d'une table, tout alias autre que la base doit
  apparaître comme `alias_left` ou `alias_right` d'une jointure ; sinon `SqlCompilationError`
  nommant l'alias et proposant de le joindre ou de le retirer.
- Chemin DataFrame : même détection, mais **`logger.warning`** nommant l'alias (pas d'erreur :
  comportement historique conservé). Aucun YAML du dépôt (`examples/`, `tests/`) n'est dans ce
  cas — vérifié par scan ; si un test existant casse, s'arrêter et le signaler.

**Hors périmètre.** `run_union_sources_to_table` (une seule table, injectée par
`dataframes_in`) ; les `partials`.

### 47.4 — `SELECT *` sur une jointure à clés de noms différents (R5)

**Fichiers.** `src/skifer/core/sql_compiler.py`, `tests/test_sql_compiler.py`,
`tests/parity/test_divergences.py`, `CHANGELOG.md`.

**Comportement attendu.** Quand la projection finale est `*` (`keep_all_columns`, ou ni
`select_final` ni `aggregate`) et qu'une jointure autre que `cross`, `left_semi`, `left_anti`
a `keys_left != keys_right`, `compile_select` lève `SqlCompilationError` : le chemin SQL
garderait la clé droite que le chemin DataFrame retire ; déclarer `select_final`.

**Tests.** Unitaires (refus, et non-refus pour clés identiques / `cross` / `semi` / `anti` /
`select_final`). Un pin de parité : sélectionner explicitement la clé droite réussit en SQL
(Spark SQL et DuckDB) et échoue en DataFrame — divergence épinglée, pas corrigée ici.

### 47.5 — Capacités conditionnées par la valeur, épinglées par la parité (R5)

**Fichiers.** `src/skifer/core/capability_matrix.py`, `src/skifer/core/sql_compiler.py`
(`used_capabilities`), `tests/test_capability_matrix.py`, `tests/test_sql_compiler.py`,
`tests/parity/test_divergences.py`, `CHANGELOG.md`.

**Comportement attendu.** Deux clés nouvelles, dans un espace `literal:` déclaré explicitement
dans `_KEYS` (le drift-guard doit les connaître comme clés « de valeur », non issues de
`op_catalog`) :

| Clé | Émise quand | databricks | duckdb / snowflake |
|---|---|---|---|
| `literal:decimal_text` | un filtre de comparaison (`equals, not_equals, greater_than, less_than, greater_than_equal, less_than_equal, between, not_between, in, not_in`) porte une valeur texte de forme `^-?\d+\.\d+$` | `Supported` | `Conditional` — « comparé à une colonne entière, Spark (ANSI) lève et la cible arrondit » |
| `literal:like_backslash` | un filtre `like, not_like, contains, not_contains, starts_with, ends_with` porte une valeur contenant `\` | `Supported` | `Refused` — « `\` est un échappement LIKE sur Spark et un caractère littéral sur la cible » |

`used_capabilities` les émet pour tous les emplacements de filtres déjà parcourus (tables,
groupes, `where`, `having`, `when`). `compile_select` refuse donc `literal:like_backslash` hors
Databricks par le mécanisme existant.

**Tests.** Trois pins de parité (Spark réel + DuckDB, fixture `parity_spark`) reproduisant les
trois divergences mesurées, chacun échouant le jour où les moteurs convergent ; rapport de
capacités et refus de compilation ; `_DUCKDB_PROVEN` inchangé.

**Hors périmètre.** `contains` qui traite `%` et `_` de la valeur comme jokers (cohérent entre
moteurs, donc pas une divergence) — consigné en §9.

### 47.6 — MCP stdio : `stdout` réservé au protocole (R7, D4)

**Fichiers.** `src/skifer/mcp/runtime.py`, `tests/test_mcp_cli.py` (c'est là que le runtime est testé),
`CHANGELOG.md`, `src/skifer/semantic/semantic.py` (une ligne, D4) et son test.

**Comportement attendu.** Dans `_serve_stdio`, **après** l'entrée dans `stdio_server()` (qui a
déjà capturé le vrai `stdout`), `server.run(...)` s'exécute sous
`contextlib.redirect_stdout(sys.stderr)`. Le `print` du SQL
(`semantic.py`, bloc `if not resolved.policy_filters:`) devient `logger.debug` — **seule ligne
existante modifiée**, les autres `print` restent.

**Tests.** Faux module `mcp.server.stdio` : un `print` émis pendant `server.run` arrive sur
`stderr` et jamais sur le flux de protocole ; `stdout` est restauré après. Le SQL
n'apparaît plus sur `stdout` et apparaît en DEBUG.

### 47.7 — Surface publique honnête par wheel (S1)

**Fichiers.** `src/skifer/__init__.py`, `src/skifer/core/__init__.py`,
`src/skifer/core/registry.py`, `tests/test_packaging.py`, `tests/test_import_boundaries.py`,
`CHANGELOG.md`.

**Comportement attendu.**

- `__getattr__` des deux racines : si l'import du module porteur échoue par
  `ModuleNotFoundError` **et** que `exc.name` est ce module `skifer.*` (ou un module `skifer.*`
  qu'il importe), relever un `ModuleNotFoundError` (même `name`) dont le message nomme la
  distribution à installer : « `SkiferEngine` ships in the `skifer-engine` distribution:
  `pip install 'skifer[spark]'` ». Une dépendance **tierce** manquante se propage inchangée.
- La table module → distribution est une constante courte dans `skifer/__init__.py` ; un test
  de `tests/test_packaging.py` la dérive des `packaging/*/pyproject.toml` et échoue si elle
  ment (elle restera donc juste après 47.18).
- `RuleRegistry.list_loaders()` sans `skifer.core.loaders` installé : renvoie les loaders
  enregistrés (les built-ins arrivent avec `skifer-engine`), sans lever.
  `get_loader(<nom absent>)` dans ce cas : message qui cite `skifer-engine`.

**Tests.** Simulation par `monkeypatch` de l'absence de chaque module porteur (comme
`test_skifer_engine_missing_gives_a_clear_install_hint`) : les 22 noms de `skifer.__all__` et
les 3 de `skifer.core.__all__` donnent soit l'objet, soit le message avec la bonne
distribution. `scripts/check_wheels.py` (47.0) : ajouter la sonde « core seul → chaque export
annonce sa distribution ».

### 47.8 — Métadonnées des wheels : descriptions, bornes, extras (S2, D8)

**Fichiers (manifestes modifiés à la demande du brief).** `pyproject.toml`,
`packaging/skifer/pyproject.toml`, `packaging/skifer-core/pyproject.toml`,
`packaging/*/README.md`, `uv.lock` (régénéré par `uv lock` ; sans réseau, le dire),
`tests/test_packaging.py`, `CHANGELOG.md`.

**Comportement attendu.**

- `sqlglot>=25,<31` partout où `sqlglot` est déclaré (`dialects`, `analyzer`, racine comprise).
- Description du wheel `skifer` : couche gouvernée (sémantique, agents, observabilité, MCP,
  API), sans « for Spark and Databricks » ; mots-clés `pyspark`/`delta-lake` retirés de ce
  wheel (ils décrivent `skifer-engine`). Suppressions légitimes : ces lignes de métadonnées.
- Extra `engine = ["skifer-engine==X"]` ajouté, alias de `spark` (qui reste).
- Chaque `packaging/<wheel>/README.md` ouvre par une phrase de rôle et la commande
  d'installation par plateforme.
- Test : les extras du `pyproject.toml` racine et de `packaging/skifer/pyproject.toml`
  portent les mêmes noms ; les écarts voulus (`spark`, `serving`, `engine`) sont une liste
  explicite dans le test.

**Hors périmètre.** Le sens de `skifer[databricks]` (D7, 47.20) ; `scripts/release.py`.

### 47.9 — Confinement des imports de plateforme

**Fichiers.** `pyproject.toml` (**contrat import-linter ajouté à la demande du brief**),
`tests/test_import_boundaries.py`, `CHANGELOG.md`.

**Comportement attendu.** Un contrat `forbidden` : aucun module hors
`skifer.serving`, `skifer.spark_factory`, `skifer.core.{spark_backend,writer,loaders,
interpreter,rule_executor}` n'importe `pyspark`, `delta`, `mlflow`, `databricks`, sauf une
liste `ignore_imports` **explicite et commentée** des imports paresseux existants (un par
ligne : module, import, raison). Cette liste est l'inventaire que 47.19/47.20 réduisent.
Test d'exécution : dans un interpréteur neuf, importer tous les modules du wheel `skifer` ne
charge aucun de ces quatre paquets.

**Terminé quand.** `lint-imports` vert avec 4 contrats ; ajouter un `import mlflow` au niveau
module dans `skifer/semantic/` fait échouer les deux garde-fous.

## 6. Lot B — Un contrat de connecteur, une orchestration, un kit (D3, D5, D6)

Prérequis hors dépôt cœur : merger (ou baser dessus) `feat/plan46-46.4-connector`
(`skifer-duckdb`) et `feat/plan46-46.6-connector` (`skifer-snowflake`), aujourd'hui non mergées.

### 47.10 — Contrat v1 normatif et `RowsContract` (S4, D3)

**Fichiers.** `src/skifer/core/connector.py`, `tests/test_connector_contracts.py`,
`docs/adr/0001-sql-first-multi-platform.md` (addendum daté, insertion), `CHANGELOG.md`.

**Comportement attendu.**

- `CONTRACT_VERSION = 1` ; `ConnectorCapabilities` gagne `contract_version: int =
  CONTRACT_VERSION` (champ à défaut, compatible).
- `RowsContract` (`runtime_checkable`) : `fetch_rows(self, result: Any, limit: int) ->
  list[dict[str, Any]]` — `result` est l'objet rendu par `execute_sql` /
  `ExecutionResult.dataframe` ; au plus `limit` lignes, clés `str`, valeurs natives ; libère la
  ressource native (curseur) ; `limit < 0` → `ValueError`.
- Les docstrings des quatre Protocols deviennent la **spécification** : noms = composants
  littéraux non cités, 1 à 3 parties, tout caractère de citation refusé ; `statement_id` =
  identifiant de la plateforme ou `None`, **jamais fabriqué** ; propriété et durée de vie du
  résultat ; écritures atomiques qui ne committent jamais la transaction de l'appelant (la
  rejoindre ou la refuser, l'un des deux, documenté) ; `append` exige les mêmes noms de
  colonnes, le contrôle de type étant annoncé par le connecteur.

**Hors périmètre.** Toute méthode de transformation ; `SparkBackend`.

### 47.11 — Orchestration partagée dans `skifer-core` (R3, S4, D5)

**Fichiers.** `src/skifer/core/pipeline.py` (nouveau, **12ᵉ module de langage**),
`packaging/skifer-core/pyproject.toml` et `pyproject.toml` (contrat import-linter n° 1) —
**manifestes modifiés à la demande du brief** —, `tests/test_pipeline.py`,
`tests/test_packaging.py`, `CHANGELOG.md`.

**Comportement attendu.** Quatre fonctions pures, sans état :
`report(parsed, connector)`, `compile(parsed, connector)`,
`execute(parsed, connector) -> ExecutionResult`, `publish(parsed, connector, fqn, mode)`.
Dialecte lu sur `connector.capabilities.dialect`, résolution par `connector.resolve_fqn`.
`publish` lève une `PublicationRefused(ValueError)` avant toute compilation si
`parsed.raw` déclare `data_product` (message : passer par `SkiferEngine`), un `sink` dont le
type n'est pas `delta`, une `materialization` dont le type n'est pas `table`, ou si `mode`
n'est pas dans `connector.capabilities.writes`.

**Tests.** Faux connecteur en mémoire : chemins nominaux, chaque refus (dont le YAML de R3),
aucune méthode du connecteur appelée quand un refus tombe.

### 47.12 — Kit de conformité et normalisation partagés (S4)

**Fichiers.** `src/skifer/testing/__init__.py`, `src/skifer/testing/normalize.py`,
`src/skifer/testing/connector_conformance.py` (nouveaux, livrés par `skifer-core` —
manifestes et contrat import-linter n° 1 à compléter), `tests/test_connector_conformance.py`,
`tests/fakes/` (connecteur de référence sur DuckDB), `CHANGELOG.md`.

**Comportement attendu.**

- `normalize.py` : reprise à l'identique de `tests/_normalize.py` des connecteurs (lire le
  fichier dans `skifer-duckdb`).
- `ConnectorConformance` : classe de base **sans dépendance à pytest** (asserts simples,
  `try/except`), à sous-classer en fournissant `make_connector()` et des drapeaux
  (`append_checks_types`, `joins_caller_transaction`). Vérifie : `isinstance` sur les quatre
  contrats ; `capabilities` valide et `contract_version` ; `resolve_fqn` (arités, refus des
  citations, parties vides) ; `table_exists`/`columns` (inconnu → `False` / `LookupError`) ;
  `write_table` overwrite puis relecture, append (table absente, vue, colonnes en trop ou
  manquantes), mode inconnu ; refus d'un non-`SELECT` et de plusieurs instructions pour vue et
  table ; `statement_id` `None` ou `str` ; `fetch_rows` borné, `list[dict]`, clés `str`.

**Terminé quand.** Le kit passe sur le connecteur de référence du dépôt et **échoue** sur une
variante volontairement fautive de chaque exigence (un test par exigence).

### 47.13 — La couche gouvernée lit des lignes par le contrat (R4, D3)

**Fichiers.** `src/skifer/semantic/rows.py` (nouveau), `src/skifer/semantic/evidence.py`
(`SemanticResult`), `src/skifer/semantic/semantic.py`, `src/skifer/agentic/data_service.py`,
`src/skifer/agentic/agent.py`, `src/skifer/agentic/candidate_agent.py`, `src/skifer/cli.py`
(site `data_df.limit(20).collect()`), tests correspondants, `CHANGELOG.md`.

**Comportement attendu.**

- `fetch_bounded_rows(backend, frame, limit)` : `list`/`tuple` → tranche (comportement
  actuel) ; `backend` satisfait `RowsContract` → `backend.fetch_rows(frame, limit)` ; objet à
  `limit` et `collect` appelables (Spark) → `frame.limit(limit).collect()` ; sinon `TypeError`
  qui nomme `RowsContract`.
- `SemanticResult.rows(limit)` délègue à cette fonction ; le moteur y attache le backend
  (champ à défaut `None`, `repr=False, compare=False`).
- Les sites listés passent par là, chacun avec la borne dont il a besoin (1 pour un scalaire,
  `max_rows + 1` pour un tableau). **S'il n'existe pas de borne naturelle à un site,
  s'arrêter et le signaler.**
- `candidate_agent` compile avec `dialect=self.semantic.dialect`.

**Hors périmètre.** `serving/` (wheel Databricks, Spark par construction),
`observability/checks.py`, `analyzer/parity.py`, `services/execution.py` (chemins moteur).

**Tests.** Faux backend `RowsContract` à curseur ; relation DuckDB réelle via le connecteur de
référence de 47.12 ; non-régression Spark (`tests/fakes/fake_backend.py`).

### 47.14 — `skifer-duckdb` adopte le contrat partagé (S4, D6)

**Dépôt.** `skifer-duckdb`, branche `feat/plan47-shared-contract`.

**Comportement attendu.** `compile/report/execute_schema/publish_schema` délèguent à
`skifer.core.pipeline` (corps dupliqués supprimés — suppressions légitimes) ; `fetch_rows`
implémenté (`relation.limit(n)` → dicts) ; `statement_id=None` (plus de `uuid4`) ; suite de
conformité = sous-classe de `ConnectorConformance` (`append_checks_types=True`,
`joins_caller_transaction=True`) ; `tests/_normalize.py` remplacé par
`skifer.testing.normalize` ; dépendances `skifer-core[dialects]>=<version publiant 47.10–47.12>,<5`,
extra `semantic = ["skifer>=…,<5"]` ; `skifer_duckdb.semantic` importe `skifer.semantic`
paresseusement avec message d'installation ; CI et README alignés.

**Terminé quand.** Les 23 cas de parité contre la référence Spark figée restent verts ;
`pip install skifer-duckdb` dans un venv neuf installe 3 distributions (plus `duckdb`).

### 47.15 — `skifer-snowflake` adopte le contrat partagé (R2, S4, D6)

**Dépôt.** `skifer-snowflake`, branche `feat/plan47-shared-contract`. Même contenu que 47.14,
plus : `_parts` refuse `` ` ``, `"`, `'` (comme DuckDB) — test avec la charge de R2 ;
`fetch_rows` par `cursor.fetchmany(limit)` + `cursor.description`, curseur fermé ; drapeaux
`append_checks_types=False`, `joins_caller_transaction=False`.

**Terminé quand.** Les 20 SQL figés et les 3 refus restent verts ; aucune capacité Snowflake
ne passe à `Supported` (critère 46.6 inchangé).

## 7. Lot C — Un wheel = un rôle ou une plateforme (D1, D2, D7)

### 47.16 — Port de stockage de gouvernance (S5, D1)

**Fichiers.** `src/skifer/observability/store_backend.py` (nouveau),
`tests/test_store_backend.py`, `CHANGELOG.md`.

**Comportement attendu.** Cinq Protocols nommant les 24 méthodes que `SparkBackend` expose
pour les stores — certification/contrats (9), incidents (5), usage sémantique (2), candidats
(5), connaissance (3) — avec les signatures exactes de `spark_backend.py`. **Aucun code
existant modifié.**

**Tests.** (a) `SparkBackend` fournit chaque méthode avec les mêmes noms de paramètres
(`inspect.signature`, sans session Spark) ; (b) scan AST : `DeltaCertificationStore`,
`DeltaCandidateStore`, `DeltaKnowledgeStore`, `DeltaUsageEventStore` n'appellent sur leur
backend que des méthodes du port ; (c) `DeltaMetadataStore` et `DeltaHistoryStore` sont listés
comme exceptions nommées (`backend.spark`), le test échouant si une troisième apparaît.

### 47.17 — `SparkConnector` : Spark/Databricks passe le même kit (S5)

**Fichiers.** `src/skifer/core/spark_connector.py` (nouveau, wheel `skifer-engine` —
`packaging/skifer-engine/pyproject.toml` et `RUNTIME_MODULES` de `tests/test_packaging.py` à
compléter), `tests/test_spark_connector.py`, `CHANGELOG.md`.

**Comportement attendu.** Adaptateur mince autour d'un `SparkBackend` existant — qui n'est pas
modifié : ses signatures (`write_table(df, fqn, mode)`, `table_exists(catalog, schema,
table)`) ne sont pas celles du contrat. `capabilities = ConnectorCapabilities("databricks",
{"overwrite", "append"})` ; FQN à 2 parties en local, 3 avec catalogue ; un `select_sql` est
validé en l'analysant comme sous-requête (`SELECT * FROM (<sql>) LIMIT 0`, sans job) ;
`fetch_rows` = `limit().collect()` → dicts.

**Tests.** Sous-classe de `ConnectorConformance` sur la session Delta locale (fixture
`spark`) ; `SemanticEngine.from_backend(SparkConnector(...))` répond à une requête.

### 47.18 — Le runtime quitte le wheel `skifer` (S6, D2)

**Fichiers (manifestes modifiés à la demande du brief).** `packaging/skifer/pyproject.toml`,
`packaging/skifer-engine/pyproject.toml`, `tests/test_packaging.py`, `packaging/*/README.md`,
`CHANGELOG.md`. **Aucun fichier de `src/` ne bouge.**

**Comportement attendu.** Les lignes `force-include` de
`core/{core,patterns,context,sandbox,rule_planner}.py` passent de `skifer` à `skifer-engine`
(5 suppressions + 5 insertions, légitimes). Restent dans `skifer` : `core/config.py`,
`core/environment.py` (importé au chargement par `services/identity.py`),
`core/catalog_inspector.py` (par `agentic/builder_agent.py`).

**Terminé quand.** `scripts/check_wheels.py` vert : dans le venv « gouverné sans moteur »,
tout s'importe, `skifer --help` fonctionne, et `from skifer import SkiferEngine` donne le
message de 47.7 nommant `skifer-engine`. Si un module gouverné se révèle importer l'un des
cinq modules au chargement : s'arrêter et le signaler (ne pas rendre l'import paresseux de
sa propre initiative).

### 47.19 — Les imports vers le code Databricks deviennent paresseux (S7)

**Fichiers.** `src/skifer/observability/__init__.py`, `src/skifer/observability/publication.py`,
`src/skifer/candidates/promotion.py`, tests correspondants, `CHANGELOG.md`.

**Comportement attendu.** `uc_mirror` (2 importeurs) et `metric_views` (1 importeur) ne sont
plus importés au chargement : export paresseux par `__getattr__` dans `observability/__init__`
(même mécanique que `skifer/__init__.py`), import local au point d'usage ailleurs. Si le
module manque, message qui nomme `skifer-databricks`. Le miroir UC reste **non bloquant** :
son absence ne fait jamais échouer une publication (un avertissement best-effort, une fois).
Suppressions légitimes : les trois lignes d'import déplacées.

**Tests.** Interpréteur neuf : `import skifer.observability`, `skifer.candidates.promotion`
ne chargent ni `uc_mirror` ni `metric_views` ; publication certifiée verte avec `uc_mirror`
rendu indisponible par `monkeypatch`.

### 47.20 — `skifer-databricks` devient la plateforme Databricks (S7, D7)

**Fichiers (manifestes, CI et script de release modifiés à la demande du brief).**
`packaging/skifer/pyproject.toml`, `packaging/skifer-databricks/pyproject.toml`,
`pyproject.toml` (extras, et liste `ignore_imports` de 47.9 réduite), `scripts/release.py`,
`.github/workflows/tests.yml` (job `serving`), `tests/test_packaging.py` (qui porte déjà les
tests du script de release), `CHANGELOG.md`.

**Comportement attendu.**

- `observability/uc_mirror.py`, `semantic/metric_views.py`, `analyzer/history.py` sont livrés
  par `skifer-databricks` (`exclude` côté `skifer` + `force-include` côté `skifer-databricks`).
- `skifer-databricks` : dépendances `skifer==X`, `databricks-sdk>=0.20.0` ; extra
  `serving = ["mlflow>=3.0,<4", "openai>=1.0.0", "databricks-connect>=16.1"]`.
- `skifer` : `databricks = ["skifer-databricks==X"]`,
  `serving = ["skifer-databricks[serving]==X"]`.
- `scripts/release.py` : `SKIFER_PIN` reconnaît un pin avec extra
  (`skifer-databricks[serving]==X`) — test à l'appui, sans quoi la prochaine release laisse un
  pin périmé.
- `serving/deploy.py` épingle déjà `skifer[serving]==<version>` : vérifier que la résolution
  reste correcte, ne pas le modifier sinon.

**Terminé quand.** `scripts/check_wheels.py` vert, avec une sonde de plus : le venv « gouverné
sans moteur » ne contient aucun des trois modules ; `skifer analyze history` et
`skifer semantic metric-views` sans `skifer-databricks` donnent le message d'installation.

## 8. Lot D — Clôture

### 47.21 — Documentation et synchronisation

**Fichiers.** `docs/packaging.md` (nouveau : quel wheel pour quelle plateforme, matrice
d'installation, contrat de connecteur v1 et kit de conformité, « écrire un connecteur »),
`mkdocs.yml`, `docs/integrations/duckdb.md`, `docs/integrations/snowflake.md`,
`docs/install_databricks.md`, `docs/databricks_supervisor.md`, `docs/yaml_spec.md`
(grammaire de `cast`, refus 47.3/47.4), `CLAUDE.md` et `AGENTS.md` (arborescence, « 12 modules
de langage », table des plans), ce plan (statut). Gate : `mkdocs build --strict`.

## 9. Ordre, parallélisme, ruptures

```text
Lot A  47.0 ─┬─ 47.1 ─ 47.2 ─ 47.3 ─ 47.4 ─ 47.5      (même fichier : en série)
             ├─ 47.6
             ├─ 47.7
             └─ 47.8 ─ 47.9
Lot B  47.10 ─ 47.11 ─ 47.12 ─┬─ 47.13
                              ├─ 47.14   (dépôt skifer-duckdb)
                              └─ 47.15   (dépôt skifer-snowflake)
Lot C  47.16 │ 47.17 (après 47.12) │ 47.18 (après 47.7) │ 47.19 ─ 47.20
Lot D  47.21
```

Changements visibles par un utilisateur, à porter au `CHANGELOG` comme tels :

| Point | Rupture |
|---|---|
| 47.1, 47.2 | un type de `cast` hors grammaire ou un nom de table mal cité, qui compilait, est refusé |
| 47.3, 47.4 | deux schémas jusque-là compilés (table non jointe, `*` sur clés différentes) sont refusés sur le chemin SQL |
| 47.5 | un `like`/`contains` avec `\` est refusé hors Databricks |
| 47.14, 47.15 | `skifer_<plateforme>.semantic` exige l'extra `[semantic]` |
| 47.20 | `pip install skifer-databricks` n'apporte plus `mlflow` ; `skifer[databricks]` installe `skifer-databricks` |

Consigné, hors plan : `contains`/`starts_with`/`ends_with` ne neutralisent pas `%` et `_` de
la valeur (cohérent entre moteurs) ; passage de `DeltaMetadataStore`/`DeltaHistoryStore` par le
port ; déplacement de `DatabricksProvider` et de l'exporter MLflow (classes dans des fichiers
mixtes — demande un découpage de fichier, pas un déplacement de packaging) ; run Snowflake réel.

## 10. Risques

| Risque | Parade |
|---|---|
| La grammaire de `cast` refuse un type légitime en production (MV Databricks) | Liste issue de l'usage réel + types composés ; message qui cite la liste ; ajout d'un nom = une ligne + un test |
| `exclude` de hatch n'agit pas sur un répertoire `force-include` (47.20) | 47.0 lit les `RECORD` des wheels réellement construits : l'échec est immédiat, pas en production |
| Borne `sqlglot<31` vite dépassée (majeures fréquentes) | Relever la borne est un point à part : SQL figés Snowflake + parité DuckDB rejoués |
| Un refus nouveau (47.3 – 47.5) casse un pipeline utilisateur | Refus limités au chemin SQL (MV, connecteurs, candidates) ; chemin DataFrame : avertissement seulement |
| Connecteurs et cœur publiés dans le désordre | Borne basse des connecteurs = version qui publie 47.10 – 47.12 ; leur CI installe le cœur depuis `main` |
| Cycle `skifer[spark]` ↔ `skifer-engine` toujours présent dans les métadonnées | Assumé : c'est un extra de confort ; le code, lui, n'a plus de cycle après 47.18 |
| Le périmètre « insertion-only » rejette les suppressions voulues | Chaque brief liste ses suppressions légitimes ; la review compare le compteur à cette liste |

## 11. Vérification

- **Par point** : gate complet du §4 + le critère « terminé quand ».
- **Par lot** : `scripts/check_wheels.py` ; rejouer les sondes de l'audit — charges R1 et R2
  refusées sur les trois cibles ; YAML de R3 refusé à la publication ; requête MCP sur DuckDB
  (R4) rendue ; les quatre divergences de R5 soit refusées, soit `Conditional` + épinglées.
- **Fin de plan** : venv neuf par plateforme (DuckDB, Snowflake, Spark local, serving) —
  nombre de distributions installées et modules tiers chargés consignés dans le handoff ;
  suites des deux connecteurs vertes contre le cœur de `main`.

## 12. Suivi

| Point | Lot | Dépôt | Statut |
|---|---|---|---|
| 47.0 – 47.9 | A | `skifer` | Implémenté — PR #60 ouverte, non mergée ; reprise : [`47_wheel_boundaries_resume.md`](47_wheel_boundaries_resume.md) |
| 47.10 – 47.13 | B | `skifer` | À faire |
| 47.14 | B | `skifer-duckdb` | À faire (prérequis : branche 46.4 mergée) |
| 47.15 | B | `skifer-snowflake` | À faire (prérequis : branche 46.6 mergée) |
| 47.16 – 47.20 | C | `skifer` | À faire |
| 47.21 | D | `skifer` | À faire |
