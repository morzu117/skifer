# Plan 39 — Portabilité SQL-first : skifer comme alternative à dbt hors Databricks

> Rédigé le 18 septembre 2026, à partir de l'analyse de couplage Spark menée sur `main` (`3746b99`).
> Motivation : le projet **skifer-plan** (ex-hub-it) doit automatiser le process projet en s'appuyant sur skifer
> plutôt que sur dbt — or tous les clients ne sont pas sur Databricks.
> Statut : **brouillon — en attente de validation des décisions du §6.**
> Aucun cas client concret à ce jour : le plan est séquencé pour livrer une preuve locale (DuckDB) avant tout
> adaptateur payant.

## 1. Contexte

### 1.1 Ce que dbt fait, et ce que skifer doit donc faire

dbt tourne partout parce qu'il n'embarque **aucun moteur** : tout est du SQL, plus un adaptateur mince par
entrepôt (exécuter une requête, lister le catalogue, quelques stratégies d'écriture). Pour remplacer dbt, skifer
doit offrir le même chemin — **YAML → SQL → n'importe quel entrepôt** — et garder le moteur DataFrame Spark comme
**un mode d'exécution parmi d'autres**, réservé à ce que le SQL ne sait pas exprimer (streaming, règles PySpark).

Tout ce qui distingue skifer de dbt est déjà au-dessus du moteur : contrats et publication certifiée, couche
sémantique, agents et MCP, lineage, incidents.

### 1.2 État du couplage à Spark (mesuré)

- **148 modules sur 151** s'importent quand `pyspark`, `delta` et `databricks` sont bloqués à l'import. Les seuls
  échecs sont `core/spark_backend.py` (1 305 L) et `spark_factory.py` (le troisième, `api/security`, dépend de
  `fastapi`, pas de Spark).
- Toute la couche haute passe par `backend.sql()` / `execute_sql()` : les 10 checks qualité de
  `observability/checks.py` exécutent `backend.sql(query).collect()` ; `QueryResolver` produit du SQL ;
  `core/sql_compiler.py` compile déjà un pipeline YAML en `WITH … SELECT` (Plan 28, materialized views).
- Le couplage réel tient à deux endroits : `core/interpreter.py` (≈ 25 appels aux primitives DataFrame de
  `SparkBackend` : `apply_op`, `build_filter`, `join`, `group_by_agg`, `when_chain`, `row_number_over`…) et
  les **règles Python** (`dict[str, Column]` PySpark), avec `RuleExecutor` (fusion en un `select()`) et
  `RuleAnalyzer` (AST sur du code PySpark) autour.
- Fonctionnalités **intrinsèquement Databricks/Delta** : streaming (`foreachBatch`, checkpoints), MV via SQL
  warehouse, `SHALLOW CLONE` (sandbox), `MERGE INTO`, `OPTIMIZE ZORDER`, `TBLPROPERTIES` (hash de définition),
  tags Unity Catalog, `dbutils`/`WorkspaceClient`, serving MLflow, registres stockés en Delta.

### 1.3 Sonde sur `examples/` (18 septembre 2026)

Compilation des pipelines d'exemple par `compile_select`, puis transpilation du SQL vers Snowflake, BigQuery et
DuckDB avec `sqlglot` (environnement jetable, rien d'installé dans le projet, **syntaxe seulement — rien exécuté**) :

| Résultat | Pipelines | Cause |
|---|---|---|
| Compilé + transpilé vers les 3 cibles | 2 / 10 | `08_materialized_view`, `13_semantic_projection` |
| Refus : source fichier (csv/json/delta) | 4 | artefact des exemples locaux ; chez un client, la donnée est déjà dans l'entrepôt |
| Refus : règle Python | 3 | `classify_order`, `invalidate_rejected_order` sont de simples `CASE WHEN` écrits en PySpark |
| Refus : `partials:` | 1 | se traduit naturellement en CTE, non implémenté |

Aucun blocage de fond : les causes de refus (`_reject_uncompilable`, `sql_compiler.py:275`) sont peu nombreuses
et connues.

### 1.4 Leçon du Plan 26

Le multi-backend a déjà existé (Snowpark 516 L, BigQuery 336 L, `sql_base` 942 L, Protocol de ~40 méthodes) et
a été retiré en juillet 2026. Sa suppression a révélé que `between`/`not_between`/`ceil` — documentés — ne
fonctionnaient **pas** sur le chemin Spark réel. Conclusion structurante pour ce plan : **une seule génération
de SQL pour tous les entrepôts**, et des adaptateurs qui ne portent que l'exécution, jamais la sémantique.

### 1.5 Pourquoi une seule branche DataFrame

Le marché se partage en deux familles : Spark (Databricks, Fabric Spark, Dataproc, EMR, Glue, Synapse Spark) et
tout le reste, accessible en SQL (Snowflake, BigQuery, Redshift, Fabric Warehouse, Postgres, ClickHouse, Trino/
Athena/Starburst/Dremio). Snowpark et BigFrames sont des frontaux **compilés en SQL** côté serveur : leur donner
une branche reviendrait à refaire ce que le fournisseur fait déjà. dbt a tiré la même conclusion (« Python
models » = Snowpark ou Spark sur Dataproc). Flink est un autre produit (streaming pur) ; Polars/pandas sont
mono-nœud ; Ibis est un frontal SQL, donc soluble dans la branche SQL si un jour des règles Python portables
sont demandées — pas par anticipation.

## 2. Architecture cible

```text
YAML ─▶ IR (core/ir.py, existe) ─▶ ┬─ SQL compilé ─▶ sqlglot (dialecte) ─▶ Adaptateur ─▶ DuckDB | Snowflake | BigQuery | …
                                   └─ DataFrame Spark (existe) ─────────────────────────▶ Databricks (streaming, règles PySpark)
```

| Composant | Rôle | Existe ? |
|---|---|---|
| `core/sql_compiler.py` | **Unique** générateur de SQL (dialecte pivot : Spark SQL) | oui — à étendre |
| `core/dialect.py` (nouveau) | Transpilation pivot → cible via `sqlglot` ; quoting, FQN 2/3 parties, fonctions de date | non |
| `core/adapters/` (nouveau) | `Adapter` Protocol mince : `execute`, `fetch`, catalogue, stratégies d'écriture, `clone`, `swap`, `tags` | non — `SparkBackend` l'implémente pour Databricks |
| `core/capabilities_matrix.py` (nouveau) | Ce que chaque adaptateur sait faire ; `load_schema` **refuse** un pipeline hors matrice | non |
| Règles `kind="sql"` | `dict[str, str]` d'expressions SQL, fusionnées dans le même `SELECT` que `select_final` | non |
| Stratégies d'écriture | `table` / `view` / `incremental` (append, merge `unique_key`, watermark) / `snapshot` (SCD2) compilées par dialecte | partiel (upsert Spark streaming uniquement) |
| Graphe inter-pipelines (`ref`) | ordre d'exécution, `--select`, équivalent du DAG dbt | presque — index de métadonnées Plan 31 |
| DuckDB local | moteur de développement et de tests en mode SQL, sans JVM | non |

Principes non négociables, hérités du projet :
- **Refus explicite plutôt que dégradation silencieuse** : un pipeline qui utilise une capacité absente de
  l'adaptateur échoue au `load_schema`, avec le nom de la capacité et de l'adaptateur.
- **Un seul chemin d'exécution par mode** : le mode SQL ne duplique jamais la logique du mode Spark ; les deux
  partagent l'IR, `op_catalog.py` et le test garde-dérive dispatch Spark ↔ compilateur SQL (déjà en place).
- **Aucune régression Databricks** : un schéma sans `engine: sql` garde le chemin actuel à l'octet près.

## 3. Rayon d'impact déclaré

Établi par `codegraph explore`/`impact` (index local) et lecture directe :

- `core/sql_compiler.py` — `_reject_uncompilable`, `compile_select`, `_compile_table_cte` ; `definition_hash`.
- `core/interpreter.py` — sélection du mode ; `core/core.py` — `run_process_to_table`, `run_from_yaml`,
  instanciation de `SparkBackend` (`core.py:236`), `_get_backend()`.
- `core/registry.py` — `VALID_KINDS` (+ `sql`) ; `core/rule_executor.py`, `core/rule_planner.py`,
  `core/rule_analyzer.py` — fusion et analyse des règles `sql`.
- `core/config.py` / `ExecutionContext` — `engine:` et `adapter:` par environnement.
- `core/constants.py` — `VALID_SOURCE_TYPES` selon l'adaptateur.
- `core/writer.py`, `observability/publication.py`, `observability/quarantine.py` — staging → validation →
  promotion via l'adaptateur (`swap` atomique).
- `observability/checks.py` — déjà SQL ; dialecte via `core/dialect.py`.
- `observability/certification_store.py`, `history.py`, `metadata_store.py`, `adaptive/store.py`,
  `observability/incidents.py` — registres : SQLite (local/skifer-plan) ou tables de l'entrepôt via l'adaptateur.
- `agentic/resolver.py` — quoting et FQN dépendants du dialecte (`resolver.py:456-465`, backticks en dur).
- `semantic/semantic.py` — `execute_sql` via l'adaptateur (déjà la seule voie).
- `spark_factory.py` — inchangé ; un `duckdb_factory` parallèle.
- `cli.py` — `skifer run --select`, `skifer compile`.
- Tests : `tests/fakes/` (un `FakeAdapter`), `tests/test_sql_compiler.py`, `tests/test_examples.py`
  (exemples exécutés en mode SQL), suite commune multi-adaptateurs.

Le flux étant piloté par le YAML, ce rayon est un **plancher**, pas un périmètre.

## 4. Phases (1 phase = 1 commit ; gate : `pytest tests/ -x --tb=short && ruff check src/`)

Estimations en jours-dev, indicatives.

### Phase 39.0 — Plan + décisions (docs only)
Ce document, ligne dans la table `CLAUDE.md`, décisions §6 tranchées.

### Phase 39.1 — Frontière `Adapter` + matrice de capacités — *8–12 j*
- `core/adapters/base.py` : Protocol **mince** (`execute(sql)`, `fetch(sql) -> list[dict]`, `list_tables`,
  `list_columns`, `table_exists`, `ensure_schema`, `create_table_as`, `create_view_as`, `clone_table`,
  `swap_tables`, `set_tags`, `identity()`).
- `SparkBackend` implémente le Protocol (aucune méthode retirée — insertion only).
- `core/capabilities_matrix.py` + garde dans `schema_loader`/`ir` : refus nominatif.
- `config.yaml` : `engine: spark|sql` (défaut `spark`), `adapter: databricks|duckdb|snowflake|bigquery`.
- Tests : `FakeAdapter`, garde-dérive par réflexion (chaque méthode du Protocol existe sur `SparkBackend`).

### Phase 39.2 — Compilateur SQL complet + dialectes — *10–15 j*

**Découpage en quatre tranches** (décidé le 18 septembre 2026 : la phase est trop large pour un cycle d'agent
unique ; chaque tranche = un commit, gate vert, livrable indépendamment) :

| Tranche | Contenu | Dépend de |
|---|---|---|
| 39.2.1 | `core/dialect.py` + extra `[sql]` + quoting dépendant du dialecte | 39.1 |
| 39.2.2 | Levée des refus compilables : partials → CTE, `drop_duplicates_on` → `ROW_NUMBER`, `dev_limit` hors définition persistée | 39.2.1 |
| 39.2.3 | Règles `kind="sql"` (registre, fusion, analyse, gouvernance `allow_raw_sql`) | 39.2.1 |
| 39.2.4 | Tests d'équivalence Spark ↔ DuckDB par construction | 39.2.2, 39.2.3 |

> 39.2.4 doit couvrir en **exécution réelle** le partial à deux niveaux (aujourd'hui seulement compilé,
> pas exécuté) et ne jamais affirmer *quelle* ligne un dédoublonnage conserve : l'ordre portant sur les clés de
> partition, toutes les lignes d'une partition sont à égalité, donc la ligne retenue dépend du moteur et n'est pas
> reproductible d'un run à l'autre — comme `dropDuplicates` sur Spark.

- `core/dialect.py` : `transpile(sql, target)` via `sqlglot` (`read="databricks"`), extra optionnel `[sql]`.
  `quote_ident`/`quote_fqn` deviennent dépendants du dialecte (`sql_compiler.py:58-68`, `resolver.py:628`).
- Levée des refus de `_reject_uncompilable` qui ont un équivalent fidèle :
  `partials` → CTE (les partials sont eux-mêmes compilés, récursivement) ;
  `drop_duplicates_on` → `ROW_NUMBER() OVER (PARTITION BY … ORDER BY <clé déclarée>) = 1` avec **ordre
  obligatoire** (sinon refus conservé) ; sources fichier → hook adaptateur (`read_csv`/`read_parquet` DuckDB,
  `read_files` Databricks, refus ailleurs) ; `dev_limit` accepté **hors définition persistée** (jamais dans une MV).
- Règles `kind="sql"` : `VALID_KINDS` + `RuleExecutor` (fusion dans le `SELECT`) + `RuleAnalyzer` (lecture des
  colonnes référencées par parsing `sqlglot`, pas par AST Python) + validation `allow_raw_sql` (une règle `sql`
  est du SQL brut : soumise à la même gouvernance que `expr:`).
- Tests : chaque construction compilée est exécutée sur DuckDB **et** comparée au résultat Spark sur la fixture
  `spark` (test d'équivalence, la seule preuve valable).

### Phase 39.3 — DuckDB de bout en bout — *5–8 j*

> **Condition d'acceptation ajoutée le 18 septembre 2026, trouvée en review de 39.2.2.**
> Le chemin Spark n'applique `dev_limit` que **hors job et hors production** (`interpreter.py:476-482` :
> `if dev_limit and not ctx.is_job_execution` puis `if not ctx.is_production`). Le compilateur SQL, lui, n'a
> aucune notion de mode d'exécution : il émet le `LIMIT` dès que `persisted_definition=False`. Tel quel, le
> chemin d'exécution batch **tronquerait silencieusement la sortie d'un job de production** — précisément ce que
> le message de refus d'origine dénonçait. L'appelant doit donc neutraliser `dev_limit` en mode job/prod avant de
> compiler, et un test doit le prouver sur les deux modes. Ce n'est pas un défaut de 39.2.2 (aucun appelant ne
> passe encore `persisted_definition=False`), mais c'est bloquant pour 39.3.
- `core/adapters/duckdb.py`, `duckdb_factory`, `engine: sql` + `adapter: duckdb` en `LOCAL`.
- `run_process_to_table` en mode SQL : `CREATE OR REPLACE TABLE … AS <select>` via l'adaptateur.

**Découpage en tranches** (chaque tranche = un commit, gate vert) :

| Tranche | Contenu | État |
|---|---|---|
| 39.3.1 | Adaptateur DuckDB, `duckdb_factory`, chemin d'exécution SQL de `run_process_to_table` | livrée |
| 39.3.2 | Sources fichier CSV / Parquet / JSON, défauts d'options alignés sur Spark | livrée |
| 39.3.3 | Colonnes résolues depuis la source : règles `kind="sql"` sur tables fichier | livrée |
| 39.3.4 | Exemples du dépôt exécutés et comparés sur les deux moteurs | livrée |
| 39.3.5 | Loaders `kind="sql"` : une expression de relation, portable sur les deux moteurs (décision D8) | — |

> **Phase 39.3 rouverte le 18 septembre 2026.** La décision D8 a été prise après la clôture de la phase, et
> son implémentation appartient topiquement ici — c'est la dernière construction YAML qui force un pipeline à
> rester sur Spark. Le rattacher à une phase ultérieure aurait rangé le travail là où personne ne le chercherait.
> Mesure faite avant de trancher : `sqlglot` transpile `SELECT * FROM VALUES (…) AS t(a, b)` vers DuckDB et
> Snowflake en parenthésant, et vers BigQuery en `UNNEST([STRUCT(…)])`. Le loader écrit donc du Spark SQL, et la
> portabilité est acquise sans qu'il ait à la connaître.

> **Critère de sortie corrigé le 18 septembre 2026, après exécution.** Le critère d'origine — « les exemples 01,
> 02, 05, 06, 07, 13 passent en mode SQL » — a été écrit avant d'avoir rien exécuté. Sondés un par un sur le
> chemin DuckDB, deux d'entre eux ne relèvent pas de cette phase et un troisième n'exécute aucun pipeline :
>
> | Exemple | Constat mesuré |
> |---|---|
> | 01 `first_pipeline` | tourne en mode SQL sans aucune modification |
> | 05 `rules_join_aggregate` | débloqué par 39.3.3 ; mêmes lignes que Spark sur ses deux pipelines |
> | 06 `nested_partials` | débloqué par 39.3.3 ; mêmes lignes que Spark |
> | 02 `quality_and_contract` | déclare `data_product:` — dépend de la publication certifiée, donc de la **phase 39.5** |
> | 07 `sources_and_shaping` | déclare `source_type: loader`, une fonction Python rendant un DataFrame. Aucun équivalent SQL n'est conçu ; le refus nominatif de l'adaptateur est le bon comportement. Décision ouverte D8. |
> | 13 `semantic_projection` | n'exécute aucun pipeline (projection pure, déjà sans Spark) : ne prouve rien sur le mode SQL |
>
> Critère retenu : **les exemples 01, 05 et 06 produisent les mêmes lignes sur les deux moteurs**, comparées par
> la fonction stricte de `tests/test_sql_spark_equivalence.py` (`tests/test_examples_sql_mode.py`), et
> `examples/24_sql_mode_portability/` le montre au lecteur. Les exemples 02 et 07 descendent respectivement en
> 39.5 et sous la décision D8.

### Phase 39.4 — Stratégies d'écriture — *10–15 j*
- `materialization: table|view|incremental|snapshot` ; `incremental: {strategy: append|merge, unique_key,
  watermark_column}` ; `snapshot: {strategy: timestamp|check, unique_key}` (SCD2 : `valid_from`/`valid_to`).
- Compilé par dialecte : `MERGE INTO` (Databricks, Snowflake, BigQuery, DuckDB).
- Mode Spark : `merge` réutilise le chemin upsert existant (Plan 27), pas un second.

> **Deux mesures faites le 18 septembre 2026, avant découpage.**
>
> **`MERGE INTO` est natif sur DuckDB 1.5.5** — exécuté, avec `WHEN MATCHED` et `WHEN NOT MATCHED`. Le repli
> `DELETE + INSERT` transactionnel envisagé dans ce plan n'est donc pas nécessaire : la question est close.
>
> **`UPDATE SET *` / `INSERT *` ne se transpile pas.** `sqlglot` laisse ces formes passer **verbatim** vers
> Snowflake et BigQuery, qui les refusent : ce sont des extensions Databricks/DuckDB. Un `MERGE` compilé avec
> des étoiles paraîtrait donc portable, passerait la transpilation sans erreur, et échouerait chez le premier
> client Snowflake. **La phase émet des listes de colonnes explicites**, obtenues par
> `Adapter.list_relation_columns` (livré en 39.3.3). C'est le même piège que `format = 'auto'` en 39.3.2 :
> une correspondance qui a l'air juste parce que rien ne proteste.

**Découpage en tranches** :

| Tranche | Contenu | État |
|---|---|---|
| 39.4.1 | Grammaire et IR : `type: view\|incremental\|snapshot`, validation au load, refus nominatifs, capacités | livrée |
| 39.4.2 | `view` : `CREATE OR REPLACE VIEW`, compilée comme définition persistée | livrée |
| 39.4.3 | `incremental: append` : `INSERT INTO`, création si la cible est absente, borne de watermark | livrée |
| 39.4.4 | `incremental: merge` : `MERGE INTO` à colonnes explicites sur les deux chemins | livrée |
| 39.4.5.1 | Garde-fous SCD2 et préflight : six constats, chacun portant son fragment YAML | livrée |
| 39.4.5.2 | Écriture SCD2 : `timestamp` et `check`, préflight obligatoire, reprise convergente | livrée |
| 39.4.6 | Équivalence Spark ↔ DuckDB des quatre stratégies | livrée |

> **Le préflight compte six constats, pas cinq.** Le sixième — une clé à `NULL` — a été trouvé en relisant
> 39.4.5.1 et mesuré sur DuckDB : une ligne à clé nulle passe le contrôle de doublons (`GROUP BY` rend un
> groupe d'un) puis ne s'apparie jamais à sa propre version, `t.key = s.key` n'étant jamais vrai pour `NULL`.
> Elle serait réinsérée à chaque run et la table grossirait en silence.

> **Condition d'acceptation de 39.4.6.** Une stratégie incrémentale ne se prouve pas en un run : le premier
> remplit une table vide et ne distingue `append` ni de `merge` ni d'un `CREATE TABLE AS`. Chaque test
> d'équivalence de cette phase exécute donc le pipeline **au moins deux fois**, avec une source modifiée entre
> les deux — une ligne nouvelle, une ligne mise à jour, une ligne inchangée — et compare l'état final des deux
> moteurs. Un test à un seul run serait vert sur une implémentation qui écrase tout à chaque exécution.

#### Spécification de 39.4.5 — SCD2 : refuser, puis suggérer

> **Décidé le 18 septembre 2026 avec l'utilisateur.** Le SCD2 n'est pas une traduction mécanique : c'est
> l'endroit du framework où une erreur de déclaration détruit des données **sans rien signaler**. Cette tranche
> livre donc autant de garde-fous que d'écriture.

**Les cinq pièges.** Quatre corrompent durablement en silence ; le cinquième est invisible par construction.

| # | Piège | Sans garde-fou |
|---|---|---|
| 1 | `unique_key` non unique dans le lot | Deux lignes courantes (`valid_to IS NULL`) pour une même clé ; toute jointure aval fait un fanout |
| 2 | Ligne disparue de la source | Soit elle reste courante à jamais, soit on la ferme — et fermer un **extrait partiel** pris pour un snapshot complet ferme toute la table en un run |
| 3 | `updated_at` à `NULL` | La ligne n'est ni nouvelle ni inchangée : comparaison indécidable |
| 4 | `updated_at` en arrière | Donnée en retard, plus ancienne que la version courante : réécrire l'histoire en silence |
| 5 | **Dérive de schéma entre deux runs** | 39.4 émet des **listes de colonnes explicites** (Snowflake refuse `UPDATE SET *`). Une colonne **ajoutée** à la source est donc silencieusement ignorée : la cible ne la voit jamais, et aucune erreur n'est levée |

**Les trois décisions produit.**

| # | Question | Décision |
|---|---|---|
| D9 | Ligne disparue | **`on_missing` est obligatoire** — pas de défaut. Un schéma `snapshot` qui ne le déclare pas est refusé au chargement. Skifer ne peut pas deviner si le pipeline lit un snapshot complet ou un delta, et se tromper ferme toute la table : un défaut serait ici une supposition à conséquence durable |
| D10 | Rayon d'action | **Garde-fou avec seuil par défaut ajustable.** `max_closed_ratio: 0.2` ; un run qui fermerait une part supérieure des lignes ouvertes **refuse** et rapporte. C'est exactement le run qui vide une table sans erreur, et dbt n'a rien de tel |
| D11 | Donnée en retard | **Refuser et rapporter.** `on_late_arrival: refuse` par défaut. Réinsérer chronologiquement est le plus correct sémantiquement et de loin le plus coûteux (toute version postérieure doit être réécrite) : hors v1 |

**Grammaire ajoutée par cette tranche** (39.4.1 a livré le reste ; `on_missing` y devient requis — ce n'est pas
une régression de 39.4.1, aucun schéma `snapshot` ne s'exécute avant 39.4.5) :

```yaml
materialization:
  type: snapshot
  strategy: timestamp
  unique_key: [order_id]
  updated_at: modified_at
  on_missing: close          # close | ignore — REQUIS, aucun défaut
  max_closed_ratio: 0.2      # défaut ; 1.0 désactive le garde-fou
  on_late_arrival: refuse    # refuse (défaut) | ignore
```

**Le préflight.** Il tourne **avant toute écriture** `snapshot`, sur les deux chemins, et il est fail-closed.
Sa forme reprend celle que le projet a déjà éprouvée avec `SemanticSynchronizer` : un rapport dont
`safe_to_apply` est faux **dès qu'il y a une suggestion**, et qui n'écrit rien tant que ce n'est pas propre
(`semantic/sync.py:60-77`). Ne pas créer un second mécanisme de rapport.

**Chaque constat porte le fragment YAML à coller** — c'est la demande explicite de l'utilisateur, et la
différence entre un refus utile et un refus qui laisse l'auteur chercher. Exemple pour le piège 1 :

```text
[snapshot] REFUSED — 'unique_key' [order_id] is not unique in this batch:
  412 keys carry more than one row. SCD2 cannot decide which is current.
  Suggested — deduplicate on the key, declaring the order explicitly:
    quality_checks:
      drop_duplicates_on: [order_id]
    preprocess:
      qualify: {order_by: "modified_at DESC"}
  or extend the key so it identifies one row.
```

**Aucune suggestion n'est jamais appliquée d'office** — règle de la maison : `--promote` refuse plutôt que
d'écraser, `adaptive accept` garantit la non-destruction au niveau syscall.

**Messages sans valeurs de données** : un constat nomme les **colonnes** et un **compte**, jamais les valeurs
fautives. C'est déjà la règle des alertes d'incident (Plan 31).

**CLI** : `skifer snapshot check <pipeline>`, codes de sortie alignés sur `skifer semantic sync --check`
(`0` propre · `1` erreur technique · `2` constat · `3` refus), pour qu'une CI puisse le porter.

### Phase 39.5 — Qualité, publication certifiée, registres — *10–15 j*
- `PublicationCoordinator` : staging → checks (déjà SQL) → `swap_tables` atomique / quarantaine via adaptateur.
  Snowflake `SWAP WITH`, BigQuery copy job + rename, DuckDB transaction.
- `definition_hash` : `TBLPROPERTIES` sur Databricks, `COMMENT`/tags ailleurs, table `_skifer_meta` en repli.
- Registres (certification, incidents, usage, metadata) : implémentation **SQL générique** via l'adaptateur,
  ou SQLite hébergé par skifer-plan (décision D4). Les `_get_backend().spark` résiduels (`history.py:192`,
  `metadata_store.py:360`) sont remplacés.

> **Défaut trouvé le 19 septembre 2026, en préparant 39.5.4b — et corrigé aussitôt.**
> Un schéma déclarant `data_product:` était **écrit sans publication certifiée** sur le chemin SQL. La branche
> `engine_mode() == "sql"` de `patterns.run_process_to_table` rend la main avant le contrôle que le chemin
> Spark applique (`certification_store` **et** `monitor` obligatoires, sinon fail-fast) : la table partait sans
> contrôle de contrat, sans enregistrement de certification, sans quarantaine, **et sans erreur**. Un pipeline
> certifié déplacé vers un autre moteur perdait toute sa couche de gouvernance en silence — exactement la
> dégradation silencieuse que le §2 interdit.
> Corrigé par une capacité `certified_publication`, déclarée par Databricks et pas par DuckDB : le refus est
> nominatif et aucune table n'est écrite. **39.5.4b la fera déclarer par DuckDB**, et pas avant.

> **Mesure du 18 septembre 2026, avant découpage.** L'état réel des registres n'est pas celui que ce plan
> supposait, et il commande le découpage.
>
> Ils se répartissent en **deux familles**, pas une :
>
> | Famille | Modules | Comment ils atteignent le moteur |
> |---|---|---|
> | Déjà en forme d'adaptateur | `certification_store.py`, `adaptive/store.py` | appellent des **méthodes nommées** du backend (`append_certification_contract`, `get_certification_run`, `append_semantic_usage_event`…), déjà implémentées en SQL dans `SparkBackend` |
> | Encore liés à Spark | `observability/history.py`, `observability/metadata_store.py` | atteignent `backend.spark` et utilisent `createDataFrame`, `.write.format("delta")`, `.collect()` |
>
> La première famille paraît facile à porter — il « suffirait » d'implémenter ces méthodes sur `DuckDBAdapter`.
> **C'est le piège.** Elles sont **16** sur `SparkBackend` — compte corrigé le 18 septembre 2026, la première
> mesure en annonçait 14 et oubliait `get_incident` et `list_incidents` — contre un Protocol `Adapter` de
> **20 membres** : les exiger de chaque adaptateur porterait la frontière à 36, et rendrait chaque nouvel
> entrepôt presque deux fois plus cher. C'est exactement le Protocol à ~40 méthodes que le Plan 26 a supprimé (§1.4).

| # | Question | Recommandation |
|---|---|---|
| D12 | Où vit la logique de registre | **Validée le 18 septembre 2026.** Un registre SQL générique écrit **une seule fois**, au-dessus du Protocol mince (`execute_sql`, `fetch`, `ensure_schema_exists`, `list_relation_columns`). Aucun membre ajouté à `Adapter` : la frontière reste à 20 membres au lieu de 36. Les 16 méthodes de `SparkBackend` restent en place — aucune régression Databricks — et deviennent à terme des appels au registre générique. |
| D13 | Dialecte des checks qualité | **Tranchée le 19 septembre 2026, trouvée en préparant 39.5.4.** Le plan disait `checks.py` « déjà SQL » : les requêtes le sont, mais elles émettent du **pivot** (`sql_compiler.quote_ident`, backticks) et lisent par `.collect()`, une méthode de DataFrame Spark. Aucun check ne tournait donc hors Spark. Les checks **continuent d'émettre du pivot** — un seul SQL à écrire — et un point de passage unique transpile vers le dialecte de l'adaptateur avant de lire par `fetch`. Mesuré : Databricks ressort à l'octet près, DuckDB exécute. Quoter par dialecte dans les checks n'aurait traité que le quoting, pas les fonctions. |

**Découpage proposé** :

| Tranche | Contenu | Dépend de |
|---|---|---|
| 39.5.1 | Registre SQL générique (schéma des tables, écriture, lecture) au-dessus du Protocol mince, sans toucher aux appelants | 39.4 |
| 39.5.2 | `certification_store` et `adaptive/store` branchés dessus quand l'adaptateur n'est pas Databricks | 39.5.1 |
| 39.5.3 | `history.py` et `metadata_store.py` : suppression de `backend.spark`, réécriture sur le registre générique | 39.5.1 |
| 39.5.4a | Les checks qualité lisent par l'adaptateur, pas par `.collect()` (décision D13) | livrée |
| 39.5.4b | `PublicationCoordinator` : staging, checks, promotion/quarantaine via l'adaptateur (`swap_tables` : Snowflake `SWAP WITH`, BigQuery copy + rename, DuckDB transaction) | 39.5.4a |
| 39.5.5 | `definition_hash` hors Databricks : `COMMENT`/tags, table `_skifer_meta` en repli | 39.5.4 |
| 39.5.6 | Exemple 02 en mode SQL — le critère de sortie déplacé depuis la phase 39.3 | 39.5.4 |

### Phase 39.6 — Graphe inter-pipelines et sélection — *5–8 j*
- `ref()` implicite : une table déclarée dans `tables:` qui est la sortie d'un autre pipeline du projet crée une
  arête. Construit sans Spark depuis l'index Plan 31 (`observability/metadata_index.py`).
- `skifer run --select <pipeline>[+]`, `skifer compile <pipeline> --target <adapter>` (SQL affiché, rien
  exécuté — l'équivalent de `dbt compile`, premier outil de skifer-plan).

**Découpage** :

| Tranche | Contenu | État |
|---|---|---|
| 39.6.1 | `skifer compile PIPELINE --target …` : SQL sur stdout, diagnostics sur stderr, aucune connexion ouverte | livrée |
| 39.6.2 | Graphe inter-pipelines : arêtes implicites depuis l'index Plan 31, `skifer graph` — sans exécution | — |
| 39.6.3 | `skifer run --select <pipeline>[+]` : ordre topologique et sélection | 39.6.2 |

> **Ce que 39.6.1 a établi et qui vaut pour la suite.** La commande n'ouvre aucune connexion, donc elle ne peut
> pas lire le catalogue. Une règle `kind="sql"` a besoin des colonnes de ses tables pour savoir si elle ajoute
> ou réécrit une colonne : elle est donc **refusée**, avec le code `2`. C'est délibéré — du SQL plausible mais
> faux serait pire qu'un refus, parce qu'il se copie-colle. Même règle pour 39.6.2 : un graphe qui devinerait
> une arête ne vaut pas mieux qu'un graphe qui dit ce qu'il ne sait pas.

### Phase 39.7 — Adaptateur Snowflake — *15–25 j*
`snowflake-connector-python` (pas Snowpark), `CLONE` (sandbox), `SWAP WITH`, tags, Dynamic Tables pour
`materialized_view`, identité `CURRENT_USER()`. CI sur compte réel (coût à budgéter).

### Phase 39.8 — Adaptateur BigQuery — *15–20 j*
FQN `projet.dataset.table`, clustering, labels, copy jobs, MV natives, identité via ADC.

### Phase 39.9 — Documentation et exemples — *5–10 j*
`docs/core.md` (modes d'exécution, matrice), `docs/yaml_spec.md` (incremental/snapshot, `kind: sql`), un
exemple `24_sql_mode_duckdb`, un `25_incremental_snapshot`, guide « venir de dbt » (correspondance
`ref`/`source`/`tests`/`snapshots`).

**Total indicatif : ≈ 55–85 j jusqu'à Snowflake inclus (39.1–39.7, 39.9) ; + 15–20 j par entrepôt supplémentaire.**

## 5. Options écartées

- **Second backend DataFrame (Snowpark, Ibis, Polars)** — refaire ce que le Plan 26 a retiré ; Snowpark est
  déjà un compilateur SQL ; Ibis est une dépendance structurante à ne prendre que sur demande client.
- **Faire passer Databricks par le mode SQL** — perdrait le streaming, les règles PySpark existantes et
  l'optimisation Catalyst des règles fusionnées ; le mode Spark reste le chemin natif Databricks.
- **Un `sql_base` maison** (dialectes écrits à la main) — c'est ce qui a divergé en 2026 ; `sqlglot` couvre
  ~30 dialectes et est le composant de dbt-core-like tools (SQLMesh).
- **Snowpark Connect for Spark** (PySpark sur Snowflake) — pourrait rendre la branche Spark partiellement
  portable ; **non vérifié**, hors plan tant qu'un essai réel n'a pas eu lieu.

## 6. Décisions à valider (conception)

| # | Question | Recommandation |
|---|---|---|
| D1 | Dialecte pivot du compilateur | **Spark SQL** (celui déjà émis ; `sqlglot read="databricks"`), transpilé vers la cible |
| D2 | Forme des règles portables | `kind="sql"` renvoyant `dict[str, str]` ; soumises à `allow_raw_sql` |
| D3 | Sélection du mode | par environnement dans `config.yaml` (`engine`, `adapter`), jamais dans le YAML de pipeline |
| D4 | Hébergement des registres hors Databricks | tables de l'entrepôt via adaptateur **par défaut** ; SQLite skifer-plan en option client |
| D5 | Périmètre Spark-only assumé en v1 | streaming, serving MLflow, règles PySpark, MV via SQL warehouse |
| D6 | Ordre des adaptateurs | DuckDB → Snowflake → BigQuery ; Fabric Warehouse/Postgres/Trino ensuite selon demande |
| D7 | Dépendances | `sqlglot` + `duckdb` dans un extra `[sql]` ; le cœur s'importe sans eux (comme `[tracing]`, `[mcp]`) |
| D8 | Loaders portables | **Validée le 18 septembre 2026.** Un loader `kind="sql"` rend une **expression de relation** placée là où `Adapter.resolve_source` place déjà la sienne — un loader SQL est donc l'équivalent, côté utilisateur, de ce que l'adaptateur fait pour une source fichier. Soumis à `allow_raw_sql` comme les règles `kind="sql"`. Le loader Python reste refusé nominativement hors Spark. |
| D9 | SCD2 — ligne disparue de la source | **Validée.** `on_missing` obligatoire, aucun défaut. Voir la spécification de 39.4.5 |
| D10 | SCD2 — rayon d'action d'un run | **Validée.** `max_closed_ratio: 0.2` par défaut, ajustable ; au-delà le run refuse et rapporte |
| D11 | SCD2 — donnée arrivée en retard | **Validée.** `on_late_arrival: refuse` par défaut ; la réinsertion chronologique est hors v1 |

## 7. Risques

- **Longue traîne des dialectes** (merge, types, quoting, permissions) — c'est ce que dbt a accumulé en années.
  Mitigation : suite d'équivalence Spark ↔ DuckDB dès 39.2, CI sur comptes réels dès 39.7, refus plutôt
  qu'approximation.
- **Sémantique divergente entre modes** (le même YAML, deux résultats). Mitigation : le test d'équivalence est
  le gate de chaque construction ; un écart connu est un refus nominatif dans la matrice, jamais une note.
- **Dérive documentation/fonctionnalité** (leçon Plan 26). Mitigation : le test garde-dérive dispatch Spark ↔
  compilateur existe ; l'étendre à la matrice (chaque capacité déclarée a un test par adaptateur).
- **Règles PySpark existantes chez les utilisateurs** — non portables. Mitigation : elles restent valides en
  mode Spark ; guide de migration vers `kind="sql"` ; `skifer compile` signale les règles bloquantes.
- **Coût CI** (Snowflake/BigQuery) — à budgéter avant 39.7 ; DuckDB porte 100 % de la CI jusque-là.

## 8. Vérification

- Par phase : gate complet ; `python -c "import skifer"` avec `sqlglot`/`duckdb`/`pyspark` bloqués → succès.
- 39.2 : test d'équivalence par construction (`filter`, `join`, `aggregate`, `when_chain`, `add_columns`,
  partials, dédoublonnage) — même jeu de données, résultats Spark et DuckDB identiques après tri.
- 39.3 : `tests/test_examples.py` exécute les exemples SQL-compatibles dans les deux modes.
- 39.5 : scénario publication certifiée complet (promotion + quarantaine) sur DuckDB.
- 39.7/39.8 : la suite commune d'adaptateurs sur compte réel, déclenchée manuellement (pas à chaque PR).
- Non-régression Databricks : les 182 tests `spark` et les exemples existants inchangés, à l'octet près pour
  les fichiers `generated_sql/`.
