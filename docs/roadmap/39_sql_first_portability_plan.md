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
- `core/adapters/duckdb.py`, `duckdb_factory`, `engine: sql` + `adapter: duckdb` en `LOCAL`.
- `run_process_to_table` en mode SQL : `CREATE OR REPLACE TABLE … AS <select>` via l'adaptateur.
- Critère de sortie : les exemples 01, 02, 05, 06, 07, 13 passent en mode SQL (règles réécrites en `kind="sql"`
  dans une variante, les originaux PySpark restant pour le mode Spark).

### Phase 39.4 — Stratégies d'écriture — *10–15 j*
- `materialization: table|view|incremental|snapshot` ; `incremental: {strategy: append|merge, unique_key,
  watermark_column}` ; `snapshot: {strategy: timestamp|check, unique_key}` (SCD2 : `valid_from`/`valid_to`).
- Compilé par dialecte : `MERGE INTO` (Databricks, Snowflake, BigQuery, DuckDB ≥ 1.x — à vérifier, sinon
  `DELETE + INSERT` transactionnel).
- Mode Spark : `merge` réutilise le chemin upsert existant (Plan 27), pas un second.

### Phase 39.5 — Qualité, publication certifiée, registres — *10–15 j*
- `PublicationCoordinator` : staging → checks (déjà SQL) → `swap_tables` atomique / quarantaine via adaptateur.
  Snowflake `SWAP WITH`, BigQuery copy job + rename, DuckDB transaction.
- `definition_hash` : `TBLPROPERTIES` sur Databricks, `COMMENT`/tags ailleurs, table `_skifer_meta` en repli.
- Registres (certification, incidents, usage, metadata) : implémentation **SQL générique** via l'adaptateur,
  ou SQLite hébergé par skifer-plan (décision D4). Les `_get_backend().spark` résiduels (`history.py:192`,
  `metadata_store.py:360`) sont remplacés.

### Phase 39.6 — Graphe inter-pipelines et sélection — *5–8 j*
- `ref()` implicite : une table déclarée dans `tables:` qui est la sortie d'un autre pipeline du projet crée une
  arête. Construit sans Spark depuis l'index Plan 31 (`observability/metadata_index.py`).
- `skifer run --select <pipeline>[+]`, `skifer compile <pipeline> --target <adapter>` (SQL affiché, rien
  exécuté — l'équivalent de `dbt compile`, premier outil de skifer-plan).

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
