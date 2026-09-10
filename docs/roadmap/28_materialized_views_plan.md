# Plan 28 — Materialized views (SQL natif compilé) + agrégations déclaratives

> Branche de travail : `feat/materialized-views`
> Statut : **implémenté** (3 août 2026) — phases 28.0 → 28.6 livrées, blockers revus avant le dev
>
> Écarts assumés vs le plan initial :
> - Le DDL est assemblé dans `sql_compiler.compile_materialized_view_ddl` (et non dans `core.py`) : c'est de la génération de texte SQL, donc testable purement.
> - Le hash de définition couvre le SELECT **et** les options définissantes (`schedule`, `comment`, `cluster_by`, `partition_by`) : sans cela, changer le `schedule` dans le YAML n'aurait déclenché aucun `CREATE OR REPLACE` — même piège silencieux que celui que la décision #15 corrige.
> - `docs/yaml_spec.md` n'a pas été touché : contrairement à ce qu'annonçait la phase 28.6, ce fichier documente les **modèles sémantiques** (dimensions/métriques), pas le YAML de pipeline. La référence du YAML de pipeline est `docs/core.md`, mise à jour.
> - `SchemaInterpreter.resolve_source_table` a été extrait pour que le chemin SQL et le chemin DataFrame partagent une seule résolution sandbox (décision #21).
> Prérequis : Plan 27 mergé (PR #53)

## Contexte

Le Plan 27 a livré les streaming tables et **différé explicitement** les materialized views : `materialization: materialized_view` est reconnu mais rejeté au load ([schema_loader.py:492-497](src/skifer/core/schema_loader.py#L492-L497)), avec un message qui annonce « Lakeflow export ». Trois autres endroits du code pointent la MV comme la réponse attendue : le rejet des agrégations en streaming ([interpreter.py:311](src/skifer/core/interpreter.py#L311) — *« or a future materialized view »*), le refus de `run_union_sources_to_table` en streaming ([patterns.py:186-191](src/skifer/core/patterns.py#L186-L191)), et le commentaire de [constants.py:14-16](src/skifer/core/constants.py#L14-L16).

**La tension de fond.** Toutes les écritures du moteur reçoivent un DataFrame (`_write_dataframe`), alors qu'une materialized view Databricks se **définit par une requête SQL** (`CREATE MATERIALIZED VIEW … AS <query>`) et ne peut pas être créée depuis `spark.sql` sur un cluster all-purpose ou Databricks Connect — il faut un SQL warehouse Pro/Serverless (ou un pipeline Lakeflow). Le dépôt n'a aucun générateur DataFrame→SQL.

**Décisions actées avec l'utilisateur (3 août 2026).**
1. **SQL natif compilé depuis le YAML** — nouveau `core/sql_compiler.py` : le schéma normalisé est compilé en un `SELECT`, puis émis en DDL. Les `business_rules` Python ne sont pas exprimables en SQL → rejet au load avec message actionnable. L'export Lakeflow (qui, lui, préserverait les règles Python) reste un plan ultérieur.
2. **Bloc `aggregate:` déclaratif** — aujourd'hui agréger impose d'écrire une règle Python `kind="aggregation"` et d'attacher manuellement `agg_keys` (documenté seulement dans `docs/rules.md:71`, zéro site d'affectation dans `src/`). Le nouveau bloc sert **aussi au batch**, pas uniquement aux MV.
3. **Exécution DDL hybride** — SQL warehouse via `databricks-sdk` si `params.sql_warehouse_id` est configuré, sinon génération d'un `.sql` + avertissement ; en local (`catalog: null`), équivalent exécuté par le chemin batch existant.

**Résultat visé.** `engine.run_from_yaml("schemas/gold/fact_orders.yaml", target_layer="gold")` crée une vraie materialized view Unity Catalog dont Databricks gère le refresh incrémental, sans changer l'appel côté notebook.

## Surface YAML cible

```yaml
materialization:
  type: materialized_view
  schedule: "EVERY 6 HOURS"        # optionnel — ou "CRON '0 0 6 * * ?' AT TIME ZONE 'Europe/Paris'"
  comment: "CA agrégé par pays et par mois"   # optionnel
  cluster_by: [country]            # optionnel (exclusif avec partition_by)
  refresh: auto                    # auto (défaut) | never (laisse faire le SCHEDULE)

tables:
  - name: "{{ catalog }}.silver.orders"
    alias: ord
    filter:
      - "status:equals:DONE"
  - name: "{{ catalog }}.silver.customers"
    alias: cust

join:
  - table_from: [ord, customer_id]
    table_to: [cust, id]
    type: left

add_columns:                        # appliqué AVANT l'agrégation
  - [order_date, order_month, [to_date:yyyy-MM]]

aggregate:                          # nouveau bloc — exclusif avec select_final / keep_all_columns
  group_by: [country, order_month]
  measures:
    - [amount, total_amount, sum]           # [source, target, func]
    - [order_id, nb_orders, count_distinct]
    - source: amount
      target: panier_moyen
      func: avg
  having:                                   # optionnel — grammaire filtre, sur les alias de mesures
    - "total_amount:greater_than:1000"
```

Compilé en :

```sql
CREATE MATERIALIZED VIEW IF NOT EXISTS `cat`.`gold`.`fact_orders`
  CLUSTER BY (country)
  COMMENT 'CA agrégé par pays et par mois'
  TBLPROPERTIES ('skifer.definition_hash' = '<sha256>')
  SCHEDULE EVERY 6 HOURS
AS
WITH ord AS (SELECT * FROM `cat`.`silver`.`orders` WHERE `status` = 'DONE'),
     cust AS (SELECT * FROM `cat`.`silver`.`customers`)
SELECT `country`, to_date(`order_date`, 'yyyy-MM') AS `order_month`,
       SUM(`amount`) AS `total_amount`, COUNT(DISTINCT `order_id`) AS `nb_orders`,
       AVG(`amount`) AS `panier_moyen`
FROM ord LEFT JOIN cust ON ord.`customer_id` = cust.`id`
GROUP BY `country`, `order_month`
HAVING `total_amount` > 1000
```

`aggregate:` est utilisable **seul, sans MV**, sur une table batch classique (exécuté en `groupBy().agg()` par l'interpréteur).

## Décisions de design

| # | Sujet | Décision |
|---|---|---|
| 1 | Entrée du compilateur | L'**IR** (`parse_to_ir` → `ParsedSchema`), pas le dict brut : types garantis, filtres/ops déjà canonisés. Le compilateur est pur Python, sans Spark. |
| 2 | Forme du SQL | Une **CTE par alias de table** (projection `fields` + `WHERE` + dédup), puis `FROM <alias0> JOIN …`, puis `SELECT` final. Lisible, débuggable, et isole les filtres par table comme le fait le chemin DataFrame. |
| 3 | Mapping des opérateurs | Tables de dispatch `_SQL_FILTER_DISPATCH` / `_SQL_OP_DISPATCH` **miroir 1:1** de celles de [spark_backend.py:67-89](src/skifer/core/spark_backend.py#L67-L89), alimentées par le même `op_catalog`. Un test vérifie que les deux dispatch couvrent exactement les mêmes clés (garde anti-dérive). |
| 4 | Risque de dérive sémantique | **Test de parité** sur session Spark locale : pour N schémas, `spark.sql(compiled).collect()` == `engine.process_schema(schema).collect()` (tri stable). C'est la mitigation principale du risque #1. |
| 5 | `business_rules` + MV | Rejet au load : *« les règles Python ne sont pas compilables en SQL — matérialisez-les dans une table silver en amont, puis agrégez dans la MV »*. Cohérent avec la doctrine sub-layers du Plan 27. |
| 6 | `keep_all_columns` + MV | Autorisé **uniquement si une seule table et aucun join** (sinon `SELECT *` produit des colonnes dupliquées, illégales pour une MV). Sinon rejet demandant `select_final` ou `aggregate`. |
| 7 | `source:` fichier / `source_type: loader` + MV | Rejet au load : une MV doit référencer des tables Unity Catalog. Message : « ingérez d'abord en table (bronze), la MV se pose au-dessus ». |
| 8 | `partials:` + MV | Rejet au load en v1 (composition Python). Compilables plus tard en CTE imbriquées — noté en suite. |
| 9 | `dev_limit` + MV | Rejet au load (même arbitrage que le Plan 27 pour le streaming : un `LIMIT` figé dans une définition de MV est un piège). |
| 10 | `sink: postgres/jdbc` + MV | Rejet au load (les deux axes sont orthogonaux mais incompatibles : une MV vit dans UC). Miroir du rejet streaming [schema_loader.py:665-668](src/skifer/core/schema_loader.py#L665-L668). |
| 11 | `streaming: true` + MV | Rejet au load (bijection stricte déjà en place côté streaming, étendue). |
| 12 | Allowlist `materialization` | **Refactor : allowlist par type** au lieu de l'ensemble plat actuel + le `elif any(...)` de [schema_loader.py:552-556](src/skifer/core/schema_loader.py#L552-L556). `table` → `{type}` ; `streaming_table` → `{type, trigger, checkpoint, write_mode, keys}` ; `materialized_view` → `{type, schedule, comment, cluster_by, partition_by, refresh}`. Supprime un piège (aujourd'hui `checkpoint` serait accepté sur une MV). |
| 13 | Grammaire `schedule` | Chaîne non vide commençant (insensible à la casse) par `EVERY ` ou `CRON ` — sinon erreur listant les deux formes. Le reste est passé tel quel à Databricks (qui valide finement). |
| 14 | `cluster_by` / `partition_by` | Listes de noms de colonnes non vides, **mutuellement exclusives** (contrainte Databricks). |
| 15 | Idempotence / dérive de définition | Le hash SHA-256 du `SELECT` compilé est stocké dans `TBLPROPERTIES ('skifer.definition_hash' = …)`. Au run : MV absente → `CREATE` ; hash identique → `REFRESH` ; hash différent → `CREATE OR REPLACE` + log explicite du changement. Lecture du hash impossible → `CREATE OR REPLACE` (repli sûr, juste plus coûteux). **Évite le piège silencieux « le YAML a changé mais la MV pas ».** |
| 16 | `refresh: auto \| never` | `auto` (défaut) : `REFRESH MATERIALIZED VIEW` après un `CREATE IF NOT EXISTS` sur MV existante. `never` : on laisse le `SCHEDULE` faire, utile pour ne pas déclencher un recompute à chaque exécution du notebook. |
| 17 | Où brancher l'exécution | **Court-circuit dans `run_process_to_table` avant `process_schema`** — on ne construit aucun DataFrame. Plus une garde défensive dans `_write_dataframe` (lève si `type == materialized_view`), même doctrine « triple barrière » que le Plan 27. |
| 18 | Exécution du DDL sur Databricks | `SparkBackend.execute_sql_on_warehouse(sql, warehouse_id)` via `databricks-sdk` (`w.statement_execution.execute_statement` + polling jusqu'à état terminal). Toute erreur est **ré-levée enrichie, jamais avalée** (décision #7 du Plan 27 : une MV silencieusement non créée = perte de données). |
| 19 | Absence de `sql_warehouse_id` | **Interactif** → le SQL est écrit dans `{sql_output_dir}/{schema}.{table}.sql` (défaut `./generated_sql/`) + `logger.warning` explicite « MV NON CRÉÉE » nommant la clé de config manquante. **Job/prod** (`is_running_as_job()` ou `is_production`) → `ValueError` fail-fast **avant toute compilation**. Un job qui ne crée rien en silence est inacceptable. |
| 20 | Mode local (`catalog: null`) | Pas de MV en Delta OSS : on exécute `df = spark.sql(<select compilé>)` puis le `write_table` batch existant. Réutilise 100 % du chemin testé, **et prouve au passage que le SQL compilé est valide et exécutable**. |
| 21 | Sandbox | Le compilateur reçoit un `resolve_table: Callable[[str], str]` fourni par le moteur, qui applique exactement la même résolution sandbox que l'interpréteur ([interpreter.py:399-407](src/skifer/core/interpreter.py#L399-L407)). Pas de duplication de logique. |
| 22 | Monitor | Exécuté après création **uniquement si le DDL a réellement été exécuté** (une MV est requêtable, les checks fonctionnent). Sauté en mode génération-seule. |
| 23 | `full_refresh` | Étendu : branche MV → `DROP MATERIALIZED VIEW IF EXISTS` (pas de checkpoint à purger). La signature actuelle reste compatible. |
| 24 | `aggregate:` — exclusivités | Exclusif avec `select_final` et `keep_all_columns`. `add_columns` reste autorisé et s'applique **avant** l'agrégation (permet de dériver une clé de groupe). |
| 25 | `aggregate:` — fonctions | Catalogue `AGGREGATE_FUNCTIONS` dans `op_catalog.py` (source de vérité unique, comme `FILTER_OPERATORS`) : `sum`, `avg`, `min`, `max`, `count`, `count_distinct`, `sum_distinct`, `approx_count_distinct`, `stddev`, `variance`, `first`, `last`. `count` accepte `source: "*"` → `COUNT(*)`. |
| 26 | `aggregate:` — `having` | Réutilise la grammaire de filtre existante (`"col:operator:value"`), appliquée sur les **alias de mesures**. En DataFrame : `.filter()` après le `.agg()`. |
| 27 | `databricks-sdk` | Aujourd'hui **importé sans être déclaré** dans `pyproject.toml` (gap préexistant, cf. `environment.py:9-28`). Le plan le déclare dans un nouvel extra `[databricks]`, avec import paresseux et message actionnable si absent. |
| 28 | `run_union_sources_to_table` + MV | **Hors périmètre v1** : ce pattern découvre ses sources à l'exécution (`table_exists`) et injecte un DataFrame via `dataframes_in` — le compilateur devrait apprendre l'injection d'alias et l'`UNION ALL`. `NotImplementedError` explicite pointant la suite. Idem `run_process_and_split`. |

## BLOCKERS — incompatibilités et surfaçage (jamais silencieux)

Format d'erreur existant conservé : préfixe `[section]`, valeurs valides listées, agrégation « Schema validation failed with N error(s): » pour les validations croisées.

**Rejetés au load** (`schema_loader`) : `business_rules` non vide + MV ; `keep_all_columns` + MV avec join ou >1 table ; `source:` fichier + MV ; `source_type: loader` + MV ; `partials:` + MV ; `dev_limit` (table ou schéma) + MV ; `sink` postgres/jdbc + MV ; `streaming: true` + MV ; `cluster_by` et `partition_by` simultanés ; `schedule` mal formé ; clés hors allowlist du type ; `aggregate` + `select_final` ; `aggregate` + `keep_all_columns` ; `aggregate.group_by` vide ; mesure sans `func` valide ; `having` référençant un alias de mesure inexistant.

**Rejetés au run** : `sql_warehouse_id` absent en job/prod (avant compilation) ; `run_process_and_split` / `run_union_sources_to_table` + MV (`NotImplementedError`) ; DataFrame passé à `_write_dataframe` avec `type: materialized_view` (garde défensive) ; `databricks-sdk` absent alors qu'un warehouse est configuré (message d'installation) ; erreur du statement warehouse (ré-levée enrichie).

## Phases (1 phase = 1 commit ; gate `set -o pipefail; pytest tests/ -x --tb=short` ; jamais de bump de version)

### Phase 28.0 — Document de plan
Créer `docs/roadmap/28_materialized_views_plan.md` (ce document) sur `feat/materialized-views` ; ligne roadmap dans `CLAUDE.md` ; corriger au passage la ligne Plan 27 (« PR en cours » → « mergé via PR #53 »).

### Phase 28.1 — Bloc `aggregate:` déclaratif (indépendant des MV)
- `core/op_catalog.py` : `AggregateSpec` + catalogue `AGGREGATE_FUNCTIONS` (nom canonique, alias, template SQL, arité).
- `core/schema_loader.py` : `_normalize_aggregate` (formes liste `[source, target, func]` et mapping, défauts, validation `group_by`/`measures`/`having`, exclusivités avec `select_final`/`keep_all_columns`).
- `core/ir.py` : `ParsedMeasure`, `ParsedAggregate`, champ `ParsedSchema.aggregate`, câblage dans `parse_to_ir`.
- `core/json_schema.py` + régénération de `schemas/skifer-pipeline.schema.json`.
- `core/interpreter.py` : application après `business_rules`/`add_columns` — `groupBy(*keys).agg(...)` puis `having` via `build_filter`. Nouvelles primitives backend `group_by_agg` / `agg_expr` (SparkBackend d'abord, puis FakeBackend, à cause du drift-guard unidirectionnel).
- Tests : nouveau `tests/test_aggregate.py` (validation load + exécution FakeBackend + un cas Spark réel), extensions `test_ir.py` / `test_json_schema.py`. CHANGELOG.

### Phase 28.2 — Compilateur SQL
- **Nouveau** `core/sql_compiler.py` : `compile_select(parsed: ParsedSchema, resolve_table: Callable[[str], str]) -> str`, plus `_SQL_FILTER_DISPATCH` / `_SQL_OP_DISPATCH`, quoting systématique en backticks, littéraux échappés (`'` doublé), CTE par alias, `filter_groups` en OR-of-ANDs, `drop_nulls_in`, `drop_duplicates_on` via sous-requête `ROW_NUMBER()` (pas de `QUALIFY` — compatibilité Spark local), joins, `select_final`/`add_columns`/`aggregate`, gouvernance `allow_raw_sql` sur `expr:`/`sql:`.
- Fonctions non compilables → `SqlCompilationError` explicite (jamais de SQL approximatif).
- Tests : nouveau `tests/test_sql_compiler.py` — assertions sur le texte pour chaque opérateur/op, garde anti-dérive des dispatch, **et surtout les tests de parité** DataFrame vs `spark.sql` sur la session locale (décision #4). CHANGELOG.

### Phase 28.3 — Surface YAML `materialized_view` + validation load-time
- `core/constants.py` : `materialized_view` ajouté à `VALID_MATERIALIZATION_TYPES`, `MATERIALIZATION_ALLOWED_KEYS` (dict par type), `VALID_MV_REFRESH_MODES`, préfixes de `schedule`.
- `core/schema_loader.py` : refactor de `_normalize_materialization` en allowlist par type (décision #12), branche `materialized_view` (défauts, grammaire `schedule`, exclusivité `cluster_by`/`partition_by`), suppression du rejet actuel, nouveau `_validate_materialized_view` agrégeant tous les BLOCKERS.
- `core/json_schema.py` : `MaterializationDef` étendu (les enums dérivent déjà de la constante) + régénération de l'artefact.
- Tests : bascule de `test_schema_loader.py::test_materialized_view_rejected` en cas nominal + ~15 cas de rejet, sur le modèle de `TestStreamingValidation`. CHANGELOG.

### Phase 28.4 — Exécution : backend + moteur + patterns
- `core/spark_backend.py` : `create_materialized_view(ddl, warehouse_id=None)`, `refresh_materialized_view(fqn, warehouse_id=None)`, `drop_materialized_view(fqn, warehouse_id=None)`, `get_table_property(fqn, key)`, `execute_sql_on_warehouse(sql, warehouse_id)` (SDK, polling, erreurs ré-levées), branche locale de la décision #20.
- `core/core.py` : `resolve_sql_warehouse_id()` (miroir de `resolve_checkpoint_location`, fail-fast nommant `environments.<ENV>.params.sql_warehouse_id`), orchestration `_create_materialized_view(...)` (compile → hash → create/refresh/replace), branche MV dans `full_refresh`, garde défensive dans `_write_dataframe`.
- `core/patterns.py` : court-circuit MV dans `run_process_to_table` avant `process_schema`, monitor conditionné à l'exécution réelle, `NotImplementedError` dans les deux autres patterns.
- `tests/fakes/fake_backend.py` : miroirs + recorder `_materialized_views`.
- Tests : nouveau `tests/test_materialized_view.py` — dispatch, hash create/refresh/replace, fail-fast job sans warehouse, génération `.sql` en interactif, refus des patterns, **E2E local** (MV → table Delta via SQL compilé, contenu vérifié). CHANGELOG.

### Phase 28.5 — Packaging `databricks-sdk`
`pyproject.toml` : nouvel extra `[databricks]` (`databricks-sdk`), import paresseux avec message d'installation actionnable ; documenter que l'extra est inutile sur un runtime Databricks (SDK préinstallé). Test du message d'erreur en l'absence du module. CHANGELOG.

### Phase 28.6 — Documentation
`CLAUDE.md` (bloc YAML + section « ### Materialized views (Plan 28) » + « ### Agrégations déclaratives » + ligne roadmap), `AGENTS.md`, `docs/core.md` (section de référence sur le modèle de la section streaming), `README.md`, `docs/getting_started.md` (`sql_warehouse_id` à côté de `checkpoint_base`), `docs/yaml_spec.md` (gap identifié : ne documente ni le streaming ni la matérialisation), `CHANGELOG.md` finalisé, statut du plan → implémenté.

## Risques

| Risque | Mitigation |
|---|---|
| **Dérive sémantique SQL vs DataFrame** (le même YAML donne deux résultats) | Tests de parité sur session Spark réelle (décision #4) + garde anti-dérive sur les tables de dispatch + `op_catalog` comme source unique |
| Le YAML change mais la MV garde son ancienne définition | Hash de définition en `TBLPROPERTIES` → `CREATE OR REPLACE` automatique (décision #15) |
| MV silencieusement non créée faute de warehouse | Génération `.sql` + warning explicite en interactif, **fail-fast dur en job/prod** (décision #19) |
| `CREATE MATERIALIZED VIEW` refusé par le compute (cluster all-purpose / Connect) | On ne passe jamais par `spark.sql` sur Databricks : chemin dédié Statement Execution API, erreur ré-levée enrichie |
| Injection SQL via les valeurs de filtre YAML | Quoting/échappement systématique dans le compilateur, testé ; `expr:`/`sql:` restent soumis à la gouvernance `allow_raw_sql` |
| Périmètre du plan (2 features en un) | `aggregate:` est livré en 28.1 **indépendamment utilisable en batch** : même si les MV glissent, la phase a de la valeur seule |
| `CREATE OR REPLACE` à chaque run détruirait le bénéfice du refresh incrémental | `IF NOT EXISTS` + `REFRESH` par défaut ; `REPLACE` seulement sur changement de hash |
| Coût/latence : la création d'une MV déclenche un pipeline serverless (plusieurs minutes) | Polling explicite avec logs de progression ; `refresh: never` pour les runs de notebook |
| `databricks-sdk` absent | Extra `[databricks]` + message d'installation ; aucun import au niveau module |

## Vérification

1. Par phase : `set -o pipefail; pytest tests/ -x --tb=short` puis `ruff check src/` (18 erreurs de baseline préexistantes — ne gate que sur les fichiers touchés).
2. Unitaires : chaque BLOCKER du tableau (load + run), chaque opérateur/op du compilateur, résolution du warehouse (fail-fast, génération, exécution), les trois branches hash (create / refresh / replace).
3. **Parité** : pour un jeu de schémas couvrant filtres, filter_groups, joins, ops chaînées, when/else, dédup et agrégations, `spark.sql(compiled)` et `process_schema` donnent le même résultat sur session Delta locale.
4. **E2E local** : un schéma `materialization: materialized_view` produit une table Delta au contenu attendu via le SQL compilé.
5. Smoke manuel Databricks (post-merge) : MV créée sur un warehouse serverless avec `SCHEDULE` ; second run sans changement → `REFRESH` seul ; modification du YAML → `CREATE OR REPLACE` ; absence de `sql_warehouse_id` en job → `ValueError` avant compilation.

## Hors périmètre (suites identifiées)

- **Export Lakeflow** (`@materialized_view` Python) pour les pipelines qui exigent des `business_rules` Python — le seul chemin qui lève la contrainte SQL.
- `run_union_sources_to_table` compilé en `UNION ALL` (décision #28) et `partials:` compilés en CTE imbriquées (décision #8).
- Métadonnées déclaratives (`COMMENT` par colonne, tags, `TBLPROPERTIES` libres) — plan dédié.
- SCD Type 2, watermarks, Auto Loader (suites du Plan 27).
