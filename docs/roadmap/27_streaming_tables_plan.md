# Plan 27 — Tables streaming (Structured Streaming natif)

> Branche de travail : `feat/streaming-tables`
> Statut : Implémenté (31 juillet 2026) — phases 27.0 → 27.6 livrées ; reste le smoke manuel Databricks post-merge
> Amendement majeur post-revue : **upsert CDC Type 1 intégré** (`write_mode` + `keys`, `foreachBatch` + `MERGE INTO`)

## Contexte

Le moteur est 100 % batch : chaque `run_process_to_table` relit l'intégralité des sources et **overwrite** la cible ([writer.py:39](src/skifer/core/writer.py#L39)). Pour les sources append-only (événements, tables bronze alimentées en continu), c'est un coût quadratique. Décisions actées avec l'utilisateur : **approche moteur d'abord** — Structured Streaming natif (`readStream`/`writeStream` + checkpoint, trigger `availableNow` par défaut), qui fonctionne en local ET en job Databricks **sans dépendance DLT**. L'export Lakeflow/DLT est un plan futur ; les materialized views sont **différées** (`materialization: materialized_view` → rejet « prévu dans un plan ultérieur »).

Le pipeline actuel comporte de nombreuses opérations illégales en streaming (`limit`, `row_number`, `isEmpty`, `dropDuplicates` sans watermark…) : le cœur du plan est autant la **validation fail-fast** que le chemin d'exécution. Aucun code streaming n'existe dans le repo (greenfield vérifié).

## Surface YAML cible

```yaml
materialization:                 # top-level ; shorthand string acceptée
  type: streaming_table          # table (défaut) | streaming_table
  trigger: available_now         # défaut ; ou "interval:30 seconds"
  checkpoint: auto               # défaut ; ou chemin explicite ({{ param }} OK)
  write_mode: upsert             # append (défaut) | upsert (CDC Type 1)
  keys: [event_id]               # requis ssi write_mode: upsert — clé de MERGE

tables:
  - name: "{{ catalog }}.bronze.raw_events"
    alias: ev
    streaming: true              # lu via spark.readStream.table(...)
  - name: "{{ catalog }}.silver.dim_country"
    alias: dim                   # batch → côté static du join

join:
  - table_from: [ev, country_id] # le stream DOIT être la base des joins
    table_to: [dim, id]
    type: left                   # inner | left uniquement avec streaming
```

Appel inchangé : `engine.run_from_yaml(...)` — run 1 = backfill complet, run 2 = uniquement les nouvelles lignes (checkpoint). Config Databricks : `environments.<ENV>.params.checkpoint_base` requis si `checkpoint: auto`.

## Décisions de design

| # | Sujet | Décision |
|---|---|---|
| 1 | Sources fichier streaming v1 | Restreint à `delta`/`text` (seuls types sans schéma explicite requis). `csv`/`json`/`parquet`/`orc`/`avro` → rejet au load (« convertissez en Delta ou lisez une table catalogue »). Bloc `schema:` = plan ultérieur. |
| 2 | Rules `aggregation` | Rejet **au démarrage du run** (préflight interpreter via `RuleRegistry.get_rule(name).kind` — impossible au load, le registre exige les imports). Rules `transform` invérifiables : autorisées, `AnalysisException` au `start()` ré-levée avec hint. |
| 3 | `checkpoint: auto` | `SparkBackend.default_checkpoint_root()` : local → `{warehouse_dir}/_checkpoints` ; Databricks → `params.checkpoint_base` (flux `env_params` existant), **fail-fast avant `process_schema`** s'il manque. Chemin final `{root}/{schema_avec_suffixe_sandbox}/{table}` — jamais sous le chemin de la table, jamais partagé entre users. |
| 4 | Trigger | `available_now` → `.trigger(availableNow=True)` ; `"interval:<durée>"` → `.trigger(processingTime=...)`. Pas de `once` (déprécié) ni `continuous`. |
| 5 | outputMode | `append`, fixe (cohérent avec le rejet des agrégations). |
| 6 | `process_schema` | Retourne le DF streaming **non démarré** (`isStreaming=True`) ; contrat documenté. Signature inchangée. |
| 7 | Databricks Connect v2 | `write_stream_table` **ré-lève** l'erreur avec message actionnable — pas de swallow (contrairement à `optimize_table`) : une écriture silencieusement absente = perte de données. |
| 8 | .gitignore | Ajouter `.spark-warehouse/` (gap préexistant vérifié : seul `spark-warehouse/` est ignoré ; la factory crée `.spark-warehouse`). |
| 9 | `run_process_to_table` | `awaitTermination()` interne à `write_stream_table` ; retourne toujours `None`. `available_now` : le bloc monitor s'exécute après le backfill (données complètes). `interval:` : bloque volontairement, monitor jamais atteint (documenté). |
| 10 | Défense écriture batch | `SparkBackend.write_table` lève `ValueError` si `df.isStreaming` — sinon le guard `isEmpty` de writer.py:27-32 (try/except silencieux) sauterait l'écriture **sans erreur**. |
| 11 | Partials | Streaming interdit dans les schémas enfants (rejet load, récursif). `intermediate_mode=table` + streaming → `ValueError` au run. |
| 12 | Joins | **Exactement une** table `streaming: true` par schéma (stream-stream hors scope v1) ; elle doit être la base des joins, jamais en `table_to` ; types `inner`/`left` uniquement. Tout au load (seule barrière : le chemin d'exécution ne valide pas les types de join). |
| 13 | Sémantique | `streaming_table` = append incrémental (vs overwrite batch), ou upsert (décision #15). Assumé/documenté. Pas de `mergeSchema` v1. |
| 14 | Normalisation | `materialization` normalisé **seulement si présent** (zéro churn sur les schémas batch existants). |
| 15 | **Upsert CDC Type 1** (revue blockers) | `write_mode: append` (défaut) \| `upsert` + `keys: [...]` (liste non vide, requise ssi upsert). Implémentation : `foreachBatch` → dédup du micro-batch sur `keys` (batch, légal) → `MERGE INTO` cible (`matched → UPDATE SET *`, `not matched → INSERT *`). Unicité inter-batches garantie par la cible, **zéro état streaming**, idempotent au rejeu (at-least-once de foreachBatch neutralisé). Brique réutilisable par le futur SCD1/2 batch. `drop_duplicates_on` sur table streaming → erreur guidant vers `write_mode: upsert`. |
| 16 | Composition de flux (revue blockers) | Pattern de référence documenté : **sub-layers** (`silver_landing` → `silver_enrich`, chaque étage = streaming table avec son checkpoint, la suivante lit la précédente en `streaming: true`). Chaîner des streaming tables n'est PAS du stream-stream. Le fan-out type `run_process_and_split` se fait par N pipelines streaming filtrés en aval d'une table pivot (`filter` est stream-safe). |
| 17 | Bijection stricte (revue pièges) | `streaming: true` sans `materialization: streaming_table` = **erreur** (pas d'inférence automatique) : la sémantique d'écriture (append incrémental vs upsert, checkpoint, trigger) doit être une décision consciente et visible dans le YAML. Relâchable plus tard sans breaking change. |
| 18 | **`engine.full_refresh(layer, table)`** (revue pièges) | Helper inclus au Plan 27 : purge atomiquement le checkpoint résolu (inconnu de l'utilisateur en mode `auto`) ET drop la table cible. Évite l'état incohérent « purgé à moitié » (doublons ou backfill incomplet). |
| 19 | Checkpoint dev → prod (revue pièges) | En job/prod, pas de suffixe sandbox : LE checkpoint de prod est volontairement partagé entre runs planifiés (c'est l'incrémentalité). Promotion dev→prod = nouveau checkpoint = premier run prod en backfill complet — sain (pas d'héritage d'offsets d'un bac à sable), documenté. |

## BLOCKERS — incompatibilités et surfaçage (jamais silencieux)

Format d'erreur existant : préfixe `[section]`, valeurs valides listées, agrégation « Schema validation failed with N error(s): ».

**Rejetés au load** (`schema_loader._normalize_schema`) : `dev_limit` (table/schéma) avec streaming ; `preprocess.qualify` ; `quality_checks.drop_duplicates_on` (→ « utilisez `write_mode: upsert` + `keys:` ») ; `source.type` ∉ {delta, text} ; `source_type: loader` + streaming ; >1 table streaming ; stream en `table_to` ou join ∉ {inner, left} (contournement `left_anti` = `left` + filtre `is_null` dans le message) ; `sink` postgres/jdbc + streaming_table ; `streaming: true` ⇔ `materialization: streaming_table` (bijection obligatoire) ; `materialized_view` ; streaming dans un enfant `partials:` ; `write_mode: upsert` sans `keys` (ou `keys` sans upsert, ou `keys` non-liste-de-strings).

**Rejetés au run** : rules `aggregation` (préflight interpreter — message : « les agrégations relèvent d'un pipeline batch aval ou des materialized views, plan ultérieur ») ; `intermediate_mode=table` + streaming ; `run_process_and_split` (message → pattern fan-out par sub-layers) / `run_union_sources_to_table` (message → « use case materialized table, plan ultérieur ») + streaming (`NotImplementedError`, précédent JDBC patterns.py:90-94/:144-148) ; Databricks + `checkpoint: auto` sans `params.checkpoint_base` (avant `process_schema`) ; Connect v2 sans writeStream (erreur ré-levée) ; DF streaming dans `write_table` (dict brut hors load).

### Revue des blockers (31 juillet 2026) — décisions utilisateur 1 par 1

1. **Dédup streaming** → upsert CDC Type 1 intégré au plan (décision #15). 2. **dev_limit** → rejet pur (jeux dev petits par construction). 3. **Aggregations** → rejet préflight, conforme aux best practices (agg = materialized views/batch aval). 4. **Joins** → restrictions validées telles quelles. 5. **Sources fichiers** → delta/text validé. 6. **Partials** → rejet validé ; la composition se fait par sub-layers (décision d'archi plutôt que complexité YAML). 7. **Sink JDBC** → rejet validé (exposition Postgres = plan futur avec idempotence). 8. **Patterns split/union** → refus validé ; split remplacé par le fan-out sub-layers, union identifié comme use case materialized table.

## Phases (1 phase = 1 commit ; gate `pytest tests/ -x --tb=short` ; jamais de bump de version)

### Phase 27.0 — Document de plan
Créer `docs/roadmap/27_streaming_tables_plan.md` (ce document) sur `feat/streaming-tables` ; ligne roadmap CLAUDE.md.

### Phase 27.1 — Surface YAML + validation load-time
- `core/constants.py` : `VALID_STREAMING_SOURCE_TYPES = {"delta","text"}`, `VALID_MATERIALIZATION_TYPES`, `DEFAULT_TRIGGER`.
- `core/schema_loader.py` : boucle par table (~:509, après le bloc `source:` — modèle :477-508) : `streaming` bool strict + croisements par table ; top-level (~:588, avant `_validate_schema_ops`) : normalisation `materialization` (shorthand→dict, **idiome allowlist du sink** :559-587, `allowed_keys={type,trigger,checkpoint,write_mode,keys}`, défauts `write_mode=append`, grammaire trigger, rejet materialized_view, `keys` requis ssi `upsert` + liste non vide de strings) ; croisements schéma (bijection, joins, unicité, sink, partials récursif).
- Tests `tests/test_schema_loader.py` (~20 cas de rejet + nominal append/upsert + `{{ param }}` dans checkpoint). CHANGELOG.

### Phase 27.2 — IR + JSON Schema (+ fix gap `partials`)
- `core/ir.py` : `ParsedTable.streaming` (:137, set ~:326), `ParsedSchema.materialization` (:178, set ~:266) — descriptif (describe_schema/lineage).
- `core/json_schema.py` : propriété top-level `materialization` (~:92 — **obligatoire**, `additionalProperties: False` à :53) + `$def MaterializationDef` (modèle SinkDef :346-360, oneOf string-ou-dict comme SelectEntry :227-234) ; `streaming` dans TableDef (~:309) ; **fix en passant** : `partials` jamais enregistré dans le JSON Schema (tout YAML avec partials échoue la validation éditeur aujourd'hui).
- Régénérer `schemas/skifer-pipeline.schema.json` (test anti-drift). Étendre `test_json_schema.py:67-79`. CHANGELOG (Added + Fixed).

### Phase 27.3 — Primitives backend + writer local + FakeBackend + .gitignore
- `core/spark_backend.py` (à côté de :254-284) : `read_table_stream(fqn)` ; `read_source_stream(type, path, options)` (garde types) ; `write_stream_table(df, fqn, checkpoint, trigger)` → `writeStream.format("delta").outputMode("append")` + checkpoint + trigger + `awaitTermination()`, wrapping Connect v2 (décision #7) ; `default_checkpoint_root()` ; garde `isStreaming` dans `write_table`.
- `core/writer.py` : `write_stream_dataframe_local` — twin de `write_dataframe_local` :44-69, mais `start(path)` **puis** `CREATE TABLE ... USING DELTA LOCATION` **seulement si `{path}/_delta_log` existe** (premier run à 0 ligne → skip + warning).
- `tests/fakes/fake_backend.py` : miroirs (drift guard unidirectionnel → SparkBackend d'abord) ; recorder `self._streams`.
- `.gitignore` (+`.spark-warehouse/`) ; `pyproject.toml` : section `markers = ["streaming: ..."]` (aucune n'existe).
- Tests : call-shapes MagicMock + round-trip spark réel marqué `streaming` (tmp_path). CHANGELOG.

### Phase 27.4 — Upsert CDC Type 1 (`foreachBatch` + `MERGE INTO`)
- `core/spark_backend.py` : `write_stream_table` gagne `write_mode`/`keys` — branche upsert : `writeStream.foreachBatch(_merge_batch)` où `_merge_batch(batch_df, batch_id)` fait (1) `batch_df.dropDuplicates(keys)` (batch → légal), (2) `MERGE INTO {fqn} ON {clauses ON par clé}` via `DeltaTable.forName(...).merge(...).whenMatchedUpdateAll().whenNotMatchedInsertAll()` — fallback SQL `MERGE INTO` si l'API DeltaTable est indisponible (Connect). Premier run : la cible peut ne pas exister → create-if-absent (write batch du premier micro-batch ou CREATE TABLE à partir du schéma du DF).
- `core/writer.py` : twin local de l'upsert (le MERGE Delta fonctionne en local ; enregistrement metastore identique au twin append).
- `tests/fakes/fake_backend.py` : recorder `_streams` enrichi (`write_mode`, `keys`).
- Tests : call-shapes (foreachBatch câblé, clauses ON), spark réel marqué `streaming` : upsert nominal (2 lignes même clé dans un micro-batch → 1 ligne cible), idempotence (rejeu du même batch → pas de doublon), update (nouvelle valeur même clé → écrasée). CHANGELOG.

### Phase 27.5 — Câblage moteur + `full_refresh` + tests E2E
- `core/interpreter.py` : préflight en tête de `process_schema` (~:300) — rejet `intermediate_mode=table`, rejet rules `aggregation` ; boucle tables (~:320-344) : `streaming` → `read_source_stream`/`read_table_stream` (résolution sandbox inchangée) ; défenses run-time (qualify/dev_limit/drop_duplicates_on sur dicts bruts).
- `core/core.py` : `resolve_checkpoint_location(actual_schema, table, materialization)` près de `get_target_schema` (:501) ; `_write_dataframe` (:441-452) : param `materialization=None`, branche streaming → `write_stream_table`.
- `core/patterns.py` : `run_process_to_table` (:34-74) — lire `materialization`, résoudre checkpoint **avant** `process_schema` (fail-fast), monitor inchangé de position (après `awaitTermination()` interne), gardé sur `available_now` ; refus dur streaming dans `run_process_and_split`/`run_union_sources_to_table`.
- `core/core.py` : **`full_refresh(target_layer, target_table_name, checkpoint=None)`** (décision #18) — résout le checkpoint comme `run_process_to_table` (sandbox-aware), supprime le répertoire de checkpoint puis `drop_table` la cible, logs explicites ; le run suivant repart en backfill complet.
- Tests : FakeBackend + `_make_minimal_engine` (modèle test_partials.py) pour dispatch/checkpoint/préflight ; **nouveau `tests/test_streaming.py`** (marqueur `streaming`, spark réel) — **preuve d'incrémentalité en 2 runs** : run 1 = N lignes, append 2 lignes source, run 2 même checkpoint = N+2 en append pur ; **E2E upsert** : 2 runs avec clés en doublon → mise à jour, pas de doublon. CHANGELOG.

### Phase 27.6 — Documentation
CLAUDE.md (bloc YAML :110-173 + prose « ### Streaming tables (Plan 27) » + roadmap), AGENTS.md, docs/core.md (référence schéma + section dédiée modèle partials + run_process_to_table + sandbox + dev local/checkpoints + **pattern sub-layers documenté comme référence de composition de flux**), README.md (:337-443), getting_started (`checkpoint_base`), CHANGELOG finalisé, statut plan → implémenté.

## Risques

| Risque | Mitigation |
|---|---|
| Swallow silencieux `isEmpty` (writer.py:27-32) — DF streaming sur chemin batch « écrit » sans erreur ni données | Triple barrière : bijection au load, branche dédiée `_write_dataframe`, garde `isStreaming` dans `write_table` |
| Corruption de checkpoints entre users | Suffixe sandbox dans le chemin auto ; doc : drop de la cible ⇒ purger le checkpoint |
| Monitor lit une table incomplète | `awaitTermination()` interne garantit l'ordre ; monitor gardé sur `available_now` |
| Quirks Derby local (premier run 0 ligne → pas de `_delta_log`) | Enregistrement metastore conditionné à l'existence du `_delta_log`, sinon skip + warning |
| Sandbox `missing_table=copy` : le stream lit une copie figée | Documenté (comportement attendu en interactif) |
| Évolution de schéma source casse la query append | Documenté ; pas de `mergeSchema` v1 ; remède = purge checkpoint + full refresh |
| Connect v2 sans writeStream | Échec bruyant et actionnable (décision #7), jamais silencieux |
| Rejeu de micro-batch (at-least-once de `foreachBatch`) → doublons | En `append` : documenté (rare, restart après crash) ; en `upsert` : neutralisé par construction (MERGE idempotent sur `keys`) — recommandation doc : upsert dès qu'une clé métier existe |
| MERGE sur cible inexistante au premier run | Create-if-absent dans la branche upsert (testé) |

## Vérification

1. Par phase : `pytest tests/ -x --tb=short`.
2. Unitaires : chaque BLOCKER du tableau (load + run), call-shapes backend, dispatch interpreter/patterns, résolution checkpoint (auto local, explicite, suffixe sandbox, fail-fast Databricks).
3. **E2E local** : `tests/test_streaming.py` — incrémentalité prouvée en 2 runs sur session Delta réelle.
4. Smoke manuel Databricks (post-merge) : job `availableNow` + `checkpoint_base` → run 1 backfill, append, run 2 = delta seul ; `checkpoint: auto` sans `checkpoint_base` → `ValueError` avant toute lecture ; Connect interactif → message de gate.
