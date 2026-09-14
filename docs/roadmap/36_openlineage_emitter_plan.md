# Plan 36 — Émetteur OpenLineage

> Rédigé le 13 septembre 2026. Suite du benchmark [30](30_openmetadata_benchmark.md) (§7.3, §9) et de
> l'annexe différée du [Plan 31](31_lib_development_scenario.md) (§8), après le [Plan 35](35_governance_wiring_plan.md).
> Statut : **D1–D6 adoptées telles que recommandées et GO donné par délégation le 13 septembre 2026** (« je te laisse dérouler »). Branche `feat/plan36-openlineage`, partie de `feat/plan35-governance-wiring` (dépend de 35.2a).
> Tâche mémoire : `skifer:plan:openlineage-emitter`.
> **Changement d'agent (13 septembre 2026, 17:47)** : limite d'usage Codex atteinte au lancement de 36.1 (retour annoncé
> 20:20) ; décision humaine « Passe avec claude » — le dev bascule sur l'adaptateur `claude-dev` à partir de 36.1
> (36.0 a été développé par Codex). La review reste Claude sans outil : même fournisseur, repli déclaré par le noyau.
> **Implémenté (13 septembre 2026)** — 36.0 `fbb316c` · 36.1 `2916a08` + `710e472` + `7333860` (avec 36.1b `609ba0c`) ·
> 36.2 `22dce2e` + `498a5c6` + `7efbbbe` · 36.3 `e2ea53b` + `3268b87` + `72c6a85` + `d3d58ca` (redev 3 exceptionnel autorisé par
> l'humain après escalade : allowlist du nom d'hôte) · 36.4 `1035d2e` + `9118ecd` + `a1f1e56`. Gate Windows vert à chaque commit (248
> échecs connus de la base, aucun nouveau) ; rejeux orchestrateur du vrai chemin (lineage, émetteur, câblage, namespace, doc) verts.
> CI Linux à confirmer à la PR.

## 1. Contexte

Skifer produit ce qu'un catalogue affiche : schéma de sortie, lineage colonne de design, résultats de checks,
contrat versionné, certification calculée. Depuis les Plans 31 et 35, tout est persisté ou reconstructible
sans Spark : `DatasetRecord` (colonnes, classification, owner, lineage), `StoredCheckResult`, `RunEvent` de
certification, définitions de contrat relisibles par hash (35.2a).

Rien ne sort encore vers un catalogue. Choix humain du 13 septembre 2026 : **un émetteur OpenLineage**, neutre
vis-à-vis du catalogue (Marquez, DataHub, Atlan, OpenMetadata via son connecteur), plutôt qu'un adaptateur
OpenMetadata dédié.

Principes hérités (benchmark §7.3, Plan 31 §8) : **Skifer produit, le catalogue reflète** ; non bloquant comme
`uc_mirror.py` ; aucune dépendance obligatoire ; même redaction que les preuves (jamais de SQL, de valeur de
filtre, de valeur observée ni de message d'exception) ; idempotent par `run_id`.

### Rappel du modèle OpenLineage (spec 1.53.0)

- `RunEvent` : `eventTime`, `producer`, `schemaURL`, `eventType` (`START`, `RUNNING`, `COMPLETE`, `ABORT`,
  `FAIL`, `OTHER`), `run.runId` (UUID, v7 recommandé), `job` (`namespace`, `name`), `inputs`, `outputs`.
- Facets de dataset utilisées : `schema` ; `columnLineage` (1-2-0, sur la **sortie** :
  `fields.<col>.inputFields[{namespace, name, field, transformations[{type, subtype, description, masking}]}]`,
  `type` `DIRECT` (`IDENTITY`, `TRANSFORMATION`, `AGGREGATION`) ou `INDIRECT` (`JOIN`, `GROUP_BY`, `FILTER`,
  `SORT`, `WINDOW`, `CONDITIONAL`)) ; `dataQualityAssertions` (1-1-0 : `assertions[{assertion, success}]`
  obligatoires, `column`, `name`, `severity` `error|warn`, `expected`, `actual` optionnels).
- Nommage : Unity Catalog `unitycatalog://{host}` + `{catalog}.{schema}.{table}` ; Hive `hive://{host}:{port}` +
  `{database}.{table}` ; fichier local `file` + `{path}`. Le namespace de job vient de la configuration du client.

## 2. Rayon d'impact déclaré

Obtenu par `codegraph_explore`. Projet piloté par YAML : **plancher, pas périmètre.**

| Zone | Symboles |
|---|---|
| Configuration | `parse_tracing_config` (modèle à suivre), `ConfigurationManager._load_config_file` (`core/config.py`) |
| Construction | nouveau module `observability/openlineage.py` (builder pur + émetteurs) ; `index_schema` (`observability/metadata_index.py`), `LineageEdge`/`LineageGraph` (`lineage/tracker.py`), `DatasetRecord`/`ColumnRecord` (`observability/metadata_store.py`) consommés |
| Publication | `PublicationCoordinator.publish/_publish_run/resume` (`observability/publication.py`) ; `StoredCheckResult`, `CheckResult`, `CheckStatus` consommés |
| Orchestration | `run_process_to_table` (`core/patterns.py`), `SkiferEngine.__init__` (`core/core.py`) |
| Modèle non bloquant | `uc_mirror.mirror_certification`, `_best_effort_build` / `_warning_once` (`observability/tracing_exporters.py`) |
| Tests | nouveau `tests/test_openlineage.py` ; `test_certified_publication.py`, `test_patterns.py`, `test_config.py` |
| Non-code | `CHANGELOG.md`, `docs/observability.md`, un exemple sous `examples/` (exécuté par `tests/test_examples.py`) |

## 3. Sous-tâches

Une sous-tâche = un commit `feat(plan36-N): …`, chacun avec tests et entrée `CHANGELOG.md [Unreleased]`.

| # | Sous-tâche | Fichiers visés | Acceptation | Difficulté |
|---|---|---|---|---|
| 36.0 | **Configuration** `observability.lineage` (globale, comme `tracing`) : `emitter: none \| http` (défaut `none`), `url`, `endpoint` (défaut `/api/v1/lineage`), `job_namespace`, `dataset_namespace`, `timeout_seconds`, validés au chargement ; **clé d'API lue uniquement dans `OPENLINEAGE_API_KEY`**, jamais dans `config.yaml` ; aucune valeur d'URL ni de secret dans une erreur | `core/config.py`, `tests/test_config.py` | config absente = `none`, rien d'importé ni d'émis ; valeur invalide refusée avec message nommant la clé | standard |
| 36.1 | **Builder pur** `build_run_event(...) -> dict` (aucune I/O, aucun Spark) : job (`job_namespace`, nom = FQN cible), run (`runId` = `run_id` Skifer), `inputs` (sources du lineage), `outputs` (cible) avec facets `schema` (colonnes projetées, `logical_type`), `columnLineage` (mapping `edge_type` → type/subtype, D4), `dataQualityAssertions` redactées (D5) et facet custom `skifer` allowlistée (`contract_version`, `definition_hash`, `certification`, classification par colonne) | `observability/openlineage.py`, `tests/test_openlineage.py` | JSON golden déterministe sous tout `PYTHONHASHSEED` ; champs obligatoires présents ; aucune valeur observée, SQL ou message dans l'événement (test canari) | standard |
| 36.1b | **Tracker : résolution des alias dans `select_final` / `add_columns`** (ajoutée le 13 septembre 2026 — décision humaine après rejeu orchestrateur) : `LineageTracker.from_schema` attribuait toute source `alias.col` à la table primaire en gardant le préfixe (`[c.name, customer_name]` → `silver.orders.c.name`), ce qui faussait le lineage du builder 36.1, du registre (`index_schema`), du dictionnaire et des agents. Correctif : `alias.col` → (table de l'alias, `col`) via `alias_to_table` déjà construit pour les joins ; source non qualifiée → table primaire ; sorties de règle → `RULE_ORIGIN` inchangé | `lineage/tracker.py`, `tests/test_lineage_tracker.py`, consommateurs dont la sortie change (exemple 11 si concerné), `CHANGELOG.md` | colonne jointe attribuée à sa table avec un nom nu ; comportement inchangé pour les sources non qualifiées, les littéraux, les règles et les joins | standard |
| 36.2 | **Émetteurs** : Protocol `LineageEmitter`, `NoOpEmitter` (défaut), `InMemoryEmitter` (tests), `HttpEmitter` en bibliothèque standard (D1) — POST JSON `url + endpoint`, `Authorization: Bearer` si la variable d'environnement existe, timeout, avertissement unique rate-limité portant le seul nom de classe, jamais le corps ni la clé | `observability/openlineage.py`, `tests/test_openlineage.py` | serveur HTTP local de test : événement reçu et en-tête présent ; serveur en erreur / injoignable → aucun raise, un seul warning | standard |
| 36.3 | **Câblage** : `SkiferEngine` construit l'émetteur depuis 36.0 ; publication certifiée → `START` au staging puis `COMPLETE` (PROMOTED) ou `FAIL` (QUARANTINED / CHECK_ERROR) sur le même `run_id`, `resume()` → `COMPLETE` ; écriture batch sans `data_product` de `run_process_to_table` → `COMPLETE` (schéma + lineage, sans assertions ni certification) ; streaming, MV, split, union exclus en v1 (D3). **Tout est non bloquant**, un seul flux quel que soit l'émetteur | `observability/publication.py`, `core/patterns.py`, `core/core.py`, tests associés | émetteur qui lève → résultats, exceptions et lignes persistées inchangés ; `none` → zéro appel ; événements dans l'ordre attendu par chemin | complexe |
| 36.4 | **Documentation et exemple** : section `docs/observability.md` (config, mapping, redaction, nommage, compatibilité consommateurs) ; exemple exécutable écrivant les événements d'un pipeline local via `InMemoryEmitter` dans un fichier JSON | `docs/observability.md`, `examples/23_openlineage/`, `CHANGELOG.md` | `mkdocs build --strict` vert ; l'exemple passe dans `tests/test_examples.py` | standard |

Dépendances : 36.0 et 36.1 (indépendantes) → 36.2 → 36.3 → 36.4.

**Routage modèle** (écrasable au GO). Dev Codex `gpt-5.6-sol`, effort `medium` (standard) / `high` (36.3).
Review Claude sans outil, prompt par stdin, plafond notionnel relevé : `claude-sonnet-5`, `claude-opus-5` pour
36.3 **en passes source / tests séparées** ; consignes de review portant les faits vérifiés du code, jamais un
résumé des livrables (leçon du Plan 35).

## 4. Options écartées

- **Adaptateur OpenMetadata dédié** : mapping plus riche (contrat ODCS, data product, tag de certification) mais SDK
  couplé à la version serveur et serveur requis en test ; choix humain en faveur de la neutralité.
- **Émettre depuis des objets vivants (DataFrame, session)** : l'annexe du Plan 31 impose une source Spark-free ;
  le builder consomme un `DatasetRecord` construit par `index_schema`, identique à celui du registre.
- **Émission bloquante ou mode `required`** : viole « rien ne bloque le métier » ; un catalogue indisponible ne fait
  jamais échouer une publication.
- **Clé d'API dans `config.yaml`** : reproduirait le problème des webhooks du Plan 35 (§8).
- **Valeurs `expected` / `actual` dans les assertions** : elles citent souvent les données fautives.

## 5. Décisions à valider (conception)

| # | Question | Recommandation |
|---|---|---|
| D1 | Transport | **HTTP en bibliothèque standard** derrière le Protocol `LineageEmitter`, sans dépendance ; un extra `[openlineage]` (SDK `openlineage-python`, transports Kafka/console) reste un ajout non cassant ultérieur |
| D2 | Cycle d'événements | **`START` au staging puis `COMPLETE` / `FAIL`** sur le même `run_id` (cycle nominal de la spec) ; pas de `RUNNING` |
| D3 | Périmètre v1 | publication certifiée **et** écriture batch sans `data_product` de `run_process_to_table` ; streaming, MV, split, union exclus |
| D4 | Mapping du lineage | `select` → `DIRECT/IDENTITY` si aucune transformation, sinon `DIRECT/TRANSFORMATION` ; `rule` → `DIRECT/TRANSFORMATION` ; `metric` → `DIRECT/AGGREGATION` ; `join` → `INDIRECT/JOIN` ; `description` = noms d'opérations allowlistés, jamais une expression `expr:` |
| D5 | Assertions | `assertion` = type de check, `success` = `PASS`, `column` si portée ligne, `severity` `critical`→`error` / autre→`warn`, `SKIPPED` omis ; **ni `expected`, ni `actual`, ni message** |
| D6 | Nommage | `dataset_namespace` configuré ; défauts : Databricks → `unitycatalog://{host}` (hôte du workspace), local → `skifer://local` ; nom = FQN physique ; `job_namespace` défaut `skifer` |

## 6. Risques

| Risque | Mitigation |
|---|---|
| OpenMetadata consomme OpenLineage via Kafka, pas en HTTP | documenté ; l'extra SDK/Kafka est l'étape suivante si OpenMetadata devient la cible (D1) |
| Divergence de spec / versions de facets | `_schemaURL` épinglés en constantes, tests golden ; montée de version explicite |
| Fuite de donnée ou de secret vers un tiers | builder allowlisté + test canari ; clé en variable d'environnement ; warnings au seul nom de classe |
| Coût à la publication | builder pur, un POST par événement avec timeout court ; aucun `count()` Spark |
| `run_id` en UUIDv4 alors que la spec recommande v7 | la spec exige un UUID ; v7 n'est qu'une recommandation — pas de changement de l'identité d'audit existante |
| Poste Windows (winutils, `:` dans des noms de fichiers) | gate comparé à la base comme au Plan 35 ; tests de l'émetteur 100 % Spark-free |

## 7. Vérification

- Gate de chaque sous-tâche sur ce poste : suite complète comparée à la base enregistrée, aucun échec nouveau ;
  `ruff check src/ --select E4,E7,E9,F` ; `mkdocs build --strict` pour 36.4 ; `tests/test_examples.py` pour l'exemple.
- Périmètre : `git --no-pager show <COMMIT> | grep -E "^-" | grep -vE "^---" | wc -l` proche de zéro.
- Revérification par l'orchestrateur (gate relancé, périmètre, reproduction) avant chaque review.
- CI Linux au push pour les tests Spark.

## 8. Suivi hors plan (constaté pendant le cycle)

| Sujet | Nature | Suite proposée |
|---|---|---|
| `warnings.warn` non protégé sur les chemins best-effort : sous un filtre « warnings as errors » (`python -W error`, `filterwarnings = error`), l'avertissement lève et casse la garantie non bloquante — `publication.py` (6 sites, dont les alertes du Plan 35), `patterns.py:60` | pré-existant, reproduit sur l'émetteur 36.2 (corrigé là) | helper d'avertissement protégé partagé, appliqué aux chemins best-effort existants |
| `alerts.py` : quatre messages d'avertissement recopient `{exc}` ; une erreur réseau cite souvent l'URL du webhook (secret) | pré-existant (Plan 31) | messages au seul nom de classe, comme ailleurs |
| Exemple 11 plante sous Windows (`re.error: bad escape \U` dans `_inject_params` sur un chemin Windows) | pré-existant, dans la base | échapper le remplacement (`re.sub` avec fonction) |
| Lineage colonne OpenLineage : une vraie colonne dont le nom n'est pas un identifiant ASCII (accent, espace, chiffre en tête) est exclue sans avertissement | limite assumée de 36.1 (fail-safe) | documentée en 36.4 ; lever la limite demanderait un nom canonique fourni par le tracker |
| Arêtes de règle du tracker attribuées à la table primaire (étape 4 de `from_schema`) | pré-existant, hors 36.1b | même résolution que 36.1b quand l'analyse AST rend des colonnes qualifiées |
| `LineageTracker.from_schema` ne produit aucune arête pour un pipeline `aggregate:` déclaratif, et les arêtes `join` visent une table source : `DIRECT/AGGREGATION` et `INDIRECT/JOIN` ne sont jamais émis ; un pipeline agrégé n'a ni `columnLineage` ni `inputs` (registre, dictionnaire et impact muets aussi) | pré-existant (Plans 28/31), constaté au rejeu 36.4 | plan dédié au tracker (arêtes `metric` depuis `aggregate:`, clés de jointure vers la sortie) ; limite documentée en 36.4 |
| Deux événements terminaux pour un même `run_id` (FAIL quand la publication lève, puis COMPLETE au `resume()`) | conséquence de D2/D3 | documenté en 36.4 ; un rattachement explicite relèverait d'une révision de D2 |
| Pas de FAIL côté écriture batch non certifiée quand le monitor post-écriture lève (aucun événement) | hors D3 | extension possible de D3 |
| Avec un émetteur réel, le `DatasetRecord` est reconstruit à chaque événement (deux fois par publication certifiée) ; coût résiduel à vide (uuid4, closure) | review 36.3 (MINEUR) | mémoïser par run si le coût devient mesurable |
| `DATABRICKS_HOST` : identifiants bien formés → `skifer://local` (rejet, pas nettoyage) ; hôte terminé par un point rejeté ; nom à un seul label accepté (`dapi0123abcd` deviendrait le namespace) ; IPvFuture sans crochets | décision humaine (redev 3) + review d3d58ca | exiger au moins un point si l'humain le décide ; `dataset_namespace` explicite comme contournement |
| Docstring de `_resolve_lineage_dataset_namespace` : exemple de fuite inexact (`svc:secret@host` bien formé ne fuit pas ; la fuite vient d'un `/`, `?` ou `#` avant `@`) ; aucun test « zéro warning » pour valeur vide / hôte vide | review d3d58ca (MINEUR) | corriger le docstring, ajouter les deux cas |
| Doc OpenLineage : « no snapshot is guaranteed » pour `CHECK_ERROR` alors qu'aucun snapshot n'est jamais écrit ; la phrase sur les assertions d'un `aggregate:` certifié ne rappelle pas l'exception de la publication qui lève (FAIL sans assertions) | review a1f1e56 (MINEUR) | resserrer les deux formulations |

## Sources

- Modèle objet : https://openlineage.io/docs/spec/object-model
- Facet column lineage : https://openlineage.io/docs/spec/facets/dataset-facets/column_lineage_facet
- Facet data quality assertions : https://openlineage.io/docs/spec/facets/dataset-facets/data_quality_assertions/
- Nommage : https://openlineage.io/docs/spec/naming
- Client Python : https://openlineage.io/docs/client/python
