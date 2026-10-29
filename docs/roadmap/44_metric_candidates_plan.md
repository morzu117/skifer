# Plan 44 — Agent de repli, métriques candidates et cycle de certification

> **Statut :** plan rédigé le 28 octobre 2026 à partir du brief de conception du même jour
> (« agent builder de backup et cycle de certification des métriques », 16 décisions actées).
> Les décisions D1 à D9 ci-dessous **contournent les cinq points qui bloquaient** le brief tel
> quel face au code existant. **D1 à D5, D8 et D9 validées le 28 octobre 2026.** D6 révisée le
> même jour (notebook d'arbitrage plutôt qu'API + app : la facilité d'abord). **D7 en suspens
> jusqu'au benchmark de 44.7** : aucun défaut d'index n'est arrêté. Les décisions du brief sont
> reprises telles quelles sauf mention explicite (tableau « Écarts avec le brief »).
> Branche du plan : `memory-improvements`. Rien n'est implémenté.
> **Découpé en 11 specs de développement** (33 slices) dans
> [`44_metric_candidates/`](44_metric_candidates/README.md) — ordre, dépendances, contrats,
> exigences testables. Les écarts que l'écriture des contrats a imposés au présent plan y sont
> listés (« Écarts assumés avec le plan parent ») ; en cas de divergence, la spec fait foi pour
> le *comment*, ce plan pour les *décisions*.

## Objectif

Qu'une question **hors du périmètre du semantic layer** ne finisse plus ni sans réponse, ni
dans une hallucination. Aujourd'hui `GenBIAgent` refuse (à raison) tout nom de métrique qu'il
ne connaît pas ; Genie, lui, répond avec du SQL libre. Entre les deux, rien ne capitalise :
la question est perdue, et le semantic layer n'apprend pas des usages.

Le plan ajoute :

1. un **agent de repli** qui, quand la couche certifiée ne sait pas répondre, construit une
   réponse **dans la grammaire fermée de Skifer** (un schéma de pipeline, pas du SQL libre),
   la vérifie, l'exécute, et la **rend explicitement comme non certifiée** ;
2. un **registre de métriques candidates** (question, structure, preuves, exécutions) et un
   **historique append-only** de chaque changement d'état ;
3. une **chaîne de validation** à trois critères (cohérence, fréquence, stabilité) qui promeut
   automatiquement une candidate au niveau *publié non certifié*, et prépare pour un humain
   sa promotion au niveau *certifié* ;
4. un **graphe de connaissance et un index de métadonnées** propres à Skifer, en Delta
   (SQLite en local), sans Vector Search ;
5. des **signaux de drift d'usage** et un **jeu de questions de référence** qui évalue
   l'index et l'agent, et vit avec le semantic layer.

Le principe directeur reste celui des plans 29 et 43 : **déterministe d'abord**. Le LLM
propose, le code vérifie, et tout ce qui est stocké ou publié a été validé par `load_schema`
et recompilé par `compile_select`.

```text
question ──▶ AgenticHub ──▶ GenBIAgent (modèles certifiés, QueryResolver)
                 │               │ résolu ──▶ réponse « métrique officielle »
                 │               └ needs_clarification sans candidat proche
                 ▼
         MetricCandidateAgent
           1. recherche dans l'index (tables, colonnes, métriques, règles)      knowledge/
           2. LLM → schema_dict dans la grammaire fermée (tables, join, aggregate…)
           3. load_schema ─▶ compile_select ─▶ EXPLAIN ─▶ exécution bornée
           4. réponse « métrique non certifiée » + evidence
           5. candidate enregistrée (fingerprint, structure, run, trace)         candidates/
                 │
                 ▼  job skifer candidates validate
         cohérence · fréquence · stabilité
                 ├─ 3/3 ──▶ promoted (niveau 1) : vue + metric view dans <semantic>_candidates
                 ├─ partiel ──▶ needs_review : CLI / API / app d'arbitrage
                 └─ incohérent ──▶ rejected
                 │
                 ▼  humain : skifer candidates promote
         drafts gérés (pipeline + modèle sémantique) ──▶ PR ──▶ semantic_models/ ──▶ metric view certifiée
                 │
                 ▼  drift d'usage (repli, orphelines, reformulations) ──▶ under_review ──▶ demoted
```

## Constats sur l'existant (28 octobre 2026)

1. **Le cycle « usage → proposition → décision humaine → évaluation » existe déjà** dans
   `adaptive/` (Plan 29) : événements append-only, fingerprints `sha256:v1:…`, seuils nommés
   et explicables, `accept` en `O_EXCL`, `evaluate` qui ne conclut jamais au-delà de ses
   données. Le cycle de vie des candidates et le drift d'usage s'y raccrochent plutôt que
   d'être réinventés.
2. **Mais `adaptive` interdit par construction de stocker la question et le SQL**
   (`usage_event_from_evidence` ne copie ni question, ni SQL, ni valeur de filtre). Le brief a
   besoin de la question d'origine et de la structure de la requête. Les deux ne peuvent pas
   cohabiter dans le même store : il en faut un second, avec une classification explicite (D2).
3. **La source du semantic layer est `semantic_models/*.yaml` dans git**, publiée en metric
   views par `SemanticEngine.publish_metric_views()` (Plan 42). Une promotion automatique qui
   écrirait directement en Unity Catalog ferait diverger le dépôt et ce que Genie sert. Et
   `metric_views.py` refuse les `relationships` (Plan 42 D4) : une candidate avec jointure ne se
   traduit pas en metric view mono-table (D1).
4. **La preuve d'une réponse porte la certification des *sources*, pas de la *métrique*.**
   `SemanticEvidence.sources[].certification_status` et `_certification_text()` dans
   `serving/_response_serializer.py` résument l'état des tables lues. Une réponse de repli
   construite sur des tables certifiées afficherait « certifié le … » — exactement ce que le
   brief interdit (décision 2). Il faut un second axe sur `metrics[]` (D4).
5. **Le routage existe dans `AgenticHub._dispatch`** : intent par mots-clés, puis LLM, puis
   repli vers `GenBIAgent`. Le point d'insertion de l'agent de repli est le résultat
   `needs_clarification` de GenBI quand `_suggest_closest` ne trouve rien d'assez proche (D5).
6. **Le Plan 43 fournit la grammaire d'arrivée et son oracle.** `analyzer/mapper.py` convertit
   un `SELECT` en `schema_dict` sur le catalogue d'opérations fermé (`core/op_catalog.py`), avec
   résidus explicites, et `compile_select` le recompile. L'agent de repli peut donc laisser le
   LLM écrire du SQL **et** n'accepter que ce qui se mappe sans résidu — ou lui demander
   directement le `schema_dict`. Dans les deux cas la sortie est bornée.
7. **Les briques de la couche 2 existent à moitié.** `observability/metadata_store.py` indexe
   les pipelines Skifer (colonnes, lignage, `search_columns` textuel, `_content_fingerprint`),
   `core/catalog_inspector.py` lit Unity Catalog, `lineage/` construit le graphe statique. Il
   manque : les tables UC *hors* Skifer, les métriques et règles comme nœuds, un score de
   recherche (BM25), et la persistance Delta du graphe.
8. **Le tracing est en place** (`observability/tracing.py`, extra `[tracing]` avec MLflow 3) :
   `AgenticHub` ouvre déjà des spans ; `SemanticEvidence.trace_id` existe. Le `mlflow_trace_id`
   du brief est ce champ.
9. **Les stores suivent le catalogue de l'environnement**, pas un nom fixe
   (`default_certification_store` : SQLite en local, Delta dans `<catalog>._skifer_certification`
   sur Databricks). Le brief écrit `skifer.governance.*` ; on garde la convention existante.
10. **Deux détails de DDL du brief ne passent pas tels quels** : `request_count INT DEFAULT 1`
    exige `delta.feature.allowColumnDefaults` ; et `information_schema` est fait de vues, sans
    Change Data Feed. La synchronisation UC → graphe se fait par comparaison d'instantanés
    (fingerprints), et le CDF est activé sur les tables **de Skifer** pour leurs consommateurs.

## Décisions

Les cinq premières répondent aux cinq bloquants identifiés à la lecture du brief.

| # | Décision | Choix | Alternative écartée |
|---|---|---|---|
| D1 | **Deux niveaux de promotion** (bloquant : promotion automatique vs source git, mono-table) | **Niveau 1, automatique, propriété de la machine** : une candidate qui passe les trois critères est publiée dans un schéma UC séparé `<semantic_metric_views_schema>_candidates` sous la forme d'une **vue SQL** (le `compile_select` du schéma candidat, jointures résolues) **et** d'une **metric view mono-table posée sur cette vue** — ce qui reste dans le périmètre de Plan 42 D4 sans y toucher. Tag `skifer.certification = CANDIDATE`. L'espace Genie certifié ne voit pas ce schéma ; l'agent Skifer, si. **Niveau 2, humain, propriété de git** : `skifer candidates promote` émet des **drafts gérés** (pipeline `schemas/.drafts/`, modèle `semantic_models/.drafts/`) via `SemanticDraftBuilder`, puis le chemin existant `semantic sync --promote` → PR → `publish_metric_views`. Ce n'est qu'à ce niveau que la métrique devient « officielle ». | Écrire les YAML dans git depuis un job : commit automatique hors revue, divergence dépôt/UC. Étendre les metric views aux jointures : c'est un plan à part (fanout). |
| D2 | **Un second store, « la structure, pas les valeurs »** (bloquant : confidentialité d'`adaptive`) | `adaptive` reste intact. Le registre des candidates vit dans `<catalog>._skifer_candidates` (SQLite `.skifer_candidates.db` en local), avec sa propre classification documentée (`internal`, accès par GRANT UC). Il stocke la **question** et le **`schema_dict`** de la requête (filtres en `column:operator`, **jamais la valeur** ; les valeurs sont remplacées par des paramètres `{{ p1 }}` pour la ré-exécution de stabilité), le SQL compilé **haché**, et l'identité pseudonymisée par la `TraceAttributePolicy` existante. Rétention configurable (`candidates_retention_days`). Option `candidates_store_questions: full \| redacted` (`redacted` = question hachée + mots-clés retenus par l'index ; la détection de reformulation marche encore, le jeu de référence ne s'enrichit plus automatiquement). | Tout dans `adaptive` : il faudrait casser sa garantie ; un seul test (`test_adaptive_privacy`) deviendrait faux pour tout le module. Ne rien stocker : le cycle de certification n'a plus de matière. |
| D3 | **Sortie de l'agent bornée à la grammaire Skifer** (bloquant : qu'est-ce qu'une « métrique candidate » ?) | L'agent de repli ne produit pas de SQL libre mais un **`schema_dict`** (`tables`, `join`, `filter`/`where`, `aggregate` ou `select_final`) validé par `load_schema`, recompilé par `compile_select`, puis `EXPLAIN` et exécution bornée (`dev_limit`, timeout). Le LLM peut répondre en SQL : il est alors passé dans `analyzer.mapper.map_statement`, et **un résidu (`expr:`, `needs_curation`) rend la candidate exécutable mais jamais auto-promouvable**. Une candidate est donc, par construction, un pipeline Skifer en puissance : la promotion niveau 2 ne traduit rien, elle écrit ce qui est déjà là. | SQL libre + validation syntaxique : rien ne garantit la convertibilité, la promotion deviendrait un travail manuel de réécriture. |
| D4 | **Certification portée par la métrique dans la preuve** (bloquant : l'affichage dit « certifié » pour une réponse de repli) | `SemanticEvidence.metrics[]` gagne `certification: CERTIFIED \| CANDIDATE` et `candidate_id`; `schema_version` passe à `2`. `_certification_text()` prend le **minimum** entre sources et métriques : dès qu'une métrique est `CANDIDATE`, le texte est « **métrique non certifiée** (candidate `<id>`, demandée N fois) » et le libellé « certifié le … » n'apparaît pas. Même preuve, même sérialiseur, un champ de plus — la décision 12 du brief est respectée. | Un bandeau séparé dans la réponse : deux mécanismes à garder cohérents. |
| D5 | **Routage dans le hub, pas dans le superviseur** (bloquant : contradiction avec Plan 41) | La voie rapide est **`GenBIAgent` sur les modèles certifiés** (déterministe), pas Genie. Le repli se déclenche quand GenBI renvoie `needs_clarification` et qu'aucune suggestion n'atteint le seuil de proximité, ou qu'aucun modèle ne couvre la question. Mode `candidate_fallback: off \| shadow \| on` par environnement (`shadow` : la candidate est construite et enregistrée, l'utilisateur reçoit toujours la clarification — pour la phase d'observation). Côté superviseur Agent Bricks (Plan 41), rien ne change : Genie garde l'exploration libre ; ce que l'agent de repli apporte en plus de Genie, c'est une réponse **bornée à la grammaire, vérifiée, tracée et capitalisée**. | Mettre le routeur dans le superviseur : impossible d'y observer la raison du refus de GenBI ; et le superviseur est payant (Model Serving), hors contrainte section 0. |
| D6 | **Arbitrage humain : un notebook sur Databricks, la CLI en local** (section 0 vs décision 7 ; **révisée le 28 octobre 2026**, la facilité d'abord) | La même API Python `skifer.candidates` (`CandidateService` : `list`, `show`, `approve`, `reject`, `promote`, `demote`, `history`, `drift`) est exposée par deux canaux minces. **Sur Databricks : un notebook d'arbitrage** livré dans l'exemple (`examples/25_metric_candidates/notebooks/arbitrage.py`, format source Databricks) — une cellule `display()` des candidates `needs_review` avec leurs critères et chiffres, des widgets `candidate_id` / `decision` / `reason`, une cellule qui applique la décision. Zéro infrastructure, aucun déploiement, tourne sur le compute que l'utilisateur a déjà (notebook serverless à l'usage) ; le décideur est l'utilisateur connecté (`get_clean_username()`, pseudonymisé dans l'historique), donc `decided_by = human:<…>` sans aucune couche d'auth à écrire. **En local : la CLI** `skifer candidates …` sur SQLite. Le suivi (drift, historique) se lit en SQL sur les tables Delta ; un dashboard AI/BI et une Databricks App sont **des options ultérieures**, pas des livrables : ils n'arrivent que si le notebook ne suffit plus. L'agent de repli tourne dans le hub (notebook ou job) ; la validation est un **job serverless** : coût à l'usage, zéro coût fixe. | Apps obligatoire : contredit « sans coût fixe ». API `services/` + routes + app dès la v1 : trois couches à écrire et à sécuriser avant la première décision humaine, pour un besoin que trois cellules couvrent. |
| D7 | **Index : défaut en suspens jusqu'au benchmark** (décision 15) | **Rien n'est arrêté.** BM25 (Python pur, zéro dépendance) est implémenté en 44.1 parce que l'agent a besoin d'une recherche pour fonctionner et que c'est la seule qui marche partout sans installer quoi que ce soit. Les embeddings denses (`intfloat/multilingual-e5-small`, `BAAI/bge-m3`) sont implémentés en 44.8 dans l'extra **`[knowledge-embeddings]`** (import paresseux, poids mis en cache dans un Volume UC parce que HuggingFace est souvent bloqué en entreprise) **pour pouvoir être mesurés**. Le défaut — BM25 seul, dense seul, ou hybride — est tranché après `skifer knowledge benchmark` (44.7), sur le jeu de référence, et consigné ici. | Décider maintenant, dans un sens ou dans l'autre : c'est exactement ce que la décision 16 du brief (benchmark maison) demande de ne pas faire. |
| D8 | **Synchronisation UC → graphe par instantanés** (décision 9 ajustée) | `knowledge/sync.py` lit `information_schema.{tables,columns}` (et `system.access.table_lineage` quand accessible), calcule un fingerprint par nœud et n'écrit que les différences. Le CDF est **activé sur les tables de Skifer** (graphe, index, candidates, historique) pour qui veut les consommer en incrémental. | CDF sur les tables système : elles n'en ont pas. |
| D9 | Version | Nouvelle fonctionnalité → **4.1.0**, décidée par l'utilisateur au moment de la release. | — |

### Écarts avec le brief

| Décision du brief | Sort dans ce plan |
|---|---|
| 1 Routeur : certifié → Genie, sinon backup | **Ajustée (D5)** : certifié → GenBI/QueryResolver (déterministe) ; Genie reste l'exploration du Plan 41. |
| 7 Databricks Apps obligatoire | **Ajustée (D6)** : notebook d'arbitrage sur Databricks, CLI en local ; app et dashboard en options ultérieures. |
| 8 Index maison, pas Vector Search | Maintenue. |
| 9 CDF pour la sync | **Ajustée (D8)** : instantanés pour UC → graphe ; CDF sur les tables Skifer. |
| 10 MLflow retenu, Agent Framework écarté | Maintenue (l'endpoint du Plan 41 reste **optionnel**, il n'est pas requis par ce plan). |
| 11 Promotion automatique | **Précisée (D1)** : automatique au niveau 1 (publié non certifié), humaine au niveau 2 (certifié, git). |
| 12 Même mécanisme d'affichage | Maintenue (D4) : un champ de plus dans la preuve. |
| 15 `e5-small` par défaut | **En suspens (D7)** : les deux modèles sont implémentés en option pour le benchmark ; le défaut est tranché après 44.7. |
| Tables `skifer.governance.*` | Convention existante : `<catalog>._skifer_candidates`, SQLite en local. |
| `request_count DEFAULT 1` | Défaut appliqué dans le code du store, pas dans le DDL. |
| Toutes les autres (2, 3, 4, 5, 6, 13, 14, 16) | Maintenues telles quelles. |

## Périmètre v1 et exclusions

**Dans la v1** : agent de repli sur `aggregate`/`select_final` mono- et multi-tables avec
jointures equi, filtres du catalogue, `where:` post-jointure ; registre + historique ; chaîne de
validation et promotion niveau 1 ; arbitrage par notebook (Databricks) et CLI (local), promotion
niveau 2 par drafts ; graphe + index BM25, embeddings denses en option pour le benchmark ; trois
signaux de drift **calculés et exposés, sans action automatique** (décision 14) ; jeu de
référence et évaluation MLflow ; benchmark d'embedding.

**Hors v1** : rétrogradation automatique sur seuil (les seuils se calibrent sur la phase
d'observation) ; metric views avec jointures (plan suivant) ; fenêtres, pivots, UDF dans les
candidates (résidu → `needs_curation`, jamais promu) ; sync du lineage de colonnes UC
(`system.access.column_lineage`) ; **routes `api/` et Databricks App d'arbitrage** (repris
seulement si le notebook ne suffit plus) ; dashboard AI/BI de suivi.

## Modèle de données

Deux stores, `Sqlite*`/`Delta*` comme partout, accès par Protocol. Noms de colonnes en anglais
comme les stores existants.

**`metric_candidates`** — une ligne par candidate (upsert sur `candidate_id`, dérivé du
contenu : `candidate:v1:<sha256(fingerprint)>`) : `candidate_id`, `metric_name`,
`metric_description`, `origin_question` (ou son hash en mode `redacted`), `schema_dict`
(JSON, valeurs de filtre paramétrées), `sql_hash`, `fingerprint` (`sha256:v1:…`, même
discipline qu'`adaptive`), `tables_used`, `columns_used`, `joins_used`, `business_rules_refs`,
`residues` (JSON, vide = auto-promouvable), `state`, `request_count`, `stability_score`,
`coherence` (JSON : verdict + raisons nommées), `trace_id`, `reviewer` (pseudonymisé),
`published_view`, `published_metric_view`, `created_at`, `updated_at`, `last_requested_at`.

**`metric_candidate_runs`** — append-only : `run_id`, `candidate_id`, `executed_at`,
`result_hash`, `row_count`, `duration_ms`, `trace_id`, `error_type`, `purpose`
(`answer | stability | evaluation`).

**`metric_certification_history`** — append-only, jamais modifié : `event_id`, `metric_id`,
`metric_name`, `event_type`, `previous_state`, `new_state`, `decided_by`
(`human:<pseudonyme> | agent:<chaîne> | system:drift`), `justification`, `evidence` (JSON des
chiffres au moment de la décision), `replaced_by_metric`, `replaces_metric`,
`semantic_layer_version` (hash du `semantic_catalog.yaml`), `trace_id`, `event_at`.

**`reference_questions`** — `question_id`, `question`, `family` (`A | B | C`), `expected_model`
/ `expected_metric` (A), `expected_schema_dict` + `expected_result_hash` (B), `expected_outcome`
(C : `refuse | clarify`), `source` (`yaml_bootstrap | promoted | rejected | human`),
`semantic_layer_version`, `created_at`.

**États** (fermés, transitions vérifiées dans `candidates/models.py`) :

```text
candidate ─▶ validated ─▶ promoted ─▶ under_review ─▶ promoted | demoted
    │            │                                        │
    ▼            ▼                                        ▼
 rejected    needs_review ─▶ promoted | rejected      candidate (retour possible)
```

`promoted` = niveau 1 (publié non certifié). Le passage au certifié n'est **pas** un état du
registre : c'est la présence du modèle dans `semantic_catalog.yaml` ; le registre l'enregistre
par un événement `certified` avec `replaces_metric` si une métrique certifiée est remplacée.

## Points

Chaque point est un commit séparé (`feat(plan44-N): …`), avec ses tests et son entrée
`CHANGELOG.md` sous `## [Unreleased]`. Ne pas toucher à la version dans `pyproject.toml`.
Après chaque point : `pytest tests/ -x --tb=short && ruff check src/`. Tout import de
`sentence-transformers`, `mlflow` ou du SDK Databricks est paresseux ; le cœur s'importe sans
eux (test par `sys.modules`, comme `tracing` et `analyzer`).

### 44.0 — Modèles, états et stores des candidates

**But** : le registre et l'historique, sans agent — pour que tout ce qui suit écrive dans
quelque chose de testé.

- `src/skifer/candidates/__init__.py`, `models.py` : dataclasses `MetricCandidate`,
  `CandidateRun`, `CertificationEvent`, `ReferenceQuestion` ; validation à la construction
  (datetimes aware, états dans l'énumération, transition autorisée) ; `to_dict()` par allowlist.
  `fingerprint_candidate(schema_dict)` réutilise la discipline d'`adaptive.fingerprint_query`
  (ordre non sémantique ignoré, valeurs de filtre exclues).
- `store.py` : `CandidateStore` (Protocol), `SqliteCandidateStore`, `DeltaCandidateStore`
  (schéma `<catalog>._skifer_candidates`, `TBLPROPERTIES (delta.enableChangeDataFeed = true)`),
  `default_candidate_store(engine)` sur le modèle de `default_certification_store`. L'historique
  n'a que `append` et `list` — aucune méthode d'update n'existe, et un test l'épingle au niveau
  de la source. `request_count` par défaut dans le code.
- `redaction.py` : paramétrage des valeurs de filtre du `schema_dict` (`{{ p1 }}`…), hash de
  question, pseudonymisation via `TraceAttributePolicy`.
- Config : `candidates_store_questions` (`full` par défaut), `candidates_retention_days`.
- Tests : `tests/test_candidates_models.py` (transitions refusées), `tests/test_candidates_store.py`
  (SQLite ; Delta via backend factice), `tests/test_candidates_redaction.py` (aucune valeur de
  filtre ne survit dans la ligne stockée — même test que `test_adaptive_privacy`, appliqué ici).

### 44.1 — Graphe de connaissance et index BM25 (`skifer/knowledge/`)

**But** : la couche 2 du brief, interrogeable sans Spark en local.

- `knowledge/models.py` : `Node` (`kind: table | column | metric | rule | domain`, `fqn`,
  `label`, `text`, `status: certified | candidate | reference`, `fingerprint`), `Edge`
  (`kind: has_column | joins | derives | belongs_to | shortcut`, `weight`, `source`).
- `knowledge/store.py` : `Sqlite*`/`Delta*`, `<catalog>._skifer_knowledge`, upsert par
  fingerprint (rien n'est réécrit si rien n'a changé).
- `knowledge/sync.py` : sources dans l'ordre — pipelines Skifer (`metadata_store`), modèles
  sémantiques (`semantic_catalog.yaml` → nœuds `metric` certifiés), règles business
  (`RuleRegistry` + docstrings), Unity Catalog (`information_schema`, et
  `system.access.table_lineage` si lisible ; sinon rapport « lineage UC indisponible », pas
  d'erreur). Nœuds `domain` = schéma UC, pour la recherche hiérarchique. **Arêtes de raccourci**
  calculées depuis les jointures des pipelines et des candidates promues.
- `knowledge/index.py` : BM25 (Okapi, k1/b fixés et documentés), tokenisation qui coupe le
  `snake_case` et le camelCase, champs pondérés (nom > synonymes > description). `search(query,
  k, kinds, domain)` renvoie des `(node, score, matched_terms)` — les termes matchés servent à
  l'explicabilité et à la détection de reformulation (44.6). Traversée bornée
  (`max_depth`, `min_weight`) pour `neighbors()`.
- CLI : `skifer knowledge sync [--catalog …]`, `skifer knowledge search "…" [--kind …]`.
- Tests : fixtures de schémas/modèles d'`examples/` ; un test montre qu'une table inconnue de
  Skifer mais présente dans UC (backend factice) devient un nœud `reference`.

### 44.2 — Agent de repli : `agentic/candidate_agent.py`

**But** : la réponse hors périmètre, bornée, vérifiée, tracée.

- `MetricCandidateAgent(hub_deps…)` : (1) `knowledge.search` sur la question → contexte compact
  (tables et colonnes candidates, métriques voisines, règles pertinentes) ; (2) prompt système
  qui demande un **`schema_dict` JSON** dans la grammaire (clés autorisées listées, extraits de
  `core/json_schema.py`), ou à défaut un `SELECT` ; (3) un `SELECT` passe par
  `analyzer.mapper.map_statement` (extra `[analyzer]` requis pour ce chemin, sinon le mode SQL
  est désactivé) ; (4) `load_schema` → `compile_select` → `EXPLAIN` → exécution avec `dev_limit`
  et timeout ; boucle de correction : 3 essais, l'erreur du validateur renvoyée au LLM, comme
  `SemanticBuilder`. (5) Refus explicites : aucune table trouvée dans l'index, jointure sans clé
  identifiable, fanout détecté par `find_unsafe_fanout`, résidu en mode `--strict`. Le refus
  nomme ce qui manque — c'est la famille C du jeu de référence.
- Sortie : `AgentResponse` en `mode="candidate"` avec `SemanticEvidence` dont `metrics[]` porte
  `certification="CANDIDATE"` et `candidate_id` (le champ est ajouté ici ; `schema_version`
  → `2`, `to_dict` l'inclut par allowlist). Candidate et run persistés dans le store ; la
  `trace_id` du span `skifer.candidate.build` est stockée.
- Tests : LLM factice qui renvoie successivement un `schema_dict` invalide puis valide ; un SQL
  avec `LAG` → candidate avec résidu ; question sans table → refus nommé ; aucune valeur de
  filtre dans la ligne stockée ; `schema_version == "2"` et rétrocompatibilité de la lecture
  d'une evidence `"1"`.

### 44.3 — Routage dans le hub et affichage de confiance

**But** : brancher l'agent sans changer le comportement par défaut.

- `AgenticHub._dispatch` : après `_fallback_to_genbi()`, si la réponse est
  `needs_clarification` et que `suggestions` est vide ou sous `candidate_fallback_min_similarity`,
  et que `candidate_fallback != off` → `MetricCandidateAgent.ask()`. En `shadow`, la candidate est
  construite et enregistrée mais la réponse renvoyée est celle de GenBI, enrichie d'une mention
  « une réponse non certifiée a été préparée (candidate `<id>`) ».
- `serving/_response_serializer.py` : `_certification_text()` lit `metrics[].certification` ;
  toute métrique `CANDIDATE` → « métrique non certifiée … », jamais « certifié le ». `hub_response_to_markdown`
  préfixe la réponse en mode `candidate`.
- `serving/_agent_core.py` : rien à changer si l'allowlist de l'evidence est la seule porte —
  test qui le vérifie.
- Config par environnement : `candidate_fallback` (`off` par défaut), `candidate_fallback_min_similarity`.
- Tests : `tests/test_hub.py` (les trois modes), `tests/test_response_serializer.py` (une réponse
  candidate sur deux tables certifiées n'affiche pas « certifié le »).

### 44.4 — Chaîne de validation et promotion niveau 1

**But** : la décision 11 du brief, avec le niveau 1 de D1.

- `candidates/validator.py` : trois critères, chacun renvoyant verdict + raisons nommées + chiffres
  (même forme que `RecommendationRefusal`) :
  - **cohérence** : `load_schema` passe, aucune `residue`, chaque jointure correspond à une arête
    `joins` du graphe ou à une `relationship` d'un modèle, pas de fanout, règles référencées
    existantes ;
  - **fréquence** : `request_count` ≥ `candidates_min_requests` **sur des demandeurs distincts**
    (pseudonymes) et sur ≥ `candidates_min_days` jours ;
  - **stabilité** : N ré-exécutions (`purpose=stability`) avec les mêmes paramètres →
    `stability_score` = part de `result_hash` identiques ; seuil `candidates_min_stability`.
- `candidates/promotion.py` : niveau 1 = `CREATE OR REPLACE VIEW <schema>_candidates.<name>` à
  partir de `compile_select`, puis `metric_view_ddl()` (Plan 42, inchangé) sur cette vue,
  `COMMENT` avec provenance (`candidate_id`, hash) et tag `skifer.certification = CANDIDATE`.
  Événement `promoted` avec `decided_by = agent:validator_v1` et `evidence` complète. Politique
  de certification de l'environnement appliquée aux **sources** de la vue (gate existant) : une
  candidate sur une table non certifiée en `enforce` reste `needs_review`.
- Job : `skifer candidates validate [--dry-run]` (code `0` / `1` technique / `2` au moins une
  candidate en `needs_review`, pour qu'un job puisse notifier).
- Tests : chaque critère isolément avec seuils injectés ; promotion via backend factice qui
  enregistre les DDL ; une candidate avec résidu n'est jamais promue quels que soient ses
  chiffres.

### 44.5 — Arbitrage humain, promotion niveau 2, rétrogradation

**But** : la décision est humaine pour tout ce qui touche au certifié — par le canal le plus
simple qui existe là où l'on est (D6).

- `candidates/service.py` : `CandidateService(store, engine=None)` — **l'unique implémentation**
  des décisions, appelée telle quelle par la CLI et par le notebook : `list(state=…)`,
  `show(id)`, `approve(id, reason)` (→ `promoted` niveau 1 pour une `needs_review`),
  `reject(id, reason)`, `promote(id, pipeline_output, model_output)` (→ drafts gérés :
  `schema_dict` écrit par `write_yaml_atomic` avec marqueur `_generated_by: metric_candidates` ;
  modèle sémantique par `SemanticDraftBuilder` depuis la projection du pipeline ; refus `O_EXCL`
  d'écraser ; événement `certification_requested`), `demote(id, reason, replaced_by=None)`
  (→ `demoted`, `DROP VIEW` des objets niveau 1), `history(id)`. Le décideur est passé par
  l'appelant et pseudonymisé par le service ; chaque méthode renvoie un DTO sérialisable par
  allowlist (pour `display()` comme pour le JSON de la CLI). Erreurs typées
  (`StaleCandidateError`, `StateConflictError`, `OutputExistsError`).
- CLI `skifer candidates list|show|approve|reject|promote|demote|history` : adaptateur mince
  sur le service. Codes de sortie alignés sur `adaptive` : `0`, `1` technique, `2` usage, `3`
  candidate périmée (le `semantic_layer_version` a changé depuis sa validation : revalider),
  `4` conflit d'état ou sortie existante.
- **Notebook d'arbitrage** `examples/25_metric_candidates/notebooks/arbitrage.py` (format
  source Databricks, trois cellules utiles) : (1) `display()` des candidates `needs_review` —
  question, tables, critères avec chiffres et raisons, `request_count`, `stability_score` ;
  (2) widgets `candidate_id`, `decision` (`approve | reject | promote | demote`), `reason` ;
  (3) appel du service avec `get_clean_username()` comme décideur, puis `display()` de
  l'historique de la candidate. Une cellule optionnelle montre `drift()` sur la fenêtre
  courante. Le notebook est **exécuté dans la suite** comme script Python contre SQLite et un
  `dbutils` factice (c'est un exemple, donc un livrable testé), pour que ses cellules ne
  cassent pas en silence.
- Hook de certification : quand `semantic sync --promote` publie un modèle dont le
  `metadata.source_candidate_id` est renseigné, événement `certified` dans l'historique.
- Tests : service bout en bout sur SQLite (chaque transition, chaque erreur typée) ; CLI
  (codes de sortie) ; `promote` refuse d'écraser ; `history` reconstitue « X rétrogradée au
  profit de Y » ; le notebook s'exécute.

### 44.6 — Signaux de drift d'usage (observation seule)

- `candidates/drift.py` : sur une fenêtre `[since, until)` et une partition
  (environnement) : **taux de repli** (réponses `candidate` / total, depuis les runs et les
  événements du hub), **métriques certifiées orphelines** (modèles du catalogue sans usage dans
  `adaptive` sur la fenêtre — jointure par `model_key`), **reformulations** (candidates dont les
  `matched_terms` de l'index recouvrent ≥ `x` % d'une métrique certifiée). Chaque signal
  renvoie valeur, fenêtre, taille d'échantillon ; sous `candidates_min_sample`, `inconclusive`.
- Aucune transition automatique : `under_review` ne se déclenche que par `skifer candidates review
  ID --signal …` (humain) tant que `candidates_drift_thresholds` est absent de la config.
  La config accepte les seuils dès maintenant pour la calibration future, sans les appliquer.
- CLI `skifer candidates drift [--since … --until …] [--json]` ; route API en lecture.
- Tests : fenêtres demi-ouvertes (aucun événement compté deux fois), `inconclusive` sous le
  seuil d'échantillon, reformulation détectée sur un cas construit.

### 44.7 — Jeu de questions de référence et évaluation

- `candidates/reference.py` : `bootstrap_from_models()` (famille A : une à deux questions par
  métrique certifiée, générées par gabarit déterministe depuis `description` et `synonyms` —
  pas par LLM, pour rester reproductible), boucle automatique : `promoted` niveau 2 → famille A
  avec `expected_schema_dict` ; `rejected` pour incohérence → famille C. Import manuel
  `skifer candidates reference add --family B …`.
- `skifer candidates evaluate [--mlflow]` : famille A → rappel@5/10 de l'index et exactitude de
  routage (GenBI résout) ; B → `result_hash` identique à la référence sur échantillon ; C → taux
  de refus correct (vrai refus / faux refus / fausse réponse). Sans `--mlflow`, rapport JSON ;
  avec, run MLflow (extra `[tracing]`) qui logge les métriques, la version du jeu et le
  `semantic_layer_version`. Une dégradation sur A ou B est **listée** comme signal de drift
  supplémentaire dans `drift`.
- `skifer knowledge benchmark --models …` : mêmes questions, rappel@k et rang moyen par
  modèle d'embedding et pour BM25 seul ; c'est ce rapport qui tranche D7.
- Tests : bootstrap déterministe (deux appels → mêmes questions) ; évaluation sur un hub factice.

### 44.8 — Embeddings denses en option, pour le benchmark (`[knowledge-embeddings]`)

**But** : rendre la mesure possible, pas trancher (D7). Ce point précède l'exécution du
benchmark de 44.7 ; le défaut de l'index est décidé ensuite et consigné dans ce plan.

- `pyproject.toml` : extra `knowledge-embeddings = ["sentence-transformers>=3"]`.
- `knowledge/embeddings.py` : import paresseux, `multilingual-e5-small` (préfixes
  `query:`/`passage:`) et `bge-m3` sélectionnables par config ; cache des poids dans
  `knowledge_model_cache` (chemin local ou Volume UC) ; erreur explicite nommant l'extra et le
  cache si le modèle n'est pas disponible hors ligne.
- Store : colonne `embedding ARRAY<FLOAT>` + `embedding_model` ; seules les lignes dont le
  fingerprint a changé sont ré-embeddées. Trois modes de recherche exposés au benchmark :
  `bm25`, `dense`, `hybrid` (fusion de rangs RRF, en Python sur quelques milliers de lignes).
  Tant que D7 n'est pas tranchée, la config `knowledge_search_mode` n'a pas de défaut
  documenté autre que « ce qui est installé » : `bm25` sans l'extra.
- Tests : le cœur s'importe sans `sentence_transformers` ; fusion de rangs sur des scores
  injectés ; modèle factice pour le chemin d'indexation.

### 44.9 — Exemple complet et documentation

- `examples/25_metric_candidates/` : un hub local (`catalog: null`) avec deux modèles
  certifiés, une question hors périmètre qui produit une candidate, `validate` qui la promeut
  au niveau 1 (SQLite + Spark local), `promote` qui émet les drafts, `drift` et `evaluate` sur
  le jeu amorcé ; `notebooks/arbitrage.py` (44.5) y est rangé et documenté, avec la marche à
  suivre pour l'importer dans un workspace.
- `docs/metric_candidates.md` (nav « Semantic & agentic ») : pourquoi, les deux niveaux de
  promotion, ce qui est stocké et ce qui ne l'est jamais, configuration, l'arbitrage (notebook
  sur Databricks, CLI en local, et pourquoi pas d'app en v1), codes de sortie, signaux de
  drift, jeu de référence, benchmark et décision D7 une fois prise, limites.
  `docs/databricks_supervisor.md` : ce que l'agent
  de repli ajoute à Genie. `docs/adaptive.md` : renvoi et frontière (« `adaptive` ne stocke pas
  la question ; `candidates` oui, et voici pourquoi »). `CLAUDE.md` / `AGENTS.md` : lignes de
  structure `candidates/`, `knowledge/`, `candidate_agent.py` ; section courte.
- `mkdocs build --strict`.

## Risques

| # | Risque | Parade |
|---|---|---|
| R1 | Une metric view n'accepte pas une **vue** comme `source` | À vérifier en premier sur `sts_demo_catalog` (44.4). Repli : publier la vue SQL seule au niveau 1 ; la metric view n'arrive qu'au niveau 2 (mono-table après matérialisation par le pipeline promu). |
| R2 | Le LLM ne produit pas de `schema_dict` fiable | Le chemin SQL + `map_statement` est le repli (D3) ; les deux chemins passent par le même validateur. Mesuré par la famille B du jeu de référence. |
| R3 | `information_schema` trop lent ou interdit sur un gros catalogue | Sync par schéma (`--schema`), bornée aux domaines déclarés dans la config ; `system.access.*` optionnel. |
| R4 | `sentence-transformers` n'installe pas sur le driver (taille, réseau) | Extra optionnel, cache Volume, BM25 toujours fonctionnel ; si le benchmark lui-même ne peut pas tourner sur le workspace cible, il tourne en local sur un export du graphe (Delta → SQLite), le jeu de référence étant le même. |
| R8 | Le notebook d'arbitrage ne suffit plus (plusieurs arbitres, besoin de traçabilité d'écran, file d'attente) | C'est le signal pour reprendre les routes `api/` et l'app, **sur le même `CandidateService`** — rien du notebook n'est à jeter. |
| R5 | La question stockée contient une donnée personnelle | Mode `redacted` (D2), rétention, pseudonymisation ; documenté comme classification `internal`. |
| R6 | Boucle : une candidate promue niveau 1 est reproposée comme candidate | Le routeur consulte d'abord les candidates `promoted` (fingerprint et index) : une question déjà couverte réutilise la candidate (`request_count` += 1) au lieu d'en créer une. |
| R7 | Divergence entre la vue niveau 1 et le pipeline promu niveau 2 | Les deux viennent du même `schema_dict` via `compile_select` ; test d'égalité des SQL compilés. |

**R1 levé — vérifié en live le 28 octobre 2026 (S05.1).** Sur `sts_demo_catalog`, warehouse
Pro/Serverless : une metric view accepte une vue comme `source`, `MEASURE()` la relit (BE = 7.5,
FR = 15.0 sur le jeu de test), et `ALTER VIEW … SET TAGS` s'applique à la metric view.
`LEVEL1_PUBLISHES_METRIC_VIEW = True`. **Un écart en est sorti** : Unity Catalog refuse `.` et `=`
dans une clé de tag (`Tag key contains reserved characters (., =): skifer.certification`) ; la clé
`skifer.certification` de D1 et de 44.4 ne peut donc pas exister. Le niveau 1 pose
`certification = CANDIDATE`, la clé que le miroir de tags du Plan 41 utilise déjà.

## Vérification

- Gate complet : `pytest tests/ -x --tb=short` avec Java, `ruff check src/`, `mkdocs build --strict`.
- Local : `examples/25_metric_candidates/` de bout en bout.
- Live (après 44.4) : sur `sts_demo_catalog`, une question hors modèles → candidate → `validate`
  → vue + metric view dans `semantic_candidates`, `MEASURE()` sur la vue, et la réponse du hub
  affiche « métrique non certifiée ». Puis `promote` → drafts → `semantic sync --promote` →
  `publish_metric_views` → la même question passe désormais par GenBI et affiche « certifié ».
