# SkiferUI — Carte des features, entrées/sorties et emplacement

> Document de référence destiné au dépôt SkiferUI et à la revue de la première version de l'UI.
> Rédigé le 7 septembre 2026 depuis l'état réel de la librairie `skifer` (branche
> `plan-29-feat-07`) et des plans [02_client_graphique_plan.md](02_client_graphique_plan.md) et
> [11_gui_framework_prerequisites_plan.md](11_gui_framework_prerequisites_plan.md).
> Le scénario de développement côté librairie est dans [31_lib_development_scenario.md](31_lib_development_scenario.md).

---

## 1. Objet

SkiferUI est un client no-code qui permet de créer et faire vivre des pipelines, des règles, des
contrats et des modèles sémantiques Skifer sans écrire de YAML ni de Python. Ce document
répond à trois questions pour chaque feature : **quelles entrées, quelles sorties, et où vit la
logique** (librairie existante, librairie à créer, API locale, UI).

Règle absolue : **l'UI ne contient aucune logique métier.** Toute validation, projection, exécution,
décision de certification vit dans la librairie et lui est demandée via l'API locale. L'UI affiche,
saisit, orchestre des appels et rend visibles les états. Si une règle de validation n'existe pas dans
`schema_loader`, l'UI ne l'invente pas : elle est ajoutée à la librairie d'abord.

---

## 2. Architecture cible

```text
┌──────────────────────────────────────────────────────────────────────────────┐
│  SkiferUI  (Electron + React/TypeScript)               dépôt skiferui │
│  modules M1…M12 · client TypeScript généré depuis l'OpenAPI · aucun métier    │
└──────────────────────────────┬───────────────────────────────────────────────┘
                               │ HTTP localhost (FastAPI lancée par Electron)
┌──────────────────────────────┴───────────────────────────────────────────────┐
│  skifer.api   extra [api]                     dépôt skifer │
│  routes fines 1:1 sur services/ · OpenAPI versionnée · scopes sur chaque route │
├──────────────────────────────────────────────────────────────────────────────┤
│  skifer.services   (Feature 31.1)                                   │
│  Project · Rule · Execution · Governance · Quality · Semantic · Identity       │
│  RequestContext + scopes · vues dataclass .to_dict() · jamais d'objet Spark    │
├──────────────────────────────────────────────────────────────────────────────┤
│  librairie : core · lineage · observability · semantic · agentic · mcp        │
│  stores SQLite (local) / Delta (Databricks) · SparkSession · Unity Catalog     │
└──────────────────────────────────────────────────────────────────────────────┘
```

Trois décisions déjà actées dans le plan 2 et conservées : **Electron** (application desktop), **API
FastAPI locale lancée par Electron** (pas de serveur déployé en v1), **anglais uniquement** pour l'UI v1.
Une décision nouvelle : **l'API vit dans le dépôt de la librairie** derrière un extra `[api]`, versionnée
avec elle, et l'UI consomme l'OpenAPI générée. Le dépôt UI ne contient que du TypeScript.

---

## 3. Connexion à Databricks et modes d'exécution

### 3.1 Comment la librairie se connecte

La librairie ne connaît qu'un runtime : Spark. `spark_factory.get_spark_session()` détecte le contexte
dans cet ordre, et l'UI doit **afficher le mode obtenu** :

| Ordre | Mode | Condition | Ce que ça implique pour l'UI |
|---|---|---|---|
| 1 | `databricks_notebook` | session Spark active (on tourne dans Databricks) | N'arrive jamais depuis Electron |
| 2 | `databricks_connect` | `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_CLUSTER_ID` présents dans l'environnement ou un `.env` | Cluster distant, init 10 à 30 s, Unity Catalog 3-parties, sandbox actif |
| 3 | `local` | rien de ce qui précède | PySpark `local[*]` + Delta + Derby, entrepôt `.spark-warehouse/`, FQN 2-parties, nécessite Java et l'extra `[spark]` |

L'environnement logique (LOCAL / DEV / QA / PROD) vient de `config.yaml` : `priority_check` est parcouru
dans l'ordre, le premier catalogue accessible gagne, `catalog: null` désigne le mode local, `is_production`
coupe le sandbox et les `dev_limit`. `SkiferEngine(config_path=…, force_env=…)` permet de **forcer un
environnement** : c'est le sélecteur d'environnement de l'UI. Chaque environnement peut porter des `params`
par défaut injectés dans les `{{ placeholders }}` des YAML.

Deux clients Databricks distincts coexistent et l'UI doit le savoir :

- **la session Spark** (Connect v2) exécute les pipelines, les previews, les checks ;
- **le workspace client** (`environment.get_workspace_client()`, extra `[databricks]`) sert au SQL
  warehouse : les materialized views exigent `params.sql_warehouse_id` et un warehouse Pro/Serverless,
  jamais le cluster all-purpose. Sans warehouse, la librairie génère un `.sql` dans `generated_sql/` et
  avertit « NOT CREATED ».

### 3.2 Ce que l'UI gère, et ce qu'elle ne gère pas

| L'UI fait | L'UI ne fait pas |
|---|---|
| Choisir un dossier projet contenant `config.yaml`, `schemas/`, `rules/`, `semantic_models/`, `calendars/` | Stocker un secret : les credentials restent dans `.env` / variables d'environnement, lus par le processus API |
| Choisir ou forcer l'environnement (`force_env`) | Ouvrir une session Spark elle-même |
| Afficher mode Spark, environnement, catalogue, utilisateur, suffixe sandbox (`.skifer_user`) | Contourner le sandbox ou `is_production` |
| Relancer la session quand la config change (une session par config active, plan 2) | Maintenir un pool de sessions |
| Afficher « Connecting to Spark… » puis l'état `ready` / `failed` avec la cause | Réessayer en silence |

États de connexion à rendre visibles : `no_project` → `project_opened` (services sans Spark disponibles :
validation, projection, lineage statique, règles) → `spark_connecting` → `spark_ready` / `spark_failed`.
**La majorité des écrans fonctionne sans Spark** ; seuls le catalogue, l'exécution, la sémantique en
requête et la qualité sur données réelles l'exigent. L'UI doit dégrader proprement, pas bloquer.

### 3.3 Authentification v1 et v2

v1 (plan 2, conservée) : token PAT dans `.env`, bouton « Connect with Databricks » purement visuel.
v2 : OAuth PKCE Databricks. Indépendamment, l'API locale attribue à l'utilisateur une **identité locale**
(`get_clean_username()`) avec des scopes nommés ; c'est la même identité que le serveur MCP en stdio.
Un client ne peut jamais s'attribuer un scope lui-même.

---

## 4. Catalogue des features

Légende de la colonne **Emplacement** : `lib ✓` existe dans la librairie · `lib →31.N` à créer, voir la
feature N du scénario 31 · `api` route fine · `ui` rendu et saisie uniquement.
**État** : servable aujourd'hui / partiel / manque.

### M1 — Projet et connexion

| Feature | Entrées | Sorties | Emplacement | État |
|---|---|---|---|---|
| Ouvrir un projet | chemin dossier | arborescence (`config.yaml`, pipelines, règles, modèles, calendriers), env détectés, erreurs de config | `lib →31.1` ProjectService.open · `api` · `ui` | manque |
| Sélecteur d'environnement | nom d'env | env actif, catalogue, `is_production`, `params` | `lib ✓` ConfigurationManager + `force_env` · `lib →31.1` | partiel |
| Session Spark | config active | mode (`local` / `databricks_connect`), état, durée d'init, message d'échec | `lib ✓` spark_factory · `lib →31.6` ExecutionService.session | partiel |
| Identité et scopes | — | subject, scopes accordés | `lib →31.1` LocalIdentity (aligné MCP 7.4) | manque |

### M2 — Catalogue Explorer

| Feature | Entrées | Sorties | Emplacement | État |
|---|---|---|---|---|
| Lister les schémas du catalogue | env actif | `list[str]` | `lib ✓` CatalogInspector.list_schemas · `api` | servable |
| Lister les tables d'un schéma | schéma | `list[str]` | `lib ✓` list_tables | servable |
| Décrire une table | FQN | `{colonne: type_sql}` | `lib ✓` describe_table (vide si non supporté) | servable |
| Suggestions sur nom inconnu | nom saisi | candidats Levenshtein | `lib ✓` CatalogInspector (CatalogError) | servable |
| Résolution sandbox | FQN | FQN suffixé réellement lu, clone effectué ou non | `lib ✓` SandboxResolver.resolve · `api` | partiel (à exposer) |
| Glisser une table dans le builder | FQN + colonnes | bloc `tables:` pré-rempli | `ui` | — |

### M3 — Pipeline Builder (YAML bidirectionnel)

| Feature | Entrées | Sorties | Emplacement | État |
|---|---|---|---|---|
| Schéma de formulaire | — | JSON Schema du pipeline | `lib ✓` `core/json_schema.generate_json_schema()` (aussi `schemas/skifer-pipeline.schema.json`) | servable |
| Valider un YAML | texte YAML + params | dict normalisé **ou** erreurs localisées (chemin JSON, message) | `lib ✓` parse_schema / load_schema · `lib →31.1` localisation des erreurs | partiel |
| Décrire le pipeline | dict | sources, joins, règles, checks, colonnes de sortie, mode, env, suffixe sandbox | `lib ✓` `describe_schema(print_summary=False)` | servable |
| Inférer le schéma de sortie sans Spark | dict | `[{name, source, ops, type}]` (type indicatif) | `lib ✓` `infer_output_schema` | servable |
| Projeter le schéma de sortie (précis, avec contrat) | dict | `ProjectedSchema` (types, `needs_curation`, grain) | `lib ✓` `semantic/output_projection.OutputProjector` | servable |
| Analyser les règles référencées | dict | rapport OVERWRITE / SHARED_READ / DUPLICATE_EXPR / PHOTON_BREAKING / COMPLEXITY_HIGH | `lib ✓` `explain_rules` (imprime ; à retourner en dict `→31.1`) | partiel |
| Partials (sous-transformations) | chemins relatifs | alias exposés, cycles refusés au load | `lib ✓` `partials:` | servable |
| Materialisation (table / streaming_table / materialized_view) | bloc `materialization:` | contraintes validées au load ; SQL compilé pour MV | `lib ✓` schema_loader + `sql_compiler.compile_select` | servable |
| Agrégations déclaratives | bloc `aggregate:` | validation `having` contre `group_by ∪ mesures` | `lib ✓` `AGGREGATE_FUNCTIONS` (op_catalog) | servable |
| Catalogue des opérateurs et ops | — | filtres, ops `select_final`, fonctions d'agrégat | `lib ✓` `core/op_catalog.py` (à exposer `→31.1`) | partiel |
| Écrire le YAML | chemin + texte | fichier écrit atomiquement, refus hors projet | `lib ✓` `persistence.write_yaml_atomic` · `lib →31.1` scope `pipelines:write` | partiel |
| Synchronisation canvas ↔ YAML | — | — | `ui` (parsing YAML côté frontend pour l'affichage, validation toujours côté lib) | — |

### M4 — Rule Studio

| Feature | Entrées | Sorties | Emplacement | État |
|---|---|---|---|---|
| Scanner les règles du projet | `rules/**/*.py` + chemins déclarés | `[RuleView(name, kind, reads, writes, warnings, file, line)]` | `lib ✓` RuleRegistry + RuleAnalyzer · `lib →31.1` scan importlib | partiel |
| Profil d'une règle | nom | colonnes lues/écrites, kind, avertissements de performance | `lib ✓` `RuleAnalyzer.analyze_rule` | servable |
| Graphe de dépendances entre règles | liste de noms | stages, ordre topologique, cycles | `lib ✓` RulePlanner / `build_dependency_graph` | servable |
| Wizard règle simple (constante, cast, when/otherwise, withColumn) | formulaire | snippet `@RuleRegistry.register_rule()` déterministe | `lib →31.1` RuleService.generate_snippet | manque |
| Écrire une règle | fichier + snippet | fichier écrit, rechargement à chaud, module invalide isolé | `lib →31.1` scope `rules:write` | manque |
| Éditeur Python guidé (règles complexes) | code | validation syntaxique (`ast.parse`), autocomplétion Spark | `ui` (éditeur) + `lib →31.1` validation | manque |

### M5 — Exécution

| Feature | Entrées | Sorties | Emplacement | État |
|---|---|---|---|---|
| Preview (dry-run avec données) | dict + params + limite | `job_id` puis lignes JSON-natives (≤ 1 000, total indiqué), schéma | `lib ✓` `process_schema` · `lib →31.6` ExecutionService | partiel |
| Exécuter vers une table | YAML + layer + table + params + `intermediate_mode` | `job_id`, run_id, table écrite, rapport monitor, décision de publication | `lib ✓` `run_from_yaml` / `run_process_to_table` · `lib →31.6` | partiel |
| Statut, logs, annulation d'un job | `job_id` | état, progression, logs, annulé | `lib →31.6` | manque |
| Full refresh (streaming / MV) | layer + table (+ materialization) | checkpoint purgé, table droppée | `lib ✓` `full_refresh` | servable |
| Optimiser une table | layer + table + zorder | — | `lib ✓` `optimize_table` | servable |
| Run par split / union | dict + partitions | tables écrites | `lib ✓` `run_process_and_split`, `run_union_sources_to_table` | servable |
| Cadence de polling | — | 5 s (0–60 s), 15 s (1–5 min), puis manuel | `ui` (décision plan 2) | — |

### M6 — Lineage et dictionnaire

| Feature | Entrées | Sorties | Emplacement | État |
|---|---|---|---|---|
| Lineage statique d'un pipeline | dict | `LineageGraph` (arêtes colonne, types select/join/rule/metric), Mermaid, JSON | `lib ✓` `build_lineage`, `LineageRenderer` | servable |
| Lineage d'un modèle sémantique | modèle | arêtes `metric` | `lib ✓` `LineageTracker.from_semantic_model` | servable |
| Dictionnaire d'un pipeline | graphe | `FieldEntry(name, table, description, source_fields, transformations)` | `lib ✓` `DataDictionary` (+ glossaire fuzzy) | servable |
| Lineage inter-pipelines, impact d'une colonne | FQN.colonne, direction | graphe fusionné persistant, `ImpactReport` | `lib →31.2` registre de métadonnées | manque |
| Recherche de colonne dans le projet | texte | datasets et colonnes correspondants | `lib →31.2` | manque |

### M7 — Qualité et observabilité

| Feature | Entrées | Sorties | Emplacement | État |
|---|---|---|---|---|
| Checks dérivés du YAML | dict | liste de checks (Null, Unique, Type, FilterInvariant, Freshness, Volume, SchemaDrift, CustomSql…) | `lib ✓` `ContractExtractor` | servable |
| Lancer les checks sur une table | FQN ou dict | `MonitorReport.summary()` (PASS/FAIL/ERROR/SKIPPED, sévérité) | `lib ✓` `DataMonitor` (Spark requis) | servable |
| Historique des rapports | dataset | rapports horodatés | `lib ✓` `SqliteHistoryStore` / `DeltaHistoryStore` | servable |
| Rapport HTML / JSON / texte | rapport | rendu | `lib ✓` `MonitorReporter` | servable |
| Incidents (ouvrir, accuser, assigner, résoudre) | échec de check | `Incident` avec statut et cause racine | `lib →31.4` | manque |
| Routage d'alertes par owner et lineage aval | incident | destinataires, canaux (webhook, Slack, SMTP, + Teams, Google Chat) | `lib ✓` AlertDispatcher · `lib →31.4` | partiel |
| Diff entre deux versions de contrat | deux définitions | `ContractDiff` avec indicateur `breaking` | `lib →31.3` | manque |

### M8 — Contrats, data products, certification

| Feature | Entrées | Sorties | Emplacement | État |
|---|---|---|---|---|
| Éditer `data_product:` et `contract:` | formulaire depuis JSON Schema | blocs YAML validés (grain ⊆ output, output produit par le pipeline) | `lib ✓` `_normalize_agent_ready_metadata` · `ui` formulaire | servable (champs actuels) |
| Champs de gouvernance : classification typée, owner structuré, statut, reviewers, SLA, sécurité | formulaire | validation et JSON Schema enrichis | `lib →31.3` | manque |
| Identité et hash du contrat | dict | `ContractDefinition` (id, version, hash SHA-256, canonical JSON) | `lib ✓` `canonicalize_contract` | servable |
| Export ODCS 3.1 | définition | document ODCS + warnings | `lib ✓` `export_odcs_31` | servable |
| Import ODCS 3.1 | document | blocs `data_product` / `contract` + warnings | `lib →31.3` | manque |
| Statut de certification | dataset, classe de consommateur | `Certification(status, contract_version, certified_at, checks_passed)` | `lib ✓` `CertificationStore.get_certification` | servable |
| Historique des runs et check results | FQN | `RunEvent`, `StoredCheckResult` | `lib ✓` certification store | servable |
| Parcourir la quarantaine | FQN, run_id | lignes taguées `_violations` / `_run_id` / `_contract_version` (limitées) | `lib ✓` `_skifer_quarantine` · `lib →31.6` lecture bornée | partiel |
| Reprise d'une publication interrompue | run | `coordinator.resume()` | `lib ✓` PublicationCoordinator | servable |
| Miroir de tags Unity Catalog | certification | 4 tags posés, `UcSyncResult` | `lib ✓` `uc_mirror` (non bloquant) | servable |

### M9 — Sémantique

| Feature | Entrées | Sorties | Emplacement | État |
|---|---|---|---|---|
| Parcourir le catalogue sémantique | filtres tags / layer | résumés de modèles (catalogue seul chargé, modèles à la demande) | `lib ✓` `SemanticEngine.list_models`, `get_model_summary` · `lib ✓` AgentReadyDataService | servable |
| Draft depuis le pipeline | pipeline | draft dans `semantic_models/.drafts/`, reproductible | `lib ✓` `SemanticDraftBuilder` | servable |
| Synchroniser (check / write-draft / promote) | pipeline, modèle | `SyncReport` (changes, conflicts, suggestions, `safe_to_apply`), codes 0/2/3 | `lib ✓` `SemanticSynchronizer` + CLI | servable |
| Valider modèle ↔ projection | modèle, pipeline | erreurs de cross-validation | `lib ✓` `SemanticValidator` | servable |
| Éditer un modèle (dimensions, métriques, entités, relations, calendrier) | formulaire | YAML validé (noms `^[A-Za-z_][A-Za-z0-9_]*$`) | `lib ✓` validator · `ui` | servable |
| Requête sémantique par noms | `SemanticQuery` (modèle, dimensions, métriques, filtres, période, limite) | `SemanticResult` : lignes + `SemanticEvidence` (hash SQL, hashs de métriques, lineage, certification, décision) | `lib ✓` `query_with_evidence` | servable |
| Plan de jointure multi-modèles | requête | `SemanticPlan` ou refus nommé (aucun chemin, plusieurs chemins, fanout, many_to_many) | `lib ✓` `SemanticPlanner` | servable |
| Créer une vue | requête + nom | FQN de la vue | `lib ✓` `create_view` | servable |
| Gate de certification | modèle, contexte consommateur | ALLOW / WARN / DENY / REQUIRE_HUMAN + raison | `lib ✓` `access_policy.evaluate`, `enforce_certification_gate` | servable |
| Override break-glass | raison, acteur, expiration | usage journalisé, refus si scope absent | `lib ✓` `CertificationOverride` (jamais depuis le client) | servable |
| Enrichissement LLM contraint | modèle projeté | descriptions et synonymes seulement | `lib ✓` `SemanticBuilder.build_from_projection` | servable |

### M10 — Agents

| Feature | Entrées | Sorties | Emplacement | État |
|---|---|---|---|---|
| Poser une question au hub | texte, profil utilisateur | `HubResponse` (agent routé, `AgentResponse` en mode query / view / ambiguous / needs_clarification / no_model / error, `FormattedResult` kpi / table / chart / text) | `lib ✓` `AgenticHub.ask` · sérialisation `_response_serializer` | servable (DataFrame à sérialiser) |
| Générer un pipeline par description | texte, dossier | `BuilderResponse(success, yaml_content, output_path, clarification_question)` | `lib ✓` `BuilderAgent.ask` | servable |
| Wizard sans LLM | réponses pas à pas | YAML | `lib ✓` `BuilderAgent.wizard` (interactif stdin, à rendre pilotable `→31.1`) | partiel |
| Historique de session et export PDF | session | `HistoryEntry`, PDF | `lib ✓` `SessionHistory`, `HistoryExporter` (extra `[semantic-pdf]`) | servable |
| Fournisseur LLM | clés d'API en env | `LLMProvider` auto-détecté (Anthropic, OpenAI, Google) | `lib ✓` `get_llm_provider` | servable |
| Preuve et ligne de provenance dans la réponse | — | même redaction que le JSON | `lib ✓` Feature 4 | servable |

### M11 — Gouvernance transverse (après le scénario 31)

| Feature | Entrées | Sorties | Emplacement | État |
|---|---|---|---|---|
| Audit de couverture du projet | YAMLs | % avec data_product, contrat, owner, descriptions, classification ; seuil | `lib →31.7` | manque |
| Propositions Adaptive Gold (accepter / rejeter) | usage sémantique réel | propositions YAML explicables dans `.skifer_proposals/` | Plan 29 Feature 8 | non démarré |
| Approbations de capabilities | action proposée | shadow → supervised → guarded_autonomous | Plan 29 Feature 9 | non démarré |

### M12 — Paramètres

| Feature | Entrées | Sorties | Emplacement | État |
|---|---|---|---|---|
| Tracing | bloc `observability.tracing` de config.yaml | exporter none / otlp / mlflow / dual, redaction | `lib ✓` `parse_tracing_config` | servable |
| Policy de certification par env | `semantic_certification_policy`, `max_age`, `consumer_class` | off / warn / enforce / supervised | `lib ✓` config | servable |
| `allow_raw_sql` par env | booléen | `expr:` et filtre `sql` refusés | `lib ✓` | servable |
| Sandbox | `sandbox.missing_table` | copy / error | `lib ✓` | servable |

---

## 5. Découpe des modules et correspondance

| Module UI | Package(s) librairie | Service (31.1) | Préfixe de routes API |
|---|---|---|---|
| M1 Projet et connexion | `core/config`, `core/environment`, `spark_factory` | ProjectService, ExecutionService.session, LocalIdentity | `/project`, `/session`, `/me` |
| M2 Catalogue | `core/catalog_inspector`, `core/sandbox` | ProjectService.catalog | `/catalog` |
| M3 Pipeline Builder | `core/schema_loader`, `core/json_schema`, `core/op_catalog`, `core/ir`, `semantic/output_projection`, `core/sql_compiler` | ProjectService | `/pipelines` |
| M4 Rule Studio | `core/registry`, `core/rule_analyzer`, `core/rule_planner` | RuleService | `/rules` |
| M5 Exécution | `core/core`, `core/spark_backend`, `observability/publication` | ExecutionService | `/jobs` |
| M6 Lineage | `lineage/*`, registre (31.2) | ProjectService.lineage, GovernanceService.impact | `/lineage`, `/dictionary` |
| M7 Qualité | `observability/checks, monitor, history, alerts, incidents (31.4)` | QualityService | `/quality`, `/incidents` |
| M8 Contrats | `observability/certification*, odcs, quarantine, uc_mirror` | GovernanceService | `/contracts`, `/certifications`, `/data-products` |
| M9 Sémantique | `semantic/*`, `agentic/resolver`, `agentic/data_service` | SemanticService | `/semantic` |
| M10 Agents | `agentic/hub, builder_agent, history, exporter` | AgentService (façade hub) | `/agents` |
| M11 Gouvernance | `observability/audit` (31.7), Features 8 et 9 | GovernanceService | `/audit`, `/proposals` |
| M12 Paramètres | `core/config` | ProjectService.config | `/config` |

Conventions transverses, à figer dans l'UI dès la v1 :

- **1 000 lignes maximum** par résultat, total réel indiqué, pagination par curseur opaque pour les listes.
- **Erreurs structurées** : `{code, message, path}` où `path` est un chemin JSON dans le YAML ; l'UI
  surligne l'élément fautif, elle ne reformule pas le message.
- **Scopes** sur chaque route ; une route refusée renvoie `403` avec le scope manquant, l'UI grise
  l'action au lieu de la cacher.
- **Aucun SQL affiché par défaut** : le SQL compilé et les valeurs de filtre ne sont montrés que si la
  policy de preuve le permet, sinon l'UI affiche « retenu par la policy », jamais une zone vide.
- **Jobs** : tout ce qui touche Spark passe par `job_id` ; aucun appel bloquant depuis l'UI.
- **Identifiants stables** : un pipeline est identifié par son chemin relatif au projet, un dataset par
  son FQN physique, un contrat par `(id, version)`, un run par `run_id`.

---

## 6. Ce que la revue de l'UI v1 doit intégrer

Points de besoin apparus depuis la première version, à traiter avec Claude Design :

1. **Le mode sans Spark est le mode normal.** L'ouverture de projet, la validation, la projection, le
   lineage statique, l'édition de règles et de contrats n'ont pas besoin de cluster. La connexion Spark
   est une action explicite, pas un préalable à tout.
2. **Un bandeau d'état permanent** : environnement, catalogue, mode Spark, utilisateur, suffixe sandbox,
   indicateur `is_production`. Le sandbox est invisible pour un débutant et surprend ; il doit se voir.
3. **Le YAML Builder est bidirectionnel mais la validation est unique** : les erreurs viennent de la
   librairie avec un chemin JSON et se surlignent aussi bien sur le canvas que dans l'éditeur.
4. **Contrat et data product sont des blocs du même YAML** que le pipeline, éditables dans le même écran,
   avec la projection du schéma de sortie affichée en regard (grain, types, `needs_curation`).
5. **Certification et preuve sont des états de première classe** : badge certifié / warn / deny sur
   chaque dataset et chaque réponse sémantique, panneau « preuve » (hash du SQL exécuté, sources, décision,
   fraîcheur), ligne de provenance unique.
6. **Le Rule Studio ne remplace pas un IDE** : wizard pour les règles simples, éditeur guidé pour le
   reste, jamais d'exécution de code arbitraire depuis l'UI.
7. **La sémantique naît du pipeline** : l'écran Semantic Browser montre le draft, le diff trois voies et
   les conflits ; la promotion est une action explicite qui refuse de perdre du contenu curé.
8. **Les écrans « à venir »** (incidents, impact inter-pipelines, audit de couverture, propositions,
   approbations) doivent être dessinés maintenant avec leurs états vides, pour que la lib serve exactement
   ce qu'ils attendent.

---

## 7. Glossaire des objets échangés

| Objet | Origine | Champs principaux |
|---|---|---|
| Pipeline (dict normalisé) | `parse_schema` | `tables`, `join`, `business_rules`, `select_final` / `keep_all_columns` / `aggregate`, `add_columns`, `partials`, `materialization`, `sink`, `data_product`, `contract`, `semantic`, `observability` |
| `ProjectedSchema` | `OutputProjector` | colonnes avec type logique, `needs_curation`, `grain`, `unmapped_measures` |
| `LineageGraph` | `LineageTracker` | arêtes `(source_table, source_column, target_table, target_column, transformations, edge_type)` |
| `RuleProfile` | `RuleAnalyzer` | `input_columns`, `output_columns`, `kind`, avertissements |
| `MonitorReport` | `DataMonitor` | résultats par check avec statut et sévérité, `has_critical_failures` |
| `ContractDefinition` | `canonicalize_contract` | `contract_id`, `contract_version`, `definition_hash`, `canonical_json`, `owner`, `status` |
| `Certification` | store | `dataset`, `consumer_class`, `status`, `contract_version`, `certified_at`, `checks_passed` |
| `SyncReport` | `SemanticSynchronizer` | `changes`, `conflicts`, `suggestions`, `safe_to_apply` |
| `SemanticEvidence` | `query_with_evidence` | `sql_hash`, hashs de métriques, sources et lineage, snapshot de certification, décision, statut d'exécution |
| `HubResponse` / `AgentResponse` / `FormattedResult` | hub | agent, mode, format (kpi / table / chart / text_analysis), titre, données, preuve |
| `BuilderResponse` | `BuilderAgent` | `success`, `yaml_content`, `output_path`, `clarification_question` |
| `RequestContext` | services | `subject`, `scopes`, `consumer_class`, `trace_context` |
