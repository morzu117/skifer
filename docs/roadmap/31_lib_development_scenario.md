# Plan 31 — Scénario de développement de la librairie, feature par feature

> Rédigé le 7 septembre 2026. Remplace le brouillon « Programme 31 — fondations de gouvernance ».
> Périmètre : **la librairie `skifer` uniquement.** SkiferUI a son dépôt et se réfère à
> [32_skiferui_feature_map.md](32_skiferui_feature_map.md).
> OpenMetadata est mis de côté ; seules les idées jugées pertinentes du benchmark
> [30_openmetadata_benchmark.md](30_openmetadata_benchmark.md) sont conservées (§2).
> Statut : **arbitrages §7 tranchés le 11 septembre 2026 ; en attente de validation des plans
> exécutables par feature avant tout code.** 7 features, 27 slices, une slice = un commit
> `feat(plan31-F.N): …`.

---

## 1. Contexte et objectif

Le Plan 29 a donné à la librairie une mécanique de confiance complète : contrat appliqué avant
promotion, certification calculée, sémantique exécutable avec sécurité de grain, preuves, tracing
redacté, MCP read-only. Ce qui manque n'est pas une mécanique de plus, c'est de la **mémoire** et une
**surface applicative** :

- le lineage est recalculé et jeté ; le dictionnaire n'est jamais stocké ; aucune vue inter-pipelines ;
- `classification` est une chaîne libre sans effet ; `owner` est une chaîne ; le contrat n'a ni statut,
  ni reviewers, ni SLA ; l'ODCS s'exporte mais ne s'importe pas ;
- un échec de check finit en quarantaine et en rapport, puis rien ne le suit ;
- la seule couche transport-neutre (`AgentReadyDataService`, slice 7.1) ne couvre que la lecture
  sémantique ; ouvrir un projet, valider, lister des règles, lancer un job n'ont pas de service.

Ce scénario comble ces manques dans l'ordre qui sert d'abord la librairie elle-même, ensuite
SkiferUI, ensuite les agents. Chaque feature a une valeur autonome : aucune n'existe uniquement pour
l'UI.

---

## 2. Idées conservées du benchmark, et idées écartées

| Conservée | Pourquoi elle est pertinente pour Skifer seul |
|---|---|
| Persistance du lineage et impact inter-pipelines | Répondre à « qui lit cette colonne ? » sans re-parser tout le projet ; base de la propagation de classification et d'alerte |
| Taxonomie de classification avec propagation | La redaction des preuves, la policy MCP et le bloc sécurité d'un contrat en dépendent |
| Ownership structuré (équipe, steward, domaine) | Router une alerte, exiger une approbation (Feature 9), remplir l'ODCS |
| Cycle de vie du contrat : statut, reviewers, dates d'effet, SLA, sécurité | Un contrat est une promesse datée ; un SLA de fraîcheur est un check ; un contrat déprécié doit peser sur la certification |
| Import ODCS 3.1 | Symétrie avec l'export existant ; accepter un contrat rédigé ailleurs |
| Diff structuré entre versions de contrat, notion de changement cassant | Alerter avant qu'un consommateur casse ; alimenter `semantic sync` |
| Incidents avec cycle de vie et routage aux owners aval | Donner une suite à la quarantaine ; mesurer la reprise |
| Audit de couverture | Gate CI sans dépendance ; mesure avant de durcir une règle |
| Service applicatif unique pour MCP, CLI, API | Une frontière de sécurité, testée une fois |

| Écartée ou différée | Raison |
|---|---|
| Adaptateur OpenMetadata, émetteur OpenLineage | Mis de côté à la demande ; reprendre en annexe (§8) quand un catalogue cible existe |
| Glossaire natif, recherche, UI de catalogue, connecteurs, IAM, workflows BPMN | Hors périmètre produit (Plan 26) |
| Lineage observé depuis les tables système Unity Catalog | Enrichissement optionnel déjà prévu en Feature 8, jamais une dépendance |

---

## 3. Principes

1. **Le YAML reste la source de vérité.** Registre, incidents, certification sont des dérivés du YAML
   et des runs, ré-indexables à tout moment. Aucun consommateur n'écrit dans un store pour modifier un
   pipeline.
2. **Le JSON Schema suit chaque bloc YAML dans la même slice.** `generate_json_schema()` est le contrat
   avec tout client de formulaire.
3. **Rien ne bloque le métier.** Registre et routage d'alertes suivent `uc_mirror.py` : résultat
   d'erreur journalisé, jamais une publication échouée.
4. **Une vue = une dataclass avec `to_dict()` allowlisté**, comme `SemanticEvidence`. Aucun objet Spark
   ne traverse `services/`.
5. **Scopes exacts partout**, y compris pour une identité locale.
6. **Pas d'`exec`** : les règles sont importées par `importlib` depuis des chemins déclarés.
7. **Stores existants comme modèle** : `Sqlite*` en local, `Delta*` sur Databricks, indexés sur le
   `target_fqn` physique et le `definition_hash`.
8. **Rétrocompatibilité stricte** : un YAML d'aujourd'hui charge, hashe et s'exécute exactement pareil.

---

> **Plans exécutables détaillés par feature** (signatures réelles, cas de test, commit par slice) :
> [`31_lib_development_scenario/`](31_lib_development_scenario/README.md). Le README y consolide les
> corrections transverses relevées en confrontant les slices au code réel.

## 4. Les features

### Feature 31.1 — Service applicatif transport-neutre `services/`

**Objectif.** Une couche unique entre la librairie et ses consommateurs (MCP, CLI, future API), qui
ouvre un projet, valide, projette, liste des règles, lit la gouvernance et la sémantique, sous scopes.

**Bénéfice.** Une seule frontière de sécurité, testée une fois. Les Features 8 et 9 du Plan 29 (propositions,
capabilities) tomberont dans le même moule. Rien de ce qui existe ne change de comportement.

| Slice | Contenu | Fichiers | Tests |
|---|---|---|---|
| 1.1 | Package `services/` : extraction de `RequestContext`, `require_scope`, `ServiceLimits`, hiérarchie d'erreurs et sérialisation JSON-native depuis `agentic/data_service.py` ; ré-exports rétrocompatibles ; scopes nommés (`project:read`, `pipelines:write`, `rules:write`, `execute:run`, `contracts:write`, `incidents:write` + `models:read`, `contracts:read`, `lineage:read`, `query:execute`) | `services/__init__.py`, `services/context.py`, `services/serialization.py`, `agentic/data_service.py` | imports historiques inchangés ; scope exact ; test d'architecture : `mcp/` n'importe plus `agentic.data_service` pour le contexte |
| 1.2 | `ProjectService` : `open(project_dir) -> ProjectView` (config, environnements, pipelines, règles, modèles, calendriers), `get_pipeline(path) -> PipelineView` (YAML brut, dict normalisé, **erreurs localisées** `{code, message, path}`), `json_schema()`, `op_catalog()`, `describe(path)`, `project_output(path)`, `lineage(path)`, `explain_rules(path) -> dict` (le rapport imprimé devient une structure), `write_pipeline(path, text)` atomique sous `pipelines:write`. Sans Spark. | `services/project.py`, `core/schema_loader.py` (erreurs avec chemin), `core/core.py` (`explain_rules` retourne) | erreurs localisées sur filtre, join, contrat ; refus de chemin hors projet ; écriture atomique |
| 1.3 | `RuleService` : `scan(paths) -> ScanReport` par `importlib` (convention `rules/**/*.py` + chemins déclarés, module invalide isolé), `list() -> [RuleView(name, kind, reads, writes, warnings, file, line)]`, `dependency_graph(names)`, `generate_snippet(spec) -> str` déterministe pour constante, cast, when/otherwise, withColumn simple, `write_rule(file, code)` sous `rules:write` après `ast.parse` | `services/rules.py`, `core/rule_analyzer.py` | snippet reproductible octet pour octet ; module cassé n'empêche pas les autres ; rechargement |
| 1.4 | `GovernanceService` (contrats et versions, certification, data products, quarantaine en lecture bornée) et `QualityService` (checks dérivés d'un YAML, historique, dernier rapport) sur les stores existants | `services/governance.py`, `services/quality.py` | scopes lecture ; vues allowlistées ; aucun DataFrame |
| 1.5 | `SemanticService` : façade sur `AgentReadyDataService` + `SemanticSynchronizer` (`check`, `write_draft`, `promote`) + `SemanticValidator` ; mêmes codes de résultat que la CLI (0 / 2 / 3) ; `AgentService` : `ask(question, profile)` via `AgenticHub` avec sérialisation `hub_response_to_text` et `to_dict`, `build(description)` via `BuilderAgent.ask` ; `BuilderAgent.wizard` rendu pilotable par un dict de réponses au lieu de `input()` | `services/semantic.py`, `services/agents.py`, `agentic/builder_agent.py` | promotion refusée si perte de contenu humain ; wizard sans stdin |
| 1.6 | `LocalIdentity(subject=get_clean_username(), scopes=LOCAL_DEFAULT_SCOPES)` pour stdio MCP et tout client local ; jamais `certification_override` ; jamais un scope venant du client ; aligné sur la slice 7.4 | `services/identity.py`, `mcp/server.py` | escalade impossible ; subject non vide |

**Acceptation.** `mcp/` n'importe que `services/` ; un test d'architecture échoue si `mcp/` ou un futur
`api/` importe `SemanticEngine`, `SkiferEngine` ou un store directement.
**Dépendances.** Commiter la slice 7.3 (`mcp/tools.py`, non commité, sans test) **avant** 1.1.

### Feature 31.2 — Registre de métadonnées persistant

**Objectif.** Persister ce que la librairie sait déjà calculer : schéma de sortie projeté, lineage
colonne, profils de règles, rattachés au FQN physique, au hash de définition et au dernier `run_id`.

**Bénéfice.** Impact inter-pipelines, dictionnaire consultable hors session, base de la propagation de
classification (31.3) et du routage d'alertes (31.4). `LineageGraph.merge()` trouve enfin un appelant.

| Slice | Contenu | Fichiers | Tests |
|---|---|---|---|
| 2.1 | `DatasetRecord(target_fqn, pipeline_path, data_product_id, contract_version, definition_hash, owner, columns: [ColumnRecord(name, logical_type, classification, description, sources)], indexed_at, last_run_id)` ; Protocol `MetadataStore` ; `SqliteMetadataStore` (`.skifer_metadata.db`) ; `DeltaMetadataStore` (`_skifer_metadata`) | `observability/metadata_store.py` | round-trip ; upsert idempotent par (fqn, hash) |
| 2.2 | `index_schema(schema_dict, path) -> DatasetRecord` pur, depuis `OutputProjector` + `LineageTracker.from_schema` + `RuleAnalyzer` ; CLI `skifer index PATHS` sans Spark ; hook non bloquant après `PROMOTED` dans `PublicationCoordinator` qui rattache le `run_id` | `observability/metadata_index.py`, `cli.py`, `observability/publication.py` | même entrée = même record ; échec du store ≠ échec de publication |
| 2.3 | Requêtes sur le graphe fusionné de tous les records : `upstream(fqn, column)`, `downstream(fqn, column)`, `impact(fqn) -> ImpactReport`, `search_columns(text)` ; exposées par `GovernanceService` | `observability/metadata_store.py`, `lineage/tracker.py`, `services/governance.py` | trois pipelines chaînés ; cycle refusé ; profondeur bornée |
| 2.4 | CLI `skifer lineage FQN[.column] --direction up|down --format mermaid|json` et `skifer dictionary FQN` lisant le registre | `cli.py` | codes de sortie ; sortie stable |

**Acceptation.** Un projet de trois pipelines indexé sans Spark répond à l'impact d'une colonne Silver
sur toutes les tables Gold ; ré-indexer sans changement n'écrit rien.

### Feature 31.3 — Gouvernance dans le YAML

**Objectif.** Donner un sens et un effet aux champs de gouvernance : classification typée et propagée,
ownership structuré, cycle de vie du contrat, import ODCS, diff de contrat.

**Bénéfice.** La redaction des preuves et la policy MCP s'appuient sur une classification ; le SLA devient
un check ; un contrat déprécié pèse sur la décision de certification ; un changement cassant est détecté
avant qu'un consommateur le subisse.

| Slice | Contenu | Fichiers | Tests |
|---|---|---|---|
| 3.1 | Taxonomie `classification: public | internal | confidential | restricted | pii` validée au load et dans le JSON Schema ; **propagation** dans `LineageTracker` : une colonne dérivée hérite du niveau le plus haut de ses sources sauf déclaration explicite (abaissement journalisé) ; **v1 `warn` sur élévation inférée non déclarée, durcissement `strict` après 31.7 (arbitrage §7.5)** ; exposition dans `SourceEvidence`, `EvidencePolicy` (une colonne `pii`/`restricted` retient les valeurs de filtre), ODCS `classification`, `ColumnRecord` (31.2) | `core/schema_loader.py`, `core/json_schema.py`, `lineage/tracker.py`, `semantic/evidence.py`, `observability/odcs.py` | valeur inconnue refusée ; héritage via join et règle ; abaissement explicite |
| 3.2 | Ownership : `owner` accepte une chaîne (rétrocompat) ou `{team, steward, domain, contact}` ; `data_product.domain` ; ODCS `team[]` complété ; tags UC `skifer_owner`, `skifer_domain` ; owner toujours hors hash | `core/schema_loader.py`, `core/json_schema.py`, `observability/odcs.py`, `observability/uc_mirror.py` | chaîne et mapping → même `ContractDefinition` |
| 3.3 | Cycle de vie : `contract.status: draft | active | deprecated` (défaut `active`), `contract.reviewers[]`, `contract.effective_from/until`, `contract.sla: {refresh_frequency, max_latency}`, `contract.security: {level, access_policy}` ; `ContractExtractor` dérive `LoadFreshnessCheck` du SLA ; `access_policy.evaluate()` → `WARN` sur `deprecated`, `DENY` hors fenêtre d'effet, avec raisons dédiées ; **hash** : `sla` et `security` dedans, `status` / `reviewers` / dates dehors ; `canonicalization_version` incrémentée | `core/schema_loader.py`, `core/json_schema.py`, `observability/certification.py`, `observability/contracts.py`, `semantic/access_policy.py`, `observability/odcs.py` | hash stable sur changement de statut ; nouveau hash sur SLA ; gate sur deprecated ; anciens hashs lisibles |
| 3.4 | Import ODCS 3.1 : `import_odcs_31(doc) -> {data_product, contract, warnings}` symétrique de l'export ; CLI `skifer contract import FILE` qui imprime le bloc YAML | `observability/odcs.py`, `cli.py` | export → import → export idempotent sur les champs mappés |
| 3.5 | `diff_contracts(a, b) -> ContractDiff(added, removed, retyped, required_changed, classification_changed, sla_changed, breaking: bool)` ; utilisé par `semantic sync` (signalement) et exposé par `GovernanceService` | `observability/certification.py`, `semantic/sync.py`, `services/governance.py` | chaque catégorie ; `breaking` sur retrait, retypage, abaissement de classification, SLA relâché |

**Acceptation.** Un YAML existant sans ces blocs charge et hashe comme avant (test de non-régression sur
les hashs des fixtures). Le JSON Schema décrit chaque nouveau champ.

### Feature 31.4 — Incidents et routage d'alertes

**Objectif.** Donner une suite à un échec de check : un objet incident avec un cycle de vie, routé aux
bonnes personnes, fermé quand le dataset se rétablit.

**Bénéfice.** Reprise mesurable ; alerte aux owners aval et non à un canal global ; vocabulaire commun
avec les outils d'observabilité du marché.

| Slice | Contenu | Fichiers | Tests |
|---|---|---|---|
| 4.1 | `Incident(id, target_fqn, run_id, check_name, severity, status: NEW → ACKNOWLEDGED → ASSIGNED → RESOLVED, assignee, root_cause, opened_at, resolved_at)` dans le certification store (SQLite et Delta) ; ouverture automatique sur quarantaine, un incident par check critique, dédupliqué tant qu'ouvert ; résolution automatique `recovered` au `PROMOTED` suivant | `observability/incidents.py`, `observability/certification_store.py`, `observability/publication.py` | dédup ; recovered ; `resume()` n'ouvre pas de doublon |
| 4.2 | Routage : destinataires = owner du dataset (31.3) + owners aval via `downstream()` (31.2), profondeur bornée ; alerte sur `ContractDiff.breaking` (31.3) ; canaux MS Teams et Google Chat ajoutés à `AlertDispatcher` ; tout échec de canal reste un warning | `observability/alerts.py`, `observability/incidents.py` | propagation N niveaux ; canal en échec isolé ; aucune valeur de donnée dans le message |
| 4.3 | CLI `skifer incidents list|ack|assign|resolve` ; `QualityService` expose les transitions sous `incidents:write` | `cli.py`, `services/quality.py` | transitions invalides refusées |

### Feature 31.5 — Exécution asynchrone

**Objectif.** Rendre pilotable, hors notebook, tout ce qui touche Spark : preview, run, full refresh,
checks, avec un identifiant de job, un statut, des logs et une annulation.

**Bénéfice.** Un client (CLI longue, API, UI) n'appelle plus un moteur bloquant ; le `run_id` d'audit du
pipeline est celui du job ; la session Spark a un cycle de vie explicite.

| Slice | Contenu | Fichiers | Tests |
|---|---|---|---|
| 5.1 | `ExecutionService.session` : `connect(config_path, force_env) -> SessionView(mode, env, catalog, user, sandbox_suffix, is_production, state)` ; une session par config active, fermée au changement ; états `connecting / ready / failed` avec cause | `services/execution.py` | session recréée sur changement de config ; échec exposé, pas levé |
| 5.2 | Jobs : `submit(kind: preview | run | full_refresh | check, path, params) -> job_id`, `status(job_id)`, `cancel(job_id)`, `logs(job_id, after)` ; un seul job actif par projet en v1 ; TTL et annulation à la fermeture de session ; `run_id` du job = `run_id` frappé par le pipeline | `services/execution.py`, `core/core.py` (exposition du run_id) | concurrence refusée proprement ; annulation ; run_id identique |
| 5.3 | Résultats : `result(job_id) -> ResultView(rows ≤ 1 000 JSON-natives, total, schema, monitor_report, publication_decision)` via `services/serialization` ; lecture bornée de la quarantaine | `services/execution.py`, `services/governance.py` | Decimal, date, timestamp, null ; total exact ; quarantaine limitée |

### Feature 31.6 — API locale `[api]`

**Objectif.** Un transport HTTP fin sur `services/`, dans ce dépôt, derrière un extra optionnel, avec une
OpenAPI versionnée que tout client peut consommer.

**Bénéfice.** Versions en lockstep avec la librairie ; les tests de la lib couvrent l'API ; aucun client
n'a de raison de réimplémenter un appel.

| Slice | Contenu | Fichiers | Tests |
|---|---|---|---|
| 6.1 | Package `api/` : application FastAPI importée paresseusement (extra `[api]` : `fastapi`, `uvicorn`), routes 1:1 sur les services (`/project`, `/session`, `/me`, `/catalog`, `/pipelines`, `/rules`, `/jobs`, `/lineage`, `/dictionary`, `/quality`, `/incidents`, `/contracts`, `/certifications`, `/data-products`, `/semantic`, `/agents`, `/config`), erreurs `{code, message, path}`, scope vérifié sur chaque route, CORS localhost seulement | `api/__init__.py`, `api/app.py`, `api/routes/*.py`, `api/errors.py`, `pyproject.toml` | `TestClient` ; test d'architecture : aucune route sans scope ; aucun import de moteur |
| 6.2 | CLI `skifer api serve --project DIR [--port]` et `skifer mcp serve` (slice 7.5 du Plan 29 livrée ensemble) ; healthcheck ; OpenAPI exportée par `skifer api openapi > openapi.json` et snapshot en test | `cli.py`, `api/app.py` | snapshot OpenAPI stable ; healthcheck sans Spark |

### Feature 31.7 — Audit de couverture

| Slice | Contenu | Fichiers | Tests |
|---|---|---|---|
| 7.1 | `audit_project(paths) -> AuditReport` pur : part des pipelines avec `data_product`, `contract`, owner structuré, description par champ de sortie, classification déclarée ; CLI `skifer audit PATHS [--json] [--min-coverage N]` avec exit 2 sous le seuil ; exposé par `ProjectService.audit()` | `observability/audit.py`, `cli.py`, `services/project.py` | calcul sur fixtures ; seuils ; JSON stable |

**Bénéfice.** Gate CI immédiat ; mesure avant de durcir la propagation de classification (31.3).

---

## 5. Articulation avec le reste du Plan 29

| Reste du Plan 29 | Interaction |
|---|---|
| 7.3 tool `query_semantic_model` (écrit, non commité, sans test) | À commiter avec ses tests **avant** 31.1.1 |
| 7.4 auth déléguée et scopes | Partage `services/identity.py` (31.1.6) ; à livrer avant 31.6 |
| 7.5 packaging MCP | Livrée avec 31.6.2 |
| Feature 8 Adaptive Gold | 8.5 `accept/reject` devient un `ProposalService` dans `services/` ; 8.1 peut lire le registre (31.2) pour détecter les relations manquantes |
| Feature 9 capabilities | 9.4 machine d'états et 9.7 tools gouvernés passent par `services/` ; l'identité locale de 31.1.6 est le cas `stdio` de 9.5 |

---

## 6. Ordonnancement

```text
Phase 0   commit 7.3 + tests  →  31.1.1 → 31.1.6 → 7.4        (fondation et identité)
Phase 1   31.1.2 · 31.1.3 · 31.1.4 · 31.1.5                   (services sans Spark)
          31.6.1 dès que 31.1.2 et 31.1.3 existent : une API v0 (projet, pipelines, règles,
          catalogue, sémantique en lecture) suffit pour brancher un premier client
Phase 2   31.3 (3.1 → 3.5) EN TÊTE  ∥  31.2 (2.1 → 2.4)  ∥  31.7   (gouvernance prioritaire,
          registre en parallèle, audit remonté pour mesurer la couverture de classification —
          arbitrage §7.5 : 31.3.1 propage en `warn`, le durcissement `strict` attend 31.7)
Phase 3   31.5 (5.1 → 5.3)  →  31.6.2 (+ 7.5)                   exécution et packaging
Phase 4   31.4 (4.1 → 4.3)                                       incidents (routage)
Phase 5   Plan 29 Feature 8, puis Feature 9
```

Dépendances dures : 31.1.1 avant tout autre 31.1 ; 7.4 avant 31.6 ; 31.2.3 et 31.3.2 avant 31.4.2 ;
31.3.5 avant 31.4.2 ; 31.7 avant le passage de 31.3.1 en `strict`. Tout le reste est parallélisable.

Après la phase 1, un client externe dispose déjà de : ouverture de projet, validation avec erreurs
localisées, JSON Schema, description, projection de schéma, lineage statique, règles et snippets,
catalogue sémantique et contrats en lecture. Tout cela sans Spark.

---

## 7. Décisions arbitrées (tranchées le 11 septembre 2026)

1. **API dans ce dépôt** derrière l'extra `[api]`. Raison : versions en lockstep avec la lib, les
   tests de la lib couvrent l'API, OpenAPI publiée à chaque release, aucun client ne réimplémente un
   appel. Cohérent avec `[mcp]` / `[tracing]` (cœur importable sans l'extra).
2. **Registre : SQLite local + Delta sur Databricks**, derrière le `Protocol MetadataStore`, comme
   `Sqlite*`/`Delta*History`/`Certification`. Delta seul casserait le mode local, cas de première classe.
3. **Hash du contrat : `sla` et `security` dedans, `status` / `reviewers` / `effective_from/until`
   dehors.** Le hash identifie ce que le contrat promet *sur la donnée* ; un changement de statut ou de
   reviewer ne change pas la donnée. Implique une nouvelle `canonicalization_version` et un test de
   **non-régression sur les hashs des fixtures existantes** (déjà exigé par l'acceptation de 31.3).
   *Précision découverte à la rédaction du plan détaillé* : `classification` est **déjà** dans le payload
   de hash (niveau champ), `owner`/`description` **déjà** exclus. Donc **31.3.1 ne bump pas** la version ;
   seule **31.3.3** la passe de 1 → 2 en y ajoutant `sla` et `security`.
4. **Identité locale : tous les scopes sauf `certification_override`.** En local l'utilisateur est le
   propriétaire du projet ; le break-glass reste hors de portée par défaut. Jamais un scope venant du
   client (aligné 7.4).
5. **Classification : `warn` en v1, puis `strict` après mesure.** La propagation (héritage du niveau le
   plus haut, abaissement explicite journalisé) est livrée d'emblée, mais une élévation inférée non
   déclarée **avertit** en v1 au lieu d'échouer au load. Le refus dur n'est activé qu'une fois la
   couverture réelle mesurée par `skifer audit` (31.7). **Conséquence d'ordonnancement : 31.7 est
   remontée en phase 1/2, avant le durcissement de 31.3.1.**
6. **31.3 (gouvernance) avant 31.2 (registre).** L'écran prioritaire de SkiferUI est le Pipeline Builder
   (M3) couplé à l'éditeur de contrat / data product (M8) — cœur du no-code ; le lineage/impact (M6,
   alimenté par 31.2) est un écran d'exploration secondaire. Les deux restent parallélisables (fichiers
   disjoints) ; seule dépendance dure conservée : 31.2.3 et 31.3.2 avant 31.4.2, 31.3.5 avant 31.4.2.
7. **Un seul job actif par projet en v1.** Simple, testable, suffisant pour l'usage mono-utilisateur ;
   la concurrence est refusée proprement (testé en 5.2). Une file d'attente est un ajout non cassant.

---

## 8. Annexe — différé : interopérabilité catalogue

Conservé pour mémoire, à reprendre quand un catalogue cible est choisi. Prérequis : 31.2, 31.3, 31.4.1.
Principe : l'adaptateur lit le **registre**, jamais des objets vivants ; non bloquant ; extra optionnel ;
idempotent par FQN et hash ; même redaction que les preuves (jamais de SQL ni de valeur de filtre).

- Adaptateur OpenMetadata : tables et colonnes, lineage colonne, résultats de checks, contrat via ODCS,
  data product, certification en tag ; mapping détaillé dans le benchmark §7.2.
- Émetteur OpenLineage : `RunEvent` par run avec facets `columnLineage` et `dataQualityAssertions`,
  transport HTTP vers tout consommateur ; garde la neutralité vis-à-vis du catalogue.
- Direction inverse : glossaire et ownership lus depuis le catalogue si absents du YAML, provenance
  explicite dans le record.

---

## 9. Definition of Done

- [x] arbitrages de la section 7 tranchés (11 septembre 2026) ;
- [ ] pour chaque feature, plan exécutable (fichiers exacts, cas de tests) validé avant le premier commit ;
- [ ] chaque slice = un commit `feat(plan31-F.N): …` avec tests et entrée `CHANGELOG.md` ;
- [ ] aucune version `pyproject.toml` modifiée ;
- [ ] `CLAUDE.md`, `AGENTS.md`, `docs/` mis à jour par feature ;
- [ ] `pytest tests/ -x --tb=short` vert après chaque phase.
