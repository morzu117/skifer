# Programme 29 — Semantic layer en parallèle du data model et données certifiées pour agents

> **Statut :** réflexion détaillée à valider — aucun développement autorisé par ce document seul
> **Date :** 1er septembre 2026
> **Origine :** analyse de l'article *Making Your Data Ready for Agentic AI* (Martin Fowler / Thoughtworks, 27 août 2026)
> **Objectif produit prioritaire :** accélérer la création de la couche sémantique pendant la construction du modèle de données, sans attendre que les tables Gold existent.
> **Périmètre technique :** Spark / Databricks uniquement, conformément au Plan 26.
> **Estimation actuelle :** **53 slices** réparties sur 10 features ; ce nombre mesure des unités de livraison, pas des jours de travail.

> **Plans de build détaillés :** [`29_agent_ready_semantic_layer/README.md`](29_agent_ready_semantic_layer/README.md).
> Chaque feature possède dans ce sous-dossier un plan autonome avec architecture cible, fichiers,
> ordre des commits, tests, erreurs, hors-périmètre et Definition of Done.

---

## 1. Résumé de la décision

L'article confirme la trajectoire de Skifer : YAML-as-contract, qualité, lineage,
modèles sémantiques versionnés et SQL déterministe sont déjà les bonnes briques. Il met cependant
en évidence qu'elles sont encore juxtaposées : le pipeline produit une table, puis le
`SemanticBuilder` reconstruit après coup un modèle depuis des notebooks, un glossaire et un LLM.

La nouvelle direction n'est donc pas « ajouter un agent de plus ». Elle est :

> **Compiler un même modèle déclaratif en trois artefacts synchronisés : une donnée physique,
> un contrat certifiable et un modèle sémantique consommable par les agents.**

Le chemin cible est le suivant :

```text
                    ┌─────────────────────────────┐
                    │ YAML pipeline en cours       │
                    │ transformations + output    │
                    │ contract + semantic seed    │
                    └──────────────┬──────────────┘
                                   │ parse/IR, sans Spark
                  ┌────────────────┼────────────────┐
                  ▼                ▼                ▼
          DataFrame / SQL     contrat de sortie   draft sémantique
          prévisible          versionné           incrémental
                  │                │                │
                  └────────────────┴───────┬────────┘
                                           ▼
                                  CI : cohérence croisée
                                           │
                                           ▼
                              staging → checks → promotion
                                           │
                                           ▼
                                      Gold certifié
                                           │
                                           ▼
                      SemanticEngine → preuves → traces → MCP
                                           │
                                           ▼
                              recommandations Adaptive Gold
```

### Conséquence sur la priorité

La liste initiale de neuf features reste valable, mais le besoin exprimé impose une **Feature 0**
fondatrice : la projection sémantique déterministe depuis le pipeline. Sans elle, les autres
features sécurisent l'accès aux données sans accélérer la construction parallèle de la semantic
layer.

---

## 2. État actuel vérifié dans le dépôt

| Capacité | Existant | Limite structurante |
|---|---|---|
| Construction sémantique | `semantic/builder.py` génère un YAML via LLM depuis notebook, glossaire et contexte libre | Construction postérieure et probabiliste ; le pipeline YAML/IR n'est pas la source du schéma sémantique |
| Validation sémantique | `SemanticValidator` vérifie modèle, table, dimensions et métriques | Ne vérifie pas les références contre le schéma de sortie prédit du pipeline |
| Requête sémantique | `QueryResolver` accepte uniquement des noms déclarés et compile le SQL sans LLM | Un modèle pointe vers une table unique ; pas de graphe d'entités ni de plan de jointure sémantique |
| Contrats | `ContractExtractor` dérive nullité, unicité, types, filtres, fraîcheur, volume et drift | Les checks portent principalement sur les sources et sont exécutés après l'écriture finale |
| Fraîcheur | `FreshnessCheck` compare `MAX(timestamp_column)` au temps courant | Confond fraîcheur métier et dernier chargement réussi |
| Lineage | `LineageTracker` construit un graphe statique depuis YAML et SQL | Ne trace pas l'exécution d'une question précise ni la décision de l'agent |
| Serving | `SkiferChatModel.predict()` transmet la dernière question au Hub | Pas d'identité appelante, preuve, statut de certification ni trace ID dans la réponse |
| Matérialisation | Plan 28 : SQL compiler, agrégations, materialized views et hash de définition | Brique disponible mais non reliée aux usages réels observés par la couche sémantique |

### Standards et primitives à réutiliser

- **ODCS 3.1** couvre schéma, qualité, équipe, rôles et SLA. Skifer ne doit pas
  remplacer son YAML de pipeline par ODCS, mais fournir un export/import d'interopérabilité :
  <https://github.com/bitol-io/open-data-contract-standard>.
- Les objets Unity Catalog acceptent commentaires et tags sur tables et colonnes. Les tags ne
  doivent jamais contenir de données sensibles :
  <https://docs.databricks.com/aws/en/database-objects/tags>.
- Unity Catalog expose des system tables de lineage et de query history. Elles sont une source
  d'enrichissement, pas une dépendance runtime obligatoire :
  <https://docs.databricks.com/aws/en/admin/system-tables/lineage> et
  <https://docs.databricks.com/aws/en/admin/system-tables/query-history>.
- MLflow Tracing est compatible OpenTelemetry et peut stocker ses traces dans des tables Delta
  gouvernées par Unity Catalog :
  <https://docs.databricks.com/aws/en/mlflow3/genai/tracing/>.
- MCP distingue Resources et Tools et permet de filtrer les ressources selon les autorisations
  présentées par la requête :
  <https://modelcontextprotocol.io/specification/draft/server/resources>.

---

## 3. Définition d'une slice

Une **slice** est une unité verticale qui :

1. produit une capacité observable et testable ;
2. conserve la compatibilité de l'API publique sauf mention explicite ;
3. possède ses tests et son entrée `CHANGELOG.md` si `src/` change ;
4. tient dans un commit distinct, conformément à `AGENTS.md` ;
5. passe `pytest tests/ -x --tb=short` et `ruff check src/` avant commit.

Une slice n'est ni une classe Python, ni une estimation en jours. Plusieurs fichiers peuvent être
touchés si cela est nécessaire pour livrer un comportement de bout en bout.

---

## 4. Ordonnancement global

| Vague | Feature | Intitulé | Slices | Dépendance principale |
|---|---:|---|---:|---|
| A | 0 | Projection sémantique parallèle | **6** | aucune |
| A | 6 | Entités, relations et jointures sémantiques | **6** | Feature 0 |
| B | 2 | Registre de contrats et certifications | **4** | Feature 0.1–0.2 |
| B | 1 | Validation avant publication et quarantaine | **5** | Feature 2.1–2.2 |
| B | 3 | Garde de certification dans SemanticEngine | **4** | Features 1–2 |
| C | 4 | Réponses sémantiques avec preuves | **4** | Features 2–3, 6 |
| C | 5 | Tracing runtime OpenTelemetry / MLflow | **5** | Feature 4.1 |
| D | 7 | Exposition MCP read-only | **5** | Features 3–5 |
| D | 8 | Adaptive Gold supervisé | **6** | Features 4–5, Plan 28 |
| E | 9 | Capability model et write-back gouverné | **8** | Features 5 et 7 |
|  |  | **Total** | **53** |  |

La Feature 2 précède techniquement une partie de la Feature 1, contrairement à l'ordre conceptuel
initial : la promotion a besoin d'un endroit fiable où enregistrer le contrat et le résultat de
certification.

---

# Feature 0 — Projection sémantique parallèle depuis le pipeline

## 0.1 Pourquoi cette feature est fondatrice

Le workflow actuel suppose que la table existe ou que son sens peut être retrouvé dans un notebook.
Cela force souvent la séquence suivante :

```text
construire Silver/Gold → déployer → inspecter → demander au LLM → corriger le modèle sémantique
```

Le workflow cible doit commencer dès la première PR du modèle de données :

```text
déclarer la sortie du pipeline
→ prédire son schéma sans Spark
→ produire un draft sémantique
→ faire évoluer pipeline et sémantique dans la même PR
```

### Principe de propriété

Les informations ne doivent pas être dupliquées sans règle de propriété :

| Information | Source de vérité |
|---|---|
| Nom physique, type physique, provenance, transformation | pipeline YAML + IR |
| Clé, grain, nullité, unicité, classification | contrat de sortie du pipeline |
| Description métier, synonymes, rôle d'entité | semantic seed puis curation humaine |
| Formule d'une agrégation déjà déclarée dans `aggregate.measures` | pipeline YAML |
| Métrique métier dérivée ou contestée | modèle sémantique curaté |
| Suggestions de description ou de métrique | LLM, jamais autorité finale |

Le générateur ne doit jamais réécrire silencieusement une définition humaine. Il produit un draft et
un diff, avec des identifiants stables permettant une fusion incrémentale.

## 0.2 Surface YAML proposée

La surface exacte devra être validée avant développement. Une direction cohérente serait :

```yaml
data_product:
  id: sales.orders
  version: 1.0.0
  owner: sales-data
  description: Commandes validées, au grain d'une commande

contract:
  grain: [order_id]
  output:
    order_id:
      logical_type: string
      required: true
      unique: true
      classification: internal
    customer_id:
      logical_type: string
      required: true
      entity: customer
    net_revenue:
      logical_type: number
      description: Montant net hors retours

semantic:
  model_key: orders
  entity: order
  default_time_dimension: order_date
  dimensions: [order_date, region, customer_id]
  # Les mesures issues d'aggregate peuvent être projetées automatiquement.
```

Le bloc `contract.output` référence uniquement des colonnes finales. Une référence absente de
`select_final`, `keep_all_columns` prévisible ou `aggregate` est une erreur au load.

## 0.3 Plan de développement — 6 slices

### Slice 0.1 — IR du produit et du contrat de sortie

- Ajouter les structures typées `ParsedDataProduct`, `ParsedOutputField` et
  `ParsedSemanticSeed` dans `core/ir.py`.
- Normaliser et valider les blocs dans `schema_loader.py`.
- Étendre `core/json_schema.py` et régénérer le JSON Schema commité.
- Utiliser des identifiants stables (`data_product.id`, champ physique) ; ne pas baser les liens sur
  la position d'une liste.
- Tests : formes nominales, clés inconnues, doublons, références à des sorties absentes,
  compatibilité des anciens YAML sans ces blocs.

**Valeur livrée :** le pipeline peut porter son intention de produit et son contrat sans exécuter
Spark.

### Slice 0.2 — Prédiction déterministe du schéma de sortie

- Nouveau `semantic/output_projection.py` pur Python.
- Compiler `select_final`, `aggregate`, `add_columns` et les casts vers un `ProjectedSchema`.
- Produire pour chaque champ : nom, type connu ou `unknown`, sources, transformations, rôle candidat.
- Un type non inférable à cause d'une règle Python reste `unknown` avec une raison ; il n'est jamais
  inventé.
- Tests de parité : projection statique contre le `df.schema` d'une exécution Spark locale.

**Valeur livrée :** la semantic layer peut être validée avant la création physique de la table.

### Slice 0.3 — Générateur de draft sémantique sans LLM

- Nouveau `SemanticDraftBuilder` consommant `ProjectedSchema`, contrat et semantic seed.
- Projeter dimensions explicites, entités, descriptions, types et mesures déclaratives.
- Écrire dans `semantic_models/.drafts/<model_key>.yaml`, sans modifier le modèle publié.
- Ajouter `source_contract_id`, `source_contract_version` et `source_definition_hash` au draft.
- Tests snapshot sur batch simple, jointure, agrégation et type inconnu.

**Valeur livrée :** premier modèle sémantique disponible dans la même PR que le pipeline.

### Slice 0.4 — Synchronisation incrémentale et diff à trois voies

- Comparer : dernière projection connue, nouvelle projection, modèle curaté courant.
- Classer les changements : ajout compatible, suppression, rename probable, changement de type,
  changement de grain, modification de formule.
- Préserver descriptions, synonymes et métriques humaines si leur dépendance reste valide.
- Produire des conflits explicites plutôt que choisir automatiquement lors d'un changement ambigu.
- Tests : conservation de curation, suppression de source, rename, changement incompatible de type,
  rerun idempotent.

**Valeur livrée :** les évolutions du modèle physique ne détruisent pas le travail sémantique.

### Slice 0.5 — CLI et gate CI

- Commandes proposées :
  - `skifer semantic sync <pipeline> --check` : diff, aucun write ;
  - `--write-draft` : écrit le draft ;
  - `--promote` : promotion explicite après validation ;
  - `skifer semantic validate <pipeline> <model>` : cohérence croisée.
- Codes de sortie distincts pour drift compatible, conflit et modèle invalide.
- Tests CLI sans Spark ni LLM.

**Valeur livrée :** pipeline et modèle sémantique deviennent une seule unité de revue CI.

### Slice 0.6 — Enrichissement LLM optionnel et E2E

- Rebrancher `SemanticBuilder` comme enrichisseur du draft déterministe, pas comme générateur libre.
- Le prompt reçoit uniquement les champs projetés et demande descriptions, synonymes et propositions
  de métriques ; aucune colonne nouvelle n'est acceptée.
- Ajouter un exemple E2E : création d'un pipeline Gold, génération du draft, enrichissement, validation
  et promotion sans table déployée.
- Documenter le workflow équipe data engineer + analytics engineer en parallèle.

**Valeur livrée :** accélération assistée par LLM sans transférer au LLM la source de vérité.

## 0.4 Critères d'acceptation de la feature

- Un nouveau YAML Gold produit un modèle sémantique validable sans Spark.
- Une modification du pipeline produit un diff déterministe et idempotent.
- Une description curatée n'est jamais écrasée automatiquement.
- Une métrique référençant une colonne supprimée bloque la CI avant déploiement.
- Les anciens projets sans `data_product`, `contract` ou `semantic` restent compatibles.

---

# Feature 1 — Validation avant publication et quarantaine

## 1.1 Approfondissement

Aujourd'hui, `run_process_to_table` écrit la cible puis exécute le monitor. L'échec protège le job,
mais pas la frontière de publication : une version invalide peut déjà être visible.

Le comportement cible batch est :

```text
DataFrame → table staging isolée → contrôles → promotion Delta atomique
                                  └→ échec : quarantaine + cible précédente intacte
```

Il faut distinguer :

- **checks dataset** : fraîcheur, volume, schéma, unicité globale ;
- **checks row-level** : nullité, domaine de valeurs, SQL transformable en prédicat ;
- **erreur d'exécution** : check impossible ou backend indisponible ; ce n'est jamais un PASS ;
- **quarantaine de lignes** : possible seulement pour un check row-level ;
- **quarantaine de snapshot** : nécessaire pour les échecs globaux.

La cible existante doit rester lisible tant que la nouvelle version n'est pas certifiée. Delta rend
atomique la visibilité de chaque commit d'overwrite, mais le plan ne doit pas promettre un rename
transactionnel multi-table qui n'existe pas partout.

Le streaming est hors de la première version : son append/upsert et ses checkpoints exigent une
politique de dead-letter par micro-batch distincte.

## 1.2 Plan de développement — 5 slices

### Slice 1.1 — Modèle de check pré-publication

- Enrichir `DataContract` avec `scope: row|dataset` et, pour les checks row-level,
  `violation_predicate()`.
- Interdire le fallback actuel « opérateur non supporté = passed » ; introduire `SKIPPED` et `ERROR`.
- Définir la matrice `severity × status × publication_policy`.
- Tests exhaustifs de classification et compatibilité des rapports existants.

### Slice 1.2 — Écriture staging et run identity

- Ajouter un `run_id` stable par exécution et une résolution de nom staging sûre.
- Écrire le DataFrame dans `_skifer_staging` avec le même format et les mêmes options que la cible.
- Ne jamais toucher la cible si l'écriture staging échoue.
- Nettoyage best-effort uniquement après résolution explicite du nom ; aucune suppression large.
- Tests writer/pattern avec FakeBackend et Spark local.

### Slice 1.3 — Monitor sur staging et quarantaine de snapshot

- Exécuter les contrats sur le FQN staging.
- En cas d'échec bloquant, conserver ou cloner le snapshot sous `_skifer_quarantine`.
- Écrire un manifeste : run, contrat, checks, cible prévue, staging, quarantaine et timestamps.
- La cible précédente reste inchangée.
- Tests nominal, critical failure, check error et échec de persistance du manifeste.

### Slice 1.4 — Quarantaine de lignes avec raisons

- Combiner les prédicats row-level sans perdre la liste des règles violées.
- Produire `_violations: array<string>`, `_contract_version`, `_run_id`, `_quarantined_at`.
- Ne pas réinjecter automatiquement les lignes corrigées ; fournir une API de lecture/rejeu explicite.
- Tests multi-violations, nulls, domaines, caractères spéciaux et absence de fuite de colonnes masquées.

### Slice 1.5 — Promotion, reprise et E2E

- Promouvoir le staging certifié via une écriture Delta atomiquement visible.
- Marquer le run `PROMOTED` seulement après succès du commit cible.
- Au redémarrage, détecter les runs `STAGED`, `VALIDATED` ou `PROMOTING` et proposer une reprise
  idempotente.
- Ajouter l'E2E : cible v1 valide, tentative v2 invalide, cible v1 inchangée, v3 valide promue.
- Documenter explicitement les limites streaming/JDBC/MV.

## 1.3 Critères d'acceptation

- Aucun échec critique ne remplace la dernière cible certifiée.
- Tout échec est distingué d'un check sauté.
- Chaque donnée quarantinée est reliée à un run et un contrat.
- Un rerun après crash ne publie pas deux fois et ne perd pas le staging utile.

---

# Feature 2 — Registre de contrats, fraîcheur et certifications

## 2.1 Approfondissement

Un unique statut mutable `CERTIFIED` serait insuffisant. Le registre doit séparer :

1. la **définition** versionnée du contrat ;
2. chaque **run de matérialisation** ;
3. chaque **évaluation** des checks ;
4. une vue calculée de l'**état courant**.

Schéma logique proposé :

```text
_skifer.contract_definitions
_skifer.materialization_runs
_skifer.check_results
_skifer.current_certifications   # vue
```

Deux fraîcheurs doivent être exposées :

- `load_freshness` : temps depuis le dernier run réussi, même si aucune valeur métier n'a changé ;
- `data_freshness` : âge du dernier événement métier, si une colonne temporelle est déclarée.

Le contrat peut demander des SLA différents selon le consommateur : dashboard, agent read-only ou
capacité opérationnelle. Le registre local doit fonctionner avec SQLite ou Delta local ; Unity
Catalog est un miroir de découverte, pas la seule source de vérité.

## 2.2 Plan de développement — 4 slices

### Slice 2.1 — Identité, hash et export de contrat

- Canonicaliser le contrat depuis l'IR et calculer un SHA-256 stable.
- Définir `contract_id`, `version`, `definition_hash`, `owner`, `status`, `effective_from`.
- Ajouter un export ODCS 3.1 best-effort avec mapping documenté ; aucune prétention de supporter
  tout ODCS en import natif.
- Tests de hash stable, ordre YAML indifférent, changement définissant et export.

### Slice 2.2 — Stores local et Delta

- Introduire une interface étroite `CertificationStore` et deux implémentations : SQLite et Delta.
- Écritures append-only pour définitions, runs et résultats ; vue/current calculée.
- Migrations de schéma versionnées et idempotentes.
- Tests contractuels partagés entre stores ; appels Delta mockés, E2E local minimal.

### Slice 2.3 — Heartbeat et double fraîcheur

- Enregistrer `started_at`, `materialized_at`, `validated_at`, `promoted_at`, `failed_at`.
- Remplacer l'ambiguïté de `FreshnessCheck` par `LoadFreshnessCheck` et
  `DataFreshnessCheck`, avec alias de compatibilité temporaire.
- Supporter des seuils par `consumer_class`.
- Tests table stable mais chargement sain, pipeline bloqué, donnée future, timezone et premier run.

### Slice 2.4 — Projection Unity Catalog et API de consultation

- API `get_certification(dataset, consumer_class)` et historique par run.
- Publier commentaires/tags UC non sensibles : owner, contract version, certification status,
  definition hash tronqué ; les détails restent dans les tables de registre.
- La synchronisation UC est observable et retryable, mais ne change pas un échec de certification en
  succès.
- Tests DDL, permissions insuffisantes, retry et absence de données sensibles dans les tags.

## 2.3 Critères d'acceptation

- Toute certification est reconstructible historiquement.
- Un même dataset peut être frais pour un dashboard et périmé pour un agent.
- Le statut courant est dérivé des faits, pas modifié manuellement sans événement d'audit.
- Le mode local ne dépend pas de Unity Catalog.

---

# Feature 3 — Garde de certification dans SemanticEngine

## 3.1 Approfondissement

Une certification qui n'est pas consultée au moment de la requête reste informative. La garde doit
se placer avant compilation/exécution SQL et produire une décision déterministe :

```text
ALLOW | WARN | DENY | REQUIRE_HUMAN
```

La politique ne doit pas être codée dans le prompt. Elle dépend de l'environnement, du consommateur
et du cas d'usage. Pour conserver l'adoption progressive :

- `off` : compatibilité ; aucune consultation ;
- `warn` : réponse enrichie d'un avertissement ;
- `enforce` : refus avant SQL ;
- `supervised` : résultat possible mais marqué non actionnable.

Un modèle sémantique doit déclarer ses datasets physiques et versions de contrat attendues. Une
certification périmée entre la planification et l'exécution doit être revérifiée juste avant SQL.

## 3.2 Plan de développement — 4 slices

### Slice 3.1 — Policy model et configuration

- Créer `semantic/access_policy.py` avec décisions et raisons structurées.
- Ajouter la configuration par environnement et défaut sûr en production agentique.
- Valider les valeurs au démarrage ; aucune faute de frappe ne doit revenir silencieusement à `off`.
- Tests de priorité config/params et compatibilité sans store.

### Slice 3.2 — Préflight des dépendances d'un modèle

- Résoudre toutes les sources requises par la requête sémantique.
- Consulter certification, SLA consommateur et hash attendu.
- Agréger les raisons sans masquer plusieurs datasets invalides.
- Tests certifié, absent, périmé, hash divergent et multi-source.

### Slice 3.3 — Enforcement dans `query` et `create_view`

- Garde avant `QueryResolver.resolve()` puis recheck immédiatement avant `execute_sql()`.
- En `DENY`, aucune méthode backend n'est appelée.
- Les vues persistées gardent la politique et la version de contrat utilisées lors de leur création.
- Tests espion backend et courses simulées entre préflight/recheck.

### Slice 3.4 — UX, override contrôlé et compatibilité

- Erreur structurée avec dataset, check/SLA fautif, dernière certification et action recommandée.
- Override uniquement explicite, autorisé par politique, avec identité, raison et trace obligatoire.
- Ajouter les messages Hub et la documentation de migration `off → warn → enforce`.
- Tests serialization, override refusé/accepté et journalisation.

## 3.3 Critères d'acceptation

- En `enforce`, aucune requête SQL ne touche une source non certifiée.
- Le refus indique comment rétablir le service sans exposer de secret.
- Tout override est attribuable et temporaire.

---

# Feature 4 — Réponses sémantiques avec preuves

## 4.1 Approfondissement

`SemanticEngine.query()` retourne actuellement un DataFrame. Pour un agent, il faut aussi retourner
les preuves déterministes utilisées, sans casser les notebooks existants.

API cible additive :

```python
result = semantic.query_with_evidence(query, consumer_context=ctx)
result.dataframe
result.evidence.model_key
result.evidence.definition_hash
result.evidence.metrics
result.evidence.sources
result.evidence.certifications
result.evidence.lineage
result.evidence.trace_id
```

La preuve ne contient pas une chaîne de pensée du LLM. Elle contient des faits auditables : choix
structurés, définitions, sources, décisions de policy, SQL hashé, timestamps et IDs d'exécution.

## 4.2 Plan de développement — 4 slices

### Slice 4.1 — Modèle `SemanticEvidence` et API additive

- Dataclasses sérialisables et versionnées ; `query()` garde son retour historique.
- `query_with_evidence()` retourne `SemanticResult`.
- Définir les champs obligatoires et optionnels, sans objet Spark dans la preuve.
- Tests serialization et compatibilité API.

### Slice 4.2 — Preuve de compilation

- Capturer modèle/version/hash, métriques, dimensions, filtres normalisés, FQN sources et hash SQL.
- Relier chaque métrique aux colonnes source via le lineage statique.
- Ne pas exposer le SQL brut par défaut si la policy le classe sensible.
- Tests déterminisme, redaction et multi-source.

### Slice 4.3 — Preuve d'exécution et certification

- Ajouter statut de policy, certifications, fraîcheur, run IDs, statement ID si disponible, temps
  d'exécution et nombre de lignes rendu.
- Distinguer clairement `observed_at`, `materialized_at` et `data_event_at`.
- Tests backend sans statement ID, échec SQL, résultat vide et WARN.

### Slice 4.4 — Hub et serving

- Étendre les modèles de réponse et `_response_serializer.py`.
- Produire une réponse humaine concise plus un bloc machine-readable optionnel.
- Ajouter `trace_id` et `evidence_id` à la réponse OpenAI-compatible sans casser `choices`.
- Tests serving et exemple de vérification de provenance côté client.

## 4.3 Critères d'acceptation

- Une réponse peut être reliée à la formule, aux sources et aux certifications exactes.
- La preuve est sérialisable sans Spark et reste stable après fermeture de session.
- Aucun raisonnement privé du modèle n'est requis pour expliquer la réponse.

---

# Feature 5 — Tracing runtime OpenTelemetry / MLflow

## 5.1 Approfondissement

Le lineage statique répond à « comment cette colonne est définie ? ». Le tracing runtime répond à
« qu'est-il arrivé lors de cette requête précise ? ».

Trace cible :

```text
hub.ask
├── intent.resolve
├── semantic.model.select
├── certification.preflight
├── semantic.compile
├── spark.sql.execute
├── result.format
└── response.serialize
```

Une abstraction interne doit éviter de coupler le cœur à MLflow : provider no-op par défaut,
OpenTelemetry comme modèle, adaptateur MLflow optionnel. Les entrées/sorties sensibles sont
redactées selon policy. Les traces stockent décisions, sources et règles appliquées, jamais la chaîne
de pensée privée du LLM.

## 5.2 Plan de développement — 5 slices

### Slice 5.1 — API d'instrumentation no-op

- Nouveau `observability/tracing.py` : `Tracer`, `Span`, context propagation et no-op.
- Générer/propager `trace_id`, `run_id`, `session_id`, `user_id` pseudonymisable.
- Interdire les attributs arbitraires non filtrés dans les chemins sensibles.
- Tests nested spans, exception, async/context isolation et no-op.

### Slice 5.2 — Instrumentation data pipeline et certification

- Spans load, parse, resolve sources, process, staging write, checks, promotion/quarantaine.
- Attributs : schema hash, contract ID/version, target, counts, décision ; jamais les valeurs métier.
- Tests ordre des spans, erreur et reprise.

### Slice 5.3 — Instrumentation semantic et agentic

- Spans modèle, LLM, resolver, policy, SQL, formatting et serving.
- Relier `SemanticEvidence.trace_id` à la trace racine.
- Capturer token/cost si le provider les expose sans rendre ces champs obligatoires.
- Tests avec faux tracer et faux LLM.

### Slice 5.4 — Adaptateur MLflow/OTel et stockage UC

- Extra optionnel de dépendances ; imports paresseux.
- Configuration MLflow tracking, OTLP ou dual export.
- Mode Databricks recommandé : trace location Unity Catalog ; mode local : console/in-memory.
- Tests de configuration et message d'installation, sans endpoint réel.

### Slice 5.5 — Rétention, redaction et tests de conformité

- Policy de capture par environnement, allowlist d'attributs, taille maximale, hashing des identités.
- Tests canary garantissant qu'un token, prompt marqué secret ou valeur PII ne sort pas.
- Documentation de rétention et droits UC ; exemple d'audit d'une réponse.

## 5.3 Critères d'acceptation

- Toute réponse servie possède un trace ID reconstructible.
- Les erreurs conservent les spans déjà produits.
- Désactiver l'export ne change pas le comportement métier.
- Les tests prouvent l'absence de secrets dans les attributs standards.

---

# Feature 6 — Entités, relations et jointures sémantiques

## 6.1 Approfondissement

Cette feature complète directement l'objectif de construction parallèle. Les jointures du pipeline
contiennent déjà une partie du futur modèle de domaine : clés, directions et tables. Elles peuvent
alimenter des **propositions** de relations, mais leur sens métier et leur cardinalité doivent rester
curatés.

Surface sémantique cible indicative :

```yaml
entities:
  - id: order
    key: order_id
    type: primary
  - id: customer
    key: customer_id
    type: foreign

relationships:
  - id: order_customer
    from: order.customer_id
    to: customer.customer_id
    cardinality: many_to_one
    join_type: left

semantic_models:
  - key: orders
    grain: [order]
    default_time_dimension: order_date
```

Les identifiants d'entité doivent être stables et distincts des noms physiques. Le premier objectif
n'est pas un moteur RDF/OWL : un graphe YAML en mémoire suffit tant que les parcours ont une
profondeur bornée et que le SQL peut être compilé déterministement.

## 6.2 Plan de développement — 6 slices

### Slice 6.1 — Spécification entités, grain et relations

- Étendre validator, catalog summary et JSON/schema documentaire sémantique.
- Valider clés, rôles primary/foreign, cardinalités, grains, IDs uniques et cycles illégaux.
- Conserver le chargement catalog-first/lazy : le catalogue ne contient que les résumés utiles.
- Tests de validation et cache lazy.

### Slice 6.2 — Inférence de candidats depuis pipeline et contrat

- Mapper `contract.output.entity`, grain et joins vers des candidats.
- Un join physique ne prouve pas une cardinalité : marquer `unknown` tant qu'elle n'est pas déclarée
  ou vérifiée par un contrat d'unicité.
- Ajouter candidats au diff de Feature 0, sans auto-promotion.
- Tests joins simples, composites, anti/semi/cross non sémantiques et ambiguïtés.

### Slice 6.3 — Graphe de domaine en mémoire

- Nouveau `semantic/domain_graph.py` : entités, relations, modèles, chemins valides.
- Détection des chemins multiples et coût déterministe simple.
- API inspectable par DictionaryAgent et LineageAgent.
- Tests traversal, cycles autorisés du domaine, ambiguïtés et bounded contexts.

### Slice 6.4 — Planificateur de jointures déterministe

- Étendre `SemanticQuery` pour référencer dimensions/métriques de modèles reliés.
- `QueryResolver` choisit uniquement un chemin déclaré ; ambiguïté = erreur avec choix possibles.
- Compilation SQL qualifiée et quoting systématique ; aucune jointure inventée par LLM.
- Tests single model inchangé, multi-model, clé composite, filtre distant et chemin ambigu.

### Slice 6.5 — Compatibilité métrique/dimension et calendriers

- Déclarer les dimensions applicables à chaque métrique/grain.
- Ajouter calendriers/fiscal periods comme définitions versionnées, sans dates devinées dans le prompt.
- Refuser les fanouts qui changeraient la mesure ; supporter une stratégie explicite seulement.
- Tests métrique non additive, fanout, fiscal Q3 et dimension incompatible.

### Slice 6.6 — CI adversariale et E2E parallèle

- Jeu de questions qui tente noms inconnus, joins inexistants, métriques à mauvais grain et conflit de
  définition.
- Chaque échec doit pointer une lacune de modèle, pas conseiller une modification de prompt.
- E2E : deux pipelines non déployés → drafts reliés → question compilée → tables locales déployées →
  même SQL validé.

## 6.3 Critères d'acceptation

- Les relations sont préparées pendant la construction des tables.
- Le LLM choisit des noms ; il ne construit jamais les chemins SQL.
- Une ambiguïté de relation bloque avant Spark.
- Une métrique ne peut pas être groupée sur une dimension qui change son grain de manière invalide.

---

# Feature 7 — Exposition MCP read-only

## 7.1 Approfondissement

MCP est une porte d'accès, pas la source de vérité. La première version doit être strictement
read-only et refléter les policies déjà appliquées dans Python.

Resources/templates proposés :

```text
skifer://semantic/catalog
skifer://semantic/models/{model_key}
skifer://certification/{dataset}
skifer://lineage/{dataset}/{column}
skifer://contracts/{contract_id}/{version}
```

Une requête analytique paramétrée peut être exposée comme **Tool read-only**
`query_semantic_model`, car elle déclenche un calcul ; l'annotation read-only ne remplace jamais la
garde serveur. Aucun accès aux tables brutes ni outil SQL générique.

## 7.2 Plan de développement — 5 slices

### Slice 7.1 — Service applicatif read-only indépendant du transport

- Façade `AgentReadyDataService` sur catalogue, contrats, certification, lineage et query.
- `ConsumerContext` obligatoire : identité, scopes, consumer class, trace context.
- Tests d'autorisation au niveau service ; le transport ne peut pas les contourner.

### Slice 7.2 — Serveur MCP Resources

- Implémenter liste/read/templates pour catalogue, modèles, contrats et lineage.
- Pagination, erreurs structurées, payloads bornés et ETags/hash de définition.
- Tests protocolaires sans réseau.

### Slice 7.3 — Tool analytique read-only

- Entrée = `SemanticQuery` structurée ; aucune chaîne SQL.
- Appel obligatoire à certification gate et `query_with_evidence`.
- Limites de lignes, timeout, métriques/scopes autorisés et format de sortie borné.
- Tests injection, dépassement, source non certifiée et preuve retournée.

### Slice 7.4 — Auth déléguée et scopes

- HTTP : token par requête et scopes minimaux ; stdio : identité/config locale explicite.
- Scopes proposés : `models:read`, `contracts:read`, `lineage:read`, `semantic:query`.
- Aucun token dans logs/traces ; tests cross-user et scope escalation.

### Slice 7.5 — Packaging, observabilité et compatibilité clients

- Extra `[mcp]`, commande de lancement, healthcheck et documentation.
- Traces de chaque resource/tool call avec identité déléguée pseudonymisée.
- Tests smoke avec client MCP de référence et matrice versions supportées.

## 7.3 Critères d'acceptation

- Aucun endpoint ne permet du SQL brut ou une lecture de table brute.
- Le serveur ne retourne que les ressources visibles par l'appelant.
- Toute query MCP reçoit les mêmes guards et preuves que l'API Python.

---

# Feature 8 — Adaptive Gold supervisé

## 8.1 Approfondissement

Adaptive Gold transforme l'usage réel de la couche sémantique en recommandations de modèles
physiques : materialized view, agrégation ou nouvelle table Gold. Il ne doit pas auto-déployer.

Les traces Skifer sont la source primaire, car elles connaissent métriques, dimensions,
certifications et hash. `system.query.history` et les system tables de lineage Databricks sont des
enrichissements optionnels ; leur disponibilité peut être retardée et leur accès est restreint.

Signaux utiles :

- fréquence d'une combinaison métriques/dimensions/filtres ;
- latence et volume scanné ;
- répétition de mêmes jointures ;
- échecs dus à un modèle sémantique incomplet ;
- divergence entre grain demandé et modèle disponible.

Le résultat est une **proposition versionnée** avec preuve, estimation et YAML diffable.

## 8.2 Plan de développement — 6 slices

### Slice 8.1 — Événements d'usage normalisés

- Nouveau schéma `SemanticUsageEvent` sans question brute obligatoire.
- Fingerprint stable basé sur IDs de métriques/dimensions, filtres normalisés et modèles.
- Store SQLite/Delta avec politique de rétention.
- Tests anonymisation, fingerprint et versioning.

### Slice 8.2 — Agrégateur de patterns

- Fenêtres fréquence/latence/coût, seuils minimums et exclusion des requêtes ponctuelles.
- Ne pas mélanger environnements, consumers ou versions de définition incompatibles.
- Tests statistiques déterministes et données insuffisantes.

### Slice 8.3 — Moteur de recommandations explicable

- Règles v1 déterministes : MV agrégée, nouvelle dimension préparée, relation manquante, indexation
  hors périmètre Spark.
- Chaque recommandation contient signaux, requêtes couvertes, bénéfice attendu et risques.
- Pas de ML tant que les règles ne disposent pas d'un historique évalué.
- Tests de chaque règle et absence de recommandation dangereuse.

### Slice 8.4 — Génération YAML via Plan 28

- Réutiliser `aggregate`, `sql_compiler` et `materialized_view` ; aucune seconde génération SQL.
- Générer dans `.skifer_proposals/`, jamais dans `schemas/` directement.
- Valider le YAML avec le loader et produire le draft sémantique Feature 0 associé.
- Tests de compilation et parité.

### Slice 8.5 — Workflow humain accept/reject

- CLI list/show/diff/accept/reject avec raison.
- `accept` copie vers une branche/espace de travail explicite ; ne lance pas le déploiement.
- L'issue du review nourrit l'évaluation future des règles.
- Tests idempotence, proposition devenue obsolète et permissions filesystem.

### Slice 8.6 — Mesure après livraison

- Relier proposition acceptée, définition déployée et traces postérieures.
- Comparer latence/coût/couverture avant-après et détecter une régression.
- Recommander la dépréciation, jamais la suppression automatique.
- E2E local et documentation Databricks query-history optionnelle.

## 8.3 Critères d'acceptation

- Toute proposition est explicable et reproductible.
- Aucun artefact de production n'est créé sans validation humaine.
- Le YAML proposé passe les mêmes validateurs que le code écrit à la main.
- Le bénéfice réel peut être mesuré après adoption.

---

# Feature 9 — Capability model et write-back gouverné

## 9.1 Approfondissement

Cette feature est volontairement dernière. Elle étend Skifer au-delà de l'analytique vers
des actions sur systèmes métiers. Le design doit porter sur des capacités métier, pas sur un wrapper
automatique de chaque endpoint.

Surface indicative :

```yaml
capabilities:
  - id: support.create_ticket
    owner: support-platform
    mode: write
    acting_as: delegated_user
    required_scopes: [tickets:create]
    input_schema: CreateTicketInput
    preconditions:
      - rule: service_is_known
      - rule: no_open_duplicate
    reversibility: compensatable
    compensation: support.close_ticket
    approval: supervised
    idempotency_key: request_id
    provenance:
      policy: policies/support-ticket-v3.md
```

Les préconditions sont du code/règles déterministes et consultent l'état live au moment de l'action.
Un document récupéré ou un LLM peut informer la proposition, jamais autoriser l'action. Un cas non
déclaré escalade au lieu d'être improvisé.

## 9.2 Plan de développement — 8 slices

### Slice 9.1 — Schéma du capability model

- IDs, owner, mode, input/output schema, scopes, préconditions, réversibilité, approval,
  idempotence et provenance.
- Validation stricte et modèle catalog-first/lazy séparé du modèle sémantique.
- Tests de toutes les incompatibilités.

### Slice 9.2 — Registry et executor read-only

- Registry de capacités déclarées ; aucun import dynamique arbitraire.
- Commencer par des capacités live-read pour éprouver sélection, auth et trace.
- Résultat structuré avec preuves et freshness.

### Slice 9.3 — Évaluateur déterministe de préconditions

- Registry de règles pures ; résultat allow/deny/unknown avec raisons.
- Recheck juste avant l'action ; `unknown` = supervision obligatoire.
- Tests changement d'état entre plan et action.

### Slice 9.4 — Machine d'état d'autonomie

- `shadow → supervised → guarded_autonomous`; pas de full autonomy en v1.
- Transitions basées sur configuration et preuves d'évaluation, jamais décidées par le LLM.
- Journal des propositions et décisions humaines.

### Slice 9.5 — Identité déléguée et credentials JIT

- Interface de token exchange/factory ; aucun credential persistant dans le capability YAML.
- Scope et durée minimaux par invocation.
- Tests expiration, mauvais audience/resource, cross-user et redaction.

### Slice 9.6 — Exécution idempotente et compensation

- Idempotency key obligatoire pour write ; états `PREPARED`, `APPROVED`, `EXECUTED`, `COMPENSATED`.
- Compensation explicite seulement si déclarée ; une action irréversible reste supervisée.
- Tests retry réseau, réponse perdue, double submit et compensation échouée.

### Slice 9.7 — MCP Tools gouvernés

- Exposer seulement les capacités approuvées pour l'appelant.
- Description riche mais schéma borné ; pas de conversion automatique API→tools.
- Tool call relié à capability version, policy, actor, approval et trace.

### Slice 9.8 — Harness d'évaluation et pilote limité

- Replay/mocks déterministes des outils et LLM.
- Dataset de décisions attendues, faux positifs/négatifs, taux d'escalade et réversibilité.
- Pilote sur une capacité réversible à faible blast radius ; aucune capacité financière en premier.

## 9.3 Critères d'acceptation

- Aucun write sans identité, scope, preconditions, idempotency key et trace.
- Aucun texte récupéré ne peut autoriser une action.
- Une action irréversible exige une approbation humaine.
- Le mode shadow peut être évalué entièrement sans effet externe.

---

## 5. Découpage recommandé en plans exécutables

Ce document est trop large pour une branche. Après validation architecturale, le scinder ainsi :

| Futur plan | Contenu | Slices | Résultat livrable autonome |
|---|---|---:|---|
| Plan 29A | Feature 0 — projection sémantique parallèle | 6 | draft et CI sémantiques avant déploiement |
| Plan 29B | Feature 6 — domaine et jointures sémantiques | 6 | requêtes multi-modèles déterministes |
| Plan 30 | Features 2, 1, 3 — certification gateway | 13 | seuls les datasets certifiés alimentent les agents |
| Plan 31 | Features 4 et 5 — preuves et tracing | 9 | réponse reconstructible de bout en bout |
| Plan 32 | Features 7 et 8 — MCP read-only et Adaptive Gold | 11 | accès agent standard + optimisation supervisée |
| Plan 33 | Feature 9 — capabilities write-back | 8 | première action gouvernée en shadow/supervised |

Le suffixe `29A/29B` est une notation de programme. Lors de la création réelle des fichiers, utiliser
les prochains numéros libres afin de conserver la convention numérique du dépôt.

---

## 6. Chemin critique recommandé

Pour répondre au besoin immédiat des projets intégrateurs, ne pas attendre les 53 slices.

### MVP utile après 5 slices

1. Slice 0.1 — contrat/semantic seed dans l'IR ;
2. Slice 0.2 — projection statique du schéma ;
3. Slice 0.3 — draft déterministe ;
4. Slice 0.5 — `semantic sync --check` en CI ;
5. Slice 6.1 — entités et grain dans le modèle.

Ce MVP permet déjà à deux chantiers de progresser en parallèle :

```text
Data engineer                         Analytics engineer
-------------                         ------------------
modifie pipeline YAML       ───────▶  reçoit le draft et le diff
précise types/grain/clés               définit sens, synonymes, métriques
voit les conflits semantic  ◀───────  valide dans la même PR
```

### Premier incrément production après 12 slices

Livrer toute la Feature 0 puis toute la Feature 6. Le bénéfice est indépendant de la certification :
la couche sémantique est construite plus tôt, reste synchronisée, et les queries multi-modèles ne
reposent pas sur des joins inventés.

### Deuxième incrément production après 25 slices cumulées

Ajouter le Plan 30 (13 slices). La couche sémantique accélérée devient alors une frontière de
confiance réellement exploitable par des agents.

---

## 7. Risques transverses et décisions proposées

| Risque | Décision / mitigation |
|---|---|
| Le YAML pipeline devient un fourre-tout | Séparer transformation, contrat et semantic seed en blocs typés ; chaque information a un propriétaire explicite |
| Deux sources de vérité entre pipeline et modèle sémantique | Projection déterministe + merge à trois voies + IDs stables ; jamais de réécriture silencieuse |
| Le LLM invente colonnes ou joins | LLM limité à l'enrichissement ; validator contre `ProjectedSchema`; QueryResolver reste seul compilateur SQL |
| Une règle Python empêche d'inférer le schéma | Type `unknown` et diagnostic ; possibilité future d'enrichir le décorateur de règle avec outputs/types |
| Explosion du périmètre avant valeur utilisateur | MVP Feature 0 en cinq slices utiles ; certification et MCP restent indépendants |
| Confusion data freshness / load freshness | Deux checks et deux timestamps distincts dès le registre |
| Fausse promesse de raisonnement auditable | Stocker décisions structurées, règles, preuves et sources ; jamais exiger la chaîne de pensée privée du LLM |
| Données sensibles dans tags ou traces | Allowlist/redaction ; tags UC réservés aux métadonnées non sensibles |
| Adaptive Gold produit des coûts ou objets inutiles | Proposition uniquement, seuils minimums, review humaine et mesure avant/après |
| Write-back élargit trop le produit | Plan séparé, après preuves/auth/tracing ; premier pilote réversible et supervisé |

---

## 8. Points à arbitrer avant de transformer ce programme en plan de build

1. **Emplacement du contrat et du semantic seed** : dans le même YAML pipeline (proposition actuelle)
   ou dans des sidecars reliés par `data_product.id` ?
2. **Granularité du modèle sémantique** : un modèle par table Gold, par data product ou plusieurs
   modèles/bounded contexts sur une même table ?
3. **Promotion du draft** : copie explicite vers `semantic_models/` ou statut `draft/published` dans
   un seul fichier ? La copie explicite réduit les risques d'écrasement.
4. **Compatibilité ODCS** : export seulement en v1 (recommandé) ou import également ?
5. **Contrats sur Silver** : autorisés pour CI/qualité, mais exposables aux agents ou Gold uniquement ?
   Recommandation : agents = Gold certifié par défaut, override explicite en développement.
6. **Business rules Python** : imposer à terme `outputs` et types dans le décorateur pour rendre la
   projection complète, ou accepter durablement `unknown` ?
7. **Catalogue sémantique fédéré** : répertoire Git uniquement en v1, ou miroir Unity Catalog dès la
   Feature 0 ? Recommandation : Git d'abord, miroir UC dans Feature 2.
8. **Consumer classes initiales** : `dashboard`, `agent_read`, `agent_action` semblent suffisantes ;
   éviter un système de policy générique avant les cas réels.

---

## 9. Definition of Done du programme de réflexion

Avant tout développement :

- [ ] objectif produit et Feature 0 validés par le superviseur ;
- [ ] arbitrages de la section 8 tranchés ;
- [ ] validation/re-plan par Claude selon le workflow du Plan 20 ;
- [ ] découpage en plans exécutables numérotés, avec fichiers exacts et cas de tests détaillés ;
- [ ] page GBrain `plan:<tâche>` écrite par l'architecte lorsque `gbrain` est de nouveau disponible ;
- [ ] aucune version `pyproject.toml` modifiée ;
- [ ] chaque slice de build future correspond à un commit séparé.

---

## 10. Conclusion

Le chemin le plus différenciant pour Skifer n'est pas de générer plus vite un YAML
sémantique après la livraison d'une table. Il est de rendre le modèle sémantique **co-évolutif** du
pipeline : prévisible avant Spark, diffable en CI, enrichissable sans perte de curation, puis relié à
un contrat et à une certification runtime.

La certification, les preuves, MCP et Adaptive Gold deviennent alors des extensions naturelles de
ce même artefact, plutôt que quatre sous-systèmes parallèles.
