# Plan 41 — Skifer derrière un superviseur Genie

> **Statut :** plan rédigé et décisions D1 à D3 validées le 14 octobre 2026 (recommandations retenues).
> Branche : `feat/plan41-genie-supervisor`, partie de `main` (`82ddb3b`).

## Objectif

Sur Databricks, l'utilisateur pose ses questions à **un seul** agent : un superviseur multi-agents
(Agent Bricks). Celui-ci route :

- les questions exploratoires vers un **espace Genie** posé sur les tables Gold publiées par Skifer ;
- les questions sur les **chiffres officiels** vers un **endpoint Skifer** sur Model Serving, dont
  chaque réponse porte sa preuve (SQL haché, sources, certification, décision de politique) et
  refuse un nom de métrique inventé.

```text
utilisateur ─▶ superviseur Agent Bricks
                 ├─▶ espace Genie          (SQL écrit par LLM, tables certifiées uniquement)
                 └─▶ endpoint Skifer       (noms seulement, SQL déterministe, preuve)
                       └─ ResponsesAgent → AgenticHub → SemanticEngine → Spark (Connect serverless)
```

## Constats qui motivent le plan (mesurés le 14 octobre 2026)

1. **L'endpoint de serving n'a jamais fonctionné.** `mlflow.pyfunc.save_model(python_model=…)`
   appelle `predict()` avec des objets `ChatMessage` et non des dicts :
   `_extract_last_user_message` ne trouve aucun message `user` et lève `ValueError`. Reproduit avec
   MLflow 2.22.5 et 3.17.1, avec un hub factice. `params` est lui aussi un `ChatParams` et non un
   dict, et `predict()` renvoie un dict là où MLflow attend une `ChatCompletionResponse`.
2. **`ChatModel` est déprécié** depuis MLflow 3.0 au profit de `ResponsesAgent`. Côté MLflow 3,
   `ResponsesAgentRequest` porte `context` et `custom_inputs`, et `ResponsesAgentResponse` porte
   `custom_outputs` : la place prévue pour la preuve, au lieu d'un champ ajouté au premier niveau.
3. **Il n'y a pas de Spark dans un conteneur Model Serving.** `load_context` appelle
   `SparkSession.builder.getOrCreate()` : sans cluster ni Java, il n'y a pas de session. Le risque R1
   du Plan 19 le notait déjà. `spark_factory` ne sait faire Databricks Connect qu'avec un
   `DATABRICKS_CLUSTER_ID`.
4. **Le serving lit un store de certification vide.** `load_context` et `SkiferEngine.get_agent`
   instancient `SqliteCertificationStore()` par défaut : en `enforce`, toute requête est refusée
   (`MISSING`), puisque les certifications vivent dans le `DeltaCertificationStore` du workspace.
5. **Le modèle LLM par défaut est retiré.** `DATABRICKS_LLM_MODEL` vaut `databricks-dbrx-instruct`
   par défaut, et Databricks a retiré DBRX.
6. **Genie ne voit pas la certification.** `mirror_certification` existe dans
   `observability/uc_mirror.py`, mais rien ne l'appelle.

## Décisions à valider

| # | Décision | Recommandation | Alternative écartée |
|---|---|---|---|
| D1 | Interface de serving | `mlflow.pyfunc.ResponsesAgent`, extra `serving` repinné sur `mlflow>=3,<4`. `SkiferChatModel` est **supprimé** : il n'a jamais pu être sauvegardé (constat 1), aucun déploiement ne peut en dépendre. | Réparer `ChatModel` : interface dépréciée, et la preuve resterait un champ hors schéma. |
| D2 | Exécution Spark depuis le serving | **Databricks Connect serverless** (`DATABRICKS_SERVERLESS_COMPUTE_ID=auto`). Le moteur reste `SparkBackend`, conformément au Plan 26. | Statement Execution API sur un SQL warehouse : un second runtime, contraire au Plan 26. Connect sur un cluster : un cluster toujours allumé à payer. |
| D3 | Route MCP dans une Databricks App | **Hors périmètre**, plan suivant. Le superviseur n'a besoin que de l'endpoint. | La faire ici : demande en plus un vérificateur de jetons Databricks côté MCP HTTP. |

## Points

Chaque point est un commit séparé (`feat(plan41-N): …`), avec ses tests et son entrée
`CHANGELOG.md` sous `## [Unreleased]`. Ne pas toucher à la version dans `pyproject.toml`.

### 41.1 — Databricks Connect serverless dans `spark_factory`

- **Fichier :** `src/skifer/spark_factory.py`.
- **Comportement :** si `DATABRICKS_SERVERLESS_COMPUTE_ID` est défini (valeur `auto`), et qu'il n'y a
  pas de session notebook active, créer la session par
  `DatabricksSession.builder.serverless().getOrCreate()` avec l'authentification par défaut du SDK
  (`DATABRICKS_HOST` + jeton, ou principal de service par `DATABRICKS_CLIENT_ID`/`SECRET`).
  Priorité : notebook actif > Connect serverless > Connect cluster (`DATABRICKS_CLUSTER_ID`) > local.
  Le mode renvoyé est `"databricks_connect"`, comme pour le chemin cluster.
- **Si `databricks-connect` est absent :** erreur explicite qui nomme l'extra à installer, pas de
  repli silencieux sur `local[*]`.
- **Tests :** `tests/test_spark_factory.py`, `databricks.connect` simulé par `sys.modules`. Couvrir la
  priorité serverless > cluster, le message d'erreur sans paquet, et l'absence de régression locale.

### 41.2 — Store de certification choisi par l'environnement

- **Fichiers :** `src/skifer/observability/certification_store.py` (fonction
  `default_certification_store(engine)`), `src/skifer/core/core.py` (`get_agent`).
- **Comportement :** `catalog` non nul (Databricks) → `DeltaCertificationStore(engine.backend)` ;
  `catalog: null` → `SqliteCertificationStore()`. `get_agent(certification_store=None)` utilise cette
  fonction au lieu de SQLite en dur. Un store passé explicitement gagne toujours.
- **Qualification par le catalogue** (ajouté en cours de point) : le schéma Delta est
  `<catalog>._skifer_certification`. Skifer ne fixe jamais le catalogue courant de la session ;
  un schéma nu se résoudrait donc dans le catalogue par défaut, qui peut différer entre un
  notebook et l'endpoint. `SparkBackend` cite le schéma des stores avec `quote_fqn` au lieu de
  `quote_ident`, sans changement pour un schéma à une partie.
- **Tests :** les deux branches, plus le store explicite prioritaire, et le SQL émis pour un schéma
  qualifié et pour un schéma nu (`SparkBackend` sur une session simulée).

### 41.3 — Plus de modèle LLM par défaut pour Databricks

- **Fichier :** `src/skifer/semantic/llm_provider.py` (`DatabricksProvider`).
- **Comportement :** `DATABRICKS_LLM_MODEL` devient obligatoire. Absent → `ValueError` qui dit quoi
  définir et renvoie à la page *Serving endpoints* du workspace. Aucun nom de modèle codé en dur.
- **Tests :** `tests/test_llm_provider.py`, variable absente → erreur, présente → utilisée.

### 41.4 — `SkiferResponsesAgent` remplace `SkiferChatModel`

- **Fichiers :** `src/skifer/serving/responses_agent.py` (nouveau), suppression de
  `serving/chat_model.py`, `serving/__init__.py`, `pyproject.toml` (extras `serving` et voisins :
  `mlflow>=3,<4`, ajouter `databricks-connect` à `serving`).
- **`load_context` :** session par `get_spark_session()` (41.1), store par
  `default_certification_store` (41.2), LLM par `get_llm_provider("databricks")` (41.3). Les chemins
  `config` et `models_dir` viennent toujours de `context.artifacts`.
- **`predict(request: ResponsesAgentRequest) -> ResponsesAgentResponse` :**
  - question = dernier élément `input` de rôle `user` (contenu texte ou liste de parties texte) ;
  - `ConsumerContext` construit depuis `request.context.user_id` / `conversation_id` et
    `custom_inputs` (`consumer_id`, `trace_id`), **jamais** `scopes` : la règle actuelle reste
    (constat du Plan 29, `docs/agentic.md`) ;
  - sortie : un élément texte (`create_text_output_item`) avec `hub_response_to_text()` ;
  - preuve : `custom_outputs = {"skifer": {"schema_version": "1", "evidence": …}}` quand elle existe.
- **Ce qui ne change pas :** `AgenticHub`, le sérialiseur `_response_serializer.py`, la politique
  de certification, le tracing (`skifer.response.serialize`).
- **Tests :** remplacer `tests/test_serving_chat_model.py` par `tests/test_serving_responses_agent.py`,
  avec un hub factice. Cas : extraction de la question (texte et parties), refus sans message
  `user`, `scopes` jamais rempli même si `custom_inputs` en contient, preuve dans `custom_outputs`,
  absence de `custom_outputs.skifer` sans preuve.

### 41.5 — Test d'intégration MLflow réel

- **Fichier :** `tests/test_serving_mlflow_roundtrip.py`, ignoré par
  `pytest.importorskip("mlflow")`.
- **Ce qu'il fait :** la sonde du constat 1, en positif. Il sauvegarde l'agent avec un
  `load_context` qui pose un hub factice, recharge par `mlflow.pyfunc.load_model`, appelle `predict`
  au format Responses, et vérifie le texte et `custom_outputs`.
- **CI :** ajouter une étape au job Python 3.12 de `.github/workflows/tests.yml` qui installe
  l'extra `serving` et lance ce seul fichier. C'est ce test qui aurait détecté le constat 1.

### 41.6 — `log_agent` : journaliser l'agent avec ses ressources

- **Fichier :** `src/skifer/serving/deploy.py`, exporté par `skifer.serving`.
- **Signature :**
  `log_agent(config_path, models_dir, llm_endpoint, *, registered_model_name=None, warehouse_id=None)`.
- **Comportement :** `mlflow.pyfunc.log_model(python_model=SkiferResponsesAgent(), artifacts={...},
  resources=[DatabricksServingEndpoint(llm_endpoint), …], pip_requirements=["skifer[serving]==<version installée>"])`.
  Déclarer les ressources permet au serving de fournir l'authentification automatique vers l'endpoint
  LLM. Renvoie le `ModelInfo`. Ne déploie rien : le déploiement reste une étape du runbook (41.8).
- **Tests :** `mlflow` simulé : artefacts, ressources et version de `pip_requirements`.

### 41.7 — Projeter la certification dans Unity Catalog

- **Fichiers :** `src/skifer/observability/publication.py`, `src/skifer/core/config.py`.
- **Comportement :** option d'environnement `uc_certification_tags: true` (défaut `false`). Après
  une promotion **ou** une quarantaine, appeler `mirror_certification` sur la table cible. Le
  résultat `SYNC_ERROR` est journalisé en WARNING et **n'échoue jamais** la publication
  (comportement documenté de `uc_mirror.py`).
- **Pourquoi :** l'espace Genie peut alors être restreint aux tables taguées
  `certification = CERTIFIED`, et un humain voit la certification dans Catalog Explorer.
- **Tests :** option absente → aucun appel ; présente → un appel par publication, avec le statut ;
  erreur de permission → publication réussie et warning.

### 41.8 — Documentation et runbook

- **Fichiers :** `docs/databricks_supervisor.md` (nouveau, dans la nav de `mkdocs.yml`),
  `docs/agentic.md` (section *Serving* réécrite), `CLAUDE.md` (entrée `serving/`).
- **Contenu du runbook :**
  1. Prérequis : serverless activé, un endpoint LLM, `uc_certification_tags: true` en PROD.
  2. `log_agent(...)` depuis un notebook, puis déploiement de l'endpoint.
  3. Espace Genie : tables Gold taguées `CERTIFIED` uniquement.
  4. Superviseur Agent Bricks : deux agents, avec les **descriptions de routage** à copier, par
     exemple « Chiffres officiels, KPI et métriques certifiées : Skifer » et « Exploration libre :
     Genie ».
  5. Vérification : une question KPI renvoie `custom_outputs.skifer.evidence`, une métrique
     inventée est refusée, une table non certifiée est refusée en `enforce`.
- **Garde-fou :** `mkdocs build --strict` sans warning.

### 41.9 — Validation manuelle sur le workspace (hors CI)

À faire par l'utilisateur ou avec lui, pas par un agent seul : il faut le workspace.

- l'endpoint démarre (session Connect serverless, store Delta, LLM configuré) ;
- les cinq vérifications du runbook 41.8, étape 5 ;
- le superviseur accepte l'endpoint comme agent. **À confirmer en premier** : le type d'endpoint
  que le superviseur accepte dépend du produit au moment du test, et ce dépôt ne peut pas le prouver.

## Hors périmètre

- Le serveur MCP de Skifer dans une Databricks App (D3).
- L'identité authentifiée de l'appelant (on-behalf-of-user). Le `user_id` reste une métadonnée
  déclarée par le client, jamais une autorisation.
- Le streaming des réponses (`predict_stream`).
- L'export du modèle sémantique vers Genie (instructions, exemples SQL) ou vers des metric views UC.

## Risques

| # | Risque | Mitigation |
|---|---|---|
| R1 | Le superviseur n'accepte pas un endpoint `ResponsesAgent` personnalisé | Vérifié en premier en 41.9. Repli : l'endpoint reste utilisable seul, depuis un notebook, une app ou le Playground. |
| R2 | Démarrage à froid lent de Connect serverless | Session ouverte une seule fois dans `load_context` ; à mesurer en 41.9. |
| R3 | Le passage à `mlflow>=3` casse un autre extra | 41.4 met à jour tous les extras qui pinnent `mlflow<3` ; la CI de 41.5 le vérifie. |
