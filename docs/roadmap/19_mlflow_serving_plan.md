# Plan 19 — MLflow ChatModel Serving & Databricks Deployment

> **Branche :** `feat/mlflow-serving`
> **Statut :** En attente de validation utilisateur
> **Exécutant :** Claude Sonnet 4.6 (en session, supervisé)
> **Superviseur :** Julien Houbart
> **Date :** 2026-06-17

---

## Contexte

Le CLI `skifer hub` est aujourd'hui le seul point d'entrée utilisateur. Il fonctionne
en local ou sur un poste connecté à un cluster Databricks via Connect. L'objectif est de
rendre l'`AgenticHub` accessible nativement sur Databricks : via un endpoint REST
OpenAI-compatible (Model Serving), intégrable dans Genie, des notebooks, ou des
applications externes.

**Décision architecturale clé** : `AgenticHub.ask(str) → HubResponse` est déjà l'API
propre. Le CLI n'est qu'un REPL terminal autour de cet appel. Le plan consiste à ajouter
une couche de sérialisation MLflow — sans modifier l'`AgenticHub` ni aucun agent existant.

### Ce qui est déjà en place

| Existant | Pertinence |
|---|---|
| `AgenticHub.ask(question)` | Interface unique à exposer — inchangée |
| `OpenAIProvider` accepte `base_url` en kwarg | `DatabricksProvider` en est une sous-classe triviale |
| `LLMProvider` ABC + `get_llm_provider()` factory | Il suffit d'ajouter `"databricks"` à `_PROVIDER_MAP` |
| `HubResponse` types (`AgentResponse`, `LineageResponse`, …) | Tous ont `text_output` / `narrative` / `explanation` — sérialisables en string |
| `pyproject.toml` groupes optionnels (`llm-openai`, `llm-anthropic`, …) | Ajouter un groupe `serving` |

### Ce qui manque

1. **`DatabricksLLMProvider`** — appels LLM via Foundation Models API (OpenAI-compatible)
2. **`SkiferChatModel`** — wrapper `mlflow.pyfunc.ChatModel` autour de l'hub
3. **`_response_serializer.py`** — conversion `HubResponse → str` propre (partagée CLI + serving)
4. **`scripts/deploy_to_databricks.py`** — script de log MLflow + création de l'endpoint
5. **Groupe optionnel `[serving]`** dans `pyproject.toml`
6. **Tests** et mise à jour des docs

---

## Règles d'exécution

1. **Un point du plan = un commit séparé.** Message conventionnel :
   `feat(plan19-1): add DatabricksLLMProvider`.
2. **Après chaque point** : `pytest tests/ -x --tb=short` au vert + `ruff check src/` propre.
3. **Chaque modification de `src/`** : test associé + entrée `CHANGELOG.md` sous
   `## [Unreleased]`.
4. **Ne jamais toucher au numéro de version** (`pyproject.toml`).
5. **Aucun breaking change sur l'API publique** — le CLI, `AgenticHub`, et tous les agents
   restent identiques.
6. **Transversal** : docstrings et messages en anglais dans les nouveaux fichiers.

---

## Vue d'ensemble et ordre d'implémentation

```
Phase 1 — DatabricksLLMProvider          autonome
Phase 2 — Package serving/ + sérialiseur  autonome (peut être en parallèle avec 1)
Phase 3 — SkiferChatModel            dépend de 1 + 2
Phase 4 — Script de déploiement          dépend de 3
Phase 5 — Tests                          dépend de 1 + 2 + 3
Phase 6 — Docs + CLAUDE.md / AGENTS.md   dépend de tout
```

Ordre linéaire recommandé : **1 → 2 → 3 → 4 → 5 → 6**

---

## Architecture cible

```
┌────────────────────────────────────────────────────────────────────┐
│  DATABRICKS                                                        │
│                                                                    │
│  ┌───────────────┐  ┌─────────────────┐  ┌──────────────────────┐ │
│  │  Genie Space  │  │ Notebook / REST │  │ External App (curl…) │ │
│  │  (Tool call)  │  │     client      │  │                      │ │
│  └───────┬───────┘  └────────┬────────┘  └──────────┬───────────┘ │
│          └───────────────────┴──────────────────────┘             │
│                               │ POST /invocations                  │
│                  ┌────────────▼─────────────────────┐             │
│                  │   Model Serving Endpoint          │             │
│                  │   (OpenAI /chat/completions)      │             │
│                  └────────────┬─────────────────────┘             │
│                               │                                    │
│                  ┌────────────▼─────────────────────┐             │
│                  │   SkiferChatModel              │             │
│                  │   mlflow.pyfunc.ChatModel          │             │
│                  │                                   │             │
│                  │   load_context() → hub init       │             │
│                  │   predict(messages) → hub.ask()   │             │
│                  └────────────┬─────────────────────┘             │
│                               │                                    │
│              ┌────────────────▼──────────────────┐                │
│              │  AgenticHub  (inchangé)            │                │
│              │  intent router → GenBIAgent        │                │
│              │               → LineageAgent       │                │
│              │               → QualityAgent       │                │
│              │               → DictionaryAgent    │                │
│              │               → BuilderAgent       │                │
│              └────────────┬──────────────┬────────┘                │
│                           │              │                         │
│          ┌────────────────▼──┐   ┌───────▼──────────────────┐     │
│          │ DatabricksLLM     │   │ SparkSession              │     │
│          │ Provider (new)    │   │ (Unity Catalog / Delta)   │     │
│          │ Foundation Models │   │                           │     │
│          │ API (DBRX, Llama) │   │                           │     │
│          └───────────────────┘   └───────────────────────────┘     │
└────────────────────────────────────────────────────────────────────┘
```

---

## Phase 1 — `DatabricksLLMProvider`

**Fichier modifié :** `src/skifer/semantic/llm_provider.py`

### Rationale

Databricks Foundation Models expose une API **OpenAI-compatible** à l'URL
`{DATABRICKS_HOST}/serving-endpoints`. L'`OpenAIProvider` existant accepte déjà
`base_url` en kwarg (ligne 98 du fichier actuel). Le `DatabricksProvider` est donc
une spécialisation qui pré-configure `base_url` et utilise `DATABRICKS_TOKEN` comme
clé API.

### Contrat

```python
class DatabricksProvider(LLMProvider):
    """
    LLM provider for Databricks Foundation Models API (OpenAI-compatible).

    Configuration via environment variables:
        DATABRICKS_HOST  — workspace URL (e.g. https://adb-xxx.azuredatabricks.net)
        DATABRICKS_TOKEN — personal access token or service principal secret
        DATABRICKS_LLM_MODEL — model endpoint name (default: databricks-dbrx-instruct)
    """
    def __init__(
        self,
        host: str | None = None,
        token: str | None = None,
        model: str | None = None,
        **kwargs: Any,
    ) -> None: ...
```

**Auto-détection ajoutée dans `_auto_detect_provider()`** :
si `DATABRICKS_HOST` et `DATABRICKS_TOKEN` sont présents → retourne `"databricks"`.

**Modèles supportés (liste non exhaustive) :**
- `databricks-dbrx-instruct` (défaut)
- `databricks-meta-llama-3-1-70b-instruct`
- `databricks-mixtral-8x7b-instruct`

### Tests

**Fichier modifié :** `tests/test_llm_provider.py`

- `test_databricks_provider_init_from_env` — vérifie que `DATABRICKS_HOST` +
  `DATABRICKS_TOKEN` construisent le client sans erreur (mock `openai.OpenAI`).
- `test_databricks_provider_complete` — mock `_client.chat.completions.create`,
  vérifie le bon passage du modèle et de `base_url`.
- `test_auto_detect_databricks` — `DATABRICKS_HOST` + `DATABRICKS_TOKEN` présents,
  `_auto_detect_provider()` retourne `"databricks"`.
- `test_get_llm_provider_databricks` — `get_llm_provider("databricks")` retourne
  une instance `DatabricksProvider`.

---

## Phase 2 — Package `serving/` + sérialiseur de réponses

### 2.1 — Structure du package

**Fichiers créés :**

```
src/skifer/serving/
    __init__.py          # exports : SkiferChatModel
    chat_model.py        # SkiferChatModel
    _response_serializer.py  # hub_response_to_text()
```

**`pyproject.toml`** — ajout du groupe optionnel :

```toml
[project.optional-dependencies]
serving = [
    "mlflow>=2.12.0",
    "openai>=1.0.0",   # requis par DatabricksProvider
]
```

Installation : `pip install -e ".[serving]"`

### 2.2 — `_response_serializer.py`

Convertit n'importe quel `HubResponse` en chaîne lisible.
Cette logique est extraite du `_render_response()` existant dans `cli.py` et
centralisée ici pour être partagée entre le CLI et le serving.

```python
def hub_response_to_text(response: object) -> str:
    """Convert any HubResponse to a plain text string."""
```

Règles de sérialisation par type :

| Type | Texte retourné |
|---|---|
| `AgentResponse` mode `error` | `response.error` |
| `AgentResponse` mode `kpi` | `"{kpi_label}: {kpi_value}"` |
| `AgentResponse` mode `table` | résumé tabulaire (colonnes + N lignes) |
| `AgentResponse` autre | `response.explanation` |
| `LineageResponse` | `response.diagram or response.narrative` |
| `QualityResponse` | `response.text_output or response.narrative` |
| `DictionaryResponse` | `response.text_output or response.narrative` |
| `BuilderResponse` succès | `response.yaml_content` |
| `BuilderResponse` échec | `response.error` |
| autre | `str(response)` |

> **Note :** Le `cli.py` devra être mis à jour pour importer `hub_response_to_text`
> depuis `serving._response_serializer` et supprimer sa logique de rendu dupliquée.
> Ce refactor CLI est inclus dans ce point.

### Tests

**Fichier créé :** `tests/test_serving_response_serializer.py`

Un test par type de réponse, couvrant les chemins `error` et `success`.

---

## Phase 3 — `SkiferChatModel`

**Fichier créé :** `src/skifer/serving/chat_model.py`

### Contrat

```python
class SkiferChatModel(mlflow.pyfunc.ChatModel):
    """
    MLflow ChatModel wrapping AgenticHub for Databricks Model Serving.

    Artifacts expected at log time:
        "config"     — path to config.yaml
        "models_dir" — path to semantic models directory

    Environment variables expected at serving time:
        DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_LLM_MODEL (optional)
    """

    def load_context(self, context: mlflow.pyfunc.PythonModelContext) -> None:
        """
        Called once when the endpoint pod starts.
        Initializes SparkSession, LLMProvider, and AgenticHub.
        """

    def predict(
        self,
        context: mlflow.pyfunc.PythonModelContext,
        messages: list[dict],
        params: dict | None = None,
    ) -> dict:
        """
        Receives OpenAI-format messages, calls hub.ask(), returns OpenAI-format response.
        """
```

### Détails d'implémentation

**`load_context()`** :

```python
def load_context(self, context):
    from pyspark.sql import SparkSession
    from skifer.core.config import ConfigurationManager
    from skifer.core.core import SkiferEngine
    from skifer.semantic.llm_provider import get_llm_provider
    from skifer.semantic.semantic import SemanticEngine
    from skifer.agentic.hub import AgenticHub

    spark = SparkSession.builder.getOrCreate()
    config_path = context.artifacts.get("config", "config.yaml")
    models_dir = context.artifacts.get("models_dir", "semantic")

    config_mgr = ConfigurationManager(config_path=config_path)
    engine = SkiferEngine(spark, config_manager=config_mgr)
    semantic = SemanticEngine(engine, models_dir=models_dir)
    llm = get_llm_provider("databricks")

    self._hub = AgenticHub(semantic_engine=semantic, llm_provider=llm)
```

**`predict()`** :

```python
def predict(self, context, messages, params=None):
    question = _extract_last_user_message(messages)
    response = self._hub.ask(question)
    text = hub_response_to_text(response)
    return {
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": text},
            "finish_reason": "stop",
        }]
    }
```

**`_extract_last_user_message(messages)`** : extrait le dernier `{"role": "user", ...}`
de la liste. Si aucun message `user` n'est trouvé, lève `ValueError`.

### Gestion de la session Spark

Sur un endpoint cluster-based Databricks, `SparkSession.builder.getOrCreate()` retourne
la session active du cluster. Le pod est **long-running** : `load_context()` est appelé
une seule fois au démarrage, le hub reste chaud en mémoire pour tous les appels suivants
sur ce pod.

> **Contrainte connue** : les endpoints serverless n'ont pas de Spark natif. Pour les
> agents qui n'en ont pas besoin (Lineage, Dictionary, Builder), une future évolution
> pourrait détecter l'absence de Spark et désactiver les agents dépendants plutôt que
> de lever une erreur. Ce cas est hors périmètre du présent plan.

### Tests

**Fichier créé :** `tests/test_serving_chat_model.py`

```python
# Fixtures : mock AgenticHub.ask(), mock load_context() artifacts
def test_predict_returns_openai_format(mock_hub, mock_context): ...
def test_predict_extracts_last_user_message(mock_hub, mock_context): ...
def test_predict_no_user_message_raises(mock_context): ...
def test_load_context_wires_hub(mock_spark, mock_config, mock_artifacts): ...
```

Aucun Spark réel ni appel LLM dans les tests — tout est mocké.

---

## Phase 4 — Script de déploiement

**Fichier créé :** `scripts/deploy_to_databricks.py`

Script autonome, exécutable depuis un notebook Databricks ou un terminal.
Il réalise les étapes suivantes :

1. **Log MLflow** : enregistre `SkiferChatModel` avec les artifacts (config.yaml,
   répertoire semantic/) et les dépendances pip dans l'expérience MLflow active.
2. **Enregistrement UC** : enregistre le modèle dans le Unity Catalog sous
   `{catalog}.skifer.hub`.
3. **Affichage des instructions** pour créer l'endpoint Model Serving via l'UI ou l'API
   REST Databricks (la création de l'endpoint n'est pas automatisée pour éviter un
   déploiement non voulu).

```python
# Usage :
#   python scripts/deploy_to_databricks.py
#   python scripts/deploy_to_databricks.py --config path/config.yaml --models-dir semantic/
```

Paramètres CLI du script :

| Paramètre | Défaut | Description |
|---|---|---|
| `--config` | `config.yaml` | Chemin vers config.yaml à inclure dans les artifacts |
| `--models-dir` | `semantic/` | Répertoire des modèles sémantiques |
| `--experiment` | `/skifer/hub` | Nom de l'expérience MLflow |
| `--registered-name` | `skifer_hub` | Nom dans le UC Model Registry |

> Ce script n'a pas de tests automatisés (il nécessite un workspace Databricks réel).
> Il inclut en revanche des assertions de validation (existence des fichiers, variables
> d'environnement requises) avec des messages d'erreur explicites.

---

## Phase 5 — Tests de la phase 3 (intégration légère)

**Fichier modifié :** `tests/test_serving_chat_model.py` (complément)

En plus des tests unitaires de la phase 3, ajouter un test d'intégration léger qui
instancie réellement `SkiferChatModel`, lui injecte un hub mocké complet
(tous les agents) et vérifie le round-trip :

```
messages=[{"role": "user", "content": "D'où vient amount_eur ?"}]
→ intent lineage
→ LineageAgent mock retourne LineageResponse(diagram="graph LR...")
→ predict() retourne {"choices": [{"message": {"content": "graph LR..."}}]}
```

---

## Phase 6 — Documentation

### 6.1 — `CLAUDE.md`

- Ajouter `serving/chat_model.py` et `serving/_response_serializer.py` dans la table
  **Package structure**.
- Ajouter le plan 19 dans la table **Active plans**.
- Ajouter le groupe `[serving]` dans la section **Commands**.

### 6.2 — `AGENTS.md`

- Ajouter une section **Databricks Deployment** décrivant le `SkiferChatModel` et
  son rôle.

### 6.3 — `CHANGELOG.md`

- Entrée sous `## [Unreleased]` pour chaque phase (phases 1 à 4 au minimum).

### 6.4 — `docs/` (optionnel, hors scope implémentation)

- `docs/databricks_deployment.md` — guide pas-à-pas : prérequis, installation,
  variables d'environnement, log MLflow, création de l'endpoint, test via curl,
  intégration Genie.

---

## Risques et décisions ouvertes

| # | Risque | Probabilité | Impact | Mitigation |
|---|---|---|---|---|
| R1 | SparkSession indisponible sur endpoint serverless | Certaine | Moyen | Cluster-based en V1 ; serverless prévu en V2 (détection + dégradation gracieuse) |
| R2 | `mlflow.pyfunc.ChatModel` signature change entre versions MLflow | Faible | Élevé | Pin `mlflow>=2.12,<3` dans le groupe `[serving]` |
| R3 | Agents stateful (GenBI avec cache SemanticEngine) sur pod multi-worker | Faible | Faible | AgenticHub est déjà stateless entre appels ; SemanticEngine lazy-load est thread-safe en lecture |
| R4 | Genie ne supporte pas encore toutes les réponses structurées (tableau Spark) | Probable | Faible | `hub_response_to_text()` dégrade toujours vers du texte plain |

### Décisions ouvertes

- **D1 — Multi-turn** : la V1 prend uniquement le dernier message `user`. Une V2 pourrait
  passer l'historique complet à `AgenticHub` (qui a déjà `complete_with_history` via
  `LLMProvider`). Hors scope du plan 19.
- **D2 — Streaming** : MLflow ChatModel supporte `predict_stream()`. Non prévu en V1.
- **D3 — Auth Genie Tools** : l'intégration Genie Tools nécessite un endpoint public ou
  une configuration réseau Databricks spécifique. Le plan 19 crée l'endpoint ; la
  configuration Genie est un step opérationnel documenté, pas codé.

---

## Récapitulatif des fichiers

| Fichier | Action | Phase |
|---|---|---|
| `src/skifer/semantic/llm_provider.py` | Modifié — ajout `DatabricksProvider` + auto-detect | 1 |
| `src/skifer/serving/__init__.py` | Créé | 2 |
| `src/skifer/serving/_response_serializer.py` | Créé | 2 |
| `src/skifer/serving/chat_model.py` | Créé | 3 |
| `src/skifer/cli.py` | Modifié — import `hub_response_to_text` | 2 |
| `pyproject.toml` | Modifié — groupe `[serving]` | 2 |
| `scripts/deploy_to_databricks.py` | Créé | 4 |
| `tests/test_llm_provider.py` | Modifié — 4 nouveaux tests | 1 |
| `tests/test_serving_response_serializer.py` | Créé | 2 |
| `tests/test_serving_chat_model.py` | Créé | 3 + 5 |
| `CHANGELOG.md` | Modifié — entrées par phase | continu |
| `CLAUDE.md` | Modifié | 6 |
| `AGENTS.md` | Modifié | 6 |

**Total estimé** : ~350 lignes de code de production + ~200 lignes de tests.
Aucun module existant cassé. API publique inchangée.
