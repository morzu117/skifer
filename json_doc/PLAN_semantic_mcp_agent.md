# Plan — Couche Sémantique, MCP & Agent GenBI
> Document de travail — v0.11 — mis à jour suite échanges

---

## Décisions actées (v0.7)

| # | Décision | Détail |
|---|---|---|
| 1 | **Source des YAML** | Notebooks `.ipynb` + docstrings rules + Business Glossary |
| 2 | **LLM abstrait** | Interface provider-agnostic : OpenAI, Anthropic, Google |
| 3 | **Périmètre v1** | Uniquement création de **vues SQL** |
| 4 | **Split YAML** | Arborescence : `semantic_models/<model_name>/<split_value>.yaml` |
| 5 | **Split values** | Toujours explicites |
| 6 | **Validation** | 3 boucles de correction max → `.errors/` si KO |
| 7 | **Prompt strategy** | Multi-step : col inventory → dimensions → metrics → assemblage |
| 8 | **Layer by layer** | Génération à n'importe quel layer, lineage optionnel |
| 9 | **Config LLM** | Lue depuis `.env` — kwargs explicites prioritaires |
| 10 | **Model key convention** | `<model_name>.<split_value>` (ex: `kpi_orders.erp`) |
| 11 | **Business Glossary** | Document de contexte métier — optionnel mais prioritaire sur l'inférence |
| 12 | **Catalogue sémantique** | Fichier index `semantic_catalog.yaml` auto-généré — chargement lazy des YAML |
| 13 | **Agent — sélection modèle** | 2 steps LLM : sélection depuis catalogue → traduction query |
| 14 | **Agent — mode dual** | `query()` retourne DataFrame ; `create_view()` persiste la vue |
| 15 | **Agent — historique** | Historique conversationnel optionnel (multi-turn) |
| 16 | **Séparation LLM / exécution** | LLM = vocabulaire YAML uniquement. `QueryResolver` déterministe = résolution nom → SQL |
| 17 | **LLM sans accès DB** | Le LLM ne voit jamais de données, ne touche jamais à Spark |
| 18 | **Clarification loop** | Ambiguïté ou champ inconnu → l'agent demande des précisions avant d'exécuter |
| 19 | **Format de retour** | Détecté depuis la question : `kpi`, `chart`, `table`, `text_analysis` |
| 20 | **Placement des vues** | Schema dédié configurable par environnement dans `config.yaml` |
| 22 | **Historique session** | Chaque interaction agent loggée dans `SessionHistory` |
| 23 | **Export PDF** | `HistoryExporter.to_pdf()` — rapport d'analyse simple depuis l'historique |
| 21 | **Traçabilité** | Ce fichier MD est commité à chaque échange |

---

## 1. Problème : l'explosion des YAML

Avec le mécanisme de split, le nombre de fichiers YAML peut croître rapidement :

```
kpi_orders      × {erp, crm, web}        × {emea, na, apac}  =  9 fichiers
kpi_marketing   × {fr, de, uk, es, it}                        =  5 fichiers
kpi_ops         × {warehouse_a, b, c}                         =  3 fichiers
silver_orders   × {erp, crm}                                  =  2 fichiers
...
                                                  Total        = 50+ fichiers
```

Le problème est **triple** — des tags seuls ne suffisent pas à le résoudre :

| Problème | Cause | Impact |
|---|---|---|
| **Coût au chargement** | Parser N YAML à chaque instanciation | Lenteur au démarrage |
| **Contexte LLM saturé** | Injecter tous les modèles dans chaque prompt | Dépassement tokens, confusion |
| **Pertinence** | L'agent ne sait pas quel modèle choisir sans tout lire | Mauvaise sélection ou hallucination |

> Des tags seuls ne résolvent pas le problème : il faut quand même ouvrir chaque YAML pour les lire. La solution est un **fichier catalogue** qui centralise les métadonnées légères, permettant un chargement lazy des YAML complets.

---

## 2. Solution : `semantic_catalog.yaml`

### Principe

Le `SemanticBuilder` **génère et maintient automatiquement** un fichier `semantic_catalog.yaml` à la racine de `semantic_models/`. Ce fichier est une **vue légère** de tous les modèles — sans le SQL, sans les filtres inline.

```
semantic_models/
├── semantic_catalog.yaml    ← INDEX — seul fichier chargé au démarrage
├── kpi_orders/
│   ├── erp.yaml             ← chargé uniquement si sélectionné
│   ├── crm.yaml
│   └── web.yaml
├── kpi_marketing/
│   └── global.yaml
└── .errors/
```

### Contenu du catalogue

```yaml
# semantic_catalog.yaml
# Auto-généré par SemanticBuilder — ne pas modifier à la main
_generated_at: "2026-03-27T14:35:00"
_total_models: 9

models:
  - key: kpi_orders.erp
    file: kpi_orders/erp.yaml
    layer: gold
    table: gold.fact_orders
    description: "Métriques commandes ERP — source SAP"
    tags: [orders, revenue, erp, gold]
    dimensions: [region, order_date, product_category]
    metrics: [gross_revenue, nb_orders, avg_basket, high_value_count]
    base_filter: "source_system = 'ERP'"
    parent_model: kpi_orders.base        # lineage optionnel
    generated_at: "2026-03-27"

  - key: kpi_orders.crm
    file: kpi_orders/crm.yaml
    layer: gold
    table: gold.fact_orders
    description: "Métriques commandes CRM — source Salesforce"
    tags: [orders, revenue, crm, gold]
    dimensions: [region, order_date, customer_segment]
    metrics: [gross_revenue, nb_orders, conversion_rate]
    base_filter: "source_system = 'CRM'"
    generated_at: "2026-03-27"

  - key: kpi_marketing.global
    file: kpi_marketing/global.yaml
    layer: gold
    table: gold.fact_campaigns
    description: "Métriques campagnes marketing — tous canaux"
    tags: [marketing, campaigns, gold]
    dimensions: [channel, campaign_date, country]
    metrics: [impressions, clicks, conversions, cpa]
    generated_at: "2026-03-27"
```

### Ce que le catalogue ne contient PAS

- Le SQL des dimensions et métriques
- Les filtres inline des métriques
- Les métadonnées de lineage détaillées
- Le format d'affichage

Ces informations restent dans le YAML complet, chargé uniquement à la demande.

---

## 3. Chargement lazy dans le `SemanticEngine`

### Comportement au démarrage

```python
class SemanticEngine:

    def __init__(self, core_engine, models_dir: str = "semantic_models"):
        self.core        = core_engine
        self.spark       = core_engine.spark
        self.models_dir  = self._resolve_models_dir(models_dir)
        self._catalog    = self._load_catalog()   # O(1) — un seul fichier YAML léger
        self._cache      = {}                      # cache des YAML complets chargés
```

### Résolution lazy

```python
    def _get_model(self, model_key: str) -> dict:
        """
        Retourne le modèle complet.
        Charge le YAML depuis le disque uniquement si pas déjà en cache.
        """
        if model_key not in self._cache:
            entry = self._catalog.get(model_key)
            if not entry:
                raise ValueError(
                    f"Model '{model_key}' not found in catalog. "
                    f"Available: {self.list_models(summary=True)}"
                )
            file_path = os.path.join(self.models_dir, entry["file"])
            with open(file_path, encoding="utf-8") as f:
                content = yaml.safe_load(f)
            self._cache[model_key] = content["models"][0]
        return self._cache[model_key]

    def create_view(self, model_name: str, ...) -> str:
        model = self._get_model(model_name)   # chargement lazy ici
        ...
```

### `list_models()` — navigation sans chargement

```python
    def list_models(
        self,
        tags: list[str] = None,
        layer: str = None,
        summary: bool = False,
    ) -> list[dict]:
        """
        Liste les modèles depuis le catalogue uniquement (pas de lecture YAML).

        tags  : filtre sur les tags (ex: ["orders", "gold"])
        layer : filtre sur le layer (ex: "gold")
        summary : si True, retourne seulement key + description (pour les prompts LLM)
        """
        results = list(self._catalog.values())

        if tags:
            results = [m for m in results if any(t in m.get("tags", []) for t in tags)]
        if layer:
            results = [m for m in results if m.get("layer") == layer]
        if summary:
            return [{"key": m["key"], "description": m["description"]} for m in results]
        return results
```

---

## 4. Tags — convention et usage

Les tags permettent de filtrer les modèles sans ouvrir les YAML. Ils sont générés automatiquement par le `SemanticBuilder` et peuvent être enrichis manuellement dans le catalogue.

### Tags auto-générés

| Source | Tags générés |
|---|---|
| `layer` du modèle | `gold`, `silver`, `bronze` |
| Dossier parent | `kpi_orders`, `kpi_marketing` |
| Valeur de split | `erp`, `crm`, `web`, `emea` |
| Table source (schéma) | `fact_orders`, `dim_customers` |

### Tags personnalisés (ajoutés dans le catalogue manuellement)

```yaml
tags: [orders, revenue, erp, gold, finance, q4-2024]
```

### Usage

```python
# Trouver tous les modèles Gold liés aux commandes
engine.list_models(tags=["orders"], layer="gold")

# Résumé compact pour injection dans un prompt LLM
engine.list_models(tags=["gold"], summary=True)
# → [{"key": "kpi_orders.erp", "description": "Métriques commandes ERP"}, ...]
```

---

## 5. Mise à jour automatique du catalogue par le SemanticBuilder

Chaque fois qu'un YAML est créé ou mis à jour, le `SemanticBuilder` met à jour le catalogue en conséquence. Il n'y a jamais de désynchronisation.

```python
def _update_catalog(self, output_dir: str, new_entry: dict):
    """
    Met à jour semantic_catalog.yaml avec le nouveau modèle.
    Si le modèle existe déjà dans le catalogue, le remplace.
    """
    catalog_path = os.path.join(output_dir, "semantic_catalog.yaml")

    # Charger le catalogue existant ou en créer un vide
    if os.path.exists(catalog_path):
        with open(catalog_path) as f:
            catalog = yaml.safe_load(f) or {"models": []}
    else:
        catalog = {"models": []}

    # Remplacer ou ajouter l'entrée
    catalog["models"] = [
        m for m in catalog["models"] if m["key"] != new_entry["key"]
    ]
    catalog["models"].append(new_entry)

    # Trier par key pour la lisibilité
    catalog["models"].sort(key=lambda m: m["key"])
    catalog["_generated_at"] = datetime.datetime.now().isoformat()
    catalog["_total_models"] = len(catalog["models"])

    with open(catalog_path, "w") as f:
        yaml.dump(catalog, f, allow_unicode=True, sort_keys=False)
```

---

## 6. Impact sur le prompt de l'agent

Au lieu d'injecter tous les YAML dans le contexte LLM, on n'injecte que le **résumé du catalogue** (quelques lignes par modèle). L'agent sélectionne le bon modèle, puis on charge le YAML complet pour l'exécution.

```
Prompt agent (avant) :
  → 50 modèles YAML complets injectés = ~20 000 tokens

Prompt agent (après) :
  → list_models(summary=True) = ~500 tokens
  → Sélection du modèle par l'agent
  → Chargement lazy du YAML complet pour create_view()
```

---

## 7. Structure de fichiers finale

```
src/skifer/
├── semantic/
│   ├── __init__.py
│   ├── semantic.py       # SemanticEngine (lazy loading, catalog-first)
│   ├── builder.py        # SemanticBuilder (génère YAML + met à jour catalogue)
│   ├── extractor.py      # NotebookExtractor + RuleInspector
│   ├── glossary.py       # GlossaryReader (JSON/YAML/TXT/PDF/PPTX)
│   ├── validator.py      # SemanticValidator + ValidationResult
│   └── llm_provider.py   # LLMProvider ABC + factory
```

```
# Côté projet utilisateur
semantic_models/
├── semantic_catalog.yaml        ← INDEX auto-généré (seul fichier chargé au boot)
├── kpi_orders/
│   ├── erp.yaml
│   ├── crm.yaml
│   └── web.yaml
├── kpi_marketing/
│   └── global.yaml
└── .errors/

glossaries/
├── orders_glossary.json
└── marketing_definitions.pdf
```

---

## 8. Couche Agentic — `GenBIAgent`

### 8.0 Principe fondamental : séparation stricte LLM / exécution

```
┌─────────────────────────────────────────────────────────────────────┐
│  LE LLM NE TOUCHE JAMAIS LA BASE DE DONNÉES                         │
│  LE LLM NE PRODUIT QUE DES NOMS — JAMAIS DU SQL                    │
└─────────────────────────────────────────────────────────────────────┘
```

La couche agentic repose sur une **frontière stricte** entre deux responsabilités :

| Responsabilité | Qui | Ce qu'il voit | Ce qu'il produit |
|---|---|---|---|
| **Compréhension** | LLM | Vocabulaire YAML (noms de dims/metrics, descriptions) | `SemanticQuery` — noms uniquement |
| **Résolution** | `QueryResolver` (déterministe) | YAML complet (sql, type, filters) | SQL réel prêt à exécuter |
| **Exécution** | `SemanticEngine` | SQL résolu | DataFrame / Vue Databricks |

Le LLM ne peut **pas inventer** de champ — s'il retourne un nom absent du YAML, le `QueryResolver` rejette immédiatement avec une erreur claire, sans toucher à Spark.

```
Question NL
    │
    ▼  [LLM — Step A]
Sélection du modèle depuis catalogue (noms + descriptions seulement)
    │
    ▼  [LLM — Step B]
SemanticQuery {metrics: ["gross_revenue"], group_by: ["region"]}
    │         ← uniquement des noms du YAML, jamais du SQL
    ▼  [QueryResolver — déterministe, 0 LLM]
ResolvedQuery {SELECT SUM(amount_ttc) AS gross_revenue ... WHERE source_system='ERP'}
    │         ← SQL construit depuis les définitions YAML
    ▼  [SemanticEngine]
DataFrame ou CREATE VIEW dans Databricks
```

### 8.1 Problème du POC actuel

Le `GenBIAgent` du POC souffre de plusieurs faiblesses structurelles :

| Limitation | Impact |
|---|---|
| Modèle cible hard-codé (`default_model`) | Impossible de choisir parmi plusieurs modèles |
| Injection de tout le contexte en un prompt | Explose les tokens si N modèles |
| JSON parsing fragile (`replace('```json','')`) | Casse dès que le LLM dévie |
| Couplé OpenAI | Non réutilisable avec Anthropic / Google |
| Pas d'historique | Chaque question est isolée |

### 8.2 Architecture : pipeline 2 étapes LLM

La clé est de **séparer la sélection du modèle de la traduction de la requête**. Les deux problèmes ne nécessitent pas le même contexte.

```
Question utilisateur
        │
        ▼
╔══════════════════════════════════════════════════════════════════╗
║  STEP A — Model Selection                                        ║
║                                                                  ║
║  Input  : question + catalog summary (compact, ~500 tokens)     ║
║  Prompt : "Parmi ces modèles sémantiques, lequel est le plus    ║
║            pertinent pour répondre à cette question ?"           ║
║  Output : model_key (ex: "kpi_orders.erp")                      ║
║                                                                  ║
║  → Si aucun modèle pertinent : retourner un message clair        ║
║  → Si ambiguïté : retourner les 2-3 candidats + demander        ║
╚══════════════════════════════════════════════════════════════════╝
                           │
                           ▼  chargement lazy du YAML complet
╔══════════════════════════════════════════════════════════════════╗
║  STEP B — Query Translation                                      ║
║                                                                  ║
║  Input  : question + modèle complet (dims + metrics + desc)     ║
║           + historique conversationnel (optionnel)               ║
║  Prompt : "Traduis cette question en requête sémantique.        ║
║            Utilise uniquement les dims/metrics listés."          ║
║  Output : {metrics, group_by, filters, mode}                    ║
║                                                                  ║
║  → mode: "query" (DataFrame) | "view" (vue persistée)           ║
╚══════════════════════════════════════════════════════════════════╝
                           │
                           ▼
╔══════════════════════════════════════════════════════════════════╗
║  STEP C — Execution                                              ║
║                                                                  ║
║  mode="query"  → semantic_engine.query()   → DataFrame          ║
║  mode="view"   → semantic_engine.create_view() → FQN vue        ║
╚══════════════════════════════════════════════════════════════════╝
```

### 8.3 `SemanticQuery` — objet frontière LLM / QueryResolver

`SemanticQuery` est l'**unique point de passage** entre le LLM et le système d'exécution. Il ne contient que des noms — aucune expression SQL.

```python
@dataclass
class SemanticQuery:
    model_name: str              # ex: "kpi_orders.erp"
    metrics: list[str]           # ex: ["gross_revenue", "nb_orders"]
    group_by: list[str]          # ex: ["region", "order_date"]
    filters: list[dict]          # ex: [{"column": "region", "operator": "eq", "value": "EMEA"}]
                                 #     ← "column" doit être un nom de dimension du YAML
    date_from: str | None        # ex: "2024-01-01"
    date_to: str | None
    mode: str                    # "query" | "view"
    view_name: str | None        # requis si mode="view"
    explanation: str             # ce que le LLM a compris (pour debug/UX)
```

**Garanties du contrat :**
- `metrics` : chaque élément doit exister dans `model.metrics[*].name`
- `group_by` : chaque élément doit exister dans `model.dimensions[*].name`
- `filters[*].column` : doit exister dans `model.dimensions[*].name`
- **Aucun SQL, aucune expression, aucune valeur inventée**

### 8.4 `QueryResolver` — résolution déterministe

Le `QueryResolver` est un module **purement déterministe, sans LLM**. Il prend une `SemanticQuery` validée et produit le SQL complet en lisant les définitions du YAML.

```python
@dataclass
class ResolvedQuery:
    select_exprs: list[str]    # ex: ["SUM(amount_ttc) AS gross_revenue", "region AS region"]
    from_fqn: str              # ex: "`catalog`.`gold`.`fact_orders`"
    where_clauses: list[str]   # ex: ["source_system = 'ERP'", "status != 'Cancelled'"]
    group_by_exprs: list[str]  # ex: ["region"]
    full_sql: str              # SQL complet assemblé — prêt à exécuter


class QueryResolver:

    def resolve(self, query: SemanticQuery, model: dict, catalog_fqn: str) -> ResolvedQuery:
        """
        Valide chaque nom dans SemanticQuery contre le YAML.
        Résout les noms en expressions SQL depuis les définitions YAML.
        Lève SemanticQueryError si un nom est absent — jamais de SQL inventé.
        Ne touche pas à Spark, ne lit pas la base.
        """
        self._validate(query, model)   # erreur immédiate si nom inconnu
        return self._build_sql(query, model, catalog_fqn)

    def _validate(self, query: SemanticQuery, model: dict):
        dim_names    = {d["name"] for d in model.get("dimensions", [])}
        metric_names = {m["name"] for m in model.get("metrics", [])}

        for m in query.metrics:
            if m not in metric_names:
                raise SemanticQueryError(
                    f"Metric '{m}' not found in model '{query.model_name}'. "
                    f"Available: {sorted(metric_names)}"
                )
        for d in query.group_by:
            if d not in dim_names:
                raise SemanticQueryError(
                    f"Dimension '{d}' not found in model '{query.model_name}'. "
                    f"Available: {sorted(dim_names)}"
                )
        for f in query.filters:
            if f["column"] not in dim_names:
                raise SemanticQueryError(
                    f"Filter column '{f['column']}' not found in model dimensions."
                )

    def _build_sql(self, query: SemanticQuery, model: dict, catalog_fqn: str) -> ResolvedQuery:
        """
        Construit le SQL depuis les champs 'sql', 'type', 'filters' du YAML.
        Toute la logique SQL vient du YAML — jamais du LLM.
        """
        ...
```

**Exemple de résolution :**

```
SemanticQuery :
  metrics  = ["gross_revenue"]
  group_by = ["region"]
  filters  = [{"column": "region", "operator": "eq", "value": "EMEA"}]

YAML kpi_orders.erp :
  dimensions:
    - name: region   → sql: "region"
  metrics:
    - name: gross_revenue
      sql: "amount_ttc"
      type: sum
      filters: [{sql: "status != 'Cancelled'"}]
  base_filter: "source_system = 'ERP'"

ResolvedQuery :
  select_exprs  = ["region AS region",
                   "SUM(CASE WHEN status != 'Cancelled' THEN amount_ttc END) AS gross_revenue"]
  from_fqn      = "`catalog`.`gold`.`fact_orders`"
  where_clauses = ["source_system = 'ERP'",         ← base_filter YAML
                   "region = 'EMEA'"]               ← filtre SemanticQuery
  group_by_exprs = ["region"]

SQL généré :
  SELECT
      region AS region,
      SUM(CASE WHEN status != 'Cancelled' THEN amount_ttc END) AS gross_revenue
  FROM `catalog`.`gold`.`fact_orders`
  WHERE source_system = 'ERP'
    AND region = 'EMEA'
  GROUP BY region
```

### 8.5 Détection du mode : `query` vs `create_view`

Le mode est déduit de la question par le LLM dans Step B — pas besoin de paramètre explicite dans la plupart des cas.

| Formulation | Mode déduit |
|---|---|
| "Montre-moi le CA par région" | `query` → DataFrame |
| "Quel est le panier moyen ?" | `query` → DataFrame |
| "Crée une vue KPI pour Power BI" | `view` → `create_view()` |
| "Génère la vue du CA mensuel" | `view` → `create_view()` |
| "Donne-moi les commandes ERP du mois" | `query` → DataFrame |

Un paramètre `mode` reste disponible pour forcer le comportement si besoin.

### 8.6 Format de sortie du Step B (structured output)

On utilise le **structured output** (JSON schema contraint) plutôt que du parsing de texte libre. Tous les providers modernes le supportent (`response_format`, `tool_use`).

```python
# Schema JSON attendu du LLM pour Step B
QUERY_SCHEMA = {
    "model_name": "string",       # confirmé ou affiné vs Step A
    "metrics": ["string"],        # noms exacts du YAML
    "group_by": ["string"],       # noms exacts du YAML
    "filters": [                  # optionnel
        {"column": "string", "operator": "string", "value": "any"}
    ],
    "date_from": "string|null",   # ex: "2024-01-01"
    "date_to": "string|null",
    "mode": "query|view",
    "view_name": "string|null",   # requis si mode=view
    "explanation": "string"       # ce que l'agent a compris (pour debug/UX)
}
```

### 8.7 Interface publique du `GenBIAgent`

```python
# skifer/agentic/agent.py

class GenBIAgent:

    def __init__(
        self,
        semantic_engine: SemanticEngine,
        llm_provider: LLMProvider,       # via get_llm_provider() depuis .env
        history: bool = False,           # activer l'historique conversationnel
    ):
        self.semantic  = semantic_engine
        self.llm       = llm_provider
        self._history  = [] if history else None

    def ask(
        self,
        question: str,
        mode: str = "auto",              # "auto" | "query" | "view"
        tags: list[str] = None,          # restreindre la recherche de modèles
        layer: str = None,               # restreindre par layer
    ) -> AgentResponse:
        """
        Prend une question NL, sélectionne le modèle, traduit et exécute.
        Retourne un AgentResponse (DataFrame ou FQN vue + métadonnées).
        """
        ...

    def reset_history(self):
        """Vide l'historique conversationnel."""
        if self._history is not None:
            self._history = []
```

### 8.8 `AgentResponse` — réponse structurée

```python
@dataclass
class AgentResponse:
    question: str
    model_used: str              # ex: "kpi_orders.erp"
    mode: str                    # "query" | "view"
    query_params: dict           # {metrics, group_by, filters, ...}
    explanation: str             # ce que l'agent a compris
    result: DataFrame | str      # DataFrame si query, FQN si view
    error: str | None            # None si succès
```

### 8.9 Historique conversationnel

L'historique est une liste de tours `{role, content}` injectée dans le prompt du Step B. Il permet au LLM de comprendre le contexte des questions de suivi.

```python
# Tour 1
agent.ask("CA par région en 2024")
# → metrics=["gross_revenue"], group_by=["region"], date_from="2024-01-01"

# Tour 2 — question de suivi
agent.ask("Et maintenant seulement pour l'ERP ?")
# → L'historique contient le tour 1
# → LLM comprend : même métriques/dims + filtre source_system='ERP'
# → metrics=["gross_revenue"], group_by=["region"],
#   filters=[{column: "source_system", operator: "eq", value: "ERP"}]
```

L'historique est optionnel (`history=False` par défaut) — dans un contexte batch ou Databricks Job, on ne veut pas d'état entre les appels.

### 8.10 Gestion des ambiguïtés de sélection de modèle

Quand le Step A identifie plusieurs modèles candidats, l'agent ne choisit pas arbitrairement — il retourne une réponse `AMBIGUOUS` avec les options.

```python
# Step A retourne :
{
  "selected": None,
  "candidates": ["kpi_orders.erp", "kpi_orders.crm"],
  "reason": "La question ne précise pas la source (ERP ou CRM). Les deux modèles couvrent 'gross_revenue' et 'region'."
}

# AgentResponse en cas d'ambiguïté :
AgentResponse(
    mode="ambiguous",
    explanation="Plusieurs modèles correspondent. Précisez la source : ERP ou CRM ?",
    result=None,
    error=None
)
```

L'utilisateur peut affiner sa question ou passer `tags=["erp"]` pour forcer.

---

## 8bis. Clarification loop — ambiguïtés et champs inconnus

### Principe

Quand le `QueryResolver` détecte un problème (nom inconnu, modèle ambigu, information manquante), l'agent **ne lève pas d'exception silencieuse** — il formule une question claire à l'utilisateur et attend une réponse avant d'exécuter quoi que ce soit.

### Types de clarification

| Situation | Exemple | Question posée |
|---|---|---|
| Metric inconnue | "chiffre d'affaires net" absent du YAML | "Je ne trouve pas 'chiffre d'affaires net'. Voulez-vous dire : gross_revenue (CA brut TTC) ou avg_basket (panier moyen) ?" |
| Dimension inconnue | "pays" absent, "region" existe | "La dimension 'pays' n'existe pas. Voulez-vous utiliser 'region' (EMEA / NA / APAC) ?" |
| Modèle ambigu | "commandes" → kpi_orders.erp ET kpi_orders.crm | "Deux modèles correspondent. S'agit-il des commandes ERP (SAP) ou CRM (Salesforce) ?" |
| Info manquante | question avec "ce mois" mais pas de date dans la query | "Pour quelle période souhaitez-vous le CA ? (ex: 2024-01-01 à 2024-12-31)" |
| Aucun modèle trouvé | domaine inexistant dans le catalogue | "Aucun modèle sémantique ne couvre ce sujet. Modèles disponibles : [liste]" |

### `AgentResponse` en mode clarification

```python
# Quand une clarification est nécessaire, l'agent retourne un état intermédiaire
AgentResponse(
    mode="needs_clarification",
    explanation="La dimension 'pays' n'existe pas dans kpi_orders.erp.",
    clarification_question="Voulez-vous utiliser 'region' (EMEA/NA/APAC) à la place ?",
    suggestions=["region", "product_category"],   # alternatives proposées depuis le YAML
    result=None,
    error=None
)
```

L'utilisateur répond → `agent.ask()` reçoit la réponse → re-traitement avec l'historique enrichi.

### Génération des suggestions

Les suggestions sont produites **de façon déterministe** par le `QueryResolver` (distance de Levenshtein sur les noms du YAML), puis les descriptions lisibles viennent du YAML. Pas de LLM pour ça.

```python
# QueryResolver._suggest_closest(unknown_name, candidates) → list[str]
# ex: "pays" → ["region", "country_code"] triés par proximité
```

### Flux avec clarification

```
ask("CA par pays en 2024")
    │
    ▼  Step A : modèle = kpi_orders.erp
    ▼  Step B : SemanticQuery(group_by=["pays"])  ← "pays" inventé par le LLM
    │
    ▼  QueryResolver._validate() → "pays" absent
    │                              suggestion : "region"
    │
    ▼  AgentResponse(mode="needs_clarification",
                     clarification_question="Voulez-vous dire 'region' ?")
    │
    [Utilisateur répond "oui"]
    │
    ▼  ask("oui") — historique contient la question + la suggestion
    ▼  Step B : SemanticQuery(group_by=["region"])  ← corrigé
    ▼  QueryResolver → OK → ResolvedQuery → Exécution
```

---

## 8ter. Format de retour — détection et rendu

### Principe

La même query peut servir des usages très différents. L'agent détecte le format attendu depuis la **formulation de la question** et depuis la **structure du résultat**.

### 4 formats supportés

```python
class ResponseFormat(str, Enum):
    KPI            = "kpi"            # valeur unique + label
    TABLE          = "table"          # données tabulaires brutes (DataFrame)
    CHART          = "chart"          # visualisation (config axes + type)
    TEXT_ANALYSIS  = "text_analysis"  # résumé narratif en langage naturel
```

### Règles de détection (Step B — LLM + heuristiques)

Le LLM produit un `format` dans la `SemanticQuery`. Des heuristiques post-traitent en fallback :

| Signal | Format déduit |
|---|---|
| "quel est", "combien", "valeur de" + 1 metric + 0 group_by | `kpi` |
| "montre", "tableau", "liste", "détail" | `table` |
| "évolution", "tendance", "par mois/semaine/année", "graphique" | `chart` |
| "analyse", "explique", "résume", "commente", "pourquoi" | `text_analysis` |
| Défaut si aucun signal | `table` |

### `FormattedResult` — objet de retour enrichi

```python
@dataclass
class FormattedResult:
    format: ResponseFormat
    data: DataFrame              # toujours présent (source de vérité)
    title: str                   # titre généré (ex: "CA brut par région — 2024")

    # Spécifique à chaque format
    kpi_value: any | None        # valeur scalaire si format=kpi
    kpi_label: str | None        # label lisible si format=kpi

    chart_config: dict | None    # si format=chart
    # {
    #   "type": "bar" | "line" | "pie",
    #   "x_axis": "region",       ← dimension du group_by
    #   "y_axis": "gross_revenue", ← première métrique
    #   "series": [...],          ← autres métriques si multi-metric
    # }

    text_summary: str | None     # si format=text_analysis
    # Généré par un appel LLM supplémentaire (Step D) sur le résultat
    # LLM reçoit les données agrégées (pas les données brutes) + la question
```

### Détail : format `text_analysis`

C'est le seul format où le LLM voit des données — mais uniquement les données **agrégées** issues du `ResolvedQuery`, jamais les données brutes de la table source.

```
Step D — Text Analysis (optionnel, uniquement si format=text_analysis)

Input  : résultat agrégé (ex: DataFrame 3 lignes × 2 colonnes) converti en markdown
         + question originale
         + modèle utilisé + métadonnées

Prompt : "Voici le résultat d'une requête sur les KPIs. Rédige une analyse
          concise en 3-5 phrases. Ne réinvente pas de données."

Output : texte narratif (ex: "La région EMEA génère 68% du CA total sur 2024,
          en hausse de 12% vs 2023. L'Amérique du Nord représente 24%...")
```

### Détail : format `chart`

Le `chart_config` n'est pas une image — c'est une **configuration déclarative** que le client (notebook, dashboard, MCP) peut rendre avec la librairie de son choix (matplotlib, plotly, Databricks native charts).

```python
chart_config = {
    "type": "bar",
    "title": "CA brut par région — 2024",
    "x_axis": {"field": "region",        "label": "Région"},
    "y_axis": {"field": "gross_revenue", "label": "CA brut TTC (€)", "format": "€ #,##0"},
    "series": []   # vide si une seule métrique
}
```

---

## 8quater. Placement des vues — configuration par environnement

### Problème

Quand l'agent crée une vue (`mode="view"`), où la place-t-il ? La réponse dépend de la plateforme et de l'environnement (dev/qa/prod).

### Solution : clé `semantic_views_schema` dans `config.yaml`

On étend la configuration existante par environnement :

```yaml
# config.yaml (existant + extensions semantic)
environments:
  dev:
    catalog: my_catalog_dev
    schemas:
      bronze: bronze
      silver: silver
      gold:   gold
    semantic_views_schema: semantic_views_dev   # ← NOUVEAU

  prod:
    catalog: my_catalog_prod
    schemas:
      bronze: bronze
      silver: silver
      gold:   gold
    semantic_views_schema: semantic_views        # ← NOUVEAU
    is_production: true
```

**Résultat :**

| Environnement | Vue créée |
|---|---|
| dev (user: jdupont) | `my_catalog_dev.semantic_views_dev_jdupont.v_ca_region` |
| prod (job) | `my_catalog_prod.semantic_views.v_ca_region` |

Le suffix sandbox (`_jdupont`) suit la même logique que le reste du framework — automatique en dev interactif, absent en job/prod.

### Résolution dans `SemanticEngine.create_view()`

```python
def _resolve_view_schema(self, target_layer: str = None) -> str:
    """
    Si target_layer est fourni → utilise get_target_schema(layer) (comportement existant).
    Si target_layer est None → utilise semantic_views_schema depuis config + sandbox suffix.
    """
    if target_layer:
        return self.core.get_target_schema(target_layer)

    base_schema = (
        self.core.config
        .get("environments", {})
        .get(self.core.env.lower(), {})
        .get("semantic_views_schema", "semantic_views")  # fallback si non configuré
    )
    return f"{base_schema}{self.core.schema_suffix}"   # + "_username" si sandbox
```

### Comportement si `semantic_views_schema` absent du config

Si la clé n'est pas définie, fallback sur `"semantic_views"` avec un warning — le framework ne bloque jamais.

```
[SemanticEngine] ⚠️ 'semantic_views_schema' not configured for env 'DEV'.
                    Using default: 'semantic_views'. Add it to config.yaml to customize.
```

---

## 8quinquies. Historique de session et export PDF

### Principe

Chaque interaction avec le `GenBIAgent` est **automatiquement loggée** dans un `SessionHistory`. À tout moment, l'historique peut être exporté en PDF — un rapport d'analyse simple et lisible, sans configuration.

### `HistoryEntry` — une interaction loggée

```python
@dataclass
class HistoryEntry:
    timestamp: str                  # ISO 8601 — ex: "2026-03-27T14:35:12"
    question: str                   # question originale de l'utilisateur
    model_used: str                 # ex: "kpi_orders.erp"
    explanation: str                # ce que l'agent a compris
    query_params: dict              # {metrics, group_by, filters, date_from, date_to}
    response_format: str            # "kpi" | "table" | "chart" | "text_analysis"

    # Résultat sérialisé (pas le DataFrame complet)
    kpi_value: any | None           # si format=kpi
    kpi_label: str | None
    table_markdown: str | None      # si format=table  → to_markdown() du DataFrame
    chart_config: dict | None       # si format=chart  → config axes
    chart_image_b64: str | None     # si format=chart  → image PNG base64 (matplotlib)
    text_summary: str | None        # si format=text_analysis
    error: str | None               # None si succès, message sinon
```

### `SessionHistory` — container de la session

```python
class SessionHistory:

    def __init__(self, session_title: str = "Analyse KPI"):
        self.title      = session_title
        self.created_at = datetime.datetime.now().isoformat()
        self._entries: list[HistoryEntry] = []

    def add(self, entry: HistoryEntry):
        self._entries.append(entry)

    def entries(self) -> list[HistoryEntry]:
        return list(self._entries)

    def to_pdf(self, output_path: str):
        """Exporte l'historique en PDF. Raccourci vers HistoryExporter."""
        HistoryExporter().to_pdf(self, output_path)

    def to_json(self, output_path: str):
        """Sauvegarde brute de l'historique en JSON (pour debug ou réimport)."""
        ...
```

### Intégration dans `GenBIAgent`

Le `GenBIAgent` instancie et alimente automatiquement un `SessionHistory` :

```python
class GenBIAgent:

    def __init__(self, semantic_engine, llm_provider, history: bool = False,
                 session_title: str = "Analyse KPI"):
        ...
        self._session = SessionHistory(session_title)

    def ask(self, question: str, ...) -> AgentResponse:
        resp = self._process(question, ...)
        self._session.add(self._to_entry(resp))   # log automatique
        return resp

    @property
    def session(self) -> SessionHistory:
        """Accès à l'historique pour export."""
        return self._session
```

### `HistoryExporter` — génération PDF

Librairie choisie : **`fpdf2`** — légère, pure Python, aucune dépendance système, suffisante pour un rapport structuré.

```python
# skifer/agentic/exporter.py

class HistoryExporter:

    def to_pdf(self, session: SessionHistory, output_path: str):
        """
        Génère un PDF depuis l'historique de session.
        Structure : page de garde → une section par interaction.
        """
        from fpdf import FPDF
        pdf = FPDF()
        pdf.set_auto_page_break(auto=True, margin=15)

        self._add_cover_page(pdf, session)

        for i, entry in enumerate(session.entries(), start=1):
            self._add_entry_section(pdf, i, entry)

        pdf.output(output_path)
        print(f"[HistoryExporter] PDF exported: {output_path}")
```

### Structure du PDF généré

```
┌─────────────────────────────────────────┐
│  PAGE DE GARDE                          │
│                                         │
│  [Titre session]                        │
│  Généré le : 2026-03-27 14:35           │
│  Nombre d'analyses : 4                  │
└─────────────────────────────────────────┘

┌─────────────────────────────────────────┐
│  SECTION 1 — 14:35:12                   │
│                                         │
│  Question : "CA par région en 2024"     │
│  Modèle   : kpi_orders.erp              │
│  Analyse  : gross_revenue, group_by     │
│             region, 2024-01-01→12-31    │
│                                         │
│  [FORMAT : KPI]                         │
│  ┌─────────────────┐                    │
│  │  2 847 320 €    │  CA brut TTC       │
│  └─────────────────┘                    │
└─────────────────────────────────────────┘

┌─────────────────────────────────────────┐
│  SECTION 2 — 14:36:40                   │
│                                         │
│  Question : "Détail par région"         │
│  [FORMAT : TABLE]                       │
│                                         │
│  region  | gross_revenue | nb_orders    │
│  --------|---------------|----------    │
│  EMEA    |  1 937 058 €  |    1 204     │
│  NA      |    681 836 €  |      487     │
│  APAC    |    228 426 €  |      193     │
└─────────────────────────────────────────┘

┌─────────────────────────────────────────┐
│  SECTION 3 — 14:38:05                   │
│                                         │
│  Question : "Évolution mensuelle"       │
│  [FORMAT : CHART — bar]                 │
│                                         │
│  [IMAGE PNG intégrée — matplotlib]      │
│                                         │
└─────────────────────────────────────────┘

┌─────────────────────────────────────────┐
│  SECTION 4 — 14:39:22                   │
│                                         │
│  Question : "Analyse les résultats"     │
│  [FORMAT : TEXT ANALYSIS]               │
│                                         │
│  "La région EMEA concentre 68% du CA   │
│   total sur 2024. L'Amérique du Nord   │
│   représente 24%..."                    │
└─────────────────────────────────────────┘
```

### Rendu des charts dans le PDF

Quand `format=chart`, le `GenBIAgent` génère une image PNG avec `matplotlib` et la stocke encodée en base64 dans `HistoryEntry.chart_image_b64`. L'exporteur la décode et l'intègre dans le PDF.

```python
# Dans GenBIAgent._build_formatted_result() si format=chart
import matplotlib
matplotlib.use("Agg")   # mode non-interactif (pas de fenêtre)
import matplotlib.pyplot as plt
import base64, io

fig, ax = plt.subplots()
# build chart from chart_config + df.toPandas()
buf = io.BytesIO()
fig.savefig(buf, format="png", dpi=120, bbox_inches="tight")
entry.chart_image_b64 = base64.b64encode(buf.getvalue()).decode()
plt.close(fig)
```

`matplotlib` est une dépendance optionnelle — si absente, le PDF inclut `chart_config` en JSON à la place de l'image, avec un message d'information.

### Usage

```python
agent = GenBIAgent(semantic, llm, history=True, session_title="Review Q4 2024")

agent.ask("CA total par région en 2024")
agent.ask("Évolution mensuelle du CA EMEA")
agent.ask("Analyse les tendances")
agent.ask("Crée une vue Power BI du CA mensuel")

# Export PDF
agent.session.to_pdf("reports/review_q4_2024.pdf")

# Ou export JSON pour réimport / debug
agent.session.to_json("reports/review_q4_2024.json")
```

---

## 9. Structure de fichiers finale (complète)

```
src/skifer/
├── semantic/
│   ├── __init__.py
│   ├── semantic.py       # SemanticEngine (lazy loading, catalog, create_view)
│   ├── builder.py        # SemanticBuilder (from_notebook, multi-step, catalogue)
│   ├── extractor.py      # NotebookExtractor + RuleInspector
│   ├── glossary.py       # GlossaryReader (JSON/YAML/TXT/PDF/PPTX)
│   ├── validator.py      # SemanticValidator + ValidationResult
│   └── llm_provider.py   # LLMProvider ABC + providers + factory
├── agentic/
│   ├── __init__.py
│   ├── agent.py          # GenBIAgent + AgentResponse + SemanticQuery
│   ├── resolver.py       # QueryResolver + ResolvedQuery + SemanticQueryError
│   ├── history.py        # SessionHistory + HistoryEntry
│   └── exporter.py       # HistoryExporter → PDF (fpdf2) + JSON
└── mcp/                  # Itération 3
```

```
# Côté projet utilisateur
semantic_models/
├── semantic_catalog.yaml
├── kpi_orders/
│   ├── erp.yaml
│   └── crm.yaml
├── kpi_marketing/
│   └── global.yaml
└── .errors/

glossaries/
├── orders_glossary.json
└── kpi_specs.pdf
```

---

## 10. Exemple d'usage bout-en-bout

```python
from skifer import SkiferEngine
from skifer.semantic import SemanticEngine, get_llm_provider
from skifer.agentic import GenBIAgent

# Setup
engine   = SkiferEngine()
semantic = SemanticEngine(engine, models_dir="semantic_models")
llm      = get_llm_provider()   # lit LLM_PROVIDER + clé depuis .env
agent    = GenBIAgent(semantic, llm, history=True)

# Query interactive → DataFrame
resp = agent.ask("Quel est le CA brut par région sur 2024 pour l'ERP ?")
print(resp.explanation)   # "J'ai sélectionné kpi_orders.erp, métriques: gross_revenue, group_by: region"
resp.result.show()        # DataFrame PySpark

# Question de suivi (historique actif)
resp2 = agent.ask("Et le nombre de commandes ?")
# LLM conserve region + filtre ERP, ajoute nb_orders

# Création de vue persistée
resp3 = agent.ask("Crée une vue Power BI du CA mensuel par catégorie")
print(resp3.result)       # "catalog.gold.v_ca_mensuel_categorie"

# Filtrage explicite par tags si ambiguïté
resp4 = agent.ask("CA par région", tags=["crm"])
```

---

## 11. Ordre de priorité v1 (final complet)

| Priorité | Composant | Fichier |
|---|---|---|
| 1 | `LLMProvider` ABC + providers + factory `.env`-aware | `llm_provider.py` |
| 2 | `NotebookExtractor` + `RuleInspector` | `extractor.py` |
| 3 | `GlossaryReader` (JSON/YAML/TXT/MD + PDF/PPTX optionnels) | `glossary.py` |
| 4 | `SemanticValidator` + `ValidationResult` | `validator.py` |
| 5 | `SemanticBuilder` multi-step + catalogue auto-mis à jour | `builder.py` |
| 6 | `SemanticEngine` lazy loading + catalogue + `create_view()` | `semantic.py` |
| 7 | `QueryResolver` — validation + résolution nom→SQL déterministe | `resolver.py` |
| 8 | `GenBIAgent` 2-step + SemanticQuery + AgentResponse + historique | `agent.py` |
| 9 | `SessionHistory` + `HistoryEntry` — log automatique par interaction | `history.py` |
| 10 | `HistoryExporter.to_pdf()` — rapport PDF (fpdf2 + matplotlib optionnel) | `exporter.py` |
