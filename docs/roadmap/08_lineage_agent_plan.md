# Plan — LineageAgent

## Context

Le BuilderAgent est mergé. L'étape suivante de l'agentic hub est d'implémenter les 3 agents spécialisés avant de créer le routeur central (AgenticHub). On commence par le plus simple : **LineageAgent**, un agent déterministe (pas de LLM requis au cœur) qui expose le module `lineage/` sous une API conversationnelle cohérente avec GenBIAgent.

Branche dédiée : `lineage_agent`

---

## Objectif

Créer `LineageAgent` — un agent qui répond à :
- *"D'où vient le champ amount_eur ?"* → traversée upstream (provenance)
- *"Quel est l'impact de order_id ?"* → traversée downstream (impact)
- *"Génère un diagramme de la table gold.fact_orders"* → rendu Mermaid/HTML
- *"Que signifie gross_revenue ?"* → lookup DataDictionary

---

## Architecture

### Nouveau dataclass : `LineageResponse` (dans `models.py`)

```python
@dataclass
class LineageResponse:
    question: str
    mode: str           # "trace" | "impact" | "render" | "lookup" | "error"
    table: str = ""
    column: str = ""
    edges: list = field(default_factory=list)      # list[LineageEdge]
    field_entry: Any = None                        # FieldEntry | None
    diagram: str = ""  # mermaid / json / html string
    narrative: str = ""  # explication LLM optionnelle
    error: str | None = None
    suggestions: list[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return self.error is None
```

### Classe `LineageAgent` (nouveau fichier `agentic/lineage_agent.py`)

```python
class LineageAgent:
    def __init__(
        self,
        schema_dict: dict | None = None,
        schema_type: str = "core",     # "core" | "semantic"
        target_name: str | None = None,
        llm_provider: LLMProvider | None = None,
        glossary_path: str | None = None,
        history: bool = False,
        session_title: str = "Lineage Analysis",
    ) -> None

    # API principale
    def ask(self, question: str) -> LineageResponse

    # API directe (pas de parsing NL)
    def trace(self, table: str, column: str, narrative: bool = False) -> LineageResponse
    def impact(self, table: str, column: str, narrative: bool = False) -> LineageResponse
    def render(self, format: str = "mermaid", direction: str = "LR") -> str
    def lookup(self, table: str, column: str) -> LineageResponse

    # Chargement dynamique du schéma
    def load_schema(
        self, schema_dict: dict,
        schema_type: str = "core",
        target_name: str | None = None,
    ) -> None

    @property
    def graph(self) -> LineageGraph
    @property
    def dictionary(self) -> DataDictionary
    @property
    def session(self) -> SessionHistory
    def reset_history(self) -> None
```

### Routing dans `ask()` — déterministe d'abord, LLM optionnel

**Sans LLM** : parsing par mots-clés sur la question normalisée (minuscule, sans accents) :

| Mots-clés détectés | Mode |
|---|---|
| "d'où", "vient", "provenance", "upstream", "source de", "where does" | `trace` |
| "impact", "downstream", "affecte", "utilisé par", "dépend" | `impact` |
| "visualis", "diagram", "mermaid", "html", "graph", "render" | `render` |
| "définit", "signifie", "meaning", "what is", "définition", "lookup" | `lookup` |

Extraction de `(table, column)` depuis la question : regex `schema.table.column` ou `table.column` ou `column`.

**Avec LLM** (si `llm_provider` fourni) : step A → JSON `{mode, table, column}` → dispatch.

### LLM narrative (optionnel)

Quand `narrative=True` dans `trace()`/`impact()` : prompt compact avec les edges trouvés → LLM génère une explication en prose (1-3 phrases). Désactivé par défaut pour garder l'agent léger.

---

## Fichiers à créer / modifier

| Action | Fichier |
|---|---|
| **Créer** | `src/skifer/agentic/lineage_agent.py` |
| **Modifier** | `src/skifer/agentic/models.py` — ajouter `LineageResponse` |
| **Modifier** | `src/skifer/agentic/__init__.py` — exporter `LineageAgent`, `LineageResponse` |
| **Créer** | `tests/test_lineage_agent.py` |
| **Modifier** | `CHANGELOG.md` — section `[Unreleased]` |
| **Créer** | `docs/roadmap/06_lineage_agent_plan.md` (copie de ce plan) |

---

## Phases d'implémentation

### Phase 1 — `LineageResponse` dans `models.py`
- Ajouter le dataclass après `BuilderResponse`
- Imports : `LineageEdge` en TYPE_CHECKING pour éviter les imports circulaires

### Phase 2 — `lineage_agent.py`
- Implémenter `LineageAgent.__init__` : build graph + dictionary si `schema_dict` fourni
- Implémenter `load_schema()` : `LineageTracker.from_schema()` ou `from_semantic_model()` selon `schema_type`
- Implémenter `trace()`, `impact()` : wrappent `graph.upstream()` / `graph.downstream()`
- Implémenter `render()` : délègue à `LineageRenderer`
- Implémenter `lookup()` : `dictionary.get(table, column)` + suggestions si non trouvé
- Implémenter `ask()` : keyword routing → dispatch + log `HistoryEntry`
- Implémenter `_step_llm_parse()` (optionnel, appelé si `llm_provider` et keyword non trouvé)
- Implémenter `_narrative()` : génère prose via LLM si demandé

### Phase 3 — Exports `agentic/__init__.py`
- Ajouter `LineageAgent`, `LineageResponse` dans `__all__`

### Phase 4 — Tests
Fichier : `tests/test_lineage_agent.py`

Tests (tous mockés, pas de Spark ni LLM) :
- `test_load_schema_core` : schema core → graph non vide
- `test_load_schema_semantic` : schema semantic → graph non vide
- `test_trace_returns_edges` : `trace("target", "col")` → edges upstream
- `test_impact_returns_edges` : `impact("source", "col")` → edges downstream
- `test_render_mermaid` : retourne string avec `graph LR`
- `test_lookup_found` : retourne `FieldEntry`
- `test_lookup_not_found` : `success=False`, suggestions dans `LineageResponse`
- `test_ask_routing_trace` : question "d'où vient amount_eur" → mode="trace"
- `test_ask_routing_impact` : question "impact de order_id" → mode="impact"
- `test_ask_routing_render` : question "génère un diagramme" → mode="render"
- `test_ask_no_schema` : `ask()` sans schema chargé → `error` non None
- `test_session_history` : `history=True` → `len(agent.session) == N` après N ask()
- `test_narrative_with_llm` : mock LLMProvider → narrative non vide dans réponse

### Phase 5 — CHANGELOG + commit

---

## Réutilisation des modules existants

| Module existant | Usage dans LineageAgent |
|---|---|
| `lineage/tracker.py` : `LineageTracker.from_schema()`, `from_semantic_model()` | Construction du graphe |
| `lineage/tracker.py` : `LineageGraph.upstream()`, `downstream()` | Traversée |
| `lineage/dictionary.py` : `DataDictionary(graph)`, `.get()`, `.enrich_from_glossary()` | Lookup champs |
| `lineage/renderer.py` : `LineageRenderer.to_mermaid()`, `.to_html()`, `.to_json()` | Rendu |
| `agentic/history.py` : `SessionHistory`, `HistoryEntry` | Log des interactions |
| `semantic/llm_provider.py` : `LLMProvider`, `get_llm_provider()` | LLM optionnel |
| `agentic/models.py` : `AgentResponse` pattern | Structure `LineageResponse` |

---

## Vérification

```bash
# Tests unitaires
pytest tests/test_lineage_agent.py -v --tb=short

# Suite complète (non-régression)
pytest tests/ -x --tb=short

# Lint
ruff check src/skifer/agentic/lineage_agent.py
```
