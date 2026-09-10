# Plan — DictionaryAgent

## Context

QualityAgent est mergé sur `quality_agent`. Troisième et dernier agent spécialisé :
**DictionaryAgent**, qui expose `DataDictionary` + `GlossaryReader` sous la même API
conversationnelle. Il est complémentaire de `LineageAgent` : là où LineageAgent traverse le
graphe (edges upstream/downstream), DictionaryAgent expose les métadonnées des champs
(définitions, sources, transformations) et leur enrichissement par glossaire.

Branche dédiée : `dictionary_agent`

---

## Objectif

Créer `DictionaryAgent` — un agent qui répond à :
- *"Que signifie amount_eur ?"* → lookup FieldEntry + narrative LLM optionnelle
- *"Liste tous les champs de gold.fact_orders"* → list_fields() → list[FieldEntry]
- *"Exporte le dictionnaire en JSON"* → DataDictionary.to_dict()

---

## Architecture

### Nouveau dataclass : `DictionaryResponse` (dans `models.py`)

```python
@dataclass
class DictionaryResponse:
    question: str
    mode: str = "lookup"       # "lookup" | "list" | "export" | "error"
    table: str = ""
    column: str = ""
    entry: Any = None           # FieldEntry | None (mode lookup)
    entries: list = field(default_factory=list)  # list[FieldEntry] (mode list)
    text_output: str = ""       # export formatté
    narrative: str = ""         # explication LLM optionnelle
    error: str | None = None
    suggestions: list[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return self.error is None
```

### Classe `DictionaryAgent` (nouveau fichier `agentic/dictionary_agent.py`)

```python
class DictionaryAgent:
    def __init__(
        self,
        schema_dict: dict | None = None,
        schema_type: str = "core",          # "core" | "semantic"
        target_name: str | None = None,
        glossary_path: str | None = None,
        llm_provider: LLMProvider | None = None,
        session_history: bool = False,
        session_title: str = "Dictionary",
    ) -> None

    # Chargement / enrichissement
    def load_schema(schema_dict, schema_type, target_name) -> None
    def enrich(glossary_path: str) -> None

    # API directe
    def lookup(table: str, column: str, narrative: bool = False) -> DictionaryResponse
    def list_fields(table: str | None = None) -> DictionaryResponse
    def export(format: str = "text") -> str  # "text" | "json"

    # API conversationnelle
    def ask(question: str) -> DictionaryResponse

    @property
    def dictionary -> DataDictionary | None
    @property
    def session -> SessionHistory | None
    def reset_history() -> None
```

### Routing dans `ask()` — même pattern que LineageAgent / QualityAgent

| Mots-clés | Mode |
|---|---|
| "signifie", "definition", "what is", "what does", "que veut dire", "explique", "trouve", "cherche", "find", "lookup", "get" | `lookup` |
| "liste", "list", "show", "montre", "all", "tous", "toutes", "champs", "colonnes", "fields", "columns" | `list` |
| "export", "json", "rapport", "report", "dictionnaire", "dictionary", "genere", "generate" | `export` |

Extraction de `(table, column)` : même logique que `LineageAgent._extract_field()`.
Extraction de `table` seule (mode list) : même que `QualityAgent._extract_fqn()`.

**Avec LLM** (si fourni) : fallback JSON `{mode, table, column}` quand keywords échouent.

### Narrative LLM (optionnel)

`narrative=True` dans `lookup()` : prompt compact sur le FieldEntry → LLM génère 1-2 phrases.
Format : `"Explain in 1-2 sentences what column '{column}' means: {entry_dict}"`.

---

## Fichiers à créer / modifier

| Action | Fichier |
|---|---|
| **Créer** | `src/skifer/agentic/dictionary_agent.py` |
| **Modifier** | `src/skifer/agentic/models.py` — ajouter `DictionaryResponse` |
| **Modifier** | `src/skifer/agentic/__init__.py` — exporter `DictionaryAgent`, `DictionaryResponse` |
| **Créer** | `tests/test_dictionary_agent.py` |
| **Modifier** | `CHANGELOG.md` — section `[Unreleased]` |
| **Créer** | `docs/roadmap/10_dictionary_agent_plan.md` |

---

## Phases d'implémentation

### Phase 1 — `DictionaryResponse` dans `models.py`
Ajouter après `QualityResponse`.

### Phase 2 — `dictionary_agent.py`
- `__init__` : `load_schema()` si `schema_dict` fourni, `enrich()` si `glossary_path` fourni
- `load_schema()` : `LineageTracker.from_schema()` ou `from_semantic_model()` → `DataDictionary(graph)`
- `enrich()` : `dictionary.enrich_from_glossary(path)`
- `lookup()` :
  - `dictionary.get(table, column)` ; si table vide → cherche sur toutes les tables
  - Si non trouvé : suggestions `difflib.get_close_matches` sur tous les noms de colonnes
  - Si `narrative` et `llm_provider` : `_narrative(entry)`
- `list_fields()` : `dictionary.list_fields(table)` → liste triée
- `export()` : `dictionary.to_dict()` (json) ou `_format_text()` (text)
- `ask()` : keyword routing → dispatch + log SessionHistory
- `_step_llm_parse()` : fallback LLM si aucun keyword détecté

### Phase 3 — Exports `agentic/__init__.py`

### Phase 4 — Tests `tests/test_dictionary_agent.py`

Tous mockés, pas de Spark ni LLM.

- `test_load_schema_core` — schema core → dictionary non None
- `test_load_schema_semantic` — schema semantic → dictionary non None
- `test_load_schema_replaces_previous` — double load → nouveau dictionnaire
- `test_lookup_found` — FieldEntry retournée
- `test_lookup_no_table` — cherche sur toutes les tables
- `test_lookup_not_found_suggestions` — suggestions Levenshtein
- `test_lookup_no_schema` — erreur explicite
- `test_list_all_fields` — liste complète sans filtre table
- `test_list_fields_by_table` — filtre par table
- `test_list_no_schema` — erreur explicite
- `test_export_json` — JSON valide
- `test_export_text` — string non vide
- `test_export_no_schema` — string vide
- `test_enrich_updates_descriptions` — descriptions remplies après enrich
- `test_ask_routing_lookup` — "Que signifie amount_eur ?" → mode="lookup"
- `test_ask_routing_list` — "Liste les champs de fact_orders" → mode="list"
- `test_ask_routing_export` — "Exporte le dictionnaire" → mode="export"
- `test_ask_no_schema` — erreur explicite
- `test_ask_unknown_intent_no_llm` — erreur avec message d'aide
- `test_session_history_disabled` — session=None par défaut
- `test_session_history_enabled` — log après ask()
- `test_reset_history` — session vidée
- `test_narrative_with_llm` — mock LLM → narrative non vide
- `test_llm_fallback_called` — question ambiguë → LLM appelé
- `test_llm_parse_failure_returns_error`

### Phase 5 — CHANGELOG + roadmap doc + commit

---

## Réutilisation des modules existants

| Module existant | Usage dans DictionaryAgent |
|---|---|
| `lineage/tracker.py` : `LineageTracker.from_schema()`, `from_semantic_model()` | Construction du graphe |
| `lineage/dictionary.py` : `DataDictionary(graph)`, `.get()`, `.list_fields()`, `.enrich_from_glossary()`, `.to_dict()` | Cœur du dictionnaire |
| `semantic/glossary.py` : `GlossaryReader().read(path)` | Enrichissement glossaire |
| `agentic/history.py` : `SessionHistory`, `HistoryEntry` | Log des interactions |
| `semantic/llm_provider.py` : `LLMProvider` | LLM optionnel |
| `agentic/lineage_agent.py` : `_extract_field()` pattern | Extraction (table, column) |
| `agentic/quality_agent.py` : `_extract_fqn()` pattern | Extraction table |

---

## Vérification

```bash
pytest tests/test_dictionary_agent.py -v --tb=short
pytest tests/ -x --tb=short
ruff check src/skifer/agentic/dictionary_agent.py
```
