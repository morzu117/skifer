# Plan — QualityAgent

## Context

LineageAgent est mergé sur `lineage_agent`. Deuxième agent spécialisé de l'agentic hub :
**QualityAgent**, qui expose le module `observability/` sous la même API conversationnelle
(ask / mode direct / history / SessionHistory). Branche dédiée : `quality_agent`.

Différence clé avec LineageAgent : QualityAgent **nécessite un backend** pour exécuter
les checks SQL (pas d'analyse purement statique). Le backend est injecté au constructeur.

---

## Objectif

Créer `QualityAgent` — un agent qui répond à :
- *"La table gold.fact_orders est-elle saine ?"* → run checks → QualityResponse
- *"Montre l'historique de fact_orders"* → get_last_n → liste de MonitorReport
- *"Génère un rapport sur fact_orders"* → MonitorReporter.to_text() / to_html()

---

## Architecture

### Nouveau dataclass : `QualityResponse` (dans `models.py`)

```python
@dataclass
class QualityResponse:
    question: str
    mode: str = "check"       # "check" | "history" | "report" | "error"
    table: str = ""
    report: Any = None         # MonitorReport | None
    history_reports: list = field(default_factory=list)  # list[MonitorReport]
    text_output: str = ""      # résultat formatté (text / json / html)
    narrative: str = ""        # explication LLM optionnelle
    error: str | None = None

    @property
    def success(self) -> bool:
        return self.error is None

    @property
    def passed(self) -> bool | None:
        """None si aucun check, True si pas de critical failure."""
        if self.report is None:
            return None
        return not self.report.has_critical_failures()
```

### Classe `QualityAgent` (nouveau fichier `agentic/quality_agent.py`)

```python
class QualityAgent:
    def __init__(
        self,
        backend,                          # Backend protocol — requis pour SQL
        history_store=None,               # HistoryStore | None — persistance MonitorReport
        llm_provider=None,                # LLMProvider | None — narrative + NL parsing
        alert_config: dict | None = None, # config AlertDispatcher (webhook, slack, email)
        session_history: bool = False,    # log SessionHistory interactions
        session_title: str = "Quality Analysis",
    ) -> None

    # API directe
    def check(
        self,
        fqn: str,
        contracts: list | None = None,   # list[DataContract] ; None = extraire du schema
        schema_dict: dict | None = None, # schema source pour ContractExtractor
        raise_on_critical: bool = False,
        narrative: bool = False,
    ) -> QualityResponse

    def get_history(self, fqn: str, n: int = 5) -> QualityResponse

    def report(self, fqn: str, format: str = "text") -> str

    # API conversationnelle
    def ask(self, question: str) -> QualityResponse

    @property
    def session(self) -> SessionHistory | None
    def reset_history(self) -> None
```

### Routing dans `ask()` — même pattern que LineageAgent

**Mots-clés → mode :**

| Mots-clés | Mode |
|---|---|
| "sain", "healthy", "check", "vérifie", "verifie", "audit", "ok", "qualite", "analyse" | `check` |
| "historique", "history", "tendance", "trend", "evolution", "derniers", "precedents" | `history` |
| "rapport", "report", "résumé", "resume", "affiche", "montre", "summary" | `report` |

**Extraction du FQN** depuis la question : regex `\b[\w]+(?:\.[\w]+){1,2}\b` (dotted 2-3 parts),
sinon dernier mot snake_case non-stopword.

**Avec LLM** (si fourni) : fallback JSON `{mode, table}` quand keywords échouent.

### LLM narrative (optionnel)

Quand `narrative=True` dans `check()` : summary du MonitorReport → LLM génère prose 1-3 phrases.
Format du prompt : `"Summarize the quality check for table '{fqn}': {summary_dict}"`.

---

## Fichiers à créer / modifier

| Action | Fichier |
|---|---|
| **Créer** | `src/skifer/agentic/quality_agent.py` |
| **Modifier** | `src/skifer/agentic/models.py` — ajouter `QualityResponse` |
| **Modifier** | `src/skifer/agentic/__init__.py` — exporter `QualityAgent`, `QualityResponse` |
| **Créer** | `tests/test_quality_agent.py` |
| **Modifier** | `CHANGELOG.md` — section `[Unreleased]` |
| **Créer** | `docs/roadmap/09_quality_agent_plan.md` |

---

## Phases d'implémentation

### Phase 1 — `QualityResponse` dans `models.py`
- Ajouter après `LineageResponse`
- `passed` property : `None` si pas de report, `bool` sinon
- `MonitorReport` en `Any` (pas d'import cross-module)

### Phase 2 — `quality_agent.py`

- `__init__` : instancier `DataMonitor(backend, history_store)`, `ContractExtractor()`,
  `MonitorReporter()`, `AlertDispatcher()`, `SessionHistory` si demandé
- `check()` :
  1. Si `contracts` est None et `schema_dict` fourni → `ContractExtractor().extract(schema_dict)`
  2. `DataMonitor.check_table(fqn, contracts, raise_on_critical)` → `MonitorReport`
  3. Si `history_store` → `history_store.store(report)`
  4. Si `alert_config` → `AlertDispatcher().dispatch(report, alert_config)`
  5. Si `narrative` et `llm_provider` → `_narrative(report)`
  6. Retourner `QualityResponse(mode="check", report=report, ...)`
- `get_history()` :
  - Si pas de `history_store` → erreur explicite
  - `history_store.get_last_n(fqn, n)` → `QualityResponse(mode="history", history_reports=...)`
- `report()` :
  - Récupère dernier report (history_store ou run live si schema fourni)
  - `MonitorReporter().to_text()` / `to_json()` / `to_html()`
  - Retourne la string (pas de QualityResponse — cohérent avec `LineageAgent.render()`)
- `ask()` : keyword routing → dispatch + log SessionHistory

### Phase 3 — Exports `agentic/__init__.py`

### Phase 4 — Tests `tests/test_quality_agent.py`

FakeBackend inline (même pattern que `test_observability.py`) — pas de Spark.

Tests :
- `test_check_with_contracts` — contracts fournis → MonitorReport dans QualityResponse
- `test_check_from_schema` — schema_dict fourni → ContractExtractor déduit les checks
- `test_check_no_contracts_no_schema` — erreur explicite
- `test_check_passed_property` — rapport sans failure → `passed=True`
- `test_check_failed_property` — rapport avec critical → `passed=False`
- `test_get_history_no_store` — erreur si pas de history_store
- `test_get_history_with_store` — SqliteHistoryStore mock → liste retournée
- `test_report_text` — `report(fqn, format="text")` → string non vide
- `test_report_json` — `report(fqn, format="json")` → JSON valide
- `test_ask_routing_check` — "La table est-elle saine ?" → mode="check"
- `test_ask_routing_history` — "Montre l'historique" → mode="history"
- `test_ask_routing_report` — "Génère un rapport" → mode="report"
- `test_ask_no_table` — question sans FQN → erreur
- `test_ask_unknown_intent_no_llm` — erreur avec message d'aide
- `test_session_history_disabled` — session=None par défaut
- `test_session_history_enabled` — log après ask()
- `test_narrative_with_llm` — mock LLM → narrative non vide
- `test_alert_dispatch_called` — mock AlertDispatcher → dispatch appelé si alert_config

### Phase 5 — CHANGELOG + roadmap doc + commit

---

## Réutilisation des modules existants

| Module existant | Usage dans QualityAgent |
|---|---|
| `observability/monitor.py` : `DataMonitor`, `MonitorReport` | Exécution des checks |
| `observability/contracts.py` : `ContractExtractor` | Extraction depuis schema YAML |
| `observability/history.py` : `HistoryStore`, `SqliteHistoryStore` | Persistance / tendances |
| `observability/reporter.py` : `MonitorReporter` | Formatage text/json/html |
| `observability/alerts.py` : `AlertDispatcher` | Notifications webhook/slack/email |
| `agentic/history.py` : `SessionHistory`, `HistoryEntry` | Log interactions agent |
| `semantic/llm_provider.py` : `LLMProvider` | LLM optionnel |
| `agentic/models.py` : `LineageResponse` pattern | Structure `QualityResponse` |

---

## Vérification

```bash
pytest tests/test_quality_agent.py -v --tb=short
pytest tests/ -x --tb=short
ruff check src/skifer/agentic/quality_agent.py
```
