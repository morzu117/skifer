# Plan 31 — Feature 1 : Service applicatif transport-neutre `services/`

**But** : extraire du cœur une couche `services/` **transport-neutre, JSON-native, fail-closed** (contexte de requête, scopes nommés, sérialisation, erreurs) puis y poser des services applicatifs (`ProjectService`, `RuleService`, `GovernanceService`, `QualityService`, `SemanticService`, `AgentService`, `LocalIdentity`) que MCP — et une future `api/` — consomment **sans jamais toucher `SkiferEngine`, `SemanticEngine` ni un store directement**, aucun objet Spark ne franchissant la frontière.

Les arbitrages §7 (décidés le 2026-09-11) sont **appliqués** dans ce document :
- **§7.1** : l'API vit dans **ce dépôt** derrière l'extra `[api]` (hors périmètre de cette feature ; seul l'`architecture test` la nomme comme importeur interdit futur).
- **§7.4** : `LocalIdentity` reçoit **tous** les scopes nommés **sauf** `certification_override`.

**Discipline de commit** : *une slice = un commit* `feat(plan31-1.M): …` + tests + une entrée `CHANGELOG.md` sous `## [Unreleased]`. **Ne jamais bumper la version** de `pyproject.toml`.

**Dépendance Phase-0 déjà close** : le dépôt est sur un unique commit de release publique (`main`). `src/skifer/mcp/tools.py` **et** `tests/test_mcp_tools.py` existent et sont commités — la note « committer la slice 7.3 avant 31.1.1 » est **déjà satisfaite**, aucune action préalable requise.

---

## État actuel du code

Chemins absolus sous `src/skifer/`. Signatures citées telles qu'elles existent aujourd'hui.

### `agentic/data_service.py` (~736 lignes) — **SOURCE d'extraction de la slice 1.1**
Contient, dans l'ordre :

- Constantes de plafonds durs (lignes 24-31) :
  ```python
  HARD_MAX_PAGE_SIZE = 100
  HARD_MAX_QUERY_ROWS = 1_000
  HARD_MAX_FILTERS = 50
  HARD_MAX_FILTER_VALUE_LENGTH = 1_024
  _CONSUMER_SCOPE_ALLOWLIST: frozenset[str] = frozenset()   # vide → certification_override ne franchit jamais MCP
  ```
- Hiérarchie d'erreurs (lignes 38-67) :
  ```python
  class AgentReadyDataError(Exception): ...
  class ScopeDenied(AgentReadyDataError): ...
  class InvalidRequest(AgentReadyDataError): ...
  class LimitExceeded(InvalidRequest): ...
  class InvalidCursor(InvalidRequest): ...
  class ResourceNotFound(AgentReadyDataError): ...
  class ResourceUnavailable(AgentReadyDataError): ...
  class SerializationError(AgentReadyDataError): ...
  ```
- `@dataclass(frozen=True) class RequestContext` (lignes 70-87) :
  ```python
  subject: str
  scopes: frozenset[str]
  consumer_class: str
  trace_context: TraceContext            # from skifer.observability.tracing
  def __post_init__(self) -> None: ...   # rejette subject/scopes/consumer_class/trace_context malformés
  ```
- `def require_scope(ctx: RequestContext, scope: str) -> None` (lignes 90-93) : `if not isinstance(ctx, RequestContext) or scope not in ctx.scopes: raise ScopeDenied(...)`.
- `@dataclass(frozen=True) class ServiceLimits` (lignes 96-118) : champs `max_page_size / max_query_rows / max_filters / max_filter_value_length`, `__post_init__` valide `1..HARD_MAX_*` sinon `LimitExceeded`.
- **View DTOs** (dataclasses `frozen=True` avec `to_dict()` allowlisté) : `ModelSummary`, `GovernedModelView`, `ContractFieldView`, `ContractSemanticView`, `ContractView`, `CertificationView`, `LineageEdgeView`, `LineageView`, `Page[T]` (générique), `QueryEnvelope`.
- `class AgentReadyDataService` (ligne 314) : `__init__(self, semantic_engine, *, lineage_graph=None, limits=None)`. Méthodes gated par `require_scope` : `list_models(ctx, cursor, limit)` (`models:read`), `get_model(ctx, key)` (`models:read`), `get_contract(ctx, id, version)` (`contracts:read`), `get_certification(ctx, dataset)` (`contracts:read`), `get_lineage(ctx, dataset, column)` (`lineage:read`), `query(ctx, query, limit)` (`query:execute`).
- **Sérialisation JSON-native** (méthodes privées, lignes 660-698) : `_row_to_json(self, row, index)` et `_json_value(self, value, field_name)` — c'est le code à extraire en fonctions de module dans `services/serialization.py`. `_json_value` accepte `None/str/bool/int`, `float` fini (sinon `SerializationError`), `Decimal→str`, `datetime/date→isoformat()`, `list/tuple`, `dict` à clés `str` ; tout autre type → `SerializationError`.

### Importeurs actuels de `skifer.agentic.data_service` (à repointer en slice 1.1)
Tous dans `mcp/` :
- `mcp/config.py` : `LimitExceeded, ServiceLimits`
- `mcp/server.py` : `AgentReadyDataService, RequestContext`
- `mcp/auth.py` : `RequestContext`
- `mcp/runtime.py` : `AgentReadyDataService`
- `mcp/capability_tools.py` : `RequestContext, ScopeDenied, require_scope`
- `mcp/resources.py` : `AgentReadyDataService, InvalidCursor, InvalidRequest, LimitExceeded, RequestContext, ResourceNotFound, ResourceUnavailable, ScopeDenied`
- `mcp/tools.py` : `AgentReadyDataService, HARD_MAX_FILTERS, HARD_MAX_FILTER_VALUE_LENGTH, ServiceLimits, InvalidRequest, LimitExceeded, QueryEnvelope, RequestContext, ResourceNotFound, ResourceUnavailable, ScopeDenied, SerializationError`

Tests important le symbole depuis `agentic.data_service` : `tests/test_agent_ready_data_service.py`, `tests/test_mcp_resources.py`, `tests/test_mcp_cli.py`, `tests/test_mcp_capabilities.py`, `tests/test_mcp_tools.py`, `tests/test_capability_harness.py`. **Ces imports doivent continuer à fonctionner** (re-exports).

### `core/schema_loader.py`
- `parse_schema(yaml_str, params=None, *, base_dir=None, _seen_paths=None) -> dict` (ligne 1395) et `load_schema(path, params=None, *, _seen_paths=None) -> dict` (ligne 1428). **Toutes** les erreurs de validation sont des `ValueError` dont le message est un texte multi-lignes agrégé (`"Schema validation failed with N error(s):\n  [location] …"`). Il n'existe **aucune** erreur structurée `{code, message, path}` aujourd'hui — la slice 1.2 l'ajoute.
- Helpers de validation existants qui produisent ces messages : `_validate_schema_ops`, `_validate_streaming`, `_validate_materialized_view`, `_normalize_agent_ready_metadata`, plus la validation des références de join dans `_normalize_schema` (lignes 1316-1329).

### `core/core.py`
- `SkiferEngine.explain_rules(self, schema_dict, shared_read_threshold: int = 2)` (ligne 1253) : construit `RuleAnalyzer`, `analyze_rules`, `detect_warnings`, **appelle `analyzer.print_report(...)` (effet de bord stdout)** puis `return profiles, warnings`. La slice 1.2 doit exposer une **structure** (dict) sans imposer le print.

### `core/rule_analyzer.py`
- `@dataclass class RuleProfile` (ligne 78) : `name, output_columns, input_columns, raw_expressions, source_available, has_python_udf, loc, withcolumn_count`.
- `@dataclass class RuleWarning` (ligne 92) : `level, code, message, rules, column`.
- `class RuleAnalyzer` : `analyze_rule(func, name=None)`, `analyze_rules(rule_names)`, `build_dependency_graph(profiles) -> dict[str, list[str]]`, `detect_warnings(profiles, shared_read_threshold=2, loc_threshold=30, withcolumn_threshold=5)`, `print_report(profiles, warnings)`.

### `core/op_catalog.py`
- Dicts source-de-vérité : `FILTER_OPERATORS: dict[str, OperatorSpec]`, `COLUMN_OPS: dict[str, OpSpec]`, `AGGREGATE_FUNCTIONS: dict[str, AggregateSpec]`. Résolveurs : `resolve_filter_operator(name)`, `resolve_column_op(name)`, `resolve_aggregate_function(name)`, `suggest(name, catalog)`.

### `agentic/builder_agent.py`
- `class BuilderAgent` : `wizard(self, output_dir="schemas") -> str` (ligne 175) délègue à `_wizard_tables/_wizard_filters/_wizard_joins/_wizard_rules/_wizard_select/_wizard_options`, chacun appelant **`input(...)` directement** (lignes 336, 351, 353, 366, 391-397, 419, 430-445, 453-456) et un `input("Sauvegarder ?…")` final (ligne 233). `ask(self, description, output_dir="schemas") -> BuilderResponse` (ligne 242) = chemin LLM. La slice 1.5 rend `wizard` pilotable par un dict au lieu de stdin.

### `agentic/hub.py`
- `AgenticHub.__init__(self, semantic_engine=None, llm_provider=None, lineage_agent=None, quality_agent=None, dictionary_agent=None, builder_agent=None, genbi_agent=None, session_title="SkiferHub", profile=None, capability_invoker=None)` (ligne 120).
- `ask(self, question: str, **kwargs) -> HubResponse` (ligne 157) : lit `kwargs["consumer_context"]`, ouvre des spans, délègue à `_ask`.

### `serving/_response_serializer.py`
- `hub_response_to_text(response: Any) -> str` (ligne 32).

### `semantic/sync.py`
- `@dataclass(frozen=True) class SyncReport` (ligne 57) : `changes, conflicts, suggestions, payload, wrote, output_path` + propriétés `safe_to_apply`, `has_changes`.
- `class SemanticSynchronizer.__init__(self, output_dir="semantic_models", draft_builder=None)` ; `sync(self, projected: ProjectedSchema, schema: ParsedSchema, *, write=False) -> SyncReport` (ligne 88).

### `semantic/validator.py`
- `class SemanticValidator` : `validate(self, model: dict) -> ValidationResult` (ligne 85) ; `validate_against_projection(self, yaml_content: dict, projected: ProjectedSchema, *, contract_output: list[str] | None = None) -> ValidationResult` (ligne 167).

### `semantic/semantic.py`
- `SemanticEngine.__init__(self, …, certification_store=None, …)` (ligne 76). `list_models(...)`, `get_model_summary(model_key) -> dict`, attribut `certification_store`. **La façade SemanticService enveloppe cet objet ; les services ne le sous-classent pas.**

### `semantic/persistence.py`
- `write_yaml_atomic(path: str | Path, payload: dict) -> str` (ligne 21) — écriture atomique déjà disponible, réutilisée par `write_pipeline`/`write_rule`.
- `build_catalog_entry(...)` (ligne 55).

### `observability/certification_store.py`
- `Protocol CertificationStore` : `get_contract(contract_id, version) -> ContractDefinition | None`, `get_certification(dataset, consumer_class="default") -> Certification`, `list_history(dataset, limit=50) -> list[RunEvent]`. Implémentations `SqliteCertificationStore`, `DeltaCertificationStore`. Dataclasses `RunEvent`, `StoredCheckResult`, `Certification`.

### `observability/history.py`
- `Protocol HistoryStore` : `store(report)`, `get_last_n(table, n) -> list[MonitorReport]`, `get_latest(table) -> MonitorReport | None`. Implémentations `SqliteHistoryStore(db_path=".skifer_observability.db")`, `DeltaHistoryStore(backend, table_fqn)`.

### `observability/contracts.py`
- `class ContractExtractor.extract(self, schema_dict: dict) -> list[DataContract]` (ligne 38) — dérive les checks depuis `quality_checks` d'un schéma. Base de `QualityService` (slice 1.4).

### `core/environment.py`
- `get_clean_username(spark=None, config=None, dbutils=None, workspace_client_fn=None, find_file_fn=None) -> str` (ligne 143) ; `is_running_as_job(...)` (ligne 104).

### CLI (`cli.py`)
- `argparse` avec `subparsers = parser.add_subparsers(dest="command")` ; sous-commandes `validate`, `hub`, `semantic {sync,validate}`, `mcp serve`, `adaptive {list,show,diff,accept,reject,evaluate}`. Codes de sortie sémantiques déjà définis : `SEMANTIC_EXIT_OK=0 / ERROR=1 / DRIFT=2 / CONFLICT=3`.

### Convention de tests
Un fichier par module `tests/test_<module>.py` ; LLM toujours mocké ; fixture `spark` (`tests/conftest.py`) requise **uniquement** pour les tests qui construisent un `F.col(...)` ou exécutent Spark. Les tests des services doivent rester **Spark-free** (aucun DataFrame ne franchit `services/`).

---

## Slice 31.1.1 — Package `services/` : contexte, scopes nommés, sérialisation, erreurs (extraction)

**Objectif** : créer le package `services/` et y déplacer `RequestContext`, `require_scope`, `ServiceLimits`, la hiérarchie d'erreurs, les plafonds durs et la sérialisation JSON-native ; ajouter les scopes nommés ; garder `agentic/data_service.py` fonctionnel par re-export ; repointer `mcp/` vers `services/`.

### Fichiers
- `src/skifer/services/__init__.py` — **création** : re-exporte la surface publique complète (voir « Signatures »).
- `src/skifer/services/context.py` — **création** : erreurs, `RequestContext`, `require_scope`, `ServiceLimits`, `HARD_MAX_*`, `_CONSUMER_SCOPE_ALLOWLIST`, **scopes nommés**.
- `src/skifer/services/serialization.py` — **création** : `to_json_value`, `row_to_json` (fonctions de module extraites de `_json_value`/`_row_to_json`).
- `src/skifer/agentic/data_service.py` — **modification** : supprime les définitions déplacées ; `from skifer.services.context import *` (imports explicites) + `from skifer.services.serialization import row_to_json`; `AgentReadyDataService._row_to_json`/`_json_value` délèguent aux fonctions de module ; conserve `AgentReadyDataService` et les View DTOs ; **re-exporte** tous les symboles historiques + `__all__`.
- `src/skifer/mcp/{config,server,auth,runtime,capability_tools,resources,tools}.py` — **modification** : `from skifer.agentic.data_service import …` → `from skifer.services import …` (mêmes noms). `AgentReadyDataService` et `QueryEnvelope` restent importables via `skifer.services` (re-export).

### Signatures Python
```python
# services/context.py
from __future__ import annotations
from dataclasses import dataclass
from skifer.observability.tracing import TraceContext

HARD_MAX_PAGE_SIZE = 100
HARD_MAX_QUERY_ROWS = 1_000
HARD_MAX_FILTERS = 50
HARD_MAX_FILTER_VALUE_LENGTH = 1_024
_CONSUMER_SCOPE_ALLOWLIST: frozenset[str] = frozenset()

# --- Scopes nommés (arbitrage §7.4) ---
CERTIFICATION_OVERRIDE_SCOPE = "certification_override"   # JAMAIS accordé localement / via MCP
# scopes lecture historiques (déjà en service dans AgentReadyDataService) :
SCOPE_MODELS_READ = "models:read"
SCOPE_CONTRACTS_READ = "contracts:read"
SCOPE_LINEAGE_READ = "lineage:read"
SCOPE_QUERY_EXECUTE = "query:execute"
# scopes nouveaux pour services/ :
SCOPE_PROJECT_READ = "project:read"
SCOPE_PIPELINES_WRITE = "pipelines:write"
SCOPE_RULES_WRITE = "rules:write"
SCOPE_EXECUTE_RUN = "execute:run"
SCOPE_CONTRACTS_WRITE = "contracts:write"
SCOPE_INCIDENTS_WRITE = "incidents:write"

NAMED_SCOPES: frozenset[str] = frozenset({
    SCOPE_MODELS_READ, SCOPE_CONTRACTS_READ, SCOPE_LINEAGE_READ, SCOPE_QUERY_EXECUTE,
    SCOPE_PROJECT_READ, SCOPE_PIPELINES_WRITE, SCOPE_RULES_WRITE, SCOPE_EXECUTE_RUN,
    SCOPE_CONTRACTS_WRITE, SCOPE_INCIDENTS_WRITE,
})   # NB : certification_override est délibérément ABSENT de NAMED_SCOPES.

class AgentReadyDataError(Exception): ...
class ScopeDenied(AgentReadyDataError): ...
class InvalidRequest(AgentReadyDataError): ...
class LimitExceeded(InvalidRequest): ...
class InvalidCursor(InvalidRequest): ...
class ResourceNotFound(AgentReadyDataError): ...
class ResourceUnavailable(AgentReadyDataError): ...
class SerializationError(AgentReadyDataError): ...

@dataclass(frozen=True)
class RequestContext:                 # identique à l'actuel, __post_init__ inchangé
    subject: str
    scopes: frozenset[str]
    consumer_class: str
    trace_context: TraceContext
    def __post_init__(self) -> None: ...

def require_scope(ctx: RequestContext, scope: str) -> None: ...

@dataclass(frozen=True)
class ServiceLimits:                  # identique à l'actuel, __post_init__ inchangé
    max_page_size: int = HARD_MAX_PAGE_SIZE
    max_query_rows: int = HARD_MAX_QUERY_ROWS
    max_filters: int = HARD_MAX_FILTERS
    max_filter_value_length: int = HARD_MAX_FILTER_VALUE_LENGTH
    def __post_init__(self) -> None: ...
```
```python
# services/serialization.py
from __future__ import annotations
from typing import Any
from skifer.services.context import SerializationError

def to_json_value(value: Any, field_name: str) -> Any:
    """Corps identique à AgentReadyDataService._json_value (lignes 675-698)."""

def row_to_json(row: Any, index: int) -> dict[str, Any]:
    """Corps identique à AgentReadyDataService._row_to_json (lignes 660-673),
    appelant to_json_value au lieu de self._json_value."""
```
```python
# services/__init__.py  — re-export de LA surface publique consommée par mcp/ et api/
from skifer.services.context import (
    HARD_MAX_PAGE_SIZE, HARD_MAX_QUERY_ROWS, HARD_MAX_FILTERS,
    HARD_MAX_FILTER_VALUE_LENGTH, _CONSUMER_SCOPE_ALLOWLIST,
    NAMED_SCOPES, CERTIFICATION_OVERRIDE_SCOPE,
    SCOPE_MODELS_READ, SCOPE_CONTRACTS_READ, SCOPE_LINEAGE_READ, SCOPE_QUERY_EXECUTE,
    SCOPE_PROJECT_READ, SCOPE_PIPELINES_WRITE, SCOPE_RULES_WRITE, SCOPE_EXECUTE_RUN,
    SCOPE_CONTRACTS_WRITE, SCOPE_INCIDENTS_WRITE,
    AgentReadyDataError, ScopeDenied, InvalidRequest, LimitExceeded, InvalidCursor,
    ResourceNotFound, ResourceUnavailable, SerializationError,
    RequestContext, require_scope, ServiceLimits,
)
from skifer.services.serialization import to_json_value, row_to_json
# Re-export du service historique + View DTOs (restent définis dans agentic/data_service.py).
from skifer.agentic.data_service import (
    AgentReadyDataService, ModelSummary, GovernedModelView, ContractFieldView,
    ContractSemanticView, ContractView, CertificationView, LineageEdgeView,
    LineageView, Page, QueryEnvelope,
)
__all__ = [ ... tous les noms ci-dessus ... ]
```
```python
# agentic/data_service.py  — en tête, après le module docstring
from skifer.services.context import (          # re-export backward-compat
    HARD_MAX_PAGE_SIZE, HARD_MAX_QUERY_ROWS, HARD_MAX_FILTERS, HARD_MAX_FILTER_VALUE_LENGTH,
    _CONSUMER_SCOPE_ALLOWLIST,
    AgentReadyDataError, ScopeDenied, InvalidRequest, LimitExceeded, InvalidCursor,
    ResourceNotFound, ResourceUnavailable, SerializationError,
    RequestContext, require_scope, ServiceLimits,
)
from skifer.services.serialization import row_to_json
# … View DTOs et AgentReadyDataService restent définis ici …
# _row_to_json devient :
    def _row_to_json(self, row, index):
        return row_to_json(row, index)
# _json_value peut être supprimée (plus référencée) — row_to_json appelle to_json_value.
```

### Comportement & règles
- **Backward compat stricte** : tout `from skifer.agentic.data_service import X` déjà utilisé continue de résoudre (les noms sont ré-exportés). Ne casser aucun test existant.
- **Import direct de sous-module** dans `agentic/data_service.py` (`from skifer.services.context import …`, pas `from skifer.services import …`) pour éviter le cycle `services/__init__ → agentic.data_service → services/__init__`. `services/__init__.py` importe `agentic.data_service` en dernier (après context/serialization).
- **`mcp/` n'importe plus `agentic.data_service`** : tous les imports passent par `skifer.services`. C'est la précondition de l'`architecture test`.
- Scopes nommés : `NAMED_SCOPES` **n'inclut jamais** `certification_override`. `_CONSUMER_SCOPE_ALLOWLIST` reste `frozenset()` (le scope d'override ne franchit pas la frontière MCP).
- **Fail-closed inchangé** : `require_scope` rejette un `ctx` non-`RequestContext` ou un scope absent ; `ServiceLimits.__post_init__` rejette hors `1..HARD_MAX_*`.

### Cas de test — `tests/test_services_context.py`, `tests/test_services_serialization.py` (Spark-free)
- `test_named_scopes_exclude_certification_override` : `CERTIFICATION_OVERRIDE_SCOPE not in NAMED_SCOPES`.
- `test_named_scopes_exact_set` : `NAMED_SCOPES == frozenset({"models:read","contracts:read","lineage:read","query:execute","project:read","pipelines:write","rules:write","execute:run","contracts:write","incidents:write"})` (assertion d'égalité exacte).
- `test_require_scope_denies_missing` : `require_scope(ctx_sans_scope, "models:read")` lève `ScopeDenied`.
- `test_require_scope_denies_non_context` : `require_scope(object(), "x")` lève `ScopeDenied`.
- `test_service_limits_rejects_out_of_range` : `ServiceLimits(max_query_rows=10_000)` lève `LimitExceeded`.
- `test_to_json_value_finite_float_ok_and_nan_rejected` : `to_json_value(1.5, "f") == 1.5` ; `to_json_value(float("nan"), "f")` lève `SerializationError`.
- `test_to_json_value_decimal_and_datetime` : `Decimal("1.20") → "1.20"` ; `date(2026,9,11) → "2026-09-11"`.
- `test_row_to_json_requires_string_keys` : un `row` mapping à clé non-`str` lève `SerializationError`.
- `test_row_to_json_uses_asdict_recursive` : objet exposant `asDict(recursive=True)` sérialisé correctement.

### Cas de test — backward compat & architecture
- `tests/test_agent_ready_data_service.py` (existant) : **inchangé et vert** — prouve que les imports historiques marchent (`RequestContext`, `require_scope`, `ServiceLimits`, erreurs, `AgentReadyDataService`, `QueryEnvelope`).
- `tests/test_services_architecture.py` — **création** (Spark-free) :
  - `test_mcp_does_not_import_agentic_data_service` : pour chaque fichier de `src/skifer/mcp/`, lire la source et asserter que `"agentic.data_service"` n'y apparaît pas.
  - `test_services_reexport_matches_data_service_identity` : `skifer.services.RequestContext is skifer.agentic.data_service.RequestContext` (même objet, pas une copie divergente) ; idem `ServiceLimits`, `ScopeDenied`.

### Commit
```
feat(plan31-1.1): extract transport-neutral services package (context, scopes, serialization)
```
CHANGELOG `[Unreleased]` :
```
- **Plan 31 (1.1)** — Nouveau package `services/` transport-neutre : `RequestContext`, `require_scope`, `ServiceLimits`, hiérarchie d'erreurs et sérialisation JSON-native extraites d'`agentic/data_service.py` (re-exports rétrocompatibles). Scopes nommés (`NAMED_SCOPES`, sans `certification_override`). `mcp/` n'importe plus `agentic.data_service`.
```

### DoD
- [ ] `services/{__init__,context,serialization}.py` créés ; extraction sans duplication de logique.
- [ ] `agentic/data_service.py` re-exporte tous les symboles historiques ; suite existante verte.
- [ ] 7 modules `mcp/` repointés vers `skifer.services`.
- [ ] `test_services_architecture.py` vert (aucun import `agentic.data_service` dans `mcp/`).
- [ ] `ruff check src/` propre ; CHANGELOG mis à jour.

---

## Slice 31.1.2 — `ProjectService` : ouverture, inspection, écriture atomique (sans Spark)

**Objectif** : exposer un service qui ouvre un projet Skifer, inspecte pipelines/règles/modèles/contrats/calendriers, renvoie des **erreurs localisées `{code, message, path}`**, transforme le rapport imprimé de `explain_rules` en **structure**, et écrit un pipeline **atomiquement** sous scope `pipelines:write`. **Aucun Spark.**

### Fichiers
- `src/skifer/services/project.py` — **création** : `ProjectService`, `ProjectView`, `PipelineView`, `LocalizedError`.
- `src/skifer/core/schema_loader.py` — **modification** : ajouter une variante de validation qui **collecte des erreurs structurées** sans changer le contrat public. Nouvelle fonction `parse_schema_localized(yaml_str, params=None, *, base_dir=None) -> tuple[dict | None, list[LocalizedIssue]]` où `LocalizedIssue = dataclass(code: str, message: str, path: str)`. `parse_schema` reste inchangée (lève toujours `ValueError`) ; la variante réutilise les helpers `_validate_*` en leur passant une liste et en dérivant `code`/`path` depuis le préfixe `[location]` déjà présent dans les messages.
- `src/skifer/core/core.py` — **modification** : ajouter `SkiferEngine.explain_rules_report(self, schema_dict, shared_read_threshold=2) -> dict` qui retourne la structure **sans imprimer** ; `explain_rules` existante inchangée (garde print + retour tuple, rétrocompat des exemples/tests).

### Signatures Python
```python
# services/project.py
from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from skifer.services.context import (
    RequestContext, require_scope, InvalidRequest, ResourceNotFound,
    SCOPE_PROJECT_READ, SCOPE_PIPELINES_WRITE,
)

@dataclass(frozen=True)
class LocalizedError:
    code: str            # ex: "filter.unknown_operator", "join.unknown_alias", "contract.output.unknown_column"
    message: str         # texte humain déjà produit par le loader
    path: str            # ex: "tables[0].filter[1]", "join[0].table_to", "contract.output.amount"
    def to_dict(self) -> dict[str, str]:
        return {"code": self.code, "message": self.message, "path": self.path}

@dataclass(frozen=True)
class PipelineView:
    path: str                              # chemin relatif au projet
    raw_yaml: str                          # texte brut lu sur disque
    normalized: dict[str, Any] | None      # schéma normalisé, ou None si erreurs
    errors: tuple[LocalizedError, ...] = ()
    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "raw_yaml": self.raw_yaml,
            "normalized": self.normalized,
            "errors": [e.to_dict() for e in self.errors],
        }

@dataclass(frozen=True)
class ProjectView:
    root: str
    config: dict[str, Any]                 # config.yaml chargée (sans secrets)
    environments: tuple[str, ...]
    pipelines: tuple[str, ...]             # chemins relatifs des .yaml sous schemas/
    rules: tuple[str, ...]                 # chemins relatifs des .py sous rules/
    models: tuple[str, ...]                # clés du semantic_catalog.yaml
    calendars: tuple[str, ...]             # clés des calendars/*.yaml
    def to_dict(self) -> dict[str, Any]: ...

class ProjectService:
    def __init__(self, project_dir: str): ...     # normalise en chemin absolu; ne lit rien

    def open(self, ctx: RequestContext) -> ProjectView:
        require_scope(ctx, SCOPE_PROJECT_READ); ...

    def get_pipeline(self, ctx: RequestContext, path: str) -> PipelineView:
        require_scope(ctx, SCOPE_PROJECT_READ); ...

    def json_schema(self, ctx: RequestContext) -> dict[str, Any]:
        require_scope(ctx, SCOPE_PROJECT_READ); ...   # JSON Schema statique décrivant un pipeline YAML

    def op_catalog(self, ctx: RequestContext) -> dict[str, Any]:
        require_scope(ctx, SCOPE_PROJECT_READ); ...   # sérialise FILTER_OPERATORS / COLUMN_OPS / AGGREGATE_FUNCTIONS

    def describe(self, ctx: RequestContext, path: str) -> dict[str, Any]:
        require_scope(ctx, SCOPE_PROJECT_READ); ...   # tables/joins/rules/output d'un pipeline normalisé

    def project_output(self, ctx: RequestContext, path: str) -> dict[str, Any]:
        require_scope(ctx, SCOPE_PROJECT_READ); ...   # OutputProjector.project(parse_to_ir(schema)) → colonnes prédites

    def lineage(self, ctx: RequestContext, path: str) -> dict[str, Any]:
        require_scope(ctx, SCOPE_PROJECT_READ); ...   # LineageTracker statique (aucun Spark)

    def explain_rules(self, ctx: RequestContext, path: str) -> dict[str, Any]:
        require_scope(ctx, SCOPE_PROJECT_READ); ...   # SkiferEngine.explain_rules_report → {"profiles": [...], "warnings": [...]}

    def write_pipeline(self, ctx: RequestContext, path: str, text: str) -> str:
        require_scope(ctx, SCOPE_PIPELINES_WRITE); ...  # valide (parse_schema_localized) puis write_yaml_atomic-like
```
```python
# core/schema_loader.py  (ajout)
from dataclasses import dataclass
@dataclass(frozen=True)
class LocalizedIssue:
    code: str
    message: str
    path: str

def parse_schema_localized(
    yaml_str: str, params=None, *, base_dir: str | None = None
) -> tuple[dict | None, list[LocalizedIssue]]:
    """Comme parse_schema, mais renvoie (schema|None, issues) au lieu de lever.
    Réutilise les helpers _validate_* ; dérive code/path du préfixe '[location]'."""
```
```python
# core/core.py  (ajout)
def explain_rules_report(self, schema_dict, shared_read_threshold: int = 2) -> dict:
    rule_names = schema_dict.get("business_rules", [])
    analyzer = RuleAnalyzer()
    profiles = analyzer.analyze_rules(rule_names)
    warnings = analyzer.detect_warnings(profiles, shared_read_threshold=shared_read_threshold)
    return {
        "profiles": [
            {"name": p.name, "writes": list(p.output_columns), "reads": list(p.input_columns),
             "source_available": p.source_available, "has_python_udf": p.has_python_udf,
             "loc": p.loc, "withcolumn_count": p.withcolumn_count}
            for p in profiles
        ],
        "warnings": [
            {"level": w.level, "code": w.code, "message": w.message,
             "rules": list(w.rules), "column": w.column}
            for w in warnings
        ],
    }
```

### Comportement & règles
- **Refus de chemin hors projet** : `get_pipeline`/`describe`/`project_output`/`lineage`/`explain_rules`/`write_pipeline` résolvent `path` puis vérifient `Path(resolved).resolve().is_relative_to(self._root.resolve())` ; sinon `InvalidRequest("path escapes the project root")`. Traiter symlinks et `..` via `resolve()`.
- **Erreurs localisées** : `get_pipeline` ne lève **pas** sur un YAML invalide — il renvoie `PipelineView(normalized=None, errors=(…,))`. Mapping `code`/`path` dérivé du préfixe entre crochets des messages existants : `"  [table 'ord' filter] …"` → `code="filter.unknown_operator"`, `path="tables[?].filter"` ; `"  [join] 'table_to' references unknown alias"` → `code="join.unknown_alias"`, `path="join[i].table_to"` ; `"  [contract.output] column 'x' is not produced…"` → `code="contract.output.unknown_column"`, `path="contract.output.x"`. Fournir une table de correspondance préfixe→code et un `path` best-effort ; les trois cas testés (filter, join, contract) doivent être exacts.
- **`write_pipeline` fail-closed & atomique** : (1) `require_scope(pipelines:write)` ; (2) refus hors racine ; (3) valider `text` via `parse_schema_localized` — si erreurs, lever `InvalidRequest` **sans écrire** ; (4) écriture atomique (fichier temporaire `NamedTemporaryFile` dans le **même répertoire** cible, `os.replace`) — jamais d'écriture partielle. Réutiliser le motif de `semantic/persistence.write_yaml_atomic` (mais ici on écrit du texte brut déjà fourni, pas un dump YAML).
- **Aucun Spark** : `project_output` passe par `OutputProjector` + `parse_to_ir` (statique) ; `lineage` par `LineageTracker`/`LineageGraph` (statiques). Aucun DataFrame, aucune session.
- `open` est tolérant : un projet sans `rules/`, sans `calendars/` ou sans `semantic_catalog.yaml` renvoie des tuples vides, pas une erreur.

### Cas de test — `tests/test_services_project.py` (Spark-free ; utilise `tmp_path`)
- `test_open_lists_project_assets` : arbo temporaire (`config.yaml`, `schemas/gold/f.yaml`, `rules/r.py`, `calendars/fy.yaml`, `semantic_catalog.yaml`) → `ProjectView` liste chacun.
- `test_open_requires_project_scope` : `open(ctx_sans_project_read)` lève `ScopeDenied`.
- `test_get_pipeline_localized_filter_error` : YAML avec `region:eqals:EMEA` → `errors` contient `code="filter.unknown_operator"`, `path` pointant la table/filter ; `normalized is None`.
- `test_get_pipeline_localized_join_error` : `table_to` vers alias inconnu → `code="join.unknown_alias"`.
- `test_get_pipeline_localized_contract_error` : `contract.output` référence une colonne non produite → `code="contract.output.unknown_column"`.
- `test_get_pipeline_valid_returns_normalized` : YAML valide → `errors == ()`, `normalized` non nul, `raw_yaml` == texte disque.
- `test_path_outside_project_refused` : `get_pipeline(ctx, "../../etc/passwd")` lève `InvalidRequest`.
- `test_write_pipeline_atomic` : écrit un YAML valide → fichier présent et relisible ; un YAML invalide → `InvalidRequest` **et** fichier absent/inchangé (prouver l'atomicité : pré-écrire un contenu, tenter un write invalide, vérifier contenu original intact).
- `test_write_pipeline_requires_scope` : `write_pipeline(ctx_sans_pipelines_write, …)` lève `ScopeDenied`.
- `test_explain_rules_returns_structure` : pipeline avec `business_rules` → dict `{"profiles": [...], "warnings": [...]}`, aucun stdout (capsys vide).
- `test_op_catalog_lists_operators` : `op_catalog` contient les clés `filters`, `column_ops`, `aggregate_functions` non vides.
- `tests/test_core.py` (existant) : ajouter `test_explain_rules_report_is_silent` — `explain_rules_report` ne print pas ; `explain_rules` (ancienne) print toujours (rétrocompat).

### Commit
```
feat(plan31-1.2): ProjectService with localized errors, structured explain_rules and atomic write
```
CHANGELOG :
```
- **Plan 31 (1.2)** — `services/project.py` : `ProjectService.open/get_pipeline/json_schema/op_catalog/describe/project_output/lineage/explain_rules/write_pipeline`. Erreurs de schéma localisées `{code, message, path}` (`parse_schema_localized`), `explain_rules` disponible en structure (`SkiferEngine.explain_rules_report`), écriture de pipeline atomique sous scope `pipelines:write`, refus des chemins hors projet. Sans Spark.
```

### DoD
- [ ] `ProjectService` complet, toutes méthodes sous scope.
- [ ] `parse_schema_localized` + `explain_rules_report` ajoutés sans casser `parse_schema`/`explain_rules`.
- [ ] Atomicité de `write_pipeline` prouvée par test.
- [ ] Aucun import Spark dans `services/project.py`.

---

## Slice 31.1.3 — `RuleService` : scan `importlib`, catalogue, graphe, snippets, écriture

**Objectif** : découvrir les règles par `importlib` (jamais `exec`), les cataloguer, produire un graphe de dépendances, générer des snippets déterministes et écrire une règle après `ast.parse`, sous scope `rules:write`.

### Fichiers
- `src/skifer/services/rules.py` — **création** : `RuleService`, `RuleView`, `ScanReport`, `SnippetSpec`.
- `src/skifer/core/rule_analyzer.py` — **modification (minimale)** : ajouter `analyze_source(source: str, name: str) -> RuleProfile` qui parse une source brute (réutilise `_ColumnVisitor`/`_PerformanceVisitor`), pour permettre au scan d'analyser un module sans dépendre du registre. Ne pas toucher aux méthodes existantes.

### Signatures Python
```python
# services/rules.py
from __future__ import annotations
import ast, importlib.util
from dataclasses import dataclass, field
from typing import Any
from skifer.services.context import (
    RequestContext, require_scope, InvalidRequest, ResourceNotFound,
    SCOPE_PROJECT_READ, SCOPE_RULES_WRITE,
)

@dataclass(frozen=True)
class RuleView:
    name: str
    kind: str                 # "projection" | "transform" | "aggregation" (défaut "projection")
    reads: tuple[str, ...]
    writes: tuple[str, ...]
    warnings: tuple[str, ...]
    file: str
    line: int
    def to_dict(self) -> dict[str, Any]: ...

@dataclass(frozen=True)
class ScanReport:
    rules: tuple[RuleView, ...]
    invalid_modules: tuple[dict[str, str], ...]   # [{"file":..., "error": <ClassName>}] — module isolé
    def to_dict(self) -> dict[str, Any]: ...

@dataclass(frozen=True)
class SnippetSpec:
    kind: str                 # "constant" | "cast" | "when_otherwise" | "with_column"
    target: str               # nom de colonne de sortie (identifiant validé)
    # champs optionnels selon kind :
    value: Any = None         # constant / else de when
    source: str | None = None # cast / with_column
    to_type: str | None = None
    condition: str | None = None   # when_otherwise: "col:op:val" (grammaire filtre)
    then: Any = None
    expression: str | None = None  # with_column: expression PySpark littérale simple

class RuleService:
    def __init__(self, project_dir: str, extra_paths: tuple[str, ...] = ()): ...

    def scan(self, ctx: RequestContext, paths: tuple[str, ...] = ()) -> ScanReport:
        require_scope(ctx, SCOPE_PROJECT_READ); ...

    def list(self, ctx: RequestContext) -> tuple[RuleView, ...]:
        require_scope(ctx, SCOPE_PROJECT_READ); ...

    def dependency_graph(self, ctx: RequestContext, names: tuple[str, ...]) -> dict[str, list[str]]:
        require_scope(ctx, SCOPE_PROJECT_READ); ...

    def generate_snippet(self, ctx: RequestContext, spec: SnippetSpec) -> str:
        require_scope(ctx, SCOPE_PROJECT_READ); ...   # déterministe, byte-for-byte

    def write_rule(self, ctx: RequestContext, file: str, code: str) -> str:
        require_scope(ctx, SCOPE_RULES_WRITE); ...    # ast.parse(code) avant écriture atomique
```
```python
# core/rule_analyzer.py  (ajout)
def analyze_source(self, source: str, name: str) -> RuleProfile:
    import textwrap
    try:
        tree = ast.parse(textwrap.dedent(source))
    except (SyntaxError, IndentationError):
        return RuleProfile(name=name, source_available=False)
    v = _ColumnVisitor(); v.visit(tree); v.resolve_returned_dicts()
    pv = _PerformanceVisitor(); pv.visit(tree)
    ...  # même assemblage que analyze_rule
```

### Comportement & règles
- **Découverte via `importlib`, jamais `exec`** : `scan` collecte `rules/**/*.py` sous la racine + `paths` déclarés + `extra_paths` ; charge chaque module via `importlib.util.spec_from_file_location` + `module_from_spec` + `spec.loader.exec_module`. **Un module qui échoue à l'import est isolé** : capté, ajouté à `invalid_modules` (nom de classe d'exception uniquement), et n'empêche pas les autres. `kind`/`reads`/`writes`/`line` dérivés par introspection du module (fonctions décorées `@RuleRegistry.register_rule`) et `RuleAnalyzer.analyze_rule` sur chaque fonction ; `line` = `func.__code__.co_firstlineno`.
- **`dependency_graph`** délègue à `RuleAnalyzer.build_dependency_graph` après avoir résolu les profils des `names` (via le registre peuplé par le scan). Un nom inconnu → profil `source_available=False` (conservateur), pas d'exception.
- **`generate_snippet` déterministe byte-for-byte** : sortie fonction du seul `spec` (aucun horodatage, aucun `id()`, aucune itération de set non triée). Le `target` et toute colonne source doivent matcher `^[A-Za-z_][A-Za-z0-9_]*$` sinon `InvalidRequest` (même règle anti-injection que la couche sémantique). Formes supportées :
  - `constant` → `def {name}(df):\n    return {{"{target}": F.lit({value!r})}}\n`
  - `cast` → `... F.col("{source}").cast("{to_type}") ...`
  - `when_otherwise` → `F.when(<condition compilée>, F.lit({then!r})).otherwise(F.lit({value!r}))`
  - `with_column` (kind="transform") → `def {name}(df):\n    return df.withColumn("{target}", {expression})\n`
- **`write_rule` fail-closed** : `require_scope(rules:write)` → refus hors racine → `ast.parse(code)` (lève `InvalidRequest` sur `SyntaxError`, **sans écrire**) → écriture atomique (`os.replace`). Ne **jamais** exécuter `code`.

### Cas de test — `tests/test_services_rules.py` (Spark-free)
- `test_scan_isolates_broken_module` : deux fichiers, l'un valide, l'autre `def r(:` → `ScanReport.rules` contient la règle valide, `invalid_modules` contient le fichier cassé ; aucune exception propagée.
- `test_scan_reload_picks_up_new_rule` : scanner, ajouter un fichier, re-scanner → nouvelle règle présente (prouve rechargement).
- `test_list_returns_rule_views_with_line` : `RuleView.line` == ligne réelle de `def`.
- `test_dependency_graph_edges` : règle B lit une colonne écrite par A → `graph["B"] == ["A"]`.
- `test_generate_snippet_constant_byte_for_byte` : deux appels identiques → chaîne identique et égale à une constante attendue littérale.
- `test_generate_snippet_when_otherwise_byte_for_byte` : idem pour `when_otherwise`.
- `test_generate_snippet_rejects_bad_identifier` : `target="1bad"` → `InvalidRequest`.
- `test_write_rule_rejects_syntax_error` : `code="def ("` → `InvalidRequest`, fichier absent.
- `test_write_rule_requires_scope` : sans `rules:write` → `ScopeDenied`.
- `test_write_rule_atomic_ok` : code valide → fichier écrit, `ast.parse` du contenu OK.
- Note : ces tests ne construisent pas de `F.col(...)` (ils comparent des **chaînes** de snippet et parsent de l'AST) → **pas de fixture `spark`**.

### Commit
```
feat(plan31-1.3): RuleService — importlib scan, catalog, dependency graph, deterministic snippets, guarded write
```
CHANGELOG :
```
- **Plan 31 (1.3)** — `services/rules.py` : `RuleService.scan/list/dependency_graph/generate_snippet/write_rule`. Découverte par `importlib` (jamais `exec`), module invalide isolé, snippets déterministes byte-for-byte (constant/cast/when-otherwise/withColumn), écriture sous `rules:write` après `ast.parse`. Ajout `RuleAnalyzer.analyze_source`.
```

### DoD
- [ ] Aucun `exec`, aucun `eval` dans `services/rules.py`.
- [ ] Module cassé isolé, autres chargés.
- [ ] Snippets reproductibles (test d'égalité littérale).
- [ ] `write_rule` refuse une source non parsable sans écrire.

---

## Slice 31.1.4 — `GovernanceService` & `QualityService` sur les stores existants

**Objectif** : lecture gouvernée des contrats/versions, certifications, data products et d'une quarantaine bornée (`GovernanceService`) ; checks dérivés d'un YAML, historique et dernier rapport (`QualityService`). Vues allowlistées, **aucun DataFrame** ne franchit la frontière.

### Fichiers
- `src/skifer/services/governance.py` — **création** : `GovernanceService`, `ContractVersionView`, `DataProductView`, `QuarantineView`.
- `src/skifer/services/quality.py` — **création** : `QualityService`, `CheckDefinitionView`, `CheckRunView`, `QualityReportView`.

### Signatures Python
```python
# services/governance.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Any
from skifer.services.context import (
    RequestContext, require_scope, ResourceUnavailable, ResourceNotFound,
    SCOPE_CONTRACTS_READ,
)
from skifer.services.context import HARD_MAX_PAGE_SIZE

@dataclass(frozen=True)
class ContractVersionView:
    contract_id: str
    versions: tuple[str, ...]
    def to_dict(self) -> dict[str, Any]: ...

@dataclass(frozen=True)
class DataProductView:
    data_product_id: str
    latest_version: str | None
    owner: str | None
    def to_dict(self) -> dict[str, Any]: ...

@dataclass(frozen=True)
class QuarantineView:
    dataset: str
    rows: tuple[dict[str, Any], ...]   # borné (<= max_rows), JSON-native via to_json_value
    truncated: bool
    def to_dict(self) -> dict[str, Any]: ...

class GovernanceService:
    def __init__(self, certification_store, *, max_rows: int = HARD_MAX_PAGE_SIZE): ...

    def get_contract(self, ctx: RequestContext, contract_id: str, version: str): ...        # contracts:read → ContractView (réutilise DTO existant)
    def list_certification_history(self, ctx: RequestContext, dataset: str, limit: int = 50): ...  # contracts:read → list[dict]
    def get_certification(self, ctx: RequestContext, dataset: str): ...                       # contracts:read → CertificationView
    def read_quarantine(self, ctx: RequestContext, dataset: str, limit: int = 50) -> QuarantineView: ...  # contracts:read, borné

# services/quality.py
from skifer.services.context import RequestContext, require_scope, SCOPE_CONTRACTS_READ, ResourceUnavailable

@dataclass(frozen=True)
class CheckDefinitionView:
    name: str; kind: str; column: str | None; params: dict[str, Any]
    def to_dict(self) -> dict[str, Any]: ...

@dataclass(frozen=True)
class QualityReportView:
    table: str; passed: bool; checks: tuple[dict[str, Any], ...]; run_at: str | None
    def to_dict(self) -> dict[str, Any]: ...

class QualityService:
    def __init__(self, history_store, *, contract_extractor=None): ...

    def checks_from_schema(self, ctx: RequestContext, schema_dict: dict) -> tuple[CheckDefinitionView, ...]:
        require_scope(ctx, SCOPE_CONTRACTS_READ); ...     # via ContractExtractor.extract(schema_dict)
    def history(self, ctx: RequestContext, table: str, limit: int = 20) -> tuple[QualityReportView, ...]:
        require_scope(ctx, SCOPE_CONTRACTS_READ); ...     # via HistoryStore.get_last_n
    def last_report(self, ctx: RequestContext, table: str) -> QualityReportView | None:
        require_scope(ctx, SCOPE_CONTRACTS_READ); ...     # via HistoryStore.get_latest
```

### Comportement & règles
- **Scopes de lecture** : tout est `contracts:read` (aligné sur les scopes historiques du service data). `read_quarantine` reste `contracts:read` (lecture gouvernée bornée) ; **pas** de scope d'écriture ici (l'écriture certifiée passe par le pipeline, hors périmètre de cette feature).
- **Vues allowlistées, aucun DataFrame** : `read_quarantine` collecte au plus `min(limit, max_rows)` lignes et les sérialise via `services.serialization.row_to_json` (JSON-native) ; `truncated=True` si le store en avait davantage. Jamais renvoyer un DataFrame ni un objet Spark. Si le store ne supporte pas la lecture demandée → `ResourceUnavailable` (fail-closed), jamais un retour vide silencieux ambigu.
- `checks_from_schema` réutilise `ContractExtractor.extract` puis mappe chaque `DataContract` en `CheckDefinitionView` (allowlist des champs). `history`/`last_report` mappent `MonitorReport` en `QualityReportView` sans exposer d'objets internes.
- Store absent / méthode manquante (`get_contract` non exposé, etc.) → `ResourceUnavailable`, même convention que `AgentReadyDataService`.

### Cas de test — `tests/test_services_governance.py`, `tests/test_services_quality.py` (Spark-free ; stores doublés)
- `test_get_contract_requires_scope` / `test_get_certification_requires_scope` : `ScopeDenied` sans `contracts:read`.
- `test_read_quarantine_is_bounded` : store rend 200 lignes, `max_rows=100`, `limit=50` → `len(rows) == 50`, `truncated is True`.
- `test_read_quarantine_returns_json_native` : une ligne avec `Decimal`/`date` → sérialisée en `str`/isoformat ; **aucun** objet non-JSON dans `to_dict()`.
- `test_read_quarantine_no_dataframe` : le double de store peut renvoyer un faux « DataFrame » (objet avec `.collect()`), mais la vue ne contient que des dicts — asserter que `QuarantineView.rows` sont des `dict`.
- `test_history_maps_reports` : `HistoryStore.get_last_n` doublé → `QualityReportView` corrects, ordre préservé.
- `test_checks_from_schema` : schéma avec `quality_checks: {drop_nulls_in: [amount]}` → `CheckDefinitionView` attendu.
- `test_store_missing_method_unavailable` : store sans `get_contract` → `ResourceUnavailable`.

### Commit
```
feat(plan31-1.4): GovernanceService and QualityService over existing stores (allowlisted views, no DataFrame)
```
CHANGELOG :
```
- **Plan 31 (1.4)** — `services/governance.py` (contrats/versions, certification, data products, lecture bornée de quarantaine) et `services/quality.py` (checks dérivés d'un YAML, historique, dernier rapport) sur les stores existants. Vues allowlistées JSON-native ; aucun DataFrame ne franchit `services/` ; lecture sous scope `contracts:read`.
```

### DoD
- [ ] Toutes les lectures sous `contracts:read`.
- [ ] `read_quarantine` borné et JSON-native, `truncated` correct.
- [ ] Aucun objet Spark dans les vues (test dédié).

---

## Slice 31.1.5 — `SemanticService` & `AgentService` ; `wizard` pilotable par dict

**Objectif** : façade `SemanticService` sur `AgentReadyDataService` + `SemanticSynchronizer` + `SemanticValidator` (mêmes codes que le CLI : 0/2/3) ; `AgentService.ask/build` sur `AgenticHub`/`BuilderAgent` ; rendre `BuilderAgent.wizard` pilotable par un **dict de réponses** au lieu de `input()`.

### Fichiers
- `src/skifer/services/semantic.py` — **création** : `SemanticService`, `SyncOutcome`.
- `src/skifer/services/agents.py` — **création** : `AgentService`.
- `src/skifer/agentic/builder_agent.py` — **modification** : injecter un fournisseur d'entrées ; `wizard` reste identique par défaut (stdin), mais accepte des réponses programmatiques.

### Signatures Python
```python
# services/semantic.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Any
from skifer.services.context import (
    RequestContext, require_scope, SCOPE_MODELS_READ, SCOPE_PIPELINES_WRITE, InvalidRequest,
)
from skifer.semantic.sync import SemanticSynchronizer, SyncReport
from skifer.semantic.validator import SemanticValidator

# Codes alignés sur cli.py : OK=0, DRIFT=2, CONFLICT=3 (1 = erreur technique, remontée par exception).
SEMANTIC_OK = 0
SEMANTIC_DRIFT = 2
SEMANTIC_CONFLICT = 3

@dataclass(frozen=True)
class SyncOutcome:
    code: int                       # 0 | 2 | 3
    report: dict[str, Any]          # report allowlisté (changes/conflicts/suggestions/has_changes)
    wrote: bool
    def to_dict(self) -> dict[str, Any]: ...

class SemanticService:
    def __init__(self, data_service, *, models_dir: str = "semantic_models"): ...

    def check(self, ctx: RequestContext, pipeline_path: str) -> SyncOutcome:
        require_scope(ctx, SCOPE_MODELS_READ); ...         # write=False → 0/2/3, n'écrit jamais
    def write_draft(self, ctx: RequestContext, pipeline_path: str) -> SyncOutcome:
        require_scope(ctx, SCOPE_PIPELINES_WRITE); ...     # applique si rapport propre
    def promote(self, ctx: RequestContext, pipeline_path: str) -> SyncOutcome:
        require_scope(ctx, SCOPE_PIPELINES_WRITE); ...     # refuse si perte de contenu humain

    # Lecture gouvernée déléguée au data_service (mêmes scopes que MCP) :
    def list_models(self, ctx, cursor=None, limit=50): ...   # models:read
    def get_model(self, ctx, key): ...                        # models:read
```
```python
# services/agents.py
from skifer.services.context import RequestContext, require_scope, SCOPE_QUERY_EXECUTE, SCOPE_PIPELINES_WRITE

class AgentService:
    def __init__(self, hub, *, builder_agent=None): ...

    def ask(self, ctx: RequestContext, question: str, profile: Any = None) -> dict[str, Any]:
        require_scope(ctx, SCOPE_QUERY_EXECUTE)
        response = self._hub.ask(question, profile=profile)     # HubResponse
        from skifer.serving._response_serializer import hub_response_to_text
        text = hub_response_to_text(response)
        return {"text": text, "response": _response_to_dict(response)}   # to_dict allowlisté

    def build(self, ctx: RequestContext, description: str, output_dir: str = "schemas") -> dict[str, Any]:
        require_scope(ctx, SCOPE_PIPELINES_WRITE)
        resp = self._builder.ask(description, output_dir=output_dir)      # BuilderResponse
        return {"success": resp.success, "yaml_content": resp.yaml_content,
                "output_path": resp.output_path, "error": resp.error}
```
```python
# agentic/builder_agent.py  (modification)
class BuilderAgent:
    def __init__(self, ..., input_provider: Callable[[str], str] | None = None):
        self._input = input_provider or input     # défaut : builtins.input (stdin)
    # remplacer chaque appel `input(prompt)` des _wizard_* et du "Sauvegarder ?" par `self._input(prompt)`
    def wizard(self, output_dir: str = "schemas", answers: dict[str, list[str]] | None = None) -> str:
        # si answers fourni, construire un input_provider séquentiel piloté par 'answers'
        # (une file de réponses), sinon self._input inchangé.
        ...
```
`answers` : soit une **file** de chaînes consommée dans l'ordre des prompts, soit — plus robuste — un mapping `{clé_de_prompt: [réponses]}`. Retenir la **file ordonnée** (`list[str]`) pour rester déterministe et simple ; documenter l'ordre exact des prompts du wizard.

### Comportement & règles
- **Codes identiques au CLI** : `check` renvoie `SEMANTIC_DRIFT` si `report.has_changes` sans conflit/suggestion, `SEMANTIC_CONFLICT` si `report.conflicts or report.suggestions`, sinon `SEMANTIC_OK`. Une exception d'inspection (pipeline illisible) est **propagée** (le CLI la mappe en 1) ; `SemanticService` ne l'avale pas.
- **`promote` refuse la perte de contenu humain** : réutiliser la logique de `cli.run_semantic_sync` (`_assert_no_curation_loss`) — **factoriser** cette fonction dans un helper importable (par ex. la déplacer/exposer telle quelle et l'importer depuis `services/semantic.py`) plutôt que dupliquer. Si une promotion supprimerait du contenu curé → `SyncOutcome(code=SEMANTIC_CONFLICT, …)` **sans écrire** (ou lever `InvalidRequest` — choisir `code=3` pour rester aligné sur le CLI).
- **`wizard` sans stdin** : avec `answers` fourni, aucun appel à `input()` réel. La séquence de réponses couvre : tables (fqn, alias, "continuer?"), filtres, jointures, rules, select, options, "Sauvegarder ?". Le wizard doit être **entièrement** dérivable de la file.
- `AgentService.ask` : `hub_response_to_text` pour le canal texte + `to_dict` allowlisté ; **jamais** de DataFrame dans la sortie (si `AgentResponse.result.data` est un DataFrame, ne pas l'inclure — n'exposer que `text_summary`/`kpi`/titres).
- `build` sous `pipelines:write` (produit un YAML sur disque).

### Cas de test — `tests/test_services_semantic.py`, `tests/test_services_agents.py`, `tests/test_builder_agent.py` (Spark-free ; LLM/hub mockés)
- `test_check_returns_drift_code` : synchronizer doublé renvoyant un `SyncReport` avec changements → `code == 2`, `wrote is False`.
- `test_check_returns_conflict_code` : rapport avec conflit → `code == 3`.
- `test_check_returns_ok_code` : rapport vide → `code == 0`.
- `test_check_requires_models_read` : `ScopeDenied` sans scope.
- `test_promote_refuses_curation_loss` : modèle curé existant avec champ humain absent du payload promu → `code == 3`, **aucune** écriture (vérifier fichier inchangé).
- `test_write_draft_requires_pipelines_write` : `ScopeDenied`.
- `test_agent_ask_returns_text_and_dict` : hub mocké renvoyant un `AgentResponse` → `{"text": ..., "response": {...}}`, aucun DataFrame dans le dict.
- `test_agent_ask_requires_query_execute` : `ScopeDenied`.
- `test_wizard_driven_by_answers_without_stdin` (dans `test_builder_agent.py`) : `BuilderAgent(input_provider=...)` ou `wizard(answers=[...])` produit un YAML **sans** jamais appeler `builtins.input` (monkeypatch `builtins.input` pour lever si appelé) — prouve l'absence de stdin.
- `test_wizard_default_still_uses_input` : sans `answers`, `self._input is builtins.input` (rétrocompat).

### Commit
```
feat(plan31-1.5): SemanticService + AgentService facades; BuilderAgent wizard driveable without stdin
```
CHANGELOG :
```
- **Plan 31 (1.5)** — `services/semantic.py` (`check/write_draft/promote`, mêmes codes que le CLI 0/2/3, refus de promotion si perte de contenu humain ; `list_models/get_model` délégués) et `services/agents.py` (`ask` via `AgenticHub`+`hub_response_to_text`, `build` via `BuilderAgent.ask`). `BuilderAgent.wizard` pilotable par un dict/liste de réponses (plus de dépendance à `input()`).
```

### DoD
- [ ] Codes 0/2/3 identiques au CLI ; `check`/`promote` n'écrivent pas quand ils doivent refuser.
- [ ] `_assert_no_curation_loss` factorisée, pas dupliquée.
- [ ] `wizard(answers=…)` sans aucun `input()` (test qui fait échouer tout appel stdin).
- [ ] Aucun DataFrame dans les sorties d'`AgentService`.

---

## Slice 31.1.6 — `LocalIdentity` : identité locale fail-closed pour stdio & clients locaux

**Objectif** : fournir `LocalIdentity(subject=get_clean_username(), scopes=LOCAL_DEFAULT_SCOPES)` pour le MCP stdio et tout client local ; **jamais** `certification_override` ; **jamais** un scope venu du client ; aligné avec la slice 7.4 (config stdio du serveur MCP).

### Fichiers
- `src/skifer/services/identity.py` — **création** : `LocalIdentity`, `LOCAL_DEFAULT_SCOPES`, `local_request_context(...)`.
- `src/skifer/mcp/server.py` — **modification (mineure)** : documenter/relier que le provider stdio local dérive ses scopes de `LOCAL_DEFAULT_SCOPES` (le provider stdio réel est dans `mcp/auth.py`/`runtime.py` ; ici on expose la source des scopes locaux réutilisable par la CLI et une future `api/`).

### Signatures Python
```python
# services/identity.py
from __future__ import annotations
from dataclasses import dataclass
from skifer.services.context import (
    RequestContext, NAMED_SCOPES, CERTIFICATION_OVERRIDE_SCOPE, InvalidRequest,
)
from skifer.core.environment import get_clean_username
from skifer.observability.tracing import NoOpTracer, current_trace_context

# Arbitrage §7.4 : tous les scopes nommés SAUF certification_override (déjà absent de NAMED_SCOPES).
LOCAL_DEFAULT_SCOPES: frozenset[str] = NAMED_SCOPES

@dataclass(frozen=True)
class LocalIdentity:
    subject: str
    scopes: frozenset[str] = LOCAL_DEFAULT_SCOPES
    consumer_class: str = "local"

    def __post_init__(self) -> None:
        if not isinstance(self.subject, str) or not self.subject.strip():
            raise InvalidRequest("Local identity subject must be non-empty text.")
        if CERTIFICATION_OVERRIDE_SCOPE in self.scopes:
            raise InvalidRequest("certification_override is never granted to a local identity.")

    @classmethod
    def resolve(cls, *, scopes: frozenset[str] = LOCAL_DEFAULT_SCOPES) -> "LocalIdentity":
        return cls(subject=get_clean_username() or "local-user", scopes=scopes)

    def to_request_context(self, *, tracer=None) -> RequestContext:
        active = tracer or NoOpTracer()
        return RequestContext(
            subject=self.subject,
            scopes=self.scopes,
            consumer_class=self.consumer_class,
            trace_context=current_trace_context(active),
        )

def local_request_context(*, tracer=None) -> RequestContext:
    return LocalIdentity.resolve().to_request_context(tracer=tracer)
```

### Comportement & règles
- **Escalade impossible** : `LocalIdentity(scopes=frozenset({"certification_override"}))` (ou tout sur-ensemble le contenant) lève `InvalidRequest` dans `__post_init__`. `LOCAL_DEFAULT_SCOPES` = `NAMED_SCOPES`, qui **n'inclut jamais** `certification_override`.
- **Sujet non vide** : `get_clean_username()` peut être vide dans un environnement dégradé → fallback `"local-user"` non vide ; `__post_init__` rejette un sujet vide explicitement fourni.
- **Jamais un scope venu du client** : `LocalIdentity` ne lit aucune entrée réseau/param ; ses scopes sont statiques. Aligné sur `mcp/auth.create_stdio_context_provider` (qui ignore déjà la requête). La CLI `skifer mcp serve --transport stdio` (slice 7.4) peut alimenter ses scopes stdio depuis `LOCAL_DEFAULT_SCOPES` ; documenter la relation, ne pas dupliquer la liste de scopes.
- Fail-closed : `certification_override` reste interdit de bout en bout (cohérent avec `mcp/auth._validated_scopes` qui rejette déjà ce scope).

### Cas de test — `tests/test_services_identity.py` (Spark-free)
- `test_local_default_scopes_are_named_scopes_without_override` : `LOCAL_DEFAULT_SCOPES == NAMED_SCOPES` et `"certification_override" not in LOCAL_DEFAULT_SCOPES`.
- `test_local_identity_rejects_override_scope` : `LocalIdentity(subject="u", scopes=frozenset({"certification_override"}))` lève `InvalidRequest`.
- `test_local_identity_rejects_empty_subject` : `LocalIdentity(subject="  ")` lève `InvalidRequest`.
- `test_resolve_subject_non_empty` : monkeypatch `get_clean_username` → "" ; `LocalIdentity.resolve().subject` non vide (fallback).
- `test_to_request_context_carries_scopes` : `to_request_context()` renvoie un `RequestContext` valide portant `LOCAL_DEFAULT_SCOPES`.
- `test_escalation_impossible_via_named_scopes` : asserter qu'aucun scope d'écriture ne peut être `certification_override` (sanity).

### Commit
```
feat(plan31-1.6): LocalIdentity for stdio MCP and local clients (never certification_override)
```
CHANGELOG :
```
- **Plan 31 (1.6)** — `services/identity.py` : `LocalIdentity`/`LOCAL_DEFAULT_SCOPES`/`local_request_context` pour le MCP stdio et tout client local. Tous les scopes nommés sauf `certification_override` (arbitrage §7.4) ; escalade impossible, sujet toujours non vide, aucun scope venu du client.
```

### DoD
- [ ] `certification_override` refusé dans `__post_init__` et absent de `LOCAL_DEFAULT_SCOPES`.
- [ ] Sujet non vide garanti (fallback testé).
- [ ] Aligné avec `mcp/auth` stdio (pas de duplication de la liste de scopes).

---

## Ordre & dépendances internes

1. **31.1.1 avant tout** : crée `services/` et les primitives (`context`, `serialization`, scopes nommés) dont dépendent 1.2 → 1.6. Repointe `mcp/` — l'`architecture test` de 1.1 devient le garde-fou permanent.
2. **31.1.2** dépend de 1.1 (contexte, scopes, erreurs) et modifie `core/schema_loader.py` + `core/core.py`.
3. **31.1.3** dépend de 1.1 ; modifie `core/rule_analyzer.py` (ajout non intrusif).
4. **31.1.4** dépend de 1.1 (erreurs, `row_to_json`) ; stores existants inchangés.
5. **31.1.5** dépend de 1.1 (+ réutilise `AgentReadyDataService` via `services/`, `SemanticSynchronizer`, `SemanticValidator`, `AgenticHub`, `BuilderAgent`, `hub_response_to_text`). Factorise `_assert_no_curation_loss`.
6. **31.1.6** dépend de 1.1 (scopes nommés, `RequestContext`) ; **relation avec la slice 7.4** : la CLI `skifer mcp serve --transport stdio` configure une identité stdio statique via `mcp/auth.create_stdio_context_provider` ; 1.6 fournit la **source unique** des scopes locaux (`LOCAL_DEFAULT_SCOPES`) que 7.4 réutilise — ne pas redéfinir la liste des scopes côté MCP.

**Acceptance globale de la feature** : après 1.1, `mcp/` importe **uniquement** `services/` ; l'`architecture test` (`tests/test_services_architecture.py`) échoue si `mcp/` — ou une future `api/` — importe `SemanticEngine`, `SkiferEngine` ou un store (`certification_store`/`history`) directement. Étendre ce test au fil des slices pour couvrir aussi `api/` (grep de patterns d'import interdits sur `src/skifer/mcp/` et, si présent, `src/skifer/api/`).

---

## Risques & pièges

- **Cycle d'import pendant l'extraction (1.1)** : `services/__init__` importe `agentic.data_service`, qui importe `services.context`/`services.serialization`. Importer les **sous-modules** directement (`from skifer.services.context import …`) dans `agentic/data_service.py`, et placer le re-export d'`AgentReadyDataService` **en dernier** dans `services/__init__.py`. Ne jamais faire `from skifer.services import X` dans `agentic/data_service.py`.
- **Complétude des re-exports (1.1)** : `mcp/tools.py` importe aussi `HARD_MAX_FILTERS`, `HARD_MAX_FILTER_VALUE_LENGTH`, `QueryEnvelope` ; `mcp/config.py` importe `LimitExceeded`. Le `__all__` de `services/__init__` doit couvrir **exactement** l'union des symboles importés par les 7 modules `mcp/` + les symboles historiques utilisés par les tests. Vérifier avec `test_services_reexport_matches_data_service_identity` (identité d'objet, pas seulement présence).
- **Divergence de sérialisation** : extraire `_json_value`/`_row_to_json` sans changer leur comportement (float non fini → `SerializationError`, `Decimal→str`, datetime→isoformat). `AgentReadyDataService._row_to_json` doit continuer à passer par la fonction extraite pour éviter deux implémentations.
- **Écriture atomique (1.2, 1.3)** : le fichier temporaire doit être créé **dans le répertoire cible** (même système de fichiers) puis `os.replace` — sinon `os.replace` cross-device échoue. Tester qu'un échec de validation laisse le fichier existant **inchangé**.
- **Mapping code/path des erreurs localisées (1.2)** : les messages du loader sont un texte agrégé ; le mapping préfixe→`code` doit être exhaustif pour les trois cas testés (filter, join, contract) et robuste (fallback `code="schema.invalid"`, `path=""`) pour les autres, sans planter.
- **Déterminisme des snippets (1.3)** : n'utiliser aucune source non déterministe (pas d'itération de `set`, pas d'`id()`, pas d'horodatage) ; comparer à une **constante littérale** dans le test.
- **Isolation des modules invalides (1.3)** : `importlib` exécute le module — un module qui lève à l'import ne doit pas faire échouer le scan ; capter largement et n'enregistrer que le **nom de classe** de l'exception (jamais le message, qui peut citer un chemin).
- **Aucun Spark ne franchit `services/` (1.4, 1.5)** : `read_quarantine` et `AgentService.ask` doivent matérialiser en dicts/texte ; interdire tout retour d'un objet exposant `.collect()`/`.toPandas()`. Tests dédiés.
- **`explain_rules` rétrocompat (1.2)** : ne pas supprimer le `print` de l'`explain_rules` historique (exemples/tests en dépendent) ; ajouter `explain_rules_report` à côté.
- **`wizard` rétrocompat (1.5)** : défaut = `builtins.input` ; ne changer le comportement interactif que lorsque `answers`/`input_provider` est fourni.
- **Non-régression du programme MCP (Plan 29)** : après repointage, relancer toute la suite `tests/test_mcp_*` — elle importe encore parfois depuis `agentic.data_service` (compat) et depuis `mcp` ; les deux chemins doivent rester verts.
