# Plan 31 — Feature 6 : API locale `[api]`

**But** : poser un **transport HTTP fin** (FastAPI) sur la couche `services/` (créée en features 31.1–31.5),
dans **ce dépôt**, derrière l'extra optionnel `[api]` (`fastapi`, `uvicorn`), avec des routes **1:1 sur les
services**, une erreur uniforme `{code, message, path}`, un **scope vérifié sur chaque route**, un bind
**loopback**, un **CORS localhost-only**, un healthcheck sans Spark, et une **OpenAPI versionnée**
snapshotée en test. Aucune logique métier dans `api/`, **aucun import de moteur ni de store** dans `api/`.

**Arbitrages §7 (tranchés le 2026-09-11) appliqués :**
- **§7.1** — l'API vit dans ce dépôt derrière `[api]` : versions **en lockstep** avec la lib, les tests de
  la lib couvrent l'API, l'OpenAPI est publiée à chaque release, aucun client ne réimplémente d'appel.
  Cohérent avec `[mcp]`/`[tracing]` : le cœur s'importe **sans** l'extra (imports FastAPI strictement
  paresseux). C'est la justification de « API in-repo ».
- **§7.4** — auth déléguée / scopes partagent `services/identity.py` (`LocalIdentity`, slice 31.1.6) :
  **7.4 doit être livrée AVANT 31.6.1** (dépendance dure). Le `context_provider` par défaut de l'API dérive
  d'une identité locale (tous les scopes nommés sauf `certification_override`) ; un vérificateur bearer
  injecté reste possible (même frontière que `mcp/auth.create_http_context_provider`).
- **§7.5** — packaging MCP : `skifer mcp serve` est **finalisé/livré avec 31.6.2**.

**Discipline de commit** : *une slice = un commit* `feat(plan31-6.M): …` + tests + une entrée
`CHANGELOG.md` sous `## [Unreleased]`. **Ne jamais bumper la version** de `pyproject.toml`. Le dépôt est
sur `release: 2.1.0` et **n'a pas** de section `[Unreleased]` : la **première slice qui touche le
CHANGELOG (31.6.1) crée** `## [Unreleased]` (avec `### Added`) juste sous l'en-tête, avant `## [2.1.0]`.

---

## État actuel du code

Chemins absolus sous `/Users/julhouba/PycharmProjects/skifer/`. Signatures citées telles qu'elles existent
aujourd'hui (repo HEAD `2.1.0`).

### `pyproject.toml` — style **exact** des extras à copier
Les extras sont déclarés sous `[project.optional-dependencies]` avec un commentaire `# --- … ---` par
groupe. Les extras à « imports paresseux » portent la mention `(imports strictement paresseux)` :

```toml
# --- Model Context Protocol (imports strictement paresseux) ---
mcp = [
    "mcp>=2.1,<3",
]

# --- Export de traces (imports strictement paresseux) ---
tracing = [
    "opentelemetry-sdk>=1.24.0",
    "opentelemetry-exporter-otlp-proto-http>=1.24.0",
    "mlflow>=2.12.0,<3",
]

# --- Databricks workspace API (SQL warehouses, materialized views, Connect) ---
databricks = [
    "databricks-sdk>=0.20.0"
]
```

- `version = "2.1.0"` (ligne 6) — **jamais modifiée**.
- `[tool.setuptools.packages.find]` → `where = ["src"]` : un nouveau package `src/skifer/api/` est
  découvert automatiquement (aucune config à ajouter).
- `[project.scripts]` → `skifer = "skifer.cli:main"` : la CLI passe par `skifer/cli.py:main` (argparse).
- `[tool.pytest.ini_options]` → `pythonpath = ["src"]`.

### `src/skifer/mcp/config.py` — motif « bind non-loopback = erreur au démarrage »
```python
def _is_loopback(host: str) -> bool:
    if host.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False
# … dans load_mcp_config (stdio) :
if not _is_loopback(bind.host):
    raise MCPConfigError("MCP stdio refuses a non-loopback bind at startup.")
```
`MCP_SCOPES = frozenset({"models:read","contracts:read","lineage:read","query:execute"})`. Erreurs de
config = `MCPConfigError` (jamais de citation du contenu du fichier).

### `src/skifer/mcp/server.py` + `mcp/runtime.py` — lazy import de l'extra + health figé
- `create_server(...)` fait `importlib.import_module("mcp")` **dans la fonction** ; en `ImportError` lève
  `MCPDependencyError(f"MCP server support requires the optional dependencies: `{MCP_EXTRA}`.")` où
  `MCP_EXTRA = 'pip install -e ".[mcp]"'`. Le cœur importe `mcp/server.py` sans l'extra installé.
- `mcp/runtime.py` : `HEALTH_PATH="/health"`, `HEALTH_BODY=b'{"status":"ok"}'`, `health_payload()` renvoie
  `json.loads(HEALTH_BODY)` — **constante figée, sans donnée métier**. `_serve_http` importe `uvicorn`
  paresseusement et `uvicorn.run(app, host=config.bind.host, port=config.bind.port, log_config=None)`.
- `MCPStartupError` : refus runtime sanitisé (n'expose que le nom de classe d'exception).

### `src/skifer/mcp/auth.py` — deux fournisseurs de contexte (motif à réutiliser)
- `create_stdio_context_provider(*, subject, scopes, consumer_class, …) -> Callable[[Any], RequestContext]`
  : identité et scopes **statiques**, la requête est ignorée.
- `create_http_context_provider(verifier, *, audience, issuer, resource=None, required_scopes=frozenset(),
  clock=None, …) -> Callable[[Any], RequestContext]` : bearer validé par un **vérificateur injecté**,
  expiry sans tolérance, `certification_override` rejeté (`_validated_scopes`). Aucun détail du
  vérificateur ne franchit la frontière.
- `VerifiedBearerToken`, `BearerTokenVerifier(Protocol)` : shapes de claims. `CERTIFICATION_OVERRIDE_SCOPE
  = "certification_override"`.

### `src/skifer/cli.py` — style argparse du sous-parser `mcp` (à copier pour `api`)
```python
subparsers = parser.add_subparsers(dest="command")
mcp_parser = subparsers.add_parser("mcp", help="Run the optional read-only MCP server.")
mcp_subparsers = mcp_parser.add_subparsers(dest="mcp_command")
mcp_serve_parser = mcp_subparsers.add_parser("serve", help="Serve governed resources and semantic queries over MCP.")
mcp_serve_parser.add_argument("--transport", required=True, choices=("stdio","http"), help="…")
mcp_serve_parser.add_argument("--config", required=True, help="…")
# dispatch dans main() :
elif args.command == "mcp":
    _run_mcp(args)
```
`_run_mcp(args)` importe l'extra **dans** la fonction et sanitise l'erreur :
```python
def _run_mcp(args):
    if args.mcp_command != "serve":
        print("MCP command missing. Use 'skifer mcp --help'.", file=sys.stderr); sys.exit(1)
    try:
        from skifer.mcp.config import load_mcp_config
        config = load_mcp_config(args.config, transport=args.transport)
        from skifer.mcp.runtime import serve_mcp
        serve_mcp(config)
    except Exception as exc:
        from skifer.mcp.config import MCPConfigError
        from skifer.mcp.runtime import MCPStartupError
        from skifer.mcp.server import MCPDependencyError
        if isinstance(exc, (MCPConfigError, MCPDependencyError, MCPStartupError)):
            message = str(exc)
        else:
            message = f"MCP startup failed ({type(exc).__name__})."
        print(f"[mcp] {message}", file=sys.stderr); sys.exit(1)
```

### `services/` — contrat DTO / scope / erreur (features 31.1–31.5, consommé tel quel par `api/`)
Extrait de `agentic/data_service.py` vers `src/skifer/services/` (slice 31.1.1). L'API n'importe **que**
`skifer.services` :

- **Erreurs** (hiérarchie, `services/context.py`) : `AgentReadyDataError` (base) →
  `ScopeDenied`, `InvalidRequest` (→ `LimitExceeded`, `InvalidCursor`), `ResourceNotFound`,
  `ResourceUnavailable`, `SerializationError`.
- **`RequestContext(subject, scopes: frozenset[str], consumer_class, trace_context: TraceContext)`** —
  `frozen=True`, `__post_init__` fail-closed.
- **`require_scope(ctx: RequestContext, scope: str) -> None`** : lève `ScopeDenied(f"Scope '{scope}' is
  required.")` si `ctx` n'est pas un `RequestContext` **ou** si le scope est absent. **Le message contient
  le scope manquant** — c'est ce que l'API expose en 403.
- **Scopes nommés** (`services/context.py`) :
  `SCOPE_MODELS_READ="models:read"`, `SCOPE_CONTRACTS_READ="contracts:read"`,
  `SCOPE_LINEAGE_READ="lineage:read"`, `SCOPE_QUERY_EXECUTE="query:execute"`,
  `SCOPE_PROJECT_READ="project:read"`, `SCOPE_PIPELINES_WRITE="pipelines:write"`,
  `SCOPE_RULES_WRITE="rules:write"`, `SCOPE_EXECUTE_RUN="execute:run"`,
  `SCOPE_CONTRACTS_WRITE="contracts:write"`, `SCOPE_INCIDENTS_WRITE="incidents:write"`.
  `NAMED_SCOPES: frozenset[str]` = l'union exacte (10 scopes) ; `CERTIFICATION_OVERRIDE_SCOPE` **absent**.
- **View DTOs** allowlistés `to_dict()` : `ModelSummary`, `GovernedModelView`, `ContractView`,
  `CertificationView`, `LineageView`, `Page[T]`, `QueryEnvelope` (+ `LocalizedError`, `PipelineView`,
  `ProjectView`, `ScanReport`, `RuleView`, `QuarantineView`, `QualityReportView`, `SyncOutcome`,
  `SessionView`, `ResultView` selon les slices 31.1–31.5). **Chaque service renvoie un objet à `to_dict()`
  JSON-native** — l'API ne fait que `return service_method(ctx, …).to_dict()`.
- **Services** (constructeurs / méthodes gated par `require_scope`) :
  - `ProjectService(project_dir)` : `open(ctx)→ProjectView` (`project:read`), `get_pipeline(ctx,path)→PipelineView`,
    `json_schema(ctx)`, `op_catalog(ctx)`, `describe(ctx,path)`, `project_output(ctx,path)`,
    `lineage(ctx,path)`, `explain_rules(ctx,path)→dict` (`project:read`),
    `write_pipeline(ctx,path,text)→str` (`pipelines:write`).
  - `RuleService(project_dir, extra_paths=())` : `scan(ctx,paths=())→ScanReport`, `list(ctx)`,
    `dependency_graph(ctx,names)`, `generate_snippet(ctx,spec)` (`project:read`),
    `write_rule(ctx,file,code)→str` (`rules:write`).
  - `SemanticService(data_service, *, models_dir="semantic_models")` : `check(ctx,pipeline_path)→SyncOutcome`
    (`models:read`, codes 0/2/3), `write_draft(ctx,…)` / `promote(ctx,…)` (`pipelines:write`),
    `list_models(ctx,cursor=None,limit=50)` / `get_model(ctx,key)` (`models:read`). `data_service` =
    `AgentReadyDataService` (porte aussi `query(ctx, query, limit)→QueryEnvelope` sous `query:execute` et
    `get_lineage(ctx,dataset,column)→LineageView` sous `lineage:read`).
  - `GovernanceService(certification_store, *, max_rows=HARD_MAX_PAGE_SIZE)` :
    `get_contract(ctx,id,version)→ContractView`, `list_certification_history(ctx,dataset,limit=50)`,
    `get_certification(ctx,dataset)→CertificationView`, `read_quarantine(ctx,dataset,limit=50)→QuarantineView`
    (tout `contracts:read`).
  - `QualityService(history_store, *, contract_extractor=None)` : `checks_from_schema(ctx,schema_dict)`,
    `history(ctx,table,limit=20)`, `last_report(ctx,table)` (`contracts:read`) ; **transitions incidents**
    (31.4.3) `ack/assign/resolve` sous `incidents:write`, `list` incidents sous `contracts:read`.
  - `AgentService(hub, *, builder_agent=None)` : `ask(ctx,question,profile=None)→dict` (`query:execute`),
    `build(ctx,description,output_dir="schemas")→dict` (`pipelines:write`). Aucun DataFrame en sortie.
  - `ExecutionService` (31.5) : `session.connect(config_path, force_env)→SessionView` (`execute:run`) ;
    `submit(kind, path, params)→job_id`, `status(job_id)`, `cancel(job_id)`, `logs(job_id, after)`,
    `result(job_id)→ResultView` (`execute:run`).
  - `LocalIdentity` (`services/identity.py`, slice 31.1.6) : `LOCAL_DEFAULT_SCOPES = NAMED_SCOPES`,
    `LocalIdentity.resolve()`, `to_request_context(*, tracer=None)→RequestContext`,
    `local_request_context(*, tracer=None)→RequestContext`. **Jamais** `certification_override`.

> Les services 31.1–31.5 **enveloppent** `SkiferEngine`/`SemanticEngine`/stores : c'est la seule couche
> autorisée à les importer. `api/` n'importe **que** `skifer.services`.

### Convention de tests
Un fichier par module `tests/test_<module>.py`. LLM toujours mocké. Fixture `spark` (`tests/conftest.py`)
requise **uniquement** pour les tests qui construisent un `F.col(...)` ou exécutent Spark. Les tests d'API
utilisent `fastapi.testclient.TestClient` (**pas de serveur vivant**) et **doivent rester Spark-free**.
Motif de skip d'un extra optionnel déjà en place : `pytest.importorskip("mcp", reason="…")`
(`tests/test_mcp_cli.py:451`). L'API mirroir : `pytest.importorskip("fastapi")`.

---

## Slice 31.6.1 — Package `api/` : app FastAPI paresseuse, routes 1:1, erreurs, scopes, CORS loopback

**Objectif** : créer `src/skifer/api/` — une app FastAPI **importée paresseusement** (extra `[api]`), des
routes **1:1 sur les services**, un handler d'erreurs `{code, message, path}` mappant la hiérarchie
`AgentReadyDataError` sur des statuts HTTP, un **scope vérifié sur chaque route** (dépendance `Scope`),
un CORS **localhost-only**. `api/` **n'importe aucun moteur ni store** — uniquement `skifer.services`.

### Fichiers
- `pyproject.toml` — **modification** : ajouter l'extra `[api]` (copie du style `mcp`/`tracing`). **Ne pas
  toucher `version`.**
- `src/skifer/api/__init__.py` — **création** : docstring + `__all__` ; **aucun import FastAPI au niveau
  module** ; expose `create_app` via import différé (`from skifer.api.app import create_app` est sûr car
  `app.py` n'importe FastAPI qu'à l'intérieur de `create_app`).
- `src/skifer/api/app.py` — **création** : `create_app(...)`, `APIDependencyError`, `API_EXTRA`,
  constantes (`API_TITLE`, `API_SCHEMA_VERSION`, `HEALTH_BODY`, `CORS_ORIGIN_REGEX`, `LOOPBACK_HOSTS`),
  enregistrement des handlers d'erreur + route `/health` + inclusion des routers.
- `src/skifer/api/security.py` — **création** : dépendance `Scope` (extraction du contexte → `require_scope`).
- `src/skifer/api/errors.py` — **création** : `error_payload(exc, path)` + `register_error_handlers(app)`.
- `src/skifer/services/container.py` — **création** (dans la couche **autorisée** à importer les moteurs) :
  `ServiceContainer` (dataclass des services) + `build_services(project_dir) -> ServiceContainer`.
  *(Ajout hors liste littérale de la feature map : indispensable pour garder tout import moteur/store
  HORS de `api/` — voir « Risques ».)*
- `src/skifer/api/routes/__init__.py` — **création** : `all_routers() -> list[Callable[[ServiceContainer], APIRouter]]`.
- `src/skifer/api/routes/*.py` — **création**, un module par groupe de routes :
  `project.py` (`/project`, `/config`), `pipelines.py` (`/pipelines`), `rules.py` (`/rules`),
  `catalog.py` (`/catalog`), `semantic.py` (`/semantic`), `lineage.py` (`/lineage`),
  `dictionary.py` (`/dictionary`), `quality.py` (`/quality`), `incidents.py` (`/incidents`),
  `contracts.py` (`/contracts`), `certifications.py` (`/certifications`),
  `data_products.py` (`/data-products`), `agents.py` (`/agents`), `identity.py` (`/me`),
  `session.py` (`/session`), `jobs.py` (`/jobs`).

### Signatures Python
```python
# src/skifer/api/app.py
from __future__ import annotations
from typing import Any, Callable

API_TITLE = "Skifer local API"
# Version DU CONTRAT OpenAPI, découplée de pyproject.version (jamais bumpée ici) : garantit un
# snapshot OpenAPI stable release après release. À incrémenter délibérément si le contrat change.
API_SCHEMA_VERSION = "0"
API_EXTRA = 'pip install -e ".[api]"'
HEALTH_BODY: dict[str, str] = {"status": "ok"}          # constante figée, sans donnée métier
# CORS localhost-only : http://localhost[:port] et http://127.0.0.1[:port] uniquement.
CORS_ORIGIN_REGEX = r"^http://(localhost|127\.0\.0\.1)(:\d+)?$"
LOOPBACK_HOSTS = frozenset({"127.0.0.1", "::1", "localhost"})


class APIDependencyError(RuntimeError):
    """The optional [api] extra (fastapi, uvicorn) is not installed."""


def create_app(
    project_dir: str,
    *,
    context_provider: Callable[[Any], "RequestContext"] | None = None,
    services: "ServiceContainer" | None = None,
):
    """Build the FastAPI app. FastAPI is imported HERE, never at module import time."""
    try:
        import fastapi  # noqa: F401  (import paresseux : le cœur s'importe sans l'extra)
    except ImportError:
        raise APIDependencyError(
            f"The local API requires the optional dependencies: `{API_EXTRA}`."
        ) from None
    from fastapi import FastAPI
    from fastapi.middleware.cors import CORSMiddleware
    from skifer.services.container import build_services
    from skifer.services.identity import local_request_context
    from skifer.api.errors import register_error_handlers
    from skifer.api.routes import all_routers

    container = services if services is not None else build_services(project_dir)
    app = FastAPI(title=API_TITLE, version=API_SCHEMA_VERSION)
    app.add_middleware(
        CORSMiddleware,
        allow_origin_regex=CORS_ORIGIN_REGEX,   # localhost-only, jamais "*"
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.state.services = container
    # §7.4 : par défaut, identité LOCALE (tous les scopes nommés sauf certification_override).
    # Un vérificateur bearer injecté peut remplacer ce provider (même frontière que mcp/auth).
    app.state.context_provider = context_provider or (lambda _request: local_request_context())

    register_error_handlers(app)

    @app.get("/health", include_in_schema=True)   # NON scope-gated ; exclue du test "route sans scope"
    def health() -> dict[str, str]:
        return dict(HEALTH_BODY)                   # sans Spark, sans toucher ExecutionService

    for build in all_routers():
        app.include_router(build(container))
    return app
```

```python
# src/skifer/api/security.py
from __future__ import annotations
from typing import Any
from skifer.services import RequestContext, require_scope, NAMED_SCOPES


class Scope:
    """FastAPI dependency: resolve the request context and enforce ONE named scope.

    Every route depends on exactly one Scope(...). The architecture test walks the
    route dependency graph and refuses any route that carries no Scope.
    """

    def __init__(self, scope: str):
        if scope not in NAMED_SCOPES:                      # aucune route ne peut exiger un scope inconnu
            raise ValueError(f"Unknown API scope: {scope!r}")
        self.scope = scope

    def __call__(self, request: Any) -> RequestContext:
        ctx = request.app.state.context_provider(request)  # LocalIdentity par défaut, ou bearer injecté
        require_scope(ctx, self.scope)                      # ScopeDenied -> 403 via error handler
        return ctx
```

```python
# src/skifer/api/errors.py  — mappe la hiérarchie de services sur {code, message, path} + statut HTTP
from __future__ import annotations
from typing import Any
from skifer.services import (
    AgentReadyDataError, ScopeDenied, InvalidRequest, LimitExceeded, InvalidCursor,
    ResourceNotFound, ResourceUnavailable, SerializationError,
)

# Ordre = sous-classes AVANT bases (isinstance s'arrête au premier match).
_ERROR_TABLE: tuple[tuple[type, int, str], ...] = (
    (ScopeDenied,         403, "scope_denied"),      # message = "Scope '<x>' is required." (scope manquant)
    (LimitExceeded,       400, "limit_exceeded"),
    (InvalidCursor,       400, "invalid_cursor"),
    (InvalidRequest,      400, "invalid_request"),
    (ResourceNotFound,    404, "not_found"),
    (ResourceUnavailable, 503, "unavailable"),
    (SerializationError,  500, "serialization_error"),
    (AgentReadyDataError, 500, "service_error"),
)


def error_payload(exc: Exception, path: str) -> tuple[int, dict[str, str]]:
    for cls, status, code in _ERROR_TABLE:
        if isinstance(exc, cls):
            # str(exc) est sûr : les services ne mettent jamais de valeur de donnée dans un message.
            return status, {"code": code, "message": str(exc), "path": path}
    # Fallback : ne jamais divulguer un message d'exception inconnue (peut citer un chemin/secret).
    return 500, {"code": "internal_error", "message": type(exc).__name__, "path": path}


def register_error_handlers(app: Any) -> None:
    from fastapi.responses import JSONResponse

    async def _handle(request: Any, exc: Exception) -> Any:
        status, body = error_payload(exc, request.url.path)     # path = chemin HTTP de la requête
        return JSONResponse(status_code=status, content=body)

    app.add_exception_handler(AgentReadyDataError, _handle)     # couvre toute la hiérarchie
```

```python
# src/skifer/services/container.py  (couche services : AUTORISÉE à importer moteurs/stores)
from __future__ import annotations
from dataclasses import dataclass


@dataclass(frozen=True)
class ServiceContainer:
    project: "ProjectService"
    rules: "RuleService"
    semantic: "SemanticService"
    governance: "GovernanceService"
    quality: "QualityService"
    agents: "AgentService"
    execution: "ExecutionService"
    data_service: "AgentReadyDataService"


def build_services(project_dir: str) -> ServiceContainer:
    """Wire every service from a project directory.

    Reads config, builds the SemanticEngine / certification store / history store / hub
    (Spark-free where possible), and hands each service its backend. This is the ONLY
    place engine/store construction happens for the API — api/ stays engine-free.
    ExecutionService connects lazily (its state is exposed, not raised) so /health and the
    read-only routes never require Spark.
    """
    ...  # constructeurs exacts fournis par 31.1–31.5 (ProjectService(project_dir), etc.)
```

```python
# src/skifer/api/routes/__init__.py
from __future__ import annotations
from typing import Callable
from skifer.services.container import ServiceContainer


def all_routers() -> list[Callable[[ServiceContainer], "APIRouter"]]:
    # Importés ici (dans une fonction) : FastAPI n'est touché qu'après create_app().
    from skifer.api.routes import (
        project, pipelines, rules, catalog, semantic, lineage, dictionary,
        quality, incidents, contracts, certifications, data_products,
        agents, identity, session, jobs,
    )
    return [
        project.build, pipelines.build, rules.build, catalog.build, semantic.build,
        lineage.build, dictionary.build, quality.build, incidents.build,
        contracts.build, certifications.build, data_products.build,
        agents.build, identity.build, session.build, jobs.build,
    ]
```

```python
# src/skifer/api/routes/project.py  — MODULE DE ROUTE REPRÉSENTATIF (motif 1:1 thin)
from __future__ import annotations
from skifer.services.container import ServiceContainer


def build(container: ServiceContainer) -> "APIRouter":
    from fastapi import APIRouter, Depends, Body
    from skifer.api.security import Scope
    from skifer.services import RequestContext, SCOPE_PROJECT_READ, SCOPE_PIPELINES_WRITE

    router = APIRouter(tags=["project"])
    svc = container.project

    @router.get("/project")                     # scope extrait -> service -> view.to_dict()
    def open_project(ctx: RequestContext = Depends(Scope(SCOPE_PROJECT_READ))) -> dict:
        return svc.open(ctx).to_dict()

    @router.get("/config")
    def config(ctx: RequestContext = Depends(Scope(SCOPE_PROJECT_READ))) -> dict:
        return svc.open(ctx).to_dict()["config"]

    @router.get("/project/json-schema")
    def json_schema(ctx: RequestContext = Depends(Scope(SCOPE_PROJECT_READ))) -> dict:
        return svc.json_schema(ctx)

    @router.get("/project/op-catalog")
    def op_catalog(ctx: RequestContext = Depends(Scope(SCOPE_PROJECT_READ))) -> dict:
        return svc.op_catalog(ctx)

    return router


# src/skifer/api/routes/pipelines.py  (même motif ; illustre lecture project:read + écriture pipelines:write)
def build(container: ServiceContainer) -> "APIRouter":
    from fastapi import APIRouter, Depends, Body
    from skifer.api.security import Scope
    from skifer.services import RequestContext, SCOPE_PROJECT_READ, SCOPE_PIPELINES_WRITE
    router = APIRouter(tags=["pipelines"])
    svc = container.project

    @router.get("/pipelines/{path:path}")
    def get_pipeline(path: str, ctx: RequestContext = Depends(Scope(SCOPE_PROJECT_READ))) -> dict:
        return svc.get_pipeline(ctx, path).to_dict()      # PipelineView : YAML invalide -> errors[], jamais 500

    @router.get("/pipelines/{path:path}/describe")
    def describe(path: str, ctx: RequestContext = Depends(Scope(SCOPE_PROJECT_READ))) -> dict:
        return svc.describe(ctx, path)

    @router.get("/pipelines/{path:path}/output")
    def project_output(path: str, ctx: RequestContext = Depends(Scope(SCOPE_PROJECT_READ))) -> dict:
        return svc.project_output(ctx, path)

    @router.get("/pipelines/{path:path}/explain-rules")
    def explain_rules(path: str, ctx: RequestContext = Depends(Scope(SCOPE_PROJECT_READ))) -> dict:
        return svc.explain_rules(ctx, path)

    @router.put("/pipelines/{path:path}")
    def write_pipeline(path: str, ctx: RequestContext = Depends(Scope(SCOPE_PIPELINES_WRITE)),
                       text: str = Body(..., embed=True)) -> dict:
        return {"path": svc.write_pipeline(ctx, path, text)}

    return router
```

### Table des routes → service → scope (16 modules, prefixes de la feature map)
| Module | Route(s) | Service.method | Scope |
|---|---|---|---|
| `project.py` | `GET /project`, `GET /config`, `GET /project/json-schema`, `GET /project/op-catalog` | `ProjectService.open/json_schema/op_catalog` | `project:read` |
| `pipelines.py` | `GET /pipelines/{path}` (+`/describe`,`/output`,`/explain-rules`) | `ProjectService.get_pipeline/describe/project_output/explain_rules` | `project:read` |
| `pipelines.py` | `PUT /pipelines/{path}` | `ProjectService.write_pipeline` | `pipelines:write` |
| `rules.py` | `GET /rules`, `GET /rules/graph`, `POST /rules/snippet` | `RuleService.scan/list/dependency_graph/generate_snippet` | `project:read` |
| `rules.py` | `PUT /rules/{file}` | `RuleService.write_rule` | `rules:write` |
| `catalog.py` | `GET /catalog`, `GET /catalog/{key}` | `SemanticService.list_models/get_model` | `models:read` |
| `semantic.py` | `POST /semantic/query` | `data_service.query` | `query:execute` |
| `semantic.py` | `POST /semantic/check` | `SemanticService.check` | `models:read` |
| `semantic.py` | `POST /semantic/write-draft`, `POST /semantic/promote` | `SemanticService.write_draft/promote` | `pipelines:write` |
| `lineage.py` | `GET /lineage/{dataset}/{column}` | `data_service.get_lineage` | `lineage:read` |
| `dictionary.py` | `GET /dictionary/{dataset}` | dictionary accessor (voir divergence) | `lineage:read` |
| `quality.py` | `POST /quality/checks`, `GET /quality/history/{table}`, `GET /quality/last/{table}` | `QualityService.checks_from_schema/history/last_report` | `contracts:read` |
| `incidents.py` | `GET /incidents` | `QualityService.list_incidents` | `contracts:read` |
| `incidents.py` | `POST /incidents/{id}/ack`, `/assign`, `/resolve` | `QualityService.ack/assign/resolve` | `incidents:write` |
| `contracts.py` | `GET /contracts/{id}/{version}`, `GET /contracts/{id}` | `GovernanceService.get_contract/versions` | `contracts:read` |
| `certifications.py` | `GET /certifications/{dataset}`, `GET /certifications/{dataset}/history` | `GovernanceService.get_certification/list_certification_history` | `contracts:read` |
| `data_products.py` | `GET /data-products`, `GET /data-products/{id}` | `GovernanceService` data products | `contracts:read` |
| `agents.py` | `POST /agents/ask` | `AgentService.ask` | `query:execute` |
| `agents.py` | `POST /agents/build` | `AgentService.build` | `pipelines:write` |
| `identity.py` | `GET /me` | `LocalIdentity` (contexte courant : subject/scopes/consumer_class) | `project:read` |
| `session.py` | `POST /session/connect`, `GET /session` | `ExecutionService.session.connect/status` | `execute:run` |
| `jobs.py` | `POST /jobs`, `GET /jobs/{id}`, `POST /jobs/{id}/cancel`, `GET /jobs/{id}/logs`, `GET /jobs/{id}/result` | `ExecutionService.submit/status/cancel/logs/result` | `execute:run` |

### Comportement & règles
- **Import paresseux (extra `[api]`)** : ni `api/__init__.py` ni le **niveau module** d'`api/app.py`
  n'importent FastAPI. `import skifer.api` et `from skifer.api.app import create_app` fonctionnent **sans**
  l'extra. Seul l'**appel** `create_app(...)` importe FastAPI ; en son absence → `APIDependencyError`
  citant `pip install -e ".[api]"`. Les `routes/*.py` importent FastAPI **dans** `build(...)` (donc jamais
  au chargement du package). Miroir exact du motif `mcp/server.create_server`.
- **Un scope par route** : chaque endpoint dépend d'exactement un `Scope(<SCOPE>)`. `Scope.__call__`
  résout le contexte puis `require_scope` → un scope absent lève `ScopeDenied` **avant** tout appel
  service. `/health` est la **seule** route non gated (exclue du test d'architecture, avec `/openapi.json`,
  `/docs`, `/redoc`).
- **403 avec le scope manquant** : `ScopeDenied` → 403 `{code:"scope_denied", message:"Scope '<x>' is
  required.", path:"<chemin>"}`. Le nom du scope manquant est **dans `message`** (le format `{code,
  message, path}` est respecté sans champ supplémentaire).
- **Erreurs uniformes `{code, message, path}`** : un seul handler sur `AgentReadyDataError` couvre toute
  la hiérarchie ; `path` = `request.url.path`. Un `PipelineView` avec YAML invalide **ne lève pas** — ses
  `LocalizedError` (`{code, message, path}` au sens YAML) sortent dans le corps `200` via `to_dict()`
  (deux notions de « path » cohabitent : chemin HTTP côté handler, localisation YAML côté PipelineView —
  documenté).
- **Loopback / CORS** : le CORS n'autorise que `http://localhost[:port]` et `http://127.0.0.1[:port]`
  (regex, jamais `"*"`). Le bind loopback est garanti côté CLI (slice 6.2, host figé `127.0.0.1`).
- **Aucune logique métier, aucun import moteur** : les routes font `service.method(ctx, …).to_dict()` (ou
  renvoient le dict déjà produit par le service). Toute construction d'`SemanticEngine`/store/hub est dans
  `services/container.py` (couche autorisée). `api/` n'importe que `skifer.services` + FastAPI + stdlib.
- **Aucun DataFrame ne franchit `api/`** : garanti en amont par les services (vues JSON-native) ; l'API ne
  fait que sérialiser des dicts.

### Cas de test — `tests/test_api_app.py`, `tests/test_api_routes.py`, `tests/test_api_architecture.py`
Tous en tête : `fastapi = pytest.importorskip("fastapi")` puis
`from fastapi.testclient import TestClient`. Spark-free : le projet est un `tmp_path` minimal (`config.yaml`
+ `schemas/`), `ExecutionService` reste non connecté, aucun `F.col(...)`.

- `test_import_skifer_api_without_fastapi_is_safe` : `import importlib; importlib.import_module("skifer.api")`
  réussit ; `from skifer.api.app import create_app` réussit (aucun `fastapi` requis au niveau module). *(Ce
  test **ne** skippe pas — il prouve l'innocuité de l'import sans l'extra.)*
- `test_create_app_without_extra_raises_dependency_error` : en simulant l'absence de FastAPI (monkeypatch
  `builtins.__import__` ou `sys.modules["fastapi"]=None`), `create_app(tmp)` lève `APIDependencyError` dont
  le message contient `pip install -e ".[api]"`.
- `test_every_route_exercised_returns_json` : pour chaque route du contrat (table ci-dessus), un appel
  `TestClient` avec l'identité locale par défaut (tous scopes) répond en JSON `{code,message,path}` en
  erreur ou une vue en succès — **jamais** une stacktrace 500 non mappée. *(Acceptance : « TestClient
  exercises every route ».)*
- `test_refused_scope_returns_403_with_missing_scope` : `create_app(tmp, context_provider=lambda r:
  RequestContext(subject="u", scopes=frozenset({"project:read"}), consumer_class="test",
  trace_context=<noop>))` ; `PUT /pipelines/foo.yaml` (exige `pipelines:write`) → **403**, corps
  `{"code":"scope_denied", "message":"Scope 'pipelines:write' is required.", "path":"/pipelines/foo.yaml"}`.
- `test_scope_denied_maps_to_403`, `test_not_found_maps_to_404`, `test_invalid_request_maps_to_400`,
  `test_resource_unavailable_maps_to_503` : `error_payload(exc, "/x")` renvoie le bon `(status, code)` pour
  chaque classe (test unitaire de la table, sans TestClient).
- `test_error_payload_shape_is_exactly_code_message_path` : `set(body) == {"code","message","path"}`.
- `test_cors_allows_localhost_only` : requête préflight `OPTIONS` avec `Origin: http://localhost:5173` →
  en-tête `access-control-allow-origin` présent ; avec `Origin: http://evil.example` → **absent**.
- `test_pipeline_invalid_yaml_returns_200_with_localized_errors` : `GET /pipelines/bad.yaml` (YAML avec
  `region:eqals:EMEA`) → 200, `body["errors"]` non vide avec `code`/`message`/`path`, `body["normalized"]
  is None` (le service ne lève pas).
- **Architecture** — `tests/test_api_architecture.py` (**Spark-free, sans FastAPI requis pour le grep**) :
  - `test_no_route_without_scope` : `app = create_app(tmp)` ; pour chaque `route` de `app.routes` de type
    `APIRoute` dont `route.path not in {"/health","/openapi.json","/docs","/redoc","/docs/oauth2-redirect"}`,
    parcourir récursivement `route.dependant` (`.dependencies`) et asserter qu'**au moins une** dépendance
    a `.call` instance de `skifer.api.security.Scope` avec `.scope in NAMED_SCOPES`.
  - `test_api_never_imports_engine_or_store` : pour chaque fichier `.py` sous `src/skifer/api/`, lire la
    source et asserter l'**absence** des motifs interdits : `SkiferEngine`, `SemanticEngine`,
    `spark_backend`, `SparkBackend`, `from skifer.core`, `import skifer.core`,
    `from skifer.observability`, `certification_store`, `history import`, `import pyspark`, `spark_factory`.
    Seuls autorisés : `skifer.services`, `fastapi`, `starlette`, `uvicorn`, stdlib.
  - `test_api_imports_only_services_from_skifer` : chaque `from skifer.X import …` sous `api/` a
    `X == "services"` ou `X.startswith("services.")` ou `X == "api"`/`X.startswith("api.")`.

### Commit
```
feat(plan31-6.1): local FastAPI app over services/ — 1:1 scoped routes, {code,message,path} errors, loopback CORS
```
CHANGELOG `[Unreleased]` (créer la section) :
```
## [Unreleased]

### Added

- **Plan 31 (6.1)** — Nouveau package `api/` (extra optionnel `[api]` : `fastapi`, `uvicorn`, imports
  strictement paresseux — le cœur s'importe sans l'extra). App FastAPI fine sur `services/` : routes 1:1
  (`/project`, `/config`, `/pipelines`, `/rules`, `/catalog`, `/semantic`, `/lineage`, `/dictionary`,
  `/quality`, `/incidents`, `/contracts`, `/certifications`, `/data-products`, `/agents`, `/me`,
  `/session`, `/jobs`), scope vérifié sur chaque route (dépendance `Scope`), erreurs uniformes
  `{code, message, path}` mappées sur la hiérarchie `AgentReadyDataError`, CORS localhost-only. Aucun
  moteur ni store importé dans `api/` (test d'architecture). Wiring des services dans
  `services/container.py`.
```

### DoD
- [ ] `import skifer.api` et `from skifer.api.app import create_app` OK **sans** `[api]`.
- [ ] `create_app` sans FastAPI → `APIDependencyError` citant l'extra.
- [ ] Les 16 modules de routes présents ; chaque route porte exactement un `Scope`.
- [ ] `TestClient` exerce chaque route ; scope refusé → 403 avec le scope manquant dans `message`.
- [ ] `test_api_architecture` vert : aucune route sans scope, aucun import moteur/store dans `api/`.
- [ ] CORS localhost-only prouvé ; `ruff check src/` propre ; extra `[api]` ajouté (version inchangée).

---

## Slice 31.6.2 — CLI `skifer api serve|openapi`, healthcheck sans Spark, OpenAPI snapshotée (+ finalisation `mcp serve`, §7.5)

**Objectif** : exposer `skifer api serve --project DIR [--port]` (bind **loopback** figé) et
`skifer api openapi` (dump JSON déterministe sur stdout) ; un **healthcheck sans Spark** ; un **snapshot
OpenAPI stable** en test ; et **finaliser le packaging `skifer mcp serve`** (Plan 29 slice 7.5, livrée ici).

### Fichiers
- `src/skifer/cli.py` — **modification** : sous-parser `api` (`serve`, `openapi`) sur le modèle du
  sous-parser `mcp` ; `_run_api(args)` (imports paresseux + sanitisation d'erreur, miroir `_run_mcp`) ;
  dispatch `elif args.command == "api": _run_api(args)`.
- `src/skifer/api/app.py` — **modification (mineure)** : `openapi_document(project_dir) -> dict` helper
  déterministe (construit l'app, renvoie `app.openapi()`), réutilisé par la CLI et le test snapshot.
- `pyproject.toml` — **déjà** modifié en 6.1 (extra `[api]`). Vérifier `[mcp]` présent (packaging §7.5).
- `tests/data/openapi_snapshot.json` — **création** : golden file du contrat OpenAPI (voir test).

### Signatures Python
```python
# src/skifer/api/app.py  (ajout)
def openapi_document(project_dir: str) -> dict:
    """Deterministic OpenAPI schema for `skifer api openapi` and the snapshot test.

    info.version is API_SCHEMA_VERSION (a fixed contract version), NOT the package
    version, so a release never perturbs the snapshot.
    """
    return create_app(project_dir).openapi()
```

```python
# src/skifer/cli.py  (ajout)
API_LOOPBACK_HOST = "127.0.0.1"   # bind figé : pas de flag --host, un bind non-loopback est impossible

# --- dans main(), après le sous-parser mcp ---
api_parser = subparsers.add_parser("api", help="Run the optional local HTTP API.")
api_subparsers = api_parser.add_subparsers(dest="api_command")
api_serve_parser = api_subparsers.add_parser("serve", help="Serve services/ over a loopback HTTP API.")
api_serve_parser.add_argument("--project", required=True, metavar="DIR", help="Skifer project directory.")
api_serve_parser.add_argument("--port", type=int, default=8000, help="Loopback port (default: 8000).")
api_openapi_parser = api_subparsers.add_parser("openapi", help="Print the OpenAPI schema (JSON) to stdout.")
api_openapi_parser.add_argument("--project", default=".", metavar="DIR", help="Project directory (default: cwd).")

# --- dispatch ---
elif args.command == "api":
    _run_api(args)


def _run_api(args) -> None:
    """Load and serve the optional API without importing FastAPI for other commands."""
    command = getattr(args, "api_command", None)
    if command == "openapi":
        _run_api_openapi(args)
        return
    if command != "serve":
        print("API command missing. Use 'skifer api --help'.", file=sys.stderr)
        sys.exit(1)
    try:
        from skifer.api.app import create_app
        app = create_app(args.project)          # loopback garanti : host figé ci-dessous
        import uvicorn
        uvicorn.run(app, host=API_LOOPBACK_HOST, port=args.port, log_config=None)
    except Exception as exc:
        from skifer.api.app import APIDependencyError
        message = str(exc) if isinstance(exc, APIDependencyError) else f"API startup failed ({type(exc).__name__})."
        print(f"[api] {message}", file=sys.stderr)
        sys.exit(1)


def _run_api_openapi(args) -> None:
    try:
        from skifer.api.app import openapi_document
        print(json.dumps(openapi_document(args.project), sort_keys=True, indent=2, ensure_ascii=False))
    except Exception as exc:
        from skifer.api.app import APIDependencyError
        message = str(exc) if isinstance(exc, APIDependencyError) else f"OpenAPI export failed ({type(exc).__name__})."
        print(f"[api] {message}", file=sys.stderr)
        sys.exit(1)
```

### Comportement & règles
- **Bind loopback figé** : `api serve` n'a **pas** de `--host` ; `uvicorn.run(host="127.0.0.1", …)`. Un
  bind non-loopback est donc impossible par construction (miroir de l'esprit `mcp/config._is_loopback`,
  mais ici c'est structurel — documenté). `--port` seul est configurable.
- **Imports paresseux dans la CLI** : `create_app` / `openapi_document` / `uvicorn` importés **dans**
  `_run_api`/`_run_api_openapi`, jamais au niveau module — les autres sous-commandes (`validate`, `hub`,
  `semantic`, `mcp`, `adaptive`) n'exigent pas `[api]`. Sanitisation d'erreur identique à `_run_mcp`
  (seul `APIDependencyError` affiche son message ; sinon nom de classe uniquement).
- **Healthcheck sans Spark** : `GET /health` (défini en 6.1) renvoie la constante figée `{"status":"ok"}`
  sans toucher `ExecutionService`/Spark. Le test l'exerce sur un `create_app(tmp)` d'un projet **sans**
  session Spark.
- **OpenAPI stable** : `info.version = API_SCHEMA_VERSION = "0"` (découplé de `pyproject.version`) et
  `info.title = API_TITLE` fixes → le snapshot ne bouge pas à chaque release. Le dump CLI est trié
  (`sort_keys=True`). Toute évolution du contrat (route/scope) casse volontairement le snapshot et exige
  sa régénération dans la même PR.
- **§7.5 packaging MCP** : `skifer mcp serve` existe déjà (Plan 29) ; 6.2 **finalise** = (a) confirmer
  l'extra `[mcp]` déclaré, (b) test fumée `skifer mcp serve --help` (exit 0, pas d'`ImportError`), (c)
  entrée CHANGELOG notant la disponibilité packagée. Aucun changement de comportement MCP.

### Cas de test — `tests/test_api_cli.py`, `tests/test_api_openapi.py`
- `test_health_without_spark` : `client.get("/health")` → 200, `{"status":"ok"}`, sur un `create_app(tmp)`
  sans session Spark (aucune fixture `spark`).
- `test_openapi_snapshot_matches` : `doc = openapi_document(tmp)` ; **contrat normalisé** =
  `sorted((path, method.upper()) for path, item in doc["paths"].items() for method in item)` +
  l'ensemble trié des `paths` ; comparé à `tests/data/openapi_snapshot.json` **byte-for-byte** après
  `json.dumps(sort_keys=True, indent=2)`. *(Le snapshot stocke le contrat normalisé — routes + méthodes +
  `info.version` — robuste aux versions patch de FastAPI ; un plein `app.openapi()` serait fragile entre
  versions, cf. Risques.)*
- `test_openapi_info_version_is_contract_version` : `doc["info"]["version"] == "0"` (jamais la version
  pyproject) — garantit la stabilité release après release.
- `test_openapi_every_path_has_declared_scope` : croiser `doc["paths"]` avec la table de scopes de 6.1 —
  chaque path (hors `/health`) apparaît avec son scope attendu (le snapshot peut inclure une colonne
  `x-skifer-scope` posée via `openapi_extra`/`Scope`, sinon dériver du test d'architecture 6.1).
- `test_api_serve_help_exits_zero` : `subprocess`/`main(["api","serve","--help"])` → sortie contenant
  `--project` et `--port`, exit 0.
- `test_api_openapi_command_prints_json` : `main(["api","openapi","--project",tmp])` (ou `_run_api_openapi`)
  imprime un JSON parsable dont `["openapi"]` commence par `"3."` et `["info"]["version"] == "0"`.
- `test_api_command_without_extra_reports_dependency` : FastAPI simulé absent → `[api]` message d'erreur
  citant `pip install -e ".[api]"`, exit 1 (miroir du comportement `[mcp]`).
- `test_mcp_serve_help_still_works` (§7.5) : `main(["mcp","serve","--help"])` exit 0 ; `"--transport"` et
  `"--config"` présents ; pas d'`ImportError` (finalisation packaging).
- Tous ces tests : `pytest.importorskip("fastapi")` en tête (sauf le fumée `--help` argparse pur si
  possible). Spark-free.

### Commit
```
feat(plan31-6.2): skifer api serve|openapi CLI, Spark-free health, stable OpenAPI snapshot; finalize mcp serve packaging
```
CHANGELOG `[Unreleased]` :
```
- **Plan 31 (6.2)** — CLI `skifer api serve --project DIR [--port]` (bind loopback figé, imports
  paresseux) et `skifer api openapi` (dump OpenAPI JSON déterministe sur stdout). Healthcheck `/health`
  sans Spark. Snapshot OpenAPI stable (`info.version` = version de contrat `"0"`, découplée de la version
  du paquet) verrouillé par un test. Finalisation du packaging `skifer mcp serve` (Plan 29 slice 7.5,
  extra `[mcp]`).
```

### DoD
- [ ] `skifer api serve` bind `127.0.0.1` uniquement ; `--port` respecté ; erreurs sanitizées.
- [ ] `skifer api openapi` imprime un JSON trié ; `info.version == "0"`.
- [ ] `/health` répond 200 `{"status":"ok"}` **sans Spark**.
- [ ] Snapshot OpenAPI verrouillé et régénérable ; `info.version` découplée de pyproject.
- [ ] `skifer mcp serve --help` OK (packaging §7.5) ; extras `[api]`/`[mcp]` présents, version inchangée.

---

## Ordre & dépendances internes

- **Dépendances externes dures (§7)** :
  - **7.4 (auth déléguée + scopes, partage `services/identity.py`) doit être livrée AVANT 31.6.1.**
    Le `context_provider` par défaut de l'API repose sur `LocalIdentity`/`local_request_context`
    (slice 31.1.6) et, en HTTP délégué, sur le motif `create_http_context_provider` de 7.4. Sans 7.4/1.6,
    pas de contexte à injecter dans `Scope`.
  - **Tous les services 31.1–31.5** sont requis pour que **chaque** route de la table existe et soit
    exerçable par `TestClient` (acceptance « exercises every route ») :
    ProjectService/RuleService/SemanticService (31.1), GovernanceService/QualityService (31.1.4 + incidents
    31.4.3), AgentService (31.1.5), ExecutionService (31.5), registre/dictionnaire (31.2).
- **31.6.1 en API v0 dès que 31.1.2 et 31.1.3 existent** : on peut brancher un premier client sur
  `/project`, `/config`, `/pipelines`, `/rules`, `/catalog`, `/semantic` (lecture), `/lineage`, `/me` —
  les modules de routes des services non encore livrés (`session`, `jobs`, `incidents`, `data-products`,
  `dictionary`, `quality`) sont ajoutés au fil de leur arrivée. `all_routers()` n'enregistre que les
  routers dont le service est présent dans le `ServiceContainer` (v0), puis la liste se complète.
- **31.6.2 après 31.6.1** et après 31.5 (les routes `/session`,`/jobs` doivent exister pour figer
  l'OpenAPI complète). Le snapshot est régénéré à chaque ajout de route.
- **Ordre interne** : 6.1 (package + routes + erreurs + architecture) → 6.2 (CLI + health + OpenAPI +
  packaging MCP). Une slice = un commit.

---

## Risques & pièges

- **Modèle d'auth HTTP (vérificateur injecté vs identité locale)** : par défaut l'API est **loopback +
  identité locale** (tous les scopes nommés sauf `certification_override`) — donc **aucune** route n'est
  refusée en usage normal ; le test « scope refusé → 403 » **doit** injecter un `context_provider`
  restreint. Ne **jamais** peupler les scopes depuis une entrée client (query/body/header non signé) : ce
  serait une auto-attribution de scope. En mode HTTP délégué, réutiliser le motif
  `create_http_context_provider` (7.4/`mcp/auth`) — bearer validé par un vérificateur **injecté**, expiry
  sans tolérance, `certification_override` rejeté.
- **Stabilité du snapshot OpenAPI entre versions de Python / FastAPI** : un `app.openapi()` plein varie
  entre versions de FastAPI/pydantic (ordre de champs, mots-clés de schéma). **Snapshoter le contrat
  normalisé** (paths + méthodes + `info.version`, JSON trié), pas le dump brut ; borner FastAPI dans
  l'extra (`fastapi>=0.110`) et documenter qu'un bump de FastAPI qui change le contrat exige une
  régénération **revue** du golden file. `info.version` figée (`"0"`) découple le snapshot de la version
  du paquet (jamais bumpée ici).
- **Application effective du CORS/loopback** : `allow_origin_regex` (jamais `allow_origins=["*"]`) ; le
  test doit prouver qu'une origine non-localhost n'obtient **pas** l'en-tête `access-control-allow-origin`.
  Bind loopback structurel côté CLI (pas de `--host`) — documenter que si un `--host` était ajouté un jour,
  il faudrait un garde `_is_loopback` calqué sur `mcp/config`.
- **Sûreté de l'import optionnel** : le piège classique est un `import fastapi` au **niveau module** d'un
  `routes/*.py` ou d'`api/__init__.py` — il casse `import skifer` sur une install sans `[api]`. Règle
  stricte : FastAPI n'est importé **que** dans un corps de fonction (`create_app`, `build(...)`,
  `register_error_handlers`). Test dédié `test_import_skifer_api_without_fastapi_is_safe` (ne skippe pas).
- **Interdiction d'import moteur/store dans `api/`** (test d'architecture) : le wiring des services touche
  forcément `SemanticEngine`/stores/hub — il **doit** vivre dans `services/container.py` (couche
  autorisée), pas dans `api/`. C'est pourquoi `services/container.py` est ajouté hors liste littérale de la
  feature map. Le grep d'architecture porte sur `src/skifer/api/**` uniquement.
- **Deux sens de « path »** : dans `{code, message, path}` renvoyé par le **handler HTTP**, `path` = chemin
  de la requête ; dans un `LocalizedError`/`PipelineView` (corps 200), `path` = localisation YAML. Ne pas
  confondre ni fusionner : documenter, tester les deux.
- **Un pipeline invalide ne doit pas donner un 500** : `ProjectService.get_pipeline` renvoie un
  `PipelineView(errors=…, normalized=None)` — la route renvoie 200 + erreurs localisées, pas une
  exception. Ne pas ré-lever côté route.
- **Divergences feature map ↔ services réels** (à trancher à l'implémentation) :
  - `/dictionary` : aucune `DictionaryService` n'est spécifiée dans 31.1 ; la source est `lineage/dictionary.py`
    (`DataDictionary`) — l'exposer soit via un accessor sur `ProjectService`, soit via une petite façade
    `services/` livrée avec 31.2 (registre). Scope `lineage:read`. **À confirmer avec 31.2.**
  - `/audit` : présent dans « ProjectService→…,/audit » de la description mais **absent** de la liste
    verbatim de la slice 6.1 — il arrive avec 31.7 (`ProjectService.audit()`), ajouté comme route
    ultérieure ; ne pas le créer en 6.1.
  - `/incidents` : les transitions viennent de 31.4.3 (`QualityService` sous `incidents:write`) ; il n'y a
    pas de scope `incidents:read` nommé → la lecture (`GET /incidents`) est gated `contracts:read`.
  - `/session`,`/jobs` : `ExecutionService` (31.5) — scope `execute:run` (pas d'entrée dédiée dans la
    liste de scopes autre que `execute:run`). Ces routes exigent 31.5 livrée avant le snapshot 6.2.
