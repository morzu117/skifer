"""FastAPI application factory with no import-time optional dependency."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from skifer.services import RequestContext
    from skifer.services.container import ServiceContainer


API_TITLE = "Skifer local API"
API_SCHEMA_VERSION = "0"
API_EXTRA = 'pip install -e ".[api]"'
HEALTH_BODY: dict[str, str] = {"status": "ok"}
CORS_ORIGIN_REGEX = r"^http://(localhost|127\.0\.0\.1)(:\d+)?$"
LOOPBACK_HOSTS = frozenset({"127.0.0.1", "::1", "localhost"})


class APIDependencyError(RuntimeError):
    """The optional API dependencies are not installed."""


def create_app(
    project_dir: str,
    *,
    context_provider: Callable[[Any], "RequestContext"] | None = None,
    services: "ServiceContainer" | None = None,
):
    """Build the local API while keeping FastAPI strictly lazy."""
    try:
        import fastapi  # noqa: F401
    except ImportError:
        raise APIDependencyError(
            f"The local API requires the optional dependencies: `{API_EXTRA}`."
        ) from None

    from fastapi import FastAPI
    from fastapi.middleware.cors import CORSMiddleware

    from skifer.api.errors import register_error_handlers
    from skifer.api.routes import all_routers
    from skifer.services.container import build_services
    from skifer.services.identity import local_request_context

    container = services if services is not None else build_services(project_dir)
    app = FastAPI(title=API_TITLE, version=API_SCHEMA_VERSION)
    app.add_middleware(
        CORSMiddleware,
        allow_origin_regex=CORS_ORIGIN_REGEX,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.state.services = container
    app.state.context_provider = context_provider or (
        lambda _request: local_request_context()
    )
    register_error_handlers(app)

    @app.get("/health", include_in_schema=True)
    def health() -> dict[str, str]:
        return dict(HEALTH_BODY)

    for build in all_routers():
        app.include_router(build(container))
    return app


__all__ = [
    "APIDependencyError",
    "API_EXTRA",
    "API_SCHEMA_VERSION",
    "API_TITLE",
    "CORS_ORIGIN_REGEX",
    "HEALTH_BODY",
    "LOOPBACK_HOSTS",
    "create_app",
]
