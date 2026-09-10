"""CLI runtime assembly for optional stdio and Streamable HTTP MCP transports."""

from __future__ import annotations

import asyncio
import importlib
import json
import logging
from typing import Any, Callable

from skifer.agentic.data_service import AgentReadyDataService
from skifer.mcp.auth import (
    create_http_context_provider,
    create_stdio_context_provider,
)
from skifer.mcp.config import MCPServerConfig
from skifer.mcp.server import MCPDependencyError, MCP_EXTRA, create_server


logger = logging.getLogger(__name__)

HEALTH_PATH = "/health"
MCP_HTTP_PATH = "/mcp"
HEALTH_BODY = b'{"status":"ok"}'


class MCPStartupError(RuntimeError):
    """A sanitized runtime refusal that cannot disclose factory details."""


def serve_mcp(config: MCPServerConfig) -> None:
    """Build the governed service and run the explicitly selected transport."""
    _require_sdk()
    service = _create_service(config)
    context_provider = _create_context_provider(config)
    server = create_server(service, context_provider)

    # Do not add subject, scopes, factories, config paths, or arbitrary config
    # values here. This fixed-vocabulary event is the complete startup log.
    logger.info("Starting read-only MCP server (transport=%s).", config.transport)
    if config.transport == "stdio":
        asyncio.run(_serve_stdio(server))
        return
    _serve_http(server, config)


def create_http_application(server: Any):
    """Create the HTTP ASGI app lazily; the health route is business-data free."""
    try:
        manager_module = importlib.import_module(
            "mcp.server.streamable_http_manager"
        )
        manager_type = manager_module.StreamableHTTPSessionManager
    except (ImportError, AttributeError):
        raise MCPDependencyError(
            f"MCP server support requires the optional dependencies: `{MCP_EXTRA}`."
        ) from None
    try:
        manager = manager_type(app=server)
    except Exception as exc:
        raise MCPStartupError(
            f"MCP HTTP transport initialization failed ({type(exc).__name__})."
        ) from None
    return _MCPHTTPApplication(manager)


class _MCPHTTPApplication:
    """Small ASGI router exposing only Streamable HTTP and a minimal health check."""

    def __init__(self, session_manager: Any):
        self._session_manager = session_manager

    async def __call__(self, scope, receive, send) -> None:
        scope_type = scope.get("type")
        if scope_type == "lifespan":
            await self._lifespan(receive, send)
            return
        if scope_type != "http":
            await _empty_response(send, 404)
            return
        path = scope.get("path")
        method = scope.get("method")
        if path == HEALTH_PATH and method in {"GET", "HEAD"}:
            body = b"" if method == "HEAD" else HEALTH_BODY
            await _json_response(send, 200, body)
            return
        if path in {MCP_HTTP_PATH, f"{MCP_HTTP_PATH}/"}:
            await self._session_manager.handle_request(scope, receive, send)
            return
        await _empty_response(send, 404)

    async def _lifespan(self, receive, send) -> None:
        message = await receive()
        if message.get("type") != "lifespan.startup":
            await send({"type": "lifespan.startup.failed", "message": "startup"})
            return
        started = False
        try:
            async with self._session_manager.run():
                await send({"type": "lifespan.startup.complete"})
                started = True
                while True:
                    message = await receive()
                    if message.get("type") == "lifespan.shutdown":
                        break
                await send({"type": "lifespan.shutdown.complete"})
        except Exception as exc:
            await send(
                {
                    "type": (
                        "lifespan.shutdown.failed"
                        if started
                        else "lifespan.startup.failed"
                    ),
                    "message": type(exc).__name__,
                }
            )


async def _json_response(send, status: int, body: bytes) -> None:
    await send(
        {
            "type": "http.response.start",
            "status": status,
            "headers": [(b"content-type", b"application/json")],
        }
    )
    await send({"type": "http.response.body", "body": body})


async def _empty_response(send, status: int) -> None:
    await send({"type": "http.response.start", "status": status, "headers": []})
    await send({"type": "http.response.body", "body": b""})


def health_payload() -> dict[str, str]:
    """Return the allowlisted health document used by tests and operators."""
    return json.loads(HEALTH_BODY)


def _require_sdk() -> None:
    try:
        importlib.import_module("mcp")
    except ImportError:
        raise MCPDependencyError(
            f"MCP server support requires the optional dependencies: `{MCP_EXTRA}`."
        ) from None


def _create_service(config: MCPServerConfig) -> AgentReadyDataService:
    factory = _resolve_callable(config.service_factory, "service")
    try:
        service = factory(limits=config.limits)
    except Exception as exc:
        raise MCPStartupError(
            f"MCP service initialization failed ({type(exc).__name__})."
        ) from None
    if not isinstance(service, AgentReadyDataService):
        raise MCPStartupError(
            "MCP service factory did not return AgentReadyDataService."
        )
    return service


def _create_context_provider(config: MCPServerConfig):
    if config.transport == "stdio":
        if config.stdio is None:
            raise MCPStartupError("MCP stdio authority is not configured.")
        return create_stdio_context_provider(
            subject=config.stdio.subject,
            scopes=config.stdio.scopes,
            consumer_class=config.stdio.consumer_class,
        )
    if config.http_auth is None:
        raise MCPStartupError("MCP HTTP authentication is not configured.")
    verifier_factory = _resolve_callable(
        config.http_auth.verifier_factory, "HTTP verifier"
    )
    try:
        verifier = verifier_factory()
        return create_http_context_provider(
            verifier,
            audience=config.http_auth.audience,
            issuer=config.http_auth.issuer,
            resource=config.http_auth.resource,
            required_scopes=config.http_auth.required_scopes,
        )
    except Exception as exc:
        raise MCPStartupError(
            f"MCP HTTP authentication initialization failed ({type(exc).__name__})."
        ) from None


def _resolve_callable(target: str, purpose: str) -> Callable[..., Any]:
    module_name, attribute_path = target.split(":", 1)
    try:
        value: Any = importlib.import_module(module_name)
        for component in attribute_path.split("."):
            value = getattr(value, component)
    except Exception as exc:
        raise MCPStartupError(
            f"MCP {purpose} factory could not be loaded ({type(exc).__name__})."
        ) from None
    if not callable(value):
        raise MCPStartupError(f"MCP {purpose} factory is not callable.")
    return value


async def _serve_stdio(server: Any) -> None:
    try:
        stdio_module = importlib.import_module("mcp.server.stdio")
        async with stdio_module.stdio_server() as streams:
            read_stream, write_stream = streams
            await server.run(
                read_stream,
                write_stream,
                _initialization_options(server),
            )
    except MCPDependencyError:
        raise
    except Exception as exc:
        raise MCPStartupError(
            f"MCP stdio transport failed ({type(exc).__name__})."
        ) from None


def _initialization_options(server: Any):
    creator = getattr(server, "create_initialization_options", None)
    if callable(creator):
        return creator()
    try:
        models = importlib.import_module("mcp.server.models")
        lowlevel = importlib.import_module("mcp.server.lowlevel.server")
        capabilities = server.get_capabilities(
            notification_options=lowlevel.NotificationOptions(),
            experimental_capabilities={},
        )
        return models.InitializationOptions(
            server_name="skifer-readonly",
            server_version="1",
            capabilities=capabilities,
        )
    except Exception as exc:
        raise MCPStartupError(
            f"MCP SDK initialization failed ({type(exc).__name__})."
        ) from None


def _serve_http(server: Any, config: MCPServerConfig) -> None:
    app = create_http_application(server)
    try:
        uvicorn = importlib.import_module("uvicorn")
        uvicorn.run(
            app,
            host=config.bind.host,
            port=config.bind.port,
            log_config=None,
        )
    except ImportError:
        raise MCPDependencyError(
            f"MCP server support requires the optional dependencies: `{MCP_EXTRA}`."
        ) from None
    except Exception as exc:
        raise MCPStartupError(
            f"MCP HTTP transport failed ({type(exc).__name__})."
        ) from None


__all__ = [
    "HEALTH_BODY",
    "HEALTH_PATH",
    "MCP_HTTP_PATH",
    "MCPStartupError",
    "create_http_application",
    "health_payload",
    "serve_mcp",
]
