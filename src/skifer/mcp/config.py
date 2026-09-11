"""Strict, dependency-free startup configuration for the read-only MCP server."""

from __future__ import annotations

from dataclasses import dataclass
import ipaddress
from pathlib import Path
import re
from typing import Any

import yaml

from skifer.services import LimitExceeded, ServiceLimits


MCP_SCOPES = frozenset(
    {"models:read", "contracts:read", "lineage:read", "query:execute"}
)
_IMPORT_TARGET_RE = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_.]*:[A-Za-z_][A-Za-z0-9_.]*$"
)


class MCPConfigError(ValueError):
    """A sanitized startup refusal safe to display and log."""


@dataclass(frozen=True)
class BindConfig:
    host: str = "127.0.0.1"
    port: int = 8000


@dataclass(frozen=True)
class StdioConfig:
    subject: str
    consumer_class: str
    scopes: frozenset[str]


@dataclass(frozen=True)
class HTTPAuthConfig:
    verifier_factory: str
    audience: str
    issuer: str
    resource: str
    required_scopes: frozenset[str]


@dataclass(frozen=True)
class MCPServerConfig:
    transport: str
    bind: BindConfig
    service_factory: str
    limits: ServiceLimits
    stdio: StdioConfig | None = None
    http_auth: HTTPAuthConfig | None = None


def load_mcp_config(path: str | Path, *, transport: str) -> MCPServerConfig:
    """Load one closed MCP configuration without ever echoing file contents."""
    if transport not in {"stdio", "http"}:
        raise MCPConfigError("Unsupported MCP transport.")
    try:
        text = Path(path).read_text(encoding="utf-8")
    except (OSError, TypeError, ValueError):
        raise MCPConfigError("MCP configuration file is missing or unreadable.") from None
    try:
        raw = yaml.safe_load(text)
    except yaml.YAMLError:
        # Parser diagnostics include source lines and therefore may quote secrets.
        raise MCPConfigError("MCP configuration file is malformed YAML.") from None
    if not isinstance(raw, dict):
        raise MCPConfigError("MCP configuration root must be a mapping.")
    _closed(raw, {"transport", "bind", "service", "stdio", "http"})

    configured_transport = _text(raw.get("transport"), "transport")
    if configured_transport not in {"stdio", "http"}:
        raise MCPConfigError("MCP configuration has an unsupported transport.")
    if configured_transport != transport:
        raise MCPConfigError("CLI transport does not match MCP configuration.")

    bind = _parse_bind(raw.get("bind", {}))
    service_factory, limits = _parse_service(raw.get("service"))
    if transport == "stdio":
        if "http" in raw:
            raise MCPConfigError("MCP stdio configuration must not contain HTTP fields.")
        stdio = _parse_stdio(raw.get("stdio"))
        if not _is_loopback(bind.host):
            raise MCPConfigError(
                "MCP stdio refuses a non-loopback bind at startup."
            )
        return MCPServerConfig(
            transport=transport,
            bind=bind,
            service_factory=service_factory,
            limits=limits,
            stdio=stdio,
        )

    if "stdio" in raw:
        raise MCPConfigError("MCP HTTP configuration must not contain stdio fields.")
    http_auth = _parse_http(raw.get("http"))
    return MCPServerConfig(
        transport=transport,
        bind=bind,
        service_factory=service_factory,
        limits=limits,
        http_auth=http_auth,
    )


def _parse_bind(value: Any) -> BindConfig:
    mapping = _mapping(value, "bind")
    _closed(mapping, {"host", "port"})
    host = _text(mapping.get("host", "127.0.0.1"), "bind host")
    port = mapping.get("port", 8000)
    if type(port) is not int or not 1 <= port <= 65535:
        raise MCPConfigError("MCP bind port must be an integer in 1..65535.")
    return BindConfig(host=host, port=port)


def _parse_service(value: Any) -> tuple[str, ServiceLimits]:
    mapping = _mapping(value, "service")
    _closed(mapping, {"factory", "limits"})
    factory = _import_target(mapping.get("factory"), "service factory")
    limits_mapping = _mapping(mapping.get("limits", {}), "service limits")
    allowed_limits = {
        "max_page_size",
        "max_query_rows",
        "max_filters",
        "max_filter_value_length",
    }
    _closed(limits_mapping, allowed_limits)
    try:
        limits = ServiceLimits(**limits_mapping)
    except (LimitExceeded, TypeError, ValueError):
        raise MCPConfigError("MCP service limits are invalid or exceed hard ceilings.") from None
    return factory, limits


def _parse_stdio(value: Any) -> StdioConfig:
    mapping = _mapping(value, "stdio")
    _closed(mapping, {"subject", "consumer_class", "scopes"})
    return StdioConfig(
        subject=_text(mapping.get("subject"), "stdio subject"),
        consumer_class=_text(
            mapping.get("consumer_class"), "stdio consumer class"
        ),
        scopes=_scopes(mapping.get("scopes"), "stdio scopes"),
    )


def _parse_http(value: Any) -> HTTPAuthConfig:
    mapping = _mapping(value, "http")
    _closed(mapping, {"auth"})
    auth = _mapping(mapping.get("auth"), "HTTP auth")
    _closed(
        auth,
        {
            "verifier_factory",
            "audience",
            "issuer",
            "resource",
            "required_scopes",
        },
    )
    return HTTPAuthConfig(
        verifier_factory=_import_target(
            auth.get("verifier_factory"), "HTTP verifier factory"
        ),
        audience=_text(auth.get("audience"), "HTTP audience"),
        issuer=_text(auth.get("issuer"), "HTTP issuer"),
        resource=_text(auth.get("resource"), "HTTP resource"),
        required_scopes=_scopes(
            auth.get("required_scopes"), "HTTP required scopes"
        ),
    )


def _mapping(value: Any, field_name: str) -> dict[str, Any]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise MCPConfigError(f"MCP {field_name} must be a mapping.")
    return value


def _closed(mapping: dict[str, Any], allowed: set[str]) -> None:
    if set(mapping) - allowed:
        # Never quote an unknown key: a hostile or mistaken config can put a
        # credential itself in a key just as easily as in a value.
        raise MCPConfigError("MCP configuration contains unknown fields.")


def _text(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise MCPConfigError(f"MCP {field_name} must be non-empty text.")
    return value


def _import_target(value: Any, field_name: str) -> str:
    target = _text(value, field_name)
    if not _IMPORT_TARGET_RE.fullmatch(target):
        raise MCPConfigError(f"MCP {field_name} must use 'module:callable' syntax.")
    return target


def _scopes(value: Any, field_name: str) -> frozenset[str]:
    if (
        not isinstance(value, list)
        or not value
        or not all(isinstance(scope, str) for scope in value)
    ):
        raise MCPConfigError(f"MCP {field_name} must be a non-empty list.")
    scopes = frozenset(value)
    if len(scopes) != len(value) or not scopes.issubset(MCP_SCOPES):
        raise MCPConfigError(f"MCP {field_name} contains an invalid scope.")
    return scopes


def _is_loopback(host: str) -> bool:
    if host.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


__all__ = [
    "BindConfig",
    "HTTPAuthConfig",
    "MCPConfigError",
    "MCPServerConfig",
    "MCP_SCOPES",
    "StdioConfig",
    "load_mcp_config",
]
