"""Plan 29 slice 7.5: strict MCP startup, health, and optional-SDK smoke."""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
import subprocess
import sys

import pytest
import yaml

from skifer.agentic.data_service import (
    AgentReadyDataService,
    GovernedModelView,
    ModelSummary,
    Page,
    QueryEnvelope,
    RequestContext,
    ServiceLimits,
)
from skifer.mcp.config import MCPConfigError, load_mcp_config
from skifer.observability.tracing import TraceContext


ALL_SCOPES = ["models:read", "contracts:read", "lineage:read", "query:execute"]
TOKEN_CANARY = "TOKEN-CANARY-7-5"
SECRET_CANARY = "SECRET-CANARY-7-5"
PASSWORD_CANARY = "PASSWORD-CANARY-7-5"
CREDENTIAL_PATH_CANARY = "/credentials/CREDENTIAL-PATH-CANARY-7-5.json"


def _stdio_payload(host: str = "127.0.0.1") -> dict:
    return {
        "transport": "stdio",
        "bind": {"host": host, "port": 8000},
        "service": {"factory": "deployment.mcp:create_service", "limits": {}},
        "stdio": {
            "subject": "local-agent",
            "consumer_class": "mcp",
            "scopes": ALL_SCOPES,
        },
    }


def _http_payload(host: str = "127.0.0.1") -> dict:
    return {
        "transport": "http",
        "bind": {"host": host, "port": 8080},
        "service": {"factory": "deployment.mcp:create_service", "limits": {}},
        "http": {
            "auth": {
                "verifier_factory": "deployment.auth:create_verifier",
                "audience": "https://agents.example",
                "issuer": "https://identity.example",
                "resource": "skifer:mcp",
                "required_scopes": ALL_SCOPES,
            }
        },
    }


def _config(tmp_path: Path, payload: object, name: str = "mcp.yaml") -> Path:
    path = tmp_path / name
    path.write_text(yaml.safe_dump(payload), encoding="utf-8")
    return path


def _run_cli(*arguments: str) -> subprocess.CompletedProcess:
    code = (
        f"import sys; sys.argv = {['skifer', *arguments]!r}; "
        "from skifer.cli import main; main()"
    )
    return subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
    )


@pytest.mark.parametrize("host", ["0.0.0.0", "::", "192.0.2.10", "mcp.example"])
def test_stdio_refuses_every_non_loopback_bind(tmp_path, host):
    path = _config(tmp_path, _stdio_payload(host))

    with pytest.raises(MCPConfigError, match="non-loopback"):
        load_mcp_config(path, transport="stdio")


@pytest.mark.parametrize("host", ["0.0.0.0", "::", "198.51.100.7"])
def test_cli_refuses_public_stdio_before_optional_sdk_loading(tmp_path, host):
    path = _config(tmp_path, _stdio_payload(host))

    result = _run_cli(
        "mcp", "serve", "--transport", "stdio", "--config", str(path)
    )

    output = result.stdout + result.stderr
    assert result.returncode != 0
    assert "non-loopback" in output
    assert "optional dependencies" not in output
    assert "Traceback" not in output


@pytest.mark.parametrize("host", ["127.0.0.1", "127.42.0.9", "::1", "localhost"])
def test_stdio_accepts_loopback_bind(tmp_path, host):
    path = _config(tmp_path, _stdio_payload(host))

    config = load_mcp_config(path, transport="stdio")

    assert config.bind.host == host
    assert config.stdio is not None
    assert config.http_auth is None


@pytest.mark.parametrize(
    "missing",
    ["verifier_factory", "audience", "issuer", "resource", "required_scopes"],
)
def test_http_refuses_incomplete_auth_even_on_loopback(tmp_path, missing):
    payload = _http_payload()
    del payload["http"]["auth"][missing]
    path = _config(tmp_path, payload)

    with pytest.raises(MCPConfigError):
        load_mcp_config(path, transport="http")


def test_http_refuses_public_bind_without_auth_block(tmp_path):
    payload = _http_payload("0.0.0.0")
    del payload["http"]["auth"]
    path = _config(tmp_path, payload)

    with pytest.raises(MCPConfigError):
        load_mcp_config(path, transport="http")


def test_http_accepts_public_bind_only_with_complete_explicit_auth(tmp_path):
    path = _config(tmp_path, _http_payload("0.0.0.0"))

    config = load_mcp_config(path, transport="http")

    assert config.bind.host == "0.0.0.0"
    assert config.http_auth is not None
    assert config.http_auth.resource == "skifer:mcp"


@pytest.mark.parametrize(
    ("payload", "transport"),
    [
        ({"transport": "websocket"}, "http"),
        ({"transport": "stdio", "unknown": True}, "stdio"),
        (_stdio_payload() | {"http": {}}, "stdio"),
        (_http_payload() | {"stdio": {}}, "http"),
    ],
)
def test_config_is_closed_and_transport_specific(tmp_path, payload, transport):
    path = _config(tmp_path, payload)

    with pytest.raises(MCPConfigError):
        load_mcp_config(path, transport=transport)


@pytest.mark.parametrize("scope", ["semantic:query", "admin", "", " models:read"])
def test_config_refuses_invalid_scope(tmp_path, scope):
    payload = _stdio_payload()
    payload["stdio"]["scopes"] = [scope]
    path = _config(tmp_path, payload)

    with pytest.raises(MCPConfigError, match="invalid scope"):
        load_mcp_config(path, transport="stdio")


@pytest.mark.parametrize("contents", ["[not, a, mapping]", "http: [", "null"])
def test_missing_or_malformed_config_is_a_sanitized_refusal(tmp_path, contents):
    path = tmp_path / "mcp.yaml"
    path.write_text(contents, encoding="utf-8")

    with pytest.raises(MCPConfigError) as caught:
        load_mcp_config(path, transport="http")

    assert "Traceback" not in str(caught.value)
    with pytest.raises(MCPConfigError, match="missing or unreadable"):
        load_mcp_config(tmp_path / "absent.yaml", transport="stdio")


def test_config_errors_never_echo_token_secret_or_password_canaries(tmp_path):
    payload = _stdio_payload()
    payload["token"] = TOKEN_CANARY
    payload["secret"] = SECRET_CANARY
    payload["password"] = PASSWORD_CANARY
    path = _config(tmp_path, payload)

    with pytest.raises(MCPConfigError) as caught:
        load_mcp_config(path, transport="stdio")

    message = str(caught.value)
    assert TOKEN_CANARY not in message
    assert SECRET_CANARY not in message
    assert PASSWORD_CANARY not in message


def test_cli_invalid_config_exits_nonzero_without_secret_or_traceback(tmp_path):
    payload = _stdio_payload()
    payload["credentials"] = {
        "token": TOKEN_CANARY,
        "secret": SECRET_CANARY,
        "password": PASSWORD_CANARY,
        "path": CREDENTIAL_PATH_CANARY,
    }
    path = _config(tmp_path, payload)

    result = _run_cli(
        "mcp", "serve", "--transport", "stdio", "--config", str(path)
    )

    output = result.stdout + result.stderr
    assert result.returncode != 0
    assert "unknown fields" in output
    assert "Traceback" not in output
    assert TOKEN_CANARY not in output
    assert SECRET_CANARY not in output
    assert PASSWORD_CANARY not in output
    assert CREDENTIAL_PATH_CANARY not in output


def test_invalid_limit_error_never_echoes_rejected_value(tmp_path):
    payload = _stdio_payload()
    payload["service"]["limits"] = {"max_query_rows": SECRET_CANARY}
    path = _config(tmp_path, payload)

    with pytest.raises(MCPConfigError) as caught:
        load_mcp_config(path, transport="stdio")

    assert SECRET_CANARY not in str(caught.value)


def test_startup_log_is_fixed_vocabulary_and_contains_no_config_canary(
    tmp_path, monkeypatch, caplog, capsys
):
    from skifer.mcp import runtime

    payload = _http_payload()
    payload["http"]["auth"]["audience"] = TOKEN_CANARY + PASSWORD_CANARY
    payload["http"]["auth"]["issuer"] = SECRET_CANARY
    payload["http"]["auth"]["resource"] = CREDENTIAL_PATH_CANARY
    config = load_mcp_config(_config(tmp_path, payload), transport="http")
    monkeypatch.setattr(runtime, "_require_sdk", lambda: None)
    monkeypatch.setattr(runtime, "_create_service", lambda _config: object())
    monkeypatch.setattr(runtime, "_create_context_provider", lambda _config: object())
    monkeypatch.setattr(runtime, "create_server", lambda service, provider: object())
    monkeypatch.setattr(runtime, "_serve_http", lambda server, server_config: None)

    with caplog.at_level(logging.INFO):
        runtime.serve_mcp(config)

    assert "transport=http" in caplog.text
    assert TOKEN_CANARY not in caplog.text
    assert SECRET_CANARY not in caplog.text
    assert PASSWORD_CANARY not in caplog.text
    assert CREDENTIAL_PATH_CANARY not in caplog.text
    assert "deployment" not in caplog.text
    captured = capsys.readouterr()
    terminal_output = captured.out + captured.err
    assert TOKEN_CANARY not in terminal_output
    assert SECRET_CANARY not in terminal_output
    assert PASSWORD_CANARY not in terminal_output
    assert CREDENTIAL_PATH_CANARY not in terminal_output


def test_health_endpoint_contains_status_and_no_business_data():
    from skifer.mcp.runtime import HEALTH_BODY, _MCPHTTPApplication

    class Manager:
        async def handle_request(self, scope, receive, send):  # pragma: no cover
            raise AssertionError("health must not enter the MCP session")

    sent = []

    async def receive():
        return {"type": "http.request", "body": b""}

    async def send(message):
        sent.append(message)

    app = _MCPHTTPApplication(Manager())
    asyncio.run(
        app(
            {"type": "http", "path": "/health", "method": "GET"},
            receive,
            send,
        )
    )

    body = next(message["body"] for message in sent if "body" in message)
    assert body == HEALTH_BODY == b'{"status":"ok"}'
    lowered = body.lower()
    for forbidden in (b"orders", b"model", b"dataset", b"table", b"contract", b"count"):
        assert forbidden not in lowered


def test_mcp_serve_accepts_valid_stdio_config_without_changing_other_cli_paths(
    tmp_path, monkeypatch
):
    from skifer import cli
    from skifer.mcp import runtime

    path = _config(tmp_path, _stdio_payload())
    seen = []
    monkeypatch.setattr(runtime, "serve_mcp", seen.append)
    monkeypatch.setattr(
        sys,
        "argv",
        ["skifer", "mcp", "serve", "--transport", "stdio", "--config", str(path)],
    )

    cli.main()

    assert len(seen) == 1
    assert seen[0].transport == "stdio"


def test_mcp_serve_without_sdk_is_actionable_and_has_no_raw_import_error(
    tmp_path, monkeypatch, capsys
):
    from skifer import cli
    from skifer.mcp import runtime
    from skifer.mcp.server import MCPDependencyError, MCP_EXTRA

    path = _config(tmp_path, _stdio_payload())

    def missing(_config):
        raise MCPDependencyError(
            f"MCP server support requires the optional dependencies: `{MCP_EXTRA}`."
        )

    monkeypatch.setattr(runtime, "serve_mcp", missing)
    monkeypatch.setattr(
        sys,
        "argv",
        ["skifer", "mcp", "serve", "--transport", "stdio", "--config", str(path)],
    )

    with pytest.raises(SystemExit) as caught:
        cli.main()

    output = capsys.readouterr()
    assert caught.value.code == 1
    assert 'pip install -e ".[mcp]"' in output.err
    assert "ImportError:" not in output.err
    assert "Traceback" not in output.err


def test_core_and_cli_help_do_not_import_optional_mcp_sdk():
    code = r"""
import importlib.abc
import sys

class BlockMCP(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "mcp" or fullname.startswith("mcp."):
            raise AssertionError("optional MCP SDK imported")
        return None

sys.meta_path.insert(0, BlockMCP())
import skifer
from skifer.cli import main
sys.argv = ["skifer", "--help"]
try:
    main()
except SystemExit as exc:
    assert exc.code == 0
assert "mcp" not in sys.modules
assert not any(name.startswith("mcp.") for name in sys.modules)
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize(
    "arguments",
    [
        ["hub", "--help"],
        ["validate", "--help"],
        ["semantic", "--help"],
        ["semantic", "sync", "--help"],
        ["semantic", "validate", "--help"],
    ],
)
def test_existing_cli_help_exit_codes_are_unchanged(arguments):
    code = (
        f"import sys; sys.argv = {['skifer', *arguments]!r}; "
        "from skifer.cli import main; main()"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stdout + result.stderr


class _SmokeService(AgentReadyDataService):
    def __init__(self):
        self.limits = ServiceLimits()

    def list_models(self, ctx, cursor=None, limit=50):
        return Page(
            items=(ModelSummary("orders", "Orders", "gold", ("sales",)),),
            next_cursor=None,
            total=1,
        )

    def get_model(self, ctx, key):
        return GovernedModelView(
            key="orders",
            description="Orders",
            layer="gold",
            tags=("sales",),
            dimensions=("country",),
            metrics=("revenue",),
            entities=("order",),
            related_models=(),
        )

    def query(self, ctx, query, limit=100):
        return QueryEnvelope(
            rows=[{"revenue": 42}],
            evidence={"format_version": "1"},
            truncated=False,
        )


def _smoke_context(_request) -> RequestContext:
    return RequestContext(
        subject="sdk-smoke",
        scopes=frozenset(ALL_SCOPES),
        consumer_class="mcp",
        trace_context=TraceContext(),
    )


def test_official_sdk_smoke_list_read_and_query():
    pytest.importorskip("mcp", reason="optional MCP SDK is not installed")
    anyio = pytest.importorskip("anyio")
    from mcp import ClientSession

    from skifer.mcp.runtime import _initialization_options
    from skifer.mcp.server import create_server

    async def smoke():
        server = create_server(_SmokeService(), _smoke_context)
        client_send, server_receive = anyio.create_memory_object_stream(0)
        server_send, client_receive = anyio.create_memory_object_stream(0)
        async with (
            client_send,
            server_receive,
            server_send,
            client_receive,
            anyio.create_task_group() as tasks,
        ):
            tasks.start_soon(
                server.run,
                server_receive,
                server_send,
                _initialization_options(server),
            )
            async with ClientSession(client_receive, client_send) as client:
                await client.initialize()
                listed = await client.list_resources()
                assert any(str(item.uri) == "skifer://semantic/catalog" for item in listed.resources)
                read = await client.read_resource("skifer://semantic/models/orders")
                assert json.loads(read.contents[0].text)["key"] == "orders"
                queried = await client.call_tool(
                    "query_semantic_model",
                    {"model": "orders", "metrics": ["revenue"], "limit": 1},
                )
                assert json.loads(queried.content[0].text)["rows"] == [{"revenue": 42}]
            tasks.cancel_scope.cancel()

    anyio.run(smoke)
