"""Plan 29 slice 7.4 MCP request authentication contract."""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import threading
from types import SimpleNamespace

import pytest

from skifer.mcp.auth import (
    AUTHENTICATION_FAILED,
    CERTIFICATION_OVERRIDE_SCOPE,
    MCP_AUTH_SPAN_NAME,
    VerifiedBearerToken,
    create_http_context_provider,
    create_stdio_context_provider,
)
from skifer.mcp.resources import MCPResourceError
from skifer.observability.tracing import InMemoryTracer, TraceContext


NOW = datetime(2026, 9, 7, 12, 0, 0, tzinfo=timezone.utc)
AUDIENCE = "https://mcp.skifer.example"
ISSUER = "https://identity.example"
RESOURCE = "skifer:mcp"


def _claims(
    subject="agent-a",
    *,
    issuer=ISSUER,
    audience=AUDIENCE,
    resource=RESOURCE,
    expires_at=None,
    scopes=frozenset(("models:read", "query:execute")),
    consumer_class="agent_read",
):
    return VerifiedBearerToken(
        subject=subject,
        issuer=issuer,
        audience=audience,
        resource=resource,
        expires_at=expires_at or NOW + timedelta(minutes=5),
        scopes=scopes,
        consumer_class=consumer_class,
    )


class MappingVerifier:
    def __init__(self, tokens):
        self.tokens = tokens
        self.seen = []

    def verify(self, token):
        self.seen.append(token)
        result = self.tokens[token]
        if isinstance(result, BaseException):
            raise result
        return result


def _request(token="token-a", **untrusted_fields):
    return SimpleNamespace(
        headers={"Authorization": f"Bearer {token}"},
        **untrusted_fields,
    )


def _provider(verifier, **overrides):
    options = {
        "audience": AUDIENCE,
        "issuer": ISSUER,
        "resource": RESOURCE,
        "clock": lambda: NOW,
    }
    options.update(overrides)
    return create_http_context_provider(verifier, **options)


def _assert_refused(call):
    with pytest.raises(MCPResourceError) as raised:
        call()
    assert raised.value.code == AUTHENTICATION_FAILED
    assert raised.value.message == "Bearer authentication failed."
    assert raised.value.data == {"error_type": "authentication_failed"}
    return raised.value


def test_stdio_uses_only_explicit_static_identity_and_scopes():
    provider = create_stdio_context_provider(
        subject="local-developer",
        scopes=frozenset(("models:read",)),
        consumer_class="agent_read",
    )

    context = provider(
        SimpleNamespace(
            subject="remote-impostor",
            scopes=frozenset(("query:execute", CERTIFICATION_OVERRIDE_SCOPE)),
            headers={"Authorization": "Bearer ignored"},
        )
    )

    assert context.subject == "local-developer"
    assert context.scopes == frozenset(("models:read",))
    assert context.consumer_class == "agent_read"
    assert context.trace_context == TraceContext()


def test_stdio_refuses_empty_or_override_startup_scopes():
    with pytest.raises(ValueError):
        create_stdio_context_provider(
            subject="local-developer",
            scopes=frozenset(),
            consumer_class="agent_read",
        )
    with pytest.raises(ValueError):
        create_stdio_context_provider(
            subject="local-developer",
            scopes=frozenset((CERTIFICATION_OVERRIDE_SCOPE,)),
            consumer_class="agent_read",
        )


def test_http_valid_token_builds_context_from_verified_claims_only():
    verifier = MappingVerifier(
        {
            "token-a": _claims(
                "subject-from-token",
                scopes=frozenset(("contracts:read", "lineage:read")),
                consumer_class="agent_action",
            )
        }
    )
    provider = _provider(verifier)

    context = provider(
        _request(
            "token-a",
            subject="client-claim",
            scopes=frozenset(("query:execute",)),
            consumer_class="dashboard",
        )
    )

    assert context.subject == "subject-from-token"
    assert context.scopes == frozenset(("contracts:read", "lineage:read"))
    assert context.consumer_class == "agent_action"
    assert verifier.seen == ["token-a"]


def test_http_refuses_missing_bearer_token_before_verifier_call():
    verifier = MappingVerifier({})
    provider = _provider(verifier)

    _assert_refused(lambda: provider(SimpleNamespace(headers={})))

    assert verifier.seen == []


@pytest.mark.parametrize(
    "authorization",
    [
        "Basic abc",
        "Bearer",
        "Bearer ",
        "Bearer two parts",
        "Bearer token\nsecond-header: value",
    ],
)
def test_http_refuses_each_malformed_authorization_header(authorization):
    verifier = MappingVerifier({})
    provider = _provider(verifier)

    _assert_refused(
        lambda: provider(SimpleNamespace(headers={"Authorization": authorization}))
    )

    assert verifier.seen == []


def test_http_refuses_expired_token():
    verifier = MappingVerifier(
        {"expired": _claims(expires_at=NOW - timedelta(microseconds=1))}
    )

    _assert_refused(lambda: _provider(verifier)(_request("expired")))


def test_http_expiry_has_no_silent_leeway_at_the_exact_instant():
    verifier = MappingVerifier(
        {
            "equal": _claims(expires_at=NOW),
            "future": _claims(expires_at=NOW + timedelta(seconds=1)),
        }
    )
    provider = _provider(verifier)

    _assert_refused(lambda: provider(_request("equal")))
    assert provider(_request("future")).subject == "agent-a"


def test_http_compares_expiry_in_utc_for_non_utc_aware_datetimes():
    paris = timezone(timedelta(hours=2))
    verifier = MappingVerifier(
        {"valid": _claims(expires_at=(NOW + timedelta(seconds=1)).astimezone(paris))}
    )

    assert _provider(verifier)(_request("valid")).subject == "agent-a"


def test_http_refuses_wrong_audience():
    verifier = MappingVerifier({"wrong-aud": _claims(audience="another-service")})

    _assert_refused(lambda: _provider(verifier)(_request("wrong-aud")))


def test_http_refuses_wrong_resource():
    verifier = MappingVerifier({"wrong-resource": _claims(resource="other-resource")})

    _assert_refused(lambda: _provider(verifier)(_request("wrong-resource")))


def test_http_refuses_wrong_issuer():
    verifier = MappingVerifier({"wrong-issuer": _claims(issuer="https://attacker")})

    _assert_refused(lambda: _provider(verifier)(_request("wrong-issuer")))


@pytest.mark.parametrize("scopes", [None, frozenset()])
def test_http_refuses_absent_or_empty_scopes(scopes):
    verifier = MappingVerifier({"no-scopes": _claims(scopes=scopes)})

    _assert_refused(lambda: _provider(verifier)(_request("no-scopes")))


def test_http_refuses_when_configured_required_scope_is_missing():
    verifier = MappingVerifier(
        {"insufficient": _claims(scopes=frozenset(("models:read",)))}
    )
    provider = _provider(
        verifier, required_scopes=frozenset(("query:execute",))
    )

    _assert_refused(lambda: provider(_request("insufficient")))


def test_http_refuses_verifier_exception_without_exposing_its_message():
    sentinel = "TOKEN_SENTINEL_7_4"
    verifier = MappingVerifier(
        {sentinel: RuntimeError(f"signature rejected for {sentinel}")}
    )

    error = _assert_refused(lambda: _provider(verifier)(_request(sentinel)))

    serialized = json.dumps(
        {"message": str(error), "data": error.data, "repr": repr(error)}
    )
    assert sentinel not in serialized
    assert "signature rejected" not in serialized


def test_http_refuses_naive_expiry_datetime():
    verifier = MappingVerifier(
        {"naive-expiry": _claims(expires_at=NOW.replace(tzinfo=None))}
    )

    _assert_refused(lambda: _provider(verifier)(_request("naive-expiry")))


def test_http_refuses_naive_authentication_clock():
    verifier = MappingVerifier({"naive-clock": _claims()})
    provider = _provider(verifier, clock=lambda: NOW.replace(tzinfo=None))

    _assert_refused(lambda: provider(_request("naive-clock")))


def test_certification_override_scope_refuses_the_entire_http_token():
    verifier = MappingVerifier(
        {
            "override": _claims(
                scopes=frozenset(("models:read", CERTIFICATION_OVERRIDE_SCOPE))
            )
        }
    )

    _assert_refused(lambda: _provider(verifier)(_request("override")))


def test_http_credentials_are_isolated_between_concurrent_threads():
    barrier = threading.Barrier(2)

    class ConcurrentVerifier(MappingVerifier):
        def verify(self, token):
            barrier.wait(timeout=5)
            return super().verify(token)

    verifier = ConcurrentVerifier(
        {
            "token-a": _claims(
                "subject-a", scopes=frozenset(("models:read",))
            ),
            "token-b": _claims(
                "subject-b", scopes=frozenset(("query:execute",))
            ),
        }
    )
    provider = _provider(verifier)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda token: provider(_request(token)), ("token-a", "token-b")))

    observed = {(context.subject, context.scopes) for context in results}
    assert observed == {
        ("subject-a", frozenset(("models:read",))),
        ("subject-b", frozenset(("query:execute",))),
    }


def test_http_credentials_and_trace_context_are_isolated_between_async_tasks():
    tracer = InMemoryTracer()
    verifier = MappingVerifier(
        {
            "token-a": _claims(
                "subject-a", scopes=frozenset(("models:read",))
            ),
            "token-b": _claims(
                "subject-b", scopes=frozenset(("query:execute",))
            ),
        }
    )
    provider = _provider(verifier, tracer=tracer)
    both_ready = asyncio.Event()
    ready = 0
    lock = asyncio.Lock()

    async def worker(token, trace_id):
        nonlocal ready
        with tracer.use_context(TraceContext(trace_id=trace_id)):
            async with lock:
                ready += 1
                if ready == 2:
                    both_ready.set()
            await asyncio.wait_for(both_ready.wait(), timeout=5)
            await asyncio.sleep(0)
            return provider(_request(token))

    async def run_workers():
        return await asyncio.gather(
            worker("token-a", "a" * 32),
            worker("token-b", "b" * 32),
        )

    first, second = asyncio.run(run_workers())

    assert (first.subject, first.scopes, first.trace_context.trace_id) == (
        "subject-a",
        frozenset(("models:read",)),
        "a" * 32,
    )
    assert (second.subject, second.scopes, second.trace_context.trace_id) == (
        "subject-b",
        frozenset(("query:execute",)),
        "b" * 32,
    )
    assert {span.trace_id for span in tracer.spans} == {"a" * 32, "b" * 32}


def test_canary_token_never_reaches_error_log_or_trace(caplog):
    sentinel = "MCP_TOKEN_CANARY_DO_NOT_LEAK"
    tracer = InMemoryTracer()
    verifier = MappingVerifier(
        {
            sentinel: MCPResourceError(
                999,
                f"backend included {sentinel}",
                f"unsafe-{sentinel}",
            )
        }
    )
    provider = _provider(verifier, tracer=tracer)

    error = _assert_refused(lambda: provider(_request(sentinel)))

    serialized_error = json.dumps(
        {"message": str(error), "data": error.data, "repr": repr(error)}
    )
    serialized_traces = json.dumps(
        [
            {
                "name": span.name,
                "attributes": span.attributes,
                "exceptions": [item.type_name for item in span.exceptions],
            }
            for span in tracer.spans
        ]
    )
    serialized_logs = json.dumps([record.getMessage() for record in caplog.records])
    assert sentinel not in serialized_error
    assert sentinel not in serialized_traces
    assert sentinel not in serialized_logs


def test_trace_identity_is_omitted_by_default():
    subject = "person@example.test"
    tracer = InMemoryTracer()
    verifier = MappingVerifier({"valid": _claims(subject)})

    context = _provider(verifier, tracer=tracer)(_request("valid"))

    assert context.subject == subject
    assert len(tracer.spans) == 1
    assert tracer.spans[0].name == MCP_AUTH_SPAN_NAME
    assert tracer.spans[0].attributes == {"skifer.trace_version": "w3c-v1"}


def test_trace_hmac_without_secret_omits_identity_not_unkeyed_digest():
    subject = "person@example.test"
    tracer = InMemoryTracer()
    verifier = MappingVerifier({"valid": _claims(subject)})

    _provider(
        verifier,
        tracer=tracer,
        tracing_user_identity="hmac",
        environ={},
    )(_request("valid"))

    serialized = json.dumps(tracer.spans[0].attributes)
    assert "user_pseudonym" not in tracer.spans[0].attributes
    assert hashlib.sha256(subject.encode()).hexdigest() not in serialized
    assert subject not in serialized


def test_trace_hmac_uses_the_feature_five_secret_policy():
    subject = "person@example.test"
    tracer = InMemoryTracer()
    verifier = MappingVerifier({"valid": _claims(subject)})

    _provider(
        verifier,
        tracer=tracer,
        tracing_user_identity="hmac",
        environ={"SKIFER_TRACING_HMAC_SECRET": "test-secret"},
    )(_request("valid"))

    pseudonym = tracer.spans[0].attributes["user_pseudonym"]
    assert len(pseudonym) == 32
    assert subject not in pseudonym


def test_auth_module_imports_without_optional_mcp_sdk():
    project_root = Path(__file__).resolve().parents[1]
    script = """
import sys
assert 'mcp' not in sys.modules
import skifer.mcp.auth
assert 'mcp' not in sys.modules
"""

    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=project_root,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr


def test_auth_factories_are_available_from_optional_mcp_package():
    from skifer import mcp

    assert mcp.create_http_context_provider is create_http_context_provider
    assert mcp.create_stdio_context_provider is create_stdio_context_provider
