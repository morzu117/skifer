"""Dependency-free request authentication for the optional MCP transports.

This module establishes :class:`RequestContext` instances but never issues,
signs, or decodes access tokens.  An HTTP host injects a verifier responsible
for cryptographic token authentication and returns a small trusted claim set;
this boundary then checks every authorization claim again before constructing
a context.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Protocol

from skifer.services import RequestContext
from skifer.mcp.resources import MCPResourceError
from skifer.observability.tracing import (
    TRACE_FORMAT_VERSION,
    NoOpTracer,
    TraceAttributePolicy,
    build_attribute_policy,
    configured_span_scope,
    current_trace_context,
)


AUTHENTICATION_FAILED = -32005
CERTIFICATION_OVERRIDE_SCOPE = "certification_override"
MCP_AUTH_SPAN_NAME = "skifer.mcp.authenticate"


@dataclass(frozen=True)
class VerifiedBearerToken:
    """Trusted claims returned after an injected verifier authenticates a token.

    ``audience`` and ``resource`` deliberately accept only immutable values.
    The verifier owns parsing provider-specific claim shapes; this transport
    boundary owns their strict interpretation and refuses ambiguity.
    """

    subject: str
    issuer: str
    audience: str | frozenset[str]
    expires_at: datetime
    scopes: frozenset[str] | None
    consumer_class: str
    resource: str | frozenset[str] | None = None


class BearerTokenVerifier(Protocol):
    """Host-supplied cryptographic bearer-token verifier.

    Implementations validate authenticity/signature and decode provider-specific
    claims.  Skifer intentionally supplies no authorization server and
    no home-grown JWT implementation.
    """

    def verify(self, token: str) -> VerifiedBearerToken: ...


def create_stdio_context_provider(
    *,
    subject: str,
    scopes: frozenset[str],
    consumer_class: str,
    tracer: Any = None,
    tracing_user_identity: str = "omit",
    environ: Mapping[str, str] | None = None,
) -> Callable[[Any], RequestContext]:
    """Create a local stdio provider from explicit, immutable startup config.

    The request object is intentionally ignored.  Stdio runs as the local OS
    user and has no OAuth flow, so request-supplied identity or scopes can never
    enlarge the statically configured authority.
    """
    static_scopes = _validated_scopes(scopes, required=frozenset())
    static_subject = _required_text(subject, "stdio subject")
    static_consumer_class = _required_text(consumer_class, "stdio consumer class")
    active_tracer, identity_policy = _tracing(
        tracer, tracing_user_identity, environ
    )

    def context_provider(_request: Any) -> RequestContext:
        return _request_context(
            subject=static_subject,
            scopes=static_scopes,
            consumer_class=static_consumer_class,
            tracer=active_tracer,
            identity_policy=identity_policy,
        )

    return context_provider


def create_http_context_provider(
    verifier: BearerTokenVerifier,
    *,
    audience: str,
    issuer: str,
    resource: str | None = None,
    required_scopes: frozenset[str] = frozenset(),
    clock: Callable[[], datetime] | None = None,
    tracer: Any = None,
    tracing_user_identity: str = "omit",
    environ: Mapping[str, str] | None = None,
) -> Callable[[Any], RequestContext]:
    """Create a fail-closed HTTP provider that authenticates every request.

    Only immutable policy configuration is captured.  The bearer credential,
    verified claims, and resulting context remain call-local, preventing a
    connection or singleton from lending one caller another caller's identity.
    Expiry has no implicit leeway: ``expires_at <= now`` is refused.
    """
    verify = getattr(verifier, "verify", None)
    if verify is None or not callable(verify):
        raise TypeError("verifier must implement verify(token).")
    expected_audience = _required_text(audience, "audience")
    expected_issuer = _required_text(issuer, "issuer")
    expected_resource = (
        None
        if resource is None
        else _required_text(resource, "resource")
    )
    required = _validated_scopes(
        required_scopes,
        required=frozenset(),
        allow_empty=True,
    )
    now = clock or (lambda: datetime.now(timezone.utc))
    if not callable(now):
        raise TypeError("clock must be callable.")
    active_tracer, identity_policy = _tracing(
        tracer, tracing_user_identity, environ
    )

    def context_provider(request: Any) -> RequestContext:
        try:
            token = _bearer_token(request)
            claims = verify(token)
            if not isinstance(claims, VerifiedBearerToken):
                raise ValueError("untrusted claim shape")
            subject = _required_text(claims.subject, "token subject")
            consumer_class = _required_text(
                claims.consumer_class, "token consumer class"
            )
            token_issuer = _required_text(claims.issuer, "token issuer")
            if token_issuer != expected_issuer:
                raise ValueError("issuer mismatch")

            audiences = _claim_values(claims.audience, "token audience")
            resources = _claim_values(
                claims.resource, "token resource", allow_none=True
            )
            if expected_audience not in audiences:
                raise ValueError("audience mismatch")
            if (
                expected_resource is not None
                and expected_resource not in audiences
                and expected_resource not in resources
            ):
                raise ValueError("resource mismatch")

            expires_at = _aware_datetime(claims.expires_at, "token expiry")
            current = _aware_datetime(now(), "authentication clock")
            if expires_at.astimezone(timezone.utc) <= current.astimezone(timezone.utc):
                raise ValueError("token expired")
            scopes = _validated_scopes(claims.scopes, required=required)
        except Exception:
            # Verifier messages routinely contain a token, key id, endpoint, or
            # rejected claim.  No exception detail crosses this boundary.
            raise _authentication_refused() from None

        return _request_context(
            subject=subject,
            scopes=scopes,
            consumer_class=consumer_class,
            tracer=active_tracer,
            identity_policy=identity_policy,
        )

    return context_provider


def _bearer_token(request: Any) -> str:
    headers = _request_headers(request)
    authorization = None
    for key, value in headers.items():
        if isinstance(key, str) and key.lower() == "authorization":
            if authorization is not None:
                raise ValueError("duplicate authorization header")
            authorization = value
    if not isinstance(authorization, str):
        raise ValueError("authorization header missing")
    parts = authorization.split(" ", 1)
    if (
        len(parts) != 2
        or parts[0].lower() != "bearer"
        or not parts[1]
        or any(character.isspace() for character in parts[1])
    ):
        raise ValueError("authorization header malformed")
    return parts[1]


def _request_headers(request: Any) -> Mapping[Any, Any]:
    if isinstance(request, Mapping):
        nested = request.get("headers")
        headers = request if nested is None else nested
    else:
        headers = getattr(request, "headers", None)
        if headers is None:
            http_request = getattr(request, "request", None)
            headers = getattr(http_request, "headers", None)
    if not isinstance(headers, Mapping):
        raise ValueError("request headers unavailable")
    return headers


def _validated_scopes(
    scopes: Any,
    *,
    required: frozenset[str],
    allow_empty: bool = False,
) -> frozenset[str]:
    if not isinstance(scopes, frozenset) or not all(
        isinstance(scope, str) and scope and scope == scope.strip()
        for scope in scopes
    ):
        raise ValueError("scopes must be an immutable set of non-empty text")
    if not scopes and not allow_empty:
        raise ValueError("at least one scope is required")
    if CERTIFICATION_OVERRIDE_SCOPE in scopes:
        raise ValueError("certification override is forbidden over MCP")
    if not required.issubset(scopes):
        raise ValueError("required scopes are missing")
    return scopes


def _claim_values(
    value: Any, field_name: str, *, allow_none: bool = False
) -> frozenset[str]:
    if value is None and allow_none:
        return frozenset()
    if isinstance(value, str):
        values = frozenset((value,))
    elif isinstance(value, frozenset):
        values = value
    else:
        raise ValueError(f"{field_name} has an invalid shape")
    if not values or not all(
        isinstance(item, str) and item and item == item.strip() for item in values
    ):
        raise ValueError(f"{field_name} is invalid")
    return values


def _aware_datetime(value: Any, field_name: str) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise ValueError(f"{field_name} must be timezone-aware")
    return value


def _required_text(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(f"{field_name} must be non-empty text")
    return value


def _tracing(
    tracer: Any,
    user_identity: str,
    environ: Mapping[str, str] | None,
) -> tuple[Any, TraceAttributePolicy]:
    if user_identity not in {"omit", "hmac"}:
        raise ValueError("tracing_user_identity must be 'omit' or 'hmac'.")
    active_tracer = tracer if tracer is not None else NoOpTracer()
    if not callable(getattr(active_tracer, "start_span", None)):
        raise TypeError("tracer must implement start_span().")
    policy = build_attribute_policy(
        SimpleNamespace(user_identity=user_identity), environ
    )
    return active_tracer, policy


def _request_context(
    *,
    subject: str,
    scopes: frozenset[str],
    consumer_class: str,
    tracer: Any,
    identity_policy: TraceAttributePolicy,
) -> RequestContext:
    attributes: dict[str, str] = {
        "skifer.trace_version": TRACE_FORMAT_VERSION,
    }
    pseudonym = identity_policy.pseudonymize(subject)
    if pseudonym is not None:
        attributes["user_pseudonym"] = pseudonym
    with configured_span_scope(
        tracer,
        MCP_AUTH_SPAN_NAME,
        attributes=attributes,
    ):
        trace_context = current_trace_context(tracer)
    return RequestContext(
        subject=subject,
        scopes=scopes,
        consumer_class=consumer_class,
        trace_context=trace_context,
    )


def _authentication_refused() -> MCPResourceError:
    return MCPResourceError(
        AUTHENTICATION_FAILED,
        "Bearer authentication failed.",
        "authentication_failed",
    )


__all__ = [
    "AUTHENTICATION_FAILED",
    "BearerTokenVerifier",
    "CERTIFICATION_OVERRIDE_SCOPE",
    "MCP_AUTH_SPAN_NAME",
    "VerifiedBearerToken",
    "create_http_context_provider",
    "create_stdio_context_provider",
]
