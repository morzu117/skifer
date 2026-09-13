"""Fail-closed identity for local clients and the MCP stdio transport."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from skifer.core.environment import get_clean_username
from skifer.observability.tracing import NoOpTracer, current_trace_context
from skifer.services.context import (
    CERTIFICATION_OVERRIDE_SCOPE,
    NAMED_SCOPES,
    InvalidRequest,
    RequestContext,
)


# The single authority set for local clients. NAMED_SCOPES deliberately excludes
# certification_override, which must never cross a local or MCP boundary.
LOCAL_DEFAULT_SCOPES: frozenset[str] = NAMED_SCOPES


@dataclass(frozen=True)
class LocalIdentity:
    """Static local identity that never derives authority from a request."""

    subject: str
    scopes: frozenset[str] = LOCAL_DEFAULT_SCOPES
    consumer_class: str = "local"

    def __post_init__(self) -> None:
        if not isinstance(self.subject, str) or not self.subject.strip():
            raise InvalidRequest("Local identity subject must be non-empty text.")
        if CERTIFICATION_OVERRIDE_SCOPE in self.scopes:
            raise InvalidRequest(
                "certification_override is never granted to a local identity."
            )

    @classmethod
    def resolve(
        cls, *, scopes: frozenset[str] = LOCAL_DEFAULT_SCOPES
    ) -> "LocalIdentity":
        return cls(subject=get_clean_username() or "local-user", scopes=scopes)

    def to_request_context(self, *, tracer: Any = None) -> RequestContext:
        active = tracer or NoOpTracer()
        return RequestContext(
            subject=self.subject,
            scopes=self.scopes,
            consumer_class=self.consumer_class,
            trace_context=current_trace_context(active),
        )


def local_request_context(*, tracer: Any = None) -> RequestContext:
    """Resolve the local OS identity without consulting a client request."""
    return LocalIdentity.resolve().to_request_context(tracer=tracer)


__all__ = [
    "LOCAL_DEFAULT_SCOPES",
    "LocalIdentity",
    "local_request_context",
]
