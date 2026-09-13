"""Tests for transport-neutral service context and named scopes."""

import pytest

from skifer.observability.tracing import TraceContext
from skifer.services.context import (
    CERTIFICATION_OVERRIDE_SCOPE,
    NAMED_SCOPES,
    LimitExceeded,
    RequestContext,
    ScopeDenied,
    ServiceLimits,
    require_scope,
)


def _context(*scopes: str) -> RequestContext:
    return RequestContext(
        subject="local-user",
        scopes=frozenset(scopes),
        consumer_class="local",
        trace_context=TraceContext(),
    )


def test_named_scopes_exclude_certification_override():
    assert CERTIFICATION_OVERRIDE_SCOPE not in NAMED_SCOPES


def test_named_scopes_exact_set():
    assert NAMED_SCOPES == frozenset(
        {
            "models:read",
            "contracts:read",
            "lineage:read",
            "query:execute",
            "project:read",
            "pipelines:write",
            "rules:write",
            "execute:run",
            "contracts:write",
            "incidents:write",
        }
    )


def test_require_scope_denies_missing():
    with pytest.raises(ScopeDenied):
        require_scope(_context(), "models:read")


def test_require_scope_denies_non_context():
    with pytest.raises(ScopeDenied):
        require_scope(object(), "x")


def test_service_limits_rejects_out_of_range():
    with pytest.raises(LimitExceeded):
        ServiceLimits(max_query_rows=10_000)
