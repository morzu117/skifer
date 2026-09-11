"""Spark-free tests for the fail-closed local identity."""

import pytest

from skifer.services import (
    CERTIFICATION_OVERRIDE_SCOPE,
    LOCAL_DEFAULT_SCOPES,
    NAMED_SCOPES,
    InvalidRequest,
    LocalIdentity,
    RequestContext,
)
from skifer.services import identity as identity_module


def test_local_default_scopes_are_named_scopes_without_override():
    assert LOCAL_DEFAULT_SCOPES == NAMED_SCOPES
    assert CERTIFICATION_OVERRIDE_SCOPE not in LOCAL_DEFAULT_SCOPES


def test_local_identity_rejects_override_scope():
    with pytest.raises(InvalidRequest):
        LocalIdentity(
            subject="u", scopes=frozenset({CERTIFICATION_OVERRIDE_SCOPE})
        )


def test_local_identity_rejects_empty_subject():
    with pytest.raises(InvalidRequest):
        LocalIdentity(subject="  ")


def test_resolve_subject_non_empty(monkeypatch):
    monkeypatch.setattr(identity_module, "get_clean_username", lambda: "")

    assert LocalIdentity.resolve().subject == "local-user"


def test_to_request_context_carries_scopes():
    context = LocalIdentity(subject="local-user").to_request_context()

    assert isinstance(context, RequestContext)
    assert context.subject == "local-user"
    assert context.scopes == LOCAL_DEFAULT_SCOPES
    assert context.consumer_class == "local"


def test_escalation_impossible_via_named_scopes():
    assert CERTIFICATION_OVERRIDE_SCOPE not in NAMED_SCOPES
    for scope in LOCAL_DEFAULT_SCOPES:
        assert scope != CERTIFICATION_OVERRIDE_SCOPE


def test_mcp_server_uses_local_default_scopes_as_single_source():
    from skifer.mcp import server

    assert server.LOCAL_DEFAULT_SCOPES is LOCAL_DEFAULT_SCOPES
