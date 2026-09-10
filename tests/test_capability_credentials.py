"""Delegated credentials stay bounded, ephemeral, and unobservable."""

from __future__ import annotations

from copy import copy
from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
import json
import logging
from pathlib import Path
import pickle
import traceback
from typing import Any

import pytest

from skifer.capabilities import (
    ActingAs,
    ApprovalMode,
    AutonomyMode,
    CapabilityDefinition,
    CapabilityMode,
    CredentialBroker,
    CredentialError,
    CredentialLease,
    MAX_ACTIVE_CREDENTIAL_LEASES,
    MAX_CREDENTIAL_TTL,
    Reversibility,
)


NOW = datetime(2026, 9, 8, 10, tzinfo=timezone.utc)
SECRET = "token-value-that-must-never-appear"


def _definition(*, scopes: tuple[str, ...] = ("tickets:read",)) -> CapabilityDefinition:
    return CapabilityDefinition(
        id="support.read_ticket",
        version="1.0.0",
        owner="support-platform",
        description="Read a ticket",
        mode=CapabilityMode.READ,
        executor="support_read",
        acting_as=ActingAs.DELEGATED_USER,
        required_scopes=scopes,
        input_schema={
            "type": "object",
            "additionalProperties": False,
            "required": [],
            "properties": {},
        },
        preconditions=(),
        reversibility=Reversibility.REVERSIBLE,
        compensation=None,
        approval=ApprovalMode.SUPERVISED,
        idempotency_key=None,
        policy_uri="policies/support.md",
        policy_hash="sha256:" + "a" * 64,
    )


def _lease(
    *,
    subject: str = "user-1",
    audience: str = "support-api",
    scopes: frozenset[str] = frozenset({"tickets:read"}),
    expires_at: datetime = NOW + timedelta(minutes=5),
) -> CredentialLease:
    return CredentialLease(
        subject=subject,
        audience=audience,
        scopes=scopes,
        expires_at=expires_at,
        secret=SECRET,
    )


class Provider:
    def __init__(self, factory=None) -> None:
        self.calls: list[dict[str, Any]] = []
        self.factory = factory

    def issue(self, **kwargs):
        self.calls.append(kwargs)
        if self.factory is not None:
            return self.factory(**kwargs)
        return _lease(
            subject=kwargs["subject"],
            audience=kwargs["audience"],
            scopes=kwargs["scopes"],
        )


def test_lease_secret_is_revealed_only_explicitly_and_never_rendered(caplog) -> None:
    lease = _lease()
    rendered = [repr(lease), str(lease), f"{lease}"]
    with caplog.at_level(logging.INFO):
        logging.getLogger(__name__).info("lease=%s", lease)
    rendered.append(caplog.text)

    with pytest.raises(TypeError) as json_error:
        json.dumps(lease)
    rendered.append(str(json_error.value))
    with pytest.raises(CredentialError) as pickle_error:
        pickle.dumps(lease)
    rendered.append(str(pickle_error.value))
    with pytest.raises(CredentialError) as copy_error:
        copy(lease)
    rendered.append(str(copy_error.value))

    try:
        with nullcontext(lease) as held_lease:
            assert held_lease.reveal() == SECRET
            raise RuntimeError("safe failure")
    except RuntimeError:
        rendered.append(traceback.format_exc())

    assert lease.reveal() == SECRET
    assert all(SECRET not in value for value in rendered)
    assert all(str(len(SECRET)) not in value for value in rendered[:3])
    assert not hasattr(lease, "to_dict")


def test_lease_exact_expiry_refuses_without_tolerance() -> None:
    lease = _lease(expires_at=NOW + timedelta(seconds=1))
    assert lease.is_valid(NOW)
    with pytest.raises(CredentialError, match="expires_at"):
        lease.assert_valid(NOW + timedelta(seconds=1))


@pytest.mark.parametrize("where", ["lease", "validity", "broker"])
def test_naive_datetime_anywhere_fails_closed(where: str) -> None:
    naive = NOW.replace(tzinfo=None)
    if where == "lease":
        with pytest.raises(CredentialError, match="expires_at"):
            _lease(expires_at=naive)
    elif where == "validity":
        with pytest.raises(CredentialError, match="now"):
            _lease().assert_valid(naive)
    else:
        with pytest.raises(CredentialError, match="now"):
            CredentialBroker(Provider(), clock=lambda: naive).issue_for(
                _definition(),
                subject="user-1",
                audience="support-api",
                ttl_seconds=60,
            )


def test_broker_refuses_a_provider_lease_mutated_to_naive_expiry() -> None:
    def factory(**kwargs):
        lease = _lease(
            subject=kwargs["subject"],
            audience=kwargs["audience"],
            scopes=kwargs["scopes"],
        )
        object.__setattr__(lease, "_CredentialLease__expires_at", NOW.replace(tzinfo=None))
        return lease

    with pytest.raises(CredentialError, match="expires_at"):
        CredentialBroker(Provider(factory), clock=lambda: NOW).issue_for(
            _definition(),
            subject="user-1",
            audience="support-api",
            ttl_seconds=60,
        )


def test_broker_requests_exact_declared_scope_set() -> None:
    provider = Provider()
    broker = CredentialBroker(provider, clock=lambda: NOW)
    definition = _definition(scopes=("tickets:read", "users:read"))

    lease = broker.issue_for(
        definition,
        subject="user-1",
        audience="support-api",
        ttl_seconds=60,
    )

    assert provider.calls == [
        {
            "subject": "user-1",
            "audience": "support-api",
            "scopes": frozenset(definition.required_scopes),
            "ttl_seconds": 60,
        }
    ]
    assert lease.scopes == frozenset(definition.required_scopes)


@pytest.mark.parametrize("ttl", [True, 0, -1, 1.5, 901])
def test_ttl_bound_is_an_enforced_refusal_not_a_clamp(ttl) -> None:
    provider = Provider()
    broker = CredentialBroker(provider, clock=lambda: NOW)
    with pytest.raises(CredentialError, match="ttl_seconds|TTL"):
        broker.issue_for(
            _definition(),
            subject="user-1",
            audience="support-api",
            ttl_seconds=ttl,
        )
    assert provider.calls == []


@pytest.mark.parametrize(
    "field,factory",
    [
        ("lease", lambda **_: object()),
        ("subject", lambda **kw: _lease(subject="other", audience=kw["audience"])),
        ("audience", lambda **kw: _lease(subject=kw["subject"], audience="other")),
        (
            "scopes",
            lambda **kw: _lease(
                subject=kw["subject"],
                audience=kw["audience"],
                scopes=kw["scopes"] | {"admin:all"},
            ),
        ),
        (
            "expires_at",
            lambda **kw: _lease(
                subject=kw["subject"], audience=kw["audience"], expires_at=NOW
            ),
        ),
        (
            "expires_at",
            lambda **kw: _lease(
                subject=kw["subject"],
                audience=kw["audience"],
                expires_at=NOW + MAX_CREDENTIAL_TTL + timedelta(seconds=1),
            ),
        ),
    ],
)
def test_broker_never_trusts_each_provider_field(field, factory) -> None:
    broker = CredentialBroker(Provider(factory), clock=lambda: NOW)
    with pytest.raises(CredentialError, match=field) as exc:
        broker.issue_for(
            _definition(),
            subject="user-1",
            audience="support-api",
            ttl_seconds=60,
        )
    assert SECRET not in str(exc.value)


def test_provider_failure_exposes_only_exception_class_even_in_traceback() -> None:
    class FailingProvider:
        def issue(self, **kwargs):
            raise RuntimeError(f"{SECRET}: {kwargs['subject']}")

    broker = CredentialBroker(FailingProvider(), clock=lambda: NOW)
    with pytest.raises(CredentialError, match="RuntimeError") as exc:
        broker.issue_for(
            _definition(),
            subject="private-user",
            audience="support-api",
            ttl_seconds=60,
        )
    rendered = "".join(traceback.format_exception(exc.value))
    assert SECRET not in rendered
    assert "private-user" not in rendered
    assert exc.value.__context__ is None


def test_shadow_refuses_before_touching_provider() -> None:
    provider = Provider()
    with pytest.raises(CredentialError, match="SHADOW"):
        CredentialBroker(provider, clock=lambda: NOW).issue_for(
            _definition(),
            subject="user-1",
            audience="support-api",
            ttl_seconds=60,
            mode=AutonomyMode.SHADOW,
        )
    assert provider.calls == []


def test_active_lease_count_is_an_enforced_refusal() -> None:
    provider = Provider()
    broker = CredentialBroker(provider, clock=lambda: NOW)
    leases = [
        broker.issue_for(
            _definition(),
            subject="user-1",
            audience="support-api",
            ttl_seconds=60,
        )
        for _ in range(MAX_ACTIVE_CREDENTIAL_LEASES)
    ]
    assert len(leases) == MAX_ACTIVE_CREDENTIAL_LEASES
    with pytest.raises(CredentialError, match="lease count"):
        broker.issue_for(
            _definition(),
            subject="user-1",
            audience="support-api",
            ttl_seconds=60,
        )
    assert len(provider.calls) == MAX_ACTIVE_CREDENTIAL_LEASES


def test_broker_recheck_uses_a_clock_that_moves_between_calls() -> None:
    instants = iter((NOW, NOW + timedelta(seconds=2)))
    provider = Provider(
        lambda **kw: _lease(
            subject=kw["subject"],
            audience=kw["audience"],
            scopes=kw["scopes"],
            expires_at=NOW + timedelta(seconds=1),
        )
    )
    broker = CredentialBroker(provider, clock=lambda: next(instants))
    lease = broker.issue_for(
        _definition(),
        subject="user-1",
        audience="support-api",
        ttl_seconds=1,
    )
    with pytest.raises(CredentialError, match="expires_at"):
        broker.assert_valid(lease)


def test_capability_surfaces_have_no_persisted_credential_material() -> None:
    root = Path(__file__).parents[1]
    capability_dir = root / "src" / "skifer" / "capabilities"
    yaml_files = list(root.glob("**/*capabilit*.yaml"))
    forbidden_yaml_keys = {
        "token:",
        "secret:",
        "password:",
        "credential:",
        "api_key:",
        "private_key:",
    }
    for path in yaml_files:
        content = path.read_text(encoding="utf-8").lower()
        assert not any(key in content for key in forbidden_yaml_keys), path

    forbidden_accesses = (
        '["token"]',
        "['token']",
        '.get("token")',
        ".get('token')",
        '["secret"]',
        "['secret']",
        '.get("secret")',
        ".get('secret')",
    )
    for path in capability_dir.glob("*.py"):
        if path.name == "credentials.py":
            continue
        content = path.read_text(encoding="utf-8").lower()
        assert not any(access in content for access in forbidden_accesses), path

    credential_source = (capability_dir / "credentials.py").read_text(encoding="utf-8")
    forbidden_implementation = (
        "os.environ",
        "os.getenv",
        "getenv(",
        "open(",
        "write_text(",
        "write_bytes(",
        "Path(",
        "DEFAULT_SECRET",
        "DEFAULT_TOKEN",
    )
    assert all(fragment not in credential_source for fragment in forbidden_implementation)
