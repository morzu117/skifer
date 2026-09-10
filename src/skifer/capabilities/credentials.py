"""In-memory delegated credentials issued by an injected provider.

Skifer is not an authorization server: it never mints, signs, renews,
persists, or discovers credentials.  An application-owned provider issues each
short-lived lease just in time, and this module validates the provider's answer
before an executor may use it.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from threading import Lock
from typing import Callable, Protocol
from weakref import WeakSet

from .autonomy import AutonomyMode
from .models import CapabilityDefinition


MAX_CREDENTIAL_TTL = timedelta(minutes=15)
MAX_ACTIVE_CREDENTIAL_LEASES = 32


class CredentialError(RuntimeError):
    """A credential request or returned lease failed closed validation."""


def _require_aware(value: datetime, field_name: str) -> None:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise CredentialError(f"Credential field '{field_name}' must be timezone-aware.")


class CredentialLease:
    """A non-serializable, short-lived secret held only for an explicit reveal."""

    __slots__ = (
        "__subject",
        "__audience",
        "__scopes",
        "__expires_at",
        "__secret",
        "__weakref__",
    )

    def __init__(
        self,
        *,
        subject: str,
        audience: str,
        scopes: frozenset[str],
        expires_at: datetime,
        secret: str,
    ) -> None:
        if not isinstance(subject, str) or not subject:
            raise CredentialError("Credential field 'subject' must be non-empty text.")
        if not isinstance(audience, str) or not audience:
            raise CredentialError("Credential field 'audience' must be non-empty text.")
        if not isinstance(scopes, frozenset) or not all(
            isinstance(scope, str) and scope for scope in scopes
        ):
            raise CredentialError("Credential field 'scopes' must be a frozenset of text values.")
        _require_aware(expires_at, "expires_at")
        if not isinstance(secret, str) or not secret:
            raise CredentialError("Credential secret must be non-empty text.")
        self.__subject = subject
        self.__audience = audience
        self.__scopes = scopes
        self.__expires_at = expires_at
        self.__secret = secret

    @property
    def subject(self) -> str:
        return self.__subject

    @property
    def audience(self) -> str:
        return self.__audience

    @property
    def scopes(self) -> frozenset[str]:
        return self.__scopes

    @property
    def expires_at(self) -> datetime:
        return self.__expires_at

    def reveal(self) -> str:
        """Return the secret at the one explicit, greppable use boundary."""
        return self.__secret

    def is_valid(self, now: datetime) -> bool:
        """Return whether the lease is strictly unexpired at an aware instant."""
        _require_aware(now, "now")
        _require_aware(self.expires_at, "expires_at")
        return self.expires_at.astimezone(timezone.utc) > now.astimezone(timezone.utc)

    def assert_valid(self, now: datetime) -> None:
        """Refuse an expired lease with no clock tolerance."""
        if not self.is_valid(now):
            raise CredentialError("Credential field 'expires_at' is not in the future.")

    def __repr__(self) -> str:
        return (
            "CredentialLease(subject=<redacted>, audience=<redacted>, "
            "scopes=<redacted>, expires_at=<redacted>, secret=<redacted>)"
        )

    __str__ = __repr__

    def __reduce__(self):
        raise CredentialError("CredentialLease serialization is forbidden.")


class CredentialProvider(Protocol):
    """Application-owned source of delegated, short-lived credentials."""

    def issue(
        self,
        *,
        subject: str,
        audience: str,
        scopes: frozenset[str],
        ttl_seconds: int,
    ) -> CredentialLease: ...


def assert_no_credential_for_shadow(mode: AutonomyMode | None) -> None:
    """Enforce that shadow evaluation cannot touch a credential provider."""
    if mode is not None and not isinstance(mode, AutonomyMode):
        raise CredentialError("Credential autonomy mode must be an AutonomyMode.")
    if mode is AutonomyMode.SHADOW:
        raise CredentialError("Credential issuance is forbidden in SHADOW mode.")


class CredentialBroker:
    """Request and verify least-privilege leases without retaining their secrets."""

    def __init__(
        self,
        provider: CredentialProvider,
        *,
        clock: Callable[[], datetime],
        max_ttl: timedelta = MAX_CREDENTIAL_TTL,
    ) -> None:
        if not callable(getattr(provider, "issue", None)):
            raise TypeError("Credential provider must define issue().")
        if not callable(clock):
            raise TypeError("Credential broker clock must be callable.")
        if not isinstance(max_ttl, timedelta) or max_ttl <= timedelta(0):
            raise ValueError("Credential max_ttl must be a positive timedelta.")
        self._provider = provider
        self._clock = clock
        self._max_ttl = max_ttl
        self._active_leases: WeakSet[CredentialLease] = WeakSet()
        self._pending_issues = 0
        self._lease_lock = Lock()

    @property
    def max_ttl_seconds(self) -> int:
        """Expose the enforced ceiling for callers selecting a shorter request."""
        return int(self._max_ttl.total_seconds())

    def issue_for(
        self,
        definition: CapabilityDefinition,
        *,
        subject: str,
        audience: str,
        ttl_seconds: int,
        mode: AutonomyMode | None = None,
    ) -> CredentialLease:
        """Issue exactly one validated lease for the capability's declared scopes."""
        if not isinstance(definition, CapabilityDefinition):
            raise TypeError("definition must be a CapabilityDefinition.")
        assert_no_credential_for_shadow(mode)
        if not isinstance(subject, str) or not subject:
            raise CredentialError("Credential field 'subject' must be non-empty text.")
        if not isinstance(audience, str) or not audience:
            raise CredentialError("Credential field 'audience' must be non-empty text.")
        if not isinstance(ttl_seconds, int) or isinstance(ttl_seconds, bool) or ttl_seconds <= 0:
            raise CredentialError("Credential ttl_seconds must be a positive integer.")
        if timedelta(seconds=ttl_seconds) > self._max_ttl:
            raise CredentialError("Credential ttl_seconds exceeds the enforced TTL ceiling.")

        requested_scopes = frozenset(definition.required_scopes)
        with self._lease_lock:
            if (
                len(self._active_leases) + self._pending_issues
                >= MAX_ACTIVE_CREDENTIAL_LEASES
            ):
                raise CredentialError("Credential active lease count exceeds the enforced maximum.")
            self._pending_issues += 1

        lease = None
        try:
            provider_error_type = None
            try:
                lease = self._provider.issue(
                    subject=subject,
                    audience=audience,
                    scopes=requested_scopes,
                    ttl_seconds=ttl_seconds,
                )
            except Exception as exc:
                provider_error_type = type(exc).__name__
            if provider_error_type is not None:
                # Raise outside the provider's exception handler. This prevents
                # even ``__context__`` from retaining an exception whose message
                # may quote an identity or the credential itself.
                raise CredentialError(
                    f"Credential provider failed with {provider_error_type}."
                )
            now = self._clock()
            _require_aware(now, "now")
            self._verify_lease(
                lease,
                subject=subject,
                audience=audience,
                requested_scopes=requested_scopes,
                now=now,
            )
            with self._lease_lock:
                self._active_leases.add(lease)
            return lease
        finally:
            # The reservation remains part of the enforced count until a valid
            # lease has atomically replaced it (or validation has refused it).
            with self._lease_lock:
                self._pending_issues -= 1

    def assert_valid(self, lease: CredentialLease) -> None:
        """Recheck a lease against a fresh clock reading immediately before use."""
        if not isinstance(lease, CredentialLease):
            raise CredentialError("Credential field 'lease' must be a CredentialLease.")
        lease.assert_valid(self._clock())

    def _verify_lease(
        self,
        lease: CredentialLease,
        *,
        subject: str,
        audience: str,
        requested_scopes: frozenset[str],
        now: datetime,
    ) -> None:
        if not isinstance(lease, CredentialLease):
            raise CredentialError("Credential field 'lease' must be a CredentialLease.")
        if lease.subject != subject:
            raise CredentialError("Credential field 'subject' does not match the request.")
        if lease.audience != audience:
            raise CredentialError("Credential field 'audience' does not match the request.")
        if not lease.scopes.issubset(requested_scopes):
            raise CredentialError("Credential field 'scopes' exceeds the requested scope set.")
        _require_aware(lease.expires_at, "expires_at")
        lease.assert_valid(now)
        if lease.expires_at.astimezone(timezone.utc) > (
            now.astimezone(timezone.utc) + self._max_ttl
        ):
            raise CredentialError("Credential field 'expires_at' exceeds the TTL ceiling.")
