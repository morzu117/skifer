"""Adversarial external dependencies for capability harness tests."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta, timezone
from typing import Any

from skifer.capabilities import CredentialLease


class ReplayLiveState:
    """Return copied live-state snapshots in their declared replay order."""

    def __init__(self, states: Sequence[Mapping[str, Any]]) -> None:
        self._states = [dict(state) for state in states]
        self.reads = 0

    def read(self) -> Mapping[str, Any]:
        if self.reads >= len(self._states):
            raise AssertionError("fake live-state replay exhausted")
        state = dict(self._states[self.reads])
        self.reads += 1
        return state


class FakeCredentialProvider:
    """Issue deterministic delegated leases while retaining no framework state."""

    def __init__(self, *, clock, secret: str) -> None:
        self._clock = clock
        self.secret = secret
        self.issue_calls: list[dict[str, Any]] = []

    def issue(self, *, subject, audience, scopes, ttl_seconds):
        self.issue_calls.append(
            {
                "subject": subject,
                "audience": audience,
                "scopes": frozenset(scopes),
                "ttl_seconds": ttl_seconds,
            }
        )
        return CredentialLease(
            subject=subject,
            audience=audience,
            scopes=frozenset(scopes),
            expires_at=self._clock() + timedelta(minutes=5),
            secret=self.secret,
        )


class FakeExternalSystem:
    """A bounded support-ticket service whose log is the side-effect authority."""

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.tickets: dict[str, dict[str, Any]] = {}
        self.timeout_requests: set[str] = set()
        self.compensation_failures: set[str] = set()
        self.returned_description = "ticket created"

    def call_log(self) -> tuple[str, ...]:
        return tuple(self.calls)

    def create_ticket(self, arguments, lease):
        lease.reveal()
        request_id = arguments["request_id"]
        self.calls.append(f"create:{request_id}")
        if request_id in self.timeout_requests:
            raise TimeoutError("DO_NOT_REPORT timeout response detail")
        ticket = {
            "ticket_id": f"ticket-{len(self.tickets) + 1}",
            "description": self.returned_description,
        }
        self.tickets[request_id] = ticket
        return dict(ticket)

    def lookup_ticket(self, arguments):
        ticket = self.tickets.get(arguments["request_id"])
        return dict(ticket) if ticket is not None else None

    def close_ticket(self, arguments, lease):
        lease.reveal()
        original = arguments["original_request_hash"]
        self.calls.append(f"close:{original}")
        if original in self.compensation_failures:
            raise RuntimeError("DO_NOT_REPORT compensation credential detail")
        return {"closed": True}


class RecoveringHistory:
    """Lose the first post-call journal response, then recover for reconciliation."""

    def __init__(self, delegate) -> None:
        self._delegate = delegate
        self._appends = 0
        self._failed = False

    def append(self, event) -> None:
        self._appends += 1
        if self._appends == 2 and not self._failed:
            self._failed = True
            raise OSError("journal response lost")
        self._delegate.append(event)

    def events_for(self, request_hash, *, limit=100):
        return self._delegate.events_for(request_hash, limit=limit)

    def latest_state(self, request_hash):
        return self._delegate.latest_state(request_hash)


def advancing_clock(start: datetime, *, seconds: int = 1):
    """Return an unbounded strictly advancing UTC test clock."""
    if start.tzinfo is None or start.utcoffset() != timedelta(0):
        raise ValueError("test clock start must be UTC")
    tick = -1

    def clock() -> datetime:
        nonlocal tick
        tick += 1
        return start.astimezone(timezone.utc) + timedelta(seconds=tick * seconds)

    return clock
