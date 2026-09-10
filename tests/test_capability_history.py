"""Append-only, bounded capability execution history contracts."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from skifer.capabilities import (
    CapabilityHistoryError,
    CapabilityStateEvent,
    CredentialLease,
    ExecutionState,
    MAX_EVENT_DETAIL_BYTES,
    MAX_HISTORY_PAGE_SIZE,
    SqliteCapabilityHistoryStore,
    request_hash,
)


NOW = datetime(2026, 9, 8, 12, tzinfo=timezone.utc)
REQUEST_HASH = request_hash(
    capability_id="support.create_ticket",
    capability_version="1.0.0",
    subject="user-1",
    arguments={"request_id": "request-1"},
)


def _event(
    event_id: str = "event-1",
    *,
    state: ExecutionState = ExecutionState.EXECUTING,
    detail=None,
) -> CapabilityStateEvent:
    return CapabilityStateEvent(
        event_id=event_id,
        request_hash=REQUEST_HASH,
        capability_id="support.create_ticket",
        capability_version="1.0.0",
        subject="user-1",
        state=state,
        occurred_at=NOW,
        detail=detail or {"kind": "external_call"},
    )


def test_sqlite_history_appends_ignores_duplicate_event_id_and_orders_ties(
    tmp_path: Path,
) -> None:
    store = SqliteCapabilityHistoryStore(str(tmp_path / "history.db"))
    first = _event()
    second = _event(
        "event-2",
        state=ExecutionState.EXECUTED,
        detail={"kind": "external_outcome", "result": {"status": "succeeded"}},
    )

    store.append(first)
    store.append(first)
    store.append(second)

    assert store.events_for(REQUEST_HASH) == [first, second]
    assert store.latest_state(REQUEST_HASH) == second


def test_event_to_dict_is_field_by_field_and_copies_detail() -> None:
    detail = {"nested": {"safe": [1]}}
    event = _event(detail=detail)
    detail["nested"]["safe"][0] = 2
    object.__setattr__(event, "future_private_field", "must-not-leak")

    payload = event.to_dict()
    assert set(payload) == {
        "event_id",
        "request_hash",
        "capability_id",
        "capability_version",
        "subject",
        "state",
        "occurred_at",
        "detail",
    }
    assert payload["detail"] == {"nested": {"safe": [1]}}
    assert "must-not-leak" not in str(payload)


def test_event_detail_byte_bound_is_an_enforced_refusal() -> None:
    with pytest.raises(CapabilityHistoryError, match="byte limit"):
        _event(detail={"safe": "x" * MAX_EVENT_DETAIL_BYTES})


def test_history_page_bound_is_an_enforced_refusal(tmp_path: Path) -> None:
    store = SqliteCapabilityHistoryStore(str(tmp_path / "history.db"))
    with pytest.raises(CapabilityHistoryError, match="between 1"):
        store.events_for(REQUEST_HASH, limit=MAX_HISTORY_PAGE_SIZE + 1)
    with pytest.raises(CapabilityHistoryError, match="between 1"):
        store.events_for(REQUEST_HASH, limit=0)


def test_secret_key_and_credential_lease_are_refused_before_storage(
    tmp_path: Path,
) -> None:
    lease = CredentialLease(
        subject="user-1",
        audience="support-api",
        scopes=frozenset({"tickets:create"}),
        expires_at=datetime(2026, 9, 8, 13, tzinfo=timezone.utc),
        secret="raw-secret",
    )
    store = SqliteCapabilityHistoryStore(str(tmp_path / "history.db"))

    with pytest.raises(CapabilityHistoryError, match="secret key"):
        store.append(_event(detail={"api_token": "raw-secret"}))
    with pytest.raises(CapabilityHistoryError, match="CredentialLease"):
        store.append(_event(detail={"lease": lease}))
    assert store._conn.execute("SELECT COUNT(*) FROM capability_state_events").fetchone() == (0,)


def test_history_source_has_no_sql_that_can_rewrite_or_remove_events() -> None:
    source = (
        Path(__file__).parents[1]
        / "src"
        / "skifer"
        / "capabilities"
        / "history.py"
    ).read_text(encoding="utf-8")
    assert "UPDATE " not in source
    assert "DELETE " not in source


@pytest.mark.parametrize(
    "occurred_at",
    [datetime(2026, 9, 8, 12), datetime(2026, 9, 8, 14, tzinfo=timezone.utc).astimezone()],
)
def test_event_requires_utc_not_merely_an_aware_datetime(occurred_at) -> None:
    if occurred_at.utcoffset() == timezone.utc.utcoffset(occurred_at) and occurred_at.tzinfo:
        pytest.skip("Local timezone is UTC, so this case is not non-UTC.")
    with pytest.raises(CapabilityHistoryError, match="UTC"):
        CapabilityStateEvent(
            event_id="event",
            request_hash=REQUEST_HASH,
            capability_id="support.create_ticket",
            capability_version="1.0.0",
            subject="user-1",
            state=ExecutionState.EXECUTING,
            occurred_at=occurred_at,
            detail={},
        )
