from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace
from urllib.error import URLError

import pytest

from skifer.observability.alerts import AlertDispatcher
from skifer.observability.checks import CheckResult, CheckStatus, NullCheck
from skifer.observability.incidents import AlertRouter
from skifer.observability.incidents import Recipient, incidents_from_report
from skifer.observability.monitor import MonitorReport


def _report(*, message: str = "amount failed", actual_value=None) -> MonitorReport:
    return MonitorReport(
        "silver.orders",
        [
            CheckResult(
                NullCheck(table="silver.orders", column="amount"),
                status=CheckStatus.FAIL,
                severity="critical",
                message=message,
                actual_value=actual_value,
                expected_value=0,
            )
        ],
        datetime(2026, 9, 11, tzinfo=timezone.utc),
    )


def test_msteams_channel_notified():
    dispatcher = AlertDispatcher()
    calls = []
    dispatcher._send_webhook = lambda url, payload: calls.append((url, payload))

    notified = dispatcher.dispatch(_report(), {"msteams_webhook": "http://teams"})

    assert notified == ["msteams"]
    assert calls[0][0] == "http://teams"
    assert calls[0][1]["@type"] == "MessageCard"
    assert calls[0][1]["sections"][1]["text"] == "[CRITICAL] NullCheck"


def test_google_chat_channel_notified():
    dispatcher = AlertDispatcher()
    calls = []
    dispatcher._send_webhook = lambda url, payload: calls.append((url, payload))

    notified = dispatcher.dispatch(_report(), {"google_chat_webhook": "http://chat"})

    assert notified == ["google_chat"]
    assert calls == [
        ("http://chat", {"text": "Table: silver.orders\n[CRITICAL] NullCheck"})
    ]


def test_failed_channel_isolated():
    dispatcher = AlertDispatcher()
    calls = []

    def send(url, payload):
        if url == "http://bad":
            raise URLError("boom")
        calls.append((url, payload))

    dispatcher._send_webhook = send

    with pytest.warns(RuntimeWarning, match="msteams send failed: URLError"):
        notified = dispatcher.dispatch(
            _report(),
            {
                "msteams_webhook": "http://bad",
                "google_chat_webhook": "http://good",
            },
        )

    assert notified == ["google_chat"]
    assert calls[0][0] == "http://good"


def test_no_data_value_in_incident_message():
    dispatcher = AlertDispatcher()
    calls = []
    dispatcher._send_webhook = lambda url, payload: calls.append(payload)
    report = _report(message="amount = 999.99", actual_value=999.99)
    incident = incidents_from_report(
        report,
        run_id="run-1",
        target_fqn="gold.orders",
        at=datetime(2026, 9, 11, tzinfo=timezone.utc),
    )[0]

    notified = dispatcher.dispatch_incident(
        target_fqn="gold.orders",
        incidents=[incident],
        recipients=[Recipient(contact="owner@example.com", team="data-team")],
        config={
            "webhook_url": "http://webhook",
            "msteams_webhook": "http://teams",
        },
    )

    serialized = json.dumps(calls, sort_keys=True)
    assert notified == ["webhook", "msteams"]
    assert "999.99" not in serialized
    assert "amount = 999.99" not in serialized
    assert "actual" not in serialized
    assert "expected" not in serialized
    assert "NullCheck:amount" in serialized
    assert "critical" in serialized


class _Governance:
    def get_dataset_owner(self, ctx, target_fqn):
        return {"contact": "owner@example.com", "team": "data-team"}

    def get_dataset(self, ctx, target_fqn):
        return SimpleNamespace(columns=[SimpleNamespace(name="amount")])

    def registry_downstream(self, ctx, fqn, column, max_depth=None):
        return []


def test_no_data_value_in_breaking_change_message():
    dispatcher = AlertDispatcher()
    calls = []
    dispatcher._send_webhook = lambda url, payload: calls.append(payload)
    router = AlertRouter(_Governance(), dispatcher)

    notified = router.alert_breaking_change(
        object(),
        target_fqn="gold.orders",
        diff=SimpleNamespace(
            breaking=True,
            from_version="1.0.0",
            to_version="2.0.0",
            message="amount = 999.99",
            actual_value=999.99,
            expected_value=0,
        ),
        config={"webhook_url": "http://webhook"},
    )

    serialized = json.dumps(calls, sort_keys=True)
    assert notified == ["webhook"]
    assert "999.99" not in serialized
    assert "amount = 999.99" not in serialized
    assert "actual" not in serialized
    assert "expected" not in serialized
    assert "1.0.0" in serialized
    assert "2.0.0" in serialized
