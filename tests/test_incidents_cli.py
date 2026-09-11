"""Plan 31 slice 4.3: incident transitions through QualityService and CLI."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import sys
from unittest.mock import patch

import pytest

from skifer.cli import (
    INCIDENTS_EXIT_INVALID_TRANSITION,
    INCIDENTS_EXIT_NOT_FOUND,
    INCIDENTS_EXIT_OK,
    main,
    run_incidents_command,
)
from skifer.observability.certification_store import SqliteCertificationStore
from skifer.observability.incidents import Incident, IncidentStatus


def _incident(
    incident_id: str,
    *,
    check_name: str = "NullCheck:amount",
    target_fqn: str = "gold.orders",
    opened_at: datetime | None = None,
) -> Incident:
    return Incident(
        id=incident_id,
        target_fqn=target_fqn,
        run_id=incident_id.split(":", 1)[0],
        check_name=check_name,
        severity="critical",
        status=IncidentStatus.NEW,
        opened_at=opened_at or datetime(2026, 9, 11, tzinfo=timezone.utc),
    )


def _args(command: str, store, **values) -> argparse.Namespace:
    defaults = {
        "incidents_command": command,
        "store": str(store),
        "incident_id": "run-1:NullCheck:amount",
        "status": None,
        "target_fqn": None,
        "limit": 50,
        "assignee": None,
        "root_cause": None,
    }
    defaults.update(values)
    return argparse.Namespace(**defaults)


def _invoke(*args: str) -> int:
    with patch.object(sys, "argv", ["skifer", *args]), pytest.raises(SystemExit) as exc:
        main()
    return int(exc.value.code)


def test_cli_list_incidents(tmp_path, capsys):
    db = tmp_path / "certification.db"
    store = SqliteCertificationStore(str(db))
    store.open_incident(
        _incident(
            "run-1:NullCheck:amount",
            check_name="NullCheck:amount",
            opened_at=datetime(2026, 9, 11, tzinfo=timezone.utc),
        )
    )
    store.open_incident(
        _incident(
            "run-2:UniqueCheck",
            check_name="UniqueCheck",
            target_fqn="gold.customers",
            opened_at=datetime(2026, 9, 10, tzinfo=timezone.utc),
        )
    )

    assert run_incidents_command(_args("list", db)) == INCIDENTS_EXIT_OK

    payload = json.loads(capsys.readouterr().out)
    assert {item["id"] for item in payload} == {
        "run-1:NullCheck:amount",
        "run-2:UniqueCheck",
    }


def test_cli_ack_then_assign_then_resolve(tmp_path, capsys):
    db = tmp_path / "certification.db"
    store = SqliteCertificationStore(str(db))
    store.open_incident(_incident("run-1:NullCheck:amount"))

    assert run_incidents_command(_args("ack", db)) == INCIDENTS_EXIT_OK
    assert store.get_incident("run-1:NullCheck:amount").status is IncidentStatus.ACKNOWLEDGED

    assert (
        run_incidents_command(
            _args("assign", db, assignee="data-ops@example.com")
        )
        == INCIDENTS_EXIT_OK
    )
    assigned = store.get_incident("run-1:NullCheck:amount")
    assert assigned.status is IncidentStatus.ASSIGNED
    assert assigned.assignee == "data-ops@example.com"

    assert (
        run_incidents_command(
            _args("resolve", db, root_cause="source system fixed")
        )
        == INCIDENTS_EXIT_OK
    )
    resolved = store.get_incident("run-1:NullCheck:amount")
    assert resolved.status is IncidentStatus.RESOLVED
    assert resolved.root_cause == "source system fixed"
    assert resolved.resolved_at is not None
    output = capsys.readouterr().out
    assert '"status": "ACKNOWLEDGED"' in output
    assert '"status": "ASSIGNED"' in output
    assert '"status": "RESOLVED"' in output


def test_cli_invalid_transition_refused(tmp_path, capsys):
    db = tmp_path / "certification.db"
    store = SqliteCertificationStore(str(db))
    store.open_incident(_incident("run-1:NullCheck:amount"))
    store.update_incident(
        "run-1:NullCheck:amount",
        IncidentStatus.RESOLVED,
        at=datetime(2026, 9, 12, tzinfo=timezone.utc),
        root_cause="fixed",
    )

    assert (
        run_incidents_command(_args("ack", db))
        == INCIDENTS_EXIT_INVALID_TRANSITION
    )

    assert "Illegal incident transition" in capsys.readouterr().err
    assert store.get_incident("run-1:NullCheck:amount").status is IncidentStatus.RESOLVED


def test_cli_unknown_incident_not_found(tmp_path, capsys):
    db = tmp_path / "certification.db"

    assert (
        run_incidents_command(
            _args("resolve", db, incident_id="missing", root_cause="fixed")
        )
        == INCIDENTS_EXIT_NOT_FOUND
    )

    assert "not found" in capsys.readouterr().err


def test_cli_assign_requires_assignee():
    assert _invoke("incidents", "assign", "run-1:NullCheck:amount") == 2


def test_cli_resolve_requires_root_cause():
    assert _invoke("incidents", "resolve", "run-1:NullCheck:amount") == 2


def test_cli_incidents_help_exits_zero():
    assert _invoke("incidents", "--help") == 0
