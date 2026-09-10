"""
Tests unitaires pour SessionHistory et HistoryEntry.
"""
import json
import os
import datetime as dt
import pytest

from skifer.agentic.history import HistoryEntry, SessionHistory


# ---------------------------------------------------------------------------
# HistoryEntry
# ---------------------------------------------------------------------------

def test_history_entry_to_dict():
    entry = HistoryEntry(
        timestamp="2026-03-27T14:35:12",
        question="Quel est le CA ?",
        model_used="kpi_orders.erp",
        response_format="kpi",
        kpi_value=2847320.0,
        kpi_label="gross_revenue",
    )
    d = entry.to_dict()
    assert d["question"] == "Quel est le CA ?"
    assert d["model_used"] == "kpi_orders.erp"
    assert d["kpi_value"] == 2847320.0


# ---------------------------------------------------------------------------
# SessionHistory
# ---------------------------------------------------------------------------

def test_session_history_add_and_len():
    session = SessionHistory("Test Session")
    assert len(session) == 0

    entry = HistoryEntry(timestamp="2026-03-27T00:00:00", question="Q1")
    session.add(entry)
    assert len(session) == 1


def test_session_history_entries_returns_copy():
    session = SessionHistory()
    entry = HistoryEntry(timestamp="2026-03-27T00:00:00", question="Q1")
    session.add(entry)

    entries = session.entries()
    entries.clear()  # modifier la copie ne doit pas affecter la session
    assert len(session) == 1


def test_session_history_to_json(tmp_path):
    session = SessionHistory("Mon Rapport")
    session.add(HistoryEntry(
        timestamp="2026-03-27T10:00:00",
        question="CA par région ?",
        model_used="kpi_orders.erp",
        response_format="table",
    ))

    output_path = str(tmp_path / "rapport.json")
    session.to_json(output_path)

    assert os.path.exists(output_path)
    with open(output_path) as f:
        data = json.load(f)

    assert data["title"] == "Mon Rapport"
    assert data["total_entries"] == 1
    assert data["entries"][0]["question"] == "CA par région ?"


def test_session_history_from_json(tmp_path):
    """from_json doit reconstruire la session depuis le fichier."""
    session = SessionHistory("Original")
    session.add(HistoryEntry(
        timestamp="2026-03-27T10:00:00",
        question="Q1",
        model_used="kpi.erp",
        response_format="kpi",
    ))

    json_path = str(tmp_path / "test.json")
    session.to_json(json_path)

    loaded = SessionHistory.from_json(json_path)
    assert loaded.title == "Original"
    assert len(loaded) == 1
    assert loaded.entries()[0].question == "Q1"


def test_session_history_empty_to_json(tmp_path):
    session = SessionHistory("Empty")
    output_path = str(tmp_path / "empty.json")
    session.to_json(output_path)

    with open(output_path) as f:
        data = json.load(f)
    assert data["total_entries"] == 0
    assert data["entries"] == []


# ---------------------------------------------------------------------------
# Phase B — TTL + persistance
# ---------------------------------------------------------------------------

def test_session_not_expired():
    session = SessionHistory(ttl_days=15)
    assert not session.is_expired


def test_session_is_expired():
    session = SessionHistory(ttl_days=0)
    # TTL=0 jours → expirée si au moins 1 jour s'est écoulé
    # On force la date de création dans le passé
    session.created_at = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=1)
    assert session.is_expired


def test_session_id_generated():
    session = SessionHistory()
    assert session.session_id is not None
    assert len(session.session_id) == 36  # UUID4 format


def test_session_id_explicit():
    session = SessionHistory(session_id="my-custom-id")
    assert session.session_id == "my-custom-id"


def test_save_and_load(tmp_path, monkeypatch):
    import skifer.agentic.history as h_mod
    monkeypatch.setattr(h_mod, "_SESSIONS_DIR", tmp_path)

    session = SessionHistory("Saved Session", ttl_days=15)
    session.add(HistoryEntry(timestamp="2026-05-01T10:00:00", question="Q1"))
    session.save("test_scope")

    loaded = SessionHistory.load_or_create("test_scope", ttl_days=15)
    assert loaded.title == "Saved Session"
    assert len(loaded) == 1
    assert loaded.entries()[0].question == "Q1"


def test_purge_expired(tmp_path, monkeypatch):
    import skifer.agentic.history as h_mod
    monkeypatch.setattr(h_mod, "_SESSIONS_DIR", tmp_path)

    session = SessionHistory(ttl_days=0)
    session.created_at = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=2)
    session.save("expired_scope")

    removed = SessionHistory.purge_if_expired("expired_scope", ttl_days=0)
    assert removed == 1
    assert not (tmp_path / "expired_scope.json").exists()


def test_load_or_create_returns_new_if_expired(tmp_path, monkeypatch):
    import skifer.agentic.history as h_mod
    monkeypatch.setattr(h_mod, "_SESSIONS_DIR", tmp_path)

    old_session = SessionHistory(session_title="Old", ttl_days=0)
    old_session.created_at = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=2)
    old_session.save("scope_x")

    fresh = SessionHistory.load_or_create("scope_x", ttl_days=15)
    # Doit retourner une nouvelle session (pas l'ancienne expirée)
    assert fresh.title != "Old" or len(fresh) == 0


def test_backward_compat_existing_api(tmp_path):
    """to_json / from_json toujours fonctionnels."""
    session = SessionHistory("Compat")
    session.add(HistoryEntry(timestamp="2026-03-01T00:00:00", question="Q compat"))

    path = str(tmp_path / "compat.json")
    session.to_json(path)

    loaded = SessionHistory.from_json(path)
    assert loaded.title == "Compat"
    assert len(loaded) == 1
    assert loaded.entries()[0].question == "Q compat"
