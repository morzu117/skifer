"""
Tests unitaires pour UserProfile + Semantic Alias Layer — Phase C.
"""

from __future__ import annotations

import pathlib
from unittest.mock import MagicMock

import pytest

from skifer.agentic import user_profile
from skifer.agentic.user_profile import UserProfile, _normalize
from skifer.agentic.hub import AgenticHub
from skifer.agentic.models import AgentResponse, LineageResponse


# ---------------------------------------------------------------------------
# _normalize
# ---------------------------------------------------------------------------

def test_normalize_lower_and_strip_accents():
    assert _normalize("Ventes") == "ventes"
    assert _normalize("CA") == "ca"
    assert _normalize("réGion") == "region"


# ---------------------------------------------------------------------------
# resolve_alias
# ---------------------------------------------------------------------------

def test_resolve_known_alias():
    profile = UserProfile(aliases={"ventes": "catalog.silver.orders"})
    assert profile.resolve_alias("ventes") == "catalog.silver.orders"


def test_resolve_alias_case_insensitive():
    profile = UserProfile(aliases={"ca": "amount_eur"})
    assert profile.resolve_alias("CA") == "amount_eur"
    assert profile.resolve_alias("Ca") == "amount_eur"


def test_resolve_unknown_returns_none():
    profile = UserProfile()
    assert profile.resolve_alias("unknown_term") is None


# ---------------------------------------------------------------------------
# add_alias / reject_suggestion
# ---------------------------------------------------------------------------

def test_add_alias_persists(tmp_path, monkeypatch):
    """``add_alias`` sauvegarde de lui-même, sans ``save()`` explicite.

    Le chemin par défaut est redirigé : sans cela le test écrit dans le vrai ``$HOME`` de qui lance
    la suite, et ne vérifie que le ``save()`` explicite qui suivait — jamais la sauvegarde implicite
    qu'il prétend couvrir.
    """
    profile_path = tmp_path / "profile.yaml"
    monkeypatch.setattr(user_profile, "_PROFILE_PATH", profile_path)

    profile = UserProfile()
    profile.add_alias("ventes", "catalog.silver.orders")

    assert profile_path.exists()
    loaded = UserProfile.load(profile_path)
    assert loaded.resolve_alias("ventes") == "catalog.silver.orders"


def test_add_alias_never_touches_the_real_home(tmp_path, monkeypatch):
    """Garde-fou : la sauvegarde implicite ne doit jamais viser le dossier personnel."""
    monkeypatch.setattr(user_profile, "_PROFILE_PATH", tmp_path / "profile.yaml")
    monkeypatch.setattr(
        pathlib.Path, "home", lambda: pytest.fail("le profil a resolu $HOME pendant un test")
    )

    UserProfile().add_alias("ventes", "catalog.silver.orders")


def test_add_alias_normalizes_key():
    profile = UserProfile()
    profile.save = MagicMock()  # ne pas écrire sur le disque
    profile.add_alias("Réservations", "catalog.silver.bookings")
    assert profile.resolve_alias("reservations") == "catalog.silver.bookings"


def test_reject_suggestion():
    profile = UserProfile()
    profile.save = MagicMock()
    profile.reject_suggestion("env:equals:PROD")
    assert "env:equals:PROD" in profile.blacklist_suggestions
    # Deuxième appel ne duplique pas
    profile.reject_suggestion("env:equals:PROD")
    assert profile.blacklist_suggestions.count("env:equals:PROD") == 1


# ---------------------------------------------------------------------------
# save / load
# ---------------------------------------------------------------------------

def test_load_missing_file_returns_empty(tmp_path):
    profile = UserProfile.load(tmp_path / "nonexistent.yaml")
    assert profile.aliases == {}
    assert profile.default_filters == []
    assert profile.preferred_format == "table"
    assert profile.blacklist_suggestions == []


def test_save_and_reload(tmp_path):
    p = tmp_path / "profile.yaml"
    profile = UserProfile(
        aliases={"ca": "amount_eur"},
        default_filters=["region:equals:EMEA"],
        preferred_format="kpi",
        blacklist_suggestions=["env:equals:PROD"],
    )
    profile.save(p)

    loaded = UserProfile.load(p)
    assert loaded.aliases == {"ca": "amount_eur"}
    assert loaded.default_filters == ["region:equals:EMEA"]
    assert loaded.preferred_format == "kpi"
    assert "env:equals:PROD" in loaded.blacklist_suggestions


# ---------------------------------------------------------------------------
# Integration avec AgenticHub
# ---------------------------------------------------------------------------

def test_hub_resolves_alias_before_routing():
    """L'alias 'ventes' doit être remplacé par le FQN avant routage."""
    profile = UserProfile(aliases={"ventes": "catalog.silver.orders"})

    lineage = MagicMock()
    lineage.ask.return_value = LineageResponse(
        question="d'ou vient catalog.silver.orders", mode="trace"
    )

    hub = AgenticHub(lineage_agent=lineage, profile=profile)
    resp = hub.ask("d'ou vient ventes ?")

    # L'agent doit avoir reçu la question avec l'alias résolu
    call_args = lineage.ask.call_args[0][0]
    assert "catalog.silver.orders" in call_args


def test_hub_alias_no_substring_replacement():
    """L'alias 'ca' ne doit pas remplacer 'cascade' (frontière de mot)."""
    profile = UserProfile(aliases={"ca": "amount_eur"})

    genbi = MagicMock()
    genbi.ask.return_value = AgentResponse(question="cascade", mode="query")

    hub = AgenticHub(genbi_agent=genbi, profile=profile)
    hub.ask("montre-moi la cascade")

    call_args = genbi.ask.call_args[0][0]
    # "cascade" ne doit pas être altéré
    assert "cascade" in call_args
    assert "amount_eurscade" not in call_args


def test_hub_no_profile_no_error():
    """Sans profil, le hub fonctionne normalement."""
    hub = AgenticHub()
    resp = hub.ask("quel est le CA ?")
    assert isinstance(resp, AgentResponse)
    assert resp.mode == "error"  # genbi non configuré
