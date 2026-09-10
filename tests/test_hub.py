"""
Tests unitaires pour AgenticHub — Phase A.

Tous les tests sont sans LLM ni Spark : chaque agent est mocké avec MagicMock.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from skifer.agentic.hub import (
    _INTENT_TO_KEYWORDS,
    AgenticHub,
    _detect_intent,
    _normalize,
)
from skifer.agentic.models import (
    AgentResponse,
    BuilderResponse,
    CapabilityRequest,
    CapabilityResponse,
    DictionaryResponse,
    LineageResponse,
    QualityResponse,
)
from skifer.capabilities import CapabilityResult
from skifer.capabilities.registry import CapabilityRegistryError
from skifer.semantic.access_policy import ConsumerContext


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_lineage_response(q: str) -> LineageResponse:
    return LineageResponse(question=q, mode="trace")


def _make_dict_response(q: str) -> DictionaryResponse:
    return DictionaryResponse(question=q, mode="lookup")


def _make_quality_response(q: str) -> QualityResponse:
    return QualityResponse(question=q, mode="check")


def _make_builder_response(q: str) -> BuilderResponse:
    return BuilderResponse(success=True, yaml_content="tables: []")


def _make_genbi_response(q: str, **_kwargs) -> AgentResponse:
    return AgentResponse(question=q, mode="query")


# ---------------------------------------------------------------------------
# _normalize / _detect_intent (unité)
# ---------------------------------------------------------------------------

def test_normalize_strips_accents():
    assert _normalize("D'où") == "d'ou"
    assert _normalize("qualité") == "qualite"


def test_detect_intent_lineage():
    assert _detect_intent("D'où vient le champ amount_eur ?") == "lineage"
    assert _detect_intent("quelle est la provenance de ce champ") == "lineage"
    assert _detect_intent("upstream source de orders") == "lineage"


def test_detect_intent_dictionary():
    assert _detect_intent("que signifie gross_revenue ?") == "dictionary"
    assert _detect_intent("donne-moi la definition de amount_eur") == "dictionary"


def test_detect_intent_quality():
    assert _detect_intent("la table fact_orders est-elle saine ?") == "quality"
    assert _detect_intent("vérifie la qualite de la table") == "quality"
    assert _detect_intent("check les doublons") == "quality"


def test_detect_intent_builder():
    assert _detect_intent("cree un yaml pour orders") == "builder"
    assert _detect_intent("genere un pipeline qui joint orders et customers") == "builder"
    assert _detect_intent("construis le schema silver") == "builder"


def test_detect_intent_fallback_genbi():
    assert _detect_intent("quel est le CA par region ce trimestre ?") == "genbi"
    assert _detect_intent("montre-moi les ventes de juin") == "genbi"


def test_free_text_intents_contain_no_capability_route():
    assert "capability" not in {intent for intent, _keywords in _INTENT_TO_KEYWORDS}


# ---------------------------------------------------------------------------
# Phase A — Routage déterministe
# ---------------------------------------------------------------------------

def _hub_with_all_mocks():
    lineage = MagicMock()
    lineage.ask.side_effect = _make_lineage_response

    dictionary = MagicMock()
    dictionary.ask.side_effect = _make_dict_response

    quality = MagicMock()
    quality.ask.side_effect = _make_quality_response

    builder = MagicMock()
    builder.ask.return_value = _make_builder_response("mock")

    genbi = MagicMock()
    genbi.ask.side_effect = _make_genbi_response

    hub = AgenticHub(
        lineage_agent=lineage,
        quality_agent=quality,
        dictionary_agent=dictionary,
        builder_agent=builder,
        genbi_agent=genbi,
    )
    return hub, lineage, dictionary, quality, builder, genbi


def test_route_lineage_deterministic():
    hub, lineage, *_ = _hub_with_all_mocks()
    resp = hub.ask("D'où vient le champ amount_eur ?")
    assert isinstance(resp, LineageResponse)
    lineage.ask.assert_called_once()


def test_route_dictionary_deterministic():
    hub, _, dictionary, *_ = _hub_with_all_mocks()
    resp = hub.ask("que signifie gross_revenue ?")
    assert isinstance(resp, DictionaryResponse)
    dictionary.ask.assert_called_once()


def test_route_quality_deterministic():
    hub, _, _, quality, *_ = _hub_with_all_mocks()
    resp = hub.ask("la table fact_orders est-elle saine ?")
    assert isinstance(resp, QualityResponse)
    quality.ask.assert_called_once()


def test_route_builder_deterministic():
    hub, _, _, _, builder, _ = _hub_with_all_mocks()
    resp = hub.ask("cree un yaml pour orders et customers")
    assert isinstance(resp, BuilderResponse)
    builder.ask.assert_called_once()


def test_route_fallback_genbi():
    hub, _, _, _, _, genbi = _hub_with_all_mocks()
    resp = hub.ask("quel est le CA par region ce trimestre ?")
    assert isinstance(resp, AgentResponse)
    genbi.ask.assert_called_once()


def test_agent_not_configured():
    """Si l'agent cible est None, retourner AgentResponse mode='error'."""
    hub = AgenticHub()  # tous les agents None
    resp = hub.ask("D'où vient le champ amount_eur ?")
    assert isinstance(resp, AgentResponse)
    assert resp.mode == "error"
    assert "lineage" in resp.error.lower()


def test_agent_not_configured_genbi():
    """Fallback genbi absent → AgentResponse mode='error'."""
    hub = AgenticHub()
    resp = hub.ask("quel est le CA ?")
    assert isinstance(resp, AgentResponse)
    assert resp.mode == "error"
    assert "genbi" in resp.error.lower()


def test_hub_history_accessible():
    hub = AgenticHub(session_title="Test")
    assert hub.history is not None
    assert hub.history.title == "Test"


def test_builder_kwargs_forwarded():
    """output_dir doit être transmis à builder.ask()."""
    hub, _, _, _, builder, _ = _hub_with_all_mocks()
    hub.ask("cree un yaml", output_dir="my_schemas")
    _, call_kwargs = builder.ask.call_args
    assert call_kwargs.get("output_dir") == "my_schemas"


def test_hub_forwards_consumer_context_to_genbi_fallback():
    context = ConsumerContext(
        consumer_id="consumer-1",
        consumer_class="agent_read",
        user_id="user-1",
        trace_id="trace-1",
    )
    hub, _, _, _, _, genbi = _hub_with_all_mocks()

    hub.ask("quel est le CA par region ?", consumer_context=context)

    genbi.ask.assert_called_once_with(
        "quel est le CA par region ?",
        consumer_context=context,
    )


@pytest.mark.parametrize("llm_answer", ["capability", "inventory.lookup_record"])
def test_llm_cannot_authorize_a_capability(llm_answer: str):
    capability_invoker = MagicMock()
    llm = MagicMock()
    llm.complete.return_value = llm_answer
    genbi = MagicMock()
    genbi.ask.side_effect = _make_genbi_response
    hub = AgenticHub(
        llm_provider=llm,
        genbi_agent=genbi,
        capability_invoker=capability_invoker,
    )

    response = hub.ask("show inventory status")

    assert isinstance(response, AgentResponse)
    genbi.ask.assert_called_once()
    capability_invoker.invoke.assert_not_called()


def test_free_text_capability_id_still_routes_to_normal_agent():
    capability_invoker = MagicMock()
    genbi = MagicMock()
    genbi.ask.side_effect = _make_genbi_response
    hub = AgenticHub(genbi_agent=genbi, capability_invoker=capability_invoker)

    response = hub.ask("inventory.lookup_record")

    assert isinstance(response, AgentResponse)
    genbi.ask.assert_called_once_with(
        "inventory.lookup_record",
        consumer_context=None,
    )
    capability_invoker.invoke.assert_not_called()


def test_structured_capability_request_uses_dedicated_entry_point():
    result = CapabilityResult(
        capability_id="inventory.lookup_record",
        capability_version="1.2.0",
        status="succeeded",
        output={"found": True},
        error_type=None,
    )
    capability_invoker = MagicMock()
    capability_invoker.invoke.return_value = result
    hub = AgenticHub(capability_invoker=capability_invoker)
    request = CapabilityRequest(
        capability_id="inventory.lookup_record",
        arguments={"record_id": "item-1"},
    )

    response = hub.invoke_capability(request)

    assert isinstance(response, CapabilityResponse)
    assert response.to_dict()["result"]["output"] == {"found": True}
    capability_invoker.invoke.assert_called_once_with(
        "inventory.lookup_record",
        {"record_id": "item-1"},
    )


def test_structured_unregistered_capability_is_refused():
    capability_invoker = MagicMock()
    capability_invoker.invoke.side_effect = CapabilityRegistryError(
        "Capability id 'inventory.missing' is not present in the catalog."
    )
    hub = AgenticHub(capability_invoker=capability_invoker)

    with pytest.raises(CapabilityRegistryError, match="inventory.missing"):
        hub.invoke_capability(CapabilityRequest("inventory.missing", {}))
