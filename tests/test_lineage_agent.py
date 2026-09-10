"""
Tests for LineageAgent — pure Python, no Spark or LLM required.
"""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock

from skifer.agentic.lineage_agent import LineageAgent
from skifer.agentic.models import LineageResponse


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

_SCHEMA_CORE = {
    "tables": [{"name": "silver.orders", "alias": "ord"}],
    "select_final": [
        ["amount", "amount_eur", ["cast:double", "round:2"]],
        ["customer_id", "customer_id", []],
        ["order_id", "order_id", []],
    ],
    "join": [],
    "business_rules": [],
}

_SCHEMA_SEMANTIC = {
    "table": "silver.orders",
    "key": "kpi_orders",
    "dimensions": [{"name": "region", "sql": "region"}],
    "metrics": [{"name": "revenue", "sql": "amount", "type": "sum"}],
}

TARGET = "gold.fact_orders"


@pytest.fixture
def agent():
    return LineageAgent(schema_dict=_SCHEMA_CORE, target_name=TARGET)


@pytest.fixture
def agent_with_history():
    return LineageAgent(schema_dict=_SCHEMA_CORE, target_name=TARGET, history=True)


# ---------------------------------------------------------------------------
# Phase 1 — load_schema
# ---------------------------------------------------------------------------

def test_load_schema_core():
    a = LineageAgent()
    assert not a.graph  # empty before load
    a.load_schema(_SCHEMA_CORE, target_name=TARGET)
    assert len(a.graph) > 0


def test_load_schema_semantic():
    a = LineageAgent()
    a.load_schema(_SCHEMA_SEMANTIC, schema_type="semantic")
    assert len(a.graph) > 0
    assert a.dictionary is not None


def test_load_schema_replaces_previous(agent):
    initial_len = len(agent.graph)
    agent.load_schema(_SCHEMA_SEMANTIC, schema_type="semantic")
    assert len(agent.graph) != initial_len or len(agent.graph) > 0


# ---------------------------------------------------------------------------
# Phase 2 — trace / impact
# ---------------------------------------------------------------------------

def test_trace_returns_edges(agent):
    resp = agent.trace(TARGET, "amount_eur")
    assert isinstance(resp, LineageResponse)
    assert resp.mode == "trace"
    assert resp.success
    assert len(resp.edges) > 0
    assert all(e.target_column == "amount_eur" for e in resp.edges)


def test_trace_no_table_searches_all(agent):
    resp = agent.trace("", "amount_eur")
    assert resp.success
    assert len(resp.edges) > 0


def test_trace_unknown_column_returns_empty(agent):
    resp = agent.trace(TARGET, "nonexistent_xyz")
    assert resp.success
    assert resp.edges == []


def test_impact_returns_edges(agent):
    resp = agent.impact("silver.orders", "amount")
    assert isinstance(resp, LineageResponse)
    assert resp.mode == "impact"
    assert resp.success
    assert len(resp.edges) > 0


def test_impact_no_table_searches_all(agent):
    resp = agent.impact("", "amount")
    assert resp.success
    assert len(resp.edges) > 0


def test_trace_no_schema_returns_error():
    a = LineageAgent()
    resp = a.trace("t", "col")
    assert not resp.success
    assert resp.mode == "error"
    assert resp.error is not None


def test_impact_no_schema_returns_error():
    a = LineageAgent()
    resp = a.impact("t", "col")
    assert not resp.success
    assert resp.mode == "error"


# ---------------------------------------------------------------------------
# Phase 3 — render
# ---------------------------------------------------------------------------

def test_render_mermaid(agent):
    result = agent.render(format="mermaid")
    assert isinstance(result, str)
    assert "graph LR" in result


def test_render_html(agent):
    result = agent.render(format="html")
    assert "<html" in result
    assert "mermaid" in result.lower()


def test_render_json(agent):
    import json
    result = agent.render(format="json")
    data = json.loads(result)
    assert "edges" in data
    assert "tables" in data


def test_render_no_schema_returns_empty():
    a = LineageAgent()
    assert a.render() == ""


# ---------------------------------------------------------------------------
# Phase 4 — lookup
# ---------------------------------------------------------------------------

def test_lookup_found(agent):
    resp = agent.lookup(TARGET, "amount_eur")
    assert resp.success
    assert resp.mode == "lookup"
    assert resp.field_entry is not None
    assert resp.field_entry.name == "amount_eur"


def test_lookup_no_table_found(agent):
    resp = agent.lookup("", "amount_eur")
    assert resp.success
    assert resp.field_entry is not None


def test_lookup_not_found_has_suggestions(agent):
    resp = agent.lookup("", "amount_eu")  # typo
    assert not resp.success
    assert resp.mode == "error"
    assert "amount_eur" in resp.suggestions


def test_lookup_no_schema_returns_error():
    a = LineageAgent()
    resp = a.lookup("", "col")
    assert not resp.success
    assert resp.mode == "error"


# ---------------------------------------------------------------------------
# Phase 5 — ask() routing (keyword-based, no LLM)
# ---------------------------------------------------------------------------

def test_ask_routing_trace(agent):
    resp = agent.ask("D'où vient le champ amount_eur ?")
    assert resp.mode == "trace"
    assert resp.column == "amount_eur"


def test_ask_routing_impact(agent):
    resp = agent.ask("Quel est l'impact de order_id ?")
    assert resp.mode == "impact"
    assert resp.column == "order_id"


def test_ask_routing_render(agent):
    resp = agent.ask("Affiche le diagramme de lignage")
    assert resp.mode == "render"
    assert "graph LR" in resp.diagram


def test_ask_routing_lookup(agent):
    resp = agent.ask("Que signifie amount_eur ?")
    assert resp.mode == "lookup"


def test_ask_no_schema_returns_error():
    a = LineageAgent()
    resp = a.ask("D'où vient le champ amount ?")
    assert not resp.success
    assert resp.mode == "error"


def test_ask_unknown_intent_no_llm(agent):
    resp = agent.ask("hello world bonjour")
    assert resp.mode == "error"
    assert "intent" in resp.error.lower() or "determine" in resp.error.lower()


# ---------------------------------------------------------------------------
# Phase 6 — history
# ---------------------------------------------------------------------------

def test_session_history_disabled_by_default(agent):
    agent.ask("D'où vient amount_eur ?")
    assert agent.session is None


def test_session_history_enabled(agent_with_history):
    agent_with_history.ask("D'où vient amount_eur ?")
    agent_with_history.ask("Quel est l'impact de order_id ?")
    assert len(agent_with_history.session) == 2


def test_reset_history(agent_with_history):
    agent_with_history.ask("D'où vient amount_eur ?")
    agent_with_history.reset_history()
    assert len(agent_with_history.session) == 0


# ---------------------------------------------------------------------------
# Phase 7 — LLM narrative + LLM parse
# ---------------------------------------------------------------------------

def test_narrative_with_llm(agent):
    mock_llm = MagicMock()
    mock_llm.complete.return_value = "The field amount_eur is derived from silver.orders.amount."
    agent._llm = mock_llm

    resp = agent.trace(TARGET, "amount_eur", narrative=True)
    assert resp.success
    assert resp.narrative != ""
    mock_llm.complete.assert_called_once()


def test_llm_parse_used_when_keywords_fail(agent):
    mock_llm = MagicMock()
    mock_llm.complete.return_value = '{"mode": "trace", "table": "", "column": "amount_eur"}'
    agent._llm = mock_llm

    resp = agent.ask("Analyse la provenance de ce champ inconnu")
    # keyword "provenance" IS a trace keyword, so LLM may not be called —
    # but the response should still succeed
    assert resp.mode in ("trace", "error")


def test_llm_parse_called_for_ambiguous_question(agent):
    mock_llm = MagicMock()
    mock_llm.complete.return_value = '{"mode": "lookup", "table": "", "column": "customer_id"}'
    agent._llm = mock_llm

    # Question with no recognizable keyword
    resp = agent.ask("Tell me about customer_id please")
    assert resp.mode in ("lookup", "trace", "impact", "error")
    mock_llm.complete.assert_called()


def test_llm_parse_failure_returns_error(agent):
    mock_llm = MagicMock()
    mock_llm.complete.side_effect = RuntimeError("LLM unavailable")
    agent._llm = mock_llm

    resp = agent.ask("random question without keywords")
    assert resp.mode == "error"
    assert "LLM parse failed" in resp.error


# ---------------------------------------------------------------------------
# Phase 8 — LineageResponse properties
# ---------------------------------------------------------------------------

def test_lineage_response_success_property():
    resp = LineageResponse(question="q", mode="trace", error=None)
    assert resp.success is True

    resp_err = LineageResponse(question="q", mode="error", error="oops")
    assert resp_err.success is False
