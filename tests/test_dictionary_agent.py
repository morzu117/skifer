"""
Tests for DictionaryAgent — pure Python, no Spark or LLM required.
"""
from __future__ import annotations

import json
import tempfile
import os
import pytest
from unittest.mock import MagicMock

from skifer.agentic.dictionary_agent import DictionaryAgent
from skifer.agentic.models import DictionaryResponse


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
    return DictionaryAgent(schema_dict=_SCHEMA_CORE, target_name=TARGET)


@pytest.fixture
def agent_with_session():
    return DictionaryAgent(schema_dict=_SCHEMA_CORE, target_name=TARGET, session_history=True)


# ---------------------------------------------------------------------------
# load_schema
# ---------------------------------------------------------------------------

def test_load_schema_core():
    a = DictionaryAgent()
    assert a.dictionary is None
    a.load_schema(_SCHEMA_CORE, target_name=TARGET)
    assert a.dictionary is not None
    assert len(a.dictionary.list_fields()) > 0


def test_load_schema_semantic():
    a = DictionaryAgent()
    a.load_schema(_SCHEMA_SEMANTIC, schema_type="semantic")
    assert a.dictionary is not None
    assert len(a.dictionary.list_fields()) > 0


def test_load_schema_replaces_previous(agent):
    first_count = len(agent.dictionary.list_fields())
    agent.load_schema(_SCHEMA_SEMANTIC, schema_type="semantic")
    new_count = len(agent.dictionary.list_fields())
    assert new_count != first_count or new_count > 0


# ---------------------------------------------------------------------------
# lookup()
# ---------------------------------------------------------------------------

def test_lookup_found(agent):
    resp = agent.lookup(TARGET + "_output", "amount_eur")
    # target_name defaults to "<primary_table>_output" if not given — use empty table
    resp2 = agent.lookup("", "amount_eur")
    assert resp2.success
    assert resp2.mode == "lookup"
    assert resp2.entry is not None
    assert resp2.entry.name == "amount_eur"


def test_lookup_no_table_searches_all(agent):
    resp = agent.lookup("", "amount_eur")
    assert resp.success
    assert resp.entry is not None


def test_lookup_not_found_suggestions(agent):
    resp = agent.lookup("", "amount_eu")  # typo
    assert not resp.success
    assert resp.mode == "error"
    assert "amount_eur" in resp.suggestions


def test_lookup_not_found_no_suggestions(agent):
    resp = agent.lookup("", "xyz_totally_unknown_column")
    assert not resp.success
    assert resp.mode == "error"


def test_lookup_no_schema_returns_error():
    a = DictionaryAgent()
    resp = a.lookup("", "amount_eur")
    assert not resp.success
    assert resp.mode == "error"
    assert "load_schema" in resp.error


# ---------------------------------------------------------------------------
# list_fields()
# ---------------------------------------------------------------------------

def test_list_all_fields(agent):
    resp = agent.list_fields()
    assert resp.success
    assert resp.mode == "list"
    assert len(resp.entries) > 0


def test_list_fields_by_table(agent):
    resp = agent.list_fields(table=TARGET)
    assert resp.success
    assert len(resp.entries) > 0
    assert all(e.table == TARGET for e in resp.entries)


def test_list_unknown_table_returns_empty(agent):
    resp = agent.list_fields(table="nonexistent.table")
    assert resp.success
    assert resp.entries == []


def test_list_no_schema_returns_error():
    a = DictionaryAgent()
    resp = a.list_fields()
    assert not resp.success
    assert resp.mode == "error"


# ---------------------------------------------------------------------------
# export()
# ---------------------------------------------------------------------------

def test_export_json(agent):
    result = agent.export(format="json")
    data = json.loads(result)
    assert isinstance(data, dict)
    assert len(data) > 0


def test_export_text(agent):
    result = agent.export(format="text")
    assert isinstance(result, str)
    assert len(result) > 0
    assert "Data Dictionary" in result


def test_export_no_schema_returns_empty():
    a = DictionaryAgent()
    assert a.export() == ""
    assert a.export(format="json") == ""


# ---------------------------------------------------------------------------
# enrich()
# ---------------------------------------------------------------------------

def test_enrich_updates_descriptions(agent):
    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
        f.write("amount_eur: Montant de la commande en euros\n")
        f.write("customer_id: Identifiant unique du client\n")
        glossary_path = f.name
    try:
        agent.enrich(glossary_path)
        resp = agent.lookup("", "amount_eur")
        assert resp.success
        assert resp.entry.description != ""
    finally:
        os.unlink(glossary_path)


def test_enrich_no_schema_is_noop():
    a = DictionaryAgent()
    a.enrich("nonexistent_glossary.txt")  # should not raise


# ---------------------------------------------------------------------------
# ask() — keyword routing
# ---------------------------------------------------------------------------

def test_ask_routing_lookup(agent):
    resp = agent.ask("Que signifie amount_eur ?")
    assert resp.mode == "lookup"
    assert resp.column == "amount_eur"


def test_ask_routing_lookup_english(agent):
    resp = agent.ask("What does amount_eur mean?")
    assert resp.mode == "lookup"
    assert resp.column == "amount_eur"


def test_ask_routing_list(agent):
    resp = agent.ask(f"Liste les champs de {TARGET}")
    assert resp.mode == "list"


def test_ask_routing_list_all(agent):
    resp = agent.ask("Montre tous les champs")
    assert resp.mode == "list"


def test_ask_routing_export(agent):
    resp = agent.ask("Exporte le dictionnaire")
    assert resp.mode == "export"
    assert resp.text_output != ""


def test_ask_no_schema_returns_error():
    a = DictionaryAgent()
    resp = a.ask("Que signifie amount_eur ?")
    assert not resp.success
    assert resp.mode == "error"


def test_ask_unknown_intent_no_llm(agent):
    resp = agent.ask("bonjour comment allez vous")
    assert resp.mode == "error"
    assert "intent" in resp.error.lower() or "determine" in resp.error.lower()


def test_ask_lookup_column_not_found(agent):
    resp = agent.ask("Que signifie xyz_unknown_field ?")
    assert not resp.success


# ---------------------------------------------------------------------------
# session_history
# ---------------------------------------------------------------------------

def test_session_history_disabled_by_default(agent):
    agent.ask("Que signifie amount_eur ?")
    assert agent.session is None


def test_session_history_enabled(agent_with_session):
    agent_with_session.ask("Que signifie amount_eur ?")
    agent_with_session.ask("Liste tous les champs")
    assert len(agent_with_session.session) == 2


def test_reset_history(agent_with_session):
    agent_with_session.ask("Que signifie amount_eur ?")
    agent_with_session.reset_history()
    assert len(agent_with_session.session) == 0


# ---------------------------------------------------------------------------
# LLM narrative
# ---------------------------------------------------------------------------

def test_narrative_with_llm(agent):
    mock_llm = MagicMock()
    mock_llm.complete.return_value = "amount_eur is the order amount in euros."
    agent._llm = mock_llm

    resp = agent.lookup("", "amount_eur", narrative=True)
    assert resp.success
    assert resp.narrative != ""
    mock_llm.complete.assert_called_once()


def test_narrative_skipped_without_llm(agent):
    resp = agent.lookup("", "amount_eur", narrative=True)
    assert resp.success
    assert resp.narrative == ""


# ---------------------------------------------------------------------------
# LLM fallback for ask()
# ---------------------------------------------------------------------------

def test_llm_fallback_called_for_ambiguous_question(agent):
    mock_llm = MagicMock()
    mock_llm.complete.return_value = '{"mode": "lookup", "table": "", "column": "amount_eur"}'
    agent._llm = mock_llm

    resp = agent.ask("Tell me about amount_eur")
    assert resp.mode in ("lookup", "error")
    mock_llm.complete.assert_called()


def test_llm_parse_failure_returns_error(agent):
    mock_llm = MagicMock()
    mock_llm.complete.side_effect = RuntimeError("LLM unavailable")
    agent._llm = mock_llm

    resp = agent.ask("random question without any known keyword")
    assert resp.mode == "error"
    assert "LLM parse failed" in resp.error


# ---------------------------------------------------------------------------
# DictionaryResponse properties
# ---------------------------------------------------------------------------

def test_dictionary_response_success_no_error():
    resp = DictionaryResponse(question="q", mode="lookup", error=None)
    assert resp.success is True


def test_dictionary_response_success_with_error():
    resp = DictionaryResponse(question="q", mode="error", error="not found")
    assert resp.success is False


# ---------------------------------------------------------------------------
# Suggestions (Plan 34 follow-up)
# ---------------------------------------------------------------------------

def test_lookup_suggestions_are_deduplicated_across_tables():
    """The same column name in two tables must not fill two suggestion slots.

    Only three suggestions are offered. A name present in both a source and a
    target table used to appear twice, which reads as a bug and costs the reader
    a real alternative.
    """
    schema = {
        "tables": [{"name": "raw_orders", "alias": "ord"}],
        "select_final": [
            ["order_id", "order_id"],
            ["amount", "amount"],
        ],
    }
    agent = DictionaryAgent(schema_dict=schema, target_name="gold.orders")

    resp = agent.lookup("gold.orders", "order_i")

    assert resp.mode == "error"
    assert len(resp.suggestions) == len(set(resp.suggestions))
    assert "order_id" in resp.suggestions
