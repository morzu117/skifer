"""
Tests for QualityAgent — pure Python, no Spark or LLM required.
"""
from __future__ import annotations

import json
import pytest
from unittest.mock import MagicMock, patch

from skifer.agentic.quality_agent import QualityAgent
from skifer.agentic.models import QualityResponse
from skifer.observability.checks import NullCheck, VolumeCheck
from skifer.observability.monitor import MonitorReport


# ---------------------------------------------------------------------------
# Shared FakeBackend (same pattern as test_observability.py)
# ---------------------------------------------------------------------------

class FakeRow(dict):
    def __getattr__(self, item):
        try:
            return self[item]
        except KeyError:
            raise AttributeError(item)


class FakeResult:
    def __init__(self, rows):
        self._rows = [FakeRow(r) for r in rows]

    def collect(self):
        return self._rows


class FakeBackend:
    def __init__(self, default_rows=None, sql_override=None):
        self._default_rows = default_rows or []
        self._sql_override = sql_override
        self.queries = []

    def sql(self, query):
        self.queries.append(query)
        if self._sql_override:
            return FakeResult(self._sql_override(query))
        return FakeResult(self._default_rows)


FQN = "gold.fact_orders"

# Backend that returns 0 nulls (NullCheck passes) and 1000 rows (VolumeCheck passes)
def _smart_backend():
    def router(query):
        q = query.lower()
        if "count(*)" in q and "is null" in q:
            return [{"null_count": 0}]
        if "count(*)" in q:
            return [{"row_count": 1000}]
        return [{"result": 0}]
    return FakeBackend(sql_override=router)


@pytest.fixture
def backend():
    return _smart_backend()


_SCHEMA = {
    "tables": [{"name": FQN, "quality_checks": {"drop_nulls_in": ["amount"]}}],
    "select_final": [],
}


@pytest.fixture
def agent(backend):
    return QualityAgent(backend=backend, schema_dict=_SCHEMA)


@pytest.fixture
def agent_with_session(backend):
    return QualityAgent(backend=backend, schema_dict=_SCHEMA, session_history=True)


# ---------------------------------------------------------------------------
# check()
# ---------------------------------------------------------------------------

def test_check_with_contracts(agent):
    contracts = [NullCheck(table=FQN, column="amount", severity="critical")]
    resp = agent.check(FQN, contracts=contracts)
    assert isinstance(resp, QualityResponse)
    assert resp.mode == "check"
    assert resp.success
    assert resp.report is not None
    assert resp.text_output != ""


def test_check_from_schema(backend):
    schema = {
        "tables": [{"name": FQN, "quality_checks": {"drop_nulls_in": ["amount"]}}],
        "select_final": [],
    }
    ag = QualityAgent(backend=backend)
    resp = ag.check(FQN, schema_dict=schema)
    assert resp.mode == "check"
    assert resp.success
    assert resp.report is not None


def test_check_no_contracts_no_schema(agent):
    resp = agent.check(FQN)
    assert not resp.success
    assert resp.mode == "error"
    assert "contracts" in resp.error or "schema_dict" in resp.error


def test_check_passed_property(agent):
    contracts = [NullCheck(table=FQN, column="amount", severity="critical")]
    resp = agent.check(FQN, contracts=contracts)
    # NullCheck with 0 nulls → passed=True
    assert resp.passed is True


def test_check_failed_property():
    # Backend returns 5 nulls → NullCheck fails (critical)
    def router(query):
        q = query.lower()
        if "is null" in q:
            return [{"null_count": 5}]
        return [{"result": 0}]
    bad_backend = FakeBackend(sql_override=router)
    ag = QualityAgent(backend=bad_backend)
    contracts = [NullCheck(table=FQN, column="amount", severity="critical")]
    resp = ag.check(FQN, contracts=contracts)
    assert resp.passed is False


def test_check_backend_exception_produces_failed_report(backend):
    # DataMonitor catches exceptions inside evaluate() → failed CheckResult, not raised
    def boom(query):
        raise RuntimeError("DB unreachable")
    ag = QualityAgent(backend=FakeBackend(sql_override=boom))
    contracts = [NullCheck(table=FQN, column="amount")]
    resp = ag.check(FQN, contracts=contracts)
    # check() succeeds structurally but report contains a failed check
    assert resp.mode == "check"
    assert resp.report is not None
    assert len(resp.report.failures()) > 0


# ---------------------------------------------------------------------------
# get_history()
# ---------------------------------------------------------------------------

def test_get_history_no_store(agent):
    resp = agent.get_history(FQN)
    assert not resp.success
    assert resp.mode == "error"
    assert "history_store" in resp.error


def test_get_history_with_store(backend):
    mock_store = MagicMock()
    contracts = [NullCheck(table=FQN, column="amount")]
    fake_report = MonitorReport(table=FQN, results=[])
    mock_store.get_last_n.return_value = [fake_report, fake_report]

    ag = QualityAgent(backend=backend, history_store=mock_store)
    resp = ag.get_history(FQN, n=2)
    assert resp.success
    assert resp.mode == "history"
    assert len(resp.history_reports) == 2
    mock_store.get_last_n.assert_called_once_with(FQN, 2)


def test_get_history_store_exception(backend):
    mock_store = MagicMock()
    mock_store.get_last_n.side_effect = RuntimeError("store broken")
    ag = QualityAgent(backend=backend, history_store=mock_store)
    resp = ag.get_history(FQN)
    assert not resp.success
    assert resp.mode == "error"


# ---------------------------------------------------------------------------
# report()
# ---------------------------------------------------------------------------

def test_report_no_store_returns_empty(agent):
    result = agent.report(FQN, format="text")
    assert result == ""


def test_report_text(backend):
    mock_store = MagicMock()
    mock_store.get_latest.return_value = MonitorReport(table=FQN, results=[])
    ag = QualityAgent(backend=backend, history_store=mock_store)
    result = ag.report(FQN, format="text")
    assert isinstance(result, str)
    assert len(result) > 0


def test_report_json(backend):
    mock_store = MagicMock()
    mock_store.get_latest.return_value = MonitorReport(table=FQN, results=[])
    ag = QualityAgent(backend=backend, history_store=mock_store)
    result = ag.report(FQN, format="json")
    data = json.loads(result)
    assert "table" in data or "summary" in data


def test_report_no_latest_returns_empty(backend):
    mock_store = MagicMock()
    mock_store.get_latest.return_value = None
    ag = QualityAgent(backend=backend, history_store=mock_store)
    result = ag.report(FQN)
    assert result == ""


# ---------------------------------------------------------------------------
# ask() — keyword routing
# ---------------------------------------------------------------------------

def test_ask_routing_check(agent):
    resp = agent.ask(f"La table {FQN} est-elle saine ?")
    assert resp.mode == "check"
    assert resp.table == FQN


def test_ask_routing_check_english(agent):
    resp = agent.ask(f"Check the quality of {FQN}")
    assert resp.mode == "check"


def test_ask_routing_history(backend):
    mock_store = MagicMock()
    mock_store.get_last_n.return_value = []
    ag = QualityAgent(backend=backend, history_store=mock_store)
    resp = ag.ask(f"Montre l'historique de {FQN}")
    assert resp.mode == "history"
    assert resp.table == FQN


def test_ask_routing_report(backend):
    mock_store = MagicMock()
    mock_store.get_latest.return_value = None
    ag = QualityAgent(backend=backend, history_store=mock_store)
    resp = ag.ask(f"Génère un rapport sur {FQN}")
    assert resp.mode == "report"


def test_ask_no_table_returns_error(agent):
    resp = agent.ask("La table est-elle saine ?")
    assert not resp.success
    assert resp.mode == "error"
    assert "table" in resp.error.lower() or "identify" in resp.error.lower()


def test_ask_unknown_intent_no_llm(agent):
    resp = agent.ask("bonjour comment allez vous")
    assert resp.mode == "error"
    assert "intent" in resp.error.lower() or "determine" in resp.error.lower()


# ---------------------------------------------------------------------------
# session_history
# ---------------------------------------------------------------------------

def test_session_history_disabled_by_default(agent):
    agent.ask(f"La table {FQN} est-elle saine ?")
    assert agent.session is None


def test_session_history_enabled(agent_with_session):
    agent_with_session.ask(f"La table {FQN} est-elle saine ?")
    agent_with_session.ask(f"Montre l'historique de {FQN}")
    assert len(agent_with_session.session) == 2


def test_reset_history(agent_with_session):
    agent_with_session.ask(f"La table {FQN} est-elle saine ?")
    agent_with_session.reset_history()
    assert len(agent_with_session.session) == 0


# ---------------------------------------------------------------------------
# LLM narrative
# ---------------------------------------------------------------------------

def test_narrative_with_llm(backend):
    mock_llm = MagicMock()
    mock_llm.complete.return_value = "All 3 checks passed. The table is healthy."
    ag = QualityAgent(backend=backend, llm_provider=mock_llm)
    contracts = [NullCheck(table=FQN, column="amount")]
    resp = ag.check(FQN, contracts=contracts, narrative=True)
    assert resp.success
    assert resp.narrative != ""
    mock_llm.complete.assert_called_once()


def test_narrative_skipped_without_llm(agent):
    contracts = [NullCheck(table=FQN, column="amount")]
    resp = agent.check(FQN, contracts=contracts, narrative=True)
    assert resp.success
    assert resp.narrative == ""


# ---------------------------------------------------------------------------
# LLM fallback for ask()
# ---------------------------------------------------------------------------

def test_llm_parse_called_for_ambiguous_question(backend):
    mock_llm = MagicMock()
    mock_llm.complete.return_value = f'{{"mode": "check", "table": "{FQN}"}}'
    ag = QualityAgent(backend=backend, llm_provider=mock_llm)
    resp = ag.ask("Tell me if everything is fine")
    assert resp.mode in ("check", "error")
    mock_llm.complete.assert_called()


def test_llm_parse_failure_returns_error(backend):
    mock_llm = MagicMock()
    mock_llm.complete.side_effect = RuntimeError("LLM unavailable")
    ag = QualityAgent(backend=backend, llm_provider=mock_llm)
    resp = ag.ask("random question without keywords")
    assert resp.mode == "error"
    assert "LLM parse failed" in resp.error


# ---------------------------------------------------------------------------
# Alert dispatch
# ---------------------------------------------------------------------------

def test_alert_dispatch_called(backend):
    alert_cfg = {"webhook_url": "http://fake.webhook/notify"}
    ag = QualityAgent(backend=backend, alert_config=alert_cfg)

    with patch.object(ag._dispatcher, "dispatch", return_value=["webhook"]) as mock_dispatch:
        contracts = [NullCheck(table=FQN, column="amount")]
        resp = ag.check(FQN, contracts=contracts)
        assert resp.success
        mock_dispatch.assert_called_once()


# ---------------------------------------------------------------------------
# QualityResponse properties
# ---------------------------------------------------------------------------

def test_quality_response_success_no_error():
    resp = QualityResponse(question="q", mode="check", error=None)
    assert resp.success is True


def test_quality_response_success_with_error():
    resp = QualityResponse(question="q", mode="error", error="oops")
    assert resp.success is False


def test_quality_response_passed_no_report():
    resp = QualityResponse(question="q", mode="check")
    assert resp.passed is None


def test_quality_response_passed_with_report():
    report = MonitorReport(table=FQN, results=[])
    resp = QualityResponse(question="q", mode="check", report=report)
    assert resp.passed is True  # empty results = no critical failures
