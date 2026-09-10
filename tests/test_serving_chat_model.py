"""
Tests for serving.chat_model.SkiferChatModel and _extract_last_user_message.
"""

from __future__ import annotations

import sys
import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from skifer.serving.chat_model import (
    SkiferChatModel,
    _extract_last_user_message,
)
from skifer.agentic.models import (
    AgentResponse,
    LineageResponse,
)
from skifer.serving._response_serializer import hub_response_to_text
from skifer.observability.certification_store import SqliteCertificationStore
from skifer.semantic.access_policy import ConsumerContext
from skifer.semantic.access_policy import CertificationDecision, PolicyEvaluation
from skifer.semantic.evidence import SemanticEvidence, SourceEvidence


# ---------------------------------------------------------------------------
# _extract_last_user_message
# ---------------------------------------------------------------------------

def test_extract_last_user_message_basic():
    messages = [{"role": "user", "content": "Where does amount_eur come from?"}]
    assert _extract_last_user_message(messages) == "Where does amount_eur come from?"


def test_extract_last_user_message_picks_last():
    messages = [
        {"role": "user", "content": "First question"},
        {"role": "assistant", "content": "Answer"},
        {"role": "user", "content": "Follow-up question"},
    ]
    assert _extract_last_user_message(messages) == "Follow-up question"


def test_extract_last_user_message_no_user_raises():
    messages = [{"role": "assistant", "content": "Hello"}]
    with pytest.raises(ValueError, match="role='user'"):
        _extract_last_user_message(messages)


def test_extract_last_user_message_empty_raises():
    with pytest.raises(ValueError, match="role='user'"):
        _extract_last_user_message([])


# ---------------------------------------------------------------------------
# hub_response_to_text()
# ---------------------------------------------------------------------------

def test_hub_response_to_text_serializes_actionable_access_denial():
    response = AgentResponse(
        question="q",
        mode="error",
        error="SemanticAccessDenied(...)",
        access_denied=True,
        policy_decision="DENY",
        policy_reasons=("MISSING",),
        recommended_action="Contact the data owner...",
    )

    text = hub_response_to_text(response)

    assert "DENY" in text
    assert "MISSING" in text
    assert "Contact the data owner..." in text


def test_hub_response_to_text_keeps_technical_errors_unchanged():
    response = AgentResponse(
        question="q",
        mode="error",
        error="Table not found",
        access_denied=False,
    )

    assert hub_response_to_text(response) == "Error: Table not found"


# ---------------------------------------------------------------------------
# predict() — unit tests with a pre-wired hub
# ---------------------------------------------------------------------------

def _make_model_with_hub(hub_response) -> SkiferChatModel:
    """Return a SkiferChatModel whose hub.ask() returns hub_response."""
    model = SkiferChatModel()
    mock_hub = MagicMock()
    mock_hub.ask.return_value = hub_response
    model._hub = mock_hub
    return model


def _evidence(*, execution_status="succeeded"):
    now = datetime(2026, 9, 4, 12, tzinfo=timezone.utc)
    return SemanticEvidence(
        evidence_id="evidence-1",
        trace_id=None,
        model_keys=("sales.orders",),
        metrics=(),
        dimensions=(),
        normalized_filters=(
            {"column": "email", "operator": "eq", "value": "secret@example.com"},
        ),
        sources=(
            SourceEvidence(
                dataset="gold.fact_orders",
                contract_id=None,
                contract_version=None,
                definition_hash=None,
                certification_status="CERTIFIED",
                certified_at=now,
                load_age_seconds=7200,
                data_age_seconds=None,
                certification_run_id=None,
            ),
        ),
        policy=PolicyEvaluation(CertificationDecision.ALLOW, (), now),
        sql_hash="sha256:v1:hash",
        sql_text="SELECT * FROM orders WHERE email = 'secret@example.com'",
        statement_id=None,
        compiled_at=now,
        executed_at=now,
        execution_status=execution_status,
    )


def test_predict_returns_openai_format():
    r = AgentResponse(question="q", mode="query", explanation="All good.", result=None)
    model = _make_model_with_hub(r)

    result = model.predict(context=None, messages=[{"role": "user", "content": "Tell me something"}])

    assert "choices" in result
    assert len(result["choices"]) == 1
    choice = result["choices"][0]
    assert choice["message"]["role"] == "assistant"
    assert choice["finish_reason"] == "stop"
    assert isinstance(choice["message"]["content"], str)


def test_predict_keeps_choices_shape_for_legacy_clients_and_adds_versioned_evidence():
    response = AgentResponse(question="q", mode="query", explanation="All good.", evidence=_evidence())
    model = _make_model_with_hub(response)

    result = model.predict(context=None, messages=[{"role": "user", "content": "q"}])

    # An existing client reads exactly the old path and need not know metadata exists.
    legacy_content = result["choices"][0]["message"]["content"]
    assert legacy_content == "All good.\nProvenance : source gold.fact_orders · certifié le 2026-09-04 · fraîcheur 2 h"
    assert result["choices"] == [
        {
            "index": 0,
            "message": {"role": "assistant", "content": legacy_content},
            "finish_reason": "stop",
        }
    ]
    assert result["skifer"]["schema_version"] == "1"
    assert json.loads(json.dumps(result["skifer"])) == result["skifer"]
    encoded = json.dumps(result)
    assert "secret@example.com" not in encoded
    assert "SELECT" not in encoded


def test_predict_exposes_failed_evidence_without_presenting_it_as_complete():
    model = _make_model_with_hub(
        AgentResponse(question="q", mode="error", error="backend failed", evidence=_evidence(execution_status="failed"))
    )

    result = model.predict(context=None, messages=[{"role": "user", "content": "q"}])

    assert result["skifer"]["evidence"]["execution_status"] == "failed"
    assert "exécution incomplète (échec)" in result["choices"][0]["message"]["content"]


def test_predict_passes_last_user_message_to_hub():
    r = LineageResponse(question="q", diagram="graph LR; A-->B")
    model = _make_model_with_hub(r)

    messages = [
        {"role": "user", "content": "First"},
        {"role": "assistant", "content": "Reply"},
        {"role": "user", "content": "Where does amount_eur come from?"},
    ]
    model.predict(context=None, messages=messages)

    model._hub.ask.assert_called_once_with(
        "Where does amount_eur come from?",
        consumer_context=None,
    )


def test_predict_builds_consumer_context_from_params():
    r = AgentResponse(question="q", mode="query", explanation="All good.", result=None)
    model = _make_model_with_hub(r)

    model.predict(
        context=None,
        messages=[{"role": "user", "content": "q"}],
        params={
            "consumer_id": "consumer-1",
            "user_id": "user-1",
            "trace_id": "trace-1",
        },
    )

    _, call_kwargs = model._hub.ask.call_args
    assert call_kwargs["consumer_context"] == ConsumerContext(
        consumer_id="consumer-1",
        consumer_class="agent_read",
        user_id="user-1",
        trace_id="trace-1",
    )


def test_predict_lineage_response_content():
    r = LineageResponse(question="q", diagram="graph LR; A-->B")
    model = _make_model_with_hub(r)

    result = model.predict(context=None, messages=[{"role": "user", "content": "lineage?"}])
    assert result["choices"][0]["message"]["content"] == "graph LR; A-->B"


def test_predict_error_response_content():
    r = AgentResponse(question="q", mode="error", error="Table not found")
    model = _make_model_with_hub(r)

    result = model.predict(context=None, messages=[{"role": "user", "content": "q"}])
    assert "Table not found" in result["choices"][0]["message"]["content"]


def test_predict_no_user_message_raises():
    model = _make_model_with_hub(AgentResponse(question="q"))
    with pytest.raises(ValueError, match="role='user'"):
        model.predict(context=None, messages=[{"role": "assistant", "content": "hi"}])


# ---------------------------------------------------------------------------
# load_context() — test with sys.modules mocking (local imports inside method)
# ---------------------------------------------------------------------------

def test_load_context_wires_hub(monkeypatch, tmp_path):
    """load_context() should build an AgenticHub and store it as self._hub."""
    monkeypatch.chdir(tmp_path)
    mock_hub = MagicMock()
    mock_spark_session = MagicMock()

    # Build fake modules that load_context() will import locally
    fake_pyspark_sql = MagicMock()
    fake_pyspark_sql.SparkSession.builder.getOrCreate.return_value = mock_spark_session

    fake_hub_module = MagicMock()
    fake_hub_module.AgenticHub.return_value = mock_hub
    fake_semantic_module = MagicMock()

    fake_modules = {
        "pyspark": MagicMock(sql=fake_pyspark_sql),
        "pyspark.sql": fake_pyspark_sql,
        "skifer.core.config": MagicMock(ConfigurationManager=MagicMock()),
        "skifer.core.core": MagicMock(SkiferEngine=MagicMock()),
        "skifer.observability.certification_store": MagicMock(SqliteCertificationStore=SqliteCertificationStore),
        "skifer.semantic.llm_provider": MagicMock(get_llm_provider=MagicMock()),
        "skifer.semantic.semantic": fake_semantic_module,
        "skifer.agentic.hub": fake_hub_module,
    }

    mock_context = MagicMock()
    mock_context.artifacts = {"config": "config.yaml", "models_dir": "semantic"}

    model = SkiferChatModel()
    with patch.dict(sys.modules, fake_modules):
        model.load_context(mock_context)

    assert model._hub is mock_hub
    _, semantic_kwargs = fake_semantic_module.SemanticEngine.call_args
    assert isinstance(semantic_kwargs["certification_store"], SqliteCertificationStore)
    semantic_kwargs["certification_store"].close()
