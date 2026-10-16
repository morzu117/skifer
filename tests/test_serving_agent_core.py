"""
Tests for serving._agent_core — the MLflow-free decisions of the serving agent (Plan 41.4).

The MLflow adapter itself (responses_agent.py) is exercised end to end by
test_serving_mlflow_roundtrip.py when MLflow is installed.
"""

from __future__ import annotations

import sys
import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from types import SimpleNamespace

from skifer.serving._agent_core import (
    answer,
    business_labels,
    build_hub,
    consumer_context_from_request,
    extract_question,
)
from skifer.agentic.models import (
    AgentResponse,
    FormattedResult,
    LineageResponse,
    ResponseFormat,
)
from skifer.serving._response_serializer import hub_response_to_text
from skifer.semantic.access_policy import ConsumerContext
from skifer.semantic.access_policy import CertificationDecision, PolicyEvaluation
from skifer.semantic.evidence import SemanticEvidence, SourceEvidence


# ---------------------------------------------------------------------------
# extract_question
# ---------------------------------------------------------------------------

def test_extract_question_basic():
    items = [{"role": "user", "content": "Where does amount_eur come from?"}]
    assert extract_question(items) == "Where does amount_eur come from?"


def test_extract_question_picks_last_user_item():
    items = [
        {"role": "user", "content": "First question"},
        {"role": "assistant", "content": "Answer"},
        {"role": "user", "content": "Follow-up question"},
    ]
    assert extract_question(items) == "Follow-up question"


def test_extract_question_joins_text_parts():
    items = [{"role": "user", "content": [
        {"type": "input_text", "text": "CA par région"},
        {"type": "input_text", "text": "pour janvier 2026"},
    ]}]
    assert extract_question(items) == "CA par région\npour janvier 2026"


def test_extract_question_reads_pydantic_like_items():
    class Item:
        def model_dump(self):
            return {"role": "user", "content": "from a model"}

    assert extract_question([Item()]) == "from a model"


def test_extract_question_no_user_raises():
    with pytest.raises(ValueError, match="role='user'"):
        extract_question([{"role": "assistant", "content": "Hello"}])


def test_extract_question_empty_raises():
    with pytest.raises(ValueError, match="role='user'"):
        extract_question([])


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
# answer() — text and custom_outputs, with a pre-wired hub
# ---------------------------------------------------------------------------

def _hub(hub_response):
    hub = MagicMock()
    hub.ask.return_value = hub_response
    hub.tracer = None
    hub._tracing_required = False
    return hub


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


def test_answer_without_evidence_has_no_custom_outputs():
    r = AgentResponse(question="q", mode="query", explanation="All good.", result=None)

    text, custom_outputs = answer(_hub(r), "Tell me something", None)

    assert isinstance(text, str)
    assert custom_outputs is None


def test_answer_puts_versioned_redacted_evidence_in_custom_outputs():
    response = AgentResponse(question="q", mode="query", explanation="All good.", evidence=_evidence())

    text, custom_outputs = answer(_hub(response), "q", None)

    # Rendered as Markdown since the Playground collapsed the plain-text line break.
    assert text == (
        "All good.\n\n---\n**Provenance**\n"
        "- Source : `gold.fact_orders`\n"
        "- Certification : ✅ certifiée le 2026-09-04\n"
        "- Fraîcheur : il y a 2 h"
    )
    assert custom_outputs["skifer"]["schema_version"] == "1"
    assert json.loads(json.dumps(custom_outputs)) == custom_outputs
    encoded = json.dumps(custom_outputs)
    assert "secret@example.com" not in encoded
    assert "SELECT" not in encoded


def test_answer_exposes_failed_evidence_without_presenting_it_as_complete():
    hub = _hub(AgentResponse(
        question="q", mode="error", error="backend failed", evidence=_evidence(execution_status="failed"),
    ))

    text, custom_outputs = answer(hub, "q", None)

    assert custom_outputs["skifer"]["evidence"]["execution_status"] == "failed"
    assert "Exécution incomplète (échec)" in text


def test_answer_passes_question_and_consumer_to_hub():
    hub = _hub(LineageResponse(question="q", diagram="graph LR; A-->B"))
    consumer = ConsumerContext(consumer_id="c-1", consumer_class="agent_read")

    text, _ = answer(hub, "Where does amount_eur come from?", consumer)

    hub.ask.assert_called_once_with("Where does amount_eur come from?", consumer_context=consumer)
    assert text == "graph LR; A-->B"


def test_answer_error_response_text():
    text, _ = answer(_hub(AgentResponse(question="q", mode="error", error="Table not found")), "q", None)
    assert "Table not found" in text


# ---------------------------------------------------------------------------
# consumer_context_from_request — declared metadata, never an authorization
# ---------------------------------------------------------------------------

def test_consumer_context_from_custom_inputs():
    consumer = consumer_context_from_request(
        None, {"consumer_id": "consumer-1", "user_id": "user-1", "trace_id": "trace-1"},
    )

    assert consumer == ConsumerContext(
        consumer_id="consumer-1",
        consumer_class="agent_read",
        user_id="user-1",
        trace_id="trace-1",
    )


def test_consumer_context_falls_back_to_request_context_user():
    consumer = consumer_context_from_request(SimpleNamespace(user_id="u-9", conversation_id="c"), None)

    assert consumer.consumer_id == "u-9"
    assert consumer.user_id == "u-9"


def test_consumer_context_is_none_without_identity():
    assert consumer_context_from_request(None, None) is None
    assert consumer_context_from_request(SimpleNamespace(user_id=None), {}) is None


def test_consumer_context_never_takes_scopes_from_the_request():
    consumer = consumer_context_from_request(
        None, {"consumer_id": "c-1", "scopes": ["certification_override"]},
    )

    assert "certification_override" not in tuple(consumer.scopes or ())


def test_consumer_context_never_takes_its_class_from_the_request():
    # consumer_class selects the certification policy applied: a client cannot pick it.
    consumer = consumer_context_from_request(
        None, {"consumer_id": "c-1", "consumer_class": "dashboard"},
    )

    assert consumer.consumer_class == "agent_read"


# ---------------------------------------------------------------------------
# build_hub() — wiring, with sys.modules mocking (local imports inside the function)
# ---------------------------------------------------------------------------

def test_build_hub_wires_engine_store_and_databricks_llm():
    mock_hub = MagicMock()
    engine = MagicMock(name="engine")
    store = MagicMock(name="store")
    fake_hub_module = MagicMock()
    fake_hub_module.AgenticHub.return_value = mock_hub
    fake_core = MagicMock()
    fake_core.SkiferEngine.return_value = engine
    fake_store_module = MagicMock()
    fake_store_module.default_certification_store.return_value = store
    fake_llm = MagicMock()
    fake_semantic_module = MagicMock()

    fake_modules = {
        "skifer.core.core": fake_core,
        "skifer.observability.certification_store": fake_store_module,
        "skifer.semantic.llm_provider": fake_llm,
        "skifer.semantic.semantic": fake_semantic_module,
        "skifer.agentic.hub": fake_hub_module,
    }

    with patch.dict(sys.modules, fake_modules):
        hub = build_hub({"config": "config.yaml", "models_dir": "semantic"})

    assert hub is mock_hub
    # No Spark session is injected: get_spark_session() picks Connect serverless.
    assert "spark" not in fake_core.SkiferEngine.call_args.kwargs
    # Regression: SkiferEngine takes config_path directly, not a config_manager kwarg
    # (that construction never existed on SkiferEngine and broke log_agent in production).
    assert fake_core.SkiferEngine.call_args.kwargs.get("config_path") == "config.yaml"
    fake_store_module.default_certification_store.assert_called_once_with(engine)
    _, semantic_kwargs = fake_semantic_module.SemanticEngine.call_args
    assert semantic_kwargs["certification_store"] is store
    assert semantic_kwargs["models_dir"] == "semantic"
    fake_llm.get_llm_provider.assert_called_once_with("databricks")


def test_build_hub_uses_the_llm_endpoint_from_model_config():
    fake_llm = MagicMock()
    fake_modules = {
        "skifer.core.core": MagicMock(),
        "skifer.observability.certification_store": MagicMock(),
        "skifer.semantic.llm_provider": fake_llm,
        "skifer.semantic.semantic": MagicMock(),
        "skifer.agentic.hub": MagicMock(),
    }

    with patch.dict(sys.modules, fake_modules):
        build_hub({}, {"llm_endpoint": "my-chat-endpoint"})

    fake_llm.get_llm_provider.assert_called_once_with("databricks", model="my-chat-endpoint")


# ---------------------------------------------------------------------------
# business_labels() — presentation only, never fails the answer
# ---------------------------------------------------------------------------

def _labelled_hub(response, model):
    hub = _hub(response)
    hub._semantic_engine._get_model.return_value = model
    return hub


def test_answer_renders_kpis_with_the_label_declared_in_the_model():
    response = AgentResponse(
        question="q",
        mode="query",
        result=FormattedResult(
            format=ResponseFormat.KPI, data=None, kpi_value=16200985.33, kpi_label="total_revenue",
        ),
        evidence=_evidence(),
    )
    model = {"metrics": [{"name": "total_revenue", "description": "CA total TTC (€)"}]}

    text, _ = answer(_labelled_hub(response, model), "Quel est le CA total ?", None)

    assert text.startswith("**CA total TTC (€)**\n\n### 16 200 985,33")


def test_business_labels_prefer_label_over_description_and_cover_dimensions():
    model = {
        "metrics": [{"name": "ca", "label": "CA", "description": "Chiffre d'affaires TTC"}],
        "dimensions": [{"name": "region", "description": "Région commerciale"}, "junk", {"name": 3}],
    }
    hub = _labelled_hub(None, model)

    assert business_labels(hub, {"model_keys": ["sales.orders"]}) == {
        "ca": "CA",
        "region": "Région commerciale",
    }


def test_business_labels_never_fail_the_answer():
    hub = _hub(None)
    hub._semantic_engine._get_model.side_effect = ValueError("unknown model")

    assert business_labels(hub, {"model_keys": ["missing"]}) == {}
    assert business_labels(hub, None) == {}
    assert business_labels(object(), {"model_keys": ["sales.orders"]}) == {}
