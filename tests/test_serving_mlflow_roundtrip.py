"""
SkiferResponsesAgent through real MLflow: save, load, predict (Plan 41.5).

This is the check that was missing when SkiferChatModel shipped: it could not even be
saved, because MLflow handed it objects it read as dicts. Skipped unless the serving
extra is installed; the CI job ``serving`` installs it and runs this file.
"""
from __future__ import annotations

import threading
from datetime import datetime, timezone

import pytest

mlflow = pytest.importorskip("mlflow")
pytest.importorskip("mlflow.types.responses")

from skifer.agentic.models import AgentResponse  # noqa: E402
from skifer.semantic.access_policy import CertificationDecision, PolicyEvaluation  # noqa: E402
from skifer.semantic.evidence import SemanticEvidence, SourceEvidence  # noqa: E402
from skifer.serving.responses_agent import SkiferResponsesAgent  # noqa: E402

REQUEST = {"input": [{"role": "user", "content": "CA par région ?"}]}
# Module-level: unwrap_python_model() does not reach a ResponsesAgent's instance.
HUB_CALLS: list = []


def _evidence():
    now = datetime(2026, 9, 4, 12, tzinfo=timezone.utc)
    return SemanticEvidence(
        evidence_id="evidence-1",
        trace_id=None,
        model_keys=("sales.orders",),
        metrics=(),
        dimensions=(),
        normalized_filters=(),
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
        sql_text="SELECT 1",
        statement_id=None,
        compiled_at=now,
        executed_at=now,
        execution_status="succeeded",
    )


class _Hub:
    """Stands in for AgenticHub. The lock makes it unpicklable, like a Spark session."""

    tracer = None
    _tracing_required = False

    def __init__(self):
        self._lock = threading.Lock()

    def ask(self, question, consumer_context=None):
        HUB_CALLS.append((question, consumer_context))
        return AgentResponse(question=question, mode="query", explanation="All good.", evidence=_evidence())


class _Agent(SkiferResponsesAgent):
    def load_context(self, context):
        self._hub = _Hub()


@pytest.fixture(autouse=True)
def tracking_in_tmp(tmp_path, monkeypatch):
    # MLflow 3 defaults to sqlite:///mlflow.db in the working directory.
    monkeypatch.setenv("MLFLOW_TRACKING_URI", f"sqlite:///{tmp_path / 'mlflow.db'}")


@pytest.fixture
def served(tmp_path):
    path = str(tmp_path / "model")
    mlflow.pyfunc.save_model(path=path, python_model=_Agent(), input_example=REQUEST)
    return mlflow.pyfunc.load_model(path)


def test_agent_saves_although_its_hub_does_not_pickle(served):
    # Saving runs load_context() + predict() then pickles: the hub must stay out.
    assert served is not None


def test_predict_sets_a_top_level_response_id(served):
    # The OpenAI Responses API schema treats "id" as required at the top level, even
    # though mlflow's own ResponsesAgentResponse leaves it optional (default None,
    # which the serializer then omits entirely) — strict clients validating against
    # that schema reject a response missing it.
    out = served.predict(REQUEST)

    assert isinstance(out["id"], str) and out["id"]


def test_predict_returns_text_and_evidence_in_custom_outputs(served):
    out = served.predict(REQUEST)

    message = out["output"][0]
    assert message["role"] == "assistant"
    assert message["content"][0]["text"].startswith("All good.")
    skifer = out["custom_outputs"]["skifer"]
    assert skifer["schema_version"] == "2"
    assert skifer["evidence"]["sql_hash"] == "sha256:v1:hash"
    assert "SELECT" not in str(out)


def test_request_scopes_never_reach_the_consumer_context(served):
    out = served.predict({
        **REQUEST,
        "custom_inputs": {"consumer_id": "c-1", "scopes": ["certification_override"]},
    })

    assert out["output"][0]["content"][0]["text"].startswith("All good.")
    _, consumer = HUB_CALLS[-1]
    assert consumer.consumer_id == "c-1"
    assert consumer.scopes == frozenset()


CANDIDATE_ID = "candidate:v1:" + "ab" * 32


class _CandidateHub(_Hub):
    def ask(self, question, consumer_context=None):
        from dataclasses import replace

        from skifer.semantic.evidence import MetricEvidence

        metric = MetricEvidence(
            name="revenue_by_segment", model_key=CANDIDATE_ID, definition_hash="sha256:v1:fp",
            source_columns=(), certification="CANDIDATE", candidate_id=CANDIDATE_ID,
        )
        evidence = replace(_evidence(), model_keys=(), metrics=(metric,), sql_text=None)
        return AgentResponse(question=question, mode="candidate", explanation="42",
                             evidence=evidence, candidate_id=CANDIDATE_ID)


class _CandidateAgent(SkiferResponsesAgent):
    def load_context(self, context):
        self._hub = _CandidateHub()


def test_candidate_response_roundtrip(tmp_path):
    path = str(tmp_path / "candidate-model")
    mlflow.pyfunc.save_model(path=path, python_model=_CandidateAgent(), input_example=REQUEST)
    out = mlflow.pyfunc.load_model(path).predict(REQUEST)

    assert out["output"][0]["content"][0]["text"].startswith(
        "> Réponse non certifiée — calculée hors du semantic layer, à valider.")
    skifer = out["custom_outputs"]["skifer"]
    assert skifer["schema_version"] == "2"
    [metric] = skifer["evidence"]["metrics"]
    assert metric["certification"] == "CANDIDATE" and metric["candidate_id"] == CANDIDATE_ID
