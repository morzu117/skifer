"""
SkiferResponsesAgent — AgenticHub served as an MLflow ResponsesAgent (Plan 41.4).

Deployed on Databricks Model Serving, it answers in the Responses format that
Databricks agent tooling (Playground, supervisors) speaks. The evidence travels in
``custom_outputs["skifer"]``.

This module imports MLflow: install the ``serving`` extra. The decisions themselves
live in ``_agent_core`` and are tested without MLflow.

Expected artifacts at log time:
    "config"     — path to config.yaml
    "models_dir" — path to the semantic models directory

Expected environment variables at serving time:
    DATABRICKS_SERVERLESS_COMPUTE_ID — "auto", for Spark through Connect serverless
    DATABRICKS_HOST / DATABRICKS_TOKEN — workspace URL and credential
    DATABRICKS_LLM_MODEL — chat serving endpoint name, unless ``log_agent`` wrote
        it into the model configuration

Logging, serving and the Genie supervisor: docs/databricks_supervisor.md.
"""

from __future__ import annotations

from uuid import uuid4

from mlflow.pyfunc import ResponsesAgent
from mlflow.types.responses import ResponsesAgentRequest, ResponsesAgentResponse

from skifer.serving import _agent_core


class SkiferResponsesAgent(ResponsesAgent):
    """ResponsesAgent delegating every request to Skifer's AgenticHub."""

    def load_context(self, context) -> None:
        self._hub = _agent_core.build_hub(
            getattr(context, "artifacts", None), getattr(context, "model_config", None)
        )

    def __getstate__(self) -> dict:
        # MLflow calls load_context() and predict() on an input example while saving,
        # then pickles the instance. The hub holds a Spark session and an LLM client,
        # which do not pickle: it is rebuilt by load_context() on the serving side.
        state = self.__dict__.copy()
        state.pop("_hub", None)
        return state

    def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:
        question = _agent_core.extract_question(request.input)
        consumer = _agent_core.consumer_context_from_request(
            request.context, request.custom_inputs
        )
        text, custom_outputs = _agent_core.answer(self._hub, question, consumer)
        return ResponsesAgentResponse(
            id=str(uuid4()),
            output=[self.create_text_output_item(text=text, id=str(uuid4()))],
            custom_outputs=custom_outputs,
        )
