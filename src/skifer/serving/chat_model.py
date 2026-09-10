"""
SkiferChatModel — MLflow ChatModel wrapping AgenticHub.

Exposes AgenticHub.ask() as an OpenAI-compatible /chat/completions endpoint
on Databricks Model Serving. load_context() is called once at pod startup;
predict() handles each inbound request.

Expected artifacts at log time:
    "config"     — path to config.yaml
    "models_dir" — path to the semantic models directory

Expected environment variables at serving time:
    DATABRICKS_HOST   — workspace URL
    DATABRICKS_TOKEN  — PAT or service principal secret
    DATABRICKS_LLM_MODEL — Foundation Model endpoint name (optional)
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


class SkiferChatModel:
    """
    MLflow pyfunc ChatModel that delegates to AgenticHub.

    Lazily imports mlflow so the class can be imported in environments
    where mlflow is not installed (e.g. during unit tests that mock it).
    """

    def load_context(self, context: Any) -> None:
        """
        Called once when the serving endpoint pod starts.

        Initialises SparkSession, ConfigurationManager, SkiferEngine,
        SemanticEngine, DatabricksLLMProvider, and AgenticHub. All heavy
        dependencies are imported here so the module itself stays importable
        without them installed.
        """
        from pyspark.sql import SparkSession

        from skifer.agentic.hub import AgenticHub
        from skifer.core.config import ConfigurationManager
        from skifer.core.core import SkiferEngine
        from skifer.observability.certification_store import SqliteCertificationStore
        from skifer.semantic.llm_provider import get_llm_provider
        from skifer.semantic.semantic import SemanticEngine

        artifacts = getattr(context, "artifacts", {}) or {}
        config_path = artifacts.get("config", "config.yaml")
        models_dir = artifacts.get("models_dir", "semantic")

        spark = SparkSession.builder.getOrCreate()
        config_mgr = ConfigurationManager(config_path=config_path)
        engine = SkiferEngine(spark, config_manager=config_mgr)
        # Safe when certification is off: policy modes that do not check certification never call the store.
        semantic = SemanticEngine(engine, models_dir=models_dir, certification_store=SqliteCertificationStore())
        llm = get_llm_provider("databricks")

        self._hub = AgenticHub(semantic_engine=semantic, llm_provider=llm)
        logger.info("SkiferChatModel: AgenticHub initialised.")

    def predict(
        self,
        context: Any,
        messages: list[dict[str, Any]],
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Handle a single chat completion request.

        Extracts the last user message from the OpenAI-format messages list,
        calls hub.ask(), and returns an OpenAI-compatible response dict.
        """
        from skifer.semantic.access_policy import ConsumerContext
        from skifer.observability.tracing import NoOpTracer, configured_span_scope
        from skifer.serving._response_serializer import (
            hub_response_to_text,
            semantic_evidence_to_dict,
        )

        question = _extract_last_user_message(messages)
        consumer_context = None
        if params and (params.get("consumer_id") or params.get("user_id")):
            # These params are client-declared metadata copied from the OpenAI-compatible request,
            # not an authenticated Model Serving identity. They may feed consumer_class/logs/traces,
            # but must never drive sensitive authorization decisions. In particular, scopes is
            # intentionally never populated from params here, so an API caller cannot self-assign
            # the certification_override scope.
            consumer_context = ConsumerContext(
                consumer_id=params.get("consumer_id") or params.get("user_id"),
                consumer_class="agent_read",
                user_id=params.get("user_id"),
                trace_id=params.get("trace_id"),
            )
        response = self._hub.ask(question, consumer_context=consumer_context)
        tracer = getattr(self._hub, "tracer", None) or NoOpTracer()
        required = bool(getattr(self._hub, "_tracing_required", False))
        with configured_span_scope(
            tracer,
            "skifer.response.serialize",
            required=required,
        ):
            text = hub_response_to_text(response)

            result = {
                "choices": [
                    {
                        "index": 0,
                        "message": {"role": "assistant", "content": text},
                        "finish_reason": "stop",
                    }
                ]
            }
            evidence = semantic_evidence_to_dict(getattr(response, "evidence", None))
            if evidence is not None:
                # `mlflow>=2.12,<3` is the only compatibility information known in
                # this repository.  MLflow is intentionally not installed in this
                # environment, so acceptance of a top-level extension has not been
                # empirically verified.  Keeping it beside (not inside) `choices`
                # is the least invasive option: legacy OpenAI clients read their
                # existing message shape byte-for-byte unchanged.
                result["skifer"] = {
                    "schema_version": "1",
                    "evidence": evidence,
                }
        return result


def _extract_last_user_message(messages: list[dict[str, Any]]) -> str:
    """
    Return the content of the last message with role='user'.

    Raises ValueError if no user message is found.
    """
    for msg in reversed(messages):
        if isinstance(msg, dict) and msg.get("role") == "user":
            return str(msg.get("content", ""))
    raise ValueError(
        "SkiferChatModel.predict(): no message with role='user' found in messages."
    )


def _build_mlflow_model() -> "SkiferChatModel":  # noqa: F821
    """
    Return a SkiferChatModel instance registered as an mlflow.pyfunc.ChatModel.

    Registers the mlflow.pyfunc flavour so the returned object can be passed
    directly to mlflow.pyfunc.log_model(python_model=...).

    This is a factory rather than direct inheritance because mlflow is an
    optional dependency — we avoid a top-level mlflow import.
    """
    import mlflow

    class _Registered(SkiferChatModel, mlflow.pyfunc.ChatModel):
        pass

    return _Registered()
