"""
MLflow-free core of the serving agent (Plan 41.4).

Everything the endpoint decides lives here, so it is tested without MLflow installed:
which question is asked, who declares to be asking, what text and evidence go back,
and how the hub is built at pod start. ``responses_agent.py`` only adapts these
functions to ``mlflow.pyfunc.ResponsesAgent``.
"""

from __future__ import annotations

import logging
from typing import Any, Mapping

logger = logging.getLogger(__name__)

EVIDENCE_SCHEMA_VERSION = "2"


def build_hub(
    artifacts: Mapping[str, str] | None,
    model_config: Mapping[str, Any] | None = None,
) -> Any:
    """Build the AgenticHub served by the endpoint; called once, at pod start.

    No Spark session is passed to the engine: ``get_spark_session()`` picks
    Databricks Connect serverless when ``DATABRICKS_SERVERLESS_COMPUTE_ID`` is set,
    which is the only way a Model Serving container gets Spark.

    ``model_config["llm_endpoint"]``, written by ``log_agent``, names the chat endpoint;
    without it ``DATABRICKS_LLM_MODEL`` is required.
    """
    from skifer.agentic.hub import AgenticHub
    from skifer.core.core import SkiferEngine
    from skifer.observability.certification_store import default_certification_store
    from skifer.semantic.llm_provider import get_llm_provider
    from skifer.semantic.semantic import SemanticEngine

    artifacts = artifacts or {}
    config_path = artifacts.get("config", "config.yaml")
    models_dir = artifacts.get("models_dir", "semantic")

    engine = SkiferEngine(config_path=config_path)
    semantic = SemanticEngine(
        engine,
        models_dir=models_dir,
        certification_store=default_certification_store(engine),
    )
    llm_endpoint = (model_config or {}).get("llm_endpoint")
    llm = get_llm_provider("databricks", **({"model": llm_endpoint} if llm_endpoint else {}))
    candidate_agent = _build_candidate_agent(engine, semantic, llm)
    if candidate_agent is None:
        hub = AgenticHub(semantic_engine=semantic, llm_provider=llm)
    else:
        hub = AgenticHub(
            semantic_engine=semantic, llm_provider=llm, candidate_agent=candidate_agent
        )
    logger.info("Skifer serving agent: AgenticHub initialised.")
    return hub


def _build_candidate_agent(engine: Any, semantic: Any, llm: Any) -> Any:
    """The Plan 44 fallback agent, or None — nothing is opened when ``candidate_fallback`` is off."""
    from skifer.core.config import candidate_fallback_mode

    reader = getattr(semantic, "_env_config", None)
    env_config = reader() if callable(reader) else None
    if candidate_fallback_mode(env_config if isinstance(env_config, dict) else None) == "off":
        return None
    from skifer.agentic.candidate_agent import MetricCandidateAgent
    from skifer.candidates import default_candidate_store
    from skifer.knowledge import KnowledgeIndex, default_knowledge_store

    return MetricCandidateAgent(
        semantic,
        llm,
        KnowledgeIndex.from_store(default_knowledge_store(engine)),
        default_candidate_store(engine),
        policy=getattr(semantic.tracer, "attribute_policy", None),
    )


def _as_dict(item: Any) -> dict:
    if isinstance(item, Mapping):
        return dict(item)
    if hasattr(item, "model_dump"):
        return item.model_dump()
    return {}


def _content_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = [_as_dict(part).get("text") for part in content]
        return "\n".join(part for part in parts if isinstance(part, str))
    return ""


def extract_question(input_items: list[Any]) -> str:
    """Return the text of the last ``user`` item of a Responses ``input`` list.

    Items may be MLflow pydantic models or plain dicts, and a content may be a string
    or a list of text parts.
    """
    for item in reversed(input_items or []):
        data = _as_dict(item)
        if data.get("role") == "user":
            return _content_text(data.get("content"))
    raise ValueError("Skifer serving agent: no input item with role='user' in the request.")


def consumer_context_from_request(context: Any, custom_inputs: Mapping[str, Any] | None):
    """Build the ConsumerContext from client-declared request metadata, or None.

    ``consumer_id``, ``user_id`` and ``trace_id`` are declared by the client, not an
    authenticated Model Serving identity: they may feed consumer_class, logs and traces,
    never an authorization. ``scopes`` is therefore never populated here, whatever the
    request carries, so a caller cannot self-assign ``certification_override``.
    """
    from skifer.semantic.access_policy import ConsumerContext

    custom_inputs = custom_inputs or {}
    user_id = custom_inputs.get("user_id") or getattr(context, "user_id", None)
    consumer_id = custom_inputs.get("consumer_id") or user_id
    if not consumer_id:
        return None
    return ConsumerContext(
        consumer_id=consumer_id,
        consumer_class="agent_read",
        user_id=user_id,
        trace_id=custom_inputs.get("trace_id"),
    )


def answer(hub: Any, question: str, consumer_context: Any) -> tuple[str, dict | None]:
    """Ask the hub; return the answer text and the ``custom_outputs`` to send back.

    The evidence goes into ``custom_outputs["skifer"]``, the field Responses agents
    provide for it, versioned so a client can tell an evolution from a regression.
    """
    from skifer.observability.tracing import NoOpTracer, configured_span_scope
    from skifer.serving._markdown_renderer import hub_response_to_markdown
    from skifer.serving._response_serializer import semantic_evidence_to_dict

    response = hub.ask(question, consumer_context=consumer_context)
    tracer = getattr(hub, "tracer", None) or NoOpTracer()
    required = bool(getattr(hub, "_tracing_required", False))
    with configured_span_scope(tracer, "skifer.response.serialize", required=required):
        evidence = semantic_evidence_to_dict(getattr(response, "evidence", None))
        # Chat surfaces (Playground, supervisor) render the text as Markdown.
        text = hub_response_to_markdown(response, business_labels(hub, evidence))
    if evidence is None:
        return text, None
    return text, {"skifer": {"schema_version": EVIDENCE_SCHEMA_VERSION, "evidence": evidence}}


def business_labels(hub: Any, evidence: Mapping[str, Any] | None) -> dict[str, str]:
    """Map the metric and dimension names of the answered models to their labels.

    A metric's ``label``, else its ``description``, read from the semantic models the
    evidence names. Presentation only, hence best-effort: a model that cannot be read
    leaves the technical names in place and never fails the answer.
    """
    semantic = getattr(hub, "_semantic_engine", None)
    if not evidence or semantic is None:
        return {}
    labels: dict[str, str] = {}
    for model_key in evidence.get("model_keys") or ():
        try:
            # Already loaded and cached by the query that produced the evidence.
            model = semantic._get_model(model_key)
        except Exception:
            logger.debug("Skifer serving agent: no labels for model %r.", model_key)
            continue
        if not isinstance(model, Mapping):
            continue
        for section in ("metrics", "dimensions"):
            items = model.get(section)
            if not isinstance(items, list):
                continue
            for item in items:
                if not isinstance(item, Mapping) or not isinstance(item.get("name"), str):
                    continue
                label = item.get("label") or item.get("description")
                if isinstance(label, str) and label.strip():
                    labels.setdefault(item["name"], label.strip())
    return labels
