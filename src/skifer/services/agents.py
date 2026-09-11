"""Transport-neutral façades for conversational and builder agents."""

from __future__ import annotations

from typing import Any

from skifer.agentic.models import AgentResponse
from skifer.services.context import (
    RequestContext,
    SCOPE_PIPELINES_WRITE,
    SCOPE_QUERY_EXECUTE,
    SerializationError,
    require_scope,
)
from skifer.services.serialization import to_json_value


class AgentService:
    """Expose agent results as allowlisted JSON-native data and text."""

    def __init__(self, hub: Any, *, builder_agent: Any = None):
        self._hub = hub
        self._builder = builder_agent or getattr(hub, "_builder", None)

    def ask(
        self, ctx: RequestContext, question: str, profile: Any = None
    ) -> dict[str, Any]:
        require_scope(ctx, SCOPE_QUERY_EXECUTE)
        response = self._hub.ask(question, profile=profile)
        from skifer.serving._response_serializer import hub_response_to_text

        return {
            "text": hub_response_to_text(response),
            "response": _response_to_dict(response),
        }

    def build(
        self,
        ctx: RequestContext,
        description: str,
        output_dir: str = "schemas",
    ) -> dict[str, Any]:
        require_scope(ctx, SCOPE_PIPELINES_WRITE)
        if self._builder is None:
            raise RuntimeError("No BuilderAgent is configured.")
        response = self._builder.ask(description, output_dir=output_dir)
        return {
            "success": response.success,
            "yaml_content": response.yaml_content,
            "output_path": response.output_path,
            "error": response.error,
        }


def _response_to_dict(response: Any) -> dict[str, Any]:
    """Serialize only known response fields; never include result.data."""
    if isinstance(response, AgentResponse):
        result = response.result
        result_view: Any = None
        if isinstance(result, str):
            result_view = result
        elif result is not None:
            result_view = {
                "format": to_json_value(getattr(result, "format", None), "result.format"),
                "title": to_json_value(getattr(result, "title", ""), "result.title"),
                "kpi_value": to_json_value(
                    getattr(result, "kpi_value", None), "result.kpi_value"
                ),
                "kpi_label": to_json_value(
                    getattr(result, "kpi_label", None), "result.kpi_label"
                ),
                "text_summary": to_json_value(
                    getattr(result, "text_summary", None), "result.text_summary"
                ),
            }
        return {
            "question": response.question,
            "mode": response.mode,
            "model_used": response.model_used,
            "explanation": response.explanation,
            "query_params": to_json_value(response.query_params, "query_params"),
            "result": result_view,
            "error": response.error,
            "clarification_question": response.clarification_question,
            "suggestions": list(response.suggestions),
            "candidates": list(response.candidates),
            "access_denied": response.access_denied,
            "policy_decision": response.policy_decision,
            "policy_reasons": (
                list(response.policy_reasons)
                if response.policy_reasons is not None
                else None
            ),
            "recommended_action": response.recommended_action,
        }

    converter = getattr(response, "to_dict", None)
    if converter is None or not callable(converter):
        raise SerializationError(
            f"Unsupported hub response type '{type(response).__name__}'."
        )
    payload = converter()
    if not isinstance(payload, dict):
        raise SerializationError("Hub response to_dict() must return a mapping.")
    return to_json_value(payload, "response")


__all__ = ["AgentService"]
