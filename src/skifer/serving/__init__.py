"""
skifer.serving — Databricks Model Serving layer.

Exposes AgenticHub as an MLflow ResponsesAgent on Databricks Model Serving.

Public exports:
    SkiferResponsesAgent — mlflow.pyfunc.ResponsesAgent wrapping AgenticHub (needs the serving extra)
    log_agent            — log the agent to MLflow with its artifacts and resources (needs the serving extra)
    hub_response_to_text — convert any HubResponse to a plain text string
"""

from ._response_serializer import hub_response_to_text

__all__ = ["SkiferResponsesAgent", "hub_response_to_text", "log_agent"]


def __getattr__(name: str):  # noqa: N807
    # Lazy: importing skifer.serving must not require MLflow.
    if name == "SkiferResponsesAgent":
        from .responses_agent import SkiferResponsesAgent  # noqa: PLC0415
        return SkiferResponsesAgent
    if name == "log_agent":
        from .deploy import log_agent  # noqa: PLC0415
        return log_agent
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
