"""
skifer.serving — MLflow ChatModel serving layer.

Exposes AgenticHub as an OpenAI-compatible REST endpoint on Databricks Model Serving.

Public exports:
    SkiferChatModel  — mlflow.pyfunc.ChatModel wrapping AgenticHub
    hub_response_to_text — convert any HubResponse to a plain text string
"""

from ._response_serializer import hub_response_to_text

__all__ = ["SkiferChatModel", "hub_response_to_text"]


def __getattr__(name: str):  # noqa: N807
    if name == "SkiferChatModel":
        from .chat_model import SkiferChatModel  # noqa: PLC0415
        return SkiferChatModel
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
