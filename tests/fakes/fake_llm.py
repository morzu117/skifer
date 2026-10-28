"""Scripted LLM provider for tests: no network, no API key, every call recorded."""

from __future__ import annotations

import json
from typing import Any

from skifer.semantic.llm_provider import LLMProvider


class FakeLLM(LLMProvider):
    """Returns the scripted responses in order; a dict is sent as JSON text.

    Running out of responses fails the test loudly rather than looping.
    """

    def __init__(self, *responses: Any, model: str = "fake-model"):
        self._responses = list(responses)
        self._model = model
        self.calls: list[dict[str, Any]] = []

    @property
    def provider_name(self) -> str:
        return "fake"

    def _next(self) -> str:
        if not self._responses:
            raise AssertionError("FakeLLM has no scripted response left.")
        response = self._responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response if isinstance(response, str) else json.dumps(response)

    def complete(self, system_prompt, user_message, temperature=0.0, response_format=None, **kwargs):
        self.calls.append({"system": system_prompt, "user": user_message})
        return self._next()

    def complete_with_history(self, system_prompt, history, temperature=0.0, response_format=None,
                              **kwargs):
        self.calls.append({"system": system_prompt, "history": list(history)})
        return self._next()
