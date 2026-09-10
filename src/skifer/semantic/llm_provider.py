"""
LLMProvider — interface provider-agnostic pour les appels LLM.

Supporte : OpenAI, Anthropic, Google Gemini.
Configuration lue depuis les variables d'environnement (via python-dotenv).

Usage :
    from skifer.semantic.llm_provider import get_llm_provider
    llm = get_llm_provider()          # auto-detect depuis .env
    llm = get_llm_provider("openai")  # forcer un provider
"""

from __future__ import annotations

import os
from abc import ABC, abstractmethod
from contextlib import contextmanager
import time
from typing import Any, Iterator

from ..observability.tracing import (
    NoOpTracer,
    Tracer,
    configured_span_scope,
    set_span_attribute,
)


# ---------------------------------------------------------------------------
# ABC
# ---------------------------------------------------------------------------

class LLMProvider(ABC):
    """Interface commune à tous les providers LLM."""

    @abstractmethod
    def complete(
        self,
        system_prompt: str,
        user_message: str,
        temperature: float = 0.0,
        response_format: dict | None = None,
        **kwargs: Any,
    ) -> str:
        """
        Appel texte simple au LLM.

        Args:
            system_prompt:   Instructions système.
            user_message:    Question / contenu utilisateur.
            temperature:     0.0 = déterministe, 1.0 = créatif.
            response_format: Schema JSON pour les providers qui le supportent
                             (ex: {"type": "json_object"}).
            **kwargs:        Paramètres supplémentaires passés au provider.

        Returns:
            Contenu textuel de la réponse du LLM.
        """

    @abstractmethod
    def complete_with_history(
        self,
        system_prompt: str,
        history: list[dict],
        temperature: float = 0.0,
        response_format: dict | None = None,
        **kwargs: Any,
    ) -> str:
        """
        Appel LLM avec historique conversationnel multi-turn.

        Args:
            system_prompt: Instructions système.
            history:       Liste de tours [{role, content}]. Le dernier élément
                           doit avoir role="user".
            temperature:   0.0 = déterministe.
            response_format: Schema JSON optionnel.

        Returns:
            Contenu textuel de la réponse du LLM.
        """

    @property
    @abstractmethod
    def provider_name(self) -> str:
        """Nom lisible du provider (ex: 'openai', 'anthropic', 'google')."""

    @property
    def model_name(self) -> str:
        """Safe model label used by tracing without changing completion results."""
        model = getattr(self, "_model_name", None)
        if not isinstance(model, str):
            model = getattr(self, "_model", None)
        return model if isinstance(model, str) and model else "unknown"


@contextmanager
def llm_span_scope(
    provider: Any,
    tracer: Tracer | None,
    *,
    model_name: str | None = None,
    required: bool = False,
) -> Iterator[Any]:
    """Trace one existing provider call without retaining prompts or messages.

    Token counts are deliberately absent: the current public provider contract
    returns text only, so obtaining usage would require an additional call or a
    concurrency-unsafe side channel. Exporters may add them in a later slice
    when a provider already exposes usage alongside its response.
    """
    tracer = tracer or NoOpTracer()
    provider_name = getattr(provider, "provider_name", None)
    if not isinstance(provider_name, str) or not provider_name:
        provider_name = type(provider).__name__
    resolved_model = model_name or getattr(provider, "model_name", None)
    if not isinstance(resolved_model, str) or not resolved_model:
        candidate = getattr(provider, "_model", None)
        resolved_model = candidate if isinstance(candidate, str) else "unknown"
    started = time.monotonic()
    with configured_span_scope(
        tracer,
        "skifer.llm.complete",
        attributes={"provider": provider_name, "model": resolved_model},
        required=required,
    ) as span:
        try:
            yield span
        finally:
            set_span_attribute(
                span,
                "latency_seconds",
                max(0.0, time.monotonic() - started),
                required=required,
            )


# ---------------------------------------------------------------------------
# OpenAI
# ---------------------------------------------------------------------------

class OpenAIProvider(LLMProvider):
    """Provider OpenAI (GPT-4o, GPT-4 Turbo, …)."""

    def __init__(self, api_key: str | None = None, model: str | None = None, **kwargs: Any):
        try:
            import openai as _openai
        except ImportError as exc:
            raise ImportError(
                "[LLMProvider] 'openai' n'est pas installé. "
                "Faites : pip install openai"
            ) from exc

        self._client = _openai.OpenAI(
            api_key=api_key or os.environ.get("OPENAI_API_KEY", ""),
            **{k: v for k, v in kwargs.items() if k in ("base_url", "organization", "timeout")},
        )
        self._model = model or os.environ.get("OPENAI_MODEL", "gpt-4o")

    @property
    def provider_name(self) -> str:
        return "openai"

    def complete(
        self,
        system_prompt: str,
        user_message: str,
        temperature: float = 0.0,
        response_format: dict | None = None,
        **kwargs: Any,
    ) -> str:
        return self.complete_with_history(
            system_prompt=system_prompt,
            history=[{"role": "user", "content": user_message}],
            temperature=temperature,
            response_format=response_format,
            **kwargs,
        )

    def complete_with_history(
        self,
        system_prompt: str,
        history: list[dict],
        temperature: float = 0.0,
        response_format: dict | None = None,
        **kwargs: Any,
    ) -> str:
        call_kwargs: dict[str, Any] = {
            "model": kwargs.pop("model", self._model),
            "messages": [{"role": "system", "content": system_prompt}] + history,
            "temperature": temperature,
        }
        if response_format:
            call_kwargs["response_format"] = response_format
        call_kwargs.update(kwargs)

        response = self._client.chat.completions.create(**call_kwargs)
        return response.choices[0].message.content


# ---------------------------------------------------------------------------
# Anthropic
# ---------------------------------------------------------------------------

class AnthropicProvider(LLMProvider):
    """Provider Anthropic (Claude Sonnet, Haiku, Opus, …)."""

    def __init__(self, api_key: str | None = None, model: str | None = None, **kwargs: Any):
        try:
            import anthropic as _anthropic
        except ImportError as exc:
            raise ImportError(
                "[LLMProvider] 'anthropic' n'est pas installé. "
                "Faites : pip install anthropic"
            ) from exc

        self._client = _anthropic.Anthropic(
            api_key=api_key or os.environ.get("ANTHROPIC_API_KEY", ""),
        )
        self._model = model or os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-4-6")
        self._max_tokens = int(kwargs.get("max_tokens", 4096))

    @property
    def provider_name(self) -> str:
        return "anthropic"

    def complete(
        self,
        system_prompt: str,
        user_message: str,
        temperature: float = 0.0,
        response_format: dict | None = None,
        **kwargs: Any,
    ) -> str:
        return self.complete_with_history(
            system_prompt=system_prompt,
            history=[{"role": "user", "content": user_message}],
            temperature=temperature,
            response_format=response_format,
            **kwargs,
        )

    def complete_with_history(
        self,
        system_prompt: str,
        history: list[dict],
        temperature: float = 0.0,
        response_format: dict | None = None,
        **kwargs: Any,
    ) -> str:
        model = kwargs.pop("model", self._model)
        max_tokens = int(kwargs.pop("max_tokens", self._max_tokens))

        response = self._client.messages.create(
            model=model,
            system=system_prompt,
            messages=history,
            temperature=temperature,
            max_tokens=max_tokens,
            **kwargs,
        )
        return response.content[0].text


# ---------------------------------------------------------------------------
# Google Gemini
# ---------------------------------------------------------------------------

class GoogleProvider(LLMProvider):
    """Provider Google Gemini."""

    def __init__(self, api_key: str | None = None, model: str | None = None, **kwargs: Any):
        try:
            import google.generativeai as _genai
        except ImportError as exc:
            raise ImportError(
                "[LLMProvider] 'google-generativeai' n'est pas installé. "
                "Faites : pip install google-generativeai"
            ) from exc

        key = api_key or os.environ.get("GOOGLE_API_KEY", "")
        _genai.configure(api_key=key)
        model_name = model or os.environ.get("GOOGLE_MODEL", "gemini-1.5-pro")
        self._model_name = model_name
        self._model = _genai.GenerativeModel(model_name)

    @property
    def provider_name(self) -> str:
        return "google"

    def complete(
        self,
        system_prompt: str,
        user_message: str,
        temperature: float = 0.0,
        response_format: dict | None = None,
        **kwargs: Any,
    ) -> str:
        prompt = f"{system_prompt}\n\n{user_message}"
        response = self._model.generate_content(prompt)
        return response.text

    def complete_with_history(
        self,
        system_prompt: str,
        history: list[dict],
        temperature: float = 0.0,
        response_format: dict | None = None,
        **kwargs: Any,
    ) -> str:
        # NOTE: Gemini's native chat API (start_chat / send_message) supports
        # structured multi-turn history. This implementation concatenates turns
        # into a single flat prompt for simplicity, which degrades quality on
        # long conversations. Migrate to start_chat() for production multi-turn use.
        parts = [system_prompt]
        for turn in history:
            parts.append(f"[{turn['role']}]: {turn['content']}")
        prompt = "\n\n".join(parts)
        response = self._model.generate_content(prompt)
        return response.text


# ---------------------------------------------------------------------------
# Databricks Foundation Models
# ---------------------------------------------------------------------------

class DatabricksProvider(LLMProvider):
    """
    LLM provider for Databricks Foundation Models API (OpenAI-compatible).

    The Databricks Foundation Models API is OpenAI-compatible and is exposed at
    ``{DATABRICKS_HOST}/serving-endpoints``. This provider wraps the OpenAI client
    with the correct base_url and uses a Databricks PAT as the API key.

    Configuration via environment variables:
        DATABRICKS_HOST       — workspace URL (e.g. https://adb-xxx.azuredatabricks.net)
        DATABRICKS_TOKEN      — personal access token or service principal secret
        DATABRICKS_LLM_MODEL  — serving endpoint name (default: databricks-dbrx-instruct)
    """

    def __init__(
        self,
        host: str | None = None,
        token: str | None = None,
        model: str | None = None,
        **kwargs: Any,
    ) -> None:
        try:
            import openai as _openai
        except ImportError as exc:
            raise ImportError(
                "[LLMProvider] 'openai' is required for DatabricksProvider. "
                "Install it with: pip install openai"
            ) from exc

        resolved_host = host or os.environ.get("DATABRICKS_HOST", "")
        resolved_token = token or os.environ.get("DATABRICKS_TOKEN", "")

        if not resolved_host:
            raise ValueError(
                "[DatabricksProvider] DATABRICKS_HOST is not set. "
                "Set it to your workspace URL (e.g. https://adb-xxx.azuredatabricks.net)."
            )
        if not resolved_token:
            raise ValueError(
                "[DatabricksProvider] DATABRICKS_TOKEN is not set. "
                "Set it to a personal access token or service principal secret."
            )

        base_url = resolved_host.rstrip("/") + "/serving-endpoints"
        self._client = _openai.OpenAI(
            api_key=resolved_token,
            base_url=base_url,
        )
        self._model = model or os.environ.get(
            "DATABRICKS_LLM_MODEL", "databricks-dbrx-instruct"
        )

    @property
    def provider_name(self) -> str:
        return "databricks"

    def complete(
        self,
        system_prompt: str,
        user_message: str,
        temperature: float = 0.0,
        response_format: dict | None = None,
        **kwargs: Any,
    ) -> str:
        return self.complete_with_history(
            system_prompt=system_prompt,
            history=[{"role": "user", "content": user_message}],
            temperature=temperature,
            response_format=response_format,
            **kwargs,
        )

    def complete_with_history(
        self,
        system_prompt: str,
        history: list[dict],
        temperature: float = 0.0,
        response_format: dict | None = None,
        **kwargs: Any,
    ) -> str:
        call_kwargs: dict[str, Any] = {
            "model": kwargs.pop("model", self._model),
            "messages": [{"role": "system", "content": system_prompt}] + history,
            "temperature": temperature,
        }
        if response_format:
            call_kwargs["response_format"] = response_format
        call_kwargs.update(kwargs)

        response = self._client.chat.completions.create(**call_kwargs)
        return response.choices[0].message.content


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

_PROVIDER_MAP = {
    "openai": OpenAIProvider,
    "anthropic": AnthropicProvider,
    "google": GoogleProvider,
    "databricks": DatabricksProvider,
}


def get_llm_provider(
    provider: str | None = None,
    **kwargs: Any,
) -> LLMProvider:
    """
    Retourne une instance LLMProvider configurée.

    La détection du provider suit cet ordre de priorité :
      1. Argument ``provider`` explicite.
      2. Variable d'environnement ``LLM_PROVIDER``.
      3. Auto-détection depuis les clés API présentes dans l'environnement
         (DATABRICKS_HOST+DATABRICKS_TOKEN → databricks, ANTHROPIC_API_KEY → anthropic,
         OPENAI_API_KEY → openai, GOOGLE_API_KEY → google).
      4. Erreur si aucune clé détectée.

    Args:
        provider: Nom du provider parmi {'openai', 'anthropic', 'google'}.
        **kwargs: Paramètres supplémentaires passés au constructeur du provider
                  (ex: ``model="gpt-4o-mini"``, ``api_key="sk-..."``,
                  ``max_tokens=2048``).

    Returns:
        Instance LLMProvider prête à l'emploi.

    Raises:
        ValueError: Si le provider demandé est inconnu.
        ImportError: Si la librairie du provider n'est pas installée.
    """
    name = provider or os.environ.get("LLM_PROVIDER") or _auto_detect_provider()
    name = name.lower()

    if name not in _PROVIDER_MAP:
        raise ValueError(
            f"[LLMProvider] Provider inconnu : '{name}'. "
            f"Valeurs acceptées : {sorted(_PROVIDER_MAP.keys())}"
        )

    cls = _PROVIDER_MAP[name]
    return cls(**kwargs)


def _auto_detect_provider() -> str:
    """Détecte le provider depuis les clés API présentes dans l'environnement."""
    if os.environ.get("DATABRICKS_HOST") and os.environ.get("DATABRICKS_TOKEN"):
        return "databricks"
    if os.environ.get("ANTHROPIC_API_KEY"):
        return "anthropic"
    if os.environ.get("OPENAI_API_KEY"):
        return "openai"
    if os.environ.get("GOOGLE_API_KEY"):
        return "google"
    raise ValueError(
        "[LLMProvider] Aucune clé API détectée. "
        "Définissez DATABRICKS_HOST+DATABRICKS_TOKEN, ANTHROPIC_API_KEY, "
        "OPENAI_API_KEY ou GOOGLE_API_KEY dans votre .env ou passez provider= explicitement."
    )
