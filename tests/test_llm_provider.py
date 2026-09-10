"""
Tests unitaires pour LLMProvider — interface provider-agnostic.

Les providers LLM (openai, anthropic) sont des dépendances optionnelles.
Les tests mockent les modules entiers via sys.modules pour ne pas nécessiter
leur installation dans l'environnement de test.
"""
import sys
import types
import pytest
from unittest.mock import MagicMock

from skifer.semantic.llm_provider import (
    get_llm_provider,
    _auto_detect_provider,
)


# ---------------------------------------------------------------------------
# Helpers — mock de modules optionnels
# ---------------------------------------------------------------------------

def _make_openai_module():
    """Crée un faux module openai pour les tests."""
    mod = types.ModuleType("openai")
    mod.OpenAI = MagicMock
    return mod


def _make_anthropic_module():
    """Crée un faux module anthropic pour les tests."""
    mod = types.ModuleType("anthropic")
    mod.Anthropic = MagicMock
    return mod


# ---------------------------------------------------------------------------
# _auto_detect_provider
# ---------------------------------------------------------------------------

def test_auto_detect_anthropic_key(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-test")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)
    assert _auto_detect_provider() == "anthropic"


def test_auto_detect_openai_key(monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)
    assert _auto_detect_provider() == "openai"


def test_auto_detect_fallback(monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)
    with pytest.raises(ValueError, match="Aucune clé API détectée"):
        _auto_detect_provider()


# ---------------------------------------------------------------------------
# get_llm_provider — factory
# ---------------------------------------------------------------------------

def test_get_llm_provider_explicit_openai(monkeypatch):
    """get_llm_provider('openai') doit retourner une instance OpenAIProvider."""
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    fake_mod = _make_openai_module()
    monkeypatch.setitem(sys.modules, "openai", fake_mod)

    from skifer.semantic.llm_provider import OpenAIProvider
    provider = get_llm_provider("openai")
    assert isinstance(provider, OpenAIProvider)
    assert provider.provider_name == "openai"


def test_get_llm_provider_explicit_anthropic(monkeypatch):
    """get_llm_provider('anthropic') doit retourner une instance AnthropicProvider."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-test")
    fake_mod = _make_anthropic_module()
    monkeypatch.setitem(sys.modules, "anthropic", fake_mod)

    from skifer.semantic.llm_provider import AnthropicProvider
    provider = get_llm_provider("anthropic")
    assert isinstance(provider, AnthropicProvider)
    assert provider.provider_name == "anthropic"


def test_get_llm_provider_unknown_raises():
    with pytest.raises(ValueError, match="Provider inconnu"):
        get_llm_provider("unknown_provider")


def test_get_llm_provider_env_variable(monkeypatch):
    """La variable LLM_PROVIDER doit être respectée."""
    monkeypatch.setenv("LLM_PROVIDER", "openai")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    fake_mod = _make_openai_module()
    monkeypatch.setitem(sys.modules, "openai", fake_mod)

    from skifer.semantic.llm_provider import OpenAIProvider
    provider = get_llm_provider()
    assert isinstance(provider, OpenAIProvider)


# ---------------------------------------------------------------------------
# OpenAIProvider — complete()
# ---------------------------------------------------------------------------

def test_openai_complete_calls_api(monkeypatch):
    """OpenAIProvider.complete() doit appeler l'API OpenAI et retourner le contenu."""
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")

    # Réponse mockée
    mock_response = MagicMock()
    mock_response.choices[0].message.content = '{"selected": "kpi_orders.erp"}'

    mock_client_instance = MagicMock()
    mock_client_instance.chat.completions.create.return_value = mock_response

    mock_openai_cls = MagicMock(return_value=mock_client_instance)
    fake_mod = types.ModuleType("openai")
    fake_mod.OpenAI = mock_openai_cls
    monkeypatch.setitem(sys.modules, "openai", fake_mod)

    from skifer.semantic.llm_provider import OpenAIProvider
    provider = OpenAIProvider(api_key="sk-test")

    result = provider.complete(
        system_prompt="Tu es un assistant.",
        user_message="Quelle est la météo ?",
    )
    assert result == '{"selected": "kpi_orders.erp"}'
    mock_client_instance.chat.completions.create.assert_called_once()


def test_openai_complete_with_history(monkeypatch):
    """complete_with_history() doit injecter l'historique dans les messages."""
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")

    mock_response = MagicMock()
    mock_response.choices[0].message.content = "Réponse"

    mock_client_instance = MagicMock()
    mock_client_instance.chat.completions.create.return_value = mock_response

    mock_openai_cls = MagicMock(return_value=mock_client_instance)
    fake_mod = types.ModuleType("openai")
    fake_mod.OpenAI = mock_openai_cls
    monkeypatch.setitem(sys.modules, "openai", fake_mod)

    from skifer.semantic.llm_provider import OpenAIProvider
    provider = OpenAIProvider(api_key="sk-test")

    history = [
        {"role": "user", "content": "Q1"},
        {"role": "assistant", "content": "R1"},
        {"role": "user", "content": "Q2"},
    ]
    provider.complete_with_history("Système", history)

    call_kwargs = mock_client_instance.chat.completions.create.call_args[1]
    messages = call_kwargs["messages"]
    assert messages[0]["role"] == "system"
    assert messages[1]["content"] == "Q1"
    assert messages[-1]["content"] == "Q2"


# ---------------------------------------------------------------------------
# AnthropicProvider — complete()
# ---------------------------------------------------------------------------

def test_anthropic_complete_calls_api(monkeypatch):
    """AnthropicProvider.complete() doit appeler l'API Anthropic."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-test")

    mock_response = MagicMock()
    mock_response.content[0].text = '{"model": "kpi_orders.erp"}'

    mock_client_instance = MagicMock()
    mock_client_instance.messages.create.return_value = mock_response

    mock_anthropic_cls = MagicMock(return_value=mock_client_instance)
    fake_mod = types.ModuleType("anthropic")
    fake_mod.Anthropic = mock_anthropic_cls
    monkeypatch.setitem(sys.modules, "anthropic", fake_mod)

    from skifer.semantic.llm_provider import AnthropicProvider
    provider = AnthropicProvider(api_key="sk-ant-test")

    result = provider.complete("Système", "Question")
    assert result == '{"model": "kpi_orders.erp"}'


def test_anthropic_provider_name(monkeypatch):
    fake_mod = types.ModuleType("anthropic")
    fake_mod.Anthropic = MagicMock
    monkeypatch.setitem(sys.modules, "anthropic", fake_mod)

    from skifer.semantic.llm_provider import AnthropicProvider
    provider = AnthropicProvider(api_key="sk-test")
    assert provider.provider_name == "anthropic"


# ---------------------------------------------------------------------------
# DatabricksProvider
# ---------------------------------------------------------------------------

def test_databricks_provider_init_from_env(monkeypatch):
    """DatabricksProvider reads host and token from env vars."""
    monkeypatch.setenv("DATABRICKS_HOST", "https://adb-123.azuredatabricks.net")
    monkeypatch.setenv("DATABRICKS_TOKEN", "dapi-test-token")

    fake_mod = _make_openai_module()
    monkeypatch.setitem(sys.modules, "openai", fake_mod)

    from skifer.semantic.llm_provider import DatabricksProvider
    provider = DatabricksProvider()
    assert provider.provider_name == "databricks"
    assert provider._model == "databricks-dbrx-instruct"


def test_databricks_provider_custom_model(monkeypatch):
    """DatabricksProvider uses DATABRICKS_LLM_MODEL env var as default model."""
    monkeypatch.setenv("DATABRICKS_HOST", "https://adb-123.azuredatabricks.net")
    monkeypatch.setenv("DATABRICKS_TOKEN", "dapi-test-token")
    monkeypatch.setenv("DATABRICKS_LLM_MODEL", "databricks-meta-llama-3-1-70b-instruct")

    fake_mod = _make_openai_module()
    monkeypatch.setitem(sys.modules, "openai", fake_mod)

    from skifer.semantic.llm_provider import DatabricksProvider
    provider = DatabricksProvider()
    assert provider._model == "databricks-meta-llama-3-1-70b-instruct"


def test_databricks_provider_complete(monkeypatch):
    """DatabricksProvider.complete() passes the correct base_url and model."""
    monkeypatch.setenv("DATABRICKS_HOST", "https://adb-123.azuredatabricks.net")
    monkeypatch.setenv("DATABRICKS_TOKEN", "dapi-test-token")

    mock_response = MagicMock()
    mock_response.choices[0].message.content = "lineage"

    mock_client_instance = MagicMock()
    mock_client_instance.chat.completions.create.return_value = mock_response

    mock_openai_cls = MagicMock(return_value=mock_client_instance)
    fake_mod = types.ModuleType("openai")
    fake_mod.OpenAI = mock_openai_cls
    monkeypatch.setitem(sys.modules, "openai", fake_mod)

    from skifer.semantic.llm_provider import DatabricksProvider
    provider = DatabricksProvider()
    result = provider.complete("System", "Where does amount_eur come from?")

    assert result == "lineage"
    mock_client_instance.chat.completions.create.assert_called_once()
    # Verify base_url was set with the /serving-endpoints suffix
    init_kwargs = mock_openai_cls.call_args[1]
    assert init_kwargs["base_url"].endswith("/serving-endpoints")
    assert init_kwargs["api_key"] == "dapi-test-token"


def test_databricks_provider_missing_host_raises(monkeypatch):
    monkeypatch.delenv("DATABRICKS_HOST", raising=False)
    monkeypatch.setenv("DATABRICKS_TOKEN", "dapi-test-token")

    fake_mod = _make_openai_module()
    monkeypatch.setitem(sys.modules, "openai", fake_mod)

    from skifer.semantic.llm_provider import DatabricksProvider
    with pytest.raises(ValueError, match="DATABRICKS_HOST"):
        DatabricksProvider()


def test_databricks_provider_missing_token_raises(monkeypatch):
    monkeypatch.setenv("DATABRICKS_HOST", "https://adb-123.azuredatabricks.net")
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)

    fake_mod = _make_openai_module()
    monkeypatch.setitem(sys.modules, "openai", fake_mod)

    from skifer.semantic.llm_provider import DatabricksProvider
    with pytest.raises(ValueError, match="DATABRICKS_TOKEN"):
        DatabricksProvider()


def test_auto_detect_databricks(monkeypatch):
    """DATABRICKS_HOST + DATABRICKS_TOKEN take priority in auto-detection."""
    monkeypatch.setenv("DATABRICKS_HOST", "https://adb-123.azuredatabricks.net")
    monkeypatch.setenv("DATABRICKS_TOKEN", "dapi-test-token")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-test")  # present but lower priority

    assert _auto_detect_provider() == "databricks"


def test_get_llm_provider_databricks(monkeypatch):
    """get_llm_provider('databricks') returns a DatabricksProvider instance."""
    monkeypatch.setenv("DATABRICKS_HOST", "https://adb-123.azuredatabricks.net")
    monkeypatch.setenv("DATABRICKS_TOKEN", "dapi-test-token")

    fake_mod = _make_openai_module()
    monkeypatch.setitem(sys.modules, "openai", fake_mod)

    from skifer.semantic.llm_provider import DatabricksProvider
    provider = get_llm_provider("databricks")
    assert isinstance(provider, DatabricksProvider)
    assert provider.provider_name == "databricks"
