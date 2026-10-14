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

def test_databricks_provider_requires_a_model(monkeypatch):
    """No default model: the former default (DBRX) was retired by Databricks (Plan 41.3)."""
    monkeypatch.setenv("DATABRICKS_HOST", "https://adb-123.azuredatabricks.net")
    monkeypatch.setenv("DATABRICKS_TOKEN", "dapi-test-token")
    monkeypatch.delenv("DATABRICKS_LLM_MODEL", raising=False)

    fake_mod = _make_openai_module()
    monkeypatch.setitem(sys.modules, "openai", fake_mod)

    from skifer.semantic.llm_provider import DatabricksProvider
    with pytest.raises(ValueError, match="DATABRICKS_LLM_MODEL"):
        DatabricksProvider()


def test_databricks_provider_explicit_model_argument(monkeypatch):
    monkeypatch.setenv("DATABRICKS_HOST", "https://adb-123.azuredatabricks.net")
    monkeypatch.setenv("DATABRICKS_TOKEN", "dapi-test-token")
    monkeypatch.delenv("DATABRICKS_LLM_MODEL", raising=False)

    fake_mod = _make_openai_module()
    monkeypatch.setitem(sys.modules, "openai", fake_mod)

    from skifer.semantic.llm_provider import DatabricksProvider
    provider = DatabricksProvider(model="my-chat-endpoint")
    assert provider.provider_name == "databricks"
    assert provider._model == "my-chat-endpoint"


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
    monkeypatch.setenv("DATABRICKS_LLM_MODEL", "my-chat-endpoint")

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

    _without_databricks_sdk(monkeypatch)
    fake_mod = _make_openai_module()
    monkeypatch.setitem(sys.modules, "openai", fake_mod)

    from skifer.semantic.llm_provider import DatabricksProvider
    with pytest.raises(ValueError, match="DATABRICKS_HOST"):
        DatabricksProvider()


def test_databricks_provider_missing_token_raises(monkeypatch):
    monkeypatch.setenv("DATABRICKS_HOST", "https://adb-123.azuredatabricks.net")
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)

    _without_databricks_sdk(monkeypatch)
    fake_mod = _make_openai_module()
    monkeypatch.setitem(sys.modules, "openai", fake_mod)

    from skifer.semantic.llm_provider import DatabricksProvider
    with pytest.raises(ValueError, match="DATABRICKS_TOKEN"):
        DatabricksProvider()


def _without_databricks_sdk(monkeypatch):
    # None in sys.modules makes the import raise: the SDK fallback has nothing to use,
    # whatever ~/.databrickscfg the machine running the tests has.
    monkeypatch.setitem(sys.modules, "databricks.sdk.core", None)


def _install_fake_databricks_sdk(monkeypatch, *, host="https://adb-9.azuredatabricks.net", error=None):
    """Install a fake databricks.sdk.core whose Config counts authenticate() calls."""
    calls = []

    class Config:
        def __init__(self, host=None):
            if error is not None:
                raise error
            self.host = host or default_host

        def authenticate(self):
            calls.append(len(calls) + 1)
            return {"Authorization": f"Bearer token-{len(calls)}"}

    default_host = host
    core = types.ModuleType("databricks.sdk.core")
    core.Config = Config
    monkeypatch.setitem(sys.modules, "databricks", types.ModuleType("databricks"))
    monkeypatch.setitem(sys.modules, "databricks.sdk", types.ModuleType("databricks.sdk"))
    monkeypatch.setitem(sys.modules, "databricks.sdk.core", core)
    return calls


def test_databricks_provider_falls_back_to_sdk_auth_without_variables(monkeypatch):
    """A notebook sets neither DATABRICKS_HOST nor DATABRICKS_TOKEN: the SDK authenticates."""
    httpx = pytest.importorskip("httpx")
    monkeypatch.delenv("DATABRICKS_HOST", raising=False)
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
    calls = _install_fake_databricks_sdk(monkeypatch)
    mock_openai_cls = MagicMock()
    fake_mod = types.ModuleType("openai")
    fake_mod.OpenAI = mock_openai_cls
    monkeypatch.setitem(sys.modules, "openai", fake_mod)

    from skifer.semantic.llm_provider import DatabricksProvider
    DatabricksProvider(model="my-chat-endpoint")

    kwargs = mock_openai_cls.call_args[1]
    assert kwargs["base_url"] == "https://adb-9.azuredatabricks.net/serving-endpoints"
    # The header is recomputed per request, so a refreshed OAuth token is picked up.
    auth = kwargs["http_client"].auth
    for expected in ("Bearer token-1", "Bearer token-2"):
        request = httpx.Request("POST", kwargs["base_url"] + "/chat/completions")
        assert next(auth.auth_flow(request)).headers["Authorization"] == expected
    assert calls == [1, 2]


def test_databricks_provider_sdk_fallback_keeps_an_explicit_host(monkeypatch):
    pytest.importorskip("httpx")
    monkeypatch.setenv("DATABRICKS_HOST", "https://adb-123.azuredatabricks.net/")
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
    _install_fake_databricks_sdk(monkeypatch)
    mock_openai_cls = MagicMock()
    fake_mod = types.ModuleType("openai")
    fake_mod.OpenAI = mock_openai_cls
    monkeypatch.setitem(sys.modules, "openai", fake_mod)

    from skifer.semantic.llm_provider import DatabricksProvider
    DatabricksProvider(model="my-chat-endpoint")

    assert mock_openai_cls.call_args[1]["base_url"] == "https://adb-123.azuredatabricks.net/serving-endpoints"


def test_databricks_provider_sdk_without_credentials_names_what_to_set(monkeypatch):
    monkeypatch.delenv("DATABRICKS_HOST", raising=False)
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
    _install_fake_databricks_sdk(monkeypatch, error=ValueError("default auth: cannot configure default credentials"))
    monkeypatch.setitem(sys.modules, "openai", _make_openai_module())

    from skifer.semantic.llm_provider import DatabricksProvider
    with pytest.raises(ValueError) as raised:
        DatabricksProvider(model="my-chat-endpoint")

    message = str(raised.value)
    assert "DATABRICKS_HOST and DATABRICKS_TOKEN not set" in message
    assert "cannot configure default credentials" in message


class _TemperatureRefused(Exception):
    status_code = 400


def _databricks_provider_with_client(monkeypatch, client):
    monkeypatch.setenv("DATABRICKS_HOST", "https://adb-123.azuredatabricks.net")
    monkeypatch.setenv("DATABRICKS_TOKEN", "dapi-test-token")
    fake_mod = types.ModuleType("openai")
    fake_mod.OpenAI = MagicMock(return_value=client)
    monkeypatch.setitem(sys.modules, "openai", fake_mod)
    from skifer.semantic.llm_provider import DatabricksProvider
    return DatabricksProvider(model="my-chat-endpoint")


def _response(content):
    response = MagicMock()
    response.choices[0].message.content = content
    return response


def test_databricks_provider_drops_temperature_an_endpoint_refuses(monkeypatch):
    """Claude 5.5 on Databricks answers 400 to any temperature: retry without, then stop sending it."""
    client = MagicMock()

    def create(**kwargs):
        if "temperature" in kwargs:
            raise _TemperatureRefused("BAD_REQUEST: Model does not support the temperature parameter.")
        return _response("OK")

    client.chat.completions.create.side_effect = create
    provider = _databricks_provider_with_client(monkeypatch, client)

    assert provider.complete("System", "ping") == "OK"
    assert provider.complete("System", "ping") == "OK"

    sent = [call.kwargs for call in client.chat.completions.create.call_args_list]
    assert ["temperature" in kwargs for kwargs in sent] == [True, False, False]


def test_databricks_provider_keeps_temperature_on_other_errors(monkeypatch):
    client = MagicMock()
    client.chat.completions.create.side_effect = _TemperatureRefused("BAD_REQUEST: context too long")
    provider = _databricks_provider_with_client(monkeypatch, client)

    with pytest.raises(_TemperatureRefused):
        provider.complete("System", "ping")
    assert client.chat.completions.create.call_count == 1
    assert provider._omit_temperature is False


def test_databricks_provider_reads_text_parts_of_a_reasoning_answer(monkeypatch):
    """gpt-oss endpoints return the content as a list of reasoning and text parts."""
    client = MagicMock()
    client.chat.completions.create.return_value = _response([
        {"type": "reasoning", "summary": [{"type": "summary_text", "text": "thinking"}]},
        {"type": "text", "text": "OK"},
    ])
    provider = _databricks_provider_with_client(monkeypatch, client)

    assert provider.complete("System", "ping") == "OK"


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
    monkeypatch.setenv("DATABRICKS_LLM_MODEL", "my-chat-endpoint")

    fake_mod = _make_openai_module()
    monkeypatch.setitem(sys.modules, "openai", fake_mod)

    from skifer.semantic.llm_provider import DatabricksProvider
    provider = get_llm_provider("databricks")
    assert isinstance(provider, DatabricksProvider)
    assert provider.provider_name == "databricks"
