"""
Tests for serving.deploy.log_agent (Plan 41.6), with MLflow simulated via sys.modules.
"""
from __future__ import annotations

import sys
import types
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def fake_mlflow(monkeypatch):
    mlflow = types.ModuleType("mlflow")
    mlflow.pyfunc = MagicMock(name="mlflow.pyfunc")
    models = types.ModuleType("mlflow.models")
    resources = types.ModuleType("mlflow.models.resources")
    resources.DatabricksServingEndpoint = lambda endpoint_name: ("endpoint", endpoint_name)
    resources.DatabricksSQLWarehouse = lambda warehouse_id: ("warehouse", warehouse_id)
    agent_mod = types.ModuleType("skifer.serving.responses_agent")
    agent_mod.SkiferResponsesAgent = MagicMock(name="SkiferResponsesAgent")
    monkeypatch.setitem(sys.modules, "mlflow", mlflow)
    monkeypatch.setitem(sys.modules, "mlflow.pyfunc", mlflow.pyfunc)
    monkeypatch.setitem(sys.modules, "mlflow.models", models)
    monkeypatch.setitem(sys.modules, "mlflow.models.resources", resources)
    monkeypatch.setitem(sys.modules, "skifer.serving.responses_agent", agent_mod)
    return mlflow


def _logged(fake_mlflow):
    return fake_mlflow.pyfunc.log_model.call_args.kwargs


def test_log_agent_ships_artifacts_llm_config_and_resources(fake_mlflow, monkeypatch):
    import skifer
    from skifer.serving.deploy import log_agent
    monkeypatch.setattr(skifer, "__version__", "2.2.0")

    log_agent("conf/config.yaml", "semantic_models", "my-chat-endpoint", warehouse_id="wh-1")

    kwargs = _logged(fake_mlflow)
    assert kwargs["artifacts"] == {"config": "conf/config.yaml", "models_dir": "semantic_models"}
    assert kwargs["model_config"] == {"llm_endpoint": "my-chat-endpoint"}
    assert kwargs["resources"] == [("endpoint", "my-chat-endpoint"), ("warehouse", "wh-1")]
    assert kwargs["pip_requirements"] == ["skifer[serving]==2.2.0"]
    assert kwargs["name"] == "skifer_agent"
    assert "input_example" not in kwargs


def test_log_agent_example_question_becomes_a_responses_input(fake_mlflow, monkeypatch):
    import skifer
    from skifer.serving.deploy import log_agent
    monkeypatch.setattr(skifer, "__version__", "2.2.0")

    log_agent("c.yaml", "m", "ep", example_question="CA par région ?")

    assert _logged(fake_mlflow)["input_example"] == {
        "input": [{"role": "user", "content": "CA par région ?"}]
    }


def test_log_agent_refuses_to_pin_an_unknown_version(fake_mlflow, monkeypatch):
    import skifer
    from skifer.serving.deploy import log_agent
    monkeypatch.setattr(skifer, "__version__", "unknown")

    with pytest.raises(ValueError, match="pip_requirements"):
        log_agent("c.yaml", "m", "ep")
    fake_mlflow.pyfunc.log_model.assert_not_called()


def test_log_agent_explicit_requirements_win(fake_mlflow, monkeypatch):
    import skifer
    from skifer.serving.deploy import log_agent
    monkeypatch.setattr(skifer, "__version__", "unknown")

    log_agent("c.yaml", "m", "ep", pip_requirements=["skifer[serving] @ file:///dist/skifer.whl"])

    assert _logged(fake_mlflow)["pip_requirements"] == ["skifer[serving] @ file:///dist/skifer.whl"]
