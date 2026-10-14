"""
Tests unitaires pour get_spark_session() — ordre de détection des contextes.

databricks-connect n'est pas installé dans l'environnement de test : le module est
simulé via sys.modules. Aucun test ne démarre de session Spark réelle.
"""
import sys
import types
from unittest.mock import MagicMock

import pytest
from pyspark.sql import SparkSession

from skifer.spark_factory import get_spark_session


@pytest.fixture
def no_active_session(monkeypatch):
    monkeypatch.setattr(SparkSession, "getActiveSession", staticmethod(lambda: None))


@pytest.fixture
def clean_env(monkeypatch):
    for var in (
        "DATABRICKS_SERVERLESS_COMPUTE_ID",
        "DATABRICKS_HOST",
        "DATABRICKS_TOKEN",
        "DATABRICKS_CLUSTER_ID",
    ):
        monkeypatch.delenv(var, raising=False)


def _install_fake_connect(monkeypatch):
    """Installe un faux databricks.connect et renvoie le DatabricksSession simulé."""
    fake_session_cls = MagicMock(name="DatabricksSession")
    connect_mod = types.ModuleType("databricks.connect")
    connect_mod.DatabricksSession = fake_session_cls
    sdk_core = types.ModuleType("databricks.sdk.core")
    sdk_core.Config = MagicMock(name="Config")
    monkeypatch.setitem(sys.modules, "databricks", types.ModuleType("databricks"))
    monkeypatch.setitem(sys.modules, "databricks.connect", connect_mod)
    monkeypatch.setitem(sys.modules, "databricks.sdk", types.ModuleType("databricks.sdk"))
    monkeypatch.setitem(sys.modules, "databricks.sdk.core", sdk_core)
    return fake_session_cls


def test_serverless_session_when_compute_id_is_set(monkeypatch, no_active_session, clean_env):
    fake = _install_fake_connect(monkeypatch)
    monkeypatch.setenv("DATABRICKS_SERVERLESS_COMPUTE_ID", "auto")

    session, mode = get_spark_session()

    assert mode == "databricks_connect"
    fake.builder.serverless.assert_called_once_with()
    assert session is fake.builder.serverless.return_value.getOrCreate.return_value


def test_serverless_takes_priority_over_cluster(monkeypatch, no_active_session, clean_env):
    fake = _install_fake_connect(monkeypatch)
    monkeypatch.setenv("DATABRICKS_SERVERLESS_COMPUTE_ID", "auto")
    monkeypatch.setenv("DATABRICKS_HOST", "https://adb.example")
    monkeypatch.setenv("DATABRICKS_TOKEN", "t")
    monkeypatch.setenv("DATABRICKS_CLUSTER_ID", "c-1")

    get_spark_session()

    fake.builder.serverless.assert_called_once_with()
    fake.builder.sdkConfig.assert_not_called()


def test_cluster_connect_unchanged_without_serverless(monkeypatch, no_active_session, clean_env):
    fake = _install_fake_connect(monkeypatch)
    monkeypatch.setenv("DATABRICKS_HOST", "https://adb.example")
    monkeypatch.setenv("DATABRICKS_TOKEN", "t")
    monkeypatch.setenv("DATABRICKS_CLUSTER_ID", "c-1")

    _, mode = get_spark_session()

    assert mode == "databricks_connect"
    fake.builder.sdkConfig.assert_called_once()
    fake.builder.serverless.assert_not_called()


def test_serverless_without_package_names_the_extra(monkeypatch, no_active_session, clean_env):
    # None in sys.modules makes the import raise ImportError.
    monkeypatch.setitem(sys.modules, "databricks.connect", None)
    monkeypatch.setenv("DATABRICKS_SERVERLESS_COMPUTE_ID", "auto")

    with pytest.raises(ImportError, match=r"skifer\[serving\]"):
        get_spark_session()


def test_serverless_failure_does_not_fall_back_to_local(monkeypatch, no_active_session, clean_env):
    fake = _install_fake_connect(monkeypatch)
    fake.builder.serverless.return_value.getOrCreate.side_effect = RuntimeError("auth refused")
    monkeypatch.setenv("DATABRICKS_SERVERLESS_COMPUTE_ID", "auto")
    local_builder = MagicMock(name="local builder")
    monkeypatch.setattr(SparkSession, "builder", local_builder)

    with pytest.raises(RuntimeError, match="auth refused"):
        get_spark_session()
    local_builder.appName.assert_not_called()


def test_active_session_still_wins(monkeypatch, clean_env):
    active = MagicMock(name="active")
    monkeypatch.setattr(SparkSession, "getActiveSession", staticmethod(lambda: active))
    monkeypatch.setenv("DATABRICKS_SERVERLESS_COMPUTE_ID", "auto")

    assert get_spark_session() == (active, "databricks_notebook")
