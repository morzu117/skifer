import pytest
import yaml
from unittest.mock import MagicMock
from skifer.core.config import ConfigurationManager
from skifer.core.spark_backend import SparkBackend

# ==============================================================================
# TESTS FOR ConfigurationManager
# ==============================================================================

def test_config_loads_and_detects_env(spark, tmp_path):
    """
    Tests that ConfigurationManager can correctly load a YAML file
    and auto-detect the active environment via backend.check_catalog_access.
    """
    config_content = {
        "priority_check": ["non_existent_env", "test_env"],
        "environments": {
            "non_existent_env": {"catalog": "fake_catalog"},
            "test_env": {"catalog": "spark_catalog"}
        }
    }
    config_file = tmp_path / "config.yaml"
    with open(config_file, 'w') as f:
        yaml.dump(config_content, f)

    backend = SparkBackend(spark=spark, is_local=True)
    manager = ConfigurationManager(config_path=str(config_file), backend=backend)

    assert manager.current_env_name == "test_env"
    assert manager.get_db() == "spark_catalog"
    assert manager.get_value("catalog") == "spark_catalog"


def test_config_raises_error_if_no_catalog_matches(tmp_path):
    """
    Tests that ConfigurationManager raises an EnvironmentError when
    the backend cannot connect to any of the catalogs listed.
    """
    config_content = {
        "priority_check": ["non_existent_env_1", "non_existent_env_2"],
        "environments": {
            "non_existent_env_1": {"catalog": "cat_1"},
            "non_existent_env_2": {"catalog": "cat_2"}
        }
    }
    config_file = tmp_path / "config.yaml"
    with open(config_file, 'w') as f:
        yaml.dump(config_content, f)

    mock_backend = MagicMock()
    mock_backend.check_catalog_access.return_value = False

    with pytest.raises(EnvironmentError, match="Could not connect to any catalog"):
        ConfigurationManager(config_path=str(config_file), backend=mock_backend)


def test_config_raises_error_if_file_not_found():
    """FileNotFoundError raised if config_path does not exist."""
    with pytest.raises(FileNotFoundError):
        ConfigurationManager(config_path="/path/to/non/existent/config.yaml")


def test_config_local_env_no_backend(tmp_path):
    """Null-catalog environment (local) requires no backend."""
    config_content = {
        "priority_check": ["local"],
        "environments": {"local": {"catalog": None}}
    }
    config_file = tmp_path / "config.yaml"
    with open(config_file, 'w') as f:
        yaml.dump(config_content, f)

    manager = ConfigurationManager(config_path=str(config_file))
    assert manager.current_env_name == "local"
    assert manager.get_db() is None


def test_config_raises_error_for_empty_yaml(tmp_path):
    """Empty YAML files should fail with a clear configuration error."""
    config_file = tmp_path / "config.yaml"
    config_file.write_text("")

    with pytest.raises(ValueError, match="top-level config must be a mapping"):
        ConfigurationManager(config_path=str(config_file))


def test_config_invalid_semantic_certification_policy_raises_at_load(tmp_path):
    """semantic_certification_policy is validated while loading config.yaml."""
    config_content = {
        "priority_check": ["local"],
        "environments": {
            "local": {
                "catalog": None,
                "semantic_certification_policy": "strict",
            }
        },
    }
    config_file = tmp_path / "config.yaml"
    with open(config_file, "w") as f:
        yaml.dump(config_content, f)

    with pytest.raises(ValueError, match="semantic_certification_policy"):
        ConfigurationManager(config_path=str(config_file))


@pytest.mark.parametrize("value", ["guarded", "GUARDED", "automatic", None, 1])
def test_config_invalid_capability_autonomy_raises_at_load(tmp_path, value):
    config_content = {
        "priority_check": ["local"],
        "environments": {"local": {"catalog": None, "capability_autonomy": value}},
    }
    config_file = tmp_path / "config.yaml"
    with open(config_file, "w") as f:
        yaml.dump(config_content, f)

    with pytest.raises(
        ValueError, match="guarded is deliberately not configurable in v1"
    ):
        ConfigurationManager(config_path=str(config_file))


@pytest.mark.parametrize("value", ["shadow", "supervised"])
def test_config_accepts_v1_capability_autonomy(tmp_path, value):
    config_content = {
        "priority_check": ["local"],
        "environments": {"local": {"catalog": None, "capability_autonomy": value}},
    }
    config_file = tmp_path / "config.yaml"
    with open(config_file, "w") as f:
        yaml.dump(config_content, f)

    manager = ConfigurationManager(config_path=str(config_file))
    assert manager.get_value("capability_autonomy") == value


def test_config_absent_capability_autonomy_is_none(tmp_path):
    config_file = tmp_path / "config.yaml"
    with open(config_file, "w") as f:
        yaml.dump(
            {"priority_check": ["local"], "environments": {"local": {"catalog": None}}},
            f,
        )

    manager = ConfigurationManager(config_path=str(config_file))
    assert manager.get_value("capability_autonomy") is None
