import pytest
import yaml
from unittest.mock import MagicMock
from skifer.core.config import ConfigurationManager
from skifer.core.config import LineageConfig, parse_lineage_config
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


def _write_local_config(tmp_path, **environment_values):
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        yaml.safe_dump(
            {
                "priority_check": ["local"],
                "environments": {
                    "local": {"catalog": None, **environment_values}
                },
            }
        )
    )
    return config_file


def test_config_accepts_valid_alerts(tmp_path):
    alerts = {
        "webhook_url": "https://example.test/webhook",
        "slack_webhook": "https://example.test/slack",
        "msteams_webhook": "https://example.test/teams",
        "google_chat_webhook": "https://example.test/chat",
        "email": {"recipients": ["ops@example.test"], "provider": "smtp"},
        "min_severity": "warning",
        "max_depth": 0,
    }

    manager = ConfigurationManager(
        config_path=str(_write_local_config(tmp_path, alerts=alerts))
    )

    assert manager.get_value("alerts") == alerts


@pytest.mark.parametrize("value", [None, [], "webhook"])
def test_config_rejects_non_mapping_alerts(tmp_path, value):
    config_file = _write_local_config(tmp_path, alerts=value)

    with pytest.raises(ValueError, match="environment 'local'.*alerts.*mapping"):
        ConfigurationManager(config_path=str(config_file))


def test_config_rejects_unknown_alert_key_without_echoing_value(tmp_path):
    secret = "https://hooks.example.test/unknown-secret"
    config_file = _write_local_config(
        tmp_path, alerts={"pagerduty_webhook": secret}
    )

    with pytest.raises(ValueError) as exc_info:
        ConfigurationManager(config_path=str(config_file))

    message = str(exc_info.value)
    assert "environment 'local'" in message
    assert "pagerduty_webhook" in message
    assert secret not in message


@pytest.mark.parametrize("value", ["emergency", None, []])
def test_config_rejects_bad_alert_min_severity(tmp_path, value):
    config_file = _write_local_config(
        tmp_path, alerts={"min_severity": value}
    )

    with pytest.raises(
        ValueError, match="alerts.min_severity.*environment 'local'.*Expected one of"
    ):
        ConfigurationManager(config_path=str(config_file))


@pytest.mark.parametrize("value", [-1, True, 1.5, "3"])
def test_config_rejects_invalid_alert_max_depth(tmp_path, value):
    config_file = _write_local_config(tmp_path, alerts={"max_depth": value})

    with pytest.raises(
        ValueError, match="alerts.max_depth.*environment 'local'.*integer"
    ):
        ConfigurationManager(config_path=str(config_file))


@pytest.mark.parametrize(
    "key",
    [
        "webhook_url",
        "slack_webhook",
        "msteams_webhook",
        "google_chat_webhook",
    ],
)
def test_config_rejects_empty_alert_webhook(tmp_path, key):
    config_file = _write_local_config(tmp_path, alerts={key: ""})

    with pytest.raises(ValueError, match=rf"alerts.{key}.*environment 'local'"):
        ConfigurationManager(config_path=str(config_file))


def test_config_webhook_error_does_not_echo_secret_url(tmp_path):
    secret = "https://hooks.example.test/private-token"
    config_file = _write_local_config(
        tmp_path, alerts={"webhook_url": [secret]}
    )

    with pytest.raises(ValueError) as exc_info:
        ConfigurationManager(config_path=str(config_file))

    message = str(exc_info.value)
    assert "environment 'local'" in message
    assert "alerts.webhook_url" in message
    assert secret not in message


def test_config_rejects_non_mapping_alert_email(tmp_path):
    config_file = _write_local_config(tmp_path, alerts={"email": []})

    with pytest.raises(
        ValueError, match="alerts.email.*environment 'local'.*mapping"
    ):
        ConfigurationManager(config_path=str(config_file))


@pytest.mark.parametrize("value", ["warn", "strict"])
def test_config_accepts_classification_propagation(tmp_path, value):
    manager = ConfigurationManager(
        config_path=str(
            _write_local_config(tmp_path, classification_propagation=value)
        )
    )

    assert manager.get_value("classification_propagation") == value


@pytest.mark.parametrize("value", ["off", "WARN", None, 1, []])
def test_config_rejects_invalid_classification_propagation(tmp_path, value):
    config_file = _write_local_config(
        tmp_path, classification_propagation=value
    )

    with pytest.raises(
        ValueError,
        match="classification_propagation.*environment 'local'.*Expected one of",
    ):
        ConfigurationManager(config_path=str(config_file))


def test_config_accepts_absent_governance_wiring_keys(tmp_path):
    manager = ConfigurationManager(
        config_path=str(_write_local_config(tmp_path))
    )

    assert manager.get_value("alerts") is None
    assert manager.get_value("classification_propagation") is None


@pytest.mark.parametrize(
    "config",
    [
        None,
        {},
        {"observability": None},
        {"observability": {}},
        {"observability": {"lineage": None}},
    ],
)
def test_parse_lineage_config_defaults(config):
    assert parse_lineage_config(config) == LineageConfig()


def test_parse_lineage_config_accepts_full_http_config():
    config = parse_lineage_config(
        {
            "observability": {
                "lineage": {
                    "emitter": "http",
                    "url": "https://lineage.example.test",
                    "endpoint": "/events",
                    "job_namespace": "analytics",
                    "dataset_namespace": "unitycatalog://workspace.example.test",
                    "timeout_seconds": 12.5,
                }
            }
        }
    )

    assert config == LineageConfig(
        emitter="http",
        url="https://lineage.example.test",
        endpoint="/events",
        job_namespace="analytics",
        dataset_namespace="unitycatalog://workspace.example.test",
        timeout_seconds=12.5,
    )


@pytest.mark.parametrize(
    ("lineage", "key"),
    [
        ([], "observability.lineage"),
        ({"unexpected": True}, "observability.lineage.unexpected"),
        ({"emitter": "console"}, "observability.lineage.emitter"),
        ({"emitter": "http"}, "observability.lineage.url"),
        ({"url": "ftp://lineage.example.test"}, "observability.lineage.url"),
        ({"url": 1}, "observability.lineage.url"),
        ({"endpoint": ""}, "observability.lineage.endpoint"),
        ({"endpoint": "events"}, "observability.lineage.endpoint"),
        ({"endpoint": None}, "observability.lineage.endpoint"),
        ({"job_namespace": ""}, "observability.lineage.job_namespace"),
        ({"job_namespace": None}, "observability.lineage.job_namespace"),
        ({"dataset_namespace": ""}, "observability.lineage.dataset_namespace"),
        ({"dataset_namespace": 1}, "observability.lineage.dataset_namespace"),
        ({"timeout_seconds": True}, "observability.lineage.timeout_seconds"),
        ({"timeout_seconds": 0}, "observability.lineage.timeout_seconds"),
        ({"timeout_seconds": -1}, "observability.lineage.timeout_seconds"),
        ({"timeout_seconds": 61}, "observability.lineage.timeout_seconds"),
        ({"timeout_seconds": "5"}, "observability.lineage.timeout_seconds"),
    ],
)
def test_parse_lineage_config_rejects_invalid_values(lineage, key):
    with pytest.raises(ValueError) as exc_info:
        parse_lineage_config({"observability": {"lineage": lineage}})

    assert key in str(exc_info.value)


def test_parse_lineage_config_rejects_non_mapping_observability():
    with pytest.raises(ValueError, match="observability.*mapping"):
        parse_lineage_config({"observability": []})


@pytest.mark.parametrize("key", ["api_key", "apiKey", "token"])
def test_parse_lineage_config_rejects_api_key_settings(key):
    with pytest.raises(ValueError) as exc_info:
        parse_lineage_config({"observability": {"lineage": {key: "secret"}}})

    message = str(exc_info.value)
    assert f"observability.lineage.{key}" in message
    assert "OPENLINEAGE_API_KEY" in message
    assert "secret" not in message


def test_lineage_config_never_discloses_url():
    secret_url = "https://user:s3cret@host"

    assert secret_url not in repr(LineageConfig(url=secret_url))
    with pytest.raises(ValueError) as exc_info:
        parse_lineage_config(
            {"observability": {"lineage": {"url": secret_url, "endpoint": []}}}
        )
    assert secret_url not in str(exc_info.value)


def test_configuration_manager_validates_global_lineage_config(tmp_path):
    config_file = _write_local_config(tmp_path)
    config_file.write_text(
        yaml.safe_dump(
            {
                "priority_check": ["local"],
                "environments": {"local": {"catalog": None}},
                "observability": {"lineage": {"timeout_seconds": 0}},
            }
        )
    )

    with pytest.raises(ValueError, match="observability.lineage.timeout_seconds"):
        ConfigurationManager(config_path=str(config_file))


def test_configuration_manager_accepts_absent_global_lineage_config(tmp_path):
    manager = ConfigurationManager(config_path=str(_write_local_config(tmp_path)))

    assert manager.current_env_name == "local"
