"""
Configuration module. Handles YAML loading and Environment detection.
Includes auto-discovery mechanism to find config.yaml at project root.
"""

from __future__ import annotations
import yaml
import os
from dataclasses import dataclass, field
from typing import Mapping


VALID_TRACING_EXPORTERS = ("none", "otlp", "mlflow", "dual")


@dataclass(frozen=True)
class TracingConfig:
    """Validated global tracing configuration from ``config.yaml``."""

    exporter: str = "none"
    required: bool = False
    capture_prompts: bool = False
    capture_sql: bool = False
    user_identity: str = "omit"
    trace_location: str | None = None
    otlp_endpoint: str | None = field(default=None, repr=False)
    otlp_headers: Mapping[str, str] = field(default_factory=dict, repr=False)
    mlflow_tracking_uri: str | None = field(default=None, repr=False)

    def __repr__(self) -> str:
        """Never disclose credentials embedded in exporter configuration."""
        endpoint = "<configured>" if self.otlp_endpoint else None
        headers = "<redacted>" if self.otlp_headers else {}
        tracking_uri = "<configured>" if self.mlflow_tracking_uri else None
        return (
            "TracingConfig("
            f"exporter={self.exporter!r}, required={self.required!r}, "
            f"capture_prompts={self.capture_prompts!r}, "
            f"capture_sql={self.capture_sql!r}, user_identity={self.user_identity!r}, "
            f"trace_location={self.trace_location!r}, otlp_endpoint={endpoint!r}, "
            f"otlp_headers={headers!r}, mlflow_tracking_uri={tracking_uri!r})"
        )


def parse_tracing_config(config: dict | None) -> TracingConfig:
    """Return validated global tracing settings without loading exporter SDKs."""
    if not config:
        return TracingConfig()
    observability = config.get("observability", {})
    if observability is None:
        observability = {}
    if not isinstance(observability, dict):
        raise ValueError("Global 'observability' configuration must be a mapping.")
    raw = observability.get("tracing", {})
    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raise ValueError("Global 'observability.tracing' configuration must be a mapping.")

    exporter = raw.get("exporter", "none")
    if exporter not in VALID_TRACING_EXPORTERS:
        raise ValueError(
            f"Unsupported tracing exporter {exporter!r}. Valid values: "
            f"{', '.join(VALID_TRACING_EXPORTERS)}."
        )
    for key in ("required", "capture_prompts", "capture_sql"):
        if key in raw and not isinstance(raw[key], bool):
            raise ValueError(f"observability.tracing.{key} must be a boolean.")
    user_identity = raw.get("user_identity", "omit")
    if user_identity not in ("omit", "hmac"):
        raise ValueError("observability.tracing.user_identity must be 'omit' or 'hmac'.")
    trace_location = raw.get("trace_location")
    if trace_location is not None and not isinstance(trace_location, str):
        raise ValueError("observability.tracing.trace_location must be a string or null.")
    otlp_endpoint = raw.get("otlp_endpoint", raw.get("endpoint"))
    if otlp_endpoint is not None and not isinstance(otlp_endpoint, str):
        raise ValueError("observability.tracing.otlp_endpoint must be a string or null.")
    otlp_headers = raw.get("otlp_headers", raw.get("headers", {}))
    if otlp_headers is None:
        otlp_headers = {}
    if not isinstance(otlp_headers, dict) or not all(
        isinstance(key, str) and isinstance(value, str)
        for key, value in otlp_headers.items()
    ):
        raise ValueError(
            "observability.tracing.otlp_headers must be a mapping of strings."
        )
    mlflow_tracking_uri = raw.get("mlflow_tracking_uri")
    if mlflow_tracking_uri is not None and not isinstance(mlflow_tracking_uri, str):
        raise ValueError(
            "observability.tracing.mlflow_tracking_uri must be a string or null."
        )
    return TracingConfig(
        exporter=exporter,
        required=raw.get("required", False),
        capture_prompts=raw.get("capture_prompts", False),
        capture_sql=raw.get("capture_sql", False),
        user_identity=user_identity,
        trace_location=trace_location,
        otlp_endpoint=otlp_endpoint,
        otlp_headers=dict(otlp_headers),
        mlflow_tracking_uri=mlflow_tracking_uri,
    )


VALID_LINEAGE_EMITTERS = ("none", "http")


@dataclass(frozen=True)
class LineageConfig:
    """Validated global lineage configuration from ``config.yaml``."""

    emitter: str = "none"
    url: str | None = field(default=None, repr=False)
    endpoint: str = "/api/v1/lineage"
    job_namespace: str = "skifer"
    dataset_namespace: str | None = None
    timeout_seconds: float = 5.0

    def __repr__(self) -> str:
        """Never disclose credentials embedded in the lineage URL."""
        url = "<configured>" if self.url else None
        return (
            "LineageConfig("
            f"emitter={self.emitter!r}, url={url!r}, endpoint={self.endpoint!r}, "
            f"job_namespace={self.job_namespace!r}, "
            f"dataset_namespace={self.dataset_namespace!r}, "
            f"timeout_seconds={self.timeout_seconds!r})"
        )


def parse_lineage_config(config: dict | None) -> LineageConfig:
    """Return validated global lineage settings without loading emitter code."""
    if not config:
        return LineageConfig()
    observability = config.get("observability", {})
    if observability is None:
        observability = {}
    if not isinstance(observability, dict):
        raise ValueError("Global 'observability' configuration must be a mapping.")
    raw = observability.get("lineage", {})
    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raise ValueError("Global 'observability.lineage' configuration must be a mapping.")

    allowed_keys = {
        "emitter",
        "url",
        "endpoint",
        "job_namespace",
        "dataset_namespace",
        "timeout_seconds",
    }
    for key in raw:
        if key not in allowed_keys:
            message = f"Unknown configuration key observability.lineage.{key}."
            if key in {"api_key", "apiKey", "token"}:
                message += (
                    " The API key is read only from the OPENLINEAGE_API_KEY "
                    "environment variable."
                )
            raise ValueError(message)

    emitter = raw.get("emitter", "none")
    if emitter not in VALID_LINEAGE_EMITTERS:
        raise ValueError(
            f"Unsupported observability.lineage.emitter {emitter!r}. Valid values: "
            f"{', '.join(VALID_LINEAGE_EMITTERS)}."
        )
    url = raw.get("url")
    if url is not None and (
        not isinstance(url, str)
        or not url.startswith(("http://", "https://"))
    ):
        raise ValueError(
            "observability.lineage.url must be a string starting with http:// or https://, or null."
        )
    if emitter == "http" and url is None:
        raise ValueError(
            "observability.lineage.url is required when observability.lineage.emitter is 'http'."
        )
    endpoint = raw.get("endpoint", "/api/v1/lineage")
    if not isinstance(endpoint, str) or not endpoint or not endpoint.startswith("/"):
        raise ValueError(
            "observability.lineage.endpoint must be a non-empty string starting with '/'."
        )
    job_namespace = raw.get("job_namespace", "skifer")
    if not isinstance(job_namespace, str) or not job_namespace:
        raise ValueError(
            "observability.lineage.job_namespace must be a non-empty string."
        )
    dataset_namespace = raw.get("dataset_namespace")
    if dataset_namespace is not None and (
        not isinstance(dataset_namespace, str) or not dataset_namespace
    ):
        raise ValueError(
            "observability.lineage.dataset_namespace must be a non-empty string or null."
        )
    timeout_seconds = raw.get("timeout_seconds", 5.0)
    if (
        isinstance(timeout_seconds, bool)
        or not isinstance(timeout_seconds, (int, float))
        or not 0 < timeout_seconds <= 60
    ):
        raise ValueError(
            "observability.lineage.timeout_seconds must be a number greater than 0 and at most 60."
        )
    return LineageConfig(
        emitter=emitter,
        url=url,
        endpoint=endpoint,
        job_namespace=job_namespace,
        dataset_namespace=dataset_namespace,
        timeout_seconds=timeout_seconds,
    )



CANDIDATE_FALLBACK_MODES = ("off", "shadow", "on")


def candidate_fallback_mode(env_config: dict | None, *, env_name: str | None = None) -> str:
    """``candidate_fallback`` of an environment: ``off`` (default), ``shadow`` or ``on``.

    YAML 1.1 reads a bare ``off``/``on`` as a boolean, so ``False``/``True`` are
    accepted as those two words; anything else is a configuration error.
    """
    value = (env_config or {}).get("candidate_fallback", "off")
    if value is False:
        value = "off"
    elif value is True:
        value = "on"
    if value not in CANDIDATE_FALLBACK_MODES:
        where = f" for environment '{env_name}'" if env_name else ""
        raise ValueError(
            f"Invalid candidate_fallback{where}: {value!r}. Expected one of "
            f"{list(CANDIDATE_FALLBACK_MODES)}."
        )
    return value

# config key -> ValidationThresholds field (Plan 44, S05)
CANDIDATE_THRESHOLD_KEYS = {
    "candidates_min_requests": "min_requests",
    "candidates_min_distinct_requesters": "min_distinct_requesters",
    "candidates_min_days": "min_days",
    "candidates_stability_runs": "stability_runs",
    "candidates_min_stability": "min_stability",
}


def candidate_validation_thresholds(
    env_config: dict | None, *, env_name: str | None = None
) -> dict:
    """The ``ValidationThresholds`` keyword arguments an environment overrides.

    Counts are non-negative integers (``candidates_stability_runs`` at least 1),
    ``candidates_min_stability`` is within ``[0, 1]``; anything else is a
    configuration error, never a silently clamped threshold.
    """
    where = f" for environment '{env_name}'" if env_name else ""
    thresholds: dict = {}
    for key, field_name in CANDIDATE_THRESHOLD_KEYS.items():
        if key not in (env_config or {}):
            continue
        value = env_config[key]
        if key == "candidates_min_stability":
            valid = (not isinstance(value, bool) and isinstance(value, (int, float))
                     and 0 <= value <= 1)
            expected = "a number within [0, 1]"
        else:
            minimum = 1 if key == "candidates_stability_runs" else 0
            valid = not isinstance(value, bool) and isinstance(value, int) and value >= minimum
            expected = "a positive integer" if minimum else "a non-negative integer"
        if not valid:
            raise ValueError(f"Invalid {key}{where}: {value!r}. Expected {expected}.")
        thresholds[field_name] = value
    return thresholds


DEFAULT_CANDIDATES_MIN_SAMPLE = 30
DRIFT_THRESHOLD_SIGNALS = ("fallback_rate", "orphan_certified_metrics")


def candidate_drift_settings(
    env_config: dict | None, *, env_name: str | None = None
) -> tuple[int, dict]:
    """(``candidates_min_sample``, ``candidates_drift_thresholds``) of an environment.

    Both are validated. The thresholds are read for a future calibration and are
    never applied in v1: a drift report always says ``thresholds_applied: False``.
    """
    where = f" for environment '{env_name}'" if env_name else ""
    env_config = env_config or {}
    min_sample = env_config.get("candidates_min_sample", DEFAULT_CANDIDATES_MIN_SAMPLE)
    if isinstance(min_sample, bool) or not isinstance(min_sample, int) or min_sample < 0:
        raise ValueError(f"Invalid candidates_min_sample{where}: {min_sample!r}. "
                         "Expected a non-negative integer.")
    thresholds = env_config.get("candidates_drift_thresholds") or {}
    if not isinstance(thresholds, dict):
        raise ValueError(f"Invalid candidates_drift_thresholds{where}: expected a mapping.")
    for name, value in thresholds.items():
        if name not in DRIFT_THRESHOLD_SIGNALS:
            raise ValueError(f"Invalid candidates_drift_thresholds{where}: unknown signal "
                             f"{name!r}; expected one of {list(DRIFT_THRESHOLD_SIGNALS)}.")
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not 0 <= value <= 1:
            raise ValueError(f"Invalid candidates_drift_thresholds.{name}{where}: {value!r}. "
                             "Expected a number within [0, 1].")
    return min_sample, dict(thresholds)


class ConfigurationManager:
    """
    Manages the loading and retrieval of configuration from a YAML file.
    Automatically detects the current environment by testing catalog access.
    """
    def __init__(self, config_path=None, backend=None):
        """
        Initializes the ConfigurationManager.

        Args:
            config_path (str, optional): Absolute path to the config.yaml file.
                                          If not provided, the framework will attempt
                                          to auto-discover it by searching parent directories.
            backend: SparkBackend instance used for catalog access checks.
                     When None, only null-catalog (local) environments can be detected.

        Raises:
            FileNotFoundError: If the configuration file cannot be found.
        """
        self._backend = backend
        self.config = {}
        self.current_env_name = None
        self.db_prefix = None
        
        # 1. Path Resolution logic
        final_path = config_path
        
        if not final_path:
            # Try to auto-discover at current location or parents
            print("[Config] No path provided. Searching for 'config.yaml' in parent directories...")
            final_path = self._find_config_upwards("config.yaml")
            
        if final_path:
            self._load_config_file(final_path)
            self.current_env_name = self._detect_environment()
            self.db_prefix = self.get_value("catalog") or None  # None for local envs
        else:
            # Critical error: Framework cannot run without config
            raise FileNotFoundError(
                "[Config] CRITICAL: 'config.yaml' not found in current directory or any parent directory. "
                "Please verify your project structure."
            )

    def _find_config_upwards(self, filename):
        """
        Searches for a file starting from current working directory and moving up.
        
        Args:
            filename (str): The name of the file to search for.
            
        Returns:
            str: Absolute path to the file if found, else None.
        """
        current_dir = os.getcwd()
        
        # Loop until we hit the root of the filesystem
        while True:
            check_path = os.path.join(current_dir, filename)
            if os.path.exists(check_path):
                print(f"   [Config] Auto-discovered config file at: {check_path}")
                return check_path
            
            parent_dir = os.path.dirname(current_dir)
            if parent_dir == current_dir:
                # We hit the filesystem root (e.g. /) without finding the file
                return None
            current_dir = parent_dir

    def _load_config_file(self, path):
        """
        Loads the YAML configuration file into the object's state.
        
        Args:
            path (str): The absolute path to the YAML file.
            
        Raises:
            ValueError: If the YAML file contains syntax errors.
        """
        print(f"   [Config] Loading configuration from: {path}")
        try:
            with open(path, 'r') as f:
                loaded = yaml.safe_load(f)
            if not isinstance(loaded, dict):
                raise ValueError(
                    "Error parsing YAML file: top-level config must be a mapping with "
                    "'environments' and 'priority_check' keys."
                )
            self.config = loaded
            parse_tracing_config(self.config)
            parse_lineage_config(self.config)
            envs = self.config.get("environments", {})
            if isinstance(envs, dict):
                valid_policies = {"off", "warn", "enforce", "supervised"}
                for env_name, env_config in envs.items():
                    if not isinstance(env_config, dict):
                        continue
                    if "semantic_certification_policy" in env_config:
                        policy = env_config["semantic_certification_policy"]
                        if policy not in valid_policies:
                            raise ValueError(
                                "Invalid semantic_certification_policy for environment "
                                f"'{env_name}': {policy!r}. Expected one of "
                                f"{sorted(valid_policies)}."
                            )
                    if "uc_certification_tags" in env_config and not isinstance(
                        env_config["uc_certification_tags"], bool
                    ):
                        raise ValueError(
                            "Invalid uc_certification_tags for environment "
                            f"'{env_name}': {env_config['uc_certification_tags']!r}. "
                            "Expected true or false."
                        )
                    if "classification_propagation" in env_config:
                        propagation = env_config["classification_propagation"]
                        valid_propagation = {"warn", "strict"}
                        if (
                            not isinstance(propagation, str)
                            or propagation not in valid_propagation
                        ):
                            raise ValueError(
                                "Invalid classification_propagation for environment "
                                f"'{env_name}': {propagation!r}. Expected one of "
                                f"{sorted(valid_propagation)}."
                            )
                    if "alerts" in env_config:
                        alerts = env_config["alerts"]
                        if not isinstance(alerts, dict):
                            raise ValueError(
                                f"Invalid alerts for environment '{env_name}': "
                                "alerts must be a mapping."
                            )
                        allowed_alert_keys = {
                            "webhook_url",
                            "slack_webhook",
                            "msteams_webhook",
                            "google_chat_webhook",
                            "email",
                            "min_severity",
                            "max_depth",
                        }
                        for alert_key in alerts:
                            if alert_key not in allowed_alert_keys:
                                raise ValueError(
                                    f"Invalid alerts for environment '{env_name}': "
                                    f"unknown key {alert_key!r}."
                                )
                        webhook_keys = {
                            "webhook_url",
                            "slack_webhook",
                            "msteams_webhook",
                            "google_chat_webhook",
                        }
                        for webhook_key in webhook_keys:
                            if webhook_key in alerts and (
                                not isinstance(alerts[webhook_key], str)
                                or not alerts[webhook_key]
                            ):
                                raise ValueError(
                                    f"Invalid alerts.{webhook_key} for environment "
                                    f"'{env_name}': expected a non-empty string."
                                )
                        if "email" in alerts and not isinstance(alerts["email"], dict):
                            raise ValueError(
                                f"Invalid alerts.email for environment '{env_name}': "
                                "expected a mapping."
                            )
                        if "min_severity" in alerts:
                            severity = alerts["min_severity"]
                            valid_severities = {"info", "warning", "critical"}
                            if (
                                not isinstance(severity, str)
                                or severity not in valid_severities
                            ):
                                raise ValueError(
                                    f"Invalid alerts.min_severity for environment "
                                    f"'{env_name}': {severity!r}. Expected one of "
                                    f"{sorted(valid_severities)}."
                                )
                        if "max_depth" in alerts:
                            max_depth = alerts["max_depth"]
                            if (
                                isinstance(max_depth, bool)
                                or not isinstance(max_depth, int)
                                or max_depth < 0
                            ):
                                raise ValueError(
                                    f"Invalid alerts.max_depth for environment "
                                    f"'{env_name}': expected an integer greater than "
                                    "or equal to 0."
                                )
                    if "capability_autonomy" in env_config:
                        autonomy = env_config["capability_autonomy"]
                        if autonomy not in {"shadow", "supervised"}:
                            raise ValueError(
                                "Invalid capability_autonomy for environment "
                                f"'{env_name}': {autonomy!r}. Expected 'shadow' or "
                                "'supervised'; guarded is deliberately not configurable in v1."
                            )
                    if "candidate_fallback" in env_config:
                        candidate_fallback_mode(env_config, env_name=env_name)
                    candidate_validation_thresholds(env_config, env_name=env_name)
                    candidate_drift_settings(env_config, env_name=env_name)
                    if "candidates_decider_identity" in env_config:
                        identity = env_config["candidates_decider_identity"]
                        if identity not in {"hmac", "plain"}:
                            raise ValueError(
                                "Invalid candidates_decider_identity for environment "
                                f"'{env_name}': {identity!r}. Expected 'hmac' or 'plain'."
                            )
                    if "candidates_store_questions" in env_config:
                        mode = env_config["candidates_store_questions"]
                        if mode not in {"full", "redacted"}:
                            raise ValueError(
                                "Invalid candidates_store_questions for environment "
                                f"'{env_name}': {mode!r}. Expected 'full' or 'redacted'."
                            )
                    if "candidates_retention_days" in env_config:
                        retention = env_config["candidates_retention_days"]
                        if (
                            isinstance(retention, bool)
                            or not isinstance(retention, int)
                            or retention <= 0
                        ):
                            raise ValueError(
                                "Invalid candidates_retention_days for environment "
                                f"'{env_name}': {retention!r}. Expected a positive integer."
                            )
        except yaml.YAMLError as exc:
            raise ValueError(f"Error parsing YAML file: {exc}")

    def _detect_environment(self):
        """
        Auto-detects the active environment by iterating through the 'priority_check'
        list in the config and attempting to connect to the associated catalog.

        A null/empty catalog means a local environment (no Unity Catalog required):
        it is always considered accessible and selected immediately.

        Returns:
            str: The name of the successfully detected environment.

        Raises:
            EnvironmentError: If no catalogs defined in the configuration are accessible.
        """
        envs = self.config.get("environments", {})
        priority = self.config.get("priority_check", envs.keys())

        print("   [Config] Auto-detecting environment...")

        for env_name in priority:
            if env_name not in envs:
                continue

            catalog = envs[env_name].get("catalog")

            # Null catalog = local environment, no Databricks catalog needed.
            if not catalog:
                print(f"   [Config] Local environment '{env_name.upper()}' selected (no catalog).")
                return env_name

            try:
                if self._backend is not None:
                    if self._backend.check_catalog_access(catalog):
                        print(f"   [Config] Success: Connected to catalog '{catalog}'. Environment is '{env_name.upper()}'.")
                        return env_name
                else:
                    raise EnvironmentError(
                        f"No backend available to verify catalog '{catalog}'. "
                        f"Pass a backend= to ConfigurationManager or use a null-catalog (local) environment."
                    )
            except Exception:
                continue

        raise EnvironmentError("Could not connect to any catalog defined in the configuration file.")

    def get_value(self, key):
        """
        Retrieves a configuration value specific to the currently active environment.
        
        Args:
            key (str): The configuration key to retrieve.
            
        Returns:
            Any: The value associated with the key, or None if not found or no env is active.
        """
        if not self.current_env_name:
            return None
        return self.config["environments"][self.current_env_name].get(key)
    
    def get_db(self):
        """
        Retrieves the database (catalog) prefix for the current environment.
        
        Returns:
            str: The database/catalog prefix.
        """
        return self.db_prefix
