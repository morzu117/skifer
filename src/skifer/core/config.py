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
                    if "capability_autonomy" in env_config:
                        autonomy = env_config["capability_autonomy"]
                        if autonomy not in {"shadow", "supervised"}:
                            raise ValueError(
                                "Invalid capability_autonomy for environment "
                                f"'{env_name}': {autonomy!r}. Expected 'shadow' or "
                                "'supervised'; guarded is deliberately not configurable in v1."
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
