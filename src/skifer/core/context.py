"""
ExecutionContext — unified, serialisable snapshot of SkiferEngine runtime state.

Created by ``SkiferEngine.__init__`` from detected environment values.
The engine's historical attributes (``env``, ``db``, ``schema_suffix``, …) are
delegation properties that read/write through an ``ExecutionContext`` instance.
"""

from __future__ import annotations

from dataclasses import dataclass, field


IN_MEMORY_DATABASE = ":memory:"


def _environment_config(config: dict, force_env: str | None) -> dict:
    """Return the active environment block, matching its key case-insensitively."""
    environments = config.get("environments", {})
    selected_env = force_env or config.get("default_env")
    if selected_env is None and isinstance(environments, dict) and len(environments) == 1:
        selected_env = next(iter(environments))

    if not isinstance(environments, dict):
        return {}
    target = str(selected_env).casefold()
    return next(
        (
            value
            for key, value in environments.items()
            if str(key).casefold() == target and isinstance(value, dict)
        ),
        {},
    )


def resolve_runtime_mode(
    config: dict, force_env: str | None
) -> tuple[str, str]:
    """Resolve and validate the engine/adapter pair for one environment."""
    env_config = _environment_config(config, force_env)

    engine = env_config.get("engine", "spark")
    allowed_engines = {"spark", "sql"}
    if engine not in allowed_engines:
        raise ValueError(
            f"Invalid config key 'engine': received {engine!r}; "
            f"allowed values are {sorted(allowed_engines)}."
        )

    adapter = env_config.get("adapter", "databricks")
    allowed_adapters = {"databricks", "duckdb", "snowflake", "bigquery"}
    if adapter not in allowed_adapters:
        raise ValueError(
            f"Invalid config key 'adapter': received {adapter!r}; "
            f"allowed values are {sorted(allowed_adapters)}."
        )
    return engine, adapter


def resolve_adapter_database(config: dict, force_env: str | None) -> str:
    """Resolve the database an adapter connects to, defaulting to in-memory.

    The default keeps every existing configuration working, but an in-memory
    database is destroyed with the process: nothing a run writes survives it.
    Callers that persist results check for that rather than assume a path.
    """
    env_config = _environment_config(config, force_env)
    database = env_config.get("database", IN_MEMORY_DATABASE)
    if database is None:
        return IN_MEMORY_DATABASE
    if not isinstance(database, str) or not database.strip():
        raise ValueError(
            "Invalid config key 'database': expected a non-empty path or "
            f"':memory:'; received {database!r}."
        )
    return database.strip()


@dataclass
class ExecutionContext:
    """
    Holds all runtime-configuration values for one engine instance.

    Attributes:
        env:              Upper-cased environment name (e.g. ``"LOCAL"``, ``"DEV"``).
        db:               Active Unity Catalog name, or ``None`` for local mode.
        config:           Full parsed ``config.yaml`` dict.
        current_user:     Cleaned username suffix (e.g. ``"jdoe"``).
        is_job_execution: ``True`` when launched by an automated Databricks Job.
        is_local:         ``True`` when running in local PySpark mode (no catalog).
        schema_suffix:    Sandbox suffix appended to schema names (e.g. ``"_jdoe"``).
    """

    env: str = ""
    db: str | None = None
    config: dict = field(default_factory=dict)
    current_user: str = ""
    is_job_execution: bool = False
    is_local: bool = False
    schema_suffix: str = ""

    def env_config(self) -> dict:
        """
        Active environment's config block, matched case-insensitively.

        ``self.env`` is upper-cased (``"DEV"``) while ``config.yaml`` keys may be
        written in any case. Returns ``{}`` when no environment matches.
        """
        envs = (self.config or {}).get("environments", {})
        if not isinstance(envs, dict):
            return {}
        target = (self.env or "").lower()
        for key, value in envs.items():
            if key.lower() == target:
                return value if isinstance(value, dict) else {}
        return {}

    @property
    def is_production(self) -> bool:
        """``True`` when the active environment is flagged ``is_production``."""
        return bool(self.env_config().get("is_production", False))

    def env_params(self) -> dict:
        """
        Optional ``params`` mapping declared on the active environment.

        Returns ``{}`` when absent. Raises ``ValueError`` (fail-fast) when present
        but not a mapping, to keep config inconsistencies from passing silently.
        """
        params = self.env_config().get("params")
        if params is None:
            return {}
        if not isinstance(params, dict):
            raise ValueError(
                f"config.yaml: environments.{self.env}.params must be a mapping, "
                f"got {type(params).__name__}."
            )
        return params

    def alerts_config(self) -> dict:
        """Alert routing settings declared on the active environment."""
        return self.env_config().get("alerts", {})

    def classification_propagation(self) -> str:
        """Classification propagation mode for the active environment."""
        return self.env_config().get("classification_propagation", "warn")

    def engine_mode(self) -> str:
        """Execution engine selected for the active environment."""
        engine, _ = resolve_runtime_mode(self.config, self.env)
        return engine

    def adapter_name(self) -> str:
        """Runtime adapter selected for the active environment."""
        _, adapter = resolve_runtime_mode(self.config, self.env)
        return adapter

    def adapter_database(self) -> str:
        """Database the adapter connects to for the active environment."""
        return resolve_adapter_database(self.config, self.env)

    @property
    def default_params(self) -> dict:
        """
        Convenience dict for ``load_schema(params=…)`` injection.

        Merges the active environment's ``params`` (if any) under the built-in
        ``catalog``/``env`` keys. Built-ins win on collision so a stray
        ``params: {catalog: ...}`` cannot shadow the resolved catalog.
        """
        return {**self.env_params(), "catalog": self.db, "env": self.env}
