"""
ExecutionContext — unified, serialisable snapshot of SkiferEngine runtime state.

Created by ``SkiferEngine.__init__`` from detected environment values.
The engine's historical attributes (``env``, ``db``, ``schema_suffix``, …) are
delegation properties that read/write through an ``ExecutionContext`` instance.
"""

from __future__ import annotations

from dataclasses import dataclass, field


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

    @property
    def default_params(self) -> dict:
        """
        Convenience dict for ``load_schema(params=…)`` injection.

        Merges the active environment's ``params`` (if any) under the built-in
        ``catalog``/``env`` keys. Built-ins win on collision so a stray
        ``params: {catalog: ...}`` cannot shadow the resolved catalog.
        """
        return {**self.env_params(), "catalog": self.db, "env": self.env}
