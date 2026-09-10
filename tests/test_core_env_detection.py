"""
Tests for SkiferEngine catalog access and environment auto-detection.
v1.0: _check_catalog_access delegates entirely to the backend.
"""
import pytest
from unittest.mock import MagicMock, patch
from skifer.core.core import SkiferEngine


def _make_engine(config, backend=None):
    from skifer.core.context import ExecutionContext
    from skifer.core.interpreter import SchemaInterpreter
    engine = object.__new__(SkiferEngine)
    ctx = ExecutionContext()
    object.__setattr__(engine, "_context", ctx)
    engine.config = config
    if backend is not None:
        engine._backend = backend
        engine._interpreter = SchemaInterpreter(backend=backend, context=ctx)
    return engine


CONFIG_TWO_ENVS = {
    "priority_check": ["dev", "prod"],
    "environments": {
        "dev":  {"catalog": "catalog_dev"},
        "prod": {"catalog": "catalog_prd"},
    },
}

CONFIG_WITH_DEFAULT = {
    "priority_check": ["dev", "prod"],
    "default_env": "dev",
    "environments": {
        "dev":  {"catalog": "catalog_dev"},
        "prod": {"catalog": "catalog_prd"},
    },
}

CONFIG_EMPTY = {}


# ──────────────────────────────────────────────────────────────────────────────
# _check_catalog_access
# ──────────────────────────────────────────────────────────────────────────────

class TestCheckCatalogAccess:

    def test_returns_true_when_backend_grants_access(self):
        backend = MagicMock()
        backend.check_catalog_access.return_value = True
        engine = _make_engine(CONFIG_TWO_ENVS, backend=backend)
        assert engine._check_catalog_access("catalog_dev") is True
        backend.check_catalog_access.assert_called_once_with("catalog_dev")

    def test_returns_false_when_backend_denies_access(self):
        backend = MagicMock()
        backend.check_catalog_access.return_value = False
        engine = _make_engine(CONFIG_TWO_ENVS, backend=backend)
        assert engine._check_catalog_access("catalog_dev") is False
class TestAutoDetectEnvironment:

    def test_detects_first_accessible_env(self):
        engine = _make_engine(CONFIG_TWO_ENVS)
        engine._check_catalog_access = MagicMock(side_effect=lambda c: c == "catalog_dev")

        engine._auto_detect_environment()

        assert engine.env == "DEV"
        assert engine.db == "catalog_dev"

    def test_skips_inaccessible_env_and_detects_next(self):
        engine = _make_engine(CONFIG_TWO_ENVS)
        engine._check_catalog_access = MagicMock(side_effect=lambda c: c == "catalog_prd")

        engine._auto_detect_environment()

        assert engine.env == "PROD"
        assert engine.db == "catalog_prd"

    def test_uses_default_env_when_all_checks_fail(self):
        engine = _make_engine(CONFIG_WITH_DEFAULT)
        engine._check_catalog_access = MagicMock(return_value=False)

        engine._auto_detect_environment()

        assert engine.env == "DEV"
        assert engine.db == "catalog_dev"

    def test_raises_when_all_fail_and_no_default_env(self):
        engine = _make_engine(CONFIG_TWO_ENVS)
        engine._check_catalog_access = MagicMock(return_value=False)

        with pytest.raises(ValueError, match="CRITICAL"):
            engine._auto_detect_environment()

    def test_raises_on_invalid_config(self):
        engine = _make_engine(CONFIG_EMPTY)

        with pytest.raises(ValueError, match="CRITICAL"):
            engine._auto_detect_environment()

    def test_default_env_not_used_when_sql_succeeds(self):
        """default_env must not override a successfully detected environment."""
        engine = _make_engine(CONFIG_WITH_DEFAULT)
        engine._check_catalog_access = MagicMock(side_effect=lambda c: c == "catalog_prd")

        engine._auto_detect_environment()

        assert engine.env == "PROD"
        assert engine.db == "catalog_prd"