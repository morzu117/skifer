"""
Tests for core/context.py — ExecutionContext.

Covers environment-default YAML params (config-backed engine.default_params)
and the case-insensitive environment matching that also fixes is_production
resolution (config keys are upper-cased, self.env is upper-cased).
"""
from __future__ import annotations

import pytest

from skifer.core.context import ExecutionContext


def _ctx(env="DEV", db="demo_catalog", env_block=None):
    config = {"environments": {"DEV": env_block or {}}}
    return ExecutionContext(env=env, db=db, config=config)


# ---------------------------------------------------------------------------
# default_params includes environment params
# ---------------------------------------------------------------------------

def test_default_params_includes_env_params():
    ctx = _ctx(env_block={
        "catalog": "demo_catalog",
        "is_production": False,
        "params": {"inbound_base_path": "/Volumes/demo_catalog/demo_schema/inbound"},
    })
    assert ctx.default_params == {
        "catalog": "demo_catalog",
        "env": "DEV",
        "inbound_base_path": "/Volumes/demo_catalog/demo_schema/inbound",
    }


def test_default_params_unchanged_without_params():
    """No env params configured -> only built-ins, behaviour unchanged."""
    ctx = _ctx(env_block={"catalog": "demo_catalog", "is_production": False})
    assert ctx.default_params == {"catalog": "demo_catalog", "env": "DEV"}


def test_default_params_no_environments_block():
    ctx = ExecutionContext(env="LOCAL", db=None, config={})
    assert ctx.default_params == {"catalog": None, "env": "LOCAL"}


# ---------------------------------------------------------------------------
# Priority: built-ins win over env params on collision (point 1)
# ---------------------------------------------------------------------------

def test_builtins_win_over_env_params_on_collision():
    """A stray params.catalog cannot shadow the resolved catalog."""
    ctx = _ctx(db="real_catalog", env_block={
        "catalog": "real_catalog",
        "params": {"catalog": "hijacked", "env": "WRONG", "extra": "kept"},
    })
    resolved = ctx.default_params
    assert resolved["catalog"] == "real_catalog"
    assert resolved["env"] == "DEV"
    assert resolved["extra"] == "kept"


# ---------------------------------------------------------------------------
# Defensive: params present but not a mapping -> fail-fast
# ---------------------------------------------------------------------------

def test_non_mapping_params_raises():
    ctx = _ctx(env_block={"params": ["not", "a", "mapping"]})
    with pytest.raises(ValueError, match="params must be a mapping"):
        _ = ctx.default_params


# ---------------------------------------------------------------------------
# Case-insensitive env matching — fixes is_production / params resolution
# ---------------------------------------------------------------------------

def test_env_matching_is_case_insensitive_for_params():
    """self.env='DEV' resolves a lower-case 'dev' config key (and vice versa)."""
    config = {"environments": {"dev": {"params": {"k": "v"}, "is_production": True}}}
    ctx = ExecutionContext(env="DEV", db="c", config=config)
    assert ctx.default_params["k"] == "v"
    assert ctx.is_production is True


def test_is_production_resolves_for_uppercase_keys():
    """Regression: PROD env with upper-case 'PROD' key is correctly detected."""
    config = {"environments": {"PROD": {"catalog": "p", "is_production": True}}}
    ctx = ExecutionContext(env="PROD", db="p", config=config)
    assert ctx.is_production is True


def test_is_production_defaults_false_when_absent():
    ctx = _ctx(env_block={"catalog": "c"})
    assert ctx.is_production is False
