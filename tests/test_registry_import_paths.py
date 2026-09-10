"""
Tests for RuleRegistry import path compatibility and core __init__ exports.
All import paths must resolve to the same singleton class.
"""
from skifer.core.registry import RuleRegistry as RR_core
from skifer.registry import RuleRegistry as RR_shim
from skifer import RuleRegistry as RR_top


def test_all_import_paths_resolve_to_same_class():
    """All three import paths must return the exact same class (same singleton)."""
    assert RR_core is RR_shim
    assert RR_core is RR_top


def test_rule_registered_via_shim_is_visible_everywhere():
    """A rule registered using the shim import must be retrievable from all paths."""
    @RR_shim.register_rule(name="__test_shim_rule__")
    def _my_rule(df):
        return {}

    assert RR_core.get_rule("__test_shim_rule__").func is _my_rule
    assert RR_top.get_rule("__test_shim_rule__").func is _my_rule

    # Cleanup
    RR_core._rules.pop("__test_shim_rule__", None)


# ==============================================================================
# TESTS FOR core/__init__.py EXPORTS AND LOADER REGISTRATION
# ==============================================================================

def test_core_init_exports_public_api():
    """Importing skifer.core must expose SkiferEngine, RuleRegistry, ConfigurationManager."""
    import skifer.core as core_pkg

    assert hasattr(core_pkg, "SkiferEngine"), "SkiferEngine missing from skifer.core"
    assert hasattr(core_pkg, "RuleRegistry"), "RuleRegistry missing from skifer.core"
    assert hasattr(core_pkg, "ConfigurationManager"), "ConfigurationManager missing from skifer.core"


def test_core_init_triggers_loader_registration():
    """Importing skifer.core must register built-in loaders via the loaders side-effect import."""
    import importlib

    import skifer.core  # noqa: F401 — ensures the __init__ has been executed
    import skifer.core.loaders as _loaders_mod

    # Order-independent: another test may have called RuleRegistry.clear(),
    # which empties the loader registry. Re-run the module's decorator
    # side-effects so this test checks the import wiring, not global state.
    importlib.reload(_loaders_mod)

    assert "load_and_union_tables" in RR_core._loaders, (
        "load_and_union_tables not registered — loaders side-effect import may be broken"
    )
    assert "load_generic_explode_union" in RR_core._loaders, (
        "load_generic_explode_union not registered — loaders side-effect import may be broken"
    )
