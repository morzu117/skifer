import logging

import pytest
from skifer.core.registry import RuleRegistry, RuleSpec

def test_register_rule():
    @RuleRegistry.register_rule(name="my_rule")
    def my_rule(df):
        return {}

    assert "my_rule" in RuleRegistry.list_rules()
    spec = RuleRegistry.get_rule("my_rule")
    assert isinstance(spec, RuleSpec)
    assert spec.func is my_rule
    assert spec.kind == "projection"


def test_register_rule_kind_aggregation():
    @RuleRegistry.register_rule(name="my_agg_rule", kind="aggregation")
    def my_agg_rule(df):
        return df

    spec = RuleRegistry.get_rule("my_agg_rule")
    assert spec.kind == "aggregation"


def test_register_rule_kind_transform():
    @RuleRegistry.register_rule(name="my_transform_rule", kind="transform")
    def my_transform_rule(df):
        return df

    spec = RuleRegistry.get_rule("my_transform_rule")
    assert spec.kind == "transform"


def test_register_rule_invalid_kind():
    with pytest.raises(ValueError, match="Invalid rule kind"):
        @RuleRegistry.register_rule(name="bad_kind_rule", kind="unknown_kind")
        def bad_kind_rule(df):
            return {}


def test_rule_spec_is_callable():
    """RuleSpec should remain callable for backward compatibility."""
    @RuleRegistry.register_rule(name="callable_rule_test", kind="transform")
    def callable_rule_test(df):
        return df

    spec = RuleRegistry.get_rule("callable_rule_test")
    sentinel = object()
    assert spec(sentinel) is sentinel


def test_register_loader():
    @RuleRegistry.register_loader(name="my_loader")
    def my_loader(config, table_fqns, **kwargs):
        pass

    assert RuleRegistry.get_loader("my_loader") == my_loader


def test_list_loaders_returns_list_of_strings():
    """list_loaders() should return a list of strings (API symmetry with list_rules)."""
    @RuleRegistry.register_loader(name="loader_for_list_test")
    def loader_for_list_test(config, **kwargs):
        pass

    result = RuleRegistry.list_loaders()
    assert isinstance(result, list)
    assert all(isinstance(item, str) for item in result)
    assert "loader_for_list_test" in result


def test_register_rule_overwrite_emits_logger_warning(caplog):
    """Re-registering an existing rule name should emit a logger.warning (not print)."""
    @RuleRegistry.register_rule(name="overwrite_target")
    def first(df):
        return {}

    with caplog.at_level(logging.WARNING, logger="skifer.core.registry"):
        @RuleRegistry.register_rule(name="overwrite_target")
        def second(df):
            return {}

    assert any(
        "Overwriting existing rule: overwrite_target" in record.message
        and record.levelno == logging.WARNING
        for record in caplog.records
    )
    RuleRegistry._rules.pop("overwrite_target", None)


def test_bare_callable_in_rules_dict_wrapped_transparently():
    """Entries stored directly as callables (e.g. in tests) are wrapped into RuleSpec."""
    def _bare_rule(df):
        return df

    RuleRegistry._rules["_bare_compat"] = _bare_rule
    try:
        spec = RuleRegistry.get_rule("_bare_compat")
        assert isinstance(spec, RuleSpec)
        assert spec.func is _bare_rule
        assert spec.kind == "transform"
    finally:
        RuleRegistry._rules.pop("_bare_compat", None)


def test_rule_spec_profile_cached_on_second_access():
    """RuleSpec.profile is computed once and cached on the instance (plan17-0.4)."""
    @RuleRegistry.register_rule()
    def _cached_rule(df):
        return {"x": None}

    spec = RuleRegistry.get_rule("_cached_rule")
    try:
        profile1 = spec.profile
        profile2 = spec.profile
        # Same object — proves no re-analysis on second call.
        assert profile1 is profile2
        assert profile1.name == "_cached_rule"
    finally:
        RuleRegistry._rules.pop("_cached_rule", None)
