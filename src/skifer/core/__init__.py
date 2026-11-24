import importlib

_EXPORTS = {
    "SkiferEngine": "core",
    "RuleRegistry": "registry",
    "ConfigurationManager": "config",
}


def __getattr__(name):
    module = _EXPORTS.get(name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    obj = getattr(importlib.import_module(f"{__name__}.{module}"), name)
    globals()[name] = obj
    return obj


def __dir__():
    return sorted(__all__)


__all__ = ["SkiferEngine", "RuleRegistry", "ConfigurationManager"]
