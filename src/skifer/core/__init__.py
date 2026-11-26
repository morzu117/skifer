import importlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .core import SkiferEngine
    from .registry import RuleRegistry
    from .config import ConfigurationManager

_EXPORTS = {
    "SkiferEngine": "core",
    "RuleRegistry": "registry",
    "ConfigurationManager": "config",
}


def __getattr__(name):
    module = _EXPORTS.get(name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    try:
        carrier = importlib.import_module(f"{__name__}.{module}")
    except ModuleNotFoundError as exc:
        # Plan 47.7: name the distribution to install; a third-party one propagates.
        from skifer import _raise_if_distribution_missing

        _raise_if_distribution_missing(name, exc)
        raise
    obj = getattr(carrier, name)
    globals()[name] = obj
    return obj


def __dir__():
    return sorted(__all__)


__all__ = ["SkiferEngine", "RuleRegistry", "ConfigurationManager"]
