"""Skifer — declarative data engineering framework for Spark and Databricks."""

import importlib
from typing import TYPE_CHECKING
from importlib.metadata import PackageNotFoundError, version as _distribution_version

try:
    __version__ = _distribution_version("skifer")
except PackageNotFoundError:  # running from an uninstalled source checkout
    __version__ = "unknown"

if TYPE_CHECKING:
    from .core.core import SkiferEngine
    from .core.spark_backend import SparkBackend
    from .core.registry import RuleRegistry
    from .core.config import ConfigurationManager
    from .utils import safe_columns
    from .utils import setup_notebook
    from .utils import configure_logging
    from .core.schema_loader import load_schema
    from .core.schema_loader import parse_schema
    from .core.rule_analyzer import RuleAnalyzer
    from .core.rule_planner import RulePlanner
    from .core.rule_planner import RuleCycleError
    from .core.rule_executor import RuleExecutor
    from .core.catalog_inspector import CatalogInspector
    from .core.catalog_inspector import CatalogError
    from .agentic.builder_agent import BuilderAgent
    from .lineage import LineageTracker
    from .lineage import LineageGraph
    from .lineage import LineageEdge
    from .lineage import DataDictionary
    from .lineage import LineageRenderer

_EXPORTS = {
    "SkiferEngine": "core.core",
    "RuleRegistry": "core.registry",
    "ConfigurationManager": "core.config",
    "safe_columns": "utils",
    "setup_notebook": "utils",
    "configure_logging": "utils",
    "load_schema": "core.schema_loader",
    "parse_schema": "core.schema_loader",
    "RuleAnalyzer": "core.rule_analyzer",
    "RulePlanner": "core.rule_planner",
    "RuleCycleError": "core.rule_planner",
    "RuleExecutor": "core.rule_executor",
    "CatalogInspector": "core.catalog_inspector",
    "CatalogError": "core.catalog_inspector",
    "BuilderAgent": "agentic.builder_agent",
    "LineageTracker": "lineage",
    "LineageGraph": "lineage",
    "LineageEdge": "lineage",
    "DataDictionary": "lineage",
    "LineageRenderer": "lineage",
    "SparkBackend": "core.spark_backend",
}


# Plan 47.7 — which distribution ships a module outside skifer-core (the wheel that
# ships this file, so its own modules are never the missing ones). Every other
# skifer.* module ships in `skifer`. tests/test_packaging.py derives this table from
# packaging/*/pyproject.toml and fails if it lies.
_DISTRIBUTIONS = {
    "skifer.spark_factory": "skifer-engine",
    "skifer.core.spark_backend": "skifer-engine",
    "skifer.core.writer": "skifer-engine",
    "skifer.core.loaders": "skifer-engine",
    "skifer.core.interpreter": "skifer-engine",
    "skifer.core.rule_executor": "skifer-engine",
    "skifer.serving": "skifer-databricks",
}
_INSTALL_HINTS = {
    "skifer": "pip install skifer",
    "skifer-engine": "pip install 'skifer[spark]'",
    "skifer-databricks": "pip install 'skifer[serving]'",
}


def _distribution_of(module):
    for prefix, distribution in _DISTRIBUTIONS.items():
        if module == prefix or module.startswith(prefix + "."):
            return distribution
    return "skifer"


def _raise_if_distribution_missing(name, exc):
    """Name the distribution to install when a lazy export's skifer module is absent.

    A missing third-party dependency is left alone: the caller re-raises it unchanged.
    """
    missing = exc.name or ""
    if not missing.startswith("skifer."):
        return
    distribution = _distribution_of(missing)
    raise ModuleNotFoundError(
        f"`{name}` ships in the `{distribution}` distribution: "
        f"`{_INSTALL_HINTS[distribution]}` (module `{missing}` is not installed).",
        name=missing,
    ) from exc


def __getattr__(name):
    module = _EXPORTS.get(name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    try:
        carrier = importlib.import_module(f"{__name__}.{module}")
    except ModuleNotFoundError as exc:
        _raise_if_distribution_missing(name, exc)
        raise
    obj = getattr(carrier, name)
    globals()[name] = obj
    return obj


def __dir__():
    return sorted(__all__)


__all__ = [
    "__version__",
    "SkiferEngine",
    "SparkBackend",
    "RuleRegistry",
    "ConfigurationManager",
    "safe_columns",
    "setup_notebook",
    "configure_logging",
    "load_schema",
    "parse_schema",
    "RuleAnalyzer",
    "RulePlanner",
    "RuleCycleError",
    "RuleExecutor",
    "CatalogInspector",
    "CatalogError",
    "BuilderAgent",
    "LineageTracker",
    "LineageGraph",
    "LineageEdge",
    "DataDictionary",
    "LineageRenderer",
]
