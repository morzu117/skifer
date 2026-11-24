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


def __getattr__(name):
    module = _EXPORTS.get(name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    obj = getattr(importlib.import_module(f"{__name__}.{module}"), name)
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
