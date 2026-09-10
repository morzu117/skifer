from .core.core import SkiferEngine
from .core.registry import RuleRegistry
from .core.config import ConfigurationManager
from .utils import safe_columns, setup_notebook, configure_logging
from .core.schema_loader import load_schema, parse_schema
from .core.rule_analyzer import RuleAnalyzer
from .core.rule_planner import RulePlanner, RuleCycleError
from .core.rule_executor import RuleExecutor
from .core.catalog_inspector import CatalogInspector, CatalogError
from .agentic.builder_agent import BuilderAgent
from .lineage import LineageTracker, LineageGraph, LineageEdge, DataDictionary, LineageRenderer


def __getattr__(name):
    # Lazy export: SparkBackend requires pyspark (optional [spark] extra), so
    # importing the package must not pull it in (CLI validate, semantic layer).
    if name == "SparkBackend":
        from .core.spark_backend import SparkBackend
        return SparkBackend
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
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
