"""
Compatibility shim — keeps `from skifer.registry import RuleRegistry` working.
The canonical location is skifer.core.registry.
"""
from skifer.core.registry import RuleRegistry

__all__ = ["RuleRegistry"]
