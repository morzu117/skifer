"""
Registry module to manage dynamic registration of Business Rules and Data Loaders.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from skifer.core.rule_analyzer import RuleProfile

logger = logging.getLogger(__name__)

#: Valid rule kinds.
VALID_KINDS = frozenset({"projection", "aggregation", "transform"})


@dataclass
class RuleSpec:
    """Metadata container for a registered Business Rule."""

    name: str
    func: Callable
    kind: str  # "projection" | "aggregation" | "transform"
    # Lazy AST-analysis cache — not part of the public dataclass init.
    _profile: "RuleProfile | None" = field(default=None, init=False, compare=False, repr=False)

    def __call__(self, df):
        """Allow backward-compatible direct invocation (rule_spec(df))."""
        return self.func(df)

    @property
    def profile(self) -> "RuleProfile":
        """Return the cached AST profile, computing it on first access."""
        if self._profile is None:
            # Lazy import to avoid circular dependency (rule_analyzer → registry).
            from skifer.core.rule_analyzer import RuleAnalyzer  # noqa: PLC0415
            self._profile = RuleAnalyzer().analyze_rule(self.func, self.name)
        return self._profile


class RuleRegistry:
    """
    Singleton registry to store functions decorated with @register_rule or @register_loader.
    """
    _rules: dict[str, RuleSpec] = {}
    _loaders = {}

    @classmethod
    def register_rule(cls, name=None, kind: str = "projection"):
        """
        Decorator to register a Business Rule function.

        Args:
            name (str, optional): The name to register the rule under. If not provided,
                                  the function's name will be used.
            kind (str): Rule execution kind — ``"projection"`` (default),
                        ``"aggregation"``, or ``"transform"``.

                        * **projection** — the function must return
                          ``dict[str, Column]``.  All consecutive projection
                          rules are fused by the engine into a single
                          ``select()`` call.
                        * **aggregation** — the function must return a
                          ``DataFrame`` produced by ``groupBy(...).agg(...)``.
                          Adjacent aggregation rules that share the same
                          ``groupBy`` keys are fused automatically.
                        * **transform** — escape hatch for complex logic; the
                          function must return a ``DataFrame`` (legacy
                          ``df → df`` contract).

        Returns:
            function: The original function (unchanged), so the decorator is
            transparent to callers who still hold a reference to it.
        """
        if kind not in VALID_KINDS:
            raise ValueError(
                f"Invalid rule kind '{kind}'. Must be one of: "
                + ", ".join(sorted(VALID_KINDS))
            )

        def decorator(func):
            rule_name = name if name else func.__name__
            if rule_name in cls._rules:
                logger.warning("Overwriting existing rule: %s", rule_name)
            cls._rules[rule_name] = RuleSpec(name=rule_name, func=func, kind=kind)
            return func
        return decorator

    @classmethod
    def register_loader(cls, name=None):
        """
        Decorator to register a Data Loader function.
        
        Args:
            name (str, optional): The name to register the loader under. If not provided,
                                  the function's name will be used.
        
        Returns:
            function: The decorator function.
        """
        def decorator(func):
            loader_name = name if name else func.__name__
            cls._loaders[loader_name] = func
            return func
        return decorator

    @classmethod
    def get_rule(cls, name) -> "RuleSpec":
        """
        Retrieve a Business Rule by name.

        Args:
            name (str): The name of the registered business rule.

        Returns:
            RuleSpec: The registered rule metadata (access the function via ``.func``).

        Raises:
            ValueError: If the rule is not found in the registry.
        """
        if name not in cls._rules:
            raise ValueError(f"Business Rule '{name}' not found. Ensure the module containing it is imported.")
        entry = cls._rules[name]
        # Backward compat: if the entry was stored as a bare callable (e.g. set
        # directly in tests via _rules[name] = func), wrap it transparently.
        if callable(entry) and not isinstance(entry, RuleSpec):
            return RuleSpec(name=name, func=entry, kind="transform")
        return entry

    @classmethod
    def get_loader(cls, name):
        """
        Retrieve a Data Loader by name.
        
        Args:
            name (str): The name of the registered data loader.
            
        Returns:
            function: The registered data loader function.
            
        Raises:
            ValueError: If the loader is not found in the registry.
        """
        if name not in cls._loaders:
            raise ValueError(f"Data Loader '{name}' not found.")
        return cls._loaders[name]
    
    @classmethod
    def list_rules(cls):
        """
        List all registered rules.

        Returns:
            list[str]: A list of names of all currently registered business rules.
        """
        return list(cls._rules.keys())

    @classmethod
    def list_loaders(cls):
        """
        List all registered loaders.

        Returns:
            list[str]: A list of names of all currently registered data loaders.
        """
        return list(cls._loaders.keys())

    @classmethod
    def clear(cls, rules: bool = True, loaders: bool = True) -> None:
        """
        Remove all registered rules and/or loaders.

        Intended for test isolation — call in setUp/teardown to prevent
        rules registered in one test from bleeding into another.

        Args:
            rules (bool): Clear the rule registry (default True).
            loaders (bool): Clear the loader registry (default True).
        """
        if rules:
            cls._rules.clear()
        if loaders:
            cls._loaders.clear()
