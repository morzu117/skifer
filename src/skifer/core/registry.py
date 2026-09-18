"""
Registry module to manage dynamic registration of Business Rules and Data Loaders.
"""

from __future__ import annotations

import inspect
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from skifer.core.rule_analyzer import RuleProfile

logger = logging.getLogger(__name__)

#: Valid rule kinds.
VALID_KINDS = frozenset({"projection", "aggregation", "transform", "sql"})

#: Valid loader kinds.
VALID_LOADER_KINDS = frozenset({"dataframe", "sql"})


@dataclass
class LoaderSpec:
    """Metadata container for a registered Data Loader.

    ``kind="dataframe"`` is the historical loader: Python builds a DataFrame and
    therefore needs an engine. ``kind="sql"`` returns a SQL **relation expression**
    instead, which is placed exactly where an adapter's file-source relation goes —
    so a portable loader is the user-space counterpart of ``resolve_source``.
    """

    name: str
    func: Callable
    kind: str  # "dataframe" | "sql"


@dataclass
class RuleSpec:
    """Metadata container for a registered Business Rule."""

    name: str
    func: Callable
    kind: str  # "projection" | "aggregation" | "transform" | "sql"
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
            from skifer.core.rule_analyzer import RuleAnalyzer, RuleProfile  # noqa: PLC0415
            if self.kind == "sql":
                # SQL expressions are deliberately opaque to the Python AST
                # analyzer. SQL parsing belongs to execution-time validation.
                self._profile = RuleProfile(name=self.name, source_available=False)
            else:
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
                        ``"aggregation"``, ``"transform"``, or ``"sql"``.

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
                        * **sql** — the function must return ``dict[str, str]``
                          mapping target columns to SQL expressions. It takes no
                          DataFrame argument and must be callable with no
                          arguments. These rules run on both the Spark and
                          compiled-SQL paths.

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
            if kind == "sql":
                required_parameters = [
                    parameter
                    for parameter in inspect.signature(func).parameters.values()
                    if parameter.default is inspect.Parameter.empty
                    and parameter.kind
                    not in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)
                ]
                if required_parameters:
                    raise TypeError(
                        f"SQL rule '{rule_name}' does not receive a DataFrame; "
                        "kind='sql' functions must be callable without arguments."
                    )
            if rule_name in cls._rules:
                logger.warning("Overwriting existing rule: %s", rule_name)
            cls._rules[rule_name] = RuleSpec(name=rule_name, func=func, kind=kind)
            return func
        return decorator

    @classmethod
    def register_loader(cls, name=None, kind="dataframe"):
        """
        Decorator to register a Data Loader function.

        Args:
            name (str, optional): The name to register the loader under. If not provided,
                                  the function's name will be used.
            kind (str): ``"dataframe"`` (default, historical: receives the engine and
                        returns a DataFrame) or ``"sql"`` (returns a SQL relation
                        expression and runs on any adapter).

        Returns:
            function: The decorator function.
        """
        if kind not in VALID_LOADER_KINDS:
            raise ValueError(
                f"Invalid loader kind '{kind}'. Valid kinds: {sorted(VALID_LOADER_KINDS)}."
            )

        def decorator(func):
            loader_name = name if name else func.__name__
            if kind == "sql":
                parameters = inspect.signature(func).parameters
                # A portable loader must not see the engine. Accepting `backend`
                # here would let one reach for Spark and compile everywhere but run
                # in one place — the failure would surface at the client, not here.
                forbidden = sorted({"backend", "config"} & set(parameters))
                if forbidden:
                    raise ValueError(
                        f"Loader '{loader_name}' is declared as kind='sql' but takes "
                        f"{forbidden} — a portable loader receives only the YAML "
                        "'arguments:' and never the engine."
                    )
            cls._loaders[loader_name] = LoaderSpec(
                name=loader_name, func=func, kind=kind
            )
            return func
        return decorator

    @classmethod
    def get_loader_spec(cls, name) -> "LoaderSpec":
        """Retrieve a Data Loader with its kind."""
        if name not in cls._loaders:
            raise ValueError(f"Data Loader '{name}' not found.")
        return cls._loaders[name]

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
        return cls._loaders[name].func
    
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
