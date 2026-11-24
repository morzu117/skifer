"""Deprecated module alias; use :mod:`skifer.semantic.resolver`."""

import importlib
import sys
import warnings

warnings.warn(
    "skifer.agentic.resolver moved to skifer.semantic.resolver",
    DeprecationWarning,
    stacklevel=2,
)

sys.modules[__name__] = importlib.import_module("skifer.semantic.resolver")
