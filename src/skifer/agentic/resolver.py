"""Deprecated compatibility shim; use :mod:`skifer.semantic.resolver`."""

# Imports follow the warning so callers see the deprecation at the old path.
# ruff: noqa: E402
import warnings

warnings.warn(
    "skifer.agentic.resolver moved to skifer.semantic.resolver",
    DeprecationWarning,
    stacklevel=2,
)

from skifer.semantic.resolver import *  # noqa: F401,F403
from skifer.semantic.resolver import (
    QueryResolver as QueryResolver,
    ResolvedQuery as ResolvedQuery,
    SelectedExpression as SelectedExpression,
    SemanticQuery as SemanticQuery,
    SemanticQueryError as SemanticQueryError,
)
