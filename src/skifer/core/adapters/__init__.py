"""Runtime adapter contracts."""

from skifer.core.adapters.base import Adapter
from skifer.core.adapters.duckdb import DuckDBAdapter, DuckDBAdapterError

__all__ = ["Adapter", "DuckDBAdapter", "DuckDBAdapterError"]
