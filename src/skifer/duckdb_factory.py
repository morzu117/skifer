"""Lazy DuckDB connection factory used only by the SQL execution mode."""

from __future__ import annotations

from typing import Any


def get_duckdb_connection(database: str = ":memory:") -> Any:
    """Create one non-shared DuckDB connection for the requested database."""
    try:
        import duckdb
    except ImportError as exc:
        raise RuntimeError(
            "DuckDB SQL execution requires the optional dependencies; "
            'install them with `pip install -e ".[sql]"`.'
        ) from exc
    return duckdb.connect(database=database)
