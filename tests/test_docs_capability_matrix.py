"""The documented adapter matrix must match the code that enforces it."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from skifer.core.capabilities_matrix import ALL_CAPABILITIES, DATABRICKS_CAPABILITIES

CORE_DOC = Path(__file__).resolve().parent.parent / "docs" / "core.md"
_ROW = re.compile(r"^\|.*\|\s*`(?P<capability>\w+)`\s*\|\s*(?P<databricks>[✅❌])\s*\|\s*(?P<duckdb>[✅❌])\s*\|")


def _documented() -> dict[str, tuple[bool, bool]]:
    """Read the matrix out of the published page, capability by capability."""
    documented: dict[str, tuple[bool, bool]] = {}
    for line in CORE_DOC.read_text(encoding="utf-8").splitlines():
        match = _ROW.match(line.strip())
        if match:
            documented[match.group("capability")] = (
                match.group("databricks") == "✅",
                match.group("duckdb") == "✅",
            )
    return documented


def _duckdb_capabilities() -> frozenset[str]:
    duckdb = pytest.importorskip("duckdb")
    from skifer.core.adapters.duckdb import DuckDBAdapter

    connection = duckdb.connect()
    try:
        return frozenset(DuckDBAdapter(connection).capabilities)
    finally:
        connection.close()


def test_every_capability_appears_in_the_documented_matrix():
    """A capability added to the code and not to the page is invisible to users.

    They would meet it for the first time as a refusal, with no page saying which
    engine can do it — which is the one question a refusal makes them ask.
    """
    assert set(_documented()) == set(ALL_CAPABILITIES)


def test_the_documented_matrix_matches_what_the_adapters_declare():
    """A stale table is worse than none: it is trusted and it is wrong."""
    duckdb_capabilities = _duckdb_capabilities()

    drift = {
        capability: {
            "documented": documented,
            "actual": (
                capability in DATABRICKS_CAPABILITIES,
                capability in duckdb_capabilities,
            ),
        }
        for capability, documented in _documented().items()
        if documented
        != (capability in DATABRICKS_CAPABILITIES, capability in duckdb_capabilities)
    }

    assert not drift, f"docs/core.md disagrees with the adapters: {drift}"
