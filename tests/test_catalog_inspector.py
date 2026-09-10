"""
Tests unitaires pour CatalogInspector.
"""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock

from skifer.core.catalog_inspector import (
    CatalogInspector,
    CatalogError,
    _levenshtein,
    _top_suggestions,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_backend(
    tables: dict | None = None,
    columns: dict | None = None,
    exists: bool = True,
):
    """Create a mock Backend with configurable behaviour."""
    backend = MagicMock()
    backend.build_fqn.side_effect = lambda cat, sch, tbl: (
        f"`{cat}`.`{sch}`.`{tbl}`" if cat else f"`{sch}`.`{tbl}`"
    )
    backend.table_exists.return_value = exists
    backend.list_tables.return_value = list(tables.keys()) if tables else []
    if columns:
        def _list_columns(fqn):
            clean = fqn.replace("`", "")
            return columns.get(clean, [])
        backend.list_columns.side_effect = _list_columns
    else:
        backend.list_columns.return_value = []
    return backend


# ---------------------------------------------------------------------------
# Levenshtein
# ---------------------------------------------------------------------------

def test_levenshtein_identical():
    assert _levenshtein("abc", "abc") == 0


def test_levenshtein_insertion():
    assert _levenshtein("abc", "ab") == 1


def test_levenshtein_substitution():
    assert _levenshtein("abc", "axc") == 1


def test_top_suggestions_order():
    candidates = ["amount", "amunt", "region", "status"]
    suggestions = _top_suggestions("amoumt", candidates, n=2)
    # "amount" and "amunt" should be closer than "region"/"status"
    assert "amount" in suggestions or "amunt" in suggestions


def test_top_suggestions_empty():
    assert _top_suggestions("foo", []) == []


# ---------------------------------------------------------------------------
# fqn()
# ---------------------------------------------------------------------------

def test_fqn_with_catalog():
    backend = _make_backend()
    inspector = CatalogInspector(backend, catalog="my_catalog")
    result = inspector.fqn("silver", "orders")
    backend.build_fqn.assert_called_once_with("my_catalog", "silver", "orders")


def test_fqn_without_catalog():
    backend = _make_backend()
    inspector = CatalogInspector(backend, catalog=None)
    result = inspector.fqn("silver", "orders")
    backend.build_fqn.assert_called_once_with(None, "silver", "orders")


# ---------------------------------------------------------------------------
# _parse_fqn()
# ---------------------------------------------------------------------------

def test_parse_fqn_three_parts():
    inspector = CatalogInspector(_make_backend())
    cat, sch, tbl = inspector._parse_fqn("catalog.silver.orders")
    assert (cat, sch, tbl) == ("catalog", "silver", "orders")


def test_parse_fqn_two_parts_uses_default_catalog():
    inspector = CatalogInspector(_make_backend(), catalog="def_cat")
    cat, sch, tbl = inspector._parse_fqn("silver.orders")
    assert (cat, sch, tbl) == ("def_cat", "silver", "orders")


def test_parse_fqn_single_part_local_mode():
    inspector = CatalogInspector(_make_backend(), catalog=None)
    cat, sch, tbl = inspector._parse_fqn("orders_raw")
    assert tbl == "orders_raw"
    assert cat is None
    assert sch == ""


def test_parse_fqn_malformed_raises():
    inspector = CatalogInspector(_make_backend())
    with pytest.raises(ValueError, match="FQN mal formé"):
        inspector._parse_fqn("a.b.c.d")  # 4 parts → invalide


# ---------------------------------------------------------------------------
# list_tables / list_columns
# ---------------------------------------------------------------------------

def test_list_tables_delegates_to_backend():
    backend = _make_backend(tables={"orders": [], "customers": []})
    inspector = CatalogInspector(backend, catalog="cat")
    result = inspector.list_tables("silver")
    backend.list_tables.assert_called_once_with("silver", catalog="cat")


def test_list_columns_delegates_to_backend():
    cols = {"cat.silver.orders": ["id", "amount", "region"]}
    backend = _make_backend(columns=cols)
    inspector = CatalogInspector(backend, catalog="cat")
    result = inspector.list_columns("cat.silver.orders")
    assert set(result) == {"id", "amount", "region"}


# ---------------------------------------------------------------------------
# validate_table
# ---------------------------------------------------------------------------

def test_validate_table_success():
    backend = _make_backend(exists=True)
    inspector = CatalogInspector(backend)
    # Should not raise
    inspector.validate_table("silver.orders")


def test_validate_table_not_found_raises_catalog_error():
    backend = _make_backend(exists=False)
    backend.list_tables.return_value = ["orders", "customers"]
    inspector = CatalogInspector(backend)
    with pytest.raises(CatalogError, match="Table introuvable"):
        inspector.validate_table("silver.ordrers")  # typo


def test_validate_table_not_found_shows_suggestions():
    backend = _make_backend(exists=False)
    backend.list_tables.return_value = ["orders", "customers", "products"]
    inspector = CatalogInspector(backend)
    with pytest.raises(CatalogError) as exc_info:
        inspector.validate_table("silver.orderss")
    assert "orders" in str(exc_info.value)


# ---------------------------------------------------------------------------
# validate_columns
# ---------------------------------------------------------------------------

def test_validate_columns_success():
    cols = {"silver.orders": ["id", "amount", "region"]}
    backend = _make_backend(columns=cols)
    inspector = CatalogInspector(backend)
    # Should not raise
    inspector.validate_columns("silver.orders", ["id", "amount"])


def test_validate_columns_unknown_raises_catalog_error():
    cols = {"silver.orders": ["id", "amount", "region"]}
    backend = _make_backend(columns=cols)
    inspector = CatalogInspector(backend)
    with pytest.raises(CatalogError, match="Colonnes inconnues"):
        inspector.validate_columns("silver.orders", ["amoumt"])  # typo


def test_validate_columns_shows_levenshtein_suggestions():
    cols = {"silver.orders": ["amount", "region", "status"]}
    backend = _make_backend(columns=cols)
    inspector = CatalogInspector(backend)
    with pytest.raises(CatalogError) as exc_info:
        inspector.validate_columns("silver.orders", ["amoumt"])
    # Should suggest "amount"
    assert "amount" in str(exc_info.value)


def test_validate_columns_empty_real_cols_no_error():
    """If list_columns returns empty (unsupported backend), skip validation silently."""
    backend = _make_backend(columns={})
    backend.list_columns.return_value = []
    inspector = CatalogInspector(backend)
    # Should not raise even with made-up columns
    inspector.validate_columns("silver.orders", ["nonexistent"])


# ---------------------------------------------------------------------------
# list_schemas
# ---------------------------------------------------------------------------

def test_list_schemas_returns_list():
    backend = _make_backend()
    backend.list_schemas.return_value = ["silver", "gold"]
    inspector = CatalogInspector(backend, catalog="dev")
    assert inspector.list_schemas() == ["silver", "gold"]
    backend.list_schemas.assert_called_once_with("dev")


def test_list_schemas_empty_if_unsupported():
    backend = _make_backend()
    backend.list_schemas.return_value = []
    inspector = CatalogInspector(backend, catalog="dev")
    assert inspector.list_schemas() == []


# ---------------------------------------------------------------------------
# describe_table
# ---------------------------------------------------------------------------

def test_describe_table_returns_types():
    backend = _make_backend()
    backend.list_column_types.return_value = {"id": "bigint", "amount": "double"}
    inspector = CatalogInspector(backend, catalog="dev")
    result = inspector.describe_table("dev.gold.orders")
    assert result == {"id": "bigint", "amount": "double"}
    backend.list_column_types.assert_called_once_with("dev.gold.orders")


def test_describe_table_empty_if_unsupported():
    backend = _make_backend()
    backend.list_column_types.return_value = {}
    inspector = CatalogInspector(backend, catalog="dev")
    assert inspector.describe_table("dev.gold.orders") == {}
