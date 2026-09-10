"""
Tests for SandboxResolver.
v1.0 breaking change: spark param removed — backend is now required.
"""
import pytest
from unittest.mock import MagicMock, patch, call
from skifer.core.sandbox import SandboxResolver


@pytest.fixture
def mock_backend():
    b = MagicMock()
    b.schema_exists.return_value = False
    b.table_exists.return_value = False
    return b


@pytest.fixture
def resolver(mock_backend):
    """SandboxResolver backed by a mock backend."""
    mock_backend.schema_exists.return_value = True
    mock_backend.table_exists.return_value = False
    return SandboxResolver(backend=mock_backend)


@pytest.fixture
def local_resolver(mock_backend):
    """Fixture for local-mode tests (schema_exists=True for simplicity)."""
    mock_backend.schema_exists.return_value = True
    return SandboxResolver(backend=mock_backend)


# ==============================================================================
# parse_fqn
# ==============================================================================

def test_parse_fqn_two_part(resolver):
    catalog, schema, table = resolver.parse_fqn("silver.orders")
    assert catalog is None
    assert schema == "silver"
    assert table == "orders"


def test_parse_fqn_three_part(resolver):
    catalog, schema, table = resolver.parse_fqn("my_catalog.silver.orders")
    assert catalog == "my_catalog"
    assert schema == "silver"
    assert table == "orders"


def test_parse_fqn_with_backticks(resolver):
    catalog, schema, table = resolver.parse_fqn("`cat`.`sch`.`tbl`")
    assert catalog == "cat"
    assert schema == "sch"
    assert table == "tbl"


# ==============================================================================
# build_suffixed_fqn
# ==============================================================================

def test_build_suffixed_fqn_no_catalog(resolver):
    cat, schema, table = resolver.build_suffixed_fqn(None, "silver", "orders", "_jdoe")
    assert cat is None
    assert schema == "silver_jdoe"
    assert table == "orders"


def test_build_suffixed_fqn_with_catalog(resolver):
    cat, schema, table = resolver.build_suffixed_fqn("my_cat", "silver", "orders", "_jdoe")
    assert cat == "my_cat"
    assert schema == "silver_jdoe"
    assert table == "orders"


# ==============================================================================
# resolve
# ==============================================================================

def test_resolve_table_exists_in_sandbox(resolver):
    resolver.schema_exists = MagicMock(return_value=True)
    resolver.table_exists = MagicMock(return_value=True)
    result = resolver.resolve("silver.orders", "_jdoe")
    assert result == "`silver_jdoe`.`orders`"
    resolver.table_exists.assert_called_with(None, "silver_jdoe", "orders")


def test_resolve_table_missing_in_sandbox_clones(resolver):
    resolver.schema_exists = MagicMock(return_value=True)
    resolver.table_exists = MagicMock(side_effect=[False, True])
    resolver.clone_table = MagicMock()
    result = resolver.resolve("silver.orders", "_jdoe", missing_table_behavior="copy")
    assert result == "`silver_jdoe`.`orders`"
    resolver.clone_table.assert_called_once_with(
        None, "silver", "orders", None, "silver_jdoe", "orders"
    )


def test_resolve_table_missing_in_sandbox_error_behavior(resolver):
    resolver.schema_exists = MagicMock(return_value=True)
    resolver.table_exists = MagicMock(return_value=False)
    with pytest.raises(ValueError, match="not found in sandbox schema"):
        resolver.resolve("silver.orders", "_jdoe", missing_table_behavior="error")


def test_resolve_schema_missing_creates_and_clones(resolver):
    resolver.schema_exists = MagicMock(return_value=False)
    resolver.table_exists = MagicMock(return_value=True)
    resolver.create_schema = MagicMock()
    resolver.clone_table = MagicMock()
    result = resolver.resolve("silver.orders", "_jdoe")
    resolver.create_schema.assert_called_once_with(None, "silver_jdoe")
    resolver.clone_table.assert_called_once()
    assert "silver_jdoe" in result


def test_resolve_schema_missing_error_behavior(resolver):
    resolver.schema_exists = MagicMock(return_value=False)
    resolver.create_schema = MagicMock()
    with pytest.raises(ValueError, match="not found in sandbox schema"):
        resolver.resolve("silver.orders", "_jdoe", missing_table_behavior="error")
    resolver.create_schema.assert_called_once()


def test_resolve_source_table_missing_in_main_raises(resolver):
    resolver.schema_exists = MagicMock(return_value=True)
    resolver.table_exists = MagicMock(return_value=False)
    with pytest.raises(ValueError, match="not found in main schema"):
        resolver.resolve("silver.orders", "_jdoe", missing_table_behavior="copy")


# ==============================================================================
# clone_table / create_schema / schema_exists / table_exists — all delegate to backend
# ==============================================================================

def test_schema_exists_delegates_to_backend(mock_backend):
    mock_backend.schema_exists.return_value = True
    r = SandboxResolver(backend=mock_backend)
    assert r.schema_exists("my_cat", "silver") is True
    mock_backend.schema_exists.assert_called_once_with("my_cat", "silver")


def test_table_exists_delegates_to_backend(mock_backend):
    mock_backend.table_exists.return_value = False
    r = SandboxResolver(backend=mock_backend)
    assert r.table_exists("my_cat", "silver", "orders") is False
    mock_backend.table_exists.assert_called_once_with("my_cat", "silver", "orders")


def test_create_schema_delegates_to_backend_with_catalog(mock_backend):
    r = SandboxResolver(backend=mock_backend)
    r.create_schema("my_cat", "silver")
    mock_backend.ensure_schema_exists.assert_called_once_with("my_cat.silver")


def test_create_schema_delegates_to_backend_no_catalog(mock_backend):
    r = SandboxResolver(backend=mock_backend)
    r.create_schema(None, "silver")
    mock_backend.ensure_schema_exists.assert_called_once_with("silver")


def test_clone_table_delegates_to_backend(mock_backend):
    r = SandboxResolver(backend=mock_backend)
    r.clone_table("cat", "silver", "orders", "cat", "silver_sandbox", "orders")
    mock_backend.clone_table.assert_called_once_with(
        "cat", "silver", "orders", "cat", "silver_sandbox", "orders"
    )



# ==============================================================================
# parse_fqn
# ==============================================================================

def test_parse_fqn_two_part(resolver):
    """2-part FQN: silver.orders -> (None, silver, orders)"""
    catalog, schema, table = resolver.parse_fqn("silver.orders")
    assert catalog is None
    assert schema == "silver"
    assert table == "orders"


def test_parse_fqn_three_part(resolver):
    """3-part FQN: my_catalog.silver.orders -> (my_catalog, silver, orders)"""
    catalog, schema, table = resolver.parse_fqn("my_catalog.silver.orders")
    assert catalog == "my_catalog"
    assert schema == "silver"
    assert table == "orders"


def test_parse_fqn_with_backticks(resolver):
    """`catalog`.`schema`.`table` is parsed correctly."""
    catalog, schema, table = resolver.parse_fqn("`cat`.`sch`.`tbl`")
    assert catalog == "cat"
    assert schema == "sch"
    assert table == "tbl"


# ==============================================================================
# build_suffixed_fqn
# ==============================================================================

def test_build_suffixed_fqn_no_catalog(resolver):
    """Without catalog, schema is suffixed."""
    cat, schema, table = resolver.build_suffixed_fqn(None, "silver", "orders", "_jdoe")
    assert cat is None
    assert schema == "silver_jdoe"
    assert table == "orders"


def test_build_suffixed_fqn_with_catalog(resolver):
    """With catalog, schema is suffixed, catalog is preserved."""
    cat, schema, table = resolver.build_suffixed_fqn("my_cat", "silver", "orders", "_jdoe")
    assert cat == "my_cat"
    assert schema == "silver_jdoe"
    assert table == "orders"


# ==============================================================================
# resolve — table exists in sandbox
# ==============================================================================

def test_resolve_table_exists_in_sandbox(resolver):
    """When table exists in sandbox, returns sandbox FQN transparently."""
    resolver.schema_exists = MagicMock(return_value=True)
    resolver.table_exists = MagicMock(return_value=True)

    result = resolver.resolve("silver.orders", "_jdoe")
    assert result == "`silver_jdoe`.`orders`"
    resolver.table_exists.assert_called_with(None, "silver_jdoe", "orders")


# ==============================================================================
# resolve — table missing in sandbox (auto-clone)
# ==============================================================================

def test_resolve_table_missing_in_sandbox_clones(resolver):
    """When table missing in sandbox, clones from main schema."""
    resolver.schema_exists = MagicMock(return_value=True)
    # table_exists: False for sandbox, True for main
    resolver.table_exists = MagicMock(side_effect=[False, True])
    resolver.clone_table = MagicMock()

    result = resolver.resolve("silver.orders", "_jdoe", missing_table_behavior="copy")

    assert result == "`silver_jdoe`.`orders`"
    resolver.clone_table.assert_called_once_with(
        None, "silver", "orders", None, "silver_jdoe", "orders"
    )


def test_resolve_table_missing_in_sandbox_error_behavior(resolver):
    """When table missing in sandbox and behavior=error, raises ValueError."""
    resolver.schema_exists = MagicMock(return_value=True)
    resolver.table_exists = MagicMock(return_value=False)

    with pytest.raises(ValueError, match="not found in sandbox schema"):
        resolver.resolve("silver.orders", "_jdoe", missing_table_behavior="error")


# ==============================================================================
# resolve — schema missing (create + clone)
# ==============================================================================

def test_resolve_schema_missing_creates_and_clones(resolver):
    """When sandbox schema doesn't exist, creates it and clones the table."""
    resolver.schema_exists = MagicMock(return_value=False)
    resolver.table_exists = MagicMock(return_value=True)  # Source table exists
    resolver.create_schema = MagicMock()
    resolver.clone_table = MagicMock()

    result = resolver.resolve("silver.orders", "_jdoe")

    resolver.create_schema.assert_called_once_with(None, "silver_jdoe")
    resolver.clone_table.assert_called_once()
    assert "silver_jdoe" in result


def test_resolve_schema_missing_error_behavior(resolver):
    """When sandbox schema missing and behavior=error, raises ValueError after creating schema."""
    resolver.schema_exists = MagicMock(return_value=False)
    resolver.create_schema = MagicMock()

    with pytest.raises(ValueError, match="not found in sandbox schema"):
        resolver.resolve("silver.orders", "_jdoe", missing_table_behavior="error")

    resolver.create_schema.assert_called_once()


# ==============================================================================
# resolve — source table missing in main schema
# ==============================================================================

def test_resolve_source_table_missing_in_main_raises(resolver):
    """When sandbox schema exists but sandbox table and main table are both missing, raises ValueError."""
    resolver.schema_exists = MagicMock(return_value=True)
    # Both sandbox and main table missing
    resolver.table_exists = MagicMock(return_value=False)

    with pytest.raises(ValueError, match="not found in main schema"):
        resolver.resolve("silver.orders", "_jdoe", missing_table_behavior="copy")

