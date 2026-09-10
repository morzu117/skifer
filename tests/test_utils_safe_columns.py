"""
Tests for skifer.utils.safe_columns.
"""
import pytest
from unittest.mock import MagicMock, PropertyMock
from skifer import safe_columns


class TestSafeColumns:

    def test_returns_df_columns_normally(self):
        df = MagicMock()
        type(df).columns = PropertyMock(return_value=["id", "name", "value"])

        assert safe_columns(df) == ["id", "name", "value"]

    def test_returns_empty_list_on_usercontext_grpc_error(self):
        df = MagicMock()
        type(df).columns = PropertyMock(
            side_effect=Exception("gRPC: Missing required field 'UserContext' in the request.")
        )

        result = safe_columns(df)

        assert result == []

    def test_returns_empty_list_on_invalid_argument_grpc_error(self):
        df = MagicMock()
        type(df).columns = PropertyMock(
            side_effect=Exception("StatusCode.INVALID_ARGUMENT details: something")
        )

        result = safe_columns(df)

        assert result == []

    def test_reraises_unrelated_exceptions(self):
        df = MagicMock()
        type(df).columns = PropertyMock(side_effect=RuntimeError("disk full"))

        with pytest.raises(RuntimeError, match="disk full"):
            safe_columns(df)

    def test_importable_from_skifer_top_level(self):
        from skifer import safe_columns as sc
        assert callable(sc)
