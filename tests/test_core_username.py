"""
Tests for SkiferEngine._get_clean_username.
Covers: Spark SQL strategy, Databricks SDK REST API, DBUtils fallback, unknown_user.

Updated for plan17-2.1: _get_clean_username now delegates to environment.get_clean_username,
so Strategy 1 is Spark SQL (not backend.get_current_user).
"""
import sys
import pytest
from unittest.mock import MagicMock, patch
from skifer.core.core import SkiferEngine


def _make_engine(backend_user=None, spark=None, dbutils=None):
    """
    Create a minimal engine instance for username testing.

    - If ``spark`` is provided, it is used directly as ``_backend.spark``.
    - If ``backend_user`` is provided, the mock spark is configured so that
      ``spark.sql("SELECT current_user()").collect()[0][0]`` returns that value.
      Pass ``None`` to make Strategy 1 (Spark SQL) return no result (empty rows),
      causing fall-through to the next strategy.
    """
    engine = object.__new__(SkiferEngine)
    engine.dbutils = dbutils or MagicMock()
    mock_backend = MagicMock()
    if spark is not None:
        mock_backend.spark = spark
    elif backend_user is not None:
        # Strategy 1: configure mock so collect()[0][0] returns the email string.
        row = MagicMock()
        row.__getitem__ = MagicMock(return_value=backend_user)
        mock_backend.spark.sql.return_value.collect.return_value = [row]
    else:
        # No user from Spark SQL → empty collect() → Strategy 1 skipped.
        mock_backend.spark.sql.return_value.collect.return_value = []
    engine._backend = mock_backend
    return engine


class TestGetCleanUsername:

    def test_returns_username_from_backend(self):
        """Strategy 1 (Spark SQL) returns email → cleaned suffix returned."""
        engine = _make_engine(backend_user="john.doe@company.com")

        assert engine._get_clean_username() == "john_doe"

    def test_cleans_dots_and_dashes(self):
        """Dots and dashes in the email prefix are replaced with underscores."""
        engine = _make_engine(backend_user="jean-pierre.dupont@company.com")

        assert engine._get_clean_username() == "jean_pierre_dupont"

    def test_falls_back_to_sdk_when_backend_returns_none(self):
        """Strategy 1 yields nothing → Strategy 2 (Databricks SDK) is tried."""
        engine = _make_engine(backend_user=None)

        mock_me = MagicMock()
        mock_me.user_name = "jdoe@company.com"
        mock_wc = MagicMock()
        mock_wc.current_user.me.return_value = mock_me

        fake_sdk = MagicMock()
        fake_sdk.WorkspaceClient.return_value = mock_wc
        sys.modules["databricks.sdk"] = fake_sdk

        with patch.dict("os.environ", {"DATABRICKS_HOST": "https://adb.net", "DATABRICKS_TOKEN": "dapi123"}):
            result = engine._get_clean_username()

        assert result == "jdoe"

    def test_falls_back_to_dbutils_when_sql_and_sdk_fail(self):
        """Spark SQL and SDK both fail → Strategy 3 (DBUtils context tags) is used."""
        spark = MagicMock()
        spark.sql.side_effect = Exception("SQL error")
        engine = _make_engine(spark=spark)

        fake_sdk = MagicMock()
        fake_sdk.WorkspaceClient.side_effect = Exception("SDK error")
        sys.modules["databricks.sdk"] = fake_sdk

        context = MagicMock()
        context.tags.return_value.apply.return_value = "fallback.user@company.com"
        engine.dbutils.notebook.entry_point.getDbutils.return_value.notebook.return_value.getContext.return_value = context

        with patch.dict("os.environ", {"DATABRICKS_HOST": "https://adb.net", "DATABRICKS_TOKEN": "dapi123"}):
            result = engine._get_clean_username()

        assert result == "fallback_user"

    def test_returns_unknown_user_when_all_strategies_fail(self):
        """All strategies fail → 'unknown_user' is returned."""
        spark = MagicMock()
        spark.sql.side_effect = Exception("SQL error")
        engine = _make_engine(spark=spark)

        fake_sdk = MagicMock()
        fake_sdk.WorkspaceClient.side_effect = Exception("SDK error")
        sys.modules["databricks.sdk"] = fake_sdk

        engine.dbutils.notebook.entry_point.getDbutils.side_effect = Exception("no context")

        with patch.dict("os.environ", {}, clear=True), \
             patch.object(engine, "_find_file_upwards", return_value=None), \
             patch("skifer.core.environment.getpass.getuser", side_effect=Exception("no OS user")):
            result = engine._get_clean_username()

        assert result == "unknown_user"

    def test_sdk_not_called_when_no_credentials(self):
        """WorkspaceClient must not be instantiated when no DATABRICKS_* env vars are set."""
        spark = MagicMock()
        spark.sql.side_effect = Exception("SQL error")
        engine = _make_engine(spark=spark)

        fake_sdk = MagicMock()
        sys.modules["databricks.sdk"] = fake_sdk

        engine.dbutils.notebook.entry_point.getDbutils.side_effect = Exception("no context")

        with patch.dict("os.environ", {}, clear=True):
            engine._get_clean_username()

        # WorkspaceClient must not be called without credentials
        fake_sdk.WorkspaceClient.assert_not_called()
