"""
Tests for SparkBackend._patch_debugging (formerly SkiferEngine._patch_connect_debugging).
The Connect v2 patches have moved to SparkBackend as of v1.0.
"""
import pytest
import pyspark.errors.utils as _pu
from unittest.mock import MagicMock, patch
from skifer.core.spark_backend import SparkBackend


def _make_backend(spark=None):
    b = object.__new__(SparkBackend)
    b._spark = spark or MagicMock()
    b._is_local = False
    b._workspace_client_cache = None
    return b


class TestPatchConnectDebugging:

    def setup_method(self):
        _pu._enable_debugging_cache = None

    def teardown_method(self):
        _pu._enable_debugging_cache = None

    def test_no_patch_when_conf_get_succeeds(self):
        spark = MagicMock()
        spark.conf.get.return_value = "false"
        backend = _make_backend(spark=spark)
        backend._patch_debugging()
        spark.conf.get.assert_called_once_with(
            "spark.python.sql.dataFrameDebugging.enabled", "false"
        )
        assert _pu._enable_debugging_cache is None

    def test_patches_cache_to_false_when_conf_get_fails(self):
        spark = MagicMock()
        spark.conf.get.side_effect = Exception("gRPC: Missing UserContext")
        backend = _make_backend(spark=spark)
        backend._patch_debugging()
        assert _pu._enable_debugging_cache is False

    def test_does_not_overwrite_existing_cache(self):
        _pu._enable_debugging_cache = True
        spark = MagicMock()
        spark.conf.get.side_effect = Exception("gRPC error")
        backend = _make_backend(spark=spark)
        backend._patch_debugging()
        assert _pu._enable_debugging_cache is True

    def test_silent_when_pyspark_utils_not_importable(self):
        spark = MagicMock()
        spark.conf.get.side_effect = Exception("gRPC error")
        backend = _make_backend(spark=spark)
        with patch.dict("sys.modules", {"pyspark.errors.utils": None}):
            backend._patch_debugging()  # Must not raise
