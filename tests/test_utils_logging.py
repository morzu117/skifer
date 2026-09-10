"""
Tests for skifer.utils.configure_logging and the log_level
parameter of setup_notebook — restores step-by-step pipeline visibility
through the standard logging module (Plan 17/18 replaced print() with
logger.info(), which is silent without a configured handler).
"""
import logging

import pytest

from skifer.utils import configure_logging, setup_notebook, _PACKAGE_LOGGER_NAME


@pytest.fixture(autouse=True)
def _reset_package_logger():
    """Ensure each test starts from a clean 'skifer' logger state."""
    pkg_logger = logging.getLogger(_PACKAGE_LOGGER_NAME)
    original_handlers = list(pkg_logger.handlers)
    original_level = pkg_logger.level
    original_propagate = pkg_logger.propagate
    pkg_logger.handlers.clear()
    yield
    pkg_logger.handlers.clear()
    pkg_logger.handlers.extend(original_handlers)
    pkg_logger.setLevel(original_level)
    pkg_logger.propagate = original_propagate


def test_configure_logging_attaches_a_stream_handler():
    pkg_logger = logging.getLogger(_PACKAGE_LOGGER_NAME)
    assert pkg_logger.handlers == []

    configure_logging()

    assert len(pkg_logger.handlers) == 1
    assert isinstance(pkg_logger.handlers[0], logging.StreamHandler)
    assert pkg_logger.level == logging.INFO
    assert pkg_logger.propagate is False


def test_configure_logging_is_idempotent_no_duplicate_handlers():
    configure_logging()
    configure_logging()
    configure_logging()

    pkg_logger = logging.getLogger(_PACKAGE_LOGGER_NAME)
    assert len(pkg_logger.handlers) == 1


def test_configure_logging_respects_custom_level():
    configure_logging(level=logging.DEBUG)

    pkg_logger = logging.getLogger(_PACKAGE_LOGGER_NAME)
    assert pkg_logger.level == logging.DEBUG


def test_configure_logging_makes_info_steps_visible(capsys):
    configure_logging()
    logger = logging.getLogger(f"{_PACKAGE_LOGGER_NAME}.core.interpreter")

    logger.info("    -> [Load] Loading table/source: %s", "silver.orders")

    captured = capsys.readouterr()
    assert "-> [Load] Loading table/source: silver.orders" in captured.err


def test_configure_logging_prefixes_warnings_with_level(capsys):
    configure_logging()
    logger = logging.getLogger(f"{_PACKAGE_LOGGER_NAME}.core.interpreter")

    logger.warning("[RuleAnalyzer] SHARED_READ: something")

    captured = capsys.readouterr()
    assert captured.err.startswith("WARNING: [RuleAnalyzer]")


def test_setup_notebook_configures_logging_by_default(tmp_path, monkeypatch):
    (tmp_path / "config.yaml").write_text("catalog: null\n")
    monkeypatch.chdir(tmp_path)

    setup_notebook()

    pkg_logger = logging.getLogger(_PACKAGE_LOGGER_NAME)
    assert len(pkg_logger.handlers) == 1
    assert pkg_logger.level == logging.INFO


def test_setup_notebook_log_level_none_skips_logging_setup(tmp_path, monkeypatch):
    (tmp_path / "config.yaml").write_text("catalog: null\n")
    monkeypatch.chdir(tmp_path)

    setup_notebook(log_level=None)

    pkg_logger = logging.getLogger(_PACKAGE_LOGGER_NAME)
    assert pkg_logger.handlers == []
