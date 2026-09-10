"""
Utility helpers for rule authoring — compatible with Databricks Connect v2.
"""

from __future__ import annotations
import logging
import sys
from pathlib import Path

#: Logger namespace shared by every skifer module (``getLogger(__name__)``
#: in submodules all nest under this name, e.g. "skifer.core.interpreter").
_PACKAGE_LOGGER_NAME = "skifer"


class _StepFormatter(logging.Formatter):
    """
    Prints INFO/DEBUG records as a bare message (matching the historical
    ``print("   -> [Load] ...")`` step output, which already carries its own
    indentation/markers) while still prefixing WARNING+ with the level name.
    """

    def format(self, record: logging.LogRecord) -> str:
        message = record.getMessage()
        if record.levelno >= logging.WARNING:
            return f"{record.levelname}: {message}"
        return message


def configure_logging(level: int | str = logging.INFO) -> None:
    """
    Make Skifer's internal pipeline logs visible.

    Since Plan 17/18, the engine reports its steps (table loads, joins,
    business rules, writes…) through the standard ``logging`` module
    (logger namespace ``"skifer"``) instead of ``print()``. Python's
    root logger defaults to ``WARNING`` with no handler attached, so without
    this call only warnings/errors surface (and only via the bare-bones
    ``logging.lastResort`` handler).

    This attaches a single ``StreamHandler`` directly to the
    ``"skifer"`` logger (not the root logger, so other libraries'
    logging configuration is left untouched) and disables propagation to
    avoid duplicate lines if the caller also configured root logging.

    Safe to call multiple times — it won't attach duplicate handlers.

    Args:
        level: Minimum level to display (default ``logging.INFO``, i.e. all
            pipeline steps). Pass ``logging.WARNING`` to silence step logs
            again without removing the handler.

    Usage::

        from skifer.utils import configure_logging
        configure_logging()  # or configure_logging(logging.DEBUG)
    """
    pkg_logger = logging.getLogger(_PACKAGE_LOGGER_NAME)
    pkg_logger.setLevel(level)

    if not any(isinstance(h, logging.StreamHandler) for h in pkg_logger.handlers):
        handler = logging.StreamHandler()
        handler.setFormatter(_StepFormatter())
        pkg_logger.addHandler(handler)

    pkg_logger.propagate = False


def setup_notebook(*extra_paths: str, log_level: int | str | None = logging.INFO) -> str:
    """
    Notebook bootstrap helper — replaces the boilerplate setup cell.

    Walks up from the current working directory to find the project root
    (the first parent that contains a config.yaml), adds any extra_paths
    relative to that root to sys.path, configures pipeline step logging
    (see :func:`configure_logging`), and returns the root as a string.

    Usage::

        from skifer.utils import setup_notebook
        REPO_ROOT = setup_notebook('rules')   # adds <root>/rules to sys.path
        import order_rules                    # now importable

    Args:
        *extra_paths: Subdirectory names (relative to project root) to add to sys.path.
        log_level: Forwarded to :func:`configure_logging`. Pass ``None`` to skip
            logging setup entirely (e.g. if the notebook configures it differently).

    Returns:
        str: Absolute path to the project root.
    """
    cwd = Path.cwd()
    root = cwd
    for parent in [cwd, *cwd.parents]:
        if (parent / "config.yaml").exists():
            root = parent
            break

    for sub in extra_paths:
        p = str(root / sub)
        if p not in sys.path:
            sys.path.insert(0, p)

    if log_level is not None:
        configure_logging(level=log_level)

    return str(root)


def safe_columns(df):
    """
    Returns the column names of a DataFrame without triggering a failing AnalyzePlan RPC.

    On native Databricks (cluster / notebook), this delegates to ``df.columns`` as normal.
    On Databricks Connect v2 from local environments (PyCharm / VS Code on Windows), the
    ``AnalyzePlan`` gRPC call that backs ``df.columns`` can fail with
    ``Missing required field 'UserContext'``.  In that case this function returns an empty
    list so that optional-column branches in business rules are silently skipped rather than
    crashing.  Core transformations (withColumn, filter, write …) are unaffected because they
    go through ``ExecutePlan``, not ``AnalyzePlan``.

    Args:
        df: A PySpark DataFrame.

    Returns:
        list[str]: Column names, or ``[]`` when schema inspection fails locally.
    """
    try:
        return df.columns
    except Exception as e:
        if "UserContext" in str(e) or "INVALID_ARGUMENT" in str(e):
            return []
        raise
