import inspect
import subprocess
import sys
import warnings
from pathlib import Path

from skifer.observability.best_effort import warn_best_effort


def test_warn_best_effort_records_runtime_warning_at_call_site():
    with warnings.catch_warnings(record=True) as recorded:
        warnings.simplefilter("always")
        result = warn_best_effort("best effort failed")

    assert result is None
    assert len(recorded) == 1
    assert recorded[0].category is RuntimeWarning
    assert str(recorded[0].message) == "best effort failed"
    assert recorded[0].filename == __file__


def test_warn_best_effort_honours_custom_category():
    with warnings.catch_warnings(record=True) as recorded:
        warnings.simplefilter("always")
        warn_best_effort("custom warning", UserWarning)

    assert len(recorded) == 1
    assert recorded[0].category is UserWarning
    assert str(recorded[0].message) == "custom warning"


def _warn_from_helper() -> None:
    warn_best_effort("from helper", stacklevel=2)


def test_warn_best_effort_stacklevel_matches_warnings_warn_call_site_semantics():
    with warnings.catch_warnings(record=True) as recorded:
        warnings.simplefilter("always")
        caller_line = inspect.currentframe().f_lineno + 1
        _warn_from_helper()

    assert len(recorded) == 1
    assert recorded[0].filename == __file__
    assert recorded[0].lineno == caller_line


def test_warn_best_effort_never_raises_under_warnings_as_errors():
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        result = warn_best_effort("must not raise")

    assert result is None


def test_best_effort_import_is_standard_library_only():
    project_root = Path(__file__).resolve().parents[1]
    script = r"""
import importlib
import pathlib
import sys
import types
import warnings

source_root = pathlib.Path.cwd() / "src"
skifer = types.ModuleType("skifer")
skifer.__path__ = [str(source_root / "skifer")]
observability = types.ModuleType("skifer.observability")
observability.__path__ = [str(source_root / "skifer" / "observability")]
sys.modules["skifer"] = skifer
sys.modules["skifer.observability"] = observability

before = set(sys.modules)
importlib.import_module("skifer.observability.best_effort")
added = set(sys.modules) - before

assert added == {"skifer.observability.best_effort"}, sorted(added)
assert not any(name == "pyspark" or name.startswith("pyspark.") for name in sys.modules)
"""

    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=project_root,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
