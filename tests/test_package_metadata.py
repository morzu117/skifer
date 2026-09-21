"""Package-level metadata exposed by ``import skifer``.

The version has a single source of truth, ``pyproject.toml``.  ``__version__`` reads it back from
the installed distribution rather than restating it, so the two can never drift.
"""

from importlib.metadata import PackageNotFoundError, version as distribution_version

import pytest

import skifer


def test_version_is_a_non_empty_string():
    assert isinstance(skifer.__version__, str)
    assert skifer.__version__


def test_version_is_part_of_the_public_api():
    assert "__version__" in skifer.__all__


def test_version_matches_the_installed_distribution():
    """When Skifer is installed, ``__version__`` is exactly what pip reports."""
    try:
        expected = distribution_version("skifer")
    except PackageNotFoundError:
        pytest.skip("skifer is not installed in this environment (source checkout)")

    assert skifer.__version__ == expected


def test_uninstalled_source_checkout_reports_unknown_rather_than_raising():
    """A source checkout must still import.

    Reading distribution metadata is the right source of truth, but it only exists once the package
    is installed.  Letting ``PackageNotFoundError`` escape would make ``import skifer`` fail in a
    plain ``PYTHONPATH=src`` checkout — the exact setup the test suite itself runs in.
    """
    if skifer.__version__ == "unknown":
        try:
            distribution_version("skifer")
        except PackageNotFoundError:
            return
        pytest.fail("__version__ is 'unknown' although the distribution is installed")


# ---------------------------------------------------------------------------
# The CI must install the extras the suite needs to actually run
#
# Three modules call `pytest.importorskip("duckdb")` so that the documented
# `pip install -e ".[dev]"` yields a smaller suite rather than a collection
# error. That kindness has a cost: an install line missing `sql` makes CI green
# while the Spark ↔ DuckDB equivalence suite — the guarantee the whole SQL-first
# work rests on — never runs. This test is what makes the two choices safe
# together. Measured: the first CI run on this branch failed with
# `ModuleNotFoundError: No module named 'duckdb'` on all four Python versions.
# ---------------------------------------------------------------------------

#: extra → why the suite is weaker without it.
_EXTRAS_THE_SUITE_NEEDS = {
    "dev": "pytest itself",
    "spark": "every test on a real local Delta session",
    "sql": "the Spark ↔ DuckDB equivalence suite, which skips silently without it",
}

#: Optional import the suite calls `importorskip` on → the extra that provides
#: it. Every entry is either in `_EXTRAS_THE_SUITE_NEEDS`, and therefore run in
#: CI, or listed below with the reason it is not. A new `importorskip` on an
#: unlisted dependency fails the guard, which is the point: skipping is a
#: decision, and an undecided skip is indistinguishable from a passing test.
_OPTIONAL_IMPORT_TO_EXTRA = {
    "duckdb": "sql",
    "sqlglot": "sql",
    "fastapi": "api",
    "anyio": "api",
    "mcp": "mcp",
}

#: Extras deliberately absent from CI, with what that costs. Measured on
#: 21 September 2026: 7 tests behind `fastapi`, 1 behind `mcp`.
_EXTRAS_NOT_EXERCISED_IN_CI = {
    "api": "7 tests of the loopback FastAPI layer never run in CI",
    "mcp": "1 test of the MCP server never runs in CI",
}


def test_the_test_workflow_installs_every_extra_the_suite_needs():
    import re
    from pathlib import Path

    workflow = Path(__file__).resolve().parent.parent / ".github" / "workflows" / "tests.yml"
    if not workflow.exists():
        pytest.skip("workflow file is not part of this checkout")

    install_lines = [
        line
        for line in workflow.read_text(encoding="utf-8").splitlines()
        if "pip install" in line and "-e" in line
    ]
    assert install_lines, "no editable install step found in the test workflow"

    declared = set()
    for line in install_lines:
        for match in re.findall(r'\[([^\]]+)\]', line):
            declared.update(part.strip() for part in match.split(","))

    missing = sorted(set(_EXTRAS_THE_SUITE_NEEDS) - declared)

    assert missing == [], (
        f"The test workflow does not install {missing}. "
        + " ".join(f"'{extra}' carries {_EXTRAS_THE_SUITE_NEEDS[extra]}." for extra in missing)
        + " Modules that importorskip a missing extra are skipped, so CI stays "
        "green while the tests it was added for never run."
    )


def test_every_optional_import_the_suite_skips_on_is_accounted_for():
    """An `importorskip` on an unclassified dependency is an invisible gap."""
    import re
    from pathlib import Path

    tests_dir = Path(__file__).resolve().parent
    targets = set()
    for path in tests_dir.rglob("test_*.py"):
        targets.update(
            re.findall(r'importorskip\(\s*["\']([A-Za-z_][\w.]*)["\']', path.read_text(encoding="utf-8"))
        )
    assert targets, "no importorskip found; this guard would pass vacuously"

    unclassified = sorted(targets - set(_OPTIONAL_IMPORT_TO_EXTRA))

    assert unclassified == [], (
        f"Optional import(s) {unclassified} are skipped by the suite but map to no "
        "extra. Add them to _OPTIONAL_IMPORT_TO_EXTRA, then decide whether CI "
        "installs that extra or accepts that those tests never run there."
    )


def test_each_extra_behind_a_skip_is_either_run_in_ci_or_declared_unrun():
    covered = set(_EXTRAS_THE_SUITE_NEEDS) | set(_EXTRAS_NOT_EXERCISED_IN_CI)
    undecided = sorted(set(_OPTIONAL_IMPORT_TO_EXTRA.values()) - covered)

    assert undecided == [], (
        f"Extra(s) {undecided} gate tests in this suite without any statement of "
        "whether CI runs them. Either add the extra to the workflow, or record "
        "what stays untested in _EXTRAS_NOT_EXERCISED_IN_CI."
    )
