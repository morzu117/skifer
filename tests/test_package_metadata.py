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
