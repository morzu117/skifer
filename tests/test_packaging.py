"""Guards for the 46.7 two-wheel split (packaging/skifer-core + packaging/skifer).

The skifer-core wheel must ship exactly the Spark-free language-core modules named
by the import-linter "Language core stays free of ..." contract — no more, no less.
A core module added to the contract but missing from the wheel force-include (or the
reverse) would publish a broken or incomplete skifer-core, and no other test notices.
"""
from __future__ import annotations

import pathlib

import pytest

try:
    import tomllib
except ModuleNotFoundError:  # Python 3.10 has no tomllib
    tomllib = None

ROOT = pathlib.Path(__file__).parents[1]


def _load(rel_path: str) -> dict:
    with open(ROOT / rel_path, "rb") as handle:
        return tomllib.load(handle)


def _contract_core_modules() -> set[str]:
    """The language-core module basenames from the import-linter contract."""
    contracts = _load("pyproject.toml")["tool"]["importlinter"]["contracts"]
    core = next(
        c for c in contracts
        if c.get("source_modules")
        and all(m.startswith("skifer.core.") for m in c["source_modules"])
        and "pyspark" in c.get("forbidden_modules", [])
    )
    return {module.rsplit(".", 1)[-1] for module in core["source_modules"]}


def _wheel_core_modules() -> set[str]:
    """The skifer.core.* module basenames shipped by the skifer-core wheel."""
    force_include = _load("packaging/skifer-core/pyproject.toml")[
        "tool"]["hatch"]["build"]["targets"]["wheel"]["force-include"]
    return {
        pathlib.Path(dest).stem
        for dest in force_include.values()
        if dest.startswith("skifer/core/") and not dest.endswith("__init__.py")
    }


@pytest.mark.skipif(tomllib is None, reason="tomllib requires Python 3.11+")
def test_skifer_core_wheel_ships_exactly_the_contract_core_modules():
    contract = _contract_core_modules()
    wheel = _wheel_core_modules()
    assert wheel == contract, (
        "skifer-core wheel diverges from the import-linter language-core contract: "
        f"only in wheel={sorted(wheel - contract)}, only in contract={sorted(contract - wheel)}"
    )


@pytest.mark.skipif(tomllib is None, reason="tomllib requires Python 3.11+")
def test_core_and_governed_wheels_share_one_version():
    # The governed wheel pins skifer-core==<its version>; a drifting pair would be
    # unco-installable. Keep the two [project] versions and the dependency aligned.
    core = _load("packaging/skifer-core/pyproject.toml")["project"]["version"]
    governed = _load("packaging/skifer/pyproject.toml")["project"]
    assert governed["version"] == core
    assert f"skifer-core=={core}" in governed["dependencies"]
