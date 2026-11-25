"""Guards for the 46.7 two-wheel split (packaging/skifer-core + packaging/skifer).

The skifer-core wheel must ship exactly the Spark-free language-core modules named
by the import-linter "Language core stays free of ..." contract — no more, no less.
A core module added to the contract but missing from the wheel force-include (or the
reverse) would publish a broken or incomplete skifer-core, and no other test notices.
"""
from __future__ import annotations

import pathlib
import subprocess
import sys

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


def test_importing_core_engine_does_not_eagerly_load_the_runtime():
    # The skifer-engine modules must not be imported at `import skifer.core.core`
    # load time, so the skifer wheel imports without skifer-engine installed (the
    # files are absent there). Run in a subprocess: this process already imported
    # them. SkiferEngine() still pulls them lazily — that is the engine boundary.
    code = (
        "import skifer.core.core, sys; "
        "runtime = {'skifer.core.interpreter', 'skifer.core.loaders'}; "
        "leaked = sorted(runtime & set(sys.modules)); "
        "print(leaked); "
        "sys.exit(1 if leaked else 0)"
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert result.returncode == 0, (
        f"import skifer.core.core eagerly loaded runtime modules: {result.stdout.strip()}\n"
        f"{result.stderr.strip()}"
    )


@pytest.mark.skipif(tomllib is None, reason="tomllib requires Python 3.11+")
def test_core_and_governed_wheels_share_one_version():
    # The governed wheel pins skifer-core==<its version>; a drifting pair would be
    # unco-installable. Keep the two [project] versions and the dependency aligned.
    core = _load("packaging/skifer-core/pyproject.toml")["project"]["version"]
    governed = _load("packaging/skifer/pyproject.toml")["project"]
    assert governed["version"] == core
    assert f"skifer-core=={core}" in governed["dependencies"]
