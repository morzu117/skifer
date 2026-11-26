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


RUNTIME_MODULES = {
    "skifer/spark_factory.py",
    "skifer/core/spark_backend.py",
    "skifer/core/writer.py",
    "skifer/core/loaders.py",
    "skifer/core/interpreter.py",
    "skifer/core/rule_executor.py",
}


def _force_include(pyproject_rel: str) -> dict:
    wheel = _load(pyproject_rel)["tool"]["hatch"]["build"]["targets"]["wheel"]
    return wheel.get("force-include", {})


def test_skifer_engine_missing_gives_a_clear_install_hint(monkeypatch):
    # Without skifer-engine the runtime modules are absent; instantiating
    # SkiferEngine must point the user at the right distribution rather than
    # surfacing a raw ImportError. Simulate the absence by blocking the import.
    import skifer

    monkeypatch.setitem(sys.modules, "skifer.spark_factory", None)
    with pytest.raises(RuntimeError, match="skifer-engine"):
        skifer.SkiferEngine()


@pytest.mark.skipif(tomllib is None, reason="tomllib requires Python 3.11+")
def test_skifer_engine_wheel_ships_exactly_the_runtime_modules():
    dests = set(_force_include("packaging/skifer-engine/pyproject.toml").values())
    assert dests == RUNTIME_MODULES


@pytest.mark.skipif(tomllib is None, reason="tomllib requires Python 3.11+")
def test_packaging_wheels_are_disjoint():
    # No source file may be shipped by two distributions — co-installing them would
    # be a path collision, and which one wins is install-order-dependent.
    seen: dict[str, str] = {}
    duplicates = []
    for pyproject in sorted((ROOT / "packaging").glob("*/pyproject.toml")):
        name = pyproject.parent.name
        for dest in _force_include(str(pyproject.relative_to(ROOT))).values():
            if dest in seen:
                duplicates.append((dest, seen[dest], name))
            seen[dest] = name
    assert not duplicates, f"files shipped by two wheels: {duplicates}"


@pytest.mark.skipif(tomllib is None, reason="tomllib requires Python 3.11+")
def test_all_packaging_distributions_share_one_version():
    # The distributions pin each other by exact version; a drifting pair would be
    # unco-installable. Keep every [project] version aligned.
    versions = {}
    for pyproject in sorted((ROOT / "packaging").glob("*/pyproject.toml")):
        project = _load(str(pyproject.relative_to(ROOT)))["project"]
        versions[project["name"]] = project["version"]
    assert len(set(versions.values())) == 1, versions
    shared = next(iter(versions.values()))
    # The dev root pyproject is the single source of truth release.py bumps from;
    # the four distributions must track it or a release would ship mismatched wheels.
    assert shared == _load("pyproject.toml")["project"]["version"], (
        f"packaging versions {shared} drift from root pyproject"
    )
    governed = _load("packaging/skifer/pyproject.toml")["project"]
    assert f"skifer-core=={shared}" in governed["dependencies"]
    engine = _load("packaging/skifer-engine/pyproject.toml")["project"]
    assert f"skifer=={shared}" in engine["dependencies"]
    databricks = _load("packaging/skifer-databricks/pyproject.toml")["project"]
    assert f"skifer=={shared}" in databricks["dependencies"]


# ---------------------------------------------------------------------------
# Plan 47.0 — every source file ships in exactly one wheel
# ---------------------------------------------------------------------------
# The prediction lives once, in scripts/check_wheels.py, which also compares it
# to the RECORD of the wheels it really builds (that part needs the network and
# runs in the CI `wheels` job). Here only the prediction is checked: no build.

def _check_wheels_module(root: pathlib.Path = ROOT):
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "_skifer_check_wheels", root / "scripts" / "check_wheels.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.skipif(tomllib is None, reason="tomllib requires Python 3.11+")
def test_every_source_file_ships_in_exactly_one_wheel():
    check_wheels = _check_wheels_module()
    owners, orphans = check_wheels.ownership()
    duplicates = {path: wheels for path, wheels in owners.items() if len(wheels) > 1}
    assert not orphans, (
        "src/skifer files shipped by no wheel — add a force-include line to the "
        f"packaging/<wheel>/pyproject.toml that should own them: {orphans}"
    )
    assert not duplicates, f"src/skifer files shipped by several wheels: {duplicates}"


@pytest.mark.skipif(tomllib is None, reason="tomllib requires Python 3.11+")
def test_a_source_file_without_a_force_include_line_is_reported_as_an_orphan(
    tmp_path, monkeypatch
):
    # Same manifests, same src/ tree, plus one stray module under skifer/core/ —
    # which no wheel ships, because skifer/core/ is listed file by file.
    import shutil

    shutil.copytree(ROOT / "packaging", tmp_path / "packaging")
    shutil.copytree(
        ROOT / "src" / "skifer", tmp_path / "src" / "skifer",
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )
    (tmp_path / "src" / "skifer" / "core" / "stray_module.py").write_text("X = 1\n")
    check_wheels = _check_wheels_module()
    monkeypatch.setattr(check_wheels, "ROOT", tmp_path)
    monkeypatch.setattr(check_wheels, "SRC", tmp_path / "src")

    owners, orphans = check_wheels.ownership()

    assert orphans == ["skifer/core/stray_module.py"]
    assert all(len(wheels) == 1 for path, wheels in owners.items() if path not in orphans)


@pytest.mark.skipif(tomllib is None, reason="tomllib requires Python 3.11+")
def test_exclude_globs_drop_bytecode_from_directory_force_includes():
    check_wheels = _check_wheels_module()
    patterns = ["**/__pycache__", "**/*.pyc"]
    assert check_wheels._excluded("skifer/semantic/__pycache__/x.cpython-312.pyc", patterns)
    assert check_wheels._excluded("skifer/semantic/x.pyc", patterns)
    assert not check_wheels._excluded("skifer/semantic/semantic.py", patterns)
    shipped = check_wheels.predicted_files("skifer")
    assert "skifer/semantic/semantic.py" in shipped
    assert not any("__pycache__" in path or path.endswith(".pyc") for path in shipped)


@pytest.mark.skipif(tomllib is None, reason="tomllib requires Python 3.11+")
def test_optional_import_names_cover_declared_third_party_extras_only():
    # The governed-without-engine probe tolerates a ModuleNotFoundError only for these
    # names; a sibling Skifer wheel (skifer-engine, skifer-databricks) is never one.
    names = _check_wheels_module().optional_import_names("skifer-core", "skifer")
    assert {"sqlglot", "mcp", "fastapi", "jsonschema"} <= names
    assert not {"skifer_engine", "skifer_databricks", "pyspark", "delta"} & names
