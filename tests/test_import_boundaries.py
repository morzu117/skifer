"""Import boundaries checked in fresh interpreters, without pytest's module cache."""

import ast
import importlib
import importlib.util
import os
from pathlib import Path
import subprocess
import sys
import textwrap

import pytest


LANGUAGE_MODULES = {
    "skifer.core.capability_matrix",
    "skifer.core.connector",
    "skifer.core.dialect",
    "skifer.core.constants",
    "skifer.core.op_catalog",
    "skifer.core.ir",
    "skifer.core.schema_loader",
    "skifer.core.sql_compiler",
    "skifer.core.json_schema",
    "skifer.core.registry",
    "skifer.core.rule_analyzer",
}


def _assert_checkout_origin(module_file):
    source_root = Path(__file__).resolve().parents[1] / "src"
    resolved = Path(module_file).resolve()
    assert resolved.is_relative_to(source_root), (
        f"Subprocess imported {resolved}, expected a module under {source_root}"
    )


def _run_python(source):
    env = os.environ.copy()
    env.pop("PYTHONPATH", None)
    code = "import skifer\nprint(skifer.__file__, flush=True)\n" + textwrap.dedent(source)
    result = subprocess.run(
        [sys.executable, "-c", code],
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    assert result.returncode == 0, (
        f"Subprocess exited with {result.returncode}:\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    module_file, separator, output = result.stdout.partition("\n")
    assert separator, f"Missing skifer.__file__ in subprocess output: {result.stdout!r}"
    _assert_checkout_origin(module_file)
    return output


@pytest.mark.parametrize(
    ("module", "measured_count"),
    [
        ("skifer.core.capability_matrix", 3),
        ("skifer.core.connector", 4),
        ("skifer.core.dialect", 3),
        ("skifer.core.constants", 3),
        ("skifer.core.op_catalog", 3),
        ("skifer.core.ir", 4),
        ("skifer.core.schema_loader", 6),
        ("skifer.core.sql_compiler", 8),
        ("skifer.core.json_schema", 5),
        ("skifer.core.registry", 3),
        ("skifer.core.rule_analyzer", 4),
    ],
)
def test_language_imports_stay_within_language(module, measured_count):
    names = _run_python(
        f"""
        import sys, {module}
        print('\\n'.join(sorted(
            m for m in sys.modules
            if m == 'skifer' or m.startswith(('skifer.', 'pyspark', 'delta'))
        )))
        """
    ).splitlines()
    assert module in names
    assert not any(name.startswith(("pyspark", "delta")) for name in names), names
    assert all(
        name in {"skifer", "skifer.core"} or name.startswith("skifer.core.")
        for name in names
    ), names
    assert set(names) <= LANGUAGE_MODULES | {"skifer", "skifer.core"}, names
    skifer_count = sum(name == "skifer" or name.startswith("skifer.") for name in names)
    assert skifer_count == measured_count, (
        f"{module}: {skifer_count} Skifer modules loaded; "
        f"Plan 46.1 measured baseline: {measured_count}. {names}"
    )


def test_package_import_does_not_load_pyspark():
    _run_python(
        """
        import sys
        import skifer

        assert not any(name.startswith('pyspark') for name in sys.modules)
        """
    )


def test_public_exports_resolve_lazily_and_are_cached():
    _run_python(
        """
        import importlib
        import skifer
        import skifer.core
        import skifer.semantic
        import skifer.agentic

        exports = [
            (skifer, 'SkiferEngine', 'skifer.core.core'),
            (skifer, 'load_schema', 'skifer.core.schema_loader'),
            (skifer.core, 'RuleRegistry', 'skifer.core.registry'),
            (skifer.semantic, 'SemanticEngine', 'skifer.semantic.semantic'),
            (skifer.agentic, 'GenBIAgent', 'skifer.agentic.agent'),
        ]
        for package, name, module in exports:
            assert name not in vars(package), (package.__name__, name)
        for package in (skifer, skifer.core, skifer.semantic, skifer.agentic):
            assert dir(package) == sorted(package.__all__)
        for package, name, module in exports:
            obj = getattr(package, name)
            assert obj is getattr(importlib.import_module(module), name)
            assert vars(package)[name] is obj
            assert getattr(package, name) is obj
        """
    )


# 46.7: core.core ships in the skifer wheel, loaders in skifer-engine, so core.core
# no longer imports loaders at module load (it would break `import skifer` without the
# engine). SkiferEngine.__init__ imports them instead, and
# test_packaging.test_importing_core_engine_does_not_eagerly_load_the_runtime guards
# that core.core stays runtime-free. The interpreter is an engine module shipped
# alongside loaders, so importing it still registers the built-ins.
@pytest.mark.parametrize("module", ["skifer.core.interpreter"])
def test_engine_import_registers_builtin_loaders(module):
    _run_python(
        f"""
        import sys
        from skifer.core.registry import RuleRegistry

        assert 'skifer.core.loaders' not in sys.modules
        import {module}

        assert 'skifer.core.loaders' in sys.modules
        assert RuleRegistry.list_loaders()
        """
    )


@pytest.mark.parametrize(
    ("module", "expected_modules"),
    [
        ("skifer", {"skifer"}),
        ("skifer.core", {"skifer", "skifer.core"}),
        ("skifer.semantic", {"skifer", "skifer.semantic"}),
        ("skifer.agentic", {"skifer", "skifer.agentic"}),
    ],
)
def test_package_roots_load_only_themselves(module, expected_modules):
    names = _run_python(
        f"""
        import sys, {module}
        print('\\n'.join(sorted(m for m in sys.modules if m == 'skifer' or m.startswith('skifer.'))))
        """
    ).splitlines()
    assert set(names) == expected_modules


@pytest.mark.parametrize("module", ["skifer", "skifer.core", "skifer.semantic", "skifer.agentic"])
def test_all_public_exports_resolve(module):
    package = importlib.import_module(module)
    assert set(package._EXPORTS) == set(package.__all__) - {"__version__"}
    for name in package.__all__:
        if name == "__version__":
            continue
        obj = getattr(package, name)
        target = _resolve_export_target(package, name)
        if module == "skifer.semantic" and name == "ExecutionResult":
            from skifer.core.connector import ExecutionResult

            assert target == "skifer.semantic.evidence"
            assert obj is ExecutionResult
            continue
        assert obj.__module__ == target or obj.__module__.startswith(target + "."), (
            module, name, obj, target
        )


def _resolve_export_target(package, name):
    target = package._EXPORTS[name]
    if not target.startswith("."):
        target = "." + target
    return importlib.util.resolve_name(target, package.__name__)


@pytest.mark.parametrize("module", ["skifer", "skifer.core", "skifer.semantic", "skifer.agentic"])
def test_type_checking_imports_match_lazy_exports(module):
    package = importlib.import_module(module)
    tree = ast.parse(Path(package.__file__).read_text())
    blocks = [
        node for node in tree.body
        if isinstance(node, ast.If)
        and isinstance(node.test, ast.Name)
        and node.test.id == "TYPE_CHECKING"
    ]
    assert len(blocks) == 1
    imports = {}
    for node in ast.walk(blocks[0]):
        if isinstance(node, ast.ImportFrom):
            source = importlib.util.resolve_name(
                "." * node.level + (node.module or ""), package.__name__
            )
            for alias in node.names:
                imports[alias.asname or alias.name] = source
    assert imports == {
        name: _resolve_export_target(package, name)
        for name in package._EXPORTS if name != "__version__"
    }


def test_registry_lists_builtin_loaders_without_engine():
    output = _run_python(
        """
        import sys
        from skifer import RuleRegistry

        print(bool(RuleRegistry.list_loaders()))
        assert 'skifer.core.core' not in sys.modules
        assert 'skifer.core.interpreter' not in sys.modules
        RuleRegistry.clear(loaders=True)
        assert RuleRegistry.list_loaders() == []
        """
    )
    assert output.strip() == "True"


def test_registry_gets_builtin_loader_without_engine():
    _run_python(
        """
        import sys
        from skifer import RuleRegistry

        loader = RuleRegistry.get_loader('load_and_union_tables')
        assert loader.__module__ == 'skifer.core.loaders'
        assert 'skifer.core.core' not in sys.modules
        assert 'skifer.core.interpreter' not in sys.modules
        RuleRegistry.clear(loaders=True)
        try:
            RuleRegistry.get_loader('load_and_union_tables')
        except ValueError:
            pass
        else:
            raise AssertionError('get_loader re-registered a cleared loader')
        assert RuleRegistry.list_loaders() == []
        """
    )


# --- Plan 47.7: the registry without skifer-engine ---------------------------------------

def test_registry_lists_registered_loaders_without_the_engine_distribution(monkeypatch):
    from skifer.core.registry import RuleRegistry

    def mine():
        return None

    monkeypatch.setattr(RuleRegistry, "_loaders", {"mine": mine})
    monkeypatch.setitem(sys.modules, "skifer.core.loaders", None)
    assert RuleRegistry.list_loaders() == ["mine"]
    assert RuleRegistry.get_loader("mine") is mine
    with pytest.raises(ValueError, match="`skifer-engine` distribution"):
        RuleRegistry.get_loader("load_and_union_tables")


def test_registry_lets_a_missing_third_party_dependency_propagate(monkeypatch):
    import types

    from skifer.core import registry

    error = ModuleNotFoundError("No module named 'pyspark'", name="pyspark")

    def import_module(_name):
        raise error

    monkeypatch.setattr(registry, "importlib", types.SimpleNamespace(import_module=import_module))
    with pytest.raises(ModuleNotFoundError) as excinfo:
        registry.RuleRegistry.list_loaders()
    assert excinfo.value is error


# --- Plan 47.9: platform imports stay in the engine and serving modules -----------------

PLATFORM_PACKAGES = ("pyspark", "delta", "mlflow", "databricks")


def _skifer_wheel_modules_and_optional_names():
    spec = importlib.util.spec_from_file_location(
        "_skifer_check_wheels_47_9",
        Path(__file__).resolve().parents[1] / "scripts" / "check_wheels.py",
    )
    check_wheels = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(check_wheels)
    owners, _ = check_wheels.ownership()
    modules = sorted(
        check_wheels._module_name(path) for path, wheels in owners.items() if wheels == ["skifer"]
    )
    return modules, sorted(check_wheels.optional_import_names("skifer-core", "skifer"))


@pytest.mark.skipif(sys.version_info < (3, 11), reason="check_wheels.py needs tomllib")
def test_importing_every_skifer_wheel_module_loads_no_platform_package():
    import json

    modules, optional = _skifer_wheel_modules_and_optional_names()
    assert "skifer.semantic.semantic" in modules and "skifer.serving" not in modules
    output = _run_python(
        f"""
        import importlib, json, sys
        modules, optional = json.loads({json.dumps(json.dumps([modules, optional]))})
        platform = {PLATFORM_PACKAGES!r}
        skipped, attempted = [], []
        for module in modules:
            try:
                importlib.import_module(module)
            except ModuleNotFoundError as exc:
                missing = (exc.name or "").split(".")[0]
                if missing in platform:  # tried at load time, merely not installed here
                    attempted.append(f"{{module}} -> {{missing}}")
                elif missing not in optional:
                    raise
                else:
                    skipped.append(module)
        imported = [m for m in modules if m in sys.modules]
        loaded = sorted(m for m in sys.modules if m.split(".")[0] in platform)
        print(json.dumps({{"loaded": loaded + attempted, "skipped": skipped,
                          "imported": len(imported)}}))
        """
    )
    result = json.loads(output.strip().splitlines()[-1])
    assert result["loaded"] == [], (
        f"importing the skifer wheel loaded platform packages: {result['loaded']}"
    )
    assert len(result["skipped"]) < len(modules) // 10, result["skipped"]
    assert result["imported"] == len(modules) - len(result["skipped"])
