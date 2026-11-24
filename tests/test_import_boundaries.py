"""Import boundaries checked in fresh interpreters, without pytest's module cache."""

import os
import subprocess
import sys
import textwrap

import pytest


LANGUAGE_MODULES = {
    "skifer.core.constants",
    "skifer.core.op_catalog",
    "skifer.core.ir",
    "skifer.core.schema_loader",
    "skifer.core.sql_compiler",
    "skifer.core.json_schema",
    "skifer.core.registry",
    "skifer.core.rule_analyzer",
}


def _run_python(source):
    env = os.environ.copy()
    env.pop("PYTHONPATH", None)
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(source)],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    ).stdout


@pytest.mark.parametrize(
    ("module", "measured_count"),
    [
        ("skifer.core.op_catalog", 3),
        ("skifer.core.sql_compiler", 6),
        ("skifer.core.schema_loader", 6),
    ],
)
def test_language_imports_stay_within_language(module, measured_count):
    names = _run_python(
        f"""
        import sys, {module}
        print('\\n'.join(sorted(
            m for m in sys.modules
            if m.startswith(('skifer', 'pyspark', 'delta'))
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
    skifer_count = sum(name.startswith("skifer") for name in names)
    assert skifer_count <= 12, (
        f"{module}: {skifer_count} Skifer modules loaded; "
        f"Plan 46.1 measured baseline: {measured_count}; ceiling: 12. {names}"
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


def test_engine_import_registers_builtin_loaders():
    _run_python(
        """
        import sys
        from skifer.core.registry import RuleRegistry

        assert not RuleRegistry.list_loaders()
        import skifer.core.core

        assert 'skifer.core.loaders' in sys.modules
        assert RuleRegistry.list_loaders()
        """
    )
