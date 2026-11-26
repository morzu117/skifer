#!/usr/bin/env python3
"""Build the four Skifer wheels, check their file lists, and import each one alone (Plan 47.0).

The split into ``skifer-core`` / ``skifer`` / ``skifer-engine`` / ``skifer-databricks`` is
declared in ``packaging/<wheel>/pyproject.toml`` through hatch ``force-include`` tables.
Reading those tables is not proof: this script builds the wheels for real, reads what
landed in each ``RECORD``, and installs them into throw-away virtual environments.

Checks, in order:

1. every ``src/skifer/**`` file is predicted to ship in exactly one wheel;
2. the four ``RECORD`` file lists are disjoint and equal to that prediction;
3. **core alone** (``skifer-core[dialects]``): ``import skifer``, ``load_schema`` /
   ``parse_schema`` and ``compile_select`` on the three dialects;
4. **governed without the engine** (``skifer`` + ``skifer-core``, no extra): every module of
   the two wheels imports, or fails only with a ``ModuleNotFoundError`` whose ``.name`` is an
   optional third-party dependency the two wheels declare; afterwards ``sys.modules`` holds
   none of ``pyspark``, ``delta``, ``mlflow``, ``databricks``.

Exit code 0 when everything holds, 1 otherwise, with a report naming module, wheel and cause.
Needs the network (hatchling and the declared dependencies are fetched by pip).

Usage::

    python scripts/check_wheels.py [--keep DIR]
"""
from __future__ import annotations

import argparse
import csv
import fnmatch
import io
import json
import pathlib
import re
import subprocess
import sys
import tempfile
import venv
import zipfile

try:
    import tomllib
except ModuleNotFoundError:  # Python 3.10 has no tomllib
    tomllib = None

ROOT = pathlib.Path(__file__).resolve().parents[1]
SRC = ROOT / "src"

#: Build order matters only for readability of the report.
WHEELS = ("skifer-core", "skifer", "skifer-engine", "skifer-databricks")

#: Platform packages that must never be loaded by the governed layer on its own.
FORBIDDEN_MODULES = ("pyspark", "delta", "mlflow", "databricks")

#: Distribution name -> top-level import name, where the two differ. Anything not
#: listed imports under its normalised distribution name (``-`` -> ``_``).
IMPORT_NAMES = {
    "pyyaml": "yaml",
    "python-dotenv": "dotenv",
    "google-generativeai": "google",
    "fpdf2": "fpdf",
    "databricks-sdk": "databricks",
    "databricks-connect": "databricks",
    "opentelemetry-sdk": "opentelemetry",
    "opentelemetry-exporter-otlp-proto-http": "opentelemetry",
    "sentence-transformers": "sentence_transformers",
    "mkdocstrings": "mkdocstrings",
    "pytest-cov": "pytest_cov",
    "pytest-mock": "pytest_mock",
    "import-linter": "importlinter",
}


# ---------------------------------------------------------------------------
# Prediction from the manifests (no build)
# ---------------------------------------------------------------------------

def _wheel_table(wheel: str) -> dict:
    with open(ROOT / "packaging" / wheel / "pyproject.toml", "rb") as handle:
        return tomllib.load(handle)["tool"]["hatch"]["build"]["targets"]["wheel"]


def _excluded(dest: str, patterns: list[str]) -> bool:
    """True when ``dest`` (or one of its parent directories) matches an ``exclude`` glob.

    Covers the gitignore subset the manifests use: ``**/name`` and ``**/*.ext``.
    """
    parts = dest.split("/")
    prefixes = ["/".join(parts[: i + 1]) for i in range(len(parts))]
    for pattern in patterns:
        bare = pattern[3:] if pattern.startswith("**/") else pattern
        for prefix in prefixes:
            if fnmatch.fnmatchcase(prefix, pattern) or fnmatch.fnmatchcase(
                prefix.rsplit("/", 1)[-1], bare
            ):
                return True
    return False


def predicted_files(wheel: str) -> set[str]:
    """Destination paths (``skifer/...``) the ``force-include`` table of ``wheel`` ships."""
    table = _wheel_table(wheel)
    patterns = list(table.get("exclude", []))
    base = ROOT / "packaging" / wheel
    shipped: set[str] = set()
    for source, dest in table.get("force-include", {}).items():
        path = (base / source).resolve()
        if path.is_dir():
            for item in path.rglob("*"):
                if item.is_file():
                    target = f"{dest.rstrip('/')}/{item.relative_to(path).as_posix()}"
                    if not _excluded(target, patterns):
                        shipped.add(target)
        elif not _excluded(dest, patterns):
            shipped.add(dest)
    return shipped


def source_files(suffix: str = ".py") -> set[str]:
    """Every ``src/skifer/**`` file with ``suffix``, as a ``skifer/...`` destination path."""
    return {
        path.relative_to(SRC).as_posix()
        for path in (SRC / "skifer").rglob(f"*{suffix}")
        if path.is_file() and "__pycache__" not in path.parts and path.suffix != ".pyc"
    }


def ownership(suffix: str = ".py") -> tuple[dict[str, list[str]], list[str]]:
    """Map each source file to the wheels predicted to ship it; also list orphans."""
    owners: dict[str, list[str]] = {path: [] for path in source_files(suffix)}
    for wheel in WHEELS:
        for dest in predicted_files(wheel):
            if dest in owners:
                owners[dest].append(wheel)
    orphans = sorted(path for path, wheels in owners.items() if not wheels)
    return owners, orphans


# ---------------------------------------------------------------------------
# Build + RECORD
# ---------------------------------------------------------------------------

def build_wheels(out_dir: pathlib.Path) -> dict[str, pathlib.Path]:
    built: dict[str, pathlib.Path] = {}
    for wheel in WHEELS:
        subprocess.run(
            [sys.executable, "-m", "pip", "wheel", "--no-deps", "-q", "--disable-pip-version-check",
             str(ROOT / "packaging" / wheel), "-w", str(out_dir)],
            check=True,
        )
        prefix = wheel.replace("-", "_") + "-"
        matches = sorted(out_dir.glob(f"{prefix}*.whl"))
        if not matches:
            raise RuntimeError(f"pip wheel produced no wheel for {wheel} in {out_dir}")
        built[wheel] = matches[-1]
    return built


def record_files(wheel_path: pathlib.Path) -> set[str]:
    """Package files listed in a wheel's RECORD (``.dist-info`` entries excluded)."""
    with zipfile.ZipFile(wheel_path) as archive:
        record = next(n for n in archive.namelist() if n.endswith(".dist-info/RECORD"))
        rows = csv.reader(io.StringIO(archive.read(record).decode("utf-8")))
        return {row[0] for row in rows if row and ".dist-info/" not in row[0]}


def check_records(built: dict[str, pathlib.Path]) -> list[str]:
    problems: list[str] = []
    seen: dict[str, str] = {}
    for wheel, path in built.items():
        actual = record_files(path)
        expected = predicted_files(wheel)
        for extra in sorted(actual - expected):
            problems.append(f"{extra} | {wheel} | shipped by the built wheel but not predicted")
        for missing in sorted(expected - actual):
            problems.append(f"{missing} | {wheel} | predicted but absent from the built wheel")
        for dest in sorted(actual):
            if dest in seen:
                problems.append(f"{dest} | {wheel} | also shipped by {seen[dest]}")
            seen[dest] = wheel
    return problems


# ---------------------------------------------------------------------------
# Throw-away environments
# ---------------------------------------------------------------------------

def _make_venv(path: pathlib.Path) -> pathlib.Path:
    venv.EnvBuilder(with_pip=True, clear=True).create(path)
    bindir = "Scripts" if sys.platform == "win32" else "bin"
    return path / bindir / ("python.exe" if sys.platform == "win32" else "python")


def _pip_install(python: pathlib.Path, *requirements: str) -> None:
    subprocess.run(
        [str(python), "-m", "pip", "install", "-q", "--disable-pip-version-check", *requirements],
        check=True,
    )


def _run_probe(python: pathlib.Path, code: str, *args: str) -> dict:
    # -I: isolated mode, so neither the checkout nor the cwd shadows the installed wheels.
    result = subprocess.run(
        [str(python), "-I", "-c", code, *args], capture_output=True, text=True,
        cwd=tempfile.gettempdir(),
    )
    if result.returncode != 0 or not result.stdout.strip():
        return {"crashed": (result.stderr or result.stdout).strip()[-2000:]}
    return json.loads(result.stdout.strip().splitlines()[-1])


CORE_PROBE = r'''
import json, sys
out = {"errors": []}
try:
    import skifer
    from skifer import load_schema, parse_schema
    from skifer.core.ir import parse_to_ir
    from skifer.core.sql_compiler import compile_select
    schema = parse_schema("""
tables:
  - name: main.sales.orders
    alias: ord
    filter:
      - "status:equals:PAID"
select_final:
  - [order_id, order_id]
  - [amount, amount_eur, [cast:double, round:2]]
""")
    assert callable(load_schema)
    parsed = parse_to_ir(schema)
    for dialect in ("databricks", "duckdb", "snowflake"):
        sql = compile_select(parsed, dialect=dialect)
        if "SELECT" not in sql.upper():
            out["errors"].append(f"compile_select | {dialect} | no SELECT in output")
except Exception as exc:  # report, never crash the probe
    out["errors"].append(f"core probe | skifer-core | {type(exc).__name__}: {exc}")
out["forbidden_loaded"] = sorted(
    m for m in sys.modules if m.split(".")[0] in ("pyspark", "delta", "mlflow", "databricks")
)
print(json.dumps(out))
'''

GOVERNED_PROBE = r'''
import importlib, json, sys
modules = json.loads(sys.argv[1])
results = {}
for module in modules:
    try:
        importlib.import_module(module)
        results[module] = None
    except ModuleNotFoundError as exc:
        results[module] = {"type": "ModuleNotFoundError", "name": exc.name, "message": str(exc)}
    except Exception as exc:
        results[module] = {"type": type(exc).__name__, "name": None, "message": str(exc)[:300]}
loaded = sorted(
    m for m in sys.modules
    if m.split(".")[0] in ("pyspark", "delta", "mlflow", "databricks")
)
print(json.dumps({"results": results, "forbidden_loaded": loaded}))
'''


EXPORTS_PROBE = r'''
import importlib, importlib.util, json
results = {}
for package_name in ("skifer", "skifer.core"):
    package = importlib.import_module(package_name)
    for name in package.__all__:
        if name == "__version__":
            continue
        carrier = importlib.util.resolve_name("." + package._EXPORTS[name], package_name)
        key = f"{package_name}.{name}"
        try:
            getattr(package, name)
            results[key] = {"carrier": carrier, "error": None}
        except Exception as exc:
            results[key] = {"carrier": carrier, "error": {
                "type": type(exc).__name__, "message": str(exc)[:300]}}
loaders_error = None
try:
    from skifer.core.registry import RuleRegistry
    RuleRegistry.list_loaders()
except Exception as exc:
    loaders_error = f"{type(exc).__name__}: {exc}"[:300]
print(json.dumps({"results": results, "list_loaders": loaders_error}))
'''


def check_exports_alone(python: pathlib.Path) -> list[str]:
    """Plan 47.7 — with skifer-core alone, each export is the object or names its wheel."""
    out = _run_probe(python, EXPORTS_PROBE)
    if "crashed" in out:
        return [f"exports probe | skifer-core | crashed: {out['crashed']}"]
    owners, _ = ownership()
    owner_of = {_module_name(path): wheels[0] for path, wheels in owners.items()}
    problems = []
    for export, result in sorted(out["results"].items()):
        owner = owner_of.get(result["carrier"], "?")
        error = result["error"]
        if owner == "skifer-core":
            if error is not None:
                problems.append(f"{export} | skifer-core | {error['type']}: {error['message']}")
        elif error is None:
            problems.append(f"{export} | {owner} | resolved without its distribution")
        elif error["type"] != "ModuleNotFoundError" or f"`{owner}` distribution" not in (
            error["message"]
        ):
            problems.append(
                f"{export} | {owner} | does not name its distribution: "
                f"{error['type']}: {error['message']}"
            )
    if out["list_loaders"] is not None:
        problems.append(f"RuleRegistry.list_loaders | skifer-core | {out['list_loaders']}")
    return problems


def _requirement_name(requirement: str) -> str:
    return re.split(r"[\s\[<>=!~;]", requirement.strip(), maxsplit=1)[0].lower()


def optional_import_names(*wheels: str) -> set[str]:
    """Top-level import names of the optional third-party dependencies ``wheels`` declare."""
    own = {wheel.lower() for wheel in WHEELS}
    names: set[str] = set()
    for wheel in wheels:
        with open(ROOT / "packaging" / wheel / "pyproject.toml", "rb") as handle:
            extras = tomllib.load(handle)["project"].get("optional-dependencies", {})
        for requirements in extras.values():
            for requirement in requirements:
                dist = _requirement_name(requirement)
                if dist in own:  # a sibling Skifer wheel is not a third-party dependency
                    continue
                names.add(IMPORT_NAMES.get(dist, dist.replace("-", "_")))
    return names


def _module_name(dest: str) -> str:
    module = dest[: -len(".py")].replace("/", ".")
    return module[: -len(".__init__")] if module.endswith(".__init__") else module


def check_core_alone(python: pathlib.Path, built: dict[str, pathlib.Path]) -> list[str]:
    _pip_install(python, f"{built['skifer-core']}[dialects]")
    out = _run_probe(python, CORE_PROBE)
    if "crashed" in out:
        return [f"core probe | skifer-core | crashed: {out['crashed']}"]
    problems = list(out["errors"])
    problems += [f"{m} | skifer-core | loaded by core alone" for m in out["forbidden_loaded"]]
    return problems


def check_governed_without_engine(
    python: pathlib.Path, built: dict[str, pathlib.Path]
) -> list[str]:
    _pip_install(python, str(built["skifer-core"]), str(built["skifer"]))
    owner = {}
    for wheel in ("skifer-core", "skifer"):
        for dest in record_files(built[wheel]):
            if dest.endswith(".py"):
                owner[_module_name(dest)] = wheel
    out = _run_probe(python, GOVERNED_PROBE, json.dumps(sorted(owner)))
    if "crashed" in out:
        return [f"governed probe | skifer | crashed: {out['crashed']}"]
    allowed = optional_import_names("skifer-core", "skifer")
    problems = []
    for module, failure in sorted(out["results"].items()):
        if failure is None:
            continue
        missing = (failure["name"] or "").split(".")[0]
        if failure["type"] == "ModuleNotFoundError" and missing in allowed:
            continue
        problems.append(
            f"{module} | {owner[module]} | {failure['type']}"
            f"({failure['name']!r}): {failure['message']}"
        )
    problems += [
        f"{m} | skifer | loaded by the governed layer without the engine"
        for m in out["forbidden_loaded"]
    ]
    return problems


# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--keep", type=pathlib.Path, default=None,
                        help="work in DIR and keep the wheels and venvs (default: a temp dir)")
    args = parser.parse_args(argv)
    if tomllib is None:
        print("check_wheels.py needs Python 3.11+ (tomllib)", file=sys.stderr)
        return 1

    report: dict[str, list[str]] = {}
    owners, orphans = ownership(suffix="")
    report["ownership"] = [f"{path} | - | shipped by no wheel" for path in orphans] + [
        f"{path} | {', '.join(wheels)} | shipped by several wheels"
        for path, wheels in sorted(owners.items()) if len(wheels) > 1
    ]

    with tempfile.TemporaryDirectory(prefix="skifer-wheels-") as tmp:
        work = args.keep or pathlib.Path(tmp)
        work.mkdir(parents=True, exist_ok=True)
        built = build_wheels(work / "dist")
        report["records"] = check_records(built)
        core_venv = _make_venv(work / "venv-core")
        report["core alone"] = check_core_alone(core_venv, built)
        report["core alone: exports name their distribution"] = check_exports_alone(core_venv)
        report["governed without engine"] = check_governed_without_engine(
            _make_venv(work / "venv-governed"), built
        )
        sizes = {wheel: (path.name, len(record_files(path))) for wheel, path in built.items()}

    failed = False
    for section, problems in report.items():
        status = "FAIL" if problems else "ok"
        print(f"[{status}] {section}")
        for problem in problems:
            print(f"    {problem}")
        failed = failed or bool(problems)
    for wheel, (name, count) in sizes.items():
        print(f"built {wheel}: {name} ({count} files)")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
