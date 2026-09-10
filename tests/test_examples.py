"""The examples in examples/ must actually run.

Documentation that is never executed drifts silently: an example is only useful
if the reader can trust that it does today what it says it does. Running them
here means a broken example fails the build like any other regression.

They run in a SUBPROCESS, exactly as the READMEs tell a reader to run them
(`python examples/<name>/run.py`). In-process execution was both less faithful
and actively harmful: the engine reconfigures global logging on startup, which
silently broke `caplog` capture for every test that ran afterwards.
"""

from __future__ import annotations

from pathlib import Path
import subprocess
import sys
from functools import lru_cache

import pytest


REPO_ROOT = Path(__file__).resolve().parent.parent
EXAMPLES_DIR = REPO_ROOT / "examples"


def _example_scripts() -> list[Path]:
    return sorted(EXAMPLES_DIR.glob("*/run.py"))


@lru_cache(maxsize=None)
def _run(script: Path) -> subprocess.CompletedProcess:
    """One subprocess per example, cached so the assertions below share a single run.

    Each example gets its own process on purpose. A reader runs one example alone, so
    that is how the suite must run them: a shared process would let an example pass
    only because a previous one had started Spark or registered a rule for it.
    """
    return subprocess.run(
        [sys.executable, str(script)],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=600,
    )


def _failure(script: Path, completed: subprocess.CompletedProcess) -> str:
    return (
        f"{script.parent.name} failed:\n"
        f"{completed.stdout[-3000:]}\n{completed.stderr[-3000:]}"
    )

def test_every_example_directory_has_a_runnable_entry_point():
    """A directory that documents itself but cannot be run is the failure mode."""
    documented = sorted(path.parent for path in EXAMPLES_DIR.glob("*/README.md"))
    runnable = sorted(path.parent for path in _example_scripts())

    assert documented == runnable, (
        "Every example directory needs both a README.md and a run.py."
    )


@pytest.mark.parametrize(
    "script", _example_scripts(), ids=lambda path: path.parent.name
)
def test_example_runs(script: Path):
    """Run the example the way the README says to, and fail loudly if it does not."""
    completed = _run(script)

    assert completed.returncode == 0, _failure(script, completed)
    assert completed.stdout.strip(), (
        f"{script.parent.name} produced no output to explain itself."
    )


EXPECTED_OUTPUT_HEADING = "## What you should see"


def _expected_fragments(readme: Path) -> list[str]:
    """The lines a README promises, taken from its `What you should see` block.

    A line is matched as a fragment, not compared for equality: real output carries
    timestamps, sandbox suffixes and Spark chatter that no README should pin down.
    Anything after `...` on a line is dropped, so a README can elide a varying tail.
    """
    lines = readme.read_text(encoding="utf-8").splitlines()
    try:
        start = lines.index(EXPECTED_OUTPUT_HEADING)
    except ValueError:
        return []

    fenced, inside = [], False
    for line in lines[start + 1 :]:
        if line.startswith("```"):
            if inside:
                break
            inside = True
            continue
        if inside:
            fenced.append(line)

    fragments = []
    for line in fenced:
        fragment = line.split("...")[0].strip()
        if fragment:
            fragments.append(fragment)
    return fragments


def test_every_example_documents_the_output_it_produces():
    """An example that still runs while printing something else has gone stale silently."""
    undocumented = sorted(
        script.parent.name
        for script in _example_scripts()
        if not _expected_fragments(script.parent / "README.md")
    )

    assert not undocumented, (
        "These examples promise no output under "
        f"'{EXPECTED_OUTPUT_HEADING}': {', '.join(undocumented)}"
    )


@pytest.mark.parametrize(
    "script", _example_scripts(), ids=lambda path: path.parent.name
)
def test_example_prints_what_its_readme_promises(script: Path):
    """The README is the contract a reader checks their own run against."""
    completed = _run(script)
    assert completed.returncode == 0, _failure(script, completed)

    missing = [
        fragment
        for fragment in _expected_fragments(script.parent / "README.md")
        if fragment not in completed.stdout
    ]
    assert not missing, (
        f"{script.parent.name} no longer prints what its README promises.\n"
        f"Missing: {missing}\n"
        f"Actual output:\n{completed.stdout[-3000:]}"
    )


DOCS_DIR = REPO_ROOT / "docs"

# Working artifacts, not reader documentation. Kept in step with mkdocs.yml's
# exclude_docs: a plan that happens to mention an example does not make that
# example findable, because plans are not published.
UNPUBLISHED_DOC_TREES = ("roadmap", "reviews")


def _reader_documentation() -> list[Path]:
    return [
        page
        for page in DOCS_DIR.rglob("*.md")
        if not any(tree in page.relative_to(DOCS_DIR).parts for tree in UNPUBLISHED_DOC_TREES)
        and page.name != "ROADMAP_EVOLUTION.md"
    ]


def test_every_example_is_reachable_from_the_documentation():
    """An example nobody can find from the docs is an example nobody runs.

    The suite already proves each example works. This proves a reader has a path
    to it: the same failure the MkDocs navigation guard exists to prevent, one
    directory over.
    """
    published = "\n".join(
        page.read_text(encoding="utf-8") for page in _reader_documentation()
    )

    unreachable = sorted(
        script.parent.name
        for script in _example_scripts()
        if script.parent.name not in published
    )

    assert not unreachable, (
        "Examples no documentation page points to: " + ", ".join(unreachable)
    )
