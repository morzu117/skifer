"""Guards for what the documentation site publishes, and for what it must not."""

import importlib.util
import subprocess
import sys
from fnmatch import fnmatch
from pathlib import Path

import pytest
import yaml


REPO_ROOT = Path(__file__).resolve().parent.parent
DOCS_DIR = REPO_ROOT / "docs"

# Directories and files that hold working artifacts rather than reader documentation.
# They are expected to be excluded by mkdocs.yml, and this list is what the
# non-publication test looks for in a built site.
WORKING_ARTIFACTS = (
    Path("roadmap/29_agent_ready_semantic_layer/09_governed_capabilities_plan.md"),
    Path("reviews/plan-29-feat-03-review.md"),
    Path("ROADMAP_EVOLUTION.md"),
)


def _config() -> dict:
    return yaml.safe_load((REPO_ROOT / "mkdocs.yml").read_text(encoding="utf-8"))


def _nav_pages(node: object) -> set[Path]:
    if isinstance(node, str):
        return {Path(node)} if node.endswith(".md") else set()
    if isinstance(node, list):
        return set().union(*(_nav_pages(item) for item in node), set())
    if isinstance(node, dict):
        return set().union(*(_nav_pages(value) for value in node.values()), set())
    return set()


def _exclusion_patterns(config: dict) -> list[str]:
    """The site's own exclusions, so this test never duplicates them."""
    raw = config.get("exclude_docs") or ""
    return [line.strip() for line in raw.splitlines() if line.strip()]


def _is_excluded(page: Path, patterns: list[str]) -> bool:
    posix = page.as_posix()
    for pattern in patterns:
        if pattern.endswith("/"):
            if posix.startswith(pattern):
                return True
        elif fnmatch(posix, pattern) or fnmatch(page.name, pattern):
            return True
    return False


def test_every_public_documentation_page_appears_in_mkdocs_navigation():
    config = _config()
    patterns = _exclusion_patterns(config)
    navigation = _nav_pages(config["nav"])
    documentation = {
        page.relative_to(DOCS_DIR)
        for page in DOCS_DIR.rglob("*.md")
        if not _is_excluded(page.relative_to(DOCS_DIR), patterns)
    }

    missing = sorted(documentation - navigation)
    assert not missing, "Documentation pages missing from mkdocs nav: " + ", ".join(
        str(page) for page in missing
    )


def test_every_mkdocs_navigation_page_exists():
    missing = sorted(
        page for page in _nav_pages(_config()["nav"]) if not (DOCS_DIR / page).is_file()
    )

    assert not missing, "MkDocs nav points to missing pages: " + ", ".join(
        str(page) for page in missing
    )


def test_working_artifacts_are_excluded_by_the_site_configuration():
    """Omitting a page from `nav` only hides the menu entry; the page still ships."""
    patterns = _exclusion_patterns(_config())

    published = [page for page in WORKING_ARTIFACTS if not _is_excluded(page, patterns)]
    assert not published, "Working artifacts not excluded from the site: " + ", ".join(
        str(page) for page in published
    )


@pytest.fixture(scope="module")
def built_site(tmp_path_factory):
    if importlib.util.find_spec("mkdocs") is None:
        pytest.skip('mkdocs is not installed; install it with pip install -e ".[docs]"')

    site_dir = tmp_path_factory.mktemp("site")
    completed = subprocess.run(
        [sys.executable, "-m", "mkdocs", "build", "--strict", "--site-dir", str(site_dir)],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    return completed, site_dir


def test_the_documentation_site_builds_without_warnings(built_site):
    completed, _ = built_site
    assert completed.returncode == 0, completed.stdout + completed.stderr


def test_the_built_site_publishes_no_working_artifact(built_site):
    _, site_dir = built_site

    # Matched against the whole published path: a page becomes a directory holding
    # an index.html, so the artifact name shows up in a parent segment, not the file.
    markers = ("roadmap", "reviews", "dev-handoff", "ROADMAP_EVOLUTION")
    leaked = sorted(
        relative
        for relative in (
            path.relative_to(site_dir).as_posix()
            for path in site_dir.rglob("*")
            if path.is_file()
        )
        if any(marker in relative for marker in markers)
    )
    assert not leaked, "Working artifacts published on the site: " + ", ".join(leaked)
