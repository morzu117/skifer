#!/usr/bin/env python3
"""Prepare a Skifer release: bump the version, promote [Unreleased], commit and tag.

Usage:
    python3 scripts/release.py patch|minor|major     # bump from pyproject.toml
    python3 scripts/release.py 3.1.0                 # explicit version
    python3 scripts/release.py minor --dry-run       # print what would happen, touch nothing
    python3 scripts/release.py minor --no-git        # edit files only (no commit, no tag)

The script never pushes. Locally, push with `git push origin main vX.Y.Z`.
In CI, .github/workflows/release.yml pushes and triggers the PyPI publication.

Files touched:
    pyproject.toml, uv.lock (skifer entry), CHANGELOG.md, docs/install_databricks.md,
    and packaging/*/pyproject.toml (the four wheel versions and their cross-pins).
"""

from __future__ import annotations

import argparse
import datetime as dt
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PYPROJECT = ROOT / "pyproject.toml"
UV_LOCK = ROOT / "uv.lock"
CHANGELOG = ROOT / "CHANGELOG.md"
INSTALL_DOC = ROOT / "docs" / "install_databricks.md"
FILES = ["pyproject.toml", "uv.lock", "CHANGELOG.md", "docs/install_databricks.md"]
PACKAGING = ROOT / "packaging"

SEMVER = re.compile(r"(\d+)\.(\d+)\.(\d+)")
# Cross-distribution pins inside packaging/*/pyproject.toml (e.g. skifer-core==X.Y.Z).
SKIFER_PIN = re.compile(r"(skifer(?:-core|-engine|-databricks)?==)\d+\.\d+\.\d+")


def git(*args: str) -> str:
    return subprocess.check_output(["git", *args], cwd=ROOT, text=True).strip()


def current_version() -> str:
    m = re.search(r'(?m)^version = "([^"]+)"$', PYPROJECT.read_text())
    if not m:
        raise SystemExit("Could not find the project version in pyproject.toml.")
    return m.group(1)


def resolve_version(target: str, current: str) -> str:
    target = target.removeprefix("v")
    if target in {"patch", "minor", "major"}:
        m = SEMVER.fullmatch(current)
        if not m:
            raise SystemExit(f"Current version {current!r} is not X.Y.Z, pass an explicit version.")
        major, minor, patch = map(int, m.groups())
        return {
            "major": f"{major + 1}.0.0",
            "minor": f"{major}.{minor + 1}.0",
            "patch": f"{major}.{minor}.{patch + 1}",
        }[target]
    if not SEMVER.fullmatch(target):
        raise SystemExit(f"Unsupported version {target!r}: expected X.Y.Z, patch, minor or major.")
    return target


def ensure_greater(new: str, current: str) -> None:
    key = lambda v: tuple(int(x) for x in SEMVER.fullmatch(v).groups())  # noqa: E731
    if SEMVER.fullmatch(current) and key(new) <= key(current):
        raise SystemExit(f"New version {new} must be greater than the current {current}.")


def ensure_git_ready(tag: str) -> None:
    branch = git("branch", "--show-current")
    if branch != "main":
        raise SystemExit(f"Release must run from main, currently on {branch!r}.")
    status = git("status", "--porcelain", "--untracked-files=no")
    if status:
        raise SystemExit(f"Working tree has tracked changes, commit or stash them first:\n{status}")
    subprocess.run(["git", "fetch", "--quiet", "origin", "main", "--tags"], cwd=ROOT, check=True)
    if git("rev-parse", "HEAD") != git("rev-parse", "origin/main"):
        raise SystemExit("main is not in sync with origin/main. Pull first.")
    if git("tag", "--list", tag) or git("ls-remote", "--tags", "origin", tag):
        raise SystemExit(f"Tag {tag} already exists.")


def unreleased_body(text: str) -> str:
    m = re.search(r"(?ms)^## \[Unreleased\][^\n]*\n(.*?)(?=^## \[|\Z)", text)
    if not m:
        raise SystemExit("Could not find '## [Unreleased]' in CHANGELOG.md.")
    return m.group(1).strip()


def edit_pyproject(new: str) -> None:
    text, n = re.subn(r'(?m)^version = "[^"]+"$', f'version = "{new}"', PYPROJECT.read_text(), count=1)
    if n != 1:
        raise SystemExit("Could not rewrite the version in pyproject.toml.")
    PYPROJECT.write_text(text)


def edit_uv_lock(new: str) -> None:
    # Only the editable `skifer` package entry. No `uv lock`: it could re-resolve other pins.
    text, n = re.subn(
        r'(?m)(^name = "skifer"\nversion = ")[^"]+(")', rf"\g<1>{new}\g<2>", UV_LOCK.read_text(), count=1
    )
    if n != 1:
        raise SystemExit("Could not find the skifer entry in uv.lock.")
    UV_LOCK.write_text(text)


def edit_changelog(new: str, today: str) -> None:
    text = CHANGELOG.read_text()
    if not unreleased_body(text):
        raise SystemExit("[Unreleased] is empty: nothing to release.")
    text = text.replace("## [Unreleased]\n", f"## [Unreleased]\n\n## [{new}] - {today}\n", 1)
    CHANGELOG.write_text(text)


def edit_install_doc(old: str, new: str) -> None:
    text = INSTALL_DOC.read_text()
    text = text.replace(f"skifer=={old}", f"skifer=={new}").replace(f"e.g. {old}", f"e.g. {new}")
    INSTALL_DOC.write_text(text)


def packaging_pyprojects() -> list[Path]:
    return sorted(PACKAGING.glob("*/pyproject.toml"))


def edit_packaging(new: str) -> list[str]:
    """Bump the version and the cross-distribution pins in every packaging wheel.

    The four distributions share one version and pin each other by exact version;
    test_packaging guards that they never drift. Returns the files changed.
    """
    changed = []
    for pyproject in packaging_pyprojects():
        text = pyproject.read_text()
        text, n = re.subn(r'(?m)^version = "[^"]+"$', f'version = "{new}"', text, count=1)
        if n != 1:
            raise SystemExit(f"Could not rewrite the version in {pyproject.relative_to(ROOT)}.")
        text = SKIFER_PIN.sub(rf"\g<1>{new}", text)
        pyproject.write_text(text)
        changed.append(str(pyproject.relative_to(ROOT)))
    return changed


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("target", help="patch | minor | major | X.Y.Z")
    ap.add_argument("--dry-run", action="store_true", help="check and print, change nothing")
    ap.add_argument("--no-git", action="store_true", help="edit files only: no commit, no tag")
    args = ap.parse_args()

    old = current_version()
    new = resolve_version(args.target, old)
    tag = f"v{new}"
    ensure_greater(new, old)
    ensure_git_ready(tag)
    unreleased_body(CHANGELOG.read_text()) or sys.exit("[Unreleased] is empty: nothing to release.")

    print(f"{old} -> {new} (tag {tag})")
    if args.dry_run:
        print("Dry run: nothing changed.")
        return 0

    edit_pyproject(new)
    edit_uv_lock(new)
    edit_changelog(new, dt.date.today().isoformat())
    edit_install_doc(old, new)
    packaging_files = edit_packaging(new)

    if not args.no_git:
        git("add", *FILES, *packaging_files)
        git("commit", "-m", f"chore(release): {new}")
        git("tag", tag)
        print(f"Committed and tagged {tag}. Push with: git push origin main {tag}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
