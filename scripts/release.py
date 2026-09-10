#!/usr/bin/env python3
"""Prepare and publish a Skifer release.

Usage:
    python3 scripts/release.py 1.4.2
    python3 scripts/release.py 1.4.2 --no-push
    python3 scripts/release.py 1.4.2 --dry-run
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
CHANGELOG = ROOT / "CHANGELOG.md"
PUBLISH_PYPI = ROOT / ".github" / "workflows" / "publish-pypi.yml"
PUBLISH_TESTPYPI = ROOT / ".github" / "workflows" / "publish-testpypi.yml"


def run(args: list[str], *, dry_run: bool = False) -> str:
    printable = " ".join(args)
    if dry_run:
        print(f"+ {printable}")
        return ""
    result = subprocess.run(
        args,
        cwd=ROOT,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    if result.stdout:
        print(result.stdout, end="")
    return result.stdout


def git_output(args: list[str]) -> str:
    return subprocess.check_output(args, cwd=ROOT, text=True).strip()


def ensure_clean_worktree() -> None:
    status = git_output(["git", "status", "--porcelain"])
    if status:
        raise SystemExit(
            "Working tree is not clean. Commit, stash, or discard changes before releasing:\n"
            f"{status}"
        )


def ensure_on_main() -> None:
    branch = git_output(["git", "branch", "--show-current"])
    if branch != "main":
        raise SystemExit(f"Release must run from main, currently on {branch!r}.")


def ensure_synced_with_origin() -> None:
    run(["git", "fetch", "origin"])
    local = git_output(["git", "rev-parse", "main"])
    remote = git_output(["git", "rev-parse", "origin/main"])
    if local != remote:
        raise SystemExit(
            "main is not in sync with origin/main. Pull/rebase first, then rerun release."
        )


def ensure_workflow_secrets() -> None:
    prod = PUBLISH_PYPI.read_text()
    test = PUBLISH_TESTPYPI.read_text()
    missing: list[str] = []
    if "secrets.PYPI_API_TOKEN" not in prod:
        missing.append("publish-pypi.yml must reference secrets.PYPI_API_TOKEN")
    if "secrets.TEST_PYPI_API_TOKEN" not in test:
        missing.append("publish-testpypi.yml must reference secrets.TEST_PYPI_API_TOKEN")
    if missing:
        raise SystemExit("\n".join(missing))


def validate_version(version: str) -> None:
    if not re.fullmatch(r"\d+\.\d+\.\d+(?:[a-zA-Z0-9_.+-]+)?", version):
        raise SystemExit(f"Unsupported version format: {version!r}")


def replace_pyproject_version(version: str) -> None:
    text = PYPROJECT.read_text()
    updated, count = re.subn(
        r'(?m)^version = "[^"]+"$',
        f'version = "{version}"',
        text,
        count=1,
    )
    if count != 1:
        raise SystemExit("Could not find exactly one project version in pyproject.toml.")
    PYPROJECT.write_text(updated)


def promote_changelog(version: str) -> None:
    text = CHANGELOG.read_text()
    heading = f"## [{version}] - {dt.date.today().isoformat()}"
    if heading in text:
        return
    marker = "## [Unreleased]\n"
    if marker not in text:
        raise SystemExit("Could not find ## [Unreleased] in CHANGELOG.md.")
    updated = text.replace(marker, f"{marker}\n{heading}\n\n", 1)
    CHANGELOG.write_text(updated)


def ensure_tag_available(tag: str) -> None:
    local = subprocess.run(
        ["git", "rev-parse", "--verify", "--quiet", f"refs/tags/{tag}"],
        cwd=ROOT,
    )
    if local.returncode == 0:
        raise SystemExit(f"Local tag already exists: {tag}")
    remote = git_output(["git", "ls-remote", "--tags", "origin", tag])
    if remote:
        raise SystemExit(f"Remote tag already exists: {tag}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("version", help="Release version, for example 1.4.2")
    parser.add_argument("--no-push", action="store_true", help="Commit and tag locally only")
    parser.add_argument("--dry-run", action="store_true", help="Print commands without editing")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    version = args.version.removeprefix("v")
    tag = f"v{version}"

    validate_version(version)
    ensure_on_main()
    ensure_clean_worktree()
    ensure_synced_with_origin()
    ensure_workflow_secrets()
    ensure_tag_available(tag)

    if args.dry_run:
        print(f"Would release {version} with tag {tag}")
        return 0

    replace_pyproject_version(version)
    promote_changelog(version)

    run(["git", "add", "pyproject.toml", "CHANGELOG.md"])
    run(["git", "commit", "-m", f"chore(release): {version}"])
    run(["git", "tag", tag])

    if not args.no_push:
        run(["git", "push", "origin", "main"])
        run(["git", "push", "origin", tag])
        print(
            f"Released {version}. TestPyPI is triggered by main; PyPI is triggered by {tag}."
        )
    else:
        print(f"Prepared {version}. Push with: git push origin main && git push origin {tag}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
