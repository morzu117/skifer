"""docs/metric_candidates.md stays in step with the code it documents (Plan 44, S10)."""

from __future__ import annotations

import ast
from pathlib import Path
import re

REPO_ROOT = Path(__file__).resolve().parents[1]
PAGE = (REPO_ROOT / "docs" / "metric_candidates.md").read_text(encoding="utf-8")
PLAN44_KEY = re.compile(r"^(candidate_|candidates_|knowledge_)[a-z_]+$")


MODULES_READING_ENV = ("candidates", "knowledge", "agentic/candidate_agent.py")


def _is_env_config(node: ast.AST) -> bool:
    """``env_config`` or ``….env_config()`` — the environment mapping of ``config.yaml``."""
    if isinstance(node, ast.Call):
        node = node.func
    return (isinstance(node, ast.Name) and node.id == "env_config") or (
        isinstance(node, ast.Attribute) and node.attr == "env_config")


def _config_keys() -> set[str]:
    """Every Plan 44 key read from an environment.

    ``core/config.py``: string constants named like a key. The Plan 44 packages:
    ``env_config.get("…")`` reads, so a key validated nowhere central is still found.
    """
    tree = ast.parse((REPO_ROOT / "src/skifer/core/config.py").read_text(encoding="utf-8"))
    keys = {node.value for node in ast.walk(tree)
            if isinstance(node, ast.Constant) and isinstance(node.value, str)
            and PLAN44_KEY.match(node.value)}
    src = REPO_ROOT / "src" / "skifer"
    for module in MODULES_READING_ENV:
        paths = [src / module] if module.endswith(".py") else sorted((src / module).glob("*.py"))
        for path in paths:
            for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
                if (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                        and node.func.attr == "get" and _is_env_config(node.func.value)
                        and node.args and isinstance(node.args[0], ast.Constant)
                        and isinstance(node.args[0].value, str)):
                    keys.add(node.args[0].value)
    return keys


def _cli_exit_codes(prefix: str) -> set[int]:
    tree = ast.parse((REPO_ROOT / "src/skifer/cli.py").read_text(encoding="utf-8"))
    return {node.value.value for node in tree.body
            if isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant)
            and any(isinstance(t, ast.Name) and t.id.startswith(f"{prefix}_EXIT_")
                    for t in node.targets)}


def _documented_exit_codes(command: str) -> set[int]:
    section = PAGE.split("\n## Exit codes\n", 1)[1].split("\n## ", 1)[0]
    codes: set[int] = set()
    for block in section.split("\n### ")[1:]:
        heading, _, body = block.partition("\n")
        if f"skifer {command}" in heading:
            codes |= {int(code) for code in re.findall(r"^\| `(\d+)` \|", body, re.MULTILINE)}
    return codes


def test_every_config_key_is_documented():
    keys = _config_keys()
    assert len(keys) >= 15 and "knowledge_domains" in keys, sorted(keys)        # guard: the scan itself still finds them
    missing = sorted(key for key in keys if f"{key}:" not in PAGE)
    assert not missing, f"config keys missing from docs/metric_candidates.md: {missing}"


def test_every_exit_code_is_documented():
    for command, prefix in (("candidates", "CANDIDATES"), ("knowledge", "KNOWLEDGE")):
        used, documented = _cli_exit_codes(prefix), _documented_exit_codes(command)
        assert used, prefix
        assert used == documented, (command, sorted(used), sorted(documented))
