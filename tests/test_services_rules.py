"""Spark-free tests for the transport-neutral rule service."""

import ast
from pathlib import Path

import pytest

from skifer.core.registry import RuleRegistry
from skifer.observability.tracing import TraceContext
from skifer.services import (
    InvalidRequest,
    RequestContext,
    RuleService,
    SCOPE_PROJECT_READ,
    SCOPE_RULES_WRITE,
    ScopeDenied,
    SnippetSpec,
)


VALID_RULE = """\
from skifer.core.registry import RuleRegistry

@RuleRegistry.register_rule()
def valid_rule(df):
    return {"flag": F.lit(True)}
"""


def _context(*scopes: str) -> RequestContext:
    return RequestContext(
        subject="local-user",
        scopes=frozenset(scopes),
        consumer_class="local",
        trace_context=TraceContext(),
    )


def _write_rule(tmp_path: Path, name: str, source: str) -> Path:
    path = tmp_path / "rules" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(source, encoding="utf-8")
    return path


@pytest.fixture(autouse=True)
def _isolated_rule_registry():
    previous = dict(RuleRegistry._rules)
    RuleRegistry.clear(rules=True, loaders=False)
    yield
    RuleRegistry._rules.clear()
    RuleRegistry._rules.update(previous)


def test_scan_isolates_broken_module(tmp_path):
    _write_rule(tmp_path, "valid.py", VALID_RULE)
    _write_rule(tmp_path, "broken.py", "def r(:\n")

    report = RuleService(str(tmp_path)).scan(_context(SCOPE_PROJECT_READ))

    assert [rule.name for rule in report.rules] == ["valid_rule"]
    assert report.invalid_modules == (
        {"file": "rules/broken.py", "error": "SyntaxError"},
    )


def test_scan_reload_picks_up_new_rule(tmp_path):
    _write_rule(tmp_path, "first.py", VALID_RULE)
    service = RuleService(str(tmp_path))
    context = _context(SCOPE_PROJECT_READ)
    assert [rule.name for rule in service.scan(context).rules] == ["valid_rule"]

    _write_rule(
        tmp_path,
        "second.py",
        VALID_RULE.replace("valid_rule", "second_rule").replace('"flag"', '"other"'),
    )

    assert [rule.name for rule in service.scan(context).rules] == [
        "second_rule",
        "valid_rule",
    ]


def test_list_returns_rule_views_with_line(tmp_path):
    _write_rule(tmp_path, "valid.py", VALID_RULE)

    rules = RuleService(str(tmp_path)).list(_context(SCOPE_PROJECT_READ))

    assert len(rules) == 1
    assert rules[0].line == 3
    assert rules[0].file == "rules/valid.py"
    assert rules[0].kind == "projection"


def test_dependency_graph_edges(tmp_path):
    _write_rule(
        tmp_path,
        "graph.py",
        """\
from skifer.core.registry import RuleRegistry

@RuleRegistry.register_rule()
def A(df):
    return {"made": F.lit(1)}

@RuleRegistry.register_rule()
def B(df):
    return {"result": F.col("made") + 1}
""",
    )
    service = RuleService(str(tmp_path))
    context = _context(SCOPE_PROJECT_READ)
    service.scan(context)

    graph = service.dependency_graph(context, ("A", "B"))

    assert graph["B"] == ["A"]


def test_generate_snippet_constant_byte_for_byte(tmp_path):
    service = RuleService(str(tmp_path))
    context = _context(SCOPE_PROJECT_READ)
    spec = SnippetSpec(kind="constant", target="flag", value=True)
    expected = 'def flag(df):\n    return {"flag": F.lit(True)}\n'

    assert service.generate_snippet(context, spec) == expected
    assert service.generate_snippet(context, spec) == expected


def test_generate_snippet_when_otherwise_byte_for_byte(tmp_path):
    service = RuleService(str(tmp_path))
    context = _context(SCOPE_PROJECT_READ)
    spec = SnippetSpec(
        kind="when_otherwise",
        target="is_active",
        condition="status:equals:ACTIVE",
        then=1,
        value=0,
    )
    expected = (
        'def is_active(df):\n'
        '    return {"is_active": F.when(F.col("status") == \'ACTIVE\', '
        "F.lit(1)).otherwise(F.lit(0))}\n"
    )

    assert service.generate_snippet(context, spec) == expected
    assert service.generate_snippet(context, spec) == expected


def test_generate_snippet_rejects_bad_identifier(tmp_path):
    with pytest.raises(InvalidRequest):
        RuleService(str(tmp_path)).generate_snippet(
            _context(SCOPE_PROJECT_READ),
            SnippetSpec(kind="constant", target="1bad", value=1),
        )


def test_write_rule_rejects_syntax_error(tmp_path):
    target = tmp_path / "rules" / "bad.py"
    with pytest.raises(InvalidRequest):
        RuleService(str(tmp_path)).write_rule(
            _context(SCOPE_RULES_WRITE), "rules/bad.py", "def ("
        )
    assert not target.exists()


def test_write_rule_requires_scope(tmp_path):
    with pytest.raises(ScopeDenied):
        RuleService(str(tmp_path)).write_rule(
            _context(), "rules/new.py", "def rule(df):\n    return df\n"
        )


def test_write_rule_atomic_ok(tmp_path):
    code = "def rule(df):\n    return df\n"
    written = RuleService(str(tmp_path)).write_rule(
        _context(SCOPE_RULES_WRITE), "rules/new.py", code
    )

    content = Path(written).read_text(encoding="utf-8")
    assert content == code
    ast.parse(content)


@pytest.mark.parametrize("file", ("config.yaml", "../x", "rules/foo.yaml"))
def test_write_rule_rejects_targets_outside_rules_python_files(tmp_path, file):
    config = tmp_path / "config.yaml"
    config.write_text("environment: DEV\n", encoding="utf-8")

    with pytest.raises(InvalidRequest):
        RuleService(str(tmp_path)).write_rule(
            _context(SCOPE_RULES_WRITE), file, "x = 1\n"
        )

    assert config.read_text(encoding="utf-8") == "environment: DEV\n"
