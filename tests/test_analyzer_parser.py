"""Tests for the project analyzer's SQL front-end and lazy sqlglot parsing (Plan 43 — 43.0)."""
from __future__ import annotations

import subprocess
import sys

import pytest

from skifer.analyzer.frontends import RawStatement, split_sql_file
from skifer.analyzer.parser import (
    ANALYZER_EXTRA,
    AnalyzerDependencyError,
    StatementParseError,
    parse_script,
)


class TestSplitSqlFile:
    def test_splits_on_top_level_semicolons(self, tmp_path):
        path = tmp_path / "a.sql"
        path.write_text("SELECT 1;\nSELECT 2;\n")

        statements = split_sql_file(path)

        assert [s.text for s in statements] == ["SELECT 1", "SELECT 2"]

    def test_reports_1_indexed_line_of_first_statement_character(self, tmp_path):
        path = tmp_path / "a.sql"
        path.write_text("\n\nSELECT 1;\n")

        statements = split_sql_file(path)

        assert statements[0].line == 3

    def test_skips_leading_blank_lines_and_comments_for_line_number(self, tmp_path):
        path = tmp_path / "a.sql"
        path.write_text(
            "-- a leading comment\n"
            "-- a second comment line\n"
            "SELECT 1;\n"
        )

        statements = split_sql_file(path)

        assert statements[0].line == 3
        assert statements[0].text == "SELECT 1"

    def test_skips_leading_block_comment_for_line_number(self, tmp_path):
        path = tmp_path / "a.sql"
        path.write_text("/* line one\nline two */\nSELECT 1;\n")

        statements = split_sql_file(path)

        assert statements[0].line == 3

    def test_semicolon_inside_string_literal_is_not_a_boundary(self, tmp_path):
        path = tmp_path / "a.sql"
        path.write_text("SELECT 'a;b' AS x;\nSELECT 2;\n")

        statements = split_sql_file(path)

        assert len(statements) == 2
        assert statements[0].text == "SELECT 'a;b' AS x"

    def test_semicolon_inside_line_comment_is_not_a_boundary(self, tmp_path):
        path = tmp_path / "a.sql"
        path.write_text("SELECT 1 -- a comment with a ; inside\n;\nSELECT 2;\n")

        statements = split_sql_file(path)

        assert len(statements) == 2

    def test_semicolon_inside_block_comment_is_not_a_boundary(self, tmp_path):
        path = tmp_path / "a.sql"
        path.write_text("SELECT 1 /* a ; inside a block comment */;\nSELECT 2;\n")

        statements = split_sql_file(path)

        assert len(statements) == 2

    def test_backslash_escaped_quote_does_not_close_the_literal(self, tmp_path):
        path = tmp_path / "a.sql"
        path.write_text("SELECT 'O\\'Brien' AS name;\nSELECT 2;\n")

        statements = split_sql_file(path)

        assert len(statements) == 2
        assert statements[0].text == "SELECT 'O\\'Brien' AS name"

    def test_backslash_escaped_quote_followed_by_embedded_semicolon(self, tmp_path):
        path = tmp_path / "a.sql"
        path.write_text(
            "SELECT 'O\\'Brien; Co' AS name;\n"
            "SELECT 2;\n"
        )

        statements = split_sql_file(path)

        assert len(statements) == 2
        assert statements[0].text == "SELECT 'O\\'Brien; Co' AS name"

    def test_trailing_comment_only_text_is_dropped(self, tmp_path):
        path = tmp_path / "a.sql"
        path.write_text("SELECT 1;\n-- trailing comment, no statement\n")

        statements = split_sql_file(path)

        assert len(statements) == 1

    def test_empty_file_yields_no_statements(self, tmp_path):
        path = tmp_path / "a.sql"
        path.write_text("")

        assert split_sql_file(path) == []

    def test_statement_without_trailing_semicolon_is_kept(self, tmp_path):
        path = tmp_path / "a.sql"
        path.write_text("SELECT 1")

        statements = split_sql_file(path)

        assert [s.text for s in statements] == ["SELECT 1"]

    def test_source_path_is_recorded(self, tmp_path):
        path = tmp_path / "a.sql"
        path.write_text("SELECT 1;\n")

        statements = split_sql_file(path)

        assert statements[0].source_path == str(path)


class TestParseScript:
    def test_parses_each_statement_with_sqlglot(self, tmp_path):
        path = tmp_path / "a.sql"
        path.write_text("SELECT 1 AS x;\nSELECT 2 AS y;\n")

        statements = parse_script(path)

        assert len(statements) == 2
        assert statements[0].ast is not None
        assert statements[1].ast is not None

    def test_carries_source_path_and_line_through_to_statement(self, tmp_path):
        path = tmp_path / "a.sql"
        path.write_text("\nSELECT 1;\n")

        statements = parse_script(path)

        assert statements[0].source_path == str(path)
        assert statements[0].line == 2

    def test_bad_statement_raises_statement_parse_error_naming_file_and_line(self, tmp_path):
        path = tmp_path / "a.sql"
        path.write_text("SELECT 1;\nSELECT FROM WHERE;\n")

        with pytest.raises(StatementParseError) as caught:
            parse_script(path)

        message = str(caught.value)
        assert str(path) in message
        assert "2" in message
        assert caught.value.source_path == str(path)
        assert caught.value.line == 2

    def test_accepts_an_explicit_dialect(self, tmp_path):
        path = tmp_path / "a.sql"
        path.write_text("SELECT 1;\n")

        statements = parse_script(path, dialect="databricks")

        assert len(statements) == 1


def test_analyzer_extra_names_the_pip_install_command():
    assert ANALYZER_EXTRA == 'pip install -e ".[analyzer]"'


def test_core_package_imports_without_sqlglot_installed():
    """skifer.analyzer (and skifer as a whole) must stay importable without
    the optional sqlglot dependency — only calling parse_script() needs it."""
    code = (
        "import sys\n"
        "import builtins\n"
        "_real_import = builtins.__import__\n"
        "def _blocking_import(name, *args, **kwargs):\n"
        "    if name == 'sqlglot' or name.startswith('sqlglot.'):\n"
        "        raise ImportError('sqlglot is not installed')\n"
        "    return _real_import(name, *args, **kwargs)\n"
        "builtins.__import__ = _blocking_import\n"
        "import skifer\n"
        "import skifer.analyzer\n"
        "from skifer.analyzer import split_sql_file, AnalyzerDependencyError\n"
        "assert 'sqlglot' not in sys.modules\n"
        "print('OK')\n"
    )
    result = subprocess.run([sys.executable, "-I", "-c", code], capture_output=True, text=True)

    assert result.returncode == 0, result.stdout + result.stderr
    assert "OK" in result.stdout


def test_parse_script_without_sqlglot_raises_analyzer_dependency_error(tmp_path, monkeypatch):
    import builtins

    path = tmp_path / "a.sql"
    path.write_text("SELECT 1;\n")

    real_import = builtins.__import__

    def blocking_import(name, *args, **kwargs):
        if name == "sqlglot" or name.startswith("sqlglot."):
            raise ImportError("simulated: sqlglot not installed")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", blocking_import)

    with pytest.raises(AnalyzerDependencyError) as caught:
        parse_script(path)

    assert ANALYZER_EXTRA in str(caught.value)
