"""SQL front-end (Plan 43 — 43.0).

Splits a ``.sql`` file into individual statements, each carrying the file path
and the 1-indexed line of its first non-blank, non-comment character. This is
the only front-end today — a notebook front-end (``.py``/``.ipynb`` cells with
embedded SQL) is future scope (43.7).

Deliberately independent of ``sqlglot``: locating statement boundaries is a
lexical problem (top-level ``;``, string literals, comments), not a parsing
one, and the core package must stay importable without the optional
dependency even to report *where* a statement starts.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class RawStatement:
    """One SQL statement as lexical text, not yet parsed."""

    source_path: str
    line: int
    text: str


def _strip_leading_trivia(text: str) -> tuple[int, str]:
    """Strip leading whitespace and comments; return (newlines consumed, remainder).

    A statement's raw segment starts right after the previous statement's
    ``;`` (or at the top of the file), so it may begin with blank lines or a
    comment explaining what follows — neither is part of the statement, and
    both would otherwise throw off the reported line number.
    """
    newlines = 0
    i = 0
    n = len(text)
    while i < n:
        ch = text[i]
        if ch.isspace():
            if ch == "\n":
                newlines += 1
            i += 1
            continue
        if text[i : i + 2] == "--":
            end = text.find("\n", i)
            if end == -1:
                i = n
            else:
                newlines += 1
                i = end + 1
            continue
        if text[i : i + 2] == "/*":
            end = text.find("*/", i + 2)
            if end == -1:
                i = n
            else:
                newlines += text.count("\n", i, end + 2)
                i = end + 2
            continue
        break
    return newlines, text[i:]


def split_sql_file(path: str | Path) -> list[RawStatement]:
    """Split a ``.sql`` file into statements on top-level ``;``.

    Tracks single-quoted string literals (with ``\\`` as an escape, matching
    Spark/Databricks SQL — unlike ANSI SQL, its grammar does not treat ``''``
    as an escaped quote; two adjacent quoted literals are concatenated
    instead), ``--`` line comments and ``/* ... */`` block comments, so a
    ``;`` inside any of those is not treated as a statement boundary. Does
    not special-case backtick- or double-quoted identifiers containing ``;``
    or a comment marker — standard SQL identifiers don't need to.

    Blank or comment-only statements (e.g. trailing text after the last
    ``;``) are dropped.
    """
    path = Path(path)
    text = path.read_text()
    source_path = str(path)

    statements: list[RawStatement] = []
    segment_start = 0
    segment_start_line = 1
    line = 1
    state = "default"  # default | single_quote | line_comment | block_comment
    i = 0
    n = len(text)

    while i < n:
        ch = text[i]
        if ch == "\n":
            line += 1

        if state == "default":
            if ch == "'":
                state = "single_quote"
            elif text[i : i + 2] == "--":
                state = "line_comment"
            elif text[i : i + 2] == "/*":
                state = "block_comment"
            elif ch == ";":
                _append_statement(statements, source_path, segment_start_line, text[segment_start:i])
                segment_start = i + 1
                segment_start_line = line
        elif state == "single_quote":
            if ch == "\\":
                i += 1  # backslash escape — the next char can't close the literal
            elif ch == "'":
                state = "default"
        elif state == "line_comment":
            if ch == "\n":
                state = "default"
        elif state == "block_comment":
            if text[i : i + 2] == "*/":
                state = "default"
                i += 1

        i += 1

    _append_statement(statements, source_path, segment_start_line, text[segment_start:])
    return statements


def _append_statement(statements: list[RawStatement], source_path: str, start_line: int, raw_text: str) -> None:
    newlines, remainder = _strip_leading_trivia(raw_text)
    stripped = remainder.rstrip()
    if not stripped:
        return
    statements.append(
        RawStatement(
            source_path=source_path,
            line=start_line + newlines,
            text=stripped,
        )
    )
