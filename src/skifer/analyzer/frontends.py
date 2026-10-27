"""SQL front-ends (Plan 43 — 43.0 for ``.sql`` files, 43.7 for notebooks).

Splits a ``.sql`` file, or the SQL embedded in a Databricks ``.py`` export or
a Jupyter ``.ipynb`` notebook, into individual statements, each carrying its
origin: the file path and the 1-indexed line of its first non-blank,
non-comment character (plus, for a notebook, its cell index, encoded as a
``path::cell:N`` ``source_path`` — the same ``::``-suffix convention 43.3's
splitter already uses for a CTE's own synthetic statement, so grouping "by
script" elsewhere in the analyzer keeps working unchanged: ``source_path``
before the first ``::`` is still the one real file on disk).

Deliberately independent of ``sqlglot`` for statement *boundaries* — that is
a lexical problem (top-level ``;``, string literals, comments), not a parsing
one, and the core package must stay importable without the optional
dependency even to report *where* a statement starts. The notebook front-ends
do need the standard library's own ``ast`` module to read a ``spark.sql(...)``
call's literal argument; that is not an optional dependency.

A ``spark.sql(...)`` call whose argument is not a single literal string
(an f-string, a variable, a concatenation, a second positional/keyword
argument) is never guessed at — guessing at a parameterized query would
either silently drop the parameterization or risk running something other
than what the notebook actually runs. It is reported as an
:class:`ExtractionResidue` instead, named with the construct and its exact
line, the same "report, never silently drop" discipline the rest of this
package applies to a residue it cannot map.
"""
from __future__ import annotations

import ast as _pyast
import json
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
    return split_sql_text(path.read_text(), str(path))


def split_sql_text(text: str, source_path: str, *, line_offset: int = 0) -> list[RawStatement]:
    """Same splitting as :func:`split_sql_file`, over SQL text already read
    from somewhere other than a plain ``.sql`` file (a notebook's ``%sql``
    cell). ``line_offset`` is the 0-indexed line *text* starts at within the
    real file, so a reported line is relative to that file, not the cell.
    """
    statements: list[RawStatement] = []
    segment_start = 0
    segment_start_line = 1 + line_offset
    line = 1 + line_offset
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


# ---------------------------------------------------------------------------
# Notebook front-ends (Plan 43 — 43.7)
# ---------------------------------------------------------------------------

_COMMAND_SEPARATOR = "# COMMAND ----------"
_MAGIC_PREFIX = "# MAGIC"


@dataclass(frozen=True)
class ExtractionResidue:
    """Something a notebook front-end could not safely turn into SQL text —
    reported, never silently dropped (same D7 discipline as a mapper residue,
    a different stage: this one is about reading the cell at all)."""

    source_path: str
    line: int
    reason: str

    def to_dict(self) -> dict:
        return {"source_path": self.source_path, "line": self.line, "reason": self.reason}


def extract_spark_sql_calls(
    source: str, source_path: str, *, line_offset: int = 0
) -> tuple[list[RawStatement], list[ExtractionResidue]]:
    """Find every ``spark.sql(...)`` call in *source* (plain Python) via the
    standard library's ``ast`` module and return its literal argument as a
    :class:`RawStatement`. A call whose argument is not a single literal
    string — an f-string, a variable, string concatenation, a keyword
    argument — is reported as an :class:`ExtractionResidue` instead of
    guessed at: this front-end never interpolates or evaluates anything.
    """
    try:
        tree = _pyast.parse(source)
    except SyntaxError as exc:
        return [], [ExtractionResidue(source_path, 1 + line_offset, f"cell is not valid Python: {exc}")]

    statements: list[RawStatement] = []
    residues: list[ExtractionResidue] = []

    for node in _pyast.walk(tree):
        if not isinstance(node, _pyast.Call):
            continue
        func = node.func
        if not (isinstance(func, _pyast.Attribute) and func.attr == "sql"):
            continue
        if not (isinstance(func.value, _pyast.Name) and func.value.id == "spark"):
            continue

        line = node.lineno + line_offset
        if node.keywords or len(node.args) != 1:
            residues.append(ExtractionResidue(source_path, line, "spark.sql(...) call is not a single positional argument"))
            continue

        (arg,) = node.args
        if isinstance(arg, _pyast.Constant) and isinstance(arg.value, str):
            statements.append(RawStatement(source_path=source_path, line=line, text=arg.value.strip()))
        elif isinstance(arg, _pyast.JoinedStr):
            residues.append(ExtractionResidue(source_path, line, "spark.sql(...) argument is an f-string, never interpolated"))
        else:
            residues.append(ExtractionResidue(source_path, line, "spark.sql(...) argument is not a literal string"))

    return statements, residues


def _is_magic_line(line: str) -> bool:
    return line.strip() == "" or line.startswith(_MAGIC_PREFIX)


def _strip_magic_prefix(line: str) -> str:
    stripped = line[len(_MAGIC_PREFIX):]
    return stripped[1:] if stripped.startswith(" ") else stripped


def split_py_notebook(path: str | Path) -> tuple[list[RawStatement], list[ExtractionResidue]]:
    """Split a Databricks ``.py`` notebook export into its cells (separated
    by ``# COMMAND ----------``) and extract SQL from each: a ``%sql`` magic
    cell (every line prefixed ``# MAGIC``) via :func:`split_sql_text`, a
    plain Python cell via :func:`extract_spark_sql_calls`. Any other magic
    (``%md``, ``%scala``, ``%r``, ``%sh``, ``%run``, …) has no embedded SQL
    this front-end extracts, so it is skipped, not reported — D7 residues are
    for a recurring pattern this package could not translate, not every cell
    in a different language.
    """
    path = Path(path)
    lines = path.read_text().splitlines(keepends=True)
    source_path = str(path)

    statements: list[RawStatement] = []
    residues: list[ExtractionResidue] = []

    # The export's own first line marks the file as a notebook; it is not
    # part of cell 1's content, whose magic-cell detection would otherwise
    # see a non-"# MAGIC" line and misclassify the whole cell as plain Python.
    cell_start = 1 if lines and lines[0].rstrip("\n") == "# Databricks notebook source" else 0
    for idx in range(cell_start, len(lines) + 1):
        at_boundary = idx == len(lines) or lines[idx].rstrip("\n") == _COMMAND_SEPARATOR
        if not at_boundary:
            continue
        cell_lines = lines[cell_start:idx]
        cell_line_offset = cell_start  # 0-indexed line the cell starts at
        if cell_lines and all(_is_magic_line(line.rstrip("\n")) for line in cell_lines):
            magic_lines = [_strip_magic_prefix(line.rstrip("\n")) for line in cell_lines]
            directive_idx = next((i for i, lt in enumerate(magic_lines) if lt.strip()), None)
            directive = magic_lines[directive_idx].strip() if directive_idx is not None else ""
            # Drop the directive line itself from the extracted content, but keep its
            # line slot (as an empty line) so every later line keeps its real line number.
            body_lines = list(magic_lines)
            if directive_idx is not None:
                body_lines[directive_idx] = ""

            if directive == "%sql":
                sql_text = "\n".join(body_lines)
                statements.extend(split_sql_text(sql_text, source_path, line_offset=cell_line_offset))
            elif directive in ("%python", ""):
                python_text = "\n".join(body_lines)
                cell_statements, cell_residues = extract_spark_sql_calls(
                    python_text, source_path, line_offset=cell_line_offset
                )
                statements.extend(cell_statements)
                residues.extend(cell_residues)
            # Any other magic (%md, %scala, %r, %sh, %run, ...): out of scope, skipped.
        elif cell_lines:
            python_text = "".join(cell_lines)
            cell_statements, cell_residues = extract_spark_sql_calls(
                python_text, source_path, line_offset=cell_line_offset
            )
            statements.extend(cell_statements)
            residues.extend(cell_residues)
        cell_start = idx + 1

    return statements, residues


def split_ipynb_notebook(path: str | Path) -> tuple[list[RawStatement], list[ExtractionResidue]]:
    """Split a Jupyter ``.ipynb`` notebook's code cells, extracting SQL from
    a ``%%sql`` cell-magic cell via :func:`split_sql_text` and from a plain
    code cell via :func:`extract_spark_sql_calls`. Each statement's
    ``source_path`` is ``<path>::cell:<index>`` (0-indexed, matching the
    notebook's own cell order) — the same ``::``-suffix convention 43.3 uses
    for a synthetic statement, so "distinct scripts" counting elsewhere in
    the analyzer still groups every cell under the one real notebook file.
    A notebook has no serialized line numbers spanning cells, so a reported
    ``line`` is relative to that cell's own source, not the file.
    """
    path = Path(path)
    notebook = json.loads(path.read_text())
    source_path = str(path)

    statements: list[RawStatement] = []
    residues: list[ExtractionResidue] = []

    for idx, cell in enumerate(notebook.get("cells", [])):
        if cell.get("cell_type") != "code":
            continue
        cell_source = "".join(cell.get("source", []))
        if not cell_source.strip():
            continue
        cell_path = f"{source_path}::cell:{idx}"

        first_line, _, rest = cell_source.partition("\n")
        if first_line.strip() == "%%sql":
            statements.extend(split_sql_text(rest, cell_path, line_offset=1))
        else:
            cell_statements, cell_residues = extract_spark_sql_calls(cell_source, cell_path)
            statements.extend(cell_statements)
            residues.extend(cell_residues)

    return statements, residues
