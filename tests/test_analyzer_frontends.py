"""Tests for the notebook front-ends (Plan 43 — 43.7): extracting embedded
SQL from a Databricks ``.py`` export and a Jupyter ``.ipynb`` notebook.
Pure Python, no sqlglot and no Spark needed — these functions only locate
and lexically split text.
"""
from __future__ import annotations

import json
import textwrap

from skifer.analyzer.frontends import (
    ExtractionResidue,
    extract_spark_sql_calls,
    split_ipynb_notebook,
    split_py_notebook,
    split_sql_text,
)


class TestSplitSqlText:
    def test_line_offset_shifts_the_reported_line(self):
        statements = split_sql_text("SELECT 1\n", "a.sql", line_offset=10)

        assert statements[0].line == 11


class TestExtractSparkSqlCalls:
    def test_a_literal_string_argument_is_extracted(self):
        source = 'df = spark.sql("SELECT 1 FROM t")\n'

        statements, residues = extract_spark_sql_calls(source, "nb.py")

        assert residues == []
        assert len(statements) == 1
        assert statements[0].text == "SELECT 1 FROM t"
        assert statements[0].line == 1

    def test_line_offset_is_applied_to_the_reported_line(self):
        source = 'df = spark.sql("SELECT 1")\n'

        statements, _residues = extract_spark_sql_calls(source, "nb.py", line_offset=5)

        assert statements[0].line == 6

    def test_an_fstring_argument_is_reported_as_a_residue_not_guessed_at(self):
        source = 'col = "x"\ndf = spark.sql(f"SELECT {col} FROM t")\n'

        statements, residues = extract_spark_sql_calls(source, "nb.py")

        assert statements == []
        assert len(residues) == 1
        assert isinstance(residues[0], ExtractionResidue)
        assert "f-string" in residues[0].reason
        assert residues[0].line == 2

    def test_a_variable_argument_is_reported_as_a_residue(self):
        source = "query = 'SELECT 1'\ndf = spark.sql(query)\n"

        statements, residues = extract_spark_sql_calls(source, "nb.py")

        assert statements == []
        assert "not a literal string" in residues[0].reason

    def test_a_keyword_argument_is_reported_as_a_residue(self):
        source = 'df = spark.sql(sqlQuery="SELECT 1")\n'

        statements, residues = extract_spark_sql_calls(source, "nb.py")

        assert statements == []
        assert "not a single positional argument" in residues[0].reason

    def test_a_call_on_a_different_object_is_not_matched(self):
        source = 'df = other.sql("SELECT 1")\n'

        statements, residues = extract_spark_sql_calls(source, "nb.py")

        assert statements == []
        assert residues == []

    def test_a_call_to_a_different_method_is_not_matched(self):
        source = 'df = spark.read.csv("x.csv")\n'

        statements, residues = extract_spark_sql_calls(source, "nb.py")

        assert statements == []
        assert residues == []

    def test_invalid_python_source_is_reported_not_raised(self):
        source = "def broken(:\n"

        statements, residues = extract_spark_sql_calls(source, "nb.py")

        assert statements == []
        assert len(residues) == 1
        assert "not valid Python" in residues[0].reason

    def test_multiple_calls_are_all_extracted_in_order(self):
        source = (
            'a = spark.sql("SELECT 1")\n'
            'b = spark.sql("SELECT 2")\n'
        )

        statements, _residues = extract_spark_sql_calls(source, "nb.py")

        assert [s.text for s in statements] == ["SELECT 1", "SELECT 2"]
        assert [s.line for s in statements] == [1, 2]


class TestSplitPyNotebook:
    def test_sql_magic_cell_is_extracted_with_correct_line(self, tmp_path):
        path = tmp_path / "nb.py"
        path.write_text(
            "# Databricks notebook source\n"
            "# MAGIC %sql\n"
            "# MAGIC CREATE TABLE silver.t AS\n"
            "# MAGIC SELECT id FROM bronze.t_raw\n"
        )

        statements, residues = split_py_notebook(path)

        assert residues == []
        assert len(statements) == 1
        assert statements[0].text == "CREATE TABLE silver.t AS\nSELECT id FROM bronze.t_raw"
        assert statements[0].line == 3  # the line right after the %sql directive

    def test_plain_python_cell_after_a_command_separator_is_scanned_for_spark_sql(self, tmp_path):
        path = tmp_path / "nb.py"
        path.write_text(
            "# Databricks notebook source\n"
            "# MAGIC %sql\n"
            "# MAGIC SELECT 1\n"
            "\n"
            "# COMMAND ----------\n"
            "\n"
            'df = spark.sql("SELECT 2")\n'
        )

        statements, _residues = split_py_notebook(path)

        assert [s.text for s in statements] == ["SELECT 1", "SELECT 2"]
        assert statements[1].line == 7

    def test_an_unsupported_magic_cell_is_skipped_not_reported(self, tmp_path):
        path = tmp_path / "nb.py"
        path.write_text(
            "# Databricks notebook source\n"
            "# MAGIC %md\n"
            "# MAGIC ### Just a header\n"
        )

        statements, residues = split_py_notebook(path)

        assert statements == []
        assert residues == []

    def test_an_fstring_spark_sql_call_in_a_plain_cell_is_a_residue(self, tmp_path):
        path = tmp_path / "nb.py"
        path.write_text(
            "# Databricks notebook source\n"
            'df = spark.sql(f"SELECT {x}")\n'
        )

        statements, residues = split_py_notebook(path)

        assert statements == []
        assert len(residues) == 1
        assert "f-string" in residues[0].reason

    def test_source_path_is_the_real_file_path(self, tmp_path):
        path = tmp_path / "nb.py"
        path.write_text(
            "# Databricks notebook source\n"
            "# MAGIC %sql\n"
            "# MAGIC SELECT 1\n"
        )

        statements, _residues = split_py_notebook(path)

        assert statements[0].source_path == str(path)


class TestSplitIpynbNotebook:
    def _write(self, tmp_path, cells):
        path = tmp_path / "nb.ipynb"
        path.write_text(json.dumps({"cells": cells, "nbformat": 4, "nbformat_minor": 5}))
        return path

    def test_sql_magic_cell_is_extracted(self, tmp_path):
        path = self._write(tmp_path, [
            {"cell_type": "code", "source": ["%%sql\n", "SELECT id FROM silver.t\n"]},
        ])

        statements, residues = split_ipynb_notebook(path)

        assert residues == []
        assert len(statements) == 1
        assert statements[0].text == "SELECT id FROM silver.t"

    def test_source_path_encodes_the_cell_index(self, tmp_path):
        path = self._write(tmp_path, [
            {"cell_type": "markdown", "source": ["# header\n"]},
            {"cell_type": "code", "source": ["%%sql\n", "SELECT 1\n"]},
        ])

        statements, _residues = split_ipynb_notebook(path)

        assert statements[0].source_path == f"{path}::cell:1"

    def test_markdown_cells_are_skipped(self, tmp_path):
        path = self._write(tmp_path, [
            {"cell_type": "markdown", "source": ["%%sql\nSELECT 1\n"]},
        ])

        statements, residues = split_ipynb_notebook(path)

        assert statements == []
        assert residues == []

    def test_plain_code_cell_extracts_spark_sql(self, tmp_path):
        path = self._write(tmp_path, [
            {"cell_type": "code", "source": ['df = spark.sql("SELECT 1")\n']},
        ])

        statements, _residues = split_ipynb_notebook(path)

        assert statements[0].text == "SELECT 1"

    def test_an_fstring_argument_in_a_code_cell_is_a_residue_naming_the_cell(self, tmp_path):
        path = self._write(tmp_path, [
            {"cell_type": "code", "source": ['df = spark.sql(f"SELECT {x}")\n']},
        ])

        statements, residues = split_ipynb_notebook(path)

        assert statements == []
        assert residues[0].source_path == f"{path}::cell:0"

    def test_empty_cell_is_skipped(self, tmp_path):
        path = self._write(tmp_path, [
            {"cell_type": "code", "source": []},
        ])

        statements, residues = split_ipynb_notebook(path)

        assert statements == []
        assert residues == []


class TestNoTableNameOrFilterValueEverLeaksIntoAResidue:
    """D7 discipline check: a residue names the construct, never anything
    this module could not prove safe to report back verbatim."""

    def test_residue_never_contains_literal_filter_text(self):
        source = textwrap.dedent(
            """
            customer_id = "secret_42"
            df = spark.sql(f"SELECT * FROM bronze.customers WHERE id = '{customer_id}'")
            """
        )

        _statements, residues = extract_spark_sql_calls(source, "nb.py")

        assert "secret_42" not in residues[0].reason
        assert "bronze.customers" not in residues[0].reason
