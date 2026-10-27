"""Tests for the project-analyzer parity runner (Plan 43 — 43.5).

``ParityReport``/``ColumnDifference``/``_row_hash`` are pure Python and run
without Spark. ``ParityRunner`` and ``compare_tables`` need a real engine and
two real tables — they use the ``spark`` fixture, the same local Delta
session as the rest of the Spark-backed suite (``tests/conftest.py``).
"""
from __future__ import annotations

import json

import pytest

from skifer.analyzer.parity import ColumnDifference, ParityReport, ParityRunner, _row_hash, compare_tables
from skifer.core.core import SkiferEngine
from skifer.core.schema_loader import parse_schema

CATALOG = "spark_catalog"
SCHEMA = "silver_parity_test"
COLUMNS = ["id", "name", "country"]
ROWS = [(1, "Alice", "FR"), (2, "Bob", "BE"), (3, "Chloe", "FR")]


class TestRowHash:
    def test_same_content_same_hash_regardless_of_list_identity(self):
        a = _row_hash([(1, "x"), (2, "y")])
        b = _row_hash([(1, "x"), (2, "y")])

        assert a == b
        assert a.startswith("sha256:v1:")

    def test_different_content_different_hash(self):
        assert _row_hash([(1, "x")]) != _row_hash([(1, "y")])

    def test_none_is_distinguishable_from_the_string_none(self):
        assert _row_hash([(None,)]) != _row_hash([("None",)])


class TestParityReportNeverLeaksAValue:
    def test_to_dict_contains_only_counts_names_and_booleans(self):
        report = ParityReport(
            draft_fqn="cat.silver.draft",
            legacy_fqn="cat.silver.legacy",
            key=("id",),
            draft_row_count=10,
            legacy_row_count=9,
            draft_columns=("id", "ssn"),
            legacy_columns=("id", "ssn"),
            type_mismatched_columns=(),
            draft_row_hash="sha256:v1:aaa",
            legacy_row_hash="sha256:v1:bbb",
            rows_only_in_draft=1,
            rows_only_in_legacy=0,
            column_differences=(ColumnDifference(column="ssn", differing_rows=3),),
        )

        serialized = json.dumps(report.to_dict(), sort_keys=True)

        assert "ssn" in serialized  # a column name is allowed
        assert report.identical is False
        # No field on the dataclass can hold a raw cell value in the first
        # place — only counts, hashes, booleans and column names exist.
        assert set(report.to_dict().keys()) == {
            "draft_fqn", "legacy_fqn", "key", "identical",
            "draft_row_count", "legacy_row_count", "row_count_matches",
            "draft_columns", "legacy_columns", "columns_match",
            "draft_only_columns", "legacy_only_columns", "type_mismatched_columns",
            "draft_row_hash", "legacy_row_hash", "row_hash_matches",
            "rows_only_in_draft", "rows_only_in_legacy", "column_differences",
        }

    def test_identical_report_round_trips_true(self):
        report = ParityReport(
            draft_fqn="cat.silver.draft",
            legacy_fqn="cat.silver.legacy",
            key=("id",),
            draft_row_count=3,
            legacy_row_count=3,
            draft_columns=("id",),
            legacy_columns=("id",),
            draft_row_hash="sha256:v1:same",
            legacy_row_hash="sha256:v1:same",
            rows_only_in_draft=0,
            rows_only_in_legacy=0,
        )

        assert report.identical is True
        assert report.to_dict()["identical"] is True


def _engine(spark, mocker):
    def mock_load_config(instance, config_path):
        instance.config = {"environments": {"test": {"is_production": False}}}
        instance.env = "test"
        instance.db = CATALOG

    mocker.patch.object(SkiferEngine, "_load_config_from_yaml", side_effect=mock_load_config, autospec=True)
    mocker.patch.object(SkiferEngine, "_get_clean_username", return_value="parity_test")
    return SkiferEngine(spark=spark, config_path="dummy/path/to/config.yaml")


def _draft_schema_dict(engine, *, filter_fr_only: bool) -> dict:
    payload = {
        "tables": [{"name": "bronze_customers_parity", "alias": "c"}],
        "keep_all_columns": True,
    }
    if filter_fr_only:
        payload["filter"] = ["country:equals:FR"]
    return parse_schema(json.dumps(payload), params=engine.default_params)


@pytest.fixture
def legacy_table(spark):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
    spark.createDataFrame(ROWS, COLUMNS).write.format("delta").mode("overwrite").saveAsTable(
        f"{CATALOG}.{SCHEMA}.legacy_customers"
    )
    spark.createDataFrame(ROWS, COLUMNS).createOrReplaceTempView("bronze_customers_parity")

    yield f"{CATALOG}.{SCHEMA}.legacy_customers"

    spark.catalog.dropTempView("bronze_customers_parity")
    spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{SCHEMA} CASCADE")


def test_parity_runner_reports_identical_for_a_faithful_draft(spark, mocker, legacy_table):
    engine = _engine(spark, mocker)
    draft = _draft_schema_dict(engine, filter_fr_only=False)

    report = ParityRunner(engine).run(draft, "silver", against=legacy_table, key=["id"])

    assert report.identical is True
    assert report.row_count_matches is True
    assert report.rows_only_in_draft == 0
    assert report.rows_only_in_legacy == 0
    assert report.column_differences == ()


def test_parity_runner_detects_a_wrongly_pushed_filter(spark, mocker, legacy_table):
    engine = _engine(spark, mocker)
    draft = _draft_schema_dict(engine, filter_fr_only=True)

    report = ParityRunner(engine).run(draft, "silver", against=legacy_table, key=["id"])

    assert report.identical is False
    assert report.draft_row_count == 2  # Alice + Chloe; Bob (BE) wrongly dropped
    assert report.legacy_row_count == 3
    assert report.rows_only_in_legacy == 1
    assert report.rows_only_in_draft == 0


def test_compare_tables_reports_schema_drift_without_a_key(spark, mocker, legacy_table):
    spark.createDataFrame([(1, "Alice")], ["id", "name"]).write.format("delta").mode("overwrite").saveAsTable(
        f"{CATALOG}.{SCHEMA}.draft_missing_column"
    )

    report = compare_tables(spark, f"{CATALOG}.{SCHEMA}.draft_missing_column", legacy_table)

    assert report.legacy_only_columns == ("country",)
    assert report.columns_match is False
    assert report.identical is False

    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.draft_missing_column")
