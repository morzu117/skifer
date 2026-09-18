from __future__ import annotations

from skifer.core.dialect import quote_fqn
from skifer.core.snapshot_preflight import (
    _null_key_count_sql,
    SnapshotPreflightInputs,
    build_snapshot_preflight_report,
    collect_snapshot_preflight_inputs,
)


def _timestamp_materialization(**overrides):
    materialization = {
        "type": "snapshot",
        "strategy": "timestamp",
        "unique_key": ["order_id"],
        "updated_at": "modified_at",
        "on_missing": "close",
        "max_closed_ratio": 0.2,
        "on_late_arrival": "refuse",
    }
    materialization.update(overrides)
    return materialization


def _single_finding(materialization, inputs):
    report = build_snapshot_preflight_report(materialization, inputs)
    assert report.safe_to_apply is False
    assert len(report.findings) == 1
    return report.findings[0]


def test_clean_snapshot_preflight_report_is_safe_to_apply():
    report = build_snapshot_preflight_report(
        _timestamp_materialization(),
        SnapshotPreflightInputs(
            source_columns=("order_id", "modified_at", "status"),
            target_columns=(
                "order_id",
                "modified_at",
                "status",
                "valid_from",
                "valid_to",
            ),
        ),
    )

    assert report.safe_to_apply is True
    assert report.findings == ()


def test_duplicate_unique_key_finding_suggests_dedup_yaml():
    finding = _single_finding(
        _timestamp_materialization(),
        SnapshotPreflightInputs(unique_key_duplicate_count=412, target_exists=False),
    )

    assert finding.kind == "unique_key_not_unique"
    assert "'unique_key' [order_id] is not unique" in finding.message
    assert "412 keys" in finding.message
    assert "drop_duplicates_on: [order_id]" in finding.suggested_yaml
    assert 'qualify: {order_by: "modified_at DESC"}' in finding.suggested_yaml


def test_missing_rows_finding_suggests_ignore_or_explicit_threshold_yaml():
    finding = _single_finding(
        _timestamp_materialization(),
        SnapshotPreflightInputs(
            target_current_count=10,
            missing_current_count=3,
            target_exists=False,
        ),
    )

    assert finding.kind == "missing_rows_exceed_max_closed_ratio"
    assert "'max_closed_ratio'" in finding.message
    assert "3 of 10" in finding.message
    assert "on_missing: ignore" in finding.suggested_yaml
    assert "max_closed_ratio: 0.3" in finding.suggested_yaml


def test_updated_at_null_finding_suggests_drop_nulls_yaml():
    finding = _single_finding(
        _timestamp_materialization(),
        SnapshotPreflightInputs(updated_at_null_count=7, target_exists=False),
    )

    assert finding.kind == "updated_at_null"
    assert "'updated_at' column 'modified_at' contains NULL" in finding.message
    assert "7 rows" in finding.message
    assert "drop_nulls_in: [modified_at]" in finding.suggested_yaml


def test_late_arrival_finding_suggests_policy_yaml():
    finding = _single_finding(
        _timestamp_materialization(),
        SnapshotPreflightInputs(late_arrival_count=2, target_exists=False),
    )

    assert finding.kind == "late_arrival"
    assert "older than the current version" in finding.message
    assert "2 rows" in finding.message
    assert "on_late_arrival: ignore" in finding.suggested_yaml


def test_column_drift_finding_suggests_projection_yaml():
    finding = _single_finding(
        _timestamp_materialization(),
        SnapshotPreflightInputs(
            source_columns=("order_id", "status"),
            target_columns=("order_id", "valid_from", "valid_to"),
        ),
    )

    assert finding.kind == "column_drift"
    assert "compiled source columns and target columns diverge" in finding.message
    assert "source-only columns: ['status']" in finding.message
    assert "select_final:" in finding.suggested_yaml
    assert "valid_from, valid_to" in finding.suggested_yaml


def test_preflight_messages_do_not_include_data_values():
    duckdb = __import__("pytest").importorskip("duckdb")
    from skifer.core.adapters.duckdb import DuckDBAdapter

    connection = duckdb.connect(database=":memory:")
    adapter = DuckDBAdapter(connection)
    adapter.execute_sql("CREATE SCHEMA source")
    adapter.execute_sql(
        "CREATE TABLE source.orders("
        "order_id VARCHAR, modified_at TIMESTAMP, status VARCHAR)"
    )
    adapter.execute_sql(
        "INSERT INTO source.orders VALUES "
        "('SECRET-ORDER-42', TIMESTAMP '2026-01-01 00:00:00', 'ready'), "
        "('SECRET-ORDER-42', TIMESTAMP '2026-01-02 00:00:00', 'done')"
    )

    try:
        inputs = collect_snapshot_preflight_inputs(
            adapter,
            source_relation=quote_fqn("source.orders", target="duckdb"),
            target_relation=quote_fqn("gold.orders", target="duckdb"),
            materialization=_timestamp_materialization(),
            target_exists=False,
        )
        report = build_snapshot_preflight_report(
            _timestamp_materialization(),
            inputs,
        )
    finally:
        connection.close()

    assert len(report.findings) == 1
    rendered = report.findings[0].render()
    assert "SECRET-ORDER-42" not in rendered
    assert "order_id" in rendered


def test_null_unique_key_is_refused_where_the_duplicate_check_sees_nothing():
    """A single NULL key passes the duplicate check and never matches its own row.

    Measured on DuckDB: ``GROUP BY`` yields one group of one, so the duplicate
    check reports nothing; and ``t.key = s.key`` is never true for NULL, so the
    row joins to no version of itself. The write would insert it again on every
    run, growing the table silently. Detecting it needs its own check.
    """
    materialization = {
        "type": "snapshot",
        "strategy": "timestamp",
        "unique_key": ["order_id"],
        "updated_at": "modified_at",
        "on_missing": "ignore",
    }
    inputs = SnapshotPreflightInputs(
        unique_key_null_count=3,
        unique_key_duplicate_count=0,
        target_exists=False,
    )

    report = build_snapshot_preflight_report(materialization, inputs)

    assert not report.safe_to_apply
    assert [finding.kind for finding in report.findings] == ["unique_key_null"]
    rendered = report.findings[0].render()
    assert "3 rows carry a NULL key" in rendered
    assert "drop_nulls_in: [order_id]" in rendered


def test_null_key_count_sql_covers_every_key_column():
    sql = _null_key_count_sql(
        '"source"."orders"', unique_key=["order_id", "src"], adapter_name="duckdb"
    )
    assert 's."order_id" IS NULL OR s."src" IS NULL' in sql
