"""
tests/test_observability.py — Tests for the Data Observability module.

All tests mock the backend — no live Spark cluster required.
Phases covered:
  Phase 1 — ContractExtractor, DataContract.evaluate(), DataMonitor end-to-end
  Phase 2 — Configurable checks (FreshnessCheck, VolumeCheck, SchemaDriftCheck, CustomSqlCheck)
  Phase 3 — SqliteHistoryStore (in-memory, no mock)
  Phase 5 — MonitorReporter
"""
from __future__ import annotations

import json
import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from skifer.core.schema_loader import parse_schema
from skifer.observability.checks import (
    NullCheck,
    UniqueCheck,
    TypeCheck,
    FilterInvariantCheck,
    FreshnessCheck,
    LoadFreshnessCheck,
    VolumeCheck,
    VolumeVariationCheck,
    SchemaDriftCheck,
    CustomSqlCheck,
    CheckResult,
    CheckStatus,
    ContractScope,
    DataQualityError,
)
from skifer.observability.contracts import ContractExtractor
from skifer.observability.monitor import DataMonitor, MonitorReport


# ---------------------------------------------------------------------------
# Helpers — FakeBackend
# ---------------------------------------------------------------------------

class FakeRow(dict):
    """Dict subclass that also supports attribute access (like PySpark Row)."""
    def __getattr__(self, item):
        try:
            return self[item]
        except KeyError:
            raise AttributeError(item)


class FakeResult:
    """A fake DataFrame-like object."""
    def __init__(self, rows: list[dict]):
        self._rows = [FakeRow(r) for r in rows]

    def collect(self):
        return self._rows


class FakeBackend:
    """
    Minimal backend for testing — routes SQL queries to pre-configured results.
    If sql_override is provided, it's called with the query string and must return
    a FakeResult. Otherwise, default_result is returned for all queries.
    """
    def __init__(self, default_rows: list[dict] | None = None, sql_override=None):
        self._default_rows = default_rows or []
        self._sql_override = sql_override
        self.queries: list[str] = []

    def sql(self, query: str) -> FakeResult:
        self.queries.append(query)
        if self._sql_override:
            return FakeResult(self._sql_override(query))
        return FakeResult(self._default_rows)


# ---------------------------------------------------------------------------
# Phase 1 — ContractExtractor
# ---------------------------------------------------------------------------

class TestContractExtractor:

    def test_extract_null_checks(self):
        schema = parse_schema("""
tables:
  - name: silver.orders
    quality_checks:
      drop_nulls_in: [amount, customer_id]
""")
        contracts = ContractExtractor().extract(schema)
        null_checks = [c for c in contracts if isinstance(c, NullCheck)]
        assert len(null_checks) == 2
        assert {c.column for c in null_checks} == {"amount", "customer_id"}
        assert all(c.severity == "critical" for c in null_checks)

    def test_extract_unique_check(self):
        schema = parse_schema("""
tables:
  - name: silver.orders
    quality_checks:
      drop_duplicates_on: [order_id]
""")
        contracts = ContractExtractor().extract(schema)
        unique = [c for c in contracts if isinstance(c, UniqueCheck)]
        assert len(unique) == 1
        assert unique[0].columns == ["order_id"]
        assert unique[0].severity == "critical"

    def test_extract_filter_invariant(self):
        schema = parse_schema("""
tables:
  - name: silver.orders
    filter:
      - status:in:ACTIVE,PENDING
""")
        contracts = ContractExtractor().extract(schema)
        filters = [c for c in contracts if isinstance(c, FilterInvariantCheck)]
        assert len(filters) == 1
        assert filters[0].operator == "in"
        assert filters[0].severity == "warning"

    def test_extract_filter_is_not_null(self):
        schema = parse_schema("""
tables:
  - name: silver.orders
    filter:
      - customer_id:is_not_null
""")
        contracts = ContractExtractor().extract(schema)
        filters = [c for c in contracts if isinstance(c, FilterInvariantCheck)]
        assert len(filters) == 1
        assert filters[0].column == "customer_id"
        assert filters[0].operator == "is_not_null"

    def test_extract_type_check_from_cast(self):
        schema = parse_schema("""
tables:
  - name: silver.orders
select_final:
  - [amount, amount_eur, [cast:double]]
""")
        contracts = ContractExtractor().extract(schema)
        type_checks = [c for c in contracts if isinstance(c, TypeCheck)]
        assert any(c.column == "amount_eur" and c.expected_type == "double" for c in type_checks)

    def test_extract_type_check_from_dict_select_final(self):
        schema = parse_schema("""
tables:
  - name: silver.orders
select_final:
  - source: amount
    target: amount_eur
    ops:
      - cast:double
""")
        contracts = ContractExtractor().extract(schema)
        type_checks = [c for c in contracts if isinstance(c, TypeCheck)]
        assert any(c.column == "amount_eur" and c.expected_type == "double" for c in type_checks)

    def test_extract_no_contracts_for_empty_schema(self):
        schema = parse_schema("tables:\n  - name: silver.orders\n")
        contracts = ContractExtractor().extract(schema)
        assert contracts == []

    def test_extract_multiple_table_null_checks(self):
        schema = parse_schema("""
tables:
  - name: silver.orders
    quality_checks:
      drop_nulls_in: [amount]
  - name: silver.customers
    quality_checks:
      drop_nulls_in: [email]
""")
        contracts = ContractExtractor().extract(schema)
        null_checks = [c for c in contracts if isinstance(c, NullCheck)]
        assert len(null_checks) == 2
        tables = {c.table for c in null_checks}
        assert "silver.orders" in tables
        assert "silver.customers" in tables


# ---------------------------------------------------------------------------
# Phase 1 — DataContract.evaluate() with FakeBackend
# ---------------------------------------------------------------------------

class TestNullCheck:

    def test_null_check_passes(self):
        backend = FakeBackend(default_rows=[{"null_count": 0}])
        check = NullCheck(table="silver.orders", column="amount", severity="critical")
        result = check.evaluate(backend, "silver.orders")
        assert result.passed
        assert result.actual_value == 0
        assert result.severity == "critical"

    def test_null_check_fails(self):
        backend = FakeBackend(default_rows=[{"null_count": 42}])
        check = NullCheck(table="silver.orders", column="amount", severity="critical")
        result = check.evaluate(backend, "silver.orders")
        assert not result.passed
        assert result.actual_value == 42
        assert result.severity == "critical"
        assert "42" in result.message

    def test_null_check_query_contains_column(self):
        backend = FakeBackend(default_rows=[{"null_count": 0}])
        check = NullCheck(table="silver.orders", column="customer_id", severity="warning")
        check.evaluate(backend, "silver.orders")
        assert "customer_id" in backend.queries[0]
        assert "IS NULL" in backend.queries[0]


class TestUniqueCheck:

    def test_unique_check_passes(self):
        backend = FakeBackend(default_rows=[{"total": 100, "distinct_count": 100}])
        check = UniqueCheck(table="silver.orders", columns=["order_id"], severity="critical")
        result = check.evaluate(backend, "silver.orders")
        assert result.passed
        assert result.actual_value == 0  # 0 duplicates

    def test_unique_check_fails(self):
        backend = FakeBackend(default_rows=[{"total": 1000, "distinct_count": 995}])
        check = UniqueCheck(table="silver.orders", columns=["order_id"], severity="critical")
        result = check.evaluate(backend, "silver.orders")
        assert not result.passed
        assert result.actual_value == 5  # 5 duplicates
        assert "5" in result.message

    def test_unique_check_composite_key(self):
        backend = FakeBackend(default_rows=[{"total": 50, "distinct_count": 50}])
        check = UniqueCheck(table="silver.orders", columns=["order_id", "line_id"], severity="critical")
        result = check.evaluate(backend, "silver.orders")
        assert result.passed
        assert "order_id, line_id" in backend.queries[0]


class TestTypeCheck:

    def test_type_check_passes(self):
        def sql_override(query):
            return [
                {"col_name": "amount_eur", "data_type": "double"},
                {"col_name": "id", "data_type": "bigint"},
            ]
        backend = FakeBackend(sql_override=sql_override)
        check = TypeCheck(table="silver.orders", column="amount_eur", expected_type="double")
        result = check.evaluate(backend, "silver.orders")
        assert result.passed

    def test_type_check_fails(self):
        def sql_override(query):
            return [
                {"col_name": "amount_eur", "data_type": "string"},
            ]
        backend = FakeBackend(sql_override=sql_override)
        check = TypeCheck(table="silver.orders", column="amount_eur", expected_type="double")
        result = check.evaluate(backend, "silver.orders")
        assert not result.passed
        assert "string" in result.message

    def test_type_check_column_not_found(self):
        backend = FakeBackend(default_rows=[{"col_name": "other_col", "data_type": "string"}])
        check = TypeCheck(table="silver.orders", column="missing_col", expected_type="double")
        result = check.evaluate(backend, "silver.orders")
        assert not result.passed


class TestFilterInvariantCheck:

    def test_filter_equals_passes(self):
        backend = FakeBackend(default_rows=[{"violation_count": 0}])
        check = FilterInvariantCheck(
            table="silver.orders", column="region", operator="equals", value="EMEA"
        )
        result = check.evaluate(backend, "silver.orders")
        assert result.passed

    def test_filter_in_fails(self):
        backend = FakeBackend(default_rows=[{"violation_count": 7}])
        check = FilterInvariantCheck(
            table="silver.orders", column="status", operator="in",
            value=["ACTIVE", "PENDING"], severity="warning"
        )
        result = check.evaluate(backend, "silver.orders")
        assert not result.passed
        assert result.actual_value == 7

    def test_filter_is_not_null_passes(self):
        backend = FakeBackend(default_rows=[{"violation_count": 0}])
        check = FilterInvariantCheck(
            table="silver.orders", column="customer_id", operator="is_not_null"
        )
        result = check.evaluate(backend, "silver.orders")
        assert result.passed
        assert "IS NULL" in backend.queries[0]

    def test_filter_unsupported_operator_skipped(self):
        backend = FakeBackend(default_rows=[])
        check = FilterInvariantCheck(
            table="silver.orders", column="col", operator="sql", value="col > 0"
        )
        result = check.evaluate(backend, "silver.orders")
        # Unsupported operator is explicitly non-passing, never a false PASS.
        assert result.status is CheckStatus.SKIPPED
        assert not result.passed
        assert "skipped" in result.message.lower()


class TestTimestamps:
    def test_checkresult_timestamp_is_utc(self):
        result = CheckResult(
            contract=NullCheck(table="silver.orders", column="id"),
            passed=True,
            actual_value=0,
            expected_value=0,
            message="ok",
            severity="warning",
        )
        assert result.timestamp.tzinfo is not None

    def test_monitorreport_timestamp_is_utc(self):
        report = MonitorReport(table="silver.orders", results=[])
        assert report.timestamp.tzinfo is not None


class TestCheckStatusesAndViolationPredicates:
    def test_legacy_passed_constructor_maps_to_status(self):
        result = CheckResult(
            contract=NullCheck(table="silver.orders", column="id"),
            passed=True,
            actual_value=0,
            expected_value=0,
            message="ok",
            severity="warning",
        )
        assert result.status is CheckStatus.PASS
        assert result.passed is True

    def test_row_contracts_expose_safely_quoted_violation_predicates(self):
        null_check = NullCheck(table="silver.orders", column="customer`id")
        filter_check = FilterInvariantCheck(
            table="silver.orders", column="status", operator="equals", value="O'Reilly"
        )

        assert null_check.scope is ContractScope.ROW
        assert null_check.violation_predicate() == "`customer``id` IS NULL"
        assert filter_check.scope is ContractScope.ROW
        assert "O''Reilly" in filter_check.violation_predicate()
        assert filter_check.violation_predicate().startswith("NOT (")
        assert UniqueCheck(table="silver.orders", columns=["id"]).scope is ContractScope.DATASET
        assert UniqueCheck(table="silver.orders", columns=["id"]).violation_predicate() is None

    def test_backend_exception_is_distinct_error_and_blocks_critical(self):
        class BrokenBackend:
            def sql(self, query):
                raise RuntimeError("backend unavailable")

        report = DataMonitor(BrokenBackend()).check_table(
            "silver.orders", [NullCheck(table="silver.orders", column="id", severity="critical")]
        )

        assert report.results[0].status is CheckStatus.ERROR
        assert report.has_critical_failures()
        assert report.summary()["status_counts"]["ERROR"] == 1

    def test_reporter_renders_non_evaluation_statuses(self):
        from skifer.observability.reporter import MonitorReporter

        report = MonitorReport(
            table="silver.orders",
            results=[
                CheckResult(
                    contract=NullCheck(table="silver.orders", column="id"),
                    status=CheckStatus.SKIPPED,
                    message="unsupported",
                    severity="warning",
                )
            ],
        )

        assert "SKIPPED" in MonitorReporter().to_text(report)


class TestVolumeVariationHydration:
    def test_check_from_schema_uses_history_previous_count(self):
        backend = FakeBackend(default_rows=[{"row_count": 120}])
        previous_report = MonitorReport(
            table="silver.orders",
            results=[
                CheckResult(
                    contract=VolumeCheck(table="silver.orders", min_rows=None, max_rows=None),
                    passed=True,
                    actual_value=100,
                    expected_value="unbounded",
                    message="ok",
                    severity="warning",
                )
            ],
        )

        class History:
            def get_latest(self, table):
                return previous_report

            def store(self, report):
                self.report = report

        monitor = DataMonitor(backend, history_store=History())
        report = monitor.check_table(
            "silver.orders",
            [VolumeVariationCheck(table="silver.orders", variation_threshold=0.3)],
        )
        assert report.results[0].passed is True


# ---------------------------------------------------------------------------
# Phase 1 — DataMonitor end-to-end
# ---------------------------------------------------------------------------

class TestDataMonitor:

    def test_monitor_returns_report(self):
        backend = FakeBackend(default_rows=[{"null_count": 0}])
        schema = parse_schema("""
tables:
  - name: silver.orders
    quality_checks:
      drop_nulls_in: [amount]
""")
        monitor = DataMonitor(backend=backend)
        report = monitor.check_from_schema("silver.orders", schema)
        assert isinstance(report, MonitorReport)
        assert report.table == "silver.orders"

    def test_monitor_all_pass_no_critical_failures(self):
        backend = FakeBackend(default_rows=[{"null_count": 0}])
        schema = parse_schema("""
tables:
  - name: silver.orders
    quality_checks:
      drop_nulls_in: [amount]
""")
        monitor = DataMonitor(backend=backend)
        report = monitor.check_from_schema("silver.orders", schema)
        assert not report.has_critical_failures()
        assert report.failures() == []
        assert report.summary()["status"] == "PASS"

    def test_monitor_critical_failure_detected(self):
        backend = FakeBackend(default_rows=[{"null_count": 10}])
        schema = parse_schema("""
tables:
  - name: silver.orders
    quality_checks:
      drop_nulls_in: [amount]
""")
        monitor = DataMonitor(backend=backend)
        report = monitor.check_from_schema("silver.orders", schema)
        assert report.has_critical_failures()
        assert len(report.failures()) == 1
        assert report.summary()["status"] == "CRITICAL"

    def test_monitor_raises_on_critical_failure(self):
        backend = FakeBackend(default_rows=[{"null_count": 10}])
        schema = parse_schema("""
tables:
  - name: silver.orders
    quality_checks:
      drop_nulls_in: [amount]
""")
        monitor = DataMonitor(backend=backend)
        with pytest.raises(DataQualityError) as exc_info:
            monitor.check_from_schema("silver.orders", schema, raise_on_critical=True)
        assert "amount" in str(exc_info.value)

    def test_monitor_does_not_raise_without_flag(self):
        backend = FakeBackend(default_rows=[{"null_count": 10}])
        schema = parse_schema("""
tables:
  - name: silver.orders
    quality_checks:
      drop_nulls_in: [amount]
""")
        monitor = DataMonitor(backend=backend)
        # raise_on_critical defaults to False — must not raise
        report = monitor.check_from_schema("silver.orders", schema)
        assert report.has_critical_failures()

    def test_monitor_warning_failure_does_not_raise(self):
        backend = FakeBackend(default_rows=[{"violation_count": 5}])
        schema = parse_schema("""
tables:
  - name: silver.orders
    filter:
      - status:in:ACTIVE,PENDING
""")
        monitor = DataMonitor(backend=backend)
        report = monitor.check_from_schema("silver.orders", schema, raise_on_critical=True)
        # Warning failures must NOT raise DataQualityError
        assert not report.has_critical_failures()
        assert len(report.failures()) == 1
        assert report.summary()["status"] == "WARN"

    def test_monitor_history_store_called(self):
        backend = FakeBackend(default_rows=[{"null_count": 0}])
        history = MagicMock()
        schema = parse_schema("""
tables:
  - name: silver.orders
    quality_checks:
      drop_nulls_in: [amount]
""")
        monitor = DataMonitor(backend=backend, history_store=history)
        monitor.check_from_schema("silver.orders", schema)
        history.store.assert_called_once()

    def test_monitor_no_contracts_empty_report(self):
        backend = FakeBackend(default_rows=[])
        schema = parse_schema("tables:\n  - name: silver.orders\n")
        monitor = DataMonitor(backend=backend)
        report = monitor.check_from_schema("silver.orders", schema)
        assert report.results == []
        assert report.summary()["status"] == "PASS"


# ---------------------------------------------------------------------------
# Phase 2 — Configurable checks
# ---------------------------------------------------------------------------

class TestFreshnessCheck:

    def test_fresh_table_passes(self):
        recent = (datetime.now() - timedelta(minutes=30)).isoformat()
        backend = FakeBackend(default_rows=[{"max_ts": recent}])
        check = FreshnessCheck(table="silver.orders", timestamp_column="updated_at", max_delay="2h")
        result = check.evaluate(backend, "silver.orders")
        assert result.passed

    def test_stale_table_fails(self):
        old = (datetime.now() - timedelta(hours=5)).isoformat()
        backend = FakeBackend(default_rows=[{"max_ts": old}])
        check = FreshnessCheck(table="silver.orders", timestamp_column="updated_at", max_delay="2h")
        result = check.evaluate(backend, "silver.orders")
        assert not result.passed
        assert "FAIL" in result.message

    def test_null_timestamp_fails(self):
        backend = FakeBackend(default_rows=[{"max_ts": None}])
        check = FreshnessCheck(table="silver.orders", timestamp_column="updated_at", max_delay="1h")
        result = check.evaluate(backend, "silver.orders")
        assert not result.passed
        assert "NULL" in result.message

    def test_parse_delay_units(self):
        assert FreshnessCheck._parse_delay("2h") == timedelta(hours=2)
        assert FreshnessCheck._parse_delay("30m") == timedelta(minutes=30)
        assert FreshnessCheck._parse_delay("1d") == timedelta(days=1)
        assert FreshnessCheck._parse_delay("3600s") == timedelta(seconds=3600)

    def test_parse_delay_invalid_raises(self):
        with pytest.raises(ValueError):
            FreshnessCheck._parse_delay("2w")

    def test_future_event_time_is_error(self):
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        backend = FakeBackend(default_rows=[{"max_ts": "2026-01-02T00:00:00+00:00"}])
        result = FreshnessCheck(table="t", timestamp_column="updated_at", clock=lambda: now).evaluate(backend, "t")
        assert result.status is CheckStatus.ERROR

    def test_load_freshness_uses_promotion_not_event_column(self):
        class Store:
            def get_latest_promoted(self, dataset):
                from skifer.observability.certification_store import RunEvent
                return RunEvent("e", "r", dataset, "PROMOTED", "c", "1.0.0", "h", datetime(2026, 1, 1, tzinfo=timezone.utc))
        check = LoadFreshnessCheck(table="t", max_delay="2h", store=Store(), clock=lambda: datetime(2026, 1, 1, 1, tzinfo=timezone.utc))
        assert check.evaluate(None, "t").passed


class TestVolumeCheck:

    def test_volume_within_bounds_passes(self):
        backend = FakeBackend(default_rows=[{"row_count": 5000}])
        check = VolumeCheck(table="silver.orders", min_rows=1000, max_rows=10_000_000)
        result = check.evaluate(backend, "silver.orders")
        assert result.passed
        assert result.actual_value == 5000

    def test_volume_below_min_fails(self):
        backend = FakeBackend(default_rows=[{"row_count": 500}])
        check = VolumeCheck(table="silver.orders", min_rows=1000)
        result = check.evaluate(backend, "silver.orders")
        assert not result.passed
        assert "FAIL" in result.message

    def test_volume_above_max_fails(self):
        backend = FakeBackend(default_rows=[{"row_count": 20_000_000}])
        check = VolumeCheck(table="silver.orders", max_rows=10_000_000)
        result = check.evaluate(backend, "silver.orders")
        assert not result.passed

    def test_volume_no_bounds_always_passes(self):
        backend = FakeBackend(default_rows=[{"row_count": 0}])
        check = VolumeCheck(table="silver.orders")
        result = check.evaluate(backend, "silver.orders")
        assert result.passed


class TestVolumeVariationCheck:

    def test_variation_within_threshold_passes(self):
        backend = FakeBackend(default_rows=[{"row_count": 1050}])
        check = VolumeVariationCheck(table="t", variation_threshold=0.3, previous_count=1000)
        result = check.evaluate(backend, "t")
        assert result.passed

    def test_variation_exceeds_threshold_fails(self):
        backend = FakeBackend(default_rows=[{"row_count": 500}])
        check = VolumeVariationCheck(table="t", variation_threshold=0.2, previous_count=1000)
        result = check.evaluate(backend, "t")
        assert not result.passed
        assert "FAIL" in result.message

    def test_variation_no_history_skipped(self):
        backend = FakeBackend(default_rows=[{"row_count": 1000}])
        check = VolumeVariationCheck(table="t", variation_threshold=0.3)
        result = check.evaluate(backend, "t")
        assert result.passed
        assert "skipped" in result.message.lower()


class TestSchemaDriftCheck:

    def test_no_drift_passes(self):
        def sql_override(query):
            return [
                {"col_name": "id", "data_type": "bigint"},
                {"col_name": "amount", "data_type": "double"},
            ]
        backend = FakeBackend(sql_override=sql_override)
        check = SchemaDriftCheck(table="t", expected_columns=["id", "amount"])
        result = check.evaluate(backend, "t")
        assert result.passed

    def test_added_column_detected(self):
        def sql_override(query):
            return [
                {"col_name": "id", "data_type": "bigint"},
                {"col_name": "amount", "data_type": "double"},
                {"col_name": "new_col", "data_type": "string"},
            ]
        backend = FakeBackend(sql_override=sql_override)
        check = SchemaDriftCheck(table="t", expected_columns=["id", "amount"])
        result = check.evaluate(backend, "t")
        assert not result.passed
        assert "added" in result.message

    def test_removed_column_detected(self):
        def sql_override(query):
            return [{"col_name": "id", "data_type": "bigint"}]
        backend = FakeBackend(sql_override=sql_override)
        check = SchemaDriftCheck(table="t", expected_columns=["id", "amount"])
        result = check.evaluate(backend, "t")
        assert not result.passed
        assert "removed" in result.message


class TestCustomSqlCheck:

    def test_custom_sql_passes(self):
        backend = FakeBackend(default_rows=[{"count": 0}])
        check = CustomSqlCheck(table="t", sql="SELECT COUNT(*) AS count FROM {table} WHERE amount < 0", expect=0)
        result = check.evaluate(backend, "t")
        assert result.passed

    def test_custom_sql_fails(self):
        backend = FakeBackend(default_rows=[{"count": 5}])
        check = CustomSqlCheck(table="t", sql="SELECT COUNT(*) AS count FROM {table} WHERE amount < 0", expect=0, severity="critical")
        result = check.evaluate(backend, "t")
        assert not result.passed
        assert result.severity == "critical"

    def test_custom_sql_placeholder_replaced(self):
        backend = FakeBackend(default_rows=[{"count": 0}])
        check = CustomSqlCheck(table="silver.orders", sql="SELECT COUNT(*) FROM {table}", expect=0)
        check.evaluate(backend, "silver.orders")
        assert "silver.orders" in backend.queries[0]


class TestContractExtractorPhase2:

    def test_extract_freshness_from_observability(self):
        schema = parse_schema("""
tables:
  - name: silver.orders
observability:
  freshness:
    max_delay: "2h"
    timestamp_column: updated_at
""")
        contracts = ContractExtractor().extract(schema)
        freshness = [c for c in contracts if isinstance(c, FreshnessCheck)]
        assert len(freshness) == 1
        assert freshness[0].timestamp_column == "updated_at"
        assert freshness[0].max_delay == "2h"

    def test_extract_volume_from_observability(self):
        schema = parse_schema("""
tables:
  - name: silver.orders
observability:
  volume:
    min_rows: 1000
    max_rows: 10000000
""")
        contracts = ContractExtractor().extract(schema)
        volume = [c for c in contracts if isinstance(c, VolumeCheck)]
        assert len(volume) == 1
        assert volume[0].min_rows == 1000

    def test_extract_volume_variation_from_observability(self):
        schema = parse_schema("""
tables:
  - name: silver.orders
observability:
  volume:
    min_rows: 1000
    variation_threshold: 0.3
""")
        contracts = ContractExtractor().extract(schema)
        variation = [c for c in contracts if isinstance(c, VolumeVariationCheck)]
        assert len(variation) == 1
        assert variation[0].variation_threshold == 0.3

    def test_extract_schema_drift_from_observability(self):
        schema = parse_schema("""
tables:
  - name: silver.orders
select_final:
  - [id, id, []]
  - [amount, amount, []]
observability:
  schema_drift:
    enabled: true
""")
        contracts = ContractExtractor().extract(schema)
        drift = [c for c in contracts if isinstance(c, SchemaDriftCheck)]
        assert len(drift) == 1
        assert set(drift[0].expected_columns) == {"id", "amount"}

    def test_extract_custom_checks_from_observability(self):
        schema = parse_schema("""
tables:
  - name: silver.orders
observability:
  custom_checks:
    - sql: "SELECT COUNT(*) FROM {table} WHERE amount < 0"
      expect: 0
      severity: critical
""")
        contracts = ContractExtractor().extract(schema)
        custom = [c for c in contracts if isinstance(c, CustomSqlCheck)]
        assert len(custom) == 1
        assert custom[0].severity == "critical"
        assert custom[0].expect == 0


# ---------------------------------------------------------------------------
# Phase 3 — SqliteHistoryStore (in-memory, no mock)
# ---------------------------------------------------------------------------

class TestSqliteHistoryStore:

    def test_store_and_get_latest(self):
        from skifer.observability.history import SqliteHistoryStore
        store = SqliteHistoryStore(db_path=":memory:")
        report = MonitorReport(table="silver.orders", results=[], timestamp=datetime.now())
        store.store(report)
        latest = store.get_latest("silver.orders")
        assert latest is not None
        assert latest.table == "silver.orders"

    def test_get_latest_empty_returns_none(self):
        from skifer.observability.history import SqliteHistoryStore
        store = SqliteHistoryStore(db_path=":memory:")
        assert store.get_latest("nonexistent.table") is None

    def test_get_last_n_returns_correct_count(self):
        from skifer.observability.history import SqliteHistoryStore
        store = SqliteHistoryStore(db_path=":memory:")
        for i in range(5):
            store.store(MonitorReport(
                table="silver.orders",
                results=[],
                timestamp=datetime.now(),
            ))
        history = store.get_last_n("silver.orders", n=3)
        assert len(history) == 3

    def test_get_last_n_newest_first(self):
        from skifer.observability.history import SqliteHistoryStore
        store = SqliteHistoryStore(db_path=":memory:")
        t1 = datetime(2026, 1, 1, 10, 0, 0)
        t2 = datetime(2026, 1, 2, 10, 0, 0)
        store.store(MonitorReport(table="silver.orders", results=[], timestamp=t1))
        store.store(MonitorReport(table="silver.orders", results=[], timestamp=t2))
        history = store.get_last_n("silver.orders", n=2)
        # Newest first (highest id = stored last)
        assert history[0].timestamp == t2
        assert history[1].timestamp == t1

    def test_store_isolates_tables(self):
        from skifer.observability.history import SqliteHistoryStore
        store = SqliteHistoryStore(db_path=":memory:")
        store.store(MonitorReport(table="silver.orders", results=[], timestamp=datetime.now()))
        store.store(MonitorReport(table="silver.customers", results=[], timestamp=datetime.now()))
        assert store.get_latest("silver.orders") is not None
        assert store.get_latest("silver.customers") is not None
        assert len(store.get_last_n("silver.orders", 10)) == 1

    def test_store_with_check_results_roundtrip(self):
        from skifer.observability.history import SqliteHistoryStore
        store = SqliteHistoryStore(db_path=":memory:")
        # Build a real report with a NullCheck result
        backend = FakeBackend(default_rows=[{"null_count": 5}])
        check = NullCheck(table="silver.orders", column="amount", severity="critical")
        result = check.evaluate(backend, "silver.orders")
        report = MonitorReport(table="silver.orders", results=[result], timestamp=datetime.now())
        store.store(report)
        loaded = store.get_latest("silver.orders")
        assert loaded is not None
        assert len(loaded.results) == 1
        assert loaded.results[0].passed is False
        assert loaded.results[0].status is CheckStatus.FAIL
        assert loaded.results[0].severity == "critical"

    def test_monitor_stores_to_history_store(self):
        from skifer.observability.history import SqliteHistoryStore
        store = SqliteHistoryStore(db_path=":memory:")
        backend = FakeBackend(default_rows=[{"null_count": 0}])
        schema = parse_schema("""
tables:
  - name: silver.orders
    quality_checks:
      drop_nulls_in: [amount]
""")
        monitor = DataMonitor(backend=backend, history_store=store)
        monitor.check_from_schema("silver.orders", schema)
        assert store.get_latest("silver.orders") is not None


# ---------------------------------------------------------------------------
# Phase 4 — Intégration SkiferEngine.monitor
# ---------------------------------------------------------------------------

class TestEngineMonitorIntegration:
    """Tests pour l'intégration de DataMonitor dans SkiferEngine."""

    def _make_engine(self, monitor=None):
        """Build a minimal SkiferEngine with a fake backend (no Spark)."""
        from unittest.mock import MagicMock, patch
        import skifer.core.core as core_module

        fake_backend = MagicMock()
        fake_backend.is_local = True
        fake_backend.check_catalog_access.return_value = True
        fake_backend.build_fqn.return_value = "silver.fact_orders"
        fake_backend.get_current_user.return_value = "test_user"

        with patch("skifer.core.core.SandboxResolver"), \
             patch.object(core_module.SkiferEngine, "_find_file_upwards") as mock_find, \
             patch.object(core_module.SkiferEngine, "_load_config_from_yaml") as mock_load, \
             patch.object(core_module.SkiferEngine, "_auto_detect_environment"), \
             patch.object(core_module.SkiferEngine, "_get_clean_username", return_value="test_user"), \
             patch.object(core_module.SkiferEngine, "_is_running_as_job", return_value=True):

            mock_find.side_effect = lambda f: f".env" if f == ".env" else None

            from skifer.core.context import ExecutionContext
            from skifer.core.interpreter import SchemaInterpreter
            engine = core_module.SkiferEngine.__new__(core_module.SkiferEngine)
            ctx = ExecutionContext()
            object.__setattr__(engine, "_context", ctx)
            engine._backend = fake_backend
            engine.spark = MagicMock()
            engine.dbutils = MagicMock()
            engine.config = {"environments": {"local": {"catalog": None}}}
            engine.env = "LOCAL"
            engine.db = None
            engine.is_local = True
            engine.is_job_execution = True
            engine.schema_suffix = ""
            engine.current_user = "test_user"
            engine.monitor = monitor
            from skifer.core.patterns import PipelinePatterns
            engine._interpreter = SchemaInterpreter(backend=fake_backend, context=ctx)
            engine._patterns = PipelinePatterns(engine=engine)

            return engine

    def test_engine_has_monitor_attribute(self):
        """SkiferEngine exposes a .monitor attribute (defaults to None)."""
        engine = self._make_engine()
        assert hasattr(engine, "monitor")
        assert engine.monitor is None

    def test_engine_monitor_set_on_init(self):
        """When monitor= is passed, it is stored on the engine."""
        mock_monitor = MagicMock()
        engine = self._make_engine(monitor=mock_monitor)
        assert engine.monitor is mock_monitor

    def test_run_process_to_table_calls_monitor(self):
        """After writing, monitor.check_from_schema is called on the target FQN."""
        mock_monitor = MagicMock()
        mock_report = MagicMock()
        mock_report.summary.return_value = {"status": "PASS", "passed": 1, "total_checks": 1}
        mock_report.has_critical_failures.return_value = False
        mock_monitor.check_from_schema.return_value = mock_report

        engine = self._make_engine(monitor=mock_monitor)
        engine._get_backend = lambda: engine._backend
        engine._build_fqn = lambda s, t: "silver.fact_orders"
        engine._ensure_schema_exists = MagicMock()
        engine._drop_table_if_exists = MagicMock()
        engine.process_schema = MagicMock(return_value=MagicMock())
        engine._write_dataframe = MagicMock()
        engine.get_target_schema = MagicMock(return_value="silver")

        schema = parse_schema("tables:\n  - name: bronze.raw_orders\n    quality_checks:\n      drop_nulls_in: [id]\n")
        engine.run_process_to_table(schema, "silver", "fact_orders")

        mock_monitor.check_from_schema.assert_called_once()
        args = mock_monitor.check_from_schema.call_args
        assert args[0][0] == "silver.fact_orders"  # fqn
        assert args[1].get("raise_on_critical") is True

    def test_run_process_to_table_raises_on_critical_failure(self):
        """DataQualityError propagates from monitor when critical check fails."""
        from skifer.observability.checks import DataQualityError

        mock_monitor = MagicMock()
        mock_monitor.check_from_schema.side_effect = DataQualityError(
            MonitorReport(table="silver.fact_orders", results=[])
        )

        engine = self._make_engine(monitor=mock_monitor)
        engine._get_backend = lambda: engine._backend
        engine._build_fqn = lambda s, t: "silver.fact_orders"
        engine._ensure_schema_exists = MagicMock()
        engine._drop_table_if_exists = MagicMock()
        engine.process_schema = MagicMock(return_value=MagicMock())
        engine._write_dataframe = MagicMock()
        engine.get_target_schema = MagicMock(return_value="silver")

        schema = parse_schema("tables:\n  - name: bronze.raw_orders\n")
        with pytest.raises(DataQualityError):
            engine.run_process_to_table(schema, "silver", "fact_orders")

    def test_run_process_to_table_no_monitor_no_error(self):
        """Without monitor, run_process_to_table completes normally."""
        engine = self._make_engine(monitor=None)
        engine._get_backend = lambda: engine._backend
        engine._build_fqn = lambda s, t: "silver.fact_orders"
        engine._ensure_schema_exists = MagicMock()
        engine._drop_table_if_exists = MagicMock()
        engine.process_schema = MagicMock(return_value=MagicMock())
        engine._write_dataframe = MagicMock()
        engine.get_target_schema = MagicMock(return_value="silver")

        schema = parse_schema("tables:\n  - name: bronze.raw_orders\n")
        engine.run_process_to_table(schema, "silver", "fact_orders")  # must not raise


# ---------------------------------------------------------------------------
# Phase 5 — MonitorReporter
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_report():
    """A MonitorReport with one PASS and one FAIL result."""
    backend_pass = FakeBackend(default_rows=[{"null_count": 0}])
    backend_fail = FakeBackend(default_rows=[{"null_count": 10}])
    pass_check = NullCheck(table="silver.orders", column="id", severity="warning")
    fail_check = NullCheck(table="silver.orders", column="amount", severity="critical")
    return MonitorReport(
        table="silver.orders",
        results=[
            pass_check.evaluate(backend_pass, "silver.orders"),
            fail_check.evaluate(backend_fail, "silver.orders"),
        ],
        timestamp=datetime(2026, 4, 28, 10, 0, 0),
    )


class TestMonitorReporter:

    def test_to_json_structure(self, sample_report):
        from skifer.observability.reporter import MonitorReporter
        reporter = MonitorReporter()
        data = json.loads(reporter.to_json(sample_report))
        assert data["table"] == "silver.orders"
        assert "results" in data
        assert "summary" in data
        assert len(data["results"]) == 2

    def test_to_json_summary_status(self, sample_report):
        from skifer.observability.reporter import MonitorReporter
        reporter = MonitorReporter()
        data = json.loads(reporter.to_json(sample_report))
        assert data["summary"]["status"] == "CRITICAL"

    def test_to_text_contains_pass_or_fail(self, sample_report):
        from skifer.observability.reporter import MonitorReporter
        reporter = MonitorReporter()
        text = reporter.to_text(sample_report)
        assert "PASS" in text
        assert "FAIL" in text

    def test_to_text_contains_table_name(self, sample_report):
        from skifer.observability.reporter import MonitorReporter
        reporter = MonitorReporter()
        text = reporter.to_text(sample_report)
        assert "silver.orders" in text

    def test_to_text_empty_report(self):
        from skifer.observability.reporter import MonitorReporter
        reporter = MonitorReporter()
        report = MonitorReport(table="t", results=[], timestamp=datetime.now())
        text = reporter.to_text(report)
        assert "no checks" in text.lower()

    def test_to_html_creates_file(self, sample_report, tmp_path):
        from skifer.observability.reporter import MonitorReporter
        reporter = MonitorReporter()
        output = str(tmp_path / "report.html")
        reporter.to_html(sample_report, output)
        import os
        assert os.path.exists(output)
        content = open(output).read()
        assert "silver.orders" in content
        assert "<table>" in content

    def test_to_html_contains_status(self, sample_report, tmp_path):
        from skifer.observability.reporter import MonitorReporter
        reporter = MonitorReporter()
        output = str(tmp_path / "report.html")
        reporter.to_html(sample_report, output)
        content = open(output).read()
        assert "CRITICAL" in content


# ---------------------------------------------------------------------------
# Phase 5 — AlertDispatcher
# ---------------------------------------------------------------------------

class TestAlertDispatcher:

    def test_no_alert_sent_when_all_pass(self):
        from skifer.observability.alerts import AlertDispatcher
        report = MonitorReport(table="t", results=[], timestamp=datetime.now())
        dispatcher = AlertDispatcher()
        notified = dispatcher.dispatch(report, {"webhook_url": "http://fake"})
        assert notified == []

    def test_webhook_called_on_failure(self, sample_report):
        from skifer.observability.alerts import AlertDispatcher
        dispatcher = AlertDispatcher()
        calls = []

        def fake_send_webhook(url, payload):
            calls.append((url, payload))

        dispatcher._send_webhook = fake_send_webhook
        notified = dispatcher.dispatch(sample_report, {"webhook_url": "http://fake/hook"})
        assert "webhook" in notified
        assert len(calls) == 1
        payload = calls[0][1]
        assert payload["table"] == "silver.orders"
        assert len(payload["failures"]) > 0

    def test_slack_called_on_failure(self, sample_report):
        from skifer.observability.alerts import AlertDispatcher
        dispatcher = AlertDispatcher()
        calls = []
        dispatcher._send_webhook = lambda url, payload: calls.append(url)
        notified = dispatcher.dispatch(sample_report, {"slack_webhook": "http://slack/hook"})
        assert "slack" in notified
        assert len(calls) == 1

    def test_min_severity_filters_warnings(self, sample_report):
        from skifer.observability.alerts import AlertDispatcher
        dispatcher = AlertDispatcher()
        calls = []
        dispatcher._send_webhook = lambda url, payload: calls.append(payload)
        # Only critical failures — sample_report has 1 critical + 1 pass
        notified = dispatcher.dispatch(
            sample_report,
            {"webhook_url": "http://fake", "min_severity": "critical"},
        )
        assert "webhook" in notified
        # Only the critical failure should appear in the payload
        assert all(f["severity"] == "critical" for f in calls[0]["failures"])

    def test_build_payload_structure(self, sample_report):
        from skifer.observability.alerts import AlertDispatcher
        dispatcher = AlertDispatcher()
        failures = sample_report.failures()
        payload = dispatcher._build_payload(sample_report, failures)
        assert payload["source"] == "skifer"
        assert payload["table"] == "silver.orders"
        assert isinstance(payload["failures"], list)
