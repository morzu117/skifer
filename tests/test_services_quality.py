"""Spark-free tests for quality definitions and report history."""

from datetime import datetime, timezone
import json

import pytest

from skifer.observability.checks import CheckResult, CheckStatus, NullCheck
from skifer.observability.monitor import MonitorReport
from skifer.observability.tracing import TraceContext
from skifer.services import (
    QualityService,
    RequestContext,
    ResourceUnavailable,
    SCOPE_CONTRACTS_READ,
    ScopeDenied,
)


def _context(*scopes: str) -> RequestContext:
    return RequestContext(
        subject="local-user",
        scopes=frozenset(scopes),
        consumer_class="local",
        trace_context=TraceContext(),
    )


def _report(table: str, passed: bool, day: int) -> MonitorReport:
    contract = NullCheck(table=table, column="amount", severity="critical")
    timestamp = datetime(2026, 9, day, tzinfo=timezone.utc)
    result = CheckResult(
        contract=contract,
        status=CheckStatus.PASS if passed else CheckStatus.FAIL,
        actual_value=0 if passed else 2,
        expected_value=0,
        message="ok" if passed else "nulls found",
        severity="critical",
        timestamp=timestamp,
    )
    return MonitorReport(table=table, results=[result], timestamp=timestamp)


class _HistoryStore:
    def __init__(self, reports):
        self.reports = reports

    def get_last_n(self, table, n):
        return [report for report in self.reports if report.table == table][:n]

    def get_latest(self, table):
        reports = self.get_last_n(table, 1)
        return reports[0] if reports else None


def test_checks_from_schema():
    service = QualityService(_HistoryStore([]))
    schema = {
        "tables": [
            {
                "name": "silver.orders",
                "quality_checks": {"drop_nulls_in": ["amount"]},
            }
        ]
    }

    checks = service.checks_from_schema(_context(SCOPE_CONTRACTS_READ), schema)

    assert len(checks) == 1
    assert checks[0].name == "NullCheck"
    assert checks[0].kind == "NullCheck"
    assert checks[0].column == "amount"
    assert checks[0].params == {"table": "silver.orders", "severity": "critical"}
    json.dumps(checks[0].to_dict())


def test_checks_from_schema_requires_scope():
    with pytest.raises(ScopeDenied):
        QualityService(_HistoryStore([])).checks_from_schema(_context(), {})


def test_history_maps_reports():
    reports = [_report("gold.orders", False, 11), _report("gold.orders", True, 10)]
    service = QualityService(_HistoryStore(reports))

    views = service.history(
        _context(SCOPE_CONTRACTS_READ), "gold.orders", limit=20
    )

    assert [view.run_at for view in views] == [
        "2026-09-11T00:00:00+00:00",
        "2026-09-10T00:00:00+00:00",
    ]
    assert [view.passed for view in views] == [False, True]
    assert views[0].checks[0]["check_type"] == "NullCheck"
    json.dumps([view.to_dict() for view in views])


def test_last_report_maps_latest_and_none():
    service = QualityService(_HistoryStore([_report("gold.orders", True, 11)]))
    ctx = _context(SCOPE_CONTRACTS_READ)

    assert service.last_report(ctx, "gold.orders").passed is True
    assert service.last_report(ctx, "gold.missing") is None


def test_history_store_missing_method_unavailable():
    with pytest.raises(ResourceUnavailable):
        QualityService(object()).history(
            _context(SCOPE_CONTRACTS_READ), "gold.orders"
        )
