from __future__ import annotations

from datetime import datetime, timezone
import inspect
from uuid import uuid4

import pytest

from skifer.core.adapters.duckdb import DuckDBAdapter
from skifer.core.spark_backend import SparkBackend
from skifer.observability.checks import CheckResult, CheckStatus, NullCheck
from skifer.observability.history import DeltaHistoryStore, SqlHistoryStore
from skifer.observability.monitor import MonitorReport


@pytest.fixture(params=("spark", "duckdb"))
def portable_history_store(request):
    schema = f"history_store_{uuid4().hex}"
    table_fqn = f"{schema}.check_history"
    if request.param == "spark":
        spark = request.getfixturevalue("spark")
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{schema}`")
        try:
            yield DeltaHistoryStore(
                SparkBackend(spark=spark, is_local=True),
                table_fqn=table_fqn,
            )
        finally:
            spark.sql(f"DROP SCHEMA IF EXISTS `{schema}` CASCADE")
        return

    duckdb = pytest.importorskip("duckdb")
    connection = duckdb.connect()
    try:
        yield SqlHistoryStore(DuckDBAdapter(connection), table_fqn=table_fqn)
    finally:
        connection.close()


def _report(table: str, timestamp: datetime, *, passed: bool) -> MonitorReport:
    check = NullCheck(table=table, column="amount", severity="critical")
    result = CheckResult(
        contract=check,
        status=CheckStatus.PASS if passed else CheckStatus.FAIL,
        actual_value=0 if passed else 1,
        expected_value=0,
        message="No null order amounts" if passed else "Null order amounts",
        severity="critical",
        timestamp=timestamp,
    )
    return MonitorReport(table=table, results=[result], timestamp=timestamp)


def test_sql_history_store_has_exactly_the_delta_store_method_signatures():
    public_methods = {
        name
        for name, member in inspect.getmembers(SqlHistoryStore, inspect.isfunction)
        if not name.startswith("_")
    }

    assert public_methods == {
        name
        for name, member in inspect.getmembers(DeltaHistoryStore, inspect.isfunction)
        if not name.startswith("_")
    }
    for name in public_methods:
        assert inspect.signature(getattr(SqlHistoryStore, name)) == inspect.signature(
            getattr(DeltaHistoryStore, name)
        )


def test_history_store_behaves_identically_on_delta_and_sql(portable_history_store):
    store = portable_history_store
    table = "silver.orders"
    other_table = "silver.customers"
    older = _report(
        table,
        datetime(2026, 9, 18, 8, 0, tzinfo=timezone.utc),
        passed=False,
    )
    newer = _report(
        table,
        datetime(2026, 9, 18, 9, 0, tzinfo=timezone.utc),
        passed=True,
    )
    other = _report(
        other_table,
        datetime(2026, 9, 18, 10, 0, tzinfo=timezone.utc),
        passed=True,
    )

    assert store.get_latest(table) is None

    store.store(older)
    store.store(newer)
    store.store(other)

    assert store.get_latest(table).timestamp == newer.timestamp
    assert store.get_last_n(table, 0) == []
    assert store.get_last_n(table, 1)[0].timestamp == newer.timestamp

    history = store.get_last_n(table, 2)
    assert [report.timestamp for report in history] == [
        newer.timestamp,
        older.timestamp,
    ]
    assert history[0].results[0].status is CheckStatus.PASS
    assert history[1].results[0].status is CheckStatus.FAIL
    assert store.get_latest(other_table).table == other_table
