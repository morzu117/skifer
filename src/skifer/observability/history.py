"""
history.py — HistoryStore Protocol + SqliteHistoryStore + DeltaHistoryStore

HistoryStore is a Protocol — any object with store() / get_last_n() / get_latest()
can be used as a history backend.

SqliteHistoryStore: local SQLite DB for development and tests.
  - Default path: .skifer_observability.db (add to .gitignore)
  - Supports in-memory databases via db_path=":memory:"

DeltaHistoryStore: production Delta table on Databricks / local Spark.
  - Stores reports as JSON rows in a Delta table.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Protocol, runtime_checkable

from skifer.observability.monitor import MonitorReport


# ---------------------------------------------------------------------------
# HistoryStore Protocol
# ---------------------------------------------------------------------------

@runtime_checkable
class HistoryStore(Protocol):
    """Protocol for MonitorReport persistence backends."""

    def store(self, report: MonitorReport) -> None:
        """Persist a MonitorReport."""
        ...

    def get_last_n(self, table: str, n: int) -> list[MonitorReport]:
        """Return the n most recent reports for a given table, newest first."""
        ...

    def get_latest(self, table: str) -> MonitorReport | None:
        """Return the most recent report for a given table, or None."""
        ...


# ---------------------------------------------------------------------------
# Serialisation helpers
# ---------------------------------------------------------------------------

def _report_to_dict(report: MonitorReport) -> dict:
    """Convert a MonitorReport to a JSON-serialisable dict."""
    return {
        "table": report.table,
        "timestamp": report.timestamp.isoformat(),
        "results": [
            {
                "check_type": type(result.contract).__name__,
                "table": result.contract.table,
                "severity": result.severity,
                "passed": result.passed,
                "status": result.status.value,
                "actual_value": str(result.actual_value) if result.actual_value is not None else None,
                "expected_value": str(result.expected_value) if result.expected_value is not None else None,
                "message": result.message,
                "timestamp": result.timestamp.isoformat(),
            }
            for result in report.results
        ],
    }


def _dict_to_report(d: dict) -> MonitorReport:
    """Reconstruct a MonitorReport from a serialised dict (partial — no contract refs)."""
    from skifer.observability.checks import DataContract, CheckResult

    @dataclass_shim
    class _PlaceholderContract(DataContract):
        check_type: str = ""
        def evaluate(self, backend, fqn):  # pragma: no cover
            raise NotImplementedError

    results = []
    for r in d.get("results", []):
        ts = datetime.fromisoformat(r.get("timestamp", datetime.now(timezone.utc).isoformat()))
        contract = _PlaceholderContract(
            table=r.get("table", ""),
            severity=r.get("severity", "warning"),
            check_type=r.get("check_type", ""),
        )
        results.append(
            CheckResult(
                contract=contract,
                status=r.get("status"),
                passed=r.get("passed", True),
                actual_value=r.get("actual_value"),
                expected_value=r.get("expected_value"),
                message=r.get("message", ""),
                severity=r.get("severity", "warning"),
                timestamp=ts,
            )
        )

    ts = datetime.fromisoformat(d.get("timestamp", datetime.now(timezone.utc).isoformat()))
    return MonitorReport(table=d["table"], results=results, timestamp=ts)


def dataclass_shim(cls):
    """Minimal dataclass decorator shim for _PlaceholderContract."""
    from dataclasses import dataclass
    return dataclass(cls)


# ---------------------------------------------------------------------------
# SqliteHistoryStore
# ---------------------------------------------------------------------------

class SqliteHistoryStore:
    """
    SQLite-backed HistoryStore for local development and tests.

    Args:
        db_path: Path to the SQLite database file.
                 Use ":memory:" for an in-memory database (tests).
    """

    _CREATE_TABLE = """
    CREATE TABLE IF NOT EXISTS monitor_history (
        id        INTEGER PRIMARY KEY AUTOINCREMENT,
        table_fqn TEXT    NOT NULL,
        timestamp TEXT    NOT NULL,
        report    TEXT    NOT NULL   -- JSON blob
    )
    """

    def __init__(self, db_path: str = ".skifer_observability.db"):
        self.db_path = db_path
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute(self._CREATE_TABLE)
        self._conn.commit()

    def store(self, report: MonitorReport) -> None:
        """Persist a MonitorReport as a JSON blob."""
        report_json = json.dumps(_report_to_dict(report))
        self._conn.execute(
            "INSERT INTO monitor_history (table_fqn, timestamp, report) VALUES (?, ?, ?)",
            (report.table, report.timestamp.isoformat(), report_json),
        )
        self._conn.commit()

    def get_last_n(self, table: str, n: int) -> list[MonitorReport]:
        """Return the n most recent reports for a given table (newest first)."""
        cursor = self._conn.execute(
            "SELECT report FROM monitor_history WHERE table_fqn = ? "
            "ORDER BY id DESC LIMIT ?",
            (table, n),
        )
        rows = cursor.fetchall()
        return [_dict_to_report(json.loads(row[0])) for row in rows]

    def get_latest(self, table: str) -> MonitorReport | None:
        """Return the most recent report for a given table, or None."""
        results = self.get_last_n(table, 1)
        return results[0] if results else None

    def close(self) -> None:
        """Close the underlying SQLite connection."""
        self._conn.close()


# ---------------------------------------------------------------------------
# DeltaHistoryStore
# ---------------------------------------------------------------------------

class DeltaHistoryStore:
    """
    Delta-table-backed HistoryStore for production on Databricks (or local Spark).

    Reports are stored as rows in a Delta table. Each row contains the full
    JSON blob of the MonitorReport.

    Args:
        backend:   A SparkBackend (or compatible backend with .spark attribute).
        table_fqn: Fully-qualified Delta table name.
                   Default: "_observability.check_history"
    """

    def __init__(self, backend, table_fqn: str = "_observability.check_history"):
        self._backend = backend
        self._table_fqn = table_fqn
        self._ensure_table()

    def _spark(self):
        spark = getattr(self._backend, "spark", None)
        if spark is None:
            raise RuntimeError("DeltaHistoryStore requires a backend with a .spark attribute.")
        return spark

    def _ensure_table(self) -> None:
        try:
            spark = self._spark()
            spark.sql(
                f"""
                CREATE TABLE IF NOT EXISTS {self._table_fqn} (
                    table_fqn   STRING,
                    ts          TIMESTAMP,
                    report_json STRING
                ) USING DELTA
                """
            )
        except Exception:
            pass  # Table may already exist or Spark not fully ready yet

    def store(self, report: MonitorReport) -> None:
        from pyspark.sql import Row
        spark = self._spark()
        report_json = json.dumps(_report_to_dict(report))
        df = spark.createDataFrame(
            [Row(table_fqn=report.table, ts=report.timestamp, report_json=report_json)]
        )
        df.write.format("delta").mode("append").saveAsTable(self._table_fqn)

    def get_last_n(self, table: str, n: int) -> list[MonitorReport]:
        spark = self._spark()
        rows = (
            spark.sql(
                f"SELECT report_json FROM {self._table_fqn} "
                f"WHERE table_fqn = '{table}' "
                f"ORDER BY ts DESC LIMIT {n}"
            )
            .collect()
        )
        return [_dict_to_report(json.loads(r["report_json"])) for r in rows]

    def get_latest(self, table: str) -> MonitorReport | None:
        results = self.get_last_n(table, 1)
        return results[0] if results else None
