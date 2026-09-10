"""
checks.py — DataContract dataclasses + CheckResult.

Each DataContract subclass implements evaluate(backend, fqn) → CheckResult.
The backend must expose a sql(query) method returning an object on which
.collect() yields rows of dicts (or Row objects with attribute access).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Callable, ClassVar

from skifer.core.ir import ParsedFilter
from skifer.core.sql_compiler import _SQL_FILTER_DISPATCH, quote_ident


# ---------------------------------------------------------------------------
# Exception
# ---------------------------------------------------------------------------

class DataQualityError(Exception):
    """Raised when one or more critical checks fail and raise_on_critical=True."""

    def __init__(self, report):
        self.report = report
        failures = report.failures()
        msg = f"DataQualityError on '{report.table}': {len(failures)} critical failure(s)."
        for r in failures:
            msg += f"\n  - {r.message}"
        super().__init__(msg)


# ---------------------------------------------------------------------------
# CheckResult
# ---------------------------------------------------------------------------

class CheckStatus(str, Enum):
    """Outcome of a contract evaluation, including non-evaluation states."""

    PASS = "PASS"
    FAIL = "FAIL"
    ERROR = "ERROR"
    SKIPPED = "SKIPPED"


class ContractScope(str, Enum):
    """Whether a contract can identify individual violating rows."""

    ROW = "ROW"
    DATASET = "DATASET"


@dataclass(init=False)
class CheckResult:
    """Result of a single DataContract evaluation.

    ``passed=`` remains accepted for persisted callers from earlier releases.
    New code should set ``status``; only ``PASS`` is considered passed.
    """

    contract: "DataContract"
    status: CheckStatus
    actual_value: Any
    expected_value: Any
    message: str
    severity: str
    timestamp: datetime

    def __init__(
        self,
        contract: "DataContract",
        status: CheckStatus | str | None = None,
        actual_value: Any = None,
        expected_value: Any = None,
        message: str = "",
        severity: str = "warning",
        timestamp: datetime | None = None,
        *,
        passed: bool | None = None,
    ):
        if status is None:
            status = CheckStatus.PASS if passed else CheckStatus.FAIL
        self.contract = contract
        self.status = CheckStatus(status)
        self.actual_value = actual_value
        self.expected_value = expected_value
        self.message = message
        self.severity = severity
        self.timestamp = timestamp or datetime.now(timezone.utc)

    @property
    def passed(self) -> bool:
        """Compatibility view: only a successful evaluation is passed."""
        return self.status is CheckStatus.PASS


# ---------------------------------------------------------------------------
# Base DataContract
# ---------------------------------------------------------------------------

@dataclass
class DataContract:
    """Abstract base for all data quality contracts."""
    table: str
    severity: str = "warning"
    scope: ClassVar[ContractScope] = ContractScope.DATASET

    def evaluate(self, backend, fqn: str) -> CheckResult:  # pragma: no cover
        raise NotImplementedError

    def violation_predicate(self) -> str | None:
        """SQL predicate selecting violating rows, when this is a row-level check."""
        return None


# ---------------------------------------------------------------------------
# NullCheck
# ---------------------------------------------------------------------------

@dataclass
class NullCheck(DataContract):
    """Checks that a column contains no NULL values."""
    column: str = ""
    scope: ClassVar[ContractScope] = ContractScope.ROW

    def violation_predicate(self) -> str:
        return f"{quote_ident(self.column)} IS NULL"

    def evaluate(self, backend, fqn: str) -> CheckResult:
        query = f"SELECT COUNT(*) AS null_count FROM {fqn} WHERE {self.violation_predicate()}"
        rows = backend.sql(query).collect()
        null_count = rows[0]["null_count"] if rows else 0
        passed = (null_count == 0)
        return CheckResult(
            contract=self,
            passed=passed,
            actual_value=null_count,
            expected_value=0,
            message=(
                f"Column '{self.column}' has {null_count} NULL value(s)."
                if not passed
                else f"Column '{self.column}' has no NULLs."
            ),
            severity=self.severity,
        )


# ---------------------------------------------------------------------------
# UniqueCheck
# ---------------------------------------------------------------------------

@dataclass
class UniqueCheck(DataContract):
    """Checks that a set of columns form a unique key (no duplicates)."""
    columns: list = field(default_factory=list)

    def evaluate(self, backend, fqn: str) -> CheckResult:
        cols_csv = ", ".join(self.columns)
        query = (
            f"SELECT COUNT(*) AS total, COUNT(DISTINCT {cols_csv}) AS distinct_count "
            f"FROM {fqn}"
        )
        rows = backend.sql(query).collect()
        total = rows[0]["total"] if rows else 0
        distinct = rows[0]["distinct_count"] if rows else 0
        duplicates = total - distinct
        passed = (duplicates == 0)
        return CheckResult(
            contract=self,
            passed=passed,
            actual_value=duplicates,
            expected_value=0,
            message=(
                f"Columns {self.columns} have {duplicates} duplicate(s) ({total} rows, {distinct} distinct)."
                if not passed
                else f"Columns {self.columns} are unique ({total} rows)."
            ),
            severity=self.severity,
        )


# ---------------------------------------------------------------------------
# TypeCheck
# ---------------------------------------------------------------------------

@dataclass
class TypeCheck(DataContract):
    """Checks the actual SQL type of a column against an expected type."""
    column: str = ""
    expected_type: str = ""

    def evaluate(self, backend, fqn: str) -> CheckResult:
        # Use DESCRIBE to get column types — works on Spark SQL and most SQL engines
        query = f"DESCRIBE {fqn}"
        rows = backend.sql(query).collect()
        actual_type = None
        for row in rows:
            # Row may be a dict or a Row object
            col_name = row["col_name"] if isinstance(row, dict) else getattr(row, "col_name", None)
            data_type = row["data_type"] if isinstance(row, dict) else getattr(row, "data_type", None)
            if col_name == self.column:
                actual_type = (data_type or "").lower().strip()
                break

        expected_norm = self.expected_type.lower().strip()
        # Allow partial match: "double" matches "double precision", "bigint" matches "bigint(20)", etc.
        passed = actual_type is not None and (
            actual_type == expected_norm or actual_type.startswith(expected_norm)
        )
        return CheckResult(
            contract=self,
            passed=passed,
            actual_value=actual_type,
            expected_value=expected_norm,
            message=(
                f"Column '{self.column}' has type '{actual_type}', expected '{expected_norm}'."
                if not passed
                else f"Column '{self.column}' has expected type '{actual_type}'."
            ),
            severity=self.severity,
        )


# ---------------------------------------------------------------------------
# FilterInvariantCheck
# ---------------------------------------------------------------------------

@dataclass
class FilterInvariantCheck(DataContract):
    """
    Checks that a filter invariant holds on the table — i.e. that no row
    violates the expected filter condition.

    For example, filter "status:in:ACTIVE,PENDING" should mean the table
    contains only ACTIVE or PENDING rows; any other value is a violation.
    """
    column: str = ""
    operator: str = ""
    value: Any = None
    scope: ClassVar[ContractScope] = ContractScope.ROW

    def violation_predicate(self) -> str | None:
        """Build a safely quoted predicate that identifies violating rows."""
        quoted_column = quote_ident(self.column)
        if self.operator == "is_not_null":
            return f"{quoted_column} IS NULL"
        if self.operator == "is_null":
            return f"{quoted_column} IS NOT NULL"
        handler = _SQL_FILTER_DISPATCH.get(self.operator)
        if handler is None:
            return None
        expected = handler(
            quoted_column,
            ParsedFilter(column=self.column, operator=self.operator, value=self.value),
        )
        return f"NOT ({expected})"

    def evaluate(self, backend, fqn: str) -> CheckResult:
        violation_cond = self.violation_predicate()
        if violation_cond is None:
            # Operator not supported — skip gracefully
            return CheckResult(
                contract=self,
                status=CheckStatus.SKIPPED,
                actual_value=None,
                expected_value=None,
                message=f"Filter invariant check skipped (operator '{self.operator}' not supported).",
                severity=self.severity,
            )

        query = f"SELECT COUNT(*) AS violation_count FROM {fqn} WHERE {violation_cond}"
        rows = backend.sql(query).collect()
        violation_count = rows[0]["violation_count"] if rows else 0
        passed = (violation_count == 0)
        return CheckResult(
            contract=self,
            passed=passed,
            actual_value=violation_count,
            expected_value=0,
            message=(
                f"Filter invariant '{self.column} {self.operator} {self.value}' "
                f"violated by {violation_count} row(s)."
                if not passed
                else f"Filter invariant '{self.column} {self.operator} {self.value}' holds."
            ),
            severity=self.severity,
        )


# ---------------------------------------------------------------------------
# FreshnessCheck  (Phase 2)
# ---------------------------------------------------------------------------

@dataclass
class FreshnessCheck(DataContract):
    """
    Checks that the most recent timestamp in a column is not older than max_delay.

    Args:
        timestamp_column: Name of the timestamp/date column to inspect.
        max_delay:        Maximum allowed delay as an ISO-8601 duration string
                          (e.g. "2h", "30m", "1d"). Supported units: h, m, d, s.
    """
    timestamp_column: str = ""
    max_delay: str = "24h"
    clock: Callable[[], datetime] | None = field(default=None, repr=False, compare=False)

    @staticmethod
    def _parse_delay(delay_str: str) -> timedelta:
        """Parse simple duration strings like '2h', '30m', '1d', '3600s'."""
        delay_str = delay_str.strip()
        if delay_str.endswith("h"):
            return timedelta(hours=float(delay_str[:-1]))
        if delay_str.endswith("m"):
            return timedelta(minutes=float(delay_str[:-1]))
        if delay_str.endswith("d"):
            return timedelta(days=float(delay_str[:-1]))
        if delay_str.endswith("s"):
            return timedelta(seconds=float(delay_str[:-1]))
        raise ValueError(f"Unsupported delay format: '{delay_str}'. Use h/m/d/s suffix.")

    def evaluate(self, backend, fqn: str) -> CheckResult:
        query = f"SELECT MAX({self.timestamp_column}) AS max_ts FROM {fqn}"
        rows = backend.sql(query).collect()
        max_ts = rows[0]["max_ts"] if rows else None

        if max_ts is None:
            return CheckResult(
                contract=self,
                passed=False,
                actual_value=None,
                expected_value=self.max_delay,
                message=f"Column '{self.timestamp_column}' returned NULL — table may be empty.",
                severity=self.severity,
            )

        # Normalise to datetime
        if isinstance(max_ts, str):
            try:
                max_ts = datetime.fromisoformat(max_ts)
            except ValueError:
                max_ts = datetime.strptime(max_ts, "%Y-%m-%d")

        allowed_delta = self._parse_delay(self.max_delay)
        if max_ts.tzinfo is None:
            max_ts = max_ts.replace(tzinfo=timezone.utc)
        else:
            max_ts = max_ts.astimezone(timezone.utc)

        now = self.clock() if self.clock else datetime.now(timezone.utc)
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        # Small clock skew is normal between source and evaluator; a material
        # future timestamp is a data-quality error, not a failed freshness SLA.
        if self.clock is not None and max_ts > now + timedelta(minutes=1):
            return CheckResult(
                contract=self,
                status=CheckStatus.ERROR,
                actual_value=max_ts.isoformat(),
                expected_value="timestamp not in the future",
                message=f"Freshness check error: '{self.timestamp_column}' is in the future.",
                severity=self.severity,
            )
        actual_age = now - max_ts
        passed = actual_age <= allowed_delta

        return CheckResult(
            contract=self,
            passed=passed,
            actual_value=str(actual_age),
            expected_value=f"<= {self.max_delay}",
            message=(
                f"Freshness check FAIL on '{self.timestamp_column}': "
                f"last record is {actual_age} old (max allowed: {self.max_delay})."
                if not passed
                else f"Freshness check PASS: last record age {actual_age} <= {self.max_delay}."
            ),
            severity=self.severity,
        )


class DataFreshnessCheck(FreshnessCheck):
    """Explicit name for event-time freshness; ``FreshnessCheck`` remains compatible."""


@dataclass
class LoadFreshnessCheck(DataContract):
    """Checks elapsed time since the latest successful promotion, never event time."""

    max_delay: str = "24h"
    store: Any = field(default=None, repr=False, compare=False)
    clock: Callable[[], datetime] | None = field(default=None, repr=False, compare=False)

    def evaluate(self, backend, fqn: str) -> CheckResult:
        event = self.store.get_latest_promoted(self.table) if self.store else None
        if event is None:
            return CheckResult(self, CheckStatus.SKIPPED, None, self.max_delay,
                               "Load freshness skipped — no successful promotion exists.", self.severity)
        now = self.clock() if self.clock else datetime.now(timezone.utc)
        now = now if now.tzinfo else now.replace(tzinfo=timezone.utc)
        promoted_at = event.occurred_at.astimezone(timezone.utc)
        age = now - promoted_at
        if age.total_seconds() < 0:
            return CheckResult(self, CheckStatus.ERROR, promoted_at.isoformat(), self.max_delay,
                               "Load freshness error: promotion timestamp is in the future.", self.severity)
        passed = age <= FreshnessCheck._parse_delay(self.max_delay)
        return CheckResult(self, passed=passed, actual_value=str(age), expected_value=f"<= {self.max_delay}",
                           message=f"Load freshness {'PASS' if passed else 'FAIL'}: {age}.", severity=self.severity)


# ---------------------------------------------------------------------------
# VolumeCheck  (Phase 2)
# ---------------------------------------------------------------------------

@dataclass
class VolumeCheck(DataContract):
    """Checks that the table row count is within [min_rows, max_rows]."""
    min_rows: int | None = None
    max_rows: int | None = None

    def evaluate(self, backend, fqn: str) -> CheckResult:
        query = f"SELECT COUNT(*) AS row_count FROM {fqn}"
        rows = backend.sql(query).collect()
        row_count = rows[0]["row_count"] if rows else 0

        passed = True
        if self.min_rows is not None and row_count < self.min_rows:
            passed = False
        if self.max_rows is not None and row_count > self.max_rows:
            passed = False

        bounds = []
        if self.min_rows is not None:
            bounds.append(f">= {self.min_rows}")
        if self.max_rows is not None:
            bounds.append(f"<= {self.max_rows}")
        bounds_str = " and ".join(bounds) or "unbounded"

        return CheckResult(
            contract=self,
            passed=passed,
            actual_value=row_count,
            expected_value=bounds_str,
            message=(
                f"Volume check FAIL: {row_count} rows (expected {bounds_str})."
                if not passed
                else f"Volume check PASS: {row_count} rows (bounds: {bounds_str})."
            ),
            severity=self.severity,
        )


# ---------------------------------------------------------------------------
# VolumeVariationCheck  (Phase 2)
# ---------------------------------------------------------------------------

@dataclass
class VolumeVariationCheck(DataContract):
    """
    Checks that the current row count does not vary more than threshold % vs
    the previous run stored in history.

    Requires that the history_store be passed at evaluation time via the
    backend's ``last_volume`` attribute, or set via ``previous_count``.
    """
    variation_threshold: float = 0.3  # 30 %
    previous_count: int | None = None  # injected from history at check time

    def evaluate(self, backend, fqn: str) -> CheckResult:
        query = f"SELECT COUNT(*) AS row_count FROM {fqn}"
        rows = backend.sql(query).collect()
        current_count = rows[0]["row_count"] if rows else 0

        if self.previous_count is None:
            # No history available yet — skip
            return CheckResult(
                contract=self,
                passed=True,
                actual_value=current_count,
                expected_value=None,
                message="Volume variation check skipped — no previous run available.",
                severity=self.severity,
            )

        if self.previous_count == 0:
            variation = float("inf") if current_count > 0 else 0.0
        else:
            variation = abs(current_count - self.previous_count) / self.previous_count

        passed = variation <= self.variation_threshold

        return CheckResult(
            contract=self,
            passed=passed,
            actual_value=f"{variation:.1%}",
            expected_value=f"<= {self.variation_threshold:.0%}",
            message=(
                f"Volume variation FAIL: {variation:.1%} change "
                f"({self.previous_count} → {current_count}), "
                f"threshold {self.variation_threshold:.0%}."
                if not passed
                else f"Volume variation PASS: {variation:.1%} change."
            ),
            severity=self.severity,
        )


# ---------------------------------------------------------------------------
# SchemaDriftCheck  (Phase 2)
# ---------------------------------------------------------------------------

@dataclass
class SchemaDriftCheck(DataContract):
    """
    Detects schema drift: columns added or removed vs the expected column list
    derived from select_final in the YAML schema.
    """
    expected_columns: list = field(default_factory=list)

    def evaluate(self, backend, fqn: str) -> CheckResult:
        query = f"DESCRIBE {fqn}"
        rows = backend.sql(query).collect()
        actual_cols = set()
        for row in rows:
            col_name = row["col_name"] if isinstance(row, dict) else getattr(row, "col_name", None)
            if col_name and not col_name.startswith("#"):
                actual_cols.add(col_name)

        expected_set = set(self.expected_columns)
        added = actual_cols - expected_set
        removed = expected_set - actual_cols
        passed = not added and not removed

        parts = []
        if added:
            parts.append(f"added: {sorted(added)}")
        if removed:
            parts.append(f"removed: {sorted(removed)}")

        return CheckResult(
            contract=self,
            passed=passed,
            actual_value=sorted(actual_cols),
            expected_value=sorted(expected_set),
            message=(
                f"Schema drift detected — {', '.join(parts)}."
                if not passed
                else f"No schema drift detected ({len(actual_cols)} columns match)."
            ),
            severity=self.severity,
        )


# ---------------------------------------------------------------------------
# CustomSqlCheck  (Phase 2)
# ---------------------------------------------------------------------------

@dataclass
class CustomSqlCheck(DataContract):
    """
    Executes a custom SQL query and compares the scalar result to an expected value.

    The SQL query must return exactly one row with one column named ``result``,
    or the first column of the first row will be used.

    The ``{table}`` placeholder in ``sql`` is replaced with ``fqn`` at runtime.
    """
    sql: str = ""
    expect: Any = 0

    def evaluate(self, backend, fqn: str) -> CheckResult:
        query = self.sql.format(table=fqn)
        rows = backend.sql(query).collect()

        if not rows:
            actual = None
        else:
            row = rows[0]
            if isinstance(row, dict):
                actual = next(iter(row.values()))
            else:
                # PySpark Row — take the first field
                actual = row[0]

        passed = actual == self.expect

        return CheckResult(
            contract=self,
            passed=passed,
            actual_value=actual,
            expected_value=self.expect,
            message=(
                f"Custom SQL check FAIL: got {actual!r}, expected {self.expect!r}. SQL: {self.sql[:80]}"
                if not passed
                else f"Custom SQL check PASS: got {actual!r}."
            ),
            severity=self.severity,
        )
