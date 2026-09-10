"""
monitor.py — DataMonitor + MonitorReport

DataMonitor orchestrates the evaluation of DataContracts against a table.
It accepts any backend that implements a sql(query) method.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from skifer.observability.checks import (
    CheckResult,
    CheckStatus,
    DataContract,
    DataQualityError,
)
from skifer.observability.contracts import ContractExtractor
from skifer.observability.tracing import NoOpTracer, configured_span_scope


@dataclass
class MonitorReport:
    """Aggregated result of a DataMonitor run."""
    table: str
    results: list[CheckResult]
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def has_critical_failures(self) -> bool:
        """True if at least one critical check failed."""
        return any(not r.passed and r.severity == "critical" for r in self.results)

    def failures(self) -> list[CheckResult]:
        """All failed checks (any severity)."""
        return [r for r in self.results if not r.passed]

    def summary(self) -> dict[str, Any]:
        """Compact summary dict for reporting / JSON export."""
        total = len(self.results)
        passed = sum(1 for r in self.results if r.status is CheckStatus.PASS)
        failed = total - passed
        critical_failures = sum(
            1 for r in self.results if not r.passed and r.severity == "critical"
        )
        return {
            "table": self.table,
            "timestamp": self.timestamp.isoformat(),
            "total_checks": total,
            "passed": passed,
            "failed": failed,
            "critical_failures": critical_failures,
            "status_counts": {
                status.value: sum(1 for r in self.results if r.status is status)
                for status in CheckStatus
            },
            "status": "PASS" if failed == 0 else ("CRITICAL" if critical_failures > 0 else "WARN"),
        }


class DataMonitor:
    """
    Runs DataContract checks against live tables and returns a MonitorReport.

    Args:
        backend: SparkBackend — any object with a sql(query) → DataFrame-like
                 method works in tests (duck typing).
        history_store: Optional HistoryStore — if provided, reports are
                       automatically persisted after each check_table() call.
    """

    def __init__(self, backend, history_store=None):
        self.backend = backend
        self.history = history_store
        self.tracer = NoOpTracer()
        self.tracing_required = False

    def check_table(
        self,
        fqn: str,
        contracts: list[DataContract],
        raise_on_critical: bool = False,
    ) -> MonitorReport:
        """
        Evaluate all contracts against an existing table.

        Args:
            fqn:              Fully-qualified table name.
            contracts:        List of DataContract instances to evaluate.
            raise_on_critical: If True, raises DataQualityError when a critical
                               check fails. Defaults to False.

        Returns:
            MonitorReport with all CheckResult objects.
        """
        if isinstance(self.tracer, NoOpTracer):
            return self._check_table(fqn, contracts, raise_on_critical)
        with configured_span_scope(
            self.tracer,
            "skifer.contract.evaluate",
            required=self.tracing_required,
        ):
            return self._check_table(fqn, contracts, raise_on_critical)

    def _check_table(
        self,
        fqn: str,
        contracts: list[DataContract],
        raise_on_critical: bool,
    ) -> MonitorReport:
        self._hydrate_volume_variation_contracts(contracts, fqn)

        results: list[CheckResult] = []
        for contract in contracts:
            try:
                result = contract.evaluate(self.backend, fqn)
            except Exception as exc:  # pragma: no cover
                result = CheckResult(
                    contract=contract,
                    status=CheckStatus.ERROR,
                    actual_value=None,
                    expected_value=None,
                    message=f"Check execution error: {exc}",
                    severity=contract.severity,
                    timestamp=datetime.now(timezone.utc),
                )
            results.append(result)

        report = MonitorReport(table=fqn, results=results)

        if self.history is not None:
            try:
                self.history.store(report)
            except Exception:
                pass  # history is best-effort

        if raise_on_critical and report.has_critical_failures():
            raise DataQualityError(report)

        return report

    def check_from_schema(
        self,
        fqn: str,
        schema_dict: dict,
        raise_on_critical: bool = False,
    ) -> MonitorReport:
        """
        Shortcut: derive contracts from the YAML schema dict, then run check_table().

        Args:
            fqn:              Fully-qualified table name to check.
            schema_dict:      Normalized schema dict (output of parse_schema).
            raise_on_critical: Propagated to check_table().

        Returns:
            MonitorReport.
        """
        contracts = ContractExtractor().extract(schema_dict)
        return self.check_table(fqn, contracts, raise_on_critical=raise_on_critical)

    def _hydrate_volume_variation_contracts(self, contracts: list[DataContract], fqn: str) -> None:
        """Populate previous_count from history when available."""
        if self.history is None or not hasattr(self.history, "get_latest"):
            return

        try:
            report = self.history.get_latest(fqn)
        except Exception:
            return

        if report is None:
            return

        previous_count = None
        for result in report.results:
            check_type = getattr(result.contract, "check_type", type(result.contract).__name__)
            if check_type == "VolumeCheck" and result.actual_value is not None:
                try:
                    previous_count = int(result.actual_value)
                    break
                except (TypeError, ValueError):
                    continue

        if previous_count is None:
            return

        for contract in contracts:
            if type(contract).__name__ == "VolumeVariationCheck" and getattr(contract, "previous_count", None) is None:
                contract.previous_count = previous_count
