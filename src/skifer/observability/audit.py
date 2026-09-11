"""Pure governance coverage audit for Skifer pipeline YAML files."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
import re
from typing import Any

from skifer.core.schema_loader import parse_schema


_PLACEHOLDER_RE = re.compile(r"\{\{\s*(\w+)\s*\}\}")


class MetricKey:
    """Stable metric key constants for coverage reports."""

    DATA_PRODUCT = "data_product"
    CONTRACT = "contract"
    STRUCTURED_OWNER = "structured_owner"
    FIELD_DESCRIPTIONS = "field_descriptions"
    FIELD_CLASSIFICATION = "field_classification"


METRIC_KEYS: tuple[str, ...] = (
    MetricKey.DATA_PRODUCT,
    MetricKey.CONTRACT,
    MetricKey.STRUCTURED_OWNER,
    MetricKey.FIELD_DESCRIPTIONS,
    MetricKey.FIELD_CLASSIFICATION,
)


@dataclass(frozen=True)
class PipelineAudit:
    """Audit result for one pipeline, deterministic and Spark-free."""

    path: str
    parsed: bool
    error: str | None
    has_data_product: bool
    has_contract: bool
    has_structured_owner: bool
    has_field_descriptions: bool
    has_field_classification: bool
    output_field_count: int
    described_field_count: int
    classified_field_count: int

    def metric(self, key: str) -> bool:
        """Return whether this pipeline satisfies one audit metric."""
        if not self.parsed:
            return False
        return {
            "data_product": self.has_data_product,
            "contract": self.has_contract,
            "structured_owner": self.has_structured_owner,
            "field_descriptions": self.has_field_descriptions,
            "field_classification": self.has_field_classification,
        }[key]

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "parsed": self.parsed,
            "error": self.error,
            "has_data_product": self.has_data_product,
            "has_contract": self.has_contract,
            "has_structured_owner": self.has_structured_owner,
            "has_field_descriptions": self.has_field_descriptions,
            "has_field_classification": self.has_field_classification,
            "output_field_count": self.output_field_count,
            "described_field_count": self.described_field_count,
            "classified_field_count": self.classified_field_count,
        }


@dataclass(frozen=True)
class AuditReport:
    """Aggregated coverage report for a collection of pipelines."""

    total: int
    parsed: int
    errored: int
    coverage: dict[str, float]
    overall_coverage_pct: float
    pipelines: tuple[PipelineAudit, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "total": self.total,
            "parsed": self.parsed,
            "errored": self.errored,
            "coverage": {key: self.coverage[key] for key in METRIC_KEYS},
            "overall_coverage_pct": self.overall_coverage_pct,
            "pipelines": [pipeline.to_dict() for pipeline in self.pipelines],
        }


def audit_project(paths: Sequence[str]) -> AuditReport:
    """Audit governance coverage for pipeline YAML files.

    The function reads and parses each file with sentinel template parameters,
    opens no Spark session, and records unreadable or invalid files as
    ``parsed=False`` so they count as uncovered in the denominator.
    """
    pipelines = tuple(_audit_one(path) for path in sorted(paths))
    total = len(pipelines)
    parsed = sum(1 for pipeline in pipelines if pipeline.parsed)
    coverage = {
        key: _coverage_pct(sum(1 for pipeline in pipelines if pipeline.metric(key)), total)
        for key in METRIC_KEYS
    }
    overall = (
        round(sum(coverage[key] for key in METRIC_KEYS) / len(METRIC_KEYS), 2)
        if total
        else 0.0
    )
    return AuditReport(
        total=total,
        parsed=parsed,
        errored=total - parsed,
        coverage=coverage,
        overall_coverage_pct=overall,
        pipelines=pipelines,
    )


def _audit_one(path: str) -> PipelineAudit:
    try:
        with open(path, encoding="utf-8") as handle:
            yaml_text = handle.read()
        schema = parse_schema(yaml_text, params=_sentinel_params(yaml_text))
    except Exception as exc:
        return PipelineAudit(
            path=path,
            parsed=False,
            error=_format_error(exc),
            has_data_product=False,
            has_contract=False,
            has_structured_owner=False,
            has_field_descriptions=False,
            has_field_classification=False,
            output_field_count=0,
            described_field_count=0,
            classified_field_count=0,
        )

    data_product = schema.get("data_product")
    contract = schema.get("contract")
    output = contract.get("output", {}) if isinstance(contract, dict) else {}
    if not isinstance(output, dict):
        output = {}

    output_field_count = len(output)
    described_field_count = sum(
        1
        for metadata in output.values()
        if isinstance(metadata, dict) and bool(metadata.get("description"))
    )
    classified_field_count = sum(
        1
        for metadata in output.values()
        if isinstance(metadata, dict) and bool(metadata.get("classification"))
    )

    owner = data_product.get("owner") if isinstance(data_product, dict) else None
    return PipelineAudit(
        path=path,
        parsed=True,
        error=None,
        has_data_product="data_product" in schema,
        has_contract="contract" in schema,
        has_structured_owner=_is_structured_owner(owner),
        has_field_descriptions=(
            output_field_count > 0 and described_field_count == output_field_count
        ),
        has_field_classification=(
            output_field_count > 0 and classified_field_count == output_field_count
        ),
        output_field_count=output_field_count,
        described_field_count=described_field_count,
        classified_field_count=classified_field_count,
    )


def _coverage_pct(satisfying: int, total: int) -> float:
    """Return a two-decimal percentage; zero pipelines cover nothing."""
    if total == 0:
        return 0.0
    return round(100 * satisfying / total, 2)


def _sentinel_params(yaml_text: str) -> dict[str, str]:
    return {key: f"__sentinel_{key}__" for key in _PLACEHOLDER_RE.findall(yaml_text)}


def _format_error(exc: Exception) -> str:
    summary = str(exc).strip().splitlines()[0] if str(exc).strip() else ""
    return f"{type(exc).__name__}: {summary}" if summary else type(exc).__name__


def _is_structured_owner(owner: object) -> bool:
    return isinstance(owner, dict) or owner.__class__.__name__ == "ParsedOwner"


__all__ = ["MetricKey", "METRIC_KEYS", "AuditReport", "PipelineAudit", "audit_project"]
