"""Non-sensitive semantic usage events for supervised Gold optimization."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import math
from typing import Any, Literal

from skifer.semantic.evidence import SemanticEvidence


FINGERPRINT_ALGORITHM = "sha256"
FINGERPRINT_VERSION = "v1"


def _require_non_empty_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Usage event field '{field_name}' must be a non-empty string.")
    return value


def _require_aware_datetime(value: Any, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"Usage event field '{field_name}' must be a datetime.")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"Usage event datetime field '{field_name}' must be timezone-aware.")
    return value


def _require_string_tuple(value: Any, field_name: str) -> tuple[str, ...]:
    if not isinstance(value, tuple):
        raise TypeError(f"Usage event field '{field_name}' must be a tuple of strings.")
    for index, item in enumerate(value):
        _require_non_empty_string(item, f"{field_name}[{index}]")
    return value


def _require_optional_non_negative_int(value: Any, field_name: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"Usage event field '{field_name}' must be an integer or None.")
    if value < 0:
        raise ValueError(f"Usage event field '{field_name}' must be non-negative.")
    return value


@dataclass(frozen=True)
class SemanticUsageEvent:
    """Allowlisted usage facts; raw query content has no field to enter through."""

    event_id: str
    occurred_at: datetime
    environment: str
    consumer_class: str
    model_hashes: tuple[str, ...]
    metric_ids: tuple[str, ...]
    dimension_ids: tuple[str, ...]
    normalized_filter_shape: tuple[str, ...]
    query_fingerprint: str
    duration_ms: int | None
    rows_returned: int | None
    bytes_scanned: int | None
    status: str

    def __post_init__(self) -> None:
        _require_non_empty_string(self.event_id, "event_id")
        _require_aware_datetime(self.occurred_at, "occurred_at")
        _require_non_empty_string(self.environment, "environment")
        _require_non_empty_string(self.consumer_class, "consumer_class")
        _require_string_tuple(self.model_hashes, "model_hashes")
        _require_string_tuple(self.metric_ids, "metric_ids")
        _require_string_tuple(self.dimension_ids, "dimension_ids")
        _require_string_tuple(
            self.normalized_filter_shape, "normalized_filter_shape"
        )
        fingerprint = _require_non_empty_string(
            self.query_fingerprint, "query_fingerprint"
        )
        if not fingerprint.startswith(
            f"{FINGERPRINT_ALGORITHM}:{FINGERPRINT_VERSION}:"
        ):
            raise ValueError(
                "Usage event query_fingerprint must use the supported "
                f"'{FINGERPRINT_ALGORITHM}:{FINGERPRINT_VERSION}:' prefix."
            )
        expected_fingerprint = fingerprint_query(
            model_hashes=self.model_hashes,
            metric_ids=self.metric_ids,
            dimension_ids=self.dimension_ids,
            normalized_filter_shape=self.normalized_filter_shape,
        )
        if fingerprint != expected_fingerprint:
            raise ValueError(
                "Usage event query_fingerprint does not match its logical query shape."
            )
        _require_optional_non_negative_int(self.duration_ms, "duration_ms")
        _require_optional_non_negative_int(self.rows_returned, "rows_returned")
        _require_optional_non_negative_int(self.bytes_scanned, "bytes_scanned")
        status = _require_non_empty_string(self.status, "status")
        if status not in {"succeeded", "failed"}:
            raise ValueError(
                "Usage event field 'status' must be 'succeeded' or 'failed'."
            )

    def to_dict(self) -> dict[str, Any]:
        """Serialize by explicit allowlist so future fields stay private."""
        return {
            "event_id": self.event_id,
            "occurred_at": self.occurred_at.astimezone(timezone.utc).isoformat(),
            "environment": self.environment,
            "consumer_class": self.consumer_class,
            "model_hashes": list(self.model_hashes),
            "metric_ids": list(self.metric_ids),
            "dimension_ids": list(self.dimension_ids),
            "normalized_filter_shape": list(self.normalized_filter_shape),
            "query_fingerprint": self.query_fingerprint,
            "duration_ms": self.duration_ms,
            "rows_returned": self.rows_returned,
            "bytes_scanned": self.bytes_scanned,
            "status": self.status,
        }


@dataclass(frozen=True)
class OptimizationProposal:
    """A reviewable optimization proposal; generation paths arrive in slice 8.4."""

    proposal_id: str
    kind: Literal[
        "materialized_view", "aggregate_table", "semantic_gap", "deprecation"
    ]
    rule_id: str
    rule_version: str
    evidence_event_ids: tuple[str, ...]
    expected_benefit: dict
    risks: tuple[str, ...]
    generated_schema_path: str | None
    generated_semantic_draft_path: str | None
    source_definition_hashes: tuple[str, ...]
    status: Literal["proposed", "accepted", "rejected", "stale", "measured"]

    def __post_init__(self) -> None:
        _require_non_empty_string(self.proposal_id, "proposal_id")
        if self.kind not in {
            "materialized_view",
            "aggregate_table",
            "semantic_gap",
            "deprecation",
        }:
            raise ValueError("Optimization proposal kind is not supported.")
        _require_non_empty_string(self.rule_id, "rule_id")
        _require_non_empty_string(self.rule_version, "rule_version")
        _require_string_tuple(self.evidence_event_ids, "evidence_event_ids")
        if not self.evidence_event_ids:
            raise ValueError("Optimization proposal requires evidence event IDs.")
        if not isinstance(self.expected_benefit, dict):
            raise TypeError("Optimization proposal expected_benefit must be a dict.")
        _copy_json_value(self.expected_benefit, "expected_benefit")
        _require_string_tuple(self.risks, "risks")
        for field_name in (
            "generated_schema_path",
            "generated_semantic_draft_path",
        ):
            value = getattr(self, field_name)
            if value is not None:
                _require_non_empty_string(value, field_name)
        _require_string_tuple(
            self.source_definition_hashes, "source_definition_hashes"
        )
        if not self.source_definition_hashes:
            raise ValueError("Optimization proposal requires source definition hashes.")
        if self.status not in {"proposed", "accepted", "rejected", "stale", "measured"}:
            raise ValueError("Optimization proposal status is not supported.")

    def to_dict(self) -> dict[str, Any]:
        """Serialize the public proposal contract field by field."""
        return {
            "proposal_id": self.proposal_id,
            "kind": self.kind,
            "rule_id": self.rule_id,
            "rule_version": self.rule_version,
            "evidence_event_ids": list(self.evidence_event_ids),
            "expected_benefit": _copy_json_value(
                self.expected_benefit, "expected_benefit"
            ),
            "risks": list(self.risks),
            "generated_schema_path": self.generated_schema_path,
            "generated_semantic_draft_path": self.generated_semantic_draft_path,
            "source_definition_hashes": list(self.source_definition_hashes),
            "status": self.status,
        }


def _copy_json_value(value: Any, field_name: str) -> Any:
    """Copy a JSON value through a closed recursive type allowlist."""
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"Optimization proposal field '{field_name}' must be finite.")
        return value
    if isinstance(value, list):
        return [
            _copy_json_value(item, f"{field_name}[{index}]")
            for index, item in enumerate(value)
        ]
    if isinstance(value, dict):
        copied: dict[str, Any] = {}
        for key in sorted(value):
            if not isinstance(key, str) or not key:
                raise TypeError(
                    f"Optimization proposal field '{field_name}' must have string keys."
                )
            copied[key] = _copy_json_value(value[key], f"{field_name}.{key}")
        return copied
    raise TypeError(
        f"Optimization proposal field '{field_name}' contains a non-JSON value."
    )


def fingerprint_query(
    *,
    model_hashes: tuple[str, ...],
    metric_ids: tuple[str, ...],
    dimension_ids: tuple[str, ...],
    normalized_filter_shape: tuple[str, ...],
) -> str:
    """Hash only logical IDs and value-free shapes using canonical JSON."""
    _require_string_tuple(model_hashes, "model_hashes")
    _require_string_tuple(metric_ids, "metric_ids")
    _require_string_tuple(dimension_ids, "dimension_ids")
    _require_string_tuple(normalized_filter_shape, "normalized_filter_shape")
    payload = {
        "dimension_ids": sorted(dimension_ids),
        "metric_ids": sorted(metric_ids),
        "model_hashes": sorted(model_hashes),
        "normalized_filter_shape": sorted(normalized_filter_shape),
    }
    canonical = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return f"{FINGERPRINT_ALGORITHM}:{FINGERPRINT_VERSION}:{digest}"


def usage_event_from_evidence(
    evidence: SemanticEvidence,
    *,
    environment: str,
    consumer_class: str,
) -> SemanticUsageEvent:
    """Convert standalone evidence without admitting query text or values."""
    if not isinstance(evidence, SemanticEvidence):
        raise TypeError("evidence must be a SemanticEvidence instance.")

    # Validate the evidence's current public allowlist, but never copy its dict:
    # unknown future fields therefore remain invisible to this conversion.
    evidence.to_dict()
    _require_non_empty_string(environment, "environment")
    _require_non_empty_string(consumer_class, "consumer_class")

    if evidence.executed_at is None:
        raise ValueError("SemanticEvidence.executed_at is required for a usage event.")
    occurred_at = _require_aware_datetime(evidence.executed_at, "occurred_at")

    if evidence.execution_status not in {"succeeded", "failed"}:
        raise ValueError(
            "SemanticEvidence.execution_status must be 'succeeded' or 'failed'."
        )

    model_hashes = tuple(
        sorted(
            _require_non_empty_string(source.definition_hash, "sources.definition_hash")
            for source in evidence.sources
        )
    )
    if not model_hashes:
        raise ValueError("SemanticEvidence.sources must contain at least one definition hash.")

    metric_ids = tuple(
        sorted(
            f"{_require_non_empty_string(metric.model_key, 'metrics.model_key')}."
            f"{_require_non_empty_string(metric.name, 'metrics.name')}"
            for metric in evidence.metrics
        )
    )
    dimension_ids = tuple(
        sorted(
            _require_non_empty_string(dimension, "dimensions")
            for dimension in evidence.dimensions
        )
    )

    filter_shapes = []
    for index, filter_def in enumerate(evidence.normalized_filters):
        if not isinstance(filter_def, dict):
            raise TypeError(f"SemanticEvidence.normalized_filters[{index}] must be a dict.")
        if "column" not in filter_def or "operator" not in filter_def:
            raise ValueError(
                f"SemanticEvidence.normalized_filters[{index}] requires column and operator."
            )
        column = _require_non_empty_string(
            filter_def["column"], f"normalized_filters[{index}].column"
        )
        operator = _require_non_empty_string(
            filter_def["operator"], f"normalized_filters[{index}].operator"
        )
        filter_shapes.append(f"{column}:{operator}")
    normalized_filter_shape = tuple(sorted(filter_shapes))

    duration_ms = None
    if evidence.execution_duration_seconds is not None:
        duration = evidence.execution_duration_seconds
        if isinstance(duration, bool) or not isinstance(duration, (int, float)):
            raise TypeError("SemanticEvidence.execution_duration_seconds must be numeric.")
        if not math.isfinite(duration) or duration < 0:
            raise ValueError(
                "SemanticEvidence.execution_duration_seconds must be finite and non-negative."
            )
        duration_ms = round(duration * 1000)

    query_fingerprint = fingerprint_query(
        model_hashes=model_hashes,
        metric_ids=metric_ids,
        dimension_ids=dimension_ids,
        normalized_filter_shape=normalized_filter_shape,
    )
    return SemanticUsageEvent(
        event_id=_require_non_empty_string(evidence.evidence_id, "evidence_id"),
        occurred_at=occurred_at,
        environment=environment,
        consumer_class=consumer_class,
        model_hashes=model_hashes,
        metric_ids=metric_ids,
        dimension_ids=dimension_ids,
        normalized_filter_shape=normalized_filter_shape,
        query_fingerprint=query_fingerprint,
        duration_ms=duration_ms,
        rows_returned=None,
        bytes_scanned=None,
        status=evidence.execution_status,
    )
