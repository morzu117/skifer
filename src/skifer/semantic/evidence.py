"""Safe, standalone evidence returned with semantic query results."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import math
import re
from typing import Any

from .access_policy import PolicyEvaluation


SQL_HASH_ALGORITHM = "sha256"
SQL_NORMALIZATION_VERSION = "v1"

# A single-quoted SQL literal, with '' as the escape for an embedded quote.
_STRING_LITERAL = re.compile(r"'(?:[^']|'')*'")
_WHITESPACE = re.compile(r"\s+")


class EvidenceRedactionError(Exception):
    """Raised when redacted evidence content is requested."""


class MissingCertificationSnapshot(Exception):
    """An ALLOW decision that no certification snapshot backs up."""


class SemanticExecutionError(Exception):
    """Semantic SQL failure carrying a safe, explicitly partial evidence record."""

    def __init__(self, message: str, *, evidence: "SemanticEvidence"):
        self.evidence = evidence
        super().__init__(message)


@dataclass(frozen=True)
class ExecutionResult:
    """Optional backend hook result; ``execute_sql`` remains unchanged."""

    dataframe: Any
    statement_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class EvidencePolicy:
    """Pure decision on what an evidence payload may expose.

    Deliberately free of I/O, configuration reads and clocks, like
    ``access_policy.evaluate`` — it must stay testable on its own.
    """

    include_sql: bool = False
    include_filter_values: bool = False
    sensitive_columns: frozenset[str] = frozenset()

    @classmethod
    def redacted(cls) -> "EvidencePolicy":
        """The fail-closed default: neither raw SQL nor filter values."""
        return cls()


def normalize_sql(sql: str) -> str:
    r"""Collapse formatting without touching semantics.

    Whitespace is normalized **outside string literals only**. A blanket
    ``re.sub(r"\s+", " ", sql)`` would make ``x = 'a  b'`` and ``x = 'a b'``
    hash alike — two different queries attested as the same one, which is the
    one failure an evidence layer cannot afford.
    """
    pieces: list[str] = []
    cursor = 0
    for match in _STRING_LITERAL.finditer(sql):
        pieces.append(_WHITESPACE.sub(" ", sql[cursor : match.start()]))
        pieces.append(match.group(0))
        cursor = match.end()
    pieces.append(_WHITESPACE.sub(" ", sql[cursor:]))
    return "".join(pieces).strip()


def hash_sql(sql: str) -> str:
    """Hash the executed SQL, carrying its algorithm and normalization version.

    The prefix matters: once the normalization changes, an old and a new hash
    must not be comparable in silence as if they were the same kind of thing.
    """
    digest = hashlib.sha256(normalize_sql(sql).encode("utf-8")).hexdigest()
    return f"{SQL_HASH_ALGORITHM}:{SQL_NORMALIZATION_VERSION}:{digest}"


def hash_definition(*parts: Any) -> str:
    """Hash a semantic definition so that a formula change changes the hash."""
    payload = "\u241f".join("" if part is None else str(part) for part in parts)
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return f"{SQL_HASH_ALGORITHM}:{SQL_NORMALIZATION_VERSION}:{digest}"


def _unsupported_type(field_name: str, value: Any) -> TypeError:
    return TypeError(
        f"Evidence field '{field_name}' has unsupported type "
        f"'{type(value).__name__}'."
    )


def _serialize_value(value: Any, field_name: str) -> Any:
    """Serialize only deliberately admitted, JSON-native evidence values."""
    if isinstance(value, float) and not math.isfinite(value):
        # json.dumps emits a bare NaN/Infinity token, which is not valid JSON:
        # a strict parser on the consuming side rejects the whole document. An
        # evidence artefact nobody can parse is worse than a loud failure here.
        raise ValueError(
            f"Evidence field '{field_name}' is {value}; evidence must stay valid JSON."
        )
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"Evidence datetime field '{field_name}' must be timezone-aware.")
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, (tuple, list)):
        return [
            _serialize_value(item, f"{field_name}[{index}]")
            for index, item in enumerate(value)
        ]
    if isinstance(value, dict):
        if not all(isinstance(key, str) for key in value):
            raise _unsupported_type(field_name, next(key for key in value if not isinstance(key, str)))
        return {
            key: _serialize_value(value[key], f"{field_name}.{key}")
            for key in sorted(value)
        }
    raise _unsupported_type(field_name, value)


@dataclass(frozen=True)
class SourceEvidence:
    dataset: str
    contract_id: str | None
    contract_version: str | None
    definition_hash: str | None
    certification_status: str
    certified_at: datetime | None
    load_age_seconds: float | None
    data_age_seconds: float | None
    certification_run_id: str | None


@dataclass(frozen=True)
class MetricEvidence:
    name: str
    model_key: str
    definition_hash: str
    source_columns: tuple[str, ...]
    # An empty `source_columns` alone is ambiguous: it reads as "this metric has
    # no sources", which is false when lineage simply could not be resolved.
    lineage_status: str = "resolved"


@dataclass(frozen=True, kw_only=True)
class SemanticEvidence:
    schema_version: str = field(default="1", init=False)
    evidence_id: str
    trace_id: str | None
    model_keys: tuple[str, ...]
    metrics: tuple[MetricEvidence, ...]
    dimensions: tuple[str, ...]
    normalized_filters: tuple[dict, ...]
    sources: tuple[SourceEvidence, ...]
    policy: PolicyEvaluation
    sql_hash: str | None
    sql_text: str | None
    sql_redacted: bool = False
    statement_id: str | None
    compiled_at: datetime
    executed_at: datetime | None
    execution_status: str = "pending"
    execution_duration_seconds: float | None = None
    execution_error_type: str | None = None

    def to_dict(
        self,
        *,
        include_sql: bool = False,
        include_filter_values: bool = False,
        sensitive_columns: frozenset[str] = frozenset(),
    ) -> dict:
        """Return an explicit, redacted and JSON-native evidence representation.

        This is intentionally an allowlist instead of ``asdict``: future fields
        are private until this method is deliberately updated to expose them.
        """
        payload = {
            "schema_version": _serialize_value(self.schema_version, "schema_version"),
            "evidence_id": _serialize_value(self.evidence_id, "evidence_id"),
            "trace_id": _serialize_value(self.trace_id, "trace_id"),
            "model_keys": _serialize_value(self.model_keys, "model_keys"),
            "metrics": [
                {
                    "name": _serialize_value(metric.name, f"metrics[{index}].name"),
                    "model_key": _serialize_value(
                        metric.model_key, f"metrics[{index}].model_key"
                    ),
                    "definition_hash": _serialize_value(
                        metric.definition_hash, f"metrics[{index}].definition_hash"
                    ),
                    "source_columns": _serialize_value(
                        metric.source_columns, f"metrics[{index}].source_columns"
                    ),
                    "lineage_status": _serialize_value(
                        metric.lineage_status, f"metrics[{index}].lineage_status"
                    ),
                }
                for index, metric in enumerate(self.metrics)
            ],
            "dimensions": _serialize_value(self.dimensions, "dimensions"),
            "normalized_filters": [
                self._serialize_filter(
                    filter_def,
                    index,
                    include_filter_values,
                    sensitive_columns,
                )
                for index, filter_def in enumerate(self.normalized_filters)
            ],
            "sources": [
                {
                    "dataset": _serialize_value(source.dataset, f"sources[{index}].dataset"),
                    "contract_id": _serialize_value(
                        source.contract_id, f"sources[{index}].contract_id"
                    ),
                    "contract_version": _serialize_value(
                        source.contract_version, f"sources[{index}].contract_version"
                    ),
                    "definition_hash": _serialize_value(
                        source.definition_hash, f"sources[{index}].definition_hash"
                    ),
                    "certification_status": _serialize_value(
                        source.certification_status,
                        f"sources[{index}].certification_status",
                    ),
                    "certified_at": _serialize_value(
                        source.certified_at, f"sources[{index}].certified_at"
                    ),
                    "load_age_seconds": _serialize_value(
                        source.load_age_seconds, f"sources[{index}].load_age_seconds"
                    ),
                    "data_age_seconds": _serialize_value(
                        source.data_age_seconds, f"sources[{index}].data_age_seconds"
                    ),
                    "certification_run_id": _serialize_value(
                        source.certification_run_id,
                        f"sources[{index}].certification_run_id",
                    ),
                }
                for index, source in enumerate(self.sources)
            ],
            "policy": {
                "decision": _serialize_value(self.policy.decision.value, "policy.decision"),
                "reasons": _serialize_value(self.policy.reasons, "policy.reasons"),
                "evaluated_at": _serialize_value(
                    self.policy.evaluated_at, "policy.evaluated_at"
                ),
            },
            "sql_hash": _serialize_value(self.sql_hash, "sql_hash"),
            "statement_id": _serialize_value(self.statement_id, "statement_id"),
            "compiled_at": _serialize_value(self.compiled_at, "compiled_at"),
            "executed_at": _serialize_value(self.executed_at, "executed_at"),
            "execution_status": _serialize_value(
                self.execution_status, "execution_status"
            ),
            "execution_duration_seconds": _serialize_value(
                self.execution_duration_seconds, "execution_duration_seconds"
            ),
            "execution_error_type": _serialize_value(
                self.execution_error_type, "execution_error_type"
            ),
        }
        if include_sql:
            if self.sql_redacted:
                # Returning None here would leave the consumer unable to tell
                # "there was no SQL" from "the SQL was withheld" — precisely the
                # distinction an audit turns on.
                raise EvidenceRedactionError(
                    "Raw SQL was withheld by the evidence policy for evidence "
                    f"'{self.evidence_id}'; it cannot be disclosed after the fact."
                )
            payload["sql_text"] = _serialize_value(self.sql_text, "sql_text")
        return payload

    @staticmethod
    def _serialize_filter(
        filter_def: dict,
        index: int,
        include_filter_values: bool,
        sensitive_columns: frozenset[str],
    ) -> dict:
        if not isinstance(filter_def, dict):
            raise _unsupported_type(f"normalized_filters[{index}]", filter_def)
        payload = {
            "column": _serialize_value(
                filter_def.get("column"), f"normalized_filters[{index}].column"
            ),
            "operator": _serialize_value(
                filter_def.get("operator"), f"normalized_filters[{index}].operator"
            ),
        }
        if "value" in filter_def:
            include_value = (
                include_filter_values
                or filter_def.get("column") in sensitive_columns
            )
            payload["value"] = (
                _serialize_value(
                    filter_def["value"], f"normalized_filters[{index}].value"
                )
                if include_value
                else "<redacted>"
            )
        return payload


@dataclass
class SemanticResult:
    dataframe: Any
    evidence: SemanticEvidence


def build_metric_evidence(
    selected_expressions,
    lineage_columns: dict,
) -> tuple["MetricEvidence", ...]:
    """Turn the compiled logical plan into per-metric evidence.

    ``lineage_columns`` maps ``(model_key, metric_name)`` to the source columns
    resolved from the lineage subgraph. A key that is absent means lineage could
    not be resolved for that metric, which is recorded rather than flattened
    into an empty source list.
    """
    metrics = []
    for expression in selected_expressions:
        if expression.kind != "metric":
            continue
        key = (expression.model_key, expression.name)
        resolved = lineage_columns.get(key)
        metrics.append(
            MetricEvidence(
                name=expression.name,
                model_key=expression.model_key,
                definition_hash=hash_definition(
                    expression.model_key,
                    expression.name,
                    expression.sql,
                    expression.aggregate_type,
                    *expression.inline_filters,
                ),
                source_columns=tuple(sorted(resolved)) if resolved else (),
                lineage_status="resolved" if resolved is not None else "unresolved",
            )
        )
    return tuple(metrics)
