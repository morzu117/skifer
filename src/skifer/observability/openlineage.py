"""Pure OpenLineage RunEvent builder — no I/O, no Spark, no network (Plan 36.1)."""
from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Sequence

from skifer.lineage.tracker import LineageGraph, RULE_ORIGIN
from skifer.observability.checks import CheckStatus

if TYPE_CHECKING:
    from skifer.observability.checks import CheckResult
    from skifer.observability.metadata_store import DatasetRecord


PRODUCER = "https://github.com/morzu117/skifer"
RUN_EVENT_SCHEMA_URL = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"
SCHEMA_FACET_URL = "https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json"
COLUMN_LINEAGE_FACET_URL = "https://openlineage.io/spec/facets/1-2-0/ColumnLineageDatasetFacet.json"
DATA_QUALITY_ASSERTIONS_FACET_URL = (
    "https://openlineage.io/spec/facets/1-1-0/DataQualityAssertionsDatasetFacet.json"
)
SKIFER_FACET_URL = "https://github.com/morzu117/skifer/blob/main/docs/observability.md#openlineage"

_VALID_EVENT_TYPES = frozenset({"START", "COMPLETE", "FAIL"})
_OP_NAME_RE = re.compile(r"^[a-z_][a-z0-9_]*$")


def build_run_event(
    *,
    event_type: str,
    run_id: str,
    event_time: datetime,
    record: "DatasetRecord",
    job_namespace: str,
    dataset_namespace: str,
    check_results: Sequence["CheckResult"] | None = None,
    certification_status: str | None = None,
) -> dict:
    """Build a deterministic OpenLineage RunEvent dict from a DatasetRecord."""
    _validate_arguments(event_type, run_id, event_time, job_namespace, dataset_namespace)

    edges = LineageGraph.from_dict(record.lineage).edges
    target_edges = [edge for edge in edges if edge.target_table == record.target_fqn]

    return {
        "eventTime": _format_event_time(event_time),
        "eventType": event_type,
        "producer": PRODUCER,
        "schemaURL": RUN_EVENT_SCHEMA_URL,
        "run": {"runId": run_id},
        "job": {"namespace": job_namespace, "name": record.target_fqn},
        "inputs": _build_inputs(target_edges, record.target_fqn, dataset_namespace),
        "outputs": [
            _build_output(
                record,
                target_edges,
                dataset_namespace,
                check_results,
                certification_status,
            )
        ],
    }


def _validate_arguments(
    event_type: str,
    run_id: str,
    event_time: datetime,
    job_namespace: str,
    dataset_namespace: str,
) -> None:
    if event_type not in _VALID_EVENT_TYPES:
        raise ValueError(
            f"event_type must be one of {sorted(_VALID_EVENT_TYPES)}, got {event_type!r}"
        )
    if event_time.tzinfo is None or event_time.utcoffset() is None:
        raise ValueError("event_time must be timezone-aware")
    if not run_id:
        raise ValueError("run_id must be a non-empty string")
    if not job_namespace:
        raise ValueError("job_namespace must be a non-empty string")
    if not dataset_namespace:
        raise ValueError("dataset_namespace must be a non-empty string")


def _format_event_time(event_time: datetime) -> str:
    return event_time.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _build_inputs(target_edges, target_fqn: str, dataset_namespace: str) -> list[dict]:
    real_sources = {
        edge.source_table
        for edge in target_edges
        if edge.source_table not in (target_fqn, RULE_ORIGIN)
    }
    return [
        {"namespace": dataset_namespace, "name": name}
        for name in sorted(real_sources)
    ]


def _build_output(
    record: "DatasetRecord",
    target_edges,
    dataset_namespace: str,
    check_results,
    certification_status: str | None,
) -> dict:
    facets: dict = {
        "schema": _build_schema_facet(record),
        "skifer": _build_skifer_facet(record, certification_status),
    }
    if target_edges:
        facets["columnLineage"] = _build_column_lineage_facet(target_edges, dataset_namespace)
    if check_results:
        assertions_facet = _build_assertions_facet(check_results)
        if assertions_facet is not None:
            facets["dataQualityAssertions"] = assertions_facet
    return {
        "namespace": dataset_namespace,
        "name": record.target_fqn,
        "facets": facets,
    }


def _build_schema_facet(record: "DatasetRecord") -> dict:
    fields = []
    for column in record.columns:
        field = {"name": column.name}
        if column.logical_type is not None:
            field["type"] = column.logical_type
        fields.append(field)
    return {"_producer": PRODUCER, "_schemaURL": SCHEMA_FACET_URL, "fields": fields}


def _op_names(transformations: Sequence[str]) -> list[str]:
    names = {t.split(":", 1)[0] for t in transformations}
    return sorted(name for name in names if _OP_NAME_RE.match(name))


def _transformation_for_edge(edge) -> dict:
    if edge.edge_type == "join":
        kind, subtype = "INDIRECT", "JOIN"
    elif edge.edge_type == "metric":
        kind, subtype = "DIRECT", "AGGREGATION"
    elif edge.edge_type == "rule":
        kind, subtype = "DIRECT", "TRANSFORMATION"
    elif edge.transformations:
        kind, subtype = "DIRECT", "TRANSFORMATION"
    else:
        kind, subtype = "DIRECT", "IDENTITY"

    transformation = {"type": kind, "subtype": subtype}
    names = _op_names(edge.transformations)
    if names:
        transformation["description"] = ", ".join(names)
    return transformation


def _build_column_lineage_facet(target_edges, dataset_namespace: str) -> dict:
    edges_by_target: dict[str, list] = {}
    for edge in target_edges:
        edges_by_target.setdefault(edge.target_column, []).append(edge)

    fields = {}
    for target_column, column_edges in edges_by_target.items():
        input_fields = [
            {
                "namespace": dataset_namespace,
                "name": edge.source_table,
                "field": edge.source_column,
                "transformations": [_transformation_for_edge(edge)],
            }
            for edge in column_edges
        ]
        input_fields.sort(key=lambda f: (f["namespace"], f["name"], f["field"]))
        fields[target_column] = {"inputFields": input_fields}

    return {
        "_producer": PRODUCER,
        "_schemaURL": COLUMN_LINEAGE_FACET_URL,
        "fields": fields,
    }


def _build_assertions_facet(check_results: Sequence["CheckResult"]) -> dict | None:
    assertions = []
    for result in check_results:
        if result.status == CheckStatus.SKIPPED:
            continue
        assertion = {
            "assertion": type(result.contract).__name__,
            "success": result.status == CheckStatus.PASS,
            "severity": "error" if result.severity == "critical" else "warn",
        }
        column = getattr(result.contract, "column", None)
        if isinstance(column, str) and column:
            assertion["column"] = column
        assertions.append(assertion)

    if not assertions:
        return None
    return {
        "_producer": PRODUCER,
        "_schemaURL": DATA_QUALITY_ASSERTIONS_FACET_URL,
        "assertions": assertions,
    }


def _build_skifer_facet(record: "DatasetRecord", certification_status: str | None) -> dict:
    facet: dict = {
        "_producer": PRODUCER,
        "_schemaURL": SKIFER_FACET_URL,
        "definitionHash": record.definition_hash,
    }
    if record.contract_version is not None:
        facet["contractVersion"] = record.contract_version
    if record.data_product_id is not None:
        facet["dataProductId"] = record.data_product_id
    if certification_status is not None:
        facet["certification"] = certification_status
    classifications = {
        column.name: column.classification
        for column in record.columns
        if column.classification is not None
    }
    if classifications:
        facet["classifications"] = classifications
    return facet
