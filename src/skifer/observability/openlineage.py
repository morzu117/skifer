"""Pure OpenLineage RunEvent builder — no I/O, no Spark, no network (Plan 36.1).

Emitters (Plan 36.2) live in the same module: `NoOpEmitter`, `InMemoryEmitter` and
`HttpEmitter`. Importing this module never imports `urllib.request` — the only
non-stdlib-safe-to-import-eagerly dependency — which stays confined to `HttpEmitter`
methods, mirroring the lazy-SDK-import discipline of `observability/tracing_exporters.py`.
"""
from __future__ import annotations

import copy
import json
import os
import re
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Mapping, Protocol, Sequence

from skifer.lineage.tracker import LineageGraph, RULE_ORIGIN
from skifer.observability.best_effort import warn_best_effort
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
_COLUMN_NAME_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")

# Documentation only: synthetic source_column markers the tracker (LineageGraph) can
# emit for constants and unresolved rule outputs. Not consulted by _is_real_edge below
# — an unqualified identifier check (allowlist) already rejects these along with any
# other non-column shape (literal:*, lit:*, expr:*, dotted/nested paths).
_LITERAL_COLUMN_MARKER = "<literal>"
_UNKNOWN_COLUMN_MARKER = "<unknown>"


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


def _is_real_edge(edge, target_fqn: str) -> bool:
    """Allowlist: an edge is a real column edge only if it comes from another dataset
    and its source_column is a plain identifier. Rejects literal shorthand
    (literal:ERP, lit:X), expressions, synthetic tracker markers (<literal>,
    <unknown>) and dotted/nested paths (address.city) — none of these are columns the
    consumer can resolve, and none may leave the process as a value (plan D5)."""
    if edge.source_table in (target_fqn, RULE_ORIGIN):
        return False
    return bool(_COLUMN_NAME_RE.fullmatch(edge.source_column))


def _build_inputs(target_edges, target_fqn: str, dataset_namespace: str) -> list[dict]:
    real_sources = {
        edge.source_table
        for edge in target_edges
        if _is_real_edge(edge, target_fqn)
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
    column_lineage_facet = _build_column_lineage_facet(
        target_edges, record.target_fqn, dataset_namespace
    )
    if column_lineage_facet is not None:
        facets["columnLineage"] = column_lineage_facet
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
    return sorted(name for name in names if _OP_NAME_RE.fullmatch(name))


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


def _build_column_lineage_facet(
    target_edges, target_fqn: str, dataset_namespace: str
) -> dict | None:
    edges_by_target: dict[str, list] = {}
    for edge in target_edges:
        if not _is_real_edge(edge, target_fqn):
            continue
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

    if not fields:
        return None
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


OPENLINEAGE_API_KEY_ENV = "OPENLINEAGE_API_KEY"


def _warn_best_effort(message: str) -> None:
    """Emit a RuntimeWarning without ever raising.

    Under a warnings-as-errors filter (`-W error`, `filterwarnings = error`,
    `simplefilter("error")`), `warnings.warn` itself raises — which would defeat the
    only reason these emitters exist: a catalog being down must never crash the
    pipeline it only observes.
    """
    warn_best_effort(message, stacklevel=3)


class LineageEmitter(Protocol):
    """Anything that can send a built RunEvent somewhere. Never raises."""

    def emit(self, event: dict) -> None: ...


class NoOpEmitter:
    """Default emitter: `observability.lineage.emitter: none`. Holds no state."""

    __slots__ = ()

    def emit(self, event: dict) -> None:
        return None


class InMemoryEmitter:
    """Collects emitted events for tests and examples.

    Stores independent deep copies so later in-place mutation of a `record`/`event`
    by the caller can never retroactively alter what was already "emitted".
    """

    def __init__(self) -> None:
        self.events: list[dict] = []

    def emit(self, event: dict) -> None:
        self.events.append(copy.deepcopy(event))


class _NonSuccessResponse(Exception):
    """Internal: an HTTP response outside the 2xx range."""


class HttpEmitter:
    """POST OpenLineage RunEvents over HTTP using only the standard library.

    `emit` never raises: a catalog being unreachable or erroring must never affect
    the pipeline it observes (Plan 36 §1, mirroring `observability/uc_mirror.py`).
    Failures are rate-limited per instance and per failure kind — like
    `_warning_once` in `observability/tracing_exporters.py` — and the warning,
    `repr()` and any log line never disclose the url, endpoint query, event body,
    or API key.
    """

    def __init__(
        self,
        url: str,
        *,
        endpoint: str = "/api/v1/lineage",
        timeout_seconds: float = 5.0,
        environ: Mapping[str, str] | None = None,
        opener: Any = None,
    ) -> None:
        self._url = url
        self._endpoint = endpoint
        self._timeout_seconds = timeout_seconds
        self._environ = environ
        self._opener = opener
        self._warned_kinds: set[str] = set()
        self._warned_kinds_lock = threading.Lock()

    def __repr__(self) -> str:
        return f"HttpEmitter(url=<configured>, endpoint={self._endpoint!r})"

    def emit(self, event: dict) -> None:
        try:
            self._post(event)
        except Exception as exc:
            self._warn_once(type(exc).__name__)

    def _post(self, event: dict) -> None:
        import urllib.request

        body = json.dumps(event, sort_keys=True, separators=(",", ":")).encode("utf-8")
        environ = self._environ if self._environ is not None else os.environ
        api_key = environ.get(OPENLINEAGE_API_KEY_ENV)
        headers = {"Content-Type": "application/json"}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        target = self._url.rstrip("/") + self._endpoint
        request = urllib.request.Request(target, data=body, headers=headers, method="POST")
        opener = self._opener if self._opener is not None else urllib.request.urlopen
        with opener(request, timeout=self._timeout_seconds) as response:
            status = getattr(response, "status", None)
            if status is None:
                status = response.getcode()
            if not 200 <= status < 300:
                raise _NonSuccessResponse(f"HTTP {status}")

    def _warn_once(self, kind: str) -> None:
        with self._warned_kinds_lock:
            if kind in self._warned_kinds:
                return
            self._warned_kinds.add(kind)
        _warn_best_effort(f"[OpenLineage] emission failed: {kind}")


def create_lineage_emitter(config: Any, *, environ: Mapping[str, str] | None = None) -> LineageEmitter:
    """Build the configured emitter without ever raising (Plan 36 §1: non-blocking)."""
    if config.emitter == "none":
        return NoOpEmitter()
    try:
        return HttpEmitter(
            config.url,
            endpoint=config.endpoint,
            timeout_seconds=config.timeout_seconds,
            environ=environ,
        )
    except Exception as exc:
        _warn_best_effort(f"[OpenLineage] emitter construction failed: {type(exc).__name__}")
        return NoOpEmitter()


@dataclass(frozen=True)
class LineageContext:
    """Namespaces resolved once by the engine and shared by every emission point (Plan 36 D6)."""

    job_namespace: str
    dataset_namespace: str


def emit_run_event_best_effort(
    emitter: Any,
    *,
    event_type: str,
    run_id: str,
    record_factory: Any,
    job_namespace: str,
    dataset_namespace: str,
    check_results: Sequence["CheckResult"] | None = None,
    certification_status: str | None = None,
) -> None:
    """Build and emit one RunEvent without ever affecting the caller (Plan 36.3).

    A `NoOpEmitter` returns before `record_factory` runs, so `emitter: none` builds
    nothing. Any failure — record, event or emission — becomes one warning naming
    only the exception class, never its message, which often cites the data.
    """
    if emitter is None or isinstance(emitter, NoOpEmitter):
        return
    try:
        event = build_run_event(
            event_type=event_type,
            run_id=run_id,
            event_time=datetime.now(timezone.utc),
            record=record_factory(),
            job_namespace=job_namespace,
            dataset_namespace=dataset_namespace,
            check_results=check_results,
            certification_status=certification_status,
        )
        emitter.emit(event)
    except Exception as exc:
        _warn_best_effort(f"[OpenLineage] event skipped: {type(exc).__name__}")
