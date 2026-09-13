"""Transport-independent, fail-closed service for governed agent data access."""

from __future__ import annotations

import base64
from dataclasses import dataclass
from datetime import datetime
import hashlib
import hmac
import json
import re
from typing import Any, Generic, TypeVar

from skifer.agentic.resolver import SemanticQuery
from skifer.observability.certification import ContractDefinition
from skifer.observability.certification_store import Certification
from skifer.semantic.access_policy import ConsumerContext
from skifer.semantic.evidence import EvidencePolicy, SemanticEvidence
from skifer.services.context import (
    HARD_MAX_FILTERS,
    HARD_MAX_FILTER_VALUE_LENGTH,
    HARD_MAX_PAGE_SIZE,
    HARD_MAX_QUERY_ROWS,
    _CONSUMER_SCOPE_ALLOWLIST,
    AgentReadyDataError,
    InvalidCursor,
    InvalidRequest,
    LimitExceeded,
    RequestContext,
    ResourceNotFound,
    ResourceUnavailable,
    ScopeDenied,
    SerializationError,
    ServiceLimits,
    require_scope,
)
from skifer.services.serialization import row_to_json

_CURSOR_VERSION = 1
_CURSOR_DOMAIN = b"skifer.agent-ready.models.v1\0"

MAX_CONTRACT_ID_LENGTH = 256
_CONTRACT_ID_RE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_.-]{0,255}$")
_SEMVER_RE = re.compile(r"^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$")


@dataclass(frozen=True)
class ModelSummary:
    key: str
    description: str
    layer: str | None
    tags: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "description": self.description,
            "layer": self.layer,
            "tags": list(self.tags),
        }


@dataclass(frozen=True)
class GovernedModelView:
    key: str
    description: str
    layer: str | None
    tags: tuple[str, ...]
    dimensions: tuple[str, ...]
    metrics: tuple[str, ...]
    entities: tuple[str, ...]
    related_models: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "description": self.description,
            "layer": self.layer,
            "tags": list(self.tags),
            "dimensions": list(self.dimensions),
            "metrics": list(self.metrics),
            "entities": list(self.entities),
            "related_models": list(self.related_models),
        }


@dataclass(frozen=True)
class ContractFieldView:
    name: str
    logical_type: str | None
    required: bool
    unique: bool
    classification: str | None
    entity: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "logical_type": self.logical_type,
            "required": self.required,
            "unique": self.unique,
            "classification": self.classification,
            "entity": self.entity,
        }


@dataclass(frozen=True)
class ContractSemanticView:
    model_key: str | None
    entity: str | None
    default_time_dimension: str | None
    dimensions: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "model_key": self.model_key,
            "entity": self.entity,
            "default_time_dimension": self.default_time_dimension,
            "dimensions": list(self.dimensions),
        }


@dataclass(frozen=True)
class ContractView:
    contract_id: str
    contract_version: str
    definition_hash: str
    data_product_id: str
    owner: str | None
    status: str
    created_at: str | None
    grain: tuple[str, ...]
    output: tuple[ContractFieldView, ...]
    semantic: ContractSemanticView | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "contract_id": self.contract_id,
            "contract_version": self.contract_version,
            "definition_hash": self.definition_hash,
            "data_product_id": self.data_product_id,
            "owner": self.owner,
            "status": self.status,
            "created_at": self.created_at,
            "grain": list(self.grain),
            "output": [field.to_dict() for field in self.output],
            "semantic": self.semantic.to_dict() if self.semantic is not None else None,
        }


@dataclass(frozen=True)
class CertificationView:
    dataset: str
    consumer_class: str
    status: str
    contract_version: str | None
    definition_hash: str | None
    certified_at: str | None
    checks_passed: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset": self.dataset,
            "consumer_class": self.consumer_class,
            "status": self.status,
            "contract_version": self.contract_version,
            "definition_hash": self.definition_hash,
            "certified_at": self.certified_at,
            "checks_passed": self.checks_passed,
        }


@dataclass(frozen=True)
class LineageEdgeView:
    source_table: str
    source_column: str
    target_table: str
    target_column: str
    transformations: tuple[str, ...]
    edge_type: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_table": self.source_table,
            "source_column": self.source_column,
            "target_table": self.target_table,
            "target_column": self.target_column,
            "transformations": list(self.transformations),
            "edge_type": self.edge_type,
        }


@dataclass(frozen=True)
class LineageView:
    dataset: str
    column: str
    upstream: tuple[LineageEdgeView, ...]
    downstream: tuple[LineageEdgeView, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset": self.dataset,
            "column": self.column,
            "upstream": [edge.to_dict() for edge in self.upstream],
            "downstream": [edge.to_dict() for edge in self.downstream],
        }


T = TypeVar("T")


@dataclass(frozen=True)
class Page(Generic[T]):
    items: tuple[T, ...]
    next_cursor: str | None
    total: int | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "items": [item.to_dict() for item in self.items],
            "next_cursor": self.next_cursor,
            "total": self.total,
        }


@dataclass(frozen=True)
class QueryEnvelope:
    rows: list[dict[str, Any]]
    evidence: dict[str, Any]
    truncated: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "rows": self.rows,
            "evidence": self.evidence,
            "truncated": self.truncated,
        }


class AgentReadyDataService:
    """The sole application security boundary intended for future MCP adapters."""

    def __init__(self, semantic_engine, *, lineage_graph=None, limits=None):
        self.semantic_engine = semantic_engine
        self.lineage_graph = lineage_graph
        if limits is not None and not isinstance(limits, ServiceLimits):
            raise TypeError("limits must be a ServiceLimits instance.")
        self.limits = limits or ServiceLimits()

    def list_models(
        self, ctx: RequestContext, cursor: str | None = None, limit: int = 50
    ) -> Page[ModelSummary]:
        require_scope(ctx, "models:read")
        self._validate_limit(limit, self.limits.max_page_size, "pagination limit")
        entries = self._catalog_entries()
        keys = [self._required_text(entry, "key", "catalog model") for entry in entries]
        start = self._decode_cursor(cursor, keys) if cursor is not None else 0
        selected = entries[start : start + limit]
        end = start + len(selected)
        next_cursor = self._encode_cursor(keys[end - 1], keys) if end < len(entries) else None
        return Page(
            items=tuple(self._model_summary(entry) for entry in selected),
            next_cursor=next_cursor,
            total=len(entries),
        )

    def get_model(self, ctx: RequestContext, key: str) -> GovernedModelView:
        require_scope(ctx, "models:read")
        entry = self._model_entry(key)
        return GovernedModelView(
            key=self._required_text(entry, "key", "catalog model"),
            description=self._optional_text(entry.get("description"), "description") or "",
            layer=self._optional_text(entry.get("layer"), "layer"),
            tags=self._text_tuple(entry.get("tags", []), "tags"),
            dimensions=self._text_tuple(entry.get("dimensions", []), "dimensions"),
            metrics=self._text_tuple(entry.get("metrics", []), "metrics"),
            entities=self._text_tuple(entry.get("entities", []), "entities"),
            related_models=self._text_tuple(
                entry.get("related_models", []), "related_models"
            ),
        )

    def get_contract(
        self, ctx: RequestContext, contract_id: str, version: str
    ) -> ContractView:
        require_scope(ctx, "contracts:read")
        # Shape-checked here, at the boundary, on top of the storage layer's own
        # escaping. These two values are the only free text an external caller
        # gets to put on a path that reaches SQL, so they are constrained to
        # what a contract identifier and a semantic version can actually be.
        self._validate_contract_ref(contract_id, version)
        store = self._certification_store()
        reader = getattr(store, "get_contract", None)
        if reader is None or not callable(reader):
            raise ResourceUnavailable(
                "The certification store does not support governed contract reads."
            )
        definition = reader(contract_id, version)
        if definition is None:
            raise ResourceNotFound(
                f"Contract '{contract_id}' version '{version}' was not found."
            )
        if not isinstance(definition, ContractDefinition):
            raise ResourceUnavailable("The certification store returned an invalid contract.")
        return self._contract_view(definition)

    def get_certification(
        self, ctx: RequestContext, dataset: str
    ) -> CertificationView:
        require_scope(ctx, "contracts:read")
        physical_dataset = self._resolve_dataset(dataset)
        certification = self._certification_store().get_certification(
            physical_dataset, consumer_class=ctx.consumer_class
        )
        if not isinstance(certification, Certification):
            raise ResourceUnavailable(
                "The certification store returned an invalid certification."
            )
        return CertificationView(
            dataset=certification.dataset,
            consumer_class=certification.consumer_class,
            status=certification.status,
            contract_version=certification.contract_version,
            definition_hash=certification.definition_hash,
            certified_at=self._datetime_text(certification.certified_at, "certified_at"),
            checks_passed=certification.checks_passed,
        )

    def get_lineage(
        self, ctx: RequestContext, dataset: str, column: str
    ) -> LineageView:
        require_scope(ctx, "lineage:read")
        if self.lineage_graph is None:
            raise ResourceUnavailable("No lineage graph is configured.")
        upstream = self.lineage_graph.upstream(dataset, column)
        downstream = self.lineage_graph.downstream(dataset, column)
        if not upstream and not downstream:
            raise ResourceNotFound(
                f"Lineage for dataset '{dataset}' and column '{column}' was not found."
            )
        return LineageView(
            dataset=dataset,
            column=column,
            upstream=tuple(self._lineage_edge(edge) for edge in upstream),
            downstream=tuple(self._lineage_edge(edge) for edge in downstream),
        )

    def query(
        self, ctx: RequestContext, query: SemanticQuery, limit: int = 100
    ) -> QueryEnvelope:
        require_scope(ctx, "query:execute")
        self._validate_limit(limit, self.limits.max_query_rows, "query limit")
        self._validate_query(query)
        self._model_entry(query.model_name)
        consumer_context = ConsumerContext(
            consumer_id=ctx.subject,
            consumer_class=ctx.consumer_class,
            scopes=frozenset(
                scope for scope in ctx.scopes if scope in _CONSUMER_SCOPE_ALLOWLIST
            ),
            trace_id=ctx.trace_context.trace_id,
        )
        result = self.semantic_engine.query_with_evidence(
            query,
            consumer_context=consumer_context,
            evidence_policy=EvidencePolicy.redacted(),
        )
        if not isinstance(result.evidence, SemanticEvidence):
            raise ResourceUnavailable("The semantic engine returned invalid evidence.")
        collected = result.dataframe.limit(limit + 1).collect()
        truncated = len(collected) > limit
        rows = [self._row_to_json(row, index) for index, row in enumerate(collected[:limit])]
        return QueryEnvelope(
            rows=rows,
            evidence=result.evidence.to_dict(),
            truncated=truncated,
        )

    def _catalog_entries(self) -> list[dict]:
        entries = self.semantic_engine.list_models()
        if not isinstance(entries, list) or not all(isinstance(item, dict) for item in entries):
            raise ResourceUnavailable("The semantic catalog returned an invalid model list.")
        return sorted(entries, key=lambda entry: self._required_text(entry, "key", "catalog model"))

    def _model_entry(self, key: str) -> dict:
        if not isinstance(key, str) or not key:
            raise ResourceNotFound("A non-empty governed model key is required.")
        try:
            entry = self.semantic_engine.get_model_summary(key)
        except (KeyError, ValueError) as exc:
            raise ResourceNotFound(f"Model '{key}' was not found.") from exc
        if not isinstance(entry, dict) or entry.get("key") != key:
            raise ResourceUnavailable("The semantic catalog returned an invalid model entry.")
        return entry

    def _resolve_dataset(self, dataset: str) -> str:
        if not isinstance(dataset, str) or not dataset:
            raise ResourceNotFound("A non-empty governed dataset identifier is required.")
        matches = []
        for entry in self._catalog_entries():
            key = entry.get("key")
            table = entry.get("table")
            if dataset == key and isinstance(table, str) and table:
                matches.append(table)
            elif dataset == table and isinstance(table, str) and table:
                matches.append(table)
        unique = sorted(set(matches))
        if len(unique) != 1:
            raise ResourceNotFound(f"Dataset '{dataset}' was not found in the governed catalog.")
        return unique[0]

    def _certification_store(self):
        store = getattr(self.semantic_engine, "certification_store", None)
        if store is None:
            raise ResourceUnavailable("No certification store is configured.")
        return store

    def _validate_query(self, query: SemanticQuery) -> None:
        if not isinstance(query, SemanticQuery):
            raise InvalidRequest("query must be a SemanticQuery instance.")
        if query.mode != "query" or query.view_name is not None:
            raise InvalidRequest("Only read-only semantic queries are accepted.")
        if not isinstance(query.filters, list):
            raise InvalidRequest("Semantic query filters must be a list.")
        if len(query.filters) > self.limits.max_filters:
            raise LimitExceeded(
                f"A query may contain at most {self.limits.max_filters} filters."
            )
        for index, filter_definition in enumerate(query.filters):
            if not isinstance(filter_definition, dict):
                raise InvalidRequest(f"Filter {index} must be an object.")
            if "value" in filter_definition:
                self._validate_filter_value(filter_definition["value"], index)

    def _validate_filter_value(self, value: Any, index: int) -> None:
        values = value if isinstance(value, list) else [value]
        for item in values:
            if isinstance(item, str) and len(item) > self.limits.max_filter_value_length:
                raise LimitExceeded(
                    f"Filter {index} value exceeds {self.limits.max_filter_value_length} characters."
                )

    @staticmethod
    def _validate_contract_ref(contract_id: str, version: str) -> None:
        if not isinstance(contract_id, str) or not _CONTRACT_ID_RE.match(contract_id):
            raise InvalidRequest(
                "A contract id must be dotted alphanumeric text of at most "
                f"{MAX_CONTRACT_ID_LENGTH} characters."
            )
        if not isinstance(version, str) or not _SEMVER_RE.match(version):
            raise InvalidRequest("A contract version must be a semantic version.")

    @staticmethod
    def _validate_limit(limit: int, maximum: int, name: str) -> None:
        if type(limit) is not int or not 1 <= limit <= maximum:
            raise LimitExceeded(f"{name} must be an integer in 1..{maximum}; got {limit!r}.")

    def _encode_cursor(self, after: str, keys: list[str]) -> str:
        # Integrity, NOT authentication: this digest is unkeyed, and the domain
        # constant is public, so a caller can forge a cursor. That is acceptable
        # only because a cursor grants nothing — `models:read` is checked before
        # decoding, and it can address no model the caller cannot already list.
        # Do not reuse this shape for anything that carries authority.
        fingerprint = self._catalog_fingerprint(keys)
        signed = f"{after}\0{fingerprint}".encode("utf-8")
        payload = {
            "after": after,
            "catalog": fingerprint,
            "sig": hashlib.sha256(_CURSOR_DOMAIN + signed).hexdigest(),
            "v": _CURSOR_VERSION,
        }
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return base64.urlsafe_b64encode(raw).decode("ascii")

    def _decode_cursor(self, cursor: str, keys: list[str]) -> int:
        try:
            if not isinstance(cursor, str) or not cursor:
                raise ValueError
            raw = base64.b64decode(cursor.encode("ascii"), altchars=b"-_", validate=True)
            payload = json.loads(raw.decode("utf-8"))
            if set(payload) != {"after", "catalog", "sig", "v"}:
                raise ValueError
            after = payload["after"]
            fingerprint = payload["catalog"]
            if (
                payload["v"] != _CURSOR_VERSION
                or not isinstance(after, str)
                or fingerprint != self._catalog_fingerprint(keys)
            ):
                raise ValueError
            expected = hashlib.sha256(
                _CURSOR_DOMAIN + f"{after}\0{fingerprint}".encode("utf-8")
            ).hexdigest()
            if not isinstance(payload["sig"], str) or not hmac.compare_digest(
                payload["sig"], expected
            ):
                raise ValueError
            position = keys.index(after)
            if position >= len(keys) - 1:
                raise ValueError
            return position + 1
        except (UnicodeError, ValueError, TypeError, json.JSONDecodeError) as exc:
            raise InvalidCursor("The model pagination cursor is invalid or stale.") from exc

    @staticmethod
    def _catalog_fingerprint(keys: list[str]) -> str:
        return hashlib.sha256("\0".join(keys).encode("utf-8")).hexdigest()

    def _model_summary(self, entry: dict) -> ModelSummary:
        return ModelSummary(
            key=self._required_text(entry, "key", "catalog model"),
            description=self._optional_text(entry.get("description"), "description") or "",
            layer=self._optional_text(entry.get("layer"), "layer"),
            tags=self._text_tuple(entry.get("tags", []), "tags"),
        )

    def _contract_view(self, definition: ContractDefinition) -> ContractView:
        try:
            payload = json.loads(definition.canonical_json)
        except (TypeError, json.JSONDecodeError) as exc:
            raise ResourceUnavailable("The stored contract payload is invalid.") from exc
        if not isinstance(payload, dict):
            raise ResourceUnavailable("The stored contract payload is invalid.")
        contract = payload.get("contract", {})
        if not isinstance(contract, dict):
            raise ResourceUnavailable("The stored contract body is invalid.")
        output_raw = contract.get("output", [])
        if not isinstance(output_raw, list) or not all(
            isinstance(field, dict) for field in output_raw
        ):
            raise ResourceUnavailable("The stored contract output is invalid.")
        fields = tuple(
            ContractFieldView(
                name=self._required_text(field, "name", "contract field"),
                logical_type=self._optional_text(field.get("logical_type"), "logical_type"),
                required=self._optional_bool(field.get("required", False), "required"),
                unique=self._optional_bool(field.get("unique", False), "unique"),
                classification=self._optional_text(
                    field.get("classification"), "classification"
                ),
                entity=self._optional_text(field.get("entity"), "entity"),
            )
            for field in output_raw
        )
        semantic_raw = payload.get("semantic")
        if semantic_raw is not None and not isinstance(semantic_raw, dict):
            raise ResourceUnavailable("The stored contract semantic block is invalid.")
        semantic = (
            ContractSemanticView(
                model_key=self._optional_text(semantic_raw.get("model_key"), "model_key"),
                entity=self._optional_text(semantic_raw.get("entity"), "entity"),
                default_time_dimension=self._optional_text(
                    semantic_raw.get("default_time_dimension"), "default_time_dimension"
                ),
                dimensions=self._text_tuple(
                    semantic_raw.get("dimensions", []), "dimensions"
                ),
            )
            if semantic_raw is not None
            else None
        )
        return ContractView(
            contract_id=definition.contract_id,
            contract_version=definition.contract_version,
            definition_hash=definition.definition_hash,
            data_product_id=definition.data_product_id,
            owner=definition.owner,
            status=definition.status,
            created_at=self._datetime_text(definition.created_at, "created_at"),
            grain=self._text_tuple(contract.get("grain", []), "grain"),
            output=fields,
            semantic=semantic,
        )

    @staticmethod
    def _lineage_edge(edge) -> LineageEdgeView:
        return LineageEdgeView(
            source_table=edge.source_table,
            source_column=edge.source_column,
            target_table=edge.target_table,
            target_column=edge.target_column,
            transformations=tuple(edge.transformations),
            edge_type=edge.edge_type,
        )

    def _row_to_json(self, row: Any, index: int) -> dict[str, Any]:
        return row_to_json(row, index)

    @staticmethod
    def _required_text(source: dict, key: str, label: str) -> str:
        value = source.get(key)
        if not isinstance(value, str) or not value:
            raise ResourceUnavailable(f"The {label} has an invalid '{key}' field.")
        return value

    @staticmethod
    def _optional_text(value: Any, field_name: str) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str):
            raise ResourceUnavailable(f"Field '{field_name}' must be text or null.")
        return value

    @staticmethod
    def _optional_bool(value: Any, field_name: str) -> bool:
        if not isinstance(value, bool):
            raise ResourceUnavailable(f"Field '{field_name}' must be boolean.")
        return value

    @staticmethod
    def _text_tuple(value: Any, field_name: str) -> tuple[str, ...]:
        if not isinstance(value, (list, tuple)) or not all(
            isinstance(item, str) for item in value
        ):
            raise ResourceUnavailable(f"Field '{field_name}' must be a list of text values.")
        return tuple(value)

    @staticmethod
    def _datetime_text(value: datetime | None, field_name: str) -> str | None:
        if value is None:
            return None
        if not isinstance(value, datetime):
            raise ResourceUnavailable(f"Field '{field_name}' must be a datetime or null.")
        return value.isoformat()


__all__ = [
    "HARD_MAX_PAGE_SIZE",
    "HARD_MAX_QUERY_ROWS",
    "HARD_MAX_FILTERS",
    "HARD_MAX_FILTER_VALUE_LENGTH",
    "_CONSUMER_SCOPE_ALLOWLIST",
    "AgentReadyDataError",
    "ScopeDenied",
    "InvalidRequest",
    "LimitExceeded",
    "InvalidCursor",
    "ResourceNotFound",
    "ResourceUnavailable",
    "SerializationError",
    "RequestContext",
    "require_scope",
    "ServiceLimits",
    "ModelSummary",
    "GovernedModelView",
    "ContractFieldView",
    "ContractSemanticView",
    "ContractView",
    "CertificationView",
    "LineageEdgeView",
    "LineageView",
    "Page",
    "QueryEnvelope",
    "AgentReadyDataService",
]
