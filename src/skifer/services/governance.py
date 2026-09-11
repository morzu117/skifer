"""Transport-neutral reads over the existing certification store."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import inspect
import json
from typing import TYPE_CHECKING, Any

from skifer.observability.certification import ContractDefinition
from skifer.observability.certification_store import Certification, RunEvent
from skifer.services.context import (
    HARD_MAX_PAGE_SIZE,
    RequestContext,
    ResourceNotFound,
    ResourceUnavailable,
    SCOPE_CONTRACTS_READ,
    require_scope,
)
from skifer.services.serialization import row_to_json, to_json_value

if TYPE_CHECKING:
    from skifer.agentic.data_service import CertificationView, ContractView


@dataclass(frozen=True)
class ContractVersionView:
    contract_id: str
    versions: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {"contract_id": self.contract_id, "versions": list(self.versions)}


@dataclass(frozen=True)
class DataProductView:
    data_product_id: str
    latest_version: str | None
    owner: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "data_product_id": self.data_product_id,
            "latest_version": self.latest_version,
            "owner": self.owner,
        }


@dataclass(frozen=True)
class QuarantineView:
    dataset: str
    rows: tuple[dict[str, Any], ...]
    truncated: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset": self.dataset,
            "rows": [dict(row) for row in self.rows],
            "truncated": self.truncated,
        }


class GovernanceService:
    """Governed, allowlisted access to certification data."""

    def __init__(self, certification_store, *, max_rows: int = HARD_MAX_PAGE_SIZE):
        if not isinstance(max_rows, int) or isinstance(max_rows, bool) or max_rows < 1:
            raise ValueError("max_rows must be a positive integer.")
        self._store = certification_store
        self._max_rows = min(max_rows, HARD_MAX_PAGE_SIZE)

    def get_contract(
        self, ctx: RequestContext, contract_id: str, version: str
    ) -> ContractView:
        require_scope(ctx, SCOPE_CONTRACTS_READ)
        reader = self._reader("get_contract", "governed contract reads")
        definition = reader(contract_id, version)
        if definition is None:
            raise ResourceNotFound(
                f"Contract '{contract_id}' version '{version}' was not found."
            )
        if not isinstance(definition, ContractDefinition):
            raise ResourceUnavailable("The certification store returned an invalid contract.")
        return _contract_view(definition)

    def list_certification_history(
        self, ctx: RequestContext, dataset: str, limit: int = 50
    ) -> list[dict[str, Any]]:
        require_scope(ctx, SCOPE_CONTRACTS_READ)
        reader = self._reader("list_history", "certification history reads")
        events = reader(dataset, limit=limit)
        if not isinstance(events, (list, tuple)) or not all(
            isinstance(event, RunEvent) for event in events
        ):
            raise ResourceUnavailable(
                "The certification store returned invalid certification history."
            )
        return [_run_event_dict(event) for event in events]

    def get_certification(
        self, ctx: RequestContext, dataset: str
    ) -> CertificationView:
        from skifer.agentic.data_service import CertificationView

        require_scope(ctx, SCOPE_CONTRACTS_READ)
        reader = self._reader("get_certification", "certification reads")
        certification = reader(dataset, consumer_class=ctx.consumer_class)
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
            certified_at=_datetime_text(certification.certified_at, "certified_at"),
            checks_passed=certification.checks_passed,
        )

    def read_quarantine(
        self, ctx: RequestContext, dataset: str, limit: int = 50
    ) -> QuarantineView:
        require_scope(ctx, SCOPE_CONTRACTS_READ)
        if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
            raise ValueError("limit must be a positive integer.")
        row_limit = min(limit, self._max_rows)
        reader = self._reader("read_quarantine", "quarantine reads")
        result = _read_with_optional_limit(reader, dataset, row_limit + 1)
        rows = _bounded_rows(result, row_limit + 1)
        return QuarantineView(
            dataset=dataset,
            rows=tuple(row_to_json(row, index) for index, row in enumerate(rows[:row_limit])),
            truncated=len(rows) > row_limit,
        )

    def _reader(self, name: str, operation: str):
        if self._store is None:
            raise ResourceUnavailable("The certification store is unavailable.")
        reader = getattr(self._store, name, None)
        if reader is None or not callable(reader):
            raise ResourceUnavailable(
                f"The certification store does not support {operation}."
            )
        return reader


def _read_with_optional_limit(reader, dataset: str, limit: int):
    try:
        inspect.signature(reader).bind(dataset, limit=limit)
    except (TypeError, ValueError):
        return reader(dataset)
    return reader(dataset, limit=limit)


def _bounded_rows(result: Any, fetch_limit: int) -> list[Any]:
    take = getattr(result, "take", None)
    if callable(take):
        rows = take(fetch_limit)
    else:
        collect = getattr(result, "collect", None)
        rows = collect() if callable(collect) else result
    if not isinstance(rows, (list, tuple)):
        try:
            rows = list(rows)
        except TypeError as exc:
            raise ResourceUnavailable(
                "The certification store returned invalid quarantine rows."
            ) from exc
    return list(rows[:fetch_limit])


def _contract_view(definition: ContractDefinition) -> ContractView:
    from skifer.agentic.data_service import (
        ContractFieldView,
        ContractSemanticView,
        ContractView,
    )

    try:
        payload = json.loads(definition.canonical_json)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ResourceUnavailable("The stored contract payload is invalid.") from exc
    if not isinstance(payload, dict):
        raise ResourceUnavailable("The stored contract payload is invalid.")
    contract = payload.get("contract", {})
    if not isinstance(contract, dict):
        raise ResourceUnavailable("The stored contract body is invalid.")
    output = contract.get("output", [])
    if not isinstance(output, list) or not all(isinstance(field, dict) for field in output):
        raise ResourceUnavailable("The stored contract output is invalid.")
    fields = tuple(
        ContractFieldView(
            name=_required_text(field.get("name"), "name"),
            logical_type=_optional_text(field.get("logical_type"), "logical_type"),
            required=_bool(field.get("required", False), "required"),
            unique=_bool(field.get("unique", False), "unique"),
            classification=_optional_text(field.get("classification"), "classification"),
            entity=_optional_text(field.get("entity"), "entity"),
        )
        for field in output
    )
    semantic_raw = payload.get("semantic")
    if semantic_raw is not None and not isinstance(semantic_raw, dict):
        raise ResourceUnavailable("The stored contract semantic block is invalid.")
    semantic = None
    if semantic_raw is not None:
        dimensions = semantic_raw.get("dimensions", [])
        if not isinstance(dimensions, (list, tuple)) or not all(
            isinstance(item, str) for item in dimensions
        ):
            raise ResourceUnavailable("Field 'dimensions' must be a list of text values.")
        semantic = ContractSemanticView(
            model_key=_optional_text(semantic_raw.get("model_key"), "model_key"),
            entity=_optional_text(semantic_raw.get("entity"), "entity"),
            default_time_dimension=_optional_text(
                semantic_raw.get("default_time_dimension"), "default_time_dimension"
            ),
            dimensions=tuple(dimensions),
        )
    grain = contract.get("grain", [])
    if not isinstance(grain, (list, tuple)) or not all(isinstance(item, str) for item in grain):
        raise ResourceUnavailable("Field 'grain' must be a list of text values.")
    return ContractView(
        contract_id=definition.contract_id,
        contract_version=definition.contract_version,
        definition_hash=definition.definition_hash,
        data_product_id=definition.data_product_id,
        owner=definition.owner,
        status=definition.status,
        created_at=_datetime_text(definition.created_at, "created_at"),
        grain=tuple(grain),
        output=fields,
        semantic=semantic,
    )


def _run_event_dict(event: RunEvent) -> dict[str, Any]:
    return {
        "event_id": event.event_id,
        "run_id": event.run_id,
        "dataset": event.dataset,
        "state": event.state,
        "contract_id": event.contract_id,
        "contract_version": event.contract_version,
        "definition_hash": event.definition_hash,
        "occurred_at": to_json_value(event.occurred_at, "occurred_at"),
        "target_fqn": event.target_fqn,
        "staging_fqn": event.staging_fqn,
        "quarantine_fqn": event.quarantine_fqn,
    }


def _required_text(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ResourceUnavailable(f"Field '{field_name}' must be non-empty text.")
    return value


def _optional_text(value: Any, field_name: str) -> str | None:
    if value is not None and not isinstance(value, str):
        raise ResourceUnavailable(f"Field '{field_name}' must be text or null.")
    return value


def _bool(value: Any, field_name: str) -> bool:
    if not isinstance(value, bool):
        raise ResourceUnavailable(f"Field '{field_name}' must be boolean.")
    return value


def _datetime_text(value: datetime | None, field_name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, datetime):
        raise ResourceUnavailable(f"Field '{field_name}' must be a datetime or null.")
    return value.isoformat()


__all__ = [
    "ContractVersionView",
    "DataProductView",
    "GovernanceService",
    "QuarantineView",
]
