"""Spark-free metadata registry indexing (Plan 31.2)."""
from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import re

from skifer.core.ir import parse_to_ir
from skifer.core.schema_loader import parse_schema
from skifer.lineage.tracker import LineageTracker
from skifer.observability.certification import ContractDefinition
from skifer.observability.metadata_store import ColumnRecord, DatasetRecord
from skifer.semantic.output_projection import OutputProjector


_PLACEHOLDER_RE = re.compile(r"\{\{\s*(\w+)\s*\}\}")


def dataset_record_from_definition(
    definition: ContractDefinition,
    target_fqn: str,
    run_id: str,
) -> DatasetRecord:
    """Reconstruct the metadata available during publication crash recovery."""
    payload = json.loads(definition.canonical_json)
    output = payload["contract"]["output"]
    columns = tuple(
        ColumnRecord(
            name=field["name"],
            logical_type=field.get("logical_type"),
            classification=field.get("classification"),
            description=field.get("description"),
        )
        for field in output
    )
    return DatasetRecord(
        target_fqn=target_fqn,
        pipeline_path=definition.data_product_id,
        data_product_id=definition.data_product_id,
        contract_version=definition.contract_version,
        definition_hash=definition.definition_hash,
        owner=definition.owner,
        columns=columns,
        indexed_at=datetime.now(timezone.utc),
        last_run_id=run_id,
        lineage={},
    )


def index_schema(
    schema_dict: dict,
    path: str,
    *,
    target_fqn: str | None = None,
    last_run_id: str | None = None,
    now: datetime | None = None,
) -> DatasetRecord:
    """Build a deterministic DatasetRecord without opening Spark or writing state."""
    parsed = parse_to_ir(schema_dict)
    projected = OutputProjector().project(parsed)
    fqn = (
        target_fqn
        or projected.target_hint
        or projected.data_product_id
        or (parsed.tables[0].name + "_output" if parsed.tables else "unknown")
    )

    graph = LineageTracker.from_schema(schema_dict, target_name=fqn)
    declared = {field.name: field for field in parsed.contract_output}
    columns = tuple(
        ColumnRecord(
            name=field.name,
            logical_type=field.logical_type,
            classification=(
                declared[field.name].classification
                if field.name in declared
                else None
            ),
            description=(
                declared[field.name].description
                if field.name in declared
                else None
            ),
            sources=field.source_fields,
        )
        for field in projected.fields
    )
    data_product = parsed.data_product
    return DatasetRecord(
        target_fqn=fqn,
        pipeline_path=path,
        data_product_id=(data_product.id if data_product else projected.data_product_id),
        contract_version=(data_product.version if data_product else None),
        definition_hash=projected.definition_hash,
        owner=(data_product.owner_label if data_product else None),
        columns=columns,
        indexed_at=now or datetime.now(timezone.utc),
        last_run_id=last_run_id,
        lineage=graph.to_dict(),
    )


def upsert_index_record(store, record: DatasetRecord) -> bool:
    """Persist an index record and attach run_id after an idempotent no-op."""
    wrote = store.upsert(record)
    if not wrote and record.last_run_id is not None:
        attach = getattr(store, "attach_run_id", None)
        if callable(attach):
            attach(record.target_fqn, record.definition_hash, record.last_run_id)
    return wrote


def index_from_path(
    path: str,
    store,
    *,
    target_fqn: str | None = None,
    last_run_id: str | None = None,
) -> bool:
    """Load one pipeline YAML with sentinel params, build a record, and upsert it."""
    yaml_path = Path(path)
    yaml_text = yaml_path.read_text(encoding="utf-8")
    schema_dict = parse_schema(
        yaml_text,
        params=_sentinel_params(yaml_text),
        base_dir=str(yaml_path.parent),
    )
    record = index_schema(
        schema_dict,
        str(yaml_path),
        target_fqn=target_fqn,
        last_run_id=last_run_id,
    )
    return upsert_index_record(store, record)


def _sentinel_params(yaml_text: str) -> dict[str, str]:
    return {
        key: f"__sentinel_{key}__"
        for key in _PLACEHOLDER_RE.findall(yaml_text)
    }
