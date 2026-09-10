"""Catalog-first and lazy-loading contracts for governed capabilities."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from skifer.capabilities import CapabilityRegistry, CapabilityRegistryError


def valid_capability() -> dict:
    return {
        "id": "support.create_ticket",
        "version": "1.0.0",
        "owner": "support-platform",
        "description": "Create a support ticket after deterministic checks",
        "mode": "write",
        "executor": "support_ticket_v1",
        "acting_as": "delegated_user",
        "required_scopes": ["tickets:create"],
        "input_schema": {
            "type": "object",
            "additionalProperties": False,
            "required": ["request_id"],
            "properties": {
                "request_id": {"type": "string", "maxLength": 128},
            },
        },
        "preconditions": [{"rule": "service_is_known"}],
        "reversibility": "compensatable",
        "compensation": "support.close_ticket",
        "approval": "supervised",
        "idempotency_key": "request_id",
        "provenance": {
            "policy_uri": "policies/support-ticket-v3.md",
            "policy_hash": "sha256:" + "a" * 64,
        },
    }


def catalog_entry(payload: dict, path: str = "support_create_ticket.yaml") -> dict:
    return {
        "id": payload["id"],
        "version": payload["version"],
        "owner": payload["owner"],
        "description": payload["description"],
        "mode": payload["mode"],
        "approval": payload["approval"],
        "reversibility": payload["reversibility"],
        "path": path,
    }


def write_catalog(root: Path, entries: list[dict]) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / "capability_catalog.yaml").write_text(
        yaml.safe_dump({"capabilities": entries}, sort_keys=False),
        encoding="utf-8",
    )


def write_definition(root: Path, payload: dict, name: str = "support_create_ticket.yaml") -> Path:
    path = root / name
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path


def test_listing_catalog_does_not_read_invalid_definition(tmp_path: Path) -> None:
    payload = valid_capability()
    write_catalog(tmp_path, [catalog_entry(payload)])
    (tmp_path / "support_create_ticket.yaml").write_text("{ invalid", encoding="utf-8")

    registry = CapabilityRegistry(tmp_path)
    assert [item.id for item in registry.list_capabilities()] == [payload["id"]]
    with pytest.raises(CapabilityRegistryError, match="definition could not be loaded"):
        registry.get(payload["id"])


def test_get_caches_definition_without_second_file_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    payload = valid_capability()
    write_catalog(tmp_path, [catalog_entry(payload)])
    definition_path = write_definition(tmp_path, payload)
    registry = CapabilityRegistry(tmp_path)
    original_open = Path.open
    definition_reads = 0

    def counting_open(path: Path, *args, **kwargs):
        nonlocal definition_reads
        if path == definition_path:
            definition_reads += 1
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", counting_open)
    first = registry.get(payload["id"])
    second = registry.get(payload["id"])
    assert first is second
    assert definition_reads == 1


def test_catalog_definition_disagreement_names_both_values(tmp_path: Path) -> None:
    payload = valid_capability()
    entry = catalog_entry(payload)
    entry["version"] = "2.0.0"
    write_catalog(tmp_path, [entry])
    write_definition(tmp_path, payload)

    registry = CapabilityRegistry(tmp_path)
    with pytest.raises(CapabilityRegistryError) as exc_info:
        registry.get(payload["id"])
    message = str(exc_info.value)
    assert "version" in message
    assert "2.0.0" in message
    assert "1.0.0" in message


@pytest.mark.parametrize("path_kind", ["absolute", "parent"])
def test_catalog_rejects_path_escaping_root(tmp_path: Path, path_kind: str) -> None:
    payload = valid_capability()
    path = "/tmp/capability.yaml" if path_kind == "absolute" else "../capability.yaml"
    write_catalog(tmp_path, [catalog_entry(payload, path)])
    with pytest.raises(CapabilityRegistryError, match="path"):
        CapabilityRegistry(tmp_path)


def test_catalog_rejects_symlink_escaping_root(tmp_path: Path) -> None:
    root = tmp_path / "capabilities"
    outside = tmp_path / "outside.yaml"
    outside.write_text("not opened", encoding="utf-8")
    root.mkdir()
    (root / "linked.yaml").symlink_to(outside)
    payload = valid_capability()
    write_catalog(root, [catalog_entry(payload, "linked.yaml")])
    with pytest.raises(CapabilityRegistryError, match="escapes"):
        CapabilityRegistry(root)


def test_duplicate_catalog_ids_are_rejected(tmp_path: Path) -> None:
    payload = valid_capability()
    entry = catalog_entry(payload)
    write_catalog(tmp_path, [entry, dict(entry)])
    with pytest.raises(CapabilityRegistryError, match="duplicate id"):
        CapabilityRegistry(tmp_path)


def test_missing_catalog_returns_empty_registry(tmp_path: Path) -> None:
    registry = CapabilityRegistry(tmp_path)
    assert registry.list_capabilities() == ()


def test_malformed_catalog_is_rejected(tmp_path: Path) -> None:
    tmp_path.mkdir(exist_ok=True)
    (tmp_path / "capability_catalog.yaml").write_text("{ invalid", encoding="utf-8")
    with pytest.raises(CapabilityRegistryError, match="malformed"):
        CapabilityRegistry(tmp_path)


def test_definition_id_must_match_requested_catalog_id(tmp_path: Path) -> None:
    payload = valid_capability()
    write_catalog(tmp_path, [catalog_entry(payload)])
    definition = valid_capability()
    definition["id"] = "support.create_other_ticket"
    write_definition(tmp_path, definition)
    registry = CapabilityRegistry(tmp_path)
    with pytest.raises(CapabilityRegistryError, match="definition declares id"):
        registry.get(payload["id"])


def test_reload_clears_definition_cache(tmp_path: Path) -> None:
    payload = valid_capability()
    write_catalog(tmp_path, [catalog_entry(payload)])
    write_definition(tmp_path, payload)
    registry = CapabilityRegistry(tmp_path)
    first = registry.get(payload["id"])
    registry.reload()
    second = registry.get(payload["id"])
    assert first is not second
