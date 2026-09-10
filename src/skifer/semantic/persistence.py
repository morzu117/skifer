"""Shared persistence helpers for semantic model files and the catalog.

Both the LLM builder path (`SemanticBuilder`) and the deterministic CLI path
(`skifer semantic sync --promote`) write semantic model YAML and register a
catalog entry. Keeping one implementation here stops the two paths from drifting
into catalogs with different shapes for the same model.
"""
from __future__ import annotations

import datetime
import os
from pathlib import Path
import tempfile
from typing import Any, Sequence

import yaml

from .domain import parse_entities, parse_relationships


def write_yaml_atomic(path: str | Path, payload: dict) -> str:
    """Serialize ``payload`` to ``path`` atomically.

    The temp file is created in the destination directory so ``os.replace`` is a
    same-filesystem rename, and it is removed if serialization fails — a reader
    never observes a half-written model.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    content = yaml.safe_dump(
        payload,
        sort_keys=False,
        allow_unicode=True,
        default_flow_style=False,
    )
    fd, temp_path = tempfile.mkstemp(
        dir=str(path.parent),
        prefix=f".{path.stem}.",
        suffix=".tmp",
        text=True,
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(content)
        os.replace(temp_path, path)
    except Exception:
        try:
            os.unlink(temp_path)
        except FileNotFoundError:
            pass
        raise
    return str(path)


def build_catalog_entry(
    payload: dict,
    *,
    model_key: str | None = None,
    extra_tags: Sequence[str] = (),
) -> dict[str, Any]:
    """Build the lightweight catalog entry for one semantic model payload.

    ``extra_tags`` carries caller-supplied tags (the builder accepts them, the
    CLI promote path does not). Auto-derived tags are sorted so the catalog does
    not churn between runs — set iteration order varies with hash randomization.
    """
    model = payload["models"][0]
    key = model_key or model["key"]

    auto_tags = sorted(
        {
            model.get("layer", ""),
            key,
            *(model.get("table", "").split(".")[:1]),
        }
        - {""}
    )
    all_tags = list(dict.fromkeys([*auto_tags, *extra_tags, *model.get("tags", [])]))
    entities = parse_entities(model)
    relationships = parse_relationships(model)

    return {
        "key": key,
        "file": f"{key}.yaml",
        "layer": model.get("layer", ""),
        "table": model.get("table", ""),
        "description": model.get("description", ""),
        "tags": all_tags,
        "dimensions": [dimension["name"] for dimension in model.get("dimensions", [])],
        "metrics": [metric["name"] for metric in model.get("metrics", [])],
        "entities": [entity.name for entity in entities],
        "related_models": sorted(
            {
                relationship.to_entity.model_key
                for relationship in relationships
                if relationship.to_entity.model_key
            }
        ),
        "base_filter": model.get("base_filter"),
        "generated_at": datetime.date.today().isoformat(),
    }
