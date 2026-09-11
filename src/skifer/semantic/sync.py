"""Semantic draft synchronization without overwriting curated edits."""
from __future__ import annotations

from dataclasses import dataclass, field
from difflib import SequenceMatcher
import os
from pathlib import Path
from typing import Any

import yaml

from skifer.core.ir import ParsedSchema

from .draft_builder import SemanticDraftBuilder
from .persistence import write_yaml_atomic
from .output_projection import ProjectedSchema


_MANAGED_MARKER = {
    "tool": "skifer.semantic.draft_builder",
    "kind": "semantic_draft",
    "version": 1,
}
_RENAME_SIMILARITY_THRESHOLD = 0.82
_FIELD_COLLECTIONS = ("dimensions", "metrics")
# Handled by their own merge passes, so never merged as plain model-level values.
_MODEL_LEVEL_EXCLUDED_KEYS = frozenset({"dimensions", "metrics", "metadata"})
_TYPE_WIDENING = {
    ("integer", "float"),
    ("date", "datetime"),
    ("date", "timestamp"),
    ("datetime", "timestamp"),
}


@dataclass(frozen=True)
class SemanticChange:
    """One deterministic, auto-applicable sync delta."""

    kind: str
    target: str
    before: Any = None
    after: Any = None
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SemanticConflict:
    """One deterministic sync ambiguity or blocking incompatibility."""

    kind: str
    message: str
    target: str
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SyncReport:
    """Report-first sync result. Writing is allowed only when it is safe."""

    changes: tuple[SemanticChange, ...] = field(default_factory=tuple)
    conflicts: tuple[SemanticConflict, ...] = field(default_factory=tuple)
    suggestions: tuple[SemanticChange, ...] = field(default_factory=tuple)
    payload: dict[str, Any] | None = None
    wrote: bool = False
    output_path: str | None = None

    @property
    def safe_to_apply(self) -> bool:
        return not self.conflicts and not self.suggestions

    @property
    def has_changes(self) -> bool:
        return bool(self.changes)


def assert_no_curation_loss(curated_path: Path, payload: dict | None) -> None:
    """Refuse a promotion that would drop human-written curated content."""
    if payload is None or not curated_path.exists():
        return

    import yaml

    existing = yaml.safe_load(curated_path.read_text(encoding="utf-8")) or {}
    if not isinstance(existing, dict) or not existing.get("models"):
        return

    existing_model = existing["models"][0]
    new_model = (payload.get("models") or [{}])[0]
    lost: list[str] = []

    for key, value in existing_model.items():
        if key in ("dimensions", "metrics", "metadata"):
            continue
        if value and key not in new_model:
            lost.append(key)

    for collection in ("dimensions", "metrics"):
        new_by_name = {item["name"]: item for item in new_model.get(collection, [])}
        for item in existing_model.get(collection, []):
            new_item = new_by_name.get(item["name"])
            if new_item is None:
                # A removed field is a legitimate sync outcome, reported as a
                # change; only silent loss of curated attributes is refused.
                continue
            for key, value in item.items():
                if value and key not in new_item:
                    lost.append(f"{collection}.{item['name']}.{key}")

    if lost:
        raise ValueError(
            f"promoting would drop curated content {sorted(lost)} from "
            f"'{curated_path.name}'. Re-run 'semantic sync --write-draft' and resolve "
            "the report, or copy the curated values into the pipeline contract."
        )


class SemanticSynchronizer:
    """Build a fresh managed draft, merge in curation, and optionally write it."""

    def __init__(
        self,
        output_dir: str = "semantic_models",
        draft_builder: SemanticDraftBuilder | None = None,
    ):
        self.output_dir = os.path.abspath(output_dir) if not os.path.isabs(output_dir) else output_dir
        self.draft_builder = draft_builder or SemanticDraftBuilder(output_dir=self.output_dir)

    def sync(
        self,
        projected: ProjectedSchema,
        schema: ParsedSchema,
        *,
        write: bool = False,
    ) -> SyncReport:
        candidate_payload = self.draft_builder.build_draft(projected, schema)
        model_key = self._model(candidate_payload)["key"]
        draft_path = Path(self.output_dir) / ".drafts" / f"{model_key}.yaml"
        curated_path = Path(self.output_dir) / f"{model_key}.yaml"

        base_payload = self._load_yaml(draft_path) if draft_path.exists() else None
        curated_payload = self._load_yaml(curated_path) if curated_path.exists() else None

        merged_payload, changes, conflicts, suggestions = self._plan_merge(
            candidate_payload=candidate_payload,
            base_payload=base_payload,
            curated_payload=curated_payload,
        )

        if (
            write
            and merged_payload is not None
            and not conflicts
            and not suggestions
            and (base_payload is None or merged_payload != base_payload)
        ):
            written_path = write_yaml_atomic(draft_path, merged_payload)
            return SyncReport(
                changes=tuple(changes),
                conflicts=tuple(conflicts),
                suggestions=tuple(suggestions),
                payload=merged_payload,
                wrote=True,
                output_path=written_path,
            )

        return SyncReport(
            changes=tuple(changes),
            conflicts=tuple(conflicts),
            suggestions=tuple(suggestions),
            payload=merged_payload,
            wrote=False,
            output_path=str(draft_path),
        )

    def _plan_merge(
        self,
        *,
        candidate_payload: dict[str, Any],
        base_payload: dict[str, Any] | None,
        curated_payload: dict[str, Any] | None,
    ) -> tuple[dict[str, Any] | None, list[SemanticChange], list[SemanticConflict], list[SemanticChange]]:
        changes: list[SemanticChange] = []
        conflicts: list[SemanticConflict] = []
        suggestions: list[SemanticChange] = []

        if base_payload is None and curated_payload is None:
            changes.extend(self._bootstrap_changes(candidate_payload))
            return candidate_payload, changes, conflicts, suggestions

        if curated_payload is None:
            assert base_payload is not None
            changes.extend(self._diff_payloads(base_payload, candidate_payload))
            return candidate_payload, changes, conflicts, suggestions

        if base_payload is None:
            conflicts.append(
                SemanticConflict(
                    kind="missing_base_draft",
                    message=(
                        "Semantic sync conflict: missing managed draft provenance for curated model "
                        f"'{self._model(curated_payload)['key']}'."
                    ),
                    target=self._model(curated_payload)["key"],
                    details={},
                )
            )
            return None, changes, conflicts, suggestions

        base_model = self._model(base_payload)
        curated_model = self._model(curated_payload)
        candidate_model = self._model(candidate_payload)

        if (
            (base_model.get("metadata") or {}).get("source_definition_hash")
            == (candidate_model.get("metadata") or {}).get("source_definition_hash")
        ):
            return base_payload, changes, conflicts, suggestions
        else:
            provenance_conflict = self._check_provenance(base_payload, curated_payload)
            if provenance_conflict is not None:
                conflicts.append(provenance_conflict)
                return None, changes, conflicts, suggestions

        # The draft persists the projected grain (contract.grain), so a grain
        # change is compared directly rather than inferred from dimension names
        # — that inference missed select_final pipelines entirely and confused
        # "the dimension set changed" with "the grain changed".
        base_grain = tuple((base_model.get("metadata") or {}).get("grain") or ())
        candidate_grain = tuple((candidate_model.get("metadata") or {}).get("grain") or ())
        if base_grain != candidate_grain:
            conflicts.append(
                SemanticConflict(
                    kind="grain_changed",
                    message=(
                        f"Semantic sync conflict: declared grain changed from {list(base_grain)} "
                        f"to {list(candidate_grain)}. Review the semantic model before applying."
                    ),
                    target="grain",
                    details={"before": base_grain, "after": candidate_grain},
                )
            )

        merged_model: dict[str, Any] = {}
        for key in self._model_level_keys(base_model, curated_model, candidate_model):
            merged_value, scalar_changes, scalar_conflicts = self._merge_scalar(
                key=key,
                target=key,
                base_value=base_model.get(key),
                current_value=curated_model.get(key),
                candidate_value=candidate_model.get(key),
            )
            if merged_value is not None:
                merged_model[key] = merged_value
            changes.extend(scalar_changes)
            conflicts.extend(scalar_conflicts)

        for collection_name in _FIELD_COLLECTIONS:
            merged_items, collection_changes, collection_conflicts, collection_suggestions = self._merge_items(
                collection_name=collection_name,
                base_items=base_model.get(collection_name, []),
                curated_items=curated_model.get(collection_name, []),
                candidate_items=candidate_model.get(collection_name, []),
                model=curated_model,
                base_generated=self._generated_names(base_model),
            )
            merged_model[collection_name] = merged_items
            changes.extend(collection_changes)
            conflicts.extend(collection_conflicts)
            suggestions.extend(collection_suggestions)

        merged_model["metadata"] = candidate_model["metadata"]
        merged_payload = {
            "_generated_by": dict(_MANAGED_MARKER),
            "models": [merged_model],
        }

        if conflicts or suggestions:
            return None, changes, conflicts, suggestions
        return merged_payload, changes, conflicts, suggestions

    def _merge_items(
        self,
        *,
        collection_name: str,
        base_items: list[dict[str, Any]],
        curated_items: list[dict[str, Any]],
        candidate_items: list[dict[str, Any]],
        model: dict[str, Any],
        base_generated: set[str],
    ) -> tuple[list[dict[str, Any]], list[SemanticChange], list[SemanticConflict], list[SemanticChange]]:
        changes: list[SemanticChange] = []
        conflicts: list[SemanticConflict] = []
        suggestions: list[SemanticChange] = []

        base_by_name = {item["name"]: item for item in base_items}
        curated_by_name = {item["name"]: item for item in curated_items}
        candidate_by_name = {item["name"]: item for item in candidate_items}
        order: list[str] = []
        seen: set[str] = set()

        def add_order(names: list[str]) -> None:
            for name in names:
                if name not in seen:
                    seen.add(name)
                    order.append(name)

        add_order([item["name"] for item in candidate_items])
        add_order([item["name"] for item in curated_items if item["name"] not in candidate_by_name])

        removed_generated = [
            name for name in base_generated if name in base_by_name and name not in candidate_by_name
        ]
        rename_pairs = self._suggest_renames(
            removed_names=removed_generated,
            added_names=[name for name in candidate_by_name if name not in base_by_name],
        )
        renamed_removed = {removed for removed, _ in rename_pairs}
        renamed_added = {added for _, added in rename_pairs}
        for removed_name, added_name in rename_pairs:
            suggestions.append(
                SemanticChange(
                    kind="rename_suggestion",
                    target=f"{collection_name}.{removed_name}",
                    before=removed_name,
                    after=added_name,
                    details={"collection": collection_name},
                )
            )

        merged_items: list[dict[str, Any]] = []
        for name in order:
            base_item = base_by_name.get(name)
            curated_item = curated_by_name.get(name)
            candidate_item = candidate_by_name.get(name)

            if base_item is None and candidate_item is not None:
                if name in renamed_added:
                    continue
                changes.append(
                    SemanticChange(
                        kind="add_field",
                        target=f"{collection_name}.{name}",
                        before=None,
                        after=name,
                        details={"collection": collection_name},
                    )
                )
                merged_items.append(candidate_item)
                continue

            if base_item is None and candidate_item is None and curated_item is not None:
                merged_items.append(curated_item)
                continue

            if base_item is not None and candidate_item is None:
                if name in renamed_removed:
                    merged_items.append(curated_item or base_item)
                    continue
                if collection_name == "dimensions":
                    dependent_metric = self._metric_depending_on_column(model, name)
                    if dependent_metric is not None:
                        conflicts.append(
                            SemanticConflict(
                                kind="removed_dependency",
                                message=(
                                    f"Semantic sync conflict: metric '{dependent_metric}' depends on removed "
                                    f"column '{name}'."
                                ),
                                target=f"dimensions.{name}",
                                details={"metric": dependent_metric, "column": name},
                            )
                        )
                        merged_items.append(curated_item or base_item)
                        continue
                changes.append(
                    SemanticChange(
                        kind="remove_field",
                        target=f"{collection_name}.{name}",
                        before=name,
                        after=None,
                        details={"collection": collection_name},
                    )
                )
                continue

            assert base_item is not None and candidate_item is not None
            current_item = curated_item or base_item
            merged_item, item_changes, item_conflicts = self._merge_dict(
                target=f"{collection_name}.{name}",
                base_dict=base_item,
                current_dict=current_item,
                candidate_dict=candidate_item,
            )
            changes.extend(item_changes)
            conflicts.extend(item_conflicts)
            merged_items.append(merged_item)

        return merged_items, changes, conflicts, suggestions

    @staticmethod
    def _model_level_keys(
        base_model: dict[str, Any],
        curated_model: dict[str, Any],
        candidate_model: dict[str, Any],
    ) -> list[str]:
        """Every model-level key to merge, in a deterministic order.

        A fixed allow-list silently dropped any key it did not know about — most
        visibly ``synonyms``, which ``SemanticBuilder.build_from_projection()``
        is explicitly allowed to write, but equally ``tags`` or ``base_filter``
        from a hand-authored model. Candidate order comes first so generated
        fields keep the draft's layout; human-only keys are appended.
        """
        ordered: list[str] = []
        seen: set[str] = set()
        for model in (candidate_model, curated_model, base_model):
            for key in model:
                if key in _MODEL_LEVEL_EXCLUDED_KEYS or key in seen:
                    continue
                seen.add(key)
                ordered.append(key)
        return ordered

    def _merge_scalar(
        self,
        *,
        key: str,
        target: str,
        base_value: Any,
        current_value: Any,
        candidate_value: Any,
    ) -> tuple[Any, list[SemanticChange], list[SemanticConflict]]:
        if base_value == current_value == candidate_value:
            return candidate_value, [], []
        if current_value == base_value:
            if candidate_value != base_value:
                return candidate_value, [
                    SemanticChange(kind="update_model", target=target, before=base_value, after=candidate_value)
                ], []
            return candidate_value, [], []
        if candidate_value == base_value:
            return current_value, [], []
        return current_value, [], [
            SemanticConflict(
                kind="curated_scalar_conflict",
                message=f"Semantic sync conflict: curated '{key}' diverged from regenerated output.",
                target=target,
                details={"before": base_value, "current": current_value, "candidate": candidate_value},
            )
        ]

    def _merge_dict(
        self,
        *,
        target: str,
        base_dict: dict[str, Any],
        current_dict: dict[str, Any],
        candidate_dict: dict[str, Any],
    ) -> tuple[dict[str, Any], list[SemanticChange], list[SemanticConflict]]:
        merged: dict[str, Any] = {}
        changes: list[SemanticChange] = []
        conflicts: list[SemanticConflict] = []
        keys = sorted(set(base_dict) | set(current_dict) | set(candidate_dict))
        for key in keys:
            base_value = base_dict.get(key)
            current_value = current_dict.get(key)
            candidate_value = candidate_dict.get(key)

            if base_value == current_value == candidate_value:
                if candidate_value is not None:
                    merged[key] = candidate_value
                continue

            if current_value == base_value:
                if key == "type" and candidate_value != base_value:
                    type_conflict = self._type_conflict(target, base_value, current_value, candidate_value)
                    if type_conflict is None:
                        if candidate_value is not None:
                            merged[key] = candidate_value
                        changes.append(
                            SemanticChange(
                                kind="widen_type",
                                target=f"{target}.{key}",
                                before=base_value,
                                after=candidate_value,
                            )
                        )
                    else:
                        conflicts.append(type_conflict)
                        if current_value is not None:
                            merged[key] = current_value
                    continue
                if candidate_value is not None:
                    merged[key] = candidate_value
                if candidate_value != base_value:
                    kind = self._change_kind(key, base_value, candidate_value)
                    changes.append(
                        SemanticChange(kind=kind, target=f"{target}.{key}", before=base_value, after=candidate_value)
                    )
                continue

            if candidate_value == base_value:
                if current_value is not None:
                    merged[key] = current_value
                continue

            if key == "type":
                type_conflict = self._type_conflict(target, base_value, current_value, candidate_value)
                if type_conflict is None:
                    merged[key] = candidate_value
                    changes.append(
                        SemanticChange(
                            kind="widen_type",
                            target=f"{target}.{key}",
                            before=base_value,
                            after=candidate_value,
                        )
                    )
                    continue
                conflicts.append(type_conflict)
                merged[key] = current_value
                continue

            conflicts.append(
                SemanticConflict(
                    kind="curated_field_conflict",
                    message=(
                        f"Semantic sync conflict: curated field '{target}' diverged on '{key}' from regenerated output."
                    ),
                    target=f"{target}.{key}",
                    details={
                        "before": base_value,
                        "current": current_value,
                        "candidate": candidate_value,
                    },
                )
            )
            if current_value is not None:
                merged[key] = current_value

        return merged, changes, conflicts

    @staticmethod
    def _change_kind(key: str, before: Any, after: Any) -> str:
        if key == "type":
            if (str(before).lower(), str(after).lower()) in _TYPE_WIDENING:
                return "widen_type"
            if before is not None and after is not None:
                return "narrow_type"
        return "update_field"

    def _type_conflict(
        self,
        target: str,
        base_value: Any,
        current_value: Any,
        candidate_value: Any,
    ) -> SemanticConflict | None:
        if base_value is None or current_value is None or candidate_value is None:
            return SemanticConflict(
                kind="type_changed",
                message=f"Semantic sync conflict: field '{target}' changed type ambiguously.",
                target=f"{target}.type",
                details={"before": base_value, "current": current_value, "candidate": candidate_value},
            )
        base_type = str(base_value).lower()
        current_type = str(current_value).lower()
        candidate_type = str(candidate_value).lower()
        if current_type == base_type and (base_type, candidate_type) in _TYPE_WIDENING:
            return None
        kind = "narrow_type" if (candidate_type, base_type) in _TYPE_WIDENING else "type_changed"
        return SemanticConflict(
            kind=kind,
            message=f"Semantic sync conflict: field '{target}' changed type from '{base_value}' to '{candidate_value}'.",
            target=f"{target}.type",
            details={"before": base_value, "current": current_value, "candidate": candidate_value},
        )

    def _check_provenance(
        self,
        base_payload: dict[str, Any],
        curated_payload: dict[str, Any],
    ) -> SemanticConflict | None:
        base_model = self._model(base_payload)
        curated_model = self._model(curated_payload)
        base_metadata = base_model.get("metadata") or {}
        curated_metadata = curated_model.get("metadata") or {}
        if curated_metadata.get("source_definition_hash") != base_metadata.get("source_definition_hash"):
            return SemanticConflict(
                kind="stale_curated_model",
                message=(
                    "Semantic sync conflict: curated model metadata does not match the last generated draft."
                ),
                target=curated_model["key"],
                details={
                    "base_definition_hash": base_metadata.get("source_definition_hash"),
                    "curated_definition_hash": curated_metadata.get("source_definition_hash"),
                },
            )
        if tuple(curated_metadata.get("generated_fields") or ()) != tuple(base_metadata.get("generated_fields") or ()):
            return SemanticConflict(
                kind="generated_fields_mismatch",
                message=(
                    "Semantic sync conflict: curated model generated_fields do not match the last generated draft."
                ),
                target=curated_model["key"],
                details={
                    "base_generated_fields": tuple(base_metadata.get("generated_fields") or ()),
                    "curated_generated_fields": tuple(curated_metadata.get("generated_fields") or ()),
                },
            )
        return None

    def _bootstrap_changes(self, payload: dict[str, Any]) -> list[SemanticChange]:
        model = self._model(payload)
        changes = [
            SemanticChange(kind="create_draft", target=model["key"], before=None, after=model["key"]),
        ]
        for collection_name in _FIELD_COLLECTIONS:
            for item in model.get(collection_name, []):
                changes.append(
                    SemanticChange(
                        kind="add_field",
                        target=f"{collection_name}.{item['name']}",
                        before=None,
                        after=item["name"],
                        details={"collection": collection_name},
                    )
                )
        return changes

    def _diff_payloads(
        self,
        base_payload: dict[str, Any],
        candidate_payload: dict[str, Any],
    ) -> list[SemanticChange]:
        base_model = self._model(base_payload)
        candidate_model = self._model(candidate_payload)
        changes: list[SemanticChange] = []
        for collection_name in _FIELD_COLLECTIONS:
            base_names = {item["name"] for item in base_model.get(collection_name, [])}
            candidate_names = {item["name"] for item in candidate_model.get(collection_name, [])}
            for item in candidate_model.get(collection_name, []):
                if item["name"] not in base_names:
                    changes.append(
                        SemanticChange(
                            kind="add_field",
                            target=f"{collection_name}.{item['name']}",
                            before=None,
                            after=item["name"],
                            details={"collection": collection_name},
                        )
                    )
            for item in base_model.get(collection_name, []):
                if item["name"] not in candidate_names:
                    changes.append(
                        SemanticChange(
                            kind="remove_field",
                            target=f"{collection_name}.{item['name']}",
                            before=item["name"],
                            after=None,
                            details={"collection": collection_name},
                        )
                    )
            for candidate_item in candidate_model.get(collection_name, []):
                base_item = next(
                    (item for item in base_model.get(collection_name, []) if item["name"] == candidate_item["name"]),
                    None,
                )
                if base_item is None:
                    continue
                for key in sorted(set(base_item) | set(candidate_item)):
                    if base_item.get(key) != candidate_item.get(key):
                        changes.append(
                            SemanticChange(
                                kind=self._change_kind(key, base_item.get(key), candidate_item.get(key)),
                                target=f"{collection_name}.{candidate_item['name']}.{key}",
                                before=base_item.get(key),
                                after=candidate_item.get(key),
                            )
                        )
        return changes

    @staticmethod
    def _metric_depending_on_column(model: dict[str, Any], column_name: str) -> str | None:
        for metric in model.get("metrics", []):
            if metric.get("sql") == column_name:
                return metric.get("name")
        return None


    @staticmethod
    def _suggest_renames(
        *,
        removed_names: list[str],
        added_names: list[str],
    ) -> list[tuple[str, str]]:
        pairs: list[tuple[float, str, str]] = []
        used_added: set[str] = set()
        for removed_name in sorted(removed_names):
            best_score = 0.0
            best_name: str | None = None
            for added_name in sorted(added_names):
                if added_name in used_added:
                    continue
                score = SequenceMatcher(a=removed_name, b=added_name).ratio()
                if score > best_score:
                    best_score = score
                    best_name = added_name
            if best_name is not None and best_score >= _RENAME_SIMILARITY_THRESHOLD:
                pairs.append((best_score, removed_name, best_name))
                used_added.add(best_name)
        return [(removed_name, added_name) for _, removed_name, added_name in sorted(pairs)]

    @staticmethod
    def _load_yaml(path: Path) -> dict[str, Any]:
        with path.open(encoding="utf-8") as handle:
            payload = yaml.safe_load(handle) or {}
        if not isinstance(payload, dict):
            raise ValueError(f"Expected a YAML mapping in '{path}'.")
        return payload

    @staticmethod
    def _model(payload: dict[str, Any]) -> dict[str, Any]:
        models = payload.get("models")
        if not isinstance(models, list) or len(models) != 1 or not isinstance(models[0], dict):
            raise ValueError("Semantic sync expects exactly one model per YAML file.")
        return models[0]

    @staticmethod
    def _generated_names(model: dict[str, Any]) -> set[str]:
        metadata = model.get("metadata") or {}
        return set(metadata.get("generated_fields") or ())
