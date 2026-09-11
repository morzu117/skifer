"""Pure field-classification propagation over column lineage."""
from __future__ import annotations

import logging
import warnings

from skifer.core.constants import CLASSIFICATION_LEVELS, CLASSIFICATION_RANK
from skifer.lineage.tracker import LineageGraph


logger = logging.getLogger(__name__)


class ClassificationPropagationWarning(UserWarning):
    """An inferred classification elevation was not explicitly declared."""


def _max_level(levels: list[str]) -> str | None:
    ranked = [level for level in levels if level in CLASSIFICATION_RANK]
    return max(ranked, key=CLASSIFICATION_RANK.__getitem__) if ranked else None


def resolve_field_classifications(
    graph: LineageGraph,
    target_table: str,
    declared: dict[str, str],
    source_classifications: dict[str, str],
    *,
    mode: str = "warn",
) -> dict[str, str]:
    """Return each target column's declared or lineage-inferred classification.

    ``mode="warn"`` is the v1 behavior. ``mode="strict"`` is deliberately
    available for Feature 31.7 to reject undeclared inferred elevations.
    """
    if mode not in {"warn", "strict"}:
        raise ValueError("classification propagation mode must be 'warn' or 'strict'.")

    target_columns = set(declared)
    target_columns.update(
        edge.target_column for edge in graph.edges if edge.target_table == target_table
    )
    effective: dict[str, str] = {}

    for column in sorted(target_columns):
        inferred = _max_level(
            [
                source_classifications[edge.source_column]
                for edge in graph.upstream(target_table, column)
                if edge.source_column in source_classifications
            ]
        )
        explicit = declared.get(column)

        if explicit is None:
            if inferred is None:
                continue
            if CLASSIFICATION_RANK[inferred] > CLASSIFICATION_RANK[CLASSIFICATION_LEVELS[0]]:
                message = (
                    f"Column '{column}' inherits classification '{inferred}' from "
                    "lineage but has no explicit declaration."
                )
                if mode == "strict":
                    raise ValueError(message)
                warnings.warn(message, ClassificationPropagationWarning, stacklevel=2)
            effective[column] = inferred
            continue

        if inferred is not None and (
            CLASSIFICATION_RANK[explicit] < CLASSIFICATION_RANK[inferred]
        ):
            logger.warning(
                "Explicit classification lowering for column '%s': inferred '%s', "
                "declared '%s'.",
                column,
                inferred,
                explicit,
            )
        effective[column] = explicit

    return effective


# Forward coupling only: Feature 31.2 will use this mapping to populate
# ColumnRecord.classification. Its metadata registry is intentionally not created here.
