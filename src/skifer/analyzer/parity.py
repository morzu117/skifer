"""Measures data-level equivalence between a draft pipeline and the legacy
table it is meant to replace (Plan 43 — 43.5): proof on real data before a
cutover, never an inference from the SQL mapping alone.

``ParityRunner`` runs a draft schema dict through the real engine into an
isolated, disposable table — never the draft's own eventual target, and
never the legacy table — then drops it once compared. ``compare_tables``
does the actual comparison: row counts, columns (names and declared types),
a hash of sorted rows (sorted by a declared key, or by every common column
when none is given), and, when a key is given, a per-column count of rows
whose value differs between the two tables for matched keys.

``ParityReport`` never carries a data value — only counts, booleans, and
column names — the same discipline already enforced for incident alerts
(``observability/alerts.py``) and semantic evidence (``semantic/evidence.py``):
a report a human can paste into a ticket without re-exposing the very rows a
certification gate exists to protect.
"""
from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass, field

_VERSION_PREFIX = "sha256:v1:"


def _row_hash(rows: list[tuple]) -> str:
    text = "\n".join(
        "\x1f".join("\x00" if v is None else str(v) for v in row)
        for row in rows
    )
    return _VERSION_PREFIX + hashlib.sha256(text.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class ColumnDifference:
    """One common, non-key column with differing values among matched rows."""

    column: str
    differing_rows: int

    def to_dict(self) -> dict:
        return {"column": self.column, "differing_rows": self.differing_rows}


@dataclass(frozen=True)
class ParityReport:
    """Allowlisted, data-value-free comparison of two tables.

    ``to_dict()`` is an explicit field-by-field allowlist, never ``asdict`` —
    the same discipline as ``SemanticEvidence.to_dict()``: a field added here
    later stays private until this method is deliberately updated to expose
    it, and nothing in this report is ever a cell value — only counts,
    booleans, hashes and column names.
    """

    draft_fqn: str
    legacy_fqn: str
    key: tuple[str, ...] = ()
    draft_row_count: int = 0
    legacy_row_count: int = 0
    draft_columns: tuple[str, ...] = ()
    legacy_columns: tuple[str, ...] = ()
    draft_only_columns: tuple[str, ...] = ()
    legacy_only_columns: tuple[str, ...] = ()
    type_mismatched_columns: tuple[str, ...] = ()
    draft_row_hash: str = ""
    legacy_row_hash: str = ""
    rows_only_in_draft: int | None = None
    rows_only_in_legacy: int | None = None
    column_differences: tuple[ColumnDifference, ...] = field(default_factory=tuple)

    @property
    def row_count_matches(self) -> bool:
        return self.draft_row_count == self.legacy_row_count

    @property
    def columns_match(self) -> bool:
        return (
            not self.draft_only_columns
            and not self.legacy_only_columns
            and not self.type_mismatched_columns
        )

    @property
    def row_hash_matches(self) -> bool:
        return self.draft_row_hash == self.legacy_row_hash

    @property
    def identical(self) -> bool:
        return (
            self.row_count_matches
            and self.columns_match
            and self.row_hash_matches
            and not self.column_differences
            and not (self.rows_only_in_draft or self.rows_only_in_legacy)
        )

    def to_dict(self) -> dict:
        return {
            "draft_fqn": self.draft_fqn,
            "legacy_fqn": self.legacy_fqn,
            "key": list(self.key),
            "identical": self.identical,
            "draft_row_count": self.draft_row_count,
            "legacy_row_count": self.legacy_row_count,
            "row_count_matches": self.row_count_matches,
            "draft_columns": list(self.draft_columns),
            "legacy_columns": list(self.legacy_columns),
            "columns_match": self.columns_match,
            "draft_only_columns": list(self.draft_only_columns),
            "legacy_only_columns": list(self.legacy_only_columns),
            "type_mismatched_columns": list(self.type_mismatched_columns),
            "draft_row_hash": self.draft_row_hash,
            "legacy_row_hash": self.legacy_row_hash,
            "row_hash_matches": self.row_hash_matches,
            "rows_only_in_draft": self.rows_only_in_draft,
            "rows_only_in_legacy": self.rows_only_in_legacy,
            "column_differences": [c.to_dict() for c in self.column_differences],
        }


def compare_tables(spark, draft_fqn: str, legacy_fqn: str, *, key: list[str] | None = None) -> ParityReport:
    """Compare two existing tables by *fqn* — no draft execution here, just
    the measurement. ``key`` restricts the per-column diff to rows matched by
    that key; without one, only the row count and the whole-table row hash
    (sorted by every common column) are meaningful signals."""
    draft_df = spark.table(draft_fqn)
    legacy_df = spark.table(legacy_fqn)

    draft_columns = tuple(draft_df.columns)
    legacy_columns = tuple(legacy_df.columns)
    draft_types = dict(draft_df.dtypes)
    legacy_types = dict(legacy_df.dtypes)

    draft_only_columns = tuple(sorted(set(draft_columns) - set(legacy_columns)))
    legacy_only_columns = tuple(sorted(set(legacy_columns) - set(draft_columns)))
    common_columns = sorted(set(draft_columns) & set(legacy_columns))
    type_mismatched_columns = tuple(
        c for c in common_columns if draft_types.get(c) != legacy_types.get(c)
    )

    sort_columns = list(key) if key else common_columns
    if common_columns and sort_columns:
        draft_rows = [
            tuple(row[c] for c in common_columns)
            for row in draft_df.select(*common_columns).orderBy(*sort_columns).collect()
        ]
        legacy_rows = [
            tuple(row[c] for c in common_columns)
            for row in legacy_df.select(*common_columns).orderBy(*sort_columns).collect()
        ]
    else:
        draft_rows, legacy_rows = [], []

    draft_row_hash = _row_hash(draft_rows)
    legacy_row_hash = _row_hash(legacy_rows)

    rows_only_in_draft: int | None = None
    rows_only_in_legacy: int | None = None
    column_differences: list[ColumnDifference] = []

    if key:
        from pyspark.sql import functions as F

        non_key_common = [c for c in common_columns if c not in key]
        draft_marked = (
            draft_df.select(*common_columns)
            .select(*[F.col(c).alias(c if c in key else f"__d_{c}") for c in common_columns])
            .withColumn("__in_draft", F.lit(True))
        )
        legacy_marked = (
            legacy_df.select(*common_columns)
            .select(*[F.col(c).alias(c if c in key else f"__l_{c}") for c in common_columns])
            .withColumn("__in_legacy", F.lit(True))
        )
        joined = draft_marked.join(legacy_marked, on=list(key), how="full_outer")

        rows_only_in_draft = joined.filter("__in_legacy IS NULL").count()
        rows_only_in_legacy = joined.filter("__in_draft IS NULL").count()
        matched = joined.filter("__in_draft IS NOT NULL AND __in_legacy IS NOT NULL")

        for column in non_key_common:
            differing = matched.filter(f"NOT (__d_{column} <=> __l_{column})").count()
            if differing:
                column_differences.append(ColumnDifference(column=column, differing_rows=differing))

    return ParityReport(
        draft_fqn=draft_fqn,
        legacy_fqn=legacy_fqn,
        key=tuple(key) if key else (),
        draft_row_count=draft_df.count(),
        legacy_row_count=legacy_df.count(),
        draft_columns=draft_columns,
        legacy_columns=legacy_columns,
        draft_only_columns=draft_only_columns,
        legacy_only_columns=legacy_only_columns,
        type_mismatched_columns=type_mismatched_columns,
        draft_row_hash=draft_row_hash,
        legacy_row_hash=legacy_row_hash,
        rows_only_in_draft=rows_only_in_draft,
        rows_only_in_legacy=rows_only_in_legacy,
        column_differences=tuple(column_differences),
    )


class ParityRunner:
    """Runs a draft pipeline into a disposable table and compares it to a
    legacy table (Plan 43 — 43.5). Never writes to the legacy table, never
    writes to the draft's own eventual target — only to a throwaway table it
    drops itself once the comparison is done, even on failure."""

    def __init__(self, engine):
        self._engine = engine

    def run(
        self,
        draft_schema_dict: dict,
        target_layer: str,
        *,
        against: str,
        key: list[str] | None = None,
    ) -> ParityReport:
        disposable_name = f"_skifer_parity_{uuid.uuid4().hex}"
        self._engine.run_process_to_table(draft_schema_dict, target_layer, disposable_name)
        resolved_layer = self._engine.get_target_schema(target_layer)
        draft_fqn = self._engine._build_fqn(resolved_layer, disposable_name)
        try:
            return compare_tables(self._engine.spark, draft_fqn, against, key=key)
        finally:
            self._engine.spark.sql(f"DROP TABLE IF EXISTS {draft_fqn}")
