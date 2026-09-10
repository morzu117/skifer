"""
DataDictionary — field-level data dictionary built from a LineageGraph.

Provides per-column metadata (source fields, transformations, description)
with optional enrichment from a GlossaryReader.
"""

from __future__ import annotations

import difflib
from dataclasses import dataclass, field

from .tracker import RULE_ORIGIN, LineageGraph


@dataclass
class FieldEntry:
    """Metadata for a single column in a pipeline or semantic model."""
    name: str
    table: str
    description: str = ""
    source_fields: list[str] = field(default_factory=list)
    transformations: list[str] = field(default_factory=list)


class DataDictionary:
    """
    Column-level data dictionary derived from a LineageGraph.

    Usage::

        graph = LineageTracker.from_schema(schema)
        dd = DataDictionary(graph)
        entry = dd.get("silver.fact_orders", "amount_eur")
        dd.print_report()

        # Optional glossary enrichment
        dd.enrich_from_glossary("glossary.yaml")
    """

    def __init__(self, graph: LineageGraph):
        self._index: dict[tuple[str, str], FieldEntry] = {}
        self._build(graph)

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    def _build(self, graph: LineageGraph) -> None:
        """Populate the index from all edges in the graph."""
        for edge in graph.edges:
            key = (edge.target_table, edge.target_column)
            if key not in self._index:
                self._index[key] = FieldEntry(
                    name=edge.target_column,
                    table=edge.target_table,
                )
            entry = self._index[key]
            src_ref = f"{edge.source_table}.{edge.source_column}"
            if src_ref not in entry.source_fields:
                entry.source_fields.append(src_ref)
            for op in edge.transformations:
                if op and op not in entry.transformations:
                    entry.transformations.append(op)

            # Also ensure source columns appear as entries (without upstream info).
            # RULE_ORIGIN is not a table: it marks a column a business rule created
            # inside the pipeline. Indexing it would put a field nobody can query
            # in the dictionary, and list it twice among nearest-name suggestions.
            if edge.source_table == RULE_ORIGIN:
                continue
            src_key = (edge.source_table, edge.source_column)
            if src_key not in self._index:
                self._index[src_key] = FieldEntry(
                    name=edge.source_column,
                    table=edge.source_table,
                )

    # ------------------------------------------------------------------
    # Lookup
    # ------------------------------------------------------------------

    def get(self, table: str, column: str) -> FieldEntry | None:
        """Return the FieldEntry for (table, column), or None if not found."""
        return self._index.get((table, column))

    def list_fields(self, table: str | None = None) -> list[FieldEntry]:
        """Return all FieldEntry objects, optionally filtered by table."""
        entries = list(self._index.values())
        if table is not None:
            entries = [e for e in entries if e.table == table]
        return sorted(entries, key=lambda e: (e.table, e.name))

    # ------------------------------------------------------------------
    # Glossary enrichment
    # ------------------------------------------------------------------

    def enrich_from_glossary(self, glossary_path: str) -> None:
        """
        Enrich field descriptions using a glossary file.

        Loads the glossary via GlossaryReader, then fuzzy-matches each
        column name against glossary terms using difflib.get_close_matches().

        Args:
            glossary_path: Path to a glossary file (JSON, YAML, TXT, PDF, PPTX).
        """
        from ..semantic.glossary import GlossaryReader

        raw_text = GlossaryReader().read(glossary_path)
        # Parse "term: definition" lines; fall back to treating each line as a term
        glossary: dict[str, str] = {}
        for line in raw_text.splitlines():
            line = line.strip()
            if ":" in line:
                parts = line.split(":", 1)
                term = parts[0].strip().lower()
                definition = parts[1].strip()
                if term:
                    glossary[term] = definition
            elif line:
                glossary[line.lower()] = ""

        if not glossary:
            return

        glossary_terms = list(glossary.keys())
        for entry in self._index.values():
            col_lower = entry.name.lower()
            matches = difflib.get_close_matches(col_lower, glossary_terms, n=1, cutoff=0.8)
            if matches and not entry.description:
                entry.description = glossary[matches[0]]

    # ------------------------------------------------------------------
    # Export
    # ------------------------------------------------------------------

    def to_dict(self) -> dict:
        """Serialize to a plain dict."""
        return {
            f"{e.table}.{e.name}": {
                "table": e.table,
                "name": e.name,
                "description": e.description,
                "source_fields": e.source_fields,
                "transformations": e.transformations,
            }
            for e in self.list_fields()
        }

    def print_report(self) -> None:
        """Print a human-readable data dictionary report to stdout."""
        SEP = "=" * 55
        SEP2 = "-" * 40

        print(f"\n{SEP}")
        print(" Data Dictionary")
        print(SEP)

        current_table = None
        for entry in self.list_fields():
            if entry.table != current_table:
                current_table = entry.table
                print(f"\n {current_table}")
                print(f" {SEP2}")
            desc = f" — {entry.description}" if entry.description else ""
            print(f"   {entry.name}{desc}")
            if entry.source_fields:
                print(f"     from : {', '.join(entry.source_fields)}")
            if entry.transformations:
                print(f"     ops  : {', '.join(entry.transformations)}")

        print(f"\n{SEP}\n")
