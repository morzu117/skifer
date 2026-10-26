"""Analysis report: ties the mapper and the round-trip oracle together into one
per-statement verdict, and renders it as Markdown or JSON (Plan 43 — 43.2).

Serialization is an explicit allowlist, not ``asdict`` — a field added to a
dataclass later stays private until this module is deliberately updated to
expose it (the same discipline ``semantic/evidence.py`` uses for its DTOs).
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field

from skifer.analyzer.mapper import MappedStatement, Residue, map_statement
from skifer.analyzer.parser import Statement
from skifer.analyzer.roundtrip import RoundTripResult, VERIFIED, verify

EXIT_CLEAN = 0
EXIT_NEEDS_REVIEW = 2


@dataclass(frozen=True)
class StatementReport:
    source_path: str
    line: int
    target: str | None
    mapped: bool
    roundtrip_status: str | None
    roundtrip_detail: str | None
    residues: tuple[Residue, ...] = field(default_factory=tuple)

    @property
    def is_clean(self) -> bool:
        return self.mapped and self.roundtrip_status == VERIFIED and not self.residues

    def to_dict(self) -> dict:
        return {
            "source_path": self.source_path,
            "line": self.line,
            "target": self.target,
            "mapped": self.mapped,
            "roundtrip_status": self.roundtrip_status,
            "roundtrip_detail": self.roundtrip_detail,
            "residues": [
                {"code": r.code, "message": r.message, "blocking": r.blocking}
                for r in self.residues
            ],
        }


@dataclass(frozen=True)
class AnalysisReport:
    statements: tuple[StatementReport, ...] = field(default_factory=tuple)

    @property
    def exit_code(self) -> int:
        return EXIT_CLEAN if all(s.is_clean for s in self.statements) else EXIT_NEEDS_REVIEW

    def to_dict(self) -> dict:
        return {
            "statement_count": len(self.statements),
            "clean_count": sum(1 for s in self.statements if s.is_clean),
            "statements": [s.to_dict() for s in self.statements],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, sort_keys=False)

    def to_markdown(self) -> str:
        lines = [
            "# Project analyzer report",
            "",
            f"{len(self.statements)} statement(s) analyzed, "
            f"{sum(1 for s in self.statements if s.is_clean)} clean.",
            "",
        ]
        for s in self.statements:
            header = f"## {s.source_path}:{s.line}"
            if s.target:
                header += f" -> `{s.target}`"
            lines.append(header)
            if not s.mapped:
                lines.append("- **needs_curation**")
            else:
                lines.append(f"- round-trip: **{s.roundtrip_status}**")
            if s.roundtrip_detail:
                lines.append(f"- detail: {s.roundtrip_detail}")
            for r in s.residues:
                marker = "blocking" if r.blocking else "info"
                lines.append(f"- [{marker}] `{r.code}`: {r.message}")
            lines.append("")
        return "\n".join(lines)


def build_report(statements: list[Statement]) -> AnalysisReport:
    """Map and round-trip-verify every statement, assembling one report."""
    reports = []
    for stmt in statements:
        mapped: MappedStatement = map_statement(stmt)
        if mapped.needs_curation:
            reports.append(StatementReport(
                source_path=mapped.source_path,
                line=mapped.line,
                target=mapped.target,
                mapped=False,
                roundtrip_status=None,
                roundtrip_detail=None,
                residues=tuple(mapped.residues),
            ))
            continue

        result: RoundTripResult = verify(mapped, stmt)
        reports.append(StatementReport(
            source_path=mapped.source_path,
            line=mapped.line,
            target=mapped.target,
            mapped=True,
            roundtrip_status=result.status,
            roundtrip_detail=result.detail,
            residues=tuple(mapped.residues),
        ))
    return AnalysisReport(statements=tuple(reports))


# ---------------------------------------------------------------------------
# Splitting report (Plan 43 — 43.3): partials / intermediate tables / inlined
# CTEs, plus a Mermaid rendering of the materialized-table dependency graph.
# ---------------------------------------------------------------------------


def _mermaid_id(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]", "_", name)


def _render_dag_mermaid(nodes) -> str:
    """Materialized-to-materialized (and materialized-to-source) edges only —
    the same ``graph LR`` shape ``lineage/renderer.py`` renders column-level
    lineage with, applied here at table granularity."""
    lines = ["graph LR"]
    if not nodes:
        lines.append("    %% no materialized table in this corpus")
        return "\n".join(lines)
    for node in nodes:
        for source in node.reads:
            lines.append(
                f'    {_mermaid_id(source)}["{source}"] --> {_mermaid_id(node.target)}["{node.target}"]'
            )
        if not node.reads:
            lines.append(f'    {_mermaid_id(node.target)}["{node.target}"]')
    return "\n".join(lines)


@dataclass(frozen=True)
class SplitUnitReport:
    kind: str  # "partial" | "intermediate_table"
    name: str | None
    path: str | None
    source_path: str | None
    readers: tuple[str, ...]
    mapped: bool
    roundtrip_status: str | None

    def to_dict(self) -> dict:
        return {
            "kind": self.kind,
            "name": self.name,
            "path": self.path,
            "source_path": self.source_path,
            "readers": list(self.readers),
            "mapped": self.mapped,
            "roundtrip_status": self.roundtrip_status,
        }


@dataclass(frozen=True)
class InlinedCteReport:
    cte_name: str
    source_path: str

    def to_dict(self) -> dict:
        return {"cte_name": self.cte_name, "source_path": self.source_path}


@dataclass(frozen=True)
class SplitReport:
    dag_mermaid: str
    units: tuple[SplitUnitReport, ...] = field(default_factory=tuple)
    inlined: tuple[InlinedCteReport, ...] = field(default_factory=tuple)
    cycle_error: str | None = None

    def to_dict(self) -> dict:
        return {
            "cycle_error": self.cycle_error,
            "dag_mermaid": self.dag_mermaid,
            "units": [u.to_dict() for u in self.units],
            "inlined": [i.to_dict() for i in self.inlined],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, sort_keys=False)

    def to_markdown(self) -> str:
        lines = ["## Découpage", ""]
        if self.cycle_error:
            lines += [f"**{self.cycle_error}**", ""]
            return "\n".join(lines)
        lines += ["```mermaid", self.dag_mermaid, "```", ""]
        for u in self.units:
            header = f"### {u.kind}: `{u.name}`"
            if u.path:
                header += f" ({u.path})"
            lines.append(header)
            if u.source_path:
                lines.append(f"- origin: {u.source_path}")
            lines.append(f"- readers: {', '.join(u.readers) if u.readers else '(none detected)'}")
            lines.append(f"- mapped: **{u.mapped}**" + (f", round-trip: **{u.roundtrip_status}**" if u.roundtrip_status else ""))
            lines.append("")
        if self.inlined:
            lines.append("### Inlined CTEs")
            for i in self.inlined:
                lines.append(f"- `{i.cte_name}` (used once, in {i.source_path})")
            lines.append("")
        return "\n".join(lines)


def _original_script_path(statement_source_path: str) -> str:
    return statement_source_path.split("::", 1)[0]


def build_split_report(statements: list[Statement]) -> SplitReport:
    """Split *statements* into partials/intermediate tables and report, for
    each unit, its readers and whether it independently maps and round-trip
    verifies. A corpus-level cycle among materialized tables is reported
    instead of a DAG, never raised past this function."""
    from skifer.analyzer.dag import DagCycleError, build_dag
    from skifer.analyzer.splitter import split_corpus

    try:
        dag = build_dag(statements)
    except DagCycleError as exc:
        return SplitReport(dag_mermaid="", cycle_error=str(exc))

    result = split_corpus(statements)
    main_units = [u for u in result.units if u.kind == "main"]
    mapped_main = [(u, map_statement(u.statement)) for u in main_units]

    def _readers(unit_name: str) -> tuple[str, ...]:
        found = set()
        for main_unit, mapped in mapped_main:
            if mapped.needs_curation:
                continue
            for t in mapped.schema_dict.get("tables", []):
                if t.get("name") == unit_name:
                    found.add(_original_script_path(main_unit.statement.source_path))
                    break
        return tuple(sorted(found))

    unit_reports = []
    for u in result.units:
        if u.kind == "main":
            continue
        source_path = _original_script_path(u.statement.source_path) if u.statement is not None else None
        rt_status = None
        if u.statement is not None:
            mapped_unit = map_statement(u.statement)
            is_mapped = not mapped_unit.needs_curation
            if is_mapped:
                rt_status = verify(mapped_unit, u.statement).status
        else:
            # A synthetic intermediate table (grand-total subquery rewrite)
            # has no backing SQL statement to map or round-trip verify —
            # ``schema_dict`` is itself the deliverable (see
            # test_analyzer_splitter.py's compile_select coverage).
            is_mapped = u.schema_dict is not None
        unit_reports.append(SplitUnitReport(
            kind=u.kind,
            name=u.name,
            path=u.path,
            source_path=source_path,
            readers=_readers(u.name),
            mapped=is_mapped,
            roundtrip_status=rt_status,
        ))

    return SplitReport(
        dag_mermaid=_render_dag_mermaid(dag.nodes),
        units=tuple(unit_reports),
        inlined=tuple(InlinedCteReport(cte_name=n.cte_name, source_path=n.source_path) for n in result.inlined),
    )
