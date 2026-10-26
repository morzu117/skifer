"""Analysis report: ties the mapper and the round-trip oracle together into one
per-statement verdict, and renders it as Markdown or JSON (Plan 43 — 43.2).

Serialization is an explicit allowlist, not ``asdict`` — a field added to a
dataclass later stays private until this module is deliberately updated to
expose it (the same discipline ``semantic/evidence.py`` uses for its DTOs).
"""
from __future__ import annotations

import json
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
