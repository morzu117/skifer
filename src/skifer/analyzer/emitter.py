"""Turns a validated analysis of a SQL corpus into real artifacts (Plan 43 —
43.4): pipeline YAML drafts under ``.drafts/`` and shared-rule Python stubs
under ``rules/_generated/``.

Every artifact is *staged* first — built in memory, then checked (a draft
must pass ``parse_schema``, a rule stub must parse and profile via
``RuleAnalyzer.analyze_source``) — and only a validated artifact is ever
written, mirroring ``adaptive/generator.py``'s ``ProposalGenerator`` pattern.
A file already carrying the ``_generated_by`` marker and the same source hash
is left untouched (byte-for-byte reproducible re-runs); a file without the
marker is never overwritten (it is presumed hand-edited).

**Scope cuts, stated plainly:**

* Only a ``main`` unit with a real ``CREATE``/``INSERT`` target, and an
  ``intermediate_table`` unit, are ever drafted — a bare ``SELECT`` with no
  materialized target (this corpus's 05 and 08) is not a Skifer pipeline
  sink, so it is reported as skipped, never drafted.
* A unit whose mapped ``tables:`` references a ``partial`` by name is skipped
  too — rewriting a ``tables:`` entry into a ``partials:`` block (Plan 25)
  is deferred; no statement in the 43.0 corpus actually needs it today.
* A generated rule stub always falls back to ``F.expr("<fragment>")`` — the
  richer translation into native ``F.*`` calls that
  ``RuleService.generate_snippet`` offers for a narrower subset is deferred;
  ``F.expr`` is always correct, just not as native-looking.
* ``metadata`` carries ``source`` and ``needs_curation`` (the unit's own
  non-blocking residue messages); ``coverage``/``roundtrip``/``layer_reason``
  are not populated in this slice — a human reads the dry-run report for
  those today.
"""
from __future__ import annotations

import hashlib
import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from skifer.analyzer.mapper import map_statement
from skifer.analyzer.parser import Statement
from skifer.analyzer.patterns import PatternThresholds, SharedRuleCandidate, find_patterns
from skifer.analyzer.splitter import split_corpus
from skifer.core.rule_analyzer import RuleAnalyzer
from skifer.core.schema_loader import parse_schema
from skifer.semantic.persistence import write_yaml_atomic

_GENERATED_BY = "project_analyzer"


def _original_script_path(source_path: str) -> str:
    return source_path.split("::", 1)[0]


def _content_hash(text: str) -> str:
    return "sha256:v1:" + hashlib.sha256(text.encode("utf-8")).hexdigest()


def _draft_relative_path(fqn: str) -> str:
    parts = fqn.split(".")
    if len(parts) < 2:
        return f"{fqn}.yaml"
    return f"{parts[0]}/{'.'.join(parts[1:])}.yaml"


def _rule_stub_relative_path(name: str) -> str:
    return f"rules/_generated/{name}.py"


@dataclass(frozen=True)
class EmittedArtifact:
    """One attempted write — a draft or a rule stub."""

    kind: str  # "draft" | "rule_stub"
    path: str
    status: str  # "written" | "unchanged" | "refused_exists" | "refused_invalid"
    reason: str | None = None

    def to_dict(self) -> dict:
        return {"kind": self.kind, "path": self.path, "status": self.status, "reason": self.reason}


@dataclass(frozen=True)
class SkippedUnit:
    """A unit this slice never attempts to emit — named, with why."""

    name: str | None
    reason: str

    def to_dict(self) -> dict:
        return {"name": self.name, "reason": self.reason}


@dataclass(frozen=True)
class EmitReport:
    artifacts: tuple[EmittedArtifact, ...] = field(default_factory=tuple)
    skipped: tuple[SkippedUnit, ...] = field(default_factory=tuple)

    @property
    def has_residue(self) -> bool:
        return bool(self.skipped) or any(
            a.status in ("refused_exists", "refused_invalid") for a in self.artifacts
        )

    def to_dict(self) -> dict:
        return {
            "artifacts": [a.to_dict() for a in self.artifacts],
            "skipped": [s.to_dict() for s in self.skipped],
        }


# ---------------------------------------------------------------------------
# Draft construction
# ---------------------------------------------------------------------------


def _collect_draftable(statements: list[Statement]):
    """Return ``(drafts, skipped)``: ``drafts`` maps a relative path to
    ``(schema_dict, original_script_path, residue_messages)``; ``skipped``
    lists every non-``main`` unit and every ineligible ``main``/
    ``intermediate_table`` unit with the reason it was left out."""
    result = split_corpus(statements)

    partial_names = {u.name for u in result.units if u.kind == "partial"}
    drafts: dict[str, tuple[dict, str, list[str]]] = {}
    skipped: list[SkippedUnit] = []

    for unit in result.units:
        if unit.kind == "partial":
            skipped.append(SkippedUnit(
                name=unit.name,
                reason="partials are not attached to an emitted parent in this slice.",
            ))
            continue

        if unit.kind == "intermediate_table":
            if unit.schema_dict is not None:
                schema_dict, residues = unit.schema_dict, []
                source_script = None
            elif unit.statement is not None:
                mapped = map_statement(unit.statement)
                if mapped.needs_curation:
                    skipped.append(SkippedUnit(
                        name=unit.name,
                        reason=f"needs_curation: {[r.message for r in mapped.blocking_residues]}",
                    ))
                    continue
                schema_dict = mapped.schema_dict
                residues = [r.message for r in mapped.residues if not r.blocking]
                source_script = _original_script_path(unit.statement.source_path)
            else:
                skipped.append(SkippedUnit(name=unit.name, reason="no statement and no schema_dict."))
                continue
            table_names = {t.get("name") for t in schema_dict.get("tables", [])}
            if table_names & partial_names:
                skipped.append(SkippedUnit(
                    name=unit.name,
                    reason=f"references partial(s) {sorted(table_names & partial_names)} — not rewired in this slice.",
                ))
                continue
            drafts[_draft_relative_path(unit.name)] = (schema_dict, source_script, residues)
            continue

        # kind == "main"
        if unit.statement is None:
            continue
        mapped = map_statement(unit.statement)
        script = _original_script_path(unit.statement.source_path)
        if mapped.target is None:
            skipped.append(SkippedUnit(
                name=script,
                reason="no CREATE/INSERT target — a bare SELECT is not a Skifer pipeline sink.",
            ))
            continue
        if mapped.needs_curation:
            skipped.append(SkippedUnit(
                name=script,
                reason=f"needs_curation: {[r.message for r in mapped.blocking_residues]}",
            ))
            continue
        table_names = {t.get("name") for t in mapped.schema_dict.get("tables", [])}
        if table_names & partial_names:
            skipped.append(SkippedUnit(
                name=script,
                reason=f"references partial(s) {sorted(table_names & partial_names)} — not rewired in this slice.",
            ))
            continue
        residues = [r.message for r in mapped.residues if not r.blocking]
        drafts[_draft_relative_path(mapped.target)] = (mapped.schema_dict, script, residues)

    return drafts, skipped


def list_draft_output_columns(statements: list[Statement]) -> dict[str, list[str]]:
    """Output column names per draft relative path, best-effort (Plan 43 —
    43.6): read directly off ``select_final``'s target aliases when present;
    empty for a ``keep_all_columns`` draft, whose real output columns aren't
    known without executing Spark. Never a table name, a join condition or a
    filter value — only the final output aliases a human already chose,
    which is what makes this safe to hand to an LLM enrichment call.
    """
    drafts, _skipped = _collect_draftable(statements)
    result: dict[str, list[str]] = {}
    for rel_path, (schema_dict, _script, _residues) in drafts.items():
        select_final = schema_dict.get("select_final") or []
        columns = [
            entry[1] if isinstance(entry, list) else entry.get("target")
            for entry in select_final
        ]
        result[rel_path] = [c for c in columns if isinstance(c, str)]
    return result


def _sql_sha256(statements: list[Statement], script: str) -> str:
    texts = sorted(s.text for s in statements if s.source_path == script)
    return hashlib.sha256("\n".join(texts).encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Rule stub construction
# ---------------------------------------------------------------------------


def _rule_stub_source(candidate: SharedRuleCandidate) -> str:
    fragment = candidate.occurrences[0].sql_text
    return (
        f'"""Generated by {_GENERATED_BY} — do not edit by hand.\n\n'
        f"Recurring expression found in {len(candidate.occurrences)} places; "
        f'see the analyzer report for every occurrence.\n"""\n'
        "from pyspark.sql import functions as F\n\n"
        "from skifer.core.registry import RuleRegistry\n\n\n"
        "@RuleRegistry.register_rule()\n"
        f"def {candidate.name}(df):\n"
        f'    return {{"{candidate.name}": F.expr("{fragment}")}}\n\n\n'
        f"# {_GENERATED_BY}:marker\n"
        f"# {_GENERATED_BY}:source_hash={_content_hash(fragment)}\n"
    )


def _existing_source_hash_from_py(text: str) -> str | None:
    for line in text.splitlines():
        prefix = f"# {_GENERATED_BY}:source_hash="
        if line.startswith(prefix):
            return line[len(prefix):]
    return None


def _has_marker_py(text: str) -> bool:
    return f"# {_GENERATED_BY}:marker" in text


def _atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_path = tempfile.mkstemp(dir=str(path.parent), prefix=f".{path.stem}.", suffix=".tmp", text=True)
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


# ---------------------------------------------------------------------------
# Wiring: a draft that uses a shared rule references it, instead of
# repeating the raw expression in its own select_final.
# ---------------------------------------------------------------------------


def _wire_business_rules(schema_dict: dict, script: str, candidates: list[SharedRuleCandidate]) -> dict:
    wired = dict(schema_dict)
    select_final = list(wired.get("select_final") or [])
    if not select_final:
        return wired

    rule_names: set[str] = set()
    kept = []
    for entry in select_final:
        target_name = entry[1] if isinstance(entry, list) else entry.get("target")
        matched = next((
            c for c in candidates
            if c.kind == "projection"
            and any(o.source_path == script and o.output_alias == target_name for o in c.occurrences)
        ), None)
        if matched is not None:
            rule_names.add(matched.name)
        else:
            kept.append(entry)

    if not rule_names:
        return wired

    wired["select_final"] = kept
    if not kept:
        del wired["select_final"]
    existing_rules = list(wired.get("business_rules") or [])
    wired["business_rules"] = sorted(set(existing_rules) | rule_names)
    return wired


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def emit_corpus(
    statements: list[Statement],
    *,
    out_dir: str | Path,
    thresholds: PatternThresholds = PatternThresholds(),
    dry_run: bool = False,
) -> EmitReport:
    """Stage, validate and publish drafts + shared-rule stubs for *statements*
    into *out_dir* — the project root — as ``<out_dir>/schemas/.drafts/...``
    (D5) and ``<out_dir>/rules/_generated/...``.

    ``dry_run=True`` stages and validates everything exactly as a real run
    would, but never reads or writes *out_dir*: every artifact that would
    validate is reported ``"would_write"``, so a caller can decide whether a
    residue exists before a single byte is touched on disk.

    ``split_corpus``/``map_statement`` rewrite table references on the parsed
    AST in place (43.3). Calling this twice with the *same* ``Statement``
    objects therefore does not exercise the unchanged-file fast path
    faithfully — re-emit against a fresh ``parse_script()`` each time, the way
    the CLI's own one-process-per-invocation model already does.
    """
    out_dir = Path(out_dir)
    patterns = find_patterns(statements, thresholds=thresholds)
    drafts, skipped = _collect_draftable(statements)

    artifacts: list[EmittedArtifact] = []

    for rel_path, (schema_dict, script, residues) in sorted(drafts.items()):
        wired = _wire_business_rules(schema_dict, script, list(patterns.shared_rule_candidates)) if script else dict(schema_dict)
        payload = dict(wired)
        source_hash = _sql_sha256(statements, script) if script else _content_hash(rel_path)
        payload["_generated_by"] = _GENERATED_BY
        payload["metadata"] = {
            "source": {"file": script, "sql_sha256": source_hash},
            "needs_curation": residues,
        }

        label = str(Path("schemas/.drafts") / rel_path)
        try:
            parse_schema(yaml.safe_dump(payload, sort_keys=False, allow_unicode=True), params={})
        except Exception as exc:
            artifacts.append(EmittedArtifact(
                kind="draft", path=label,
                status="refused_invalid", reason=f"{type(exc).__name__}: {exc}",
            ))
            continue

        if dry_run:
            artifacts.append(EmittedArtifact(kind="draft", path=label, status="would_write"))
            continue

        full_path = out_dir / "schemas" / ".drafts" / rel_path
        artifacts.append(_publish_yaml(full_path, payload, source_hash, label))

    for candidate in patterns.shared_rule_candidates:
        rel_path = _rule_stub_relative_path(candidate.name)
        if candidate.kind != "projection":
            artifacts.append(EmittedArtifact(
                kind="rule_stub", path=rel_path, status="refused_invalid",
                reason=f"kind '{candidate.kind}' has no stub translation in this slice.",
            ))
            continue
        source_text = _rule_stub_source(candidate)
        try:
            import ast as _ast
            _ast.parse(source_text)
        except SyntaxError as exc:
            artifacts.append(EmittedArtifact(
                kind="rule_stub", path=rel_path, status="refused_invalid", reason=str(exc),
            ))
            continue
        profile = RuleAnalyzer().analyze_source(source_text, candidate.name)
        if not profile.source_available:
            artifacts.append(EmittedArtifact(
                kind="rule_stub", path=rel_path, status="refused_invalid",
                reason="RuleAnalyzer could not profile the generated source.",
            ))
            continue

        if dry_run:
            artifacts.append(EmittedArtifact(kind="rule_stub", path=rel_path, status="would_write"))
            continue

        full_path = out_dir / rel_path
        artifacts.append(_publish_py(full_path, source_text, rel_path))

    return EmitReport(artifacts=tuple(artifacts), skipped=tuple(skipped))


def _publish_yaml(full_path: Path, payload: dict, source_hash: str, rel_label: str) -> EmittedArtifact:
    if full_path.exists():
        try:
            existing = yaml.safe_load(full_path.read_text(encoding="utf-8")) or {}
        except yaml.YAMLError:
            existing = {}
        if existing.get("_generated_by") != _GENERATED_BY:
            return EmittedArtifact(
                kind="draft", path=rel_label, status="refused_exists",
                reason="file exists without the project_analyzer marker — presumed hand-edited.",
            )
        if existing.get("metadata", {}).get("source", {}).get("sql_sha256") == source_hash:
            return EmittedArtifact(kind="draft", path=rel_label, status="unchanged")

    write_yaml_atomic(full_path, payload)
    return EmittedArtifact(kind="draft", path=rel_label, status="written")


def _publish_py(full_path: Path, source_text: str, rel_label: str) -> EmittedArtifact:
    new_hash = _existing_source_hash_from_py(source_text)
    if full_path.exists():
        existing_text = full_path.read_text(encoding="utf-8")
        if not _has_marker_py(existing_text):
            return EmittedArtifact(
                kind="rule_stub", path=rel_label, status="refused_exists",
                reason="file exists without the project_analyzer marker — presumed hand-edited.",
            )
        if _existing_source_hash_from_py(existing_text) == new_hash:
            return EmittedArtifact(kind="rule_stub", path=rel_label, status="unchanged")

    _atomic_write_text(full_path, source_text)
    return EmittedArtifact(kind="rule_stub", path=rel_label, status="written")
