"""Constrained LLM enrichment of the project analyzer's output (Plan 43 —
43.6): human-readable names and descriptions, never structure.

Every call sends the LLM only what has already been proven safe to share —
qualifier-stripped, literal-abstracted expression text (the same
canonicalization ``fingerprint.py`` already applies) or bare output column
names — never a table name, a join condition, or a filter literal value. The
response is parsed as closed JSON: any key outside the allowlist for that
call rejects the *whole* response with a named reason, never a partial,
silently-coerced result.

A proposed rule name is advisory only — reported alongside the docstring,
never used to rename the generated function or its ``business_rules:``
wiring, since a rename there would cross from naming into structure.
Fingerprint grouping is a *proposal* too: nothing is ever merged without a
human accepting it explicitly (not wired to any CLI flag in this slice —
there is nothing yet for a human to accept into).

Without a configured provider, ``enrich()`` is a silent no-op: every field of
the result is empty, exactly as if ``--enrich`` had not been passed.

**Scope cut, stated plainly:** a ``needs_curation`` fragment (the plan's
third 43.6 bullet — a proposed rule body for an unsupported construct) is not
enriched in this slice. The mapper (43.2) records only an abstract residue
*message* for such a fragment, not an isolated, qualifier-stripped expression
node the way ``fingerprint.py`` does for a scalar expression; sending the
statement's full SQL text instead would leak the very table names, join
conditions and literal values this module exists to keep away from the LLM.
Isolating a safe fragment for a window function (the corpus's one residual
case, ``08_kpi_monthly.sql``'s ``LAG()``) needs mapper-level work, deferred
rather than guessed at.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field

from skifer.analyzer.patterns import PatternReport, SharedRuleCandidate

_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

_RULE_SYSTEM_PROMPT = (
    "You name and document a generated data-pipeline rule. Respond with a "
    "single JSON object with exactly these keys: \"name\" (a short "
    "snake_case Python identifier) and \"docstring\" (one sentence, no "
    "markdown). You were given only an abstract expression shape — never "
    "mention a table name, a join condition, or a literal filter value."
)
_DRAFT_SYSTEM_PROMPT = (
    "You write a one-sentence description of a data pipeline draft from its "
    "output column names alone. Respond with a single JSON object with "
    "exactly one key: \"description\". Never invent a table name, a join "
    "condition, or any value you were not given."
)
_GROUPING_SYSTEM_PROMPT = (
    "You review a list of named rule candidates with their canonical "
    "expression shapes and propose GROUPS of candidates that look like the "
    "same business concept named differently. Respond with a single JSON "
    "object with exactly one key: \"groups\", a list of objects each with "
    "exactly \"label\" and \"candidate_names\" (names drawn only from the "
    "ones given to you)."
)

_RULE_ALLOWED_KEYS = frozenset({"name", "docstring"})
_DRAFT_ALLOWED_KEYS = frozenset({"description"})
_GROUPING_ALLOWED_KEYS = frozenset({"groups"})
_GROUP_ALLOWED_KEYS = frozenset({"label", "candidate_names"})


def _closed_json(raw: str, allowed_keys: frozenset[str]) -> tuple[dict | None, str | None]:
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        return None, f"not valid JSON: {exc}"
    if not isinstance(parsed, dict):
        return None, "response is not a JSON object"
    extra = set(parsed) - allowed_keys
    if extra:
        return None, f"response has keys outside the allowlist: {sorted(extra)}"
    return parsed, None


@dataclass(frozen=True)
class RuleEnrichment:
    """Advisory only: ``proposed_name`` is never used to rename anything."""

    candidate_name: str
    proposed_name: str | None = None
    docstring: str | None = None
    rejected_reason: str | None = None

    def to_dict(self) -> dict:
        return {
            "candidate_name": self.candidate_name,
            "proposed_name": self.proposed_name,
            "docstring": self.docstring,
            "rejected_reason": self.rejected_reason,
        }


@dataclass(frozen=True)
class DraftEnrichment:
    path: str
    description: str | None = None
    rejected_reason: str | None = None

    def to_dict(self) -> dict:
        return {
            "path": self.path,
            "description": self.description,
            "rejected_reason": self.rejected_reason,
        }


@dataclass(frozen=True)
class GroupingProposal:
    """Never auto-applied — a human decides whether these names are really
    the same concept."""

    label: str
    candidate_names: tuple[str, ...]

    def to_dict(self) -> dict:
        return {"label": self.label, "candidate_names": list(self.candidate_names)}


@dataclass(frozen=True)
class EnrichmentResult:
    rules: tuple[RuleEnrichment, ...] = field(default_factory=tuple)
    drafts: tuple[DraftEnrichment, ...] = field(default_factory=tuple)
    groupings: tuple[GroupingProposal, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict:
        return {
            "rules": [r.to_dict() for r in self.rules],
            "drafts": [d.to_dict() for d in self.drafts],
            "groupings": [g.to_dict() for g in self.groupings],
        }


def _enrich_rule(provider, candidate: SharedRuleCandidate) -> RuleEnrichment:
    fragment = candidate.occurrences[0].sql_text
    prompt = (
        f"Generated name: {candidate.name}\n"
        f"Canonical expression shape: {fragment}\n"
        f"Occurrences: {len(candidate.occurrences)}"
    )
    raw = provider.complete(system_prompt=_RULE_SYSTEM_PROMPT, user_message=prompt, temperature=0.0)
    parsed, reason = _closed_json(raw, _RULE_ALLOWED_KEYS)
    if parsed is None:
        return RuleEnrichment(candidate_name=candidate.name, rejected_reason=reason)

    proposed_name = parsed.get("name")
    if proposed_name is not None:
        if not isinstance(proposed_name, str) or not _NAME_RE.match(proposed_name):
            return RuleEnrichment(
                candidate_name=candidate.name,
                rejected_reason=f"proposed name {proposed_name!r} does not match ^[A-Za-z_][A-Za-z0-9_]*$",
            )
    docstring = parsed.get("docstring")
    if docstring is not None and not isinstance(docstring, str):
        return RuleEnrichment(candidate_name=candidate.name, rejected_reason="docstring is not a string")

    return RuleEnrichment(candidate_name=candidate.name, proposed_name=proposed_name, docstring=docstring)


def _enrich_draft(provider, path: str, output_columns: list[str]) -> DraftEnrichment:
    prompt = "Output columns: " + (", ".join(output_columns) if output_columns else "(none declared)")
    raw = provider.complete(system_prompt=_DRAFT_SYSTEM_PROMPT, user_message=prompt, temperature=0.0)
    parsed, reason = _closed_json(raw, _DRAFT_ALLOWED_KEYS)
    if parsed is None:
        return DraftEnrichment(path=path, rejected_reason=reason)

    description = parsed.get("description")
    if description is not None and not isinstance(description, str):
        return DraftEnrichment(path=path, rejected_reason="description is not a string")

    return DraftEnrichment(path=path, description=description)


def _propose_groupings(provider, candidates: list[SharedRuleCandidate]) -> tuple[GroupingProposal, ...]:
    if len(candidates) < 2:
        return ()

    known_names = {c.name for c in candidates}
    shapes = "\n".join(f"- {c.name}: {c.occurrences[0].sql_text}" for c in candidates)
    raw = provider.complete(
        system_prompt=_GROUPING_SYSTEM_PROMPT,
        user_message=f"Candidates:\n{shapes}",
        temperature=0.0,
    )
    parsed, _reason = _closed_json(raw, _GROUPING_ALLOWED_KEYS)
    if parsed is None:
        return ()

    groups = parsed.get("groups")
    if not isinstance(groups, list):
        return ()

    proposals: list[GroupingProposal] = []
    for group in groups:
        if not isinstance(group, dict) or set(group) - _GROUP_ALLOWED_KEYS:
            continue
        label = group.get("label")
        candidate_names = group.get("candidate_names")
        if not isinstance(label, str) or not isinstance(candidate_names, list):
            continue
        kept = tuple(n for n in candidate_names if isinstance(n, str) and n in known_names)
        if len(kept) >= 2:
            proposals.append(GroupingProposal(label=label, candidate_names=kept))
    return tuple(proposals)


def enrich(
    report: PatternReport,
    draft_output_columns: dict[str, list[str]],
    *,
    provider,
) -> EnrichmentResult:
    """Propose names/docstrings for *report*'s shared-rule candidates, a
    description for each draft in *draft_output_columns* (relative path ->
    its declared output column names), and fingerprint groupings.
    ``provider is None`` is a silent no-op: every result list is empty.
    """
    if provider is None:
        return EnrichmentResult()

    rules = tuple(_enrich_rule(provider, c) for c in report.shared_rule_candidates)
    drafts = tuple(
        _enrich_draft(provider, path, columns)
        for path, columns in sorted(draft_output_columns.items())
    )
    groupings = _propose_groupings(provider, list(report.shared_rule_candidates))

    return EnrichmentResult(rules=rules, drafts=drafts, groupings=groupings)
