"""
DictionaryAgent — conversational agent for field-level data dictionary lookups.

Wraps DataDictionary and GlossaryReader under an ask() API consistent with
LineageAgent, QualityAgent, GenBIAgent, and BuilderAgent.

No Spark or LLM required for core functionality — lookups are purely static.
LLM is optional: used for intent parsing when keywords fail and for narrative
explanations of field definitions.
"""

from __future__ import annotations

import difflib
import json
import re
import unicodedata
from datetime import datetime
from typing import TYPE_CHECKING, Any

from ..lineage.tracker import LineageTracker
from ..lineage.dictionary import DataDictionary
from .history import HistoryEntry, SessionHistory
from .models import DictionaryResponse

if TYPE_CHECKING:
    from ..semantic.llm_provider import LLMProvider


# ---------------------------------------------------------------------------
# Keyword sets for deterministic intent routing
# ---------------------------------------------------------------------------

_LOOKUP_KEYWORDS = frozenset({
    "signifie", "definition", "what is", "what does", "que veut dire",
    "explique", "explain", "trouve", "cherche", "find", "lookup", "get",
    "defined", "signification", "que signifie",
})
_LIST_KEYWORDS = frozenset({
    "liste", "list", "show", "montre", "tous", "toutes",
    "champs", "colonnes", "fields", "columns", "enumerate",
})
_EXPORT_KEYWORDS = frozenset({
    "export", "json", "rapport", "report", "dictionnaire", "dictionary",
    "genere", "generate", "serialise", "serialize",
})

_STOPWORDS = frozenset({
    "la", "le", "les", "du", "de", "est", "un", "une", "pour", "sur",
    "dans", "par", "et", "ou", "que", "qui", "quel", "quelle", "avec",
    "the", "is", "are", "for", "on", "at", "with", "this", "that", "of",
    "table", "champ", "colonne", "field", "column",
    "what", "does", "mean", "how", "why", "where", "when",
})

_STEP_LLM_SYSTEM = """You are a data dictionary routing agent.
Parse the user question and return ONLY valid JSON with exactly these keys:
{
  "mode": "lookup" | "list" | "export",
  "table": "<table_name_or_empty_string>",
  "column": "<column_name_or_empty_string>"
}
mode meanings:
- lookup: find the definition/metadata of a specific field
- list: list all fields (optionally filtered by table)
- export: export the full data dictionary
Return ONLY the JSON object, no other text."""


def _normalize(text: str) -> str:
    """Lowercase + strip accents for keyword matching."""
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c)).lower()


# ---------------------------------------------------------------------------
# DictionaryAgent
# ---------------------------------------------------------------------------

class DictionaryAgent:
    """
    Conversational agent for field-level data dictionary lookups.

    Usage::

        agent = DictionaryAgent(schema_dict=schema, target_name="gold.fact_orders")
        agent.enrich("glossary.yaml")

        # Direct API
        resp = agent.lookup("gold.fact_orders_output", "amount_eur")
        resp = agent.lookup("", "amount_eur")          # search all tables
        resp = agent.list_fields("gold.fact_orders_output")
        txt  = agent.export(format="json")

        # Natural language
        resp = agent.ask("Que signifie amount_eur ?")
        resp = agent.ask("Liste les champs de gold.fact_orders_output")
        resp = agent.ask("Exporte le dictionnaire en JSON")
    """

    def __init__(
        self,
        schema_dict: dict | None = None,
        schema_type: str = "core",
        target_name: str | None = None,
        glossary_path: str | None = None,
        llm_provider: LLMProvider | None = None,
        session_history: bool = False,
        session_title: str = "Dictionary",
    ) -> None:
        self._dictionary: DataDictionary | None = None
        self._llm = llm_provider
        self._session_history_enabled = session_history
        self._session_title = session_title
        self._session: SessionHistory | None = (
            SessionHistory(session_title) if session_history else None
        )

        if schema_dict is not None:
            self.load_schema(schema_dict, schema_type=schema_type, target_name=target_name)

        if glossary_path and self._dictionary is not None:
            self.enrich(glossary_path)

    # ------------------------------------------------------------------
    # Schema loading / enrichissement
    # ------------------------------------------------------------------

    def load_schema(
        self,
        schema_dict: dict,
        schema_type: str = "core",
        target_name: str | None = None,
    ) -> None:
        """Build (or rebuild) the DataDictionary from a schema dict."""
        if schema_type == "semantic":
            graph = LineageTracker.from_semantic_model(schema_dict)
        else:
            graph = LineageTracker.from_schema(schema_dict, target_name=target_name)
        self._dictionary = DataDictionary(graph)

    def enrich(self, glossary_path: str) -> None:
        """Enrich field descriptions from a glossary file (JSON/YAML/TXT/PDF/PPTX)."""
        if self._dictionary is not None:
            self._dictionary.enrich_from_glossary(glossary_path)

    # ------------------------------------------------------------------
    # Direct API
    # ------------------------------------------------------------------

    def lookup(self, table: str, column: str, narrative: bool = False) -> DictionaryResponse:
        """Look up a field in the DataDictionary."""
        if self._dictionary is None:
            return DictionaryResponse(
                question=f"lookup({table}, {column})",
                mode="error",
                error="No schema loaded. Call load_schema() first.",
            )

        if table:
            entry = self._dictionary.get(table, column)
        else:
            matches = [e for e in self._dictionary.list_fields() if e.name == column]
            entry = matches[0] if matches else None

        if entry is not None:
            resp = DictionaryResponse(
                question=f"lookup({table}, {column})",
                mode="lookup",
                table=entry.table,
                column=column,
                entry=entry,
            )
            if narrative and self._llm:
                resp.narrative = self._narrative(entry)
            return resp

        # Deduplicated: a column name present in two tables would otherwise fill two
        # of the three suggestion slots with the same word, which reads as a bug and
        # costs the reader a real alternative.
        all_columns = list(dict.fromkeys(e.name for e in self._dictionary.list_fields()))
        suggestions = difflib.get_close_matches(column, all_columns, n=3, cutoff=0.6)
        return DictionaryResponse(
            question=f"lookup({table}, {column})",
            mode="error",
            table=table,
            column=column,
            error=f"Column '{column}' not found in dictionary.",
            suggestions=suggestions,
        )

    def list_fields(self, table: str | None = None) -> DictionaryResponse:
        """List all fields, optionally filtered by table."""
        if self._dictionary is None:
            return DictionaryResponse(
                question=f"list_fields({table or ''})",
                mode="error",
                error="No schema loaded. Call load_schema() first.",
            )
        entries = self._dictionary.list_fields(table=table)
        return DictionaryResponse(
            question=f"list_fields({table or ''})",
            mode="list",
            table=table or "",
            entries=entries,
        )

    def export(self, format: str = "text") -> str:
        """Export the full DataDictionary as a formatted string (text or json)."""
        if self._dictionary is None:
            return ""
        if format == "json":
            return json.dumps(self._dictionary.to_dict(), indent=2, ensure_ascii=False)
        return self._format_text()

    # ------------------------------------------------------------------
    # Conversational API
    # ------------------------------------------------------------------

    def ask(self, question: str) -> DictionaryResponse:
        """Parse a natural language question and dispatch to the right method."""
        if self._dictionary is None:
            resp = DictionaryResponse(
                question=question,
                mode="error",
                error="No schema loaded. Call load_schema() first.",
            )
            self._log(resp)
            return resp

        resp = self._process(question)
        self._log(resp)
        return resp

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _process(self, question: str) -> DictionaryResponse:
        norm = _normalize(question)
        mode = self._detect_mode(norm)

        if mode is None:
            if self._llm:
                return self._step_llm_parse(question)
            return DictionaryResponse(
                question=question,
                mode="error",
                error=(
                    "Could not determine intent from the question. "
                    "Use lookup(), list_fields(), or export() directly, "
                    "or provide an llm_provider for natural language support."
                ),
            )

        if mode == "export":
            text_out = self.export()
            return DictionaryResponse(
                question=question,
                mode="export",
                text_output=text_out,
            )

        if mode == "list":
            table = self._extract_fqn(question)
            resp = self.list_fields(table or None)
            resp.question = question
            return resp

        # lookup
        table, column = self._extract_field(question)
        if not column:
            return DictionaryResponse(
                question=question,
                mode="error",
                error="Could not identify a column name in your question.",
            )
        resp = self.lookup(table, column)
        resp.question = question
        return resp

    def _detect_mode(self, norm: str) -> str | None:
        for kw in _LOOKUP_KEYWORDS:
            if kw in norm:
                return "lookup"
        for kw in _LIST_KEYWORDS:
            if kw in norm:
                return "list"
        for kw in _EXPORT_KEYWORDS:
            if kw in norm:
                return "export"
        return None

    def _extract_field(self, question: str) -> tuple[str, str]:
        """Extract (table, column) from a question string."""
        dotted = re.findall(r'\b([\w]+(?:\.[\w]+)+)\b', question)
        if dotted:
            parts = dotted[0].rsplit(".", 1)
            return parts[0], parts[1]
        words = re.findall(r'\b([a-z_][a-z0-9_]*)\b', question.lower())
        candidates = [w for w in words if w not in _STOPWORDS and len(w) > 2]
        return ("", candidates[-1]) if candidates else ("", "")

    def _extract_fqn(self, question: str) -> str:
        """Extract a dotted FQN or bare table name from a question."""
        dotted = re.findall(r'\b([\w]+(?:\.[\w]+){1,2})\b', question)
        if dotted:
            return dotted[0]
        words = re.findall(r'\b([a-z_][a-z0-9_]*)\b', question.lower())
        candidates = [w for w in words if w not in _STOPWORDS and len(w) > 3]
        return candidates[-1] if candidates else ""

    def _step_llm_parse(self, question: str) -> DictionaryResponse:
        """Use LLM to parse intent + field when keyword routing fails."""
        try:
            raw = self._llm.complete(
                system_prompt=_STEP_LLM_SYSTEM,
                user_message=question,
                temperature=0.0,
            )
            parsed = json.loads(raw)
            mode = parsed.get("mode", "")
            table = parsed.get("table", "")
            column = parsed.get("column", "")
        except Exception as exc:
            return DictionaryResponse(
                question=question, mode="error",
                error=f"LLM parse failed: {exc}",
            )

        if mode == "export":
            return DictionaryResponse(
                question=question, mode="export", text_output=self.export()
            )
        if mode == "list":
            resp = self.list_fields(table or None)
        elif mode == "lookup":
            resp = self.lookup(table, column)
        else:
            return DictionaryResponse(
                question=question, mode="error",
                error=f"Unknown mode returned by LLM: '{mode}'.",
            )
        resp.question = question
        return resp

    def _narrative(self, entry: Any) -> str:
        """Generate a prose explanation of a FieldEntry via LLM."""
        entry_dict = {
            "name": entry.name,
            "table": entry.table,
            "description": entry.description,
            "source_fields": entry.source_fields,
            "transformations": entry.transformations,
        }
        prompt = (
            f"Explain in 1-2 sentences what column '{entry.name}' means "
            f"based on the following metadata: {json.dumps(entry_dict)}"
        )
        try:
            return self._llm.complete(
                system_prompt="You are a data documentation expert. Be concise and precise.",
                user_message=prompt,
                temperature=0.2,
            )
        except Exception:
            return ""

    def _format_text(self) -> str:
        """Format the DataDictionary as a human-readable text report."""
        lines = ["Data Dictionary", "=" * 50]
        current_table = None
        for entry in self._dictionary.list_fields():
            if entry.table != current_table:
                current_table = entry.table
                lines.append(f"\n{current_table}")
                lines.append("-" * 40)
            desc = f" — {entry.description}" if entry.description else ""
            lines.append(f"  {entry.name}{desc}")
            if entry.source_fields:
                lines.append(f"    from : {', '.join(entry.source_fields)}")
            if entry.transformations:
                lines.append(f"    ops  : {', '.join(entry.transformations)}")
        return "\n".join(lines)

    def _log(self, resp: DictionaryResponse) -> None:
        if not self._session_history_enabled or self._session is None:
            return
        count = (
            f"{len(resp.entries)} field(s)" if resp.entries
            else ("found" if resp.entry else "")
        )
        entry = HistoryEntry(
            timestamp=datetime.now().isoformat(),
            question=resp.question,
            mode=resp.mode,
            explanation=count,
            query_params={"table": resp.table, "column": resp.column},
            text_summary=resp.narrative or None,
            error=resp.error,
        )
        self._session.add(entry)

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def dictionary(self) -> DataDictionary | None:
        return self._dictionary

    @property
    def session(self) -> SessionHistory | None:
        return self._session

    def reset_history(self) -> None:
        if self._session_history_enabled:
            self._session = SessionHistory(self._session_title)
