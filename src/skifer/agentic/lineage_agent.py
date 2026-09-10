"""
LineageAgent — conversational agent for column-level lineage analysis.

Wraps LineageTracker, DataDictionary, and LineageRenderer under an ask() API
consistent with GenBIAgent and BuilderAgent.

No Spark or LLM required for core functionality — analysis is purely static.
LLM is optional: used for intent parsing when keywords fail, and for narrative
explanations of lineage paths.
"""

from __future__ import annotations

import difflib
import json
import re
import unicodedata
from datetime import datetime
from typing import TYPE_CHECKING, Any

from ..lineage.tracker import LineageGraph, LineageTracker
from ..lineage.dictionary import DataDictionary
from ..lineage.renderer import LineageRenderer
from .history import HistoryEntry, SessionHistory
from .models import LineageResponse

if TYPE_CHECKING:
    from ..semantic.llm_provider import LLMProvider


# ---------------------------------------------------------------------------
# Keyword sets for deterministic intent routing
# ---------------------------------------------------------------------------

_TRACE_KEYWORDS = frozenset({
    "d'ou", "d'où", "vient", "provenance", "upstream", "source de",
    "where does", "come from", "origine", "provient", "provenance de",
})
_IMPACT_KEYWORDS = frozenset({
    "impact", "downstream", "affecte", "utilise par", "depend",
    "depends on", "affected", "affecté", "utilisé",
})
_RENDER_KEYWORDS = frozenset({
    "visualis", "diagram", "diagramme", "mermaid", "html",
    "render", "affiche", "montre", "show graph",
})
_LOOKUP_KEYWORDS = frozenset({
    "definit", "signifie", "definition", "meaning", "what is", "what does",
    "signification", "que signifie", "defined",
})

_STOPWORDS = frozenset({
    "vient", "impact", "signifie", "definit", "champ", "colonne",
    "field", "column", "table", "ou", "de", "la", "le", "les",
    "du", "est", "what", "does", "mean", "where", "from", "the",
    "qui", "quel", "quelle", "que", "un", "une", "pour", "avec",
    "dans", "sur", "par", "and", "or", "not", "is", "are",
})

_STEP_LLM_SYSTEM = """You are a lineage analysis router.
Parse the user question and return ONLY valid JSON with exactly these keys:
{
  "mode": "trace" | "impact" | "render" | "lookup",
  "table": "<table_name_or_empty_string>",
  "column": "<column_name_or_empty_string>"
}
mode meanings:
- trace: provenance — where does this field come from?
- impact: downstream — what does this field affect?
- render: visualize the full lineage graph
- lookup: definition — what does this field mean?
Return ONLY the JSON object, no other text."""


def _normalize(text: str) -> str:
    """Lowercase + strip accents for keyword matching."""
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c)).lower()


# ---------------------------------------------------------------------------
# LineageAgent
# ---------------------------------------------------------------------------

class LineageAgent:
    """
    Conversational agent for column-level lineage analysis.

    Usage::

        agent = LineageAgent(schema_dict=schema, target_name="gold.fact_orders")

        # Direct API
        resp = agent.trace("gold.fact_orders_output", "amount_eur")
        resp = agent.impact("silver.orders", "order_id")
        resp = agent.lookup("", "gross_revenue")
        diagram = agent.render(format="mermaid")

        # Natural language
        resp = agent.ask("D'où vient le champ amount_eur ?")
        resp = agent.ask("Quel est l'impact de order_id ?")
        resp = agent.ask("Que signifie gross_revenue ?")
    """

    def __init__(
        self,
        schema_dict: dict | None = None,
        schema_type: str = "core",
        target_name: str | None = None,
        llm_provider: LLMProvider | None = None,
        glossary_path: str | None = None,
        history: bool = False,
        session_title: str = "Lineage Analysis",
    ) -> None:
        self._graph: LineageGraph = LineageGraph()
        self._dictionary: DataDictionary | None = None
        self._renderer = LineageRenderer()
        self._llm = llm_provider
        self._history_enabled = history
        self._session_title = session_title
        self._session: SessionHistory | None = SessionHistory(session_title) if history else None

        if schema_dict is not None:
            self.load_schema(schema_dict, schema_type=schema_type, target_name=target_name)

        if glossary_path and self._dictionary is not None:
            self._dictionary.enrich_from_glossary(glossary_path)

    # ------------------------------------------------------------------
    # Schema loading
    # ------------------------------------------------------------------

    def load_schema(
        self,
        schema_dict: dict,
        schema_type: str = "core",
        target_name: str | None = None,
    ) -> None:
        """Build (or rebuild) the lineage graph from a schema dict."""
        if schema_type == "semantic":
            self._graph = LineageTracker.from_semantic_model(schema_dict)
        else:
            self._graph = LineageTracker.from_schema(schema_dict, target_name=target_name)
        self._dictionary = DataDictionary(self._graph)

    # ------------------------------------------------------------------
    # Direct API
    # ------------------------------------------------------------------

    def trace(self, table: str, column: str, narrative: bool = False) -> LineageResponse:
        """Upstream traversal — where does (table, column) come from?"""
        if not self._graph:
            return LineageResponse(
                question=f"trace({table}, {column})",
                mode="error",
                error="No schema loaded. Call load_schema() first.",
            )

        if table:
            edges = self._graph.upstream(table, column)
        else:
            edges = [
                e for t in self._graph.tables()
                for e in self._graph.upstream(t, column)
            ]

        resp = LineageResponse(
            question=f"trace({table}, {column})",
            mode="trace",
            table=table,
            column=column,
            edges=edges,
        )
        if narrative and self._llm and edges:
            resp.narrative = self._narrative(edges, "trace", column)
        return resp

    def impact(self, table: str, column: str, narrative: bool = False) -> LineageResponse:
        """Downstream traversal — what does (table, column) affect?"""
        if not self._graph:
            return LineageResponse(
                question=f"impact({table}, {column})",
                mode="error",
                error="No schema loaded. Call load_schema() first.",
            )

        if table:
            edges = self._graph.downstream(table, column)
        else:
            edges = [
                e for t in self._graph.tables()
                for e in self._graph.downstream(t, column)
            ]

        resp = LineageResponse(
            question=f"impact({table}, {column})",
            mode="impact",
            table=table,
            column=column,
            edges=edges,
        )
        if narrative and self._llm and edges:
            resp.narrative = self._narrative(edges, "impact", column)
        return resp

    def render(self, format: str = "mermaid", direction: str = "LR") -> str:
        """Render the full lineage graph as a string (mermaid / html / json)."""
        if not self._graph:
            return ""
        if format == "html":
            return self._renderer.to_html(self._graph, direction=direction)
        if format == "json":
            return json.dumps(self._renderer.to_json(self._graph), indent=2)
        return self._renderer.to_mermaid(self._graph, direction=direction)

    def lookup(self, table: str, column: str) -> LineageResponse:
        """Look up a field in the DataDictionary."""
        if self._dictionary is None:
            return LineageResponse(
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
            return LineageResponse(
                question=f"lookup({table}, {column})",
                mode="lookup",
                table=entry.table,
                column=column,
                field_entry=entry,
            )

        all_columns = [e.name for e in self._dictionary.list_fields()]
        suggestions = difflib.get_close_matches(column, all_columns, n=3, cutoff=0.6)
        return LineageResponse(
            question=f"lookup({table}, {column})",
            mode="error",
            table=table,
            column=column,
            error=f"Column '{column}' not found in dictionary.",
            suggestions=suggestions,
        )

    # ------------------------------------------------------------------
    # Conversational API
    # ------------------------------------------------------------------

    def ask(self, question: str) -> LineageResponse:
        """Parse a natural language question and dispatch to the right method."""
        if not self._graph:
            resp = LineageResponse(
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

    def _process(self, question: str) -> LineageResponse:
        norm = _normalize(question)
        mode = self._detect_mode(norm)

        if mode is None:
            if self._llm:
                return self._step_llm_parse(question)
            return LineageResponse(
                question=question,
                mode="error",
                error=(
                    "Could not determine intent from the question. "
                    "Use trace(), impact(), render(), or lookup() directly, "
                    "or provide an llm_provider for natural language support."
                ),
            )

        if mode == "render":
            diagram = self.render()
            return LineageResponse(question=question, mode="render", diagram=diagram)

        table, column = self._extract_field(question)
        if not column:
            return LineageResponse(
                question=question,
                mode="error",
                error="Could not identify a column name in your question.",
            )

        if mode == "trace":
            resp = self.trace(table, column)
        elif mode == "impact":
            resp = self.impact(table, column)
        else:
            resp = self.lookup(table, column)

        resp.question = question
        return resp

    def _detect_mode(self, norm: str) -> str | None:
        for kw in _TRACE_KEYWORDS:
            if kw in norm:
                return "trace"
        for kw in _IMPACT_KEYWORDS:
            if kw in norm:
                return "impact"
        for kw in _RENDER_KEYWORDS:
            if kw in norm:
                return "render"
        for kw in _LOOKUP_KEYWORDS:
            if kw in norm:
                return "lookup"
        return None

    def _extract_field(self, question: str) -> tuple[str, str]:
        """Extract (table, column) from a question string."""
        # Dotted notation: table.column or schema.table.column
        dotted = re.findall(r'\b([\w]+(?:\.[\w]+)+)\b', question)
        if dotted:
            parts = dotted[0].rsplit(".", 1)
            return parts[0], parts[1]

        # Bare word heuristic: last snake_case identifier
        words = re.findall(r'\b([a-z_][a-z0-9_]*)\b', question.lower())
        candidates = [w for w in words if w not in _STOPWORDS and len(w) > 2]
        if candidates:
            return "", candidates[-1]
        return "", ""

    def _step_llm_parse(self, question: str) -> LineageResponse:
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
            return LineageResponse(
                question=question, mode="error",
                error=f"LLM parse failed: {exc}",
            )

        if mode == "render":
            return LineageResponse(question=question, mode="render", diagram=self.render())
        if mode == "trace":
            resp = self.trace(table, column)
        elif mode == "impact":
            resp = self.impact(table, column)
        elif mode == "lookup":
            resp = self.lookup(table, column)
        else:
            return LineageResponse(
                question=question, mode="error",
                error=f"Unknown mode returned by LLM: '{mode}'.",
            )
        resp.question = question
        return resp

    def _narrative(self, edges: list[Any], direction: str, column: str) -> str:
        """Generate a prose explanation of lineage edges via LLM."""
        edges_summary = "; ".join(
            f"{e.source_table}.{e.source_column} → {e.target_table}.{e.target_column}"
            + (f" [{', '.join(e.transformations)}]" if e.transformations else "")
            for e in edges[:10]
        )
        prompt = (
            f"Explain in 1-3 sentences the {direction} lineage of column '{column}'.\n"
            f"Edges: {edges_summary}"
        )
        try:
            return self._llm.complete(
                system_prompt="You are a data lineage expert. Be concise and precise.",
                user_message=prompt,
                temperature=0.2,
            )
        except Exception:
            return ""

    def _log(self, resp: LineageResponse) -> None:
        if not self._history_enabled or self._session is None:
            return
        entry = HistoryEntry(
            timestamp=datetime.now().isoformat(),
            question=resp.question,
            mode=resp.mode,
            explanation=f"{len(resp.edges)} edge(s) found" if resp.edges else "",
            query_params={"table": resp.table, "column": resp.column},
            text_summary=resp.narrative or None,
            error=resp.error,
        )
        self._session.add(entry)

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def graph(self) -> LineageGraph:
        return self._graph

    @property
    def dictionary(self) -> DataDictionary | None:
        return self._dictionary

    @property
    def session(self) -> SessionHistory | None:
        return self._session

    def reset_history(self) -> None:
        if self._history_enabled:
            self._session = SessionHistory(self._session_title)
