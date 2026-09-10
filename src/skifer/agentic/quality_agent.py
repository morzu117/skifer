"""
QualityAgent — conversational agent for data quality checks.

Wraps DataMonitor, ContractExtractor, HistoryStore, MonitorReporter, and
AlertDispatcher under an ask() API consistent with GenBIAgent and LineageAgent.

A backend (any object with a sql(query) method) is required to run live checks.
LLM is optional: used for intent parsing when keywords fail and for narrative
summaries of check results.
"""

from __future__ import annotations

import json
import re
import unicodedata
from datetime import datetime
from typing import TYPE_CHECKING, Any

from ..observability.monitor import DataMonitor
from ..observability.contracts import ContractExtractor
from ..observability.reporter import MonitorReporter
from ..observability.alerts import AlertDispatcher
from .history import HistoryEntry, SessionHistory
from .models import QualityResponse

if TYPE_CHECKING:
    from ..semantic.llm_provider import LLMProvider


# ---------------------------------------------------------------------------
# Keyword sets for deterministic intent routing
# ---------------------------------------------------------------------------

_CHECK_KEYWORDS = frozenset({
    "sain", "healthy", "check", "verifie", "audit", "qualite", "quality",
    "analyse", "analyze", "valide", "validate", "tester", "test",
})
_HISTORY_KEYWORDS = frozenset({
    "historique", "history", "tendance", "trend", "evolution",
    "derniers", "precedents", "previous", "last", "passe",
})
_REPORT_KEYWORDS = frozenset({
    "rapport", "report", "resume", "summary", "affiche", "montre",
    "show", "display", "genere", "generate",
})

_STOPWORDS = frozenset({
    "la", "le", "les", "du", "de", "est", "elle", "saine", "table",
    "the", "is", "are", "for", "on", "at", "with", "this", "that",
    "un", "une", "pour", "sur", "dans", "par", "et", "ou", "mais",
})

_STEP_LLM_SYSTEM = """You are a data quality routing agent.
Parse the user question and return ONLY valid JSON with exactly these keys:
{
  "mode": "check" | "history" | "report",
  "table": "<table_fqn_or_empty_string>"
}
mode meanings:
- check: run quality checks on a table
- history: retrieve past check results for a table
- report: generate a formatted report of the latest check
Return ONLY the JSON object, no other text."""


def _normalize(text: str) -> str:
    """Lowercase + strip accents for keyword matching."""
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c)).lower()


# ---------------------------------------------------------------------------
# QualityAgent
# ---------------------------------------------------------------------------

class QualityAgent:
    """
    Conversational agent for data quality checks.

    Usage::

        agent = QualityAgent(backend=spark_backend, history_store=SqliteHistoryStore())

        # Direct API
        resp = agent.check("gold.fact_orders", contracts=[NullCheck("gold.fact_orders", "amount")])
        resp = agent.check("gold.fact_orders", schema_dict=schema)
        resp = agent.get_history("gold.fact_orders", n=5)
        txt  = agent.report("gold.fact_orders", format="text")

        # Natural language
        resp = agent.ask("La table gold.fact_orders est-elle saine ?")
        resp = agent.ask("Montre l'historique de gold.fact_orders")
        resp = agent.ask("Génère un rapport sur gold.fact_orders")
    """

    def __init__(
        self,
        backend: Any,
        history_store: Any = None,
        llm_provider: LLMProvider | None = None,
        alert_config: dict | None = None,
        schema_dict: dict | None = None,
        session_history: bool = False,
        session_title: str = "Quality Analysis",
    ) -> None:
        self._monitor = DataMonitor(backend, history_store=history_store)
        self._extractor = ContractExtractor()
        self._reporter = MonitorReporter()
        self._dispatcher = AlertDispatcher()
        self._history_store = history_store
        self._llm = llm_provider
        self._alert_config = alert_config
        self._default_schema_dict = schema_dict
        self._session_history_enabled = session_history
        self._session_title = session_title
        self._session: SessionHistory | None = (
            SessionHistory(session_title) if session_history else None
        )

    # ------------------------------------------------------------------
    # Direct API
    # ------------------------------------------------------------------

    def check(
        self,
        fqn: str,
        contracts: list | None = None,
        schema_dict: dict | None = None,
        raise_on_critical: bool = False,
        narrative: bool = False,
    ) -> QualityResponse:
        """Run quality checks on a table and return a QualityResponse."""
        if contracts is None and schema_dict is None:
            return QualityResponse(
                question=f"check({fqn})",
                mode="error",
                table=fqn,
                error=(
                    "Provide either 'contracts' (list[DataContract]) or "
                    "'schema_dict' to derive contracts automatically."
                ),
            )

        try:
            if contracts is None:
                report = self._monitor.check_from_schema(
                    fqn, schema_dict, raise_on_critical=raise_on_critical
                )
            else:
                report = self._monitor.check_table(
                    fqn, contracts, raise_on_critical=raise_on_critical
                )
        except Exception as exc:
            return QualityResponse(
                question=f"check({fqn})",
                mode="error",
                table=fqn,
                error=str(exc),
            )

        if self._alert_config:
            try:
                self._dispatcher.dispatch(report, self._alert_config)
            except Exception:
                pass  # alerts are best-effort

        text_out = self._reporter.to_text(report)
        narr = ""
        if narrative and self._llm:
            narr = self._narrative(report)

        return QualityResponse(
            question=f"check({fqn})",
            mode="check",
            table=fqn,
            report=report,
            text_output=text_out,
            narrative=narr,
        )

    def get_history(self, fqn: str, n: int = 5) -> QualityResponse:
        """Retrieve the last n MonitorReports for a table from the HistoryStore."""
        if self._history_store is None:
            return QualityResponse(
                question=f"get_history({fqn})",
                mode="error",
                table=fqn,
                error=(
                    "No history_store configured. "
                    "Pass a SqliteHistoryStore or DeltaHistoryStore to QualityAgent."
                ),
            )
        try:
            reports = self._history_store.get_last_n(fqn, n)
        except Exception as exc:
            return QualityResponse(
                question=f"get_history({fqn})",
                mode="error",
                table=fqn,
                error=str(exc),
            )
        return QualityResponse(
            question=f"get_history({fqn})",
            mode="history",
            table=fqn,
            history_reports=reports,
        )

    def report(self, fqn: str, format: str = "text") -> str:
        """Return the latest MonitorReport for a table as a formatted string."""
        if self._history_store is None:
            return ""
        try:
            latest = self._history_store.get_latest(fqn)
        except Exception:
            return ""
        if latest is None:
            return ""
        if format == "json":
            return self._reporter.to_json(latest)
        return self._reporter.to_text(latest)

    # ------------------------------------------------------------------
    # Conversational API
    # ------------------------------------------------------------------

    def ask(self, question: str) -> QualityResponse:
        """Parse a natural language question and dispatch to the right method."""
        resp = self._process(question)
        self._log(resp)
        return resp

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _process(self, question: str) -> QualityResponse:
        norm = _normalize(question)
        mode = self._detect_mode(norm)

        if mode is None:
            if self._llm:
                return self._step_llm_parse(question)
            return QualityResponse(
                question=question,
                mode="error",
                error=(
                    "Could not determine intent from the question. "
                    "Use check(), get_history(), or report() directly, "
                    "or provide an llm_provider for natural language support."
                ),
            )

        fqn = self._extract_fqn(question)
        if not fqn and mode != "report":
            return QualityResponse(
                question=question,
                mode="error",
                error="Could not identify a table name in your question.",
            )

        if mode == "check":
            resp = self.check(fqn, schema_dict=self._default_schema_dict)
        elif mode == "history":
            resp = self.get_history(fqn)
        else:
            text_out = self.report(fqn) if fqn else ""
            return QualityResponse(
                question=question,
                mode="report",
                table=fqn,
                text_output=text_out,
                error=None if text_out else (
                    "No report available. Run check() first or configure a history_store."
                    if fqn else "Could not identify a table name in your question."
                ),
            )

        resp.question = question
        return resp

    def _detect_mode(self, norm: str) -> str | None:
        for kw in _CHECK_KEYWORDS:
            if kw in norm:
                return "check"
        for kw in _HISTORY_KEYWORDS:
            if kw in norm:
                return "history"
        for kw in _REPORT_KEYWORDS:
            if kw in norm:
                return "report"
        return None

    def _extract_fqn(self, question: str) -> str:
        """Extract a dotted table FQN (2-3 parts) or bare table name from a question."""
        # Dotted: schema.table or catalog.schema.table
        dotted = re.findall(r'\b([\w]+(?:\.[\w]+){1,2})\b', question)
        if dotted:
            return dotted[0]
        # Bare snake_case word
        words = re.findall(r'\b([a-z_][a-z0-9_]*)\b', question.lower())
        candidates = [w for w in words if w not in _STOPWORDS and len(w) > 3]
        return candidates[-1] if candidates else ""

    def _step_llm_parse(self, question: str) -> QualityResponse:
        """Use LLM to parse intent + table when keyword routing fails."""
        try:
            raw = self._llm.complete(
                system_prompt=_STEP_LLM_SYSTEM,
                user_message=question,
                temperature=0.0,
            )
            parsed = json.loads(raw)
            mode = parsed.get("mode", "")
            fqn = parsed.get("table", "")
        except Exception as exc:
            return QualityResponse(
                question=question, mode="error",
                error=f"LLM parse failed: {exc}",
            )

        if mode == "check":
            resp = self.check(fqn)
        elif mode == "history":
            resp = self.get_history(fqn)
        elif mode == "report":
            text_out = self.report(fqn)
            return QualityResponse(
                question=question, mode="report", table=fqn, text_output=text_out
            )
        else:
            return QualityResponse(
                question=question, mode="error",
                error=f"Unknown mode returned by LLM: '{mode}'.",
            )
        resp.question = question
        return resp

    def _narrative(self, report: Any) -> str:
        """Generate a prose summary of a MonitorReport via LLM."""
        summary = report.summary()
        prompt = (
            f"Summarize the data quality check for table '{summary.get('table', '')}' "
            f"in 1-3 sentences: {json.dumps(summary)}"
        )
        try:
            return self._llm.complete(
                system_prompt="You are a data quality expert. Be concise and precise.",
                user_message=prompt,
                temperature=0.2,
            )
        except Exception:
            return ""

    def _log(self, resp: QualityResponse) -> None:
        if not self._session_history_enabled or self._session is None:
            return
        summary_str = ""
        if resp.report is not None:
            s = resp.report.summary()
            summary_str = f"{s.get('status', '')} — {s.get('passed', 0)}/{s.get('total_checks', 0)} passed"
        entry = HistoryEntry(
            timestamp=datetime.now().isoformat(),
            question=resp.question,
            mode=resp.mode,
            explanation=summary_str,
            query_params={"table": resp.table},
            text_summary=resp.narrative or None,
            error=resp.error,
        )
        self._session.add(entry)

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def session(self) -> SessionHistory | None:
        return self._session

    def reset_history(self) -> None:
        if self._session_history_enabled:
            self._session = SessionHistory(self._session_title)
