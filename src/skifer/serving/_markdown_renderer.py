"""
hub_response_to_markdown — renders a HubResponse as Markdown for chat surfaces.

The Databricks Playground and the Agent Bricks supervisor render the Responses
``output`` text as Markdown: the plain-text form of ``hub_response_to_text``
loses its line breaks there and shows raw floats (``16200985.329999981``).
This renderer is used by the serving layer only; the CLI and the local API keep
the plain-text form.

The disclosure rules are those of the plain-text form: only allowlisted
evidence fields, never a filter value, and the weakest source wins on every
axis of the provenance block.
"""

from __future__ import annotations

import datetime as _dt
import math
from decimal import Decimal
from typing import Any, Mapping

from skifer.serving._response_serializer import (
    _oldest_age,
    hub_response_to_text,
    semantic_evidence_to_dict,
)

MAX_TABLE_ROWS = 30
# A business label longer than this is a sentence, not a column header.
MAX_LABEL_LENGTH = 48

_NUMERIC_TYPES = frozenset(
    {"tinyint", "smallint", "int", "bigint", "float", "double", "decimal", "long", "short"}
)
_TEMPORAL_TYPES = frozenset({"date", "timestamp", "timestamp_ntz"})
_TEMPORAL_HINTS = (
    "date", "day", "jour", "week", "semaine", "month", "mois",
    "quarter", "trimestre", "year", "annee", "année", "period",
)

_NBSP = "\u00a0"

_STATUS_LABELS = {
    "CERTIFIED": "✅ certifiée",
    "UNCERTIFIED": "❌ non certifiée",
    "MISSING": "❌ absente",
    "EXPIRED": "⏰ expirée",
    "NOT_EVALUATED": "⚠️ non évaluée (gate de certification désactivé)",
    "OVERRIDDEN": "⚠️ contournée (override)",
}


def hub_response_to_markdown(response: Any, labels: Mapping[str, str] | None = None) -> str:
    """Render a HubResponse as Markdown; ``labels`` maps technical names to business labels.

    Only semantic answers (``AgentResponse``) get a dedicated rendering; the other
    responses keep their plain-text form, which is already readable as Markdown.
    """
    from skifer.agentic.models import AgentResponse

    if not isinstance(response, AgentResponse):
        return hub_response_to_text(response)
    body = _agent_body(response, dict(labels or {}))
    evidence = getattr(response, "evidence", None)
    if evidence is None:
        evidence = getattr(getattr(response, "result", None), "evidence", None)
    payload = semantic_evidence_to_dict(evidence)
    if payload is None:
        return body
    # The blank line matters: "---" right under a paragraph is a setext heading.
    return f"{body}\n\n---\n{_provenance_block(payload)}"


def _agent_body(response: Any, labels: dict[str, str]) -> str:
    from skifer.agentic.models import ResponseFormat

    if response.mode == "error":
        if getattr(response, "access_denied", False):
            decision = response.policy_decision or "DENY"
            reasons = ", ".join(response.policy_reasons or ()) or "non précisée"
            action = response.recommended_action or "Contacter le propriétaire de la donnée."
            return f"⛔ **Accès refusé** ({decision}) — raison : {reasons}.\n\n{action}"
        return f"⚠️ **Impossible de répondre** : {response.error}"

    if response.mode in ("ambiguous", "needs_clarification"):
        question = response.clarification_question or response.explanation
        return f"🤔 {question or 'Pouvez-vous préciser la question ?'}"

    result = getattr(response, "result", None)
    if result is None:
        return response.explanation or "Réponse reçue."

    fmt = getattr(result, "format", None)
    title = getattr(result, "title", "") or ""

    if fmt == ResponseFormat.KPI:
        name = result.kpi_label or ""
        lines = [f"**{_escape(_label(name, labels) or title or 'Valeur')}**"]
        if title and title != name:
            lines.append(f"_{_escape(title)}_")
        lines += ["", f"### {_format_value(result.kpi_value)}"]
        return "\n".join(lines)

    table = _markdown_table(getattr(result, "data", None), labels)
    if fmt in (ResponseFormat.TABLE, ResponseFormat.CHART):
        heading = f"**{_escape(title)}**" if title else ""
        if table is None:
            return heading or "Aucune donnée."
        return f"{heading}\n\n{table}" if heading else table

    summary = getattr(result, "text_summary", None) or response.explanation
    parts = [summary or "Réponse reçue."]
    if table is not None:
        parts.append(f"**Détail**\n\n{table}")
    return "\n\n".join(parts)


def _label(name: str, labels: Mapping[str, str]) -> str:
    label = labels.get(name)
    if label and len(label) <= MAX_LABEL_LENGTH:
        return label
    return name.replace("_", " ").strip().capitalize()


def _markdown_table(data: Any, labels: Mapping[str, str]) -> str | None:
    if data is None:
        return None
    try:
        # One row more than shown, only to know whether the table was cut.
        rows = _display_order(data).limit(MAX_TABLE_ROWS + 1).collect()
        columns = list(data.columns)
    except Exception:
        return None
    if not rows:
        return "_Aucune ligne._"

    values = [list(row) for row in rows[:MAX_TABLE_ROWS]]
    numeric = [
        any(_is_number(row[index]) for row in values)
        and all(row[index] is None or _is_number(row[index]) for row in values)
        for index in range(len(columns))
    ]
    lines = [
        "| " + " | ".join(_escape(_label(column, labels)) for column in columns) + " |",
        "| " + " | ".join("---:" if flag else "---" for flag in numeric) + " |",
    ]
    lines += [
        "| " + " | ".join(_escape(_format_value(value)) for value in row) + " |"
        for row in values
    ]
    table = "\n".join(lines)
    if len(rows) > MAX_TABLE_ROWS:
        table += f"\n\n_{MAX_TABLE_ROWS} premières lignes affichées._"
    return table


def _display_order(data: Any) -> Any:
    """Order a breakdown for reading: a time axis ascending, else the first measure descending.

    The semantic query has no ORDER BY, so rows come back in whatever order Spark
    produced them — and the rows kept under the cap would be arbitrary too.
    """
    try:
        dtypes = [(str(name), str(kind).split("(")[0].lower()) for name, kind in data.dtypes]
    except Exception:
        return data
    numeric = [name for name, kind in dtypes if kind in _NUMERIC_TYPES]
    temporal = [
        name
        for name, kind in dtypes
        if name not in numeric
        and (kind in _TEMPORAL_TYPES or any(hint in name.lower() for hint in _TEMPORAL_HINTS))
    ]
    if temporal:
        return data.orderBy(*temporal)
    if numeric and len(numeric) < len(dtypes):
        return data.orderBy(numeric[0], ascending=False)
    return data


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float, Decimal)) and not isinstance(value, bool)


def _format_value(value: Any) -> str:
    """French display: narrow grouping, decimal comma, two decimals at most."""
    if value is None:
        return "—"
    if isinstance(value, bool):
        return "oui" if value else "non"
    if isinstance(value, int):
        return _french(f"{value:,}")
    if isinstance(value, (float, Decimal)):
        number = float(value)
        if math.isnan(number) or math.isinf(number):
            return str(value)
        if number == int(number):
            return _french(f"{int(number):,}")
        if abs(number) >= 1:
            return _french(f"{number:,.2f}")
        # A ratio or a share: two decimals would round 0.004 to zero.
        return _french(f"{number:.4g}")
    if isinstance(value, _dt.datetime):
        return value.isoformat(sep=" ", timespec="minutes")
    if isinstance(value, _dt.date):
        return value.isoformat()
    return str(value)


def _french(text: str) -> str:
    return text.replace(",", _NBSP).replace(".", ",")


def _escape(text: str) -> str:
    return str(text).replace("|", "\\|").replace("\n", " ")


def _provenance_block(evidence: dict[str, Any]) -> str:
    lines = ["**Provenance**"]
    if evidence["execution_status"] == "failed":
        lines.append("- ❌ Exécution incomplète (échec)")

    sources = evidence["sources"]
    if not sources:
        lines += ["- Source : inconnue", "- Certification : inconnue", "- Fraîcheur : inconnue"]
        return "\n".join(lines)

    names = [source["dataset"] for source in sources]
    shown = ", ".join(f"`{name}`" for name in names[:3])
    if len(names) > 3:
        shown += f" (+{len(names) - 3})"
    lines.append(f"- {'Source' if len(names) == 1 else 'Sources'} : {shown}")
    lines.append(f"- Certification : {_certification(sources)}")
    lines.append(f"- Fraîcheur : {_freshness(_oldest_age(sources))}")

    decision = evidence["policy"]["decision"]
    if decision and decision != "ALLOW":
        lines.append(f"- Décision du gate : {decision.lower()}")
    return "\n".join(lines)


def _status_label(status: str | None) -> str:
    if not status:
        return "❓ inconnue"
    return _STATUS_LABELS.get(status, f"❓ {status.lower()}")


def _certification(sources: list[dict[str, Any]]) -> str:
    weak = [source for source in sources if source["certification_status"] != "CERTIFIED"]
    if weak:
        if len(sources) == 1:
            return _status_label(weak[0]["certification_status"])
        detail = ", ".join(
            f"`{source['dataset']}` {_status_label(source['certification_status'])}"
            for source in weak[:3]
        )
        return f"incomplète — {detail}"
    dates = sorted(source["certified_at"] for source in sources if source["certified_at"])
    if not dates:
        return "✅ certifiée (date inconnue)"
    # Oldest certification: the answer is only as fresh as its stalest guarantee.
    return f"✅ certifiée le {dates[0][:10]}"


def _freshness(age_seconds: float | None) -> str:
    if age_seconds is None:
        return "non mesurée"
    if age_seconds < 60:
        return f"il y a {int(age_seconds)} s"
    if age_seconds < 3600:
        return f"il y a {int(age_seconds // 60)} min"
    if age_seconds < 48 * 3600:
        return f"il y a {int(age_seconds // 3600)} h"
    return f"il y a {int(age_seconds // 86400)} j"
