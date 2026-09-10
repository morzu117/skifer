"""
hub_response_to_text — converts any HubResponse to a plain text string.

Shared by the CLI renderer and the MLflow serving layer so serialization
logic is not duplicated.
"""

from __future__ import annotations

from typing import Any


def semantic_evidence_to_dict(evidence: Any) -> dict[str, Any] | None:
    """Return the only evidence representation admitted by serving.

    Evidence crosses this boundary exclusively through ``SemanticEvidence``'s
    field-by-field allowlist.  In particular, never serialize an arbitrary
    dataclass or mapping supplied by an integration.
    """
    from skifer.semantic.evidence import SemanticEvidence

    if evidence is None:
        return None
    if not isinstance(evidence, SemanticEvidence):
        raise TypeError(
            "Serving evidence must be a SemanticEvidence instance; "
            f"got {type(evidence).__name__}."
        )
    return evidence.to_dict()


def hub_response_to_text(response: Any) -> str:
    """
    Convert any HubResponse dataclass to a plain text string.

    Handles AgentResponse, LineageResponse, QualityResponse,
    DictionaryResponse, and BuilderResponse. Falls back to str() for
    any unrecognised type.
    """
    from skifer.agentic.models import (
        AgentResponse,
        BuilderResponse,
        DictionaryResponse,
        LineageResponse,
        QualityResponse,
    )

    if isinstance(response, AgentResponse):
        return _serialize_agent_response(response)

    if isinstance(response, LineageResponse):
        if response.error:
            return f"Error: {response.error}"
        return response.diagram or response.narrative or f"Lineage ({response.mode}) — column: {response.column}"

    if isinstance(response, QualityResponse):
        if response.error:
            return f"Error: {response.error}"
        return response.text_output or response.narrative or "Quality checks executed."

    if isinstance(response, DictionaryResponse):
        if response.error:
            return f"Error: {response.error}"
        return response.text_output or response.narrative or f"Field: {response.column}"

    if isinstance(response, BuilderResponse):
        if not response.success:
            return f"Error: {response.error}"
        parts = [response.yaml_content]
        if response.output_path:
            parts.append(f"Saved to: {response.output_path}")
        return "\n".join(parts)

    return str(response)


def _serialize_agent_response(response: Any) -> str:
    """Serialize an AgentResponse to plain text."""
    from skifer.agentic.models import ResponseFormat

    if response.mode == "error":
        if getattr(response, "access_denied", False):
            decision = response.policy_decision or "DENY"
            reasons = ", ".join(response.policy_reasons or ()) or "unspecified"
            action = response.recommended_action or "Contact the data owner."
            return _with_provenance(
                f"Access denied ({decision}): {reasons}. {action}", response
            )
        return _with_provenance(f"Error: {response.error}", response)

    if response.mode in ("ambiguous", "needs_clarification"):
        return _with_provenance(
            response.clarification_question or response.explanation or "Clarification needed.",
            response,
        )

    result = getattr(response, "result", None)
    if result is None:
        return _with_provenance(response.explanation or "Response received.", response)

    fmt = getattr(result, "format", None)
    title = getattr(result, "title", "")

    if fmt == ResponseFormat.KPI:
        label = result.kpi_label or title or response.explanation or "Value"
        return _with_provenance(f"{label}: {result.kpi_value}", response)

    if fmt == ResponseFormat.TABLE:
        data = getattr(result, "data", None)
        if data is not None:
            try:
                rows = data.limit(20).collect()
                cols = data.columns
                lines = [title] if title else []
                lines.append(" | ".join(cols))
                lines.append("-" * (len(" | ".join(cols))))
                for row in rows:
                    lines.append(" | ".join(str(v) for v in row))
                return _with_provenance("\n".join(lines), response)
            except Exception:
                pass
        return _with_provenance(title or "No data.", response)

    text = getattr(result, "text_summary", None) or response.explanation
    return _with_provenance(text or "Response received.", response)


def _with_provenance(text: str, response: Any) -> str:
    """Append exactly one human-readable, redacted evidence line when present."""
    evidence = getattr(response, "evidence", None)
    if evidence is None:
        evidence = getattr(getattr(response, "result", None), "evidence", None)
    payload = semantic_evidence_to_dict(evidence)
    if payload is None:
        return text
    return f"{text}\n{_provenance_line(payload)}"


def _provenance_line(evidence: dict[str, Any]) -> str:
    """Summarize every source, the weakest certification, and freshness.

    Reporting only the first source reads as a complete claim about an answer
    that also depends on the others: a query joining a certified table to an
    uncertified one would announce "certifié" and reassure the reader about
    precisely the source that is not. The weakest link wins on every axis.
    """
    parts = []
    if evidence["execution_status"] == "failed":
        parts.append("exécution incomplète (échec)")

    sources = evidence["sources"]
    if not sources:
        parts += ["source inconnue", "certification inconnue", "fraîcheur inconnue"]
        return "Provenance : " + " · ".join(parts)

    parts.append(_sources_text(sources))
    parts.append(_certification_text(sources))
    parts.append(_freshness_text(_oldest_age(sources)))

    decision = evidence["policy"]["decision"]
    if decision and decision != "ALLOW":
        parts.append(f"décision {decision.lower()}")
    return "Provenance : " + " · ".join(parts)


def _sources_text(sources: list[dict[str, Any]]) -> str:
    names = [source["dataset"] for source in sources]
    shown = ", ".join(names[:3])
    if len(names) > 3:
        shown += f" (+{len(names) - 3})"
    return f"source {shown}" if len(names) == 1 else f"{len(names)} sources : {shown}"


def _certification_text(sources: list[dict[str, Any]]) -> str:
    uncertified = [
        source for source in sources if source["certification_status"] != "CERTIFIED"
    ]
    if uncertified:
        detail = ", ".join(
            f"{source['dataset']} {(source['certification_status'] or 'inconnue').lower()}"
            for source in uncertified[:3]
        )
        return f"certification incomplète : {detail}"
    dates = sorted(
        source["certified_at"] for source in sources if source["certified_at"]
    )
    if not dates:
        return "certification inconnue"
    # Oldest certification: the answer is only as fresh as its stalest guarantee.
    return f"certifié le {dates[0][:10]}"


def _oldest_age(sources: list[dict[str, Any]]) -> float | None:
    ages = [
        age
        for source in sources
        for age in (
            source["data_age_seconds"]
            if source["data_age_seconds"] is not None
            else source["load_age_seconds"],
        )
        if age is not None
    ]
    return max(ages) if ages else None


def _freshness_text(age_seconds: float | None) -> str:
    if age_seconds is None:
        return "fraîcheur inconnue"
    if age_seconds < 60:
        return f"fraîcheur {int(age_seconds)} s"
    if age_seconds < 3600:
        return f"fraîcheur {int(age_seconds // 60)} min"
    return f"fraîcheur {int(age_seconds // 3600)} h"
