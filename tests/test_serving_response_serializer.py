"""
Tests for serving._response_serializer.hub_response_to_text().
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from skifer.serving._response_serializer import hub_response_to_text
from skifer.semantic.access_policy import CertificationDecision, PolicyEvaluation
from skifer.semantic.evidence import SemanticEvidence, SourceEvidence
from skifer.agentic.models import (
    AgentResponse,
    BuilderResponse,
    DictionaryResponse,
    FormattedResult,
    LineageResponse,
    QualityResponse,
    ResponseFormat,
)


def _evidence(*, execution_status="succeeded"):
    now = datetime(2026, 9, 4, 12, tzinfo=timezone.utc)
    return SemanticEvidence(
        evidence_id="evidence-1",
        trace_id=None,
        model_keys=("sales.orders",),
        metrics=(),
        dimensions=(),
        normalized_filters=(
            {"column": "customer_email", "operator": "eq", "value": "secret@example.com"},
        ),
        sources=(
            SourceEvidence(
                dataset="gold.fact_orders",
                contract_id=None,
                contract_version=None,
                definition_hash=None,
                certification_status="CERTIFIED",
                certified_at=now,
                load_age_seconds=7200,
                data_age_seconds=None,
                certification_run_id=None,
            ),
        ),
        policy=PolicyEvaluation(CertificationDecision.ALLOW, (), now),
        sql_hash="sha256:v1:hash",
        sql_text="SELECT * FROM orders WHERE customer_email = 'secret@example.com'",
        statement_id=None,
        compiled_at=now,
        executed_at=now,
        execution_status=execution_status,
    )


# ---------------------------------------------------------------------------
# AgentResponse
# ---------------------------------------------------------------------------

def test_agent_response_error():
    r = AgentResponse(question="q", mode="error", error="Something went wrong")
    assert hub_response_to_text(r) == "Error: Something went wrong"


def test_agent_response_ambiguous():
    r = AgentResponse(
        question="q",
        mode="ambiguous",
        clarification_question="Did you mean table A or B?",
    )
    assert hub_response_to_text(r) == "Did you mean table A or B?"


def test_agent_response_needs_clarification_falls_back_to_explanation():
    r = AgentResponse(question="q", mode="needs_clarification", explanation="Unknown field.")
    assert hub_response_to_text(r) == "Unknown field."


def test_agent_response_kpi():
    result = FormattedResult(format=ResponseFormat.KPI, data=None, kpi_value=42.0, kpi_label="Revenue")
    r = AgentResponse(question="q", mode="query", result=result)
    text = hub_response_to_text(r)
    assert "Revenue" in text
    assert "42.0" in text


def test_agent_response_evidence_adds_one_redacted_provenance_line():
    result = FormattedResult(format=ResponseFormat.KPI, data=None, kpi_value=42)
    response = AgentResponse(question="q", mode="query", result=result, evidence=_evidence())

    text = hub_response_to_text(response)

    assert text.splitlines()[-1] == (
        "Provenance : source gold.fact_orders · certifié le 2026-09-04 · fraîcheur 2 h"
    )
    assert len([line for line in text.splitlines() if line.startswith("Provenance :")]) == 1
    assert "secret@example.com" not in text
    assert "SELECT" not in text


def test_failed_evidence_is_never_presented_as_complete():
    response = AgentResponse(question="q", mode="error", error="backend failed", evidence=_evidence(execution_status="failed"))

    text = hub_response_to_text(response)

    assert "exécution incomplète (échec)" in text
    assert "secret@example.com" not in text


def test_agent_response_serializer_refuses_non_allowlisted_evidence_type():
    response = AgentResponse(question="q", explanation="ok", evidence={"not": "evidence"})

    with pytest.raises(TypeError, match="SemanticEvidence"):
        hub_response_to_text(response)


def test_agent_response_no_result_returns_explanation():
    r = AgentResponse(question="q", mode="query", explanation="Here is your answer.", result=None)
    assert hub_response_to_text(r) == "Here is your answer."


def test_agent_response_table_no_data():
    result = FormattedResult(format=ResponseFormat.TABLE, data=None, title="Orders by region")
    r = AgentResponse(question="q", mode="query", result=result)
    assert hub_response_to_text(r) == "Orders by region"


def test_agent_response_table_with_spark_df():
    mock_df = MagicMock()
    mock_df.columns = ["region", "amount"]
    mock_row = MagicMock()
    mock_row.__iter__ = MagicMock(return_value=iter(["EMEA", "1000"]))
    mock_df.limit.return_value.collect.return_value = [mock_row]

    result = FormattedResult(format=ResponseFormat.TABLE, data=mock_df, title="Result")
    r = AgentResponse(question="q", mode="query", result=result)
    text = hub_response_to_text(r)
    assert "region" in text
    assert "amount" in text


def test_agent_response_text_analysis():
    result = FormattedResult(
        format=ResponseFormat.TEXT_ANALYSIS,
        data=None,
        text_summary="Sales are up 12% YoY.",
    )
    r = AgentResponse(question="q", mode="query", result=result)
    assert hub_response_to_text(r) == "Sales are up 12% YoY."


# ---------------------------------------------------------------------------
# LineageResponse
# ---------------------------------------------------------------------------

def test_lineage_response_error():
    r = LineageResponse(question="q", error="Field not found")
    assert hub_response_to_text(r) == "Error: Field not found"


def test_lineage_response_diagram():
    r = LineageResponse(question="q", diagram="graph LR; A --> B")
    assert hub_response_to_text(r) == "graph LR; A --> B"


def test_lineage_response_narrative_fallback():
    r = LineageResponse(question="q", column="amount_eur", mode="trace", narrative="Derived from amount.")
    assert hub_response_to_text(r) == "Derived from amount."


def test_lineage_response_minimal_fallback():
    r = LineageResponse(question="q", column="amount_eur", mode="trace")
    text = hub_response_to_text(r)
    assert "amount_eur" in text


# ---------------------------------------------------------------------------
# QualityResponse
# ---------------------------------------------------------------------------

def test_quality_response_error():
    r = QualityResponse(question="q", error="Table not found")
    assert hub_response_to_text(r) == "Error: Table not found"


def test_quality_response_text_output():
    r = QualityResponse(question="q", text_output="All checks passed.")
    assert hub_response_to_text(r) == "All checks passed."


def test_quality_response_narrative_fallback():
    r = QualityResponse(question="q", narrative="No null values detected.")
    assert hub_response_to_text(r) == "No null values detected."


# ---------------------------------------------------------------------------
# DictionaryResponse
# ---------------------------------------------------------------------------

def test_dictionary_response_error():
    r = DictionaryResponse(question="q", error="Unknown field")
    assert hub_response_to_text(r) == "Error: Unknown field"


def test_dictionary_response_text_output():
    r = DictionaryResponse(question="q", text_output="amount_eur: Revenue in euros.")
    assert hub_response_to_text(r) == "amount_eur: Revenue in euros."


def test_dictionary_response_column_fallback():
    r = DictionaryResponse(question="q", column="amount_eur")
    assert "amount_eur" in hub_response_to_text(r)


# ---------------------------------------------------------------------------
# BuilderResponse
# ---------------------------------------------------------------------------

def test_builder_response_failure():
    r = BuilderResponse(success=False, error="LLM call failed")
    assert hub_response_to_text(r) == "Error: LLM call failed"


def test_builder_response_success():
    r = BuilderResponse(success=True, yaml_content="tables:\n  - name: orders")
    text = hub_response_to_text(r)
    assert "tables:" in text


def test_builder_response_with_output_path():
    r = BuilderResponse(
        success=True,
        yaml_content="tables:\n  - name: orders",
        output_path="schemas/orders.yaml",
    )
    text = hub_response_to_text(r)
    assert "schemas/orders.yaml" in text


# ---------------------------------------------------------------------------
# Fallback
# ---------------------------------------------------------------------------

def test_fallback_unknown_type():
    assert hub_response_to_text("just a string") == "just a string"
    assert hub_response_to_text(42) == "42"


# ---------------------------------------------------------------------------
# Provenance line must speak for every source (Plan 29, slice 4.4)
# ---------------------------------------------------------------------------


def _source(dataset, status, *, certified_at=None, load_age_seconds=None):
    from skifer.semantic.evidence import SourceEvidence

    return SourceEvidence(
        dataset=dataset,
        contract_id=None,
        contract_version="1.0.0",
        definition_hash="h",
        certification_status=status,
        certified_at=certified_at,
        load_age_seconds=load_age_seconds,
        data_age_seconds=None,
        certification_run_id=None,
    )


def _multi_source_evidence(sources, decision=None):
    from datetime import datetime, timezone

    from skifer.semantic.access_policy import (
        CertificationDecision,
        PolicyEvaluation,
    )
    from skifer.semantic.evidence import SemanticEvidence

    now = datetime(2026, 9, 4, 12, 0, tzinfo=timezone.utc)
    return SemanticEvidence(
        evidence_id="e",
        trace_id=None,
        model_keys=("orders", "customers"),
        metrics=(),
        dimensions=(),
        normalized_filters=(),
        sources=tuple(sources),
        policy=PolicyEvaluation(
            decision or CertificationDecision.ALLOW, (), now
        ),
        sql_hash="sha256:v1:x",
        sql_text=None,
        statement_id=None,
        compiled_at=now,
        executed_at=now,
        execution_status="succeeded",
    )


def test_provenance_never_claims_certified_when_a_joined_source_is_not():
    # Reporting only sources[0] announced "certifié" for an answer that also
    # depends on an uncertified table — reassuring the reader about exactly the
    # source that is not certified.
    from datetime import datetime, timezone

    from skifer.agentic.models import AgentResponse
    from skifer.semantic.access_policy import CertificationDecision

    certified_at = datetime(2026, 9, 4, 12, 0, tzinfo=timezone.utc)
    evidence = _multi_source_evidence(
        [
            _source("gold.orders", "CERTIFIED", certified_at=certified_at),
            _source("gold.customers", "MISSING"),
        ],
        decision=CertificationDecision.WARN,
    )

    text = hub_response_to_text(
        AgentResponse(question="q", explanation="Voici.", evidence=evidence)
    )

    assert "gold.customers" in text
    assert "certification incomplète" in text
    assert "certifié le" not in text
    assert "warn" in text
    assert len(text.splitlines()) == 2  # answer + exactly one provenance line


def test_provenance_reports_the_oldest_certification_and_freshness():
    # The answer is only as fresh, and only as certified, as its weakest source.
    from datetime import datetime, timezone

    from skifer.agentic.models import AgentResponse

    evidence = _multi_source_evidence(
        [
            _source(
                "gold.orders",
                "CERTIFIED",
                certified_at=datetime(2026, 9, 4, tzinfo=timezone.utc),
                load_age_seconds=60.0,
            ),
            _source(
                "gold.customers",
                "CERTIFIED",
                certified_at=datetime(2026, 8, 1, tzinfo=timezone.utc),
                load_age_seconds=7200.0,
            ),
        ]
    )

    text = hub_response_to_text(
        AgentResponse(question="q", explanation="Voici.", evidence=evidence)
    )

    assert "certifié le 2026-08-01" in text
    assert "fraîcheur 2 h" in text


def test_provenance_of_a_single_certified_source_stays_concise():
    from datetime import datetime, timezone

    from skifer.agentic.models import AgentResponse

    evidence = _multi_source_evidence(
        [
            _source(
                "gold.orders",
                "CERTIFIED",
                certified_at=datetime(2026, 9, 4, tzinfo=timezone.utc),
                load_age_seconds=30.0,
            )
        ]
    )

    text = hub_response_to_text(
        AgentResponse(question="q", explanation="Voici.", evidence=evidence)
    )

    assert text.splitlines()[1] == (
        "Provenance : source gold.orders · certifié le 2026-09-04 · fraîcheur 30 s"
    )
