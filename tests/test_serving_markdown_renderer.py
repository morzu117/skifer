"""
Tests for serving._markdown_renderer.hub_response_to_markdown().
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from skifer.agentic.models import (
    AgentResponse,
    FormattedResult,
    LineageResponse,
    ResponseFormat,
)
from skifer.semantic.access_policy import CertificationDecision, PolicyEvaluation
from skifer.semantic.evidence import SemanticEvidence, SourceEvidence
from skifer.serving._markdown_renderer import (
    MAX_TABLE_ROWS,
    _format_value,
    hub_response_to_markdown,
)

NOW = datetime(2026, 9, 4, 12, tzinfo=timezone.utc)


def _source(dataset, status="CERTIFIED", *, certified_at=NOW, load_age=None):
    return SourceEvidence(
        dataset=dataset,
        contract_id=None,
        contract_version=None,
        definition_hash=None,
        certification_status=status,
        certified_at=certified_at,
        load_age_seconds=load_age,
        data_age_seconds=None,
        certification_run_id=None,
    )


def _evidence(*sources, decision=CertificationDecision.ALLOW, execution_status="succeeded"):
    return SemanticEvidence(
        evidence_id="evidence-1",
        trace_id=None,
        model_keys=("sales.orders",),
        metrics=(),
        dimensions=(),
        normalized_filters=(
            {"column": "email", "operator": "eq", "value": "secret@example.com"},
        ),
        sources=sources,
        policy=PolicyEvaluation(decision, (), NOW),
        sql_hash="sha256:v1:hash",
        sql_text="SELECT * FROM orders WHERE email = 'secret@example.com'",
        statement_id=None,
        compiled_at=NOW,
        executed_at=NOW,
        execution_status=execution_status,
    )


def _dataframe(columns, rows):
    df = MagicMock()
    df.columns = columns
    df.limit.side_effect = lambda n: MagicMock(collect=MagicMock(return_value=rows[:n]))
    return df


# ---------------------------------------------------------------------------
# Values
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "value, expected",
    [
        (16200985.329999981, "16 200 985,33"),
        (Decimal("1234.5"), "1 234,50"),
        (1500, "1 500"),
        (42.0, "42"),
        (0.004321, "0,004321"),
        (None, "—"),
        (True, "oui"),
        (date(2026, 1, 31), "2026-01-31"),
        ("EMEA", "EMEA"),
    ],
)
def test_values_are_displayed_the_french_way(value, expected):
    assert _format_value(value) == expected


# ---------------------------------------------------------------------------
# Bodies
# ---------------------------------------------------------------------------

def test_kpi_shows_the_business_label_and_a_formatted_value():
    result = FormattedResult(
        format=ResponseFormat.KPI,
        data=None,
        title="total_revenue",
        kpi_value=16200985.329999981,
        kpi_label="total_revenue",
    )
    r = AgentResponse(question="q", mode="query", result=result)

    text = hub_response_to_markdown(r, {"total_revenue": "CA total TTC (€)"})

    assert text == "**CA total TTC (€)**\n\n### 16 200 985,33"


def test_kpi_without_label_humanizes_the_metric_name_and_keeps_the_period():
    result = FormattedResult(
        format=ResponseFormat.KPI,
        data=None,
        title="total_revenue — 2024-01-01–2024-12-31",
        kpi_value=10,
        kpi_label="total_revenue",
    )
    r = AgentResponse(question="q", mode="query", result=result)

    text = hub_response_to_markdown(r)

    assert text.splitlines()[:2] == ["**Total revenue**", "_total_revenue — 2024-01-01–2024-12-31_"]


def test_a_sentence_long_description_is_not_used_as_a_label():
    result = FormattedResult(
        format=ResponseFormat.KPI, data=None, kpi_value=1, kpi_label="nb_orders",
    )
    r = AgentResponse(question="q", mode="query", result=result)
    long_description = "Nombre de commandes distinctes, hors annulations et hors retours fournisseurs"

    assert hub_response_to_markdown(r, {"nb_orders": long_description}).startswith("**Nb orders**")


def test_table_is_a_markdown_table_with_right_aligned_numbers():
    df = _dataframe(["region", "total_revenue"], [("EMEA", 1000.5), ("APAC", None)])
    result = FormattedResult(format=ResponseFormat.TABLE, data=df, title="CA par région")
    r = AgentResponse(question="q", mode="query", result=result)

    text = hub_response_to_markdown(r, {"total_revenue": "CA (€)"})

    assert text == (
        "**CA par région**\n\n"
        "| Region | CA (€) |\n"
        "| --- | ---: |\n"
        "| EMEA | 1 000,50 |\n"
        "| APAC | — |"
    )


def test_table_announces_when_rows_were_cut():
    rows = [(f"r{i}", i) for i in range(MAX_TABLE_ROWS + 5)]
    result = FormattedResult(format=ResponseFormat.TABLE, data=_dataframe(["k", "v"], rows))
    r = AgentResponse(question="q", mode="query", result=result)

    text = hub_response_to_markdown(r)

    assert text.count("\n| r") == MAX_TABLE_ROWS
    assert text.endswith(f"_{MAX_TABLE_ROWS} premières lignes affichées._")


def test_table_cells_cannot_break_the_table():
    df = _dataframe(["label"], [("a|b\nc",)])
    result = FormattedResult(format=ResponseFormat.TABLE, data=df)
    r = AgentResponse(question="q", mode="query", result=result)

    assert "| a\\|b c |" in hub_response_to_markdown(r)


def test_chart_falls_back_to_the_table():
    df = _dataframe(["month", "revenue"], [("2026-01", 3)])
    result = FormattedResult(format=ResponseFormat.CHART, data=df, title="CA par mois")
    r = AgentResponse(question="q", mode="query", result=result)

    assert "| Month | Revenue |" in hub_response_to_markdown(r)


def test_text_analysis_keeps_the_summary_and_appends_the_detail():
    df = _dataframe(["region", "revenue"], [("EMEA", 3)])
    result = FormattedResult(
        format=ResponseFormat.TEXT_ANALYSIS, data=df, text_summary="EMEA mène.",
    )
    r = AgentResponse(question="q", mode="query", result=result)

    text = hub_response_to_markdown(r)

    assert text.startswith("EMEA mène.\n\n**Détail**\n\n| Region | Revenue |")


def test_access_denial_is_explicit_and_actionable():
    r = AgentResponse(
        question="q",
        mode="error",
        error="denied",
        access_denied=True,
        policy_decision="DENY",
        policy_reasons=("MISSING",),
        recommended_action="Publier gold.orders via une publication certifiée.",
    )

    assert hub_response_to_markdown(r) == (
        "⛔ **Accès refusé** (DENY) — raison : MISSING.\n\n"
        "Publier gold.orders via une publication certifiée."
    )


def test_error_and_clarification_are_marked():
    error = AgentResponse(question="q", mode="error", error="Métrique inconnue : marge")
    clarify = AgentResponse(question="q", mode="ambiguous", clarification_question="Quelle année ?")

    assert hub_response_to_markdown(error) == "⚠️ **Impossible de répondre** : Métrique inconnue : marge"
    assert hub_response_to_markdown(clarify) == "🤔 Quelle année ?"


def test_non_semantic_responses_keep_their_plain_text():
    assert hub_response_to_markdown(LineageResponse(question="q", diagram="graph LR; A-->B")) == "graph LR; A-->B"


# ---------------------------------------------------------------------------
# Provenance block
# ---------------------------------------------------------------------------

def test_provenance_is_a_separate_block_and_never_shows_filter_values():
    r = AgentResponse(
        question="q",
        mode="query",
        explanation="Voici.",
        evidence=_evidence(_source("gold.orders", load_age=7200)),
    )

    text = hub_response_to_markdown(r)

    assert text == (
        "Voici.\n\n---\n**Provenance**\n"
        "- Source : `gold.orders`\n"
        "- Certification : ✅ certifiée le 2026-09-04\n"
        "- Fraîcheur : il y a 2 h"
    )
    assert "secret@example.com" not in text
    assert "SELECT" not in text


def test_provenance_explains_a_disabled_gate():
    r = AgentResponse(
        question="q",
        mode="query",
        explanation="Voici.",
        evidence=_evidence(_source("gold.kpi_orders", "NOT_EVALUATED", certified_at=None)),
    )

    text = hub_response_to_markdown(r)

    assert "- Certification : ⚠️ non évaluée (gate de certification désactivé)" in text
    assert "- Fraîcheur : non mesurée" in text


def test_provenance_never_claims_certified_when_a_joined_source_is_not():
    r = AgentResponse(
        question="q",
        mode="query",
        explanation="Voici.",
        evidence=_evidence(
            _source("gold.orders"),
            _source("gold.customers", "UNCERTIFIED", certified_at=None),
            decision=CertificationDecision.WARN,
        ),
    )

    text = hub_response_to_markdown(r)

    assert "- Sources : `gold.orders`, `gold.customers`" in text
    assert "- Certification : incomplète — `gold.customers` ❌ non certifiée" in text
    assert "- Décision du gate : warn" in text
    assert "✅" not in text


def test_failed_execution_is_never_presented_as_complete():
    r = AgentResponse(
        question="q",
        mode="error",
        error="backend failed",
        evidence=_evidence(_source("gold.orders"), execution_status="failed"),
    )

    assert "- ❌ Exécution incomplète (échec)" in hub_response_to_markdown(r)


# ---------------------------------------------------------------------------
# Display order — the semantic query has no ORDER BY
# ---------------------------------------------------------------------------

def _table_rows(text):
    return [line for line in text.splitlines()[2:] if line.startswith("| ")]


def test_a_time_axis_is_shown_in_chronological_order(spark):
    df = spark.createDataFrame(
        [("2025-03", 3.0), ("2025-01", 1.0), ("2025-02", 2.0)],
        "order_month string, revenue double",
    )
    result = FormattedResult(format=ResponseFormat.TABLE, data=df)
    r = AgentResponse(question="q", mode="query", result=result)

    rows = _table_rows(hub_response_to_markdown(r))

    assert [row.split(" | ")[0] for row in rows] == ["| 2025-01", "| 2025-02", "| 2025-03"]


def test_a_breakdown_is_ranked_by_its_first_measure(spark):
    df = spark.createDataFrame(
        [("APAC", 2.0), ("EMEA", 9.0), ("AMER", 5.0)],
        "region string, revenue double",
    )
    result = FormattedResult(format=ResponseFormat.TABLE, data=df)
    r = AgentResponse(question="q", mode="query", result=result)

    rows = _table_rows(hub_response_to_markdown(r))

    assert [row.split(" | ")[0] for row in rows] == ["| EMEA", "| AMER", "| APAC"]


def test_the_rows_kept_under_the_cap_are_the_first_in_display_order(spark):
    months = [(f"2025-{month:02d}", float(month)) for month in range(12, 0, -1)]
    df = spark.createDataFrame(months * 3, "order_month string, revenue double")
    result = FormattedResult(format=ResponseFormat.TABLE, data=df)
    r = AgentResponse(question="q", mode="query", result=result)

    rows = _table_rows(hub_response_to_markdown(r))

    assert len(rows) == MAX_TABLE_ROWS
    assert rows[0].startswith("| 2025-01 |")
