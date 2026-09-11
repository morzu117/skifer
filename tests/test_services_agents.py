"""Spark-free tests for the agent application service."""

import json

import pytest

from skifer.agentic.models import AgentResponse, FormattedResult, ResponseFormat
from skifer.observability.tracing import TraceContext
from skifer.services import (
    AgentService,
    RequestContext,
    SCOPE_QUERY_EXECUTE,
    ScopeDenied,
)


def _context(*scopes: str) -> RequestContext:
    return RequestContext(
        subject="local-user",
        scopes=frozenset(scopes),
        consumer_class="local",
        trace_context=TraceContext(),
    )


class _DataFrameSentinel:
    pass


class _Hub:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def ask(self, question, **kwargs):
        self.calls.append((question, kwargs))
        return self.response


def test_agent_ask_returns_text_and_dict_without_dataframe():
    frame = _DataFrameSentinel()
    response = AgentResponse(
        question="Summarize orders",
        explanation="Order summary",
        result=FormattedResult(
            format=ResponseFormat.TEXT_ANALYSIS,
            data=frame,
            title="Orders",
            text_summary="Orders are stable.",
        ),
    )
    hub = _Hub(response)

    result = AgentService(hub).ask(
        _context(SCOPE_QUERY_EXECUTE), "Summarize orders"
    )

    assert result["text"] == "Orders are stable."
    assert result["response"]["result"] == {
        "format": "text_analysis",
        "title": "Orders",
        "kpi_value": None,
        "kpi_label": None,
        "text_summary": "Orders are stable.",
    }
    assert frame not in result["response"].values()
    json.dumps(result)


def test_agent_ask_requires_query_execute():
    service = AgentService(_Hub(AgentResponse(question="q")))

    with pytest.raises(ScopeDenied):
        service.ask(_context(), "q")
