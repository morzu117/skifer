"""Versioned semantic calendars and date-boundary security tests."""
from __future__ import annotations

import builtins
import os
from types import SimpleNamespace

import pytest
import yaml

from skifer.agentic.resolver import (
    QueryResolver,
    SemanticQuery,
    SemanticQueryError,
)
from skifer.semantic.calendar import parse_calendar, resolve_period
from skifer.semantic.planner import SemanticPlanner
from skifer.semantic.semantic import SemanticEngine


def test_parse_and_resolve_calendar_period():
    calendar = parse_calendar(
        {
            "key": "fiscal_fr",
            "version": "2024.1",
            "periods": [
                {"name": "FY2024_Q3", "start": "2024-10-01", "end": "2024-12-31"}
            ],
        }
    )

    period = resolve_period(calendar, "FY2024_Q3")

    assert period.start.isoformat() == "2024-10-01"
    assert period.end.isoformat() == "2024-12-31"


def test_semantic_engine_does_not_load_calendar_at_startup(tmp_path, monkeypatch):
    models_dir = tmp_path / "semantic_models"
    calendars_dir = models_dir / "calendars"
    calendars_dir.mkdir(parents=True)
    (models_dir / "semantic_catalog.yaml").write_text(
        yaml.safe_dump({"models": []}), encoding="utf-8"
    )
    calendar_path = calendars_dir / "fiscal_fr.yaml"
    calendar_path.write_text(
        yaml.safe_dump(
            {
                "key": "fiscal_fr",
                "version": "2024.1",
                "periods": [
                    {
                        "name": "FY2024_Q3",
                        "start": "2024-10-01",
                        "end": "2024-12-31",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    real_open = builtins.open
    opened_calendars: list[str] = []

    def tracking_open(path, *args, **kwargs):
        if os.fspath(path).endswith("fiscal_fr.yaml"):
            opened_calendars.append(os.fspath(path))
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", tracking_open)

    engine = SemanticEngine(SimpleNamespace(), models_dir=str(models_dir))

    assert engine._calendar_cache == {}
    assert opened_calendars == []
    assert engine._get_calendar("fiscal_fr").version == "2024.1"
    assert opened_calendars == [str(calendar_path)]


def test_date_from_injection_is_rejected_before_sql_generation():
    model = {
        "name": "orders",
        "key": "orders",
        "table": "gold.orders",
        "dimensions": [{"name": "order_date", "sql": "order_date", "type": "date"}],
        "metrics": [{"name": "revenue", "sql": "amount", "type": "sum"}],
    }
    query = SemanticQuery(
        model_name="orders",
        metrics=["revenue"],
        date_from="2024-01-01' OR '1'='1",
    )

    with pytest.raises(SemanticQueryError, match="Invalid date_from"):
        QueryResolver().resolve(query, model, "main")


def test_period_requires_a_planner():
    model = {
        "name": "orders",
        "table": "gold.orders",
        "dimensions": [{"name": "order_date", "sql": "order_date", "type": "date"}],
        "metrics": [{"name": "revenue", "sql": "amount", "type": "sum"}],
    }

    with pytest.raises(SemanticQueryError, match="requires a SemanticPlanner"):
        QueryResolver().resolve(
            SemanticQuery(model_name="orders", period="FY2024_Q3"), model, "main"
        )


def test_resolver_consumes_only_planned_calendar_bounds():
    model = {
        "name": "orders",
        "key": "orders",
        "table": "gold.orders",
        "calendar": "fiscal_fr",
        "dimensions": [{"name": "order_date", "sql": "order_date", "type": "date"}],
        "metrics": [{"name": "revenue", "sql": "amount", "type": "sum"}],
    }
    calendar = parse_calendar(
        {
            "key": "fiscal_fr",
            "version": "2024.1",
            "periods": [
                {"name": "FY2024_Q3", "start": "2024-10-01", "end": "2024-12-31"}
            ],
        }
    )

    class Semantic:
        _catalog = {
            "orders": {
                "key": "orders",
                "dimensions": ["order_date"],
                "metrics": ["revenue"],
                "entities": [],
                "related_models": [],
            }
        }

        def _get_model(self, model_key):
            return model

        def get_model_summary(self, model_key):
            return self._catalog[model_key]

        def _get_calendar(self, calendar_key):
            return calendar

    query = SemanticQuery(
        model_name="orders", metrics=["revenue"], period="FY2024_Q3"
    )
    resolved = QueryResolver(SemanticPlanner(Semantic())).resolve(query, model, "main")

    assert "m0.`order_date` >= '2024-10-01'" in resolved.where_clauses
    assert "m0.`order_date` <= '2024-12-31'" in resolved.where_clauses
