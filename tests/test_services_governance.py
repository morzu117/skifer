"""Spark-free tests for governed certification and quarantine reads."""

from datetime import date, datetime, timezone
from decimal import Decimal
import json

import pytest
import yaml

from skifer.core.ir import parse_to_ir
from skifer.core.schema_loader import parse_schema
from skifer.observability.certification import ContractDefinition, diff_contracts
from skifer.observability.certification_store import Certification, RunEvent
from skifer.observability.tracing import TraceContext
from skifer.services import (
    GovernanceService,
    RequestContext,
    ResourceUnavailable,
    SCOPE_CONTRACTS_READ,
    ScopeDenied,
)


def _context(*scopes: str) -> RequestContext:
    return RequestContext(
        subject="local-user",
        scopes=frozenset(scopes),
        consumer_class="local",
        trace_context=TraceContext(),
    )


class _FakeFrame:
    def __init__(self, rows):
        self._rows = rows

    def collect(self):
        return self._rows


class _Store:
    def __init__(self, quarantine_rows=()):
        self.quarantine_rows = list(quarantine_rows)

    def get_contract(self, contract_id, version):
        return ContractDefinition(
            contract_id=contract_id,
            contract_version=version,
            definition_hash="hash",
            canonical_json=json.dumps(
                {"contract": {"grain": ["id"], "output": []}, "semantic": None}
            ),
            data_product_id=contract_id,
            owner="data-team",
        )

    def get_certification(self, dataset, consumer_class="default"):
        return Certification(
            dataset=dataset,
            consumer_class=consumer_class,
            status="CERTIFIED",
            contract_version="1.0.0",
            definition_hash="hash",
            certified_at=datetime(2026, 9, 11, tzinfo=timezone.utc),
        )

    def list_history(self, dataset, limit=50):
        return [
            RunEvent(
                event_id="event-1",
                run_id="run-1",
                dataset=dataset,
                state="PROMOTED",
                contract_id="orders",
                contract_version="1.0.0",
                definition_hash="hash",
                occurred_at=datetime(2026, 9, 11, tzinfo=timezone.utc),
            )
        ][:limit]

    def read_quarantine(self, dataset):
        return _FakeFrame(self.quarantine_rows)


def _schema(fields: dict):
    payload = {
        "data_product": {"id": "sales.orders", "version": "1.0.0"},
        "contract": {"output": fields},
        "tables": [{"name": "silver.orders"}],
        "select_final": [[name, name] for name in fields],
    }
    return parse_to_ir(parse_schema(yaml.safe_dump(payload, sort_keys=False)))


def test_get_contract_requires_scope():
    with pytest.raises(ScopeDenied):
        GovernanceService(_Store()).get_contract(_context(), "orders", "1.0.0")


def test_get_certification_requires_scope():
    with pytest.raises(ScopeDenied):
        GovernanceService(_Store()).get_certification(_context(), "gold.orders")


def test_get_contract_and_certification_are_allowlisted_views():
    service = GovernanceService(_Store())
    ctx = _context(SCOPE_CONTRACTS_READ)

    contract = service.get_contract(ctx, "orders", "1.0.0")
    certification = service.get_certification(ctx, "gold.orders")

    assert contract.to_dict()["contract_id"] == "orders"
    assert certification.to_dict()["certified_at"] == "2026-09-11T00:00:00+00:00"


def test_list_certification_history_is_json_native():
    history = GovernanceService(_Store()).list_certification_history(
        _context(SCOPE_CONTRACTS_READ), "gold.orders"
    )

    assert history[0]["occurred_at"] == "2026-09-11T00:00:00+00:00"
    json.dumps(history)


def test_read_quarantine_is_bounded():
    service = GovernanceService(_Store({"id": index} for index in range(200)), max_rows=100)

    view = service.read_quarantine(
        _context(SCOPE_CONTRACTS_READ), "gold.orders", limit=50
    )

    assert len(view.rows) == 50
    assert view.truncated is True


def test_read_quarantine_returns_json_native():
    service = GovernanceService(
        _Store([{"amount": Decimal("1.20"), "day": date(2026, 9, 11)}])
    )

    payload = service.read_quarantine(
        _context(SCOPE_CONTRACTS_READ), "gold.orders"
    ).to_dict()

    assert payload["rows"] == [{"amount": "1.20", "day": "2026-09-11"}]
    json.dumps(payload)


def test_read_quarantine_no_dataframe():
    view = GovernanceService(_Store([{"id": 1}])).read_quarantine(
        _context(SCOPE_CONTRACTS_READ), "gold.orders"
    )

    assert all(isinstance(row, dict) for row in view.rows)
    assert not hasattr(view, "collect")


def test_diff_contracts_requires_scope():
    with pytest.raises(ScopeDenied):
        GovernanceService(_Store()).diff_contracts(
            _context(), _schema({"id": {}}), _schema({"id": {}, "amount": {}})
        )


def test_governance_service_exposes_diff():
    old = _schema({"id": {}})
    new = _schema({"id": {}, "amount": {}})

    diff = GovernanceService(_Store()).diff_contracts(
        _context(SCOPE_CONTRACTS_READ), old, new
    )

    assert diff == diff_contracts(old, new)
    assert diff.added == ("amount",)


def test_store_missing_method_unavailable():
    with pytest.raises(ResourceUnavailable):
        GovernanceService(object()).get_contract(
            _context(SCOPE_CONTRACTS_READ), "orders", "1.0.0"
        )
