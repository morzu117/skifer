"""Spark-free tests for governed certification and quarantine reads."""

from datetime import date, datetime, timezone
from decimal import Decimal
import json

import pytest
import yaml

from skifer.core.ir import parse_to_ir
from skifer.core.schema_loader import parse_schema
from skifer.observability.certification import ContractDefinition, diff_contracts
from skifer.observability.certification_store import (
    Certification,
    RunEvent,
    StoredCheckResult,
)
from skifer.observability.checks import CheckStatus, ContractScope
from skifer.lineage.tracker import LineageEdge, LineageGraph
from skifer.observability.metadata_store import (
    ColumnRecord,
    DatasetRecord,
    SqliteMetadataStore,
)
from skifer.observability.tracing import TraceContext
from skifer.services import (
    GovernanceService,
    RequestContext,
    ResourceUnavailable,
    SCOPE_CONTRACTS_READ,
    SCOPE_LINEAGE_READ,
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

    def get_run(self, run_id):
        if run_id == "missing":
            return None
        return RunEvent(
            event_id=f"{run_id}:QUARANTINED",
            run_id=run_id,
            dataset="cat.silver.t",
            state="QUARANTINED",
            contract_id="orders",
            contract_version="1.0.0",
            definition_hash="hash",
            occurred_at=datetime(2026, 9, 11, tzinfo=timezone.utc),
            target_fqn="cat.silver.t",
            quarantine_fqn="cat._skifer_quarantine.t_snapshot",
        )

    def get_check_results(self, run_id):
        return [
            StoredCheckResult(
                event_id=f"{run_id}:NullCheck:0",
                run_id=run_id,
                check_type="NullCheck",
                scope=ContractScope.ROW,
                severity="critical",
                status=CheckStatus.FAIL,
                actual_value="1",
                expected_value="0",
                message="null id",
            )
        ]


def _metadata_record(
    target_fqn: str,
    *,
    columns: tuple[str, ...] = ("amount",),
    edges: tuple[LineageEdge, ...] = (),
) -> DatasetRecord:
    graph = LineageGraph()
    for edge in edges:
        graph.add_edge(edge)
    return DatasetRecord(
        target_fqn=target_fqn,
        pipeline_path=f"schemas/{target_fqn}.yaml",
        data_product_id=target_fqn,
        contract_version="1.0.0",
        definition_hash=f"sha256:{target_fqn}",
        owner="data-team",
        columns=tuple(ColumnRecord(name=column) for column in columns),
        indexed_at=datetime(2026, 9, 11, tzinfo=timezone.utc),
        lineage=graph.to_dict() if edges else {},
    )


def _metadata_store() -> SqliteMetadataStore:
    store = SqliteMetadataStore(":memory:")
    store.upsert(_metadata_record("silver.orders"))
    store.upsert(
        _metadata_record(
            "gold.kpi",
            columns=("amount_eur",),
            edges=(
                LineageEdge(
                    "silver.orders",
                    "amount",
                    "gold.kpi",
                    "amount_eur",
                    ["cast:double"],
                ),
            ),
        )
    )
    return store


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


def test_governance_get_run_by_run_id():
    outcome = GovernanceService(_Store()).get_run(
        _context(SCOPE_CONTRACTS_READ), "run-1"
    )

    assert outcome is not None
    assert outcome.to_dict() == {
        "run_id": "run-1",
        "state": "QUARANTINED",
        "target_fqn": "cat.silver.t",
        "quarantine_fqn": "cat._skifer_quarantine.t_snapshot",
        "checks_passed": False,
    }


def test_governance_get_run_missing_returns_none():
    assert (
        GovernanceService(_Store()).get_run(
            _context(SCOPE_CONTRACTS_READ), "missing"
        )
        is None
    )


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


def test_governance_registry_impact_delegates():
    service = GovernanceService(_Store(), metadata_store=_metadata_store())

    report = service.registry_impact(
        _context(SCOPE_LINEAGE_READ),
        "silver.orders",
    )

    assert report.to_dict()["impacted_datasets"] == ["gold.kpi"]
    assert report.impacted_columns == (("gold.kpi", "amount_eur"),)


def test_governance_registry_methods_require_lineage_scope():
    service = GovernanceService(_Store(), metadata_store=_metadata_store())

    with pytest.raises(ScopeDenied):
        service.registry_downstream(
            _context(SCOPE_CONTRACTS_READ),
            "silver.orders",
            "amount",
        )


def test_governance_registry_search_columns_is_allowlisted():
    service = GovernanceService(_Store(), metadata_store=_metadata_store())

    results = service.registry_search_columns(_context(SCOPE_LINEAGE_READ), "amount")

    assert results == [
        {
            "target_fqn": "gold.kpi",
            "column": {
                "name": "amount_eur",
                "logical_type": None,
                "classification": None,
                "description": None,
                "sources": [],
            },
        },
        {
            "target_fqn": "silver.orders",
            "column": {
                "name": "amount",
                "logical_type": None,
                "classification": None,
                "description": None,
                "sources": [],
            },
        },
    ]
    json.dumps(results)


def test_governance_registry_without_store_is_unavailable():
    with pytest.raises(ResourceUnavailable):
        GovernanceService(_Store()).registry_impact(
            _context(SCOPE_LINEAGE_READ),
            "silver.orders",
        )


def test_store_missing_method_unavailable():
    with pytest.raises(ResourceUnavailable):
        GovernanceService(object()).get_contract(
            _context(SCOPE_CONTRACTS_READ), "orders", "1.0.0"
        )
