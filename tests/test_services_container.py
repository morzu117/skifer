from datetime import datetime, timezone

from skifer.lineage.tracker import LineageEdge, LineageGraph
from skifer.observability.metadata_store import DatasetRecord, SqliteMetadataStore
from skifer.services.container import build_services
from skifer.services.context import RequestContext, SCOPE_LINEAGE_READ
from skifer.observability.tracing import TraceContext


def test_build_services_does_not_scan_metadata_registry(monkeypatch, tmp_path):
    edge = LineageEdge("raw.orders", "id", "silver.orders", "id")
    graph = LineageGraph()
    graph.add_edge(edge)
    record = DatasetRecord(
        target_fqn="silver.orders",
        pipeline_path="schemas/orders.yaml",
        data_product_id="orders",
        contract_version="1.0.0",
        definition_hash="sha256:orders",
        owner=None,
        columns=(),
        indexed_at=datetime(2026, 9, 11, tzinfo=timezone.utc),
        lineage=graph.to_dict(),
    )
    scans = 0

    def counting_list_all(_self):
        nonlocal scans
        scans += 1
        return [record]

    monkeypatch.setattr(SqliteMetadataStore, "list_all", counting_list_all)

    services = build_services(str(tmp_path))

    assert scans == 0
    assert services.data_service.lineage_graph is services.governance._registry

    ctx = RequestContext(
        subject="local-user",
        scopes=frozenset({SCOPE_LINEAGE_READ}),
        consumer_class="local",
        trace_context=TraceContext(),
    )
    lineage = services.data_service.get_lineage(ctx, "silver.orders", "id")
    governance_upstream = services.governance.registry_upstream(
        ctx, "silver.orders", "id"
    )

    assert scans == 1
    assert [item.to_dict() for item in lineage.upstream] == governance_upstream
