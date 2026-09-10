"""Transport-free security contract for AgentReadyDataService (Plan 29, slice 7.1)."""

from datetime import datetime, timezone
import json

import pytest

from skifer.agentic.data_service import (
    AgentReadyDataService,
    GovernedModelView,
    InvalidCursor,
    InvalidRequest,
    LimitExceeded,
    QueryEnvelope,
    RequestContext,
    ResourceNotFound,
    ResourceUnavailable,
    ScopeDenied,
    ServiceLimits,
)
from skifer.agentic.resolver import SemanticQuery
from skifer.lineage.tracker import LineageEdge, LineageGraph
from skifer.observability.certification import ContractDefinition
from skifer.observability.certification_store import Certification
from skifer.observability.tracing import TraceContext
from skifer.semantic.access_policy import (
    CertificationDecision,
    PolicyEvaluation,
    SemanticAccessDenied,
)
from skifer.semantic.evidence import SemanticEvidence, SemanticResult


TRACE_ID = "1234567890abcdef1234567890abcdef"


def _ctx(*scopes, subject="agent-a", consumer_class="mcp"):
    return RequestContext(
        subject=subject,
        scopes=frozenset(scopes),
        consumer_class=consumer_class,
        trace_context=TraceContext(trace_id=TRACE_ID),
    )


def _catalog(count=3):
    return [
        {
            "key": f"model-{index:02d}",
            "description": f"Model {index}",
            "layer": "gold",
            "table": f"gold.table_{index:02d}",
            "tags": ["safe"],
            "dimensions": ["country"],
            "metrics": ["revenue"],
            "entities": ["order"],
            "related_models": [],
        }
        for index in range(count)
    ]


def _contract():
    canonical = {
        "contract": {
            "grain": ["order_id"],
            "output": [
                {
                    "name": "order_id",
                    "logical_type": "string",
                    "required": True,
                    "unique": True,
                    "classification": "internal",
                    "entity": "order",
                    "secret_future_field": "hidden",
                }
            ],
            "secret_future_field": "hidden",
        },
        "semantic": {
            "model_key": "model-00",
            "entity": "order",
            "default_time_dimension": "order_date",
            "dimensions": ["country"],
            "secret_future_field": "hidden",
        },
        "secret_future_field": "hidden",
    }
    return ContractDefinition(
        contract_id="orders",
        contract_version="1.0.0",
        definition_hash="hash",
        canonical_json=json.dumps(canonical),
        data_product_id="orders",
        owner="data-team",
        status="PUBLISHED",
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )


class FakeStore:
    def __init__(self):
        self.contract = _contract()

    def get_contract(self, contract_id, version):
        if (contract_id, version) == ("orders", "1.0.0"):
            return self.contract
        return None

    def get_certification(self, dataset, consumer_class="default"):
        return Certification(
            dataset=dataset,
            consumer_class=consumer_class,
            status="CERTIFIED",
            contract_version="1.0.0",
            definition_hash="hash",
            certified_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
            checks_passed=True,
        )


class BoundedDataFrame:
    def __init__(self, rows, calls=None):
        self.rows = rows
        self.calls = calls if calls is not None else []

    def limit(self, value):
        self.calls.append(value)
        return BoundedDataFrame(self.rows[:value], self.calls)

    def collect(self):
        return list(self.rows)


def _evidence():
    now = datetime(2026, 1, 3, tzinfo=timezone.utc)
    return SemanticEvidence(
        evidence_id="evidence-1",
        trace_id=TRACE_ID,
        model_keys=("model-00",),
        metrics=(),
        dimensions=("country",),
        normalized_filters=(),
        sources=(),
        policy=PolicyEvaluation(CertificationDecision.ALLOW, (), now),
        sql_hash="sha256:v1:abc",
        sql_text=None,
        statement_id=None,
        compiled_at=now,
        executed_at=now,
        execution_status="succeeded",
    )


class FakeSemanticEngine:
    def __init__(self, catalog=None, rows=None):
        self.catalog = list(catalog if catalog is not None else _catalog())
        self.certification_store = FakeStore()
        self.dataframe = BoundedDataFrame(rows if rows is not None else [{"value": 1}])
        self.query_calls = []
        self.query_error = None

    def list_models(self):
        return list(reversed(self.catalog))

    def get_model_summary(self, key):
        for entry in self.catalog:
            if entry["key"] == key:
                return entry
        raise ValueError(key)

    def query_with_evidence(self, query, **kwargs):
        self.query_calls.append((query, kwargs))
        if self.query_error is not None:
            raise self.query_error
        return SemanticResult(self.dataframe, _evidence())


def _graph():
    graph = LineageGraph()
    graph.add_edge(
        LineageEdge(
            "silver.orders",
            "country_code",
            "model-00",
            "country",
            ["upper"],
            "metric",
        )
    )
    return graph


def _service(catalog=None, rows=None, *, limits=None):
    engine = FakeSemanticEngine(catalog=catalog, rows=rows)
    return AgentReadyDataService(engine, lineage_graph=_graph(), limits=limits), engine


@pytest.mark.parametrize(
    ("method", "args"),
    [
        ("list_models", ()),
        ("get_model", ("model-00",)),
        ("get_contract", ("orders", "1.0.0")),
        ("get_certification", ("model-00",)),
        ("get_lineage", ("model-00", "country")),
        ("query", (SemanticQuery("model-00"),)),
    ],
)
def test_every_method_denies_missing_scope(method, args):
    service, _ = _service()

    with pytest.raises(ScopeDenied):
        getattr(service, method)(_ctx(), *args)


@pytest.mark.parametrize(
    ("method", "args", "neighbor_scope"),
    [
        ("list_models", (), "contracts:read"),
        ("get_model", ("model-00",), "lineage:read"),
        ("get_contract", ("orders", "1.0.0"), "models:read"),
        ("get_certification", ("model-00",), "query:execute"),
        ("get_lineage", ("model-00", "country"), "contracts:read"),
        ("query", (SemanticQuery("model-00"),), "models:read"),
    ],
)
def test_neighbor_scope_does_not_unlock_method(method, args, neighbor_scope):
    service, _ = _service()

    with pytest.raises(ScopeDenied):
        getattr(service, method)(_ctx(neighbor_scope), *args)


def test_query_filters_client_scopes_and_propagates_subject_and_trace():
    service, engine = _service()

    service.query(
        _ctx("query:execute", "certification_override", "future:dangerous"),
        SemanticQuery("model-00"),
    )

    context = engine.query_calls[0][1]["consumer_context"]
    assert context.consumer_id == "agent-a"
    assert context.consumer_class == "mcp"
    assert context.trace_id == TRACE_ID
    assert context.scopes == frozenset()


def test_different_subjects_create_distinct_consumer_contexts():
    service, engine = _service()

    service.query(_ctx("query:execute", subject="agent-a"), SemanticQuery("model-00"))
    service.query(_ctx("query:execute", subject="agent-b"), SemanticQuery("model-00"))

    subjects = [call[1]["consumer_context"].consumer_id for call in engine.query_calls]
    assert subjects == ["agent-a", "agent-b"]


def test_pagination_walks_sorted_catalog_without_duplicates_or_gaps():
    service, _ = _service(catalog=_catalog(17))
    cursor = None
    keys = []

    while True:
        page = service.list_models(_ctx("models:read"), cursor=cursor, limit=4)
        keys.extend(item.key for item in page.items)
        assert page.total == 17
        cursor = page.next_cursor
        if cursor is None:
            break

    assert keys == [f"model-{index:02d}" for index in range(17)]
    assert len(keys) == len(set(keys))


@pytest.mark.parametrize("cursor", ["forged", "eyJhZnRlciI6"])
def test_pagination_rejects_forged_or_truncated_cursor(cursor):
    service, _ = _service()

    with pytest.raises(InvalidCursor):
        service.list_models(_ctx("models:read"), cursor=cursor)


@pytest.mark.parametrize("limit", [0, 101])
def test_pagination_rejects_limits_outside_hard_bounds(limit):
    service, _ = _service()

    with pytest.raises(LimitExceeded):
        service.list_models(_ctx("models:read"), limit=limit)


def test_cursor_is_rejected_after_catalog_changes():
    service, engine = _service(catalog=_catalog(4))
    cursor = service.list_models(_ctx("models:read"), limit=2).next_cursor
    engine.catalog.append({**_catalog(1)[0], "key": "new-model"})

    with pytest.raises(InvalidCursor):
        service.list_models(_ctx("models:read"), cursor=cursor, limit=2)


def test_query_collects_only_limit_plus_one_and_marks_truncation():
    service, engine = _service(rows=[{"id": index} for index in range(10)])

    envelope = service.query(
        _ctx("query:execute"), SemanticQuery("model-00"), limit=3
    )

    assert engine.dataframe.calls == [4]
    assert envelope.rows == [{"id": 0}, {"id": 1}, {"id": 2}]
    assert envelope.truncated is True


def test_query_hard_limit_cannot_be_bypassed_by_configuration():
    with pytest.raises(LimitExceeded):
        ServiceLimits(max_query_rows=1_001)
    service, _ = _service()

    with pytest.raises(LimitExceeded):
        service.query(_ctx("query:execute"), SemanticQuery("model-00"), limit=1_001)


def test_query_rejects_too_many_filters():
    service, _ = _service(limits=ServiceLimits(max_filters=2))
    query = SemanticQuery(
        "model-00",
        filters=[{"column": "country", "operator": "eq", "value": str(index)} for index in range(3)],
    )

    with pytest.raises(LimitExceeded, match="at most 2"):
        service.query(_ctx("query:execute"), query)


def test_query_rejects_filter_value_that_is_too_long():
    service, _ = _service(limits=ServiceLimits(max_filter_value_length=3))
    query = SemanticQuery(
        "model-00",
        filters=[{"column": "country", "operator": "eq", "value": "EMEA"}],
    )

    with pytest.raises(LimitExceeded, match="3 characters"):
        service.query(_ctx("query:execute"), query)


def test_query_propagates_semantic_access_denied_for_uncertified_dataset():
    service, engine = _service()
    denial = SemanticAccessDenied(
        model_key="model-00",
        datasets=("gold.table_00",),
        decision=CertificationDecision.DENY,
        reasons=("MISSING",),
        evaluated_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        recommended_action="Certify the dataset.",
    )
    engine.query_error = denial

    with pytest.raises(SemanticAccessDenied) as raised:
        service.query(_ctx("query:execute"), SemanticQuery("model-00"))

    assert raised.value is denial


def test_model_dtos_allowlist_catalog_fields():
    catalog = _catalog(1)
    catalog[0]["secret_internal"] = "do not expose"
    service, _ = _service(catalog=catalog)

    summary = service.list_models(_ctx("models:read")).items[0].to_dict()
    detail = service.get_model(_ctx("models:read"), "model-00").to_dict()

    assert "secret_internal" not in summary
    assert "secret_internal" not in detail
    assert "table" not in detail


def test_contract_dto_allowlists_nested_canonical_payload():
    service, _ = _service()

    payload = service.get_contract(
        _ctx("contracts:read"), "orders", "1.0.0"
    ).to_dict()

    assert "secret_future_field" not in json.dumps(payload)
    assert payload["output"][0]["name"] == "order_id"


def test_query_returns_no_dataframe_and_redacts_evidence_by_default():
    service, _ = _service(rows=[{"value": 1}])

    envelope = service.query(_ctx("query:execute"), SemanticQuery("model-00"))

    assert isinstance(envelope, QueryEnvelope)
    assert set(envelope.to_dict()) == {"rows", "evidence", "truncated"}
    assert "dataframe" not in envelope.to_dict()
    assert "sql_text" not in envelope.evidence


def test_get_model_rejects_hidden_resource_without_loading_yaml():
    service, _ = _service()

    with pytest.raises(ResourceNotFound):
        service.get_model(_ctx("models:read"), "hidden-model")


def test_get_contract_rejects_hidden_resource():
    service, _ = _service()

    with pytest.raises(ResourceNotFound):
        service.get_contract(_ctx("contracts:read"), "hidden", "9.9.9")


def test_get_certification_rejects_dataset_outside_catalog():
    service, _ = _service()

    with pytest.raises(ResourceNotFound):
        service.get_certification(_ctx("contracts:read"), "arbitrary.gold.secret")


def test_get_certification_returns_uncertified_governed_dataset():
    service, engine = _service()
    engine.certification_store.get_certification = lambda dataset, consumer_class: Certification(
        dataset, consumer_class, "UNCERTIFIED", None, None, None, checks_passed=True
    )

    view = service.get_certification(_ctx("contracts:read"), "model-00")

    assert view.dataset == "gold.table_00"
    assert view.status == "UNCERTIFIED"


def test_get_certification_fails_closed_without_store():
    service, engine = _service()
    engine.certification_store = None

    with pytest.raises(ResourceUnavailable):
        service.get_certification(_ctx("contracts:read"), "model-00")


def test_get_lineage_rejects_hidden_resource():
    service, _ = _service()

    with pytest.raises(ResourceNotFound):
        service.get_lineage(_ctx("lineage:read"), "model-00", "secret")


def test_get_lineage_returns_allowlisted_edges():
    service, _ = _service()

    view = service.get_lineage(_ctx("lineage:read"), "model-00", "country")

    assert view.upstream[0].source_table == "silver.orders"
    assert set(view.upstream[0].to_dict()) == {
        "source_table",
        "source_column",
        "target_table",
        "target_column",
        "transformations",
        "edge_type",
    }


def test_get_model_returns_frozen_governed_view():
    service, _ = _service()
    view = service.get_model(_ctx("models:read"), "model-00")

    assert isinstance(view, GovernedModelView)
    with pytest.raises(AttributeError):
        view.key = "changed"


# ---------------------------------------------------------------------------
# Plan 29 — contract references are shape-checked at the boundary
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "contract_id",
    [
        "x\\",                       # backslash: escaped the closing quote in SQL
        "sales.orders' OR '1'='1",
        "sales.orders\nDROP",
        "",
        "a" * 300,
        None,
        123,
    ],
)
def test_get_contract_rejects_malformed_contract_ids(contract_id):
    """These two values are the only free text an agent puts on a path to SQL."""
    service, _ = _service()
    ctx = _ctx("contracts:read")

    with pytest.raises(InvalidRequest):
        service.get_contract(ctx, contract_id, "1.0.0")


@pytest.mark.parametrize(
    "version", ["1.0", "' OR 1=1 --", "latest", "", None, "1.0.0\\"]
)
def test_get_contract_rejects_non_semantic_versions(version):
    service, _ = _service()
    ctx = _ctx("contracts:read")

    with pytest.raises(InvalidRequest):
        service.get_contract(ctx, "sales.orders", version)


def test_get_contract_validates_before_touching_the_store():
    """A malformed reference must never reach storage in the first place."""
    service, engine = _service()
    engine.certification_store = None  # would raise if it were reached
    ctx = _ctx("contracts:read")

    with pytest.raises(InvalidRequest):
        service.get_contract(ctx, "x\\", "1.0.0")
