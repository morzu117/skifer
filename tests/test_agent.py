"""
Tests unitaires pour GenBIAgent._format_kpi.
"""
from datetime import date, datetime, timezone
import json
from types import SimpleNamespace
import pytest
import yaml
from unittest.mock import MagicMock

from skifer.agentic.agent import GenBIAgent
from skifer.agentic.resolver import SemanticQuery
from skifer.agentic.models import ResponseFormat
from skifer.observability.certification_store import Certification
from skifer.semantic.access_policy import CertificationDecision
from skifer.semantic.semantic import SemanticEngine


def _make_agent():
    """Crée un GenBIAgent avec des dépendances mockées."""
    semantic = MagicMock()
    llm = MagicMock()
    agent = GenBIAgent.__new__(GenBIAgent)
    agent.semantic = semantic
    agent.llm = llm
    agent._conv_history = None
    agent._resolver = MagicMock()
    from skifer.agentic.history import SessionHistory
    agent._session = SessionHistory("test")
    return agent


def _make_query(metrics=None, response_format="kpi"):
    return SemanticQuery(
        model_name="kpi_test",
        metrics=metrics or ["chiffre_affaires"],
        response_format=response_format,
    )


def test_model_context_lists_declared_calendar_period_names_only():
    agent = _make_agent()
    agent.semantic._get_calendar.return_value = SimpleNamespace(
        key="fiscal_fr",
        version="2024.1",
        periods=[
            SimpleNamespace(
                name="FY2024_Q3", start=date(2024, 10, 1), end=date(2024, 12, 31)
            )
        ],
    )

    context = json.loads(
        agent._build_model_context(
            "orders",
            {"calendar": "fiscal_fr", "dimensions": [], "metrics": []},
        )
    )

    assert context["calendar"] == {
        "key": "fiscal_fr",
        "version": "2024.1",
        "periods": ["FY2024_Q3"],
    }


# ---------------------------------------------------------------------------
# _format_kpi — valeur extraite correctement
# ---------------------------------------------------------------------------

def test_format_kpi_extracts_value():
    agent = _make_agent()
    sq = _make_query(metrics=["chiffre_affaires"])

    row = MagicMock()
    row.__getitem__ = MagicMock(return_value=21770.24)
    df = MagicMock()
    df.collect.return_value = [row]

    result = agent._format_kpi(df, sq, "CA 2024")
    assert result.format == ResponseFormat.KPI
    assert result.kpi_value == 21770.24
    assert result.kpi_label == "chiffre_affaires"


def test_format_kpi_empty_dataframe_returns_none():
    """Une agrégation sans données retourne kpi_value=None sans lever d'erreur."""
    agent = _make_agent()
    sq = _make_query(metrics=["chiffre_affaires"])

    df = MagicMock()
    df.collect.return_value = []

    result = agent._format_kpi(df, sq, "CA vide")
    assert result.kpi_value is None
    assert result.kpi_label == "chiffre_affaires"


def test_format_kpi_spark_exception_propagates():
    """Une exception Spark (AnalysisException, etc.) remonte — ne pas la swallow."""
    agent = _make_agent()
    sq = _make_query(metrics=["chiffre_affaires"])

    df = MagicMock()
    df.collect.side_effect = Exception("Table or view not found: orders_view")

    with pytest.raises(Exception, match="Table or view not found"):
        agent._format_kpi(df, sq, "CA 2024")


class _SpyBackend:
    def __init__(self):
        self.sql: list[str] = []

    def execute_sql(self, sql: str):
        self.sql.append(sql)
        return MagicMock()


class _FakeContext:
    def __init__(self, env_config: dict):
        self._env_config = env_config

    def env_config(self) -> dict:
        return self._env_config


class _FakeCore:
    def __init__(self):
        self.db = "main"
        self.env = "dev"
        self.config = {"environments": {"dev": {}}}
        self.schema_suffix = ""
        self._context = _FakeContext(
            {
                "semantic_certification_policy": "enforce",
                "semantic_consumer_class": "agent_read",
            }
        )
        self.backend = _SpyBackend()

    def _get_backend(self):
        return self.backend


class _UncertifiedStore:
    def get_certification(self, dataset: str, consumer_class: str = "default"):
        return Certification(
            dataset=dataset,
            consumer_class=consumer_class,
            status="UNCERTIFIED",
            contract_version=None,
            definition_hash=None,
            certified_at=None,
        )


class _CertifiedStore:
    def __init__(self):
        self.calls: list[tuple[str, str]] = []

    def get_certification(self, dataset: str, consumer_class: str = "default"):
        self.calls.append((dataset, consumer_class))
        return Certification(
            dataset=dataset,
            consumer_class=consumer_class,
            status="CERTIFIED",
            contract_version="v1",
            definition_hash="hash1",
            certified_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )


class _FlippingStore:
    def __init__(self):
        self.calls = 0

    def get_certification(self, dataset: str, consumer_class: str = "default"):
        self.calls += 1
        status = "CERTIFIED" if self.calls == 1 else "UNCERTIFIED"
        return Certification(
            dataset=dataset,
            consumer_class=consumer_class,
            status=status,
            contract_version="v1",
            definition_hash="hash1",
            certified_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )


def _write_agent_semantic_model(tmp_path):
    root = tmp_path / "semantic_models"
    root.mkdir()
    model = {
        "models": [
            {
                "key": "sales.orders",
                "table": "gold.fact_orders",
                "dimensions": [{"name": "region", "sql": "region"}],
                "metrics": [{"name": "revenue", "sql": "amount", "type": "sum"}],
            }
        ]
    }
    catalog = {
        "models": [
            {
                "key": "sales.orders",
                "file": "sales_orders.yaml",
                "description": "Sales orders",
                "dimensions": ["region"],
                "metrics": ["revenue"],
            }
        ]
    }
    (root / "sales_orders.yaml").write_text(yaml.safe_dump(model), encoding="utf-8")
    (root / "semantic_catalog.yaml").write_text(
        yaml.safe_dump(catalog),
        encoding="utf-8",
    )
    return root


def _make_semantic_agent(tmp_path, store, *, query_mode: str = "query"):
    root = _write_agent_semantic_model(tmp_path)
    core = _FakeCore()
    semantic = SemanticEngine(
        core,
        models_dir=str(root),
        certification_store=store,
    )
    view_name = '"view_name": "v_orders", ' if query_mode == "view" else ""
    llm = MagicMock()
    llm.complete.side_effect = [
        '{"selected": "sales.orders", "candidates": [], "reason": "ok"}',
        (
            '{"model_name": "sales.orders", "metrics": ["revenue"], '
            f'"group_by": ["region"], "mode": "{query_mode}", {view_name}'
            '"response_format": "table", "explanation": "revenue by region"}'
        ),
    ]
    return GenBIAgent(semantic, llm), core


def test_agent_query_mode_denies_before_resolve_for_uncertified_dependency(
    tmp_path,
    monkeypatch,
):
    from skifer.agentic import resolver as resolver_module

    resolve_calls = 0
    original_resolve = resolver_module.QueryResolver.resolve

    def spy_resolve(self, query, model, catalog_fqn):
        nonlocal resolve_calls
        resolve_calls += 1
        return original_resolve(self, query, model, catalog_fqn)

    monkeypatch.setattr(resolver_module.QueryResolver, "resolve", spy_resolve)
    agent, core = _make_semantic_agent(tmp_path, _UncertifiedStore())

    response = agent.ask("revenue by region", mode="query")

    assert response.mode == "error"
    assert response.access_denied is True
    assert response.policy_decision == CertificationDecision.DENY.value
    assert resolve_calls == 0
    assert core.backend.sql == []


def test_agent_query_mode_recheck_denies_when_certification_changes(tmp_path):
    store = _FlippingStore()
    agent, core = _make_semantic_agent(tmp_path, store)

    response = agent.ask("revenue by region", mode="query")

    assert response.mode == "error"
    assert response.access_denied is True
    assert response.policy_decision == CertificationDecision.DENY.value
    assert store.calls == 2
    assert core.backend.sql == []


def test_agent_query_mode_warn_counts_warning_once_per_logical_request(tmp_path):
    # Preflight (agent.py _process) and recheck (agent.py _execute_query) both
    # call enforce_certification_gate — the recheck must pass count_warning=False
    # or a single agent.ask() doubles the counter (merged_bug_001 regression).
    agent, core = _make_semantic_agent(tmp_path, _UncertifiedStore())
    core._context._env_config["semantic_certification_policy"] = "warn"

    response = agent.ask("revenue by region", mode="query")

    assert response.mode == "query"
    assert agent.semantic.certification_warning_count == 1


def test_agent_query_attaches_optional_evidence_without_changing_result_contract(tmp_path):
    agent, _core = _make_semantic_agent(tmp_path, _CertifiedStore())

    response = agent.ask("revenue by region", mode="query")

    assert response.mode == "query"
    assert response.evidence is not None
    assert response.evidence.execution_status == "succeeded"
    assert response.result.evidence is response.evidence


def test_agent_response_evidence_defaults_to_none():
    from skifer.agentic.models import AgentResponse

    assert AgentResponse(question="q").evidence is None


def test_agent_view_mode_uses_create_view_gate_without_agent_preflight(
    tmp_path,
    monkeypatch,
):
    create_view_calls = 0
    original_create_view = SemanticEngine.create_view
    store = _CertifiedStore()
    agent, core = _make_semantic_agent(tmp_path, store, query_mode="view")

    def spy_create_view(self, *args, **kwargs):
        nonlocal create_view_calls
        create_view_calls += 1
        return original_create_view(self, *args, **kwargs)

    monkeypatch.setattr(SemanticEngine, "create_view", spy_create_view)

    response = agent.ask("create view revenue by region", mode="view")

    assert response.mode == "view"
    assert response.result == "`main`.`semantic_views`.`v_orders`"
    assert create_view_calls == 1
    # preflight + recheck; the recheck's certifications are reused for the view
    # comment's provenance (bug_003 review follow-up removed the 3rd re-fetch).
    assert len(store.calls) == 2
    assert len(core.backend.sql) == 1


def test_agent_query_mode_denies_uncertified_dependency_before_execute(tmp_path):
    root = tmp_path / "semantic_models"
    root.mkdir()
    model = {
        "models": [
            {
                "key": "sales.orders",
                "table": "gold.fact_orders",
                "dimensions": [{"name": "region", "sql": "region"}],
                "metrics": [{"name": "revenue", "sql": "amount", "type": "sum"}],
            }
        ]
    }
    catalog = {
        "models": [
            {
                "key": "sales.orders",
                "file": "sales_orders.yaml",
                "description": "Sales orders",
                "dimensions": ["region"],
                "metrics": ["revenue"],
            }
        ]
    }
    (root / "sales_orders.yaml").write_text(yaml.safe_dump(model), encoding="utf-8")
    (root / "semantic_catalog.yaml").write_text(
        yaml.safe_dump(catalog),
        encoding="utf-8",
    )
    core = _FakeCore()
    semantic = SemanticEngine(
        core,
        models_dir=str(root),
        certification_store=_UncertifiedStore(),
    )
    llm = MagicMock()
    llm.complete.side_effect = [
        '{"selected": "sales.orders", "candidates": [], "reason": "ok"}',
        (
            '{"model_name": "sales.orders", "metrics": ["revenue"], '
            '"group_by": ["region"], "mode": "query", '
            '"response_format": "table", "explanation": "revenue by region"}'
        ),
    ]
    agent = GenBIAgent(semantic, llm)

    response = agent.ask("revenue by region", mode="query")

    assert response.mode == "error"
    assert response.access_denied is True
    assert response.error is not None
    assert response.policy_decision == CertificationDecision.DENY.value
    assert core.backend.sql == []


def test_agent_query_mode_does_not_print_sql_when_gate_denies(tmp_path, capsys):
    root = tmp_path / "semantic_models"
    root.mkdir()
    model = {
        "models": [
            {
                "key": "sales.orders",
                "table": "gold.fact_orders",
                "dimensions": [{"name": "region", "sql": "region"}],
                "metrics": [{"name": "revenue", "sql": "amount", "type": "sum"}],
            }
        ]
    }
    catalog = {
        "models": [
            {
                "key": "sales.orders",
                "file": "sales_orders.yaml",
                "description": "Sales orders",
                "dimensions": ["region"],
                "metrics": ["revenue"],
            }
        ]
    }
    (root / "sales_orders.yaml").write_text(yaml.safe_dump(model), encoding="utf-8")
    (root / "semantic_catalog.yaml").write_text(
        yaml.safe_dump(catalog),
        encoding="utf-8",
    )
    core = _FakeCore()
    semantic = SemanticEngine(
        core,
        models_dir=str(root),
        certification_store=_UncertifiedStore(),
    )
    llm = MagicMock()
    llm.complete.side_effect = [
        '{"selected": "sales.orders", "candidates": [], "reason": "ok"}',
        (
            '{"model_name": "sales.orders", "metrics": ["revenue"], '
            '"group_by": ["region"], "mode": "query", '
            '"response_format": "table", "explanation": "revenue by region"}'
        ),
    ]
    agent = GenBIAgent(semantic, llm)

    response = agent.ask("revenue by region", mode="query")
    captured = capsys.readouterr()

    assert response.mode == "error"
    assert "Exécution SQL" not in captured.out
    assert "SELECT" not in captured.out


def test_step_b_prompt_keeps_free_date_bounds_alongside_periods():
    # Declaring `period` must not remove `date_from`/`date_to` from the JSON the
    # LLM is asked for: every model without a `calendar:` would silently lose
    # date filtering and answer an "in January" question with all-time figures.
    from skifer.agentic.agent import _STEP_B_SYSTEM

    assert '"date_from"' in _STEP_B_SYSTEM
    assert '"date_to"' in _STEP_B_SYSTEM
    assert '"period"' in _STEP_B_SYSTEM
    assert "calendar.periods" in _STEP_B_SYSTEM
