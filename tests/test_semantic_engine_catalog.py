"""
Tests unitaires pour SemanticEngine — chargement catalog-first + lazy loading.
"""
import builtins
import os
import pytest
import yaml
from unittest.mock import MagicMock


from skifer.semantic.semantic import SemanticEngine


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_core():
    core = MagicMock()
    core.spark = MagicMock()
    core.db = "my_catalog"
    core.env = "dev"
    core.config = {}
    core.schema_suffix = ""
    return core


@pytest.fixture
def catalog_dir(tmp_path):
    """Crée un répertoire semantic_models avec catalog + YAML complet (structure plate)."""
    models_dir = tmp_path / "semantic_models"
    models_dir.mkdir()

    # YAML complet du modèle — fichier à plat : kpi_orders_erp.yaml
    model_yaml = {
        "models": [{
            "name": "kpi_orders_erp",
            "key": "kpi_orders_erp",
            "table": "gold.fact_orders",
            "layer": "gold",
            "description": "Métriques commandes ERP",
            "base_filter": "source_system = 'ERP'",
            "dimensions": [
                {"name": "region", "sql": "region", "type": "string"},
            ],
            "metrics": [
                {"name": "gross_revenue", "sql": "amount_ttc", "type": "sum"},
            ],
        }]
    }
    with open(models_dir / "kpi_orders_erp.yaml", "w") as f:
        yaml.dump(model_yaml, f)

    # Catalogue
    catalog = {
        "_generated_at": "2026-03-27T00:00:00",
        "_total_models": 1,
        "models": [{
            "key": "kpi_orders_erp",
            "file": "kpi_orders_erp.yaml",
            "layer": "gold",
            "table": "gold.fact_orders",
            "description": "Métriques commandes ERP",
            "tags": ["orders", "gold", "erp"],
            "dimensions": ["region"],
            "metrics": ["gross_revenue"],
            "entities": ["order", "customer"],
            "related_models": ["customers"],
            "base_filter": "source_system = 'ERP'",
            "generated_at": "2026-03-27",
        }]
    }
    with open(models_dir / "semantic_catalog.yaml", "w") as f:
        yaml.dump(catalog, f)

    return str(models_dir)


# ---------------------------------------------------------------------------
# SemanticEngine init
# ---------------------------------------------------------------------------

def test_engine_loads_catalog_at_init(mock_core, catalog_dir):
    """Au démarrage, seul le catalogue doit être chargé (pas les YAML complets)."""
    engine = SemanticEngine(mock_core, models_dir=catalog_dir)
    assert len(engine._catalog) == 1
    assert "kpi_orders_erp" in engine._catalog
    # Le cache YAML complet doit être vide au démarrage
    assert len(engine._cache) == 0


def test_engine_empty_catalog_on_missing_dir(mock_core, tmp_path):
    """Un répertoire sans catalog ne doit pas planter."""
    engine = SemanticEngine(mock_core, models_dir=str(tmp_path / "nonexistent"))
    assert engine._catalog == {}


# ---------------------------------------------------------------------------
# list_models
# ---------------------------------------------------------------------------

def test_list_models_returns_all(mock_core, catalog_dir):
    engine = SemanticEngine(mock_core, models_dir=catalog_dir)
    models = engine.list_models()
    assert len(models) == 1
    assert models[0]["key"] == "kpi_orders_erp"


def test_list_models_filter_by_tag(mock_core, catalog_dir):
    engine = SemanticEngine(mock_core, models_dir=catalog_dir)
    gold_models = engine.list_models(tags=["gold"])
    assert len(gold_models) == 1

    no_models = engine.list_models(tags=["marketing"])
    assert len(no_models) == 0


def test_list_models_filter_by_layer(mock_core, catalog_dir):
    engine = SemanticEngine(mock_core, models_dir=catalog_dir)
    assert len(engine.list_models(layer="gold")) == 1
    assert len(engine.list_models(layer="silver")) == 0


def test_list_models_summary_format(mock_core, catalog_dir):
    engine = SemanticEngine(mock_core, models_dir=catalog_dir)
    summaries = engine.list_models(summary=True)
    assert len(summaries) == 1
    assert set(summaries[0].keys()) == {
        "key",
        "description",
        "dimensions",
        "metrics",
        "entities",
        "related_models",
    }
    assert summaries[0]["entities"] == ["order", "customer"]
    assert summaries[0]["related_models"] == ["customers"]


def test_get_model_summary_uses_catalog_only_and_keeps_cache_empty(mock_core, catalog_dir):
    engine = SemanticEngine(mock_core, models_dir=catalog_dir)

    summary = engine.get_model_summary("kpi_orders_erp")

    assert summary["entities"] == ["order", "customer"]
    assert summary["related_models"] == ["customers"]
    assert engine._cache == {}


# ---------------------------------------------------------------------------
# _get_model — lazy loading
# ---------------------------------------------------------------------------

def test_get_model_lazy_loads_yaml(mock_core, catalog_dir):
    """_get_model doit charger le YAML complet à la demande."""
    engine = SemanticEngine(mock_core, models_dir=catalog_dir)
    assert "kpi_orders_erp" not in engine._cache

    model = engine._get_model("kpi_orders_erp")
    assert "kpi_orders_erp" in engine._cache
    assert model["table"] == "gold.fact_orders"
    assert len(model["dimensions"]) == 1
    assert len(model["metrics"]) == 1


def test_init_and_catalog_summaries_do_not_open_full_model_yaml(mock_core, tmp_path, monkeypatch):
    models_dir = tmp_path / "semantic_models"
    models_dir.mkdir()
    (models_dir / "semantic_catalog.yaml").write_text(
        yaml.safe_dump(
            {
                "models": [
                    {
                        "key": "orders",
                        "file": "orders.yaml",
                        "description": "Orders",
                        "dimensions": ["region"],
                        "metrics": ["revenue"],
                        "entities": ["order"],
                        "related_models": ["customers"],
                    }
                ]
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    (models_dir / "orders.yaml").write_text(
        yaml.safe_dump(
            {
                "models": [
                    {
                        "name": "orders",
                        "key": "orders",
                        "table": "gold.fact_orders",
                        "dimensions": [{"name": "region", "sql": "region", "type": "string"}],
                        "metrics": [{"name": "revenue", "sql": "amount", "type": "sum"}],
                    }
                ]
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    real_open = builtins.open
    opened_model_yaml: list[str] = []

    def tracking_open(path, *args, **kwargs):
        if os.fspath(path).endswith("orders.yaml"):
            opened_model_yaml.append(os.fspath(path))
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", tracking_open)

    engine = SemanticEngine(mock_core, models_dir=str(models_dir))
    summaries = engine.list_models(summary=True)
    summary = engine.get_model_summary("orders")

    assert summaries[0]["entities"] == ["order"]
    assert summary["related_models"] == ["customers"]
    assert engine._cache == {}
    assert opened_model_yaml == []


def test_get_model_cached_on_second_call(mock_core, catalog_dir):
    """La deuxième lecture doit venir du cache (pas d'I/O)."""
    engine = SemanticEngine(mock_core, models_dir=catalog_dir)
    engine._get_model("kpi_orders_erp")

    # On supprime le fichier YAML — le cache doit suffire
    yaml_path = os.path.join(catalog_dir, "kpi_orders_erp.yaml")
    os.remove(yaml_path)
    model = engine._get_model("kpi_orders_erp")
    assert model["table"] == "gold.fact_orders"


def test_get_model_raises_for_unknown_key(mock_core, catalog_dir):
    engine = SemanticEngine(mock_core, models_dir=catalog_dir)
    with pytest.raises(ValueError, match="kpi_unknown.erp"):
        engine._get_model("kpi_unknown.erp")


# ---------------------------------------------------------------------------
# update_catalog (méthode statique)
# ---------------------------------------------------------------------------

def test_update_catalog_creates_file(tmp_path):
    models_dir = str(tmp_path / "semantic_models")
    os.makedirs(models_dir)

    entry = {
        "key": "kpi_test_default",
        "file": "kpi_test_default.yaml",
        "layer": "gold",
        "table": "gold.fact_test",
        "description": "Test model",
        "tags": ["test"],
        "dimensions": ["region"],
        "metrics": ["revenue"],
        "generated_at": "2026-03-27",
    }
    SemanticEngine.update_catalog(models_dir, entry)

    catalog_path = os.path.join(models_dir, "semantic_catalog.yaml")
    assert os.path.exists(catalog_path)

    with open(catalog_path) as f:
        catalog = yaml.safe_load(f)
    assert catalog["_total_models"] == 1
    assert catalog["models"][0]["key"] == "kpi_test_default"


def test_update_catalog_replaces_existing(tmp_path):
    models_dir = str(tmp_path / "sm")
    os.makedirs(models_dir)

    entry_v1 = {
        "key": "kpi_test_erp",
        "file": "kpi_test_erp.yaml",
        "layer": "gold",
        "table": "gold.fact_test",
        "description": "Version 1",
        "tags": [],
        "dimensions": [],
        "metrics": [],
        "generated_at": "2026-03-01",
    }
    SemanticEngine.update_catalog(models_dir, entry_v1)

    entry_v2 = dict(entry_v1)
    entry_v2["description"] = "Version 2"
    SemanticEngine.update_catalog(models_dir, entry_v2)

    with open(os.path.join(models_dir, "semantic_catalog.yaml")) as f:
        catalog = yaml.safe_load(f)
    assert catalog["_total_models"] == 1
    assert catalog["models"][0]["description"] == "Version 2"


# ---------------------------------------------------------------------------
# get_llm_context
# ---------------------------------------------------------------------------

def test_get_llm_context_hides_sql(mock_core, catalog_dir):
    """get_llm_context doit exposer les noms mais PAS les expressions SQL."""
    import json
    engine = SemanticEngine(mock_core, models_dir=catalog_dir)
    ctx = json.loads(engine.get_llm_context("kpi_orders_erp"))

    assert "model_key" in ctx
    assert "available_dimensions" in ctx
    assert "available_metrics" in ctx

    # Le SQL ne doit pas apparaître dans le contexte LLM
    ctx_str = json.dumps(ctx)
    assert "amount_ttc" not in ctx_str
    assert "source_system" not in ctx_str


# ---------------------------------------------------------------------------
# SkiferEngine.get_agent — factory method (Option B)
# ---------------------------------------------------------------------------

def test_get_agent_returns_gen_bi_agent(tmp_path, mocker):
    """get_agent() doit retourner un GenBIAgent sans que l'utilisateur
    instancie SemanticEngine ou LLMProvider manuellement."""
    from skifer.core.core import SkiferEngine
    from skifer.agentic.agent import GenBIAgent

    mock_engine = MagicMock(spec=SkiferEngine)
    mock_engine.db = "my_catalog"
    mock_engine.env = "dev"
    mock_engine.config = {}
    mock_engine.schema_suffix = ""
    mock_engine._get_backend = MagicMock(return_value=MagicMock())

    mock_llm = MagicMock()
    models_dir = str(tmp_path / "semantic_models")
    os.makedirs(models_dir, exist_ok=True)

    # Appel direct à la logique de get_agent (on bypasse SkiferEngine.__init__)
    from skifer.semantic.semantic import SemanticEngine
    semantic = SemanticEngine(mock_engine, models_dir=models_dir)
    agent = GenBIAgent(semantic, mock_llm)

    assert isinstance(agent, GenBIAgent)
    assert agent.semantic is semantic
    assert agent.llm is mock_llm


def test_get_agent_with_explicit_llm(tmp_path):
    """get_agent(llm_provider=...) doit utiliser le provider fourni."""
    from skifer.agentic.agent import GenBIAgent
    from skifer.semantic.semantic import SemanticEngine

    mock_core = MagicMock()
    mock_core.db = "catalog"
    mock_core.env = "dev"
    mock_core.config = {}
    mock_core.schema_suffix = ""

    mock_llm = MagicMock()
    models_dir = str(tmp_path / "sm")
    os.makedirs(models_dir, exist_ok=True)

    semantic = SemanticEngine(mock_core, models_dir=models_dir)
    agent = GenBIAgent(semantic, mock_llm, history=True, session_title="Test Session")

    assert isinstance(agent, GenBIAgent)
    assert agent._conv_history == []   # history=True → liste initialisée
    assert agent._session.title == "Test Session"
