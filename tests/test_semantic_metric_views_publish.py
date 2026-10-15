"""Tests pour SemanticEngine.publish_metric_views (Plan 42.3)."""

from datetime import datetime, timezone

import pytest
import yaml

from skifer.observability.certification_store import Certification
from skifer.semantic.semantic import SemanticEngine


class FakeBackend:
    def __init__(self):
        self.sql: list[str] = []

    def execute_sql(self, sql: str):
        self.sql.append(sql)
        return {"sql": sql}


class FakeContext:
    def __init__(self, env: str, config: dict):
        self.env = env
        self.config = config

    def env_config(self) -> dict:
        return self.config.get("environments", {}).get(self.env, {})


class FakeCore:
    def __init__(self, env_config: dict, *, schema_suffix: str = ""):
        self.db = "sts_demo_catalog"
        self.env = "dev"
        self.config = {"environments": {"dev": env_config}}
        self.schema_suffix = schema_suffix
        self._context = FakeContext("dev", self.config)
        self.backend = FakeBackend()

    def _get_backend(self):
        return self.backend


class BombStore:
    """Un store que 'off' ne doit jamais consulter : Plan 29, pas de dépendance résolue."""

    def get_certification(self, dataset: str, consumer_class: str = "default"):
        raise AssertionError("get_certification ne doit pas être appelé en mode 'off'")


class MappingStore:
    def __init__(self, statuses: dict[str, str]):
        self.statuses = statuses

    def get_certification(self, dataset: str, consumer_class: str = "default"):
        status = self.statuses.get(dataset, "UNCERTIFIED")
        return Certification(
            dataset=dataset,
            consumer_class=consumer_class,
            status=status,
            contract_version="v1",
            definition_hash="hash1",
            certified_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )


def _write_catalog(root, models: list[dict]):
    for model in models:
        (root / f"{model['key']}.yaml").write_text(
            yaml.safe_dump({"models": [model]}), encoding="utf-8"
        )
    catalog = {
        "models": [
            {"key": m["key"], "file": f"{m['key']}.yaml", "description": m.get("description", "")}
            for m in models
        ]
    }
    (root / "semantic_catalog.yaml").write_text(yaml.safe_dump(catalog), encoding="utf-8")


def _model(key: str, table: str, **extra) -> dict:
    return {
        "key": key,
        "table": table,
        "description": "d",
        "dimensions": [{"name": "region", "sql": "region"}],
        "metrics": [{"name": "total_revenue", "sql": "revenue", "type": "sum"}],
        **extra,
    }


@pytest.fixture
def models_dir(tmp_path):
    root = tmp_path / "semantic_models"
    root.mkdir()
    _write_catalog(
        root,
        [_model("sales.orders", "gold.kpi_orders"), _model("sales.customers", "gold.kpi_customers")],
    )
    return str(root)


def test_off_mode_publishes_without_resolving_any_dependency(models_dir):
    core = FakeCore({"semantic_certification_policy": "off"})
    engine = SemanticEngine(core, models_dir=models_dir, certification_store=BombStore())

    report = engine.publish_metric_views()

    assert report.published == ("sales.orders", "sales.customers")
    assert report.refused == ()
    assert report.dropped == ()
    assert any("CREATE SCHEMA IF NOT EXISTS" in sql for sql in core.backend.sql)
    assert sum("CREATE OR REPLACE VIEW" in sql for sql in core.backend.sql) == 2


def test_sandbox_suffix_applies_to_the_metric_views_schema(models_dir):
    core = FakeCore({"semantic_certification_policy": "off"}, schema_suffix="_jdoe")
    engine = SemanticEngine(core, models_dir=models_dir, certification_store=BombStore())

    engine.publish_metric_views(model_keys=["sales.orders"])

    assert any("`semantic_jdoe`" in sql for sql in core.backend.sql)


def test_enforce_refuses_an_uncertified_model_and_drops_its_view(models_dir):
    core = FakeCore(
        {"semantic_certification_policy": "enforce", "semantic_consumer_class": "agent_read"}
    )
    store = MappingStore({"gold.kpi_orders": "CERTIFIED"})  # kpi_customers absent → MISSING
    engine = SemanticEngine(core, models_dir=models_dir, certification_store=store)

    report = engine.publish_metric_views()

    assert report.published == ("sales.orders",)
    assert report.dropped == ("sales.customers",)
    [refusal] = report.refused
    assert refusal["model"] == "sales.customers"
    assert "MISSING" in refusal["reason"]
    assert any(
        "DROP VIEW IF EXISTS" in sql and "`customers`" in sql for sql in core.backend.sql
    )
    assert sum("CREATE OR REPLACE VIEW" in sql for sql in core.backend.sql) == 1


def test_warn_mode_still_publishes_and_counts_the_warning(models_dir):
    core = FakeCore(
        {"semantic_certification_policy": "warn", "semantic_consumer_class": "agent_read"}
    )
    store = MappingStore({"gold.kpi_orders": "CERTIFIED"})
    engine = SemanticEngine(core, models_dir=models_dir, certification_store=store)

    report = engine.publish_metric_views()

    assert report.published == ("sales.orders", "sales.customers")
    assert report.refused == ()
    assert engine.certification_warning_count == 1


def test_dry_run_executes_nothing_but_the_report_carries_the_ddl(models_dir):
    core = FakeCore({"semantic_certification_policy": "off"})
    engine = SemanticEngine(core, models_dir=models_dir, certification_store=BombStore())

    report = engine.publish_metric_views(dry_run=True)

    assert core.backend.sql == []
    assert report.published == ("sales.orders", "sales.customers")
    assert sum("CREATE OR REPLACE VIEW" in ddl for ddl in report.ddl) == 2


def test_model_keys_restricts_which_models_are_considered(models_dir):
    core = FakeCore({"semantic_certification_policy": "off"})
    engine = SemanticEngine(core, models_dir=models_dir, certification_store=BombStore())

    report = engine.publish_metric_views(model_keys=["sales.orders"])

    assert report.published == ("sales.orders",)


def test_a_translation_refusal_does_not_block_the_other_models(tmp_path):
    root = tmp_path / "semantic_models"
    root.mkdir()
    unsupported = _model(
        "sales.orders", "gold.kpi_orders", relationships=[{"name": "r", "to": {}}]
    )
    _write_catalog(root, [unsupported, _model("sales.customers", "gold.kpi_customers")])
    core = FakeCore({"semantic_certification_policy": "off"})
    engine = SemanticEngine(core, models_dir=str(root), certification_store=BombStore())

    report = engine.publish_metric_views()

    assert report.published == ("sales.customers",)
    [refusal] = report.refused
    assert refusal["model"] == "sales.orders"
    assert "relationships" in refusal["reason"]
