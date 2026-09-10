"""
Tests unitaires pour SemanticBuilder — génération YAML + catalogue.
"""
import os
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import yaml

from skifer.core.ir import parse_to_ir
from skifer.core.schema_loader import parse_schema
from skifer.semantic.draft_builder import SemanticDraftBuilder
from skifer.semantic.builder import SemanticBuilder
from skifer.semantic.output_projection import OutputProjector
from skifer.semantic.semantic import SemanticEngine
from skifer.semantic.validator import SemanticValidator


# ---------------------------------------------------------------------------
# Fixture — YAML valide retourné par le LLM
# ---------------------------------------------------------------------------

VALID_YAML_RESPONSE = """models:
  - name: kpi_test
    key: kpi_test_default
    description: Modèle de test
    layer: gold
    table: gold.fact_test
    base_filter: "source = 'TEST'"
    tags: [test, gold]
    dimensions:
      - name: region
        sql: region
        type: string
        description: Région géographique
      - name: order_date
        sql: order_date
        type: date
        description: Date de commande
    metrics:
      - name: revenue
        sql: amount
        type: sum
        description: Chiffre d'affaires
"""

INVALID_YAML_RESPONSE = "Ce n'est pas du YAML valide : {{"


# ---------------------------------------------------------------------------
# SemanticBuilder._parse_and_validate_yaml
# ---------------------------------------------------------------------------

@pytest.fixture
def builder(tmp_path):
    mock_llm = MagicMock()
    return SemanticBuilder(
        llm_provider=mock_llm,
        output_dir=str(tmp_path / "semantic_models"),
    )


def test_parse_valid_yaml_returns_content(builder):
    content, error = builder._parse_and_validate_yaml(VALID_YAML_RESPONSE, "kpi_test_default")
    assert content is not None
    assert error is None
    assert "models" in content
    assert content["models"][0]["name"] == "kpi_test"


def test_parse_invalid_yaml_returns_error(builder):
    content, error = builder._parse_and_validate_yaml(INVALID_YAML_RESPONSE, "kpi_test_default")
    assert content is None
    assert error is not None


def test_parse_yaml_missing_models_key(builder):
    content, error = builder._parse_and_validate_yaml("name: test", "kpi_test_default")
    assert content is None
    assert "models" in error


def test_parse_yaml_missing_dimensions(builder):
    yaml_no_dims = """models:
  - name: kpi_test
    table: gold.fact_test
    dimensions: []
    metrics:
      - name: revenue
        sql: amount
        type: sum
"""
    content, error = builder._parse_and_validate_yaml(yaml_no_dims, "kpi_test_default")
    assert content is None
    assert "dimension" in error.lower()


def test_parse_yaml_strips_code_blocks(builder):
    """Le builder doit tolérer les blocs ```yaml``` du LLM."""
    yaml_with_block = f"```yaml\n{VALID_YAML_RESPONSE}\n```"
    content, error = builder._parse_and_validate_yaml(yaml_with_block, "kpi_test_default")
    assert content is not None
    assert error is None


# ---------------------------------------------------------------------------
# SemanticBuilder.build — happy path
# ---------------------------------------------------------------------------

def test_build_writes_yaml_and_updates_catalog(tmp_path):
    """build() doit écrire le YAML à plat et mettre à jour semantic_catalog.yaml."""
    mock_llm = MagicMock()
    mock_llm.complete.return_value = VALID_YAML_RESPONSE

    models_dir = str(tmp_path / "semantic_models")
    builder = SemanticBuilder(llm_provider=mock_llm, output_dir=models_dir)

    yaml_path = builder.build(
        model_key="kpi_test_default",
        table="gold.fact_test",
        layer="gold",
        description="Modèle de test",
        tags=["custom_tag"],
    )

    # Fichier YAML créé à plat (pas dans un sous-dossier)
    assert os.path.exists(yaml_path)
    assert yaml_path.endswith("kpi_test_default.yaml")
    assert "kpi_test_default" in os.path.basename(yaml_path)

    with open(yaml_path) as f:
        content = yaml.safe_load(f)
    assert content["models"][0]["name"] == "kpi_test"

    # Catalogue mis à jour
    catalog_path = os.path.join(models_dir, "semantic_catalog.yaml")
    assert os.path.exists(catalog_path)

    with open(catalog_path) as f:
        catalog = yaml.safe_load(f)
    assert catalog["_total_models"] == 1
    assert catalog["models"][0]["key"] == "kpi_test_default"
    assert catalog["models"][0]["file"] == "kpi_test_default.yaml"
    assert "custom_tag" in catalog["models"][0]["tags"]


def test_build_retries_on_invalid_yaml(tmp_path):
    """build() doit réessayer jusqu'à MAX_VALIDATION_LOOPS si le YAML est invalide."""
    mock_llm = MagicMock()
    mock_llm.complete.side_effect = [INVALID_YAML_RESPONSE, VALID_YAML_RESPONSE]

    builder = SemanticBuilder(
        llm_provider=mock_llm,
        output_dir=str(tmp_path / "sm"),
    )
    yaml_path = builder.build(
        model_key="kpi_test_default",
        table="gold.fact_test",
        layer="gold",
    )
    assert os.path.exists(yaml_path)
    assert mock_llm.complete.call_count == 2


def test_build_raises_after_max_retries(tmp_path):
    """Après MAX_VALIDATION_LOOPS tentatives KO, build() lève RuntimeError."""
    mock_llm = MagicMock()
    mock_llm.complete.return_value = INVALID_YAML_RESPONSE

    builder = SemanticBuilder(
        llm_provider=mock_llm,
        output_dir=str(tmp_path / "sm"),
    )
    with pytest.raises(RuntimeError, match="Génération KO"):
        builder.build(
            model_key="kpi_fail_default",
            table="gold.fact_fail",
            layer="gold",
        )

    errors_dir = os.path.join(str(tmp_path / "sm"), ".errors")
    assert os.path.exists(errors_dir)
    assert len(os.listdir(errors_dir)) == 1


# ---------------------------------------------------------------------------
# SemanticBuilder._build_catalog_entry
# ---------------------------------------------------------------------------

def test_build_catalog_entry_includes_auto_tags(tmp_path):
    mock_llm = MagicMock()
    builder = SemanticBuilder(llm_provider=mock_llm, output_dir=str(tmp_path))

    yaml_content = yaml.safe_load(VALID_YAML_RESPONSE)
    entry = builder._build_catalog_entry(
        model_key="kpi_test_default",
        yaml_content=yaml_content,
        tags=["custom"],
    )

    assert entry["key"] == "kpi_test_default"
    assert entry["file"] == "kpi_test_default.yaml"
    assert "dimensions" in entry
    assert "metrics" in entry
    # Tags auto : layer + model_key
    assert "gold" in entry["tags"]
    assert "kpi_test_default" in entry["tags"]
    assert "custom" in entry["tags"]


def test_build_flat_file_no_subdirectory(tmp_path):
    """Le fichier YAML doit être à plat dans output_dir, sans sous-dossier."""
    mock_llm = MagicMock()
    mock_llm.complete.return_value = VALID_YAML_RESPONSE

    models_dir = str(tmp_path / "semantic_models")
    builder = SemanticBuilder(llm_provider=mock_llm, output_dir=models_dir)
    yaml_path = builder.build(
        model_key="orders_erp",
        table="gold.fact_orders",
        layer="gold",
    )

    # Le fichier est directement dans models_dir, pas dans un sous-dossier
    assert os.path.dirname(yaml_path) == models_dir
    assert os.path.basename(yaml_path) == "orders_erp.yaml"


def _parse_ir(yaml_text: str):
    return parse_to_ir(parse_schema(yaml_text))


def _managed_draft(tmp_path, yaml_text: str):
    schema = _parse_ir(yaml_text)
    projected = OutputProjector().project(schema)
    draft_builder = SemanticDraftBuilder(output_dir=str(tmp_path / "semantic_models"))
    payload = draft_builder.build_draft(projected, schema)
    return schema, projected, payload


BASE_PROJECTED_YAML = """
data_product:
  id: sales.orders
  version: 1.0.0
  description: Orders semantic draft
contract:
  grain: [country, order_day]
  output:
    country: {logical_type: string, description: Country}
    order_day: {logical_type: date, description: Order day}
    orders: {description: Distinct orders}
    total_amount: {description: Total amount}
semantic:
  model_key: orders_summary
  entity: order
  default_time_dimension: order_day
tables: [{name: silver.orders}]
aggregate:
  group_by: [country, order_day]
  measures:
    - [order_id, orders, count_distinct]
    - [amount, total_amount, sum]
sink: {type: delta, schema: gold, table: fact_orders}
"""


def test_projection_enrichment_rejects_unknown_dimension_and_keeps_draft(tmp_path):
    schema, projected, managed_draft = _managed_draft(tmp_path, BASE_PROJECTED_YAML)
    llm_response = """models:
  - dimensions:
      - name: invented_dimension
        sql: invented_dimension
        type: string
        description: Invented
"""
    mock_llm = MagicMock()
    mock_llm.complete.return_value = llm_response
    builder = SemanticBuilder(llm_provider=mock_llm, output_dir=str(tmp_path / "semantic_models"))

    path = builder.build_from_projection(
        projected=projected,
        schema=schema,
        managed_draft=managed_draft,
    )

    with open(path, encoding="utf-8") as handle:
        payload = yaml.safe_load(handle)
    assert payload == managed_draft


def test_projection_enrichment_rejects_managed_field_sql_or_type_changes(tmp_path):
    schema, projected, managed_draft = _managed_draft(tmp_path, BASE_PROJECTED_YAML)
    llm_response = """models:
  - dimensions:
      - name: country
        sql: order_day
        type: date
        description: Mutated country
    metrics:
      - name: total_amount
        sql: orders
        type: count
        description: Mutated metric
"""
    mock_llm = MagicMock()
    mock_llm.complete.return_value = llm_response
    builder = SemanticBuilder(llm_provider=mock_llm, output_dir=str(tmp_path / "semantic_models"))

    path = builder.build_from_projection(
        projected=projected,
        schema=schema,
        managed_draft=managed_draft,
    )

    with open(path, encoding="utf-8") as handle:
        payload = yaml.safe_load(handle)
    model = payload["models"][0]
    original_model = managed_draft["models"][0]
    assert model["dimensions"] == original_model["dimensions"]
    assert model["metrics"] == original_model["metrics"]


def test_projection_enrichment_rejects_metric_with_unknown_dependency(tmp_path):
    schema, projected, managed_draft = _managed_draft(tmp_path, BASE_PROJECTED_YAML)
    llm_response = """models:
  - metrics:
      - name: invalid_margin
        sql: margin_amount
        type: sum
        description: Missing dependency
"""
    mock_llm = MagicMock()
    mock_llm.complete.return_value = llm_response
    builder = SemanticBuilder(llm_provider=mock_llm, output_dir=str(tmp_path / "semantic_models"))

    path = builder.build_from_projection(
        projected=projected,
        schema=schema,
        managed_draft=managed_draft,
    )

    with open(path, encoding="utf-8") as handle:
        payload = yaml.safe_load(handle)
    assert payload == managed_draft


def test_projection_enrichment_accepts_descriptions_synonyms_and_valid_metric(tmp_path):
    schema, projected, managed_draft = _managed_draft(tmp_path, BASE_PROJECTED_YAML)
    llm_response = """models:
  - description: Curated orders summary
    synonyms: [orders overview]
    dimensions:
      - name: country
        description: Billing country
        synonyms: [market, territory]
    metrics:
      - name: total_amount
        description: Sum of booked amount
        synonyms: [gross sales]
      - name: order_rows
        sql: orders
        type: max
        description: Re-exposed distinct order count output
        synonyms: [rows proxy]
"""
    mock_llm = MagicMock()
    mock_llm.complete.return_value = llm_response
    builder = SemanticBuilder(llm_provider=mock_llm, output_dir=str(tmp_path / "semantic_models"))

    path = builder.build_from_projection(
        projected=projected,
        schema=schema,
        managed_draft=managed_draft,
    )

    with open(path, encoding="utf-8") as handle:
        payload = yaml.safe_load(handle)
    model = payload["models"][0]
    assert model["description"] == "Curated orders summary"
    assert model["synonyms"] == ["orders overview"]
    assert model["dimensions"][0]["description"] == "Billing country"
    assert model["dimensions"][0]["synonyms"] == ["market", "territory"]
    total_amount = next(metric for metric in model["metrics"] if metric["name"] == "total_amount")
    assert total_amount["sql"] == "amount"
    assert total_amount["type"] == "sum"
    assert total_amount["description"] == "Sum of booked amount"
    assert total_amount["synonyms"] == ["gross sales"]
    order_rows = next(metric for metric in model["metrics"] if metric["name"] == "order_rows")
    assert order_rows == {
        "name": "order_rows",
        "sql": "orders",
        "type": "max",
        "description": "Re-exposed distinct order count output",
        "synonyms": ["rows proxy"],
    }

    validator = SemanticValidator()
    assert validator.validate_yaml(payload).ok is True
    assert validator.validate_against_projection(
        payload,
        projected,
        contract_output=[field.name for field in schema.contract_output],
    ).ok is True


def test_projection_enrichment_e2e_pipeline_to_lazy_catalog_validation(tmp_path):
    schema, projected, managed_draft = _managed_draft(tmp_path, BASE_PROJECTED_YAML)
    draft_builder = SemanticDraftBuilder(output_dir=str(tmp_path / "semantic_models"))
    draft_path = draft_builder.write_draft(projected, schema)
    assert os.path.exists(draft_path)

    llm_response = """models:
  - description: Published curated orders summary
    dimensions:
      - name: country
        description: Selling country
    metrics:
      - name: total_amount
        description: Total booked revenue
      - name: total_amount_shadow
        sql: total_amount
        type: max
        description: Semantic alias on projected metric output
"""
    mock_llm = MagicMock()
    mock_llm.complete.return_value = llm_response
    builder = SemanticBuilder(llm_provider=mock_llm, output_dir=str(tmp_path / "semantic_models"))
    promoted_path = builder.build_from_projection(
        projected=projected,
        schema=schema,
        managed_draft=managed_draft,
    )

    with open(promoted_path, encoding="utf-8") as handle:
        promoted_payload = yaml.safe_load(handle)
    validator = SemanticValidator()
    assert validator.validate_yaml(promoted_payload).ok is True
    assert validator.validate_against_projection(
        promoted_payload,
        projected,
        contract_output=[field.name for field in schema.contract_output],
    ).ok is True

    engine = SemanticEngine(core_engine=SimpleNamespace(db="local", env="DEV", config={}), models_dir=str(tmp_path / "semantic_models"))
    assert "orders_summary" in engine._catalog
    assert engine._cache == {}
    summary = engine.get_model_summary("orders_summary")
    assert summary["file"] == "orders_summary.yaml"
    assert engine._cache == {}
    llm_context = engine.get_llm_context("orders_summary")
    assert "Published curated orders summary" in llm_context
    assert "total_amount_shadow" in llm_context
    assert "orders_summary" in engine._cache


@pytest.mark.parametrize(
    "evil_name",
    [
        "n FROM secrets; DROP TABLE audit--",
        "revenue, (SELECT password FROM users)",
        "1_starts_with_digit",
        "has-a-dash",
        "quoted`name`",
    ],
)
def test_llm_authored_metric_name_cannot_inject_sql(evil_name):
    # QueryResolver interpolates the metric name straight into "<expr> AS <name>",
    # so an LLM-authored name that is not a plain identifier is an injection
    # vector. It must be rejected at the LLM trust boundary.
    builder = SemanticBuilder.__new__(SemanticBuilder)
    candidate = {"name": evil_name, "sql": "*", "type": "count"}

    assert builder._sanitize_new_metric(candidate, {"country"}) is None


def test_llm_authored_metric_type_must_be_a_supported_aggregate():
    builder = SemanticBuilder.__new__(SemanticBuilder)
    candidate = {"name": "nb", "sql": "country", "type": "stddev"}

    assert builder._sanitize_new_metric(candidate, {"country"}) is None


def test_plain_identifier_metric_is_still_accepted():
    builder = SemanticBuilder.__new__(SemanticBuilder)
    candidate = {"name": "nb_orders", "sql": "*", "type": "count"}

    assert builder._sanitize_new_metric(candidate, {"country"}) == {
        "name": "nb_orders",
        "sql": "*",
        "type": "count",
    }
