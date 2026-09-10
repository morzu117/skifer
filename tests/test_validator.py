"""
Tests unitaires pour SemanticValidator.
"""
import pytest

from skifer.semantic.validator import SemanticValidator, ValidationResult


@pytest.fixture
def validator():
    return SemanticValidator()


@pytest.fixture
def valid_model():
    return {
        "name": "kpi_orders",
        "table": "gold.fact_orders",
        "layer": "gold",
        "description": "Métriques commandes",
        "dimensions": [
            {"name": "region", "sql": "region", "type": "string", "description": "Région"},
            {"name": "order_date", "sql": "order_date", "type": "date", "description": "Date"},
        ],
        "metrics": [
            {
                "name": "gross_revenue",
                "sql": "amount_ttc",
                "type": "sum",
                "description": "CA brut",
            },
        ],
    }


# ---------------------------------------------------------------------------
# Validation OK
# ---------------------------------------------------------------------------

def test_valid_model_passes(validator, valid_model):
    result = validator.validate(valid_model)
    assert result.ok
    assert result.errors == []


# ---------------------------------------------------------------------------
# Champs obligatoires
# ---------------------------------------------------------------------------

def test_missing_name(validator, valid_model):
    del valid_model["name"]
    result = validator.validate(valid_model)
    assert not result.ok
    assert any("name" in e for e in result.errors)


def test_missing_table(validator, valid_model):
    del valid_model["table"]
    result = validator.validate(valid_model)
    assert not result.ok
    assert any("table" in e for e in result.errors)


def test_empty_dimensions(validator, valid_model):
    valid_model["dimensions"] = []
    result = validator.validate(valid_model)
    assert not result.ok
    assert any("dimension" in e.lower() for e in result.errors)


def test_empty_metrics(validator, valid_model):
    valid_model["metrics"] = []
    result = validator.validate(valid_model)
    assert not result.ok
    assert any("métrique" in e.lower() for e in result.errors)


# ---------------------------------------------------------------------------
# Dimensions
# ---------------------------------------------------------------------------

def test_dimension_missing_sql(validator, valid_model):
    valid_model["dimensions"][0].pop("sql")
    result = validator.validate(valid_model)
    assert not result.ok
    assert any("sql" in e for e in result.errors)


def test_dimension_duplicate_name(validator, valid_model):
    valid_model["dimensions"].append(
        {"name": "region", "sql": "region2", "type": "string"}
    )
    result = validator.validate(valid_model)
    assert not result.ok
    assert any("doublon" in e for e in result.errors)


def test_dimension_nonstandard_type_is_warning(validator, valid_model):
    valid_model["dimensions"][0]["type"] = "custom_type"
    result = validator.validate(valid_model)
    # Le type non standard doit être un warning, pas une erreur bloquante
    assert result.ok or any("type" in w.lower() for w in result.warnings)


# ---------------------------------------------------------------------------
# Métriques
# ---------------------------------------------------------------------------

def test_metric_missing_type(validator, valid_model):
    valid_model["metrics"][0].pop("type")
    result = validator.validate(valid_model)
    assert not result.ok
    assert any("type" in e for e in result.errors)


def test_metric_invalid_type(validator, valid_model):
    valid_model["metrics"][0]["type"] = "median"
    result = validator.validate(valid_model)
    assert not result.ok
    assert any("median" in e for e in result.errors)


def test_metric_duplicate_name(validator, valid_model):
    valid_model["metrics"].append(
        {"name": "gross_revenue", "sql": "amount2", "type": "sum"}
    )
    result = validator.validate(valid_model)
    assert not result.ok
    assert any("doublon" in e for e in result.errors)


@pytest.mark.parametrize("additivity", ["sometimes", "semi"])
def test_metric_rejects_invalid_additivity(validator, valid_model, additivity):
    valid_model["metrics"][0]["additivity"] = additivity
    assert not validator.validate(valid_model).ok


def test_semi_additive_requires_declared_non_additive_dimensions(validator, valid_model):
    metric = valid_model["metrics"][0]
    metric["additivity"] = "semi_additive"
    metric["non_additive_dimensions"] = ["missing_date"]

    result = validator.validate(valid_model)

    assert not result.ok
    assert "dimension non additive inconnue 'missing_date'" in " ".join(result.errors)


def test_additive_rejects_non_additive_dimensions(validator, valid_model):
    valid_model["metrics"][0]["non_additive_dimensions"] = ["order_date"]

    result = validator.validate(valid_model)

    assert not result.ok
    assert "incompatible avec additivity 'additive'" in " ".join(result.errors)


def test_calendar_validation_accepts_versioned_periods(validator):
    result = validator.validate_calendar(
        {
            "key": "fiscal_fr",
            "version": "2024.1",
            "periods": [
                {"name": "FY2024_Q3", "start": "2024-10-01", "end": "2024-12-31"}
            ],
        }
    )

    assert result.ok


@pytest.mark.parametrize(
    ("field", "value"),
    [("key", "fiscal-fr"), ("period_name", "FY2024-Q3")],
)
def test_calendar_validation_rejects_unsafe_identifiers(validator, field, value):
    payload = {
        "key": "fiscal_fr",
        "version": "2024.1",
        "periods": [
            {"name": "FY2024_Q3", "start": "2024-10-01", "end": "2024-12-31"}
        ],
    }
    if field == "key":
        payload["key"] = value
    else:
        payload["periods"][0]["name"] = value

    assert not validator.validate_calendar(payload).ok


def test_calendar_validation_rejects_malformed_period_block(validator):
    result = validator.validate_calendar(
        {"key": "fiscal_fr", "version": "2024.1", "periods": ["Q3"]}
    )

    assert not result.ok
    assert "must be a mapping" in " ".join(result.errors)


def test_calendar_validation_rejects_duplicate_period_and_inverted_dates(validator):
    duplicate = validator.validate_calendar(
        {
            "key": "fiscal_fr",
            "version": "2024.1",
            "periods": [
                {"name": "Q3", "start": "2024-10-01", "end": "2024-12-31"},
                {"name": "Q3", "start": "2025-01-01", "end": "2025-03-31"},
            ],
        }
    )
    inverted = validator.validate_calendar(
        {
            "key": "fiscal_fr",
            "version": "2024.1",
            "periods": [
                {"name": "Q3", "start": "2024-12-31", "end": "2024-10-01"}
            ],
        }
    )

    assert not duplicate.ok
    assert not inverted.ok


# ---------------------------------------------------------------------------
# Warnings
# ---------------------------------------------------------------------------

def test_missing_description_is_warning(validator, valid_model):
    del valid_model["description"]
    result = validator.validate(valid_model)
    assert result.ok  # warning, pas erreur
    assert any("description" in w.lower() for w in result.warnings)


# ---------------------------------------------------------------------------
# validate_yaml (niveau fichier YAML)
# ---------------------------------------------------------------------------

def test_validate_yaml_no_models_key(validator):
    result = validator.validate_yaml({"something": "else"})
    assert not result.ok


def test_validate_yaml_empty_models(validator):
    result = validator.validate_yaml({"models": []})
    assert not result.ok


def test_validate_yaml_valid(validator, valid_model):
    result = validator.validate_yaml({"models": [valid_model]})
    assert result.ok


# ---------------------------------------------------------------------------
# ValidationResult
# ---------------------------------------------------------------------------

def test_validation_result_repr_ok():
    r = ValidationResult()
    assert "OK" in repr(r)


def test_validation_result_repr_ko():
    r = ValidationResult()
    r.add_error("Erreur test")
    assert "KO" in repr(r)
    assert not r.ok


def test_rejects_unsafe_dimension_and_metric_names(validator):
    # QueryResolver interpolates these names directly as SQL aliases
    # ("<expr> AS <name>"), so a non-identifier name is an injection vector.
    model = {
        "models": [
            {
                "name": "m",
                "table": "gold.t",
                "dimensions": [{"name": "c FROM x--", "sql": "c", "type": "string"}],
                "metrics": [{"name": "n; DROP TABLE t", "sql": "*", "type": "count"}],
            }
        ]
    }

    result = validator.validate_yaml(model)

    assert result.ok is False
    joined = " ".join(result.errors)
    assert "nom invalide" in joined
    assert "c FROM x--" in joined
    assert "n; DROP TABLE t" in joined


def test_domain_blocks_are_optional_and_backward_compatible(validator, valid_model):
    result = validator.validate_yaml({"models": [valid_model]})

    assert result.ok is True
    assert result.errors == []


def test_accepts_domain_blocks_with_composite_keys(validator, valid_model):
    valid_model["key"] = "orders"
    valid_model["grain"] = ["order"]
    valid_model["entities"] = [
        {"name": "order", "type": "primary", "key": ["order_id", "line_id"]},
        {"name": "customer", "type": "foreign", "key": "customer_id"},
    ]
    valid_model["relationships"] = [
        {
            "name": "orders_customer",
            "from_entity": "customer",
            "to_model": "customers",
            "to_entity": "customer",
            "cardinality": "many_to_one",
            "join_type": "left",
        }
    ]

    result = validator.validate_yaml({"models": [valid_model]})

    assert result.ok is True


def test_rejects_invalid_entity_identifier(validator, valid_model):
    valid_model["entities"] = [{"name": "customer; DROP", "type": "primary", "key": "customer_id"}]

    result = validator.validate_yaml({"models": [valid_model]})

    assert result.ok is False
    assert "customer; DROP" in " ".join(result.errors)


def test_rejects_invalid_entity_role(validator, valid_model):
    valid_model["entities"] = [{"name": "customer", "type": "bridge", "key": "customer_id"}]

    result = validator.validate_yaml({"models": [valid_model]})

    assert result.ok is False
    assert "invalid type" in " ".join(result.errors)


def test_rejects_missing_entity_key(validator, valid_model):
    valid_model["entities"] = [{"name": "customer", "type": "primary"}]

    result = validator.validate_yaml({"models": [valid_model]})

    assert result.ok is False
    assert "must declare a key column" in " ".join(result.errors)


def test_rejects_duplicate_columns_in_composite_key(validator, valid_model):
    valid_model["entities"] = [{"name": "customer", "type": "primary", "key": ["id", "id"]}]

    result = validator.validate_yaml({"models": [valid_model]})

    assert result.ok is False
    assert "composite key order is significant" in " ".join(result.errors)


def test_rejects_invalid_key_column_identifier(validator, valid_model):
    valid_model["entities"] = [{"name": "customer", "type": "primary", "key": ["id", "x OR 1=1"]}]

    result = validator.validate_yaml({"models": [valid_model]})

    assert result.ok is False
    assert "key column 'x OR 1=1' is invalid" in " ".join(result.errors)


def test_rejects_grain_reference_to_unknown_entity(validator, valid_model):
    valid_model["grain"] = ["order"]
    valid_model["entities"] = [{"name": "customer", "type": "primary", "key": "customer_id"}]

    result = validator.validate_yaml({"models": [valid_model]})

    assert result.ok is False
    assert "is not declared in entities" in " ".join(result.errors)


def test_rejects_relationship_with_unknown_local_entity(validator, valid_model):
    valid_model["key"] = "orders"
    valid_model["entities"] = [{"name": "customer", "type": "foreign", "key": "customer_id"}]
    valid_model["relationships"] = [
        {
            "name": "orders_region",
            "from_entity": "region",
            "to_model": "regions",
            "to_entity": "region",
            "cardinality": "many_to_one",
            "join_type": "left",
        }
    ]

    result = validator.validate_yaml({"models": [valid_model]})

    assert result.ok is False
    assert "unknown local entity 'region'" in " ".join(result.errors)


def test_rejects_self_relation(validator, valid_model):
    valid_model["key"] = "orders"
    valid_model["entities"] = [{"name": "order", "type": "primary", "key": "order_id"}]
    valid_model["relationships"] = [
        {
            "name": "orders_order",
            "from_entity": "order",
            "to_model": "orders",
            "to_entity": "order",
            "cardinality": "one_to_one",
            "join_type": "inner",
        }
    ]

    result = validator.validate_yaml({"models": [valid_model]})

    assert result.ok is False
    assert "self relation" in " ".join(result.errors)


def test_rejects_unknown_relationship_cardinality_with_mandatory_message(validator, valid_model):
    valid_model["key"] = "orders"
    valid_model["entities"] = [{"name": "customer", "type": "foreign", "key": "customer_id"}]
    valid_model["relationships"] = [
        {
            "name": "orders_customer",
            "from_entity": "customer",
            "to_model": "customers",
            "to_entity": "customer",
            "cardinality": "unknown",
            "join_type": "left",
        }
    ]

    result = validator.validate_yaml({"models": [valid_model]})

    assert result.ok is False
    assert (
        "Relationship cardinality is unknown; declare or certify uniqueness before querying."
        in " ".join(result.errors)
    )


def test_rejects_invalid_relationship_cardinality_value(validator, valid_model):
    valid_model["key"] = "orders"
    valid_model["entities"] = [{"name": "customer", "type": "foreign", "key": "customer_id"}]
    valid_model["relationships"] = [
        {
            "name": "orders_customer",
            "from_entity": "customer",
            "to_model": "customers",
            "to_entity": "customer",
            "cardinality": "fanout",
            "join_type": "left",
        }
    ]

    result = validator.validate_yaml({"models": [valid_model]})

    assert result.ok is False
    assert "invalid cardinality 'fanout'" in " ".join(result.errors)


def test_rejects_invalid_relationship_join_type(validator, valid_model):
    valid_model["key"] = "orders"
    valid_model["entities"] = [{"name": "customer", "type": "foreign", "key": "customer_id"}]
    valid_model["relationships"] = [
        {
            "name": "orders_customer",
            "from_entity": "customer",
            "to_model": "customers",
            "to_entity": "customer",
            "cardinality": "many_to_one",
            "join_type": "full",
        }
    ]

    result = validator.validate_yaml({"models": [valid_model]})

    assert result.ok is False
    assert "supports only ['inner', 'left']" in " ".join(result.errors)


def test_rejects_invalid_related_model_identifier(validator, valid_model):
    valid_model["key"] = "orders"
    valid_model["entities"] = [{"name": "customer", "type": "foreign", "key": "customer_id"}]
    valid_model["relationships"] = [
        {
            "name": "orders_customer",
            "from_entity": "customer",
            "to_model": "customers; DROP",
            "to_entity": "customer",
            "cardinality": "many_to_one",
            "join_type": "left",
        }
    ]

    result = validator.validate_yaml({"models": [valid_model]})

    assert result.ok is False
    assert "to_model 'customers; DROP' is invalid" in " ".join(result.errors)


def test_rejects_invalid_related_entity_identifier(validator, valid_model):
    valid_model["key"] = "orders"
    valid_model["entities"] = [{"name": "customer", "type": "foreign", "key": "customer_id"}]
    valid_model["relationships"] = [
        {
            "name": "orders_customer",
            "from_entity": "customer",
            "to_model": "customers",
            "to_entity": "customer-id",
            "cardinality": "many_to_one",
            "join_type": "left",
        }
    ]

    result = validator.validate_yaml({"models": [valid_model]})

    assert result.ok is False
    assert "to_entity 'customer-id' is invalid" in " ".join(result.errors)


def test_rejects_relationship_target_entity_when_target_model_is_available(validator, valid_model):
    target_model = {
        "name": "customers",
        "key": "customers",
        "table": "gold.dim_customers",
        "dimensions": [{"name": "segment", "sql": "segment", "type": "string"}],
        "metrics": [{"name": "row_count", "sql": "*", "type": "count"}],
        "entities": [{"name": "customer", "type": "primary", "key": "customer_id"}],
    }
    valid_model["key"] = "orders"
    valid_model["entities"] = [{"name": "customer", "type": "foreign", "key": "customer_id"}]
    valid_model["relationships"] = [
        {
            "name": "orders_customer",
            "from_entity": "customer",
            "to_model": "customers",
            "to_entity": "account",
            "cardinality": "many_to_one",
            "join_type": "left",
        }
    ]

    result = validator.validate_yaml({"models": [valid_model, target_model]})

    assert result.ok is False
    assert "unknown entity 'account' in model 'customers'" in " ".join(result.errors)


def _domain_model(**extra):
    return {
        "models": [
            {
                "name": "orders",
                "key": "orders",
                "table": "gold.orders",
                "dimensions": [{"name": "region", "sql": "region", "type": "string"}],
                "metrics": [{"name": "revenue", "sql": "amount", "type": "sum"}],
                **extra,
            }
        ]
    }


def test_rejects_entities_declared_as_bare_names(validator):
    # The catalog summary stores `entities: ["order"]`; a model must declare
    # mappings. The parse layer skips non-mappings, so this used to validate
    # green with zero entities instead of failing.
    result = validator.validate_yaml(_domain_model(entities=["order"]))

    assert result.ok is False
    assert "must be a mapping" in " ".join(result.errors)


def test_rejects_domain_block_that_is_not_a_list(validator):
    result = validator.validate_yaml(_domain_model(entities={"name": "order"}))

    assert result.ok is False
    assert "must be a list of mappings" in " ".join(result.errors)


def test_rejects_relationships_declared_as_bare_names(validator):
    result = validator.validate_yaml(_domain_model(relationships=["orders_customer"]))

    assert result.ok is False
    assert "must be a mapping" in " ".join(result.errors)


def test_model_without_any_domain_block_still_validates(validator):
    assert validator.validate_yaml(_domain_model()).ok is True
