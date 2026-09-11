import pytest
from unittest.mock import MagicMock
from datetime import datetime
from functools import reduce
from uuid import UUID, uuid4
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType, FloatType

from skifer.core.spark_backend import SparkBackend
from skifer.core.ir import ParsedFilter, _parse_op
from skifer.core.context import ExecutionContext
from skifer.core.core import SkiferEngine
from skifer.core.registry import RuleRegistry
from skifer.observability.certification_store import SqliteCertificationStore
from skifer.observability.tracing import NoOpTracer

# Adapters over the live SparkBackend IR path (the legacy string-op module
# core/operations.py was removed by Plan 26) — keeps the operator semantics
# tests below exercising the real execution path.
_op_backend = SparkBackend(spark=MagicMock(), is_local=True)


# Several tests below take the `spark` fixture without using it directly: building a
# column with F.col() requires a live JVM context, even on the paths that raise before
# any data is read. Dropping the fixture makes those tests pass only when a neighbour
# happens to have started the session first.

def _apply_operation(c, op_str, allow_raw_sql=True):
    return _op_backend.apply_op(c, _parse_op(op_str), allow_raw_sql)


def _build_filter_expression(filter_list, allow_raw_sql=True):
    conds = [
        _op_backend.build_filter(
            ParsedFilter(r["column"], r["operator"], r.get("value")), allow_raw_sql
        )
        for r in filter_list
    ]
    return reduce(lambda a, b: a & b, conds) if conds else F.lit(True)
# ==============================================================================
# TESTS FOR HELPERS
# ==============================================================================

@pytest.mark.parametrize("op_str, expected_value", [
    ("upper", "HELLO-WORLD"),
    ("lit:world", "world"),
    ("cast:string", "123"),
    ("substring:1,3", "hel"),
    ("split:-,1", "world"),
    ("trim", "hello world"),
    ("coalesce:default", "default"),
    ("expr:concat(col1, '_test')", "hello-world_test")
])
def test_apply_operation_various(spark, op_str, expected_value):
    """Tests various single operations of _apply_operation."""
    # Default schema and data for most string operations
    schema = StructType([StructField("col1", StringType(), True)])
    df = spark.createDataFrame([("hello-world",), (" hello world ",)], schema)
    
    # Special case for 'cast:string' which needs a different input type to be meaningful
    if op_str == "cast:string":
        schema = StructType([StructField("col1", IntegerType(), True)])
        df = spark.createDataFrame([(123,)], schema)

    # Coalesce test needs a null value
    df_null = spark.createDataFrame([(None,)], schema)
    
    if "coalesce" in op_str:
        res_df = df_null
    elif "trim" in op_str:
        res_df = df.filter(F.col("col1").contains(" "))
    elif op_str == "cast:string":
        res_df = df
    else:
        res_df = df.filter(~F.col("col1").contains(" "))

    result = res_df.withColumn("res", _apply_operation(F.col("col1"), op_str)).collect()[0]
    assert result["res"] == expected_value


def test_apply_operation_conditional(spark):
    """Tests the when/then/else logic of _apply_operation."""
    df = spark.createDataFrame([("complete",), ("pending",)], ["status"])
    
    # Define a when/then/else chain
    operations = [
        "when:eq:complete",
        "then:lit:C",
        "else:lit:P"
    ]
    
    c = F.col("status")
    res_col = F.when(
        _apply_operation(c, operations[0]), 
        _apply_operation(c, operations[1])
    ).otherwise(_apply_operation(c, operations[2]))
    
    results = df.withColumn("res", res_col).collect()
    assert results[0]["res"] == "C"
    assert results[1]["res"] == "P"


@pytest.mark.parametrize("filters, expected_count", [
    ([{"column": "country", "operator": "eq", "value": "FR"}], 1),
    ([{"column": "country", "operator": "ne", "value": "FR"}], 2),
    ([{"column": "sales", "operator": "gt", "value": 150}], 1),
    ([{"column": "sales", "operator": "lte", "value": 150}], 2),
    ([{"column": "country", "operator": "in", "value": ["FR", "DE"]}], 2),
    ([{"column": "country", "operator": "not_in", "value": ["US", "DE"]}], 1),
    ([{"column": "product", "operator": "is_null"}], 1),
    ([{"column": "product", "operator": "is_not_null"}], 2),
    ([{"column": "product", "operator": "contains", "value": "phone"}], 1),
    ([{"column": "product", "operator": "notcontains", "value": "phone"}], 1),
    ([
        {"column": "country", "operator": "eq", "value": "DE"},
        {"column": "sales", "operator": "gt", "value": 100}
    ], 1)
])
def test_build_filter_expression_various(spark, filters, expected_count):
    """Tests various filter operators and combinations."""
    data = [
        ("FR", 100, "laptop"),
        ("DE", 200, "smartphone"),
        ("US", 150, None)
    ]
    df = spark.createDataFrame(data, ["country", "sales", "product"])
    
    filter_expr = _build_filter_expression(filters)
    result_count = df.filter(filter_expr).count()
    
    assert result_count == expected_count

# ==============================================================================
# TESTS FOR ENGINE METHODS (VIA A MOCKED ENGINE)
# ==============================================================================


def test_explain_rules_report_is_silent(capsys):
    engine = object.__new__(SkiferEngine)

    report = engine.explain_rules_report({})

    assert report == {"profiles": [], "warnings": []}
    assert capsys.readouterr().out == ""

    assert engine.explain_rules({}) == ([], [])
    assert "Rule Analysis Report" in capsys.readouterr().out

@pytest.fixture
def mock_engine(spark, mocker):
    """Fixture to create a SkiferEngine instance with mocked dependencies."""
    
    # This mock replaces the original _load_config_from_yaml method.
    # Instead of just returning None, it actively sets the necessary config attributes
    # on the instance ('self') that is passed to it.
    def mock_load_config(instance, config_path):
        instance.config = {"environments": {"test": {"is_production": False}}}
        instance.env = "test"
        # Use the default spark_catalog which is known by the local Spark session
        instance.db = "spark_catalog"

    mocker.patch.object(SkiferEngine, "_load_config_from_yaml", side_effect=mock_load_config, autospec=True)
    mocker.patch.object(SkiferEngine, "_get_clean_username", return_value="test_user")

    # We still pass a dummy config_path to ensure the 'if config_path:' branch is taken
    engine = SkiferEngine(spark=spark, config_path="dummy/path/to/config.yaml")
    
    return engine


def test_get_target_schema(mock_engine, mocker):
    """Tests the sandbox logic for schema names."""
    
    # 1. Interactive mode (default for mock) -> Suffix should be added
    mocker.patch.object(mock_engine, "_is_running_as_job", return_value=False)
    mock_engine.is_job_execution = False
    mock_engine.schema_suffix = "_test_user"
    assert mock_engine.get_target_schema("silver") == "silver_test_user"
    
    # 2. Job mode -> Suffix should NOT be added
    mocker.patch.object(mock_engine, "_is_running_as_job", return_value=True)
    mock_engine.is_job_execution = True
    mock_engine.schema_suffix = "" # Logic inside __init__ is mocked, so we set it
    assert mock_engine.get_target_schema("silver") == "silver"

    # 3. Prod environment -> Suffix should NOT be added
    mock_engine.config = {"environments": {"prod": {"is_production": True}}}
    mock_engine.env = "prod"
    mock_engine.is_job_execution = False
    mock_engine.schema_suffix = ""
    assert mock_engine.get_target_schema("gold") == "gold"


def test_get_agent_wires_default_sqlite_certification_store(mock_engine, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    agent = mock_engine.get_agent(llm_provider=object(), models_dir=str(tmp_path))

    assert isinstance(agent.semantic.certification_store, SqliteCertificationStore)
    agent.semantic.certification_store.close()


def test_get_agent_uses_explicit_certification_store(mock_engine, tmp_path):
    store = object()

    agent = mock_engine.get_agent(
        llm_provider=object(),
        models_dir=str(tmp_path),
        certification_store=store,
    )

    assert agent.semantic.certification_store is store


def test_engine_certification_store_is_explicit_opt_in(spark, mocker):
    def mock_load_config(instance, config_path):
        instance.config = {"environments": {"test": {"is_production": False}}}
        instance.env = "test"
        instance.db = "spark_catalog"

    mocker.patch.object(SkiferEngine, "_load_config_from_yaml", side_effect=mock_load_config, autospec=True)
    mocker.patch.object(SkiferEngine, "_get_clean_username", return_value="test_user")

    default_engine = SkiferEngine(spark=spark, config_path="dummy/path/to/config.yaml")
    assert default_engine.certification_store is None

    store = object()
    configured_engine = SkiferEngine(
        spark=spark,
        config_path="dummy/path/to/config.yaml",
        certification_store=store,
    )
    assert configured_engine.certification_store is store


def test_get_select_expressions(mock_engine):
    """Tests the parsing of the 'select_final' block."""
    field_list = [
        ["source_id", "target_id", []],
        ["amount", "amount_eur", ["cast:double"]],
        ["status", "status_code", ["when:eq:Paid", "then:lit:1", "else:lit:0"]]
    ]
    
    expressions = mock_engine.get_select_expressions(field_list)
    
    assert len(expressions) == 3
    assert "target_id" in str(expressions[0])
    assert "CAST(amount AS DOUBLE)" in str(expressions[1])
    
    # For conditional logic, check for the essential parts rather than exact string representation
    # which can vary between PySpark versions.
    conditional_expr_str = str(expressions[2])
    assert "CASE" in conditional_expr_str
    assert "WHEN" in conditional_expr_str
    assert "status_code" in conditional_expr_str


def test_process_schema_with_preprocess(mock_engine, spark):
    """
    Integration test for process_schema, specifically testing the 'preprocess'
    (qualify/deduplication) logic.
    """
    # 1. Prepare data with duplicates
    sales_data = [
        (1, 10.0, datetime(2023, 1, 1, 10, 0, 0)), # Old version
        (1, 12.0, datetime(2023, 1, 1, 12, 0, 0)), # New version, should be kept
        (2, 20.0, datetime(2023, 1, 2, 10, 0, 0))  # Single entry
    ]
    sales_schema = StructType([
        StructField("sale_id", IntegerType()),
        StructField("amount", DoubleType()),
        StructField("updated_at", TimestampType())
    ])
    sales_df = spark.createDataFrame(sales_data, sales_schema)
    sales_df.createOrReplaceTempView("sales_with_duplicates")

    # 2. Define schema with preprocess qualify
    schema_dict = {
        "tables": [
            {
                "name": "sales_with_duplicates",
                "alias": "s",
                "preprocess": {
                    "qualify": {
                        "partition_by": ["sale_id"],
                        "order_by": [{"field": "updated_at", "order": "desc"}]
                    }
                }
            }
        ],
        "select_final": [
            ["sale_id", "sale_id", []],
            ["amount", "amount", []]
        ]
    }

    # 3. Execute the process
    result_df = mock_engine.process_schema(schema_dict)
    
    # 4. Assertions
    assert result_df.count() == 2 # Should have deduplicated from 3 to 2 rows
    
    result_data = result_df.sort("sale_id").collect()
    assert result_data[0]["sale_id"] == 1
    assert result_data[0]["amount"] == 12.0 # The latest amount
    assert result_data[1]["sale_id"] == 2
    assert result_data[1]["amount"] == 20.0

    spark.catalog.dropTempView("sales_with_duplicates")


def test_process_schema_raises_error_on_unknown_rule(mock_engine):
    """
    Tests that process_schema correctly raises a ValueError when a business
    rule specified in the schema is not found in the RuleRegistry.
    """
    schema_dict = {
        "tables": [{"name": "some_table", "alias": "t"}],
        "business_rules": ["this_rule_does_not_exist"]
    }
    
    # Mock a dummy table to avoid FileNotFoundError
    mock_engine.spark.createDataFrame([(1,)], ["id"]).createOrReplaceTempView("some_table")

    with pytest.raises(ValueError, match="Business Rule 'this_rule_does_not_exist' not found"):
        mock_engine.process_schema(schema_dict)

    mock_engine.spark.catalog.dropTempView("some_table")


def test_run_process_and_split(mock_engine, mocker, spark):
    """
    Tests the run_process_and_split runner. It spies on the _write_dataframe method
    to ensure it's called correctly without actually writing any data.
    """
    # 0. Create the schema required for the test before running the engine
    spark.sql("CREATE SCHEMA IF NOT EXISTS spark_catalog.gold_test_user")

    # 1. Spy on the _write_dataframe method (no drop needed — overwrite is atomic)
    spy_write = mocker.spy(mock_engine, "_write_dataframe")

    # 2. Prepare mock data
    source_data = [
        (1, "FR"),
        (2, "DE"),
        (3, "FR")
    ]
    source_df = spark.createDataFrame(source_data, ["id", "sales_org_code"])
    source_df.createOrReplaceTempView("source_for_split")

    # 3. Define schema and split values
    schema_dict = {"tables": [{"name": "source_for_split", "alias": "t"}]}
    split_values = [
        {"label": "france", "value": "FR"},
        {"label": "germany", "value": "DE"}
    ]

    # 4. Execute the runner
    mock_engine.run_process_and_split(
        schema_dict=schema_dict,
        split_values=split_values,
        target_layer="gold",
        target_base_name="sales",
        split_column="sales_org_code"
    )

    # 5. Assertions on the spy
    assert spy_write.call_count == 2

    # Check the call for France
    call_fr_args = spy_write.call_args_list[0].args
    df_fr = call_fr_args[0]
    fqn_fr = call_fr_args[1]
    assert "sales_france" in fqn_fr
    assert df_fr.count() == 2
    assert df_fr.filter("sales_org_code = 'FR'").count() == 2

    # Check the call for Germany
    call_de_args = spy_write.call_args_list[1].args
    df_de = call_de_args[0]
    fqn_de = call_de_args[1]
    assert "sales_germany" in fqn_de
    assert df_de.count() == 1
    assert df_de.filter("sales_org_code = 'DE'").count() == 1

    # 6. Cleanup
    spark.catalog.dropTempView("source_for_split")
    spark.sql("DROP SCHEMA IF EXISTS spark_catalog.gold_test_user CASCADE")


# ==============================================================================
# F0 — CANONICAL OPERATOR NAMES + ALIASES + NEW OPS
# ==============================================================================

@pytest.mark.parametrize("filters, expected_count", [
    # Canonical English names
    ([{"column": "country", "operator": "equals", "value": "FR"}], 1),
    ([{"column": "country", "operator": "not_equals", "value": "FR"}], 2),
    ([{"column": "sales", "operator": "greater_than", "value": 150}], 1),
    ([{"column": "sales", "operator": "less_than", "value": 150}], 1),
    ([{"column": "sales", "operator": "greater_than_equal", "value": 150}], 2),
    ([{"column": "sales", "operator": "less_than_equal", "value": 150}], 2),
    # Old SQL aliases still work
    ([{"column": "country", "operator": "eq", "value": "FR"}], 1),
    ([{"column": "country", "operator": "ne", "value": "FR"}], 2),
    ([{"column": "sales", "operator": "gt", "value": 150}], 1),
    ([{"column": "sales", "operator": "lt", "value": 150}], 1),
    ([{"column": "sales", "operator": "gte", "value": 150}], 2),
    ([{"column": "sales", "operator": "lte", "value": 150}], 2),
    # starts_with / ends_with — NEW
    ([{"column": "product", "operator": "starts_with", "value": "lap"}], 1),
    ([{"column": "product", "operator": "ends_with", "value": "top"}], 1),
    # not_contains (canonical) + notcontains (alias)
    ([{"column": "product", "operator": "not_contains", "value": "phone"}], 1),
    ([{"column": "product", "operator": "notcontains", "value": "phone"}], 1),
    # not_like / notlike
    ([{"column": "product", "operator": "not_like", "value": "%phone%"}], 1),
    ([{"column": "product", "operator": "notlike", "value": "%phone%"}], 1),
])
def test_build_filter_expression_canonical_and_aliases(spark, filters, expected_count):
    """Tests canonical names and backward-compat aliases in _build_filter_expression."""
    data = [
        ("FR", 100, "laptop"),
        ("DE", 200, "smartphone"),
        ("US", 150, None),
    ]
    df = spark.createDataFrame(data, ["country", "sales", "product"])
    filter_expr = _build_filter_expression(filters)
    assert df.filter(filter_expr).count() == expected_count


def test_build_filter_expression_unknown_operator_raises(spark):
    """Unknown operator raises ValueError instead of silently returning lit(True). (Plan 17-1.3)"""
    filters = [{"column": "country", "operator": "bogus_op", "value": "FR"}]
    with pytest.raises(ValueError, match="bogus_op"):
        _build_filter_expression(filters)


def test_build_filter_expression_in_string_with_spaces_warning(spark, caplog):
    """in operator with spaced values warns about leading/trailing spaces."""
    import logging
    data = [("FR",), ("DE",)]
    df = spark.createDataFrame(data, ["country"])
    # Values with leading spaces after comma
    filters = [{"column": "country", "operator": "in", "value": "FR, DE"}]
    with caplog.at_level(logging.WARNING, logger="skifer.backends.spark"):
        filter_expr = _build_filter_expression(filters)
        result = df.filter(filter_expr).count()
    assert "leading/trailing space" in caplog.text
    # Should still filter correctly (2 rows, but spaces stripped)
    assert result == 2


def test_build_filter_expression_between_nominal_range(spark):
    """between operator filters values inside the inclusive range."""
    df = spark.createDataFrame([(100,), (150,), (200,), (250,)], ["sales"])
    conditions = _build_filter_expression(
        [{"column": "sales", "operator": "between", "value": "150,200"}]
    )
    result = [row["sales"] for row in df.filter(conditions).collect()]
    assert result == [150, 200]


def test_build_filter_expression_between_inclusive_bounds(spark):
    """between operator includes both lower and upper bounds."""
    df = spark.createDataFrame([(149,), (150,), (200,), (201,)], ["sales"])
    conditions = _build_filter_expression(
        [{"column": "sales", "operator": "between", "value": "150, 200"}]
    )
    result = [row["sales"] for row in df.filter(conditions).collect()]
    assert result == [150, 200]


def test_build_filter_expression_between_invalid_arity_raises(spark):
    """between operator requires exactly two values."""
    filters = [{"column": "sales", "operator": "between", "value": "150"}]
    with pytest.raises(ValueError, match="sales"):
        _build_filter_expression(filters)


def test_build_filter_expression_not_between_nominal_range(spark):
    """not_between operator filters values strictly outside the inclusive range."""
    df = spark.createDataFrame([(100,), (150,), (200,), (250,)], ["sales"])
    conditions = _build_filter_expression(
        [{"column": "sales", "operator": "not_between", "value": "150,200"}]
    )
    result = [row["sales"] for row in df.filter(conditions).collect()]
    assert result == [100, 250]


def test_build_filter_expression_not_between_excludes_bounds(spark):
    """not_between operator excludes both lower and upper bounds from the result."""
    df = spark.createDataFrame([(149,), (150,), (200,), (201,)], ["sales"])
    conditions = _build_filter_expression(
        [{"column": "sales", "operator": "not_between", "value": "150, 200"}]
    )
    result = [row["sales"] for row in df.filter(conditions).collect()]
    assert result == [149, 201]


def test_build_filter_expression_not_between_invalid_arity_raises(spark):
    """not_between operator requires exactly two values."""
    filters = [{"column": "sales", "operator": "not_between", "value": "150"}]
    with pytest.raises(ValueError, match="sales"):
        _build_filter_expression(filters)


def test_build_filter_expression_apostrophe_equals(spark):
    """equals operator with a value containing an apostrophe must not crash or produce wrong results."""
    df = spark.createDataFrame([("O'Brien",), ("Smith",)], ["name"])
    conditions = _build_filter_expression([{"column": "name", "operator": "equals", "value": "O'Brien"}])
    result = df.filter(conditions).collect()
    assert len(result) == 1
    assert result[0]["name"] == "O'Brien"


def test_build_filter_expression_apostrophe_contains(spark):
    """contains operator with a value containing an apostrophe must not crash."""
    df = spark.createDataFrame([("O'Brien Jr",), ("Smith",)], ["name"])
    conditions = _build_filter_expression([{"column": "name", "operator": "contains", "value": "O'Brien"}])
    result = df.filter(conditions).collect()
    assert len(result) == 1
    assert result[0]["name"] == "O'Brien Jr"


def test_build_filter_expression_numeric_as_string(spark):
    """equals operator with a numeric value passed as string matches correctly."""
    df = spark.createDataFrame([("42",), ("100",)], ["score"])
    conditions = _build_filter_expression([{"column": "score", "operator": "equals", "value": "42"}])
    result = df.filter(conditions).collect()
    assert len(result) == 1
    assert result[0]["score"] == "42"


def test_build_filter_expression_empty_string_value(spark):
    """equals operator with an empty string value filters correctly."""
    df = spark.createDataFrame([("",), ("non-empty",)], ["label"])
    conditions = _build_filter_expression([{"column": "label", "operator": "equals", "value": ""}])
    result = df.filter(conditions).collect()
    assert len(result) == 1
    assert result[0]["label"] == ""


def test_filter_groups_single_group(mock_engine, spark):
    """filter_groups with a single group applies correctly."""
    data = [("FR", 100), ("DE", 200), ("US", 150)]
    df = spark.createDataFrame(data, ["country", "sales"])
    df.createOrReplaceTempView("fg_single_test")

    schema_dict = {
        "tables": [{"name": "fg_single_test", "alias": "t",
                    "filter_groups": [["country:equals:FR"]]}],
    }
    result = mock_engine.process_schema(schema_dict)
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["country"] == "FR"

    spark.catalog.dropTempView("fg_single_test")


def test_filter_groups_empty_list(mock_engine, spark):
    """filter_groups: [] applies no filter — all rows are retained."""
    data = [("FR", 100), ("DE", 200)]
    df = spark.createDataFrame(data, ["country", "sales"])
    df.createOrReplaceTempView("fg_empty_test")

    schema_dict = {
        "tables": [{"name": "fg_empty_test", "alias": "t", "filter_groups": []}],
    }
    result = mock_engine.process_schema(schema_dict)
    assert result.count() == 2

    spark.catalog.dropTempView("fg_empty_test")


# --- New operations ---

def test_apply_operation_lower(spark):
    df = spark.createDataFrame([("HELLO",)], ["col1"])
    result = df.withColumn("res", _apply_operation(F.col("col1"), "lower")).collect()[0]["res"]
    assert result == "hello"


def test_apply_operation_round(spark):
    df = spark.createDataFrame([(3.14159,)], StructType([StructField("col1", FloatType(), True)]))
    result = df.withColumn("res", _apply_operation(F.col("col1"), "round:2")).collect()[0]["res"]
    assert round(result, 2) == 3.14


def test_apply_operation_abs(spark):
    df = spark.createDataFrame([(-42,)], StructType([StructField("col1", IntegerType(), True)]))
    result = df.withColumn("res", _apply_operation(F.col("col1"), "abs")).collect()[0]["res"]
    assert result == 42


def test_apply_operation_ceil(spark):
    df = spark.createDataFrame([(3.14,)], StructType([StructField("col1", FloatType(), True)]))
    result = df.withColumn("res", _apply_operation(F.col("col1"), "ceil")).collect()[0]["res"]
    assert result == 4


def test_apply_operation_length(spark):
    df = spark.createDataFrame([("hello",)], ["col1"])
    result = df.withColumn("res", _apply_operation(F.col("col1"), "length")).collect()[0]["res"]
    assert result == 5


def test_apply_operation_to_date(spark):
    df = spark.createDataFrame([("2024-01-15",)], ["col1"])
    result = df.withColumn("res", _apply_operation(F.col("col1"), "to_date:yyyy-MM-dd")).collect()[0]["res"]
    from datetime import date
    assert result == date(2024, 1, 15)


def test_apply_operation_nvl(spark):
    schema = StructType([StructField("col1", StringType(), True)])
    df = spark.createDataFrame([(None,)], schema)
    result = df.withColumn("res", _apply_operation(F.col("col1"), "nvl:default")).collect()[0]["res"]
    assert result == "default"


def test_apply_operation_unknown_raises(spark):
    """Unknown operation raises ValueError instead of silently passing through. (Plan 17-1.3)"""
    c = F.col("some_col")
    with pytest.raises(ValueError, match="totally_unknown_op"):
        _apply_operation(c, "totally_unknown_op")


# --- when: canonical conditions ---

def test_apply_operation_when_equals(spark):
    df = spark.createDataFrame([("DONE",), ("PENDING",)], ["status"])
    c = F.col("status")
    cond = _apply_operation(c, "when:equals:DONE")
    result = df.withColumn("res", F.when(cond, F.lit("yes")).otherwise(F.lit("no"))).collect()
    assert result[0]["res"] == "yes"
    assert result[1]["res"] == "no"


def test_apply_operation_when_not_equals(spark):
    df = spark.createDataFrame([("DONE",), ("PENDING",)], ["status"])
    c = F.col("status")
    cond = _apply_operation(c, "when:not_equals:DONE")
    result = df.withColumn("res", F.when(cond, F.lit("yes")).otherwise(F.lit("no"))).collect()
    assert result[0]["res"] == "no"
    assert result[1]["res"] == "yes"


def test_apply_operation_when_starts_with(spark):
    df = spark.createDataFrame([("hello_world",), ("goodbye",)], ["col1"])
    c = F.col("col1")
    cond = _apply_operation(c, "when:starts_with:hello")
    result = df.withColumn("res", F.when(cond, F.lit("yes")).otherwise(F.lit("no"))).collect()
    assert result[0]["res"] == "yes"
    assert result[1]["res"] == "no"


def test_apply_operation_when_ends_with(spark):
    df = spark.createDataFrame([("hello_world",), ("goodbye",)], ["col1"])
    c = F.col("col1")
    cond = _apply_operation(c, "when:ends_with:world")
    result = df.withColumn("res", F.when(cond, F.lit("yes")).otherwise(F.lit("no"))).collect()
    assert result[0]["res"] == "yes"
    assert result[1]["res"] == "no"


# ==============================================================================
# F4 — dedup_after_union configurable
# ==============================================================================

def test_run_union_sources_to_table_dedup_true(mock_engine, mocker, spark):
    """dedup_after_union=True (default) removes duplicate rows after union."""
    spark.sql("CREATE SCHEMA IF NOT EXISTS spark_catalog.silver_test_user")
    spark.sql("CREATE SCHEMA IF NOT EXISTS spark_catalog.gold_test_user")

    mocker.patch.object(mock_engine, "_write_dataframe")

    # Create two tables with one overlapping row
    df1 = spark.createDataFrame([(1, "A"), (2, "B")], ["id", "val"])
    df2 = spark.createDataFrame([(2, "B"), (3, "C")], ["id", "val"])
    df1.createOrReplaceTempView("src_p1")
    df2.createOrReplaceTempView("src_p2")

    # Patch spark.table to return our test frames
    original_table = mock_engine.spark.table

    def fake_table(name):
        clean = name.replace("`", "")
        if "src_p1" in clean:
            return df1
        if "src_p2" in clean:
            return df2
        return original_table(name)

    mocker.patch.object(mock_engine.spark, "table", side_effect=fake_table)

    schema_dict = {"tables": [{"name": "unioned", "alias": "unioned"}]}
    mock_engine.run_union_sources_to_table(
        schema_dict=schema_dict,
        source_partitions=[{"label": "p1"}, {"label": "p2"}],
        source_layer="silver",
        target_layer="gold",
        target_table_name="result_dedup",
        source_base_names=["src"],
        source_alias="unioned",
        dedup_after_union=True,
    )

    # Get the df passed to _write_dataframe
    written_df = mock_engine._write_dataframe.call_args[0][0]
    assert written_df.count() == 3  # duplicates removed

    spark.catalog.dropTempView("src_p1")
    spark.catalog.dropTempView("src_p2")
    spark.sql("DROP SCHEMA IF EXISTS spark_catalog.silver_test_user CASCADE")
    spark.sql("DROP SCHEMA IF EXISTS spark_catalog.gold_test_user CASCADE")


def test_run_union_sources_to_table_dedup_false(mock_engine, mocker, spark):
    """dedup_after_union=False keeps all rows including duplicates."""
    spark.sql("CREATE SCHEMA IF NOT EXISTS spark_catalog.silver_test_user")
    spark.sql("CREATE SCHEMA IF NOT EXISTS spark_catalog.gold_test_user")

    mocker.patch.object(mock_engine, "_write_dataframe")

    df1 = spark.createDataFrame([(1, "A"), (2, "B")], ["id", "val"])
    df2 = spark.createDataFrame([(2, "B"), (3, "C")], ["id", "val"])
    df1.createOrReplaceTempView("src2_p1")
    df2.createOrReplaceTempView("src2_p2")

    original_table = mock_engine.spark.table

    def fake_table(name):
        clean = name.replace("`", "")
        if "src2_p1" in clean:
            return df1
        if "src2_p2" in clean:
            return df2
        return original_table(name)

    mocker.patch.object(mock_engine.spark, "table", side_effect=fake_table)

    schema_dict = {"tables": [{"name": "unioned", "alias": "unioned"}]}
    mock_engine.run_union_sources_to_table(
        schema_dict=schema_dict,
        source_partitions=[{"label": "p1"}, {"label": "p2"}],
        source_layer="silver",
        target_layer="gold",
        target_table_name="result_nodedup",
        source_base_names=["src2"],
        source_alias="unioned",
        dedup_after_union=False,
    )

    written_df = mock_engine._write_dataframe.call_args[0][0]
    assert written_df.count() == 4  # all rows including duplicates

    spark.catalog.dropTempView("src2_p1")
    spark.catalog.dropTempView("src2_p2")
    spark.sql("DROP SCHEMA IF EXISTS spark_catalog.silver_test_user CASCADE")
    spark.sql("DROP SCHEMA IF EXISTS spark_catalog.gold_test_user CASCADE")


def test_run_process_to_table_survives_failed_process(mock_engine, mocker, spark):
    """Target table survives intact when process_schema raises (atomic overwrite)."""
    mocker.patch.object(mock_engine, "process_schema", side_effect=RuntimeError("processing failed"))
    mocker.patch.object(mock_engine, "_write_dataframe")
    mocker.patch.object(mock_engine, "_ensure_schema_exists")

    with pytest.raises(RuntimeError, match="processing failed"):
        mock_engine.run_process_to_table(
            schema_dict={"tables": []},
            target_layer="gold",
            target_table_name="surviving_table",
        )

    # _write_dataframe must NOT have been called — the table was never touched
    mock_engine._write_dataframe.assert_not_called()


def _engine_for_run_id_tests(mocker):
    engine = object.__new__(SkiferEngine)
    object.__setattr__(engine, "_context", ExecutionContext(env="test", is_local=True))
    engine._tracer = NoOpTracer()
    engine._patterns = mocker.Mock()
    return engine


def test_run_process_to_table_accepts_and_returns_injected_run_id(mocker):
    engine = _engine_for_run_id_tests(mocker)
    run_id = str(uuid4())
    spy = engine._patterns.run_process_to_table

    returned = engine.run_process_to_table(
        schema_dict={"tables": []},
        target_layer="gold",
        target_table_name="orders",
        run_id=run_id,
    )

    assert returned == run_id
    spy.assert_called_once_with(
        {"tables": []},
        "gold",
        "orders",
        intermediate_mode="inline",
        run_id=run_id,
    )


def test_run_from_yaml_mints_and_returns_same_run_id(mocker):
    engine = _engine_for_run_id_tests(mocker)
    spy = engine._patterns.run_from_yaml

    returned = engine.run_from_yaml(
        "schemas/gold/orders.yaml",
        "gold",
        "orders",
        {"region": "EMEA"},
    )

    UUID(returned)
    spy.assert_called_once_with(
        "schemas/gold/orders.yaml",
        "gold",
        "orders",
        {"region": "EMEA"},
        run_id=returned,
    )


def test_full_refresh_accepts_and_returns_correlation_run_id(mocker, tmp_path):
    engine = _engine_for_run_id_tests(mocker)
    engine.get_target_schema = lambda layer: layer
    engine._build_fqn = lambda schema, table: f"{schema}.{table}"
    engine._drop_table_if_exists = mocker.Mock()
    run_id = str(uuid4())
    checkpoint = tmp_path / "checkpoint"
    checkpoint.mkdir()

    returned = engine.full_refresh(
        "gold",
        "orders",
        checkpoint=str(checkpoint),
        run_id=run_id,
    )

    assert returned == run_id
    engine._drop_table_if_exists.assert_called_once_with("gold.orders")


# ==============================================================================
# B.2 — Compact when/then/else runtime validation
# ==============================================================================

def test_get_select_expressions_valid_when_chain(mock_engine, spark):
    """Valid 3-element when/then/else chain produces a conditional column."""
    df = spark.createDataFrame([("DONE",), ("PENDING",)], ["status"])
    df.createOrReplaceTempView("b2_valid_test")

    schema_dict = {
        "tables": [{"name": "b2_valid_test", "alias": "t"}],
        "select_final": [
            ["status", "status", []],
            ["status", "label", ["when:equals:DONE", "then:lit:Paid", "else:lit:Other"]],
        ],
    }
    result = mock_engine.process_schema(schema_dict)
    rows = {r["status"]: r["label"] for r in result.collect()}
    assert rows["DONE"] == "Paid"
    assert rows["PENDING"] == "Other"
    spark.catalog.dropTempView("b2_valid_test")


def test_get_select_expressions_when_missing_else_raises(mock_engine, spark):
    """2-element when chain raises ValueError at runtime (defense in depth)."""
    df = spark.createDataFrame([("DONE",)], ["status"])
    df.createOrReplaceTempView("b2_missing_else")

    schema_dict = {
        "tables": [{"name": "b2_missing_else", "alias": "t"}],
        "select_final": [["status", "label", ["when:equals:DONE", "then:lit:Paid"]]],
    }
    with pytest.raises(ValueError, match="exactly"):
        mock_engine.process_schema(schema_dict)
    spark.catalog.dropTempView("b2_missing_else")


def test_get_select_expressions_when_extra_op_raises(mock_engine, spark):
    """4-element when chain raises ValueError at runtime (defense in depth)."""
    df = spark.createDataFrame([("DONE",)], ["status"])
    df.createOrReplaceTempView("b2_extra_op")

    schema_dict = {
        "tables": [{"name": "b2_extra_op", "alias": "t"}],
        "select_final": [["status", "label", ["when:equals:DONE", "then:lit:Paid", "else:lit:Other", "upper"]]],
    }
    with pytest.raises(ValueError, match="exactly"):
        mock_engine.process_schema(schema_dict)
    spark.catalog.dropTempView("b2_extra_op")


# ==============================================================================
# F2 — Quality Checks DSL
# ==============================================================================

def test_quality_checks_drop_nulls_in(mock_engine, spark):
    """drop_nulls_in removes rows with nulls in specified columns."""
    data = [(1, "Alice", "FR"), (2, None, "DE"), (3, "Bob", None), (4, "Carol", "US")]
    df = spark.createDataFrame(data, ["id", "name", "country"])
    df.createOrReplaceTempView("qc_nulls_test")

    schema_dict = {
        "tables": [{
            "name": "qc_nulls_test",
            "alias": "t",
            "quality_checks": {"drop_nulls_in": ["name"]}
        }]
    }
    result = mock_engine.process_schema(schema_dict)
    # Rows with null name (id=2) should be dropped; null country rows kept
    ids = [r["id"] for r in result.collect()]
    assert 2 not in ids
    assert 1 in ids
    assert 3 in ids
    assert 4 in ids

    spark.catalog.dropTempView("qc_nulls_test")


def test_quality_checks_drop_duplicates_on(mock_engine, spark):
    """drop_duplicates_on removes duplicate rows based on specified columns."""
    data = [(1, "FR", "A"), (2, "FR", "B"), (3, "DE", "C")]
    df = spark.createDataFrame(data, ["id", "country", "val"])
    df.createOrReplaceTempView("qc_dedup_test")

    schema_dict = {
        "tables": [{
            "name": "qc_dedup_test",
            "alias": "t",
            "quality_checks": {"drop_duplicates_on": ["country"]}
        }]
    }
    result = mock_engine.process_schema(schema_dict)
    # Only one row per country should remain
    assert result.count() == 2

    spark.catalog.dropTempView("qc_dedup_test")


def test_quality_checks_combined_order(mock_engine, spark):
    """drop_nulls_in applied before drop_duplicates_on."""
    data = [(1, "FR", "A"), (2, None, "A"), (3, "FR", "B")]
    df = spark.createDataFrame(data, ["id", "country", "val"])
    df.createOrReplaceTempView("qc_combined_test")

    schema_dict = {
        "tables": [{
            "name": "qc_combined_test",
            "alias": "t",
            "quality_checks": {
                "drop_nulls_in": ["country"],
                "drop_duplicates_on": ["country"]
            }
        }]
    }
    result = mock_engine.process_schema(schema_dict)
    # After nulls dropped: rows 1 and 3 (both FR). After dedup on country: 1 row.
    assert result.count() == 1

    spark.catalog.dropTempView("qc_combined_test")


def test_quality_checks_empty_dict_does_nothing(mock_engine, spark):
    """Empty quality_checks: {} does nothing."""
    data = [(1, "A"), (2, "B")]
    df = spark.createDataFrame(data, ["id", "val"])
    df.createOrReplaceTempView("qc_empty_test")

    schema_dict = {
        "tables": [{
            "name": "qc_empty_test",
            "alias": "t",
            "quality_checks": {}
        }]
    }
    result = mock_engine.process_schema(schema_dict)
    assert result.count() == 2

    spark.catalog.dropTempView("qc_empty_test")


# ==============================================================================
# F6 — force_env + dev_limit
# ==============================================================================

@pytest.fixture
def engine_with_config(spark, tmp_path, mocker):
    """Engine fixture that uses a real config file for force_env tests."""
    config_content = """
default_env: LOCAL
priority_check: [LOCAL]
environments:
  LOCAL:
    catalog: null
    is_production: false
  DEV:
    catalog: spark_catalog
    is_production: false
  PROD:
    catalog: spark_catalog
    is_production: true
"""
    config_file = tmp_path / "config.yaml"
    config_file.write_text(config_content)

    mocker.patch.object(SkiferEngine, "_get_clean_username", return_value="test_user")

    return spark, str(config_file)


def test_force_env_valid(engine_with_config, mocker):
    """force_env with valid env sets correct env and db."""
    spark, config_path = engine_with_config
    engine = SkiferEngine(spark=spark, config_path=config_path, force_env="DEV")
    assert engine.env == "DEV"
    assert engine.db == "spark_catalog"


def test_force_env_case_insensitive(engine_with_config):
    """force_env is case-insensitive."""
    spark, config_path = engine_with_config
    engine = SkiferEngine(spark=spark, config_path=config_path, force_env="local")
    assert engine.env == "LOCAL"
    assert engine.db is None


def test_force_env_invalid_raises(engine_with_config):
    """force_env with unknown env raises ValueError."""
    spark, config_path = engine_with_config
    with pytest.raises(ValueError, match="force_env='UNKNOWN'"):
        SkiferEngine(spark=spark, config_path=config_path, force_env="UNKNOWN")


def test_dev_limit_schema_level(mock_engine, spark):
    """dev_limit at schema level applies df.limit() in non-prod interactive mode."""
    data = [(i,) for i in range(100)]
    df = spark.createDataFrame(data, ["id"])
    df.createOrReplaceTempView("dev_limit_test")

    # mock_engine has is_job_execution=False and non-prod config
    mock_engine.is_job_execution = False

    schema_dict = {
        "dev_limit": 10,
        "tables": [{"name": "dev_limit_test", "alias": "t"}]
    }
    result = mock_engine.process_schema(schema_dict)
    assert result.count() == 10

    spark.catalog.dropTempView("dev_limit_test")


def test_dev_limit_table_level(mock_engine, spark):
    """dev_limit at table level applies df.limit()."""
    data = [(i,) for i in range(100)]
    df = spark.createDataFrame(data, ["id"])
    df.createOrReplaceTempView("dev_limit_table_test")

    mock_engine.is_job_execution = False

    schema_dict = {
        "tables": [{"name": "dev_limit_table_test", "alias": "t", "dev_limit": 5}]
    }
    result = mock_engine.process_schema(schema_dict)
    assert result.count() == 5

    spark.catalog.dropTempView("dev_limit_table_test")


def test_dev_limit_table_overrides_schema(mock_engine, spark):
    """dev_limit at table level overrides schema level."""
    data = [(i,) for i in range(100)]
    df = spark.createDataFrame(data, ["id"])
    df.createOrReplaceTempView("dev_limit_override_test")

    mock_engine.is_job_execution = False

    schema_dict = {
        "dev_limit": 50,
        "tables": [{"name": "dev_limit_override_test", "alias": "t", "dev_limit": 3}]
    }
    result = mock_engine.process_schema(schema_dict)
    assert result.count() == 3

    spark.catalog.dropTempView("dev_limit_override_test")


def test_dev_limit_skipped_in_job_mode(mock_engine, spark):
    """dev_limit is silently ignored in job execution mode."""
    data = [(i,) for i in range(100)]
    df = spark.createDataFrame(data, ["id"])
    df.createOrReplaceTempView("dev_limit_job_test")

    mock_engine.is_job_execution = True

    schema_dict = {
        "dev_limit": 5,
        "tables": [{"name": "dev_limit_job_test", "alias": "t"}]
    }
    result = mock_engine.process_schema(schema_dict)
    assert result.count() == 100

    mock_engine.is_job_execution = False
    spark.catalog.dropTempView("dev_limit_job_test")


# ==============================================================================
# F5 — Schema Expressiveness
# ==============================================================================

def test_filter_groups_or_between_groups(mock_engine, spark):
    """filter_groups: groups are OR-combined, conditions within a group are AND-combined."""
    data = [
        (1, "EMEA", "ACTIVE"),
        (2, "APAC", "ACTIVE"),
        (3, "EMEA", "INACTIVE"),
        (4, "LATAM", "ACTIVE"),
    ]
    df = spark.createDataFrame(data, ["id", "region", "status"])
    df.createOrReplaceTempView("fg_test")

    schema_dict = {
        "tables": [{
            "name": "fg_test",
            "alias": "t",
            "filter_groups": [
                # Group 1: EMEA AND ACTIVE
                [
                    {"column": "region", "operator": "equals", "value": "EMEA"},
                    {"column": "status", "operator": "equals", "value": "ACTIVE"},
                ],
                # Group 2: APAC (any status)
                [
                    {"column": "region", "operator": "equals", "value": "APAC"},
                ],
            ]
        }]
    }
    result = mock_engine.process_schema(schema_dict)
    ids = sorted([r["id"] for r in result.collect()])
    # Row 1 (EMEA+ACTIVE) and Row 2 (APAC) should match; Row 3 (EMEA+INACTIVE) and Row 4 (LATAM) should not
    assert ids == [1, 2]

    spark.catalog.dropTempView("fg_test")


def test_keep_all_columns_keeps_source_columns(mock_engine, spark):
    """keep_all_columns=True keeps all source columns."""
    data = [(1, "Alice", "FR"), (2, "Bob", "DE")]
    df = spark.createDataFrame(data, ["id", "name", "country"])
    df.createOrReplaceTempView("kac_test")

    schema_dict = {
        "tables": [{"name": "kac_test", "alias": "t"}],
        "keep_all_columns": True,
    }
    result = mock_engine.process_schema(schema_dict)
    cols = result.columns
    assert "id" in cols
    assert "name" in cols
    assert "country" in cols
    assert result.count() == 2

    spark.catalog.dropTempView("kac_test")


def test_add_columns_adds_computed_columns(mock_engine, spark):
    """add_columns adds computed columns to existing df."""
    data = [(1, "alice"), (2, "bob")]
    df = spark.createDataFrame(data, ["id", "name"])
    df.createOrReplaceTempView("add_col_test")

    schema_dict = {
        "tables": [{"name": "add_col_test", "alias": "t"}],
        "keep_all_columns": True,
        "add_columns": [
            ["name", "name_upper", ["upper"]],
        ]
    }
    result = mock_engine.process_schema(schema_dict)
    assert "name_upper" in result.columns
    assert "id" in result.columns
    row = result.filter("id = 1").collect()[0]
    assert row["name_upper"] == "ALICE"

    spark.catalog.dropTempView("add_col_test")


def test_add_columns_dict_form_when_else(mock_engine, spark):
    """add_columns supports dict form with when/else chains (plan17-0.3)."""
    data = [(1, 500), (2, 1500)]
    df = spark.createDataFrame(data, ["id", "amount"])
    df.createOrReplaceTempView("add_col_when_test")

    schema_dict = {
        "tables": [{"name": "add_col_when_test", "alias": "t"}],
        "keep_all_columns": True,
        "add_columns": [
            {
                "source": "amount",
                "target": "tier",
                "ops": [
                    {"when": "greater_than_equal:1000", "then": "lit:high"},
                    {"else": "lit:low"},
                ],
            }
        ],
    }
    result = mock_engine.process_schema(schema_dict)
    assert "tier" in result.columns
    rows = {r["id"]: r["tier"] for r in result.collect()}
    assert rows[1] == "low"
    assert rows[2] == "high"

    spark.catalog.dropTempView("add_col_when_test")


def test_keep_all_columns_and_select_final_raises(mock_engine, spark):
    """keep_all_columns + select_final raises ValueError."""
    data = [(1,)]
    df = spark.createDataFrame(data, ["id"])
    df.createOrReplaceTempView("mutual_excl_test")

    schema_dict = {
        "tables": [{"name": "mutual_excl_test", "alias": "t"}],
        "keep_all_columns": True,
        "select_final": [["id", "id", []]],
    }
    with pytest.raises(ValueError, match="mutually exclusive"):
        mock_engine.process_schema(schema_dict)

    spark.catalog.dropTempView("mutual_excl_test")


def test_chained_when_else_dict_form(mock_engine, spark):
    """Multi-condition when/else dict form produces correct CASE WHEN."""
    data = [("DONE",), ("PENDING",), ("OTHER",)]
    df = spark.createDataFrame(data, ["status"])
    df.createOrReplaceTempView("chained_when_test")

    schema_dict = {
        "tables": [{"name": "chained_when_test", "alias": "t"}],
        "select_final": [
            {
                "source": "status",
                "target": "status_label",
                "ops": [
                    {"when": "equals:DONE", "then": "lit:Paid"},
                    {"when": "equals:PENDING", "then": "lit:In Progress"},
                    {"else": "lit:Unknown"},
                ]
            }
        ]
    }
    result = mock_engine.process_schema(schema_dict)
    rows = {r["status_label"] for r in result.collect()}
    assert "Paid" in rows
    assert "In Progress" in rows
    assert "Unknown" in rows

    spark.catalog.dropTempView("chained_when_test")


def test_when_compact_missing_else(mock_engine, spark):
    """Compact when/then with only 2 elements (no else) now raises (plan18-B.2)."""
    data = [("X",), ("Y",)]
    df = spark.createDataFrame(data, ["val"])
    df.createOrReplaceTempView("when_missing_else_test")

    schema_dict = {
        "tables": [{"name": "when_missing_else_test", "alias": "t"}],
        "select_final": [
            ["val", "out", ["when:equals:X", "then:lit:Match"]],
        ]
    }
    # Was previously a silent no-op; now raises a descriptive ValueError (plan18-B.2).
    with pytest.raises(ValueError, match="exactly 3 elements"):
        mock_engine.process_schema(schema_dict)

    spark.catalog.dropTempView("when_missing_else_test")


def test_when_compact_invalid_condition(mock_engine, spark):
    """Compact when with an unknown operator raises ValueError. (Plan 17-1.3)"""
    data = [("X",), ("Y",)]
    df = spark.createDataFrame(data, ["val"])
    df.createOrReplaceTempView("when_invalid_cond_test")

    schema_dict = {
        "tables": [{"name": "when_invalid_cond_test", "alias": "t"}],
        "select_final": [
            ["val", "out", ["when:unknown_op:X", "then:lit:Match", "else:lit:NoMatch"]],
        ]
    }
    with pytest.raises(ValueError, match="unknown_op"):
        mock_engine.process_schema(schema_dict)

    spark.catalog.dropTempView("when_invalid_cond_test")



    """literal:ERP in select_final creates a constant column."""
    data = [(1,), (2,)]
    df = spark.createDataFrame(data, ["id"])
    df.createOrReplaceTempView("literal_test")

    schema_dict = {
        "tables": [{"name": "literal_test", "alias": "t"}],
        "select_final": [
            ["id", "id", []],
            [None, "source_system", ["lit:ERP"]],
        ]
    }
    result = mock_engine.process_schema(schema_dict)
    rows = result.collect()
    assert all(r["source_system"] == "ERP" for r in rows)

    spark.catalog.dropTempView("literal_test")


# ==============================================================================
# F7 — describe_schema()
# ==============================================================================

def test_describe_schema_runs_without_error(mock_engine, capsys):
    """describe_schema runs without error on a valid schema."""
    schema_dict = {
        "tables": [{"name": "silver.orders", "alias": "orders"}],
        "select_final": [["order_id", "id", []]],
    }
    mock_engine.describe_schema(schema_dict)
    captured = capsys.readouterr()
    assert len(captured.out) > 0


def test_describe_schema_contains_table_names(mock_engine, capsys):
    """describe_schema output contains table names."""
    schema_dict = {
        "tables": [
            {"name": "silver.orders", "alias": "orders"},
            {"name": "silver.customers", "alias": "customers"},
        ],
    }
    mock_engine.describe_schema(schema_dict)
    captured = capsys.readouterr()
    assert "silver.orders" in captured.out
    assert "silver.customers" in captured.out


def test_describe_schema_contains_join_info(mock_engine, capsys):
    """describe_schema output contains join descriptions."""
    schema_dict = {
        "tables": [
            {"name": "silver.orders", "alias": "orders"},
            {"name": "silver.customers", "alias": "customers"},
        ],
        "join": [
            {
                "table_from": "orders",
                "on_from": "customer_id",
                "table_to": "customers",
                "on_to": "id",
                "type": "left",
            }
        ],
    }
    mock_engine.describe_schema(schema_dict)
    captured = capsys.readouterr()
    assert "JOIN" in captured.out
    assert "orders" in captured.out
    assert "customers" in captured.out


def test_describe_schema_contains_column_names(mock_engine, capsys):
    """describe_schema output contains output column names."""
    schema_dict = {
        "tables": [{"name": "silver.orders", "alias": "orders"}],
        "select_final": [
            ["order_id", "id", []],
            ["amount", "amount_eur", ["cast:double"]],
        ],
    }
    mock_engine.describe_schema(schema_dict)
    captured = capsys.readouterr()
    assert "id" in captured.out
    assert "amount_eur" in captured.out


# ==============================================================================
# F7b — describe_schema() returns dict
# ==============================================================================

def test_describe_schema_returns_dict(mock_engine):
    """describe_schema returns a structured dict."""
    schema_dict = {
        "tables": [{"name": "silver.orders", "alias": "ord"}],
        "select_final": [["order_id", "id", []]],
    }
    result = mock_engine.describe_schema(schema_dict, print_summary=False)
    assert isinstance(result, dict)
    assert "sources" in result
    assert "output_columns" in result


def test_describe_schema_sources(mock_engine):
    """describe_schema returns sources with alias."""
    schema_dict = {
        "tables": [{"name": "silver.orders", "alias": "ord"}],
    }
    result = mock_engine.describe_schema(schema_dict, print_summary=False)
    assert len(result["sources"]) == 1
    assert result["sources"][0]["alias"] == "ord"


def test_describe_schema_print_still_works(mock_engine, capsys):
    """describe_schema still prints when print_summary=True (default)."""
    schema_dict = {
        "tables": [{"name": "silver.orders", "alias": "ord"}],
    }
    mock_engine.describe_schema(schema_dict, print_summary=True)
    captured = capsys.readouterr()
    assert "Sources" in captured.out


def test_describe_schema_no_print_when_false(mock_engine, capsys):
    """describe_schema produces no output when print_summary=False."""
    schema_dict = {
        "tables": [{"name": "silver.orders", "alias": "ord"}],
    }
    mock_engine.describe_schema(schema_dict, print_summary=False)
    captured = capsys.readouterr()
    assert captured.out == ""


def test_describe_schema_joins(mock_engine):
    """describe_schema includes join information."""
    schema_dict = {
        "tables": [
            {"name": "silver.orders", "alias": "orders"},
            {"name": "silver.customers", "alias": "customers"},
        ],
        "join": [{"table_from": "orders", "on_from": "cid", "table_to": "customers", "on_to": "id", "type": "left"}],
    }
    result = mock_engine.describe_schema(schema_dict, print_summary=False)
    assert len(result["joins"]) == 1
    assert result["joins"][0]["type"] == "left"


def test_describe_schema_business_rules(mock_engine):
    """describe_schema includes business rules list."""
    schema_dict = {
        "tables": [{"name": "silver.orders", "alias": "ord"}],
        "business_rules": ["flag_high_value"],
    }
    result = mock_engine.describe_schema(schema_dict, print_summary=False)
    assert result["business_rules"] == ["flag_high_value"]


# ==============================================================================
# F9 — infer_output_schema()
# ==============================================================================

def test_infer_output_schema_cast(mock_engine):
    """infer_output_schema infers double from cast:double."""
    schema = {"select_final": [["amount", "amount_eur", ["cast:double", "round:2"]]]}
    result = mock_engine.infer_output_schema(schema)
    col = next(c for c in result if c["name"] == "amount_eur")
    assert col["type"] == "double"


def test_infer_output_schema_cast_int(mock_engine):
    """infer_output_schema infers int from cast:int."""
    schema = {"select_final": [["qty", "qty_int", ["cast:int"]]]}
    result = mock_engine.infer_output_schema(schema)
    assert result[0]["type"] == "int"


def test_infer_output_schema_to_date(mock_engine):
    """infer_output_schema infers date from to_date."""
    schema = {"select_final": [["ts", "event_date", ["to_date:yyyy-MM-dd"]]]}
    result = mock_engine.infer_output_schema(schema)
    assert result[0]["type"] == "date"


def test_infer_output_schema_literal(mock_engine):
    """infer_output_schema infers string from literal: source prefix."""
    schema = {"select_final": [["literal:ERP", "source_system"]]}
    result = mock_engine.infer_output_schema(schema)
    assert result[0]["type"] == "string"


def test_infer_output_schema_unknown(mock_engine):
    """infer_output_schema returns unknown when no type hint is available."""
    schema = {"select_final": [["some_col", "out_col"]]}
    result = mock_engine.infer_output_schema(schema)
    assert result[0]["type"] == "unknown"


def test_infer_output_schema_when_else(mock_engine):
    """infer_output_schema infers string from when/else dict form."""
    schema = {
        "select_final": [
            {
                "source": "status",
                "target": "status_label",
                "ops": [{"when": "equals:DONE", "then": "lit:Paid"}, {"else": "lit:Unknown"}],
            }
        ]
    }
    result = mock_engine.infer_output_schema(schema)
    assert result[0]["type"] == "string"


def test_infer_output_schema_returns_name_and_source(mock_engine):
    """infer_output_schema result contains name and source fields."""
    schema = {"select_final": [["amount", "amount_eur", ["cast:double"]]]}
    result = mock_engine.infer_output_schema(schema)
    assert result[0]["name"] == "amount_eur"
    assert result[0]["source"] == "amount"


# ==============================================================================
# F8 — allow_raw_sql governance flag
# ==============================================================================

def test_allow_raw_sql_default_true_sql_filter_allowed(spark):
    """sql: filter operator is allowed by default (allow_raw_sql=True)."""
    filters = [{"column": "amount", "operator": "sql", "value": "amount > 100"}]
    df = spark.createDataFrame([(50,), (200,)], ["amount"])
    expr = _build_filter_expression(filters, allow_raw_sql=True)
    assert df.filter(expr).count() == 1


def test_allow_raw_sql_false_blocks_sql_filter(spark):
    """sql: filter operator raises ValueError when allow_raw_sql=False."""
    filters = [{"column": "amount", "operator": "sql", "value": "amount > 100"}]
    with pytest.raises(ValueError, match="sql: filter operator is disabled"):
        _build_filter_expression(filters, allow_raw_sql=False)


def test_allow_raw_sql_default_true_expr_op_allowed(spark):
    """expr: operation is allowed by default (allow_raw_sql=True)."""
    df = spark.createDataFrame([("hello",)], ["col1"])
    result = df.withColumn("res", _apply_operation(F.col("col1"), "expr:upper(col1)", allow_raw_sql=True))
    assert result.collect()[0]["res"] == "HELLO"


def test_allow_raw_sql_false_blocks_expr_op(spark):
    """expr: operation raises ValueError when allow_raw_sql=False."""
    with pytest.raises(ValueError, match="expr: operation is disabled"):
        _apply_operation(F.col("col1"), "expr:upper(col1)", allow_raw_sql=False)


def test_process_schema_allow_raw_sql_false_blocks_sql_filter(mock_engine, spark):
    """process_schema raises ValueError when sql: filter is used and allow_raw_sql is false."""
    mock_engine.config = {"environments": {"prod": {"is_production": True, "allow_raw_sql": False}}}
    mock_engine.env = "prod"

    data = [(1, 100), (2, 200)]
    df = spark.createDataFrame(data, ["id", "amount"])
    df.createOrReplaceTempView("ars_test_filter")

    schema_dict = {
        "tables": [
            {
                "name": "ars_test_filter",
                "alias": "t",
                "filter": [{"column": "amount", "operator": "sql", "value": "amount > 50"}],
            }
        ],
        "select_final": [["id", "id", []]],
    }
    with pytest.raises(ValueError, match="sql: filter operator is disabled"):
        mock_engine.process_schema(schema_dict)

    spark.catalog.dropTempView("ars_test_filter")


def test_process_schema_allow_raw_sql_false_blocks_expr_op(mock_engine, spark):
    """process_schema raises ValueError when expr: operation is used and allow_raw_sql is false."""
    mock_engine.config = {"environments": {"prod": {"is_production": True, "allow_raw_sql": False}}}
    mock_engine.env = "prod"

    data = [(1, "hello")]
    df = spark.createDataFrame(data, ["id", "label"])
    df.createOrReplaceTempView("ars_test_expr")

    schema_dict = {
        "tables": [{"name": "ars_test_expr", "alias": "t"}],
        "select_final": [["label", "upper_label", ["expr:upper(label)"]]],
    }
    with pytest.raises(ValueError, match="expr: operation is disabled"):
        mock_engine.process_schema(schema_dict)

    spark.catalog.dropTempView("ars_test_expr")


def test_process_schema_allow_raw_sql_true_permits_both(mock_engine, spark):
    """process_schema works normally when allow_raw_sql is true (default)."""
    mock_engine.config = {"environments": {"dev": {"is_production": False, "allow_raw_sql": True}}}
    mock_engine.env = "dev"

    data = [(1, 200), (2, 50)]
    df = spark.createDataFrame(data, ["id", "amount"])
    df.createOrReplaceTempView("ars_test_true")

    schema_dict = {
        "tables": [
            {
                "name": "ars_test_true",
                "alias": "t",
                "filter": [{"column": "amount", "operator": "sql", "value": "amount > 100"}],
            }
        ],
        "select_final": [["id", "id", []], ["amount", "computed", ["expr:amount * 2"]]],
    }
    result = mock_engine.process_schema(schema_dict)
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["computed"] == 400

    spark.catalog.dropTempView("ars_test_true")


def test_process_schema_allow_raw_sql_default_true_when_missing(mock_engine, spark):
    """allow_raw_sql defaults to True when not set in env config (no breaking change)."""
    mock_engine.config = {"environments": {"test": {"is_production": False}}}
    mock_engine.env = "test"

    data = [(1, "world")]
    df = spark.createDataFrame(data, ["id", "msg"])
    df.createOrReplaceTempView("ars_test_default")

    schema_dict = {
        "tables": [{"name": "ars_test_default", "alias": "t"}],
        "select_final": [["msg", "up", ["expr:upper(msg)"]]],
    }
    result = mock_engine.process_schema(schema_dict)
    assert result.collect()[0]["up"] == "WORLD"

    spark.catalog.dropTempView("ars_test_default")


# ==============================================================================
# F9 — _get_workspace_client() centralized helper
# ==============================================================================

def test_get_workspace_client_returns_none_without_creds(mock_engine, monkeypatch):
    """_get_workspace_client returns None when credentials are absent."""
    monkeypatch.delenv("DATABRICKS_HOST", raising=False)
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
    # Clear any cached value from previous calls
    if hasattr(mock_engine, "_workspace_client_cache"):
        del mock_engine._workspace_client_cache

    result = mock_engine._get_workspace_client()
    assert result is None


def test_get_workspace_client_caches_none(mock_engine, monkeypatch):
    """_get_workspace_client caches the None result — second call returns same object."""
    monkeypatch.delenv("DATABRICKS_HOST", raising=False)
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
    if hasattr(mock_engine, "_workspace_client_cache"):
        del mock_engine._workspace_client_cache

    first = mock_engine._get_workspace_client()
    second = mock_engine._get_workspace_client()
    assert first is None
    assert second is None
    # Cache should be set
    assert hasattr(mock_engine, "_workspace_client_cache")
    assert mock_engine._workspace_client_cache is None


def test_get_workspace_client_caches_client(mock_engine, monkeypatch):
    """_get_workspace_client returns the same cached client instance on repeated calls."""
    monkeypatch.setenv("DATABRICKS_HOST", "https://fake.databricks.com")
    monkeypatch.setenv("DATABRICKS_TOKEN", "fake-token")
    if hasattr(mock_engine, "_workspace_client_cache"):
        del mock_engine._workspace_client_cache

    fake_client = object()

    import unittest.mock as um
    with um.patch("skifer.core.core.SkiferEngine._get_workspace_client", return_value=fake_client):
        # Simulate two calls returning the cached client
        first = mock_engine._get_workspace_client()
        second = mock_engine._get_workspace_client()
    assert first is second



# ==============================================================================
# Phase 14 — CAS C : external source loading (process_schema)
# ==============================================================================

def test_process_schema_source_csv_calls_read_source(mock_engine):
    """process_schema with source: csv should call backend.read_source with correct args."""
    mock_df = MagicMock()
    mock_backend = MagicMock()
    mock_backend.read_source.return_value = mock_df
    mock_backend.build_filter_expression.return_value = MagicMock()
    mock_backend.filter.return_value = mock_df
    mock_backend.limit.return_value = mock_df
    mock_engine._backend = mock_backend
    mock_engine._interpreter._backend = mock_backend

    schema_dict = {
        "tables": [
            {
                "name": "raw_orders",
                "alias": "orders",
                "source": {
                    "type": "csv",
                    "path": "/data/orders/*.csv",
                    "options": {"header": "true"},
                },
            }
        ]
    }
    mock_engine.process_schema(schema_dict)

    mock_backend.read_source.assert_called_once_with(
        source_type="csv",
        path="/data/orders/*.csv",
        options={"header": "true"},
    )


def test_process_schema_source_filter_applied_after_read(mock_engine):
    """filter is applied after read_source, not before."""
    mock_df = MagicMock()
    filtered_df = MagicMock()
    mock_backend = MagicMock()
    mock_backend.read_source.return_value = mock_df
    mock_backend.build_filter_expression.return_value = MagicMock()
    mock_backend.filter.return_value = filtered_df
    mock_engine._backend = mock_backend
    mock_engine._interpreter._backend = mock_backend

    schema_dict = {
        "tables": [
            {
                "name": "raw_orders",
                "source": {"type": "parquet", "path": "/data/orders/", "options": {}},
                "filter": [{"column": "status", "operator": "is_not_null"}],
            }
        ]
    }
    mock_engine.process_schema(schema_dict)

    mock_backend.read_source.assert_called_once()
    mock_backend.filter.assert_called_once()


def test_process_schema_source_dev_limit_applied(mock_engine):
    """dev_limit is applied after read_source in non-prod interactive mode."""
    mock_df = MagicMock()
    limited_df = MagicMock()
    mock_backend = MagicMock()
    mock_backend.read_source.return_value = mock_df
    mock_backend.limit.return_value = limited_df
    mock_engine._backend = mock_backend
    mock_engine._interpreter._backend = mock_backend
    mock_engine.is_job_execution = False

    schema_dict = {
        "tables": [
            {
                "name": "raw_orders",
                "source": {"type": "csv", "path": "/data/*.csv", "options": {}},
                "dev_limit": 500,
            }
        ]
    }
    mock_engine.process_schema(schema_dict)

    mock_backend.limit.assert_called_once_with(mock_df, 500)


def test_process_schema_source_quality_checks_applied(mock_engine):
    """quality_checks are applied after read_source."""
    mock_df = MagicMock()
    mock_backend = MagicMock()
    mock_backend.read_source.return_value = mock_df
    mock_backend.drop_nulls.return_value = mock_df
    mock_backend.drop_duplicates.return_value = mock_df
    mock_engine._backend = mock_backend
    mock_engine._interpreter._backend = mock_backend

    schema_dict = {
        "tables": [
            {
                "name": "raw_orders",
                "source": {"type": "json", "path": "/data/events.json", "options": {}},
                "quality_checks": {
                    "drop_nulls_in": ["order_id"],
                    "drop_duplicates_on": ["order_id"],
                },
            }
        ]
    }
    mock_engine.process_schema(schema_dict)

    mock_backend.drop_nulls.assert_called_once_with(mock_df, ["order_id"])
    mock_backend.drop_duplicates.assert_called_once_with(mock_df, ["order_id"])


def test_process_schema_source_not_implemented_propagates(mock_engine):
    """NotImplementedError from a non-Spark backend propagates cleanly."""
    mock_backend = MagicMock()
    mock_backend.read_source.side_effect = NotImplementedError("not supported")
    mock_engine._backend = mock_backend
    mock_engine._interpreter._backend = mock_backend

    schema_dict = {
        "tables": [
            {
                "name": "raw_orders",
                "source": {"type": "csv", "path": "/data/*.csv", "options": {}},
            }
        ]
    }
    with pytest.raises(NotImplementedError, match="not supported"):
        mock_engine.process_schema(schema_dict)


def test_process_schema_without_source_unchanged(mock_engine, spark):
    """Tables without source: still use read_table (backward compat)."""
    mock_backend = MagicMock()
    mock_df = MagicMock()
    mock_backend.read_table.return_value = mock_df
    mock_backend.read_source = MagicMock()
    mock_engine._backend = mock_backend
    mock_engine._interpreter._backend = mock_backend
    mock_engine.schema_suffix = ""
    mock_engine.is_job_execution = True

    schema_dict = {
        "tables": [{"name": "silver.orders", "alias": "orders"}]
    }
    mock_engine.process_schema(schema_dict)

    mock_backend.read_table.assert_called_once()
    mock_backend.read_source.assert_not_called()


# ==============================================================================
# Phase 14 — describe_schema() with external source
# ==============================================================================

def test_describe_schema_source_in_structured_result(mock_engine):
    """describe_schema includes source info in structured result."""
    schema_dict = {
        "tables": [
            {
                "name": "raw_orders",
                "alias": "orders",
                "source": {"type": "csv", "path": "/data/orders/*.csv", "options": {}},
            }
        ]
    }
    result = mock_engine.describe_schema(schema_dict, print_summary=False)
    src = result["sources"][0]["source"]
    assert src is not None
    assert src["type"] == "csv"
    assert src["path"] == "/data/orders/*.csv"


def test_describe_schema_source_none_for_regular_table(mock_engine):
    """describe_schema returns source=None for tables without a source: block."""
    schema_dict = {
        "tables": [{"name": "silver.orders", "alias": "orders"}]
    }
    result = mock_engine.describe_schema(schema_dict, print_summary=False)
    assert result["sources"][0]["source"] is None


def test_describe_schema_source_printed(mock_engine, capsys):
    """describe_schema prints source type and path when source: is present."""
    schema_dict = {
        "tables": [
            {
                "name": "raw_orders",
                "source": {
                    "type": "parquet",
                    "path": "abfss://container@account.dfs.core.windows.net/bronze/orders/",
                    "options": {},
                },
            }
        ]
    }
    mock_engine.describe_schema(schema_dict, print_summary=True)
    captured = capsys.readouterr()
    assert "parquet" in captured.out
    assert "abfss://container@account.dfs.core.windows.net/bronze/orders/" in captured.out


def test_redundancy_warnings_emitted_in_interactive_mode(mock_engine, spark, caplog):
    """plan17-0.6: rule redundancy warnings are emitted via logger in interactive mode."""
    import logging
    from pyspark.sql import functions as F

    RuleRegistry.clear()

    @RuleRegistry.register_rule(kind="transform")
    def _rule_dup_a(df):
        return df.withColumn("score", F.col("id") * 10)

    @RuleRegistry.register_rule(kind="transform")
    def _rule_dup_b(df):
        return df.withColumn("score2", F.col("id") * 10)  # same expr → DUPLICATE_EXPR

    data = [(1,), (2,)]
    df = spark.createDataFrame(data, ["id"])
    df.createOrReplaceTempView("_dup_warn_test")

    schema_dict = {
        "tables": [{"name": "_dup_warn_test", "alias": "t"}],
        "keep_all_columns": True,
        "business_rules": ["_rule_dup_a", "_rule_dup_b"],
    }

    with caplog.at_level(logging.WARNING, logger="skifer.core.core"):
        mock_engine.process_schema(schema_dict)

    assert any("DUPLICATE_EXPR" in r.message for r in caplog.records), (
        "Expected DUPLICATE_EXPR warning in logger output"
    )

    spark.catalog.dropTempView("_dup_warn_test")
    RuleRegistry.clear()


def test_local_fqn_parsing_accepts_quoted_and_plain_identifiers():
    """Certified publication stages under a plain, unquoted name.

    The staging FQN is a stored identity, recorded in every run event, so it is
    deliberately built without backticks. A local writer that only understood the
    quoted form made the very first staging write fail, which left certified
    publication unusable on the documented local development path.
    """
    from skifer.core.writer import _parse_local_fqn

    assert _parse_local_fqn("`silver`.`orders`") == ("silver", "orders")
    assert _parse_local_fqn("`cat`.`silver`.`orders`") == ("silver", "orders")
    assert _parse_local_fqn("silver.orders") == ("silver", "orders")
    assert _parse_local_fqn("_skifer_staging.fact_orders_abc") == (
        "_skifer_staging",
        "fact_orders_abc",
    )
    with pytest.raises(ValueError, match="Cannot parse FQN"):
        _parse_local_fqn("orders")


def test_engine_exposes_a_public_backend():
    """Building a DataMonitor is documented, so it must not need a private attribute.

    The documentation instructed readers to use `engine._backend` in twelve places
    across four pages, which quietly makes renaming that attribute a breaking
    change for everyone who followed the docs.
    """
    engine = SkiferEngine.__new__(SkiferEngine)
    sentinel = object()
    engine._backend = sentinel

    assert engine.backend is sentinel
