"""Public Board library contract across the two supported SQL dialects."""

from __future__ import annotations

import duckdb
import pytest
import yaml

from skifer.agentic.resolver import SemanticQuery
from skifer.semantic.access_policy import (
    CertificationDecision,
    ConsumerContext,
    SemanticAccessDenied,
)
from skifer.semantic.evidence import hash_sql
from skifer.semantic.semantic import SemanticEngine


MODEL = {
    "key": "orders",
    "name": "orders",
    "table": "skifer_board_contract.orders",
    # The OR case proves the planner parenthesizes the curated predicate
    # before appending the mandatory tenant restriction.
    "base_filter": "active = true OR region = 'South'",
    "row_policy": {"column": "tenant", "attribute": "tenant"},
    "dimensions": [{"name": "region", "sql": "region", "type": "string"}],
    "metrics": [{"name": "revenue", "sql": "amount", "type": "sum"}],
}


def _models_dir(tmp_path):
    models_dir = tmp_path / "semantic_models"
    models_dir.mkdir()
    (models_dir / "orders.yaml").write_text(
        yaml.safe_dump({"models": [MODEL]}), encoding="utf-8"
    )
    (models_dir / "semantic_catalog.yaml").write_text(
        yaml.safe_dump({"models": [{
            "key": "orders", "file": "orders.yaml",
            "dimensions": ["region"], "metrics": ["revenue"],
        }]}), encoding="utf-8"
    )
    return str(models_dir)


class DuckBackend:
    def __init__(self):
        self.connection = duckdb.connect(":memory:")
        self.sql = []
        self.connection.execute("CREATE SCHEMA skifer_board_contract")
        self.connection.execute(
            "CREATE TABLE skifer_board_contract.orders "
            "(tenant VARCHAR, region VARCHAR, amount INTEGER, active BOOLEAN)"
        )
        self.connection.execute(
            "INSERT INTO skifer_board_contract.orders VALUES "
            "('A', 'North', 12, true), ('A', 'South', 5, false), "
            "('B', 'North', 100, true), ('B', 'South', 7, false)"
        )

    def execute_sql(self, sql):
        self.sql.append(sql)
        return self.connection.execute(sql).fetchall()


class SparkBackend:
    def __init__(self, spark):
        self.spark = spark
        self.sql = []

    def execute_sql(self, sql):
        self.sql.append(sql)
        return self.spark.sql(sql)


def _rows(result):
    value = result.dataframe
    if hasattr(value, "collect"):
        value = value.collect()
    return sorted(tuple(row) for row in value)


def test_board_library_contract_is_same_on_spark_and_duckdb(tmp_path, spark):
    models_dir = _models_dir(tmp_path)
    spark.sql("CREATE DATABASE IF NOT EXISTS skifer_board_contract")
    spark.sql(
        "CREATE OR REPLACE VIEW skifer_board_contract.orders AS "
        "SELECT 'A' AS tenant, 'North' AS region, 12 AS amount, true AS active "
        "UNION ALL SELECT 'A', 'South', 5, false "
        "UNION ALL SELECT 'B', 'North', 100, true "
        "UNION ALL SELECT 'B', 'South', 7, false"
    )
    duck_backend = DuckBackend()
    spark_backend = SparkBackend(spark)
    try:
        runtimes = (
            SemanticEngine.from_backend(
                duck_backend, models_dir=models_dir, dialect="duckdb"
            ),
            SemanticEngine.from_backend(
                spark_backend, models_dir=models_dir, dialect="databricks"
            ),
        )
        query = SemanticQuery("orders", metrics=["revenue"], group_by=["region"])
        with_filter = SemanticQuery(
            "orders", metrics=["revenue"], group_by=["region"],
            filters=[{"column": "region", "operator": "eq", "value": "North"}],
        )
        alice = ConsumerContext(
            "alice", "dashboard", user_id="alice", attributes={"tenant": "A"}
        )
        bob = ConsumerContext(
            "bob", "dashboard", user_id="bob", attributes={"tenant": "B"}
        )
        admin = ConsumerContext(
            "admin", "platform_admin", user_id="admin", attributes={"tenant": "A"}
        )
        for runtime, backend in zip(runtimes, (duck_backend, spark_backend), strict=True):
            alice_result = runtime.query_with_evidence(query, consumer_context=alice)
            bob_result = runtime.query_with_evidence(query, consumer_context=bob)
            admin_result = runtime.query_with_evidence(query, consumer_context=admin)
            filtered = runtime.query_with_evidence(with_filter, consumer_context=alice)
            assert _rows(alice_result) == [("North", 12), ("South", 5)]
            assert _rows(bob_result) == [("North", 100), ("South", 7)]
            assert _rows(admin_result) == _rows(alice_result)
            assert _rows(filtered) == [("North", 12)]
            assert alice_result.evidence.sql_hash == hash_sql(backend.sql[0])
            assert alice_result.evidence.sql_hash != bob_result.evidence.sql_hash
            assert alice_result.evidence.sql_dialect == runtime.dialect
            assert all("tenant" in sql for sql in backend.sql)
            assert "'A'" not in str(alice_result.evidence.to_dict())

            before = len(backend.sql)
            with pytest.raises(SemanticAccessDenied) as denial:
                runtime.query_with_evidence(
                    query,
                    consumer_context=ConsumerContext("missing", "platform_admin"),
                )
            assert denial.value.decision == CertificationDecision.DENY
            assert len(backend.sql) == before
    finally:
        duck_backend.connection.close()
        spark.sql("DROP VIEW IF EXISTS skifer_board_contract.orders")
        spark.sql("DROP DATABASE IF EXISTS skifer_board_contract")


@pytest.mark.parametrize(
    ("mode", "decision"),
    [("enforce", CertificationDecision.DENY),
     ("supervised", CertificationDecision.REQUIRE_HUMAN)],
)
def test_board_certification_refusal_never_calls_connector(tmp_path, mode, decision):
    models_dir = _models_dir(tmp_path)
    backend = DuckBackend()
    try:
        runtime = SemanticEngine.from_backend(
            backend,
            models_dir=models_dir,
            dialect="duckdb",
            config={"environments": {"dev": {"semantic_certification_policy": mode}}},
        )
        reader = ConsumerContext(
            "alice", "dashboard", user_id="alice", attributes={"tenant": "A"}
        )
        with pytest.raises(SemanticAccessDenied) as denial:
            runtime.query_with_evidence(
                SemanticQuery("orders", metrics=["revenue"]),
                consumer_context=reader,
            )
        assert denial.value.decision == decision
        assert denial.value.datasets == ("skifer_board_contract.orders",)
        assert backend.sql == []
    finally:
        backend.connection.close()
