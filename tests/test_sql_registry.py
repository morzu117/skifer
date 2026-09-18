"""Generic SQL registry storage above the thin Adapter Protocol."""

from __future__ import annotations

import inspect
import subprocess
import sys

import pytest

from skifer.core.adapters import Adapter
from skifer.core.adapters.duckdb import DuckDBAdapter
from skifer.observability.sql_registry import (
    CHECK_RESULTS,
    CONTRACT_DEFINITIONS,
    INCIDENTS,
    MATERIALIZATION_RUNS,
    SEMANTIC_USAGE_EVENTS,
    TABLE_DEFINITIONS,
    SqlRegistry,
)


@pytest.fixture
def sql_registry():
    duckdb = pytest.importorskip("duckdb")
    connection = duckdb.connect()
    try:
        yield SqlRegistry(DuckDBAdapter(connection), "registry_test")
    finally:
        connection.close()


ROWS = {
    CONTRACT_DEFINITIONS: {
        "contract_id": "orders",
        "contract_version": "1.2.3",
        "definition_hash": "sha256:contract",
        "canonical_json": '{"contract":"orders"}',
        "data_product_id": "orders_product",
        "owner": None,
        "hash_algorithm": "sha256",
        "canonicalization_version": 2,
        "status": "DRAFT",
        "created_at": "2026-09-18T08:00:00+00:00",
    },
    MATERIALIZATION_RUNS: {
        "event_id": "event-run-1",
        "run_id": "run-1",
        "dataset": "gold.orders",
        "state": "PROMOTED",
        "contract_id": "orders",
        "contract_version": "1.2.3",
        "definition_hash": "sha256:contract",
        "occurred_at": "2026-09-18T08:01:00+00:00",
        "target_fqn": "gold.orders",
        "staging_fqn": "_skifer_staging.orders",
        "quarantine_fqn": None,
    },
    CHECK_RESULTS: {
        "event_id": "event-check-1",
        "run_id": "run-1",
        "check_type": "not_null",
        "scope": "ROW",
        "severity": "critical",
        "status": "PASS",
        "actual_value": "0",
        "expected_value": None,
        "message": "No null order identifiers",
    },
    INCIDENTS: {
        "id": "incident-1",
        "target_fqn": "gold.orders",
        "run_id": "run-1",
        "check_name": "NotNullCheck:order_id",
        "severity": "critical",
        "status": "NEW",
        "opened_at": "2026-09-18T08:02:00+00:00",
        "assignee": None,
        "root_cause": None,
        "resolved_at": None,
    },
    SEMANTIC_USAGE_EVENTS: {
        "event_id": "usage-1",
        "occurred_at": "2026-09-18T08:03:00+00:00",
        "environment": "prod",
        "consumer_class": "analyst",
        "model_hashes": '["sha256:model"]',
        "metric_ids": '["revenue"]',
        "dimension_ids": '["region"]',
        "normalized_filter_shape": '["region:eq"]',
        "query_fingerprint": "sha256:query",
        "duration_ms": 25,
        "rows_returned": 4,
        "bytes_scanned": None,
        "status": "succeeded",
    },
}


@pytest.mark.parametrize("definition", TABLE_DEFINITIONS)
def test_each_definition_is_created_written_and_read_value_for_value(
    sql_registry, definition
):
    row = ROWS[definition]

    sql_registry.ensure_table(definition)
    sql_registry.append(definition, row)

    assert sql_registry.find(
        definition, where={definition.key[0]: row[definition.key[0]]}
    ) == [row]


def test_adverse_values_round_trip_and_cannot_execute_sql(sql_registry):
    sql_registry.ensure_table(MATERIALIZATION_RUNS)
    values = (
        "l'équipe",
        "semi;colon",
        "comment -- still data",
        "'); DROP TABLE registry_test.materialization_runs; --",
    )
    for index, value in enumerate(values):
        row = dict(ROWS[CHECK_RESULTS])
        row["event_id"] = f"adverse-{index}"
        row["message"] = value
        sql_registry.append(CHECK_RESULTS, row)

    reread = sql_registry.find(CHECK_RESULTS, order_by=(("event_id", "ASC"),))

    assert [row["message"] for row in reread] == list(values)
    assert sql_registry.adapter.table_exists(
        None, "registry_test", MATERIALIZATION_RUNS.name
    )


def test_upsert_same_key_twice_keeps_only_latest_row(sql_registry):
    first = dict(ROWS[INCIDENTS])
    latest = {
        **first,
        "status": "RESOLVED",
        "root_cause": "source repaired",
        "resolved_at": "2026-09-18T09:00:00+00:00",
    }

    sql_registry.upsert(INCIDENTS, first)
    sql_registry.upsert(INCIDENTS, latest)

    assert sql_registry.find(INCIDENTS, where={"id": first["id"]}) == [latest]


def test_multiline_reads_are_explicitly_and_repeatably_sorted(sql_registry):
    rows = []
    for event_id, occurred_at in (
        ("event-b", "2026-09-18T08:00:00+00:00"),
        ("event-a", "2026-09-18T08:00:00+00:00"),
        ("event-c", "2026-09-18T09:00:00+00:00"),
    ):
        row = dict(ROWS[MATERIALIZATION_RUNS])
        row.update(event_id=event_id, occurred_at=occurred_at)
        rows.append(row)
        sql_registry.append(MATERIALIZATION_RUNS, row)

    kwargs = {
        "where": {"dataset": "gold.orders", "state": "PROMOTED"},
        "order_by": (("occurred_at", "DESC"),),
        "limit": 3,
    }
    first = sql_registry.find(MATERIALIZATION_RUNS, **kwargs)
    second = sql_registry.find(MATERIALIZATION_RUNS, **kwargs)

    assert first == second
    assert [row["event_id"] for row in first] == ["event-c", "event-a", "event-b"]


def test_ensure_table_is_idempotent(sql_registry):
    sql_registry.ensure_table(CONTRACT_DEFINITIONS)
    sql_registry.ensure_table(CONTRACT_DEFINITIONS)

    assert sql_registry.adapter.list_relation_columns(
        '"registry_test"."contract_definitions"'
    ) == list(CONTRACT_DEFINITIONS.column_names)


def test_adapter_protocol_remains_frozen_at_twenty_public_members():
    public_members = {
        name for name, _ in inspect.getmembers(Adapter) if not name.startswith("_")
    }

    assert len(public_members) == 20


def test_sql_registry_imports_when_pyspark_is_unavailable():
    script = """
import builtins
real_import = builtins.__import__
def guarded(name, *args, **kwargs):
    if name == 'pyspark' or name.startswith('pyspark.'):
        raise AssertionError(f'unexpected Spark import: {name}')
    return real_import(name, *args, **kwargs)
builtins.__import__ = guarded
import skifer.observability.sql_registry
"""

    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        check=False,
        text=True,
    )

    assert result.returncode == 0, result.stderr
