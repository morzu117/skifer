"""Row-level quarantine tagging tests for Plan 29 review F09."""
import pytest

from skifer.core.spark_backend import SparkBackend
from skifer.observability.certification import ContractDefinition
from skifer.observability.certification_store import SqliteCertificationStore
from skifer.observability.checks import CheckResult, CheckStatus, NullCheck, VolumeCheck
from skifer.observability.publication import start_publication_run, stage_dataframe
from skifer.observability.quarantine import quarantine_staging
from tests.fakes.fake_backend import FakeBackend, FakeDataFrame


def _definition():
    return ContractDefinition("sales.orders", "1.0.0", "hash", "{}", "sales.orders", None)


def _failed_null_result(table: str = "t") -> CheckResult:
    return CheckResult(
        NullCheck(table=table, column="id", severity="critical"),
        status=CheckStatus.FAIL,
        actual_value=1,
        expected_value=0,
        message="null id",
        severity="critical",
    )


def _failed_volume_result(table: str = "t") -> CheckResult:
    return CheckResult(
        VolumeCheck(table=table, min_rows=10, severity="critical"),
        status=CheckStatus.FAIL,
        actual_value=2,
        expected_value=">= 10",
        message="too few rows",
        severity="critical",
    )


def test_quarantine_row_results_reject_reserved_column_collision_before_write():
    definition = _definition()
    store, backend = SqliteCertificationStore(":memory:"), FakeBackend()
    run = stage_dataframe(
        backend,
        start_publication_run("gold.orders", definition, store),
        definition,
        store,
        FakeDataFrame([{"id": None, "_violations": "existing"}]),
    )

    with pytest.raises(ValueError, match="_violations"):
        quarantine_staging(backend, run, definition, store, results=[_failed_null_result()])

    assert all("_skifer_quarantine" not in fqn for fqn in backend._written)
    assert backend._dropped == []


def test_quarantine_row_results_tag_snapshot_with_violation_metadata():
    definition = _definition()
    store, backend = SqliteCertificationStore(":memory:"), FakeBackend()
    run = stage_dataframe(
        backend,
        start_publication_run("gold.orders", definition, store),
        definition,
        store,
        FakeDataFrame([{"id": None, "value": "bad"}, {"id": 1, "value": "ok"}]),
    )

    result = quarantine_staging(backend, run, definition, store, results=[_failed_null_result()])

    rows = backend.read_staging(result.quarantine_fqn)._rows
    assert rows == [
        {
            "id": None,
            "value": "bad",
            "_violations": "NullCheck:0",
            "_run_id": run.run_id,
            "_contract_version": definition.contract_version,
        },
        {
            "id": 1,
            "value": "ok",
            "_violations": "",
            "_run_id": run.run_id,
            "_contract_version": definition.contract_version,
        },
    ]


def test_quarantine_dataset_only_results_keep_untagged_full_snapshot():
    definition = _definition()
    store, backend = SqliteCertificationStore(":memory:"), FakeBackend()
    rows = [{"id": 1}, {"id": 2}]
    run = stage_dataframe(
        backend,
        start_publication_run("gold.orders", definition, store),
        definition,
        store,
        FakeDataFrame(rows),
    )

    result = quarantine_staging(backend, run, definition, store, results=[_failed_volume_result()])

    assert backend.read_staging(result.quarantine_fqn)._rows == rows


def test_spark_backend_tag_row_violations_evaluates_sql_predicates(spark):
    backend = SparkBackend(spark=spark, is_local=True)
    df = spark.createDataFrame(
        [(1, None), (2, "present")],
        "row_id int, id string",
    )

    tagged = backend.tag_row_violations(
        df,
        {"NullCheck:0": NullCheck(table="t", column="id").violation_predicate()},
        "run-1",
        "1.0.0",
    )

    rows = [
        row.asDict()
        for row in tagged.select("row_id", "_violations", "_run_id", "_contract_version")
        .orderBy("row_id")
        .collect()
    ]
    assert rows == [
        {
            "row_id": 1,
            "_violations": "NullCheck:0",
            "_run_id": "run-1",
            "_contract_version": "1.0.0",
        },
        {
            "row_id": 2,
            "_violations": "",
            "_run_id": "run-1",
            "_contract_version": "1.0.0",
        },
    ]
