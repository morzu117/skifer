"""Staging identity tests for Plan 29 certified publication."""
from skifer.observability.certification import ContractDefinition
from skifer.observability.certification_store import RunEvent, SqliteCertificationStore, StoredCheckResult
from skifer.observability.monitor import MonitorReport
from skifer.observability.publication import PublicationCoordinator, PublicationResult
from skifer.observability.publication import RunState
from skifer.observability.publication import start_publication_run
from skifer.observability.publication import stage_dataframe
from skifer.observability.publication import promote_staging
from skifer.observability.quarantine import quarantine_staging
from skifer.observability.quarantine import row_violation_predicates, validate_reserved_columns
from skifer.observability.checks import CheckResult, CheckStatus, ContractScope, NullCheck
from datetime import datetime, timezone
import pytest
from tests.fakes.fake_backend import FakeBackend, FakeDataFrame


class _Monitor:
    def __init__(self, result_factory):
        self.result_factory = result_factory
        self.checked_fqns = []

    def check_from_schema(self, fqn, schema_dict, raise_on_critical=False):
        self.checked_fqns.append((fqn, raise_on_critical))
        return MonitorReport(fqn, [self.result_factory(fqn)])


class _SequenceMonitor:
    def __init__(self, result_factories):
        self.result_factories = list(result_factories)

    def check_from_schema(self, fqn, schema_dict, raise_on_critical=False):
        result_factory = self.result_factories.pop(0)
        return MonitorReport(fqn, [result_factory(fqn)])


def _definition():
    return ContractDefinition("sales.orders", "1.0.0", "hash", "{}", "sales.orders", None)


def _pass_result(fqn):
    return CheckResult(
        NullCheck(table=fqn, column="id", severity="critical"),
        status=CheckStatus.PASS,
        actual_value=0,
        expected_value=0,
        message="ok",
        severity="critical",
    )


def _fail_result(fqn):
    return CheckResult(
        NullCheck(table=fqn, column="id", severity="critical"),
        status=CheckStatus.FAIL,
        actual_value=1,
        expected_value=0,
        message="null id",
        severity="critical",
    )


def test_staging_run_is_safe_and_persisted():
    definition = _definition()
    store = SqliteCertificationStore(":memory:")
    run = start_publication_run("main.gold.orders", definition, store, "123e4567-e89b-12d3-a456-426614174000")
    assert run.staging_fqn == "main._skifer_staging.orders_123e4567e89b12d3a456426614174000"
    assert store.get_run(run.run_id).state == "STARTED"


def test_staging_run_accepts_backend_quoted_target_fqn():
    definition = _definition()
    store = SqliteCertificationStore(":memory:")
    run = start_publication_run(
        "`main`.`gold`.`orders`",
        definition,
        store,
        "123e4567-e89b-12d3-a456-426614174000",
    )

    assert run.target_fqn == "main.gold.orders"
    assert run.staging_fqn == "main._skifer_staging.orders_123e4567e89b12d3a456426614174000"


def test_staging_write_records_staged_run():
    definition = _definition()
    store, backend = SqliteCertificationStore(":memory:"), FakeBackend()
    run = start_publication_run("gold.orders", definition, store)
    staged = stage_dataframe(backend, run, definition, store, FakeDataFrame([{"id": 1}]))
    assert staged.state.value == "STAGED"
    assert backend.read_staging(staged.staging_fqn)._rows[0]["id"] == 1


def test_quarantine_snapshots_before_removing_staging():
    definition = _definition()
    store, backend = SqliteCertificationStore(":memory:"), FakeBackend()
    run = stage_dataframe(backend, start_publication_run("gold.orders", definition, store), definition, store, FakeDataFrame([{"id": 1}]))
    result = quarantine_staging(backend, run, definition, store)
    assert result.state == "QUARANTINED"
    assert backend.read_staging(result.quarantine_fqn)._rows[0]["id"] == 1


def test_row_quarantine_only_keeps_failed_row_predicates_and_reserves_columns():
    result = CheckResult(NullCheck(table="t", column="id"), passed=False, message="null")
    assert row_violation_predicates([result]) == {"NullCheck:0": "`id` IS NULL"}
    with pytest.raises(ValueError, match="_violations"):
        validate_reserved_columns(["id", "_violations"])


def test_promotion_is_idempotent_after_staged_write():
    definition = _definition()
    store, backend = SqliteCertificationStore(":memory:"), FakeBackend()
    run = stage_dataframe(backend, start_publication_run("gold.orders", definition, store), definition, store, FakeDataFrame([{"id": 1}]))
    assert promote_staging(backend, run, definition, store).state.value == "PROMOTED"
    assert promote_staging(backend, run, definition, store).state.value == "PROMOTED"


def test_promotion_resumes_run_recorded_as_promoting_after_crash():
    definition = _definition()
    store, backend = SqliteCertificationStore(":memory:"), FakeBackend()
    coordinator = PublicationCoordinator(backend, _Monitor(_pass_result), store)
    run = stage_dataframe(
        backend,
        start_publication_run("gold.orders", definition, store),
        definition,
        store,
        FakeDataFrame([{"id": 1}]),
    )
    backend._written.pop("gold.orders", None)
    store.append_run_event(
        RunEvent(
            f"{run.run_id}:PROMOTING",
            run.run_id,
            run.target_fqn,
            RunState.PROMOTING.value,
            definition.contract_id,
            definition.contract_version,
            definition.definition_hash,
            datetime.now(timezone.utc),
            target_fqn=run.target_fqn,
            staging_fqn=run.staging_fqn,
        )
    )

    resumed = coordinator.resume(run, definition)

    assert resumed.state == "PROMOTED"
    assert resumed.report is None
    assert store.get_run(run.run_id).state == "PROMOTED"
    assert backend._written["gold.orders"] == [{"id": 1}]


def test_promotion_fast_path_drops_orphaned_staging_without_rewriting_target(monkeypatch):
    definition = _definition()
    store, backend = SqliteCertificationStore(":memory:"), FakeBackend()
    run = stage_dataframe(
        backend,
        start_publication_run("gold.orders", definition, store),
        definition,
        store,
        FakeDataFrame([{"id": 1}]),
    )

    assert promote_staging(backend, run, definition, store).state.value == "PROMOTED"

    write_calls = []
    original_write_table = backend.write_table

    def recording_write_table(df, fqn, mode="overwrite"):
        write_calls.append((fqn, mode, list(df._rows)))
        return original_write_table(df, fqn, mode)

    monkeypatch.setattr(backend, "write_table", recording_write_table)
    backend._tables[run.staging_fqn] = [{"id": 99}]
    dropped_before = len(backend._dropped)

    promoted = promote_staging(backend, run, definition, store)

    assert promoted.state.value == "PROMOTED"
    assert write_calls == []
    assert backend._written["gold.orders"] == [{"id": 1}]
    assert backend._dropped[dropped_before:] == [run.staging_fqn]


def test_promotion_allows_staged_run_without_recorded_checks():
    definition = _definition()
    store, backend = SqliteCertificationStore(":memory:"), FakeBackend()
    run = stage_dataframe(backend, start_publication_run("gold.orders", definition, store), definition, store, FakeDataFrame([{"id": 1}]))

    promoted = promote_staging(backend, run, definition, store)

    assert promoted.state.value == "PROMOTED"
    assert backend._written["gold.orders"] == [{"id": 1}]


def test_promotion_rejects_recorded_critical_check_failures_before_target_write():
    definition = _definition()
    store, backend = SqliteCertificationStore(":memory:"), FakeBackend()
    run = stage_dataframe(backend, start_publication_run("gold.orders", definition, store), definition, store, FakeDataFrame([{"id": 1}]))
    store.append_check_results([
        StoredCheckResult(
            "check-1",
            run.run_id,
            "NullCheck",
            ContractScope.ROW,
            "critical",
            CheckStatus.FAIL,
            "1",
            "0",
            "null",
        )
    ])

    with pytest.raises(ValueError, match="recorded critical check failures block promotion"):
        promote_staging(backend, run, definition, store)

    assert "gold.orders" not in backend._written


def test_promotion_allows_warning_check_failures():
    definition = _definition()
    store, backend = SqliteCertificationStore(":memory:"), FakeBackend()
    run = stage_dataframe(backend, start_publication_run("gold.orders", definition, store), definition, store, FakeDataFrame([{"id": 1}]))
    store.append_check_results([
        StoredCheckResult(
            "check-1",
            run.run_id,
            "NullCheck",
            ContractScope.ROW,
            "warning",
            CheckStatus.FAIL,
            "1",
            "0",
            "null",
        )
    ])

    promoted = promote_staging(backend, run, definition, store)

    assert promoted.state.value == "PROMOTED"
    assert backend._written["gold.orders"] == [{"id": 1}]


def test_publication_events_and_certification_key_on_physical_target_fqn():
    target_fqn = "gold.fact_orders"
    definition = _definition()
    store, backend = SqliteCertificationStore(":memory:"), FakeBackend()

    run = start_publication_run(target_fqn, definition, store)
    assert store.get_run(run.run_id).dataset == target_fqn

    run = stage_dataframe(backend, run, definition, store, FakeDataFrame([{"id": 1}]))
    assert store.get_run(run.run_id).dataset == target_fqn

    run = promote_staging(backend, run, definition, store)
    assert store.get_run(run.run_id).dataset == target_fqn
    assert store.get_certification(target_fqn, "default").status == "CERTIFIED"


def test_quarantine_event_keys_on_physical_target_fqn():
    target_fqn = "gold.fact_orders"
    definition = _definition()
    store, backend = SqliteCertificationStore(":memory:"), FakeBackend()
    run = stage_dataframe(
        backend,
        start_publication_run(target_fqn, definition, store),
        definition,
        store,
        FakeDataFrame([{"id": 1}]),
    )

    quarantine_staging(backend, run, definition, store)

    assert store.get_run(run.run_id).dataset == target_fqn


def test_publication_coordinator_promotes_clean_staging():
    store, backend = SqliteCertificationStore(":memory:"), FakeBackend()
    monitor = _Monitor(_pass_result)
    coordinator = PublicationCoordinator(backend, monitor, store)

    result = coordinator.publish(FakeDataFrame([{"id": 1}]), "gold.orders", {}, _definition())

    assert isinstance(result, PublicationResult)
    assert result.state == "PROMOTED"
    assert backend._written["gold.orders"] == [{"id": 1}]
    assert monitor.checked_fqns == [(result.run.staging_fqn, False)]
    assert store.get_check_results(result.run.run_id)[0].status is CheckStatus.PASS


def test_publication_coordinator_quarantines_critical_failure_without_target_write():
    store, backend = SqliteCertificationStore(":memory:"), FakeBackend()
    coordinator = PublicationCoordinator(backend, _Monitor(_fail_result), store)

    result = coordinator.publish(FakeDataFrame([{"id": None}]), "gold.orders", {}, _definition())

    assert result.state == "QUARANTINED"
    assert "gold.orders" not in backend._written
    assert store.get_run(result.run.run_id).state == "QUARANTINED"


def test_publication_coordinator_propagates_check_error_instead_of_quarantined():
    class _BrokenWriteBackend(FakeBackend):
        def write_staging(self, df, fqn):
            if "_skifer_quarantine" in fqn:
                raise RuntimeError("permission denied on quarantine schema")
            super().write_staging(df, fqn)

    store, backend = SqliteCertificationStore(":memory:"), _BrokenWriteBackend()
    coordinator = PublicationCoordinator(backend, _Monitor(_fail_result), store)

    result = coordinator.publish(FakeDataFrame([{"id": None}]), "gold.orders", {}, _definition())

    assert result.state == "CHECK_ERROR"
    assert store.get_run(result.run.run_id).state == "CHECK_ERROR"


def test_publication_coordinator_keeps_previous_target_after_failed_middle_run():
    store, backend = SqliteCertificationStore(":memory:"), FakeBackend()
    monitor = _SequenceMonitor([_pass_result, _fail_result, _pass_result])
    coordinator = PublicationCoordinator(backend, monitor, store)

    v1 = coordinator.publish(FakeDataFrame([{"id": 1, "version": "v1"}]), "gold.orders", {}, _definition())
    v2 = coordinator.publish(FakeDataFrame([{"id": None, "version": "v2"}]), "gold.orders", {}, _definition())
    assert backend._written["gold.orders"] == [{"id": 1, "version": "v1"}]
    v3 = coordinator.publish(FakeDataFrame([{"id": 3, "version": "v3"}]), "gold.orders", {}, _definition())

    assert v1.state == "PROMOTED"
    assert v2.state == "QUARANTINED"
    assert v3.state == "PROMOTED"
    assert backend._written["gold.orders"] == [{"id": 3, "version": "v3"}]
    assert store.get_run(v2.run.run_id).state == "QUARANTINED"
