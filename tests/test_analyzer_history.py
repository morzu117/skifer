"""Tests for the Databricks query-history front-end (Plan 43 — 43.7, second
sub-point). Entirely Spark-free and SDK-free: a fake workspace client stands
in for the real Databricks SDK, matching the shape
`SparkBackend.execute_sql_on_warehouse` already relies on for a materialized
view's DDL (Plan 28)."""
from __future__ import annotations

from types import SimpleNamespace

from skifer.analyzer.history import HistoryFetchResult, fetch_query_history


def _fake_client(rows, *, poll_states=None):
    """A minimal Databricks SDK WorkspaceClient double. ``poll_states``, if
    given, simulates one or more PENDING/RUNNING polls before SUCCEEDED."""

    poll_states = list(poll_states or [])

    class _FakeStatementExecution:
        def __init__(self):
            self.get_statement_calls = 0

        def execute_statement(self, *, statement, warehouse_id, wait_timeout):
            self.last_sql = statement
            self.last_warehouse_id = warehouse_id
            state = poll_states[0] if poll_states else "SUCCEEDED"
            return SimpleNamespace(
                statement_id="stmt-1",
                status=SimpleNamespace(state=SimpleNamespace(value=state), error=None),
                result=SimpleNamespace(data_array=rows) if state == "SUCCEEDED" else None,
            )

        def get_statement(self, statement_id):
            self.get_statement_calls += 1
            index = min(self.get_statement_calls, len(poll_states) - 1) if poll_states else 0
            state = poll_states[index] if poll_states else "SUCCEEDED"
            return SimpleNamespace(
                statement_id=statement_id,
                status=SimpleNamespace(state=SimpleNamespace(value=state), error=None),
                result=SimpleNamespace(data_array=rows) if state == "SUCCEEDED" else None,
            )

    return SimpleNamespace(statement_execution=_FakeStatementExecution())


class TestFetchQueryHistory:
    def test_fetches_distinct_statements_as_raw_statements(self):
        client = _fake_client([
            ["id1", "SELECT id FROM silver.orders", "2026-01-01"],
            ["id2", "SELECT id FROM bronze.customers", "2026-01-01"],
        ])

        result = fetch_query_history(client, warehouse_id="wh1", days=7)

        assert isinstance(result, HistoryFetchResult)
        assert [s.text for s in result.raw_statements] == [
            "SELECT id FROM silver.orders",
            "SELECT id FROM bronze.customers",
        ]

    def test_source_path_tags_the_statement_id_never_a_real_file(self):
        client = _fake_client([["id1", "SELECT 1", "2026-01-01"]])

        result = fetch_query_history(client, warehouse_id="wh1", days=7)

        assert result.raw_statements[0].source_path == "system.query.history::id1"

    def test_duplicate_statements_after_whitespace_normalization_are_deduped(self):
        client = _fake_client([
            ["id1", "SELECT id FROM t", "2026-01-01"],
            ["id2", "SELECT   id   FROM   t", "2026-01-01"],
        ])

        result = fetch_query_history(client, warehouse_id="wh1", days=7)

        assert len(result.raw_statements) == 1
        assert result.duplicate_count == 1
        assert result.total_fetched == 2

    def test_duplicate_statements_differing_only_by_case_are_deduped(self):
        client = _fake_client([
            ["id1", "select id from t", "2026-01-01"],
            ["id2", "SELECT ID FROM T", "2026-01-01"],
        ])

        result = fetch_query_history(client, warehouse_id="wh1", days=7)

        assert len(result.raw_statements) == 1

    def test_schema_filter_keeps_only_matching_statement_text(self):
        client = _fake_client([
            ["id1", "SELECT id FROM silver.orders", "2026-01-01"],
            ["id2", "SELECT id FROM bronze.customers", "2026-01-01"],
        ])

        result = fetch_query_history(client, warehouse_id="wh1", days=7, schema="silver")

        assert [s.text for s in result.raw_statements] == ["SELECT id FROM silver.orders"]
        assert result.matched_schema_count == 1
        assert result.total_fetched == 2

    def test_schema_filter_is_case_insensitive(self):
        client = _fake_client([["id1", "SELECT id FROM SILVER.orders", "2026-01-01"]])

        result = fetch_query_history(client, warehouse_id="wh1", days=7, schema="silver")

        assert len(result.raw_statements) == 1

    def test_a_null_statement_text_is_skipped_not_crashed_on(self):
        client = _fake_client([["id1", None, "2026-01-01"]])

        result = fetch_query_history(client, warehouse_id="wh1", days=7)

        assert result.raw_statements == ()
        assert result.total_fetched == 0

    def test_never_selects_an_author_or_user_column(self):
        client = _fake_client([["id1", "SELECT 1", "2026-01-01"]])

        fetch_query_history(client, warehouse_id="wh1", days=7)

        sql = client.statement_execution.last_sql.lower()
        assert "executed_by" not in sql
        assert "user" not in sql

    def test_never_reads_result_data_rows_only_statement_text(self):
        client = _fake_client([["id1", "SELECT 1", "2026-01-01"]])

        fetch_query_history(client, warehouse_id="wh1", days=7)

        sql = client.statement_execution.last_sql.lower()
        assert "result" not in sql

    def test_pending_then_running_then_succeeded_is_polled_to_completion(self):
        client = _fake_client(
            [["id1", "SELECT 1", "2026-01-01"]],
            poll_states=["PENDING", "RUNNING", "SUCCEEDED"],
        )

        result = fetch_query_history(client, warehouse_id="wh1", days=7)

        assert len(result.raw_statements) == 1
        assert client.statement_execution.get_statement_calls == 2

    def test_a_failed_statement_raises_with_the_error_message(self):
        import pytest

        class _FailingStatementExecution:
            def execute_statement(self, **kwargs):
                return SimpleNamespace(
                    statement_id="stmt-1",
                    status=SimpleNamespace(
                        state=SimpleNamespace(value="FAILED"),
                        error=SimpleNamespace(message="warehouse unreachable"),
                    ),
                    result=None,
                )

        client = SimpleNamespace(statement_execution=_FailingStatementExecution())

        with pytest.raises(RuntimeError, match="warehouse unreachable"):
            fetch_query_history(client, warehouse_id="wh1", days=7)

    def test_days_is_embedded_in_the_interval_clause(self):
        client = _fake_client([["id1", "SELECT 1", "2026-01-01"]])

        fetch_query_history(client, warehouse_id="wh1", days=30)

        assert "INTERVAL 30 DAYS" in client.statement_execution.last_sql


class TestBuildWorkspaceClient:
    def test_missing_sdk_raises_a_named_import_error(self, monkeypatch):
        import builtins

        import pytest

        from skifer.analyzer.history import build_workspace_client

        real_import = builtins.__import__

        def blocking_import(name, *args, **kwargs):
            if name == "databricks.sdk" or name.startswith("databricks.sdk."):
                raise ImportError("simulated: databricks-sdk not installed")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", blocking_import)

        with pytest.raises(ImportError, match="databricks"):
            build_workspace_client()
