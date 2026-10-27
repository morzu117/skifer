"""Databricks query-history front-end for the project analyzer (Plan 43 —
43.7, second sub-point, tracked separately from the notebook front-ends):
feeds ``system.query.history`` statement text into the same pipeline as a
``.sql`` file or a notebook, via the Databricks SDK's Statement Execution
API (the same SDK surface ``SparkBackend.execute_sql_on_warehouse`` already
uses for a materialized view's DDL — Plan 28).

Needs the optional ``[databricks]`` extra (``databricks-sdk``), imported
lazily inside the functions that need it, never at module import time: the
rest of the analyzer package, and this module's own dataclasses, stay
importable without it.

**Never reads who ran a query.** The fetch query selects only
``statement_id``, ``statement_text`` and ``start_time`` — an author/user
column is never selected, so there is no identity to omit from a later
report; it was never read in the first place. **Never reads result data**
either: a query's text is read, never the rows it produced.

``schema``, when given, filters on the fetched statement text itself (a
case-insensitive substring check), not on a ``WHERE`` clause pushed into
``system.query.history``: that system table's documented stable columns are
``statement_text``/``start_time``/``statement_id``; this module does not
assume a particular lineage-column name for which schema or table a
statement touched, which is not part of that stable contract.
"""
from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass, field

from skifer.analyzer.frontends import RawStatement

_VERSION_PREFIX = "sha256:v1:"

HISTORY_SOURCE = "system.query.history"

#: Poll cadence and ceiling for the history fetch statement — a short,
#: simple SELECT, not a materialized-view-creating pipeline, so this is far
#: tighter than `SparkBackend`'s own MV polling budget.
POLL_INTERVAL_SECONDS: float = 2.0
POLL_TIMEOUT_SECONDS: float = 120.0


def _statement_fingerprint(text: str) -> str:
    normalized = " ".join(text.split()).strip().lower()
    return _VERSION_PREFIX + hashlib.sha256(normalized.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class HistoryFetchResult:
    """Never carries a query's result data or who ran it — only counts and
    the deduplicated statement text itself, the same thing a ``.sql`` file
    would have given this pipeline."""

    raw_statements: tuple[RawStatement, ...] = field(default_factory=tuple)
    total_fetched: int = 0
    matched_schema_count: int = 0
    duplicate_count: int = 0

    def to_dict(self) -> dict:
        return {
            "total_fetched": self.total_fetched,
            "matched_schema_count": self.matched_schema_count,
            "duplicate_count": self.duplicate_count,
            "distinct_statements": len(self.raw_statements),
        }


def _await_statement(workspace_client, response) -> list[list]:
    """Poll a Statement Execution API response until a terminal state;
    raise on anything but SUCCEEDED. A standalone copy of
    ``SparkBackend._await_statement``'s own polling loop — not imported from
    there, since ``spark_backend.py`` imports ``pyspark`` at module level
    and this package stays importable (and testable) without it.
    """
    deadline = time.monotonic() + POLL_TIMEOUT_SECONDS
    statement_id = getattr(response, "statement_id", None)

    while True:
        status = getattr(response, "status", None)
        state = getattr(status, "state", None)
        name = str(getattr(state, "value", state) or "").upper()

        if name in ("PENDING", "RUNNING"):
            if time.monotonic() > deadline:
                raise RuntimeError(
                    f"[analyze history] Statement {statement_id} still {name} after "
                    f"{POLL_TIMEOUT_SECONDS:.0f}s."
                )
            time.sleep(POLL_INTERVAL_SECONDS)
            response = workspace_client.statement_execution.get_statement(statement_id)
            continue

        if name in ("SUCCEEDED", ""):
            result = getattr(response, "result", None)
            return list(getattr(result, "data_array", None) or [])

        error = getattr(status, "error", None)
        message = getattr(error, "message", None) or str(error or "no error detail")
        raise RuntimeError(f"[analyze history] Statement {statement_id} ended in state {name}: {message}")


def fetch_query_history(
    workspace_client,
    *,
    warehouse_id: str,
    days: int,
    schema: str | None = None,
) -> HistoryFetchResult:
    """Fetch recent statement text from ``system.query.history`` and dedupe
    it by content fingerprint, returning it as :class:`RawStatement`\\ s
    ready for the same ``sqlglot`` parsing every other front-end uses.
    """
    sql = (
        "SELECT statement_id, statement_text, start_time "
        "FROM system.query.history "
        f"WHERE start_time >= current_timestamp() - INTERVAL {int(days)} DAYS "
        "AND statement_text IS NOT NULL"
    )
    response = workspace_client.statement_execution.execute_statement(
        statement=sql, warehouse_id=warehouse_id, wait_timeout="30s",
    )
    rows = _await_statement(workspace_client, response)

    seen_fingerprints: set[str] = set()
    raw_statements: list[RawStatement] = []
    total = 0
    matched_schema = 0

    for row in rows:
        statement_id, statement_text = row[0], row[1]
        if not statement_text:
            continue
        total += 1
        if schema and schema.lower() not in statement_text.lower():
            continue
        matched_schema += 1

        fingerprint = _statement_fingerprint(statement_text)
        if fingerprint in seen_fingerprints:
            continue
        seen_fingerprints.add(fingerprint)
        raw_statements.append(
            RawStatement(
                source_path=f"{HISTORY_SOURCE}::{statement_id}",
                line=1,
                text=statement_text.strip(),
            )
        )

    return HistoryFetchResult(
        raw_statements=tuple(raw_statements),
        total_fetched=total,
        matched_schema_count=matched_schema if schema else total,
        duplicate_count=matched_schema - len(raw_statements) if schema else total - len(raw_statements),
    )


def build_workspace_client(*, profile: str | None = None):
    """Lazily import and construct a Databricks SDK ``WorkspaceClient``.

    Raises:
        ImportError: ``databricks-sdk`` is not installed — names the
            ``[databricks]`` extra, same convention as the analyzer
            package's own ``AnalyzerDependencyError`` for ``sqlglot``.
    """
    try:
        from databricks.sdk import WorkspaceClient
    except ImportError as exc:
        raise ImportError(
            'Databricks query history requires the optional dependency: `pip install -e ".[databricks]"`.'
        ) from exc
    return WorkspaceClient(profile=profile) if profile else WorkspaceClient()
