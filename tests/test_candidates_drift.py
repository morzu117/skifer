"""Usage drift signals — observation only (Plan 44, S07)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from skifer.adaptive import SemanticUsageEvent, fingerprint_query
from skifer.adaptive.store import SqliteUsageEventStore
from skifer.candidates import CandidateRun, SqliteCandidateStore
from skifer.candidates.drift import compute_drift
from skifer.knowledge.index import KnowledgeIndex
from skifer.knowledge.models import Node
from tests.test_candidates_validator import country_schema, make_candidate

SINCE = datetime(2026, 10, 1, tzinfo=timezone.utc)
UNTIL = datetime(2026, 10, 8, tzinfo=timezone.utc)
CATALOG = ("sales.orders", "sales.revenue_net", "sales.margin")
REPO = Path(__file__).resolve().parents[1]


def usage(event_id, at, *, environment="dev", metrics=("sales.orders",)):
    shape = dict(model_hashes=("sales-v1",), metric_ids=tuple(sorted(metrics)),
                 dimension_ids=(), normalized_filter_shape=())
    return SemanticUsageEvent(
        event_id=event_id, occurred_at=at, environment=environment, consumer_class="agent_read",
        query_fingerprint=fingerprint_query(**shape), duration_ms=10, rows_returned=1,
        bytes_scanned=None, status="succeeded", **shape)


def answered(store, candidate, *moments):
    for number, moment in enumerate(moments):
        store.append_run(CandidateRun(run_id=f"{candidate.candidate_id[-6:]}-{number}",
                                      candidate_id=candidate.candidate_id, executed_at=moment,
                                      purpose="answer", result_hash="sha256:v1:x"))


def certified_index():
    return KnowledgeIndex([
        Node.build(kind="metric", fqn="sales.revenue_net", label="revenue_net",
                   synonyms=("chiffre d'affaires net",), status="certified", source="semantic"),
        Node.build(kind="metric", fqn="cand:other", label="revenue_by_country",
                   status="candidate", source="candidate"),
    ])


@pytest.fixture
def stores(tmp_path):
    candidates = SqliteCandidateStore(str(tmp_path / "candidates.db"))
    events = SqliteUsageEventStore(str(tmp_path / "adaptive.db"))
    yield candidates, events
    candidates.close()
    events.close()


def drift(stores, *, since=SINCE, until=UNTIL, environment="dev", min_sample=0,
          index=None, **kwargs):
    candidates, events = stores
    return compute_drift(candidate_store=candidates, usage_store=events, catalog_models=CATALOG,
                         index=index or certified_index(), environment=environment,
                         since=since, until=until, min_sample=min_sample, **kwargs)


def populate(stores):
    candidates, events = stores
    candidate = make_candidate(origin_question="CA net par région", created_at=SINCE)
    candidates.upsert_candidate(candidate)
    answered(candidates, candidate, SINCE, SINCE + timedelta(days=2))
    for number in range(3):
        events.append(usage(f"e{number}", SINCE + timedelta(hours=number),
                            metrics=("sales.orders",)))
    events.append(usage("prod", SINCE, environment="prod", metrics=("sales.margin",)))
    return candidate


# --- S07-R1 ----------------------------------------------------------------------------


def test_half_open_windows_never_double_count(stores):
    candidates, events = stores
    candidate = make_candidate(created_at=SINCE)
    candidates.upsert_candidate(candidate)
    answered(candidates, candidate, SINCE, UNTIL)
    events.append(usage("on-since", SINCE))
    events.append(usage("on-until", UNTIL))

    first = drift(stores).signal("fallback_rate")
    assert (first.sample_size, first.value) == (2, 0.5)  # since counted, until not
    second = drift(stores, since=UNTIL, until=UNTIL + timedelta(days=7)).signal("fallback_rate")
    assert (second.sample_size, second.value) == (2, 0.5)
    whole = drift(stores, until=UNTIL + timedelta(days=7)).signal("fallback_rate")
    assert whole.sample_size == first.sample_size + second.sample_size
    assert first.window == (SINCE.isoformat(), UNTIL.isoformat())


# --- S07-R2 ----------------------------------------------------------------------------


def test_below_min_sample_is_inconclusive(stores):
    populate(stores)
    report = drift(stores, min_sample=30)
    for name, sample in (("fallback_rate", 5), ("orphan_certified_metrics", 3),
                         ("rephrasing", 1)):
        signal = report.signal(name)
        assert (signal.status, signal.value, signal.sample_size, signal.reason) == (
            "inconclusive", None, sample, "sample_below_minimum")


def test_empty_usage_store_is_evidence_unavailable(stores):
    candidates, _ = stores
    candidates.upsert_candidate(make_candidate(created_at=SINCE))
    report = drift(stores)
    for name in ("fallback_rate", "orphan_certified_metrics"):
        assert (report.signal(name).status, report.signal(name).reason) == (
            "inconclusive", "evidence_unavailable")


def test_measured_values(stores):
    populate(stores)
    report = drift(stores)
    assert report.signal("fallback_rate").value == pytest.approx(0.4)
    orphans = report.signal("orphan_certified_metrics")
    assert orphans.value == pytest.approx(2 / 3)
    assert orphans.details == ({"metric": "sales.margin"}, {"metric": "sales.revenue_net"})


# --- S07-R3 ----------------------------------------------------------------------------


def test_environments_are_strict_partitions(stores):
    populate(stores)
    dev, prod = drift(stores), drift(stores, environment="prod")
    assert dev.signal("orphan_certified_metrics").sample_size == 3
    assert prod.signal("orphan_certified_metrics").sample_size == 1
    assert {"metric": "sales.margin"} not in prod.signal("orphan_certified_metrics").details
    assert dev.environment == "dev" and prod.environment == "prod"


# --- S07-R4 ----------------------------------------------------------------------------


class Spy:
    """Delegates reads, records any other call — a write would show up here."""

    READS = {"list_candidates", "list_runs", "list_events", "store_questions", "nodes", "get",
             "edges", "search", "neighbors", "get_candidate", "list_references"}

    def __init__(self, inner):
        self._inner, self.calls = inner, []

    def __getattr__(self, name):
        if name not in self.READS:
            self.calls.append(name)
        return getattr(self._inner, name)


def test_drift_has_no_side_effect_even_with_thresholds(stores, tmp_path):
    from skifer.core.config import candidate_drift_settings

    candidate = populate(stores)
    candidates, events = stores
    before = (candidates.get_candidate(candidate.candidate_id),
              candidates.list_events(), len(events.list_events()))
    spies = [Spy(candidates), Spy(events), Spy(certified_index())]
    min_sample, thresholds = candidate_drift_settings({
        "candidates_min_sample": 0,
        "candidates_drift_thresholds": {"fallback_rate": 0.1, "orphan_certified_metrics": 0.1}})
    assert thresholds == {"fallback_rate": 0.1, "orphan_certified_metrics": 0.1}
    report = compute_drift(candidate_store=spies[0], usage_store=spies[1], catalog_models=CATALOG,
                           index=spies[2], environment="dev", since=SINCE, until=UNTIL,
                           min_sample=min_sample)
    assert report.thresholds_applied is False and report.to_dict()["thresholds_applied"] is False
    assert [spy.calls for spy in spies] == [[], [], []]
    assert (candidates.get_candidate(candidate.candidate_id), candidates.list_events(),
            len(events.list_events())) == before


# --- S07-R5 ----------------------------------------------------------------------------


def test_rephrasing_detected(stores):
    candidate = populate(stores)
    signal = drift(stores).signal("rephrasing")
    assert signal.status == "measured" and signal.value == 1.0
    assert signal.details == ({"candidate_id": candidate.candidate_id,
                               "node_id": "metric:sales.revenue_net", "overlap": 0.75},)
    unrelated = drift(stores, rephrasing_overlap=0.8).signal("rephrasing")
    assert (unrelated.value, unrelated.details) == (0.0, ())


def test_rephrasing_inconclusive_when_redacted(tmp_path):
    candidates = SqliteCandidateStore(str(tmp_path / "c.db"), store_questions="redacted")
    events = SqliteUsageEventStore(str(tmp_path / "a.db"))
    try:
        candidates.upsert_candidate(make_candidate(created_at=SINCE))
        signal = drift((candidates, events)).signal("rephrasing")
        assert (signal.status, signal.value, signal.reason, signal.sample_size) == (
            "inconclusive", None, "questions_redacted", 1)
    finally:
        candidates.close()
        events.close()


# --- S07-R6 ----------------------------------------------------------------------------


def _report_json(directory: str) -> str:
    """Rebuild the populated stores' report in this process (used under several hash seeds)."""
    candidates = SqliteCandidateStore(os.path.join(directory, "candidates.db"))
    events = SqliteUsageEventStore(os.path.join(directory, "adaptive.db"))
    try:
        report = drift((candidates, events), index=certified_index())
        return json.dumps(report.to_dict(), sort_keys=False)
    finally:
        candidates.close()
        events.close()


def test_deterministic_across_hash_seeds(stores, tmp_path):
    populate(stores)
    candidates, _ = stores
    other = make_candidate(country_schema(aggregate={
        "group_by": ["country"], "measures": [["amount", "net", "sum"]]}),
        metric_name="net_by_country", origin_question="chiffre affaires net",
        created_at=SINCE + timedelta(days=1))
    candidates.upsert_candidate(other)
    outputs = set()
    for seed in ("0", "1", "4242"):
        result = subprocess.run(
            [sys.executable, "-c", "import sys; from tests.test_candidates_drift import "
             "_report_json; sys.stdout.write(_report_json(sys.argv[1]))", str(tmp_path)],
            cwd=REPO, env={**os.environ, "PYTHONHASHSEED": seed}, capture_output=True,
            text=True, check=True)
        outputs.add(result.stdout)
    assert len(outputs) == 1
    details = json.loads(outputs.pop())["signals"][2]["details"]
    assert details == sorted(details, key=lambda d: (d["candidate_id"], d["node_id"]))
    assert len(details) == 2


# --- S07-R7 ----------------------------------------------------------------------------


def test_module_cannot_transition_or_write():
    import skifer.candidates.drift as drift_module

    import ast

    tree = ast.parse(Path(drift_module.__file__).read_text(encoding="utf-8"))
    names = {node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)}
    names |= {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)}
    names |= {alias.name for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)
              for alias in node.names}
    writes = {"check_transition", "append", "append_run", "append_event", "append_reference",
              "upsert_candidate", "upsert_nodes", "upsert_edges", "mark_removed",
              "mark_edges_removed", "purge", "purge_expired", "delete", "execute_sql",
              "apply_report", "promote_level1", "CandidateService"}
    assert names & writes == set()
    strings = [node.value for node in ast.walk(tree)
               if isinstance(node, ast.Constant) and isinstance(node.value, str)]
    assert not any(word in text for text in strings[1:] for word in ("CREATE ", "DROP "))


def test_window_and_arguments_are_validated(stores):
    with pytest.raises(ValueError, match="since must be before until"):
        drift(stores, since=UNTIL, until=SINCE)
    with pytest.raises(ValueError, match="timezone-aware"):
        drift(stores, since=datetime(2026, 10, 1))
    with pytest.raises(ValueError, match="min_sample"):
        drift(stores, min_sample=-1)


# --- S07.2: skifer candidates drift ------------------------------------------------------


def cli_args(**overrides):
    import argparse

    values = dict(candidates_command="drift", since=None, until=None, json=True,
                  environment=None, usage_store="missing.db", config=None,
                  models_dir="semantic_models")
    values.update(overrides)
    return argparse.Namespace(**values)


def run_cli(stores, capsys, *, env_config=None, **overrides):
    from skifer.cli import run_candidates_drift

    candidates, events = stores

    code = run_candidates_drift(
        cli_args(**overrides), candidate_store=candidates, usage_store=events,
        index=certified_index(), catalog_models=list(CATALOG), environment="dev",
        env_config=env_config if env_config is not None else {"candidates_min_sample": 0},
        clock=lambda: UNTIL)
    return code, capsys.readouterr()


def test_cli_drift_defaults_to_last_seven_days_and_exits_zero(stores, capsys):
    from skifer.cli import CANDIDATES_EXIT_OK

    populate(stores)
    code, out = run_cli(stores, capsys, env_config={
        "candidates_min_sample": 0,
        "candidates_drift_thresholds": {"fallback_rate": 0.01}})  # far exceeded, never applied
    assert code == CANDIDATES_EXIT_OK
    report = json.loads(out.out)
    assert report["thresholds_applied"] is False
    assert report["signals"][0]["window"] == [SINCE.isoformat(), UNTIL.isoformat()]
    assert report["signals"][0]["value"] == pytest.approx(0.4)


def test_cli_drift_usage_and_technical_errors(stores, capsys):
    from skifer.cli import CANDIDATES_EXIT_ERROR, CANDIDATES_EXIT_USAGE

    code, out = run_cli(stores, capsys, since=UNTIL.isoformat(), until=SINCE.isoformat())
    assert code == CANDIDATES_EXIT_USAGE and "--since must be before --until" in out.err
    code, out = run_cli(stores, capsys, since="2026-10-01T00:00:00")
    assert code == CANDIDATES_EXIT_USAGE and "timezone" in out.err
    code, out = run_cli(stores, capsys, until="yesterday")
    assert code == CANDIDATES_EXIT_USAGE
    code, out = run_cli(stores, capsys, env_config={"candidates_min_sample": -1})
    assert code == CANDIDATES_EXIT_ERROR and "candidates_min_sample" in out.err


def test_cli_drift_text_and_missing_usage_store(stores, capsys, tmp_path, monkeypatch):
    from skifer.cli import run_candidates_drift

    monkeypatch.chdir(tmp_path)
    populate(stores)
    candidates, _ = stores
    code = run_candidates_drift(
        cli_args(json=False), candidate_store=candidates, index=certified_index(),
        catalog_models=list(CATALOG), environment="dev", env_config={}, clock=lambda: UNTIL)
    text = capsys.readouterr().out
    assert code == 0
    assert "thresholds not applied" in text
    assert "fallback_rate: inconclusive (evidence_unavailable)" in text
    assert not (tmp_path / "missing.db").exists()  # a missing store is never created


def test_catalog_metric_ids_reads_the_semantic_catalog(tmp_path):
    from skifer.cli import _catalog_metric_ids

    (tmp_path / "semantic_catalog.yaml").write_text(
        "models:\n  - {key: sales, metrics: [revenue_net, orders]}\n  - {key: empty}\n")
    assert _catalog_metric_ids(str(tmp_path)) == ["sales.orders", "sales.revenue_net"]
    assert _catalog_metric_ids(str(tmp_path / "nowhere")) == []
