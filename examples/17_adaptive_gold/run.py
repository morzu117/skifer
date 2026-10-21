"""Turn semantic usage into an explainable Gold proposal that a human accepts or rejects.

    python examples/17_adaptive_gold/run.py
"""

import argparse
import atexit
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timedelta, timezone
from io import StringIO
from pathlib import Path
import shutil
from tempfile import mkdtemp

from skifer.adaptive import (
    AdaptiveWorkflow,
    PatternAggregator,
    ProposalGenerator,
    RecommendationContext,
    RecommendationEngine,
    RecommendationThresholds,
    SemanticUsageEvent,
    SqliteUsageEventStore,
    fingerprint_query,
)
from skifer.cli import run_adaptive_command

NOW = datetime(2026, 9, 9, 12, tzinfo=timezone.utc)
workdir = Path(mkdtemp())
atexit.register(shutil.rmtree, workdir)

# Usage events carry names and shapes only: no question, no SQL, no filter value.
shape = {
    "model_hashes": ("orders-v1",),
    "metric_ids": ("orders.revenue",),
    "dimension_ids": ("orders.region",),
    "normalized_filter_shape": ("orders.status:eq",),
}
fingerprint = fingerprint_query(**shape)
store = SqliteUsageEventStore(str(workdir / "usage.db"))
for i in range(10):
    store.append(SemanticUsageEvent(
        event_id=f"event-{i:02d}", occurred_at=NOW - timedelta(hours=i), environment="prod",
        consumer_class="dashboard", query_fingerprint=fingerprint, duration_ms=1_200,
        rows_returned=4, bytes_scanned=10_000, status="succeeded", **shape,
    ))

pattern = PatternAggregator(store, clock=lambda: NOW, windows=("7d",), percentile_min_points=5).aggregate().patterns[0]
contexts = {fingerprint: RecommendationContext(certified_source_hashes=("orders-v1",), source_grains=(("orders-v1", ("order",)),))}

recommender = RecommendationEngine(rules=("frequent_aggregate",))
proposal = recommender.recommend((pattern,), contexts=contexts).proposals[0]
print(f"Aggregated events: {pattern.event_count} in {pattern.window}")
print(f"Rule: {proposal.rule_id} v{proposal.rule_version}")
for name, check in sorted(proposal.expected_benefit["thresholds"].items()):
    print(f"  {name}: observed={check['observed']} {check['operator']} required={check['required']} -> passed={check['passed']}")
print(f"Verdict: proposal ({proposal.kind})")
print(f"Proposal id: {proposal.proposal_id}")
repeated = recommender.recommend((pattern,), contexts=contexts).proposals[0]
print(f"Content-derived id stable across recommendation repeats: {repeated.proposal_id == proposal.proposal_id}")

# A rule that does not conclude says why, instead of staying silent.
stricter = RecommendationEngine(thresholds=RecommendationThresholds(frequent_min_event_count=11), rules=("frequent_aggregate",))
refusal = stricter.recommend((pattern,), contexts=contexts).refusals[0]
print(f"RecommendationRefusal: {refusal.rule_id} v{refusal.rule_version}")
for reason in refusal.reasons:
    print(f"  reason: {reason}")
for contraindication in refusal.contraindications:
    print(f"  contraindication: {contraindication}")

pipeline = {
    "data_product": {"id": "sales.orders_summary", "version": "1.0.0", "description": "Orders summary proposal"},
    "contract": {"grain": ["region"], "output": {"region": {"logical_type": "string"}, "revenue": {"logical_type": "number"}}},
    "semantic": {"model_key": "orders_summary", "dimensions": ["region"]},
    "materialization": "materialized_view",
    "tables": [{"name": "silver.orders", "alias": "orders"}],
    "aggregate": {"group_by": ["region"], "measures": [["amount", "revenue", "sum"]]},
    "sink": {"type": "delta", "schema": "gold", "table": "orders_summary"},
}
roots = [workdir / "review-a" / ".skifer_proposals", workdir / "review-b" / ".skifer_proposals"]
ids = [ProposalGenerator(root).generate(proposal, pipeline).proposal_id for root in roots]
print(f"Path independence: same id generated under two proposal roots: {ids[0] == ids[1]}")


def cli_accept(workflow, output):
    stderr = StringIO()
    args = argparse.Namespace(adaptive_command="accept", proposal=proposal.proposal_id, output=output, if_identical=False)
    with redirect_stdout(StringIO()), redirect_stderr(stderr):
        code = run_adaptive_command(args, workflow=workflow)
    return code, stderr.getvalue().strip()


workflow = AdaptiveWorkflow(roots[0], current_source_hashes=lambda: ("orders-v1",), clock=lambda: NOW)
human_file = workdir / "human-owned.yaml"
human_file.write_text("human: authored\n", encoding="utf-8")
code, message = cli_accept(workflow, human_file)
print(f"Existing-output refusal: {message}")
print(f"Existing-output exit code: {code}")
print(f"Human rejection recorded: status={workflow.reject(proposal.proposal_id, 'Cost is not justified').status}")

# The source definition moved since the proposal was made: it is stale, not acceptable.
stale = AdaptiveWorkflow(roots[1], current_source_hashes=lambda: ("orders-v2",), clock=lambda: NOW)
code, message = cli_accept(stale, workdir / "must-not-exist.yaml")
print(f"Stale refusal: {message}")
print(f"Stale exit code: {code}")
