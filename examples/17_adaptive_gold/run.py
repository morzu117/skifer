"""Turn synthetic semantic usage into reviewable, never-deployed proposals."""

import argparse
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timedelta, timezone
from io import StringIO
from pathlib import Path
import tempfile

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
MODEL_HASH = "orders-v1"


def event(index: int, fingerprint: str) -> SemanticUsageEvent:
    return SemanticUsageEvent(
        event_id=f"event-{index:02d}",
        occurred_at=NOW - timedelta(hours=index),
        environment="prod",
        consumer_class="dashboard",
        model_hashes=(MODEL_HASH,),
        metric_ids=("orders.revenue",),
        dimension_ids=("orders.region",),
        normalized_filter_shape=("orders.status:eq",),
        query_fingerprint=fingerprint,
        duration_ms=1_200,
        rows_returned=4,
        bytes_scanned=10_000,
        status="succeeded",
    )


def pipeline() -> dict:
    return {
        "data_product": {
            "id": "sales.orders_summary",
            "version": "1.0.0",
            "description": "Orders summary proposal",
        },
        "contract": {
            "grain": ["region"],
            "output": {
                "region": {"logical_type": "string"},
                "revenue": {"logical_type": "number"},
            },
        },
        "semantic": {"model_key": "orders_summary", "dimensions": ["region"]},
        "materialization": "materialized_view",
        "tables": [{"name": "silver.orders", "alias": "orders"}],
        "aggregate": {
            "group_by": ["region"],
            "measures": [["amount", "revenue", "sum"]],
        },
        "sink": {"type": "delta", "schema": "gold", "table": "orders_summary"},
    }


def accept_args(proposal_id: str, output: Path) -> argparse.Namespace:
    return argparse.Namespace(
        adaptive_command="accept",
        proposal=proposal_id,
        output=output,
        if_identical=False,
    )


def cli_accept(workflow: AdaptiveWorkflow, proposal_id: str, output: Path):
    stdout = StringIO()
    stderr = StringIO()
    with redirect_stdout(stdout), redirect_stderr(stderr):
        code = run_adaptive_command(
            accept_args(proposal_id, output), workflow=workflow
        )
    return code, stdout.getvalue().strip(), stderr.getvalue().strip()


def main() -> None:
    temporary_path = None
    with tempfile.TemporaryDirectory() as temporary:
        temporary_path = Path(temporary)
        store = SqliteUsageEventStore(str(temporary_path / "usage.db"))
        fingerprint = fingerprint_query(
            model_hashes=(MODEL_HASH,),
            metric_ids=("orders.revenue",),
            dimension_ids=("orders.region",),
            normalized_filter_shape=("orders.status:eq",),
        )
        for index in range(10):
            store.append(event(index, fingerprint))

        aggregation = PatternAggregator(
            store,
            clock=lambda: NOW,
            windows=("7d",),
            percentile_min_points=5,
        ).aggregate()
        store.close()
        pattern = aggregation.patterns[0]
        context = RecommendationContext(
            certified_source_hashes=(MODEL_HASH,),
            source_grains=((MODEL_HASH, ("order",)),),
        )
        recommender = RecommendationEngine(rules=("frequent_aggregate",))
        result = recommender.recommend(
            (pattern,), contexts={fingerprint: context}
        )
        proposal = result.proposals[0]

        print(f"Aggregated events: {pattern.event_count} in {pattern.window}")
        print(f"Rule: {proposal.rule_id} v{proposal.rule_version}")
        for name, check in sorted(proposal.expected_benefit["thresholds"].items()):
            print(
                f"  {name}: observed={check['observed']} "
                f"{check['operator']} required={check['required']} "
                f"-> passed={check['passed']}"
            )
        print(f"Verdict: proposal ({proposal.kind})")
        print(f"Proposal id: {proposal.proposal_id}")
        repeated = recommender.recommend(
            (pattern,), contexts={fingerprint: context}
        ).proposals[0]
        print(
            "Content-derived id stable across recommendation repeats: "
            f"{repeated.proposal_id == proposal.proposal_id}"
        )

        stricter = RecommendationEngine(
            thresholds=RecommendationThresholds(frequent_min_event_count=11),
            rules=("frequent_aggregate",),
        )
        refusal = stricter.recommend(
            (pattern,), contexts={fingerprint: context}
        ).refusals[0]
        print(f"RecommendationRefusal: {refusal.rule_id} v{refusal.rule_version}")
        for reason in refusal.reasons:
            print(f"  reason: {reason}")
        for contraindication in refusal.contraindications:
            print(f"  contraindication: {contraindication}")

        roots = [
            temporary_path / "review-a" / ".skifer_proposals",
            temporary_path / "review-b" / ".skifer_proposals",
        ]
        generated = [
            ProposalGenerator(root).generate(proposal, pipeline()) for root in roots
        ]
        print(
            "Path independence: same id generated under two proposal roots: "
            f"{generated[0].proposal_id == generated[1].proposal_id}"
        )

        def current():
            return (MODEL_HASH,)

        conflict_workflow = AdaptiveWorkflow(
            roots[0], current_source_hashes=current, clock=lambda: NOW
        )
        existing_output = temporary_path / "human-owned.yaml"
        existing_output.write_text("human: authored\n", encoding="utf-8")
        conflict_code, _, conflict_message = cli_accept(
            conflict_workflow, proposal.proposal_id, existing_output
        )
        print(f"Existing-output refusal: {conflict_message}")
        print(f"Existing-output exit code: {conflict_code}")
        rejected = conflict_workflow.reject(
            proposal.proposal_id, "Cost is not justified"
        )
        print(f"Human rejection recorded: status={rejected.status}")

        stale_workflow = AdaptiveWorkflow(
            roots[1],
            current_source_hashes=lambda: ("orders-v2",),
            clock=lambda: NOW,
        )
        stale_code, _, stale_message = cli_accept(
            stale_workflow,
            proposal.proposal_id,
            temporary_path / "must-not-exist.yaml",
        )
        print(f"Stale refusal: {stale_message}")
        print(f"Stale exit code: {stale_code}")
        print("Deployment performed: no; schemas/ and Git were never touched.")

    print(f"Temporary workspace removed: {not temporary_path.exists()}")


if __name__ == "__main__":
    main()
