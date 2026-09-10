"""Plan 29 slice 8.5: the adaptive workflow is a human-only boundary."""

from __future__ import annotations

import argparse
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys
from unittest.mock import patch

import pytest

from skifer.adaptive import (
    AdaptiveWorkflow,
    AdaptiveWorkflowConflict,
    AdaptiveWorkflowError,
    DeliveryRecord,
    OptimizationProposal,
    OutcomeEvaluation,
    ProposalGenerator,
    WindowMetrics,
)
from skifer.cli import (
    ADAPTIVE_EXIT_CONFLICT,
    ADAPTIVE_EXIT_ERROR,
    ADAPTIVE_EXIT_OK,
    ADAPTIVE_EXIT_REGRESSED,
    ADAPTIVE_EXIT_STALE,
    ADAPTIVE_EXIT_USAGE,
    main,
    run_adaptive_command,
)
from skifer.core.ir import parse_to_ir
from skifer.core.schema_loader import load_schema
from skifer.observability.certification import canonicalize_contract


PROPOSAL_ID = "proposal:v1:" + "8" * 64
EVALUATED_AT = datetime(2026, 9, 8, 12, tzinfo=timezone.utc)


def _source_pipeline(*, logical_type: str = "number") -> dict:
    return {
        "data_product": {"id": "sales.orders", "version": "1.0.0"},
        "contract": {
            "grain": ["order_id"],
            "output": {
                "order_id": {"logical_type": "identifier"},
                "amount": {"logical_type": logical_type},
                "region": {"logical_type": "string"},
            },
        },
        "semantic": {"model_key": "orders", "dimensions": ["region"]},
        "tables": [{"name": "bronze.orders", "alias": "orders"}],
        "select_final": [
            ["order_id", "order_id"],
            ["amount", "amount"],
            ["region", "region"],
        ],
        "sink": {"type": "delta", "schema": "silver", "table": "orders"},
    }


def _proposal_pipeline() -> dict:
    return {
        "data_product": {"id": "sales.orders_summary", "version": "1.0.0"},
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


def _write_yaml(path: Path, payload: dict) -> None:
    import yaml

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def _definition_hash(path: Path) -> str:
    return canonicalize_contract(parse_to_ir(load_schema(str(path)))).definition_hash


@pytest.fixture
def proposal_root(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    source = tmp_path / "schemas" / "silver" / "orders.yaml"
    _write_yaml(source, _source_pipeline())
    proposal = OptimizationProposal(
        proposal_id=PROPOSAL_ID,
        kind="materialized_view",
        rule_id="frequent_aggregate",
        rule_version="1",
        evidence_event_ids=("event-1", "event-2"),
        expected_benefit={"action": "review_materialized_view"},
        risks=("human_review_required:no_automatic_deployment",),
        generated_schema_path=None,
        generated_semantic_draft_path=None,
        source_definition_hashes=(_definition_hash(source),),
        status="proposed",
    )
    ProposalGenerator().generate(proposal, _proposal_pipeline())
    return tmp_path


def _invoke(*args: str) -> int:
    with patch.object(sys, "argv", ["skifer", *args]), pytest.raises(SystemExit) as exc:
        main()
    return int(exc.value.code)


def _namespace(command: str, **values) -> argparse.Namespace:
    defaults = {
        "adaptive_command": command,
        "proposal": PROPOSAL_ID,
        "output": None,
        "reason": None,
        "if_identical": False,
        "store": ".skifer_adaptive.db",
        "window_days": 30,
    }
    defaults.update(values)
    return argparse.Namespace(**defaults)


def test_adaptive_list_success(proposal_root, capsys):
    assert _invoke("adaptive", "list") == ADAPTIVE_EXIT_OK
    output = capsys.readouterr().out
    assert f"{PROPOSAL_ID}\tmaterialized_view\tproposed" in output
    assert str(proposal_root) not in output


def test_adaptive_show_success_uses_relative_paths(proposal_root, capsys):
    assert _invoke("adaptive", "show", PROPOSAL_ID) == ADAPTIVE_EXIT_OK
    payload = json.loads(capsys.readouterr().out)
    assert payload["proposal_id"] == PROPOSAL_ID
    assert payload["generated_schema_path"] == f"{PROPOSAL_ID}/pipeline.yaml"
    assert str(proposal_root) not in json.dumps(payload)


def test_adaptive_diff_success_is_read_only(proposal_root, capsys):
    proposal_dir = proposal_root / ".skifer_proposals" / PROPOSAL_ID
    before = {path.name: path.read_bytes() for path in proposal_dir.rglob("*") if path.is_file()}

    assert _invoke("adaptive", "diff", PROPOSAL_ID) == ADAPTIVE_EXIT_OK

    output = capsys.readouterr().out
    assert "--- /dev/null" in output
    assert "+materialization: materialized_view" in output
    after = {path.name: path.read_bytes() for path in proposal_dir.rglob("*") if path.is_file()}
    assert after == before


def test_adaptive_diff_missing_target_compares_against_empty(proposal_root, capsys):
    target = Path("schemas/gold/not_created.yaml")
    assert _invoke(
        "adaptive", "diff", PROPOSAL_ID, "--output", str(target)
    ) == ADAPTIVE_EXIT_OK
    output = capsys.readouterr().out
    assert "--- schemas/gold/not_created.yaml" in output
    assert not target.exists()


def test_adaptive_accept_success_persists_status_without_deploying(proposal_root, capsys):
    output = Path("schemas/gold/orders_summary.yaml")
    assert _invoke(
        "adaptive", "accept", PROPOSAL_ID, "--output", str(output)
    ) == ADAPTIVE_EXIT_OK
    assert output.read_bytes() == (
        proposal_root / ".skifer_proposals" / PROPOSAL_ID / "pipeline.yaml"
    ).read_bytes()
    proposal_json = json.loads(
        (
            proposal_root / ".skifer_proposals" / PROPOSAL_ID / "proposal.json"
        ).read_text(encoding="utf-8")
    )
    assert proposal_json["status"] == "accepted"
    assert "deploy separately" in capsys.readouterr().out


def test_adaptive_reject_success_persists_reason_and_status(proposal_root, capsys):
    assert _invoke(
        "adaptive", "reject", PROPOSAL_ID, "--reason", "Cost is not justified"
    ) == ADAPTIVE_EXIT_OK
    directory = proposal_root / ".skifer_proposals" / PROPOSAL_ID
    assert json.loads((directory / "proposal.json").read_text())["status"] == "rejected"
    assert json.loads((directory / "decision.json").read_text()) == {
        "decision": "rejected",
        "reason": "Cost is not justified",
    }
    assert "Rejected" in capsys.readouterr().out


def test_accept_existing_output_is_refused_and_preserved_byte_for_byte(
    proposal_root, capsys
):
    output = proposal_root / "schemas" / "gold" / "human.yaml"
    output.parent.mkdir(parents=True)
    original = b"human: authored\n# exact bytes\n"
    output.write_bytes(original)

    assert _invoke(
        "adaptive", "accept", PROPOSAL_ID, "--output", str(output)
    ) == ADAPTIVE_EXIT_CONFLICT
    assert output.read_bytes() == original
    assert "refusing to overwrite" in capsys.readouterr().err


def test_accept_source_hash_drift_marks_stale_and_uses_dedicated_code(
    proposal_root, capsys
):
    source = proposal_root / "schemas" / "silver" / "orders.yaml"
    _write_yaml(source, _source_pipeline(logical_type="integer"))
    output = proposal_root / "schemas" / "gold" / "must_not_exist.yaml"

    assert _invoke(
        "adaptive", "accept", PROPOSAL_ID, "--output", str(output)
    ) == ADAPTIVE_EXIT_STALE
    assert not output.exists()
    proposal = json.loads(
        (
            proposal_root / ".skifer_proposals" / PROPOSAL_ID / "proposal.json"
        ).read_text()
    )
    assert proposal["status"] == "stale"
    assert "stale" in capsys.readouterr().err


def test_reject_missing_or_empty_reason_is_refused(proposal_root, capsys):
    assert _invoke("adaptive", "reject", PROPOSAL_ID) == ADAPTIVE_EXIT_USAGE
    assert _invoke(
        "adaptive", "reject", PROPOSAL_ID, "--reason", "   "
    ) == ADAPTIVE_EXIT_ERROR
    assert "non-empty reason" in capsys.readouterr().err


def test_adaptive_workflow_never_invokes_git_subprocess_or_engine(proposal_root):
    output = Path("schemas/gold/orders_summary.yaml")
    with (
        patch("subprocess.run", side_effect=AssertionError("no subprocess")),
        patch("subprocess.Popen", side_effect=AssertionError("no subprocess")),
        patch(
            "skifer.core.core.SkiferEngine.run_from_yaml",
            side_effect=AssertionError("engine must not run"),
        ),
        patch(
            "skifer.core.core.SkiferEngine.run_process_to_table",
            side_effect=AssertionError("engine must not run"),
        ),
    ):
        assert run_adaptive_command(
            _namespace("accept", output=str(output))
        ) == ADAPTIVE_EXIT_OK


@pytest.mark.parametrize(
    ("setup", "proposal_id", "message"),
    (
        ("unknown", PROPOSAL_ID[:-1] + "9", "Unknown proposal"),
        ("corrupt", PROPOSAL_ID, "malformed proposal.json"),
        ("valid", "../escape", "Invalid proposal id"),
    ),
)
def test_unknown_corrupt_and_invalid_proposals_fail_closed(
    proposal_root, capsys, setup, proposal_id, message
):
    if setup == "corrupt":
        path = proposal_root / ".skifer_proposals" / PROPOSAL_ID / "proposal.json"
        path.write_bytes(b"{ definitely not json")
    assert _invoke("adaptive", "show", proposal_id) == ADAPTIVE_EXIT_ERROR
    assert message in capsys.readouterr().err


def test_missing_proposals_directory_is_an_explicit_error(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    assert _invoke("adaptive", "list") == ADAPTIVE_EXIT_ERROR
    assert "is missing" in capsys.readouterr().err


def test_absolute_paths_are_not_disclosed(proposal_root, capsys):
    output = proposal_root / "outside" / "accepted.yaml"
    assert _invoke(
        "adaptive", "accept", PROPOSAL_ID, "--output", str(output)
    ) == ADAPTIVE_EXIT_OK
    combined = capsys.readouterr().out
    assert str(proposal_root) not in combined
    assert "accepted.yaml" in combined


def test_existing_cli_subcommand_help_still_parses(proposal_root):
    for command in ("validate", "hub", "semantic", "mcp", "adaptive"):
        assert _invoke(command, "--help") == 0


def test_accept_if_identical_is_an_explicit_non_destructive_idempotent_noop(
    proposal_root
):
    output = Path("schemas/gold/orders_summary.yaml")
    assert _invoke(
        "adaptive", "accept", PROPOSAL_ID, "--output", str(output)
    ) == ADAPTIVE_EXIT_OK
    before = output.read_bytes()
    assert _invoke(
        "adaptive",
        "accept",
        PROPOSAL_ID,
        "--output",
        str(output),
        "--if-identical",
    ) == ADAPTIVE_EXIT_OK
    assert output.read_bytes() == before


def test_accept_writes_a_delivery_record(proposal_root):
    delivered_at = datetime(2026, 9, 8, 10, tzinfo=timezone.utc)
    reviewer = AdaptiveWorkflow(clock=lambda: delivered_at)
    output = Path("schemas/gold/orders_summary.yaml")

    reviewer.accept(PROPOSAL_ID, output)

    record = reviewer.load_delivery(PROPOSAL_ID)
    assert record == DeliveryRecord(PROPOSAL_ID, str(output), delivered_at)
    assert json.loads(
        (
            proposal_root / ".skifer_proposals" / PROPOSAL_ID / "delivery.json"
        ).read_text()
    ) == record.to_dict()


def test_delivery_write_failure_removes_output_and_keeps_proposal_proposed(
    proposal_root, monkeypatch
):
    reviewer = AdaptiveWorkflow(clock=lambda: EVALUATED_AT)
    original_write = reviewer.generator._write_json_atomic

    def failing_write(path, payload):
        if path.name == "delivery.json":
            raise OSError("delivery write failed")
        return original_write(path, payload)

    monkeypatch.setattr(reviewer.generator, "_write_json_atomic", failing_write)
    output = Path("schemas/gold/orders_summary.yaml")

    with pytest.raises(AdaptiveWorkflowError, match="Cannot persist"):
        reviewer.accept(PROPOSAL_ID, output)

    assert not output.exists()
    assert reviewer.load(PROPOSAL_ID).status == "proposed"
    assert not (
        proposal_root / ".skifer_proposals" / PROPOSAL_ID / "delivery.json"
    ).exists()


def test_load_delivery_refuses_a_never_accepted_proposal(proposal_root):
    with pytest.raises(AdaptiveWorkflowConflict, match="has not been delivered"):
        AdaptiveWorkflow().load_delivery(PROPOSAL_ID)


def _evaluation(outcome="improved"):
    window = WindowMetrics(
        started_at=EVALUATED_AT,
        ended_at=EVALUATED_AT,
        event_count=5,
        succeeded_count=5,
        failed_count=0,
        duration_point_count=5,
        duration_percentile=100.0,
        failure_rate=0.0,
    )
    return OutcomeEvaluation(
        proposal_id=PROPOSAL_ID,
        evaluated_at=EVALUATED_AT,
        outcome=outcome,
        reasons=("duration_p95:before=200.0:after=100.0:ratio=0.50",),
        before=window,
        after=window,
        review_recommendation=(
            f"Human review recommended for proposal '{PROPOSAL_ID}'."
            if outcome == "regressed"
            else None
        ),
    )


def test_record_outcome_transitions_to_measured_and_identical_repeat_is_noop(
    proposal_root, monkeypatch
):
    reviewer = AdaptiveWorkflow(clock=lambda: EVALUATED_AT)
    reviewer.accept(PROPOSAL_ID, Path("schemas/gold/orders_summary.yaml"))
    evaluation = _evaluation()

    measured = reviewer.record_outcome(evaluation)
    assert measured.status == "measured"
    assert json.loads(
        (
            proposal_root / ".skifer_proposals" / PROPOSAL_ID / "evaluation.json"
        ).read_text()
    ) == evaluation.to_dict()

    monkeypatch.setattr(
        reviewer.generator,
        "_write_json_atomic",
        lambda *_: pytest.fail("byte-identical measurement must not write"),
    )
    assert reviewer.record_outcome(evaluation) == measured


def test_record_outcome_allows_a_later_distinct_measurement(proposal_root):
    reviewer = AdaptiveWorkflow(clock=lambda: EVALUATED_AT)
    reviewer.accept(PROPOSAL_ID, Path("schemas/gold/orders_summary.yaml"))
    reviewer.record_outcome(_evaluation())
    later = replace(
        _evaluation(),
        evaluated_at=EVALUATED_AT + timedelta(days=1),
        reasons=("duration_p95:before=200.0:after=90.0:ratio=0.45",),
    )

    measured = reviewer.record_outcome(later)

    assert measured.status == "measured"
    payload = json.loads(
        (
            proposal_root / ".skifer_proposals" / PROPOSAL_ID / "evaluation.json"
        ).read_text()
    )
    assert payload["evaluated_at"] == later.to_dict()["evaluated_at"]


class _FakeUsageStore:
    def __init__(self, path):
        self.path = path

    def close(self):
        return None


class _FakeEvaluator:
    result = None

    def __init__(self, store, **options):
        self.store = store
        self.options = options

    def evaluate(self, proposal, delivery):
        return self.result


@pytest.mark.parametrize(
    ("outcome", "exit_code"),
    (("improved", ADAPTIVE_EXIT_OK), ("regressed", ADAPTIVE_EXIT_REGRESSED)),
)
def test_adaptive_evaluate_uses_outcome_exit_code(
    proposal_root, monkeypatch, capsys, outcome, exit_code
):
    assert _invoke(
        "adaptive",
        "accept",
        PROPOSAL_ID,
        "--output",
        "schemas/gold/orders_summary.yaml",
    ) == ADAPTIVE_EXIT_OK
    capsys.readouterr()
    (proposal_root / "usage.db").touch()
    _FakeEvaluator.result = _evaluation(outcome)
    monkeypatch.setattr(
        "skifer.adaptive.evaluator.OutcomeEvaluator", _FakeEvaluator
    )
    monkeypatch.setattr(
        "skifer.adaptive.store.SqliteUsageEventStore", _FakeUsageStore
    )

    assert _invoke(
        "adaptive", "evaluate", PROPOSAL_ID, "--store", "usage.db"
    ) == exit_code
    payload = json.loads(capsys.readouterr().out)
    assert payload["outcome"] == outcome
    assert json.loads(
        (
            proposal_root / ".skifer_proposals" / PROPOSAL_ID / "proposal.json"
        ).read_text()
    )["status"] == "measured"


def test_adaptive_evaluate_not_delivered_uses_conflict_code(proposal_root, capsys):
    assert _invoke("adaptive", "evaluate", PROPOSAL_ID) == ADAPTIVE_EXIT_CONFLICT
    assert "not been delivered" in capsys.readouterr().err


def test_adaptive_evaluate_technical_error_never_discloses_store_path(
    proposal_root, monkeypatch, capsys
):
    assert _invoke(
        "adaptive",
        "accept",
        PROPOSAL_ID,
        "--output",
        "schemas/gold/orders_summary.yaml",
    ) == ADAPTIVE_EXIT_OK
    capsys.readouterr()
    secret_store = proposal_root / "private" / "usage.db"
    secret_store.parent.mkdir(parents=True, exist_ok=True)
    secret_store.touch()
    secret_path = str(secret_store)

    def fail_store(_path):
        raise OSError(f"cannot open {secret_path}")

    monkeypatch.setattr(
        "skifer.adaptive.store.SqliteUsageEventStore", fail_store
    )
    assert _invoke(
        "adaptive", "evaluate", PROPOSAL_ID, "--store", secret_path
    ) == ADAPTIVE_EXIT_ERROR
    error = capsys.readouterr().err
    assert secret_path not in error
    assert "OSError" in error


def test_adaptive_evaluate_refuses_to_create_a_missing_usage_store(
    proposal_root, capsys
):
    """A mistyped --store must fail, not fabricate an empty measurement.

    sqlite3.connect() creates the file it is given, so without this guard a typo
    produced an empty store, an "evidence_unavailable" verdict indistinguishable
    from a genuine retention purge, exit code 0, and a proposal moved to
    "measured" on a measurement that never ran.
    """
    assert _invoke(
        "adaptive",
        "accept",
        PROPOSAL_ID,
        "--output",
        "schemas/gold/orders_summary.yaml",
    ) == ADAPTIVE_EXIT_OK
    capsys.readouterr()

    assert _invoke(
        "adaptive", "evaluate", PROPOSAL_ID, "--store", "typo_usage.db"
    ) == ADAPTIVE_EXIT_ERROR
    assert "does not exist" in capsys.readouterr().err
    assert not (proposal_root / "typo_usage.db").exists()
    assert json.loads(
        (
            proposal_root / ".skifer_proposals" / PROPOSAL_ID / "proposal.json"
        ).read_text()
    )["status"] == "accepted"


def test_reject_is_idempotent_only_for_the_same_reason(proposal_root):
    args = ("adaptive", "reject", PROPOSAL_ID, "--reason", "Not enough benefit")
    assert _invoke(*args) == ADAPTIVE_EXIT_OK
    assert _invoke(*args) == ADAPTIVE_EXIT_OK
    assert _invoke(
        "adaptive", "reject", PROPOSAL_ID, "--reason", "Different reason"
    ) == ADAPTIVE_EXIT_CONFLICT
