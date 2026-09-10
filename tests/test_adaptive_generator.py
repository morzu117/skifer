"""Tests for Plan 29 slice 8.4 validated proposal artifact generation."""

import json
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from skifer.adaptive import (
    OptimizationProposal,
    ProposalGenerationError,
    ProposalGenerator,
)
from skifer.core.ir import parse_to_ir
from skifer.core.schema_loader import load_schema
from skifer.core.sql_compiler import compile_select
from skifer.semantic.validator import SemanticValidator


PROPOSAL_ID = "proposal:v1:" + "a" * 64


def _proposal(**overrides) -> OptimizationProposal:
    values = {
        "proposal_id": PROPOSAL_ID,
        "kind": "materialized_view",
        "rule_id": "frequent_aggregate",
        "rule_version": "1",
        "evidence_event_ids": ("event-001", "event-002"),
        "expected_benefit": {
            "action": "review_materialized_view",
            "score_method": "named_thresholds_v1",
        },
        "risks": ("human_review_required:no_automatic_deployment",),
        "generated_schema_path": None,
        "generated_semantic_draft_path": None,
        "source_definition_hashes": ("orders-v1",),
        "status": "proposed",
    }
    values.update(overrides)
    return OptimizationProposal(**values)


def _pipeline(*, materialization="materialized_view") -> dict:
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
        "materialization": materialization,
        "tables": [{"name": "silver.orders", "alias": "orders"}],
        "aggregate": {
            "group_by": ["region"],
            "measures": [["amount", "revenue", "sum"]],
        },
        "sink": {"type": "delta", "schema": "gold", "table": "orders_summary"},
    }


def _generator(tmp_path: Path) -> ProposalGenerator:
    return ProposalGenerator(tmp_path / ".skifer_proposals")


def test_output_root_is_restricted_to_dedicated_proposals_directory(tmp_path):
    with pytest.raises(ProposalGenerationError, match="must be named"):
        ProposalGenerator(tmp_path / "schemas")


def test_generates_loader_compiler_and_semantic_valid_artifacts(tmp_path):
    generator = _generator(tmp_path)
    generated = generator.generate(_proposal(), _pipeline())

    schema_path = generator.resolve(generated.generated_schema_path)
    draft_path = generator.resolve(generated.generated_semantic_draft_path)
    parsed = parse_to_ir(load_schema(str(schema_path)))

    assert "SUM(`amount`) AS `revenue`" in compile_select(parsed)
    draft = yaml.safe_load(draft_path.read_text(encoding="utf-8"))
    assert SemanticValidator().validate_yaml(draft).ok
    assert draft["_generated_by"]["tool"] == (
        "skifer.semantic.draft_builder"
    )
    assert schema_path.parent == tmp_path / ".skifer_proposals" / PROPOSAL_ID
    assert draft_path.is_relative_to(schema_path.parent)
    proposal_json = json.loads(
        (schema_path.parent / "proposal.json").read_text(encoding="utf-8")
    )
    assert proposal_json["proposal_id"] == PROPOSAL_ID


def test_compile_select_is_used_for_materialized_view(tmp_path):
    from skifer.adaptive import generator as generator_module

    with patch.object(
        generator_module,
        "compile_select",
        wraps=generator_module.compile_select,
    ) as compiler:
        _generator(tmp_path).generate(_proposal(), _pipeline())

    compiler.assert_called_once()


def test_invalid_pipeline_leaves_no_proposal_directory(tmp_path):
    pipeline = _pipeline()
    pipeline["aggregate"]["measures"] = [["amount", "revenue", "not_a_function"]]

    with pytest.raises(ProposalGenerationError, match="load_schema validation"):
        _generator(tmp_path).generate(_proposal(), pipeline)

    assert not (tmp_path / ".skifer_proposals" / PROPOSAL_ID).exists()
    assert not list((tmp_path / ".skifer_proposals").glob(".proposal.*"))


def test_compiler_failure_leaves_no_proposal_directory(tmp_path, monkeypatch):
    def explode(_schema):
        raise ValueError("compiler rejected proposal")

    monkeypatch.setattr("skifer.adaptive.generator.compile_select", explode)

    with pytest.raises(ProposalGenerationError, match="compile_select validation"):
        _generator(tmp_path).generate(_proposal(), _pipeline())

    assert not (tmp_path / ".skifer_proposals" / PROPOSAL_ID).exists()


def test_same_content_is_byte_reproducible_and_existing_identical_is_noop(tmp_path):
    generator = _generator(tmp_path)
    first = generator.generate(_proposal(), _pipeline())
    proposal_dir = generator.resolve(first.generated_schema_path).parent
    before = {
        path.relative_to(proposal_dir): path.read_bytes()
        for path in proposal_dir.rglob("*")
        if path.is_file()
    }

    second = generator.generate(_proposal(), _pipeline())
    after = {
        path.relative_to(proposal_dir): path.read_bytes()
        for path in proposal_dir.rglob("*")
        if path.is_file()
    }

    assert first == second
    assert before == after
    assert not list(generator.proposals_dir.glob(".proposal.*"))


def test_existing_different_directory_is_preserved(tmp_path):
    proposal_dir = tmp_path / ".skifer_proposals" / PROPOSAL_ID
    proposal_dir.mkdir(parents=True)
    marker = proposal_dir / "proposal.json"
    marker.write_bytes(b"existing bytes\n")

    with pytest.raises(ProposalGenerationError, match="different content"):
        _generator(tmp_path).generate(_proposal(), _pipeline())

    assert marker.read_bytes() == b"existing bytes\n"
    assert list(proposal_dir.iterdir()) == [marker]


@pytest.mark.parametrize(
    "unsafe_id",
    (
        "proposal:v1:.." + "a" * 62,
        "proposal/v1/" + "a" * 64,
        "proposal\\v1\\" + "a" * 64,
        "/proposal/v1/" + "a" * 64,
    ),
)
def test_path_traversal_proposal_ids_are_refused(tmp_path, unsafe_id):
    proposal = _proposal()
    object.__setattr__(proposal, "proposal_id", unsafe_id)

    with pytest.raises(ProposalGenerationError, match="path traversal"):
        _generator(tmp_path).generate(proposal, _pipeline())

    assert not (tmp_path / ".skifer_proposals").exists()


@pytest.mark.parametrize("kind", ("semantic_gap", "deprecation"))
def test_review_only_kinds_do_not_create_physical_artifacts(tmp_path, kind):
    proposal = _proposal(kind=kind)

    with pytest.raises(ProposalGenerationError, match="does not produce physical"):
        _generator(tmp_path).generate(proposal, _pipeline())

    assert not (tmp_path / ".skifer_proposals").exists()


def test_generator_never_calls_engine_git_or_writes_schemas(tmp_path):
    schemas = tmp_path / "schemas"
    schemas.mkdir()
    marker = schemas / "keep.yaml"
    marker.write_text("keep: true\n", encoding="utf-8")

    with (
        patch(
            "skifer.core.core.SkiferEngine.run_from_yaml",
            side_effect=AssertionError("engine must not run"),
        ),
        patch("subprocess.run", side_effect=AssertionError("git must not run")),
        patch("subprocess.Popen", side_effect=AssertionError("git must not run")),
    ):
        _generator(tmp_path).generate(_proposal(), _pipeline())

    assert marker.read_text(encoding="utf-8") == "keep: true\n"
    assert list(schemas.iterdir()) == [marker]


def test_serialization_allowlist_excludes_future_sensitive_fields(tmp_path):
    sentinel = "SENSITIVE-CANARY-DO-NOT-WRITE"
    proposal = _proposal()
    object.__setattr__(proposal, "raw_question", sentinel)

    generator = _generator(tmp_path)
    generated = generator.generate(proposal, _pipeline())

    proposal_dir = generator.resolve(generated.generated_schema_path).parent
    assert all(
        sentinel.encode() not in path.read_bytes()
        for path in proposal_dir.rglob("*")
        if path.is_file()
    )


def test_aggregate_table_uses_plan_28_table_materialization(tmp_path):
    proposal = _proposal(
        kind="aggregate_table",
        rule_id="repeated_join_path",
        proposal_id="proposal:v1:" + "b" * 64,
    )

    generator = _generator(tmp_path)
    generated = generator.generate(
        proposal, _pipeline(materialization="table")
    )

    loaded = load_schema(str(generator.resolve(generated.generated_schema_path)))
    assert loaded["materialization"] == {"type": "table"}
    assert loaded["aggregate"]["measures"][0] == {
        "source": "amount",
        "target": "revenue",
        "func": "sum",
    }


def test_wrong_materialization_and_non_writable_fail_closed(tmp_path, monkeypatch):
    generator = _generator(tmp_path)

    with pytest.raises(ProposalGenerationError, match="requires materialization"):
        generator.generate(_proposal(), _pipeline(materialization="table"))
    assert not (tmp_path / ".skifer_proposals" / PROPOSAL_ID).exists()

    def refuse_write(*_args, **_kwargs):
        raise PermissionError("read-only proposal directory")

    monkeypatch.setattr(
        "skifer.adaptive.generator.write_yaml_atomic", refuse_write
    )
    with pytest.raises(ProposalGenerationError, match="read-only proposal directory"):
        generator.generate(_proposal(), _pipeline())
    assert not (tmp_path / ".skifer_proposals" / PROPOSAL_ID).exists()


# ---------------------------------------------------------------------------
# Plan 29 — artifact paths are portable, not pinned to one checkout
# ---------------------------------------------------------------------------

def test_recorded_artifact_paths_are_relative_to_the_proposals_root(tmp_path):
    """An absolute path pins a reviewed artifact to the machine that made it."""
    root = tmp_path / ".skifer_proposals"
    generated = ProposalGenerator(root).generate(_proposal(), _pipeline())

    for recorded in (
        generated.generated_schema_path,
        generated.generated_semantic_draft_path,
    ):
        assert not Path(recorded).is_absolute()
        assert recorded.startswith(f"{PROPOSAL_ID}/")
        assert (root / recorded).is_file()


def test_proposal_json_leaks_no_filesystem_layout(tmp_path):
    """The reviewed artifact must not carry the local path, username included."""
    root = tmp_path / ".skifer_proposals"
    ProposalGenerator(root).generate(_proposal(), _pipeline())

    written = (root / PROPOSAL_ID / "proposal.json").read_text()
    assert str(tmp_path) not in written
    assert '": "/' not in written


def test_identical_content_is_byte_for_byte_reproducible_across_roots(tmp_path):
    """Two roots, same content: the proposal_id is content-derived, so the
    artifacts it names must match too, or comparing proposals is impossible."""
    digests = []
    for name in ("first", "second"):
        root = tmp_path / name / ".skifer_proposals"
        ProposalGenerator(root).generate(_proposal(), _pipeline())
        directory = root / PROPOSAL_ID
        digests.append(
            tuple(
                (path.relative_to(directory).as_posix(), path.read_bytes())
                for path in sorted(directory.rglob("*"))
                if path.is_file()
            )
        )

    assert digests[0] == digests[1]
