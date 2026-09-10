"""Validated, review-only artifact generation for adaptive Gold proposals."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
import json
import os
from pathlib import Path
import re
import shutil
import tempfile
from typing import Any, Mapping

import yaml

from skifer.core.ir import ParsedSchema, parse_to_ir
from skifer.core.schema_loader import load_schema
from skifer.core.sql_compiler import compile_select
from skifer.semantic.draft_builder import SemanticDraftBuilder
from skifer.semantic.output_projection import OutputProjector
from skifer.semantic.persistence import write_yaml_atomic
from skifer.semantic.validator import SemanticValidator

from .models import OptimizationProposal


_SAFE_PROPOSAL_ID = re.compile(r"^proposal:v[1-9][0-9]*:[0-9a-f]{64}$")
_PHYSICAL_KINDS = {"materialized_view", "aggregate_table"}
_EXPECTED_MATERIALIZATION = {
    "materialized_view": "materialized_view",
    "aggregate_table": "table",
}


class ProposalGenerationError(RuntimeError):
    """A proposal could not be validated and published for human review."""


def validate_proposal_id(proposal_id: str) -> str:
    """Validate the shared, path-safe proposal identifier contract."""
    if (
        not isinstance(proposal_id, str)
        or Path(proposal_id).is_absolute()
        or ".." in proposal_id
        or "/" in proposal_id
        or "\\" in proposal_id
        or not _SAFE_PROPOSAL_ID.fullmatch(proposal_id)
    ):
        raise ProposalGenerationError(
            "proposal_id must match 'proposal:v<version>:<64 lowercase hex characters>' "
            "and must not contain path traversal syntax."
        )
    return proposal_id


class ProposalGenerator:
    """Materialize validated proposals without touching runtime or schema locations."""

    def __init__(self, proposals_dir: str | Path = ".skifer_proposals") -> None:
        requested_dir = Path(proposals_dir)
        if requested_dir.name != ".skifer_proposals":
            raise ProposalGenerationError(
                "The proposal output directory must be named '.skifer_proposals'."
            )
        self.proposals_dir = requested_dir.resolve()

    def generate(
        self,
        proposal: OptimizationProposal,
        pipeline: Mapping[str, Any],
    ) -> OptimizationProposal:
        """Validate and atomically publish one physical proposal.

        ``pipeline`` is explicit because the privacy-safe recommendation object only
        carries logical usage IDs; reconstructing physical columns or table names
        from it would require guessing. The returned frozen proposal carries paths
        to the artifacts that were actually published.
        """
        self._validate_inputs(proposal, pipeline)
        target_dir = self.proposals_dir / proposal.proposal_id
        schema_path = target_dir / "pipeline.yaml"

        staging_dir: Path | None = None
        try:
            self.proposals_dir.mkdir(parents=True, exist_ok=True)
            staging_dir = Path(
                tempfile.mkdtemp(prefix=".proposal.", dir=str(self.proposals_dir))
            )
            staging_schema_path = staging_dir / "pipeline.yaml"
            write_yaml_atomic(staging_schema_path, deepcopy(dict(pipeline)))

            parsed = self._load_and_validate_pipeline(
                proposal, staging_schema_path
            )
            projected = OutputProjector().project(parsed)
            draft_builder = SemanticDraftBuilder(output_dir=str(staging_dir))
            staging_draft_path = Path(draft_builder.write_draft(projected, parsed))
            draft_relative_path = staging_draft_path.relative_to(staging_dir)
            draft_path = target_dir / draft_relative_path

            draft_payload = self._read_yaml(staging_draft_path)
            semantic_validation = SemanticValidator().validate_against_projection(
                draft_payload,
                projected,
                contract_output=[field.name for field in parsed.contract_output],
            )
            if not semantic_validation.ok:
                details = "; ".join(semantic_validation.errors)
                raise ProposalGenerationError(
                    f"Generated semantic draft failed validation: {details}"
                )

            # Recorded relative to the proposals root, never absolute. An
            # absolute path pins the artifact to one checkout: it breaks the
            # moment a proposal is reviewed elsewhere, embeds the local
            # filesystem layout (username included) in a file a human reads,
            # and makes two runs of identical content differ byte-for-byte.
            generated = replace(
                proposal,
                generated_schema_path=self._relative_artifact_path(schema_path),
                generated_semantic_draft_path=self._relative_artifact_path(draft_path),
            )
            self._write_json_atomic(staging_dir / "proposal.json", generated.to_dict())
            if self._publish(staging_dir, target_dir):
                staging_dir = None
            return generated
        except ProposalGenerationError:
            raise
        except Exception as exc:
            raise ProposalGenerationError(
                f"Proposal '{proposal.proposal_id}' was not generated: {exc}"
            ) from exc
        finally:
            if staging_dir is not None:
                try:
                    shutil.rmtree(staging_dir)
                except OSError as exc:
                    raise ProposalGenerationError(
                        f"Cannot remove incomplete proposal staging directory: {exc}"
                    ) from exc

    @staticmethod
    def _validate_inputs(
        proposal: OptimizationProposal, pipeline: Mapping[str, Any]
    ) -> None:
        if not isinstance(proposal, OptimizationProposal):
            raise ProposalGenerationError(
                "proposal must be an OptimizationProposal instance."
            )
        validate_proposal_id(proposal.proposal_id)
        if proposal.kind not in _PHYSICAL_KINDS:
            raise ProposalGenerationError(
                f"Proposal kind '{proposal.kind}' does not produce physical artifacts in slice 8.4."
            )
        if proposal.status != "proposed":
            raise ProposalGenerationError(
                "Only a proposal with status 'proposed' can be generated."
            )
        if (
            proposal.generated_schema_path is not None
            or proposal.generated_semantic_draft_path is not None
        ):
            raise ProposalGenerationError(
                "Proposal artifact paths must be empty before generation."
            )
        if not isinstance(pipeline, Mapping):
            raise ProposalGenerationError("pipeline must be a mapping.")

    @staticmethod
    def _load_and_validate_pipeline(
        proposal: OptimizationProposal, schema_path: Path
    ) -> ParsedSchema:
        try:
            normalized = load_schema(str(schema_path))
            parsed = parse_to_ir(normalized)
        except Exception as exc:
            raise ProposalGenerationError(
                f"Generated pipeline failed load_schema validation: {exc}"
            ) from exc

        materialization = (parsed.materialization or {}).get("type")
        expected = _EXPECTED_MATERIALIZATION[proposal.kind]
        if materialization != expected:
            raise ProposalGenerationError(
                f"Proposal kind '{proposal.kind}' requires materialization type '{expected}', "
                f"got '{materialization}'."
            )
        if parsed.aggregate is None:
            raise ProposalGenerationError(
                f"Proposal kind '{proposal.kind}' requires the Plan 28 'aggregate' block."
            )
        if proposal.kind == "materialized_view":
            try:
                compile_select(parsed)
            except Exception as exc:
                raise ProposalGenerationError(
                    f"Generated materialized view failed compile_select validation: {exc}"
                ) from exc
        return parsed

    @staticmethod
    def _read_yaml(path: Path) -> dict[str, Any]:
        with path.open(encoding="utf-8") as handle:
            payload = yaml.safe_load(handle)
        if not isinstance(payload, dict):
            raise ProposalGenerationError(
                "Generated semantic draft is not a YAML mapping."
            )
        return payload

    def resolve(self, recorded_path: str) -> Path:
        """Turn a path recorded in a proposal back into a usable file path.

        Recorded paths are relative to the proposals root so an artifact stays
        readable after the checkout moves. Consumers resolve through here rather
        than re-deriving the join, which would drift from the writer.
        """
        candidate = Path(recorded_path)
        if candidate.is_absolute() or ".." in candidate.parts:
            raise ProposalGenerationError("Recorded artifact path is not relative.")
        root = self.proposals_dir.resolve()
        resolved = (root / candidate).resolve()
        if not resolved.is_relative_to(root):
            raise ProposalGenerationError("Recorded artifact path escapes the root.")
        return resolved

    def _relative_artifact_path(self, path: Path) -> str:
        """Posix path relative to the proposals root, stable across checkouts."""
        return Path(path).resolve().relative_to(self.proposals_dir.resolve()).as_posix()

    @staticmethod
    def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
        content = json.dumps(
            payload,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            separators=(",", ": "),
        ) + "\n"
        fd, temp_path = tempfile.mkstemp(
            dir=str(path.parent), prefix=f".{path.stem}.", suffix=".tmp", text=True
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(content)
            os.replace(temp_path, path)
        except Exception:
            try:
                os.unlink(temp_path)
            except FileNotFoundError:
                pass
            raise

    @classmethod
    def _publish(cls, staging_dir: Path, target_dir: Path) -> bool:
        """Publish a new directory; return whether the staging dir was consumed."""
        if target_dir.exists():
            if not target_dir.is_dir():
                raise ProposalGenerationError(
                    f"Proposal path '{target_dir}' exists and is not a directory."
                )
            if cls._directory_bytes(target_dir) == cls._directory_bytes(staging_dir):
                return False
            raise ProposalGenerationError(
                f"Proposal directory '{target_dir}' already exists with different content; "
                "refusing to overwrite it."
            )
        os.replace(staging_dir, target_dir)
        return True

    @staticmethod
    def _directory_bytes(directory: Path) -> tuple[tuple[str, bytes], ...]:
        try:
            return tuple(
                (str(path.relative_to(directory)), path.read_bytes())
                for path in sorted(item for item in directory.rglob("*") if item.is_file())
            )
        except OSError as exc:
            raise ProposalGenerationError(
                f"Cannot inspect existing proposal directory '{directory}': {exc}"
            ) from exc
