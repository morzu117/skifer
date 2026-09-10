"""Human-only review workflow for generated adaptive Gold proposals."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
import difflib
import json
import os
from pathlib import Path
from typing import Callable, Iterable

from skifer.core.ir import parse_to_ir
from skifer.core.schema_loader import load_schema
from skifer.observability.certification import canonicalize_contract

from .generator import (
    ProposalGenerationError,
    ProposalGenerator,
    validate_proposal_id,
)
from .models import OptimizationProposal
from .evaluator import DeliveryRecord, OutcomeEvaluation


class AdaptiveWorkflowError(RuntimeError):
    """A proposal cannot be safely inspected or transitioned."""


class AdaptiveWorkflowConflict(AdaptiveWorkflowError):
    """A requested review transition would overwrite or contradict state."""


class StaleProposalError(AdaptiveWorkflowError):
    """A proposal no longer matches the definitions that justified it."""


class AdaptiveWorkflow:
    """Review proposals without invoking Git, Spark, an LLM, or the engine."""

    def __init__(
        self,
        proposals_dir: str | Path = ".skifer_proposals",
        *,
        schemas_dir: str | Path = "schemas",
        current_source_hashes: Callable[[], Iterable[str]] | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if clock is not None and not callable(clock):
            raise TypeError("clock must be callable or None.")
        self.generator = ProposalGenerator(proposals_dir)
        self.schemas_dir = Path(schemas_dir)
        self._current_source_hashes = current_source_hashes
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def list(self) -> tuple[OptimizationProposal, ...]:
        root = self.generator.proposals_dir
        if not root.is_dir():
            raise AdaptiveWorkflowError("Proposal directory '.skifer_proposals' is missing.")
        proposals = []
        for directory in sorted(path for path in root.iterdir() if path.is_dir()):
            if directory.name.startswith("."):
                continue
            proposals.append(self.load(directory.name))
        return tuple(proposals)

    def load(self, proposal_id: str) -> OptimizationProposal:
        self._validate_id(proposal_id)
        proposal_path = self.generator.resolve(f"{proposal_id}/proposal.json")
        if not proposal_path.is_file():
            raise AdaptiveWorkflowError(f"Unknown proposal '{proposal_id}'.")
        try:
            payload = json.loads(proposal_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise AdaptiveWorkflowError(
                f"Proposal '{proposal_id}' has a malformed proposal.json."
            ) from exc
        if not isinstance(payload, dict):
            raise AdaptiveWorkflowError(
                f"Proposal '{proposal_id}' has a malformed proposal.json."
            )
        try:
            proposal = OptimizationProposal(
                proposal_id=payload["proposal_id"],
                kind=payload["kind"],
                rule_id=payload["rule_id"],
                rule_version=payload["rule_version"],
                evidence_event_ids=tuple(payload["evidence_event_ids"]),
                expected_benefit=payload["expected_benefit"],
                risks=tuple(payload["risks"]),
                generated_schema_path=payload["generated_schema_path"],
                generated_semantic_draft_path=payload[
                    "generated_semantic_draft_path"
                ],
                source_definition_hashes=tuple(
                    payload["source_definition_hashes"]
                ),
                status=payload["status"],
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise AdaptiveWorkflowError(
                f"Proposal '{proposal_id}' has a malformed proposal.json."
            ) from exc
        if set(payload) != set(proposal.to_dict()) or proposal.proposal_id != proposal_id:
            raise AdaptiveWorkflowError(
                f"Proposal '{proposal_id}' has a malformed proposal.json."
            )
        self._resolve_artifacts(proposal, require_schema=False)
        return proposal

    def show(self, proposal_id: str) -> dict:
        """Return the allowlisted proposal payload, never filesystem internals."""
        return self.load(proposal_id).to_dict()

    def diff(self, proposal_id: str, output: str | Path | None = None) -> str:
        """Return a unified diff against an existing target or an empty file."""
        proposal = self.load(proposal_id)
        schema_path = self._schema_path(proposal)
        try:
            proposed_lines = schema_path.read_text(encoding="utf-8").splitlines(True)
            existing_lines = (
                Path(output).read_text(encoding="utf-8").splitlines(True)
                if output is not None and Path(output).exists()
                else []
            )
        except (OSError, UnicodeError) as exc:
            raise AdaptiveWorkflowError("Cannot read a schema for adaptive diff.") from exc
        target_label = self._display_path(output) if output is not None else "/dev/null"
        return "".join(
            difflib.unified_diff(
                existing_lines,
                proposed_lines,
                fromfile=target_label,
                tofile=proposal.generated_schema_path or "pipeline.yaml",
            )
        )

    def accept(
        self,
        proposal_id: str,
        output: str | Path,
        *,
        if_identical: bool = False,
    ) -> OptimizationProposal:
        """Copy a current proposal to a new human-owned path without overwriting."""
        proposal = self.load(proposal_id)
        self._assert_current_or_mark_stale(proposal)
        schema_path = self._schema_path(proposal)
        output_path = Path(output)
        if output_path.exists():
            if if_identical and output_path.is_file():
                try:
                    identical = output_path.read_bytes() == schema_path.read_bytes()
                except OSError as exc:
                    raise AdaptiveWorkflowError("Cannot compare the requested output.") from exc
                if identical and proposal.status == "accepted":
                    return proposal
            raise AdaptiveWorkflowConflict(
                "The requested output already exists; refusing to overwrite it."
            )
        if proposal.status != "proposed":
            raise AdaptiveWorkflowConflict(
                f"Proposal '{proposal_id}' has status '{proposal.status}' and cannot be accepted."
            )

        try:
            content = schema_path.read_bytes()
            output_path.parent.mkdir(parents=True, exist_ok=True)
            descriptor = os.open(
                output_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644
            )
            try:
                with os.fdopen(descriptor, "wb") as handle:
                    handle.write(content)
                    handle.flush()
                    os.fsync(handle.fileno())
            except Exception:
                try:
                    output_path.unlink()
                except OSError:
                    pass
                raise
            accepted = replace(proposal, status="accepted")
            delivery_path = self.generator.resolve(f"{proposal_id}/delivery.json")
            try:
                delivery = DeliveryRecord(
                    proposal_id=proposal_id,
                    output_path=str(output),
                    delivered_at=self._clock(),
                )
                self.generator._write_json_atomic(delivery_path, delivery.to_dict())
                self._write_proposal(accepted)
            except Exception:
                try:
                    output_path.unlink()
                except OSError:
                    pass
                try:
                    delivery_path.unlink()
                except OSError:
                    pass
                raise
            return accepted
        except AdaptiveWorkflowError:
            raise
        except FileExistsError as exc:
            raise AdaptiveWorkflowConflict(
                "The requested output already exists; refusing to overwrite it."
            ) from exc
        except OSError as exc:
            raise AdaptiveWorkflowError("Cannot persist the accepted proposal.") from exc

    def reject(self, proposal_id: str, reason: str) -> OptimizationProposal:
        if not isinstance(reason, str) or not reason.strip():
            raise AdaptiveWorkflowError("Reject requires a non-empty reason.")
        proposal = self.load(proposal_id)
        decision_path = self.generator.resolve(f"{proposal_id}/decision.json")
        if proposal.status == "rejected" and decision_path.is_file():
            try:
                previous = json.loads(decision_path.read_text(encoding="utf-8"))
            except (OSError, UnicodeError, json.JSONDecodeError) as exc:
                raise AdaptiveWorkflowError("Stored rejection decision is malformed.") from exc
            if previous == {"decision": "rejected", "reason": reason.strip()}:
                return proposal
        if proposal.status != "proposed":
            raise AdaptiveWorkflowConflict(
                f"Proposal '{proposal_id}' has status '{proposal.status}' and cannot be rejected."
            )
        rejected = replace(proposal, status="rejected")
        self._write_decision(
            proposal_id, {"decision": "rejected", "reason": reason.strip()}
        )
        self._write_proposal(rejected)
        return rejected

    def load_delivery(self, proposal_id: str) -> DeliveryRecord:
        """Load the exact delivery linked to an accepted proposal."""
        proposal = self.load(proposal_id)
        delivery_path = self.generator.resolve(f"{proposal_id}/delivery.json")
        if proposal.status not in {"accepted", "measured"} or not delivery_path.is_file():
            raise AdaptiveWorkflowConflict(
                f"Proposal '{proposal_id}' has not been delivered."
            )
        try:
            payload = json.loads(delivery_path.read_text(encoding="utf-8"))
            delivery = DeliveryRecord.from_dict(payload)
        except (OSError, UnicodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
            raise AdaptiveWorkflowError("Stored delivery record is malformed.") from exc
        if delivery.proposal_id != proposal_id:
            raise AdaptiveWorkflowError("Stored delivery record is malformed.")
        return delivery

    def record_outcome(self, evaluation: OutcomeEvaluation) -> OptimizationProposal:
        """Persist a measurement and transition an accepted proposal to measured."""
        if not isinstance(evaluation, OutcomeEvaluation):
            raise TypeError("evaluation must be an OutcomeEvaluation instance.")
        proposal = self.load(evaluation.proposal_id)
        if proposal.status not in {"accepted", "measured"}:
            raise AdaptiveWorkflowConflict(
                f"Proposal '{proposal.proposal_id}' has status '{proposal.status}' "
                "and cannot be measured."
            )
        path = self.generator.resolve(f"{proposal.proposal_id}/evaluation.json")
        payload = evaluation.to_dict()
        if proposal.status == "measured" and path.is_file():
            try:
                previous = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, UnicodeError, json.JSONDecodeError) as exc:
                raise AdaptiveWorkflowError("Stored outcome evaluation is malformed.") from exc
            if previous == payload:
                return proposal
        try:
            self.generator._write_json_atomic(path, payload)
        except OSError as exc:
            raise AdaptiveWorkflowError("Cannot persist outcome evaluation.") from exc
        measured = replace(proposal, status="measured")
        self._write_proposal(measured)
        return measured

    def _assert_current_or_mark_stale(self, proposal: OptimizationProposal) -> None:
        if proposal.status == "stale":
            raise StaleProposalError(
                f"Proposal '{proposal.proposal_id}' is stale and cannot be accepted."
            )
        current = set(self._source_hashes(proposal))
        expected = set(proposal.source_definition_hashes)
        if not expected.issubset(current):
            stale = replace(proposal, status="stale")
            self._write_proposal(stale)
            raise StaleProposalError(
                f"Proposal '{proposal.proposal_id}' is stale and cannot be accepted."
            )

    def _source_hashes(self, proposal: OptimizationProposal) -> tuple[str, ...]:
        if self._current_source_hashes is not None:
            try:
                values = tuple(self._current_source_hashes())
            except Exception as exc:
                raise AdaptiveWorkflowError(
                    "Cannot revalidate current source definitions."
                ) from exc
            if any(not isinstance(value, str) or not value for value in values):
                raise AdaptiveWorkflowError(
                    "Cannot revalidate current source definitions."
                )
            return values
        if not self.schemas_dir.is_dir():
            return ()
        try:
            proposed_schema = parse_to_ir(load_schema(str(self._schema_path(proposal))))
        except Exception as exc:
            raise AdaptiveWorkflowError(
                "Cannot revalidate current source definitions."
            ) from exc
        source_datasets = tuple(sorted({table.name for table in proposed_schema.tables}))
        matches: dict[str, list[str]] = {dataset: [] for dataset in source_datasets}
        for path in sorted(self.schemas_dir.rglob("*.y*ml")):
            try:
                schema = parse_to_ir(load_schema(str(path)))
                sink = schema.sink or {}
                if not sink.get("table"):
                    continue
                target = (
                    f"{sink['schema']}.{sink['table']}"
                    if sink.get("schema")
                    else str(sink["table"])
                )
                definition_hash = canonicalize_contract(schema).definition_hash
            except Exception:
                # A broken/non-contract file can never prove that an expected
                # source is still current. Missing proof is handled as stale.
                continue
            for dataset in source_datasets:
                if dataset == target or dataset.endswith(f".{target}"):
                    matches[dataset].append(definition_hash)
        # Zero matches cannot prove currency; several matches make the current
        # definition ambiguous. Both cases intentionally contribute no hash.
        return tuple(
            definitions[0]
            for dataset in source_datasets
            for definitions in (matches[dataset],)
            if len(definitions) == 1
        )

    def _schema_path(self, proposal: OptimizationProposal) -> Path:
        if proposal.generated_schema_path is None:
            raise AdaptiveWorkflowError(
                f"Proposal '{proposal.proposal_id}' has no generated schema."
            )
        paths = self._resolve_artifacts(proposal, require_schema=True)
        assert paths[0] is not None
        return paths[0]

    def _resolve_artifacts(
        self, proposal: OptimizationProposal, *, require_schema: bool
    ) -> tuple[Path | None, Path | None]:
        resolved: list[Path | None] = []
        for recorded in (
            proposal.generated_schema_path,
            proposal.generated_semantic_draft_path,
        ):
            if recorded is None:
                resolved.append(None)
                continue
            try:
                path = self.generator.resolve(recorded)
            except ProposalGenerationError as exc:
                raise AdaptiveWorkflowError(
                    f"Proposal '{proposal.proposal_id}' records an invalid artifact path."
                ) from exc
            if not path.is_file():
                raise AdaptiveWorkflowError(
                    f"Proposal '{proposal.proposal_id}' is missing a generated artifact."
                )
            resolved.append(path)
        if require_schema and resolved[0] is None:
            raise AdaptiveWorkflowError(
                f"Proposal '{proposal.proposal_id}' has no generated schema."
            )
        return resolved[0], resolved[1]

    def _write_proposal(self, proposal: OptimizationProposal) -> None:
        path = self.generator.resolve(f"{proposal.proposal_id}/proposal.json")
        try:
            self.generator._write_json_atomic(path, proposal.to_dict())
        except OSError as exc:
            raise AdaptiveWorkflowError("Cannot persist proposal status.") from exc

    def _write_decision(self, proposal_id: str, payload: dict) -> None:
        path = self.generator.resolve(f"{proposal_id}/decision.json")
        try:
            self.generator._write_json_atomic(path, payload)
        except OSError as exc:
            raise AdaptiveWorkflowError("Cannot persist proposal decision.") from exc

    @staticmethod
    def _validate_id(proposal_id: str) -> None:
        try:
            validate_proposal_id(proposal_id)
        except ProposalGenerationError as exc:
            raise AdaptiveWorkflowError("Invalid proposal id.") from exc

    @staticmethod
    def _display_path(path: str | Path) -> str:
        candidate = Path(path)
        if not candidate.is_absolute():
            return candidate.as_posix()
        try:
            return candidate.resolve().relative_to(Path.cwd().resolve()).as_posix()
        except ValueError:
            return candidate.name
