"""
CLI Skifer — commande `skifer hub`.

Usage :
    skifer hub                           # config.yaml du répertoire courant
    skifer hub --config path/config.yaml
    skifer hub --new-session             # force une nouvelle session
"""

from __future__ import annotations

import argparse
from dataclasses import asdict
import json
import os
from pathlib import Path
import re
import sys

from skifer.semantic.sync import assert_no_curation_loss as _assert_no_curation_loss


SEMANTIC_EXIT_OK = 0
SEMANTIC_EXIT_ERROR = 1
SEMANTIC_EXIT_DRIFT = 2
SEMANTIC_EXIT_CONFLICT = 3

# Adaptive CLI contract: 0 success; 1 malformed/technical failure; argparse uses
# 2 for command-line usage; 3 stale source definitions; 4 state/output conflict.
ADAPTIVE_EXIT_OK = 0
ADAPTIVE_EXIT_ERROR = 1
ADAPTIVE_EXIT_USAGE = 2
ADAPTIVE_EXIT_STALE = 3
ADAPTIVE_EXIT_CONFLICT = 4
ADAPTIVE_EXIT_REGRESSED = 5

INCIDENTS_EXIT_OK = 0
INCIDENTS_EXIT_ERROR = 1
INCIDENTS_EXIT_USAGE = 2
INCIDENTS_EXIT_INVALID_TRANSITION = 3
INCIDENTS_EXIT_NOT_FOUND = 4

INDEX_EXIT_OK = 0
INDEX_EXIT_ERROR = 1
INDEX_EXIT_USAGE = 2

META_EXIT_OK = 0
META_EXIT_ERROR = 1
META_EXIT_USAGE = 2
META_EXIT_NOT_FOUND = 3

AUDIT_EXIT_OK = 0
AUDIT_EXIT_ERROR = 1
AUDIT_EXIT_BELOW_THRESHOLD = 2

_PLACEHOLDER_RE = re.compile(r"\{\{\s*(\w+)\s*\}\}")


def main() -> None:
    """Point d'entrée principal de la CLI skifer."""
    parser = argparse.ArgumentParser(
        prog="skifer",
        description="Skifer — framework déclaratif de data engineering.",
    )
    subparsers = parser.add_subparsers(dest="command")

    validate_parser = subparsers.add_parser(
        "validate",
        help="Validate one or more pipeline YAML schema files (no Spark required).",
    )
    validate_parser.add_argument(
        "paths",
        nargs="+",
        metavar="PATH",
        help="Schema file path(s) or glob patterns (e.g. schemas/**/*.yaml).",
    )

    audit_parser = subparsers.add_parser(
        "audit",
        help="Audit governance coverage for pipeline YAML files (no Spark required).",
    )
    audit_parser.add_argument(
        "paths",
        nargs="+",
        metavar="PATHS",
        help="Schema file path(s) or glob patterns (e.g. schemas/**/*.yaml).",
    )
    audit_parser.add_argument(
        "--json",
        action="store_true",
        help="Print a stable sorted-key JSON report.",
    )
    audit_parser.add_argument(
        "--min-coverage",
        type=float,
        default=None,
        metavar="N",
        help="Exit 2 when overall coverage is below this percentage.",
    )

    index_parser = subparsers.add_parser(
        "index",
        help="Index pipeline metadata into the registry (no Spark).",
    )
    index_parser.add_argument(
        "paths",
        nargs="+",
        metavar="PATHS",
        help="Pipeline YAML paths to index.",
    )
    index_parser.add_argument(
        "--db",
        default=".skifer_metadata.db",
        help="SQLite registry path.",
    )
    index_parser.add_argument(
        "--target-fqn",
        default=None,
        help="Override target FQN (single path only).",
    )

    lineage_parser = subparsers.add_parser(
        "lineage",
        help="Show lineage for a dataset column from the registry.",
    )
    lineage_parser.add_argument(
        "target",
        metavar="FQN[.column]",
        help=(
            "Dataset FQN, optionally .column. If the whole value matches a "
            "registry FQN it selects the dataset; otherwise the final dot "
            "separates the column."
        ),
    )
    lineage_parser.add_argument("--direction", choices=["up", "down"], default="down")
    lineage_parser.add_argument("--format", choices=["mermaid", "json"], default="mermaid")
    lineage_parser.add_argument("--db", default=".skifer_metadata.db")

    dictionary_parser = subparsers.add_parser(
        "dictionary",
        help="Show the column dictionary for a dataset.",
    )
    dictionary_parser.add_argument("target", metavar="FQN")
    dictionary_parser.add_argument("--db", default=".skifer_metadata.db")
    dictionary_parser.add_argument("--format", choices=["text", "json"], default="text")

    hub_parser = subparsers.add_parser(
        "hub",
        help="Lance le REPL conversationnel SkiferHub.",
    )
    hub_parser.add_argument(
        "--config",
        default="config.yaml",
        help="Chemin vers config.yaml (défaut : config.yaml dans le répertoire courant).",
    )
    hub_parser.add_argument(
        "--new-session",
        action="store_true",
        help="Force une nouvelle session (efface la session précédente).",
    )

    semantic_parser = subparsers.add_parser(
        "semantic",
        help="Semantic draft synchronization and validation helpers.",
    )
    semantic_subparsers = semantic_parser.add_subparsers(dest="semantic_command")

    semantic_sync_parser = semantic_subparsers.add_parser(
        "sync",
        help="Compare a pipeline projection with its managed semantic draft.",
    )
    semantic_sync_parser.add_argument(
        "pipeline",
        metavar="PIPELINE",
        help="Pipeline YAML file path.",
    )
    semantic_sync_mode = semantic_sync_parser.add_mutually_exclusive_group(required=True)
    semantic_sync_mode.add_argument(
        "--check",
        action="store_true",
        help="Report drift only; never write drafts or curated models.",
    )
    semantic_sync_mode.add_argument(
        "--write-draft",
        action="store_true",
        help="Write the managed draft when the sync report is conflict-free.",
    )
    semantic_sync_mode.add_argument(
        "--promote",
        action="store_true",
        help="Promote the managed draft to the curated semantic model and update the catalog.",
    )

    semantic_validate_parser = semantic_subparsers.add_parser(
        "validate",
        help="Validate one semantic model YAML against one pipeline projection.",
    )
    semantic_validate_parser.add_argument(
        "pipeline",
        metavar="PIPELINE",
        help="Pipeline YAML file path.",
    )
    semantic_validate_parser.add_argument(
        "model",
        metavar="MODEL",
        help="Semantic model YAML file path.",
    )

    mcp_parser = subparsers.add_parser(
        "mcp",
        help="Run the optional read-only MCP server.",
    )
    mcp_subparsers = mcp_parser.add_subparsers(dest="mcp_command")
    mcp_serve_parser = mcp_subparsers.add_parser(
        "serve",
        help="Serve governed resources and semantic queries over MCP.",
    )
    mcp_serve_parser.add_argument(
        "--transport",
        required=True,
        choices=("stdio", "http"),
        help="MCP transport (must match the configuration file).",
    )
    mcp_serve_parser.add_argument(
        "--config",
        required=True,
        help="Path to the closed MCP server YAML configuration.",
    )

    adaptive_parser = subparsers.add_parser(
        "adaptive",
        help="Review adaptive Gold proposals; never deploy or invoke Git.",
    )
    adaptive_subparsers = adaptive_parser.add_subparsers(dest="adaptive_command")
    adaptive_subparsers.add_parser("list", help="List generated proposals.")

    adaptive_show_parser = adaptive_subparsers.add_parser(
        "show", help="Show one proposal's allowlisted review record."
    )
    adaptive_show_parser.add_argument("proposal", metavar="PROPOSAL")

    adaptive_diff_parser = adaptive_subparsers.add_parser(
        "diff", help="Diff a proposed schema against a target or an empty file."
    )
    adaptive_diff_parser.add_argument("proposal", metavar="PROPOSAL")
    adaptive_diff_parser.add_argument(
        "--output",
        help="Optional target schema to compare; a missing target is treated as empty.",
    )

    adaptive_accept_parser = adaptive_subparsers.add_parser(
        "accept", help="Copy a current proposal to a new schema path without deploying it."
    )
    adaptive_accept_parser.add_argument("proposal", metavar="PROPOSAL")
    adaptive_accept_parser.add_argument("--output", required=True, metavar="PATH")
    adaptive_accept_parser.add_argument(
        "--if-identical",
        action="store_true",
        help="Idempotent no-op only when an already accepted output is byte-identical.",
    )

    adaptive_reject_parser = adaptive_subparsers.add_parser(
        "reject", help="Reject a proposal with a mandatory human reason."
    )
    adaptive_reject_parser.add_argument("proposal", metavar="PROPOSAL")
    adaptive_reject_parser.add_argument("--reason", required=True)

    adaptive_evaluate_parser = adaptive_subparsers.add_parser(
        "evaluate", help="Measure usage before and after an accepted proposal."
    )
    adaptive_evaluate_parser.add_argument("proposal", metavar="PROPOSAL")
    adaptive_evaluate_parser.add_argument(
        "--store",
        default=".skifer_adaptive.db",
        metavar="PATH",
        help="SQLite usage store path (default: .skifer_adaptive.db).",
    )
    adaptive_evaluate_parser.add_argument(
        "--window-days",
        default=30,
        type=int,
        metavar="N",
        help="Days in each comparison window (default: 30).",
    )

    incidents_parser = subparsers.add_parser(
        "incidents",
        help="Manage data-quality incidents.",
    )
    incidents_subparsers = incidents_parser.add_subparsers(dest="incidents_command")
    incidents_list_parser = incidents_subparsers.add_parser(
        "list",
        help="List incidents.",
    )
    incidents_list_parser.add_argument(
        "--status",
        choices=["NEW", "ACKNOWLEDGED", "ASSIGNED", "RESOLVED"],
    )
    incidents_list_parser.add_argument("--target", dest="target_fqn", metavar="FQN")
    incidents_list_parser.add_argument("--limit", type=int, default=50)
    incidents_list_parser.add_argument(
        "--store",
        default=".skifer_certification.db",
        metavar="PATH",
    )

    incidents_ack_parser = incidents_subparsers.add_parser(
        "ack",
        help="Acknowledge an incident.",
    )
    incidents_ack_parser.add_argument("incident_id", metavar="INCIDENT_ID")
    incidents_ack_parser.add_argument(
        "--store",
        default=".skifer_certification.db",
    )

    incidents_assign_parser = incidents_subparsers.add_parser(
        "assign",
        help="Assign an incident.",
    )
    incidents_assign_parser.add_argument("incident_id", metavar="INCIDENT_ID")
    incidents_assign_parser.add_argument("--assignee", required=True)
    incidents_assign_parser.add_argument(
        "--store",
        default=".skifer_certification.db",
    )

    incidents_resolve_parser = incidents_subparsers.add_parser(
        "resolve",
        help="Resolve an incident.",
    )
    incidents_resolve_parser.add_argument("incident_id", metavar="INCIDENT_ID")
    incidents_resolve_parser.add_argument(
        "--root-cause",
        dest="root_cause",
        required=True,
    )
    incidents_resolve_parser.add_argument(
        "--store",
        default=".skifer_certification.db",
    )

    contract_parser = subparsers.add_parser(
        "contract",
        help="Data-contract utilities (Plan 31).",
    )
    contract_subparsers = contract_parser.add_subparsers(dest="contract_command")
    contract_import_parser = contract_subparsers.add_parser(
        "import",
        help="Import an ODCS 3.1 DataContract and print the Skifer YAML block.",
    )
    contract_import_parser.add_argument(
        "file",
        metavar="FILE",
        help="Path to an ODCS 3.1 YAML/JSON document.",
    )

    args = parser.parse_args()

    if args.command == "validate":
        _run_validate(args)
    elif args.command == "audit":
        _run_audit(args)
    elif args.command == "index":
        _run_index(args)
    elif args.command == "lineage":
        _run_lineage(args)
    elif args.command == "dictionary":
        _run_dictionary(args)
    elif args.command == "hub":
        _run_hub(args)
    elif args.command == "semantic":
        _run_semantic(args)
    elif args.command == "mcp":
        _run_mcp(args)
    elif args.command == "adaptive":
        _run_adaptive(args)
    elif args.command == "incidents":
        _run_incidents(args)
    elif args.command == "contract":
        _run_contract(args)
    else:
        parser.print_help()
        sys.exit(1)


def _run_adaptive(args: argparse.Namespace) -> None:
    """Run the human review boundary with stable, category-specific exit codes."""
    sys.exit(run_adaptive_command(args))


def _run_incidents(args: argparse.Namespace) -> None:
    """Run incident management with stable, category-specific exit codes."""
    sys.exit(run_incidents_command(args))


def _run_index(args: argparse.Namespace) -> None:
    """Run Spark-free metadata indexing with stable exit codes."""
    sys.exit(run_index_command(args))


def _run_audit(args: argparse.Namespace) -> None:
    """Run governance coverage audit with stable exit codes."""
    sys.exit(run_audit(args.paths, as_json=args.json, min_coverage=args.min_coverage))


def _run_lineage(args: argparse.Namespace) -> None:
    """Render registry-backed lineage with stable exit codes."""
    sys.exit(run_lineage_command(args))


def _run_dictionary(args: argparse.Namespace) -> None:
    """Render the registry-backed column dictionary with stable exit codes."""
    sys.exit(run_dictionary_command(args))


def run_index_command(args: argparse.Namespace, *, store=None) -> int:
    """Index one or more pipeline YAML files into the local metadata registry."""
    from skifer.observability.metadata_index import index_from_path
    from skifer.observability.metadata_store import SqliteMetadataStore

    if args.target_fqn and len(args.paths) > 1:
        print("[index] --target-fqn is only valid with a single path.", file=sys.stderr)
        return INDEX_EXIT_USAGE

    registry = store or SqliteMetadataStore(args.db)
    changed = 0
    for path in args.paths:
        try:
            wrote = index_from_path(path, registry, target_fqn=args.target_fqn)
        except Exception as exc:
            print(f"[index] Failed to index '{path}': {exc}", file=sys.stderr)
            return INDEX_EXIT_ERROR
        changed += int(wrote)
        print(f"[index] {path} -> {'updated' if wrote else 'unchanged'}")

    print(f"[index] {changed} record(s) written, {len(args.paths) - changed} unchanged.")
    return INDEX_EXIT_OK


def run_lineage_command(args: argparse.Namespace, *, store=None) -> int:
    """Show upstream or downstream lineage from the persisted metadata registry."""
    from skifer.lineage.renderer import LineageRenderer
    from skifer.lineage.tracker import LineageGraph
    from skifer.observability.metadata_store import (
        MetadataRegistryQuery,
        SqliteMetadataStore,
    )

    if args.direction not in {"up", "down"} or args.format not in {"mermaid", "json"}:
        print("[lineage] Invalid direction or format.", file=sys.stderr)
        return META_EXIT_USAGE

    registry = store or SqliteMetadataStore(args.db)
    try:
        resolved = _resolve_registry_target(registry, args.target)
        if resolved is None:
            print(
                f"[lineage] '{args.target}' was not found in the metadata registry.",
                file=sys.stderr,
            )
            return META_EXIT_NOT_FOUND
        fqn, columns = resolved
        query = MetadataRegistryQuery(registry)
        graph = LineageGraph()
        for column in columns:
            edges = (
                query.upstream(fqn, column)
                if args.direction == "up"
                else query.downstream(fqn, column)
            )
            for edge in edges:
                graph.add_edge(edge)
        if args.format == "json":
            print(json.dumps(graph.to_dict(), sort_keys=True))
        else:
            print(LineageRenderer().to_mermaid(graph))
        return META_EXIT_OK
    except Exception as exc:
        print(f"[lineage] Failed to read metadata registry: {exc}", file=sys.stderr)
        return META_EXIT_ERROR


def run_dictionary_command(args: argparse.Namespace, *, store=None) -> int:
    """Show a dataset column dictionary from the persisted metadata registry."""
    from skifer.observability.metadata_store import SqliteMetadataStore

    if args.format not in {"text", "json"}:
        print("[dictionary] Invalid format.", file=sys.stderr)
        return META_EXIT_USAGE

    registry = store or SqliteMetadataStore(args.db)
    try:
        record = registry.get(args.target)
        if record is None:
            print(
                f"[dictionary] '{args.target}' was not found in the metadata registry.",
                file=sys.stderr,
            )
            return META_EXIT_NOT_FOUND
        columns = sorted(record.columns, key=lambda column: column.name)
        if args.format == "json":
            print(
                json.dumps(
                    {
                        "target_fqn": record.target_fqn,
                        "columns": [asdict(column) for column in columns],
                    },
                    default=str,
                    sort_keys=True,
                )
            )
        else:
            print(_format_dictionary_text(columns))
        return META_EXIT_OK
    except Exception as exc:
        print(f"[dictionary] Failed to read metadata registry: {exc}", file=sys.stderr)
        return META_EXIT_ERROR


def run_audit(
    paths: list[str], *, as_json: bool, min_coverage: float | None
) -> int:
    """Expand globs, audit files, print a stable report, and return the exit code."""
    import glob as glob_mod

    from skifer.observability.audit import METRIC_KEYS, audit_project

    try:
        expanded_paths: list[str] = []
        for pattern in paths:
            expanded = sorted(glob_mod.glob(pattern, recursive=True))
            if expanded:
                expanded_paths.extend(expanded)
            elif glob_mod.has_magic(pattern):
                continue
            else:
                expanded_paths.append(pattern)

        report = audit_project(expanded_paths)
        if as_json:
            print(
                json.dumps(
                    report.to_dict(),
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                )
            )
        elif not report.total:
            print("No schema files found.")
            print(_format_audit_text(report, METRIC_KEYS))
        else:
            print(_format_audit_text(report, METRIC_KEYS))

        if report.total and min_coverage is not None:
            if report.overall_coverage_pct < min_coverage:
                return AUDIT_EXIT_BELOW_THRESHOLD
        return AUDIT_EXIT_OK
    except Exception as exc:
        print(f"[audit] Command failed ({type(exc).__name__}).", file=sys.stderr)
        return AUDIT_EXIT_ERROR


def _resolve_registry_target(store, target: str) -> tuple[str, list[str]] | None:
    records = store.list_all()
    known_fqns = {record.target_fqn for record in records}
    if target in known_fqns:
        record = store.get(target)
        if record is None:
            return None
        return target, sorted(column.name for column in record.columns)

    fqn, sep, column = target.rpartition(".")
    if not sep or fqn not in known_fqns:
        return None
    record = store.get(fqn)
    if record is None:
        return None
    column_names = {item.name for item in record.columns}
    if column not in column_names:
        return None
    return fqn, [column]


def _format_dictionary_text(columns) -> str:
    rows = [
        (
            column.name,
            column.logical_type or "",
            column.classification or "",
            ",".join(column.sources),
        )
        for column in columns
    ]
    headers = ("name", "logical_type", "classification", "sources")
    widths = [
        max(len(str(row[index])) for row in (headers, *rows))
        for index in range(len(headers))
    ]

    def render(row: tuple[str, str, str, str]) -> str:
        return "  ".join(
            str(value).ljust(widths[index])
            for index, value in enumerate(row)
        ).rstrip()

    return "\n".join([render(headers), *(render(row) for row in rows)])


def _format_audit_text(report, metric_keys: tuple[str, ...]) -> str:
    lines = [
        (
            f"Audited {report.total} pipeline(s) — "
            f"{report.parsed} parsed, {report.errored} errored."
        ),
        "",
    ]
    for key in metric_keys:
        satisfying = sum(1 for pipeline in report.pipelines if pipeline.metric(key))
        lines.append(
            f"  {key:<21}{report.coverage[key]:>7.2f}%   ({satisfying}/{report.total})"
        )
    lines.extend(["", f"  {'overall':<21}{report.overall_coverage_pct:>7.2f}%"])
    failures = [pipeline for pipeline in report.pipelines if not pipeline.parsed]
    if failures:
        lines.append("")
        for pipeline in failures:
            lines.append(f"FAIL {pipeline.path}")
            if pipeline.error:
                lines.append(f"     {pipeline.error}")
    return "\n".join(lines)


def _run_contract(args: argparse.Namespace) -> None:
    """Run data-contract utilities with stable exit codes."""
    sys.exit(run_contract_command(args))


def run_contract_command(args: argparse.Namespace) -> int:
    """0 success; 1 technical/validation error; 2 usage error."""
    import yaml

    from skifer.observability.odcs import import_odcs_31

    if getattr(args, "contract_command", None) != "import":
        print("Contract command missing. Use 'skifer contract import FILE'.")
        return 2
    try:
        with open(args.file, encoding="utf-8") as fh:
            doc = yaml.safe_load(fh)
        result = import_odcs_31(doc)
    except (OSError, ValueError) as exc:
        print(f"Import failed: {exc}")
        return 1
    block = {"data_product": result.data_product, "contract": result.contract}
    print(yaml.safe_dump(block, sort_keys=False, allow_unicode=True))
    for warning in result.warnings:
        print(f"# warning: {warning}")
    return 0


def run_adaptive_command(args: argparse.Namespace, *, workflow=None) -> int:
    """Execute one adaptive action without importing any runtime/deployment code."""
    from skifer.adaptive.workflow import (
        AdaptiveWorkflow,
        AdaptiveWorkflowConflict,
        AdaptiveWorkflowError,
        StaleProposalError,
    )

    reviewer = workflow or AdaptiveWorkflow()
    command = getattr(args, "adaptive_command", None)
    if command is None:
        print(
            "Adaptive command missing. Use 'skifer adaptive --help'.",
            file=sys.stderr,
        )
        return ADAPTIVE_EXIT_USAGE
    try:
        if command == "list":
            proposals = reviewer.list()
            for proposal in proposals:
                print(f"{proposal.proposal_id}\t{proposal.kind}\t{proposal.status}")
            return ADAPTIVE_EXIT_OK
        if command == "show":
            print(
                json.dumps(
                    reviewer.show(args.proposal),
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                )
            )
            return ADAPTIVE_EXIT_OK
        if command == "diff":
            rendered = reviewer.diff(args.proposal, args.output)
            print(rendered, end="" if rendered.endswith("\n") else "\n")
            return ADAPTIVE_EXIT_OK
        if command == "accept":
            proposal = reviewer.accept(
                args.proposal,
                args.output,
                if_identical=args.if_identical,
            )
            print(
                f"Accepted {proposal.proposal_id} to "
                f"{reviewer._display_path(args.output)}; review and deploy separately."
            )
            return ADAPTIVE_EXIT_OK
        if command == "reject":
            proposal = reviewer.reject(args.proposal, args.reason)
            print(f"Rejected {proposal.proposal_id}.")
            return ADAPTIVE_EXIT_OK
        if command == "evaluate":
            from datetime import datetime, timezone

            from skifer.adaptive.evaluator import OutcomeEvaluator
            from skifer.adaptive.store import SqliteUsageEventStore

            proposal = reviewer.load(args.proposal)
            if proposal.status == "stale":
                raise StaleProposalError(
                    f"Proposal '{proposal.proposal_id}' is stale and cannot be evaluated."
                )
            delivery = reviewer.load_delivery(args.proposal)
            # sqlite3.connect() creates a missing file, so a mistyped path would
            # silently produce an empty store, report "evidence_unavailable" -
            # indistinguishable from a genuine retention purge - and still move
            # the proposal to "measured" on a measurement that never happened.
            if not Path(args.store).is_file():
                raise AdaptiveWorkflowError(
                    "The usage store does not exist; refusing to create an empty one."
                )
            store = SqliteUsageEventStore(args.store)
            try:
                evaluation = OutcomeEvaluator(
                    store,
                    clock=lambda: datetime.now(timezone.utc),
                    window_days=args.window_days,
                ).evaluate(proposal, delivery)
            finally:
                store.close()
            reviewer.record_outcome(evaluation)
            print(
                json.dumps(
                    evaluation.to_dict(),
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                )
            )
            return (
                ADAPTIVE_EXIT_REGRESSED
                if evaluation.outcome == "regressed"
                else ADAPTIVE_EXIT_OK
            )
        print("Unknown adaptive command.", file=sys.stderr)
        return ADAPTIVE_EXIT_USAGE
    except StaleProposalError as exc:
        print(f"[adaptive] {exc}", file=sys.stderr)
        return ADAPTIVE_EXIT_STALE
    except AdaptiveWorkflowConflict as exc:
        print(f"[adaptive] {exc}", file=sys.stderr)
        return ADAPTIVE_EXIT_CONFLICT
    except AdaptiveWorkflowError as exc:
        print(f"[adaptive] {exc}", file=sys.stderr)
        return ADAPTIVE_EXIT_ERROR
    except Exception as exc:
        # Filesystem/parser failures can embed local paths or credentials. The
        # class name retains a useful failure category without disclosing them.
        print(f"[adaptive] Command failed ({type(exc).__name__}).", file=sys.stderr)
        return ADAPTIVE_EXIT_ERROR


def _local_incidents_context():
    from skifer.services.identity import LocalIdentity

    return LocalIdentity.resolve().to_request_context()


def run_incidents_command(args: argparse.Namespace, *, service=None) -> int:
    """Execute one incident action through QualityService with stable exit codes."""
    from skifer.observability.incidents import InvalidIncidentTransition
    from skifer.services.context import ResourceNotFound

    command = getattr(args, "incidents_command", None)
    if command is None:
        print(
            "Incidents command missing. Use 'skifer incidents --help'.",
            file=sys.stderr,
        )
        return INCIDENTS_EXIT_USAGE
    if service is None:
        from skifer.observability.certification_store import SqliteCertificationStore
        from skifer.services.quality import QualityService

        store = SqliteCertificationStore(args.store)
        service = QualityService(history_store=None, incident_store=store)

    ctx = _local_incidents_context()
    try:
        if command == "list":
            views = service.list_incidents(
                ctx,
                status=getattr(args, "status", None),
                target_fqn=getattr(args, "target_fqn", None),
                limit=getattr(args, "limit", 50),
            )
            print(
                json.dumps(
                    [view.to_dict() for view in views],
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                )
            )
            return INCIDENTS_EXIT_OK
        if command == "ack":
            view = service.acknowledge_incident(ctx, args.incident_id)
        elif command == "assign":
            view = service.assign_incident(ctx, args.incident_id, args.assignee)
        elif command == "resolve":
            view = service.resolve_incident(ctx, args.incident_id, args.root_cause)
        else:
            print("Unknown incidents command.", file=sys.stderr)
            return INCIDENTS_EXIT_USAGE
        print(
            json.dumps(
                view.to_dict(),
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
        )
        return INCIDENTS_EXIT_OK
    except InvalidIncidentTransition as exc:
        print(f"[incidents] {exc}", file=sys.stderr)
        return INCIDENTS_EXIT_INVALID_TRANSITION
    except ResourceNotFound as exc:
        print(f"[incidents] {exc}", file=sys.stderr)
        return INCIDENTS_EXIT_NOT_FOUND
    except Exception as exc:
        print(f"[incidents] Command failed ({type(exc).__name__}).", file=sys.stderr)
        return INCIDENTS_EXIT_ERROR


def _run_validate(args: argparse.Namespace) -> None:
    """Validate one or more pipeline YAML schema files without Spark."""
    import glob as glob_mod
    # Expand glob patterns
    paths: list[str] = []
    for pattern in args.paths:
        expanded = sorted(glob_mod.glob(pattern, recursive=True))
        if expanded:
            paths.extend(expanded)
        else:
            paths.append(pattern)

    if not paths:
        print("No schema files found.")
        sys.exit(0)

    from skifer.core.schema_loader import parse_schema

    errors_by_file: dict[str, str] = {}

    for path in paths:
        try:
            yaml_str = _read_text_file(path)
        except OSError as exc:
            errors_by_file[path] = f"Cannot read file: {exc}"
            continue

        try:
            parse_schema(yaml_str, params=_sentinel_params(yaml_str))
        except ValueError as exc:
            errors_by_file[path] = str(exc)

    for path in paths:
        if path in errors_by_file:
            print(f"FAIL  {path}")
            for line in errors_by_file[path].splitlines():
                print(f"      {line}")
        else:
            print(f"OK    {path}")

    ok_count = len(paths) - len(errors_by_file)
    fail_count = len(errors_by_file)
    print(f"\n{ok_count} passed, {fail_count} failed.")

    if errors_by_file:
        sys.exit(1)


def _run_semantic(args: argparse.Namespace) -> None:
    """Thin wrapper around semantic CLI helpers with testable return codes."""
    if args.semantic_command == "sync":
        sys.exit(run_semantic_sync(args.pipeline, mode=_semantic_sync_mode(args)))
    if args.semantic_command == "validate":
        sys.exit(run_semantic_validate(args.pipeline, args.model))
    print("Semantic command missing. Use 'skifer semantic --help'.")
    sys.exit(SEMANTIC_EXIT_ERROR)


def _run_mcp(args: argparse.Namespace) -> None:
    """Load and serve MCP without importing its optional SDK for other commands."""
    if args.mcp_command != "serve":
        print("MCP command missing. Use 'skifer mcp --help'.", file=sys.stderr)
        sys.exit(1)
    try:
        from skifer.mcp.config import load_mcp_config

        config = load_mcp_config(args.config, transport=args.transport)
        from skifer.mcp.runtime import serve_mcp

        serve_mcp(config)
    except Exception as exc:
        from skifer.mcp.config import MCPConfigError
        from skifer.mcp.runtime import MCPStartupError
        from skifer.mcp.server import MCPDependencyError

        if isinstance(exc, (MCPConfigError, MCPDependencyError, MCPStartupError)):
            message = str(exc)
        else:
            # Startup exceptions may quote tokens, passwords, or credential
            # paths. Only the class name is safe outside the process.
            message = f"MCP startup failed ({type(exc).__name__})."
        print(f"[mcp] {message}", file=sys.stderr)
        sys.exit(1)


def run_semantic_sync(pipeline_path: str, *, mode: str) -> int:
    """Return the CI contract exit code for a semantic sync action."""
    try:
        schema = _load_pipeline_ir(pipeline_path)
        from skifer.semantic.output_projection import OutputProjector
        from skifer.semantic.persistence import build_catalog_entry, write_yaml_atomic
        from skifer.semantic.semantic import SemanticEngine
        from skifer.semantic.sync import SemanticSynchronizer
        from skifer.semantic.validator import SemanticValidator

        projector = OutputProjector()
        projected = projector.project(schema)
        models_dir = _semantic_models_dir()
        synchronizer = SemanticSynchronizer(output_dir=models_dir)
        report = synchronizer.sync(projected, schema, write=False)
    except Exception as exc:
        print(f"[semantic.sync] Failed to inspect pipeline '{pipeline_path}': {exc}")
        return SEMANTIC_EXIT_ERROR

    if report.conflicts or report.suggestions:
        _print_sync_report(pipeline_path, report)
        if mode == "promote":
            model_key = _sync_model_key(report.payload)
            print(
                "[semantic.sync] Refusing to promote curated model "
                f"'{model_key}': sync report contains conflicts or rename suggestions. "
                "Resolve the report, then rerun --promote."
            )
        return SEMANTIC_EXIT_CONFLICT

    if mode == "check":
        _print_sync_report(pipeline_path, report)
        return SEMANTIC_EXIT_DRIFT if report.has_changes else SEMANTIC_EXIT_OK

    if mode == "write-draft":
        try:
            write_report = synchronizer.sync(projected, schema, write=True)
        except Exception as exc:
            print(f"[semantic.sync] Failed to write draft for pipeline '{pipeline_path}': {exc}")
            return SEMANTIC_EXIT_ERROR
        _print_sync_report(pipeline_path, write_report)
        return SEMANTIC_EXIT_DRIFT if write_report.has_changes else SEMANTIC_EXIT_OK

    if report.payload is None:
        print(
            f"[semantic.sync] Refusing to promote pipeline '{pipeline_path}': no managed draft payload "
            "was produced. Run --write-draft after resolving the sync state."
        )
        return SEMANTIC_EXIT_ERROR

    validation = SemanticValidator().validate_against_projection(
        report.payload,
        projected,
        contract_output=[field.name for field in schema.contract_output],
    )
    if not validation.ok:
        model_key = _sync_model_key(report.payload)
        print(
            "[semantic.sync] Refusing to promote curated model "
            f"'{model_key}': semantic validation failed for the projected pipeline. "
            "Fix the model or pipeline, then rerun --promote."
        )
        for error in validation.errors:
            print(error)
        return SEMANTIC_EXIT_ERROR

    model_key = _sync_model_key(report.payload)
    draft_path = Path(models_dir) / ".drafts" / f"{model_key}.yaml"
    curated_path = Path(models_dir) / f"{model_key}.yaml"

    # When the pipeline has not drifted, sync short-circuits and hands back the
    # last generated draft — which carries no curation. Promoting it would wipe
    # the curated model, so an already-promoted model with no drift is a no-op.
    if curated_path.exists() and not report.has_changes:
        print(
            f"[semantic.sync] Curated model '{model_key}' is already current — nothing to promote."
        )
        return SEMANTIC_EXIT_OK

    try:
        _assert_no_curation_loss(curated_path, report.payload)
    except ValueError as exc:
        print(f"[semantic.sync] Refusing to promote curated model '{model_key}': {exc}")
        return SEMANTIC_EXIT_ERROR

    try:
        write_yaml_atomic(draft_path, report.payload)
        write_yaml_atomic(curated_path, report.payload)
        SemanticEngine.update_catalog(models_dir, build_catalog_entry(report.payload))
    except Exception as exc:
        print(f"[semantic.sync] Failed to promote pipeline '{pipeline_path}': {exc}")
        return SEMANTIC_EXIT_ERROR

    _print_sync_report(pipeline_path, report)
    print(
        f"[semantic.sync] Promoted managed draft '{model_key}' to curated model "
        f"'{curated_path.name}' and updated semantic_catalog.yaml."
    )
    return SEMANTIC_EXIT_OK


def run_semantic_validate(pipeline_path: str, model_path: str) -> int:
    """Return the CI contract exit code for semantic model validation."""
    try:
        schema = _load_pipeline_ir(pipeline_path)
        payload = _load_yaml_file(model_path)
        from skifer.semantic.output_projection import OutputProjector
        from skifer.semantic.validator import SemanticValidator

        projected = OutputProjector().project(schema)
        result = SemanticValidator().validate_against_projection(
            payload,
            projected,
            contract_output=[field.name for field in schema.contract_output],
        )
    except Exception as exc:
        print(
            f"[semantic.validate] Failed to validate model '{model_path}' against pipeline "
            f"'{pipeline_path}': {exc}"
        )
        return SEMANTIC_EXIT_ERROR

    if not result.ok:
        print(
            f"[semantic.validate] Validation failed for model '{model_path}' against pipeline "
            f"'{pipeline_path}'."
        )
        for error in result.errors:
            print(error)
        return SEMANTIC_EXIT_ERROR

    print(
        f"[semantic.validate] Model '{model_path}' is valid against pipeline '{pipeline_path}'."
    )
    return SEMANTIC_EXIT_OK


def _semantic_sync_mode(args: argparse.Namespace) -> str:
    if args.check:
        return "check"
    if args.write_draft:
        return "write-draft"
    if args.promote:
        return "promote"
    raise ValueError("semantic sync mode is required")


def _load_pipeline_ir(path: str):
    from skifer.core.ir import parse_to_ir
    from skifer.core.schema_loader import parse_schema

    yaml_text = _read_text_file(path)
    return parse_to_ir(parse_schema(yaml_text, params=_sentinel_params(yaml_text)))


def _load_yaml_file(path: str) -> dict:
    import yaml

    return yaml.safe_load(_read_text_file(path)) or {}


def _read_text_file(path: str) -> str:
    with open(path, encoding="utf-8") as fh:
        return fh.read()


def _sentinel_params(yaml_text: str) -> dict[str, str]:
    return {key: f"__sentinel_{key}__" for key in _PLACEHOLDER_RE.findall(yaml_text)}


def _semantic_models_dir() -> str:
    return os.path.abspath("semantic_models")


def _print_sync_report(pipeline_path: str, report) -> None:
    model_key = _sync_model_key(report.payload)
    summary = (
        f"[semantic.sync] Pipeline '{pipeline_path}' → model '{model_key}': "
        f"{len(report.changes)} change(s), {len(report.conflicts)} conflict(s), "
        f"{len(report.suggestions)} suggestion(s)."
    )
    print(summary)
    for change in report.changes:
        print(f"CHANGE  {change.kind}  {change.target}")
    for conflict in report.conflicts:
        print(f"CONFLICT  {conflict.kind}  {conflict.message}")
    for suggestion in report.suggestions:
        print(f"SUGGEST  {suggestion.kind}  {suggestion.before} -> {suggestion.after}")


def _sync_model_key(payload: dict | None) -> str:
    if payload and payload.get("models"):
        return payload["models"][0].get("key", "<unknown>")
    return "<unknown>"


def _run_hub(args: argparse.Namespace) -> None:
    """Boucle REPL principale du SkiferHub."""
    try:
        from rich.console import Console
    except ImportError:
        print("[ERROR] Le module 'rich' est requis pour le hub. Installez-le avec : pip install rich")
        sys.exit(1)

    console = Console()

    # ------------------------------------------------------------------
    # Chargement config
    # ------------------------------------------------------------------
    try:
        from .core.config import ConfigurationManager
        config_mgr = ConfigurationManager(config_path=args.config)
    except FileNotFoundError:
        console.print(
            f"[red]Erreur : fichier de configuration introuvable → {args.config}[/red]\n"
            "Créez un fichier config.yaml dans le répertoire courant ou utilisez --config."
        )
        sys.exit(1)
    except Exception as exc:
        console.print(f"[red]Erreur lors du chargement de la configuration : {exc}[/red]")
        sys.exit(1)

    catalog = config_mgr.db_prefix or "local"
    env = config_mgr.current_env_name or "DEV"
    scope_id = f"{catalog}_{env}"

    # ------------------------------------------------------------------
    # Session history + profil utilisateur
    # ------------------------------------------------------------------
    from .agentic.history import SessionHistory
    from .agentic.user_profile import UserProfile

    profile = UserProfile.load()

    if args.new_session:
        session = SessionHistory(session_title="SkiferHub", ttl_days=15)
    else:
        SessionHistory.purge_expired(scope_id)
        session = SessionHistory.load_or_create(scope_id, ttl_days=15)

    # ------------------------------------------------------------------
    # Choix de session (si session existante non vide)
    # ------------------------------------------------------------------
    try:
        from importlib.metadata import version as pkg_version
        ver = pkg_version("skifer")
    except Exception:
        ver = "?"

    header = (
        f"SkiferHub v{ver} — catalog: {catalog} ({env.upper()})\n"
        "─" * 45
    )
    console.print(f"\n[bold cyan]{header}[/bold cyan]")

    if not args.new_session and len(session) > 0:
        ts = session.created_at.strftime("%Y-%m-%d") if hasattr(session.created_at, "strftime") else str(session.created_at)
        console.print("  [1] Nouvelle session  (efface la session précédente)")
        console.print(f"  [2] Reprendre la session du {ts} ({len(session)} messages)\n")
        try:
            choice = input("Choix [1/2] : ").strip()
        except (EOFError, KeyboardInterrupt):
            console.print("\nAu revoir.")
            return
        if choice == "1":
            session = SessionHistory(session_title="SkiferHub", ttl_days=15)

    # ------------------------------------------------------------------
    # Construction du hub (sans agents Spark/LLM — ils sont optionnels)
    # ------------------------------------------------------------------
    from .agentic.hub import AgenticHub

    hub = AgenticHub(
        session_title="SkiferHub",
        profile=profile,
    )
    hub._history = session

    # ------------------------------------------------------------------
    # REPL
    # ------------------------------------------------------------------
    console.print("\nTapez votre question ou une commande spéciale :")
    console.print("  [dim]exit / quit — quitter[/dim]")
    console.print("  [dim]history     — afficher les entrées récentes[/dim]")
    console.print("  [dim]export pdf  — exporter la session en PDF[/dim]\n")

    while True:
        try:
            question = input("[skifer] > ").strip()
        except (EOFError, KeyboardInterrupt):
            console.print("\nAu revoir.")
            break

        if not question:
            continue

        q_lower = question.lower()

        # Commandes spéciales
        if q_lower in ("exit", "quit"):
            console.print("Au revoir.")
            break

        if q_lower == "help":
            console.print("Commandes : exit, quit, history, export pdf")
            continue

        if q_lower == "history":
            entries = session.entries()
            if not entries:
                console.print("[dim]Aucune entrée dans l'historique.[/dim]")
            else:
                for e in entries[-5:]:
                    console.print(f"  [dim]{e.timestamp}[/dim] {e.question}")
            continue

        if q_lower == "export pdf":
            pdf_path = f"skifer_session_{scope_id}.pdf"
            try:
                session.to_pdf(pdf_path)
                console.print(f"[green]PDF exporté : {pdf_path}[/green]")
            except Exception as exc:
                console.print(f"[red]Erreur export PDF : {exc}[/red]")
            continue

        # Requête normale → hub
        try:
            response = hub.ask(question)
        except Exception as exc:
            console.print(f"[red]Erreur inattendue : {exc}[/red]")
            session.save(scope_id)
            continue

        _render_response(console, response)
        session.save(scope_id)


def _render_response(console: object, response: object) -> None:
    """Render a HubResponse in the terminal with Rich formatting."""
    from .agentic.models import (
        AgentResponse,
        BuilderResponse,
        DictionaryResponse,
        LineageResponse,
        QualityResponse,
    )
    from .serving._response_serializer import hub_response_to_text

    if isinstance(response, AgentResponse):
        if response.mode == "error":
            console.print(f"[red]{response.error}[/red]")
        elif response.mode in ("ambiguous", "needs_clarification"):
            console.print(f"[yellow]{response.clarification_question or response.explanation}[/yellow]")
        else:
            _render_agent_response_rich(console, response)

    elif isinstance(response, (LineageResponse, QualityResponse, DictionaryResponse)):
        text = hub_response_to_text(response)
        style = "red" if getattr(response, "error", None) else ""
        console.print(f"[{style}]{text}[/{style}]" if style else text)

    elif isinstance(response, BuilderResponse):
        if not response.success:
            console.print(f"[red]{response.error}[/red]")
        else:
            console.print(f"[green]YAML généré :[/green]\n{response.yaml_content}")
            if response.output_path:
                console.print(f"[dim]Sauvegardé : {response.output_path}[/dim]")

    else:
        console.print(str(response))


def _render_agent_response_rich(console: object, response: object) -> None:
    """Render an AgentResponse with Rich formatting (tables, colours)."""
    try:
        from rich.table import Table as RichTable
        from .agentic.models import ResponseFormat

        result = getattr(response, "result", None)
        if result is None:
            console.print(getattr(response, "explanation", "Réponse reçue."))
            return

        fmt = getattr(result, "format", None)
        title = getattr(result, "title", "")

        if fmt == ResponseFormat.KPI:
            console.print(f"[bold]{title or getattr(response, 'explanation', '')}[/bold]")
            console.print(f"  {result.kpi_label or ''}: [green]{result.kpi_value}[/green]")

        elif fmt == ResponseFormat.TABLE:
            data_df = getattr(result, "data", None)
            if data_df is not None:
                try:
                    rows = data_df.limit(20).collect()
                    cols = data_df.columns
                    rt = RichTable(title=title or "Résultat")
                    for c in cols:
                        rt.add_column(c)
                    for row in rows:
                        rt.add_row(*[str(v) for v in row])
                    console.print(rt)
                except Exception:
                    console.print(f"[dim]{title}[/dim]")
            else:
                console.print(f"[dim]{title or 'Aucune donnée.'}[/dim]")

        else:
            text = getattr(result, "text_summary", None) or getattr(response, "explanation", "")
            console.print(text or "Réponse reçue.")

    except Exception:
        console.print(str(response))
