"""
CLI Skifer — commande `skifer hub`.

Usage :
    skifer hub                           # config.yaml du répertoire courant
    skifer hub --config path/config.yaml
    skifer hub --new-session             # force une nouvelle session
"""

from __future__ import annotations

import argparse
import contextlib
from contextlib import redirect_stdout
from dataclasses import asdict
from io import StringIO
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

ANALYZE_EXIT_OK = 0
ANALYZE_EXIT_ERROR = 1

INDEX_EXIT_OK = 0
INDEX_EXIT_ERROR = 1
INDEX_EXIT_USAGE = 2
INDEX_EXIT_CLASSIFICATION = 3

KNOWLEDGE_EXIT_OK = 0
KNOWLEDGE_EXIT_ERROR = 1
KNOWLEDGE_EXIT_USAGE = 2

META_EXIT_OK = 0
META_EXIT_ERROR = 1
META_EXIT_USAGE = 2
META_EXIT_NOT_FOUND = 3

AUDIT_EXIT_OK = 0
AUDIT_EXIT_ERROR = 1
AUDIT_EXIT_BELOW_THRESHOLD = 2

CANDIDATES_EXIT_OK = 0
CANDIDATES_EXIT_ERROR = 1
CANDIDATES_EXIT_NEEDS_REVIEW = 2
# Arbitration (Plan 44, S06): its own contract, distinct from the validate job.
CANDIDATES_EXIT_USAGE = 2
CANDIDATES_EXIT_STALE = 3
CANDIDATES_EXIT_CONFLICT = 4
CANDIDATES_DECISIONS = ("approve", "reject", "promote", "review", "demote")
CANDIDATES_ARBITRATION = ("list", "show", "history", *CANDIDATES_DECISIONS)
DRIFT_DEFAULT_DAYS = 7

API_LOOPBACK_HOST = "127.0.0.1"

_PLACEHOLDER_RE = re.compile(r"\{\{\s*(\w+)\s*\}\}")


def main() -> None:
    """Point d'entrée principal de la CLI skifer."""
    # `skifer analyze parity ...` is intercepted before argparse ever sees it:
    # `analyze`'s own `paths` positional is `nargs="+"` and greedy, so nesting
    # a subparser under it would swallow the first legacy-SQL path as a
    # command name instead (breaking every existing flat `analyze` call).
    # A dedicated parser for the rest of argv keeps the flat parser untouched.
    if len(sys.argv) >= 3 and sys.argv[1] == "analyze" and sys.argv[2] == "parity":
        _run_analyze_parity(sys.argv[3:])
        return
    if len(sys.argv) >= 3 and sys.argv[1] == "analyze" and sys.argv[2] == "history":
        _run_analyze_history(sys.argv[3:])
        return

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

    analyze_parser = subparsers.add_parser(
        "analyze",
        help="Analyze existing SQL scripts towards a Skifer pipeline draft (no Spark; Plan 43).",
    )
    analyze_parser.add_argument(
        "paths",
        nargs="+",
        metavar="PATHS",
        help="SQL file path(s) or glob patterns (e.g. legacy_sql/*.sql).",
    )
    analyze_parser.add_argument(
        "--inventory",
        action="store_true",
        help="Print a raw AST node-type inventory (joins, functions, windows, aggregates, subqueries).",
    )
    analyze_parser.add_argument(
        "--dialect",
        default="databricks",
        metavar="DIALECT",
        help="sqlglot SQL dialect to parse with (default: databricks).",
    )
    analyze_parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Map each statement to a Skifer schema draft, round-trip-verify it "
            "against the source, and print a report — writes nothing to disk."
        ),
    )
    analyze_parser.add_argument(
        "--format",
        choices=("md", "json"),
        default="md",
        metavar="FORMAT",
        help="Report format for --dry-run: md (default) or json.",
    )
    analyze_parser.add_argument(
        "--split",
        action="store_true",
        help=(
            "With --dry-run, also classify script-level CTEs/subqueries into "
            "partials, proposed intermediate tables or inlined fragments, and "
            "append a Mermaid dependency graph (Plan 43 — 43.3). Writes nothing "
            "to disk; a cycle among materialized tables is reported, not raised."
        ),
    )
    analyze_parser.add_argument(
        "--emit",
        action="store_true",
        help=(
            "Find recurring expressions across the corpus (Plan 43 — 43.4) and "
            "write validated pipeline YAML drafts + shared-rule Python stubs "
            "into --out. A file without the project_analyzer marker is never "
            "overwritten; a file with the marker and an unchanged source hash "
            "is left untouched."
        ),
    )
    analyze_parser.add_argument(
        "--out",
        default=".",
        metavar="DIR",
        help=(
            "Project root for --emit (default: current directory). Drafts go "
            "to <DIR>/schemas/.drafts/..., rule stubs to <DIR>/rules/_generated/..."
        ),
    )
    analyze_parser.add_argument(
        "--min-occurrences",
        type=int,
        default=3,
        metavar="N",
        help="With --emit, minimum occurrences to promote a shared-rule candidate (default: 3).",
    )
    analyze_parser.add_argument(
        "--min-scripts",
        type=int,
        default=2,
        metavar="N",
        help="With --emit, minimum distinct scripts to promote a shared-rule candidate (default: 2).",
    )
    analyze_parser.add_argument(
        "--strict",
        action="store_true",
        help="With --emit, exit 2 on any residue (skipped unit, refused or invalid artifact).",
    )
    analyze_parser.add_argument(
        "--enrich",
        action="store_true",
        help=(
            "With --emit, also propose rule names/docstrings, draft "
            "descriptions and fingerprint-grouping via an LLM (Plan 43 — "
            "43.6, optional) — added to the printed report only, never "
            "written to disk in this slice. No configured provider: "
            "silent no-op, exit code unaffected."
        ),
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
    index_parser.add_argument(
        "--strict",
        action="store_true",
        help="Reject undeclared inherited classification before indexing.",
    )
    index_parser.add_argument(
        "--rules",
        action="append",
        default=[],
        metavar="MODULE",
        help=(
            "Module whose import registers business rules (e.g. rules.orders). "
            "Repeat for several. Without it a rule is unresolvable, so the "
            "columns it produces carry no lineage and inherit no classification."
        ),
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

    semantic_metric_views_parser = semantic_subparsers.add_parser(
        "metric-views",
        help="Print the Unity Catalog metric view DDL each semantic model translates to (Plan 42).",
    )
    semantic_metric_views_parser.add_argument(
        "--catalog",
        required=True,
        help="Unity Catalog catalog the metric views are published into.",
    )
    semantic_metric_views_parser.add_argument(
        "--schema",
        default="semantic",
        help="Target schema for the metric views (default: semantic).",
    )
    semantic_metric_views_parser.add_argument(
        "--models-dir",
        default=None,
        help="Semantic models directory (default: ./semantic_models).",
    )
    semantic_metric_views_parser.add_argument(
        "--model",
        default=None,
        metavar="KEY",
        help="Translate only this model key, instead of the whole catalog.",
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

    api_parser = subparsers.add_parser(
        "api",
        help="Run the optional local HTTP API.",
    )
    api_subparsers = api_parser.add_subparsers(dest="api_command")
    api_serve_parser = api_subparsers.add_parser(
        "serve",
        help="Serve services/ over a loopback HTTP API.",
    )
    api_serve_parser.add_argument(
        "--project",
        required=True,
        metavar="DIR",
        help="Skifer project directory.",
    )
    api_serve_parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Loopback port (default: 8000).",
    )
    api_openapi_parser = api_subparsers.add_parser(
        "openapi",
        help="Print the OpenAPI schema (JSON) to stdout.",
    )
    api_openapi_parser.add_argument(
        "--project",
        default=".",
        metavar="DIR",
        help="Project directory (default: cwd).",
    )

    knowledge_parser = subparsers.add_parser(
        "knowledge",
        help="Sync and search the knowledge graph (Plan 44): BM25, no LLM.",
    )
    knowledge_subparsers = knowledge_parser.add_subparsers(dest="knowledge_command")
    knowledge_sync_parser = knowledge_subparsers.add_parser(
        "sync", help="Rebuild the graph from pipelines, semantic models, rules and UC."
    )
    knowledge_sync_parser.add_argument(
        "--schema", action="append", default=None, metavar="CAT.SCHEMA",
        help="Unity Catalog schema to sync (repeatable); opens a Spark session.",
    )
    knowledge_sync_parser.add_argument("--dry-run", action="store_true",
                                       help="Report what would change; write nothing.")
    knowledge_sync_parser.add_argument("--json", action="store_true", help="Print the report as JSON.")
    knowledge_sync_parser.add_argument("--db", default=".skifer_knowledge.db",
                                       help="SQLite knowledge store (local mode).")
    knowledge_sync_parser.add_argument("--metadata-db", default=".skifer_metadata.db",
                                       help="Pipeline metadata registry written by 'skifer index'.")
    knowledge_sync_parser.add_argument("--models-dir", default="semantic_models",
                                       help="Directory holding semantic_catalog.yaml.")
    knowledge_sync_parser.add_argument("--rules", action="append", default=None, metavar="MODULE",
                                       help="Rule module to import so its rules are indexed (repeatable).")
    knowledge_sync_parser.add_argument("--config", default=None, metavar="PATH",
                                       help="Path to config.yaml, used with --schema.")
    knowledge_sync_parser.add_argument(
        "--embed", default=None, metavar="MODEL",
        help="Also embed new or changed nodes (multilingual-e5-small | bge-m3); "
             "needs the [knowledge-embeddings] extra.")
    knowledge_sync_parser.add_argument("--model-cache", default=None, metavar="PATH",
                                       help="Local model cache; nothing is ever downloaded.")
    knowledge_search_parser = knowledge_subparsers.add_parser(
        "search", help="Find the tables, columns, metrics and rules relevant to a question."
    )
    knowledge_search_parser.add_argument("question", metavar="QUESTION")
    knowledge_search_parser.add_argument("--kind", action="append", default=None,
                                         help="Node kind to keep (repeatable).")
    knowledge_search_parser.add_argument("--domain", default=None, metavar="CAT.SCHEMA")
    knowledge_search_parser.add_argument("-k", type=int, default=10, dest="k", metavar="N")
    knowledge_search_parser.add_argument("--json", action="store_true", help="Print hits as JSON.")
    knowledge_search_parser.add_argument("--db", default=".skifer_knowledge.db",
                                         help="SQLite knowledge store.")

    candidates_parser = subparsers.add_parser(
        "candidates",
        help="Validate and promote metric candidates (Plan 44).",
    )
    candidates_subparsers = candidates_parser.add_subparsers(dest="candidates_command")
    candidates_validate_parser = candidates_subparsers.add_parser(
        "validate",
        help="Judge candidates (coherence, frequency, stability) and promote them to level 1.",
    )
    candidates_validate_parser.add_argument(
        "--candidate", action="append", default=None, metavar="ID",
        help="Candidate to validate (repeatable); default: every pending one.",
    )
    candidates_validate_parser.add_argument(
        "--dry-run", action="store_true",
        help="Judge and print; no stability query, no DDL, no transition.",
    )
    candidates_validate_parser.add_argument("--json", action="store_true",
                                            help="Print the results as JSON.")
    candidates_validate_parser.add_argument("--config", default=None, metavar="PATH",
                                            help="Path to config.yaml.")
    candidates_validate_parser.add_argument("--models-dir", default="semantic_models",
                                            help="Directory holding semantic_catalog.yaml.")

    candidates_list_parser = candidates_subparsers.add_parser(
        "list", help="List candidates, optionally in one state.")
    candidates_list_parser.add_argument("--state", default=None, help="Only this state.")
    candidates_list_parser.add_argument("--json", action="store_true", help="Print JSON.")
    for name, help_text in (("show", "Show one candidate."),
                            ("history", "Show a candidate's certification events.")):
        read_parser = candidates_subparsers.add_parser(name, help=help_text)
        read_parser.add_argument("candidate_id", metavar="ID")
        read_parser.add_argument("--json", action="store_true", help="Print JSON.")
    for name, help_text in (
        ("approve", "Publish at level 1 by human decision."),
        ("reject", "Reject a candidate."),
        ("promote", "Write the level 2 pipeline and semantic drafts for a PR."),
        ("review", "Put a promoted candidate under review."),
        ("demote", "Drop a candidate's level 1 views."),
    ):
        decision_parser = candidates_subparsers.add_parser(name, help=help_text)
        decision_parser.add_argument("candidate_id", metavar="ID")
        decision_parser.add_argument("--reason", required=True, help="Why — recorded.")
        if name == "promote":
            decision_parser.add_argument("--pipeline-output", default=None, metavar="PATH",
                                         help="Pipeline draft path, under a '.drafts' directory.")
        if name == "review":
            decision_parser.add_argument("--signal", default=None, metavar="NAME",
                                         help="The signal that prompted the review.")
        if name == "demote":
            decision_parser.add_argument("--replaced-by", default=None, metavar="ID",
                                         help="Candidate id or model:<model>.<metric>.")
    candidates_drift_parser = candidates_subparsers.add_parser(
        "drift", help="Usage drift signals over a window; observation only, changes nothing.")
    candidates_drift_parser.add_argument("--since", default=None, metavar="ISO",
                                         help="Window start, inclusive (default: 7 days ago).")
    candidates_drift_parser.add_argument("--until", default=None, metavar="ISO",
                                         help="Window end, exclusive (default: now).")
    candidates_drift_parser.add_argument("--json", action="store_true", help="Print JSON.")
    candidates_drift_parser.add_argument("--environment", default=None, metavar="NAME",
                                         help="Usage partition (default: the engine's).")
    candidates_drift_parser.add_argument("--usage-store", default=".skifer_adaptive.db",
                                         metavar="PATH", help="SQLite adaptive usage store.")
    for arbitration_parser in candidates_subparsers.choices.values():
        if arbitration_parser is not candidates_validate_parser:
            arbitration_parser.add_argument("--config", default=None, metavar="PATH",
                                            help="Path to config.yaml.")
            arbitration_parser.add_argument("--models-dir", default="semantic_models",
                                            help="Directory holding semantic_catalog.yaml.")

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
    elif args.command == "analyze":
        _run_analyze(args)
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
    elif args.command == "api":
        _run_api(args)
    elif args.command == "adaptive":
        _run_adaptive(args)
    elif args.command == "knowledge":
        _run_knowledge(args)
    elif args.command == "candidates":
        sys.exit(run_candidates_command(args))
    elif args.command == "incidents":
        _run_incidents(args)
    elif args.command == "contract":
        _run_contract(args)
    else:
        parser.print_help()
        sys.exit(1)


def run_candidates_command(
    args: argparse.Namespace, *, store=None, semantic=None, knowledge_store=None, clock=None
) -> int:
    """``skifer candidates validate``: ``0`` ok, ``1`` technical error, ``2`` needs review.

    ``2`` wins over ``1``: it is the signal a job notifies on, and it means that
    at least one candidate *went* to ``needs_review`` during this run — a dry
    run, which writes no transition, never returns it.
    """
    from datetime import datetime, timezone

    from skifer.candidates.promotion import apply_report
    from skifer.candidates.store import default_candidate_store
    from skifer.candidates.validator import CandidateValidator, ValidationThresholds
    from skifer.core.config import candidate_validation_thresholds
    from skifer.knowledge.index import KnowledgeIndex
    from skifer.knowledge.store import default_knowledge_store

    if getattr(args, "candidates_command", None) in CANDIDATES_ARBITRATION:
        return run_candidates_arbitration(args)
    if getattr(args, "candidates_command", None) == "drift":
        return run_candidates_drift(args)
    if getattr(args, "candidates_command", None) != "validate":
        print("Candidates command missing. Use 'skifer candidates --help'.", file=sys.stderr)
        return CANDIDATES_EXIT_ERROR
    clock = clock or (lambda: datetime.now(timezone.utc))
    try:
        if semantic is None:
            from skifer.core.core import SkiferEngine
            from skifer.observability.certification_store import default_certification_store
            from skifer.semantic.semantic import SemanticEngine

            engine = SkiferEngine(config_path=args.config)
            semantic = SemanticEngine(engine, models_dir=args.models_dir,
                                      certification_store=default_certification_store(engine))
        core = semantic.core
        env_config = core._context.env_config()
        thresholds = ValidationThresholds(**candidate_validation_thresholds(env_config))
        store = store if store is not None else default_candidate_store(core)
        knowledge_store = (knowledge_store if knowledge_store is not None
                           else default_knowledge_store(core))
        validator = CandidateValidator(store, semantic, KnowledgeIndex.from_store(knowledge_store),
                                       thresholds=thresholds, clock=clock)
        catalog = getattr(core, "db", None)
        with contextlib.redirect_stdout(sys.stderr):  # keep --json parseable
            schema = semantic._resolve_metric_views_schema()
        backend = core._get_backend()
        ids = args.candidate or (
            [c.candidate_id for c in store.list_candidates(state="candidate")]
            + [c.candidate_id for c in store.list_candidates(state="validated")
               if c.published_view is None]
        )
    except Exception as exc:
        print(f"[candidates] setup failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return CANDIDATES_EXIT_ERROR

    rows, failed, reviewed = [], False, False
    for candidate_id in ids:
        try:
            report = validator.validate(candidate_id, execute=not args.dry_run)
            result = apply_report(report, store=store, backend=backend, catalog=catalog,
                                  schema=schema, clock=clock, knowledge_store=knowledge_store,
                                  dry_run=args.dry_run)
        except Exception as exc:
            failed = True
            rows.append({"candidate_id": candidate_id, "error_type": type(exc).__name__})
            print(f"[candidates] {candidate_id}: {type(exc).__name__}: {exc}", file=sys.stderr)
            continue
        failed = failed or result.error_type is not None
        reviewed = reviewed or (not args.dry_run and result.new_state == "needs_review"
                                and result.previous_state != "needs_review")
        rows.append({
            "candidate_id": candidate_id, "outcome": report.outcome,
            "previous_state": result.previous_state, "new_state": result.new_state,
            "statements": list(result.statements), "notes": list(result.notes),
            "error_type": result.error_type, "report": report.to_dict(),
        })
    if args.json:
        print(json.dumps({"dry_run": args.dry_run, "candidates": rows}, ensure_ascii=False,
                         sort_keys=True, default=str))
    else:
        prefix = "[candidates] (dry run) " if args.dry_run else "[candidates] "
        for row in rows:
            if "outcome" not in row:
                continue
            print(f"{prefix}{row['candidate_id']}: {row['outcome']} "
                  f"{row['previous_state']} -> {row['new_state']}"
                  + (f" (error: {row['error_type']})" if row["error_type"] else ""))
            for note in row["notes"]:
                print(f"{prefix}  {note}")
        if not rows:
            print(f"{prefix}nothing to validate")
    if reviewed:
        return CANDIDATES_EXIT_NEEDS_REVIEW
    return CANDIDATES_EXIT_ERROR if failed else CANDIDATES_EXIT_OK


def run_candidates_arbitration(args: argparse.Namespace, *, service=None, decider=None) -> int:
    """``skifer candidates list|show|history|approve|reject|promote|review|demote``.

    ``0`` success · ``1`` technical error or no traceable decider · ``2`` usage ·
    ``3`` stale candidate · ``4`` state conflict, existing output or refused approval.
    """
    from skifer.candidates.service import (
        ApprovalRefusedError,
        CandidateUsageError,
        DeciderIdentityError,
        OutputExistsError,
        StaleCandidateError,
        StateConflictError,
        build_candidate_service,
    )

    command = args.candidates_command
    try:
        if service is None:
            from skifer.core.core import SkiferEngine
            from skifer.observability.certification_store import default_certification_store
            from skifer.semantic.semantic import SemanticEngine

            with contextlib.redirect_stdout(sys.stderr):  # keep --json parseable
                engine = SkiferEngine(config_path=args.config)
                service = build_candidate_service(SemanticEngine(
                    engine, models_dir=args.models_dir,
                    certification_store=default_certification_store(engine)))
        if command in CANDIDATES_DECISIONS and decider is None:
            from skifer.services.identity import LocalIdentity

            decider = LocalIdentity.resolve().subject
        if command == "list":
            output = [view.to_dict() for view in service.list(state=args.state)]
        elif command == "show":
            output = service.show(args.candidate_id).to_dict()
        elif command == "history":
            output = service.history(args.candidate_id)
        else:
            options = {"decider": decider, "reason": args.reason}
            if command == "promote":
                options["pipeline_output"] = args.pipeline_output
            elif command == "review":
                options["signal"] = args.signal
            elif command == "demote":
                options["replaced_by"] = args.replaced_by
            with contextlib.redirect_stdout(sys.stderr):  # DDL chatter stays off stdout
                result = getattr(service, command)(args.candidate_id, **options)
            output = result if isinstance(result, dict) else result.to_dict()
    except StaleCandidateError as exc:
        print(f"[candidates] {exc}", file=sys.stderr)
        return CANDIDATES_EXIT_STALE
    except (StateConflictError, OutputExistsError, ApprovalRefusedError) as exc:
        print(f"[candidates] {type(exc).__name__}: {exc}", file=sys.stderr)
        return CANDIDATES_EXIT_CONFLICT
    except CandidateUsageError as exc:
        print(f"[candidates] {exc}", file=sys.stderr)
        return CANDIDATES_EXIT_USAGE
    except DeciderIdentityError as exc:
        print(f"[candidates] {exc}", file=sys.stderr)
        return CANDIDATES_EXIT_ERROR
    except Exception as exc:
        print(f"[candidates] {command} failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return CANDIDATES_EXIT_ERROR
    if getattr(args, "json", False) or command in CANDIDATES_DECISIONS:
        print(json.dumps(output, ensure_ascii=False, sort_keys=True, default=str))
    elif command == "list":
        for row in output:
            print(f"{row['candidate_id']}  {row['state']:<13} {row['metric_name']}")
        if not output:
            print("[candidates] no candidate")
    elif command == "show":
        for key, value in output.items():
            print(f"{key}: {value}")
    else:
        for event in output:
            print(f"{event['event_at']}  {event['event_type']}  {event['previous_state']} -> "
                  f"{event['new_state']}  {event['decided_by']}")
    return CANDIDATES_EXIT_OK


class _NoUsageEvents:
    """A usage store that does not exist: no evidence, and no empty file created for it."""

    def list_events(self, **_filters):
        return []

    def close(self) -> None:
        pass


def _catalog_metric_ids(models_dir: str) -> list[str]:
    """``model_key.metric`` for every metric of ``semantic_catalog.yaml``."""
    import yaml

    path = Path(models_dir) / "semantic_catalog.yaml"
    if not path.is_file():
        return []
    catalog = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return sorted(f"{model['key']}.{metric}" for model in catalog.get("models") or []
                  for metric in model.get("metrics") or [])


def _drift_bound(value: str | None, default, name: str):
    from datetime import datetime

    if value is None:
        return default
    try:
        moment = datetime.fromisoformat(value)
    except ValueError:
        raise ValueError(f"--{name} must be an ISO datetime; got {value!r}.") from None
    if moment.tzinfo is None:
        raise ValueError(f"--{name} must include a timezone, e.g. {value}+00:00.")
    return moment


def run_candidates_drift(
    args: argparse.Namespace, *, candidate_store=None, usage_store=None, index=None,
    catalog_models=None, environment=None, env_config=None, clock=None,
) -> int:
    """``skifer candidates drift``: ``0`` even with high signals, ``1`` technical, ``2`` usage."""
    from datetime import datetime, timedelta, timezone

    from skifer.candidates.drift import compute_drift
    from skifer.core.config import candidate_drift_settings

    now = (clock or (lambda: datetime.now(timezone.utc)))()
    try:
        until = _drift_bound(args.until, now, "until")
        since = _drift_bound(args.since, until - timedelta(days=DRIFT_DEFAULT_DAYS), "since")
        if since >= until:
            raise ValueError("--since must be before --until")
    except ValueError as exc:
        print(f"[candidates] {exc}", file=sys.stderr)
        return CANDIDATES_EXIT_USAGE
    opened = []
    try:
        if candidate_store is None or index is None or catalog_models is None or env_config is None:
            from skifer.candidates.store import default_candidate_store
            from skifer.core.core import SkiferEngine
            from skifer.knowledge.index import KnowledgeIndex
            from skifer.knowledge.store import default_knowledge_store
            from skifer.semantic.semantic import SemanticEngine

            with contextlib.redirect_stdout(sys.stderr):  # keep --json parseable
                core = SkiferEngine(config_path=args.config)
                env_config = env_config if env_config is not None else core._context.env_config()
                environment = environment or core.env
                if candidate_store is None:
                    candidate_store = default_candidate_store(core)
                    opened.append(candidate_store)
                if index is None:
                    knowledge = default_knowledge_store(core)
                    opened.append(knowledge)
                    index = KnowledgeIndex.from_store(knowledge)
                if catalog_models is None:
                    catalog_models = _catalog_metric_ids(
                        SemanticEngine(core, models_dir=args.models_dir).models_dir)
        environment = args.environment or environment
        if usage_store is None:
            from skifer.adaptive.store import SqliteUsageEventStore

            usage_store = (SqliteUsageEventStore(args.usage_store)
                           if Path(args.usage_store).is_file() else _NoUsageEvents())
            opened.append(usage_store)
        min_sample, _thresholds = candidate_drift_settings(env_config)  # validated, not applied
        report = compute_drift(
            candidate_store=candidate_store, usage_store=usage_store,
            catalog_models=catalog_models, index=index, environment=environment,
            since=since, until=until, min_sample=min_sample,
        )
    except Exception as exc:
        print(f"[candidates] drift failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return CANDIDATES_EXIT_ERROR
    finally:
        for resource in opened:
            with contextlib.suppress(Exception):
                resource.close()
    if args.json:
        print(json.dumps(report.to_dict(), ensure_ascii=False, sort_keys=True))
    else:
        print(f"[candidates] drift {report.environment} [{since.isoformat()}, "
              f"{until.isoformat()}) — thresholds not applied")
        for signal in report.signals:
            shown = (f"{signal.value:.3f}" if signal.status == "measured"
                     else f"inconclusive ({signal.reason})")
            print(f"  {signal.name}: {shown}, sample {signal.sample_size}")
            for detail in signal.details:
                print(f"    {json.dumps(detail, sort_keys=True)}")
    return CANDIDATES_EXIT_OK


def _run_knowledge(args: argparse.Namespace) -> None:
    """Run knowledge graph sync/search with stable exit codes (Plan 44, S02)."""
    sys.exit(run_knowledge_command(args))


def run_knowledge_command(args: argparse.Namespace, *, store=None, engine=None) -> int:
    """``skifer knowledge sync|search``: ``0`` success, ``1`` technical error, ``2`` usage."""
    from skifer.knowledge.sync import validate_domain

    command = getattr(args, "knowledge_command", None)
    if command not in ("sync", "search"):
        print("Knowledge command missing. Use 'skifer knowledge --help'.", file=sys.stderr)
        return KNOWLEDGE_EXIT_USAGE
    if command == "search":
        return _knowledge_search(args, store=store)
    try:
        domains = [validate_domain(value) for value in args.schema] if args.schema else None
    except ValueError:
        print("[knowledge] --schema expects CATALOG.SCHEMA", file=sys.stderr)
        return KNOWLEDGE_EXIT_USAGE
    try:
        import_rule_modules(getattr(args, "rules", None) or [])
    except ValueError as exc:
        print(f"[knowledge] {exc}", file=sys.stderr)
        return KNOWLEDGE_EXIT_USAGE
    return _knowledge_sync(args, domains, store=store, engine=engine)


def _knowledge_sync(args, domains, *, store=None, engine=None) -> int:
    from skifer.knowledge.store import SqliteKnowledgeStore, default_knowledge_store
    from skifer.knowledge.sync import KnowledgeSynchronizer
    from skifer.observability.metadata_store import SqliteMetadataStore
    from skifer.knowledge.embeddings import SUPPORTED_MODELS

    if getattr(args, "embed", None) and args.embed not in SUPPORTED_MODELS:
        print(f"[knowledge] --embed expects one of {sorted(SUPPORTED_MODELS)}", file=sys.stderr)
        return KNOWLEDGE_EXIT_USAGE

    try:
        if engine is None and domains:
            from skifer.core.core import SkiferEngine

            engine = SkiferEngine(config_path=args.config)
        if store is None:
            store = default_knowledge_store(engine) if engine is not None else SqliteKnowledgeStore(args.db)
        metadata = SqliteMetadataStore(args.metadata_db) if os.path.exists(args.metadata_db) else None
        embedder = None
        if getattr(args, "embed", None):
            from skifer.knowledge import embeddings

            embedder = embeddings.load_embedder(args.embed, cache_dir=args.model_cache)
        report = KnowledgeSynchronizer(
            store, engine=engine, metadata_store=metadata, domains=domains,
            models_dir=os.path.abspath(args.models_dir),
        ).sync(dry_run=args.dry_run)
        embedded = None
        if embedder is not None:
            embedded = (len(store.nodes_to_embed(embedder.model_name)) if args.dry_run
                        else embeddings.embed_nodes(store, embedder))
    except Exception as exc:
        print(f"[knowledge] sync failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return KNOWLEDGE_EXIT_ERROR
    if args.json and embedded is not None:
        print(json.dumps({**report.to_dict(), "embedded": embedded, "embedding_model": args.embed},
                         ensure_ascii=False, sort_keys=True))
        return KNOWLEDGE_EXIT_OK
    if args.json:
        print(json.dumps(report.to_dict(), ensure_ascii=False, sort_keys=True))
    else:
        prefix = "[knowledge] (dry run) " if args.dry_run else "[knowledge] "
        print(f"{prefix}added={report.added} updated={report.updated} "
              f"unchanged={report.unchanged} removed={report.removed}")
        for item in report.unavailable:
            print(f"[knowledge] unavailable: {item}")
        if embedded is not None:
            print(f"{prefix}embedded={embedded} model={args.embed}")
    return KNOWLEDGE_EXIT_OK


def _knowledge_search(args, *, store=None) -> int:
    from skifer.knowledge.index import KnowledgeIndex
    from skifer.knowledge.store import SqliteKnowledgeStore

    if args.k < 1:
        print("[knowledge] -k expects a positive integer", file=sys.stderr)
        return KNOWLEDGE_EXIT_USAGE
    if store is None and not os.path.exists(args.db):
        print("index is empty — run 'skifer knowledge sync'")
        return KNOWLEDGE_EXIT_OK
    try:
        index = KnowledgeIndex.from_store(store or SqliteKnowledgeStore(args.db))
        hits = index.search(args.question, k=args.k, kinds=args.kind, domain=args.domain)
    except ValueError as exc:
        print(f"[knowledge] {exc}", file=sys.stderr)
        return KNOWLEDGE_EXIT_USAGE
    except Exception as exc:
        print(f"[knowledge] search failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return KNOWLEDGE_EXIT_ERROR
    if len(index) == 0:
        print("index is empty — run 'skifer knowledge sync'")
        return KNOWLEDGE_EXIT_OK
    if args.json:
        print(json.dumps([hit.to_dict() for hit in hits], ensure_ascii=False, sort_keys=True))
        return KNOWLEDGE_EXIT_OK
    for hit in hits:
        print(f"{hit.score:.3f}\t{hit.node.kind}\t{hit.node.fqn}\t{hit.node.status}\t"
              f"{','.join(hit.matched_terms)}")
    return KNOWLEDGE_EXIT_OK


def _run_adaptive(args: argparse.Namespace) -> None:
    """Run the human review boundary with stable, category-specific exit codes."""
    sys.exit(run_adaptive_command(args))


def _run_incidents(args: argparse.Namespace) -> None:
    """Run incident management with stable, category-specific exit codes."""
    sys.exit(run_incidents_command(args))


def _run_analyze(args: argparse.Namespace) -> None:
    """Run the project analyzer with stable exit codes (Plan 43)."""
    sys.exit(run_analyze_command(args))


ANALYZE_EXIT_RESIDUE = 2
ANALYZE_EXIT_EXISTING_OUTPUT = 4
ANALYZE_EXIT_PARITY_MISMATCH = 5


def _build_analyze_parity_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="skifer analyze parity",
        description=(
            "Run a pipeline draft into a disposable table and compare it to "
            "a legacy table (Plan 43 — 43.5): proof on data before a cutover."
        ),
    )
    parser.add_argument(
        "draft",
        metavar="DRAFT_YAML",
        help="Path to a pipeline draft YAML (e.g. a schemas/.drafts/... file).",
    )
    parser.add_argument(
        "--against",
        required=True,
        metavar="FQN",
        help="Fully-qualified legacy table to compare the draft's output against.",
    )
    parser.add_argument(
        "--target-layer",
        required=True,
        metavar="LAYER",
        help=(
            "Base target layer the draft is run into (e.g. silver) — only "
            "used to resolve the disposable comparison table's schema; the "
            "draft's own eventual target table name is never written to."
        ),
    )
    parser.add_argument(
        "--key",
        default=None,
        metavar="COL,COL,...",
        help="Comma-separated key column(s) to join draft and legacy rows on; default: every common column.",
    )
    parser.add_argument(
        "--config",
        default=None,
        metavar="PATH",
        help="Path to config.yaml (default: auto-detected from the current directory).",
    )
    return parser


def _run_analyze_parity(argv: list[str]) -> None:
    """Run ``skifer analyze parity`` with stable exit codes (Plan 43 — 43.5)."""
    args = _build_analyze_parity_parser().parse_args(argv)
    sys.exit(run_analyze_parity_command(args))


def run_analyze_parity_command(args: argparse.Namespace, *, engine=None) -> int:
    """``skifer analyze parity``: run *draft* into a disposable table via the
    real engine and compare it to ``--against``. Exit codes: ``0`` identical,
    ``1`` a technical error, ``5`` a measured discrepancy — no exception
    means the comparison itself completed, whatever it found.
    """
    from skifer.analyzer.parity import ParityRunner
    from skifer.core.schema_loader import load_schema

    try:
        if engine is None:
            from skifer.core.core import SkiferEngine

            engine = SkiferEngine(config_path=args.config)
        draft_schema = load_schema(args.draft, params=engine.default_params)
        key = [c.strip() for c in args.key.split(",")] if args.key else None
        report = ParityRunner(engine).run(
            draft_schema, args.target_layer, against=args.against, key=key,
        )
    except Exception as exc:
        print(f"[analyze parity] failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return ANALYZE_EXIT_ERROR

    print(json.dumps(report.to_dict(), indent=2, sort_keys=False))
    return ANALYZE_EXIT_OK if report.identical else ANALYZE_EXIT_PARITY_MISMATCH


def _build_analyze_history_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="skifer analyze history",
        description=(
            "Fetch recent statement text from system.query.history via a "
            "Databricks SQL warehouse and feed it through the same "
            "--inventory/--dry-run/--emit pipeline as a .sql file (Plan 43 "
            "— 43.7's second sub-point). Needs the optional [databricks] "
            "extra. Never reads a query's result data or who ran it."
        ),
    )
    parser.add_argument("--warehouse-id", required=True, metavar="ID", help="SQL warehouse to query against.")
    parser.add_argument("--days", type=int, default=7, metavar="N", help="Look back this many days (default: 7).")
    parser.add_argument(
        "--schema",
        default=None,
        metavar="CAT.SCHEMA",
        help="Case-insensitive substring filter on fetched statement text (best-effort, not exact lineage).",
    )
    parser.add_argument("--profile", default=None, metavar="NAME", help="Databricks CLI config profile to use.")
    parser.add_argument("--dialect", default="spark", help="sqlglot source dialect (default: spark).")
    parser.add_argument("--inventory", action="store_true", help="Print a raw AST node-type inventory.")
    parser.add_argument("--dry-run", dest="dry_run", action="store_true", help="Map to drafts and report; write nothing.")
    parser.add_argument("--split", action="store_true", help="With --dry-run: also report a CTE/subquery split.")
    parser.add_argument("--emit", action="store_true", help="Find recurring patterns and write validated drafts.")
    parser.add_argument("--out", default=".", metavar="DIR", help="With --emit: output directory.")
    parser.add_argument("--format", default="json", choices=["json", "markdown"], help="With --dry-run: output format.")
    parser.add_argument("--min-occurrences", dest="min_occurrences", type=int, default=3, metavar="N")
    parser.add_argument("--min-scripts", dest="min_scripts", type=int, default=2, metavar="N")
    parser.add_argument("--strict", action="store_true", help="With --emit: refuse to write on any residue.")
    parser.add_argument("--enrich", action="store_true", help="With --emit: add constrained LLM naming/descriptions.")
    return parser


def _run_analyze_history(argv: list[str]) -> None:
    """Run ``skifer analyze history`` with stable exit codes (Plan 43 — 43.7)."""
    args = _build_analyze_history_parser().parse_args(argv)
    sys.exit(run_analyze_history_command(args))


def run_analyze_history_command(args: argparse.Namespace, *, workspace_client=None) -> int:
    """``skifer analyze history``: fetch ``system.query.history``, dedupe by
    content fingerprint, and run the same corpus through
    :func:`_run_analyze_modes` as any other front-end's statements.
    """
    inventory = getattr(args, "inventory", False)
    dry_run = getattr(args, "dry_run", False)
    emit = getattr(args, "emit", False)
    if not inventory and not dry_run and not emit:
        print("[analyze history] Nothing to do: pass --inventory, --dry-run or --emit.")
        return ANALYZE_EXIT_OK

    from skifer.analyzer.history import build_workspace_client, fetch_query_history
    from skifer.analyzer.parser import _parse_raw_statements

    try:
        client = workspace_client if workspace_client is not None else build_workspace_client(profile=args.profile)
        fetch_result = fetch_query_history(
            client, warehouse_id=args.warehouse_id, days=args.days, schema=args.schema,
        )
    except ImportError as exc:
        print(f"[analyze history] {exc}", file=sys.stderr)
        return ANALYZE_EXIT_ERROR
    except Exception as exc:
        print(f"[analyze history] fetch failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return ANALYZE_EXIT_ERROR

    print(
        f"[analyze history] fetched {fetch_result.total_fetched}, "
        f"{fetch_result.duplicate_count} duplicate(s), "
        f"{len(fetch_result.raw_statements)} distinct statement(s)"
    )

    try:
        all_statements = _parse_raw_statements(list(fetch_result.raw_statements), dialect=args.dialect)
    except Exception as exc:
        print(f"[analyze history] {type(exc).__name__}: {exc}", file=sys.stderr)
        return ANALYZE_EXIT_ERROR

    return _run_analyze_modes(args, all_statements, dry_run=dry_run, emit=emit)


def _parse_source_file(path: str, *, dialect: str):
    """Parse one source file into statements, dispatching on its extension
    (Plan 43 — 43.7): ``.py``/``.ipynb`` through ``parse_notebook`` (which
    also returns any cell this front-end could not safely read as SQL),
    anything else through ``parse_script``. Returns ``(statements,
    residues)`` — ``residues`` is always empty for a plain ``.sql`` file.
    """
    import os

    from skifer.analyzer.parser import parse_notebook, parse_script

    if os.path.splitext(path)[1] in (".py", ".ipynb"):
        return parse_notebook(path, dialect=dialect)
    return parse_script(path, dialect=dialect), []


def run_analyze_command(args: argparse.Namespace) -> int:
    """Parse one or more SQL files for ``--inventory`` (43.0), ``--dry-run``
    (43.2), or ``--emit`` (43.4).

    ``--inventory`` prints a raw AST node-type count — nothing is mapped to the
    Skifer grammar. ``--dry-run`` maps each statement to a schema draft,
    round-trip-verifies it, and prints a report; it writes nothing to disk.
    ``--emit`` finds recurring expressions and writes validated pipeline
    drafts + rule stubs to ``--out``.
    """
    inventory = getattr(args, "inventory", False)
    dry_run = getattr(args, "dry_run", False)
    emit = getattr(args, "emit", False)
    if not inventory and not dry_run and not emit:
        print("[analyze] Nothing to do: pass --inventory, --dry-run or --emit.")
        return ANALYZE_EXIT_OK

    import glob as glob_mod

    paths: list[str] = []
    for pattern in args.paths:
        expanded = sorted(glob_mod.glob(pattern, recursive=True))
        paths.extend(expanded if expanded else [pattern])

    if not paths:
        print("[analyze] No SQL files found.")
        return ANALYZE_EXIT_OK

    from skifer.analyzer.parser import AnalyzerDependencyError, StatementParseError

    all_statements = []
    had_error = False
    for path in paths:
        try:
            statements, residues = _parse_source_file(path, dialect=args.dialect)
        except AnalyzerDependencyError as exc:
            # Missing the optional dependency is global, not per-file: stop immediately.
            print(f"[analyze] {exc}", file=sys.stderr)
            return ANALYZE_EXIT_ERROR
        except StatementParseError as exc:
            print(f"[analyze] {exc}", file=sys.stderr)
            had_error = True
            continue
        except OSError as exc:
            print(f"[analyze] Cannot read '{path}': {exc}", file=sys.stderr)
            had_error = True
            continue
        for residue in residues:
            print(f"[analyze] {residue.source_path}:{residue.line}: {residue.reason}", file=sys.stderr)
        print(f"[analyze] {path}: {len(statements)} statement(s)")
        all_statements.extend(statements)

    if had_error:
        return ANALYZE_EXIT_ERROR

    return _run_analyze_modes(args, all_statements, dry_run=dry_run, emit=emit)


def _run_analyze_modes(args: argparse.Namespace, all_statements: list, *, dry_run: bool, emit: bool) -> int:
    """Shared ``--inventory``/``--dry-run``/``--emit`` dispatch over an
    already-built statement corpus — used by the flat ``skifer analyze``
    path and, since 43.7's second sub-point, by ``skifer analyze history``
    once it has fetched and deduplicated its own corpus."""
    if dry_run:
        from skifer.analyzer.report import build_report

        report = build_report(all_statements)
        split_report = None
        if getattr(args, "split", False):
            from skifer.analyzer.report import build_split_report

            split_report = build_split_report(all_statements)

        if args.format == "json":
            payload = report.to_dict()
            if split_report is not None:
                payload["split"] = split_report.to_dict()
            print(json.dumps(payload, indent=2, sort_keys=False))
        else:
            print(report.to_markdown())
            if split_report is not None:
                print(split_report.to_markdown())
        return report.exit_code

    if emit:
        return _run_analyze_emit(args, all_statements)

    from skifer.analyzer.inventory import inventory_corpus

    corpus = inventory_corpus(all_statements)
    _print_analyze_inventory(corpus)
    return ANALYZE_EXIT_OK


def _run_analyze_emit(args: argparse.Namespace, all_statements: list) -> int:
    """``--emit``: find recurring patterns and publish validated drafts +
    rule stubs to ``--out`` (Plan 43 — 43.4).

    A preflight dry run decides ``--strict``'s "refuse outright" before any
    file is touched: skipped units and failed validations are known without
    writing anything, so a strict residue there means zero disk I/O. Whether
    an artifact's *target* file already exists without the marker can only be
    known against the real ``--out``, so that one is still checked during the
    real (non-dry) run and reported with its own exit code (4) regardless of
    ``--strict``.
    """
    from skifer.analyzer.emitter import emit_corpus
    from skifer.analyzer.patterns import PatternThresholds

    thresholds = PatternThresholds(
        min_occurrences=args.min_occurrences,
        min_scripts=args.min_scripts,
    )

    try:
        preflight = emit_corpus(all_statements, out_dir=args.out, thresholds=thresholds, dry_run=True)
    except Exception as exc:
        print(f"[analyze] --emit failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return ANALYZE_EXIT_ERROR

    if args.strict and preflight.has_residue:
        print(json.dumps(preflight.to_dict(), indent=2, sort_keys=False))
        print("[analyze] --strict: residue detected in preflight, refusing to write anything.", file=sys.stderr)
        return ANALYZE_EXIT_RESIDUE

    # Re-parse fresh: split_corpus/map_statement rewrite the AST in place
    # (43.3), so the preflight's statements are no longer safe to reuse.
    # Re-parsed directly from each Statement's own already-held text rather
    # than re-read from disk: `Statement.text` is immutable (only the AST is
    # mutated in place), so this gives a fresh, unmutated AST without a
    # second disk read — and, since 43.7's second sub-point, without
    # assuming `source_path` is a real file at all (a query-history
    # statement's `source_path` is a synthetic `system.query.history::<id>`).
    from skifer.analyzer.frontends import RawStatement
    from skifer.analyzer.parser import _parse_raw_statements

    fresh_statements = _parse_raw_statements(
        [RawStatement(source_path=s.source_path, line=s.line, text=s.text) for s in all_statements],
        dialect=args.dialect,
    )

    try:
        report = emit_corpus(fresh_statements, out_dir=args.out, thresholds=thresholds)
    except Exception as exc:
        print(f"[analyze] --emit failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return ANALYZE_EXIT_ERROR

    payload = report.to_dict()
    if getattr(args, "enrich", False):
        payload["enrichment"] = _run_analyze_enrich(fresh_statements, thresholds).to_dict()

    print(json.dumps(payload, indent=2, sort_keys=False))

    if any(a.status == "refused_exists" for a in report.artifacts):
        print("[analyze] Some files already exist without the project_analyzer marker; left untouched.", file=sys.stderr)
        return ANALYZE_EXIT_EXISTING_OUTPUT

    if report.has_residue:
        return ANALYZE_EXIT_RESIDUE

    return ANALYZE_EXIT_OK


def _run_analyze_enrich(statements: list, thresholds):
    """``--enrich`` on ``--emit`` (Plan 43 — 43.6, optional): propose rule
    names/docstrings, draft descriptions and fingerprint groupings via an
    LLM, additive to the printed report only — nothing here is written to
    the drafts or stubs on disk in this slice. No configured provider is a
    silent no-op, matching the plan's own stated behaviour.
    """
    from skifer.analyzer.emitter import list_draft_output_columns
    from skifer.analyzer.llm_enrichment import enrich
    from skifer.analyzer.patterns import find_patterns

    try:
        from skifer.semantic.llm_provider import get_llm_provider

        provider = get_llm_provider()
    except Exception:
        provider = None

    report = find_patterns(statements, thresholds=thresholds)
    draft_output_columns = list_draft_output_columns(statements)
    return enrich(report, draft_output_columns, provider=provider)


def _print_analyze_inventory(corpus) -> None:
    print(f"\n{len(corpus.statements)} statement(s) analyzed.\n")
    print("Join types:")
    for name, count in corpus.join_types.most_common():
        print(f"  {name}: {count}")
    print("Functions:")
    for name, count in corpus.function_names.most_common():
        print(f"  {name}: {count}")
    print("Aggregates:")
    for name, count in corpus.aggregate_names.most_common():
        print(f"  {name}: {count}")
    print(f"Windows: {corpus.window_count}")
    print(f"Subqueries: {corpus.subquery_count}")
    print(f"CTEs: {corpus.cte_count}")
    print(f"UNIONs: {corpus.union_count}")
    print(f"CASE expressions: {corpus.case_count}")
    print(f"GROUP BYs: {corpus.group_by_count}")
    print(f"HAVINGs: {corpus.having_count}")


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
    from skifer.lineage.classification import ClassificationViolationError
    from skifer.observability.metadata_index import index_from_path
    from skifer.observability.metadata_store import SqliteMetadataStore

    if args.target_fqn and len(args.paths) > 1:
        print("[index] --target-fqn is only valid with a single path.", file=sys.stderr)
        return INDEX_EXIT_USAGE

    # The lineage this command stores is built by RuleAnalyzer, which can only
    # read a rule the process has imported. Without this, every column a
    # business rule produces is indexed with no provenance and inherits no
    # classification — silently, because an unresolved rule is skipped.
    try:
        import_rule_modules(getattr(args, "rules", None) or [])
    except ValueError as exc:
        print(f"[index] {exc}", file=sys.stderr)
        return INDEX_EXIT_USAGE

    try:
        registry = store or SqliteMetadataStore(args.db)
    except Exception as exc:
        print(f"[index] Failed to open metadata registry: {exc}", file=sys.stderr)
        return INDEX_EXIT_ERROR
    changed = 0
    for path in args.paths:
        try:
            if getattr(args, "strict", False):
                wrote = index_from_path(
                    path,
                    registry,
                    target_fqn=args.target_fqn,
                    classification_mode="strict",
                )
                changed += int(wrote)
                print(f"[index] {path} -> {'updated' if wrote else 'unchanged'}")
                continue
            wrote = index_from_path(path, registry, target_fqn=args.target_fqn)
        except ClassificationViolationError as exc:
            print(
                f"[index] Classification violation in '{path}': {exc}",
                file=sys.stderr,
            )
            return INDEX_EXIT_CLASSIFICATION
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
    except (OSError, ValueError, yaml.YAMLError) as exc:
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
    ctx = _local_incidents_context()
    try:
        if service is None:
            from skifer.observability.certification_store import SqliteCertificationStore
            from skifer.services.quality import QualityService

            store = SqliteCertificationStore(args.store)
            service = QualityService(history_store=None, incident_store=store)

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
    if args.semantic_command == "metric-views":
        sys.exit(
            run_semantic_metric_views(
                args.models_dir or _semantic_models_dir(),
                args.catalog,
                args.schema,
                model_key=args.model,
            )
        )
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


def _run_api(args: argparse.Namespace) -> None:
    """Load and serve the optional API without importing FastAPI eagerly."""
    command = getattr(args, "api_command", None)
    if command == "openapi":
        _run_api_openapi(args)
        return
    if command != "serve":
        print("API command missing. Use 'skifer api --help'.", file=sys.stderr)
        sys.exit(1)
    try:
        from skifer.api.app import create_app

        app = create_app(args.project)
        import uvicorn

        uvicorn.run(
            app,
            host=API_LOOPBACK_HOST,
            port=args.port,
            log_config=None,
        )
    except Exception as exc:
        from skifer.api.app import APIDependencyError

        message = (
            str(exc)
            if isinstance(exc, APIDependencyError)
            else f"API startup failed ({type(exc).__name__})."
        )
        print(f"[api] {message}", file=sys.stderr)
        sys.exit(1)


def _run_api_openapi(args: argparse.Namespace) -> None:
    """Print a stable, sorted JSON representation of the OpenAPI document."""
    try:
        from skifer.api.app import openapi_document

        with redirect_stdout(StringIO()):
            document = openapi_document(args.project)
        print(
            json.dumps(
                document,
                sort_keys=True,
                indent=2,
                ensure_ascii=False,
            )
        )
    except Exception as exc:
        from skifer.api.app import APIDependencyError

        message = (
            str(exc)
            if isinstance(exc, APIDependencyError)
            else f"OpenAPI export failed ({type(exc).__name__})."
        )
        print(f"[api] {message}", file=sys.stderr)
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
    if schema.semantic is not None and schema.semantic.source_candidate_id:
        _record_candidate_certified(schema.semantic.source_candidate_id, model_key, models_dir)
    return SEMANTIC_EXIT_OK


def _candidate_store_for_certification():
    """The local candidate registry, or ``None`` when there is none to record into."""
    from skifer.candidates.store import DEFAULT_CANDIDATES_DB, SqliteCandidateStore

    if not os.path.exists(DEFAULT_CANDIDATES_DB):
        return None
    return SqliteCandidateStore(DEFAULT_CANDIDATES_DB)


def _record_candidate_certified(candidate_id: str, model_key: str, models_dir: str) -> None:
    """Append a ``certified`` event for the promoted candidate (Plan 44, S06).

    Best-effort, like the Plan 31 indexing hooks: the model is already promoted,
    so an unavailable store or an unknown candidate is a WARNING, never a failure.
    """
    import logging
    import uuid
    from datetime import datetime, timezone

    log = logging.getLogger("skifer.candidates")
    store = None
    try:
        from skifer.candidates import CertificationEvent
        from skifer.candidates.validator import semantic_layer_version

        store = _candidate_store_for_certification()
        if store is None:
            log.warning("Candidate store unavailable: no 'certified' event for %s.", candidate_id)
            return
        candidate = store.get_candidate(candidate_id)
        if candidate is None:
            log.warning("Unknown candidate %s: no 'certified' event recorded.", candidate_id)
            return
        store.append_event(CertificationEvent(
            event_id=uuid.uuid4().hex, metric_id=candidate_id,
            metric_name=candidate.metric_name, event_type="certified",
            previous_state=candidate.state, new_state=candidate.state,
            decided_by="system:semantic_sync_promote", evidence={"model_key": model_key},
            semantic_layer_version=semantic_layer_version(models_dir),
            event_at=datetime.now(timezone.utc),
        ))
    except Exception as exc:
        log.warning("Could not record the 'certified' event for %s (%s).", candidate_id,
                    type(exc).__name__)
    finally:
        if store is not None:
            try:
                store.close()
            except Exception:
                pass


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


def run_semantic_metric_views(
    models_dir: str,
    catalog: str,
    schema: str,
    *,
    model_key: str | None = None,
) -> int:
    """Print the metric view DDL each semantic model translates to (Plan 42.4).

    Reads the catalog and model YAML from disk only — no Spark, no certification
    gate. Returns 0 if every selected model translates, 1 if any is refused
    (Plan 42, decision D4: relationships, a semi_additive metric, an unsafe
    identifier). Publishing the DDL is `SemanticEngine.publish_metric_views`'s
    job, not this command's.
    """
    from skifer.semantic.metric_views import MetricViewTranslationError, metric_view_ddl

    catalog_path = os.path.join(models_dir, "semantic_catalog.yaml")
    try:
        entries = _load_yaml_file(catalog_path).get("models", [])
    except OSError as exc:
        print(f"[semantic.metric-views] Failed to read catalog '{catalog_path}': {exc}")
        return SEMANTIC_EXIT_ERROR

    if model_key is not None:
        entries = [entry for entry in entries if entry.get("key") == model_key]
        if not entries:
            print(f"[semantic.metric-views] Model '{model_key}' not found in '{catalog_path}'.")
            return SEMANTIC_EXIT_ERROR

    exit_code = SEMANTIC_EXIT_OK
    for entry in entries:
        key = entry.get("key", "?")
        try:
            model_path = os.path.join(models_dir, entry["file"])
            model = _load_yaml_file(model_path)["models"][0]
            ddl = metric_view_ddl(model, catalog, schema)
        except (MetricViewTranslationError, KeyError, OSError) as exc:
            print(f"[semantic.metric-views] Model '{key}': {exc}")
            exit_code = SEMANTIC_EXIT_ERROR
            continue
        print(ddl)
        print()

    return exit_code


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


def import_rule_modules(module_names) -> None:
    """Import each module so its ``@register_rule`` decorators run.

    The project's rules live in the project, not in the package, so no CLI
    command can see them until something imports them. The current directory is
    put on ``sys.path`` because that is where ``config.yaml`` is discovered from,
    which is the same project root a reader means by ``rules.orders``.

    An import failure is fatal rather than a warning: continuing would refuse the
    pipeline for a missing rule and blame the pipeline, not the import.
    """
    import importlib

    if not module_names:
        return
    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)
    for module_name in module_names:
        try:
            importlib.import_module(module_name)
        except Exception as exc:
            raise ValueError(
                f"--rules {module_name!r} could not be imported: "
                f"{type(exc).__name__}: {exc}"
            ) from exc
