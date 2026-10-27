"""Map an existing legacy SQL corpus to Skifer pipeline drafts, Spark-free.

    python examples/24_project_analyzer/run.py

Calls the same ``run_analyze_command`` the ``skifer analyze`` CLI uses, so the
exit codes and report shapes are exactly the ones CI sees. Three passes over
the same corpus: ``--dry-run`` (maps + round-trip-verifies, writes nothing),
``--dry-run --split`` (adds the CTE/subquery dependency graph), and ``--emit``
(writes validated pipeline drafts and a shared-rule stub to a temporary
directory). The CLI's own per-file/JSON output is captured rather than
printed verbatim — it is exercised, just not dumped in full here.
"""

from __future__ import annotations

import argparse
import json
import os
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory

from skifer.cli import run_analyze_command

EXAMPLE_DIR = Path(__file__).parent
CORPUS = sorted(str(p) for p in (EXAMPLE_DIR / "legacy_sql").glob("*.sql"))


def _run_captured(args: argparse.Namespace) -> tuple[int, str]:
    buffer = StringIO()
    with redirect_stdout(buffer):
        exit_code = run_analyze_command(args)
    return exit_code, buffer.getvalue()


def _dry_run_report(*, split: bool) -> dict:
    args = argparse.Namespace(
        paths=CORPUS, inventory=False, dry_run=True, emit=False,
        format="json", split=split, dialect="databricks",
    )
    _, output = _run_captured(args)
    return json.loads(output[output.index("{"):])


def _emit(out_dir: Path) -> tuple[int, dict]:
    args = argparse.Namespace(
        paths=CORPUS, inventory=False, dry_run=False, emit=True, dialect="databricks",
        out=str(out_dir), strict=False, min_occurrences=3, min_scripts=2,
    )
    exit_code, output = _run_captured(args)
    return exit_code, json.loads(output[output.index("{"):])


def main() -> None:
    print("=== --dry-run (map + round-trip-verify, nothing written) ===")
    report = _dry_run_report(split=False)
    print(f"statements: {report['statement_count']}, verified clean: {report['clean_count']}")

    print("\n=== --dry-run --split (adds the CTE/subquery dependency graph) ===")
    split_report = _dry_run_report(split=True)
    units = split_report["split"]["units"]
    by_kind = sorted({u["kind"] for u in units})
    print(f"split units: {len(units)}, kinds present: {by_kind}")

    print("\n=== --emit (writes validated drafts + rule stubs) ===")
    with TemporaryDirectory() as tmp:
        out_dir = Path(tmp)
        exit_code, report = _emit(out_dir)
        print(f"exit code: {exit_code} (2 = emitted, with residue reported — expected on this corpus)")

        written = sorted(a["path"] for a in report["artifacts"] if a["status"] == "written")
        print("artifacts written:\n  " + "\n  ".join(written))
        print("skipped (partials + non-materialized SELECTs, by design — see emitter.py):")
        for name in sorted({s["name"] for s in report["skipped"]}):
            shown = os.path.relpath(name) if name.endswith(".sql") else name
            print(f"  {shown}")

        country_draft = out_dir / "schemas" / ".drafts" / "silver" / "stg_customers.yaml"
        wired = [
            line for line in country_draft.read_text().splitlines()
            if "business_rules" in line or line.strip().startswith("- country")
        ]
        print(f"\n{country_draft.relative_to(out_dir)} business_rules wiring: {wired}")


if __name__ == "__main__":
    main()
