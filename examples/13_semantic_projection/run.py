"""Project and synchronize a semantic draft without Spark.

    python examples/13_semantic_projection/run.py

Every generated artifact is kept in a temporary working directory. The real
semantic-sync command helper is called there so its exit codes are the same
ones used by the CLI and CI.
"""

from contextlib import redirect_stdout
from io import StringIO
import os
from pathlib import Path
from tempfile import TemporaryDirectory

import yaml

from skifer.cli import run_semantic_sync
from skifer.core.ir import parse_to_ir
from skifer.core.schema_loader import parse_schema
from skifer.semantic.draft_builder import SemanticDraftBuilder
from skifer.semantic.output_projection import OutputProjector


EXAMPLE_DIR = Path(__file__).parent


def _project(yaml_text: str):
    schema = parse_to_ir(parse_schema(yaml_text))
    projected = OutputProjector().project(schema)
    return schema, projected


def _sync_code(workspace: Path, yaml_text: str) -> int:
    pipeline_path = workspace / "pipeline.yaml"
    pipeline_path.write_text(yaml_text, encoding="utf-8")
    previous = Path.cwd()
    try:
        os.chdir(workspace)
        # The command's detailed report contains a workspace-relative filename.
        # This example presents the stable CI contract: the returned exit code.
        with redirect_stdout(StringIO()):
            return run_semantic_sync("pipeline.yaml", mode="check")
    finally:
        os.chdir(previous)


def main() -> None:
    base_yaml = (EXAMPLE_DIR / "pipeline.yaml").read_text(encoding="utf-8")
    with TemporaryDirectory(prefix="skifer-example-13-") as temporary:
        workspace = Path(temporary)
        models_dir = workspace / "semantic_models"
        schema, projected = _project(base_yaml)
        builder = SemanticDraftBuilder(output_dir=str(models_dir))

        draft_path = Path(builder.write_draft(projected, schema))
        first_bytes = draft_path.read_bytes()
        builder.write_draft(projected, schema)
        second_bytes = draft_path.read_bytes()
        draft = yaml.safe_load(second_bytes)
        model = draft["models"][0]

        print("Managed draft projected from pipeline:")
        print(f"  model: {model['key']}")
        print(f"  fields: {', '.join(model['metadata']['generated_fields'])}")
        print(f"  generated marker: {draft['_generated_by']['kind']}")
        print(f"Draft generated twice: bytes identical = {first_bytes == second_bytes}")

        # Promote the baseline once so subsequent checks are a real three-way
        # comparison: last generation, curated model, and new candidate.
        (workspace / "pipeline.yaml").write_text(base_yaml, encoding="utf-8")
        previous = Path.cwd()
        try:
            os.chdir(workspace)
            with redirect_stdout(StringIO()):
                promoted = run_semantic_sync("pipeline.yaml", mode="promote")
        finally:
            os.chdir(previous)
        if promoted != 0:
            raise RuntimeError(f"Baseline promotion failed with exit code {promoted}.")

        current_code = _sync_code(workspace, base_yaml)
        drift_yaml = base_yaml.replace(
            "    orders: {description: Distinct orders}\n",
            "    orders: {description: Distinct orders}\n"
            "    total_amount: {description: Total order amount}\n",
        ).replace(
            "    - [order_id, orders, count_distinct]\n",
            "    - [order_id, orders, count_distinct]\n"
            "    - [amount, total_amount, sum]\n",
        )
        drift_code = _sync_code(workspace, drift_yaml)
        conflict_yaml = base_yaml.replace(
            "  grain: [country]\n", "  grain: [country, order_day]\n"
        ).replace(
            "    country: {logical_type: string, description: Order country}\n",
            "    country: {logical_type: string, description: Order country}\n"
            "    order_day: {logical_type: date, description: Order day}\n",
        ).replace("  group_by: [country]\n", "  group_by: [country, order_day]\n")
        conflict_code = _sync_code(workspace, conflict_yaml)

        print(f"semantic sync --check (current): exit {current_code}")
        print(f"semantic sync --check (added total_amount): exit {drift_code}")
        print(f"semantic sync --check (changed grain): exit {conflict_code}")

        refusal_dir = workspace / "unmanaged" / ".drafts"
        refusal_dir.mkdir(parents=True)
        (refusal_dir / "orders_summary.yaml").write_text(
            "models:\n  - key: written_by_a_human\n", encoding="utf-8"
        )
        refusing_builder = SemanticDraftBuilder(
            output_dir=str(workspace / "unmanaged")
        )
        try:
            refusing_builder.write_draft(projected, schema)
        except ValueError as exc:
            # The exception starts with the temporary target path. Print its
            # exact stable refusal sentence, which is part of the real message.
            refusal = str(exc)
            stable_refusal = refusal[refusal.index("Refusing to overwrite") :]
            print(f"Refused unmanaged draft: {stable_refusal}")

    print(f"Temporary workspace removed: {not Path(temporary).exists()}")


if __name__ == "__main__":
    main()
