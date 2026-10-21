"""Project a pipeline into a managed semantic draft, then check it the way CI does.

    python examples/13_semantic_projection/run.py
"""

import atexit
from contextlib import redirect_stdout
from io import StringIO
import os
from pathlib import Path
import shutil
from tempfile import mkdtemp

import yaml

from skifer.cli import run_semantic_sync
from skifer.core.ir import parse_to_ir
from skifer.core.schema_loader import parse_schema
from skifer.semantic.draft_builder import SemanticDraftBuilder
from skifer.semantic.output_projection import OutputProjector

base_yaml = (Path(__file__).parent / "pipeline.yaml").read_text(encoding="utf-8")
workspace = mkdtemp(prefix="skifer-example-13-")
atexit.register(shutil.rmtree, workspace)
os.chdir(workspace)

schema = parse_to_ir(parse_schema(base_yaml))
projected = OutputProjector().project(schema)
builder = SemanticDraftBuilder(output_dir="semantic_models")

draft_path = Path(builder.write_draft(projected, schema))
first_bytes = draft_path.read_bytes()
builder.write_draft(projected, schema)
draft = yaml.safe_load(draft_path.read_bytes())
model = draft["models"][0]
print("Managed draft projected from pipeline:")
print(f"  model: {model['key']}")
print(f"  fields: {', '.join(model['metadata']['generated_fields'])}")
print(f"  generated marker: {draft['_generated_by']['kind']}")
print(f"Draft generated twice: bytes identical = {first_bytes == draft_path.read_bytes()}")


def sync(yaml_text, mode="check"):
    Path("pipeline.yaml").write_text(yaml_text, encoding="utf-8")
    with redirect_stdout(StringIO()):  # keep only the exit code, the CI contract
        return run_semantic_sync("pipeline.yaml", mode=mode)


# Promote once: later checks are a three-way merge against this last generation.
sync(base_yaml, mode="promote")

drift_yaml = base_yaml.replace(
    "    orders: {description: Distinct orders}\n",
    "    orders: {description: Distinct orders}\n    total_amount: {description: Total order amount}\n",
).replace(
    "    - [order_id, orders, count_distinct]\n",
    "    - [order_id, orders, count_distinct]\n    - [amount, total_amount, sum]\n",
)
conflict_yaml = (
    base_yaml.replace("  grain: [country]\n", "  grain: [country, order_day]\n")
    .replace(
        "    country: {logical_type: string, description: Order country}\n",
        "    country: {logical_type: string, description: Order country}\n"
        "    order_day: {logical_type: date, description: Order day}\n",
    )
    .replace("  group_by: [country]\n", "  group_by: [country, order_day]\n")
)
print(f"semantic sync --check (current): exit {sync(base_yaml)}")
print(f"semantic sync --check (added total_amount): exit {sync(drift_yaml)}")
print(f"semantic sync --check (changed grain): exit {sync(conflict_yaml)}")

# A file without the `_generated_by` marker is human-owned: the builder refuses to overwrite it.
Path("unmanaged/.drafts").mkdir(parents=True)
Path("unmanaged/.drafts/orders_summary.yaml").write_text("models:\n  - key: written_by_a_human\n", encoding="utf-8")
try:
    SemanticDraftBuilder(output_dir="unmanaged").write_draft(projected, schema)
except ValueError as exc:
    print(f"Refused unmanaged draft: {str(exc)[str(exc).index('Refusing to overwrite'):]}")
