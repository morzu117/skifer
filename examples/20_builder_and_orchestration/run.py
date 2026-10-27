"""BuilderAgent drafts pipeline YAML offline, refuses an invalid draft, and exports orchestration scaffolds.

    python examples/20_builder_and_orchestration/run.py
"""

import json
from pathlib import Path
import tempfile

from skifer.agentic.builder_agent import BuilderAgent
from skifer.semantic.llm_provider import LLMProvider


class StubLLM(LLMProvider):
    """Offline LLM: returns canned JSON answers in order."""

    def __init__(self, *answers):
        self.answers = [json.dumps(a) for a in answers]

    provider_name = "offline_stub"

    def complete(self, system_prompt, user_message, **kwargs):
        return self.answers.pop(0)

    def complete_with_history(self, system_prompt, history, **kwargs):
        return self.answers.pop(0)


class LocalCatalog:
    """The only catalog methods BuilderAgent needs: no Spark."""

    is_local = True
    columns = {"silver.orders": ["order_id", "amount", "region"]}

    def build_fqn(self, catalog, schema, table):
        return f"{schema}.{table}" if schema else table

    def table_exists(self, catalog, schema, table):
        return self.build_fqn(catalog, schema, table) in self.columns

    def list_tables(self, schema, catalog=None):
        return [name.removeprefix(f"{schema}.") for name in self.columns]

    def list_columns(self, fqn):
        return self.columns.get(fqn.replace("`", ""), [])


def proposal(operator):
    return {
        "tables": [{"fqn": "silver.orders", "alias": "orders", "filters": [f"region:{operator}:EMEA"]}],
        "joins": [],
        "business_rules": [],
        "select_final": [
            {"source": "order_id", "target": "order_id", "ops": []},
            {"source": "amount", "target": "amount", "ops": []},
        ],
        "keep_all_columns": False,
        "output_name": None,
    }


agent = BuilderAgent(LocalCatalog(), catalog=None, llm_provider=StubLLM(proposal("approximately"), proposal("equals")))

with tempfile.TemporaryDirectory() as temp:
    root = Path(temp)
    pipelines = root / "pipelines"

    invalid = agent.ask("Draft an approximate EMEA pipeline", str(pipelines))
    print("Offline stub proposed filter: region:approximately:EMEA")
    print(f"Framework refusal: {invalid.error}")
    print(f"Invalid pipeline written: {(pipelines / 'pipeline.yaml').exists()}")

    valid = agent.ask("Draft a validated EMEA pipeline", str(pipelines))
    print(f"Validated pipeline written in temporary directory: {valid.success}")

    airflow_dir = root / "airflow_export"
    airflow = agent.export_orchestration([valid.output_path], output_dir=str(airflow_dir), format="airflow")
    print(f"\nExport format requested: airflow -> produced: {airflow.format}")
    print(f"Generated files: {', '.join(sorted(p.relative_to(airflow_dir).as_posix() for p in airflow_dir.rglob('*.py')))}")
    print("DAG excerpt:")
    for line in Path(airflow.primary_path).read_text(encoding="utf-8").splitlines():
        if line.startswith("with DAG") or "TODO" in line:
            print(f"  {line.strip()}")
    print("The DAG is a scaffold: the task body is a TODO, not a working call.")

    # Off Databricks, `auto` resolves to the plain runner script every format also emits.
    auto_dir = root / "auto_export"
    auto = agent.export_orchestration([valid.output_path], output_dir=str(auto_dir), format="auto")
    print(f"\nExport format requested: auto -> produced: {auto.format}")
    print(f"Generated files: {', '.join(sorted(p.relative_to(auto_dir).as_posix() for p in auto_dir.rglob('*.py')))}")
    print("Runner script excerpt:")
    for line in Path(auto.primary_path).read_text(encoding="utf-8").splitlines():
        if line.startswith(("from skifer", "# Pipeline:")):
            print(f"  {line}")
