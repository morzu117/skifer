"""Draft validated pipeline YAML and disposable orchestration artifacts offline."""

from collections import deque
import json
from pathlib import Path
import tempfile
from typing import Any

from skifer.agentic.builder_agent import BuilderAgent
from skifer.semantic.llm_provider import LLMProvider


class StubLLMProvider(LLMProvider):
    """Return fixed ETL proposals without an SDK, key, API call, or network."""

    def __init__(self, responses: list[dict[str, Any]]):
        self._responses = deque(json.dumps(item) for item in responses)

    @property
    def provider_name(self) -> str:
        return "offline_stub"

    def complete(
        self,
        system_prompt: str,
        user_message: str,
        temperature: float = 0.0,
        response_format: dict | None = None,
        **kwargs: Any,
    ) -> str:
        return self._responses.popleft()

    def complete_with_history(
        self,
        system_prompt: str,
        history: list[dict],
        temperature: float = 0.0,
        response_format: dict | None = None,
        **kwargs: Any,
    ) -> str:
        return self.complete(
            system_prompt,
            history[-1]["content"],
            temperature,
            response_format,
            **kwargs,
        )


class LocalCatalogBackend:
    """Small in-example catalog backend; BuilderAgent needs no live Spark."""

    is_local = True
    _columns = {"silver.orders": ["order_id", "amount", "region"]}

    def build_fqn(self, catalog, schema, table):
        return f"{schema}.{table}" if schema else table

    def table_exists(self, catalog, schema, table):
        return self.build_fqn(catalog, schema, table) in self._columns

    def list_tables(self, schema, catalog=None):
        prefix = f"{schema}." if schema else ""
        return [name.removeprefix(prefix) for name in self._columns]

    def list_columns(self, fqn):
        return list(self._columns.get(fqn.replace("`", ""), []))


def proposal(operator: str) -> dict[str, Any]:
    return {
        "tables": [
            {
                "fqn": "silver.orders",
                "alias": "orders",
                "filters": [f"region:{operator}:EMEA"],
            }
        ],
        "joins": [],
        "business_rules": [],
        "select_final": [
            {"source": "order_id", "target": "order_id", "ops": []},
            {"source": "amount", "target": "amount", "ops": []},
        ],
        "keep_all_columns": False,
        "output_name": None,
    }


def _relative_files(root: Path) -> list[str]:
    return sorted(
        str(path.relative_to(root)) for path in root.rglob("*") if path.is_file()
    )

def main() -> None:
    provider = StubLLMProvider([proposal("approximately"), proposal("equals")])
    agent = BuilderAgent(
        LocalCatalogBackend(), catalog=None, llm_provider=provider
    )

    with tempfile.TemporaryDirectory(prefix="skifer-builder-") as temp:
        root = Path(temp)
        pipeline_dir = root / "pipelines"
        invalid = agent.ask("Draft an approximate EMEA pipeline", str(pipeline_dir))
        print("Offline stub proposed filter: region:approximately:EMEA")
        print(f"Framework refusal: {invalid.error}")
        print(f"Invalid pipeline written: {(pipeline_dir / 'pipeline.yaml').exists()}")

        valid = agent.ask("Draft a validated EMEA pipeline", str(pipeline_dir))
        pipeline_path = Path(valid.output_path or "")
        print(f"Validated pipeline written in temporary directory: {valid.success}")

        # Airflow: a DAG, plus the plain runner script as a fallback. Every format
        # emits that fallback, so a pipeline is never trapped in an orchestrator
        # the team turns out not to run.
        airflow_dir = root / "airflow_export"
        airflow = agent.export_orchestration(
            [str(pipeline_path)], output_dir=str(airflow_dir), format="airflow"
        )
        print(f"\nExport format requested: airflow -> produced: {airflow.format}")
        print(f"Generated files: {', '.join(_relative_files(airflow_dir))}")
        print("DAG excerpt:")
        for line in Path(airflow.primary_path).read_text(encoding="utf-8").splitlines():
            if line.startswith("with DAG") or "TODO" in line:
                print(f"  {line.strip()}")
        print("The DAG is a scaffold: the task body is a TODO, not a working call.")

        # `auto` is not a promise of Airflow: off Databricks it resolves to the
        # runner script, and the example shows that rather than describing it.
        auto_dir = root / "auto_export"
        auto = agent.export_orchestration(
            [str(pipeline_path)], output_dir=str(auto_dir), format="auto"
        )
        print(f"\nExport format requested: auto -> produced: {auto.format}")
        print(f"Generated files: {', '.join(_relative_files(auto_dir))}")
        print("Runner script excerpt:")
        for line in Path(auto.primary_path).read_text(encoding="utf-8").splitlines():
            if line.startswith("from skifer") or line.startswith("# Pipeline:"):
                print(f"  {line}")

    print(f"Temporary directory removed: {not root.exists()}")


if __name__ == "__main__":
    main()
