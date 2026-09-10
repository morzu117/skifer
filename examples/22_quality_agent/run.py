"""Run QualityAgent checks against deliberately bad local data.

    python examples/22_quality_agent/run.py
"""

import tempfile
from pathlib import Path

from skifer import SkiferEngine
from skifer.agentic.quality_agent import QualityAgent
from skifer.observability.checks import NullCheck, UniqueCheck
from skifer.observability.history import SqliteHistoryStore

TARGET_TABLE = "quality_agent_orders"


def _print_results(response) -> None:
    for result in response.report.results:
        print(
            f"  {type(result.contract).__name__}: "
            f"{result.status.value} — {result.message.splitlines()[0]}"
        )
    print(f"  QualityResponse.passed: {response.passed}")


def main() -> None:
    engine = SkiferEngine(force_env="LOCAL")
    target_schema = engine.get_target_schema("gold")
    target_fqn = f"{target_schema}.{TARGET_TABLE}"
    engine.backend.ensure_schema_exists(target_schema)
    rows = [(1, 100.0), (1, None), (2, 50.0)]
    data = engine.spark.createDataFrame(rows, "order_id INT, amount DOUBLE")
    data.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(target_fqn)

    with tempfile.TemporaryDirectory() as temp_dir:
        history_path = Path(temp_dir) / "quality_history.db"
        store = SqliteHistoryStore(str(history_path))
        try:
            agent = QualityAgent(backend=engine.backend, history_store=store)

            failed = agent.check(
                target_fqn,
                contracts=[
                    NullCheck(target_fqn, column="amount", severity="critical"),
                    UniqueCheck(
                        target_fqn,
                        columns=["order_id"],
                        severity="critical",
                    ),
                ],
            )
            print("Two critical checks on the real table:")
            _print_results(failed)

            errored = agent.check(
                target_fqn,
                contracts=[
                    NullCheck(
                        target_fqn,
                        column="missing_amount",
                        severity="warning",
                    )
                ],
            )
            print("\nA check that could not execute:")
            error_result = errored.report.results[0]
            print(f"  NullCheck status: {error_result.status.value}")
            print(f"  Framework refusal: {error_result.message.splitlines()[0]}")
            print(f"  QualityResponse.passed: {errored.passed}")

            latest_report = agent.report(target_fqn, format="text")
            latest_status_line = next(
                line.strip()
                for line in latest_report.splitlines()
                if "Status    :" in line
            )
            print(f"\nLatest text report: {latest_status_line}")

            history = agent.get_history(target_fqn, n=10)
            print("\nHistory after two runs:")
            print(f"  mode: {history.mode}")
            print(f"  reports recorded: {len(history.history_reports)}")
            print(
                "  checks per run (newest first): "
                + ", ".join(str(len(report.results)) for report in history.history_reports)
            )
        finally:
            store.close()

    print(f"Temporary history directory removed: {not Path(temp_dir).exists()}")


if __name__ == "__main__":
    main()
