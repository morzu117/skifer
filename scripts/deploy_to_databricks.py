"""
deploy_to_databricks.py — Log SkiferResponsesAgent to MLflow and register it
in the Unity Catalog Model Registry (Plan 41).

Usage:
    python scripts/deploy_to_databricks.py --llm-endpoint my-chat-endpoint \\
        --registered-name main.agents.skifer
    python scripts/deploy_to_databricks.py --config path/config.yaml \\
        --models-dir semantic/ \\
        --experiment /skifer/agent \\
        --llm-endpoint my-chat-endpoint \\
        --registered-name main.agents.skifer

Prerequisites:
    - pip install -e ".[serving]"   (without [spark]: databricks-connect brings pyspark)
    - DATABRICKS_HOST and DATABRICKS_TOKEN set in environment (or .env)
    - DATABRICKS_SERVERLESS_COMPUTE_ID=auto, outside a Databricks notebook

MLflow validates the agent by calling it while logging: the hub is built here (Spark
through Connect serverless, semantic models, LLM endpoint) and asked one question.

After running this script, create the Model Serving endpoint and wire the supervisor:
see docs/databricks_supervisor.md.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


def _check_prerequisites(config: Path, models_dir: Path) -> None:
    """Validate environment and files before starting."""
    errors = []

    if not os.environ.get("DATABRICKS_HOST"):
        errors.append("DATABRICKS_HOST is not set.")
    if not os.environ.get("DATABRICKS_TOKEN"):
        errors.append("DATABRICKS_TOKEN is not set.")
    if not config.exists():
        errors.append(f"Config file not found: {config}")
    if not models_dir.exists():
        errors.append(f"Models directory not found: {models_dir}")

    try:
        import mlflow  # noqa: F401
    except ImportError:
        errors.append(
            "'mlflow' is not installed. Run: pip install -e '.[serving]'"
        )

    if errors:
        print("[deploy] Prerequisites check failed:")
        for e in errors:
            print(f"  - {e}")
        sys.exit(1)

    print("[deploy] Prerequisites OK.")


def _log_model(
    config: Path,
    models_dir: Path,
    experiment: str,
    registered_name: str,
    llm_endpoint: str,
    warehouse_id: str | None,
    example_question: str | None,
) -> str:
    """Log the agent to MLflow and return the run_id."""
    import mlflow

    from skifer.serving import log_agent

    mlflow.set_tracking_uri("databricks")
    mlflow.set_registry_uri("databricks-uc")
    mlflow.set_experiment(experiment)

    print(f"[deploy] Logging agent to experiment '{experiment}' ...")
    with mlflow.start_run() as run:
        log_agent(
            str(config),
            str(models_dir),
            llm_endpoint,
            registered_model_name=registered_name,
            warehouse_id=warehouse_id,
            example_question=example_question,
        )
        run_id = run.info.run_id

    print(f"[deploy] Agent logged. Run ID: {run_id}")
    print(f"[deploy] Registered as: {registered_name}")
    return run_id


def _print_endpoint_instructions(registered_name: str, host: str) -> None:
    """Print the next steps to create the serving endpoint."""
    endpoint = registered_name.replace(".", "_")
    payload = '\'{"input": [{"role": "user", "content": "Quel est le CA par région ?"}]}\''
    print("\n" + "=" * 60)
    print("NEXT STEP — Create the Model Serving endpoint")
    print("=" * 60)
    print()
    print(f"  Model: {registered_name} (latest version)")
    print("  Environment variables:")
    print("    DATABRICKS_SERVERLESS_COMPUTE_ID = auto")
    print(f"    DATABRICKS_HOST                  = {host or '<workspace URL>'}")
    print("    DATABRICKS_TOKEN                 = {{secrets/<scope>/<key>}}")
    print()
    print("Test it once it is ready:")
    print(
        f"  curl -X POST \\\n"
        f"    {host}/serving-endpoints/{endpoint}/invocations \\\n"
        "    -H \"Authorization: Bearer $DATABRICKS_TOKEN\" \\\n"
        "    -H \"Content-Type: application/json\" \\\n"
        f"    -d {payload}"
    )
    print()
    print("Then wire Genie and this endpoint behind a supervisor:")
    print("  docs/databricks_supervisor.md")
    print("=" * 60)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Log SkiferResponsesAgent to Databricks MLflow and register it.",
    )
    parser.add_argument("--config", default="config.yaml", help="Path to config.yaml")
    parser.add_argument("--models-dir", default="semantic", help="Semantic models directory")
    parser.add_argument("--experiment", default="/skifer/agent", help="MLflow experiment name")
    parser.add_argument(
        "--registered-name",
        required=True,
        help="Unity Catalog registered model name (catalog.schema.model)",
    )
    parser.add_argument(
        "--llm-endpoint", required=True, help="Chat serving endpoint the agent calls"
    )
    parser.add_argument("--warehouse-id", default=None, help="SQL warehouse to declare")
    parser.add_argument(
        "--example-question",
        default=None,
        help="Question MLflow asks while logging (MLflow's default otherwise)",
    )
    args = parser.parse_args()

    config = Path(args.config)
    models_dir = Path(args.models_dir)
    host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")

    _check_prerequisites(config, models_dir)
    _log_model(
        config,
        models_dir,
        args.experiment,
        args.registered_name,
        args.llm_endpoint,
        args.warehouse_id,
        args.example_question,
    )
    _print_endpoint_instructions(args.registered_name, host)


if __name__ == "__main__":
    main()
