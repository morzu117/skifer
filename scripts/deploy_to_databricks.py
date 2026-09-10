"""
deploy_to_databricks.py — Log SkiferChatModel to MLflow and register it
in the Unity Catalog Model Registry.

Usage:
    python scripts/deploy_to_databricks.py
    python scripts/deploy_to_databricks.py --config path/config.yaml \\
        --models-dir semantic/ \\
        --experiment /skifer/hub \\
        --registered-name skifer_hub

Prerequisites:
    - pip install -e ".[serving]"
    - DATABRICKS_HOST and DATABRICKS_TOKEN set in environment (or .env)
    - mlflow tracking URI pointing to your Databricks workspace:
        export MLFLOW_TRACKING_URI=databricks

After running this script, create a Model Serving endpoint via:
    - Databricks UI: Machine Learning > Serving > Create endpoint
    - or REST API: POST /api/2.0/serving-endpoints (see printed instructions)
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
) -> str:
    """Log the model to MLflow and return the run_id."""
    import mlflow

    from skifer.serving.chat_model import _build_mlflow_model

    mlflow.set_tracking_uri("databricks")
    mlflow.set_experiment(experiment)

    pip_requirements = [
        "skifer[serving]",
        "pyspark>=3.3.0",
        "delta-spark>=2.0.0",
    ]

    print(f"[deploy] Logging model to experiment '{experiment}' ...")
    with mlflow.start_run() as run:
        mlflow.pyfunc.log_model(
            artifact_path="skifer_hub",
            python_model=_build_mlflow_model(),
            artifacts={
                "config": str(config),
                "models_dir": str(models_dir),
            },
            pip_requirements=pip_requirements,
            registered_model_name=registered_name,
        )
        run_id = run.info.run_id

    print(f"[deploy] Model logged. Run ID: {run_id}")
    print(f"[deploy] Registered as: {registered_name}")
    return run_id


def _print_endpoint_instructions(registered_name: str, host: str) -> None:
    """Print the next steps to create a Model Serving endpoint."""
    print("\n" + "=" * 60)
    print("NEXT STEP — Create a Model Serving endpoint")
    print("=" * 60)
    print()
    print("Option A — Databricks UI:")
    print("  Machine Learning > Serving > Create endpoint")
    print(f"  Model: {registered_name} (latest version)")
    print()
    print("Option B — REST API:")
    print(f"  POST {host}/api/2.0/serving-endpoints")
    print("  Body:")
    print(f"""  {{
    "name": "{registered_name.replace('.', '_')}",
    "config": {{
      "served_models": [{{
        "model_name": "{registered_name}",
        "model_version": "1",
        "workload_size": "Small",
        "scale_to_zero_enabled": true
      }}]
    }}
  }}""")
    print()
    print("Option C — Test the endpoint once created:")
    endpoint = registered_name.replace(".", "_")
    payload = '\'{"messages": [{"role": "user", "content": "Where does amount_eur come from?"}]}\''
    print(
        f"  curl -X POST \\\n"
        f"    {host}/serving-endpoints/{endpoint}/invocations \\\n"
        "    -H \"Authorization: Bearer $DATABRICKS_TOKEN\" \\\n"
        "    -H \"Content-Type: application/json\" \\\n"
        f"    -d {payload}"
    )
    print()
    print("For Genie Tools integration, register the endpoint URL above as a")
    print("custom tool in your Genie Space settings.")
    print("=" * 60)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Log SkiferChatModel to Databricks MLflow and register it.",
    )
    parser.add_argument("--config", default="config.yaml", help="Path to config.yaml")
    parser.add_argument("--models-dir", default="semantic", help="Semantic models directory")
    parser.add_argument("--experiment", default="/skifer/hub", help="MLflow experiment name")
    parser.add_argument(
        "--registered-name",
        default="skifer_hub",
        help="Unity Catalog registered model name (e.g. catalog.schema.model or plain name)",
    )
    args = parser.parse_args()

    config = Path(args.config)
    models_dir = Path(args.models_dir)
    host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")

    _check_prerequisites(config, models_dir)
    _log_model(config, models_dir, args.experiment, args.registered_name)
    _print_endpoint_instructions(args.registered_name, host)


if __name__ == "__main__":
    main()
