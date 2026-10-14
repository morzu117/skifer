"""
log_agent — log SkiferResponsesAgent to MLflow with what Model Serving needs (Plan 41.6).

Run it from a Databricks notebook. MLflow validates an agent by calling it while
logging (load_context, then predict on an example), so the hub is really built
there: Spark, the semantic models and the LLM endpoint must all be reachable. That
call is the first smoke test of the endpoint, before anything is deployed.

Nothing is deployed here: creating the serving endpoint stays an explicit step
(see docs/databricks_supervisor.md).
"""

from __future__ import annotations

from typing import Any


def log_agent(
    config_path: str,
    models_dir: str,
    llm_endpoint: str,
    *,
    name: str = "skifer_agent",
    registered_model_name: str | None = None,
    warehouse_id: str | None = None,
    example_question: str | None = None,
    pip_requirements: list[str] | None = None,
) -> Any:
    """Log the agent and return MLflow's ``ModelInfo``.

    Args:
        config_path: ``config.yaml`` shipped with the model (artifact ``config``).
        models_dir: Semantic models directory shipped with the model (artifact ``models_dir``).
        llm_endpoint: Chat serving endpoint the agent calls. Stored in ``model_config``,
            so the deployed endpoint needs no ``DATABRICKS_LLM_MODEL``, and declared as a
            resource so Databricks knows the dependency.
        name: Model name inside the MLflow run.
        registered_model_name: Unity Catalog model to register, e.g. ``main.agents.skifer``.
        warehouse_id: SQL warehouse to declare as a resource, when the models use one.
        example_question: Question MLflow sends while logging; MLflow's default otherwise.
        pip_requirements: Overrides ``skifer[serving]==<installed version>``.
    """
    import mlflow
    from mlflow.models.resources import DatabricksServingEndpoint, DatabricksSQLWarehouse

    from skifer import __version__
    from skifer.serving.responses_agent import SkiferResponsesAgent

    if pip_requirements is None:
        if __version__ == "unknown":
            raise ValueError(
                "log_agent: the installed skifer version is unknown, so the serving "
                "requirement cannot be pinned. Pass pip_requirements explicitly."
            )
        pip_requirements = [f"skifer[serving]=={__version__}"]

    resources = [DatabricksServingEndpoint(endpoint_name=llm_endpoint)]
    if warehouse_id:
        resources.append(DatabricksSQLWarehouse(warehouse_id=warehouse_id))

    extra: dict[str, Any] = {}
    if example_question:
        extra["input_example"] = {"input": [{"role": "user", "content": example_question}]}

    return mlflow.pyfunc.log_model(
        name=name,
        python_model=SkiferResponsesAgent(),
        artifacts={"config": config_path, "models_dir": models_dir},
        model_config={"llm_endpoint": llm_endpoint},
        resources=resources,
        pip_requirements=pip_requirements,
        registered_model_name=registered_model_name,
        **extra,
    )
