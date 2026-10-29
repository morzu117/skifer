# Databricks notebook source
# MAGIC %md
# MAGIC # Metric candidates — arbitration
# MAGIC
# MAGIC Review the candidates waiting for a human (`needs_review`, `under_review`), then
# MAGIC record one decision. This notebook holds **no decision logic**: every check — state,
# MAGIC staleness, waivable reasons, `O_EXCL` drafts — lives in `CandidateService`.
# MAGIC
# MAGIC Fill `candidate_id` and `decision` to decide; leave them empty to only look.

# COMMAND ----------

import json
import os

from skifer import SkiferEngine
from skifer.candidates.service import CandidateError, build_candidate_service
from skifer.core.environment import get_clean_username
from skifer.observability.certification_store import default_certification_store
from skifer.semantic.semantic import SemanticEngine

dbutils.widgets.text("config_path", "")
dbutils.widgets.text("models_dir", "semantic_models")
dbutils.widgets.text("candidate_id", "")
dbutils.widgets.dropdown("decision", "", ["", "approve", "reject", "promote", "review", "demote"])
dbutils.widgets.text("reason", "")
dbutils.widgets.text("replaced_by", "")
dbutils.widgets.dropdown("show_drift", "", ["", "yes"])

engine = SkiferEngine(spark=spark, config_path=dbutils.widgets.get("config_path") or None)
# Absolute: SemanticEngine reads a relative models_dir against the installed package.
semantic = SemanticEngine(engine, models_dir=os.path.abspath(dbutils.widgets.get("models_dir")),
                          certification_store=default_certification_store(engine))
service = build_candidate_service(semantic)


def rows(records):
    """Flat text columns, so ``display`` renders them whether or not a Spark session is there."""
    flat = [{key: value if isinstance(value, (str, int, float)) or value is None
             else json.dumps(value, default=str) for key, value in record.items()}
            for record in records]
    return spark.createDataFrame(flat) if spark is not None and flat else flat


# COMMAND ----------

display(rows([view.to_dict() for view in service.list(state="needs_review")]))
display(rows([view.to_dict() for view in service.list(state="under_review")]))

# COMMAND ----------

candidate_id = dbutils.widgets.get("candidate_id").strip()
decision = dbutils.widgets.get("decision").strip()
if candidate_id and decision:
    options = {"decider": get_clean_username(spark=spark, dbutils=dbutils),
               "reason": dbutils.widgets.get("reason")}
    if decision == "demote" and dbutils.widgets.get("replaced_by").strip():
        options["replaced_by"] = dbutils.widgets.get("replaced_by").strip()
    decide = {"approve": service.approve, "reject": service.reject, "promote": service.promote,
              "review": service.review, "demote": service.demote}.get(decision)
    try:
        if decide is None:
            raise CandidateError(f"Unknown decision {decision!r}.")
        outcome = decide(candidate_id, **options)
        display(rows([outcome if isinstance(outcome, dict) else outcome.to_dict()]))
    except CandidateError as exc:
        print(f"{type(exc).__name__}: {exc}")

# COMMAND ----------

if candidate_id:
    try:
        display(rows(service.history(candidate_id)))
    except CandidateError as exc:
        print(f"{type(exc).__name__}: {exc}")

# COMMAND ----------

# MAGIC %md
# MAGIC Optional: the usage drift signals of the last 7 days (`show_drift = yes`). Observation
# MAGIC only — a signal never changes a candidate's state; the decision above stays human.

# COMMAND ----------

if dbutils.widgets.get("show_drift") == "yes":
    import contextlib
    import io
    from argparse import Namespace

    from skifer.cli import run_candidates_drift

    drift_output = io.StringIO()
    with contextlib.redirect_stdout(drift_output):
        run_candidates_drift(Namespace(
            since=None, until=None, json=True, environment=None,
            usage_store=".skifer_adaptive.db", evaluations_dir=".skifer_evaluations",
            config=dbutils.widgets.get("config_path") or None,
            models_dir=os.path.abspath(dbutils.widgets.get("models_dir"))))
    display(rows(json.loads(drift_output.getvalue())["signals"]))
