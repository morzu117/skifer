"""
The MLflow exporter's trace destinations, against the real MLflow (Plan 41).

``test_tracing_exporters.py`` replaces ``mlflow.entities.trace_location`` with a fake,
so it cannot tell whether the names the exporter uses still exist. This file can.
Skipped unless MLflow is installed; the CI job ``serving`` installs it and runs it.
"""
from __future__ import annotations

import pytest

mlflow = pytest.importorskip("mlflow")
trace_location = pytest.importorskip("mlflow.entities.trace_location")

from skifer.observability.tracing_exporters import _mlflow_destination  # noqa: E402


@pytest.mark.parametrize(
    "location, prefix",
    [("main.traces", None), ("main.traces.skifer", "skifer")],
)
def test_unity_catalog_location_builds_a_real_destination(location, prefix):
    destination = _mlflow_destination(mlflow, location)

    assert isinstance(destination, trace_location.TraceLocationBase)
    assert (destination.catalog_name, destination.schema_name) == ("main", "traces")
    assert destination.table_prefix == prefix


def test_experiment_id_builds_a_real_destination():
    destination = _mlflow_destination(mlflow, "123456")

    assert isinstance(destination, trace_location.MlflowExperimentLocation)
    assert destination.experiment_id == "123456"
