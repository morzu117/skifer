"""Build OpenLineage RunEvents from a pipeline schema and emit them without leaking check values.

    python examples/23_openlineage/run.py
"""

import json
import warnings
from datetime import datetime, timezone
from pathlib import Path

from skifer import load_schema
from skifer.observability.checks import CheckResult, CheckStatus, NullCheck
from skifer.observability.metadata_index import index_schema
from skifer.observability.openlineage import HttpEmitter, InMemoryEmitter, build_run_event

SECRET = "sk-live-SECRET123ABC"
EVENT_TIME = datetime(2026, 9, 13, 10, 0, 0, tzinfo=timezone.utc)

# The same DatasetRecord the metadata registry persists, built without Spark.
record = index_schema(
    load_schema(str(Path(__file__).parent / "pipeline.yaml")), "examples/23_openlineage/pipeline.yaml"
)
event = dict(
    run_id="0b6a5f2a-6e9b-4c9d-8b1a-2f7e5d3c1a90",
    event_time=EVENT_TIME,
    record=record,
    job_namespace="skifer",
    dataset_namespace="unitycatalog://example-workspace",
)

# Both check results carry a secret in actual_value/message; events only keep allowlisted fields.
checks = [
    CheckResult(
        contract=NullCheck(table="gold.customer_orders", column="customer_email"),
        status=CheckStatus.FAIL,
        actual_value=f"3 nulls; leaked credential {SECRET}",
        expected_value=0,
        message=f"null check failed, credential {SECRET} found in source logs",
        severity="critical",
        timestamp=EVENT_TIME,
    ),
    CheckResult(
        contract=NullCheck(table="gold.customer_orders", column="order_id"),
        status=CheckStatus.PASS,
        actual_value=0,
        expected_value=0,
        message="ok",
        severity="warning",
        timestamp=EVENT_TIME,
    ),
]

emitter = InMemoryEmitter()
emitter.emit(build_run_event(event_type="START", **event))
complete = build_run_event(event_type="COMPLETE", check_results=checks, certification_status="CERTIFIED", **event)
emitter.emit(complete)

print("Event types emitted, in order:")
for emitted in emitter.events:
    print(f"  {emitted['eventType']}")
print(f"Job name: {complete['job']['name']}")
print("Input dataset names:")
for name in sorted(item["name"] for item in complete["inputs"]):
    print(f"  {name}")

output = complete["outputs"][0]
print(f"Output dataset name: {output['name']}")
amount = output["facets"]["columnLineage"]["fields"]["amount_eur"]["inputFields"][0]
step = amount["transformations"][0]
print(
    f"Column lineage for amount_eur: source={amount['name']}.{amount['field']} "
    f"type={step['type']} subtype={step['subtype']} description={step.get('description')}"
)

print("Data quality assertions:")
for a in output["facets"]["dataQualityAssertions"]["assertions"]:
    print(f"  assertion={a['assertion']} success={a['success']} severity={a['severity']} column={a.get('column')}")
print("Skifer facet classifications:")
for column, classification in sorted(output["facets"]["skifer"]["classifications"].items()):
    print(f"  {column}: {classification}")
print(f"Secret present in events: {SECRET in json.dumps(emitter.events)}")


def unreachable_catalog(request, timeout=None):
    raise ConnectionRefusedError("simulated unreachable catalog")


# emit() never raises: a failed export becomes one warning naming only the exception class.
with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter("always")
    HttpEmitter("http://catalog.example.internal", timeout_seconds=2.0, opener=unreachable_catalog).emit(complete)
print(f"Unreachable emitter warning category: {caught[0].category.__name__}")
print(f"Unreachable emitter reported exception: {str(caught[0].message).rsplit(': ', 1)[-1]}")
