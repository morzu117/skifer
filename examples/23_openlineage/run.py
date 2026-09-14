"""Build OpenLineage RunEvents from a pipeline schema and emit them, redaction-safe.

    python examples/23_openlineage/run.py
"""

import json
import warnings
from datetime import datetime, timezone

from skifer.core.schema_loader import parse_schema
from skifer.observability.checks import CheckResult, CheckStatus, NullCheck
from skifer.observability.metadata_index import index_schema
from skifer.observability.openlineage import HttpEmitter, InMemoryEmitter, build_run_event

SECRET = "sk-live-SECRET123ABC"

PIPELINE_YAML = """
data_product:
  id: sales.customer_orders
  version: 1.0.0
  owner: data-platform
contract:
  grain: [order_id]
  output:
    order_id:
      logical_type: identifier
      classification: internal
      description: Order identifier
    customer_email:
      logical_type: string
      classification: pii
      description: Customer email address
    amount_eur:
      logical_type: double
      classification: internal
      description: Order amount in EUR
tables:
  - name: silver.orders
    alias: ord
  - name: silver.customers
    alias: cust
join:
  - table_from: [ord, customer_id]
    table_to: [cust, id]
    type: left
select_final:
  - [ord.id, order_id]
  - [cust.email, customer_email]
  - [ord.amount, amount_eur, [cast:double]]
sink:
  type: delta
  schema: gold
  table: customer_orders
"""

RUN_ID = "0b6a5f2a-6e9b-4c9d-8b1a-2f7e5d3c1a90"
EVENT_TIME = datetime(2026, 9, 13, 10, 0, 0, tzinfo=timezone.utc)
JOB_NAMESPACE = "skifer"
DATASET_NAMESPACE = "unitycatalog://example-workspace"


def _check_results() -> list[CheckResult]:
    """A CheckResult whose actual_value/message carry an obvious secret string.

    build_run_event only ever keeps assertion/success/severity/column from a
    CheckResult — this is what proves the secret never reaches an event.
    """
    failing = CheckResult(
        contract=NullCheck(table="gold.customer_orders", column="customer_email"),
        status=CheckStatus.FAIL,
        actual_value=f"3 nulls; leaked credential {SECRET}",
        expected_value=0,
        message=f"null check failed, credential {SECRET} found in source logs",
        severity="critical",
        timestamp=EVENT_TIME,
    )
    passing = CheckResult(
        contract=NullCheck(table="gold.customer_orders", column="order_id"),
        status=CheckStatus.PASS,
        actual_value=0,
        expected_value=0,
        message="ok",
        severity="warning",
        timestamp=EVENT_TIME,
    )
    return [failing, passing]


def main() -> None:
    schema_dict = parse_schema(PIPELINE_YAML)
    record = index_schema(schema_dict, "examples/23_openlineage/pipeline.yaml")

    start_event = build_run_event(
        event_type="START",
        run_id=RUN_ID,
        event_time=EVENT_TIME,
        record=record,
        job_namespace=JOB_NAMESPACE,
        dataset_namespace=DATASET_NAMESPACE,
    )
    complete_event = build_run_event(
        event_type="COMPLETE",
        run_id=RUN_ID,
        event_time=EVENT_TIME,
        record=record,
        job_namespace=JOB_NAMESPACE,
        dataset_namespace=DATASET_NAMESPACE,
        check_results=_check_results(),
        certification_status="CERTIFIED",
    )

    emitter = InMemoryEmitter()
    emitter.emit(start_event)
    emitter.emit(complete_event)

    print("Event types emitted, in order:")
    for event in emitter.events:
        print(f"  {event['eventType']}")

    print(f"Job name: {complete_event['job']['name']}")

    input_names = sorted(item["name"] for item in complete_event["inputs"])
    print("Input dataset names:")
    for name in input_names:
        print(f"  {name}")

    output = complete_event["outputs"][0]
    print(f"Output dataset name: {output['name']}")

    amount_lineage = output["facets"]["columnLineage"]["fields"]["amount_eur"]["inputFields"][0]
    transformation = amount_lineage["transformations"][0]
    print(
        "Column lineage for amount_eur: "
        f"source={amount_lineage['name']}.{amount_lineage['field']} "
        f"type={transformation['type']} subtype={transformation['subtype']} "
        f"description={transformation.get('description')}"
    )

    print("Data quality assertions:")
    for assertion in output["facets"]["dataQualityAssertions"]["assertions"]:
        print(
            f"  assertion={assertion['assertion']} success={assertion['success']} "
            f"severity={assertion['severity']} column={assertion.get('column')}"
        )

    print("Skifer facet classifications:")
    for column, classification in sorted(output["facets"]["skifer"]["classifications"].items()):
        print(f"  {column}: {classification}")

    all_events_text = json.dumps(emitter.events)
    print(f"Secret present in events: {SECRET in all_events_text}")

    def _unreachable_catalog_opener(request, timeout=None):
        raise ConnectionRefusedError("simulated unreachable catalog")

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        unreachable_emitter = HttpEmitter(
            "http://catalog.example.internal",
            timeout_seconds=2.0,
            opener=_unreachable_catalog_opener,
        )
        unreachable_emitter.emit(complete_event)

    warning = caught[0]
    exception_name = str(warning.message).rsplit(": ", 1)[-1]
    print(f"Unreachable emitter warning category: {warning.category.__name__}")
    print(f"Unreachable emitter reported exception: {exception_name}")


if __name__ == "__main__":
    main()
