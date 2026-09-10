"""Record spans, inspect redaction, and isolate an exporter failure.

    python examples/12_tracing/run.py
"""

from types import SimpleNamespace

from skifer.observability.tracing import (
    InMemoryTracer,
    build_attribute_policy,
    configured_span_scope,
)


class FailingExporter:
    """Small exporter double whose span creation always fails."""

    def __init__(self) -> None:
        self.failure_type = None

    def start_span(self, name, *, attributes=None):
        try:
            raise RuntimeError("export unavailable")
        except RuntimeError as exc:
            self.failure_type = type(exc).__name__
            raise


def main() -> None:
    tracer = InMemoryTracer()
    with tracer.start_span(
        "skifer.pipeline.run", attributes={"run_id": "run-12"}
    ) as root:
        root.set_attribute("question", "Which customer spent the most?")
        with tracer.start_span("skifer.transform.execute") as child:
            child.set_attribute("status", "complete")
            child.set_attribute("sql_text", "SELECT * FROM private.orders")
            child.set_attribute("filter_value", "customer@example.com")

    print("Recorded span tree:")
    for span in tracer.spans:
        indent = "  " if span.parent_span_id else ""
        print(f"{indent}- {span.name} attributes={span.attributes}")

    print("Rejected attributes (key and type only):")
    for rejected in tracer.rejected_attributes:
        print(f"  key={rejected.key} | type={rejected.value_type}")

    default_policy = build_attribute_policy(SimpleNamespace(), {})
    hmac_without_secret = build_attribute_policy(
        SimpleNamespace(user_identity="hmac"), {}
    )
    identity = "customer@example.com"
    print(
        "Default user identity: "
        f"mode={default_policy.user_identity} | recorded={default_policy.pseudonymize(identity)}"
    )
    print(
        "HMAC without secret: "
        f"recorded={hmac_without_secret.pseudonymize(identity)}"
    )

    failing_exporter = FailingExporter()
    with configured_span_scope(failing_exporter, "skifer.pipeline.run"):
        business_result = 6 * 7
    print(f"Exporter failure type: {failing_exporter.failure_type}")
    print(f"Business result despite exporter failure: {business_result}")


if __name__ == "__main__":
    main()
