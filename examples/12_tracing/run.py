"""Spans, attribute redaction, identity omission, and an exporter failure that changes nothing.

    python examples/12_tracing/run.py
"""

from types import SimpleNamespace

from skifer.observability.tracing import (
    InMemoryTracer,
    build_attribute_policy,
    configured_span_scope,
)

tracer = InMemoryTracer()
with tracer.start_span("skifer.pipeline.run", attributes={"run_id": "run-12"}) as root:
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

identity = "customer@example.com"
default_policy = build_attribute_policy(SimpleNamespace(), {})
# hmac without SKIFER_TRACING_HMAC_SECRET omits the identity instead of hashing it unkeyed.
hmac_policy = build_attribute_policy(SimpleNamespace(user_identity="hmac"), {})
print(f"Default user identity: mode={default_policy.user_identity} | recorded={default_policy.pseudonymize(identity)}")
print(f"HMAC without secret: recorded={hmac_policy.pseudonymize(identity)}")


class FailingExporter:
    failure = None

    def start_span(self, name, *, attributes=None):
        self.failure = RuntimeError("export unavailable")
        raise self.failure


exporter = FailingExporter()
with configured_span_scope(exporter, "skifer.pipeline.run"):
    business_result = 6 * 7
print(f"Exporter failure type: {type(exporter.failure).__name__}")
print(f"Business result despite exporter failure: {business_result}")
