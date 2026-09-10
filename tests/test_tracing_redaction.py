"""Plan 29 slice 5.5 — redaction, limits and pseudonymisation canaries."""

from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest

from skifer.observability.tracing import (
    ALLOWED_ATTRIBUTE_KEYS,
    MAX_EVENTS_PER_SPAN,
    InMemoryTracer,
    TraceAttributePolicy,
    build_attribute_policy,
)


CANARIES = {
    "token": "ghp_TOKENCANARY1234567890",
    "email": "jean.dupont@corp.com",
    "pii": "FR7630006000011234567890189",
    "prompt": "Tu es un expert BI. Question: combien gagne Jean ?",
    "sql": "SELECT amount FROM gold.orders WHERE email = 'jean.dupont@corp.com'",
}


@pytest.mark.parametrize("label,value", sorted(CANARIES.items()))
def test_sensitive_attributes_never_survive_the_allowlist(label, value):
    # Each of these is attached under a plausible-looking key. An allowlist is
    # the only redaction that still holds when a later slice adds an attribute
    # without thinking about what it carries.
    tracer = InMemoryTracer()

    with tracer.start_span("skifer.semantic.query") as span:
        span.set_attribute(label, value)
        span.set_attribute(f"debug_{label}", value)

    recorded = repr([(s.name, s.attributes, s.events) for s in tracer.spans])
    assert value not in recorded
    assert tracer.spans[0].attributes == {}
    assert {rejected.key for rejected in tracer.rejected_attributes} == {
        label,
        f"debug_{label}",
    }


def test_rejected_attribute_never_records_the_value_it_refused():
    tracer = InMemoryTracer()

    with tracer.start_span("skifer.sql.execute") as span:
        span.set_attribute("sql_text", CANARIES["sql"])

    assert CANARIES["sql"] not in repr(tracer.rejected_attributes)


def test_allowlisted_attributes_still_pass_through():
    tracer = InMemoryTracer()

    with tracer.start_span("skifer.sql.execute") as span:
        span.set_attribute("sql_hash", "sha256:v1:abc")
        span.set_attribute("run_id", "r-1")

    assert tracer.spans[0].attributes == {"sql_hash": "sha256:v1:abc", "run_id": "r-1"}


def test_attribute_values_are_truncated_to_a_bounded_length():
    policy = TraceAttributePolicy(max_value_length=16)
    tracer = InMemoryTracer(attribute_policy=policy)

    with tracer.start_span("skifer.pipeline.run") as span:
        span.set_attribute("run_id", "x" * 200)

    assert tracer.spans[0].attributes["run_id"] == "x" * 16


def test_attribute_count_is_capped_per_span():
    policy = TraceAttributePolicy(max_attributes=2)
    tracer = InMemoryTracer(attribute_policy=policy)

    with tracer.start_span("skifer.pipeline.run") as span:
        for key in ("run_id", "environment", "model_key", "decision"):
            span.set_attribute(key, "v")

    assert len(tracer.spans[0].attributes) == 2


def test_event_count_is_capped_per_span():
    # An unbounded event list is a leak in-process and an oversized payload at
    # the exporter.
    tracer = InMemoryTracer()

    with tracer.start_span("skifer.pipeline.run") as span:
        for index in range(MAX_EVENTS_PER_SPAN + 25):
            span.add_event(f"event-{index}")

    assert len(tracer.spans[0].events) == MAX_EVENTS_PER_SPAN


def test_hmac_without_a_secret_omits_the_identity_rather_than_hashing_it(caplog):
    # An unkeyed digest of an email or an employee id is reversible by anyone
    # who can guess the input — a rainbow table over a company directory is
    # small. Omitting is the only honest option.
    policy = build_attribute_policy(SimpleNamespace(user_identity="hmac"), {})

    with caplog.at_level(logging.WARNING):
        result = policy.pseudonymize(CANARIES["email"])

    assert result is None


def test_hmac_with_a_secret_is_stable_and_hides_the_input():
    policy = build_attribute_policy(
        SimpleNamespace(user_identity="hmac"),
        {"SKIFER_TRACING_HMAC_SECRET": "s3cr3t"},
    )

    first = policy.pseudonymize(CANARIES["email"])
    second = policy.pseudonymize(CANARIES["email"])

    assert first == second
    assert CANARIES["email"] not in first
    assert policy.pseudonymize("someone.else@corp.com") != first


def test_omit_is_the_default_identity_mode():
    policy = build_attribute_policy(SimpleNamespace(), {})

    assert policy.user_identity == "omit"
    assert policy.pseudonymize(CANARIES["email"]) is None


def test_the_allowlist_matches_the_documented_taxonomy():
    # Guards against a key being added to the code without being considered.
    assert "sql_text" not in ALLOWED_ATTRIBUTE_KEYS
    assert "question" not in ALLOWED_ATTRIBUTE_KEYS
    assert "prompt" not in ALLOWED_ATTRIBUTE_KEYS
    assert {"run_id", "model_key", "decision", "sql_hash"} <= ALLOWED_ATTRIBUTE_KEYS
