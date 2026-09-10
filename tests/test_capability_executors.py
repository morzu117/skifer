"""Contracts for the explicit capability executor allowlist."""

from __future__ import annotations

from pathlib import Path

import pytest

from skifer.capabilities import (
    CapabilityExecutorError,
    CapabilityExecutorRegistry,
    CapabilityResult,
)


def test_register_and_list_executor() -> None:
    name = "test_executor_registry_list"

    @CapabilityExecutorRegistry.register(name)
    def executor(arguments):
        return dict(arguments)

    assert CapabilityExecutorRegistry.get(name) is executor
    assert name in CapabilityExecutorRegistry.list_executors()


@pytest.mark.parametrize(
    "name",
    ["Uppercase", "pkg.executor", "module:executor", "with-dash", "_private", "1first"],
)
def test_registration_rejects_non_plain_names(name: str) -> None:
    with pytest.raises(CapabilityExecutorError, match="must match"):
        CapabilityExecutorRegistry.register(name)


def test_duplicate_registration_is_refused() -> None:
    name = "test_executor_registry_duplicate"

    @CapabilityExecutorRegistry.register(name)
    def first(arguments):
        return dict(arguments)

    with pytest.raises(CapabilityExecutorError, match="already registered"):

        @CapabilityExecutorRegistry.register(name)
        def second(arguments):
            return dict(arguments)


def test_registration_requires_a_callable() -> None:
    decorator = CapabilityExecutorRegistry.register("test_executor_registry_not_callable")
    with pytest.raises(CapabilityExecutorError, match="callable"):
        decorator(None)  # type: ignore[arg-type]


def test_registration_requires_boolean_dry_run_marker() -> None:
    with pytest.raises(CapabilityExecutorError, match="supports_dry_run"):
        CapabilityExecutorRegistry.register(
            "test_executor_invalid_dry_run", supports_dry_run="yes"  # type: ignore[arg-type]
        )


def test_registration_tracks_explicit_credential_marker() -> None:
    name = "test_executor_needs_credential"

    @CapabilityExecutorRegistry.register(name, needs_credential=True)
    def executor(arguments, lease):
        return {"ok": bool(arguments), "lease": bool(lease)}

    assert CapabilityExecutorRegistry.needs_credential(name)
    assert executor.needs_credential is True


def test_registration_requires_boolean_credential_marker() -> None:
    with pytest.raises(CapabilityExecutorError, match="needs_credential"):
        CapabilityExecutorRegistry.register(
            "test_executor_invalid_credential",
            needs_credential="yes",  # type: ignore[arg-type]
        )


def test_unknown_executor_is_refused_by_name() -> None:
    with pytest.raises(CapabilityExecutorError, match="missing_executor_name"):
        CapabilityExecutorRegistry.get("missing_executor_name")


def test_executor_module_has_no_dynamic_resolution_escape_hatch() -> None:
    source = (
        Path(__file__).parents[1]
        / "src"
        / "skifer"
        / "capabilities"
        / "executors.py"
    ).read_text(encoding="utf-8")
    forbidden = ("importlib", "__import__", "eval(", "exec(", "getattr(__")
    assert all(fragment not in source for fragment in forbidden)


def test_capability_result_is_copied_and_allowlisted() -> None:
    output = {"records": [{"id": 1}]}
    result = CapabilityResult(
        capability_id="inventory.lookup_record",
        capability_version="1.0.0",
        status="succeeded",
        output=output,
        error_type=None,
    )
    output["records"][0]["id"] = 2
    object.__setattr__(result, "future_private_field", "must not leak")

    assert result.to_dict() == {
        "capability_id": "inventory.lookup_record",
        "capability_version": "1.0.0",
        "status": "succeeded",
        "output": {"records": [{"id": 1}]},
        "error_type": None,
    }


def test_capability_result_enforces_class_name_only_failures() -> None:
    with pytest.raises(ValueError, match="class name"):
        CapabilityResult(
            capability_id="inventory.lookup_record",
            capability_version="1.0.0",
            status="failed",
            output={},
            error_type="RuntimeError: leaked message",
        )
