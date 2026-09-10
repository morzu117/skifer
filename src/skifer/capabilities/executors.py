"""Explicit allowlist of callable capability executors and safe results."""

from __future__ import annotations

from dataclasses import dataclass
import math
import re
from typing import Any, Callable, Literal, Mapping


_EXECUTOR_NAME = re.compile(r"^[a-z][a-z0-9_]*$")


class CapabilityExecutorError(RuntimeError):
    """A capability executor cannot be selected or invoked safely."""


def _copy_json_safe(value: Any, *, path: str = "output") -> Any:
    """Copy a value while accepting only JSON-native, finite data."""
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if math.isfinite(value):
            return value
        raise TypeError(f"{path} contains a non-finite number.")
    if isinstance(value, list):
        return [
            _copy_json_safe(item, path=f"{path}[{index}]")
            for index, item in enumerate(value)
        ]
    if isinstance(value, Mapping):
        copied: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError(f"{path} contains a non-string key.")
            copied[key] = _copy_json_safe(item, path=f"{path}.{key}")
        return copied
    raise TypeError(f"{path} contains unsupported type '{type(value).__name__}'.")


@dataclass(frozen=True)
class CapabilityResult:
    """Allowlisted result returned across the capability execution boundary."""

    capability_id: str
    capability_version: str
    status: Literal["succeeded", "failed"]
    output: Mapping[str, Any]
    error_type: str | None

    def __post_init__(self) -> None:
        if self.status not in {"succeeded", "failed"}:
            raise ValueError("Capability result status must be 'succeeded' or 'failed'.")
        if not isinstance(self.output, Mapping):
            raise TypeError("Capability result output must be a mapping.")
        if self.status == "succeeded" and self.error_type is not None:
            raise ValueError("A succeeded capability result cannot carry an error type.")
        if self.status == "failed" and (
            not isinstance(self.error_type, str)
            or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", self.error_type) is None
        ):
            raise ValueError("A failed capability result requires an exception class name.")
        copied = _copy_json_safe(self.output)
        object.__setattr__(self, "output", copied)

    def to_dict(self) -> dict[str, Any]:
        """Serialize only public result fields, field by field."""
        return {
            "capability_id": self.capability_id,
            "capability_version": self.capability_version,
            "status": self.status,
            "output": _copy_json_safe(dict(self.output)),
            "error_type": self.error_type,
        }


class CapabilityExecutorRegistry:
    """Process-local allowlist of explicitly registered capability callables."""

    _executors: dict[str, Callable[[Mapping[str, Any]], Any]] = {}
    _dry_run_executors: set[str] = set()
    _credential_executors: set[str] = set()
    _idempotency_lookups: dict[str, Callable[[Mapping[str, Any]], Any]] = {}

    @classmethod
    def register(
        cls,
        name: str,
        *,
        supports_dry_run: bool = False,
        needs_credential: bool = False,
        lookup: Callable[[Mapping[str, Any]], Any] | None = None,
    ) -> Callable:
        """Return a decorator that registers one executor under a plain name."""
        if not isinstance(name, str) or _EXECUTOR_NAME.fullmatch(name) is None:
            raise CapabilityExecutorError(
                "Capability executor name must match ^[a-z][a-z0-9_]*$."
            )
        if not isinstance(supports_dry_run, bool):
            raise CapabilityExecutorError("supports_dry_run must be a boolean.")
        if not isinstance(needs_credential, bool):
            raise CapabilityExecutorError("needs_credential must be a boolean.")
        if lookup is not None and not callable(lookup):
            raise CapabilityExecutorError("lookup must be callable or None.")

        def decorator(func: Callable[[Mapping[str, Any]], Any]) -> Callable:
            if not callable(func):
                raise CapabilityExecutorError(
                    f"Capability executor '{name}' must be callable."
                )
            if name in cls._executors:
                raise CapabilityExecutorError(
                    f"Capability executor '{name}' is already registered."
                )
            cls._executors[name] = func
            if supports_dry_run:
                cls._dry_run_executors.add(name)
            if needs_credential:
                cls._credential_executors.add(name)
            if lookup is not None:
                cls._idempotency_lookups[name] = lookup
            # This is metadata only. Shadow dispatch checks it before making any
            # call; it never probes an external adapter to discover support.
            setattr(func, "supports_dry_run", supports_dry_run)
            setattr(func, "needs_credential", needs_credential)
            setattr(func, "lookup", lookup)
            return func

        return decorator

    @classmethod
    def get(cls, name: str) -> Callable[[Mapping[str, Any]], Any]:
        """Return an explicitly registered executor or fail closed."""
        try:
            return cls._executors[name]
        except (KeyError, TypeError) as exc:
            raise CapabilityExecutorError(
                f"Capability executor '{name}' is not registered."
            ) from exc

    @classmethod
    def list_executors(cls) -> list[str]:
        """Return registered names in deterministic order."""
        return sorted(cls._executors)

    @classmethod
    def supports_dry_run(cls, name: str) -> bool:
        """Return explicitly registered dry-run support without calling the executor."""
        cls.get(name)
        return name in cls._dry_run_executors

    @classmethod
    def needs_credential(cls, name: str) -> bool:
        """Return whether the executor explicitly accepts a credential lease."""
        cls.get(name)
        return name in cls._credential_executors

    @classmethod
    def idempotency_lookup(
        cls, name: str
    ) -> Callable[[Mapping[str, Any]], Any] | None:
        """Return only the lookup explicitly declared when the executor was registered."""
        cls.get(name)
        return cls._idempotency_lookups.get(name)
