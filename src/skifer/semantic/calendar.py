"""Versioned, declarative semantic calendar definitions."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import difflib
import re
from typing import Any


_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


@dataclass(frozen=True)
class PeriodDef:
    """One inclusive named period in a versioned calendar."""

    name: str
    start: date
    end: date


@dataclass(frozen=True)
class CalendarDef:
    """A named and versioned collection of deterministic periods."""

    key: str
    version: str
    periods: tuple[PeriodDef, ...]


def parse_iso_date(value: Any, label: str = "date") -> date:
    """Parse a strict YYYY-MM-DD string or fail closed."""
    if not isinstance(value, str) or _ISO_DATE.fullmatch(value) is None:
        raise ValueError(f"Invalid {label} {value!r}; expected YYYY-MM-DD.")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"Invalid {label} {value!r}; expected a real ISO date.") from exc


def parse_calendar(payload: Any) -> CalendarDef:
    """Parse and validate one standalone calendar YAML payload."""
    if not isinstance(payload, dict):
        raise ValueError("Calendar definition must be a mapping.")

    key = payload.get("key")
    if not isinstance(key, str) or _SAFE_IDENTIFIER.fullmatch(key) is None:
        raise ValueError("Calendar key must be a non-empty SQL-safe identifier.")

    version = payload.get("version")
    if not isinstance(version, str) or not version.strip():
        raise ValueError(f"Calendar '{key}' version must be a non-empty string.")

    raw_periods = payload.get("periods")
    if not isinstance(raw_periods, list) or not raw_periods:
        raise ValueError(f"Calendar '{key}' periods must be a non-empty list of mappings.")

    periods: list[PeriodDef] = []
    seen: set[str] = set()
    for index, raw_period in enumerate(raw_periods):
        if not isinstance(raw_period, dict):
            raise ValueError(
                f"Calendar '{key}' period #{index} must be a mapping, got "
                f"{type(raw_period).__name__}."
            )
        name = raw_period.get("name")
        if not isinstance(name, str) or _SAFE_IDENTIFIER.fullmatch(name) is None:
            raise ValueError(
                f"Calendar '{key}' period #{index} name must be a non-empty "
                "SQL-safe identifier."
            )
        if name in seen:
            raise ValueError(f"Calendar '{key}' has duplicate period name '{name}'.")
        seen.add(name)

        start = parse_iso_date(raw_period.get("start"), f"start date for period '{name}'")
        end = parse_iso_date(raw_period.get("end"), f"end date for period '{name}'")
        if start > end:
            raise ValueError(
                f"Calendar '{key}' period '{name}' has start after end."
            )
        periods.append(PeriodDef(name=name, start=start, end=end))

    return CalendarDef(key=key, version=version, periods=tuple(periods))


def resolve_period(calendar: CalendarDef, period_name: str) -> PeriodDef:
    """Resolve one exact period name without guessing or fallback."""
    for period in calendar.periods:
        if period.name == period_name:
            return period
    available = sorted(period.name for period in calendar.periods)
    suggestions = difflib.get_close_matches(period_name, available, n=3, cutoff=0.5)
    hint = f" Did you mean: {suggestions}?" if suggestions else ""
    raise ValueError(
        f"Period '{period_name}' is not declared in calendar '{calendar.key}'."
        f"{hint} Available: {available}"
    )
