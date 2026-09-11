"""Strict conversion of service results to JSON-native values."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
import math
from typing import Any

from skifer.services.context import SerializationError


def to_json_value(value: Any, field_name: str) -> Any:
    """Convert one value to bounded JSON-native data or refuse it explicitly."""
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise SerializationError(f"{field_name} must be a finite JSON number.")
        return value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, (list, tuple)):
        return [
            to_json_value(item, f"{field_name}[{index}]")
            for index, item in enumerate(value)
        ]
    if isinstance(value, dict) and all(isinstance(key, str) for key in value):
        return {
            key: to_json_value(item, f"{field_name}.{key}")
            for key, item in value.items()
        }
    raise SerializationError(
        f"{field_name} has unsupported type '{type(value).__name__}'."
    )


def row_to_json(row: Any, index: int) -> dict[str, Any]:
    """Convert a mapping-like query row to JSON-native data."""
    if isinstance(row, dict):
        values = row
    else:
        converter = getattr(row, "asDict", None)
        if converter is None or not callable(converter):
            raise SerializationError(f"Query row {index} is not mapping-like.")
        values = converter(recursive=True)
    if not isinstance(values, dict) or not all(isinstance(key, str) for key in values):
        raise SerializationError(f"Query row {index} must have string column names.")
    return {
        key: to_json_value(value, f"rows[{index}].{key}")
        for key, value in values.items()
    }


__all__ = ["to_json_value", "row_to_json"]
