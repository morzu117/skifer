"""Tests for strict JSON-native service serialization."""

from datetime import date
from decimal import Decimal

import pytest

from skifer.services.serialization import row_to_json, to_json_value
from skifer.services.context import SerializationError


def test_to_json_value_finite_float_ok_and_nan_rejected():
    assert to_json_value(1.5, "f") == 1.5
    with pytest.raises(SerializationError):
        to_json_value(float("nan"), "f")


def test_to_json_value_decimal_and_datetime():
    assert to_json_value(Decimal("1.20"), "amount") == "1.20"
    assert to_json_value(date(2026, 9, 11), "day") == "2026-09-11"


def test_row_to_json_requires_string_keys():
    with pytest.raises(SerializationError):
        row_to_json({1: "value"}, 0)


def test_row_to_json_uses_asdict_recursive():
    class Row:
        def asDict(self, *, recursive):
            assert recursive is True
            return {"amount": Decimal("1.20"), "values": (1, 2)}

    assert row_to_json(Row(), 3) == {"amount": "1.20", "values": [1, 2]}
