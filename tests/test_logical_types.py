"""Tests for the closed set of verifiable contract logical types (Plan 40)."""

from __future__ import annotations

import pytest

from skifer.core.dialect import SUPPORTED_DIALECTS
from skifer.core.logical_types import (
    COVERED_DIALECTS,
    LOGICAL_TYPES,
    UnverifiableLogicalType,
    is_verifiable,
    matches,
    physical_types_for,
)


class TestClosedSet:

    def test_identifier_is_refused_rather_than_skipped(self):
        """The type documented in yaml_spec has no physical equivalent.

        This is the whole reason the set is closed: an identifier may be a
        string or a bigint, so any verdict on it would be invented. Refusing is
        the only honest answer, and refusing loudly is what keeps it from being
        silently dropped from coverage.
        """
        assert not is_verifiable("identifier")
        with pytest.raises(UnverifiableLogicalType) as excinfo:
            physical_types_for("identifier", "duckdb")
        assert "identifier" in str(excinfo.value)
        # The message must name the way out, not just the refusal.
        assert "string" in str(excinfo.value)

    def test_an_unmeasured_dialect_is_refused(self):
        """Snowflake can be transpiled to, but its type names were never observed.

        Guessing them would put an unverified mapping behind a check whose only
        job is to be trustworthy.
        """
        assert "snowflake" in SUPPORTED_DIALECTS
        assert "snowflake" not in COVERED_DIALECTS
        with pytest.raises(UnverifiableLogicalType):
            physical_types_for("string", "snowflake")

    def test_every_logical_type_is_mapped_on_every_covered_dialect(self):
        """Drift guard: a new type or dialect cannot land half-mapped.

        A missing entry would raise at evaluation time, inside a pipeline run,
        instead of here.
        """
        assert LOGICAL_TYPES, "the closed set must not be empty"
        assert COVERED_DIALECTS, "at least one dialect must be measured"
        for logical_type in LOGICAL_TYPES:
            for dialect in COVERED_DIALECTS:
                accepted = physical_types_for(logical_type, dialect)
                assert accepted, f"{logical_type} on {dialect} maps to nothing"


class TestMatching:

    @pytest.mark.parametrize(
        "logical_type,spark_type,duckdb_type",
        [
            ("string", "string", "VARCHAR"),
            ("integer", "int", "INTEGER"),
            ("long", "bigint", "BIGINT"),
            ("double", "double", "DOUBLE"),
            ("decimal", "decimal(10,2)", "DECIMAL(10,2)"),
            ("boolean", "boolean", "BOOLEAN"),
            ("date", "date", "DATE"),
            ("timestamp", "timestamp", "TIMESTAMP"),
        ],
    )
    def test_one_contract_holds_on_both_engines(self, logical_type, spark_type, duckdb_type):
        """These physical names were measured on a real Spark 4 and a real DuckDB."""
        assert matches(spark_type, physical_types_for(logical_type, "databricks"))
        assert matches(duckdb_type.lower(), physical_types_for(logical_type, "duckdb"))

    def test_a_parameterised_type_matches_its_base_name(self):
        assert matches("decimal(38,18)", ("decimal",))

    def test_matching_stops_at_a_parenthesis_boundary(self):
        """`int` must not match `interval`.

        A bare startswith would accept it, which is how a type check ends up
        agreeing with a column it never looked at properly.
        """
        assert not matches("interval", ("int",))
        assert not matches("integer", ("int",))

    def test_a_wrong_type_does_not_match(self):
        assert not matches("string", physical_types_for("double", "databricks"))
