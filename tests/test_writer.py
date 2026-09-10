import pytest

from skifer.core.writer import _quote_namespace


@pytest.mark.parametrize(
    ("namespace", "expected"),
    [
        ("silver_jdoe", "`silver_jdoe`"),
        (" default . silver_jdoe ", "`default`.`silver_jdoe`"),
        ("`default`.`silver_jdoe`", "`default`.`silver_jdoe`"),
    ],
)
def test_quote_namespace_quotes_each_part(namespace, expected):
    assert _quote_namespace(namespace) == expected


def test_quote_namespace_rejects_empty_namespace():
    with pytest.raises(ValueError, match="Schema name cannot be empty"):
        _quote_namespace("   ")
