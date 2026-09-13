"""Tests for observability contract extraction."""
from skifer.core.schema_loader import parse_schema
from skifer.observability.checks import LoadFreshnessCheck
from skifer.observability.contracts import ContractExtractor


def test_sla_derives_load_freshness_check():
    schema = parse_schema(
        """
data_product: {id: sales.orders, version: 1.0.0}
contract:
  sla:
    max_latency: 12h
  output:
    order_id: {}
tables:
  - name: silver.orders
select_final:
  - [order_id, order_id]
"""
    )

    contracts = ContractExtractor().extract(schema)

    load_checks = [check for check in contracts if isinstance(check, LoadFreshnessCheck)]
    assert load_checks == [
        LoadFreshnessCheck(
            table="silver.orders",
            max_delay="12h",
            severity="critical",
        )
    ]
