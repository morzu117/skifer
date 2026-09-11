"""Pure coverage audit tests."""

from __future__ import annotations

import json
from pathlib import Path

from skifer.observability.audit import METRIC_KEYS, MetricKey, audit_project


FULLY_COVERED = """\
data_product:
  id: sales.orders
  version: 1.0.0
  owner: sales-data
contract:
  output:
    order_id:
      description: Order identifier
      classification: internal
    amount:
      description: Order amount
      classification: confidential
tables:
  - name: silver.orders
    alias: ord
select_final:
  - [id, order_id]
  - [amount, amount]
"""


STRUCTURED_OWNER = """\
data_product:
  id: sales.orders
  version: 1.0.0
  owner:
    team: sales-data
contract:
  output:
    order_id:
      description: Order identifier
      classification: internal
tables:
  - name: silver.orders
    alias: ord
select_final:
  - [id, order_id]
"""


BARE_PIPELINE = """\
tables:
  - name: silver.orders
    alias: ord
select_final:
  - [amount, amount]
"""


def _write(path: Path, text: str) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return str(path)


def test_audit_all_covered(tmp_path):
    first = _write(tmp_path / "a.yaml", FULLY_COVERED)
    second = _write(tmp_path / "b.yaml", FULLY_COVERED)

    report = audit_project([first, second])

    assert report.total == 2
    assert report.parsed == 2
    assert report.errored == 0
    assert report.coverage == {
        "data_product": 100.0,
        "contract": 100.0,
        "structured_owner": 0.0,
        "field_descriptions": 100.0,
        "field_classification": 100.0,
    }
    assert report.overall_coverage_pct == 80.0


def test_audit_structured_owner_mapping(tmp_path):
    path = _write(tmp_path / "structured.yaml", STRUCTURED_OWNER)

    report = audit_project([path])

    assert report.coverage["structured_owner"] == 100.0
    assert report.pipelines[0].has_structured_owner is True


def test_audit_none_covered(tmp_path):
    first = _write(tmp_path / "a.yaml", BARE_PIPELINE)
    second = _write(tmp_path / "b.yaml", BARE_PIPELINE)

    report = audit_project([first, second])

    assert report.total == 2
    assert report.parsed == 2
    assert report.errored == 0
    assert report.coverage == {key: 0.0 for key in METRIC_KEYS}
    assert report.overall_coverage_pct == 0.0


def test_audit_partial_field_coverage(tmp_path):
    path = _write(
        tmp_path / "partial.yaml",
        """\
data_product: {id: sales.orders, version: 1.0.0}
contract:
  output:
    order_id: {description: Order identifier, classification: internal}
    amount: {description: Order amount}
    status: {classification: public}
tables:
  - name: silver.orders
    alias: ord
select_final:
  - [id, order_id]
  - [amount, amount]
  - [status, status]
""",
    )

    report = audit_project([path])
    pipeline = report.pipelines[0]

    assert pipeline.output_field_count == 3
    assert pipeline.described_field_count == 2
    assert pipeline.classified_field_count == 2
    assert pipeline.has_field_descriptions is False
    assert pipeline.has_field_classification is False


def test_audit_uses_example_fixture():
    root = Path(__file__).parents[1]
    path = str(root / "examples" / "02_quality_and_contract" / "gold_orders.yaml")

    report = audit_project([path])
    pipeline = report.pipelines[0]

    assert pipeline.has_data_product is True
    assert pipeline.has_contract is True
    assert pipeline.has_field_classification is False
    assert pipeline.has_field_descriptions is False


def test_audit_unparseable_counts_as_uncovered(tmp_path):
    good = _write(tmp_path / "good.yaml", FULLY_COVERED)
    bad = _write(
        tmp_path / "bad.yaml",
        """\
data_product: {id: sales.orders, version: 1.0.0}
contract:
  output:
    amount: {description: Amount, classification: internal}
tables:
  - name: silver.orders
    alias: ord
    filter:
      - region:badoperator:EMEA
select_final:
  - [amount, amount]
""",
    )

    report = audit_project([good, bad])
    failed = next(pipeline for pipeline in report.pipelines if pipeline.path == bad)

    assert failed.parsed is False
    assert failed.error is not None
    assert all(failed.metric(key) is False for key in METRIC_KEYS)
    assert report.errored == 1
    assert report.coverage["data_product"] == 50.0


def test_audit_templated_params_neutralized(tmp_path):
    path = _write(
        tmp_path / "templated.yaml",
        """\
data_product: {id: sales.orders, version: 1.0.0}
contract:
  output:
    amount: {description: Amount, classification: internal}
tables:
  - name: "{{ catalog }}.silver.orders"
    alias: ord
select_final:
  - [amount, amount]
""",
    )

    report = audit_project([path])

    assert report.pipelines[0].parsed is True


def test_audit_zero_pipelines():
    report = audit_project([])

    assert report.total == 0
    assert report.parsed == 0
    assert report.errored == 0
    assert report.coverage == {key: 0.0 for key in METRIC_KEYS}
    assert report.overall_coverage_pct == 0.0
    assert report.pipelines == ()


def test_audit_report_json_is_sorted_and_stable(tmp_path):
    first = _write(tmp_path / "a.yaml", FULLY_COVERED)
    second = _write(tmp_path / "b.yaml", BARE_PIPELINE)

    left = json.dumps(audit_project([first, second]).to_dict(), sort_keys=True)
    right = json.dumps(audit_project([second, first]).to_dict(), sort_keys=True)

    assert left == right
    assert left == json.dumps(json.loads(left), sort_keys=True)
    for value in audit_project([first, second]).coverage.values():
        assert value == round(value, 2)


def test_audit_deterministic_across_input_order(tmp_path):
    first = _write(tmp_path / "a.yaml", FULLY_COVERED)
    second = _write(tmp_path / "b.yaml", BARE_PIPELINE)

    assert audit_project([first, second]).to_dict() == audit_project([second, first]).to_dict()


def test_metric_keys_frozen():
    assert METRIC_KEYS == (
        MetricKey.DATA_PRODUCT,
        MetricKey.CONTRACT,
        MetricKey.STRUCTURED_OWNER,
        MetricKey.FIELD_DESCRIPTIONS,
        MetricKey.FIELD_CLASSIFICATION,
    )
