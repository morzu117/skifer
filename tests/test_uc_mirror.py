"""Unity Catalog certification mirror tests."""
from skifer.observability.certification_store import Certification
from skifer.observability.uc_mirror import mirror_certification


def _certification() -> Certification:
    return Certification(
        dataset="sales.orders",
        consumer_class="agent",
        status="CERTIFIED",
        contract_version="1.0.0",
        definition_hash="hash",
        certified_at=None,
    )


class _Backend:
    def __init__(self):
        self.sql: list[str] = []

    def execute_sql(self, sql: str) -> None:
        self.sql.append(sql)


def test_uc_mirror_emits_owner_and_domain_tags():
    backend = _Backend()

    result = mirror_certification(
        backend,
        "main.gold.orders",
        _certification(),
        owner="sales-data",
        domain="commerce",
    )

    assert result.status == "SYNCED"
    assert "'skifer_owner' = 'sales-data'" in backend.sql[0]
    assert "'skifer_domain' = 'commerce'" in backend.sql[0]


def test_uc_mirror_omits_domain_tag_when_absent():
    backend = _Backend()

    result = mirror_certification(
        backend,
        "main.gold.orders",
        _certification(),
        owner="sales-data",
    )

    assert result.status == "SYNCED"
    assert "'skifer_owner' = 'sales-data'" in backend.sql[0]
    assert "skifer_domain" not in backend.sql[0]
