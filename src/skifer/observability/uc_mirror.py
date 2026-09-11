"""Best-effort, non-sensitive Unity Catalog certification projection."""
from __future__ import annotations

from dataclasses import dataclass

from skifer.core.sql_compiler import quote_fqn, sql_literal
from skifer.observability.certification_store import Certification

_ALLOWED = frozenset(
    {"skifer_owner", "skifer_domain", "contract_version", "certification", "definition_hash"}
)


@dataclass(frozen=True)
class UcSyncResult:
    status: str
    error: str | None = None


def mirror_certification(
    backend,
    table_fqn: str,
    certification: Certification,
    owner: str | None = None,
    domain: str | None = None,
) -> UcSyncResult:
    """Write only allowlisted, coarse metadata; permission errors stay local."""
    tags = {
        "skifer_owner": owner,
        "skifer_domain": domain,
        "contract_version": certification.contract_version,
        "certification": certification.status,
        "definition_hash": certification.definition_hash,
    }
    try:
        assignments = ", ".join(f"{sql_literal(k)} = {sql_literal(v)}" for k, v in tags.items() if v is not None)
        backend.execute_sql(f"ALTER TABLE {quote_fqn(table_fqn)} SET TAGS ({assignments})")
        return UcSyncResult("SYNCED")
    except Exception as exc:
        return UcSyncResult("SYNC_ERROR", str(exc))
