"""
CatalogInspector — catalog-aware validation for BuilderAgent.

Provides table and column discovery backed by any Backend implementation.
Raises ``CatalogError`` with actionable messages and Levenshtein suggestions
when a requested table or column cannot be found.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from skifer.core.spark_backend import SparkBackend


# ---------------------------------------------------------------------------
# Exception
# ---------------------------------------------------------------------------

class CatalogError(Exception):
    """Raised when a table or column is not found in the catalog."""


# ---------------------------------------------------------------------------
# Levenshtein helper
# ---------------------------------------------------------------------------

def _levenshtein(a: str, b: str) -> int:
    """Compute edit distance between two strings."""
    if len(a) < len(b):
        return _levenshtein(b, a)
    if not b:
        return len(a)
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        curr = [i]
        for j, cb in enumerate(b, 1):
            curr.append(min(prev[j] + 1, curr[j - 1] + 1, prev[j - 1] + (ca != cb)))
        prev = curr
    return prev[-1]


def _top_suggestions(name: str, candidates: list[str], n: int = 3) -> list[str]:
    """Return the top-n closest candidates by Levenshtein distance."""
    if not candidates:
        return []
    scored = sorted(candidates, key=lambda c: _levenshtein(name.lower(), c.lower()))
    return scored[:n]


# ---------------------------------------------------------------------------
# CatalogInspector
# ---------------------------------------------------------------------------

class CatalogInspector:
    """
    Validates table and column names against the live catalog.

    Wraps a Backend instance to provide:
    - Table and column listing
    - FQN building (2-part or 3-part depending on catalog presence)
    - Validation with Levenshtein suggestions on mismatch

    Args:
        backend: SparkBackend (or a duck-typed test double).
        catalog: Default catalog name, or None for local/2-part FQN mode.
    """

    def __init__(self, backend: "SparkBackend", catalog: str | None = None):
        self._backend = backend
        self._catalog = catalog

    # ------------------------------------------------------------------
    # FQN helpers
    # ------------------------------------------------------------------

    def fqn(self, schema: str, table: str) -> str:
        """Build a fully-qualified table name using the backend's convention."""
        return self._backend.build_fqn(self._catalog, schema, table)

    def _parse_fqn(self, fqn_str: str) -> tuple[str | None, str, str]:
        """
        Parse a user-supplied FQN string into (catalog, schema, table).

        Accepts:
          - ``catalog.schema.table``   → 3-part
          - ``schema.table``           → 2-part (catalog falls back to self._catalog)
        """
        clean = fqn_str.replace("`", "").replace('"', "")
        parts = clean.split(".")
        if len(parts) == 3:
            return parts[0], parts[1], parts[2]
        if len(parts) == 2:
            return self._catalog, parts[0], parts[1]
        if len(parts) == 1:
            # Mode local — nom de table sans schéma (vues Spark, tests)
            return self._catalog, "", parts[0]
        raise ValueError(
            f"FQN mal formé : '{fqn_str}'. "
            "Format attendu : 'table', 'schema.table' ou 'catalog.schema.table'."
        )

    # ------------------------------------------------------------------
    # Discovery
    # ------------------------------------------------------------------

    def list_tables(self, schema: str) -> list[str]:
        """List all tables in *schema* (using the configured catalog)."""
        return self._backend.list_tables(schema, catalog=self._catalog)

    def list_columns(self, fqn_str: str) -> list[str]:
        """List all columns for the given FQN string."""
        cat, sch, tbl = self._parse_fqn(fqn_str)
        built = self._backend.build_fqn(cat, sch, tbl)
        return self._backend.list_columns(built)

    def list_schemas(self) -> list[str]:
        """Retourne la liste des schemas du catalog courant."""
        return self._backend.list_schemas(self._catalog)

    def describe_table(self, fqn_str: str) -> dict[str, str]:
        """
        Retourne {nom_colonne: type_sql} pour la table donnée.

        Retourne {} si le backend ne supporte pas list_column_types().
        """
        return self._backend.list_column_types(fqn_str)

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def validate_table(self, fqn_str: str) -> None:
        """
        Verify that *fqn_str* exists in the catalog.

        Raises:
            ValueError: If the FQN is malformed.
            CatalogError: If the table is not found.
        """
        cat, sch, tbl = self._parse_fqn(fqn_str)
        if not self._backend.table_exists(cat, sch, tbl):
            available = self._backend.list_tables(sch, catalog=cat)
            suggestions = _top_suggestions(tbl, available)
            msg = f"Table introuvable : '{fqn_str}'."
            if suggestions:
                msg += f" Suggestions : {suggestions}"
            elif available:
                msg += f" Tables disponibles dans '{sch}' : {available[:10]}"
            raise CatalogError(msg)

    def validate_columns(self, fqn_str: str, columns: list[str]) -> None:
        """
        Verify that all *columns* exist in the table identified by *fqn_str*.

        Raises:
            ValueError: If the FQN is malformed.
            CatalogError: If one or more columns are not found (lists suggestions).
        """
        real_cols = self.list_columns(fqn_str)
        if not real_cols:
            # Cannot validate — warn but do not block
            return

        real_lower = {c.lower(): c for c in real_cols}
        unknown = [c for c in columns if c.lower() not in real_lower]
        if unknown:
            messages = []
            for col in unknown:
                suggestions = _top_suggestions(col, real_cols)
                messages.append(
                    f"  - '{col}' introuvable" +
                    (f" — suggestions : {suggestions}" if suggestions else "")
                )
            raise CatalogError(
                f"Colonnes inconnues dans '{fqn_str}' :\n" + "\n".join(messages)
            )
