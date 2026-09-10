"""
SessionHistory — logging automatique de chaque interaction GenBIAgent.

Chaque appel à GenBIAgent.ask() est loggué dans un HistoryEntry.
La session peut être exportée en PDF (via HistoryExporter) ou en JSON.

Extensions Phase B :
- session_id  : uuid4 généré automatiquement si non fourni
- created_at  : datetime.datetime (plus isoformat string)
- ttl_days    : durée de vie de la session (défaut 15 jours)
- is_expired  : True si la session a dépassé son TTL
- load_or_create(scope_id) : charge/crée depuis ~/.skifer_sessions/
- save(scope_id)           : persiste dans ~/.skifer_sessions/<scope_id>.json
- purge_expired(scope_id)  : supprime les sessions expirées
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import pathlib
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

_SESSIONS_DIR = pathlib.Path.home() / ".skifer_sessions"


# ---------------------------------------------------------------------------
# HistoryEntry
# ---------------------------------------------------------------------------

@dataclass
class HistoryEntry:
    """
    Log d'une interaction avec le GenBIAgent.

    Attributs:
        timestamp:       ISO 8601 — ex: "2026-03-27T14:35:12".
        question:        Question originale de l'utilisateur.
        model_used:      Clé du modèle (ex: "kpi_orders.erp").
        explanation:     Ce que l'agent a compris.
        query_params:    {metrics, group_by, filters, date_from, date_to}.
        response_format: "kpi" | "table" | "chart" | "text_analysis".
        mode:            "query" | "view" | "ambiguous" | "needs_clarification" | "error".
        kpi_value:       Valeur scalaire si format=kpi.
        kpi_label:       Label si format=kpi.
        table_markdown:  Tableau Markdown si format=table.
        chart_config:    Config axes si format=chart.
        chart_image_b64: Image PNG base64 si format=chart (généré par matplotlib).
        text_summary:    Résumé narratif si format=text_analysis.
        error:           Message d'erreur ou None si succès.
    """
    timestamp: str
    question: str
    model_used: str = ""
    explanation: str = ""
    query_params: dict = field(default_factory=dict)
    response_format: str = "table"
    mode: str = "query"

    kpi_value: Any = None
    kpi_label: str | None = None

    table_markdown: str | None = None

    chart_config: dict | None = None
    chart_image_b64: str | None = None

    text_summary: str | None = None

    error: str | None = None

    def to_dict(self) -> dict:
        return asdict(self)


# ---------------------------------------------------------------------------
# SessionHistory
# ---------------------------------------------------------------------------

class SessionHistory:
    """
    Container des interactions d'une session GenBIAgent.

    Usage :
        history = agent.session          # accès depuis le GenBIAgent
        history.to_pdf("rapport.pdf")    # export PDF
        history.to_json("rapport.json")  # export JSON brut

    Persistance (Phase B) :
        history = SessionHistory.load_or_create("my_catalog_DEV")
        history.save("my_catalog_DEV")
    """

    def __init__(
        self,
        session_title: str = "Analyse KPI",
        session_id: str | None = None,
        ttl_days: int = 15,
    ) -> None:
        self.title = session_title
        self.session_id: str = session_id or str(uuid.uuid4())
        self.created_at: dt.datetime = dt.datetime.now(dt.timezone.utc)
        self.ttl_days: int = ttl_days
        self._entries: list[HistoryEntry] = []

    # ------------------------------------------------------------------
    # TTL
    # ------------------------------------------------------------------

    @property
    def is_expired(self) -> bool:
        """True si la session a dépassé son TTL."""
        now = dt.datetime.now(dt.timezone.utc)
        created = self.created_at
        if created.tzinfo is None:
            created = created.replace(tzinfo=dt.timezone.utc)
        return (now - created).days > self.ttl_days

    def add(self, entry: HistoryEntry) -> None:
        """Ajoute une entrée à l'historique."""
        self._entries.append(entry)

    def entries(self) -> list[HistoryEntry]:
        """Retourne une copie de la liste des entrées."""
        return list(self._entries)

    def __len__(self) -> int:
        return len(self._entries)

    def to_pdf(self, output_path: str) -> None:
        """
        Exporte l'historique en PDF.
        Délégué à HistoryExporter (import lazy pour ne pas dépendre de fpdf2 à l'init).

        Args:
            output_path: Chemin du fichier PDF à créer.
        """
        from .exporter import HistoryExporter
        HistoryExporter().to_pdf(self, output_path)

    def to_json(self, output_path: str) -> None:
        """
        Sauvegarde brute de l'historique en JSON.

        Args:
            output_path: Chemin du fichier JSON à créer.
        """
        data = {
            "title": self.title,
            "session_id": self.session_id,
            "created_at": self.created_at.isoformat() if isinstance(self.created_at, dt.datetime) else self.created_at,
            "ttl_days": self.ttl_days,
            "total_entries": len(self._entries),
            "entries": [e.to_dict() for e in self._entries],
        }
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=str)
        print(f"[SessionHistory] JSON exported: {output_path}")

    @classmethod
    def from_json(cls, input_path: str) -> "SessionHistory":
        """Recharge un historique depuis un fichier JSON (pour debug / réimport)."""
        with open(input_path, encoding="utf-8") as f:
            data = json.load(f)
        session = cls(
            session_title=data.get("title", "Analyse KPI"),
            session_id=data.get("session_id"),
            ttl_days=data.get("ttl_days", 15),
        )
        raw_ts = data.get("created_at")
        if raw_ts:
            try:
                session.created_at = dt.datetime.fromisoformat(raw_ts)
            except (ValueError, TypeError):
                pass
        for entry_dict in data.get("entries", []):
            session.add(HistoryEntry(**entry_dict))
        return session

    # ------------------------------------------------------------------
    # Persistence (Phase B)
    # ------------------------------------------------------------------

    @classmethod
    def load_or_create(
        cls,
        scope_id: str,
        ttl_days: int = 15,
    ) -> "SessionHistory":
        """
        Charge la session persistée pour ce scope, ou en crée une nouvelle.

        Le fichier est stocké dans ~/.skifer_sessions/<scope_id>.json.
        Si la session chargée est expirée, une nouvelle est retournée à la place.
        """
        path = _SESSIONS_DIR / f"{scope_id}.json"
        if path.exists():
            try:
                session = cls.from_json(str(path))
                if not session.is_expired:
                    return session
            except Exception as exc:
                logger.warning(
                    "[SessionHistory] Could not load session '%s' — starting fresh. Cause: %s",
                    scope_id, exc,
                )
        return cls(ttl_days=ttl_days)

    def save(self, scope_id: str) -> None:
        """Persiste la session dans ~/.skifer_sessions/<scope_id>.json."""
        _SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
        path = _SESSIONS_DIR / f"{scope_id}.json"
        self.to_json(str(path))

    @staticmethod
    def purge_if_expired(scope_id: str, ttl_days: int = 15) -> int:
        """
        Supprime le fichier de session pour ce scope s'il est expiré.
        Retourne 1 si supprimé, 0 sinon.
        """
        if not _SESSIONS_DIR.exists():
            return 0
        path = _SESSIONS_DIR / f"{scope_id}.json"
        if path.exists():
            try:
                session = SessionHistory.from_json(str(path))
                if session.is_expired:
                    path.unlink()
                    return 1
            except Exception:
                pass
        return 0
