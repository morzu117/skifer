"""
UserProfile — profil utilisateur appris avec couche d'alias sémantiques.

Stocké dans ~/.skifer_profile.yaml.
Chargé automatiquement au démarrage du hub, sauvegardé après chaque
validation d'alias ou de filtre par défaut.

Structure YAML persistée :
    version: 1
    aliases: { terme_normalise: "catalog.schema.table ou colonne" }
    default_filters: ["region:equals:EMEA"]
    preferred_format: "table"
    blacklist_suggestions: ["env:equals:PROD"]
"""

from __future__ import annotations

import logging
import pathlib
import unicodedata
from dataclasses import dataclass, field

import yaml

logger = logging.getLogger(__name__)

_PROFILE_PATH = pathlib.Path.home() / ".skifer_profile.yaml"


def _normalize(term: str) -> str:
    """Minuscules + suppression des accents pour normaliser les clés d'alias."""
    text = unicodedata.normalize("NFD", term.lower())
    return "".join(c for c in text if unicodedata.category(c) != "Mn")


@dataclass
class UserProfile:
    """
    Profil utilisateur persisté localement.

    Attributs :
        aliases              : {terme_normalisé → nom canonique FQN ou colonne}
        default_filters      : filtres appliqués automatiquement à chaque requête
        preferred_format     : format de sortie préféré ("table" | "kpi" | "text_analysis")
        blacklist_suggestions: suggestions que l'utilisateur a explicitement refusées
    """

    aliases: dict[str, str] = field(default_factory=dict)
    default_filters: list[str] = field(default_factory=list)
    preferred_format: str = "table"
    blacklist_suggestions: list[str] = field(default_factory=list)

    # ------------------------------------------------------------------
    # Alias resolution
    # ------------------------------------------------------------------

    def resolve_alias(self, term: str) -> str | None:
        """
        Retourne le nom canonique si l'alias est connu (insensible à la casse
        et aux accents), sinon None.
        """
        return self.aliases.get(_normalize(term))

    def add_alias(self, term: str, canonical: str) -> None:
        """
        Ajoute un alias validé par l'utilisateur et sauvegarde le profil.
        Le terme est stocké normalisé (minuscules, sans accents).
        """
        key = _normalize(term)
        self.aliases[key] = canonical
        self.save()

    def reject_suggestion(self, term: str) -> None:
        """
        Blackliste un terme pour ne plus jamais le proposer.
        Sauvegarde le profil après chaque mise à jour.
        """
        if term not in self.blacklist_suggestions:
            self.blacklist_suggestions.append(term)
            self.save()

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def save(self, path: pathlib.Path = _PROFILE_PATH) -> None:
        """Sauvegarde le profil en YAML."""
        data = {
            "version": 1,
            "aliases": self.aliases,
            "default_filters": self.default_filters,
            "preferred_format": self.preferred_format,
            "blacklist_suggestions": self.blacklist_suggestions,
        }
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            yaml.dump(data, f, allow_unicode=True, default_flow_style=False)

    @classmethod
    def load(cls, path: pathlib.Path = _PROFILE_PATH) -> "UserProfile":
        """
        Charge le profil depuis YAML.
        Retourne un profil vide si le fichier n'existe pas.
        """
        if not path.exists():
            return cls()
        try:
            with open(path, encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
        except Exception:
            logger.warning("Could not load user profile from %s — starting fresh.", path, exc_info=True)
            return cls()
        return cls(
            aliases=data.get("aliases") or {},
            default_filters=data.get("default_filters") or [],
            preferred_format=data.get("preferred_format", "table"),
            blacklist_suggestions=data.get("blacklist_suggestions") or [],
        )
