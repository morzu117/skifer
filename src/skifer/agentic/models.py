"""
Modèles de données pour la couche agentic.

AgentResponse  — réponse structurée du GenBIAgent.
FormattedResult — résultat mis en forme selon le ResponseFormat.
ResponseFormat — enum des 4 formats de retour supportés.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping

from ..capabilities.executors import CapabilityResult


@dataclass(frozen=True)
class CapabilityRequest:
    """Structured capability selection; free text never constructs this request."""

    capability_id: str
    arguments: Mapping[str, Any]


@dataclass(frozen=True)
class CapabilityResponse:
    """Allowlisted Hub response for an explicitly selected capability."""

    capability_id: str
    capability_version: str
    result: CapabilityResult

    def to_dict(self) -> dict[str, Any]:
        """Serialize only the structured capability response fields."""
        return {
            "capability_id": self.capability_id,
            "capability_version": self.capability_version,
            "result": self.result.to_dict(),
        }


# ---------------------------------------------------------------------------
# ResponseFormat
# ---------------------------------------------------------------------------

class ResponseFormat(str, Enum):
    """Format de retour détecté depuis la formulation de la question."""
    KPI           = "kpi"           # valeur scalaire unique + label
    TABLE         = "table"         # données tabulaires brutes (DataFrame)
    CHART         = "chart"         # visualisation (config axes + type)
    TEXT_ANALYSIS = "text_analysis" # résumé narratif en langage naturel


# ---------------------------------------------------------------------------
# FormattedResult
# ---------------------------------------------------------------------------

@dataclass
class FormattedResult:
    """
    Résultat d'une requête sémantique, mis en forme selon le ResponseFormat.

    Attributs communs :
        format:  Format de rendu détecté.
        data:    DataFrame PySpark source de vérité (toujours présent si succès).
        title:   Titre généré (ex: "CA brut par région — 2024").

    Attributs spécifiques à format=kpi :
        kpi_value: Valeur scalaire (ex: 2847320.0).
        kpi_label: Label lisible (ex: "CA brut TTC (€)").

    Attributs spécifiques à format=chart :
        chart_config: Configuration déclarative pour matplotlib / plotly.
            {
                "type": "bar" | "line" | "pie",
                "title": str,
                "x_axis": {"field": str, "label": str},
                "y_axis": {"field": str, "label": str, "format": str},
                "series": [...]
            }

    Attributs spécifiques à format=text_analysis :
        text_summary: Résumé narratif généré par le LLM (Step D).
    """
    format: ResponseFormat
    data: Any                        # pyspark.sql.DataFrame (Any pour éviter l'import)
    title: str = ""

    kpi_value: Any = None
    kpi_label: str | None = None

    chart_config: dict | None = None

    text_summary: str | None = None

    # Optional and additive: callers that only consume the rendered data keep
    # their existing contract, while provenance can travel with the result.
    evidence: Any = None


# ---------------------------------------------------------------------------
# AgentResponse
# ---------------------------------------------------------------------------

@dataclass
class AgentResponse:
    """
    Réponse structurée du GenBIAgent.ask().

    Modes possibles :
        "query"              — requête exécutée, result contient un FormattedResult.
        "view"               — vue créée, result contient le FQN de la vue.
        "ambiguous"          — plusieurs modèles candidats, clarification requise.
        "needs_clarification"— nom inconnu, l'agent demande une précision.
        "no_model"           — aucun modèle ne couvre le sujet.
        "error"              — erreur technique.

    Attributs:
        question:               Question originale.
        mode:                   Mode de la réponse (voir liste ci-dessus).
        model_used:             Clé du modèle sélectionné (ex: "kpi_orders.erp").
        explanation:            Ce que l'agent a compris.
        query_params:           Paramètres sémantiques (metrics, group_by, filters, …).
        result:                 FormattedResult (query) | str FQN (view) | None.
        error:                  Message d'erreur si mode="error".
        clarification_question: Question posée à l'utilisateur si ambiguïté.
        suggestions:            Alternatives proposées (noms du YAML).
        candidates:             Modèles candidats en cas d'ambiguïté.
    """
    question: str
    mode: str = "query"
    model_used: str = ""
    explanation: str = ""
    query_params: dict = field(default_factory=dict)
    result: Any = None
    error: str | None = None
    clarification_question: str | None = None
    suggestions: list[str] = field(default_factory=list)
    candidates: list[str] = field(default_factory=list)
    access_denied: bool = False
    policy_decision: str | None = None
    policy_reasons: tuple[str, ...] | None = None
    recommended_action: str | None = None
    evidence: Any = None

    @property
    def success(self) -> bool:
        """True si la requête a été exécutée sans erreur."""
        return self.mode in ("query", "view") and self.error is None

    @property
    def needs_clarification(self) -> bool:
        return self.mode in ("ambiguous", "needs_clarification")


# ---------------------------------------------------------------------------
# BuilderResponse
# ---------------------------------------------------------------------------

@dataclass
class BuilderResponse:
    """
    Réponse structurée du BuilderAgent.ask() (mode LLM).

    Attributs:
        success:                True si le YAML a été produit et validé.
        yaml_content:           Contenu YAML du pipeline généré.
        output_path:            Chemin du fichier sauvegardé (None si non sauvegardé).
        error:                  Message d'erreur si success=False.
        clarification_question: Question à poser à l'user si une table/colonne est ambiguë.
    """
    success: bool
    yaml_content: str = ""
    output_path: str | None = None
    error: str | None = None
    clarification_question: str | None = None


# ---------------------------------------------------------------------------
# LineageResponse
# ---------------------------------------------------------------------------

@dataclass
class LineageResponse:
    """
    Réponse structurée du LineageAgent.ask() / trace() / impact() / lookup().

    Modes possibles :
        "trace"   — traversée upstream (provenance d'un champ).
        "impact"  — traversée downstream (impact d'un champ).
        "render"  — rendu du graphe complet (Mermaid / HTML / JSON).
        "lookup"  — lookup DataDictionary (définition d'un champ).
        "error"   — erreur technique ou champ non trouvé.

    Attributs:
        question:     Question originale (ou appel direct, ex: "trace(t, col)").
        mode:         Mode de la réponse.
        table:        Nom de la table ciblée (peut être vide si non spécifié).
        column:       Nom de la colonne ciblée.
        edges:        Edges de lignage trouvés (list[LineageEdge]).
        field_entry:  FieldEntry du DataDictionary (mode lookup uniquement).
        diagram:      Diagramme sérialisé en string (Mermaid / JSON / HTML).
        narrative:    Explication en prose générée par LLM (optionnel).
        error:        Message d'erreur si mode="error".
        suggestions:  Noms proches suggérés en cas de champ non trouvé.
    """
    question: str
    mode: str = "trace"
    table: str = ""
    column: str = ""
    edges: list = field(default_factory=list)   # list[LineageEdge]
    field_entry: Any = None                      # FieldEntry | None
    diagram: str = ""
    narrative: str = ""
    error: str | None = None
    suggestions: list[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        """True si la réponse ne contient pas d'erreur."""
        return self.error is None


# ---------------------------------------------------------------------------
# OrchestratorExportResult
# ---------------------------------------------------------------------------

@dataclass
class OrchestratorExportResult:
    """
    Résultat de BuilderAgent.export_orchestration().

    Attributs:
        format:               Format primaire généré : "airflow" | "databricks" | "script".
        primary_path:         Chemin du DAG Airflow ou bundle DAB.
        fallback_script_path: Chemin du script Python de fallback (toujours présent).
        pipelines:            Liste des chemins YAML traités.
    """
    format: str
    primary_path: str
    fallback_script_path: str
    pipelines: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# QualityResponse
# ---------------------------------------------------------------------------

@dataclass
class QualityResponse:
    """
    Réponse structurée du QualityAgent.ask() / check() / get_history().

    Modes possibles :
        "check"   — checks exécutés sur la table, report disponible.
        "history" — liste des derniers MonitorReport depuis le HistoryStore.
        "report"  — rendu formatté du dernier report (text / json / html).
        "error"   — erreur technique ou table non trouvée.

    Attributs:
        question:        Question originale (ou appel direct).
        mode:            Mode de la réponse.
        table:           FQN de la table ciblée.
        report:          MonitorReport du dernier check (mode check).
        history_reports: Liste de MonitorReport (mode history).
        text_output:     Résultat formatté (MonitorReporter — text/json/html).
        narrative:       Résumé en prose généré par LLM (optionnel).
        error:           Message d'erreur si mode="error".
    """
    question: str
    mode: str = "check"
    table: str = ""
    report: Any = None                            # MonitorReport | None
    history_reports: list = field(default_factory=list)  # list[MonitorReport]
    text_output: str = ""
    narrative: str = ""
    error: str | None = None

    @property
    def success(self) -> bool:
        """True si la réponse ne contient pas d'erreur."""
        return self.error is None

    @property
    def passed(self) -> bool | None:
        """None si aucun check, True si pas de critical failure."""
        if self.report is None:
            return None
        return not self.report.has_critical_failures()


# ---------------------------------------------------------------------------
# DictionaryResponse
# ---------------------------------------------------------------------------

@dataclass
class DictionaryResponse:
    """
    Réponse structurée du DictionaryAgent.ask() / lookup() / list_fields().

    Modes possibles :
        "lookup" — recherche d'un champ spécifique, entry contient le FieldEntry.
        "list"   — liste des champs d'une table (ou de tout le dictionnaire).
        "export" — export formatté du dictionnaire complet (json / text).
        "error"  — erreur technique ou champ non trouvé.

    Attributs:
        question:    Question originale (ou appel direct).
        mode:        Mode de la réponse.
        table:       Nom de la table ciblée (peut être vide).
        column:      Nom de la colonne ciblée (mode lookup).
        entry:       FieldEntry trouvé (mode lookup).
        entries:     Liste de FieldEntry (mode list).
        text_output: Export formatté du dictionnaire (mode export).
        narrative:   Explication en prose générée par LLM (optionnel).
        error:       Message d'erreur si mode="error".
        suggestions: Noms proches suggérés si champ non trouvé.
    """
    question: str
    mode: str = "lookup"
    table: str = ""
    column: str = ""
    entry: Any = None                            # FieldEntry | None
    entries: list = field(default_factory=list)  # list[FieldEntry]
    text_output: str = ""
    narrative: str = ""
    error: str | None = None
    suggestions: list[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        """True si la réponse ne contient pas d'erreur."""
        return self.error is None
