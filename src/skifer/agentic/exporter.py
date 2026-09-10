"""
HistoryExporter — export PDF d'une SessionHistory.

Librairie : fpdf2 (pure Python, aucune dépendance système).
Installation : pip install fpdf2

Structure du PDF généré :
  - Page de garde (titre session, date, nb d'analyses)
  - Une section par interaction (question, modèle, résultat selon le format)
"""

from __future__ import annotations

import base64
import tempfile
import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fpdf import FPDF

    from .history import HistoryEntry, SessionHistory


class HistoryExporter:
    """Génère un PDF depuis un SessionHistory."""

    # Configuration typographique
    _FONT_FAMILY = "Helvetica"
    _MARGIN = 15
    _PAGE_WIDTH = 210  # A4 mm

    _REPLACEMENTS = {
        "\u2014": "-",    # em dash —
        "\u2013": "-",    # en dash –
        "\u2018": "'",    # guillemet simple gauche
        "\u2019": "'",    # guillemet simple droit
        "\u201c": '"',    # guillemet double gauche
        "\u201d": '"',    # guillemet double droit
        "\u2026": "...",  # points de suspension
        "\u00b0": " deg", # degré
    }

    def to_pdf(self, session: "SessionHistory", output_path: str) -> None:
        """
        Génère un PDF depuis l'historique de session.

        Args:
            session:     SessionHistory à exporter.
            output_path: Chemin du fichier PDF à créer.

        Raises:
            ImportError: Si fpdf2 n'est pas installé.
        """
        try:
            from fpdf import FPDF
        except ImportError as exc:
            raise ImportError(
                "[HistoryExporter] 'fpdf2' n'est pas installé. "
                "Faites : pip install fpdf2"
            ) from exc

        pdf = FPDF()
        pdf.set_auto_page_break(auto=True, margin=self._MARGIN)

        self._add_cover_page(pdf, session)

        for i, entry in enumerate(session.entries(), start=1):
            self._add_entry_section(pdf, i, entry)

        pdf.output(output_path)
        print(f"[HistoryExporter] PDF exporté : {output_path}")

    # ------------------------------------------------------------------
    # Page de garde
    # ------------------------------------------------------------------

    def _add_cover_page(self, pdf: "FPDF", session: "SessionHistory") -> None:
        pdf.add_page()

        # Titre
        pdf.set_font(self._FONT_FAMILY, "B", 24)
        pdf.set_y(60)
        pdf.cell(0, 12, session.title, new_x="LMARGIN", new_y="NEXT", align="C")

        pdf.ln(10)

        # Sous-titre
        pdf.set_font(self._FONT_FAMILY, "", 12)
        pdf.cell(
            0, 8,
            f"Généré le : {self._format_datetime(session.created_at)}",
            new_x="LMARGIN", new_y="NEXT", align="C",
        )
        pdf.cell(
            0, 8,
            f"Nombre d'analyses : {len(session)}",
            new_x="LMARGIN", new_y="NEXT", align="C",
        )

    # ------------------------------------------------------------------
    # Section par entrée
    # ------------------------------------------------------------------

    def _add_entry_section(
        self, pdf: "FPDF", index: int, entry: "HistoryEntry"
    ) -> None:
        pdf.add_page()

        # En-tête section
        pdf.set_font(self._FONT_FAMILY, "B", 14)
        header = self._safe_text(
            f"Analyse {index} - {self._format_datetime(entry.timestamp)}"
        )
        pdf.cell(0, 10, header, new_x="LMARGIN", new_y="NEXT")
        pdf.line(
            self._MARGIN,
            pdf.get_y(),
            self._PAGE_WIDTH - self._MARGIN,
            pdf.get_y(),
        )
        pdf.ln(3)

        # Métadonnées
        pdf.set_font(self._FONT_FAMILY, "B", 11)
        pdf.cell(40, 7, "Question :", new_x="END", new_y="TOP")
        pdf.set_font(self._FONT_FAMILY, "", 11)
        pdf.multi_cell(0, 7, self._safe_text(entry.question))
        pdf.ln(2)

        if entry.model_used:
            pdf.set_font(self._FONT_FAMILY, "B", 11)
            pdf.cell(40, 7, "Modele :", new_x="END", new_y="TOP")
            pdf.set_font(self._FONT_FAMILY, "", 11)
            pdf.cell(0, 7, self._safe_text(entry.model_used), new_x="LMARGIN", new_y="NEXT")
            pdf.ln(2)

        if entry.explanation:
            pdf.set_font(self._FONT_FAMILY, "B", 11)
            pdf.cell(40, 7, "Compris :", new_x="END", new_y="TOP")
            pdf.set_font(self._FONT_FAMILY, "I", 10)
            pdf.multi_cell(0, 7, self._safe_text(entry.explanation))
            pdf.ln(2)

        # Erreur
        if entry.error:
            pdf.set_font(self._FONT_FAMILY, "B", 11)
            pdf.set_text_color(200, 0, 0)
            pdf.cell(0, 7, self._safe_text(f"Erreur : {entry.error}"), new_x="LMARGIN", new_y="NEXT")
            pdf.set_text_color(0, 0, 0)
            return

        # Résultat selon le format
        fmt = entry.response_format or "table"
        pdf.set_font(self._FONT_FAMILY, "B", 11)
        pdf.set_fill_color(230, 230, 230)
        pdf.cell(
            0, 8,
            f"Format : {fmt.upper()}",
            new_x="LMARGIN", new_y="NEXT",
            fill=True,
        )
        pdf.ln(4)

        if fmt == "kpi":
            self._render_kpi(pdf, entry)
        elif fmt == "table":
            self._render_table(pdf, entry)
        elif fmt == "chart":
            self._render_chart(pdf, entry)
        elif fmt == "text_analysis":
            self._render_text_analysis(pdf, entry)

    # ------------------------------------------------------------------
    # Rendus par format
    # ------------------------------------------------------------------

    def _render_kpi(self, pdf: "FPDF", entry: "HistoryEntry") -> None:
        pdf.set_font(self._FONT_FAMILY, "B", 28)
        value_str = (
            f"{entry.kpi_value:,.2f}" if isinstance(entry.kpi_value, float)
            else str(entry.kpi_value or "-")
        )
        pdf.cell(0, 16, self._safe_text(value_str), new_x="LMARGIN", new_y="NEXT", align="C")

        if entry.kpi_label:
            pdf.set_font(self._FONT_FAMILY, "I", 12)
            pdf.cell(0, 8, self._safe_text(entry.kpi_label), new_x="LMARGIN", new_y="NEXT", align="C")

    def _render_table(self, pdf: "FPDF", entry: "HistoryEntry") -> None:
        if not entry.table_markdown:
            pdf.set_font(self._FONT_FAMILY, "I", 10)
            pdf.cell(0, 7, "(Aucune donnée)", new_x="LMARGIN", new_y="NEXT")
            return

        lines = entry.table_markdown.split("\n")
        col_width = min(50, (self._PAGE_WIDTH - 2 * self._MARGIN) // max(1, len(lines[0].split("|")) - 2))

        for i, line in enumerate(lines):
            cells = [c.strip() for c in line.strip("|").split("|")]
            is_header = i == 0
            is_sep = all(set(c.strip()) <= {"-"} for c in cells if c)

            if is_sep:
                continue

            pdf.set_font(
                self._FONT_FAMILY, "B" if is_header else "", 9
            )
            for cell in cells:
                pdf.cell(col_width, 6, cell[:25], border=1, new_x="END", new_y="TOP")
            pdf.ln(6)

    def _render_chart(self, pdf: "FPDF", entry: "HistoryEntry") -> None:
        if entry.chart_image_b64:
            # Décoder le PNG et l'intégrer
            img_data = base64.b64decode(entry.chart_image_b64)
            with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
                tmp.write(img_data)
                tmp_path = tmp.name
            try:
                usable_width = self._PAGE_WIDTH - 2 * self._MARGIN
                pdf.image(tmp_path, x=self._MARGIN, w=usable_width)
            finally:
                os.unlink(tmp_path)
        elif entry.chart_config:
            # Fallback : afficher la config en texte
            pdf.set_font(self._FONT_FAMILY, "I", 10)
            cfg = entry.chart_config
            pdf.cell(
                0, 7,
                f"Type : {cfg.get('type', 'bar')} | "
                f"X : {cfg.get('x_axis', {}).get('field', '')} | "
                f"Y : {cfg.get('y_axis', {}).get('field', '')}",
                new_x="LMARGIN", new_y="NEXT",
            )
        else:
            pdf.set_font(self._FONT_FAMILY, "I", 10)
            pdf.cell(0, 7, "(Graphique non disponible)", new_x="LMARGIN", new_y="NEXT")

    def _render_text_analysis(self, pdf: "FPDF", entry: "HistoryEntry") -> None:
        pdf.set_font(self._FONT_FAMILY, "I", 11)
        text = self._safe_text(entry.text_summary or "(Aucune analyse)")
        pdf.multi_cell(0, 7, text)

    # ------------------------------------------------------------------
    # Utilitaires
    # ------------------------------------------------------------------

    @classmethod
    def _safe_text(cls, text: str) -> str:
        """Remplace les caractères hors Latin-1 pour compatibilité Helvetica."""
        for char, replacement in cls._REPLACEMENTS.items():
            text = text.replace(char, replacement)
        return text.encode("latin-1", errors="replace").decode("latin-1")

    @staticmethod
    def _format_datetime(iso_str: str) -> str:
        """Formate un timestamp ISO 8601 en format lisible."""
        try:
            dt = iso_str[:19].replace("T", " ")
            return dt
        except Exception:
            return iso_str or ""
