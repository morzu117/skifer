"""
GlossaryReader — lit un Business Glossary depuis différents formats.

Formats supportés : JSON, YAML, TXT.
Format PDF et PPTX : si les librairies correspondantes sont installées
(pypdf2/pdfminer, python-pptx).

Usage :
    reader = GlossaryReader()
    context = reader.read("glossaries/orders.json")
"""

from __future__ import annotations

import json
import os

import yaml


class GlossaryReader:
    """
    Lit un Business Glossary et retourne son contenu sous forme de texte.

    Le texte résultant est conçu pour être injecté dans un prompt LLM
    comme contexte métier.
    """

    def read(self, path: str) -> str:
        """
        Lit le glossaire depuis le fichier indiqué.

        Args:
            path: Chemin vers le fichier glossaire.

        Returns:
            Contenu textuel du glossaire.

        Raises:
            FileNotFoundError: Si le fichier n'existe pas.
            ValueError: Si le format n'est pas supporté.
        """
        if not os.path.exists(path):
            raise FileNotFoundError(
                f"[GlossaryReader] Fichier introuvable : {path}"
            )

        ext = os.path.splitext(path)[1].lower()

        if ext == ".json":
            return self._read_json(path)
        elif ext in (".yaml", ".yml"):
            return self._read_yaml(path)
        elif ext == ".txt":
            return self._read_txt(path)
        elif ext == ".pdf":
            return self._read_pdf(path)
        elif ext in (".pptx", ".ppt"):
            return self._read_pptx(path)
        else:
            raise ValueError(
                f"[GlossaryReader] Format non supporté : '{ext}'. "
                f"Formats acceptés : .json, .yaml, .yml, .txt, .pdf, .pptx"
            )

    # ------------------------------------------------------------------
    # Readers par format
    # ------------------------------------------------------------------

    def _read_json(self, path: str) -> str:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return self._flatten(data)

    def _read_yaml(self, path: str) -> str:
        with open(path, encoding="utf-8") as f:
            data = yaml.safe_load(f)
        return self._flatten(data)

    def _read_txt(self, path: str) -> str:
        with open(path, encoding="utf-8") as f:
            return f.read()

    def _read_pdf(self, path: str) -> str:
        try:
            from pypdf import PdfReader
            reader = PdfReader(path)
            pages = [page.extract_text() or "" for page in reader.pages]
            return "\n\n".join(pages)
        except ImportError:
            pass

        try:
            from pdfminer.high_level import extract_text
            return extract_text(path)
        except ImportError:
            raise ImportError(
                "[GlossaryReader] Aucune librairie PDF disponible. "
                "Installez : pip install pypdf  (ou pdfminer.six)"
            )

    def _read_pptx(self, path: str) -> str:
        try:
            from pptx import Presentation
        except ImportError:
            raise ImportError(
                "[GlossaryReader] 'python-pptx' n'est pas installé. "
                "Faites : pip install python-pptx"
            )
        prs = Presentation(path)
        texts: list[str] = []
        for slide in prs.slides:
            for shape in slide.shapes:
                if hasattr(shape, "text") and shape.text:
                    texts.append(shape.text)
        return "\n\n".join(texts)

    # ------------------------------------------------------------------
    # Utilitaire
    # ------------------------------------------------------------------

    def _flatten(self, data: object, indent: int = 0) -> str:
        """Convertit un dict/list imbriqué en texte lisible."""
        if isinstance(data, dict):
            lines = []
            for k, v in data.items():
                if isinstance(v, (dict, list)):
                    lines.append(f"{'  ' * indent}{k}:")
                    lines.append(self._flatten(v, indent + 1))
                else:
                    lines.append(f"{'  ' * indent}{k}: {v}")
            return "\n".join(lines)
        elif isinstance(data, list):
            return "\n".join(
                f"{'  ' * indent}- {self._flatten(item, indent + 1)}"
                if isinstance(item, (dict, list))
                else f"{'  ' * indent}- {item}"
                for item in data
            )
        else:
            return str(data)
