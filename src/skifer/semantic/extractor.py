"""
NotebookExtractor — extrait le contexte métier des notebooks Jupyter.
RuleInspector    — extrait les docstrings des fonctions de règles.
"""

from __future__ import annotations

import ast
import inspect
import json
import os
from typing import Callable


class NotebookExtractor:
    """
    Extrait le contexte utile d'un notebook Jupyter (.ipynb).

    Contenu extrait :
      - Cellules Markdown (descriptions métier, commentaires).
      - Cellules de code Python contenant des expressions SQL
        (détectées par la présence de SELECT / FROM / GROUP BY).
      - Noms de DataFrames et colonnes utilisées.

    Usage :
        extractor = NotebookExtractor()
        context = extractor.extract("notebooks/kpi_orders.ipynb")
    """

    # Mots-clés SQL pour détecter les cellules pertinentes
    _SQL_KEYWORDS = {"SELECT", "FROM", "JOIN", "WHERE", "GROUP BY", "HAVING", "CREATE"}

    def extract(self, notebook_path: str, max_cells: int = 50) -> str:
        """
        Extrait le contexte d'un notebook.

        Args:
            notebook_path: Chemin vers le fichier .ipynb.
            max_cells:     Nombre maximum de cellules à analyser.

        Returns:
            Contexte textuel agrégé (Markdown + SQL pertinent).
        """
        if not os.path.exists(notebook_path):
            raise FileNotFoundError(
                f"[NotebookExtractor] Notebook introuvable : {notebook_path}"
            )

        with open(notebook_path, encoding="utf-8") as f:
            nb = json.load(f)

        cells = nb.get("cells", [])[:max_cells]
        parts: list[str] = []

        for cell in cells:
            cell_type = cell.get("cell_type", "")
            source = "".join(cell.get("source", []))

            if cell_type == "markdown":
                text = source.strip()
                if text:
                    parts.append(f"[DOC] {text}")

            elif cell_type == "code":
                upper = source.upper()
                if any(kw in upper for kw in self._SQL_KEYWORDS):
                    parts.append(f"[SQL] {source.strip()}")
                elif "spark" in source.lower() or "df" in source.lower():
                    parts.append(f"[CODE] {source.strip()}")

        return "\n\n".join(parts)


class RuleInspector:
    """
    Extrait les docstrings et métadonnées des fonctions de règles.

    Usage :
        from my_rules import compute_revenue
        inspector = RuleInspector()
        context = inspector.extract_function(compute_revenue)
        # ou depuis un fichier Python :
        context = inspector.extract_from_file("rules/kpi_orders.py")
    """

    def extract_function(self, func: Callable) -> str:
        """
        Extrait docstring + signature d'une fonction.

        Args:
            func: Fonction Python à inspecter.

        Returns:
            Contexte textuel (nom + signature + docstring).
        """
        name = func.__name__
        doc = inspect.getdoc(func) or ""
        try:
            sig = str(inspect.signature(func))
        except (ValueError, TypeError):
            sig = "(…)"

        return f"[RULE] {name}{sig}\n{doc}"

    def extract_from_file(self, file_path: str) -> str:
        """
        Extrait les docstrings de toutes les fonctions d'un fichier Python.

        Args:
            file_path: Chemin vers le fichier .py.

        Returns:
            Contexte textuel agrégé.
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(
                f"[RuleInspector] Fichier introuvable : {file_path}"
            )

        with open(file_path, encoding="utf-8") as f:
            source = f.read()

        tree = ast.parse(source)
        parts: list[str] = []

        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                docstring = ast.get_docstring(node)
                if docstring:
                    parts.append(f"[RULE] {node.name}()\n{docstring}")

        return "\n\n".join(parts)
