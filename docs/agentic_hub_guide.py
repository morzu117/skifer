"""
Génère le guide de référence PDF du SkiferHub agentique.

Usage :
    python docs/agentic_hub_guide.py
    # → docs/agentic_hub_guide.pdf
"""

from __future__ import annotations

import os
import sys


# ---------------------------------------------------------------------------
# Helpers typographiques (hérités du style HistoryExporter)
# ---------------------------------------------------------------------------

def _safe(text: str) -> str:
    """Encode en Latin-1 avec remplacement des caractères non supportés."""
    replacements = {
        "\u2014": "-", "\u2013": "-",
        "\u2018": "'", "\u2019": "'",
        "\u201c": '"', "\u201d": '"',
        "\u2026": "...", "\u00b0": " deg",
        "\u2192": "->", "\u2190": "<-",
        "\u25b6": ">", "\u2022": "-",
        "\u00e9": "e", "\u00e8": "e", "\u00ea": "e", "\u00eb": "e",
        "\u00e0": "a", "\u00e2": "a",
        "\u00ee": "i", "\u00ef": "i",
        "\u00f4": "o", "\u00f9": "u", "\u00fb": "u",
        "\u00e7": "c", "\u00fc": "u",
        "\u00e9": "e",
    }
    for ch, rep in replacements.items():
        text = text.replace(ch, rep)
    return text.encode("latin-1", errors="replace").decode("latin-1")


# ---------------------------------------------------------------------------
# Layout constants
# ---------------------------------------------------------------------------

MARGIN = 15
PAGE_W = 210  # A4
INNER_W = PAGE_W - 2 * MARGIN
FONT = "Helvetica"

COLOR_BLUE   = (31,  97, 141)   # titre principal
COLOR_TEAL   = (22, 160, 133)   # section
COLOR_GRAY   = (52,  73,  94)   # corps
COLOR_LIGHT  = (236, 240, 241)  # fond alternance
COLOR_WHITE  = (255, 255, 255)
COLOR_RED    = (192,  57,  43)
COLOR_GREEN  = (39, 174,  96)
COLOR_ORANGE = (211,  84,   0)


# ---------------------------------------------------------------------------
# Hub Guide PDF generator
# ---------------------------------------------------------------------------

class HubGuidePDF:
    """Construit le PDF de référence de l'AgenticHub."""

    def __init__(self):
        try:
            from fpdf import FPDF
        except ImportError:
            print("fpdf2 requis : pip install fpdf2")
            sys.exit(1)
        self._FPDF = FPDF

    def generate(self, output_path: str) -> None:
        from fpdf import FPDF

        pdf = FPDF()
        pdf.set_auto_page_break(auto=True, margin=MARGIN)
        pdf.set_left_margin(MARGIN)
        pdf.set_right_margin(MARGIN)

        self._cover(pdf)
        self._toc(pdf)
        self._section_overview(pdf)
        self._section_cli(pdf)
        self._section_routing(pdf)
        self._section_agents(pdf)
        self._section_history(pdf)
        self._section_profile(pdf)
        self._section_responses(pdf)
        self._section_code_examples(pdf)
        self._section_quickref(pdf)

        pdf.output(output_path)
        print(f"[HubGuidePDF] PDF genere : {output_path}")

    # ------------------------------------------------------------------
    # Page de couverture
    # ------------------------------------------------------------------

    def _cover(self, pdf) -> None:
        pdf.add_page()

        # Fond coloré haut de page
        pdf.set_fill_color(*COLOR_BLUE)
        pdf.rect(0, 0, 210, 80, style="F")

        # Titre
        pdf.set_text_color(*COLOR_WHITE)
        pdf.set_font(FONT, "B", 28)
        pdf.set_y(22)
        pdf.cell(0, 12, _safe("SkiferHub"), new_x="LMARGIN", new_y="NEXT", align="C")

        pdf.set_font(FONT, "", 16)
        pdf.cell(0, 10, _safe("Guide de reference — AgenticHub"),
                 new_x="LMARGIN", new_y="NEXT", align="C")

        pdf.set_font(FONT, "I", 11)
        pdf.cell(0, 8, _safe("Interface conversationnelle pour vos pipelines Lakehouse"),
                 new_x="LMARGIN", new_y="NEXT", align="C")

        # Reset
        pdf.set_text_color(*COLOR_GRAY)
        pdf.set_y(100)

        # Encadre de presentation
        pdf.set_fill_color(*COLOR_LIGHT)
        pdf.set_draw_color(*COLOR_TEAL)
        pdf.set_line_width(0.8)
        pdf.rect(MARGIN, 95, INNER_W, 55, style="FD")

        pdf.set_font(FONT, "B", 12)
        pdf.set_text_color(*COLOR_TEAL)
        pdf.set_xy(MARGIN + 5, 102)
        pdf.cell(0, 8, _safe("Vue d'ensemble"), new_x="LMARGIN", new_y="NEXT")

        pdf.set_font(FONT, "", 10)
        pdf.set_text_color(*COLOR_GRAY)
        lines = [
            _safe("L'AgenticHub est le point d'entree unique de toutes les capacites agentiques"),
            _safe("de Skifer. Il route chaque question en langage naturel vers"),
            _safe("l'agent specialise competent, sans configuration requise."),
            "",
            _safe("5 agents integres  |  Routage sans LLM  |  Profil utilisateur persistant"),
            _safe("Session avec TTL   |  CLI REPL riche     |  Export PDF de session"),
        ]
        pdf.set_xy(MARGIN + 5, 113)
        for line in lines:
            pdf.cell(0, 7, line, new_x="LMARGIN", new_y="NEXT")

        # Bas de page
        pdf.set_text_color(150, 150, 150)
        pdf.set_font(FONT, "I", 9)
        pdf.set_y(260)
        pdf.cell(0, 6, _safe("Skifer — skifer.cli:main"),
                 new_x="LMARGIN", new_y="NEXT", align="C")

    # ------------------------------------------------------------------
    # Table des matieres
    # ------------------------------------------------------------------

    def _toc(self, pdf) -> None:
        pdf.add_page()
        self._h1(pdf, "Table des matieres")

        sections = [
            ("1.", "Vue d'ensemble de l'AgenticHub"),
            ("2.", "Demarrage : commande skifer hub"),
            ("3.", "Routage des questions"),
            ("4.", "Les 5 agents specialises"),
            ("   4.1", "GenBIAgent — requetes KPI / metier"),
            ("   4.2", "LineageAgent — provenance et impact"),
            ("   4.3", "QualityAgent — sante des donnees"),
            ("   4.4", "DictionaryAgent — glossaire metier"),
            ("   4.5", "BuilderAgent — generation de pipelines YAML"),
            ("5.", "Historique de session (TTL + persistance)"),
            ("6.", "Profil utilisateur et alias semantiques"),
            ("7.", "Formats de reponse"),
            ("8.", "Exemples de code Python"),
            ("9.", "Reference rapide des mots-cles"),
        ]

        pdf.set_font(FONT, "", 11)
        pdf.set_text_color(*COLOR_GRAY)
        pdf.ln(4)
        for num, title in sections:
            pdf.set_x(MARGIN)
            pdf.cell(20, 8, _safe(num), new_x="END", new_y="TOP")
            pdf.cell(0, 8, _safe(title), new_x="LMARGIN", new_y="NEXT")

    # ------------------------------------------------------------------
    # Section 1 — Vue d'ensemble
    # ------------------------------------------------------------------

    def _section_overview(self, pdf) -> None:
        pdf.add_page()
        self._h1(pdf, "1. Vue d'ensemble de l'AgenticHub")

        self._body(pdf,
            "L'AgenticHub est un routeur conversationnel qui expose une API unifiee "
            "hub.ask(question) vers 5 agents specialises. Chaque question en langage naturel "
            "est analysee par mots-cles (deterministe, sans LLM) puis dirigee vers l'agent "
            "le plus pertinent."
        )
        pdf.ln(4)

        # Schema textuel de l'architecture
        self._h2(pdf, "Architecture")
        self._code_block(pdf, [
            "                  hub.ask(question)",
            "                        |",
            "         [Routeur deterministe / LLM fallback]",
            "                        |",
            "   _____________________|_________________________",
            "   |          |          |           |           |",
            "GenBIAgent  LineageAgent  QualityAgent  Dictionary  BuilderAgent",
            "(KPI/metier) (provenance)  (sante)      Agent       (pipeline YAML)",
            "                                       (glossaire)",
        ])
        pdf.ln(4)

        self._h2(pdf, "Dependances minimales")
        self._body(pdf,
            "Tous les agents sont OPTIONNELS. Si l'agent cible n'est pas injecte, "
            "hub.ask() retourne un AgentResponse(mode='error') avec un message explicite. "
            "Aucun Spark ni LLM n'est requis pour le routage de base."
        )
        pdf.ln(4)

        self._h2(pdf, "Constructeur AgenticHub")
        self._code_block(pdf, [
            "AgenticHub(",
            "  semantic_engine=None,      # SemanticEngine (requis pour GenBIAgent auto)",
            "  llm_provider=None,         # LLMProvider (optionnel, active fallback LLM)",
            "  lineage_agent=None,        # LineageAgent injecte directement",
            "  quality_agent=None,        # QualityAgent injecte directement",
            "  dictionary_agent=None,     # DictionaryAgent injecte directement",
            "  builder_agent=None,        # BuilderAgent injecte directement",
            "  genbi_agent=None,          # GenBIAgent injecte directement",
            "  session_title='SkiferHub',",
            "  profile=None,              # UserProfile (alias semantiques)",
            ")",
        ])

    # ------------------------------------------------------------------
    # Section 2 — CLI
    # ------------------------------------------------------------------

    def _section_cli(self, pdf) -> None:
        pdf.add_page()
        self._h1(pdf, "2. Demarrage : commande skifer hub")

        self._h2(pdf, "Installation")
        self._code_block(pdf, [
            "pip install skifer",
            "# ou en mode dev :",
            "pip install -e '.[dev]'",
        ])
        pdf.ln(3)

        self._h2(pdf, "Lancement")
        self._code_block(pdf, [
            "# Config du repertoire courant",
            "skifer hub",
            "",
            "# Config explicite",
            "skifer hub --config path/vers/config.yaml",
            "",
            "# Forcer une nouvelle session (efface la precedente)",
            "skifer hub --new-session",
        ])
        pdf.ln(3)

        self._h2(pdf, "Demarrage de session")
        self._body(pdf,
            "Au lancement, si une session non expiree existe pour le scope "
            "(catalog + env), le hub propose deux options :"
        )
        self._code_block(pdf, [
            "SkiferHub v0.9.0 -- catalog: my_catalog (DEV)",
            "---------------------------------------------",
            "  [1] Nouvelle session  (efface la session precedente)",
            "  [2] Reprendre la session du 2026-05-01 (11 messages)",
            "",
            "Choix [1/2] :",
        ])
        pdf.ln(3)

        self._h2(pdf, "Commandes speciales dans le REPL")
        rows = [
            ("Commande",    "Action"),
            ("exit / quit", "Quitter le REPL proprement"),
            ("help",        "Afficher la liste des commandes"),
            ("history",     "Afficher les 5 dernieres questions de la session"),
            ("export pdf",  "Exporter la session en PDF (fpdf2 requis)"),
        ]
        self._table(pdf, rows)

        pdf.ln(3)
        self._h2(pdf, "Scope de session")
        self._body(pdf,
            "Chaque session est liee a un scope derive de (catalog, env) defini dans config.yaml. "
            "Deux projets avec des catalogs differents ont des sessions independantes. "
            "TTL par defaut : 15 jours. Les sessions expirees sont purgees automatiquement au demarrage."
        )

    # ------------------------------------------------------------------
    # Section 3 — Routage
    # ------------------------------------------------------------------

    def _section_routing(self, pdf) -> None:
        pdf.add_page()
        self._h1(pdf, "3. Routage des questions")

        self._h2(pdf, "Principe : deterministe d'abord, LLM en fallback")
        self._body(pdf,
            "Le hub analyse la question en 3 etapes : "
            "(1) resolution des alias utilisateur, "
            "(2) detection d'intent par mots-cles normalises (minuscules, sans accents), "
            "(3) si aucun mot-cle ne matche et qu'un LLM est disponible, un seul appel "
            "LLM minimal classifie l'intent parmi les 5 categories."
        )
        pdf.ln(4)

        self._h2(pdf, "Table de routage par mots-cles")
        rows = [
            ("Intent detecte",  "Mots-cles reconnus (exemples)",                     "Agent cible"),
            ("lineage",         "d'ou, vient, provenance, upstream, origine, impact", "LineageAgent"),
            ("lineage",         "downstream, affecte, depend, come from",             "LineageAgent"),
            ("dictionary",      "que signifie, definition, glossaire, vocabulaire",   "DictionaryAgent"),
            ("quality",         "qualite, saine, anomalie, checks, doublons, nulls",  "QualityAgent"),
            ("builder",         "cree, genere, construis, yaml, pipeline, schema",    "BuilderAgent"),
            ("genbi",           "(aucun mot-cle specifique reconnu)  --  fallback",   "GenBIAgent"),
        ]
        self._table(pdf, rows)
        pdf.ln(4)

        self._h2(pdf, "Normalisation des textes")
        self._body(pdf,
            "Avant la comparaison, chaque question est normalisee : minuscules + "
            "suppression des accents (NFD). Ainsi 'D'ou vient ?' et 'd'ou vient ?' "
            "et 'd ou vient ?' sont tous traites de la meme facon."
        )
        pdf.ln(4)

        self._h2(pdf, "Fallback LLM (optionnel)")
        self._body(pdf,
            "Si llm_provider est fourni et qu'aucun mot-cle n'a matche, un prompt "
            "minimal est envoye : question + liste des 5 intents possibles. "
            "La reponse attendue est un seul mot parmi : genbi, lineage, quality, "
            "dictionary, builder. Si la reponse LLM est invalide ou si l'appel echoue, "
            "GenBIAgent est utilise en fallback ultime."
        )

    # ------------------------------------------------------------------
    # Section 4 — Les 5 agents
    # ------------------------------------------------------------------

    def _section_agents(self, pdf) -> None:
        pdf.add_page()
        self._h1(pdf, "4. Les 5 agents specialises")

        # 4.1 GenBIAgent
        self._h2(pdf, "4.1  GenBIAgent -- requetes KPI / metier")
        self._body(pdf,
            "Agent par defaut (fallback). Traduit les questions metier en requetes "
            "semantiques, les execute via Spark et retourne un resultat formate. "
            "Necessite un SemanticEngine et un LLMProvider."
        )
        self._body(pdf, "Exemple de questions reconnues :")
        self._bullet(pdf, [
            "Quel est le CA par region ce trimestre ?",
            "Montre-moi les ventes de juin 2026",
            "Calcule le taux de transformation par canal",
            "Compare le revenu brut entre EMEA et APAC",
        ])
        self._body(pdf, "Constructeur :")
        self._code_block(pdf, [
            "GenBIAgent(",
            "  semantic_engine,           # SemanticEngine (requis)",
            "  llm_provider,              # LLMProvider (requis)",
            "  history=False,             # activer le log SessionHistory",
            "  session_title='Analyse KPI'",
            ")",
        ])
        self._body(pdf, "Retourne : AgentResponse")
        pdf.ln(3)

        # 4.2 LineageAgent
        pdf.add_page()
        self._h2(pdf, "4.2  LineageAgent -- provenance et impact")
        self._body(pdf,
            "Analyse le lignage colonne-niveau. Consomme le LineageTracker et le "
            "DataDictionary. Aucun Spark ni LLM requis pour le coeur fonctionnel."
        )
        self._body(pdf, "Modes disponibles :")
        rows = [
            ("Mode",   "Description",                             "Exemple"),
            ("trace",  "Upstream : d'ou vient ce champ ?",       "D'ou vient amount_eur ?"),
            ("impact", "Downstream : quelles cols dependent ?",   "Quel impact si on modifie order_id ?"),
            ("render", "Visualisation du graphe complet",         "Affiche le diagramme de lineage"),
            ("lookup", "Lookup DataDictionary (definition)",      "Que signifie gross_revenue ?"),
        ]
        self._table(pdf, rows)
        self._body(pdf, "Constructeur :")
        self._code_block(pdf, [
            "LineageAgent(",
            "  schema_dict=None,    # dict schema SkiferEngine (optionnel)",
            "  llm_provider=None,   # LLMProvider (optionnel -- pour narration)",
            "  history=False,       # log dans SessionHistory",
            ")",
        ])
        self._body(pdf, "Retourne : LineageResponse")
        pdf.ln(3)

        # 4.3 QualityAgent
        pdf.add_page()
        self._h2(pdf, "4.3  QualityAgent -- sante des donnees")
        self._body(pdf,
            "Execute des checks de qualite sur les tables via le DataMonitor. "
            "Un backend (objet avec methode sql()) est requis pour executer les checks. "
            "LLM optionnel pour les resumes narratifs."
        )
        self._body(pdf, "Modes disponibles :")
        rows = [
            ("Mode",    "Description",                                  "Exemple"),
            ("check",   "Executer les checks sur une table",            "La table fact_orders est-elle saine ?"),
            ("history", "Historique des derniers checks",               "Evolution de la qualite de orders ?"),
            ("report",  "Generer un rapport formate text/json/html",    "Genere le rapport de qualite orders"),
        ]
        self._table(pdf, rows)
        self._body(pdf, "Constructeur :")
        self._code_block(pdf, [
            "QualityAgent(",
            "  backend,              # Backend (requis pour les checks live)",
            "  history_store=None,   # HistoryStore (optionnel)",
            "  llm_provider=None,    # LLMProvider (optionnel)",
            ")",
        ])
        self._body(pdf, "Retourne : QualityResponse")
        pdf.ln(3)

        # 4.4 DictionaryAgent
        pdf.add_page()
        self._h2(pdf, "4.4  DictionaryAgent -- glossaire metier")
        self._body(pdf,
            "Fournit les definitions, types et meta-attributs des champs depuis le "
            "DataDictionary. Peut enrichir les reponses avec le GlossaryReader. "
            "Aucun Spark requis."
        )
        self._body(pdf, "Modes disponibles :")
        rows = [
            ("Mode",   "Description",                              "Exemple"),
            ("lookup", "Definition d'un champ specifique",        "Que signifie gross_revenue ?"),
            ("list",   "Lister les champs d'une table",           "Liste les colonnes de fact_orders"),
            ("export", "Exporter le dictionnaire complet en JSON", "Exporte le dictionnaire"),
        ]
        self._table(pdf, rows)
        self._body(pdf, "Constructeur :")
        self._code_block(pdf, [
            "DictionaryAgent(",
            "  schema_dict=None,      # dict schema (optionnel)",
            "  llm_provider=None,     # LLMProvider (optionnel)",
            "  session_history=False, # log dans SessionHistory",
            ")",
        ])
        self._body(pdf, "Retourne : DictionaryResponse")
        pdf.ln(3)

        # 4.5 BuilderAgent
        pdf.add_page()
        self._h2(pdf, "4.5  BuilderAgent -- generation de pipelines YAML")
        self._body(pdf,
            "Genere des schemas YAML pour SkiferEngine depuis une description "
            "en langage naturel (mode LLM) ou un assistant interactif (wizard). "
            "Distinct du SemanticBuilder qui genere des modeles semantiques."
        )
        self._body(pdf, "Deux modes :")
        rows = [
            ("Mode",    "Description",                                      "LLM requis ?"),
            ("ask()",   "Description NL -> JSON structure -> YAML valide",  "Oui"),
            ("wizard()", "Assistant pas-a-pas en ligne de commande",        "Non"),
        ]
        self._table(pdf, rows)
        pdf.ln(2)
        self._body(pdf, "Exemple de dialogue hub/builder :")
        self._code_block(pdf, [
            'hub.ask("cree un pipeline qui joint orders et customers',
            '         sur customer_id, filtre region EMEA")',
            "",
            "# BuilderAgent genere :",
            "# tables:",
            "#   - name: catalog.silver.orders",
            "#     alias: ord",
            "#     filter: [region:equals:EMEA]",
            "#   - name: catalog.silver.customers",
            "#     alias: cust",
            "# join:",
            "#   - table_from: [ord, customer_id]",
            "#     table_to: [cust, id]",
            "#     type: left",
        ])
        self._body(pdf, "Constructeur :")
        self._code_block(pdf, [
            "BuilderAgent(",
            "  backend,          # Backend (requis)",
            "  catalog=None,     # prefixe catalogue",
            "  llm_provider=None # LLMProvider (requis pour mode ask())",
            ")",
        ])
        self._body(pdf, "Retourne : BuilderResponse")
        self._body(pdf, "Note : BuilderAgent.ask() a une signature differente des autres agents :")
        self._code_block(pdf, [
            "# Tous les autres agents :",
            "agent.ask(question: str) -> AgentResponse|...",
            "",
            "# BuilderAgent uniquement :",
            "builder.ask(description: str, output_dir: str = 'schemas') -> BuilderResponse",
        ])

    # ------------------------------------------------------------------
    # Section 5 — Historique
    # ------------------------------------------------------------------

    def _section_history(self, pdf) -> None:
        pdf.add_page()
        self._h1(pdf, "5. Historique de session (TTL + persistance)")

        self._body(pdf,
            "Chaque session AgenticHub est loguee dans un SessionHistory unique. "
            "Les sessions sont persistees sur disque et chargees automatiquement "
            "au prochain lancement pour le meme scope."
        )
        pdf.ln(3)

        self._h2(pdf, "Localisation des fichiers")
        self._code_block(pdf, [
            "~/.skifer_sessions/<scope_id>.json",
            "",
            "scope_id = f'{catalog}_{env}'   # derive de config.yaml",
            "# Exemple : my_catalog_DEV.json",
        ])
        pdf.ln(3)

        self._h2(pdf, "Attributs de SessionHistory")
        rows = [
            ("Attribut",    "Type",     "Description"),
            ("title",       "str",      "Titre de la session"),
            ("session_id",  "str",      "UUID4 genere automatiquement"),
            ("created_at",  "datetime", "Date/heure de creation UTC"),
            ("ttl_days",    "int",      "Duree de vie en jours (defaut : 15)"),
            ("is_expired",  "bool",     "True si TTL depasse"),
        ]
        self._table(pdf, rows)
        pdf.ln(3)

        self._h2(pdf, "Methodes cles")
        rows = [
            ("Methode",                   "Description"),
            ("add(entry)",                "Ajouter une entree HistoryEntry"),
            ("entries()",                 "Retourner la liste des entrees (copie)"),
            ("save(scope_id)",            "Persister dans ~/.skifer_sessions/"),
            ("load_or_create(scope_id)",  "Charger ou creer une nouvelle session"),
            ("purge_expired(scope_id)",   "Supprimer les sessions expirees"),
            ("to_json(path)",             "Exporter en JSON"),
            ("from_json(path)",           "Recharger depuis un fichier JSON"),
            ("to_pdf(path)",              "Exporter en PDF (fpdf2 requis)"),
        ]
        self._table(pdf, rows)
        pdf.ln(3)

        self._h2(pdf, "Exemple d'utilisation directe")
        self._code_block(pdf, [
            "from skifer.agentic.history import SessionHistory",
            "",
            "# Charger ou creer une session",
            "session = SessionHistory.load_or_create('my_catalog_DEV')",
            "",
            "# Verifier le TTL",
            "if session.is_expired:",
            "    session = SessionHistory()  # nouvelle session",
            "",
            "# Sauvegarder apres chaque interaction",
            "session.save('my_catalog_DEV')",
            "",
            "# Exporter",
            "session.to_pdf('rapport_session.pdf')",
            "session.to_json('session_backup.json')",
        ])

    # ------------------------------------------------------------------
    # Section 6 — UserProfile
    # ------------------------------------------------------------------

    def _section_profile(self, pdf) -> None:
        pdf.add_page()
        self._h1(pdf, "6. Profil utilisateur et alias semantiques")

        self._body(pdf,
            "Le UserProfile permet de definir des alias personnels qui sont "
            "resolus automatiquement avant chaque routage. Il memorise aussi "
            "les filtres par defaut et le format de sortie prefere."
        )
        pdf.ln(3)

        self._h2(pdf, "Fichier de profil")
        self._code_block(pdf, [
            "# ~/.skifer_profile.yaml",
            "version: 1",
            "aliases:",
            "  ventes: catalog.silver.orders",
            "  ca: amount_eur",
            "default_filters:",
            "  - region:equals:EMEA",
            "preferred_format: table",
            "blacklist_suggestions:",
            "  - env:equals:PROD",
        ])
        pdf.ln(3)

        self._h2(pdf, "Exemple de resolution d'alias")
        self._code_block(pdf, [
            "from skifer.agentic.user_profile import UserProfile",
            "from skifer.agentic.hub import AgenticHub",
            "",
            "profile = UserProfile.load()  # charge ~/.skifer_profile.yaml",
            "# Ou creer manuellement :",
            "profile = UserProfile(aliases={'ventes': 'catalog.silver.orders'})",
            "",
            "hub = AgenticHub(lineage_agent=..., profile=profile)",
            "",
            "# Question avec alias :",
            "hub.ask(\"d'ou vient ventes ?\")",
            "# Avant routage, 'ventes' est remplace par 'catalog.silver.orders'",
            "# -> equivalent a : \"d'ou vient catalog.silver.orders ?\"",
        ])
        pdf.ln(3)

        self._h2(pdf, "Methodes de gestion des alias")
        rows = [
            ("Methode",                     "Description"),
            ("resolve_alias(term)",         "Retourner le nom canonique ou None"),
            ("add_alias(term, canonical)",  "Ajouter et sauvegarder un alias"),
            ("reject_suggestion(term)",     "Blacklister une suggestion"),
            ("save(path)",                  "Sauvegarder le profil en YAML"),
            ("UserProfile.load(path)",      "Charger le profil (vide si absent)"),
        ]
        self._table(pdf, rows)
        pdf.ln(3)

        self._h2(pdf, "Normalisation des alias")
        self._body(pdf,
            "Les cles d'alias sont normalisees automatiquement (minuscules + suppression "
            "des accents). Ainsi 'CA', 'Ca', 'ca' et 'CA' sont tous le meme alias. "
            "La resolution utilise des frontieres de mots (\\b) pour eviter les "
            "remplacements de sous-chaines (ex : 'ca' ne modifie pas 'cascade')."
        )

    # ------------------------------------------------------------------
    # Section 7 — Formats de reponse
    # ------------------------------------------------------------------

    def _section_responses(self, pdf) -> None:
        pdf.add_page()
        self._h1(pdf, "7. Formats de reponse")

        self._body(pdf,
            "Chaque agent retourne un dataclass structure specifique. "
            "Le CLI les affiche automatiquement. En Python, vous pouvez les "
            "inspecter directement."
        )
        pdf.ln(3)

        self._h2(pdf, "AgentResponse (GenBIAgent)")
        rows = [
            ("Champ",                 "Type",   "Description"),
            ("question",              "str",    "Question originale"),
            ("mode",                  "str",    "query | view | ambiguous | error"),
            ("model_used",            "str",    "Cle du modele semantique selectionne"),
            ("explanation",           "str",    "Ce que l'agent a compris"),
            ("result",                "Any",    "FormattedResult (query) ou FQN vue (view)"),
            ("error",                 "str|None","Message d'erreur si mode=error"),
            ("clarification_question","str|None","Question si mode=ambiguous"),
            ("success",               "bool",   "True si mode=query ou view, sans erreur"),
        ]
        self._table(pdf, rows)
        pdf.ln(3)

        self._h2(pdf, "LineageResponse (LineageAgent)")
        rows = [
            ("Champ",       "Description"),
            ("question",    "Question originale"),
            ("mode",        "trace | impact | render | lookup"),
            ("table",       "Table ciblee"),
            ("column",      "Colonne ciblee"),
            ("edges",       "list[LineageEdge] — aretes du graphe"),
            ("diagram",     "Diagramme Mermaid / JSON / HTML (mode render)"),
            ("narrative",   "Explication en prose (LLM optionnel)"),
            ("field_entry", "FieldEntry du DataDictionary (mode lookup)"),
            ("error",       "Message d'erreur ou None"),
        ]
        self._table(pdf, rows)
        pdf.ln(3)

        self._h2(pdf, "QualityResponse (QualityAgent)")
        rows = [
            ("Champ",            "Description"),
            ("mode",             "check | history | report"),
            ("table",            "FQN de la table ciblee"),
            ("report",           "MonitorReport du dernier check"),
            ("history_reports",  "list[MonitorReport] (mode history)"),
            ("text_output",      "Resultat formate (text/json/html)"),
            ("passed",           "bool | None — True si aucun echec critique"),
        ]
        self._table(pdf, rows)
        pdf.ln(3)

        self._h2(pdf, "DictionaryResponse (DictionaryAgent)")
        rows = [
            ("Champ",      "Description"),
            ("mode",       "lookup | list | export"),
            ("table",      "Table ciblee"),
            ("column",     "Colonne ciblee"),
            ("entry",      "FieldEntry trouve (mode lookup)"),
            ("entries",    "list[FieldEntry] (mode list)"),
            ("text_output","Dictionnaire formate (mode export)"),
            ("suggestions","Noms proches si champ non trouve"),
        ]
        self._table(pdf, rows)
        pdf.ln(3)

        self._h2(pdf, "BuilderResponse (BuilderAgent)")
        rows = [
            ("Champ",                  "Description"),
            ("success",                "True si YAML produit et valide"),
            ("yaml_content",           "Contenu YAML du pipeline genere"),
            ("output_path",            "Chemin du fichier sauvegarde ou None"),
            ("error",                  "Message d'erreur si success=False"),
            ("clarification_question", "Question si une table/col est ambigue"),
        ]
        self._table(pdf, rows)

    # ------------------------------------------------------------------
    # Section 8 — Exemples de code
    # ------------------------------------------------------------------

    def _section_code_examples(self, pdf) -> None:
        pdf.add_page()
        self._h1(pdf, "8. Exemples de code Python")

        self._h2(pdf, "Usage minimal (sans Spark ni LLM)")
        self._code_block(pdf, [
            "from skifer.agentic.hub import AgenticHub",
            "from skifer.agentic.lineage_agent import LineageAgent",
            "from skifer.agentic.models import LineageResponse",
            "",
            "lineage = LineageAgent(schema_dict=my_core_schema)",
            "hub = AgenticHub(lineage_agent=lineage)",
            "",
            "resp = hub.ask(\"D'ou vient le champ amount_eur ?\")",
            "assert isinstance(resp, LineageResponse)",
            "print(resp.narrative)",
        ])
        pdf.ln(3)

        self._h2(pdf, "Hub complet avec tous les agents")
        self._code_block(pdf, [
            "from skifer.agentic.hub import AgenticHub",
            "from skifer.agentic.lineage_agent import LineageAgent",
            "from skifer.agentic.quality_agent import QualityAgent",
            "from skifer.agentic.dictionary_agent import DictionaryAgent",
            "from skifer.agentic.builder_agent import BuilderAgent",
            "from skifer.agentic.user_profile import UserProfile",
            "",
            "hub = AgenticHub(",
            "    semantic_engine=semantic_engine,",
            "    llm_provider=llm_provider,",
            "    lineage_agent=LineageAgent(schema_dict=schema),",
            "    quality_agent=QualityAgent(backend=backend),",
            "    dictionary_agent=DictionaryAgent(schema_dict=schema),",
            "    builder_agent=BuilderAgent(backend=backend, llm_provider=llm),",
            "    profile=UserProfile.load(),",
            ")",
        ])
        pdf.ln(3)

        self._h2(pdf, "Gestion de la session persistante")
        self._code_block(pdf, [
            "from skifer.agentic.history import SessionHistory",
            "",
            "scope = 'my_catalog_DEV'",
            "SessionHistory.purge_expired(scope)",
            "session = SessionHistory.load_or_create(scope)",
            "",
            "hub._history = session   # injecter la session dans le hub",
            "",
            "resp = hub.ask('quel est le CA ?')",
            "session.save(scope)      # persister apres chaque interaction",
        ])
        pdf.ln(3)

        self._h2(pdf, "Profil avec alias")
        self._code_block(pdf, [
            "from skifer.agentic.user_profile import UserProfile",
            "",
            "profile = UserProfile()",
            "profile.add_alias('ventes', 'catalog.silver.orders')",
            "profile.add_alias('CA',     'amount_eur')",
            "",
            "hub = AgenticHub(lineage_agent=lineage, profile=profile)",
            "",
            "# Maintenant on peut dire :",
            "hub.ask(\"d'ou vient le CA dans ventes ?\")",
            "# Resolu en : \"d'ou vient le amount_eur dans catalog.silver.orders ?\"",
        ])
        pdf.ln(3)

        self._h2(pdf, "Inspecter la reponse selon le type")
        self._code_block(pdf, [
            "from skifer.agentic.models import (",
            "    AgentResponse, LineageResponse, QualityResponse,",
            "    DictionaryResponse, BuilderResponse",
            ")",
            "",
            "resp = hub.ask(question)",
            "",
            "if isinstance(resp, AgentResponse):",
            "    if resp.success:",
            "        print(resp.result)",
            "    elif resp.mode == 'error':",
            "        print('Erreur :', resp.error)",
            "",
            "elif isinstance(resp, LineageResponse):",
            "    print('Edges :', resp.edges)",
            "    if resp.diagram:",
            "        print(resp.diagram)   # Mermaid",
            "",
            "elif isinstance(resp, QualityResponse):",
            "    print('Passe :', resp.passed)",
            "    print(resp.text_output)",
            "",
            "elif isinstance(resp, BuilderResponse):",
            "    if resp.success:",
            "        print(resp.yaml_content)",
        ])

    # ------------------------------------------------------------------
    # Section 9 — Reference rapide
    # ------------------------------------------------------------------

    def _section_quickref(self, pdf) -> None:
        pdf.add_page()
        self._h1(pdf, "9. Reference rapide des mots-cles")

        self._h2(pdf, "Mots-cles de routage")

        agents = [
            ("LineageAgent",    COLOR_TEAL,  [
                "d'ou, d'ou, vient, provenance, upstream, origine",
                "source de, where does, come from, provient",
                "impact, downstream, affecte, utilise par, depend",
                "depends on, affected",
            ]),
            ("DictionaryAgent", COLOR_BLUE, [
                "que signifie, definis, definition, glossaire",
                "vocabulaire, what is, what does mean, signification",
            ]),
            ("QualityAgent",    COLOR_RED,  [
                "qualite, saine, anomalie, checks, nulls",
                "doublons, health, monitor",
            ]),
            ("BuilderAgent",    COLOR_ORANGE, [
                "cree, genere, construis, yaml, pipeline",
                "schema, build, create",
            ]),
            ("GenBIAgent",      COLOR_GREEN, [
                "(aucun mot-cle specifique -- fallback)",
                "Exemples : quel est, montre-moi, calcule, compare",
            ]),
        ]

        for agent_name, color, keywords in agents:
            pdf.set_fill_color(*color)
            pdf.set_text_color(*COLOR_WHITE)
            pdf.set_font(FONT, "B", 11)
            pdf.cell(INNER_W, 8, _safe(f"  {agent_name}"),
                     new_x="LMARGIN", new_y="NEXT", fill=True)
            pdf.set_text_color(*COLOR_GRAY)
            pdf.set_font(FONT, "", 10)
            for kw in keywords:
                pdf.set_x(MARGIN + 5)
                pdf.cell(0, 7, _safe(kw), new_x="LMARGIN", new_y="NEXT")
            pdf.ln(2)

        pdf.ln(4)
        self._h2(pdf, "Signatures de reference")
        self._code_block(pdf, [
            "# Tous les agents (sauf BuilderAgent) :",
            "agent.ask(question: str) -> AgentResponse | LineageResponse | ...",
            "",
            "# BuilderAgent specifique :",
            "builder.ask(description: str, output_dir: str = 'schemas') -> BuilderResponse",
            "builder.wizard() -> str   # REPL interactif -- ne pas appeler depuis hub.ask()",
            "",
            "# Hub :",
            "hub.ask(question: str, **kwargs) -> HubResponse",
            "hub.history  -> SessionHistory",
        ])

        pdf.ln(4)
        self._h2(pdf, "Imports essentiels")
        self._code_block(pdf, [
            "from skifer.agentic import (",
            "    AgenticHub, GenBIAgent, LineageAgent, QualityAgent,",
            "    DictionaryAgent, BuilderAgent,",
            "    AgentResponse, LineageResponse, QualityResponse,",
            "    DictionaryResponse, BuilderResponse,",
            "    SessionHistory, HistoryExporter, UserProfile,",
            ")",
        ])

        # Pied de page final
        pdf.ln(8)
        pdf.set_draw_color(*COLOR_TEAL)
        pdf.set_line_width(0.5)
        pdf.line(MARGIN, pdf.get_y(), PAGE_W - MARGIN, pdf.get_y())
        pdf.ln(4)
        pdf.set_font(FONT, "I", 9)
        pdf.set_text_color(150, 150, 150)
        pdf.cell(0, 6,
                 _safe("Skifer -- skifer.cli:main | pip install skifer"),
                 new_x="LMARGIN", new_y="NEXT", align="C")

    # ------------------------------------------------------------------
    # Composants de mise en page
    # ------------------------------------------------------------------

    def _h1(self, pdf, text: str) -> None:
        pdf.set_fill_color(*COLOR_BLUE)
        pdf.set_text_color(*COLOR_WHITE)
        pdf.set_font(FONT, "B", 16)
        pdf.cell(INNER_W, 11, _safe(f"  {text}"),
                 new_x="LMARGIN", new_y="NEXT", fill=True)
        pdf.set_text_color(*COLOR_GRAY)
        pdf.ln(4)

    def _h2(self, pdf, text: str) -> None:
        pdf.set_text_color(*COLOR_TEAL)
        pdf.set_font(FONT, "B", 12)
        pdf.cell(0, 9, _safe(text), new_x="LMARGIN", new_y="NEXT")
        pdf.set_text_color(*COLOR_GRAY)
        pdf.set_draw_color(*COLOR_TEAL)
        pdf.set_line_width(0.3)
        pdf.line(MARGIN, pdf.get_y(), PAGE_W - MARGIN, pdf.get_y())
        pdf.ln(3)

    def _body(self, pdf, text: str) -> None:
        pdf.set_font(FONT, "", 10)
        pdf.set_text_color(*COLOR_GRAY)
        pdf.multi_cell(INNER_W, 6, _safe(text))
        pdf.ln(1)

    def _bullet(self, pdf, items: list[str]) -> None:
        pdf.set_font(FONT, "", 10)
        pdf.set_text_color(*COLOR_GRAY)
        for item in items:
            pdf.set_x(MARGIN + 5)
            pdf.cell(5, 6, "-", new_x="END", new_y="TOP")
            pdf.multi_cell(INNER_W - 5, 6, _safe(item))
        pdf.ln(2)

    def _code_block(self, pdf, lines: list[str]) -> None:
        pdf.set_fill_color(245, 245, 245)
        pdf.set_draw_color(200, 200, 200)
        pdf.set_line_width(0.2)

        # Hauteur du bloc
        h_per_line = 5.5
        block_h = len(lines) * h_per_line + 4
        pdf.rect(MARGIN, pdf.get_y(), INNER_W, block_h, style="FD")

        pdf.set_font("Courier", "", 8)
        pdf.set_text_color(50, 50, 50)
        pdf.set_x(MARGIN + 3)
        y_start = pdf.get_y() + 2

        for i, line in enumerate(lines):
            pdf.set_xy(MARGIN + 3, y_start + i * h_per_line)
            pdf.cell(INNER_W - 6, h_per_line, _safe(line))

        pdf.set_y(y_start + len(lines) * h_per_line + 2)
        pdf.set_font(FONT, "", 10)
        pdf.set_text_color(*COLOR_GRAY)
        pdf.ln(2)

    def _table(self, pdf, rows: list[tuple]) -> None:
        if not rows:
            return

        n_cols = len(rows[0])
        col_w = INNER_W // n_cols

        for i, row in enumerate(rows):
            is_header = (i == 0)
            fill = is_header
            pdf.set_fill_color(*COLOR_BLUE if is_header else (250, 250, 250))
            pdf.set_text_color(*COLOR_WHITE if is_header else COLOR_GRAY)
            pdf.set_font(FONT, "B" if is_header else "", 9)
            pdf.set_draw_color(200, 200, 200)
            pdf.set_line_width(0.2)
            for cell in row:
                pdf.cell(col_w, 7, _safe(str(cell))[:40], border=1,
                         new_x="END", new_y="TOP", fill=fill)
            pdf.ln(7)
        pdf.set_text_color(*COLOR_GRAY)
        pdf.ln(2)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    out = os.path.join(os.path.dirname(__file__), "agentic_hub_guide.pdf")
    HubGuidePDF().generate(out)
