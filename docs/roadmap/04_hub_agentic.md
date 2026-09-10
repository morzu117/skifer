# Piste 4 — Hub agentic

> Priorite : 4/5 — Federe les capacites des pistes 3 et 5 dans une interface conversationnelle.
> Statut : En cours d'implementation — agents specialises fusionnes sur main (5 mai 2026)

## Nature du client

Le SkiferHub est un **REPL conversationnel dans le terminal** (`skifer hub`).
Interface personnelle, locale — pas de composant serveur, pas de partage entre utilisateurs.

---

## Demarrage de session

Au lancement, le hub propose un choix explicite :

```
SkiferHub v0.x — catalog: my_catalog (DEV)
─────────────────────────────────────────────
  [1] Nouvelle session  (efface la session precedente)
  [2] Reprendre la session du 2026-04-24 (11 messages)

Choix :
```

- **Nouvelle session** : flush complet de l'historique de session (le profil utilisateur, lui, est conserve).
- **Reprendre** : recharge la derniere session non expiree (TTL 15j) pour ce scope de config.

---

## Scope de session : lie au config.yaml

Chaque instance `config.yaml` definit un scope independant.
Trois instances = trois catalogues = trois sessions distinctes, sans interference.

```yaml
# config.yaml
catalog: my_catalog   # identifiant du scope de session
env: DEV
```

L'identifiant de session est derive de `(catalog, env)` — deux utilisateurs sur le meme catalog/env
partagent le meme scope logique, mais leurs profils restent separes (locaux).

---

## Formats de sortie

| Type de resultat | Format produit | Affichage terminal |
|---|---|---|
| KPI / tableau de donnees | Table `rich` + CSV exportable | Oui |
| Graphique / analyse visuelle | PNG | Chemin affiche, ouverture optionnelle |
| Rapport complet | PDF (`fpdf2`) | Chemin affiche |
| Reponse textuelle | Texte formate `rich` | Oui |

> Note : la structure de prompt du `GenBIAgent` (aujourd'hui unique) devra etre revisitee
> pour couvrir ces differents types de sortie selon l'intent detecte.

---

---

## Point d'acces

Le hub est exclusivement un **client terminal**. Commande declaree dans `pyproject.toml` :

```toml
[project.scripts]
skifer = "skifer.cli:main"
```

```bash
skifer hub                              # lance le REPL (config.yaml du repertoire courant)
skifer hub --config path/config.yaml   # scope explicite
```

Les **notebooks Databricks** n'utilisent pas le hub — ils importent directement les briques
(`SkiferEngine`, `SemanticEngine`, `LineageTracker`, etc.), les memes que les agents consomment.
Le hub n'est qu'une interface conversationnelle par-dessus ces briques, pas un nouveau moteur.

---

## Ce qui existe deja (mai 2026)

Tous les agents specialises sont implementes et fusionnes sur `main` :

```
agentic/
  agent.py            # GenBIAgent — pipeline LLM 2 etapes (model selection -> SemanticQuery)
  resolver.py         # QueryResolver — SQL deterministe, zero LLM, zero SQL hallucine
  lineage_agent.py    # LineageAgent — "d'ou vient ce champ ?" (fusionne)
  quality_agent.py    # QualityAgent — "cette table est-elle saine ?" (fusionne)
  builder_agent.py    # BuilderAgent — wizard + LLM mode pour generer des YAML (fusionne)
  dictionary_agent.py # DictionaryAgent — definition, type, sample distinct (fusionne)
  orchestrator.py     # OrchestratorExporter — genere Airflow DAG / DAB YAML / Python script
  models.py           # AgentResponse, FormattedResult, LineageResponse, QualityResponse, DictionaryResponse
  history.py          # SessionHistory + HistoryEntry
  exporter.py         # HistoryExporter -> PDF via fpdf2
```

**Ce qui reste a implementer sur cette branche :**
- `hub.py` — AgenticHub (routeur unifie)
- `cli.py` — commande `skifer hub` (REPL terminal)
- `user_profile.py` — UserProfile + Semantic Alias Layer
- Extension `history.py` — TTL + session persistante
- `agentic/tools/` — outils partages (refactoring interne, optionnel)

---

## Vision : un espace unifie pour toutes les capacites agentiques

### Structure cible

```
agentic/
  hub.py              # AgenticHub — point d'entree unique, routage des requetes
  agent.py            # GenBIAgent (existant)
  resolver.py         # QueryResolver (existant)
  lineage_agent.py    # Agent specialise lineage ("d'ou vient ce champ ?")
  quality_agent.py    # Agent specialise qualite ("cette table est-elle saine ?")
  builder_agent.py    # Agent specialise construction ("cree-moi un YAML pour ...")
  dictionary_agent.py # Agent specialise dictionnaire ("definis-moi ce champ")
  models.py           # Modeles partages
  history.py          # Historique partage
  exporter.py         # Export partage
  tools/              # Outils partages entre agents (pattern MCP-like)
    catalog_tool.py   # Navigation catalogue
    lineage_tool.py   # Interrogation du graphe de lineage
    quality_tool.py   # Verification de qualite
```

---

## AgenticHub — routeur

```python
class AgenticHub:
    """Point d'entree pour toutes les interactions agentiques."""

    def __init__(self, core_engine, semantic_engine, lineage_tracker=None, data_monitor=None):
        self.genbi = GenBIAgent(core_engine, semantic_engine)
        self.lineage = LineageAgent(lineage_tracker) if lineage_tracker else None
        self.quality = QualityAgent(core_engine, data_monitor) if data_monitor else None
        self.builder = BuilderAgent(core_engine, semantic_engine)
        self.dictionary = DictionaryAgent(lineage_tracker) if lineage_tracker else None
        self.history = SessionHistory()

    def ask(self, question: str, context: dict = None) -> AgentResponse:
        """
        Analyse la question, route vers le bon agent.

        Le routage peut etre :
        - Deterministe (mots-cles : "lineage", "d'ou vient", "qualite", etc.)
        - LLM-assisted (classification de l'intent par un petit prompt)
        """
        ...
```

### Strategie de routage

| Intent detecte | Agent cible | Exemple de question |
|---|---|---|
| Requete metier / KPI | `GenBIAgent` | "Quel est le CA par region ce trimestre ?" |
| Provenance / lineage | `LineageAgent` | "D'ou vient le champ amount_eur ?" |
| Qualite / sante | `QualityAgent` | "La table fact_orders est-elle saine ?" |
| Construction pipeline | `BuilderAgent` | "Cree un YAML pour joindre orders et customers" |
| Definition / dictionnaire | `DictionaryAgent` | "Que signifie gross_revenue ?" |

---

## Agents specialises

### LineageAgent

- Consomme le `LineageTracker` (piste 3).
- Repond a "d'ou vient ce champ ?" en traversant le graphe.
- Peut generer un diagramme Mermaid en reponse.

### QualityAgent

- Consomme le `DataMonitor` (piste 5).
- Repond a "cette table est-elle saine ?" en executant les checks.
- Peut comparer avec les resultats historiques ("est-ce que ca s'est degrade ?").

### BuilderAgent ⭐ brique centrale

Agent le plus strategique du hub — comble le gap entre intention metier et pipeline operationnel.

- Genere un **YAML de pipeline** (schemas Bronze→Silver→Gold) depuis une description en langage naturel.
- Distinct du `SemanticBuilder` existant (qui genere des modeles semantiques) : ici on cree
  les schemas consommes par `SkiferEngine` (joins, filtres, business_rules, quality_checks...).
- Valide le YAML genere via `parse_schema()` avant de le proposer.
- Propose a l'user de sauvegarder dans `schemas/` avec un nom derive de l'intent.
- Peut enrichir automatiquement avec `quality_checks` et `observability` selon le profil utilisateur.

```
User : "cree un pipeline qui joint orders et customers sur customer_id,
        filtre sur region EMEA, et calcule le CA en euros"

Hub  : Voici le schema genere — schemas/silver/orders_customers_emea.yaml
       [affiche le YAML complet]
       Je sauvegarde ? [Oui] [Modifier] [Non]
```

### DictionaryAgent

- Consomme le `DataDictionary` (piste 3).
- Repond avec la definition, le type, les meta-attributs, et un sample distinct.
- Peut enrichir la reponse avec le glossaire (`GlossaryReader`).

---

## Outils partages (pattern tools)

Les agents partagent un ensemble d'outils reutilisables :

```python
# agentic/tools/catalog_tool.py
class CatalogTool:
    """Navigation catalogue — list schemas, tables, columns."""
    def list_tables(self, schema: str) -> list[str]: ...
    def describe_table(self, fqn: str) -> dict: ...

# agentic/tools/lineage_tool.py
class LineageTool:
    """Interrogation du graphe de lineage."""
    def trace_field(self, table: str, column: str) -> list[LineageEdge]: ...
    def impact_analysis(self, table: str, column: str) -> list[LineageEdge]: ...

# agentic/tools/quality_tool.py
class QualityTool:
    """Verification de qualite."""
    def run_checks(self, fqn: str) -> list[CheckResult]: ...
    def get_history(self, fqn: str, last_n: int = 10) -> list[CheckResult]: ...
```

---

## Historique et export

L'historique unifie (`SessionHistory`) stocke toutes les interactions quel que soit l'agent cible.
L'export PDF existant (`HistoryExporter`) est etendu pour inclure les diagrammes de lineage et les rapports de qualite.

---

## Profil utilisateur appris (UserProfile)

> Principe : le hub observe les habitudes de l'utilisateur et en deduit un profil, sans que ce dernier
> ait a le configurer manuellement.

### Apprentissage comportemental

Le hub analyse les filtres et tables utilises au fil des requetes pour detecter des patterns recurrents :
- meme filtre applique systematiquement (`region=EMEA`, `env=PROD`, ...)
- memes tables requetees ensemble
- meme format de reponse prefere

Apres un seuil (ex. 3 occurrences du meme pattern), le hub interpelle l'utilisateur :

```
Hub : J'ai remarque que tu filtres souvent sur region = 'EMEA'.
      Veux-tu que je l'applique par defaut dans tes prochaines requetes ?
      [Oui, garder] [Modifier] [Non, ignorer]
```

Les regles validees sont persistees dans le profil (`~/.skifer_profile.yaml` ou `.skifer_user`).
Les regles rejetees sont blacklistees pour ne plus etre proposees.

### Structure du profil

```yaml
# .skifer_profile.yaml
version: 1
user_rules:
  default_filters:
    - "region:equals:EMEA"       # valide par l'utilisateur le 2026-04-29
  preferred_format: dataframe    # markdown | dataframe | pdf
  favorite_tables:
    - catalog.silver.orders
    - catalog.gold.fact_revenue
user_aliases:                    # voir section Semantic Alias Layer
  ventes: catalog.silver.orders
  CA: amount_eur
blacklist_suggestions:
  - "env:equals:PROD"            # l'user a explicitement refuse cette suggestion
```

---

## Semantic Alias Layer (couche de vocabulaire utilisateur)

> Principe : combler le gap entre le langage metier de l'entreprise et le langage naturel de l'utilisateur.

Le SemanticEngine expose des modeles YAML nommes selon la convention interne de l'entreprise.
L'utilisateur peut avoir ses propres termes. Le hub construit une couche d'alias personnels.

### Construction des alias

Quand le hub identifie une ambiguite ou resout une correspondance table/colonne, il propose de la memoriser :

```
User : "donne-moi le CA du mois de juin dans la table des ventes"
Hub  : J'ai utilise catalog.silver.orders pour 'table des ventes' et amount_eur pour 'CA'.
       C'est correct ? Tu veux que je retienne ces correspondances pour tes prochaines requetes ?
       [Oui] [Modifier] [Non]
```

Une fois valide, l'utilisateur peut dire "la table des ventes" sans jamais re-preciser le FQN.

### Resolution des alias

Le `QueryResolver` est enrichi d'une etape de pre-resolution :
1. Remplacer les alias utilisateur par les noms canoniques (table FQN, colonne reelle)
2. Passer au resolver deterministe habituel

```python
# agentic/user_profile.py
class UserProfile:
    def resolve_alias(self, term: str) -> str | None:
        """Retourne le nom canonique si l'alias est connu, sinon None."""
        ...

    def suggest_alias(self, term: str, canonical: str) -> PendingSuggestion:
        """Cree une suggestion en attente de validation utilisateur."""
        ...

    def confirm(self, suggestion_id: str) -> None: ...
    def reject(self, suggestion_id: str) -> None: ...
```

---

## Memoire de session avec TTL

La `SessionHistory` existante est etendue avec :
- un **timestamp de creation** par session
- une **TTL configurable** (defaut : 15 jours)
- un mecanisme de **purge automatique** au demarrage du hub

```python
# agentic/history.py (extension)
@dataclass
class SessionHistory:
    session_id: str
    created_at: datetime
    ttl_days: int = 15
    entries: list[HistoryEntry] = field(default_factory=list)

    @property
    def is_expired(self) -> bool:
        return (datetime.utcnow() - self.created_at).days > self.ttl_days
```

Les sessions expirees sont supprimees silencieusement. Le profil utilisateur (`UserProfile`), lui,
est permanent — seule la memoire conversationnelle a une duree de vie limitee.

---

## Dependances avec les autres pistes

| Piste | Impact |
|---|---|
| Piste 1 (Multi-plateforme) | Les outils catalogue/qualite dependend du backend abstrait |
| Piste 2 (Client graphique) | Le client graphique peut embarquer un chat connecte au hub |
| Piste 3 (Lineage) | **Dependance forte** — LineageAgent et DictionaryAgent consomment le LineageTracker et le DataDictionary |
| Piste 5 (Observabilite) | **Dependance forte** — QualityAgent consomme le DataMonitor |

---

## Plan d'implementation (branche `agentic_hub`)

> **Instructions pour Copilot** : implementer phase par phase dans l'ordre indique.
> Apres chaque phase : `pytest tests/ -x --tb=short` doit etre vert. Un commit par phase.
> Ne pas modifier `pyproject.toml` version. Ajouter une entree dans `CHANGELOG.md [Unreleased]` apres chaque phase.

---

### Signatures de reference (ne pas modifier)

```python
# Tous les agents ont ask(question: str) — SAUF BuilderAgent
GenBIAgent.ask(question: str) -> AgentResponse
LineageAgent.ask(question: str) -> LineageResponse
QualityAgent.ask(question: str) -> QualityResponse
DictionaryAgent.ask(question: str) -> DictionaryResponse
BuilderAgent.ask(description: str, output_dir: str = "schemas") -> BuilderResponse  # signature differente !
BuilderAgent.wizard() -> str  # REPL interactif, ne pas appeler depuis hub.ask()

# Constructeurs — tous les arguments sont optionnels sauf ceux marques (*)
GenBIAgent(semantic_engine*, llm_provider*, history=False, session_title="Analyse KPI")
LineageAgent(schema_dict=None, llm_provider=None, history=False)
QualityAgent(backend*, history_store=None, llm_provider=None)
DictionaryAgent(schema_dict=None, llm_provider=None, session_history=False)
BuilderAgent(backend*, catalog=None, llm_provider=None)
```

---

### Phase A — AgenticHub + routeur (`hub.py`)

**Fichiers :**
- `src/skifer/agentic/hub.py` — CREER
- `src/skifer/agentic/__init__.py` — ajouter `AgenticHub` a `__all__`
- `tests/test_hub.py` — CREER

**Classe `AgenticHub` :**

```python
from __future__ import annotations
from typing import Any
from .agent import GenBIAgent
from .lineage_agent import LineageAgent
from .quality_agent import QualityAgent
from .dictionary_agent import DictionaryAgent
from .builder_agent import BuilderAgent
from .models import AgentResponse, LineageResponse, QualityResponse, DictionaryResponse, BuilderResponse
from .history import SessionHistory

HubResponse = AgentResponse | LineageResponse | QualityResponse | DictionaryResponse | BuilderResponse

class AgenticHub:
    def __init__(
        self,
        semantic_engine=None,          # SemanticEngine — requis pour GenBIAgent
        llm_provider=None,             # LLMProvider — optionnel, active LLM routing + agents LLM
        lineage_agent: LineageAgent | None = None,
        quality_agent: QualityAgent | None = None,
        dictionary_agent: DictionaryAgent | None = None,
        builder_agent: BuilderAgent | None = None,
        genbi_agent: GenBIAgent | None = None,
        session_title: str = "SkiferHub",
    ) -> None: ...

    def ask(self, question: str, **kwargs) -> HubResponse: ...
    # kwargs transmis a l'agent cible si pertinent (ex: output_dir pour BuilderAgent)
```

**Routage deterministe (mots-cles — zero LLM) :**

| Mots-cles detectes (normalises, sans accents, minuscules) | Agent cible |
|---|---|
| `d'ou`, `d'où`, `vient`, `provenance`, `upstream`, `origine`, `source de`, `where does`, `come from` | `LineageAgent` |
| `impact`, `downstream`, `affecte`, `utilise par`, `depend` | `LineageAgent` |
| `que signifie`, `definis`, `definition`, `glossaire`, `vocabulaire`, `what is`, `what does mean` | `DictionaryAgent` |
| `qualite`, `saine`, `anomalie`, `checks`, `nulls`, `doublons`, `health`, `monitor` | `QualityAgent` |
| `cree`, `genere`, `construis`, `yaml`, `pipeline`, `schema`, `build`, `create` | `BuilderAgent` |
| (aucun match) | `GenBIAgent` (fallback) |

Fonction de normalisation a utiliser (deja presente dans lineage_agent.py) :
```python
import unicodedata
def _normalize(text: str) -> str:
    text = unicodedata.normalize("NFD", text.lower())
    return "".join(c for c in text if unicodedata.category(c) != "Mn")
```

**Fallback LLM (si llm_provider fourni et mots-cles ambigus) :**
- Un seul appel LLM, prompt minimal : question + liste des 5 intents possibles
- Retourner l'intent parmi : `"genbi"`, `"lineage"`, `"quality"`, `"dictionary"`, `"builder"`
- Si l'agent cible est None (non injecte dans le hub) : retourner un `AgentResponse` en mode `"error"` avec message explicite

**Comportement si agent non disponible :**
```python
# Si l'agent cible est None, retourner :
AgentResponse(
    question=question,
    mode="error",
    error=f"Agent '{intent}' non configure dans ce hub.",
)
```

**Tests requis (`tests/test_hub.py`) — tous sans LLM ni Spark :**
```python
def test_route_lineage_deterministic()    # "d'ou vient amount_eur ?" → LineageAgent
def test_route_dictionary_deterministic() # "que signifie gross_revenue ?" → DictionaryAgent
def test_route_quality_deterministic()    # "la table est-elle saine ?" → QualityAgent
def test_route_builder_deterministic()    # "cree un yaml pour orders" → BuilderAgent
def test_route_fallback_genbi()           # "quel est le CA ?" → GenBIAgent
def test_agent_not_configured()           # agent None → AgentResponse mode="error"
```

Mocker chaque agent avec `unittest.mock.MagicMock()` qui retourne une reponse factice.

---

### Phase B — Extension `SessionHistory` (TTL + persistance fichier)

**Fichier a modifier : `src/skifer/agentic/history.py`**

Ajouter dans `SessionHistory.__init__` :
```python
import uuid, pathlib, datetime as dt

class SessionHistory:
    def __init__(
        self,
        session_title: str = "Analyse KPI",
        session_id: str | None = None,       # NOUVEAU — generer uuid4 si None
        ttl_days: int = 15,                  # NOUVEAU
    ):
        self.title = session_title
        self.session_id = session_id or str(uuid.uuid4())
        self.created_at = dt.datetime.utcnow()   # remplace l'ancien isoformat()
        self.ttl_days = ttl_days
        self._entries: list[HistoryEntry] = []

    @property
    def is_expired(self) -> bool:
        return (dt.datetime.utcnow() - self.created_at).days > self.ttl_days
```

Ajouter des methodes de persistance **sans casser l'API existante** (`to_json` / `from_json` restent) :

```python
_SESSIONS_DIR = pathlib.Path.home() / ".skifer_sessions"

@classmethod
def load_or_create(cls, scope_id: str, ttl_days: int = 15) -> "SessionHistory":
    """Charge la session persistee pour ce scope, ou en cree une nouvelle."""
    ...

def save(self, scope_id: str) -> None:
    """Persiste la session dans ~/.skifer_sessions/<scope_id>.json"""
    ...

@staticmethod
def purge_expired(scope_id: str, ttl_days: int = 15) -> int:
    """Supprime les sessions expirees. Retourne le nombre supprime."""
    ...
```

- `scope_id` = `f"{catalog}_{env}"` (fourni par le caller, jamais calcule dans history.py)
- `~/.skifer_sessions/` cree automatiquement si absent (`mkdir(parents=True, exist_ok=True)`)
- La serialisation JSON doit inclure `session_id`, `created_at` (ISO 8601), `ttl_days`, `entries`

**Tests requis (`tests/test_history.py`) — pas de filesystem reel, utiliser `tmp_path` pytest :**
```python
def test_session_not_expired()
def test_session_is_expired()
def test_save_and_load(tmp_path)
def test_purge_expired(tmp_path)
def test_backward_compat_existing_api()  # to_json / from_json toujours fonctionnels
```

---

### Phase C — `UserProfile` + Semantic Alias Layer (`user_profile.py`)

**Fichier a creer : `src/skifer/agentic/user_profile.py`**

```python
from __future__ import annotations
import pathlib, yaml
from dataclasses import dataclass, field

_PROFILE_PATH = pathlib.Path.home() / ".skifer_profile.yaml"

@dataclass
class UserProfile:
    aliases: dict[str, str] = field(default_factory=dict)
    # cle = terme utilisateur (normalise), valeur = nom canonique (FQN ou colonne)
    default_filters: list[str] = field(default_factory=list)
    preferred_format: str = "table"          # "table" | "kpi" | "text_analysis"
    blacklist_suggestions: list[str] = field(default_factory=list)

    def resolve_alias(self, term: str) -> str | None:
        """Retourne le nom canonique si l'alias est connu, sinon None."""
        ...

    def add_alias(self, term: str, canonical: str) -> None:
        """Ajoute un alias valide et sauvegarde le profil."""
        ...

    def reject_suggestion(self, term: str) -> None:
        """Blackliste un terme pour ne plus le proposer."""
        ...

    def save(self, path: pathlib.Path = _PROFILE_PATH) -> None: ...

    @classmethod
    def load(cls, path: pathlib.Path = _PROFILE_PATH) -> "UserProfile":
        """Charge le profil depuis YAML. Retourne un profil vide si le fichier n'existe pas."""
        ...
```

**Integration dans `AgenticHub.ask()` (modifier Phase A apres coup) :**
```python
def ask(self, question: str, **kwargs) -> HubResponse:
    # 1. Pre-resolution des alias
    resolved = question
    if self._profile:
        for term, canonical in self._profile.aliases.items():
            resolved = resolved.replace(term, canonical)

    # 2. Routage + dispatch avec `resolved`
    ...
```

`AgenticHub.__init__` recoit un argument supplementaire :
```python
profile: UserProfile | None = None   # NOUVEAU dans Phase C
```

**Tests requis (`tests/test_user_profile.py`) :**
```python
def test_resolve_known_alias()
def test_resolve_unknown_returns_none()
def test_add_alias_persists(tmp_path)
def test_reject_suggestion()
def test_load_missing_file_returns_empty()
def test_hub_resolves_alias_before_routing()  # integration avec AgenticHub
```

---

### Phase D — CLI REPL (`cli.py`)

**Fichiers :**
- `src/skifer/cli.py` — CREER
- `pyproject.toml` — ajouter section `[project.scripts]` :
  ```toml
  [project.scripts]
  skifer = "skifer.cli:main"
  ```
- `tests/test_cli.py` — CREER (tests smoke uniquement)

**Structure du module `cli.py` :**

```python
"""
CLI Skifer — commande `skifer hub`.

Usage :
    skifer hub                           # config.yaml du repertoire courant
    skifer hub --config path/config.yaml
    skifer hub --new-session             # force une nouvelle session
"""
import argparse, sys
from .agentic.hub import AgenticHub
from .agentic.history import SessionHistory
from .agentic.user_profile import UserProfile
from .core.config import ConfigurationManager

def main() -> None:
    parser = argparse.ArgumentParser(prog="skifer")
    subparsers = parser.add_subparsers(dest="command")

    hub_parser = subparsers.add_parser("hub", help="Lance le REPL conversationnel")
    hub_parser.add_argument("--config", default="config.yaml")
    hub_parser.add_argument("--new-session", action="store_true")

    args = parser.parse_args()
    if args.command == "hub":
        _run_hub(args)
    else:
        parser.print_help()

def _run_hub(args) -> None:
    """Boucle REPL principale."""
    ...
```

**UX du REPL (utiliser `rich` pour l'affichage) :**

```
SkiferHub v0.x — catalog: my_catalog (DEV)
─────────────────────────────────────────────
  [1] Nouvelle session
  [2] Reprendre la session du 2026-05-01 (11 messages)

Choix [1/2] :

[skifer] > d'ou vient le champ amount_eur ?
...reponse affichee...

[skifer] > exit
Au revoir.
```

Regles UX :
- Commandes speciales reconnues : `exit`, `quit`, `help`, `history` (affiche entrees recentes), `export pdf`
- Affichage `AgentResponse` :
  - mode `query` avec FormattedResult de type TABLE → `rich.table.Table`
  - mode `query` avec FormattedResult de type KPI → ligne simple `rich.text`
  - mode `error` → message rouge avec `rich.console.Console().print("[red]...")`
- Affichage `LineageResponse` → texte ou diagramme Mermaid brut
- Affichage `QualityResponse` → `text_output` pre-formate
- Affichage `DictionaryResponse` → `text_output` pre-formate
- `Ctrl+C` → quitter proprement (pas de stack trace)
- Sauvegarder la session apres chaque reponse (`session.save(scope_id)`)

**Tests requis (`tests/test_cli.py`) — smoke tests uniquement :**
```python
def test_hub_help_exits_zero()      # skifer hub --help -> exit code 0
def test_hub_no_config_exits_clean() # pas de config.yaml -> message clair, pas de traceback
```

Les tests CLI utilisent `subprocess.run` ou `click.testing.CliRunner` si applicable.
Ne pas tester le REPL interactif (boucle infinie) — tester uniquement les chemins de sortie rapide.

---

### Ordre d'execution

```
Phase A → Phase B → Phase C → Phase D
```

Chaque phase = un commit sur `agentic_hub` avec message format `feat(agentic_hub): Phase X — ...`.
`pytest tests/ -x --tb=short` doit etre vert apres chaque phase.
Ajouter une entree dans `CHANGELOG.md` sous `## [Unreleased]` apres chaque phase.
