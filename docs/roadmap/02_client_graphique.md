# Piste 2 — Client graphique

> Priorite : 5/5 — Forte valeur ajoutee mais gros investissement frontend. A faire quand le core est stabilise.
> Statut : Reflexion initiale (21 avril 2026)

---

## Vision

Un outil visuel qui :
- Se connecte a la plateforme (Databricks, Snowflake...) pour lire les metadonnees (catalogues, schemas, tables, colonnes, types).
- Permet de construire visuellement un pipeline YAML (drag & drop de tables, definition de joins, filtres, transformations).
- Genere des regles metier simples sous forme de code Python (ou dans le langage du backend cible si piste 1 implementee).
- Exporte un fichier YAML valide + un fichier de regles pret a etre utilise par le framework.

---

## Choix technologiques recommandes

| Composant | Recommandation | Justification |
|---|---|---|
| Frontend | **React + TypeScript** | Ecosysteme riche pour les UI de type "builder", nombreuses libs de drag & drop (React Flow, dnd-kit) |
| Backend API | **FastAPI (Python)** | Reutilise directement le code Skifer existant, pas de traduction de modeles |
| Communication | REST + WebSocket (pour le preview live) | Simple, standard |
| Packaging | **Electron** ou **app web deployee** | Electron si on veut un outil desktop autonome ; webapp si on veut l'integrer a un portail |

---

## Fonctionnalites cles

### 1. Catalogue Explorer
Navigation arborescente catalog > schema > table > colonnes (types, stats).
Le backend expose les metadonnees via des endpoints REST qui appellent le `ExecutionBackend` (piste 1).

### 2. YAML Builder visuel
Canvas avec les tables comme noeuds, les joins comme aretes, panneau lateral pour filtres/transformations.
Librairies candidates : React Flow (graph/canvas), dnd-kit (drag & drop).

### 3. Rule Wizard
Interface formulaire pour creer des regles simples (`withColumn`, `when/otherwise`, lookups) sans ecrire de Python.
Le wizard genere le code Python correspondant, pret a etre enregistre via `@register_rule()`.

### 4. Preview / Dry-run
Appeler `describe_schema()` cote serveur et afficher le plan d'execution.
Permet de valider visuellement le pipeline avant execution.

### 5. Export
Generer le YAML + le fichier Python de regles.
Format directement utilisable par le framework (`load_schema()` + `RuleRegistry`).

---

## Lien avec la piste 1 (multi-plateforme)

Si le multi-plateforme est implemente, le client graphique doit pouvoir se connecter a n'importe quel backend via le Protocol. Le catalogue explorer utiliserait des methodes du backend (`list_schemas()`, `list_tables()`, `describe_table()`), a ajouter au Protocol.

**Methodes supplementaires a prevoir dans le Backend Protocol :**

```python
# Extensions catalogue pour le client graphique
def list_catalogs(self) -> list[str]: ...
def list_schemas(self, catalog: str | None) -> list[str]: ...
def list_tables(self, catalog: str | None, schema: str) -> list[str]: ...
def describe_table(self, fqn: str) -> list[dict]: ...  # colonnes, types, stats
```

---

## Architecture API (FastAPI)

```
api/
  main.py              # App FastAPI, CORS, startup
  routes/
    catalog.py         # GET /catalogs, /schemas, /tables, /columns
    builder.py         # POST /validate-yaml, POST /preview (describe_schema)
    rules.py           # POST /generate-rule (wizard → Python code)
    export.py          # POST /export (YAML + Python bundle)
  websocket/
    preview.py         # WS /ws/preview — streaming du dry-run
```

---

## Dependances avec les autres pistes

| Piste | Impact |
|---|---|
| Piste 1 (Multi-plateforme) | Le catalogue explorer depend du backend abstrait pour naviguer les metadonnees |
| Piste 3 (Lineage) | Le client graphique peut integrer la visualisation du lineage (piste 3, renderer.py) |
| Piste 4 (Hub agentic) | Le client graphique pourrait embarquer un chat connecte au hub agentic |
| Piste 5 (Observabilite) | Dashboard de monitoring integre au client |
