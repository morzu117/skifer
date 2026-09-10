# Plan — Client Graphique Skifer

> Statut : Cadrage terminé (mai 2026)  
> Branche : `feat/gui-client-planning`  
> Dépend de : piste 1 (multi-plateforme, partiel), piste 3 (lineage), piste 4 (hub agentic)

---

## Contexte

La vision initiale (`02_client_graphique.md`) pose les bases : un outil visuel pour construire des pipelines YAML, explorer le catalogue, et piloter l'agentic hub. Ce document affine le périmètre, recense les prérequis techniques, et consigne les décisions prises.

---

## Périmètre

### MVP v1 — 3 modules

| Module | Description | Entrée API | Sortie |
|---|---|---|---|
| **Catalogue Explorer** | Navigation catalog → schema → table → colonnes | `CatalogInspector` / Backend Protocol | Arbre navigable |
| **YAML Builder** | Construction visuelle d'un pipeline + éditeur YAML brut (bidirectionnel) | JSON form ↔ YAML | Fichier `.yaml` valide |
| **BuilderAgent** | Génération de pipeline par description NL + règles simples via UI | `BuilderAgent.ask()` / wizard | YAML + snippet Python |

**Hors MVP v1 :** Agentic Chat (GenBIAgent), Semantic Browser, Lineage Viewer, Quality Dashboard, Session History/PDF.  
Ces modules seront adressés par itérations après la stabilisation du v1.

### Note sur les règles métier (Rule Wizard)

Le Rule Wizard dans le MVP se limite aux **règles simples créables via formulaire graphique** (`withColumn` basique, `when/otherwise`, constante, cast). Pas de règles complexes (lookups multi-tables, logique conditionnelle avancée). L'UI génère le snippet `@register_rule()` correspondant.

---

## Décisions architecturales actées

| # | Sujet | Décision |
|---|---|---|
| **Packaging** | Desktop vs web app | **Electron** — application desktop autonome |
| **Langue UI** | FR / EN | **Anglais uniquement** pour le v1 |
| **Limite d'affichage** | Taille max des résultats | **1 000 lignes** (`limit(1000)` côté API) — affiché avec indication du total |
| **YAML Builder** | Bidirectionnel ? | **Oui** — canvas et éditeur YAML brut synchronisés en temps réel |
| **Design system** | Stack UI | Bootstrap comme base, thème **cartoon Ant Design** adapté avec les couleurs du logo Skifer |

### Authentification — approche retenue

Le client est **agnostique du backend**, cohérent avec l'architecture multi-connecteurs du framework. Pas d'OAuth spécifique à un provider pour le v1 — trop de complexité à maintenir par provider (Databricks PKCE, Snowflake OAuth2, GCP service account).

Flux retenu :
1. Au premier lancement, l'utilisateur sélectionne son `config.yaml` via un explorateur de fichiers.
2. Le client détecte le backend actif (Databricks, Snowflake, BigQuery...) et affiche un bouton **"Connect with [Backend]"** correspondant — visuel uniquement, la connexion réelle passe par le token du `config.yaml`.
3. Les credentials restent dans le `config.yaml` existant — pas de stockage supplémentaire côté client.

> **v2 :** Implémenter les flows OAuth natifs par provider (Databricks PKCE, Snowflake, GCP ADC) pour remplacer la saisie manuelle du token.

---

## Éléments techniques nécessaires

### Backend (API Python — embarquée dans Electron)

L'API FastAPI tourne en processus local lancé par Electron au démarrage. Pas de déploiement serveur pour le v1.

Routes MVP :

```
GET  /catalog/catalogs              — liste des catalogues disponibles
GET  /catalog/schemas?catalog=X     — liste des schémas
GET  /catalog/tables?schema=X       — liste des tables
GET  /catalog/columns?table=X       — colonnes + types + stats

POST /pipeline/validate             — valider un dict schema YAML
POST /pipeline/preview              — dry-run (colonnes produites, plan)
POST /pipeline/export               — sauvegarder YAML + snippet Python

POST /builder/ask                   — BuilderAgent.ask(description) → YAML
POST /builder/wizard                — wizard step-by-step (sans LLM)
POST /rule/generate                 — formulaire → snippet @register_rule()
```

Sérialisation : tous les objets de réponse (`BuilderResponse`, etc.) sérialisés en JSON. Les DataFrames Spark → `collect()` → liste de dicts, limités à 1 000 lignes.

### Frontend (React + TypeScript dans Electron)

Composants MVP :

- Arbre catalogue (expandable tree) avec recherche
- Canvas drag-and-drop YAML Builder (React Flow)
- Panneau propriétés latéral (filtres, joins, `select_final`, `add_columns`)
- Éditeur YAML brut avec syntax highlighting + validation temps réel
- Synchronisation bidirectionnelle canvas ↔ YAML (parsing côté frontend)
- Formulaire BuilderAgent (description NL ou wizard)
- Formulaire règle simple → preview du snippet Python généré
- Bouton export (YAML + Python)

### Prérequis côté framework à vérifier

- `CatalogInspector` expose-t-il déjà `list_catalogs()`, `list_schemas()`, `list_tables()`, `describe_table()` ?
- `BuilderAgent` et `BuilderResponse` sont-ils sérialisables en JSON sans objet Spark ?
- Existe-t-il un mode dry-run / describe pour `process_schema()` sans exécution complète ?

---

## Décisions techniques actées

### Session Spark — par config active

**Décision : session liée à la config chargée (Option B).**  
Une session Spark est créée au chargement d'un `config.yaml`. Si l'utilisateur change de config, l'ancienne session est fermée et une nouvelle est instanciée. L'UI affiche une barre de progression "Connecting to Spark..." pendant la re-init (~10-30s). Pas de pool, pas de session partagée entre projets.

---

### Streaming des résultats — long polling avec backoff progressif

**Décision : long polling avec stratégie de backoff.**  
`POST /pipeline/preview` retourne immédiatement un `job_id`. L'UI poll `GET /job/{id}/status` selon la cadence suivante :
- **0 – 60s** : toutes les 5s
- **60s – 5min** : toutes les 15s
- **Au-delà de 5min** : poll automatique arrêté, bouton "Check status" affiché — l'utilisateur décide

> **v2 :** Server-Sent Events (SSE) pour streamer des logs de progression en temps réel.

---

### Gestion des `@register_rule()` — scan + dictionnaire client + éditeur simplifié

**Décision : approche hybride en 3 niveaux, sans `exec`.**

1. **Auto-load par convention** : le serveur surveille `rules/**/*.py` à la racine du projet au démarrage et en rechargement à chaud. Convention imposée, zéro config.

2. **Bouton "Missing business rules ?"** : permet de pointer vers des fichiers `.py` hors convention (legacy, dossier partagé). Le client les indexe dans un **dictionnaire local de règles** persisté entre sessions (nom, description, fichier source, date d'ajout). Construit progressivement un registre de règles connues.

3. **Éditeur Python simplifié** pour les règles complexes : éditeur contextualisé (la fonction reçoit `df: DataFrame`, retourne `DataFrame`), autocomplétion sur les fonctions Spark courantes (`withColumn`, `when`, `F.col`...), validation syntaxique avant sauvegarde. Génère et sauve le fichier `.py` dans `rules/` — le rechargement à chaud fait le reste. Pas d'`exec`.

> Inspiration : Power Query editor — éditeur guidé, pas un terminal libre.

---

## Dépendances inter-pistes

| Piste | Impact sur le MVP client |
|---|---|
| Piste 1 (Multi-plateforme) | Méthodes catalogue du Backend Protocol — nécessaires pour le Catalogue Explorer |
| Piste 4 (Hub agentic) | `BuilderAgent` + `BuilderResponse` — nécessaires pour le v1 |
| Piste 3 (Lineage) | Hors MVP v1 |
| Piste 5 (Qualité) | Hors MVP v1 |

---

## Prochaines étapes

1. Vérifier les prérequis framework (`CatalogInspector`, `BuilderResponse` sérialisable, dry-run).
2. Définir la structure du repo Electron (monorepo `api/` + `app/` ou repo séparé).
3. Rédiger le plan d'implémentation détaillé par phases.
4. Prototyper : Electron + FastAPI locale + Catalogue Explorer en premier.
