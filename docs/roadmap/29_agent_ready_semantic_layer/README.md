# Plans de développement dédiés — Programme 29

> **Statut :** architecture cible proposée, à valider avant build
> **Document parent :** [`../29_agent_ready_semantic_layer_program.md`](../29_agent_ready_semantic_layer_program.md)
> **Public cible :** agent de développement ne disposant pas du contexte de conception initial

## But de ce dossier

Chaque fichier est un contrat de build autonome pour une feature du Programme 29. Un agent chargé
d'une feature doit pouvoir identifier sans interprétation implicite :

- le problème exact à résoudre ;
- l'architecture autorisée et les alternatives rejetées ;
- les fichiers à créer ou modifier ;
- l'ordre des slices et donc des commits ;
- les tests obligatoires ;
- les comportements d'erreur ;
- les limites de périmètre ;
- les conditions objectives de sortie.

Ces plans ne donnent pas l'autorisation d'implémenter plusieurs features sur une seule branche.
Une feature possède sa branche, son cycle de review et son handoff.

## Ordre de lecture et d'exécution

| Ordre | Plan | Slices | Peut commencer lorsque… |
|---:|---|---:|---|
| 1 | [`00_semantic_projection_plan.md`](00_semantic_projection_plan.md) | 6 | immédiatement |
| 2 | [`06_domain_graph_plan.md`](06_domain_graph_plan.md) | 6 | Feature 0 terminée |
| 3 | [`02_certification_registry_plan.md`](02_certification_registry_plan.md) | 4 | projections/contrats typés disponibles |
| 4 | [`01_prepublication_quarantine_plan.md`](01_prepublication_quarantine_plan.md) | 5 | store minimal de certification disponible |
| 5 | [`03_semantic_certification_gate_plan.md`](03_semantic_certification_gate_plan.md) | 4 | Features 1 et 2 terminées |
| 6 | [`04_semantic_evidence_plan.md`](04_semantic_evidence_plan.md) | 4 | gate et domain graph disponibles |
| 7 | [`05_runtime_tracing_plan.md`](05_runtime_tracing_plan.md) | 5 | modèle de preuve stabilisé |
| 8 | [`07_mcp_readonly_plan.md`](07_mcp_readonly_plan.md) | 5 | service sémantique certifié et tracé |
| 9 | [`08_adaptive_gold_plan.md`](08_adaptive_gold_plan.md) | 6 | preuves/traces + Plan 28 disponibles |
| 10 | [`09_governed_capabilities_plan.md`](09_governed_capabilities_plan.md) | 8 | tracing et façade MCP disponibles |

Total : **53 slices**.

## Règles communes non négociables

1. Spark/Databricks seulement ; ne pas recréer un backend générique.
2. Le LLM propose des noms ou enrichit des descriptions ; il ne génère jamais le SQL exécuté.
3. `QueryResolver` ou un compilateur déterministe reste la frontière de génération SQL.
4. Le catalogue sémantique reste catalog-first et lazy ; ne jamais précharger tous les modèles.
5. Toute ancienne surface YAML valide reste valide sauf breaking change explicitement approuvé.
6. Toute nouvelle clé YAML passe par normalisation, IR typé, JSON Schema et tests de validation.
7. Toute erreur de sécurité, certification ou ambiguïté échoue explicitement ; aucun fallback permissif.
8. Ne jamais stocker token, secret, PII brute ou chaîne de pensée privée dans tags, preuves ou traces.
9. Une donnée récupérée ou une sortie LLM peut informer une décision ; elle ne peut jamais autoriser
   une action ou contourner un contrat.
10. Un plan point = un commit. Avant chaque commit :
    `pytest tests/ -x --tb=short` puis `ruff check src/`.
11. Toute modification `src/` possède ses tests et une entrée `CHANGELOG.md` sous `[Unreleased]`.
12. Ne jamais modifier la version de `pyproject.toml`.

## Convention de branche et de commits

Le numéro définitif du plan sera attribué au moment du build. En attendant :

```text
branche : feat/agent-ready-<feature>
commit  : feat(planNN-X.Y): <résultat utilisateur de la slice>
tests   : test(planNN-X.Y): ... uniquement si la slice ne change que les tests
docs    : docs(planNN-X.Y): ... pour une slice purement documentaire
```

## Checklist obligatoire avant le premier commit d'une feature

- lire complètement ce `README.md`, le plan dédié et `AGENTS.md` ;
- relire les fichiers source nommés par le plan : les numéros de ligne peuvent avoir dérivé ;
- vérifier `git status --short` et préserver les changements utilisateur ;
- vérifier que les dépendances indiquées sont réellement fusionnées ;
- écrire ou mettre à jour la page GBrain `plan:<task>` si `gbrain` est disponible ;
- créer la branche dédiée ;
- ne pas commencer si une décision marquée **BLOCKER DE DESIGN** n'est pas résolue.

## Definition of Done commune

- tous les commits/slices du plan sont présents, séparés et lisibles ;
- `pytest tests/ -x` est vert ;
- `ruff check src/` est vert ou les erreurs de baseline sont documentées précisément ;
- `CHANGELOG.md`, docs et JSON Schema commité sont à jour ;
- aucun secret ou artefact local n'est commité ;
- le handoff GBrain `dev-handoff:<task>` décrit fichiers, tests, écarts et risques ;
- le superviseur reçoit « prêt pour review » ;
- aucune feature dépendante n'est commencée dans la même branche.
