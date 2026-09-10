# Plan 15 — Sanitize : modules restants

**Branche :** `sanitize_code`  
**PR en cours :** #22  
**Contexte :** Après 4 passes de code review couvrant `core/core.py`, `core/operations.py`, `core/registry.py`, `core/sandbox.py`, `core/schema_loader.py`, `core/rule_executor.py`, `semantic/`, `agentic/agent.py`, `agentic/resolver.py`, `agentic/history.py`, `agentic/models.py`, `agentic/hub.py`, `agentic/exporter.py`, `agentic/user_profile.py` — les modules ci-dessous n'ont **pas encore été revus** (ou ont été lus avec des issues non traitées).

---

## Phase A — Issues identifiées, non encore traitées

Ces issues ont été lues et documentées mais pas fixées.

### A1 — `core/catalog_inspector.py`

**Problème :** Implémentation maison de Levenshtein pour les suggestions de noms proches.  
`resolver.py` utilise déjà `difflib.get_close_matches` pour la même chose — duplication de logique.

**Fix :**
- Remplacer l'implémentation locale par `difflib.get_close_matches` (stdlib, déjà disponible).
- Vérifier que le comportement (seuil, nb de suggestions) reste identique.

---

### A2 — `core/writer.py`

**Problème :** `write_dataframe(df, spark, ...)` prend `spark` comme paramètre positionnel brut au lieu d'un `Backend`. Incohérent avec l'architecture Backend-first adoptée partout ailleurs depuis la v1.0.

**Fix :**
- Remplacer le paramètre `spark` par `backend` (type `Backend`).
- Déléguer les opérations Spark à `backend.write_table(...)` ou équivalent.
- Vérifier les appelants (chercher `write_dataframe` dans le codebase).
- Mettre à jour le test correspondant dans `tests/test_writer.py`.

---

### A3 — `spark_factory.py`

**Problème :** Version Scala `"2.13"` hardcodée dans la config Delta Lake locale. Si l'environnement utilise une autre version, l'initialisation peut échouer silencieusement.

**Fix :**
- Extraire `SCALA_VERSION = "2.13"` en constante nommée en tête de fichier (facilite la mise à jour).
- Optionnel : lire depuis une variable d'environnement `SCALA_VERSION` avec fallback `"2.13"`.

---

## Phase B — Modules jamais couverts

Pour chaque module : lire, identifier les issues, committer les fixes. Un commit par sous-phase.

### B1 — `core/backend.py` + `core/config.py`

**À vérifier :**
- `backend.py` : ABC Backend — vérifier cohérence des signatures abstraites avec les implémentations concrètes dans `backends/`.
- `config.py` : ConfigurationManager — vérifier gestion des erreurs sur `config.yaml` manquant ou malformé ; imports top-level vs lazy.

---

### B2 — `core/rule_planner.py` + `core/rule_analyzer.py`

**À vérifier :**
- Imports PySpark top-level (même pattern que `operations.py` — rendre lazy).
- Logging vs `print()` — uniformiser avec `logger = logging.getLogger(__name__)`.
- Gestion des erreurs dans `rule_analyzer.py` : silencieux ou raise ?

---

### B3 — `backends/spark.py` + `backends/sql_base.py` + `backends/snowpark.py` + `backends/bigquery.py`

**À vérifier :**
- `spark.py` : import `pyspark` top-level vs lazy ; cohérence des signatures avec l'ABC `Backend`.
- `sql_base.py` : base commune SQL — vérifier abstractions partagées, gestion des connexions.
- `snowpark.py` / `bigquery.py` : imports optionnels (comme `openai`/`anthropic` dans `llm_provider.py`) — doivent être dans un `try/except ImportError` avec message clair.

---

### B4 — `lineage/tracker.py` + `lineage/dictionary.py` + `lineage/renderer.py`

**À vérifier :**
- `tracker.py` : logging vs print ; gestion des cas `column not found` (raise ou return vide ?).
- `dictionary.py` : chargement du dictionnaire — erreur silencieuse si fichier absent ?
- `renderer.py` : import de librairies de rendu (Mermaid, graphviz) — lazy + ImportError explicite.

---

### B5 — `observability/checks.py` + `observability/monitor.py` + `observability/reporter.py`

**À vérifier :**
- Uniformiser logging vs print.
- `checks.py` : vérifier que les checks SQL ne sont pas exposés à une injection (même approche que `resolver.py`).
- `reporter.py` : même pattern que `exporter.py` — constantes au niveau classe, imports lazy.
- `observability/history.py` : vérifier `datetime.utcnow()` (même fix que `agentic/history.py`).

---

### B6 — `observability/alerts.py` + `observability/contracts.py`

**À vérifier :**
- `alerts.py` : dispatch d'alertes (email, Slack ?) — imports optionnels corrects ?
- `contracts.py` : vérifier cohérence avec `checks.py`.

---

### B7 — `agentic/builder_agent.py` + `agentic/orchestrator.py`

**À vérifier :**
- `builder_agent.py` : même vérifications que `agent.py` — logging, try/except autour des appels LLM.
- `orchestrator.py` : gestion d'erreur sur chaque étape d'orchestration ; logging cohérent.

---

### B8 — `agentic/lineage_agent.py` + `agentic/quality_agent.py` + `agentic/dictionary_agent.py`

**À vérifier :**
- Logging vs print — uniformiser.
- `_normalize` dans `lineage_agent.py` : vérifier si c'est encore une copie (hub.py en importe désormais depuis `user_profile.py` — idem à faire ici si applicable).
- Gestion des erreurs LLM (try/except + logger.error).

---

### B9 — `cli.py` + `registry.py` (racine)

**À vérifier :**
- `cli.py` : gestion des erreurs CLI (exit codes corrects, messages d'erreur utiles).
- `registry.py` à la racine : vérifier s'il s'agit d'un re-export ou d'une duplication de `core/registry.py`. Supprimer si redondant.

---

## Vérification après chaque phase

```bash
pytest tests/ -x --tb=short -q
```

Tous les tests doivent passer (1015 à date) avant de passer à la phase suivante.

---

## Résumé des commits attendus

| Commit | Contenu |
|--------|---------|
| `fix(sanitize): A1-A3 — catalog_inspector, writer, spark_factory` | Phases A1, A2, A3 |
| `fix(sanitize): B1-B2 — backend, config, rule_planner, rule_analyzer` | Phases B1, B2 |
| `fix(sanitize): B3 — backends spark/sql/snowpark/bigquery` | Phase B3 |
| `fix(sanitize): B4 — lineage tracker/dictionary/renderer` | Phase B4 |
| `fix(sanitize): B5-B6 — observability` | Phases B5, B6 |
| `fix(sanitize): B7-B8 — agentic builder/orchestrator/agents` | Phases B7, B8 |
| `fix(sanitize): B9 — cli, root registry` | Phase B9 |

Une fois tous les commits sur `sanitize_code`, merger la PR #22 (ou créer une PR distincte si #22 est déjà mergée).
