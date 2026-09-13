# Plan 35 — Câblage de la gouvernance : routage d'alertes et classification stricte

> Rédigé le 13 septembre 2026. Premier cycle de la chaîne agent-kernel sur skifer (rodage).
> Statut : **en attente de validation et de GO.**
> Tâche mémoire : `skifer:plan:governance-wiring`.

## 1. Contexte

Le benchmark OpenMetadata ([30](30_openmetadata_benchmark.md)) a produit douze manques, dont le
[Plan 31](31_lib_development_scenario.md) a livré les mécaniques. Un contrôle par l'index structurel
montre que deux d'entre elles existent mais **ne sont branchées sur aucun chemin métier** :

| Constat | Preuve (index) | Effet |
|---|---|---|
| `AlertRouter` (31.4.2) n'est construit nulle part hors des tests | `alert_incident` : 0 appelant ; `alert_breaking_change` : tests uniquement | une quarantaine ouvre un incident mais **personne n'est notifié** ; aucun changement de contrat cassant n'est signalé |
| La propagation de classification est figée en `warn` | seul appelant : `_inherit_registry_classifications`, `mode="warn"` en dur | le mode `strict` prévu après mesure (Plan 31 §7.5) est inatteignable |

Piège identifié : `_inherit_registry_classifications` tourne dans `_index_promoted_metadata`, un hook
**non bloquant** (`except Exception` → `SYNC_ERROR`). Un `strict` branché là lèverait une exception
avalée : un contrôle fantôme.

## 2. Rayon d'impact déclaré

Obtenu par `codegraph impact` / `codegraph_explore`. Projet piloté par YAML : **plancher, pas périmètre.**

| Zone | Symboles |
|---|---|
| Publication | `PublicationCoordinator.publish/_publish_run/_record_incidents/resume` (`observability/publication.py`) |
| Routage | `AlertRouter` (`observability/incidents.py`) — consommé, pas modifié sauf nécessité documentée |
| Orchestration | `run_process_to_table`, `_index_promoted_metadata`, `_inherit_registry_classifications` (`core/patterns.py`) |
| Configuration | `ExecutionContext.env_config` (`core/context.py`), `ConfigurationManager._load_config_file` (`core/config.py`), `SkiferEngine.__init__` (`core/core.py`) |
| CLI | `skifer index` (`cli.py`) |
| Store (lecture) | `get_latest_promoted`, `get_contract` (`observability/certification_store.py`) |
| Tests | `test_certified_publication.py`, `test_incidents.py`, `test_alerts.py`, `test_patterns.py`, `test_lineage_classification.py`, `test_config.py`, `test_metadata_index.py` |
| Non-code | `CHANGELOG.md`, `docs/` (page gouvernance / observabilité) |

## 3. Sous-tâches

Une sous-tâche = un commit `feat(plan35-N): …`, chacun avec tests et entrée `CHANGELOG.md [Unreleased]`.

| # | Sous-tâche | Fichiers visés | Acceptation | Difficulté |
|---|---|---|---|---|
| 35.1 | **Configuration** : clés d'environnement `alerts:` (canaux, `min_severity`, `max_depth`) et `classification_propagation: warn \| strict` (défaut `warn`), validées au chargement (valeur inconnue = erreur), lues via `ExecutionContext` | `core/config.py`, `core/context.py`, `tests/test_config.py` | config absente = comportement inchangé ; valeur invalide refusée avec message | standard |
| 35.2 | **Routage d'alertes à la publication** : `PublicationCoordinator` reçoit un routeur optionnel ; sur quarantaine, `alert_incident` après `_record_incidents` ; sur `PROMOTED`, diff entre la définition promue précédente du même `target_fqn` et la nouvelle, `alert_breaking_change` si `breaking`. Tout est **non bloquant**. `resume()` ne ré-alerte pas. `SkiferEngine` construit le routeur depuis la config de 35.1 | `observability/publication.py`, `core/patterns.py`, `core/core.py`, `tests/test_certified_publication.py`, `tests/test_incidents.py` | canal en échec ≠ publication en échec ; aucune valeur de donnée dans le message ; pas d'alerte sans routeur configuré ; pas de doublon sur `resume` | complexe |
| 35.3 | **Classification stricte** : en `strict`, contrôle **bloquant avant staging** pour un schéma `data_product` (élévation inférée non déclarée → `ValueError` qui nomme colonne et source) et `skifer index --strict` (code de sortie non nul). Le hook d'indexation reste non bloquant et en `warn` | `core/patterns.py`, `cli.py`, `tests/test_patterns.py`, `tests/test_metadata_index.py` | `warn` inchangé octet pour octet ; `strict` refuse avant toute écriture | standard |
| 35.4 | **Documentation** : page mkdocs (config `alerts:` et `classification_propagation`, parcours `warn` → mesure `skifer audit` → `strict`) | `docs/…`, `CHANGELOG.md` | `mkdocs build --strict` vert | trivial |

Dépendances : 35.1 → 35.2 et 35.3 (parallélisables) → 35.4.

**Routage modèle** (écrasable au GO). Dev Codex : modèle configuré sur le poste `gpt-5.6-sol`,
effort de raisonnement selon la difficulté — trivial `low`, standard `medium`, complexe `high`
(`-c model_reasoning_effort=…`). Review Claude : `claude-sonnet-5`, `claude-opus-5` pour 35.2
(diff transverse).

## 4. Options écartées

- **`strict` dans le hook d'indexation** : l'exception y est avalée, le contrôle ne verrait jamais la lumière.
- **`strict` par défaut** : casserait les YAML existants ; le Plan 31 §7.5 impose de mesurer la couverture avant.
- **Alerte cassante uniquement dans `semantic sync --check`** : pas de destinataires ni de config d'alerte en CI, et le code de sortie `2` y signale déjà la dérive.
- **Alerte bloquante** : viole le principe 3 du Plan 31 (« rien ne bloque le métier »).

## 5. Décisions à valider (conception)

| # | Question | Recommandation |
|---|---|---|
| D1 | Élargir le premier trou au câblage de `alert_incident` (quarantaine), pas seulement au changement cassant | **Oui** : même composant, même point d'accroche, sans quoi 31.4.2 reste mort |
| D2 | Où déclarer la config d'alertes | `config.yaml` par environnement (`alerts:`), comme `semantic_certification_policy` — plutôt qu'un paramètre `SkiferEngine(alert_config=…)` |
| D3 | Référence du diff cassant | la **dernière définition `PROMOTED` du même `target_fqn`** — plutôt que la version semver précédente du `contract_id`, qui peut ne jamais avoir été publiée sur cette cible |
| D4 | Répartition des agents | dev = **Codex**, review = **Claude** sans outil : fournisseurs distincts, la review n'est plus un repli. Implique `dev_agent: codex` dans `agent.yml` et un re-rendu du bloc |

## 6. Risques

| Risque | Mitigation |
|---|---|
| `DeltaCertificationStore.get_contract` dépend de primitives backend | tests sur `SqliteCertificationStore` et `FakeBackend` ; aucune nouvelle primitive backend |
| Double alerte à la reprise (`resume`) | test explicite : `resume` ne dispatch rien |
| Fuite de valeur dans une alerte | réutiliser `dispatch_incident` (redaction existante, déjà testée) |
| Quotas Codex courts (constat du 2026-09-01) | sous-tâches bornées ; au cap budget, escalade |

## 7. Vérification

- Gate de chaque sous-tâche : `pytest tests/ -x --tb=short && ruff check src/` en entier ; `mkdocs build --strict` pour 35.4.
- Périmètre : `git --no-pager show <COMMIT> | grep -E "^-" | grep -vE "^---" | wc -l` proche de zéro.
- Review : diff comparé au rayon d'impact du §2, tests affectés via `codegraph affected`.
