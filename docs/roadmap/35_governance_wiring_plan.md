# Plan 35 — Câblage de la gouvernance : routage d'alertes et classification stricte

> Rédigé le 13 septembre 2026. Premier cycle de la chaîne agent-kernel sur skifer (rodage).
> Statut : **D1–D4 validés et GO donné le 13 septembre 2026. 35.0 ajouté le même jour (D5).**
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

Constat du gate de référence (13 septembre 2026, poste Windows) : `SqliteCertificationStore.get_run`,
`get_latest_promoted` et `list_history` trient sur `occurred_at` seul, horodaté par
`datetime.now(timezone.utc)`. Sous Windows (résolution d'horloge ~15 ms), STARTED et STAGED d'un même
run tombent au même instant et `get_run` renvoie STARTED : `Illegal publication transition from
STARTED to PROMOTING` (24 occurrences dans `test_certified_publication.py` / `test_incidents.py`,
verts sur la CI Linux). `get_latest_promoted` est la référence du diff cassant (D3) : 35.2 ne peut
pas s'appuyer sur un ordre non déterministe.

## 2. Rayon d'impact déclaré

Obtenu par `codegraph impact` / `codegraph_explore`. Projet piloté par YAML : **plancher, pas périmètre.**

| Zone | Symboles |
|---|---|
| Publication | `PublicationCoordinator.publish/_publish_run/_record_incidents/resume`, `start_publication_run`, `stage_dataframe`, `promote_staging` (`observability/publication.py`) |
| Store | `SqliteCertificationStore.get_run/get_latest_promoted/list_history` et nouveau helper `next_run_event_time` (35.0) ; `get_latest_promoted`, `get_contract` en lecture (35.2) (`observability/certification_store.py`) |
| Quarantaine | écritures CHECK_ERROR et QUARANTINED (`observability/quarantine.py`) — 35.0 |
| Routage | `AlertRouter` (`observability/incidents.py`) — consommé, pas modifié sauf nécessité documentée |
| Orchestration | `run_process_to_table`, `_index_promoted_metadata`, `_inherit_registry_classifications` (`core/patterns.py`) |
| Configuration | `ExecutionContext.env_config` (`core/context.py`), `ConfigurationManager._load_config_file` (`core/config.py`), `SkiferEngine.__init__` (`core/core.py`) |
| CLI | `skifer index` (`cli.py`) |
| Tests | `test_certification_store.py`, `test_certified_publication.py`, `test_incidents.py`, `test_alerts.py`, `test_patterns.py`, `test_lineage_classification.py`, `test_config.py`, `test_context.py`, `test_metadata_index.py` |
| Non-code | `CHANGELOG.md`, `docs/` (page gouvernance / observabilité) |

## 3. Sous-tâches

Une sous-tâche = un commit `feat(plan35-N): …` (`fix(plan35-0)` pour 35.0), chacun avec tests et entrée `CHANGELOG.md [Unreleased]`.

| # | Sous-tâche | Fichiers visés | Acceptation | Difficulté |
|---|---|---|---|---|
| 35.0 | **Ordre déterministe des événements de run** : (a) l'`occurred_at` de **chaque** événement d'un run est strictement croissant — `max(maintenant, dernier occurred_at du run + 1 µs)` — calculé par un helper unique `next_run_event_time` de `certification_store.py` (pas d'import circulaire publication ↔ quarantaine) et utilisé par les 7 écritures d'événements : 4 dans `publication.py`, 3 dans `quarantine.py` (CHECK_ERROR ×2, QUARANTINED) ; couvre Sqlite **et** Delta sans nouvelle primitive backend ; (b) `SqliteCertificationStore.get_run/get_latest_promoted/list_history` départagent les égalités par ordre d'insertion (`rowid DESC`), sans migration. *Amendé le 13 septembre 2026 après le premier dev : `quarantine.py` avait été omis.* | `observability/certification_store.py`, `observability/publication.py`, `observability/quarantine.py`, `tests/test_certification_store.py`, `tests/test_certified_publication.py` | horloge figée (tous les `now()` identiques) : l'état courant est le dernier écrit et un run se promeut ; événements à égalité d'horodatage insérés directement dans le store : le dernier inséré gagne ; les échecs `Illegal publication transition` de la base disparaissent | standard |
| 35.1 | **Configuration** : clés d'environnement `alerts:` (canaux, `min_severity`, `max_depth`) et `classification_propagation: warn \| strict` (défaut `warn`), validées au chargement (valeur inconnue = erreur), lues via `ExecutionContext` | `core/config.py`, `core/context.py`, `tests/test_config.py`, `tests/test_context.py` | config absente = comportement inchangé ; valeur invalide refusée avec message | standard |
| 35.2 | **Routage d'alertes à la publication** : `PublicationCoordinator` reçoit un routeur optionnel ; sur quarantaine, `alert_incident` après `_record_incidents` ; sur `PROMOTED`, diff entre la définition promue précédente du même `target_fqn` et la nouvelle, `alert_breaking_change` si `breaking`. Tout est **non bloquant**. `resume()` ne ré-alerte pas. `SkiferEngine` construit le routeur depuis la config de 35.1 | `observability/publication.py`, `core/patterns.py`, `core/core.py`, `tests/test_certified_publication.py`, `tests/test_incidents.py` | canal en échec ≠ publication en échec ; aucune valeur de donnée dans le message ; pas d'alerte sans routeur configuré ; pas de doublon sur `resume` | complexe |
| 35.3 | **Classification stricte** : en `strict`, contrôle **bloquant avant staging** pour un schéma `data_product` (élévation inférée non déclarée → `ValueError` qui nomme colonne et source) et `skifer index --strict` (code de sortie non nul). Le hook d'indexation reste non bloquant et en `warn` | `core/patterns.py`, `cli.py`, `tests/test_patterns.py`, `tests/test_metadata_index.py` | `warn` inchangé octet pour octet ; `strict` refuse avant toute écriture | standard |
| 35.4 | **Documentation** : page mkdocs (config `alerts:` et `classification_propagation`, parcours `warn` → mesure `skifer audit` → `strict`) | `docs/…`, `CHANGELOG.md` | `mkdocs build --strict` vert | trivial |

Dépendances : 35.0 et 35.1 (indépendantes) → 35.2 (dépend des deux) ; 35.1 → 35.3 ; 35.2 et 35.3 → 35.4.
Exécution séquentielle sur la branche : 35.0, 35.1, 35.2, 35.3, 35.4.

**Routage modèle** (écrasable au GO). Dev Codex : modèle configuré sur le poste `gpt-5.6-sol`,
effort de raisonnement selon la difficulté — trivial `low`, standard `medium`, complexe `high`
(`-c model_reasoning_effort=…`). Review Claude : `claude-sonnet-5`, `claude-opus-5` pour 35.2
(diff transverse).

## 4. Options écartées

- **`strict` dans le hook d'indexation** : l'exception y est avalée, le contrôle ne verrait jamais la lumière.
- **`strict` par défaut** : casserait les YAML existants ; le Plan 31 §7.5 impose de mesurer la couverture avant.
- **Alerte cassante uniquement dans `semantic sync --check`** : pas de destinataires ni de config d'alerte en CI, et le code de sortie `2` y signale déjà la dérive.
- **Alerte bloquante** : viole le principe 3 du Plan 31 (« rien ne bloque le métier »).
- **35.0 par rang d'état seul** : départage un même run mais pas deux runs `PROMOTED` à égalité ; et un rang d'état figerait l'hypothèse d'une machine à états strictement monotone, que `resume()` peut contredire.
- **35.0 par colonne de séquence** : exigerait une migration Sqlite et une nouvelle primitive Delta, pour un gain nul par rapport à (a) + (b).

## 5. Décisions à valider (conception)

| # | Question | Recommandation |
|---|---|---|
| D1 | Élargir le premier trou au câblage de `alert_incident` (quarantaine), pas seulement au changement cassant | **Oui** : même composant, même point d'accroche, sans quoi 31.4.2 reste mort |
| D2 | Où déclarer la config d'alertes | `config.yaml` par environnement (`alerts:`), comme `semantic_certification_policy` — plutôt qu'un paramètre `SkiferEngine(alert_config=…)` |
| D3 | Référence du diff cassant | la **dernière définition `PROMOTED` du même `target_fqn`** — plutôt que la version semver précédente du `contract_id`, qui peut ne jamais avoir été publiée sur cette cible |
| D4 | Répartition des agents | dev = **Codex**, review = **Claude** sans outil : fournisseurs distincts, la review n'est plus un repli. Implique `dev_agent: codex` dans `agent.yml` et un re-rendu du bloc |
| D5 | Ordre non déterministe des événements de run | **Validé** : sous-tâche préalable 35.0 plutôt que hors périmètre — D3 en dépend |

## 6. Risques

| Risque | Mitigation |
|---|---|
| `DeltaCertificationStore.get_contract` dépend de primitives backend | tests sur `SqliteCertificationStore` et `FakeBackend` ; aucune nouvelle primitive backend |
| Double alerte à la reprise (`resume`) | test explicite : `resume` ne dispatch rien |
| Fuite de valeur dans une alerte | réutiliser `dispatch_incident` (redaction existante, déjà testée) |
| Quotas Codex courts (constat du 2026-09-01) | sous-tâches bornées ; au cap budget, escalade |
| Gate local non représentatif (poste Windows sans `winutils.exe`) | comparaison à la base enregistrée + CI Linux au push pour les tests Spark (§7) |
| 35.0 décale un `occurred_at` de quelques µs | seulement quand l'horloge ne progresse pas ; l'horodatage reste un instant réel à la résolution de l'horloge près |

## 7. Vérification

- Gate de chaque sous-tâche, **sur ce poste Windows** (décision du 13 septembre 2026) :
  - suite pytest **complète, sans `-x`**, comparée à la liste des échecs de la base `029cd09` (277 :
    Hadoop `winutils.exe` absent, `:` dans des noms de fichiers, ordre des runs) — **aucun échec
    nouveau** ; un échec corrigé est signalé, jamais contourné ;
  - `ruff check src/ --select E4,E7,E9,F` (sélection de la CI `tests.yml`) : ruff 0.16 active
    ~788 règles par défaut et `ruff check src/` est rouge sur la base (677 erreurs) ;
  - les tests Spark font foi sur la CI Linux au push ;
  - `mkdocs build --strict` pour 35.4.
- Périmètre : `git --no-pager show <COMMIT> | grep -E "^-" | grep -vE "^---" | wc -l` proche de zéro.
- Review : diff comparé au rayon d'impact du §2, tests affectés via `codegraph affected`.
