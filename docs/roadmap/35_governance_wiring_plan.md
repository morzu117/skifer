# Plan 35 — Câblage de la gouvernance : routage d'alertes et classification stricte

> Rédigé le 13 septembre 2026. Premier cycle de la chaîne agent-kernel sur skifer (rodage).
> Statut : **Implémenté le 13 septembre 2026** (D1–D8 validés). Commits locaux, branche non poussée ;
> la CI Linux fait encore foi pour les tests Spark.
> 35.0 `677d6b1` · 35.1 `c980f53` · 35.2a `03f777e` · 35.2b `805dd52` + `b8e2dda` (redev 1) ·
> 35.3 `afe89d7` + `ffeec3b` (redev 1) · 35.4 `69c9293`. Suivi hors plan en §8.
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
STARTED to PROMOTING` (51 occurrences dans la suite, verts sur la CI Linux). `get_latest_promoted` est la
référence du diff cassant (D3) : 35.2 ne peut pas s'appuyer sur un ordre non déterministe.

Constat de préparation de 35.2 (même jour) : **`register_contract` n'est appelé nulle part sur le chemin
métier** — la table `contract_definitions` reste vide, et un `RunEvent` ne porte que
`contract_id`/`contract_version`/`definition_hash`. De plus `get_contract(id, version)` lève
« ambiguous definitions » quand une version porte deux hashes, soit précisément un contrat modifié sans
bump de version. La référence de D3 est donc introuvable en l'état (→ D6).

## 2. Rayon d'impact déclaré

Obtenu par `codegraph impact` / `codegraph_explore`. Projet piloté par YAML : **plancher, pas périmètre.**

| Zone | Symboles |
|---|---|
| Publication | `PublicationCoordinator.publish/_publish_run/_record_incidents/resume`, `start_publication_run`, `stage_dataframe`, `promote_staging` (`observability/publication.py`) |
| Store | `SqliteCertificationStore.get_run/get_latest_promoted/list_history` et helper `next_run_event_time` (35.0) ; `register_contract`, nouveau `get_contract_by_hash` sur le Protocol et les deux stores (35.2a) ; `get_latest_promoted` en lecture (35.2b) (`observability/certification_store.py`) |
| Backend | nouvelle primitive `SparkBackend.get_certification_contract_by_hash` (`core/spark_backend.py`) et son double `tests/fakes/fake_backend.py` — 35.2a |
| Contrat | nouveau `schema_from_definition`, `diff_contracts` consommé (`observability/certification.py`) — 35.2a/35.2b |
| Quarantaine | écritures CHECK_ERROR et QUARANTINED (`observability/quarantine.py`) — 35.0 |
| Routage | `AlertRouter`, `AlertDispatcher` (`observability/incidents.py`, `observability/alerts.py`) — consommés, pas modifiés ; adaptateur de gouvernance sur `metadata_store` ajouté — 35.2b |
| Orchestration | `run_process_to_table`, `_index_promoted_metadata`, `_inherit_registry_classifications` (`core/patterns.py`) |
| Configuration | `ExecutionContext.env_config` (`core/context.py`), `ConfigurationManager._load_config_file` (`core/config.py`), `SkiferEngine.__init__` (`core/core.py`) |
| CLI | `skifer index` (`cli.py`), `index_from_path`/`index_schema` (`observability/metadata_index.py`) |
| Tests | `test_certification_store.py`, `test_certified_publication.py`, `test_contract_diff.py`, `test_incidents.py`, `test_alerts.py`, `test_patterns.py`, `test_lineage_classification.py`, `test_config.py`, `test_context.py`, `test_metadata_index.py` |
| Non-code | `CHANGELOG.md`, `docs/observability.md`, `docs/governance.md` |

## 3. Sous-tâches

Une sous-tâche = un commit `feat(plan35-N): …` (`fix(plan35-0)` pour 35.0), chacun avec tests et entrée `CHANGELOG.md [Unreleased]`.

| # | Sous-tâche | Fichiers visés | Acceptation | Difficulté |
|---|---|---|---|---|
| 35.0 | **Ordre déterministe des événements de run** : (a) l'`occurred_at` de **chaque** événement d'un run est strictement croissant — `max(maintenant, dernier occurred_at du run + 1 µs)` — calculé par un helper unique `next_run_event_time` de `certification_store.py` (pas d'import circulaire publication ↔ quarantaine) et utilisé par les 7 écritures d'événements : 4 dans `publication.py`, 3 dans `quarantine.py` (CHECK_ERROR ×2, QUARANTINED) ; couvre Sqlite **et** Delta sans nouvelle primitive backend ; (b) `SqliteCertificationStore.get_run/get_latest_promoted/list_history` départagent les égalités par ordre d'insertion (`rowid DESC`), sans migration. *Amendé le 13 septembre 2026 après le premier dev : `quarantine.py` avait été omis.* **Livré : `677d6b1`.** | `observability/certification_store.py`, `observability/publication.py`, `observability/quarantine.py`, `tests/test_certification_store.py`, `tests/test_certified_publication.py` | horloge figée (tous les `now()` identiques) : l'état courant est le dernier écrit et un run se promeut ; événements à égalité d'horodatage insérés directement dans le store : le dernier inséré gagne ; les échecs `Illegal publication transition` de la base disparaissent | standard |
| 35.1 | **Configuration** : clés d'environnement `alerts:` (canaux, `min_severity`, `max_depth`) et `classification_propagation: warn \| strict` (défaut `warn`), validées au chargement (valeur inconnue = erreur, aucune valeur de canal recopiée), lues via `ExecutionContext.alerts_config()` / `classification_propagation()` | `core/config.py`, `core/context.py`, `tests/test_config.py`, `tests/test_context.py` | config absente = comportement inchangé ; valeur invalide refusée avec message | standard |
| 35.2a | **Registre des définitions de contrat** (aucune alerte) : `start_publication_run` appelle `register_contract` avant STARTED (idempotent) ; `get_contract_by_hash(contract_id, definition_hash)` sur le Protocol, Sqlite et Delta, ce dernier via la primitive `SparkBackend.get_certification_contract_by_hash` (+ `FakeBackend`) ; `schema_from_definition(definition) -> ParsedSchema` reconstruit sortie (`required`, `unique`, `classification`, `entity`) et SLA depuis `canonical_json` | `observability/publication.py`, `observability/certification_store.py`, `observability/certification.py`, `core/spark_backend.py`, `tests/fakes/fake_backend.py`, tests associés | la définition publiée (PROMOTED comme QUARANTINED) est relisible par hash, y compris quand une version porte deux hashes ; aller-retour `diff_contracts(s, schema_from_definition(canonicalize_contract(s)))` sans changement | standard |
| 35.2b | **Routage d'alertes à la publication** : `PublicationCoordinator` reçoit un routeur optionnel et la config d'alertes ; `get_latest_promoted(target_fqn)` lu **avant** `promote_staging` ; après `PROMOTED`, si le hash diffère, `diff_contracts(schema_from_definition(précédente), schema_from_definition(nouvelle))` puis `alert_breaking_change` si `breaking` ; sur quarantaine, `alert_incident` avec les incidents ouverts par `_record_incidents`. Tout est **non bloquant** (avertissement portant le seul nom de classe). `resume()` ne ré-alerte pas. Le routeur est construit depuis `alerts_config()` (35.1) quand un canal est configuré, sur un adaptateur de gouvernance adossé à `metadata_store` (`core` n'importe pas `services/`) | `observability/publication.py`, `observability/incidents.py` (adaptateur), `core/patterns.py`, `tests/test_certified_publication.py`, `tests/test_incidents.py` | canal en échec ≠ publication en échec ; aucune valeur de donnée dans le message ; pas d'alerte sans canal configuré ; pas de doublon sur `resume` ; première publication d'une cible ou hash inchangé : aucune alerte cassante | complexe |
| 35.3 | **Classification stricte** : en `strict`, contrôle **bloquant avant `process_schema` et toute écriture** pour un schéma `data_product` — sans `metadata_store`, refus (D8) ; élévation inférée non déclarée → `ValueError` qui nomme colonne et source(s) `table.colonne` — et `skifer index --strict` (code de sortie `3`, rien écrit pour le chemin fautif). Le hook d'indexation reste non bloquant et en `warn` | `core/patterns.py`, `cli.py`, `observability/metadata_index.py`, `tests/test_patterns.py`, `tests/test_metadata_index.py` | `warn` inchangé octet pour octet (aucune lecture du registre en plus) ; `strict` refuse avant toute écriture | standard |
| 35.4 | **Documentation** : `docs/observability.md` / `docs/governance.md` — config `alerts:` (`min_severity` sans effet sur les alertes d'incident et de changement cassant, toutes critiques — D7) et `classification_propagation`, parcours `warn` → mesure `skifer audit` → `strict`, `skifer index --strict` | `docs/…`, `CHANGELOG.md` | `mkdocs build --strict` vert | trivial |

Dépendances : 35.0 et 35.1 → 35.2a → 35.2b ; 35.1 → 35.3 ; 35.2b et 35.3 → 35.4.
Exécution séquentielle sur la branche : 35.0, 35.1, 35.2a, 35.2b, 35.3, 35.4.

**Routage modèle** (écrasable au GO). Dev Codex : modèle configuré sur le poste `gpt-5.6-sol`,
effort de raisonnement selon la difficulté — trivial `low`, standard `medium`, complexe `high`
(`-c model_reasoning_effort=…`). Review Claude : `claude-sonnet-5`, `claude-opus-5` pour 35.2b
(diff transverse).

## 4. Options écartées

- **`strict` dans le hook d'indexation** : l'exception y est avalée, le contrôle ne verrait jamais la lumière.
- **`strict` par défaut** : casserait les YAML existants ; le Plan 31 §7.5 impose de mesurer la couverture avant.
- **Alerte cassante uniquement dans `semantic sync --check`** : pas de destinataires ni de config d'alerte en CI, et le code de sortie `2` y signale déjà la dérive.
- **Alerte bloquante** : viole le principe 3 du Plan 31 (« rien ne bloque le métier »).
- **35.0 par rang d'état seul** : départage un même run mais pas deux runs `PROMOTED` à égalité ; et un rang d'état figerait l'hypothèse d'une machine à états strictement monotone, que `resume()` peut contredire.
- **35.0 par colonne de séquence** : exigerait une migration Sqlite et une nouvelle primitive Delta, pour un gain nul par rapport à (a) + (b).
- **Référence cassante depuis le registre de métadonnées** (`DatasetRecord`) : aucun changement de store, mais le diff n'y voit ni le durcissement `required` ni le relâchement SLA, et il exige `metadata_store`.
- **Référence par `get_contract(id, version)`** : lève précisément dans le cas le plus grave (contrat modifié sans bump de version).
- **Routeur sur `GovernanceService`** : `core` dépendrait de `services/` et d'un `RequestContext` fabriqué hors requête.
- **`strict` sans `metadata_store` qui passe** : aucune classification source n'est visible, le contrôle serait fantôme.

## 5. Décisions à valider (conception)

| # | Question | Recommandation |
|---|---|---|
| D1 | Élargir le premier trou au câblage de `alert_incident` (quarantaine), pas seulement au changement cassant | **Oui** : même composant, même point d'accroche, sans quoi 31.4.2 reste mort |
| D2 | Où déclarer la config d'alertes | `config.yaml` par environnement (`alerts:`), comme `semantic_certification_policy` — plutôt qu'un paramètre `SkiferEngine(alert_config=…)` |
| D3 | Référence du diff cassant | la **dernière définition `PROMOTED` du même `target_fqn`** — plutôt que la version semver précédente du `contract_id`, qui peut ne jamais avoir été publiée sur cette cible |
| D4 | Répartition des agents | dev = **Codex**, review = **Claude** sans outil : fournisseurs distincts, la review n'est plus un repli. Implique `dev_agent: codex` dans `agent.yml` et un re-rendu du bloc |
| D5 | Ordre non déterministe des événements de run | **Validé** : sous-tâche préalable 35.0 plutôt que hors périmètre — D3 en dépend |
| D6 | Reconstituer la définition de référence de D3 | **Validé** : enregistrer la définition à la publication et la relire par `definition_hash` (35.2a) ; assouplit le §6 pour **une** primitive Delta. Première alerte cassante possible à la 2e publication d'une cible après déploiement |
| D7 | `min_severity` sans effet sur les alertes d'incident (toutes critiques) | **Validé** : clé gardée et validée (35.1), effet documenté en 35.4 — pas de redev de 35.1 |
| D8 | `strict` sans `metadata_store` | refus fail-closed avant toute écriture, cohérent avec la politique du projet (« fail-closed partout ») |

## 6. Risques

| Risque | Mitigation |
|---|---|
| Primitives backend Delta | une seule nouvelle primitive (`get_certification_contract_by_hash`, D6), calquée sur `get_certification_contract` ; tests sur `SqliteCertificationStore` et `FakeBackend`, Spark couvert par la CI |
| Double alerte à la reprise (`resume`) | test explicite : `resume` ne dispatch rien |
| Fuite de valeur dans une alerte | réutiliser `dispatch_incident` (redaction existante, déjà testée) |
| Fuite de secret dans une erreur de config | messages 35.1 : clé et type seulement, test sur une URL de webhook |
| Quotas Codex courts (constat du 2026-09-01) | sous-tâches bornées ; au cap budget, escalade |
| Gate local non représentatif (poste Windows sans `winutils.exe`) | comparaison à la base enregistrée + CI Linux au push pour les tests Spark (§7) |
| 35.0 décale un `occurred_at` de quelques µs ; une lecture `get_run` par événement (jusqu'à 7 requêtes Spark de plus par run en Delta — review 35.0) | décalage seulement quand l'horloge ne progresse pas ; coût à surveiller sur forte volumétrie de publications certifiées |
| Aucune alerte cassante sur les cibles publiées avant 35.2a | assumé (D6) : les définitions antérieures n'ont jamais été enregistrées |

## 7. Vérification

- Gate de chaque sous-tâche, **sur ce poste Windows** (décision du 13 septembre 2026) :
  - suite pytest **complète, sans `-x`**, comparée à la liste des échecs de la base `029cd09` (277 :
    Hadoop `winutils.exe` absent, `:` dans des noms de fichiers, ordre des runs) — **aucun échec
    nouveau** ; un échec corrigé est signalé, jamais contourné ;
  - `ruff check src/ --select E4,E7,E9,F` (sélection de la CI `tests.yml`) : ruff 0.16 active
    ~788 règles par défaut et `ruff check src/` est rouge sur la base (677 erreurs) ;
  - les tests Spark font foi sur la CI Linux au push ;
  - `mkdocs build --strict` pour 35.4 (vert sur la base).
- Périmètre : `git --no-pager show <COMMIT> | grep -E "^-" | grep -vE "^---" | wc -l` proche de zéro.
- Review : diff comparé au rayon d'impact du §2, tests affectés via `codegraph affected`.

## 8. Suivi hors plan (constaté pendant le cycle)

| Sujet | Nature | Suite proposée |
|---|---|---|
| `run_process_and_split` et `run_union_sources_to_table` ignorent `data_product` : ni publication certifiée ni contrôle `strict` sur ces chemins | trou de conception pré-existant | décision humaine : refuser `data_product` sur ces patterns, ou les router vers la publication certifiée |
| URLs de webhook et identifiants SMTP dans un `config.yaml` versionné, sans interpolation d'environnement | conséquence de D2 non instruite | plan dédié (source de secrets) |
| Doc 35.4 : « without echoing the value » inexact pour `min_severity` (énumération citée) ; exemple `email: {}` qui ne compte pas comme canal | imprécisions mineures | correction de doc |
| `run_index_command` ne ferme pas son `SqliteMetadataStore` (fichier verrouillé sous Windows) | pré-existant | correctif isolé |
| `metadata_index` importe la fonction privée `_inherit_registry_classifications` ; message CLI `--strict` sans sources amont | MINEUR de review reporté | extraction d'une fonction publique |
| Une lecture `get_run` par événement de run (coût Delta) ; incidents suivants abandonnés après un échec au milieu de la boucle | MINEUR de review reporté | à mesurer avant d'agir |
| 248 échecs de la suite propres au poste Windows (winutils, `:` dans des noms de fichiers) | environnement | la CI Linux fait foi ; `ProposalGenerator` à rendre portable |
