# Feature 2 — Registre de contrats, fraîcheur et certifications

> **Priorité :** P1 fondation confiance
> **Dépendance :** Feature 0 pour le contrat typé ; peut démarrer après 0.1–0.2
> **Slices / commits :** 4
> **Branche suggérée :** `feat/certification-registry`
> **Résultat :** état de certification historique, interrogeable localement et sur Databricks

## 1. Architecture cible

```text
Parsed contract ──canonicalize/hash──▶ contract_definitions (append-only)
pipeline run ─────────────────────────▶ materialization_runs (append-only events)
monitor results ──────────────────────▶ check_results (append-only)
                                              │
                                              ▼
                                 current_certifications (projection)
                                              │
                           ┌──────────────────┴──────────────────┐
                           ▼                                     ▼
                    Semantic gate                         UC tags/comments
                                                         (miroir best-effort)
```

SQLite et Delta implémentent le même contrat de comportement. Ne pas réintroduire un backend data
engineering générique : `CertificationStore` est une petite abstraction de persistence seulement.

## 2. Modèle de données logique

### `contract_definitions`

- `contract_id`, `contract_version`, `definition_hash` — clé logique ;
- canonical JSON/YAML, owner, data product, created_at, status ;
- aucune mise à jour destructive : nouvelle version/événement.

### `materialization_runs`

- `run_id`, dataset, contract identity/hash, state, timestamps ;
- target/staging/quarantine FQN ;
- consumer-independent.

### `check_results`

- run, check ID/type, scope, severity, status, actual/expected sérialisés, message ;
- ne pas stocker des objets pickle ou DataFrames.

### `current_certifications`

- dernière promotion réussie par dataset/contract ;
- statut calculé avec SLA et résultats ;
- `load_age`, `data_age`, `certified_at`, `expires_at` ;
- vues différentes possibles par `consumer_class`.

## 3. API cible

```python
class CertificationStore(Protocol):
    def register_contract(self, definition: ContractDefinition) -> None: ...
    def append_run_event(self, event: RunEvent) -> None: ...
    def append_check_results(self, results: Sequence[StoredCheckResult]) -> None: ...
    def get_run(self, run_id: str) -> MaterializationRun | None: ...
    def get_certification(self, dataset: str, consumer_class: str) -> Certification: ...
    def list_history(self, dataset: str, limit: int = 50) -> list[CertificationEvent]: ...
```

Les méthodes append doivent être idempotentes via IDs d'événement, pas via suppression/rewrite.

## 4. Fichiers

### À créer

- `src/skifer/observability/certification.py`
- `src/skifer/observability/certification_store.py`
- `src/skifer/observability/odcs.py`
- `tests/test_certification.py`
- `tests/test_certification_store.py`
- `tests/test_odcs_export.py`

### À modifier

- `src/skifer/observability/history.py` (réutiliser helpers, ne pas fusionner les concepts)
- `src/skifer/observability/checks.py`
- `src/skifer/observability/__init__.py`
- `src/skifer/core/spark_backend.py` pour DDL Delta/UC ciblé
- `tests/fakes/fake_backend.py`
- `docs/observability.md`, `docs/core.md`, `CHANGELOG.md`

## 5. Slices

### Slice 2.1 — Canonicalisation, identité et ODCS export

- Canonical JSON : clés triées, listes conservées si leur ordre est sémantique, timestamps exclus.
- Hash SHA-256 versionné (`hash_algorithm`, `canonicalization_version`).
- Valider SemVer de contrat sans ajouter une grosse dépendance si une regex stricte suffit.
- Export ODCS 3.1 : fundamentals, schema/properties, quality, team owner, roles read et SLA connus.
- Tout champ non mappé est listé dans un warning/report, pas perdu silencieusement.

### Slice 2.2 — Stores SQLite et Delta

- Schéma SQLite migré avec table `schema_migrations`.
- Delta : tables dans schéma configurable, `MERGE` uniquement pour dédupliquer l'event ID ; les faits
  restent append-only.
- Factory basée sur mode local/Databricks ; injection directe en tests.
- Suite contractuelle paramétrée exécutée contre SQLite et fake Delta.

### Slice 2.3 — Double fraîcheur et état courant

- `LoadFreshnessCheck` utilise dernier `PROMOTED`/heartbeat réussi.
- `DataFreshnessCheck` utilise une colonne événement déclarée.
- Garder `FreshnessCheck` comme alias déprécié vers data freshness ou migration explicite documentée ;
  ne pas changer silencieusement sa sémantique.
- Gérer horloges injectables pour tests, UTC partout, dates futures = erreur/warn policy explicite.
- Calculer certification par consumer class.

### Slice 2.4 — API et miroir Unity Catalog

- API consultation publique ou semi-publique documentée.
- Projection UC via `COMMENT`/tags avec allowlist : `skifer_owner`, `contract_version`,
  `certification`, `definition_hash`.
- Ne jamais mettre actual values, messages de violation, user IDs ou timestamps fins dans tags.
- Permission UC insuffisante : certification locale réussie, sync marquée `SYNC_ERROR`, retry possible.

## 6. Tests obligatoires

- hash stable, changement de type/formule/grain change le hash ; description définissante : décision
  à figer dans test ;
- event append idempotent ; ordre d'arrivée hors séquence ; crash avant/après append ;
- table sans changement métier mais load récent ; load ancien avec événement récent incorrect ;
- consumer dashboard vs agent ;
- store migration depuis version précédente ;
- UC SQL quoting et permission failure ;
- export ODCS validé contre le JSON Schema officiel vendored/test dependency si possible.

## 7. Hors périmètre

- moteur complet de gouvernance ODCS ;
- UI de catalogue ;
- ABAC automatique ;
- stockage de secrets ;
- rétention/purge destructive automatique.

## 8. Definition of Done

- historique reconstructible sans état mutable opaque ;
- stores passent la même suite contractuelle ;
- load freshness et data freshness ne peuvent pas être confondues dans l'API ;
- mode local complet sans UC ;
- tags UC non sensibles vérifiés par test.
