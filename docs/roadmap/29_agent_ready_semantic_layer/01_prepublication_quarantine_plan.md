# Feature 1 — Validation avant publication et quarantaine

> **Priorité :** P1 confiance
> **Dépendance :** Feature 2, slices registre/store minimal
> **Slices / commits :** 5
> **Branche suggérée :** `feat/certified-publication`
> **Résultat :** une version invalide ne remplace jamais la dernière cible certifiée

## 1. Architecture cible

```text
process_schema
     │ DataFrame
     ▼
write staging  ──failure──▶ RUN_FAILED
     │
     ▼
validate staging
  ├── PASS ──▶ promote target ──▶ PROMOTED ──▶ cleanup staging
  ├── FAIL ──▶ quarantine snapshot/rows ──▶ QUARANTINED
  └── ERROR ─▶ quarantine snapshot ───────▶ CHECK_ERROR
```

La cible précédente reste intacte sur toutes les branches sauf après un commit Delta de promotion
réussi. La promotion n'est pas un rename multi-table supposé atomique : elle écrit une nouvelle
version Delta de la cible depuis le staging validé.

## 2. Invariants

- `PASS`, `FAIL`, `ERROR` et `SKIPPED` sont distincts.
- Un check critique `ERROR` bloque comme un `FAIL`.
- Un opérateur non implémenté ne retourne jamais `passed=True`.
- Les checks row-level peuvent produire un prédicat de violation ; les checks dataset ne le peuvent
  pas.
- Chaque staging/quarantaine possède `run_id`, target résolue et contract hash.
- Aucun cleanup récursif ou basé sur glob.
- Streaming, JDBC et materialized views sont refusés explicitement en v1.

## 3. API cible

```python
class CheckStatus(str, Enum):
    PASS = "PASS"
    FAIL = "FAIL"
    ERROR = "ERROR"
    SKIPPED = "SKIPPED"

class ContractScope(str, Enum):
    ROW = "row"
    DATASET = "dataset"

class PublicationCoordinator:
    def publish(self, df, target, schema, contract, run_id=None) -> PublicationResult: ...
    def resume(self, run_id: str) -> PublicationResult: ...
```

`PipelinePatterns` orchestre, mais la machine d'état vit dans un composant dédié testable.

## 4. Fichiers

### À créer

- `src/skifer/observability/publication.py`
- `src/skifer/observability/quarantine.py`
- `tests/test_certified_publication.py`
- `tests/test_quarantine.py`

### À modifier

- `src/skifer/observability/checks.py`
- `src/skifer/observability/monitor.py`
- `src/skifer/core/patterns.py`
- `src/skifer/core/writer.py`
- `src/skifer/core/spark_backend.py`
- `src/skifer/core/core.py`
- `tests/fakes/fake_backend.py`
- `tests/test_observability.py`, `tests/test_patterns.py`, `tests/test_writer.py`
- `docs/observability.md`, `docs/core.md`, `CHANGELOG.md`

## 5. Slices

### Slice 1.1 — Statuts et prédicats de violations

- Remplacer le booléen implicite par `CheckStatus` tout en conservant une propriété `passed` de
  compatibilité (`status == PASS`).
- Ajouter `scope` et `violation_predicate()` aux contrats row-level.
- Les strings SQL générées doivent réutiliser les fonctions de quoting/littéraux sûres existantes ;
  ne pas dupliquer une interpolation vulnérable.
- Adapter reports/history/serialization avec migration backward-compatible.

Tests : tous les contrats existants, opérateur inconnu → SKIPPED, exception backend → ERROR,
critical ERROR bloque.

### Slice 1.2 — Staging et machine de run

- Noms : schéma `_skifer_staging`, table dérivée du target + run UUID validé.
- Résoudre les FQN sans variables/globs non vérifiés.
- Enregistrer `STARTED`, `STAGING`, `STAGED` dans le `CertificationStore`.
- Ajouter primitives backend minimales : écrire staging, lire staging, drop exact.

Tests : collision, caractères target, échec write, run ID fourni/invalide, sandbox.

### Slice 1.3 — Validation et snapshot quarantine

- Monitor exclusivement le staging FQN.
- Sur blocage, créer snapshot de quarantaine et manifeste avant de supprimer staging.
- Si la quarantaine échoue, conserver staging et enregistrer les deux erreurs ; ne pas toucher cible.
- Stocker références, pas DataFrames, dans le résultat sérialisable.

### Slice 1.4 — Row quarantine

- Construire colonne `_violations` avec toutes les règles row-level échouées.
- Ajouter métadonnées réservées avec collision check au load.
- Le snapshot complet reste nécessaire si un check dataset échoue.
- API de replay read-only dans cette slice ; pas de réinjection automatique.

### Slice 1.5 — Promotion et reprise E2E

- Relire staging validé et écrire cible via le writer batch existant.
- Enregistrer `PROMOTING` avant write puis `PROMOTED` après succès.
- `resume(run_id)` : transitions autorisées explicites ; un run `PROMOTED` retourne idempotemment son
  résultat sans réécrire.
- E2E Delta local v1 valide / v2 KO / v3 valide.

Chaque slice possède son commit `feat(planNN-1.X): ...`.

## 6. Matrice de publication

| Severity | PASS | FAIL | ERROR | SKIPPED |
|---|---|---|---|---|
| info | publier | publier + rapport | publier seulement si policy | publier + rapport |
| warning | publier | publier + warning | policy | publier + warning |
| critical | publier | bloquer | bloquer | bloquer par défaut |

La policy finale devient configurable, mais le défaut critical/SKIPPED est bloquant : un contrat
critique non vérifié n'est pas satisfait.

## 7. Erreurs obligatoires

- staging write failure avec target/run ;
- check error distinct d'une violation ;
- collision de colonnes `_violations`, `_run_id`, `_contract_version` ;
- mode streaming/JDBC/MV non supporté avec alternative ;
- reprise depuis transition illégale.

## 8. Hors périmètre

- dead-letter par micro-batch streaming ;
- quarantaine JDBC distante ;
- promotion de materialized view ;
- UI de correction ;
- suppression automatique après durée de rétention.

## 9. Definition of Done

- cible précédente inchangée après chaque scénario d'échec ;
- aucune violation critique publiée dans l'E2E ;
- reprise idempotente testée à chaque frontière de crash ;
- aucun check inconnu présenté comme PASS ;
- documentation des coûts de double écriture staging/promotion.
