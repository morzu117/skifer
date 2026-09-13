# Plan 31 — Plans exécutables par feature

> Sous-dossier du scénario [../31_lib_development_scenario.md](../31_lib_development_scenario.md).
> Un plan exécutable par feature, ancré dans le code réel de la branche `main` (`release: 2.1.0`),
> destiné à l'implémentation directe (Codex) sans ré-exploration. Rédigé le 11 septembre 2026.
> **Statut : rédigés, en attente de validation avant le premier commit** (DoD §9 du scénario).

Chaque plan suit le même format : *état actuel du code* (signatures réelles citées) → une section par slice
avec **Objectif · Fichiers exacts · Signatures Python · Comportement & règles · Cas de test nommés ·
Commit `feat(plan31-N.M)` + ligne CHANGELOG · DoD** → ordre interne → risques.

## Les sept plans

| Feature | Plan | Slices | Résumé |
|---|---|---|---|
| 31.1 | [feature_1_services.md](feature_1_services.md) | 1.1–1.6 | Package `services/` : frontière unique MCP/CLI/API, scopes, identité locale |
| 31.2 | [feature_2_metadata_registry.md](feature_2_metadata_registry.md) | 2.1–2.4 | Registre persistant (SQLite/Delta), lineage stocké, impact inter-pipelines |
| 31.3 | [feature_3_governance.md](feature_3_governance.md) | 3.1–3.5 | Classification typée+propagée, ownership, cycle de vie du contrat, import ODCS, diff |
| 31.4 | [feature_4_incidents_alerts.md](feature_4_incidents_alerts.md) | 4.1–4.3 | Incidents avec cycle de vie, routage aux owners aval |
| 31.5 | [feature_5_async_execution.md](feature_5_async_execution.md) | 5.1–5.3 | Jobs pilotables (job_id, statut, logs, annulation) |
| 31.6 | [feature_6_local_api.md](feature_6_local_api.md) | 6.1–6.2 | API FastAPI locale `[api]`, OpenAPI versionnée |
| 31.7 | [feature_7_coverage_audit.md](feature_7_coverage_audit.md) | 7.1 | `skifer audit` — gate CI, mesure la couverture de classification |

## Ordonnancement (rappel du scénario §6, arbitrages §7 appliqués)

```text
Phase 0   31.1.1 → 31.1.6 → 7.4                          (fondation services/ + identité)
          [7.3 déjà commité : dépendance close]
Phase 1   31.1.2 · 31.1.3 · 31.1.4 · 31.1.5              (services sans Spark)
          31.6.1 dès que 31.1.2 et 31.1.3 existent (API v0)
Phase 2   31.3 EN TÊTE ∥ 31.2 ∥ 31.7                     (gouvernance prioritaire, registre, audit tôt)
Phase 3   31.5 → 31.6.2 (+ 7.5)                           (exécution et packaging)
Phase 4   31.4                                            (incidents et routage)
Phase 5   Plan 29 Feature 8, puis Feature 9
```

Dépendances dures : 31.1.1 avant tout ; 7.4 avant 31.6 ; 31.2.3 + 31.3.2 + 31.3.5 avant 31.4.2 ;
**31.7 avant le passage de 31.3.1 en `strict`**.

## Corrections transverses relevées à la rédaction (par rapport au scénario)

Les plans ont été confrontés au code réel. Corrections qui touchent plusieurs features ou raffinent un
arbitrage — **à intégrer avant implémentation** :

1. **Dépôt = `release: 2.1.0`, pas de section `[Unreleased]` dans `CHANGELOG.md`.** La première slice qui
   touche le CHANGELOG (par feature) doit **créer** la section `## [Unreleased]`.
2. **Slice 7.3 (`mcp/tools.py`) déjà commitée avec `tests/test_mcp_tools.py`.** La dépendance « commiter
   7.3 avant 31.1.1 » du scénario est **close** — rien à faire côté 7.3.
3. **Arbitrage §7.3 raffiné.** `classification` est **déjà dans le payload de hash** au niveau champ
   (`canonicalize_contract`), et `owner`/`description` en sont **déjà exclus**. Conséquence :
   **31.3.1 ne bump PAS `CANONICALIZATION_VERSION`** (aucun changement du payload). Seule **31.3.3** le
   passe de 1 → 2, en y ajoutant `sla` et `security` (jamais `status`/`reviewers`/dates).
4. **`owner` est aujourd'hui une string seule** (`ParsedDataProduct.owner: str|None`, et
   `ContractDefinition.owner` / ODCS `team[]` supposent une string). Le passage à `{team, steward, domain,
   contact}` (31.3.2) exige un shim `owner_label` pour que la canonicalisation et l'ODCS reçoivent
   toujours une string. `owner` reste hors hash.
5. **Point de hook publication — à réconcilier entre 31.2 et 31.4.** La décision PROMOTED/QUARANTINE
   *affleure* dans `core/patterns.py` (~ligne 122-138) mais est *prise* dans
   `PublicationCoordinator._publish_run`. Le hook d'**indexation** (31.2.2) se pose après le log PROMOTED ;
   les hooks d'**incident** (31.4.1) doivent être **dans le coordinator** (`_publish_run` **et** `resume()`),
   car `resume()` (reprise après crash) court-circuite `patterns.py`. À implémenter comme deux hooks
   non-bloquants distincts au même endroit logique (le coordinator), pas dans `patterns.py`.
6. **`run_id` déjà frappé sur le chemin métier mais non retourné.** `run_process_to_table` /
   `run_from_yaml` mintent `str(uuid4())` et le plombent jusqu'à `PublicationCoordinator.publish`, mais
   **retournent `None`**. 31.5.2 rend ce changement rétrocompatible : `core.py` **accepte** `run_id=`
   (injection) et le **retourne**. Contrainte dure : `publication.start_publication_run` fait
   `str(UUID(run_id))` → `job_id` **doit** être un UUID (`str(uuid4())`) pour que `job_id == run_id`.
7. **API : `services/container.py` (`build_services`) ajouté** au-delà de la liste littérale de fichiers du
   scénario. La règle d'architecture interdit à `api/` d'importer un moteur/store ; le câblage
   moteur↔service vit donc dans la couche `services/`, seule autorisée à importer moteurs et stores.
8. **`info.version` de l'app OpenAPI = version de CONTRAT figée (`"0"`)**, découplée de
   `pyproject.version` (jamais bumpée) — sinon le snapshot OpenAPI dérive à chaque release. Snapshotter
   l'OpenAPI **normalisée**, pas le brut (dérive entre versions de FastAPI).
9. **Audit honnête (31.7).** `has_structured_owner = isinstance(owner, dict)` lira **0 %** tant que 31.3.2
   n'a pas livré l'owner structuré — c'est voulu (mesure vraie), pas un bug. C'est précisément la métrique
   qui autorisera le passage de 31.3.1 en `strict`.
10. **Signatures à respecter** : `LineageTracker.from_schema(schema_dict, target_name=None)` est un
    `@staticmethod` qui prend le **dict normalisé** (pas un `ProjectedSchema`) et appelle `parse_to_ir` ;
    `OutputProjector.project(ParsedSchema) -> ProjectedSchema` (dont on réutilise `definition_hash`).
    Les canaux d'`AlertDispatcher` sont dispatchés **par présence d'une clé de config** (pas de registre) :
    les nouveaux canaux s'ajoutent en `msteams_webhook` / `google_chat_webhook`.

### Défaut latent repéré (hors périmètre, à traiter à part)

- `lineage/tracker.py` (~354-373) : un `@staticmethod` dupliqué et `from_semantic_model` sans son
  décorateur. `from_schema` lui-même est sain. À corriger dans un commit indépendant du Plan 31.

## Definition of Done (rappel §9 du scénario)

- [x] arbitrages §7 tranchés (11 sept. 2026)
- [x] plan exécutable rédigé pour chaque feature (ce dossier)
- [ ] plans validés par l'utilisateur avant le premier commit
- [ ] chaque slice = un commit `feat(plan31-N.M): …` avec tests + entrée CHANGELOG
- [ ] `pyproject.toml` version inchangée
- [ ] `CLAUDE.md`/`AGENTS.md`/`docs/` mis à jour par feature
- [ ] `pytest tests/ -x --tb=short` vert après chaque phase
