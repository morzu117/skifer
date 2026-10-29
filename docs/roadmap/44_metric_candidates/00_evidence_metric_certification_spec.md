# S00 — Certification portée par la métrique dans la preuve

> **Décision :** D4 du plan parent
> **Dépendances :** aucune
> **Slices / commits :** 2
> **Branche :** `feat/plan44-s00-evidence-metric-certification`
> **Résultat :** une réponse dont une métrique n'est pas certifiée ne peut plus afficher
> « certifié le … », même si toutes ses tables sources le sont.

## 1. Contexte

La preuve d'une réponse (`SemanticEvidence`, `semantic/evidence.py`) porte un snapshot de
certification **par source** (`SourceEvidence.certification_status`). La ligne de provenance
affichée à l'utilisateur (`_certification_text()`, `serving/_response_serializer.py` vers la
ligne 174) ne regarde que les sources. Le plan introduit des métriques **candidates**, calculées
sur des tables qui peuvent très bien être certifiées : il faut un second axe, sur la métrique,
avant que la première réponse candidate existe.

Rien de nouveau à construire : un champ de plus dans `MetricEvidence`, admis par l'allowlist de
`to_dict()`, lu par `_certification_text()`. Toutes les réponses produites aujourd'hui par
`GenBIAgent` portent des métriques du catalogue, donc `CERTIFIED` par défaut : **aucune sortie
existante ne change de texte**.

## 2. Contrat

```python
# semantic/evidence.py
METRIC_CERTIFICATION_STATUSES = frozenset({"CERTIFIED", "CANDIDATE"})

@dataclass(frozen=True)
class MetricEvidence:
    name: str
    model_key: str
    definition_hash: str
    source_columns: tuple[str, ...]
    lineage_status: str = "resolved"
    certification: str = "CERTIFIED"          # nouveau — dans METRIC_CERTIFICATION_STATUSES
    candidate_id: str | None = None           # nouveau — requis ssi certification == "CANDIDATE"

class SemanticEvidence:
    schema_version: str = field(default="2", init=False)   # "1" → "2"
```

`to_dict()` ajoute, pour chaque entrée de `metrics` :

```json
{"name": "...", "model_key": "...", "definition_hash": "...", "source_columns": [...],
 "lineage_status": "resolved", "certification": "CANDIDATE", "candidate_id": "candidate:v1:…"}
```

`serving/_agent_core.py` : `EVIDENCE_SCHEMA_VERSION = "2"`.

`serving/_response_serializer.py` :

```python
def _certification_text(sources: list[dict], metrics: list[dict]) -> str
```

Règle d'affichage, dans cet ordre (le premier qui s'applique gagne) :

1. au moins une métrique `CANDIDATE` → `"métrique non certifiée (candidate <id court>)"` ;
   plusieurs → `"N métriques non certifiées"` ; `<id court>` = les 12 premiers caractères du
   digest après `candidate:v1:` ;
2. sinon, comportement actuel inchangé (sources non certifiées, puis « certification inconnue »,
   puis « certifié le <date la plus ancienne> »).

Une evidence lue au format `"1"` (sans les deux champs) est interprétée comme
`certification="CERTIFIED"`, `candidate_id=None`.

## 3. Exigences

- **S00-R1** — `MetricEvidence` DOIT refuser à la construction une `certification` hors de
  `METRIC_CERTIFICATION_STATUSES` (`ValueError` nommant la valeur).
- **S00-R2** — `MetricEvidence` DOIT refuser `certification="CANDIDATE"` sans `candidate_id`, et
  un `candidate_id` avec `certification="CERTIFIED"`.
- **S00-R3** — `SemanticEvidence.to_dict()` DOIT sérialiser `certification` et `candidate_id`
  pour chaque métrique, et `schema_version` DOIT valoir `"2"`.
- **S00-R4** — `_certification_text()` NE DOIT JAMAIS renvoyer un texte contenant « certifié le »
  quand au moins une métrique est `CANDIDATE`, quel que soit l'état des sources.
- **S00-R5** — Pour une evidence dont toutes les métriques sont `CERTIFIED`, le texte de
  provenance DOIT être **identique octet pour octet** à celui d'avant le lot.
- **S00-R6** — Le canal texte et le canal JSON du serving DOIVENT porter la même information
  (une réponse `CANDIDATE` en JSON n'est jamais annoncée certifiée en texte).
- **S00-R7** — La lecture d'une evidence `"1"` sérialisée DOIT rester possible là où le code en
  relit (s'il n'y en a aucune, l'exigence se réduit à un test sur le dict `"1"` passé à
  `_certification_text`).

## 4. Fichiers

**À modifier**

- `src/skifer/semantic/evidence.py` — `MetricEvidence`, `METRIC_CERTIFICATION_STATUSES`,
  `__post_init__`, `schema_version`, `to_dict()`.
- `src/skifer/serving/_response_serializer.py` — `_certification_text()` et son appelant (la
  fonction de provenance qui l'appelle vers la ligne 157 : lui passer `metrics`).
- `src/skifer/serving/_agent_core.py` — `EVIDENCE_SCHEMA_VERSION`.
- Tests existants qui épinglent `schema_version == "1"` : les trouver par
  `grep -rn 'schema_version' tests/`, les passer à `"2"` **avec une phrase de justification dans
  le message de commit**. Ne pas toucher à leurs autres assertions.
- `CHANGELOG.md`.

**À créer** : aucun fichier source. Tests dans les fichiers existants
(`tests/test_semantic_evidence.py`, `tests/test_serving_response_serializer.py`).

## 5. Slices

**S00.1 — Champ et allowlist.** `MetricEvidence` + validation + `to_dict()` + version `"2"` +
`EVIDENCE_SCHEMA_VERSION`. Mise à jour des tests qui épinglent `"1"`.

**S00.2 — Affichage.** `_certification_text(sources, metrics)`, appelant, tests de texte.

## 6. Tests d'acceptation

| Exigence | Test | Prouve |
|---|---|---|
| S00-R1 | `test_semantic_evidence.py::test_metric_certification_rejects_unknown_status` | `"GOLD"` → `ValueError` |
| S00-R2 | `…::test_candidate_metric_requires_candidate_id` et `…::test_certified_metric_rejects_candidate_id` | les deux incohérences refusées |
| S00-R3 | `…::test_to_dict_serializes_metric_certification` | champs présents, `schema_version == "2"` |
| S00-R4 | `test_serving_response_serializer.py::test_candidate_metric_on_certified_sources_is_not_announced_certified` | sources toutes `CERTIFIED` + une métrique `CANDIDATE` → pas de « certifié le », présence de « non certifiée » |
| S00-R5 | `…::test_certified_metrics_keep_previous_provenance_text` | texte attendu codé en dur, identique à l'existant, pour trois cas (certifié, source non certifiée, inconnue) |
| S00-R6 | `…::test_text_and_json_agree_on_candidate_metric` | sortie de `hub_response_to_markdown` et `semantic_evidence_to_dict` cohérentes |
| S00-R7 | `…::test_v1_evidence_dict_reads_as_certified` | dict sans les champs → traité `CERTIFIED` |

## 7. Erreurs

| Cas | Type | Message |
|---|---|---|
| statut inconnu | `ValueError` | `MetricEvidence.certification must be one of CANDIDATE, CERTIFIED; got 'X'.` |
| candidate sans id | `ValueError` | `MetricEvidence.candidate_id is required when certification is CANDIDATE.` |
| id sans candidate | `ValueError` | `MetricEvidence.candidate_id must be None when certification is CERTIFIED.` |

## 8. Hors périmètre

- produire une seule evidence `CANDIDATE` depuis le code métier (c'est S03) ;
- tout changement de la ligne de provenance pour les cas `CERTIFIED` ;
- un nouveau composant d'affichage, un bandeau, une couleur.

## 9. Definition of Done

- les 7 tests ci-dessus sont verts ;
- `tests/test_serving_mlflow_roundtrip.py` (job CI `serving`) reste vert avec la version `"2"` ;
- aucun texte de réponse existant n'a changé (S00-R5).
