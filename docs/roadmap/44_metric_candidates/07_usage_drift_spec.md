# S07 — Signaux de drift d'usage (observation seule)

> **Décision :** décision 14 du brief, réduite à l'observation tant que les seuils ne sont pas
> calibrés
> **Dépendances :** S04 et S06 mergées
> **Slices / commits :** 2
> **Branche :** `feat/plan44-s07-usage-drift`
> **Résultat :** `skifer candidates drift` dit, chiffres à l'appui, si le semantic layer couvre
> de moins en moins les questions posées. Il ne change aucun état.

## 1. Contexte

Trois signaux, calculés sur une fenêtre `[since, until)` et partitionnés par environnement, avec
la discipline d'`adaptive/evaluator.py` :

- fenêtres semi-ouvertes ;
- seuil d'échantillon minimal, sous lequel le résultat est `inconclusive` ;
- horloge injectée ;
- itération sur clés triées.

**Écart avec le parent :** le parent mentionne une « route API en lecture ». Elle est retirée,
parce que D6 révisée exclut l'API en v1. Le notebook d'arbitrage de S06 peut afficher le drift
par une cellule optionnelle, dans S10.

## 2. Contrat

```python
# candidates/drift.py
@dataclass(frozen=True)
class DriftSignal:
    name: str                      # "fallback_rate" | "orphan_certified_metrics" | "rephrasing"
    status: str                    # "measured" | "inconclusive"
    value: float | None
    sample_size: int
    window: tuple[str, str]        # ISO, [since, until)
    details: tuple[dict, ...]      # éléments nommés (métriques orphelines, paires reformulées)
    reason: str | None             # pour inconclusive : "sample_below_minimum", "evidence_unavailable"

@dataclass(frozen=True)
class DriftReport:
    environment: str
    signals: tuple[DriftSignal, ...]
    thresholds_applied: bool       # toujours False en v1
    def to_dict(self) -> dict

def compute_drift(*, candidate_store, usage_store, catalog_models: Iterable[str],
                  index, environment: str, since: datetime, until: datetime,
                  min_sample: int, rephrasing_overlap: float = 0.6) -> DriftReport
```

Les trois signaux :

- **`fallback_rate`** : runs `answer` des candidates dans la fenêtre, divisés par la somme de
  ces runs et des événements d'usage `adaptive` de la même partition, sur `occurred_at` dans la
  fenêtre. C'est `inconclusive` si le dénominateur est sous `min_sample`.
- **`orphan_certified_metrics`** : les métriques du catalogue (`model_key.metric`) qui
  n'apparaissent dans aucun `metric_ids` d'événement `adaptive` de la fenêtre. La valeur est la
  part d'orphelines. Les détails listent les métriques, triées.
- **`rephrasing`** : une candidate créée dans la fenêtre dont les termes de la question,
  `tokenize` de S02, recouvrent à au moins `rephrasing_overlap` (Jaccard) les termes d'un nœud
  `metric` certifié. Les détails donnent les paires `(candidate_id, metric node_id, overlap)`. En
  mode `redacted`, la question n'est pas stockée et le signal est `inconclusive` avec la raison
  `questions_redacted`.

Seuils de config, lus et validés mais **jamais appliqués** :

```yaml
candidates_min_sample: 30
candidates_drift_thresholds:       # optionnel ; calibration future
  fallback_rate: 0.2
  orphan_certified_metrics: 0.5
```

```bash
skifer candidates drift [--since ISO] [--until ISO] [--json]   # défaut : 7 derniers jours
```

Codes de sortie : `0` (même avec des signaux élevés), `1` technique, `2` usage.

## 3. Exigences

- **S07-R1** — Un événement qui tombe exactement sur `until` NE DOIT PAS être compté. Un événement
  sur `since` DOIT l'être. Deux fenêtres contiguës ne comptent jamais deux fois le même
  événement.
- **S07-R2** — Sous `min_sample`, le signal DOIT être `inconclusive`, avec `value=None` et la
  taille d'échantillon réelle.
- **S07-R3** — Deux environnements NE DOIVENT JAMAIS se mélanger dans un même signal.
- **S07-R4** — `compute_drift` NE DOIT écrire dans aucun store, et ne doit faire aucune transition
  ni aucun DDL, ce que prouvent des espions. `thresholds_applied` vaut toujours `False`, même si
  `candidates_drift_thresholds` est configuré.
- **S07-R5** — La reformulation DOIT être détectée sur un cas construit : « CA net par
  région » contre la métrique `revenue_net` qui a pour synonyme « chiffre d'affaires net ». Elle
  DOIT être `inconclusive` en mode `redacted`.
- **S07-R6** — Le résultat DOIT être identique sous n'importe quel `PYTHONHASHSEED`, avec les
  détails triés.
- **S07-R7** — Le module NE DOIT contenir aucun appel à `check_transition` ni à aucune méthode
  d'écriture des stores. Le test inspecte le source, à la manière de `adaptive/evaluator`.

## 4. Fichiers

**À créer** : `src/skifer/candidates/drift.py` et `tests/test_candidates_drift.py`.

**À modifier** : `src/skifer/cli.py` (`candidates drift`), `src/skifer/core/config.py`
(`candidates_min_sample`, `candidates_drift_thresholds`) et `CHANGELOG.md`.

## 5. Slices

- **S07.1 Calcul.** `drift.py` : les trois signaux, les fenêtres, `inconclusive`.
- **S07.2 CLI.** `skifer candidates drift`, et la config validée mais non appliquée.

## 6. Tests d'acceptation

| Exigence | Test (`tests/test_candidates_drift.py::…`) |
|---|---|
| S07-R1 | `test_half_open_windows_never_double_count` |
| S07-R2 | `test_below_min_sample_is_inconclusive` |
| S07-R3 | `test_environments_are_strict_partitions` |
| S07-R4 | `test_drift_has_no_side_effect_even_with_thresholds` |
| S07-R5 | `test_rephrasing_detected`, `test_rephrasing_inconclusive_when_redacted` |
| S07-R6 | `test_deterministic_across_hash_seeds` |
| S07-R7 | `test_module_cannot_transition_or_write` |

## 7. Erreurs

| Cas | Comportement |
|---|---|
| `since >= until` | code `2`, `--since must be before --until` |
| datetime naïf | `ValueError`, comme dans `adaptive` |
| store `adaptive` vide ou purgé | `fallback_rate` et `orphan_certified_metrics` sont `inconclusive` avec la raison `evidence_unavailable`, sans exception |

## 8. Hors périmètre

- toute transition automatique, notification ou alerte ;
- la dégradation du jeu de référence comme signal, qui arrive en S09 ;
- la calibration des seuils, qui demande des données réelles et viendra après la v1.

## 9. Definition of Done

- les 7 exigences ont leur test vert ;
- `drift --json` sur l'état laissé par les tests de S06 produit un rapport valide.
