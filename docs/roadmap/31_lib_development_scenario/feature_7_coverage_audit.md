# Plan 31 — Feature 31.7 — Audit de couverture

> **But (une ligne).** Une fonction pure `audit_project(paths) -> AuditReport` qui mesure, sur les
> pipelines YAML d'un projet, la part de ceux qui déclarent `data_product`, `contract`, un owner
> structuré, une description par champ de sortie et une classification déclarée ; plus une CLI
> `skifer audit PATHS [--json] [--min-coverage N]` (exit 2 sous le seuil) et une méthode
> `ProjectService.audit()`.

## Pourquoi 31.7 est remontée en phase 1/2 (arbitrage §7.5 — à lire en premier)

L'arbitrage §7.5 du Plan 31 (tranché le 2026-09-11) décide que la **classification** est propagée en
mode **`warn`** en v1 (31.3.1) : une élévation de classification inférée mais non déclarée **avertit**
au lieu d'échouer au load. Le durcissement en mode **`strict`** (refus dur au load) n'est activé
**qu'une fois la couverture réelle mesurée**. Cette mesure, c'est précisément `skifer audit`.

**Conséquence directe et non négociable : 31.7 est un PRÉREQUIS DUR du basculement `strict` de
31.3.1.** On ne durcit pas une règle dont on ne connaît pas le taux de couverture — on transformerait
un projet conforme à 40 % en projet qui ne charge plus. C'est pourquoi 31.7, bien que trivial en
volume de code, est **remonté en phase 1/2** (voir §6 « Ordonnancement » du plan parent), avant le
reste de 31.3, et non relégué en phase 4 avec les incidents.

**Contraintes de livraison.**
- **Jamais** de bump de version dans `pyproject.toml` (le user gère le versioning).
- **Une slice = un commit** `feat(plan31-7.1): …` + tests + une entrée `CHANGELOG.md` sous
  `## [Unreleased]` (section à **créer** dans ce commit — le HEAD `release: 2.1.0` n'en a pas).
- Calcul **pur** : aucune session Spark, aucune dépendance runtime. Déterministe.
- Sortie JSON **stable** (clés triées, pipelines triés, pourcentages arrondis) pour un diff CI fiable.

---

## État actuel du code

### Découverte et parsing des pipelines YAML

Il n'existe **aucun** helper de « découverte de projet » réutilisable aujourd'hui. Deux conventions
coexistent, toutes deux dans `src/skifer/cli.py` :

1. **CLI `validate` / `index` : expansion de globs sur des `PATHS` passés en argument**, puis parsing
   fichier par fichier. `skifer validate PATHS` (`cli.py:45-54`, `_run_validate` `cli.py:330-375`)
   fait :
   ```python
   import glob as glob_mod
   paths = []
   for pattern in args.paths:
       expanded = sorted(glob_mod.glob(pattern, recursive=True))
       paths.extend(expanded if expanded else [pattern])
   # si aucun fichier : print("No schema files found.") ; sys.exit(0)
   ```
   Chaque fichier est lu (`_read_text_file`, `cli.py:576-578`) puis parsé avec des **params
   sentinelles** pour neutraliser les placeholders `{{ key }}` sans avoir la config :
   ```python
   # cli.py:34, 581-582
   _PLACEHOLDER_RE = re.compile(r"\{\{\s*(\w+)\s*\}\}")
   def _sentinel_params(yaml_text: str) -> dict[str, str]:
       return {key: f"__sentinel_{key}__" for key in _PLACEHOLDER_RE.findall(yaml_text)}
   # usage : parse_schema(yaml_str, params=_sentinel_params(yaml_str))
   ```
   `validate` renvoie **exit 1** si au moins un fichier échoue au parse, sinon 0.

2. **`ProjectService.open()` (31.1.2, à venir) : liste `pipelines` = chemins relatifs des `.yaml`
   sous `schemas/`.** D'après `feature_1_services.md` (l. 335-344) :
   ```python
   @dataclass(frozen=True)
   class ProjectView:
       root: str
       config: dict[str, Any]
       environments: tuple[str, ...]
       pipelines: tuple[str, ...]   # chemins relatifs des .yaml sous schemas/
       rules: tuple[str, ...]       # chemins relatifs des .py sous rules/
       models: tuple[str, ...]      # clés du semantic_catalog.yaml
       calendars: tuple[str, ...]
   ```
   `open(ctx)` exige le scope `SCOPE_PROJECT_READ = "project:read"`.

**Décision pour 31.7 :** `audit_project(paths)` prend une **séquence de chemins de fichiers déjà
résolus** (mêmes `PATHS` que `validate` / `index`, après expansion de globs), et parse chacun avec
`parse_schema(text, params=_sentinel_params(text))`. La CLI fait l'expansion de globs ; le service
fournit la liste depuis `ProjectView.pipelines`. C'est la fonction qui lit les fichiers (pas de Spark),
elle reste déterministe.

### Forme parsée exacte de `data_product`, `contract`, `classification`, `description`

Source de vérité : `src/skifer/core/schema_loader.py`, `_normalize_agent_ready_metadata`
(l. 444-636), appelé par `_normalize_schema` (l. 1381) donc par `parse_schema` / `load_schema`. Les
trois blocs sont **optionnels** (un YAML sans eux charge inchangé). Après normalisation :

- **`data_product`** (l. 454-491) — présent seulement s'il est déclaré :
  ```python
  {"id": str, "version": str, "owner"?: str, "description"?: str}
  ```
  Clés autorisées `_DATA_PRODUCT_ALLOWED_KEYS = {"id","version","owner","description"}` (l. 37).
  **`owner` est validé comme une chaîne non vide uniquement** (l. 480-484) — voir la discordance
  ci-dessous sur « owner structuré ».

- **`contract`** (l. 495-577) — présent seulement s'il est déclaré :
  ```python
  {
    "output": {                       # obligatoire, mapping non vide
      "<field_name>": {               # normalized_field, l. 528-547
        "logical_type"?: str,
        "classification"?: str,       # chaîne libre en l'état (31.3.1 la typera)
        "entity"?: str,
        "description"?: str,          # description par champ de sortie
        "required"?: bool,
        "unique"?: bool,
      }, ...
    },
    "grain"?: [str],                  # l. 557-577
  }
  ```
  Clés de champ autorisées `_OUTPUT_FIELD_ALLOWED_KEYS =
  {"logical_type","required","unique","classification","entity","description"}` (l. 39-41).
  Chaque champ string (`logical_type`/`classification`/`entity`/`description`) est **strip** et doit
  être non vide (l. 529-538) ; un champ **peut** n'avoir aucune de ces clés (dict vide `{}` autorisé).

- **`classification`** : chaîne libre par champ de `contract.output` (aucune énumération en v1 —
  c'est justement 31.3.1 qui posera la taxonomie `public|internal|confidential|restricted|pii`).

- **`description`** par champ : clé `description` du dict de champ de `contract.output`.

Représentation JSON Schema correspondante : `src/skifer/core/json_schema.py` — `DataProductDef`
(l. 131-149), `ContractDef` (l. 162-179), `OutputFieldDef` (l. 150-161). **31.7 ne touche pas au JSON
Schema** (aucun champ YAML nouveau ; principe §3.2 du plan parent respecté par vacuité).

### Pattern d'exit-code de la CLI

`src/skifer/cli.py` définit des constantes d'exit par famille de commande et des fonctions
`run_*(...) -> int` testables directement, enveloppées par un `_run_*` qui fait `sys.exit(...)` :

```python
# cli.py:20-32
SEMANTIC_EXIT_OK = 0; SEMANTIC_EXIT_ERROR = 1; SEMANTIC_EXIT_DRIFT = 2; SEMANTIC_EXIT_CONFLICT = 3
ADAPTIVE_EXIT_OK = 0; ADAPTIVE_EXIT_ERROR = 1; ADAPTIVE_EXIT_USAGE = 2; ...
```
- `run_semantic_sync(pipeline, mode) -> int` (l. 415) renvoie 0/2/3 ; `_run_semantic` (l. 378) fait
  `sys.exit(run_semantic_sync(...))`.
- `run_adaptive_command(args) -> int` (l. 217) renvoie 0/1/2/3/4/5.
- Le dispatch se fait dans `main()` (l. 195-209) : `args = parser.parse_args()` puis `if args.command
  == "...": _run_xxx(args)`.

**Modèle repris pour audit** : constantes `AUDIT_EXIT_OK=0 / AUDIT_EXIT_ERROR=1 /
AUDIT_EXIT_BELOW_THRESHOLD=2`, une fonction `run_audit(paths, *, as_json, min_coverage) -> int`
testable, un `_run_audit(args)` qui fait `sys.exit(run_audit(...))`.

### Où placer le module et comment il s'exporte

`src/skifer/observability/__init__.py` ré-exporte chaque symbole public du package (voir la liste
`from skifer.observability.X import ...` + `__all__`). 31.7 crée `observability/audit.py` et y ajoute
`AuditReport`, `PipelineAudit`, `audit_project` dans les imports et `__all__`.

### Fixtures disponibles (aucune fixture `spark` nécessaire)

- **Exemples avec métadonnées agent-ready** (utilisables comme échantillons *couverts*) :
  - `examples/02_quality_and_contract/gold_orders.yaml` — `data_product` (id, version, owner,
    description) + `contract` (grain + output ; champs avec `logical_type`, `required`, `unique`,
    **sans `classification` ni `description`**). Bon cas « contract sans classification ni
    description ».
  - `examples/13_semantic_projection/pipeline.yaml`, `examples/10_monitor_and_quarantine/monitored_orders.yaml` — idem `data_product`+`contract`.
- **Tests de référence pour le style** : `tests/test_cli.py` — helper `_run_cli(*args)` (l. 30-37)
  qui lance la CLI en sous-processus ; tests `test_validate_*` (l. 126-199) montrent le pattern
  d'assertion sur `returncode`/`stdout`. Les tests de `run_semantic_sync` (l. 280-348) appellent la
  **fonction** directement avec `capsys` et vérifient l'`int` retourné — c'est le pattern à privilégier
  pour tester les seuils sans sous-processus.
- **Ni fixture `spark` ni cluster** : 31.7 est du pur parsing YAML. Les tests écrivent des `.yaml`
  temporaires dans `tmp_path`.

---

## Slice 31.7.1 — Audit de couverture

### Objectif

Livrer, en un commit, trois choses cohérentes :
1. `observability/audit.py` : dataclasses `PipelineAudit` / `AuditReport` (avec `to_dict()`
   allowlisté) et la fonction pure `audit_project(paths) -> AuditReport`.
2. CLI `skifer audit PATHS [--json] [--min-coverage N]` : rapport lisible ou JSON stable, **exit 2**
   sous le seuil, **exit 1** en échec technique, **exit 0** sinon.
3. `ProjectService.audit()` : façade sous scope `project:read` qui liste les pipelines via
   `open()` et délègue à `audit_project` (dépendance **soft** sur 31.1.2 — voir §« Ordre »).

### Fichiers

- `src/skifer/observability/audit.py` — **création** : `MetricKey` (constantes), `PipelineAudit`,
  `AuditReport`, `audit_project`, helpers privés `_audit_one`, `_coverage_pct`.
- `src/skifer/observability/__init__.py` — **modification** : importer et ré-exporter
  `AuditReport`, `PipelineAudit`, `audit_project` (+ `__all__`).
- `src/skifer/cli.py` — **modification** : constantes `AUDIT_EXIT_*`, sous-parseur `audit`, dispatch
  `elif args.command == "audit"`, fonctions `run_audit(...) -> int` et `_run_audit(args)`.
- `src/skifer/services/project.py` — **modification** (si 31.1.2 déjà mergé, sinon différé — voir
  §Ordre) : méthode `ProjectService.audit(ctx, *, min_coverage=None) -> AuditReport`.
- `tests/test_audit.py` — **création** : calcul sur fixtures, seuils, stabilité JSON.
- `tests/test_cli.py` — **modification** : tests CLI `skifer audit` (exit 0/1/2, `--json`).
- `tests/test_project_service.py` — **modification** (si 31.1.2 mergé) : `ProjectService.audit`
  sous scope.
- `CHANGELOG.md` — **modification** : créer `## [Unreleased]` + entrée `### Added`.

### Signatures Python

```python
# src/skifer/observability/audit.py
from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

# Le module CLI a la même regex ; on la redéfinit ici pour rester autonome (aucun import de cli).
_PLACEHOLDER_RE = re.compile(r"\{\{\s*(\w+)\s*\}\}")

# Les cinq métriques mesurées, dans un ordre stable et documenté.
METRIC_KEYS: tuple[str, ...] = (
    "data_product",         # le pipeline déclare un bloc data_product
    "contract",             # le pipeline déclare un bloc contract
    "structured_owner",     # data_product.owner est un mapping (team/steward/domain) — cf. 31.3.2
    "field_descriptions",   # contract.output non vide ET chaque champ a une description non vide
    "field_classification", # contract.output non vide ET chaque champ a une classification déclarée
)


@dataclass(frozen=True)
class PipelineAudit:
    """Résultat d'audit d'un pipeline (déterministe, sans Spark)."""
    path: str                         # chemin tel que fourni (relatif ou absolu), inchangé
    parsed: bool                      # False si le YAML n'a pas pu être parsé
    error: str | None                 # message d'erreur (nom de classe + résumé) si parsed is False
    has_data_product: bool
    has_contract: bool
    has_structured_owner: bool
    has_field_descriptions: bool
    has_field_classification: bool
    output_field_count: int           # nb de champs contract.output (0 si pas de contrat)
    described_field_count: int        # nb de champs avec description non vide
    classified_field_count: int       # nb de champs avec classification déclarée

    def metric(self, key: str) -> bool:
        """Vrai si ce pipeline satisfait la métrique `key` (un pipeline non parsé ne satisfait rien)."""
        if not self.parsed:
            return False
        return {
            "data_product": self.has_data_product,
            "contract": self.has_contract,
            "structured_owner": self.has_structured_owner,
            "field_descriptions": self.has_field_descriptions,
            "field_classification": self.has_field_classification,
        }[key]

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "parsed": self.parsed,
            "error": self.error,
            "has_data_product": self.has_data_product,
            "has_contract": self.has_contract,
            "has_structured_owner": self.has_structured_owner,
            "has_field_descriptions": self.has_field_descriptions,
            "has_field_classification": self.has_field_classification,
            "output_field_count": self.output_field_count,
            "described_field_count": self.described_field_count,
            "classified_field_count": self.classified_field_count,
        }


@dataclass(frozen=True)
class AuditReport:
    """Rapport de couverture agrégé sur un ensemble de pipelines.

    `coverage` mappe chaque clé de METRIC_KEYS vers un pourcentage [0.0, 100.0] arrondi à 2 décimales.
    `overall_coverage_pct` est la moyenne arithmétique des cinq pourcentages (arrondie à 2 décimales) ;
    c'est la valeur comparée à `--min-coverage`.
    """
    total: int                              # nb total de pipelines audités
    parsed: int                             # nb parsés avec succès
    errored: int                            # nb en échec de parse (= total - parsed)
    coverage: dict[str, float]              # {metric_key: pct} pour chaque METRIC_KEYS
    overall_coverage_pct: float
    pipelines: tuple[PipelineAudit, ...]    # triés par `path`

    def to_dict(self) -> dict[str, Any]:
        return {
            "total": self.total,
            "parsed": self.parsed,
            "errored": self.errored,
            "coverage": {k: self.coverage[k] for k in METRIC_KEYS},
            "overall_coverage_pct": self.overall_coverage_pct,
            "pipelines": [p.to_dict() for p in self.pipelines],
        }


def audit_project(paths: Sequence[str]) -> AuditReport:
    """Audite la couverture de gouvernance d'un ensemble de pipelines YAML.

    Fonction pure/déterministe : lit et parse chaque fichier (via parse_schema + params sentinelles),
    n'ouvre aucune session Spark, ne mute rien. L'ordre d'entrée n'affecte pas la sortie (pipelines
    triés par `path`). Un fichier illisible ou au YAML invalide est enregistré avec parsed=False et
    compte comme non couvert pour toutes les métriques (dénominateur = total).
    """
    ...


# helpers privés
def _audit_one(path: str) -> PipelineAudit: ...
def _coverage_pct(satisfying: int, total: int) -> float:
    """0 pipeline -> 0.0 ; sinon round(100 * satisfying / total, 2)."""
    ...
```

```python
# src/skifer/services/project.py  (ajout — modification, dépendance soft sur 31.1.2)
from skifer.observability.audit import AuditReport, audit_project

class ProjectService:
    ...
    def audit(self, ctx: "RequestContext", *, min_coverage: float | None = None) -> AuditReport:
        """Audit de couverture des pipelines du projet, sous scope project:read.

        min_coverage n'est PAS un gate ici (le service ne lève pas sous le seuil) : c'est la CLI
        qui traduit un rapport en exit-code. Le paramètre est accepté pour parité de signature et
        usages futurs (API), mais le service renvoie toujours l'AuditReport.
        """
        require_scope(ctx, SCOPE_PROJECT_READ)
        view = self.open(ctx)  # ProjectView.pipelines = chemins relatifs sous schemas/
        abs_paths = [os.path.join(self._root, rel) for rel in view.pipelines]
        return audit_project(abs_paths)
```

```python
# src/skifer/cli.py  (ajouts)
AUDIT_EXIT_OK = 0
AUDIT_EXIT_ERROR = 1
AUDIT_EXIT_BELOW_THRESHOLD = 2

def run_audit(paths: list[str], *, as_json: bool, min_coverage: float | None) -> int:
    """Expande les globs, audite, imprime (texte ou JSON stable), renvoie l'exit-code.
    0 = OK (au-dessus/égal au seuil, ou pas de seuil, ou aucun pipeline) ;
    2 = overall_coverage_pct < min_coverage ; 1 = échec technique (glob/IO inattendu)."""
    ...

def _run_audit(args: argparse.Namespace) -> None:
    sys.exit(run_audit(args.paths, as_json=args.json, min_coverage=args.min_coverage))
```

### Comportement & règles

**Les cinq métriques (par pipeline, sur le dict normalisé issu de `parse_schema`).** Un pipeline dont
le parse échoue a toutes ses métriques à `False`.
- `has_data_product` : la clé `data_product` est présente.
- `has_contract` : la clé `contract` est présente.
- `has_structured_owner` : `data_product.owner` présent **et** de type `dict` (mapping). Voir la
  discordance ci-dessous : en l'état du code (owner = chaîne uniquement), cette métrique vaut
  **toujours 0 %** jusqu'à 31.3.2 — c'est une mesure honnête, pas un bug.
- `has_field_descriptions` : `contract.output` non vide **et** *chaque* champ a une `description` non
  vide. (`described_field_count == output_field_count > 0`.)
- `has_field_classification` : `contract.output` non vide **et** *chaque* champ a une `classification`
  déclarée. (`classified_field_count == output_field_count > 0`.) **C'est la métrique-clé du
  basculement `strict` de 31.3.1.**

**Calcul du pourcentage de couverture (niveau projet).** Pour chaque métrique :
`coverage[key] = round(100 * (nb pipelines satisfaisant key) / total, 2)` où `total` = nombre total de
pipelines (les pipelines non parsés sont au dénominateur et comptent comme non satisfaisants — le gate
est strict). `overall_coverage_pct = round(mean(coverage[k] for k in METRIC_KEYS), 2)`.

**Sémantique de `--min-coverage N`.** `N` est un nombre (float, 0-100 ; argparse `type=float`). Si
`--min-coverage` est absent, **aucun gate** : exit 0 (sauf échec technique). Si présent, la CLI compare
`overall_coverage_pct` à `N` : `overall_coverage_pct < N` ⇒ **exit 2** ; `>= N` ⇒ exit 0. La
comparaison porte sur `overall_coverage_pct` (pas sur chaque métrique) — les pourcentages par métrique
restent dans le rapport pour un gate plus fin livrable ultérieurement sans casser l'interface.

**Cas 0 pipeline.** Si l'expansion des globs ne donne aucun fichier (comme `validate` : « No schema
files found. »), la CLI imprime un message, produit `total=0` et `coverage` à `0.0` partout,
`overall_coverage_pct=0.0`, et **renvoie exit 0 quel que soit `--min-coverage`** (rien à gater ; on ne
fait pas échouer un CI sur un chemin vide). Documenté et testé.

**Exit 1 (échec technique).** Réservé aux erreurs inattendues de la CLI elle-même (ex. motif de glob
illégal). Un fichier illisible ou au YAML invalide n'est **pas** un échec technique : il est capté par
`audit_project` en `PipelineAudit(parsed=False, error=…)` et compte comme non couvert. (Contraste avec
`validate` qui, lui, sort en 1 sur parse-error : `audit` mesure, il ne valide pas.)

**Sortie texte (défaut).** Un tableau lisible, déterministe :
```
Audited 3 pipeline(s) — 3 parsed, 0 errored.

  data_product          100.0%   (3/3)
  contract              100.0%   (3/3)
  structured_owner        0.0%   (0/3)
  field_descriptions     33.3%   (1/3)
  field_classification    0.0%   (0/3)

  overall               46.66%
```
(Les pipelines en erreur sont listés sous une section `FAIL <path>` avec leur `error`.)

**Sortie JSON stable (`--json`).** `print(json.dumps(report.to_dict(), sort_keys=True, indent=2,
ensure_ascii=False))`. Déterminisme garanti par : (1) `pipelines` triés par `path` dans
`audit_project` ; (2) `sort_keys=True` ; (3) pourcentages arrondis à 2 décimales (pas de bruit
flottant) ; (4) `coverage` ne contient que les clés de `METRIC_KEYS`. Aucun chemin absolu machine
n'est injecté par le module (le `path` est celui fourni par l'appelant) — pour un JSON reproductible en
CI, passer des chemins relatifs.

### Cas de test

**`tests/test_audit.py`** (pur, aucune fixture `spark`, écrit des `.yaml` dans `tmp_path`) :
- `test_audit_all_covered` : 2 pipelines avec `data_product` (owner = chaîne, la seule forme qui
  charge en v1), `contract.output` où **chaque** champ a `classification` + `description` → couverture
  100 % sur `data_product`, `contract`, `field_descriptions`, `field_classification` ;
  `structured_owner == 0 %` (owner chaîne, pas mapping — cf. discordance). Asserte `coverage` exact et
  `overall_coverage_pct == round((100+100+0+100+100)/5, 2) == 80.0`.
- `test_audit_none_covered` : 2 pipelines nus (tables + select_final seulement) → toutes les
  métriques à 0.0, `overall_coverage_pct == 0.0`, `total == 2`, `parsed == 2`, `errored == 0`.
- `test_audit_partial_field_coverage` : un contrat où **certains** champs seulement ont
  `description`/`classification` → `has_field_descriptions is False` (règle « tous les champs »),
  `described_field_count` exact, `output_field_count` exact.
- `test_audit_uses_example_fixture` : audite `examples/02_quality_and_contract/gold_orders.yaml`
  (résolu via `Path(__file__).parents[1]`) → `has_data_product and has_contract`, mais
  `has_field_classification is False` et `has_field_descriptions is False` (ce contrat n'a ni
  classification ni description). Verrouille l'interprétation des fixtures réelles.
- `test_audit_unparseable_counts_as_uncovered` : un fichier au YAML invalide (opérateur de filtre
  inconnu) → `PipelineAudit.parsed is False`, `error is not None`, toutes métriques False ;
  `report.errored == 1` et le pipeline pèse au dénominateur (`coverage['data_product']` calculé sur
  `total`).
- `test_audit_templated_params_neutralized` : un pipeline avec `{{ catalog }}` charge grâce aux
  params sentinelles (comme `validate`), `parsed is True`.
- `test_audit_zero_pipelines` : `audit_project([])` → `total == 0`, tous les `coverage` à `0.0`,
  `overall_coverage_pct == 0.0`, `pipelines == ()`.
- `test_audit_report_json_is_sorted_and_stable` : `json.dumps(report.to_dict(), sort_keys=True)`
  identique sur deux appels ; l'ordre d'entrée des paths permuté ⇒ **même** JSON (tri par `path`) ;
  les pourcentages n'ont pas plus de 2 décimales.
- `test_audit_deterministic_across_input_order` : `audit_project([a, b]).to_dict() ==
  audit_project([b, a]).to_dict()`.
- `test_metric_keys_frozen` : `METRIC_KEYS == ("data_product","contract","structured_owner",
  "field_descriptions","field_classification")` (égalité exacte — verrou d'interface).

**`tests/test_cli.py`** (ajouts, style `run_semantic_sync` — appel direct de la fonction avec `capsys`
pour les seuils, plus un `_run_cli` sous-processus pour l'exit-code de bout en bout) :
- `test_audit_cli_exit_zero_without_threshold` : `run_audit([bon_pipeline], as_json=False,
  min_coverage=None) == AUDIT_EXIT_OK`.
- `test_audit_cli_exit_two_below_threshold` : `run_audit([pipeline_nu], as_json=False,
  min_coverage=50.0) == AUDIT_EXIT_BELOW_THRESHOLD`.
- `test_audit_cli_exit_zero_at_or_above_threshold` : couverture 80.0, `min_coverage=80.0` → 0.
- `test_audit_cli_json_stable` : `run_audit([...], as_json=True, ...)` ; capture `capsys`, `json.loads`
  round-trip et re-`dumps(sort_keys=True)` identique.
- `test_audit_cli_no_files_exits_zero` : `run_audit([str(tmp_path/'none*.yaml')], as_json=False,
  min_coverage=90.0) == AUDIT_EXIT_OK` et « No schema files found. » imprimé.
- `test_audit_cli_end_to_end` : `_run_cli("audit", str(bon), "--min-coverage", "100")` →
  `returncode in (0, 2)` selon la couverture réelle du fixture ; asserte le texte du rapport.

**`tests/test_project_service.py`** (uniquement si 31.1.2 mergé — sinon reporter au moment du merge) :
- `test_project_service_audit_requires_scope` : `audit(ctx_sans_project_read, …)` lève `ScopeDenied`.
- `test_project_service_audit_lists_pipelines` : arbo temporaire `schemas/gold/f.yaml` (+ config.yaml
  minimal) → `audit(ctx).total == 1`.

### Commit

Message exact :
```
feat(plan31-7.1): coverage audit — audit_project, skifer audit CLI, ProjectService.audit
```
Entrée `CHANGELOG.md` (créer la section `## [Unreleased]` juste sous l'en-tête Keep-a-Changelog, avant
`## [2.1.0]`) :
```markdown
## [Unreleased]

### Added

- Coverage audit (`skifer.observability.audit`): pure, deterministic `audit_project(paths) ->
  AuditReport` measuring, across a project's pipeline YAMLs, the share declaring `data_product`,
  `contract`, a structured owner, a per-output-field description, and a declared classification.
  New CLI `skifer audit PATHS [--json] [--min-coverage N]` — exit 2 when `overall_coverage_pct` is
  below the threshold, exit 0 otherwise, with a stable sorted-key JSON report for CI diffing.
  Exposed transport-neutrally via `ProjectService.audit()`. This is the measurement gate that must
  precede hardening classification propagation (31.3.1) from `warn` to `strict` (Plan 31 §7.5).
```

### DoD

- [ ] `observability/audit.py` créé ; `audit_project` pur (aucun import Spark, aucun `SparkSession`),
      déterministe (tri par `path`), pourcentages arrondis à 2 décimales.
- [ ] `PipelineAudit`/`AuditReport` ont un `to_dict()` allowlisté (aucun `asdict`, aucune fuite de
      champ non prévu).
- [ ] Ré-exports ajoutés dans `observability/__init__.py` + `__all__`.
- [ ] CLI `skifer audit` : sous-parseur, `run_audit(...) -> int`, `_run_audit`, dispatch dans `main()`,
      constantes `AUDIT_EXIT_*` ; exit 2 sous le seuil, 0 sinon, 0 si 0 pipeline.
- [ ] `ProjectService.audit()` sous `project:read` (si 31.1.2 mergé ; sinon PR de suivi documentée).
- [ ] Tests verts (`pytest tests/test_audit.py tests/test_cli.py -x --tb=short`), sans fixture `spark`.
- [ ] Entrée `## [Unreleased]` créée dans `CHANGELOG.md`.
- [ ] `pyproject.toml` **non** modifié.
- [ ] `ruff check src/` propre.

---

## Ordre & dépendances internes

- **Module `observability/audit.py` : totalement autonome.** Il ne dépend que de
  `core.schema_loader.parse_schema` (déjà présent) et de la stdlib. Il peut être livré **avant** toute
  autre slice du Plan 31, y compris 31.1.x. La CLI `skifer audit` ne dépend que de ce module.
- **`ProjectService.audit()` : dépendance SOFT sur 31.1.2.** La méthode n'est qu'une façade de trois
  lignes (scope + `open()` + `audit_project`). `services/project.py` n'existe qu'après 31.1.2 (phase 1).
  L'ordonnancement du plan parent place 31.1.2 en **phase 1** et 31.7 en **phase 2** : à ce stade,
  `ProjectService` existe et la méthode s'ajoute proprement. **Si, pour une raison de séquencement,
  31.7 était implémenté avant 31.1.2**, livrer le module + la CLI dans ce commit et ajouter
  `ProjectService.audit()` (avec ses tests) dans le commit de 31.1.2 ou une PR de suivi — le message de
  commit et le CHANGELOG le mentionneraient. Le cœur de la valeur (mesure + gate CI) ne dépend pas du
  service.
- **31.7 doit précéder le basculement `strict` de 31.3.1.** C'est la seule dépendance *forte sortante*
  de cette feature : 31.3.1 propage la classification en `warn` d'emblée, mais le durcissement `strict`
  (refus dur au load) **n'est pas activable tant que `skifer audit --min-coverage` ne montre pas une
  couverture `field_classification` suffisante** sur les projets cibles. Recommandation : livrer 31.7
  en **phase 1/2** (comme tranché en §7.5), l'exécuter en CI en observation (`--min-coverage 0` ou sans
  seuil) pendant la montée de couverture, puis armer le gate quand la couverture visée est atteinte —
  et seulement ensuite basculer 31.3.1 en `strict`.
- Aucune dépendance vers 31.2 (registre) ni 31.4 (incidents). Parallélisable avec tout le reste.

---

## Risques & pièges

1. **Définir « couverture » précisément — sinon le gate est arbitraire.** Deux choix sont figés et
   testés : (a) une métrique « par champ » (descriptions, classification) exige que *tous* les champs
   de `contract.output` la portent (pas « au moins un ») ; (b) `--min-coverage` compare
   `overall_coverage_pct` = **moyenne** des cinq métriques. Ces deux règles sont explicites dans les
   tests (`test_audit_partial_field_coverage`, `test_audit_all_covered`) pour qu'un futur changement
   soit un choix conscient, pas une dérive.

2. **« Owner structuré » n'existe pas encore dans le code (discordance slice ↔ code).** Le slice liste
   « owner structuré » parmi les métriques, mais `schema_loader._normalize_agent_ready_metadata` valide
   `data_product.owner` comme **chaîne non vide uniquement** (l. 480-484) ; un owner en mapping
   `{team, steward, domain}` (introduit seulement en 31.3.2) **échoue au load** aujourd'hui. Décision :
   `has_structured_owner = isinstance(data_product.get("owner"), dict)`, donc la métrique vaut
   **0 % jusqu'à 31.3.2** — c'est une mesure honnête de non-couverture, pas un bug. `test_metric_keys_frozen`
   et `test_audit_none_covered` verrouillent ce comportement ; quand 31.3.2 autorisera le mapping, la
   métrique remontera mécaniquement sans changer le code d'audit. **À signaler au moment du commit.**

3. **Projet à 0 pipeline.** Piège classique du « 100 % de zéro ». Choix : `total==0` ⇒ tous les
   pourcentages à `0.0` **et exit 0 quel que soit le seuil** (rien à gater), en miroir de `validate`
   (« No schema files found. » → exit 0). Testé (`test_audit_zero_pipelines`,
   `test_audit_cli_no_files_exits_zero`). Alternative rejetée : vérité vacue à 100 % (masquerait un
   glob cassé en CI).

4. **Déterminisme du JSON.** Trois sources de non-déterminisme neutralisées : ordre des paths (tri par
   `path` dans `audit_project`), bruit flottant (`round(..., 2)` sur tous les pourcentages), ordre des
   clés (`sort_keys=True` + `coverage` restreint à `METRIC_KEYS`). Piège résiduel : les chemins
   **absolus** varient d'une machine à l'autre — le module n'injecte jamais de chemin absolu (il garde
   le `path` fourni) ; pour un snapshot CI stable, passer des chemins relatifs. Testé
   (`test_audit_deterministic_across_input_order`, `test_audit_report_json_is_sorted_and_stable`).

5. **`audit` mesure, il ne valide pas.** Contrairement à `validate` (exit 1 sur parse-error), `audit`
   absorbe une erreur de parse en `parsed=False` et continue — sinon un seul YAML cassé masquerait la
   couverture de tout le projet. L'exit 1 reste réservé aux échecs techniques de la CLI (glob illégal).
   Ce contraste doit être clair dans le `--help` et est verrouillé par
   `test_audit_unparseable_counts_as_uncovered`.

6. **Ne pas importer `cli` depuis `audit.py`.** Le module redéfinit sa propre `_PLACEHOLDER_RE` /
   logique de params sentinelles pour rester autonome (le cœur/observability ne doit pas dépendre de la
   CLI). C'est la CLI qui importe le module, jamais l'inverse.
