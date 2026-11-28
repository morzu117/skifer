# Plan 47 — reprise (état au 28 novembre 2026)

Document de passation : il permet de reprendre le Plan 47 sur une autre machine sans le
contexte de la session qui a livré le lot A. La source de vérité reste
[`47_wheel_boundaries_plan.md`](47_wheel_boundaries_plan.md) ; en cas de contradiction, le plan
l'emporte.

## 1. Où on en est

| Lot | Points | État |
|---|---|---|
| A | 47.0 – 47.9 | **Implémenté, poussé, PR [#60](https://github.com/morzu117/skifer/pull/60) ouverte vers `main` — non mergée.** |
| B | 47.10 – 47.15 | Non commencé. **Bloqué tant que la PR #60 n'est pas mergée** (« un lot ne démarre que lorsque le précédent est mergé », §4). |
| C | 47.16 – 47.20 | Non commencé. |
| D | 47.21 | Non commencé. |

Branche du lot A : `feat/plan47-a-hardening` (sur `origin`). Elle contient les trois commits du
plan (`89241db`, `b453b2e`, `751bd04`) puis un commit par point :

| Point | Commit | Message |
|---|---|---|
| 47.0 | `585fe33` | chore: build, install and import each wheel alone |
| 47.1 | `8bc3f5e` | fix: closed grammar for the cast type in the SQL compiler |
| 47.2 | `8e8ab97` | fix: quote_fqn accepts a back-quoted name only when well formed |
| 47.3 | `2547835` | fix: refuse (SQL) or warn about (DataFrame) a table never joined |
| 47.6 | `8710fe3` | fix: keep stdout for the protocol on the MCP stdio transport |
| 47.7 | `93900a9` | feat: each wheel's public surface names the distribution to install |
| 47.8 | `fa74799` | chore: wheel metadata — sqlglot upper bound, skifer description, engine extra |
| 47.9 | `b45973f` | chore: confine pyspark/delta/mlflow/databricks imports to engine and serving |
| 47.5 | `70262d1` | fix: value-conditioned capabilities, pinned by parity |
| 47.4 | `c665442` | fix: refuse SELECT * over a join with differently named keys |
| 47.8 | `78a2668` | chore: one README per wheel — role and install by platform |

Gate local au dernier commit : `pytest` 4713 passés / 3 sautés, `ruff` propre, `lint-imports`
4 contrats respectés, `mkdocs build --strict` vert, `scripts/check_wheels.py` vert sur les
quatre wheels réellement construits. CI GitHub de la PR : lancée au moment de la passation,
à vérifier (`gh pr checks 60`).

## 2. Écarts du lot A à reporter (déjà listés dans la PR)

À traiter au lot D (47.21) ou à garder en tête :

1. **Compteurs DuckDB** : 47.5 ajoute deux clés `literal:` → la matrice passe de 48/7/2 à
   **48/8/3** (`Supported`/`Conditional`/`Refused`). Les compteurs sont faux dans
   `docs/integrations/duckdb.md` (couvert par 47.21) **et** dans `docs/core.md` (non listé par
   47.21 — à ajouter à son périmètre).
2. **Message `SkiferEngine`** sans le moteur : nomme encore la distribution `skifer` ; corrigé
   naturellement par 47.18 (le runtime quitte le wheel `skifer`).
3. `tests/parity/test_three_way_parity.py` modifié par 47.5 alors que le brief ne le citait pas
   (nouveau cas de parité + entrée de refus ; compteur de cas 57 → 59).
4. 47.8 tient en deux commits (métadonnées, puis README) ; 47.4 arrive après 47.5 dans
   l'historique (il avait été bloqué puis repris par cherry-pick).
5. Branche locale `wip/plan47-47.4-blocked` (machine portable uniquement, jamais poussée) :
   redondante, à supprimer.

## 3. Reprendre sur une autre machine

### 3.1 Préparer les dépôts

```bash
# cœur
git clone https://github.com/morzu117/skifer.git && cd skifer   # ou git fetch dans un clone existant
git fetch origin
gh pr view 60 --json state,mergedAt          # le lot B n'attend que ça

# connecteurs (lot B, points 47.14 / 47.15) — leurs `main` ne sont qu'un squelette
git clone https://github.com/morzu117/skifer-duckdb.git
git -C skifer-duckdb switch feat/plan46-46.4-connector      # dernier commit f30b40f
git clone https://github.com/morzu117/skifer-snowflake.git
git -C skifer-snowflake switch feat/plan46-46.6-connector   # dernier commit 3b22868
```

### 3.2 Environnement

```bash
uv sync --extra dev --extra spark --extra api --extra mcp   # adapter aux extras utilisés
export JAVA_HOME=/opt/homebrew/opt/openjdk@17               # chemin propre à la machine
export PATH="$JAVA_HOME/bin:$PATH"
pytest tests/ -x --tb=short && ruff check src/ && lint-imports
```

Pièges connus :

- Le venv `uv` n'a pas `pip` : `scripts/check_wheels.py` doit être lancé avec un interpréteur
  qui en a un (sur le portable : le Python 3.12 géré par `uv`,
  `~/.local/share/uv/python/cpython-3.12.*/bin/python3`).
- Sans JRE 17, les 246 tests Spark ne tournent pas : le gate n'est alors **pas** complet, le
  dire dans le handoff.
- Les tests de divergence de parité doivent isoler les loggers PySpark
  `SQLQueryContextLogger` et `DataFrameQueryContextLogger`
  (`monkeypatch.setitem(logging.Logger.manager.loggerDict, name, PySparkLogger(name))`),
  sinon un test qui attend une exception Spark pollue les suivants.
- Jamais de `git stash` nu (pile partagée entre worktrees) : patch ou commit WIP.

## 4. Lot B — ordre et points d'attention

Démarrer **uniquement après le merge de la PR #60** :

```bash
git fetch origin && git switch -c feat/plan47-b-connector-contract origin/main
```

Ordre imposé par le §9 du plan : `47.10 → 47.11 → 47.12`, puis `47.13`, `47.14`, `47.15`
(ces trois-là indépendants entre eux). Les briefs complets sont au §6 du plan.

| Point | Dépôt / branche | À savoir avant de commencer |
|---|---|---|
| 47.10 | `skifer` | Contrat v1 + `RowsContract` dans `core/connector.py`, addendum daté à l'ADR 0001. |
| 47.11 | `skifer` | Nouveau `core/pipeline.py` (12ᵉ module de langage) : le brief **autorise** la modification de `packaging/skifer-core/pyproject.toml` et du contrat import-linter n° 1. |
| 47.12 | `skifer` | `skifer/testing/normalize.py` = reprise **à l'identique** de `tests/_normalize.py` de `skifer-duckdb` (branche `feat/plan46-46.4-connector`). Kit sans dépendance à pytest. |
| 47.13 | `skifer` | S'il n'existe pas de borne naturelle à un site de lecture, **s'arrêter et le signaler** (consigne du brief). |
| 47.14 | `skifer-duckdb`, branche `feat/plan47-shared-contract` créée depuis `feat/plan46-46.4-connector` | La borne basse `skifer-core[dialects]>=…` dépend de la version qui publiera 47.10 – 47.12 : **c'est l'utilisateur qui décide de la version** — ne pas l'inventer, laisser un marqueur et le signaler. |
| 47.15 | `skifer-snowflake`, branche `feat/plan47-shared-contract` créée depuis `feat/plan46-46.6-connector` | Même remarque de version ; aucune capacité Snowflake ne passe à `Supported`. |

Prérequis du §6 : les branches 46.4 et 46.6 des connecteurs ne sont pas mergées. Le plan
autorise à « merger ou baser dessus » : la reprise retient de **baser dessus** ; le merge reste
la décision de l'utilisateur.

## 5. Règles inchangées

Celles du §4 du plan, en particulier : un point = un commit ; insertion-only ; pas de bump de
version ; aucun push, PR ou merge sans accord explicite de l'utilisateur ; D1 – D8 arrêtées,
ne pas les rouvrir ; un test existant cassé par un nouveau refus → s'arrêter et le signaler ;
un échec pré-existant se prouve sur la base sans le diff.

## 6. Message de reprise pour l'agent

> Reprends le Plan 47 de Skifer à partir de `docs/roadmap/47_wheel_boundaries_resume.md`
> (état) et `docs/roadmap/47_wheel_boundaries_plan.md` (source de vérité). Vérifie d'abord que
> la PR #60 est mergée (`gh pr view 60`) ; si elle ne l'est pas, ne démarre pas le lot B et
> dis-le. Sinon, crée `feat/plan47-b-connector-contract` depuis `origin/main` et implémente
> 47.10, 47.11, 47.12 puis 47.13, un commit par point, gate complet après chacun.
</content>
