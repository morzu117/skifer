# Plan 31 — Feature 31.2 : Registre de métadonnées persistant

**But (une ligne).** Indexer, sans Spark, le schéma final projeté de chaque pipeline (`data_product` / `contract` / lineage / rules) dans un registre persistant (SQLite en local, Delta sur Databricks) interrogeable pour l'impact colonne-à-colonne d'une table Silver sur toutes les tables Gold.

**Arbitrages (§7, 2026-09-11) — bakés dans ce plan.**
- **#2 — Backend double.** Registre = SQLite local (`.skifer_metadata.db`) **et** Delta sur Databricks (`_skifer_metadata`), tous deux derrière un Protocol `MetadataStore`. Delta-only est **rejeté** (casserait le mode local).
- **#6 — Ordre.** La feature 31.3 (gouvernance / `GovernanceService`) est livrée **avant** 31.2 mais les fichiers sont disjoints (parallélisable). Le champ `ColumnRecord.classification` reçoit la classification typée produite par la slice 31.3.1 — **couplage doux** noté en 2.1.

**Règle de commit.** `NE JAMAIS` bumper la version. **Une slice = un commit** `feat(plan31-2.M): …` + tests + une entrée `CHANGELOG.md` sous `## [Unreleased]` (à créer au-dessus de `## [2.1.0]` — cette section n'existe pas encore).

---

## État actuel du code

Signatures **réelles** relevées dans le dépôt (à imiter, pas à réinventer).

### Stores à mirrorer

`src/skifer/observability/history.py` :
```python
@runtime_checkable
class HistoryStore(Protocol):
    def store(self, report: MonitorReport) -> None: ...
    def get_last_n(self, table: str, n: int) -> list[MonitorReport]: ...
    def get_latest(self, table: str) -> MonitorReport | None: ...

class SqliteHistoryStore:
    _CREATE_TABLE = "CREATE TABLE IF NOT EXISTS monitor_history (...)"
    def __init__(self, db_path: str = ".skifer_observability.db"):
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute(self._CREATE_TABLE); self._conn.commit()
    def close(self) -> None: self._conn.close()

class DeltaHistoryStore:                                   # self-contained : spark.sql direct
    def __init__(self, backend, table_fqn: str = "_observability.check_history"):
        self._backend = backend; self._table_fqn = table_fqn; self._ensure_table()
    def _spark(self):
        spark = getattr(self._backend, "spark", None)
        if spark is None: raise RuntimeError("DeltaHistoryStore requires a backend with a .spark attribute.")
        return spark
    def _ensure_table(self):  # CREATE TABLE IF NOT EXISTS ... USING DELTA, wrapped in try/except pass
```

`src/skifer/observability/certification_store.py` :
```python
class CertificationStore(Protocol):
    def register_contract(self, definition: ContractDefinition) -> None: ...
    def get_run(self, run_id: str) -> RunEvent | None: ...
    # ... (indexé sur target_fqn physique + definition_hash)

class SqliteCertificationStore:
    def __init__(self, db_path: str = ".skifer_certification.db"):
        self._conn = sqlite3.connect(db_path, check_same_thread=False); self._migrate()
    # upsert idempotent = "INSERT OR IGNORE INTO ... VALUES (...)"  (event_id = clé d'idempotence)

class DeltaCertificationStore:                             # adapter : délègue à backend.append_certification_*
    def __init__(self, backend, schema: str = "_skifer_certification"):
        self.backend, self.schema = backend, schema
    @staticmethod
    def _row(value) -> dict:  # asdict + .value / .isoformat() par champ
```
Deux styles Delta coexistent : **A)** `DeltaHistoryStore` (self-contained, `backend.spark` + `spark.sql`, aucune nouvelle méthode backend) ; **B)** `DeltaCertificationStore` (délègue à des méthodes `backend.append_certification_*`). **Ce plan choisit le style A** pour `DeltaMetadataStore` : pas d'élargissement de l'API backend, un `MERGE INTO` couvre l'upsert idempotent.

### Lineage (`src/skifer/lineage/tracker.py`)

```python
@dataclass
class LineageEdge:
    source_table: str
    source_column: str
    target_table: str
    target_column: str
    transformations: list[str] = field(default_factory=list)
    edge_type: Literal["select", "join", "rule", "metric"] = "select"
    # __eq__/__hash__ excluent transformations (identité = 4 nœuds + edge_type)

class LineageGraph:
    def __init__(self): self._edges: list[LineageEdge] = []
    def add_edge(self, edge: LineageEdge) -> None                 # dédup silencieuse
    def merge(self, other: "LineageGraph") -> None                # <-- AUCUN CALLER aujourd'hui ; 2.3 lui en donne un
    def upstream(self, table: str, column: str) -> list[LineageEdge]     # DIRECT (profondeur 1)
    def downstream(self, table: str, column: str) -> list[LineageEdge]   # DIRECT (profondeur 1)
    @property
    def edges(self) -> list[LineageEdge]
    def tables(self) -> set[str]
    def to_dict(self) -> dict          # {"edges":[{source_table,source_column,target_table,target_column,transformations,edge_type}], "tables":[...], "summary":{...}}
    # PAS de from_dict aujourd'hui  -> ajouté en 2.3

class LineageTracker:
    @staticmethod
    def from_schema(schema_dict: dict, target_name: str | None = None) -> LineageGraph:
        # appelle parse_to_ir(schema_dict) EN INTERNE ; construit edges depuis
        # select_final / add_columns (edge_type="select"), join ("join"), business_rules ("rule").
        # target = target_name or f"{primary_table}_output".
    @staticmethod
    def from_semantic_model(model_dict: dict) -> LineageGraph:   # edge_type="metric"
```
> **⚠ Écart texte de slice ↔ code.** La slice 2.2 dit « `LineageTracker.from_schema` ». Le nom **est exact** (`from_schema`, `@staticmethod`), mais elle prend un **schema_dict normalisé** (pas le `ProjectedSchema`), et appelle `parse_to_ir` elle-même. `index_schema` lui passe donc le `schema_dict`, pas la projection. (Note bis : `tracker.py` a un `@staticmethod` **dupliqué** au-dessus de `selected_subgraph` (l. 354-355) et `from_semantic_model` n'a **pas** de décorateur — bug latent hors périmètre ; ne pas s'appuyer dessus, ne pas le « corriger » dans ces slices sauf si un import casse.)

### Projection (`src/skifer/semantic/output_projection.py`)

```python
@dataclass(frozen=True)
class ProjectedField:
    name: str
    physical_type: str | None
    logical_type: str | None
    source_fields: tuple[str, ...]
    transformations: tuple[str, ...]
    entity: str | None
    inference_status: Literal["known","declared","unknown"]
    inference_reason: str | None

@dataclass(frozen=True)
class ProjectedSchema:
    data_product_id: str | None
    target_hint: str | None            # sink "schema.table" ou "table", sinon None
    fields: tuple[ProjectedField, ...]
    joins: tuple[ProjectedJoin, ...]
    grain: tuple[str, ...]
    definition_hash: str               # sha256 canonique du payload projeté
    is_complete: bool
    incomplete_reason: str | None = None

class OutputProjector:
    def project(self, schema: ParsedSchema) -> ProjectedSchema   # PREND UN ParsedSchema (parse_to_ir), pas un dict
```

### IR (`src/skifer/core/ir.py`)

```python
@dataclass(frozen=True)
class ParsedDataProduct:
    id: str
    version: str
    owner: str | None = None
    description: str | None = None

@dataclass(frozen=True)
class ParsedOutputField:
    name: str
    logical_type: str | None = None
    required: bool | None = None
    unique: bool | None = None
    classification: str | None = None      # <-- couplage doux 31.3.1
    entity: str | None = None
    description: str | None = None

@dataclass
class ParsedSchema:
    ...
    data_product: ParsedDataProduct | None = None
    contract_output: list[ParsedOutputField] = field(default_factory=list)
    contract_grain: list[str] = field(default_factory=list)
    raw: dict = field(default_factory=dict)

def parse_to_ir(schema_dict: dict) -> ParsedSchema: ...
```

### RuleAnalyzer (`src/skifer/core/rule_analyzer.py`)

```python
@dataclass
class RuleProfile:
    name: str
    output_columns: list[str]
    input_columns: list[str]
    raw_expressions: list[str]
    source_available: bool = True
    has_python_udf: bool = False
    loc: int = 0
    withcolumn_count: int = 0

class RuleAnalyzer:
    def analyze_rule(self, func, name: str = ...) -> RuleProfile: ...
```

### Point de PROMOTION dans la publication

Le `PublicationCoordinator` (`src/skifer/observability/publication.py`) marque PROMOTED dans `_publish_run`, qui retourne `PublicationResult(run=promoted, report=report, state="PROMOTED")`. **Le seul appelant** est `src/skifer/core/patterns.py`, `process_to_table`, l. 122-138 :
```python
if has_data_product:
    definition = canonicalize_contract(parse_to_ir(schema_dict))
    coordinator = PublicationCoordinator(e._get_backend(), e.monitor, e.certification_store)
    result = coordinator.publish(df, fqn, schema_dict, definition, run_id=run_id)
    if result.state == "QUARANTINED":
        raise DataQualityError(result.report)
    logger.info("   -> [Certified Publication] run=%s state=%s (target: %s)", result.run.run_id, result.state, fqn)
    # <<< SLICE 2.2 : HOOK NON-BLOQUANT ICI (state == "PROMOTED") >>>
```
À ce point sont disponibles : `e` (SkiferEngine, portera `e.metadata_store`), `schema_dict`, `fqn` (target physique), `result.run.run_id` (le `run_id` d'audit de bout en bout). `run_id` local existe aussi.

### Modèle non-bloquant à copier (`src/skifer/observability/uc_mirror.py`)

```python
@dataclass(frozen=True)
class UcSyncResult:
    status: str
    error: str | None = None

def mirror_certification(backend, table_fqn, certification, owner=None) -> UcSyncResult:
    try:
        ...
        return UcSyncResult("SYNCED")
    except Exception as exc:
        return UcSyncResult("SYNC_ERROR", str(exc))   # ne relève JAMAIS
```

### CLI (`src/skifer/cli.py`)

`main()` : `subparsers = parser.add_subparsers(dest="command")`, dispatch par `if args.command == "..."` (l. 197-209). Sous-commandes existantes : `validate`, `hub`, `semantic`, `mcp`, `adaptive`. Helpers réutilisables :
```python
def _load_pipeline_ir(path: str):        # -> ParsedSchema (parse_schema + parse_to_ir, params sentinelles)
def _read_text_file(path: str) -> str
def _sentinel_params(yaml_text: str) -> dict[str,str]
```
Pattern d'exit codes déjà en place (cf. `run_adaptive_command`, `run_semantic_sync`) : fonction pure `run_*_command(args) -> int`, wrapper `_run_*` qui fait `sys.exit(...)`.

### GovernanceService

`src/skifer/services/` **n'existe pas encore** et `governance.py` non plus. Créé par la **feature 31.1** (slice 31.1.4 = `GovernanceService`). La slice 2.3 **suppose son existence** et lui ajoute des méthodes de requête registre. Dépendance dure notée en « Ordre & dépendances ».

---

## Slice 31.2.1 — `MetadataStore` + records + backends SQLite/Delta

**Objectif.** Poser les dataclasses de record, le Protocol `MetadataStore`, et ses deux implémentations (SQLite local, Delta Databricks) avec upsert idempotent par `(target_fqn, definition_hash)`. Aucune logique d'indexation ici — juste persistance round-trip.

**Fichiers.**
- `src/skifer/observability/metadata_store.py` — **création** : records, Protocol, `SqliteMetadataStore`, `DeltaMetadataStore`.
- `tests/test_metadata_store.py` — **création** : round-trip + idempotence.

**Signatures Python.**
```python
"""metadata_store.py — MetadataStore Protocol + SqliteMetadataStore + DeltaMetadataStore (Plan 31.2)."""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
import json
import sqlite3
from typing import Protocol, runtime_checkable


@dataclass(frozen=True)
class ColumnRecord:
    name: str
    logical_type: str | None = None
    classification: str | None = None          # couplage doux 31.3.1 (typé plus tard)
    description: str | None = None
    sources: tuple[str, ...] = ()              # noms de champs sources (ProjectedField.source_fields)

    def __post_init__(self):
        object.__setattr__(self, "sources", tuple(self.sources))


@dataclass(frozen=True)
class DatasetRecord:
    target_fqn: str                            # clé physique (2 ou 3 parties) — index primaire
    pipeline_path: str
    data_product_id: str | None
    contract_version: str | None
    definition_hash: str                       # 2e composante de la clé d'idempotence
    owner: str | None
    columns: tuple[ColumnRecord, ...]
    indexed_at: datetime
    last_run_id: str | None = None
    # AJOUT au-delà du minimum verbatim de la slice : requis par 2.3 (merge du graphe).
    # Graphe de lineage colonne-à-colonne sérialisé (LineageGraph.to_dict()), rempli en 2.2.
    lineage: dict = field(default_factory=dict)

    def __post_init__(self):
        object.__setattr__(self, "columns", tuple(self.columns))


@runtime_checkable
class MetadataStore(Protocol):
    """Protocol de persistance du registre de métadonnées.

    Upsert idempotent par (target_fqn, definition_hash) : ré-indexer une définition
    inchangée n'écrit rien de nouveau (même ligne remplacée à l'identique)."""
    def upsert(self, record: DatasetRecord) -> bool: ...        # True si contenu changé/inséré, False si no-op
    def get(self, target_fqn: str) -> DatasetRecord | None: ...  # dernier record connu pour ce FQN
    def list_all(self) -> list[DatasetRecord]: ...
    def search_columns(self, text: str) -> list[tuple[str, ColumnRecord]]: ...  # (target_fqn, colonne)


# --- Sérialisation partagée (SQLite + Delta) -------------------------------
def _record_to_json(record: DatasetRecord) -> str: ...          # asdict, datetime -> isoformat, sort_keys=True
def _record_from_json(payload: str) -> DatasetRecord: ...        # reconstruit ColumnRecord/DatasetRecord
def _content_fingerprint(record: DatasetRecord) -> str: ...      # sha256 du JSON SANS indexed_at/last_run_id
                                                                 # -> distingue "no-op" de "vrai changement"


class SqliteMetadataStore:
    _CREATE_TABLE = """
    CREATE TABLE IF NOT EXISTS metadata_registry (
        target_fqn      TEXT NOT NULL,
        definition_hash TEXT NOT NULL,
        content_hash    TEXT NOT NULL,
        record          TEXT NOT NULL,           -- JSON blob DatasetRecord
        indexed_at      TEXT NOT NULL,
        PRIMARY KEY (target_fqn, definition_hash)
    )
    """
    def __init__(self, db_path: str = ".skifer_metadata.db"):
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute(self._CREATE_TABLE); self._conn.commit()

    def upsert(self, record: DatasetRecord) -> bool:
        # 1. lire content_hash existant pour (fqn, definition_hash)
        # 2. si identique -> return False (NO-OP, aucune écriture)
        # 3. sinon INSERT OR REPLACE (record, content_hash, indexed_at) -> return True
        ...
    def get(self, target_fqn: str) -> DatasetRecord | None:
        # dernier par indexed_at DESC pour ce FQN
        ...
    def list_all(self) -> list[DatasetRecord]: ...
    def search_columns(self, text: str) -> list[tuple[str, ColumnRecord]]:
        # match insensible à la casse sur ColumnRecord.name/description parmi list_all()
        ...
    def close(self) -> None: self._conn.close()


class DeltaMetadataStore:
    """Style DeltaHistoryStore : self-contained via backend.spark, aucune nouvelle méthode backend."""
    def __init__(self, backend, table_fqn: str = "_skifer_metadata.datasets"):
        self._backend = backend
        self._table_fqn = table_fqn
        self._ensure_table()
    def _spark(self):
        spark = getattr(self._backend, "spark", None)
        if spark is None:
            raise RuntimeError("DeltaMetadataStore requires a backend with a .spark attribute.")
        return spark
    def _ensure_table(self) -> None:
        # try/except pass ; CREATE TABLE IF NOT EXISTS <table_fqn>
        # (target_fqn STRING, definition_hash STRING, content_hash STRING,
        #  record STRING, indexed_at TIMESTAMP) USING DELTA
        ...
    def upsert(self, record: DatasetRecord) -> bool:
        # SELECT content_hash WHERE target_fqn=? AND definition_hash=? ; si identique -> False.
        # sinon MERGE INTO ... USING (VALUES ...) ON target_fqn+definition_hash
        #        WHEN MATCHED THEN UPDATE SET ... WHEN NOT MATCHED THEN INSERT ...  -> True
        ...
    def get(self, target_fqn: str) -> DatasetRecord | None: ...   # ORDER BY indexed_at DESC LIMIT 1
    def list_all(self) -> list[DatasetRecord]: ...
    def search_columns(self, text: str) -> list[tuple[str, ColumnRecord]]: ...
```

**Comportement & règles.**
- **Clé d'idempotence** = `(target_fqn, definition_hash)` (PK SQLite / clé MERGE Delta). Deux runs d'une même définition écrasent la même ligne.
- **No-op réel** : `upsert` calcule `_content_fingerprint` (JSON du record **sans** `indexed_at` ni `last_run_id`) ; si égal à la ligne existante → **retour `False`, zéro écriture** (pas même un REPLACE, pour ne pas toucher `indexed_at`). C'est ce qui satisfait « re-indexer sans changement n'écrit rien ».
- `indexed_at` et `last_run_id` **n'entrent pas** dans le fingerprint : sinon un simple re-run (nouveau `run_id`) casserait l'idempotence.
- **Parité SQLite/Delta** : même sérialisation JSON (`_record_to_json`/`_record_from_json` partagés), même sémantique d'upsert et de retour bool. Les deux stockent le record comme **blob JSON** (pas de colonnes éclatées) → une évolution de schema record n'exige pas de migration Delta.
- `search_columns` insensible à la casse, borné à `list_all()`.

**Cas de test (`tests/test_metadata_store.py`) — sans Spark (SQLite `:memory:`).** `DeltaMetadataStore` testé avec un **fake backend** exposant `.spark` factice OU marqué `@pytest.mark.skipif` si pas de session ; le round-trip principal cible SQLite.
- `test_sqlite_roundtrip_preserves_all_fields` : upsert un `DatasetRecord` à 3 colonnes → `get(fqn)` renvoie un record **égal** (colonnes, sources, owner, contract_version, lineage dict).
- `test_upsert_returns_true_on_first_insert` : premier upsert → `True`.
- `test_upsert_idempotent_returns_false_and_writes_nothing` : ré-upsert identique → `False` ; `indexed_at` stocké **inchangé** (lire la ligne brute avant/après). Assert : `SELECT COUNT(*)` inchangé, `indexed_at` identique.
- `test_upsert_new_run_id_only_is_still_noop` : même définition, `last_run_id` différent → `False` (fingerprint ignore `last_run_id`).
- `test_upsert_changed_columns_replaces_row` : même `(fqn, hash)` mais colonnes modifiées → `True` et `get` reflète le nouveau contenu (une seule ligne : `COUNT == 1`).
- `test_two_versions_same_fqn_distinct_hash_coexist` : deux `definition_hash` pour le même `target_fqn` → deux lignes ; `get(fqn)` renvoie la plus récente par `indexed_at`.
- `test_search_columns_case_insensitive` : `search_columns("AMOUNT")` trouve la colonne `amount`.
- `test_protocol_isinstance` : `isinstance(SqliteMetadataStore(...), MetadataStore)` (Protocol `runtime_checkable`).
- `test_delta_store_uses_backend_spark` (fake backend) : `_ensure_table` appelé, aucune méthode `append_certification_*` requise sur le backend (assert le fake n'expose que `.spark`).

**Commit.** `feat(plan31-2.1): metadata registry records, MetadataStore protocol, SQLite + Delta backends`
CHANGELOG (`## [Unreleased]` › `### Added`) :
> - `observability/metadata_store.py`: `DatasetRecord`/`ColumnRecord`, the `MetadataStore` Protocol and its `SqliteMetadataStore` (`.skifer_metadata.db`) / `DeltaMetadataStore` (`_skifer_metadata`) backends. Upsert is idempotent by `(target_fqn, definition_hash)`: re-indexing an unchanged definition writes nothing.

**DoD.** `pytest tests/test_metadata_store.py -q` vert ; `ruff check src/skifer/observability/metadata_store.py` propre ; aucun import Spark au niveau module ; `SqliteMetadataStore` importable et utilisable sans Spark.

---

## Slice 31.2.2 — `index_schema` (pur, sans Spark) + CLI `skifer index` + hook non-bloquant

**Objectif.** Fonction pure `index_schema(schema_dict, path) -> DatasetRecord` combinant `OutputProjector` + `LineageTracker.from_schema` + `RuleAnalyzer` ; CLI `skifer index PATHS` qui indexe sans Spark ; hook non-bloquant après `PROMOTED` qui attache le `run_id`.

**Fichiers.**
- `src/skifer/observability/metadata_index.py` — **création** : `index_schema`, helper d'attache du store.
- `src/skifer/cli.py` — **modification** : sous-commande `index`, dispatch, `run_index_command`.
- `src/skifer/core/patterns.py` — **modification** : hook non-bloquant après PROMOTED (l. ~138).
- `src/skifer/core/core.py` — **modification** : `SkiferEngine.__init__(..., metadata_store=None)` + attribut `self.metadata_store`.
- `tests/test_metadata_index.py` — **création**.
- `tests/test_patterns.py` (ou `tests/test_core.py`, selon où vit le test de publication) — **modification** : hook non-bloquant.

**Signatures Python.**
```python
# metadata_index.py
from __future__ import annotations
from datetime import datetime, timezone

from skifer.core.ir import parse_to_ir
from skifer.core.rule_analyzer import RuleAnalyzer
from skifer.lineage.tracker import LineageTracker
from skifer.observability.metadata_store import ColumnRecord, DatasetRecord
from skifer.semantic.output_projection import OutputProjector


def index_schema(
    schema_dict: dict,
    path: str,
    *,
    target_fqn: str | None = None,
    last_run_id: str | None = None,
    now: datetime | None = None,
) -> DatasetRecord:
    """Construit un DatasetRecord de façon PURE et déterministe, sans Spark.

    - parse_to_ir(schema_dict) -> ParsedSchema
    - OutputProjector().project(parsed) -> ProjectedSchema (definition_hash, fields, grain, target_hint)
    - LineageTracker.from_schema(schema_dict, target_name=<fqn résolu>) -> LineageGraph (colonnes)
    - RuleAnalyzer : rules non introspectables -> les colonnes issues de rules restent tracées
      (déjà reflété par ProjectedField.inference_status == "unknown").
    target_fqn résolu par priorité : argument explicite > projected.target_hint
      > data_product.id > f"{primary_table}_output".
    now injectable pour des tests déterministes (défaut datetime.now(timezone.utc))."""
    parsed = parse_to_ir(schema_dict)
    projected = OutputProjector().project(parsed)
    fqn = target_fqn or projected.target_hint or projected.data_product_id \
        or (parsed.tables[0].name + "_output" if parsed.tables else "unknown")

    graph = LineageTracker.from_schema(schema_dict, target_name=fqn)

    declared = {f.name: f for f in parsed.contract_output}   # ParsedOutputField : classification, description
    columns = tuple(
        ColumnRecord(
            name=pf.name,
            logical_type=pf.logical_type,
            classification=(declared[pf.name].classification if pf.name in declared else None),
            description=(declared[pf.name].description if pf.name in declared else None),
            sources=pf.source_fields,
        )
        for pf in projected.fields
    )
    dp = parsed.data_product
    return DatasetRecord(
        target_fqn=fqn,
        pipeline_path=path,
        data_product_id=(dp.id if dp else projected.data_product_id),
        contract_version=(dp.version if dp else None),
        definition_hash=projected.definition_hash,
        owner=(dp.owner if dp else None),
        columns=columns,
        indexed_at=(now or datetime.now(timezone.utc)),
        last_run_id=last_run_id,
        lineage=graph.to_dict(),
    )


def index_from_path(path: str, store, *, target_fqn=None, last_run_id=None) -> bool:
    """Charge le YAML (params sentinelles), construit le record, upsert. Retourne le bool d'upsert."""
    ...
```

```python
# patterns.py — hook non-bloquant, style uc_mirror (JAMAIS relever)
# ... juste après le logger.info "[Certified Publication]" (result.state == "PROMOTED") :
store = getattr(e, "metadata_store", None)
if store is not None:
    try:
        from skifer.observability.metadata_index import index_schema
        record = index_schema(schema_dict, schema_path_hint, target_fqn=fqn,
                              last_run_id=result.run.run_id)
        store.upsert(record)
    except Exception as exc:                      # une défaillance store NE FAIT JAMAIS échouer la publication
        logger.warning("   -> [Metadata] indexing skipped (non-blocking): %s", exc)
```
> `schema_path_hint` : `process_to_table` ne reçoit pas toujours le chemin YAML. Utiliser un hint disponible (ex. `schema_dict.get("_source_path")` s'il existe, sinon le nom du data product, sinon `""`). **À vérifier à l'implémentation** : si aucun chemin n'est disponible dans ce contexte, passer `path=data_product_id or fqn`. Ne pas ajouter d'argument obligatoire à `process_to_table`.

```python
# core.py — SkiferEngine.__init__
def __init__(self, ..., certification_store=None, metadata_store=None, ...):
    ...
    self.metadata_store = metadata_store        # None -> hook inerte (pas de registre)
```

```python
# cli.py — sous-commande
index_parser = subparsers.add_parser("index", help="Index pipeline metadata into the registry (no Spark).")
index_parser.add_argument("paths", nargs="+", metavar="PATHS", help="Pipeline YAML paths to index.")
index_parser.add_argument("--db", default=".skifer_metadata.db", help="SQLite registry path.")
index_parser.add_argument("--target-fqn", default=None, help="Override target FQN (single path only).")
# dispatch : elif args.command == "index": _run_index(args)

INDEX_EXIT_OK, INDEX_EXIT_ERROR, INDEX_EXIT_USAGE = 0, 1, 2

def run_index_command(args, *, store=None) -> int:
    from skifer.observability.metadata_store import SqliteMetadataStore
    from skifer.observability.metadata_index import index_from_path
    store = store or SqliteMetadataStore(args.db)
    if args.target_fqn and len(args.paths) > 1:
        print("[index] --target-fqn is only valid with a single path.", file=sys.stderr)
        return INDEX_EXIT_USAGE
    changed = 0
    for path in args.paths:
        try:
            wrote = index_from_path(path, store, target_fqn=args.target_fqn)
            changed += int(wrote)
            print(f"[index] {path} -> {'updated' if wrote else 'unchanged'}")
        except Exception as exc:
            print(f"[index] Failed to index '{path}': {exc}", file=sys.stderr)
            return INDEX_EXIT_ERROR
    print(f"[index] {changed} record(s) written, {len(args.paths) - changed} unchanged.")
    return INDEX_EXIT_OK

def _run_index(args): sys.exit(run_index_command(args))
```

**Comportement & règles.**
- **Pureté / déterminisme** : `index_schema` n'ouvre aucune session Spark, n'écrit rien. Même `(schema_dict, path)` (avec `now` fixé) → `DatasetRecord` **identique** (le `definition_hash` de `ProjectedSchema` est déjà canonique et trié).
- **Non-bloquant** : le hook copie le pattern `uc_mirror` — `try/except Exception` qui **log un warning une fois et continue**. Une exception du store (DB verrouillée, Delta indisponible) **ne fait jamais échouer** `process_to_table`.
- **Idempotence de bout en bout** : le hook appelle `store.upsert` ; un re-run sans changement de définition → `upsert` renvoie `False`, rien n'est écrit (le nouveau `run_id` seul ne compte pas, cf. 2.1).
- **`run_id` attaché** : `last_run_id=result.run.run_id` (l'identité d'audit frappée par le pipeline), donc le registre pointe vers le run de certification exact.
- **Store absent** → hook inerte (`self.metadata_store is None`), zéro coût, comportement historique inchangé.

**Cas de test.**
- `tests/test_metadata_index.py` (sans Spark) :
  - `test_index_schema_is_pure_and_deterministic` : deux appels avec `now` fixé sur le même dict → records **égaux** (surtout `definition_hash`, `columns`, `lineage`).
  - `test_index_schema_resolves_fqn_from_sink_hint` puis `..._from_data_product_id` puis `..._explicit_override`.
  - `test_index_schema_carries_classification_from_contract` : `contract.output` avec `classification: internal` → `ColumnRecord.classification == "internal"` (soft dep 31.3.1).
  - `test_index_schema_columns_sources_from_projection` : `sources` = `ProjectedField.source_fields`.
  - `test_index_schema_lineage_graph_populated` : `record.lineage["edges"]` non vide ; `LineageGraph.from_dict(record.lineage)` (dispo en 2.3) reconstruit un graphe équivalent — **ce sous-assert est ajouté en 2.3**, ici on assert juste la présence d'edges.
  - `test_index_from_path_upsert_and_noop` : indexer un YAML tempfile deux fois → 1er `True`, 2e `False`.
  - `test_run_index_command_exit_codes` : chemin valide → `0` ; chemin inexistant → `1` ; `--target-fqn` + 2 paths → `2`.
  - `test_index_schema_with_business_rule_marks_unknown` : un pipeline avec `business_rules` → colonnes présentes, `ColumnRecord` créées (rule non introspectable ne casse pas l'indexation). **Besoin fixture `spark`** uniquement si le test construit un `F.col(...)` dans une rule enregistrée ; sinon utiliser une rule déjà enregistrée par un module de test importé.
- `tests/test_patterns.py` (hook) — **besoin de la fixture `spark`** (le chemin certifié écrit un DataFrame) OU double backend/monitor/store existant dans le test de publication :
  - `test_promoted_publication_indexes_metadata` : après une publication PROMOTED, `metadata_store.get(fqn)` renvoie un record dont `last_run_id == result.run.run_id`.
  - `test_metadata_store_failure_does_not_fail_publication` : injecter un `metadata_store` dont `upsert` lève → la publication **réussit** (state PROMOTED, aucune exception propagée), un warning est loggé.
  - `test_publication_without_metadata_store_unchanged` : `metadata_store=None` → comportement identique à aujourd'hui.

**Commit.** `feat(plan31-2.2): index_schema (Spark-free) + skifer index CLI + non-blocking PROMOTED hook`
CHANGELOG (`### Added` + `### Changed`) :
> - `observability/metadata_index.py`: pure `index_schema(schema_dict, path)` building a `DatasetRecord` from `OutputProjector` + `LineageTracker.from_schema` + contract output, and the `skifer index PATHS` CLI (no Spark).
> - `PublicationCoordinator` promotion now triggers a **non-blocking** metadata-index hook (mirrors `uc_mirror`): a store failure never fails a publication, and the persisted record carries the pipeline `run_id`. `SkiferEngine` accepts `metadata_store=`.

**DoD.** `pytest tests/test_metadata_index.py tests/test_patterns.py -q` vert ; `skifer index <yaml>` fonctionne hors Spark ; re-run affiche `unchanged` ; hook prouvé non-bloquant par test.

---

## Slice 31.2.3 — Requêtes sur le graphe mergé + `GovernanceService`

**Objectif.** Interroger le **graphe mergé de tous les records** : `upstream(fqn, column)`, `downstream(fqn, column)`, `impact(fqn) -> ImpactReport`, `search_columns(text)`, exposés via `GovernanceService`. Traversée **récursive bornée en profondeur**, **cycles refusés** (jamais de boucle infinie).

**Fichiers.**
- `src/skifer/lineage/tracker.py` — **modification** : `LineageGraph.from_dict`, traversée récursive bornée + cycle-safe.
- `src/skifer/observability/metadata_store.py` — **modification** : `MetadataRegistryQuery` (merge des records → graphe unifié + `ImpactReport`).
- `src/skifer/services/governance.py` — **modification** (fichier créé par 31.1) : méthodes registre exposées.
- `tests/test_metadata_registry_query.py` — **création** : 3 pipelines chaînés, cycle refusé, profondeur bornée.
- `tests/test_governance.py` — **modification** (existe via 31.1) : délégation.

**Signatures Python.**
```python
# tracker.py — additions à LineageGraph
class LineageGraph:
    @classmethod
    def from_dict(cls, payload: dict) -> "LineageGraph":
        g = cls()
        for e in payload.get("edges", []):
            g.add_edge(LineageEdge(
                source_table=e["source_table"], source_column=e["source_column"],
                target_table=e["target_table"], target_column=e["target_column"],
                transformations=list(e.get("transformations", [])),
                edge_type=e.get("edge_type", "select"),
            ))
        return g

    def upstream_closure(self, table: str, column: str, *, max_depth: int = 20) -> list[LineageEdge]:
        """Provenance transitive, bornée et cycle-safe (visited set sur (table,column))."""
        ...
    def downstream_closure(self, table: str, column: str, *, max_depth: int = 20) -> list[LineageEdge]:
        """Impact transitif, borné et cycle-safe."""
        ...
    def has_cycle(self) -> bool:
        """DFS 3-couleurs sur (table,column) ; True si une arête arrière existe."""
        ...
```

```python
# metadata_store.py — additions
@dataclass(frozen=True)
class ImpactReport:
    root_fqn: str
    impacted_datasets: tuple[str, ...]              # FQN aval distincts (hors racine)
    impacted_columns: tuple[tuple[str, str], ...]   # (fqn, column) atteints
    edges: tuple[dict, ...]                          # arêtes traversées (to_dict d'edge)
    truncated: bool                                  # True si max_depth atteint

class MetadataRegistryQuery:
    """Merge tous les DatasetRecord.lineage en un LineageGraph unifié, puis interroge."""
    def __init__(self, store: MetadataStore, *, max_depth: int = 20):
        self._store = store
        self._max_depth = max_depth
    def merged_graph(self) -> "LineageGraph":
        from skifer.lineage.tracker import LineageGraph
        g = LineageGraph()
        for record in self._store.list_all():
            if record.lineage:
                g.merge(LineageGraph.from_dict(record.lineage))       # <-- 1er caller de merge()
        if g.has_cycle():
            raise ValueError("Metadata lineage graph contains a cycle; refusing to traverse.")
        return g
    def upstream(self, fqn: str, column: str) -> list["LineageEdge"]:
        return self.merged_graph().upstream_closure(fqn, column, max_depth=self._max_depth)
    def downstream(self, fqn: str, column: str) -> list["LineageEdge"]:
        return self.merged_graph().downstream_closure(fqn, column, max_depth=self._max_depth)
    def impact(self, fqn: str) -> ImpactReport:
        """Union des downstream_closure de TOUTES les colonnes de `fqn`."""
        ...
    def search_columns(self, text: str) -> list[tuple[str, "ColumnRecord"]]:
        return self._store.search_columns(text)
```

```python
# services/governance.py — méthodes ajoutées à GovernanceService (créé par 31.1)
class GovernanceService:
    def __init__(self, ..., metadata_store: MetadataStore | None = None):
        ...
        self._registry = MetadataRegistryQuery(metadata_store) if metadata_store else None
    def registry_upstream(self, fqn: str, column: str) -> list[dict]: ...   # edges sérialisés (pas de type Spark)
    def registry_downstream(self, fqn: str, column: str) -> list[dict]: ...
    def registry_impact(self, fqn: str) -> ImpactReport: ...
    def registry_search_columns(self, text: str) -> list[dict]: ...          # [{"target_fqn":..., "column":{...}}]
```

**Comportement & règles.**
- **Merge = 1er caller de `LineageGraph.merge`** : la slice donne enfin un usage à la méthode orpheline. `merge` dédup déjà (via `add_edge`).
- **Bornage de profondeur** : `*_closure(..., max_depth)` limite la traversée ; au-delà, on **arrête** et on marque `ImpactReport.truncated = True` (jamais d'explosion). Défaut `max_depth=20`, configurable.
- **Refus de cycle** : `merged_graph()` appelle `has_cycle()` (DFS 3-couleurs sur nœuds `(table, column)`) et **lève `ValueError`** avant toute traversée si un cycle existe — pas de boucle infinie, pas de résultat partiel silencieux. Les `*_closure` gardent en plus un `visited` set défensif (double protection).
- **Aucun objet Spark** ne traverse la frontière service : `GovernanceService` renvoie des `dict`/`ImpactReport` (dataclass gelée), jamais des `LineageEdge` bruts au-delà de la couche interne si 31.1 impose des DTO — aligner sur le style DTO de 31.1.
- **`impact(fqn)`** : pour chaque colonne du record `get(fqn)`, `downstream_closure` ; union des datasets/colonnes aval distincts.

**Cas de test (`tests/test_metadata_registry_query.py`, sans Spark).**
- `test_three_chained_pipelines_impact` : indexer Bronze→Silver→Gold (3 records ; le `target_fqn` de l'un = `source_table` d'une arête de l'autre) ; `impact("silver.orders")` liste **toutes les tables Gold** touchées (acceptance). Assert `gold.* in report.impacted_datasets`.
- `test_upstream_transitive` : `upstream("gold.kpi", "amount_eur")` remonte jusqu'à une colonne Bronze.
- `test_downstream_transitive` : `downstream("silver.orders", "amount")` atteint les colonnes Gold.
- `test_cycle_is_refused` : fabriquer deux records dont les graphes forment A→B→A ; `merged_graph()` / `impact()` **lève `ValueError`** (`pytest.raises`).
- `test_bounded_depth_truncates` : chaîne plus longue que `max_depth=2` ; `downstream_closure(..., max_depth=2)` s'arrête, `ImpactReport.truncated is True`, résultat fini.
- `test_empty_registry_returns_empty_impact` : store vide → `ImpactReport` vide, pas d'exception.
- `test_merge_dedups_shared_edges` : deux records partageant une arête identique → une seule arête dans `merged_graph`.
- `tests/test_governance.py` : `test_governance_registry_impact_delegates` — `GovernanceService(metadata_store=...).registry_impact(fqn)` renvoie l'`ImpactReport` attendu ; sans store → erreur explicite ou liste vide (aligner sur 31.1).
> Aucune fixture `spark` : les records sont produits par `index_schema` sur des `schema_dict` en mémoire.

**Commit.** `feat(plan31-2.3): merged-graph registry queries (upstream/downstream/impact) via GovernanceService`
CHANGELOG :
> - `LineageGraph.from_dict` + bounded, cycle-safe transitive traversal (`upstream_closure`/`downstream_closure`/`has_cycle`); `MetadataRegistryQuery` merges every record's lineage into one graph and answers `upstream`/`downstream`/`impact`/`search_columns`, exposed through `GovernanceService`. Cyclic lineage is refused; traversal depth is bounded.

**DoD.** `pytest tests/test_metadata_registry_query.py tests/test_governance.py -q` vert ; acceptance prouvée (3 pipelines → impact Silver→Gold) ; cycle → `ValueError` ; profondeur bornée testée.

---

## Slice 31.2.4 — CLI `skifer lineage` & `skifer dictionary`

**Objectif.** `skifer lineage FQN[.column] --direction up|down --format mermaid|json` et `skifer dictionary FQN`, lisant le registre. Sortie stable, exit codes contractuels.

**Fichiers.**
- `src/skifer/cli.py` — **modification** : sous-commandes `lineage`, `dictionary`, dispatch, `run_lineage_command` / `run_dictionary_command`.
- `tests/test_cli_metadata.py` — **création** : exit codes, sortie stable.

**Signatures Python.**
```python
# cli.py
lineage_parser = subparsers.add_parser("lineage", help="Show lineage for a dataset column from the registry.")
lineage_parser.add_argument("target", metavar="FQN[.column]", help="Dataset FQN, optionally .column.")
lineage_parser.add_argument("--direction", choices=["up", "down"], default="down")
lineage_parser.add_argument("--format", choices=["mermaid", "json"], default="mermaid")
lineage_parser.add_argument("--db", default=".skifer_metadata.db")

dictionary_parser = subparsers.add_parser("dictionary", help="Show the column dictionary for a dataset.")
dictionary_parser.add_argument("target", metavar="FQN")
dictionary_parser.add_argument("--db", default=".skifer_metadata.db")
dictionary_parser.add_argument("--format", choices=["text", "json"], default="text")
# dispatch : elif args.command == "lineage": _run_lineage(args) ; elif "dictionary": _run_dictionary(args)

META_EXIT_OK, META_EXIT_ERROR, META_EXIT_NOT_FOUND, META_EXIT_USAGE = 0, 1, 3, 2

def run_lineage_command(args, *, store=None) -> int:
    # 1. parse FQN[.column] : split au dernier '.' seulement si la partie gauche est un FQN connu du registre,
    #    sinon traiter tout comme FQN et lister toutes ses colonnes (via record.columns).
    # 2. MetadataRegistryQuery(store).upstream/downstream par colonne -> LineageGraph (sous-graphe)
    # 3. --format mermaid -> LineageRenderer().to_mermaid(subgraph) ; json -> json.dumps(subgraph.to_dict(), sort_keys=True)
    # exit : 0 ok ; 3 FQN/colonne inconnu du registre ; 2 usage ; 1 erreur technique.
    ...

def run_dictionary_command(args, *, store=None) -> int:
    # store.get(FQN) ; None -> META_EXIT_NOT_FOUND.
    # text : lignes "name  logical_type  classification  sources"  (tri par name, colonnes alignées)
    # json : json.dumps({"target_fqn":..., "columns":[asdict(c)...]}, sort_keys=True, default=str)
    ...
```

**Comportement & règles.**
- **Réutilise l'existant** : rendu Mermaid via `LineageRenderer().to_mermaid(graph)` (déjà présent, styles par `edge_type`) ; JSON via `LineageGraph.to_dict()` déjà trié/déterministe. Ne pas réimplémenter un renderer.
- **Parsing `FQN[.column]`** : ambigu car un FQN local est `schema.table` (contient déjà un point). Règle : si `target` **entier** matche un `record.target_fqn` connu → dataset entier (toutes colonnes) ; sinon découper au **dernier** `.` et vérifier que la partie gauche est un FQN connu ; sinon `META_EXIT_NOT_FOUND`. Documenter cette règle dans l'aide.
- **Sortie stable** : JSON `sort_keys=True` ; Mermaid déterministe (edges dans l'ordre d'insertion, déjà dédup) ; texte trié par nom de colonne. Aucune horodatage ni chemin absolu dans la sortie (reproductible entre machines).
- **Exit codes** : `0` succès · `1` erreur technique · `2` usage · `3` FQN/colonne absent du registre.

**Cas de test (`tests/test_cli_metadata.py`, sans Spark ; store SQLite `:memory:` ou tempfile pré-rempli via `index_schema`).**
- `test_lineage_down_mermaid_exit_0` : sortie commence par `graph LR` (ou `graph`), contient les nœuds attendus, exit `0`.
- `test_lineage_up_json_stable` : deux exécutions → **octet-pour-octet identiques** (déterminisme).
- `test_lineage_unknown_fqn_exit_3`.
- `test_lineage_column_form` : `skifer lineage gold.kpi.amount_eur --direction up` renvoie le sous-graphe de cette seule colonne.
- `test_dictionary_text_sorted` : colonnes triées par nom, exit `0`.
- `test_dictionary_json_contains_classification` : JSON porte `classification`/`sources`.
- `test_dictionary_unknown_fqn_exit_3`.
- `test_dictionary_bad_format_exit_2` (argparse `choices` → SystemExit code 2 : encadrer avec `pytest.raises(SystemExit)`).
> Aucune fixture `spark`.

**Commit.** `feat(plan31-2.4): skifer lineage & skifer dictionary CLI over the metadata registry`
CHANGELOG :
> - `skifer lineage FQN[.column] --direction up|down --format mermaid|json` and `skifer dictionary FQN` read the persistent registry (Mermaid via the existing `LineageRenderer`, deterministic JSON). Stable output, contractual exit codes (`0/1/2/3`).

**DoD.** `pytest tests/test_cli_metadata.py -q` vert ; sorties stables entre deux runs ; exit codes conformes ; `ruff check src/skifer/cli.py` propre.

---

## Ordre & dépendances internes

```
31.2.1  (records + Protocol + stores)          ── aucune dépendance interne
   │
31.2.2  (index_schema + CLI index + hook)       ── dépend de 2.1 (DatasetRecord/store)
   │
31.2.3  (merged-graph queries + GovernanceService) ── dépend de 2.1 (store), 2.2 (records.lineage peuplé)
   │                                              ── DÉPEND DE 31.1.4 : services/governance.py DOIT exister
31.2.4  (CLI lineage & dictionary)              ── dépend de 2.1 (store) et 2.3 (MetadataRegistryQuery)
```

- **2.1 → 2.2 → 2.3 → 2.4** strict.
- **Couplage doux 31.3.1** : `ColumnRecord.classification` / `ParsedOutputField.classification` reçoivent la classification **typée** produite par 31.3.1. En attendant, `classification` reste `str | None` (valeur brute du contrat). Aucune slice de 31.2 n'est bloquée par 31.3 ; si 31.3.1 change le type, seul le typage du champ évolue (pas la persistance JSON).
- **Dépendance dure 31.1.4** : `GovernanceService` (fichier `src/skifer/services/governance.py`) est **créé par la feature 31.1**. La slice 2.3 le **modifie** (n'invente pas le fichier). Si 31.1 n'est pas encore mergée au moment d'implémenter 2.3, livrer 2.3 sur une base où `services/governance.py` existe (ordre §6 : 31.3 avant 31.2 ; 31.1 fournit le service). **Vérifier l'existence du fichier avant 2.3** ; sinon coordonner avec 31.1.

---

## Risques & pièges

1. **Cycles de merge (2.3).** Le graphe unifié de tous les records **peut** contenir un cycle (Silver lit Gold via une vue circulaire, ou un mauvais indexage). `merged_graph()` **doit** appeler `has_cycle()` et lever `ValueError` **avant** toute traversée. Ne pas se reposer uniquement sur le `visited` set des `*_closure` : un cycle non détecté en amont donnerait un impact partiel silencieux. Double protection (has_cycle + visited).
2. **Bornage de profondeur (2.3).** `*_closure` **doit** respecter `max_depth` et exposer `truncated`. Un impact tronqué qui se présente comme complet est pire qu'une erreur : toujours signaler la troncature.
3. **Parité Delta ↔ SQLite (2.1).** Même sérialisation JSON, même sémantique de retour d'`upsert` (`bool`), même clé `(target_fqn, definition_hash)`. Le `MERGE INTO` Delta doit reproduire le no-op SQLite : **lire d'abord** le `content_hash`, ne réécrire que s'il diffère (sinon `indexed_at` bougerait et l'idempotence Delta divergerait de SQLite). Tester le no-op sur les deux backends (Delta avec fake backend ou skipif).
4. **Placement du hook non-bloquant (2.2).** Le hook va **après** le point PROMOTED dans `patterns.py` (l. ~138), **jamais** dans `PublicationCoordinator._publish_run` (qui doit rester agnostique du registre et déjà porte le tracing). Copier le pattern `uc_mirror` : `try/except Exception` + `logger.warning` unique, **jamais** de `raise`. Ne **pas** dupliquer le flux métier selon la présence du store (l'anti-pattern explicitement cité pour le tracing en slice 5.2) : un seul chemin, hook conditionné par `metadata_store is not None`.
5. **Résolution du `target_fqn` sans Spark (2.2).** Le FQN physique n'est pas toujours dans le `schema_dict` (calculé par l'engine en runtime). Priorité claire : override explicite > `projected.target_hint` (sink) > `data_product.id` > `f"{primary_table}_output"`. Le chemin certifié passe le `fqn` runtime réel via le hook ; le chemin CLI `index` s'appuie sur la résolution statique. **Risque de divergence** : un même pipeline indexé par CLI vs par hook peut recevoir deux `target_fqn` différents si le sink n'est pas déclaré → documenter et privilégier `--target-fqn` ou un `data_product.id` = FQN.
6. **`chemin YAML` indisponible dans le hook (2.2).** `process_to_table` ne reçoit pas le path source. `pipeline_path` peut retomber sur `data_product_id`/`fqn`. Vérifier à l'implémentation ; ne pas élargir la signature de `process_to_table` juste pour ça.
7. **`from_schema` prend le dict, pas la projection (2.2).** Piège de signature : `LineageTracker.from_schema(schema_dict, target_name=...)` (parse_to_ir interne), tandis que `OutputProjector().project(...)` prend un `ParsedSchema`. `index_schema` fait donc **un** `parse_to_ir` pour la projection **et** passe le `schema_dict` brut au tracker.
8. **`tracker.py` a un décorateur `@staticmethod` dupliqué et un `from_semantic_model` sans décorateur** (l. 354-373). Hors périmètre : ne pas y toucher dans 31.2 sauf si un import casse. `from_schema` (le seul utilisé ici) est correct.
9. **`## [Unreleased]` absent du CHANGELOG.** Le sommet est `## [2.1.0]`. La slice 2.1 crée la section `## [Unreleased]` au-dessus ; les suivantes y ajoutent.
10. **Fixture `spark`.** Le registre est testable **sans Spark** (index depuis un `schema_dict`/projection). Seuls les tests du hook de publication (2.2, `tests/test_patterns.py`) qui écrivent réellement un DataFrame — et toute rule construisant `F.col(...)` — exigent la fixture `spark` de `tests/conftest.py`.
```
