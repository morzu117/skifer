# Plan 31 — Feature 31.4 : Incidents et routage d'alertes

**But.** Sur QUARANTINE, ouvrir automatiquement un **incident** (un par check critique, dédupliqué
tant qu'ouvert) dans le **même** store de certification (SQLite local + Delta Databricks) ; l'auto-résoudre
`recovered` au prochain PROMOTED. Router les alertes vers le **propriétaire** du dataset (31.3) **et ses
consommateurs aval** via `downstream()` (31.2), ajouter les canaux **MS Teams** et **Google Chat** à
`AlertDispatcher`, et exposer les transitions d'incident en CLI (`skifer incidents …`) et via
`QualityService` sous le scope `incidents:write`.

**Arbitrages (§7, 2026-09-11) — bakés dans ce plan.**
- **Non-bloquant partout**, comme `observability/uc_mirror.py` : un échec d'ouverture d'incident, de
  résolution ou d'envoi sur un canal est un `warnings.warn(..., RuntimeWarning)` journalisé **une fois**,
  **jamais** une publication échouée. Le comportement métier (promote/quarantine) ne dépend jamais des
  incidents ni des alertes.
- **Ordre de dépendances** : 31.2.3 (`downstream`/`GovernanceService`) **et** 31.3.2 (`owner` structuré)
  **précèdent** 4.2 ; 31.3.5 (`ContractDiff.breaking`) précède 4.2 ; 31.1.4 (`QualityService`) précède 4.3.
- **Aucune valeur de donnée dans une alerte ni dans un incident** — redaction comme la couche preuve /
  tracing : on ne persiste et n'émet que `check_name` (nom de classe du contrat), `severity`, `target_fqn`,
  `run_id`, `column` (schéma, pas donnée). Jamais `message`, `actual_value`, `expected_value`.

**Discipline de commit.** `pyproject.toml` **jamais** touché. **Une slice = un commit**
`feat(plan31-4.M): …` + tests + une entrée `CHANGELOG.md` sous `## [Unreleased]`. La première slice qui
touche le CHANGELOG **crée** la section `## [Unreleased]` (HEAD = `release: 2.1.0`, aucune section
`[Unreleased]` n'existe encore).

---

## État actuel du code

### Store de certification (`src/skifer/observability/certification_store.py`)

Famille de stores à étendre. Protocole + deux implémentations.

```python
class CertificationStore(Protocol):
    def register_contract(self, definition: ContractDefinition) -> None: ...
    def get_contract(self, contract_id: str, version: str) -> ContractDefinition | None: ...
    def append_run_event(self, event: RunEvent) -> None: ...
    def append_check_results(self, results: Sequence[StoredCheckResult]) -> None: ...
    def get_check_results(self, run_id: str) -> list[StoredCheckResult]: ...
    def get_run(self, run_id: str) -> RunEvent | None: ...
    def get_latest_promoted(self, dataset: str) -> RunEvent | None: ...
    def get_certification(self, dataset: str, consumer_class: str = "default") -> Certification: ...
    def list_history(self, dataset: str, limit: int = 50) -> list[RunEvent]: ...
```

`SqliteCertificationStore(db_path=".skifer_certification.db")` — `sqlite3.connect(check_same_thread=False)`
puis `self._migrate()`. `_migrate()` exécute un `executescript` **idempotent** (`CREATE TABLE IF NOT EXISTS …`)
et insère `INSERT OR IGNORE INTO schema_migrations(version) VALUES (1)`. Les écritures existantes utilisent
`INSERT OR IGNORE` (immuables, `event_id` = clé d'idempotence) puis `self._conn.commit()`. Tables actuelles :
`schema_migrations`, `contract_definitions`, `materialization_runs`, `check_results`.

`DeltaCertificationStore(backend, schema="_skifer_certification")` — adaptateur mince. `_row(value)`
`asdict` + coerce (`.value` pour enums, `.isoformat()` pour datetimes). Délègue à des primitives backend :
`backend.append_certification_run/…`, `get_certification_run`, `get_latest_certification_promotion`, etc.

Primitives Delta correspondantes dans `src/skifer/core/spark_backend.py` (schéma append-only, dédup par clé) :

```python
def _append_certification(self, schema: str, table: str, row: dict, key: str) -> None:
    qualified = f"{quote_ident(schema)}.{quote_ident(table)}"
    plain = f"{schema}.{table}"
    self._spark.sql(f"CREATE SCHEMA IF NOT EXISTS {quote_ident(schema)}")
    if self._spark.catalog.tableExists(plain):
        escaped = escape_sql_string(row[key])
        if self._spark.sql(f"SELECT 1 FROM {qualified} WHERE {quote_ident(key)} = '{escaped}' LIMIT 1").collect():
            return                     # dédup par clé : réécrire un id existant est un no-op
    self._spark.createDataFrame([row]).write.format("delta").mode("append").saveAsTable(plain)
# + append_certification_run/check/contract, get_certification_run, get_latest_certification_promotion,
#   list_certification_history, get_certification_check_results (tous SELECT … tableExists-guardés).
```

> **Différence structurelle clé pour les incidents** : les run events sont **immuables** (append-only,
> `INSERT OR IGNORE`). Un incident **change d'état** (NEW→…→RESOLVED). C'est le seul enregistrement mutable
> de la famille — SQLite fera de l'`UPDATE`, Delta un `MERGE INTO` sur `id`. Voir Risques & pièges.

### Point de décision PROMOTED / QUARANTINE

Le coordinateur produit la décision, mais **elle affleure au niveau métier dans
`src/skifer/core/patterns.py`** (`PipelinePatterns.run_process_to_table`, lignes 122-138) :

```python
if has_data_product:
    from skifer.observability.publication import PublicationCoordinator
    definition = canonicalize_contract(parse_to_ir(schema_dict))
    coordinator = PublicationCoordinator(e._get_backend(), e.monitor, e.certification_store)
    result = coordinator.publish(df, fqn, schema_dict, definition, run_id=run_id)
    if result.state == "QUARANTINED":
        raise DataQualityError(result.report)          # ← QUARANTINE lève ICI
    logger.info("   -> [Certified Publication] run=%s state=%s (target: %s)",
                result.run.run_id, result.state, fqn)  # ← PROMOTED loggue ICI
```

Dans `src/skifer/observability/publication.py`, `PublicationCoordinator._publish_run` prend la décision :
branche `report.has_critical_failures()` → `quarantine_staging(...)` (span `decision=QUARANTINED`,
retourne `PublicationResult(state=outcome.state)` = `"QUARANTINED"`) ; sinon `promote_staging(...)`
(span `decision=PROMOTED`, retourne `state="PROMOTED"`). Éléments disponibles au point de décision :
`run.run_id`, `run.target_fqn`, `definition.contract_version`, et `report` (donc `report.results`,
chaque `CheckResult` avec `severity`, `contract`, `status`/`passed`, `message`, `actual_value`).
`resume(run, definition)` (reprise après crash) promeut directement (`promote_staging`) et retourne
`state="PROMOTED"` **sans passer par `_publish_run`** ni par `patterns.py`.

> **Choix de placement retenu** : le hook incident vit **dans le coordinateur**
> (`_publish_run` et `resume`), au plus près de la décision, appelant des méthodes non-bloquantes du store.
> Cela couvre `resume()` (que `patterns.py` ne voit pas) et garantit la résolution `recovered` sur reprise.
> Le coordinateur détient déjà `self.store` — aucune injection supplémentaire n'est nécessaire.

### `MonitorReport` (`src/skifer/observability/monitor.py`)

```python
@dataclass
class MonitorReport:
    table: str
    results: list[CheckResult]
    timestamp: datetime
    def has_critical_failures(self) -> bool:            # any(not r.passed and r.severity == "critical")
    def failures(self) -> list[CheckResult]:            # [r for r in results if not r.passed]  (toutes sévérités)
    def summary(self) -> dict[str, Any]:                # status: PASS | WARN | CRITICAL
```

### Sévérité & noms de check (`src/skifer/observability/checks.py`)

`CheckResult` porte `severity: str` (défaut `"warning"`), `status: CheckStatus` (`PASS|FAIL|ERROR|SKIPPED`),
`passed` (propriété : `status is PASS`), `contract`, `message`, `actual_value`, `expected_value`. Le
**nom de check** métier est `type(result.contract).__name__` (ex. `NullCheck`, `UniqueCheck`,
`FreshnessCheck`, `VolumeCheck`, `SchemaDriftCheck`, `FilterInvariantCheck`, `CustomSqlCheck`). Les contrats
à colonne exposent `.column` (`NullCheck`, `TypeCheck`, `FilterInvariantCheck`) — deux `NullCheck` sur des
colonnes différentes sont **deux checks distincts**. `severity` est une **chaîne libre** ; « critique » =
`severity == "critical"` (convention du reste du code). `ContractScope` (`ROW|DATASET`) existe mais n'est
pas nécessaire aux incidents.

### `AlertDispatcher` (`src/skifer/observability/alerts.py`)

Interface unique + **enregistrement des canaux par présence de clé de config** (pas de registre explicite) :

```python
class AlertDispatcher:
    _SEVERITY_RANK = {"info": 0, "warning": 1, "critical": 2}

    def dispatch(self, report: MonitorReport, config: dict) -> list[str]:
        # filtre les failures par min_severity (défaut "warning"), retourne [] si aucune
        # if config.get("webhook_url"):   self._send_webhook(...);  notified.append("webhook")
        # if config.get("slack_webhook"): self._send_slack(...);    notified.append("slack")
        # if config.get("email"):         self._send_email(...);     notified.append("email")
        return notified

    def _build_payload(self, report, failures) -> dict          # ⚠️ inclut "actual"/"expected" → FUITE de données
    def _send_webhook(self, url: str, payload: dict) -> None     # POST JSON, timeout=10, échec → warnings.warn RuntimeWarning
    def _send_slack(self, webhook_url, report, failures) -> None # construit {"text": …}, réutilise _send_webhook
    def _send_email(self, cfg: dict, report, failures) -> None   # SMTP, échec → warnings.warn RuntimeWarning
```

Canaux existants : `webhook`, `slack`, `email`. **Déjà non-bloquant** (webhook & email attrapent et
`warnings.warn`). Clés de config : `webhook_url`, `slack_webhook`, `email`, `min_severity`.
**Piège à corriger dans les chemins d'alerte-incident** : `_build_payload` sérialise `actual`/`expected`
= valeurs de donnée ; les nouveaux chemins ne doivent **jamais** l'utiliser.

### CLI (`src/skifer/cli.py`)

`main()` : `subparsers = parser.add_subparsers(dest="command")` ; chaque groupe a ses sous-parsers
(`semantic`, `mcp`, `adaptive`). Dispatch en cascade `if args.command == "…"`. Modèle à copier
(`adaptive`) : constantes d'exit code en tête de module, `_run_X(args)` → `sys.exit(run_X_command(args))`,
`run_X_command(args, *, service=None) -> int` qui **injecte** le service pour la testabilité, imprime en
JSON trié, et mappe chaque famille d'exception à un exit code stable.

```python
ADAPTIVE_EXIT_OK = 0 ; ADAPTIVE_EXIT_ERROR = 1 ; ADAPTIVE_EXIT_USAGE = 2
ADAPTIVE_EXIT_STALE = 3 ; ADAPTIVE_EXIT_CONFLICT = 4 ; ADAPTIVE_EXIT_REGRESSED = 5
```

### Dépendances externes (features sœurs de Plan 31 — pas encore mergées)

- **31.1.4** `services/quality.py` : `QualityService.__init__(self, history_store, *, contract_extractor=None)`,
  vues gelées `CheckDefinitionView` / `QualityReportView`. `services/context.py` fournit `RequestContext`,
  `require_scope`, la hiérarchie d'erreurs (`ScopeDenied`, `InvalidRequest`, `ResourceNotFound`,
  `ResourceUnavailable`) et les scopes nommés, dont **`SCOPE_INCIDENTS_WRITE = "incidents:write"`** et
  `SCOPE_CONTRACTS_READ = "contracts:read"`. `LocalIdentity` reçoit tous les scopes nommés **sauf**
  `certification_override` (donc `incidents:write` est disponible en CLI locale).
- **31.2.3** `GovernanceService.registry_downstream(ctx, fqn, column) -> list[dict]` (edges sérialisés,
  aucun objet Spark), fondé sur `LineageGraph.downstream_closure(table, column, *, max_depth=20)` —
  **traversée bornée en profondeur, cycles refusés (`ValueError`)**.
- **31.3.2** `owner` **structuré** sur le record de métadonnées : `{team, steward, domain, contact}`
  (le `contact` est l'adresse e-mail routable). Exposé via `GovernanceService` (DTO).
- **31.3.5** `ContractDiff` avec `.breaking: bool` (comparaison de deux versions de contrat).

Ces quatre points sont des **dépendances dures** de 4.2 (owner+downstream+ContractDiff) et 4.3
(`QualityService`+scope). Ils sont **stubbables** en test (protocoles/duck-typing) mais doivent être
implémentés avant l'intégration réelle.

---

## Slice 31.4.1 — `Incident` + persistance store (SQLite & Delta) + hooks quarantine/promoted

### Objectif
Introduire l'`Incident` (dataclass + machine à états), l'étendre à la famille de store de certification
(SQLite + Delta), ouvrir automatiquement un incident **par check critique** sur QUARANTINE (dédupliqué
tant qu'ouvert, idempotent sous `resume()`), et l'auto-résoudre `recovered` au prochain PROMOTED. Tout
non-bloquant, sans aucune valeur de donnée persistée.

### Fichiers
- `src/skifer/observability/incidents.py` — **création** : `IncidentStatus`, `Incident`,
  `InvalidIncidentTransition`, `incident_check_name()`, `incidents_from_report()`.
- `src/skifer/observability/certification_store.py` — **modification** : étendre le `Protocol`
  `CertificationStore`, la migration SQLite (v2 : table `incidents`), les méthodes SQLite, et l'adaptateur
  `DeltaCertificationStore`.
- `src/skifer/core/spark_backend.py` — **modification** : primitives Delta `upsert_incident`,
  `get_open_incident`, `list_open_incidents`, `list_incidents`, `get_incident`.
- `src/skifer/observability/publication.py` — **modification** : hook non-bloquant dans
  `_publish_run` (quarantine → open ; promote → resolve recovered) et `resume` (resolve recovered).
- `src/skifer/observability/__init__.py` — **modification** : exporter `Incident`, `IncidentStatus`.

### Signatures Python

```python
# observability/incidents.py
from __future__ import annotations
from dataclasses import dataclass, replace
from datetime import datetime
from enum import Enum


class IncidentStatus(str, Enum):
    NEW = "NEW"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    ASSIGNED = "ASSIGNED"
    RESOLVED = "RESOLVED"


# Transitions autorisées. RESOLVED est terminal. resolve() est atteignable depuis tout état ouvert
# (couvre l'auto-résolution `recovered` ET le resolve manuel). Toute autre transition est refusée.
_ALLOWED: dict[IncidentStatus, frozenset[IncidentStatus]] = {
    IncidentStatus.NEW:          frozenset({IncidentStatus.ACKNOWLEDGED, IncidentStatus.ASSIGNED, IncidentStatus.RESOLVED}),
    IncidentStatus.ACKNOWLEDGED: frozenset({IncidentStatus.ASSIGNED, IncidentStatus.RESOLVED}),
    IncidentStatus.ASSIGNED:     frozenset({IncidentStatus.ASSIGNED, IncidentStatus.RESOLVED}),  # réassignation permise
    IncidentStatus.RESOLVED:     frozenset(),                                                    # terminal
}


class InvalidIncidentTransition(ValueError):
    """Refus d'une transition d'état interdite (ex. RESOLVED→*, ASSIGNED→NEW)."""


@dataclass(frozen=True)
class Incident:
    id: str
    target_fqn: str
    run_id: str
    check_name: str          # type(contract).__name__ [+ ":" + column] — JAMAIS de valeur de donnée
    severity: str
    status: IncidentStatus
    opened_at: datetime
    assignee: str | None = None
    root_cause: str | None = None
    resolved_at: datetime | None = None

    def transition(self, new_status: IncidentStatus, *, at: datetime,
                   assignee: str | None = None, root_cause: str | None = None) -> "Incident":
        if new_status not in _ALLOWED[self.status]:
            raise InvalidIncidentTransition(f"Illegal incident transition {self.status.value} → {new_status.value}.")
        resolved_at = at if new_status is IncidentStatus.RESOLVED else self.resolved_at
        return replace(self, status=new_status,
                       assignee=assignee if assignee is not None else self.assignee,
                       root_cause=root_cause if root_cause is not None else self.root_cause,
                       resolved_at=resolved_at)


def incident_check_name(contract) -> str:
    """Nom de check stable et redacté : type + colonne (schéma), jamais de valeur."""
    column = getattr(contract, "column", None)
    return f"{type(contract).__name__}:{column}" if column else type(contract).__name__


def incidents_from_report(report, *, run_id: str, target_fqn: str, at: datetime) -> list[Incident]:
    """Un Incident NEW par check critique en échec — dédupliqué par check_name dans le rapport.

    id = f"{run_id}:{check_name}" (idempotence de resume : même run rejoué ⇒ même id).
    AUCUN message/actual_value n'est copié (redaction).
    """
    seen: set[str] = set()
    out: list[Incident] = []
    for r in report.results:
        if r.passed or r.severity != "critical":
            continue
        name = incident_check_name(r.contract)
        if name in seen:
            continue
        seen.add(name)
        out.append(Incident(id=f"{run_id}:{name}", target_fqn=target_fqn, run_id=run_id,
                            check_name=name, severity=r.severity, status=IncidentStatus.NEW, opened_at=at))
    return out
```

```python
# observability/certification_store.py — ajouts au Protocol CertificationStore
class CertificationStore(Protocol):
    ...  # existant inchangé
    def open_incident(self, incident: Incident) -> Incident: ...
    def get_open_incident(self, target_fqn: str, check_name: str) -> Incident | None: ...
    def list_open_incidents(self, target_fqn: str) -> list[Incident]: ...
    def resolve_open_incidents(self, target_fqn: str, *, root_cause: str, resolved_at: datetime) -> list[Incident]: ...
    def get_incident(self, incident_id: str) -> Incident | None: ...
    def update_incident(self, incident_id: str, new_status: IncidentStatus, *, at: datetime,
                        assignee: str | None = None, root_cause: str | None = None) -> Incident: ...
    def list_incidents(self, *, status: str | None = None, target_fqn: str | None = None,
                       limit: int = 50) -> list[Incident]: ...


# --- SqliteCertificationStore : migration v2 + méthodes -----------------------------
# Dans _migrate(), ajouter à l'executescript (idempotent) :
#   CREATE TABLE IF NOT EXISTS incidents (
#     id TEXT PRIMARY KEY, target_fqn TEXT NOT NULL, run_id TEXT NOT NULL,
#     check_name TEXT NOT NULL, severity TEXT NOT NULL, status TEXT NOT NULL,
#     opened_at TEXT NOT NULL, assignee TEXT, root_cause TEXT, resolved_at TEXT);
#   CREATE INDEX IF NOT EXISTS idx_incidents_open ON incidents(target_fqn, check_name, status);
#   INSERT OR IGNORE INTO schema_migrations(version) VALUES (2);

def open_incident(self, incident: Incident) -> Incident:
    # Dédup tant qu'ouvert : un incident non-RESOLVED existe déjà pour (target_fqn, check_name) ⇒ le rendre tel quel.
    existing = self.get_open_incident(incident.target_fqn, incident.check_name)
    if existing is not None:
        return existing
    # INSERT OR IGNORE sur id ⇒ resume (même run_id:check_name) ne crée pas de doublon.
    self._conn.execute(
        "INSERT OR IGNORE INTO incidents (id, target_fqn, run_id, check_name, severity, status, opened_at, assignee, root_cause, resolved_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (incident.id, incident.target_fqn, incident.run_id, incident.check_name, incident.severity,
         incident.status.value, incident.opened_at.isoformat(), incident.assignee, incident.root_cause, None))
    self._conn.commit()
    return self.get_incident(incident.id) or incident

def get_open_incident(self, target_fqn, check_name) -> Incident | None:      # status != 'RESOLVED', LIMIT 1
def list_open_incidents(self, target_fqn) -> list[Incident]:                 # status != 'RESOLVED'
def resolve_open_incidents(self, target_fqn, *, root_cause, resolved_at) -> list[Incident]:
    # pour chaque open : UPDATE status='RESOLVED', root_cause=?, resolved_at=? (transition validée en amont
    # via Incident.transition ; resolve est légal depuis tout état ouvert). Retourne les incidents résolus.
def get_incident(self, incident_id) -> Incident | None:                      # SELECT … WHERE id=?
def update_incident(self, incident_id, new_status, *, at, assignee=None, root_cause=None) -> Incident:
    current = self.get_incident(incident_id)
    if current is None:
        raise KeyError(incident_id)
    updated = current.transition(new_status, at=at, assignee=assignee, root_cause=root_cause)  # peut lever InvalidIncidentTransition
    self._conn.execute("UPDATE incidents SET status=?, assignee=?, root_cause=?, resolved_at=? WHERE id=?",
                       (updated.status.value, updated.assignee, updated.root_cause,
                        updated.resolved_at.isoformat() if updated.resolved_at else None, incident_id))
    self._conn.commit()
    return updated
def list_incidents(self, *, status=None, target_fqn=None, limit=50) -> list[Incident]:  # filtres optionnels, ORDER BY opened_at DESC


# --- DeltaCertificationStore : délègue aux primitives backend ------------------------
def open_incident(self, incident: Incident) -> Incident:
    existing = self.get_open_incident(incident.target_fqn, incident.check_name)
    if existing is not None:
        return existing
    self.backend.upsert_incident(self.schema, self._row(incident))   # MERGE INTO … ON id (insert-if-absent)
    return self.get_incident(incident.id) or incident
def update_incident(self, incident_id, new_status, *, at, assignee=None, root_cause=None) -> Incident:
    current = self.get_incident(incident_id)
    if current is None:
        raise KeyError(incident_id)
    updated = current.transition(new_status, at=at, assignee=assignee, root_cause=root_cause)
    self.backend.upsert_incident(self.schema, self._row(updated))    # MERGE INTO … WHEN MATCHED THEN UPDATE
    return updated
# get_open_incident / list_open_incidents / resolve_open_incidents / get_incident / list_incidents
#   ⇒ backend.get_open_incident / list_open_incidents / list_incidents / get_incident + reconstruction.
# _incident_from_row(row: Sequence | dict) -> Incident : symétrique de _run_event_from_row.
```

```python
# core/spark_backend.py — primitives incidents (mutables ⇒ MERGE, pas le helper append-only)
def upsert_incident(self, schema: str, row: dict) -> None:
    from skifer.core.sql_compiler import escape_sql_string, quote_ident
    plain = f"{schema}.incidents"
    self._spark.sql(f"CREATE SCHEMA IF NOT EXISTS {quote_ident(schema)}")
    if not self._spark.catalog.tableExists(plain):
        # première écriture : créer via l'écriture Delta (schéma inféré)
        self._spark.createDataFrame([row]).write.format("delta").saveAsTable(plain)
        return
    src = self._spark.createDataFrame([row]).createOrReplaceTempView("_skifer_incident_src")
    cols = ", ".join(f"t.{quote_ident(k)} = s.{quote_ident(k)}" for k in row)
    self._spark.sql(
        f"MERGE INTO {quote_ident(schema)}.incidents t USING _skifer_incident_src s ON t.id = s.id "
        f"WHEN MATCHED THEN UPDATE SET {cols} WHEN NOT MATCHED THEN INSERT *")
def get_open_incident(self, schema, target_fqn, check_name) -> dict | None:   # tableExists-guardé, status != 'RESOLVED', LIMIT 1
def list_open_incidents(self, schema, target_fqn) -> list[dict]:
def get_incident(self, schema, incident_id) -> dict | None:
def list_incidents(self, schema, *, status=None, target_fqn=None, limit=50) -> list[dict]:
```

```python
# observability/publication.py — hook non-bloquant dans PublicationCoordinator
import warnings
from datetime import datetime, timezone

def _record_incidents(self, run, definition, report) -> None:
    """Ouvre un incident par check critique. JAMAIS bloquant (comme uc_mirror)."""
    try:
        from skifer.observability.incidents import incidents_from_report
        now = datetime.now(timezone.utc)
        for incident in incidents_from_report(report, run_id=run.run_id, target_fqn=run.target_fqn, at=now):
            self.store.open_incident(incident)          # dédup + idempotence côté store
    except Exception as exc:                            # noqa: BLE001 — best-effort
        warnings.warn(f"[Incidents] failed to open incident(s): {type(exc).__name__}", RuntimeWarning)

def _resolve_recovered(self, run) -> None:
    """Auto-résout `recovered` les incidents ouverts du target sur PROMOTED. JAMAIS bloquant."""
    try:
        self.store.resolve_open_incidents(run.target_fqn, root_cause="recovered",
                                          resolved_at=datetime.now(timezone.utc))
    except Exception as exc:                            # noqa: BLE001
        warnings.warn(f"[Incidents] failed to resolve incidents: {type(exc).__name__}", RuntimeWarning)

# Dans _publish_run, branche quarantine (juste avant `return PublicationResult(... state=outcome.state)`) :
#     self._record_incidents(run, definition, report)
# Dans _publish_run, branche promote (juste après promote_staging) :
#     self._resolve_recovered(promoted)
# Dans resume(), après chaque promote_staging (les DEUX branches NoOp/traced) :
#     self._resolve_recovered(promoted)
```

### Comportement & règles
- **Un incident par check critique.** `incidents_from_report` ne retient que `not r.passed and
  severity=="critical"`, dédupliqué par `check_name` (`type+column`). Les warnings et SKIPPED n'ouvrent rien.
- **Dédup tant qu'ouvert.** `open_incident` cherche d'abord un incident non-RESOLVED pour
  `(target_fqn, check_name)` : trouvé ⇒ **no-op**, on renvoie l'existant. Un second run (autre `run_id`)
  sur le même check ouvert **ne crée pas** de doublon.
- **Idempotence `resume()`.** `id = run_id:check_name` + `INSERT OR IGNORE` (SQLite) / MERGE insert-if-absent
  (Delta) : rejouer le même run ne duplique pas. `resume()` promeut ⇒ passe par `_resolve_recovered`, jamais
  par `_record_incidents` ⇒ **n'ouvre aucun incident** (donc a fortiori aucun doublon).
- **Auto-résolution `recovered`.** Sur PROMOTED, tous les incidents ouverts du `target_fqn` passent
  RESOLVED avec `root_cause="recovered"`, `resolved_at=now`. `resolve` est légal depuis NEW/ACK/ASSIGNED.
- **Redaction.** L'incident ne stocke que `check_name`/`severity`/`target_fqn`/`run_id`/`column`. **Jamais**
  `message`, `actual_value`, `expected_value`. `root_cause` est saisi par un humain plus tard (ou `"recovered"`).
- **Non-bloquant.** `_record_incidents`/`_resolve_recovered` attrapent tout et `warnings.warn` (nom de classe
  seul). Le `raise DataQualityError` de `patterns.py` sur QUARANTINE reste inchangé et arrive **après** que
  l'incident a (best-effort) été ouvert.
- **Transitions.** `Incident.transition` refuse toute transition hors `_ALLOWED` (`InvalidIncidentTransition`).

### Cas de test (`tests/test_incidents.py`, `tests/test_certification_store.py`) — sans fixture `spark`
Store SQLite en `:memory:` ou fichier tmp ; `MonitorReport`/`CheckResult` fabriqués à la main (contrats
factices exposant `.column` et un `type().__name__`). **Aucune** fixture `spark` : rien ne construit de
`F.col(...)`.
- `test_incident_opened_on_quarantine_one_per_critical_check` : rapport avec 2 checks critiques FAIL +
  1 warning FAIL → `incidents_from_report` renvoie **2** incidents NEW, aucun pour le warning.
- `test_incident_dedup_while_open` : `open_incident` deux fois pour le même `(target_fqn, check_name)`
  (run_ids différents) → une seule ligne ; `list_open_incidents` en renvoie 1.
- `test_resume_opens_no_duplicate` : `open_incident(id="R:NullCheck:amount")` puis rejeu du **même** id →
  `INSERT OR IGNORE` no-op ; le compte reste 1.
- `test_recovered_on_next_promoted` : ouvrir un incident, puis `resolve_open_incidents(target, root_cause="recovered", …)`
  → status `RESOLVED`, `root_cause=="recovered"`, `resolved_at` non nul, `list_open_incidents` vide.
- `test_coordinator_quarantine_opens_then_raises` : coordinateur avec backend/monitor doubles renvoyant
  un rapport à échec critique → un incident est ouvert **avant** que `patterns` ne lève (tester au niveau
  coordinateur : `result.state == "QUARANTINED"` et store contient l'incident).
- `test_coordinator_promoted_resolves_open_incidents` : ouvrir un incident, puis publier un run PROMOTED
  sur le même target → incident RESOLVED `recovered`.
- `test_incident_hook_is_non_blocking` : store dont `open_incident` lève → `publish` retourne quand même
  QUARANTINED et un `RuntimeWarning` est émis (`pytest.warns(RuntimeWarning)`), aucune exception propagée.
- `test_invalid_transition_refused` : `Incident(status=RESOLVED).transition(NEW, …)` lève
  `InvalidIncidentTransition` ; idem `ASSIGNED→NEW`.
- `test_no_data_value_persisted` : le rapport porte un `message`/`actual_value` contenant une valeur ;
  l'`Incident` persisté n'expose aucun de ces champs (assert sur les attributs de la dataclass).
- `tests/test_certification_store.py::test_incident_roundtrip_sqlite` : open → get → update(ACK) →
  update(ASSIGN, assignee) → update(RESOLVE, root_cause) ; relire, vérifier chaque champ ; migration v2
  présente (`schema_migrations` contient 2).

### Commit
```
feat(plan31-4.1): auto-open incidents on quarantine and auto-resolve recovered on promote
```
CHANGELOG (créer la section `## [Unreleased]` en tête, avant `## [2.1.0]`) :
> - **Plan 31 (4.1)** — Incidents dans le store de certification (SQLite + Delta) : ouverture automatique
>   d'un incident par check critique sur quarantaine, dédupliqué tant qu'ouvert, idempotent sous `resume()` ;
>   auto-résolution `recovered` au prochain PROMOTED. Hook non-bloquant (un échec devient un warning, jamais
>   une publication échouée) ; aucune valeur de donnée n'est persistée.

### DoD
`pytest tests/test_incidents.py tests/test_certification_store.py -q` vert ; incident ouvert sur quarantaine
et jamais dupliqué (dédup + resume) ; `recovered` prouvé ; hook non-bloquant prouvé (`pytest.warns`) ;
transition invalide refusée ; migration SQLite v2 idempotente ; `pyproject.toml` intact.

---

## Slice 31.4.2 — Routage (owner + downstream) + canaux MS Teams / Google Chat + alerte sur breaking

### Objectif
Router les alertes vers le **propriétaire** du dataset (31.3, `owner.contact`) **et** les propriétaires
de ses **consommateurs aval** (31.2, `downstream()`, profondeur bornée) ; déclencher une alerte sur
`ContractDiff.breaking` (31.3) ; ajouter les canaux **MS Teams** et **Google Chat** à `AlertDispatcher` ;
un échec de canal reste un **warning** ; **aucune valeur de donnée** dans le message.

### Fichiers
- `src/skifer/observability/alerts.py` — **modification** : canaux `msteams` / `google_chat`,
  builder de payload **redacté** pour incidents, `dispatch_incident`.
- `src/skifer/observability/incidents.py` — **modification** : `AlertRouter`, `Recipient`,
  `resolve_recipients()`, `alert_incident()`, `alert_breaking_change()`.

### Signatures Python

```python
# observability/alerts.py — nouveaux canaux + chemin redacté
class AlertDispatcher:
    ...
    def dispatch(self, report, config) -> list[str]:
        ...
        if config.get("msteams_webhook"):
            self._send_msteams(config["msteams_webhook"], report, failures); notified.append("msteams")
        if config.get("google_chat_webhook"):
            self._send_google_chat(config["google_chat_webhook"], report, failures); notified.append("google_chat")
        return notified

    def _send_msteams(self, webhook_url: str, report, failures) -> None:
        # MessageCard : titre + sections ; RÉUTILISE _send_webhook (POST JSON, non-bloquant).
        # N'émet QUE severity + check name (redacté) — jamais r.message/r.actual_value.
        card = {"@type": "MessageCard", "@context": "http://schema.org/extensions",
                "summary": f"Skifer — {report.table}",
                "themeColor": "D93F0B",
                "title": f"Skifer alert — {report.table}",
                "sections": [{"text": t} for t in _redacted_lines(report, failures)]}
        self._send_webhook(webhook_url, card)

    def _send_google_chat(self, webhook_url: str, report, failures) -> None:
        self._send_webhook(webhook_url, {"text": "\n".join(_redacted_lines(report, failures))})

    def dispatch_incident(self, *, target_fqn: str, incidents, recipients, config: dict) -> list[str]:
        """Chemin d'alerte REDACTÉ pour incidents : payload = target_fqn + check_name + severity + id + owners.
        N'appelle JAMAIS _build_payload (qui fuiterait actual/expected). Un recipient = une config de canal.
        Chaque envoi est non-bloquant (warnings.warn en cas d'échec)."""


def _redacted_lines(report, failures) -> list[str]:
    # ["Table: <fqn>", "Critical: <CheckName>", ...] — nom de check + severity uniquement, aucune valeur.
    return [f"Table: {report.table}"] + [f"[{r.severity.upper()}] {type(r.contract).__name__}" for r in failures]
```

```python
# observability/incidents.py — routage
from dataclasses import dataclass

@dataclass(frozen=True)
class Recipient:
    contact: str                    # e-mail routable (owner.contact)
    team: str | None = None
    via: str = "email"              # "email" | "msteams" | "google_chat" | "slack" | "webhook"
    source_fqn: str | None = None   # dataset d'où vient ce destinataire (owner direct vs aval)


class AlertRouter:
    """Calcule les destinataires (owner + owners aval bornés) et route via AlertDispatcher.

    Dépendances DURES (injectées, duck-typées pour le test) :
      - governance: expose registry_downstream(ctx, fqn, column) -> list[dict] (31.2.3)
                    et un accès à l'owner structuré {team, steward, domain, contact} (31.3.2).
      - dispatcher: AlertDispatcher.
    """
    def __init__(self, governance, dispatcher: "AlertDispatcher", *, max_depth: int = 3):
        self._gov = governance
        self._dispatcher = dispatcher
        self._max_depth = max_depth

    def resolve_recipients(self, ctx, target_fqn: str) -> list[Recipient]:
        # 1) owner direct du target (31.3.2)  → Recipient(source_fqn=target_fqn)
        # 2) datasets aval via governance (31.2.3), profondeur bornée à max_depth, cycles déjà refusés en amont
        # 3) owner de chaque dataset aval → Recipient(source_fqn=<aval>)
        # Dédup par (contact, via) ; ordre déterministe (owner d'abord, puis aval trié).
        ...

    def alert_incident(self, ctx, *, target_fqn: str, incidents, config: dict) -> list[str]:
        recipients = self.resolve_recipients(ctx, target_fqn)
        return self._dispatcher.dispatch_incident(target_fqn=target_fqn, incidents=incidents,
                                                  recipients=recipients, config=config)

    def alert_breaking_change(self, ctx, *, target_fqn: str, diff, config: dict) -> list[str]:
        if not getattr(diff, "breaking", False):     # 31.3.5 : n'alerte QUE sur breaking
            return []
        recipients = self.resolve_recipients(ctx, target_fqn)   # owners aval = les impactés d'un breaking change
        # payload redacté : target_fqn + "breaking contract change" + versions (from/to) — jamais de valeur de donnée
        ...
```

### Comportement & règles
- **Destinataires = owner + owners aval bornés.** Owner direct du `target_fqn` (31.3.2, `owner.contact`) +
  owner de chaque dataset **aval** obtenu via `governance.registry_downstream` (31.2.3). Profondeur bornée
  par `max_depth` (défaut 3). Dédup par `(contact, via)`, ordre déterministe.
- **Propagation N niveaux.** La traversée aval est bornée et **sans cycle** (garantie 31.2.3 :
  `downstream_closure` borné, cycles → `ValueError` en amont). `AlertRouter` ne réimplémente pas la
  traversée : il consomme les edges sérialisés du `GovernanceService`.
- **Alerte breaking uniquement.** `alert_breaking_change` **ignore** un `ContractDiff` non-breaking
  (`diff.breaking is False` ⇒ `[]`). Sur breaking, les impactés = owners aval.
- **Canaux ajoutés par présence de clé de config** (cohérent avec l'existant) : `msteams_webhook`,
  `google_chat_webhook`. Les deux sont POST-JSON via `_send_webhook` ⇒ **déjà non-bloquants**
  (échec → `warnings.warn(RuntimeWarning)`).
- **Isolation des canaux.** Un canal en échec (`_send_webhook` attrape) **n'empêche pas** les autres :
  `dispatch_incident` appelle chaque canal dans son propre try/except ; `notified` ne liste que les canaux
  tentés/réussis, l'échec restant un warning.
- **Aucune valeur de donnée.** Les chemins d'alerte-incident/breaking utilisent `_redacted_lines` /
  payload redacté et **n'appellent jamais** `_build_payload` (qui inclut `actual`/`expected`). Message =
  `target_fqn` + nom de check + severity (+ versions pour un breaking) — schéma, jamais donnée.

### Cas de test (`tests/test_alerts.py`, `tests/test_incidents.py`) — sans fixture `spark`
- `test_msteams_channel_notified` : `dispatch(report, {"msteams_webhook": "http://x"})` avec un
  `_send_webhook` mocké → `"msteams"` dans `notified`, payload = MessageCard.
- `test_google_chat_channel_notified` : idem `google_chat_webhook` → `{"text": …}`.
- `test_failed_channel_isolated` : deux canaux, le premier `_send_webhook` lève (URLError simulée) →
  l'autre est quand même tenté, un `RuntimeWarning` est émis, aucune exception propagée
  (`pytest.warns(RuntimeWarning)`).
- `test_no_data_value_in_incident_message` : rapport dont un `CheckResult.message` contient une valeur
  (`"amount = 999.99"`) et `actual_value=999.99` ; capturer le payload envoyé (mock `_send_webhook`) et
  asserter que `"999.99"` n'apparaît **pas** ; seuls `type(contract).__name__` et `severity` présents.
- `test_recipients_owner_plus_downstream` : `governance` factice renvoyant un owner pour le target et deux
  datasets aval avec owners distincts → `resolve_recipients` renvoie les 3 contacts, dédupliqués.
- `test_n_level_propagation_bounded` : chaîne aval de profondeur 5, `max_depth=2` → seuls les owners
  jusqu'au niveau 2 sont destinataires (borne respectée).
- `test_alert_breaking_only_on_breaking` : `diff.breaking=False` → `[]` ; `diff.breaking=True` → alerte
  routée vers les owners aval.

### Commit
```
feat(plan31-4.2): route incident/breaking alerts to owner+downstream, add MS Teams and Google Chat channels
```
CHANGELOG (`## [Unreleased]`) :
> - **Plan 31 (4.2)** — Routage d'alertes : destinataires = propriétaire du dataset (owner structuré 31.3)
>   + propriétaires des consommateurs aval via `downstream()` (31.2), profondeur bornée. Canaux **MS Teams**
>   et **Google Chat** ajoutés à `AlertDispatcher` ; un échec de canal reste un warning (jamais bloquant).
>   Alerte sur `ContractDiff.breaking` uniquement. Aucune valeur de donnée dans le message (chemin redacté).

### DoD
`pytest tests/test_alerts.py tests/test_incidents.py -q` vert ; propagation N niveaux bornée prouvée ;
canal en échec isolé (`pytest.warns`) ; aucune valeur de donnée dans le payload prouvée ; alerte breaking
conditionnée à `diff.breaking`. **Dépendances 31.2.3 + 31.3.2 + 31.3.5 requises** (stubbées en test).

---

## Slice 31.4.3 — CLI `skifer incidents list|ack|assign|resolve` + `QualityService` sous `incidents:write`

### Objectif
Exposer les transitions d'incident : `QualityService` (scope `incidents:write` pour ack/assign/resolve,
`contracts:read` pour list) et une sous-commande CLI `skifer incidents list|ack|assign|resolve`. Les
transitions invalides sont refusées avec un exit code stable.

### Fichiers
- `src/skifer/services/quality.py` — **modification** : `incident_store` optionnel + `IncidentView` +
  méthodes gated.
- `src/skifer/cli.py` — **modification** : sous-parseurs `incidents`, dispatch, `run_incidents_command`,
  constantes d'exit code.

### Signatures Python

```python
# services/quality.py — ajouts
from dataclasses import dataclass
from datetime import datetime, timezone
from skifer.services.context import (
    RequestContext, require_scope, SCOPE_CONTRACTS_READ, SCOPE_INCIDENTS_WRITE,
    ResourceNotFound, ResourceUnavailable, InvalidRequest,
)
from skifer.observability.incidents import IncidentStatus, InvalidIncidentTransition

@dataclass(frozen=True)
class IncidentView:
    id: str; target_fqn: str; check_name: str; severity: str; status: str
    assignee: str | None; root_cause: str | None; opened_at: str; resolved_at: str | None
    def to_dict(self) -> dict: ...      # JSON-native, allowlist champ par champ (jamais asdict d'un objet store)

class QualityService:
    def __init__(self, history_store, *, contract_extractor=None, incident_store=None):
        ...
        self._incidents = incident_store

    def _require_incidents(self):
        if self._incidents is None:
            raise ResourceUnavailable("Incident store not configured.")
        return self._incidents

    def list_incidents(self, ctx, *, status: str | None = None, target_fqn: str | None = None,
                       limit: int = 50) -> tuple[IncidentView, ...]:
        require_scope(ctx, SCOPE_CONTRACTS_READ)                         # lecture
        return tuple(_to_view(i) for i in self._require_incidents().list_incidents(
            status=status, target_fqn=target_fqn, limit=limit))

    def acknowledge_incident(self, ctx, incident_id: str) -> IncidentView:
        require_scope(ctx, SCOPE_INCIDENTS_WRITE)
        return self._transition(incident_id, IncidentStatus.ACKNOWLEDGED)

    def assign_incident(self, ctx, incident_id: str, assignee: str) -> IncidentView:
        require_scope(ctx, SCOPE_INCIDENTS_WRITE)
        if not assignee:
            raise InvalidRequest("assignee is required.")
        return self._transition(incident_id, IncidentStatus.ASSIGNED, assignee=assignee)

    def resolve_incident(self, ctx, incident_id: str, root_cause: str) -> IncidentView:
        require_scope(ctx, SCOPE_INCIDENTS_WRITE)
        if not root_cause:
            raise InvalidRequest("root_cause is required.")
        return self._transition(incident_id, IncidentStatus.RESOLVED, root_cause=root_cause)

    def _transition(self, incident_id, new_status, *, assignee=None, root_cause=None) -> IncidentView:
        store = self._require_incidents()
        try:
            updated = store.update_incident(incident_id, new_status, at=datetime.now(timezone.utc),
                                            assignee=assignee, root_cause=root_cause)
        except KeyError:
            raise ResourceNotFound(f"Incident '{incident_id}' not found.")
        # InvalidIncidentTransition remonte telle quelle → mappée en exit code 3 par la CLI.
        return _to_view(updated)
```

```python
# cli.py — constantes + parseurs + dispatch + handler
INCIDENTS_EXIT_OK = 0
INCIDENTS_EXIT_ERROR = 1
INCIDENTS_EXIT_USAGE = 2
INCIDENTS_EXIT_INVALID_TRANSITION = 3
INCIDENTS_EXIT_NOT_FOUND = 4

# --- sous-parseurs (dans main()) ---
incidents_parser = subparsers.add_parser("incidents", help="Manage data-quality incidents.")
incidents_sub = incidents_parser.add_subparsers(dest="incidents_command")
p_list = incidents_sub.add_parser("list", help="List incidents.")
p_list.add_argument("--status", choices=["NEW", "ACKNOWLEDGED", "ASSIGNED", "RESOLVED"])
p_list.add_argument("--target", dest="target_fqn", metavar="FQN")
p_list.add_argument("--limit", type=int, default=50)
p_list.add_argument("--store", default=".skifer_certification.db", metavar="PATH")
p_ack = incidents_sub.add_parser("ack", help="Acknowledge an incident.")
p_ack.add_argument("incident_id", metavar="INCIDENT_ID"); p_ack.add_argument("--store", default=".skifer_certification.db")
p_assign = incidents_sub.add_parser("assign", help="Assign an incident.")
p_assign.add_argument("incident_id", metavar="INCIDENT_ID"); p_assign.add_argument("--assignee", required=True)
p_assign.add_argument("--store", default=".skifer_certification.db")
p_resolve = incidents_sub.add_parser("resolve", help="Resolve an incident.")
p_resolve.add_argument("incident_id", metavar="INCIDENT_ID"); p_resolve.add_argument("--root-cause", dest="root_cause", required=True)
p_resolve.add_argument("--store", default=".skifer_certification.db")

# --- dispatch (cascade args.command) ---
elif args.command == "incidents":
    _run_incidents(args)

def _run_incidents(args) -> None:
    sys.exit(run_incidents_command(args))

def run_incidents_command(args, *, service=None) -> int:
    """Transitions d'incident via QualityService, exit codes stables. Service injectable pour le test."""
    command = getattr(args, "incidents_command", None)
    if command is None:
        print("Incidents command missing. Use 'skifer incidents --help'.", file=sys.stderr)
        return INCIDENTS_EXIT_USAGE
    if service is None:
        from skifer.observability.certification_store import SqliteCertificationStore
        from skifer.services.quality import QualityService
        from skifer.services.context import RequestContext, LocalIdentity  # LocalIdentity a incidents:write, pas override
        store = SqliteCertificationStore(args.store)
        service = QualityService(history_store=None, incident_store=store)
    ctx = _local_incidents_context()   # RequestContext depuis LocalIdentity (tous scopes nommés sauf override)
    try:
        if command == "list":
            views = service.list_incidents(ctx, status=getattr(args, "status", None),
                                           target_fqn=getattr(args, "target_fqn", None),
                                           limit=getattr(args, "limit", 50))
            print(json.dumps([v.to_dict() for v in views], ensure_ascii=False, indent=2, sort_keys=True))
            return INCIDENTS_EXIT_OK
        if command == "ack":
            v = service.acknowledge_incident(ctx, args.incident_id)
        elif command == "assign":
            v = service.assign_incident(ctx, args.incident_id, args.assignee)
        elif command == "resolve":
            v = service.resolve_incident(ctx, args.incident_id, args.root_cause)
        else:
            print("Unknown incidents command.", file=sys.stderr); return INCIDENTS_EXIT_USAGE
        print(json.dumps(v.to_dict(), ensure_ascii=False, indent=2, sort_keys=True))
        return INCIDENTS_EXIT_OK
    except InvalidIncidentTransition as exc:
        print(f"[incidents] {exc}", file=sys.stderr); return INCIDENTS_EXIT_INVALID_TRANSITION
    except ResourceNotFound as exc:
        print(f"[incidents] {exc}", file=sys.stderr); return INCIDENTS_EXIT_NOT_FOUND
    except Exception as exc:                                    # noqa: BLE001
        print(f"[incidents] Command failed ({type(exc).__name__}).", file=sys.stderr)
        return INCIDENTS_EXIT_ERROR
```

### Comportement & règles
- **Scopes.** `list_incidents` → `contracts:read` ; `ack`/`assign`/`resolve` → `incidents:write`. La CLI
  locale utilise `LocalIdentity` qui porte tous les scopes nommés **sauf** `certification_override`
  (31.1.1) — donc `incidents:write` est disponible.
- **Transition invalide refusée.** `QualityService._transition` propage `InvalidIncidentTransition`
  (levée par `Incident.transition` via `store.update_incident`) → CLI exit code **3**. Un incident inconnu
  (`KeyError` → `ResourceNotFound`) → exit code **4**. Argument manquant (`assignee`/`root_cause`) →
  `InvalidRequest`.
- **`assign` sans transition d'état legale** (ex. incident déjà RESOLVED) → refus (`InvalidIncidentTransition`,
  RESOLVED est terminal) → exit 3.
- **JSON déterministe** (`sort_keys=True`), vues allowlistées (`IncidentView.to_dict`), aucun objet Spark
  ni DataFrame ne franchit la frontière service.

### Cas de test (`tests/test_incidents_cli.py`, `tests/test_quality_service.py`) — sans fixture `spark`
- `test_cli_list_incidents` : store SQLite tmp avec 2 incidents → `run_incidents_command(list args)` renvoie
  0, JSON contient les 2 ids.
- `test_cli_ack_then_assign_then_resolve` : parcours NEW→ACK→ASSIGN→RESOLVE, chaque commande renvoie 0 et
  l'état persisté progresse.
- `test_cli_invalid_transition_refused` : incident RESOLVED → `ack` → exit code **3**, message sur stderr,
  état inchangé.
- `test_cli_unknown_incident_not_found` : id inexistant → `resolve` → exit code **4**.
- `test_cli_assign_requires_assignee` / `test_cli_resolve_requires_root_cause` : argparse `required=True`
  (SystemExit) ou `InvalidRequest` selon le chemin.
- `test_quality_service_ack_requires_scope` : `RequestContext` sans `incidents:write` → `acknowledge_incident`
  lève `ScopeDenied`.
- `test_quality_service_list_requires_read_scope` : sans `contracts:read` → `ScopeDenied`.
- `test_quality_service_transition_refused` : store contenant un incident RESOLVED → `acknowledge_incident`
  lève `InvalidIncidentTransition`.
- `test_quality_service_incident_store_absent` : `QualityService(history_store=None)` (sans incident_store)
  → `acknowledge_incident` lève `ResourceUnavailable`.

### Commit
```
feat(plan31-4.3): expose incident transitions via QualityService (incidents:write) and CLI skifer incidents
```
CHANGELOG (`## [Unreleased]`) :
> - **Plan 31 (4.3)** — Transitions d'incident exposées : `QualityService` (ack/assign/resolve sous
>   `incidents:write`, list sous `contracts:read`) et CLI `skifer incidents list|ack|assign|resolve`. Les
>   transitions invalides sont refusées avec un exit code stable (3 = transition invalide, 4 = introuvable).

### DoD
`pytest tests/test_incidents_cli.py tests/test_quality_service.py -q` vert ; transitions invalides refusées
(exit 3) ; scopes appliqués (`ScopeDenied` sans le scope) ; `--help` de `skifer incidents` propre.
**Dépendance 31.1.4 (`QualityService`) requise.**

---

## Ordre & dépendances internes
1. **4.1 en premier** — introduit `Incident`, la persistance store et les hooks. Ne dépend d'aucune autre
   feature de Plan 31 (n'utilise que le store de certification et le coordinateur, déjà présents). C'est le
   socle des deux slices suivantes.
2. **4.2 après 4.1** et **après** :
   - **31.2.3** (`GovernanceService.registry_downstream` / `LineageGraph.downstream_closure` borné, cycle-safe) — routage aval ;
   - **31.3.2** (`owner` structuré `{team, steward, domain, contact}`) — destinataires ;
   - **31.3.5** (`ContractDiff.breaking`) — déclencheur d'alerte breaking.
   Ces trois dépendances sont **dures** ; en leur absence, 4.2 est développable contre des stubs duck-typés
   mais **non intégrable**.
3. **4.3 après 4.1** et **après 31.1.4** (`QualityService` + `services/context.py` avec
   `SCOPE_INCIDENTS_WRITE`, `LocalIdentity`, hiérarchie d'erreurs). Peut se faire en parallèle de 4.2.

---

## Risques & pièges
- **Placement du hook (patterns vs coordinateur).** La décision QUARANTINE/PROMOTED est *prise* dans
  `PublicationCoordinator._publish_run` mais *affleure* dans `core/patterns.py` (où QUARANTINE lève
  `DataQualityError`). Mettre le hook **dans le coordinateur** couvre aussi `resume()` (que `patterns.py`
  ne voit pas) et garantit la résolution `recovered` sur reprise. **Attention** : sur QUARANTINE,
  `_record_incidents` doit s'exécuter **avant** le `raise` (donc dans `_publish_run`, pas après le retour
  dans `patterns.py`), sinon un plantage de l'ouverture masquerait l'incident. `resume()` a **deux**
  branches (NoOp/traced) : brancher `_resolve_recovered` dans les deux.
- **Enregistrement mutable dans une famille append-only.** Les incidents sont le **seul** enregistrement
  mutable du store de certification. SQLite fait de l'`UPDATE` (le reste utilise `INSERT OR IGNORE`) ;
  Delta doit faire un **`MERGE INTO … ON id`** (le helper `_append_certification` est append-only et ne
  convient pas). Ne pas réutiliser `append_certification_*` pour les incidents.
- **Clé de dédup.** Deux niveaux : (a) `id = run_id:check_name` → idempotence de `resume()` via
  `INSERT OR IGNORE` / MERGE insert-if-absent ; (b) requête `get_open_incident(target_fqn, check_name)`
  → dédup **inter-runs** tant qu'ouvert. Les **deux** sont nécessaires : (a) seul ne dédup pas deux runs
  différents ; (b) seul ne protège pas un rejeu du même run. `check_name` doit inclure la **colonne**
  (`type+column`) sinon deux `NullCheck` sur colonnes distinctes fusionnent à tort en un incident.
- **Profondeur de propagation bornée.** `AlertRouter.max_depth` (défaut 3) et la garantie 31.2.3
  (`downstream_closure` borné, cycles → `ValueError`) évitent l'explosion / la boucle infinie. `AlertRouter`
  ne réimplémente **pas** la traversée : il consomme les edges du `GovernanceService`. Dédupliquer les
  destinataires par `(contact, via)` pour ne pas notifier deux fois un owner présent sur plusieurs chemins.
- **Redaction des messages (fuite de données).** `AlertDispatcher._build_payload` **inclut**
  `actual`/`expected` — c'est une valeur de donnée. Les chemins incident/breaking ne doivent **jamais**
  l'appeler ; utiliser `_redacted_lines` / payload redacté (target_fqn + nom de check + severity, + versions
  pour un breaking). L'`Incident` persisté ne porte lui-même aucun `message`/`actual_value`. Tester
  explicitement l'absence d'une valeur injectée dans le payload envoyé.
- **Non-blocking discipline.** Tout hook (`_record_incidents`, `_resolve_recovered`) et tout envoi de canal
  attrape `Exception` et `warnings.warn(..., RuntimeWarning)` avec le **nom de classe seul** (jamais le
  message backend, qui cite souvent la valeur fautive). Un échec ne modifie ni la valeur de retour, ni la
  donnée persistée, ni l'exception métier (`DataQualityError` sur QUARANTINE reste levée).
- **CHANGELOG `[Unreleased]`.** Absent au HEAD (`release: 2.1.0`) : la slice 4.1 crée la section en tête,
  avant `## [2.1.0]`. Ne jamais toucher la version de `pyproject.toml`.
