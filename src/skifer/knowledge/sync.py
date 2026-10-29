"""Batch synchronization of the knowledge graph from Skifer's sources (Plan 44, S02).

Sources, in priority order: pipelines (the Plan 31 metadata registry), semantic
models (``semantic_catalog.yaml`` and the models it lists, read here in batch —
``SemanticEngine``'s lazy start-up is untouched), registered rules, then Unity
Catalog. A node already provided by a higher source is never overwritten by a lower
one. Change is detected by fingerprint snapshot (D8): nothing is written for an
object that did not change, and an object gone from a source it was read from is
soft-deleted. A source that could not be read removes nothing.
"""

from __future__ import annotations

import inspect
import os
import re
from typing import Any, Iterable

import yaml

from skifer.core.registry import RuleRegistry

from .models import Edge, Node, SyncReport
from .store import KnowledgeStore


DOMAIN_RE = re.compile(r"^[^.\s`'\"]+\.[^.\s`'\"]+$")
TABLE_LINEAGE = "system.access.table_lineage"
LINEAGE_LOOKBACK_DAYS = 90
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_domain(value: Any) -> str:
    if not isinstance(value, str) or not DOMAIN_RE.match(value):
        raise ValueError(f"--schema expects CATALOG.SCHEMA; got {value!r}.")
    return value


def _domain_of(table_fqn: str) -> str | None:
    parts = table_fqn.split(".")
    return ".".join(parts[:-1]) if len(parts) > 1 else None


def _label(fqn: str) -> str:
    return fqn.rsplit(".", 1)[-1]


def _strings(values: Any) -> tuple[str, ...]:
    if not isinstance(values, (list, tuple)):
        return ()
    return tuple(str(value).strip() for value in values if str(value).strip())


class _Graph:
    """Nodes and edges gathered in priority order; the first source to name an id wins."""

    def __init__(self) -> None:
        self.nodes: dict[str, Node] = {}
        self.edges: dict[str, Edge] = {}

    def node(self, **values: Any) -> str:
        node = Node.build(**values)
        self.nodes.setdefault(node.node_id, node)
        return node.node_id

    def edge(self, **values: Any) -> None:
        edge = Edge.build(**values)
        self.edges.setdefault(edge.edge_id, edge)

    def table(self, fqn: str, *, source: str, text: str = "", status: str = "reference") -> str:
        domain = _domain_of(fqn)
        node_id = self.node(kind="table", fqn=fqn, label=_label(fqn), text=text,
                            status=status, domain=domain, source=source)
        if domain is not None:
            domain_id = self.node(kind="domain", fqn=domain, label=_label(domain),
                                  status="reference", domain=None, source=source)
            self.edge(kind="belongs_to", from_id=node_id, to_id=domain_id, source=source)
        return node_id

    def column(self, table_fqn: str, name: str, *, source: str, text: str = "",
               status: str = "reference") -> str:
        table_id = self.table(table_fqn, source=source)
        node_id = self.node(kind="column", fqn=f"{table_fqn}.{name}", label=name, text=text,
                            status=status, domain=_domain_of(table_fqn), source=source)
        self.edge(kind="has_column", from_id=table_id, to_id=node_id, source=source)
        return node_id


class KnowledgeSynchronizer:
    """Rebuild the graph from its sources and write only what changed."""

    def __init__(
        self,
        store: KnowledgeStore,
        *,
        engine: Any = None,
        metadata_store: Any = None,
        catalog_inspector: Any = None,
        rule_registry: Any = RuleRegistry,
        domains: Iterable[str] | None = None,
        models_dir: str | None = None,
    ):
        self._store = store
        self._engine = engine
        self._metadata_store = metadata_store
        self._rule_registry = rule_registry
        self._models_dir = models_dir or os.path.abspath("semantic_models")
        if domains is None and engine is not None and getattr(engine, "context", None) is not None:
            domains = engine.context.env_config().get("knowledge_domains")
        if domains is not None and (isinstance(domains, str) or not isinstance(domains, Iterable)):
            raise ValueError("knowledge_domains must be a list of CATALOG.SCHEMA strings.")
        self._domains = None if domains is None else tuple(validate_domain(d) for d in domains)
        self._inspector = catalog_inspector
        self._inspector_injected = catalog_inspector is not None
        if catalog_inspector is None and engine is not None and not engine.is_local and engine.db:
            from skifer.core.catalog_inspector import CatalogInspector

            self._inspector = CatalogInspector(engine.backend, engine.db)

    # --- public -----------------------------------------------------------------

    def sync(self, *, dry_run: bool = False) -> SyncReport:
        graph = _Graph()
        consulted: set[str] = set()
        unavailable: list[str] = []
        protected_domains: set[str] = set()
        lineage_read = True

        if self._metadata_store is not None:
            self._sync_pipelines(graph)
            consulted.add("pipeline")
        if self._sync_semantic(graph):
            consulted.add("semantic")
        if self._sync_rules(graph):
            consulted.add("rule")
        if self._inspector is not None:
            consulted.add("unity_catalog")
            protected_domains, lineage_read = self._sync_unity_catalog(graph, unavailable)

        existing = {node.node_id: node for node in self._store.list_nodes(include_removed=True)}
        live = {node.node_id for node in self._store.list_nodes()}
        added = updated = unchanged = 0
        for node_id, node in graph.nodes.items():
            current = existing.get(node_id)
            if current is None:
                added += 1
            elif current.fingerprint != node.fingerprint or node_id not in live:
                updated += 1
            else:
                unchanged += 1

        protected_nodes = {
            node_id for node_id in live
            if existing[node_id].source == "unity_catalog"
            and (existing[node_id].domain in protected_domains
                 or existing[node_id].fqn in protected_domains)
        }
        gone = sorted(
            node_id for node_id in live
            if node_id not in graph.nodes
            and existing[node_id].source in consulted
            and node_id not in protected_nodes
        )
        gone_edges = sorted(
            edge.edge_id for edge in self._store.list_edges()
            if edge.edge_id not in graph.edges
            and edge.source in consulted
            and not {edge.from_id, edge.to_id} & protected_nodes
            and not (edge.source == "unity_catalog" and edge.kind == "derives" and not lineage_read)
        )

        if not dry_run:
            self._store.upsert_nodes(graph.nodes.values())
            self._store.upsert_edges(graph.edges.values())
            self._store.mark_removed(gone)
            self._store.mark_edges_removed(gone_edges)
        return SyncReport(added=added, updated=updated, unchanged=unchanged,
                          removed=len(gone), unavailable=tuple(sorted(set(unavailable))))

    # --- pipelines ----------------------------------------------------------------

    def _sync_pipelines(self, graph: _Graph) -> None:
        records = sorted(self._metadata_store.list_all(), key=lambda record: record.target_fqn)
        for record in records:
            text = " ".join(part for part in (record.data_product_id, record.owner) if part)
            graph.table(record.target_fqn, source="pipeline", text=text)
            for column in record.columns:
                graph.column(record.target_fqn, column.name, source="pipeline",
                             text=column.description or "")
        for record in records:
            for edge in (record.lineage or {}).get("edges", []):
                self._pipeline_edge(graph, edge)

    @staticmethod
    def _pipeline_edge(graph: _Graph, edge: dict) -> None:
        source_table, target_table = edge.get("source_table"), edge.get("target_table")
        source_column, target_column = edge.get("source_column"), edge.get("target_column")
        if not all(isinstance(v, str) and v for v in
                   (source_table, target_table, source_column, target_column)):
            return
        from_id = graph.column(source_table, source_column, source="pipeline")
        to_id = graph.column(target_table, target_column, source="pipeline")
        if edge.get("edge_type") == "join":
            keys = (source_column, target_column)
            graph.edge(kind="joins", from_id=from_id, to_id=to_id, keys=keys, source="pipeline")
            graph.edge(kind="shortcut", from_id=f"table:{source_table}",
                       to_id=f"table:{target_table}", keys=keys, weight=1.0, source="pipeline")
        else:
            graph.edge(kind="derives", from_id=from_id, to_id=to_id, source="pipeline")

    # --- semantic models ---------------------------------------------------------------

    def _sync_semantic(self, graph: _Graph) -> bool:
        catalog_path = os.path.join(self._models_dir, "semantic_catalog.yaml")
        if not os.path.exists(catalog_path):
            return False
        with open(catalog_path, encoding="utf-8") as handle:
            entries = (yaml.safe_load(handle) or {}).get("models") or []
        models: dict[str, dict] = {}
        for entry in sorted(entries, key=lambda item: str(item.get("key"))):
            path = os.path.join(self._models_dir, entry["file"])
            with open(path, encoding="utf-8") as handle:
                for model in (yaml.safe_load(handle) or {}).get("models") or []:
                    if model.get("key", entry["key"]) == entry["key"]:
                        models[entry["key"]] = {**model, "_entry": entry}
                        break
        for key in sorted(models):
            self._semantic_model(graph, key, models[key])
        for key in sorted(models):
            self._semantic_relationships(graph, key, models)
        return True

    def _semantic_model(self, graph: _Graph, key: str, model: dict) -> None:
        entry = model["_entry"]
        table = model.get("table") or entry.get("table")
        description = model.get("description") or entry.get("description") or ""
        table_id = None
        if table:
            text = " ".join([description, *_strings(entry.get("tags"))]).strip()
            table_id = graph.table(table, source="semantic", text=text)
        domain = _domain_of(table) if table else None
        for kind, members in (("dimension", model.get("dimensions")),
                              ("metric", model.get("metrics"))):
            for member in members or []:
                name = member.get("name")
                if not name:
                    continue
                node_id = graph.node(
                    kind=kind, fqn=f"{key}.{name}", label=name,
                    text=member.get("description") or "",
                    synonyms=_strings(member.get("synonyms")),
                    status="certified", domain=domain, source="semantic",
                )
                if table_id is None:
                    continue
                graph.edge(kind="belongs_to", from_id=node_id, to_id=table_id, source="semantic")
                sql = str(member.get("sql") or "")
                column_id = f"column:{table}.{sql}"
                if _IDENT_RE.match(sql) and column_id in graph.nodes:
                    graph.edge(kind="derives", from_id=column_id, to_id=node_id, source="semantic")

    @staticmethod
    def _semantic_relationships(graph: _Graph, key: str, models: dict[str, dict]) -> None:
        model = models[key]
        table = model.get("table") or model["_entry"].get("table")
        keys_by_entity = {e.get("name"): e.get("key") for e in model.get("entities") or []}
        for relationship in model.get("relationships") or []:
            target = models.get(relationship.get("to_model"))
            if target is None or not table:
                continue
            target_table = target.get("table") or target["_entry"].get("table")
            target_keys = {e.get("name"): e.get("key") for e in target.get("entities") or []}
            from_key = keys_by_entity.get(relationship.get("from_entity"))
            to_key = target_keys.get(relationship.get("to_entity"))
            if not (target_table and from_key and to_key):
                continue
            graph.edge(kind="shortcut", from_id=f"table:{table}", to_id=f"table:{target_table}",
                       keys=(from_key, to_key), weight=1.0, source="semantic")

    # --- rules --------------------------------------------------------------------------

    def _sync_rules(self, graph: _Graph) -> bool:
        names = sorted(self._rule_registry.list_rules())
        # An empty registry means no rule module was imported in this process,
        # not that every rule was deleted: it removes nothing.
        if not names:
            return False
        for name in names:
            spec = self._rule_registry.get_rule(name)
            doc = inspect.getdoc(spec.func) or ""
            graph.node(kind="rule", fqn=name, label=name, text=doc, status="reference",
                       domain=None, source="rule")
        return True

    # --- Unity Catalog -------------------------------------------------------------------

    def _inspector_for(self, catalog: str) -> Any:
        if self._inspector_injected:
            return self._inspector
        from skifer.core.catalog_inspector import CatalogInspector

        return CatalogInspector(self._engine.backend, catalog)

    def _uc_domains(self, unavailable: list[str]) -> list[str]:
        if self._domains is not None:
            return list(self._domains)
        catalog = getattr(self._inspector, "_catalog", None) or getattr(self._engine, "db", None)
        try:
            schemas = self._inspector.list_schemas()
        except Exception:
            unavailable.append(f"{catalog}" if catalog else "unity_catalog")
            return []
        return sorted(f"{catalog}.{schema}" for schema in schemas if catalog and schema)

    def _sync_unity_catalog(self, graph: _Graph, unavailable: list[str]) -> tuple[set[str], bool]:
        protected: set[str] = set()
        domains = self._uc_domains(unavailable)
        for domain in domains:
            catalog, schema = domain.split(".", 1)
            inspector = self._inspector_for(catalog)
            try:
                tables = sorted(inspector.list_tables(schema))
            except Exception:
                tables = []
            # SparkBackend.list_tables answers [] on a refused permission: an
            # empty listing cannot be told apart from it, so it removes nothing.
            if not tables:
                unavailable.append(domain)
                protected.add(domain)
                continue
            for table in tables:
                fqn = f"{domain}.{table}"
                try:
                    columns = inspector.describe_table(fqn) or {
                        name: "" for name in inspector.list_columns(fqn)
                    }
                except Exception:
                    unavailable.append(fqn)
                    protected.add(domain)
                    columns = {}
                graph.table(fqn, source="unity_catalog")
                for name in sorted(columns):
                    graph.column(fqn, name, source="unity_catalog", text=str(columns[name] or ""))
        return protected, self._sync_table_lineage(graph, domains, unavailable)

    def _read_table_lineage(self, domains: list[str]) -> list[tuple[str, str]]:
        reader = getattr(self._inspector, "table_lineage", None)
        if callable(reader):
            return [tuple(pair) for pair in reader(domains)]
        if self._engine is None:
            raise LookupError(TABLE_LINEAGE)
        from skifer.core.sql_compiler import escape_sql_string

        scope = " OR ".join(
            f"(target_table_catalog = '{escape_sql_string(d.split('.', 1)[0])}' "
            f"AND target_table_schema = '{escape_sql_string(d.split('.', 1)[1])}')"
            for d in domains
        )
        rows = self._engine.backend.execute_sql(
            "SELECT DISTINCT source_table_full_name, target_table_full_name "
            f"FROM {TABLE_LINEAGE} WHERE source_table_full_name IS NOT NULL "
            "AND target_table_full_name IS NOT NULL "
            f"AND event_time >= current_date() - INTERVAL {LINEAGE_LOOKBACK_DAYS} DAYS "
            f"AND ({scope})"
        ).collect()
        return [(row[0], row[1]) for row in rows]

    def _sync_table_lineage(self, graph: _Graph, domains: list[str],
                            unavailable: list[str]) -> bool:
        if not domains:
            return True
        try:
            pairs = self._read_table_lineage(domains)
        except Exception:
            unavailable.append(TABLE_LINEAGE)
            return False
        for source, target in sorted(set(pairs)):
            from_id, to_id = f"table:{source}", f"table:{target}"
            if from_id in graph.nodes and to_id in graph.nodes and from_id != to_id:
                graph.edge(kind="derives", from_id=from_id, to_id=to_id, source="unity_catalog")
        return True
