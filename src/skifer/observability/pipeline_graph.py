"""Spark-free inter-pipeline dataset graph from the metadata registry."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re

from skifer.core.dialect import split_fqn
from skifer.core.ir import parse_to_ir
from skifer.core.schema_loader import parse_schema
from skifer.observability.metadata_store import DatasetRecord, MetadataStore


_PLACEHOLDER_RE = re.compile(r"\{\{\s*(\w+)\s*\}\}")


class PipelineGraphError(ValueError):
    """Raised when the registry cannot be converted into a dataset graph."""


class PipelineGraphCycleError(PipelineGraphError):
    """Raised when indexed pipelines form a cycle."""

    def __init__(self, cycle: tuple[str, ...]):
        self.cycle = cycle
        super().__init__(
            "Pipeline graph contains a cycle: " + " -> ".join(cycle) + "."
        )


@dataclass(frozen=True, order=True)
class PipelineEdge:
    """A producer pipeline whose output is read by a consumer pipeline."""

    producer: str
    consumer: str

    def to_dict(self) -> dict[str, str]:
        return {"producer": self.producer, "consumer": self.consumer}


@dataclass(frozen=True, order=True)
class ExternalSource:
    """A declared input with no indexed producer in this registry.

    ``kind`` says whether indexing more pipelines could turn this into an edge.
    A ``table`` is a catalog reference whose producer may simply not be indexed
    yet; a ``file`` or a ``loader`` reads outside the catalog and will never have
    one, however many pipelines are indexed afterwards. Collapsing the three into
    "external" would leave a reader waiting for an edge that cannot arrive.
    """

    consumer: str
    source: str
    kind: str = "table"

    def to_dict(self) -> dict[str, str]:
        return {"consumer": self.consumer, "source": self.source, "kind": self.kind}


@dataclass(frozen=True)
class PipelineGraph:
    nodes: tuple[str, ...]
    edges: tuple[PipelineEdge, ...]
    external_sources: tuple[ExternalSource, ...]
    pipeline_paths: tuple[tuple[str, str], ...] = ()

    def to_dict(self) -> dict:
        return {
            "nodes": list(self.nodes),
            "edges": [edge.to_dict() for edge in self.edges],
            "external_sources": [
                external.to_dict() for external in self.external_sources
            ],
            "pipeline_paths": [
                {"node": node, "pipeline_path": path}
                for node, path in self.pipeline_paths
            ],
            "summary": {
                "nodes": len(self.nodes),
                "edges": len(self.edges),
                "external_sources": len(self.external_sources),
            },
        }

    def to_text(self) -> str:
        lines = ["Pipeline graph", "Nodes:"]
        lines.extend(_indented(self.nodes))
        lines.append("Edges:")
        lines.extend(
            _indented(f"{edge.producer} -> {edge.consumer}" for edge in self.edges)
        )
        lines.append("External sources:")
        lines.extend(
            _indented(
                f"{external.consumer} <- {external.source}"
                for external in self.external_sources
            )
        )
        return "\n".join(lines)

    def to_mermaid(self, direction: str = "LR") -> str:
        node_ids = {
            name: f"n{index}"
            for index, name in enumerate(self.nodes)
        }
        external_names = sorted(
            {external.source for external in self.external_sources}
        )
        external_ids = {
            name: f"x{index}"
            for index, name in enumerate(external_names)
        }

        lines = [f"graph {direction}"]
        for name in self.nodes:
            lines.append(f'    {node_ids[name]}["{_mermaid_label(name)}"]')
        for name in external_names:
            label = f"{name} (external)"
            lines.append(f'    {external_ids[name]}["{_mermaid_label(label)}"]')
        for edge in self.edges:
            lines.append(f"    {node_ids[edge.producer]} --> {node_ids[edge.consumer]}")
        for external in self.external_sources:
            lines.append(
                f"    {external_ids[external.source]} -. external .-> "
                f"{node_ids[external.consumer]}"
            )
        if external_names:
            lines.append("    classDef external fill:#f7f7f7,stroke:#777,stroke-dasharray: 4 3")
            for name in external_names:
                lines.append(f"    class {external_ids[name]} external")
        return "\n".join(lines)


def build_pipeline_graph(store: MetadataStore) -> PipelineGraph:
    """Build the inter-pipeline graph from indexed records only."""
    return graph_from_records(store.list_all())


def graph_from_records(records: list[DatasetRecord]) -> PipelineGraph:
    latest = _latest_records(records)
    nodes = tuple(sorted(latest))
    target_index = _target_index(nodes)

    edges: set[PipelineEdge] = set()
    external_sources: set[ExternalSource] = set()
    for consumer in nodes:
        record = latest[consumer]
        for source, kind in _declared_source_tables(record):
            # Only a catalog reference can be another pipeline's output. A file
            # source or a loader reads outside the catalog, so matching its
            # declared name against a target FQN would invent a dependency that
            # does not exist — and an invented edge decides execution order.
            producer = target_index.get(_fqn_key(source)) if kind == "table" else None
            if producer is None:
                external_sources.add(
                    ExternalSource(consumer=consumer, source=source, kind=kind)
                )
            else:
                # A pipeline declaring its own target as a source is kept as a
                # self-edge on purpose, so the cycle check names it. Dropping it
                # here would make the graph look acyclic and hide the mistake.
                edges.add(PipelineEdge(producer=producer, consumer=consumer))

    graph = PipelineGraph(
        nodes=nodes,
        edges=tuple(sorted(edges)),
        external_sources=tuple(sorted(external_sources)),
        pipeline_paths=tuple(
            (node, latest[node].pipeline_path) for node in nodes
        ),
    )
    _raise_on_cycle(graph)
    return graph


def _latest_records(records: list[DatasetRecord]) -> dict[str, DatasetRecord]:
    latest: dict[str, DatasetRecord] = {}
    ordered = sorted(
        records,
        key=lambda record: (
            record.target_fqn,
            record.indexed_at.isoformat(),
            record.definition_hash,
            record.pipeline_path,
        ),
    )
    for record in ordered:
        latest[record.target_fqn] = record
    return latest


def _target_index(nodes: tuple[str, ...]) -> dict[tuple[str, ...], str]:
    by_key: dict[tuple[str, ...], str] = {}
    for target in nodes:
        key = _fqn_key(target)
        existing = by_key.get(key)
        if existing is not None and existing != target:
            raise PipelineGraphError(
                "Metadata registry contains duplicate target FQNs after "
                f"identifier normalization: {existing!r}, {target!r}."
            )
        by_key[key] = target
    return by_key


def _declared_source_tables(record: DatasetRecord) -> tuple[tuple[str, str], ...]:
    path = Path(record.pipeline_path)
    if not path.is_file():
        raise PipelineGraphError(
            f"Indexed pipeline '{record.target_fqn}' points to missing YAML "
            f"'{record.pipeline_path}'."
        )
    yaml_text = path.read_text(encoding="utf-8")
    schema = parse_schema(
        yaml_text,
        params=_sentinel_params(yaml_text),
        base_dir=str(path.parent),
    )
    parsed = parse_to_ir(schema)
    return tuple((table.name, _source_kind(table)) for table in parsed.tables)


def _source_kind(table) -> str:
    """Classify a declared input by what it actually reads."""
    if table.is_loader:
        return "loader"
    if table.source_type is not None:
        return "file"
    return "table"


def _fqn_key(fqn: str) -> tuple[str, ...]:
    return tuple(split_fqn(fqn.strip()))


def _sentinel_params(yaml_text: str) -> dict[str, str]:
    return {
        key: f"__sentinel_{key}__"
        for key in _PLACEHOLDER_RE.findall(yaml_text)
    }


def _raise_on_cycle(graph: PipelineGraph) -> None:
    adjacency: dict[str, list[str]] = {node: [] for node in graph.nodes}
    for edge in graph.edges:
        adjacency[edge.producer].append(edge.consumer)
    for node in adjacency:
        adjacency[node].sort()

    visiting: set[str] = set()
    visited: set[str] = set()
    stack: list[str] = []

    def visit(node: str) -> None:
        if node in visited:
            return
        if node in visiting:
            start = stack.index(node)
            raise PipelineGraphCycleError(tuple(stack[start:] + [node]))

        visiting.add(node)
        stack.append(node)
        for child in adjacency[node]:
            visit(child)
        stack.pop()
        visiting.remove(node)
        visited.add(node)

    for node in graph.nodes:
        visit(node)


def _indented(values) -> list[str]:
    rendered = [f"  {value}" for value in values]
    return rendered or ["  (none)"]


def _mermaid_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


class PipelineSelectionError(PipelineGraphError):
    """Raised when a ``--select`` expression names nothing, or names too much."""


@dataclass(frozen=True)
class Selector:
    """One parsed ``--select`` expression.

    ``+name`` adds what the node reads, ``name+`` what reads it, ``+name+`` both.
    The bare form selects the node alone. This is dbt's grammar, deliberately:
    the operator sits on the side the selection travels towards, so a reader who
    knows one tool reads the other without a table of equivalences.
    """

    name: str
    upstream: bool
    downstream: bool


def parse_selector(expression: str) -> Selector:
    """Parse ``[+]name[+]`` into a selector, refusing an empty name."""
    raw = expression.strip()
    if not raw:
        raise PipelineSelectionError("A --select expression cannot be empty.")

    upstream = raw.startswith("+")
    if upstream:
        raw = raw[1:]
    downstream = raw.endswith("+")
    if downstream:
        raw = raw[:-1]

    name = raw.strip()
    if not name:
        raise PipelineSelectionError(
            f"--select {expression!r} names no pipeline: '+' alone selects nothing."
        )
    return Selector(name=name, upstream=upstream, downstream=downstream)


def resolve_node(graph: PipelineGraph, name: str) -> str:
    """Resolve a selector name to exactly one node, by target FQN or YAML path.

    An exact target FQN wins outright, so a pipeline can always be named by the
    thing it produces. Otherwise the name is matched against indexed YAML paths,
    and several matches are refused by name rather than arbitrated: picking one
    would run a pipeline the caller did not ask for.
    """
    if name in graph.nodes:
        return name

    key = _fqn_key(name)
    by_key = [node for node in graph.nodes if _fqn_key(node) == key]
    if len(by_key) == 1:
        return by_key[0]

    candidates = sorted(
        {
            node
            for node, path in graph.pipeline_paths
            if _path_matches(path, name)
        }
    )
    if len(candidates) == 1:
        return candidates[0]
    if len(candidates) > 1:
        raise PipelineSelectionError(
            f"--select {name!r} matches several indexed pipelines: "
            + ", ".join(candidates)
            + ". Name the target FQN instead."
        )
    raise PipelineSelectionError(
        f"--select {name!r} matches no indexed pipeline. "
        "Index it first with `skifer index <path>`."
    )


def _path_matches(path: str, name: str) -> bool:
    candidate = Path(path)
    wanted = Path(name)
    if candidate == wanted or candidate.name == name:
        return True
    parts, target = candidate.parts, wanted.parts
    return len(target) <= len(parts) and parts[-len(target):] == target


def topological_order(graph: PipelineGraph) -> tuple[str, ...]:
    """Order every node so each producer precedes its consumers.

    Ties are broken by name, not by insertion order: two runs of the same
    registry must print the same plan, or a reviewer cannot tell a reordering
    caused by a real dependency change from one caused by dictionary iteration.
    """
    indegree = {node: 0 for node in graph.nodes}
    adjacency: dict[str, list[str]] = {node: [] for node in graph.nodes}
    for edge in graph.edges:
        adjacency[edge.producer].append(edge.consumer)
        indegree[edge.consumer] += 1

    ready = sorted(node for node, degree in indegree.items() if degree == 0)
    ordered: list[str] = []
    while ready:
        node = ready.pop(0)
        ordered.append(node)
        for child in sorted(adjacency[node]):
            indegree[child] -= 1
            if indegree[child] == 0:
                ready.append(child)
        ready.sort()

    if len(ordered) != len(graph.nodes):
        # build_pipeline_graph already refuses a cycle, so reaching this means a
        # graph built by hand. Refusing beats returning a partial order that
        # silently drops the pipelines inside the cycle.
        remaining = tuple(sorted(set(graph.nodes) - set(ordered)))
        raise PipelineGraphCycleError(remaining)
    return tuple(ordered)


def select_nodes(
    graph: PipelineGraph, expressions: list[str] | tuple[str, ...]
) -> tuple[str, ...]:
    """Resolve selectors to the nodes to run, in producer-before-consumer order."""
    if not expressions:
        return topological_order(graph)

    adjacency, reverse = _adjacency(graph)
    selected: set[str] = set()
    for expression in expressions:
        selector = parse_selector(expression)
        node = resolve_node(graph, selector.name)
        selected.add(node)
        if selector.upstream:
            selected |= _closure(node, reverse)
        if selector.downstream:
            selected |= _closure(node, adjacency)

    order = topological_order(graph)
    return tuple(node for node in order if node in selected)


def _adjacency(
    graph: PipelineGraph,
) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    forward: dict[str, list[str]] = {node: [] for node in graph.nodes}
    reverse: dict[str, list[str]] = {node: [] for node in graph.nodes}
    for edge in graph.edges:
        forward[edge.producer].append(edge.consumer)
        reverse[edge.consumer].append(edge.producer)
    return forward, reverse


def _closure(start: str, adjacency: dict[str, list[str]]) -> set[str]:
    seen: set[str] = set()
    stack = [start]
    while stack:
        node = stack.pop()
        for neighbour in adjacency.get(node, ()):
            if neighbour not in seen:
                seen.add(neighbour)
                stack.append(neighbour)
    return seen
