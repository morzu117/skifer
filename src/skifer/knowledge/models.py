"""Nodes and edges of Skifer's knowledge graph (Plan 44, S02).

A node is identified by ``<kind>:<fqn>`` and carries a ``sha256:v1:`` fingerprint of
its canonical content: synchronization compares fingerprints and writes only what
changed, so a second sync over unchanged sources writes nothing.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
from typing import Any


NODE_KINDS = ("table", "column", "metric", "dimension", "rule", "domain")
NODE_STATUSES = ("certified", "candidate", "reference")
EDGE_KINDS = ("has_column", "joins", "derives", "belongs_to", "shortcut")
SOURCES = ("pipeline", "semantic", "rule", "unity_catalog", "candidate")
HASH_PREFIX = "sha256:v1:"


def _canonical_hash(payload: Any) -> str:
    canonical = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return HASH_PREFIX + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _fail(model: str, field_name: str, message: str) -> ValueError:
    return ValueError(f"{model}.{field_name} {message}")


def _non_empty(model: str, field_name: str, value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise _fail(model, field_name, f"must be a non-empty string; got {value!r}.")
    return value


def _choice(model: str, field_name: str, value: Any, allowed: tuple[str, ...]) -> str:
    if value not in allowed:
        raise _fail(model, field_name, f"must be one of {list(allowed)}; got {value!r}.")
    return value


def _string_tuple(model: str, field_name: str, value: Any) -> tuple[str, ...]:
    if not isinstance(value, tuple):
        raise _fail(model, field_name, "must be a tuple of strings.")
    for index, item in enumerate(value):
        _non_empty(model, f"{field_name}[{index}]", item)
    return value


def node_id_for(kind: str, fqn: str) -> str:
    return f"{kind}:{fqn}"


def node_fingerprint(
    *, kind: str, fqn: str, label: str, text: str, synonyms: tuple[str, ...],
    status: str, domain: str | None, source: str,
) -> str:
    """Fingerprint of everything a node says; any change yields a new one."""
    return _canonical_hash({
        "kind": kind, "fqn": fqn, "label": label, "text": text,
        "synonyms": list(synonyms), "status": status, "domain": domain, "source": source,
    })


def edge_id_for(kind: str, from_id: str, to_id: str, keys: tuple[str, ...]) -> str:
    """Identity of an edge: its kind, its endpoints and its join keys."""
    return _canonical_hash({"kind": kind, "from": from_id, "to": to_id, "keys": list(keys)})


@dataclass(frozen=True)
class Node:
    node_id: str
    kind: str
    fqn: str
    label: str
    text: str
    synonyms: tuple[str, ...]
    status: str
    domain: str | None
    source: str
    fingerprint: str

    def __post_init__(self) -> None:
        m = "Node"
        _choice(m, "kind", self.kind, NODE_KINDS)
        _non_empty(m, "fqn", self.fqn)
        if self.node_id != node_id_for(self.kind, self.fqn):
            raise _fail(m, "node_id", f"must be '<kind>:<fqn>'; got {self.node_id!r}.")
        _non_empty(m, "label", self.label)
        if not isinstance(self.text, str):
            raise _fail(m, "text", "must be a string.")
        _string_tuple(m, "synonyms", self.synonyms)
        _choice(m, "status", self.status, NODE_STATUSES)
        if self.domain is not None:
            _non_empty(m, "domain", self.domain)
        _choice(m, "source", self.source, SOURCES)
        expected = node_fingerprint(
            kind=self.kind, fqn=self.fqn, label=self.label, text=self.text,
            synonyms=self.synonyms, status=self.status, domain=self.domain, source=self.source,
        )
        if self.fingerprint != expected:
            raise _fail(m, "fingerprint", "does not match the node content.")

    @classmethod
    def build(
        cls, *, kind: str, fqn: str, label: str, text: str = "",
        synonyms: tuple[str, ...] = (), status: str = "reference",
        domain: str | None = None, source: str,
    ) -> "Node":
        """Build a node whose id and fingerprint are derived from its content."""
        return cls(
            node_id=node_id_for(kind, fqn), kind=kind, fqn=fqn, label=label, text=text,
            synonyms=tuple(synonyms), status=status, domain=domain, source=source,
            fingerprint=node_fingerprint(
                kind=kind, fqn=fqn, label=label, text=text, synonyms=tuple(synonyms),
                status=status, domain=domain, source=source,
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "node_id": self.node_id, "kind": self.kind, "fqn": self.fqn,
            "label": self.label, "text": self.text, "synonyms": list(self.synonyms),
            "status": self.status, "domain": self.domain, "source": self.source,
            "fingerprint": self.fingerprint,
        }


@dataclass(frozen=True)
class Edge:
    edge_id: str
    kind: str
    from_id: str
    to_id: str
    keys: tuple[str, ...]
    weight: float
    source: str

    def __post_init__(self) -> None:
        m = "Edge"
        _choice(m, "kind", self.kind, EDGE_KINDS)
        _non_empty(m, "from_id", self.from_id)
        _non_empty(m, "to_id", self.to_id)
        _string_tuple(m, "keys", self.keys)
        if self.edge_id != edge_id_for(self.kind, self.from_id, self.to_id, self.keys):
            raise _fail(m, "edge_id", "is not derived from (kind, from, to, keys).")
        if (
            isinstance(self.weight, bool)
            or not isinstance(self.weight, (int, float))
            or not math.isfinite(self.weight)
            or not 0 < self.weight <= 1
        ):
            raise _fail(m, "weight", f"must be within (0, 1]; got {self.weight!r}.")
        _choice(m, "source", self.source, SOURCES)

    @classmethod
    def build(
        cls, *, kind: str, from_id: str, to_id: str, keys: tuple[str, ...] = (),
        weight: float = 1.0, source: str,
    ) -> "Edge":
        keys = tuple(keys)
        return cls(
            edge_id=edge_id_for(kind, from_id, to_id, keys), kind=kind, from_id=from_id,
            to_id=to_id, keys=keys, weight=float(weight), source=source,
        )

    @property
    def fingerprint(self) -> str:
        """Content fingerprint: the id covers identity, this covers weight and source."""
        return _canonical_hash({
            "edge_id": self.edge_id, "weight": self.weight, "source": self.source,
        })

    def to_dict(self) -> dict[str, Any]:
        return {
            "edge_id": self.edge_id, "kind": self.kind, "from_id": self.from_id,
            "to_id": self.to_id, "keys": list(self.keys), "weight": self.weight,
            "source": self.source,
        }


@dataclass(frozen=True)
class SearchHit:
    node: Node
    score: float
    matched_terms: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "node": self.node.to_dict(), "score": self.score,
            "matched_terms": list(self.matched_terms),
        }


@dataclass(frozen=True)
class SyncReport:
    added: int
    updated: int
    unchanged: int
    removed: int
    unavailable: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "added": self.added, "updated": self.updated, "unchanged": self.unchanged,
            "removed": self.removed, "unavailable": list(self.unavailable),
        }


def node_from_row(row: Any) -> Node:
    data = row if isinstance(row, dict) else row.asDict(recursive=True)
    synonyms = data["synonyms"]
    if isinstance(synonyms, str):
        synonyms = json.loads(synonyms)
    return Node(
        node_id=data["node_id"], kind=data["kind"], fqn=data["fqn"], label=data["label"],
        text=data["text"] or "", synonyms=tuple(synonyms or ()), status=data["status"],
        domain=data["domain"], source=data["source"], fingerprint=data["fingerprint"],
    )


def edge_from_row(row: Any) -> Edge:
    data = row if isinstance(row, dict) else row.asDict(recursive=True)
    keys = data["keys"]
    if isinstance(keys, str):
        keys = json.loads(keys)
    return Edge(
        edge_id=data["edge_id"], kind=data["kind"], from_id=data["from_id"],
        to_id=data["to_id"], keys=tuple(keys or ()), weight=float(data["weight"]),
        source=data["source"],
    )
