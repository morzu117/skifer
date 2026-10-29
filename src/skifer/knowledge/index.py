"""Pure-Python BM25 search and bounded graph neighborhood (Plan 44, S02).

Built in memory from the store when loaded — a few thousand nodes, no Vector
Search and no graph database. BM25 is field-weighted (BM25F): a term found in a
node's label weighs more than the same term in its synonyms, which weigh more than
its free text. No stemming in v1. Iteration is always over sorted keys, so the
same graph and query give the same ranking under any ``PYTHONHASHSEED``.
"""

from __future__ import annotations

from collections import deque
import logging
import math
import re
import unicodedata
from typing import Any, Iterable

from .models import NODE_KINDS, Edge, Node, SearchHit
from .store import KnowledgeStore

logger = logging.getLogger(__name__)


SEARCH_MODES = ("bm25", "dense", "hybrid")  # default per environment: D7, see from_config
BM25_K1 = 1.2
BM25_B = 0.75
FIELD_WEIGHTS = {"label": 3.0, "synonyms": 2.0, "text": 1.0}

# Frozen in v1: changing this list changes every ranking.
STOPWORDS = frozenset({
    # English
    "a", "an", "and", "are", "as", "at", "be", "by", "for", "from", "has", "have", "how",
    "in", "is", "it", "its", "many", "much", "of", "on", "or", "per", "show", "that", "the",
    "their", "this", "to", "was", "what", "when", "where", "which", "who", "with",
    # French, including elided forms ("d'affaires" -> "d", "affaires")
    "au", "aux", "avec", "c", "ce", "ces", "combien", "dans", "de", "des", "du", "d", "elle",
    "en", "est", "et", "j", "l", "la", "le", "les", "leur", "leurs", "m", "mais", "n", "ne",
    "ou", "où", "par", "pas", "pour", "qu", "quel", "quelle", "quelles", "quels", "que", "qui",
    "s", "sa", "se", "ses", "son", "sont", "sur", "t", "un", "une", "y",
})

_CAMEL_RE = re.compile(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")
_DIGIT_RE = re.compile(r"(?<=[A-Za-z])(?=[0-9])|(?<=[0-9])(?=[A-Za-z])")
_SPLIT_RE = re.compile(r"[^a-z0-9]+")


def _strip_accents(text: str) -> str:
    decomposed = unicodedata.normalize("NFKD", text)
    return "".join(char for char in decomposed if not unicodedata.combining(char))


def tokenize(text: str) -> tuple[str, ...]:
    """Lowercase, accent-free word pieces, without stopwords; order preserved."""
    if not text:
        return ()
    text = _DIGIT_RE.sub(" ", _CAMEL_RE.sub(" ", _strip_accents(str(text))))
    return tuple(
        token for token in _SPLIT_RE.split(text.lower()) if token and token not in STOPWORDS
    )


def _fields(node: Node) -> dict[str, tuple[str, ...]]:
    return {
        "label": tokenize(node.label),
        "synonyms": tuple(token for synonym in node.synonyms for token in tokenize(synonym)),
        "text": tokenize(node.text),
    }


class KnowledgeIndex:
    """Search and neighborhood over the live (non-removed) nodes of a store."""

    def __init__(
        self, nodes: Iterable[Node], edges: Iterable[Edge] = (), *, embedder: Any = None,
        embeddings: dict[str, list[float]] | None = None, default_mode: str = "bm25",
    ):
        if default_mode not in SEARCH_MODES:
            raise ValueError(f"default_mode must be one of {list(SEARCH_MODES)}; "
                             f"got {default_mode!r}.")
        self.default_mode = default_mode
        self._nodes = {node.node_id: node for node in sorted(nodes, key=lambda n: n.node_id)}
        # Plan 44, S08: optional dense vectors, only for live nodes, from ``embedder``'s model.
        self._embedder = embedder
        self._embeddings = {key: vector for key, vector in sorted((embeddings or {}).items())
                            if key in self._nodes}
        self._tf: dict[str, dict[str, dict[str, int]]] = {}
        lengths: dict[str, list[int]] = {field: [] for field in FIELD_WEIGHTS}
        self._lengths: dict[str, dict[str, int]] = {}
        document_frequency: dict[str, int] = {}
        for node_id, node in self._nodes.items():
            per_field: dict[str, dict[str, int]] = {}
            self._lengths[node_id] = {}
            for field, tokens in _fields(node).items():
                counts: dict[str, int] = {}
                for token in tokens:
                    counts[token] = counts.get(token, 0) + 1
                per_field[field] = counts
                self._lengths[node_id][field] = len(tokens)
                lengths[field].append(len(tokens))
            self._tf[node_id] = per_field
            for term in sorted({t for counts in per_field.values() for t in counts}):
                document_frequency[term] = document_frequency.get(term, 0) + 1
        self._average = {
            field: (sum(values) / len(values) if values and sum(values) else 1.0)
            for field, values in lengths.items()
        }
        total = len(self._nodes)
        self._idf = {
            term: math.log(1 + (total - df + 0.5) / (df + 0.5))
            for term, df in document_frequency.items()
        }
        self._adjacency: dict[str, list[tuple[str, float]]] = {}
        self._edges: list[Edge] = []
        for edge in sorted(edges, key=lambda e: e.edge_id):
            if edge.from_id in self._nodes and edge.to_id in self._nodes:
                self._edges.append(edge)
                self._adjacency.setdefault(edge.from_id, []).append((edge.to_id, edge.weight))
                self._adjacency.setdefault(edge.to_id, []).append((edge.from_id, edge.weight))

    @classmethod
    def from_store(
        cls, store: KnowledgeStore, *, embedder: Any = None, default_mode: str = "bm25"
    ) -> "KnowledgeIndex":
        """With ``embedder``, also the vectors stored for its model (``dense``/``hybrid``)."""
        embeddings = store.list_embeddings(embedder.model_name) if embedder is not None else None
        return cls(store.list_nodes(), store.list_edges(), embedder=embedder,
                   embeddings=embeddings, default_mode=default_mode)

    @classmethod
    def from_config(
        cls, store: KnowledgeStore, env_config: dict | None, *, env_name: str | None = None,
        load_embedder: Any = None,
    ) -> "KnowledgeIndex":
        """The index an environment searches with, its default mode resolved per D7.

        An explicit ``knowledge_search_mode: dense|hybrid`` whose model cannot load is an
        error. A ``hybrid`` that is only the D7 default (no ``knowledge_search_mode``)
        falls back to ``bm25`` with one warning naming the error class: an environment
        that names a model but has not copied it yet keeps answering.
        """
        from skifer.core.config import knowledge_embedding_settings

        from .embeddings import load_embedder as default_loader

        model, cache, mode = knowledge_embedding_settings(env_config, env_name=env_name)
        if mode == "bm25":
            return cls.from_store(store)
        try:
            embedder = (load_embedder or default_loader)(model, cache_dir=cache)
        except Exception as exc:
            if (env_config or {}).get("knowledge_search_mode"):
                raise
            logger.warning("Knowledge search falls back to bm25: embedding model %r unavailable "
                           "(%s).", model, type(exc).__name__)
            return cls.from_store(store)
        return cls.from_store(store, embedder=embedder, default_mode=mode)

    def __len__(self) -> int:
        return len(self._nodes)

    def get(self, node_id: str) -> Node | None:
        """The live node with this id, or None (absent or soft-deleted)."""
        return self._nodes.get(node_id)

    def nodes(self, *, kinds: Iterable[str] | None = None) -> list[Node]:
        """Live nodes, optionally of some kinds, sorted by id."""
        selected = _node_kinds(kinds)
        return [node for node_id, node in sorted(self._nodes.items())
                if selected is None or node.kind in selected]

    def edges(
        self, *, node_ids: Iterable[str] | None = None, kinds: Iterable[str] | None = None
    ) -> list[Edge]:
        """Live edges, optionally only those with both ends in ``node_ids``."""
        within = None if node_ids is None else set(node_ids)
        selected = None if kinds is None else set(kinds)
        return [
            edge for edge in self._edges
            if (selected is None or edge.kind in selected)
            and (within is None or (edge.from_id in within and edge.to_id in within))
        ]

    def _score(self, node_id: str, terms: tuple[str, ...]) -> tuple[float, tuple[str, ...]]:
        score = 0.0
        matched = []
        for term in terms:
            weighted_tf = 0.0
            for field, weight in FIELD_WEIGHTS.items():
                tf = self._tf[node_id][field].get(term, 0)
                if tf:
                    norm = 1 - BM25_B + BM25_B * self._lengths[node_id][field] / self._average[field]
                    weighted_tf += weight * tf / norm
            if weighted_tf:
                matched.append(term)
                score += self._idf[term] * weighted_tf * (BM25_K1 + 1) / (weighted_tf + BM25_K1)
        return score, tuple(matched)

    def search(
        self, query: str, *, k: int = 10, kinds: Iterable[str] | None = None,
        domain: str | None = None, mode: str | None = None,
    ) -> list[SearchHit]:
        mode = self.default_mode if mode is None else mode
        if k < 1:
            raise ValueError(f"k must be a positive integer; got {k!r}.")
        if mode not in SEARCH_MODES:
            raise ValueError(f"mode must be one of {list(SEARCH_MODES)}; got {mode!r}.")
        if mode != "bm25":
            return self._search_with_vectors(query, k=k, kinds=kinds, domain=domain, mode=mode)
        selected = _node_kinds(kinds)
        terms = tuple(sorted(set(tokenize(query))))
        if not terms:
            return []
        hits = []
        for node_id, node in self._nodes.items():
            if selected is not None and node.kind not in selected:
                continue
            if domain is not None and node.domain != domain:
                continue
            score, matched = self._score(node_id, terms)
            if score > 0:
                hits.append(SearchHit(node=node, score=score, matched_terms=matched))
        hits.sort(key=lambda hit: (-hit.score, hit.node.node_id))
        return hits[:k]

    def _search_with_vectors(
        self, query: str, *, k: int, kinds: Iterable[str] | None, domain: str | None, mode: str,
    ) -> list[SearchHit]:
        """``dense``: cosine to the query vector. ``hybrid``: RRF of the BM25 and dense rankings."""
        from .embeddings import cosine, extra_missing_error, reciprocal_rank_fusion

        if self._embedder is None:
            raise extra_missing_error()
        selected = _node_kinds(kinds)
        query_vector = self._embedder.embed_query(query)
        dense = sorted(
            (
                (cosine(query_vector, vector), node_id)
                for node_id, vector in self._embeddings.items()
                if (selected is None or self._nodes[node_id].kind in selected)
                and (domain is None or self._nodes[node_id].domain == domain)
            ),
            key=lambda pair: (-pair[0], pair[1]),
        )
        if mode == "dense":
            return [SearchHit(node=self._nodes[node_id], score=score, matched_terms=())
                    for score, node_id in dense[:k]]
        lexical = self.search(query, k=max(len(self._nodes), 1), kinds=kinds, domain=domain,
                              mode="bm25")
        matched = {hit.node.node_id: hit.matched_terms for hit in lexical}
        fused = reciprocal_rank_fusion([[hit.node.node_id for hit in lexical],
                                        [node_id for _, node_id in dense]])
        return [SearchHit(node=self._nodes[node_id], score=score,
                          matched_terms=matched.get(node_id, ()))
                for node_id, score in fused[:k]]

    def neighbors(
        self, node_id: str, *, max_depth: int = 2, min_weight: float = 0.5,
        kinds: Iterable[str] | None = None,
    ) -> list[tuple[Node, int]]:
        """Nodes reachable within ``max_depth`` hops over edges of weight ≥ ``min_weight``.

        Edges are walked in both directions; each node appears once, at its minimal
        distance (breadth-first), and a cycle is never followed twice. ``kinds``
        filters the returned nodes, not the nodes walked through.
        """
        if node_id not in self._nodes or max_depth < 1:
            return []
        selected = _node_kinds(kinds)
        distance = {node_id: 0}
        queue = deque([node_id])
        while queue:
            current = queue.popleft()
            if distance[current] >= max_depth:
                continue
            for neighbor, weight in self._adjacency.get(current, ()):
                if weight >= min_weight and neighbor not in distance:
                    distance[neighbor] = distance[current] + 1
                    queue.append(neighbor)
        found = [
            (self._nodes[other], hops)
            for other, hops in distance.items()
            if other != node_id and (selected is None or self._nodes[other].kind in selected)
        ]
        return sorted(found, key=lambda pair: (pair[1], pair[0].node_id))


def _node_kinds(kinds: Iterable[str] | None) -> set[str] | None:
    if kinds is None:
        return None
    selected = set(kinds)
    unknown = sorted(selected - set(NODE_KINDS))
    if unknown:
        raise ValueError(f"Unknown kinds {unknown}; expected a subset of {list(NODE_KINDS)}.")
    return selected
