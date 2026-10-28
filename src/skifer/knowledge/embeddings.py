"""Optional dense embeddings for the knowledge index (Plan 44, S08).

This module makes the D7 measurement possible; it decides nothing. BM25 stays the
default search mode. ``sentence_transformers`` (and so ``torch``) is imported
lazily, inside ``load_embedder``: importing ``skifer`` or ``skifer.knowledge``
never loads it, with or without the ``[knowledge-embeddings]`` extra.

Models run on the driver, no endpoint and no Vector Search. Nothing is ever
downloaded implicitly: ``local_files_only=True`` by default, and a model missing
from the cache is an explicit error naming the cache path.
"""

from __future__ import annotations

import math
import os
from typing import Any, Protocol, Sequence, runtime_checkable

SUPPORTED_MODELS = {
    "multilingual-e5-small": {"hf_id": "intfloat/multilingual-e5-small",
                              "query_prefix": "query: ", "passage_prefix": "passage: "},
    "bge-m3": {"hf_id": "BAAI/bge-m3", "query_prefix": "", "passage_prefix": ""},
}
EXTRA_INSTALL = 'pip install "skifer[knowledge-embeddings]"'
EMBED_BATCH_SIZE = 64


class EmbeddingsUnavailableError(RuntimeError):
    """Dense search was asked for and cannot run: the extra or the model is missing."""


@runtime_checkable
class Embedder(Protocol):
    model_name: str

    def embed_passages(self, texts: Sequence[str]) -> list[list[float]]: ...

    def embed_query(self, text: str) -> list[float]: ...


def validate_model_name(model_name: Any) -> str:
    if model_name not in SUPPORTED_MODELS:
        raise ValueError(f"Unknown embedding model {model_name!r}; expected one of "
                         f"{sorted(SUPPORTED_MODELS)}.")
    return model_name


def extra_missing_error() -> EmbeddingsUnavailableError:
    return EmbeddingsUnavailableError(
        f"Dense knowledge search needs sentence-transformers: {EXTRA_INSTALL}. "
        "BM25 search (the default) works without it."
    )


class PrefixedEmbedder:
    """Applies a model's query/passage prefixes around any ``encode(texts, ...)`` encoder."""

    def __init__(self, model_name: str, encoder: Any):
        self.model_name = validate_model_name(model_name)
        self._encoder = encoder
        spec = SUPPORTED_MODELS[model_name]
        self._query_prefix = spec["query_prefix"]
        self._passage_prefix = spec["passage_prefix"]

    def _encode(self, texts: list[str]) -> list[list[float]]:
        vectors = self._encoder.encode(texts, normalize_embeddings=True)
        return [[float(value) for value in vector] for vector in vectors]

    def embed_passages(self, texts: Sequence[str]) -> list[list[float]]:
        return self._encode([self._passage_prefix + text for text in texts])

    def embed_query(self, text: str) -> list[float]:
        return self._encode([self._query_prefix + text])[0]


def _default_cache() -> str:
    home = os.environ.get("HF_HOME") or os.path.join(os.path.expanduser("~"), ".cache",
                                                     "huggingface")
    return os.path.join(home, "hub")


def load_embedder(
    model_name: str, *, cache_dir: str | None = None, local_files_only: bool = True
) -> Embedder:
    """Load a supported model from the local cache; never downloads unless told to."""
    validate_model_name(model_name)
    try:
        from sentence_transformers import SentenceTransformer
    except ImportError:
        raise extra_missing_error() from None
    try:
        encoder = SentenceTransformer(SUPPORTED_MODELS[model_name]["hf_id"],
                                      cache_folder=cache_dir, local_files_only=local_files_only)
    except Exception as exc:
        where = cache_dir or _default_cache()
        raise EmbeddingsUnavailableError(
            f"Embedding model {model_name!r} could not be loaded from the cache {where!r} "
            f"(local_files_only={local_files_only}): {type(exc).__name__}. Copy the model "
            "there (knowledge_model_cache), or load it once with local_files_only=False."
        ) from None
    return PrefixedEmbedder(model_name, encoder)


def passage_text(node: Any) -> str:
    """What a node is embedded as: its label, synonyms and free text."""
    return " ".join(part for part in (node.label, *node.synonyms, node.text) if part)


def embed_nodes(store: Any, embedder: Embedder) -> int:
    """Embed the live nodes whose embedding is missing or from another model; returns how many.

    A node whose fingerprint changed was rewritten without its embedding by the
    sync, so "embedded with this model" is all there is to check.
    """
    pending = store.nodes_to_embed(embedder.model_name)
    for start in range(0, len(pending), EMBED_BATCH_SIZE):
        batch = pending[start:start + EMBED_BATCH_SIZE]
        vectors = embedder.embed_passages([passage_text(node) for node in batch])
        store.set_embeddings(embedder.model_name,
                             {node.node_id: vector for node, vector in zip(batch, vectors)})
    return len(pending)


def cosine(left: Sequence[float], right: Sequence[float]) -> float:
    dot = sum(a * b for a, b in zip(left, right))
    norm = math.sqrt(sum(a * a for a in left)) * math.sqrt(sum(b * b for b in right))
    return dot / norm if norm else 0.0


def reciprocal_rank_fusion(
    rankings: Sequence[Sequence[str]], *, k: int = 60
) -> list[tuple[str, float]]:
    """RRF: ``sum(1 / (k + rank))`` over the rankings, ranks from 1; ties broken by id."""
    if isinstance(k, bool) or not isinstance(k, int) or k < 1:
        raise ValueError(f"k must be a positive integer; got {k!r}.")
    scores: dict[str, float] = {}
    for ranking in rankings:
        for rank, item in enumerate(ranking, start=1):
            scores[item] = scores.get(item, 0.0) + 1.0 / (k + rank)
    return sorted(scores.items(), key=lambda pair: (-pair[1], pair[0]))
