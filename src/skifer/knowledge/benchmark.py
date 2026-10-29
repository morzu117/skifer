"""Search benchmark over the reference set: the measurement that decides D7 (Plan 44, S09).

Each configuration — ``bm25``, ``dense:<model>``, ``hybrid:<model>`` — is built from
the same store and asked the same family A questions. Nothing is written: dense
vectors are computed in memory, so the build time is the whole cost of the index.
"""

from __future__ import annotations

import statistics
import time
from typing import Any, Callable, Sequence

from .embeddings import SUPPORTED_MODELS
from .index import KnowledgeIndex

BENCHMARK_K = (5, 10)


def parse_config(config: str) -> tuple[str, str | None]:
    """``bm25`` | ``dense:<model>`` | ``hybrid:<model>`` → (mode, model); ``ValueError`` otherwise."""
    if config == "bm25":
        return "bm25", None
    mode, _, model = str(config).partition(":")
    if mode in ("dense", "hybrid") and model in SUPPORTED_MODELS:
        return mode, model
    raise ValueError(f"Unknown benchmark config {config!r}; expected 'bm25', 'dense:<model>' or "
                     f"'hybrid:<model>' with a model in {sorted(SUPPORTED_MODELS)}.")


def _build(store: Any, mode: str, model: str | None, load: Callable, model_cache: str | None):
    from .embeddings import passage_text

    nodes, edges = store.list_nodes(), store.list_edges()
    if mode == "bm25":
        return KnowledgeIndex(nodes, edges)
    embedder = load(model, cache_dir=model_cache)
    vectors = embedder.embed_passages([passage_text(node) for node in nodes]) if nodes else []
    return KnowledgeIndex(nodes, edges, embedder=embedder,
                          embeddings={node.node_id: vector for node, vector in zip(nodes, vectors)})


def benchmark(
    store: Any, questions: Sequence[Any], *, configs: Sequence[str],
    model_cache: str | None = None, load_embedder: Callable | None = None,
    monotonic: Callable[[], float] = time.perf_counter,
) -> dict:
    """Per config: ``recall@5``, ``recall@10``, ``mean_rank``, ``latency_ms_p50``, ``index_build_s``.

    Only family A questions count (their expected metric is known). ``mean_rank`` is
    over the questions whose metric is ranked at all, ``not_found`` counts the others;
    both are ``None``-safe on an empty set.
    """
    parsed = [(config, *parse_config(config)) for config in configs]   # fail before any work
    if not parsed:
        raise ValueError("benchmark needs at least one config.")
    if load_embedder is None:
        from .embeddings import load_embedder
    family_a = sorted((q for q in questions if q.family == "A"), key=lambda q: q.question_id)
    results: dict[str, dict] = {}
    for config, mode, model in parsed:
        started = monotonic()
        index = _build(store, mode, model, load_embedder, model_cache)
        build_seconds = monotonic() - started
        depth = max(len(index), 1)
        ranks: list[int | None] = []
        latencies: list[float] = []
        for question in family_a:
            target = f"metric:{question.expected['model_key']}.{question.expected['metric']}"
            started = monotonic()
            hits = index.search(question.question, k=depth, kinds=["metric"], mode=mode)
            latencies.append((monotonic() - started) * 1000)
            ids = [hit.node.node_id for hit in hits]
            ranks.append(ids.index(target) + 1 if target in ids else None)
        found = [rank for rank in ranks if rank is not None]
        total = len(family_a)
        results[config] = {
            **{f"recall@{k}": (round(sum(1 for r in found if r <= k) / total, 6) if total
                               else None) for k in BENCHMARK_K},
            "mean_rank": round(statistics.mean(found), 6) if found else None,
            "not_found": total - len(found),
            "latency_ms_p50": round(statistics.median(latencies), 3) if latencies else None,
            "index_build_s": round(build_seconds, 3),
            "questions": total,
        }
    return results
