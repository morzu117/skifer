"""Skifer's knowledge graph and metadata index (Plan 44, S02).

Tables, columns, certified metrics, dimensions, business rules and Unity Catalog
objects as nodes; ``has_column``/``joins``/``derives``/``belongs_to``/``shortcut``
as edges. Stored in SQLite locally and Delta on Databricks, searched with a
pure-Python BM25 index: no Spark, no embeddings and no LLM on the search path.
"""

from .index import KnowledgeIndex, tokenize
from .models import (
    EDGE_KINDS,
    NODE_KINDS,
    NODE_STATUSES,
    Edge,
    Node,
    SearchHit,
    SyncReport,
)
from .store import (
    DeltaKnowledgeStore,
    KnowledgeStore,
    SqliteKnowledgeStore,
    default_knowledge_store,
)
from .sync import KnowledgeSynchronizer

__all__ = [
    "EDGE_KINDS",
    "NODE_KINDS",
    "NODE_STATUSES",
    "DeltaKnowledgeStore",
    "Edge",
    "KnowledgeIndex",
    "KnowledgeStore",
    "KnowledgeSynchronizer",
    "Node",
    "SearchHit",
    "SqliteKnowledgeStore",
    "SyncReport",
    "default_knowledge_store",
    "tokenize",
]
