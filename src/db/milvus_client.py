from __future__ import annotations

from pymilvus import (
    connections,
    Collection,
    CollectionSchema,
    FieldSchema,
    DataType,
    utility,
)

from src.utils.config import MILVUS_HOST, MILVUS_PORT, MILVUS_COLLECTION, MILVUS_DIM
from src.utils.logger import get_logger

logger = get_logger("db.milvus")

_connected = False
_collection: Collection | None = None


# ── Connection ────────────────────────────────────────────────

def connect() -> None:
    global _connected
    if not _connected:
        connections.connect(host=MILVUS_HOST, port=str(MILVUS_PORT))
        _connected = True
        logger.info(f"Milvus connected | host={MILVUS_HOST}:{MILVUS_PORT}")


# ── Collection setup ──────────────────────────────────────────

def get_collection() -> Collection:
    """
    Returns the collection, creating it with the correct schema if it
    doesn't already exist. Safe to call on every pipeline boot.
    """
    global _collection
    connect()

    if _collection is not None:
        return _collection

    if utility.has_collection(MILVUS_COLLECTION):
        _collection = Collection(MILVUS_COLLECTION)
        _collection.load()
        logger.info(f"Milvus collection loaded | name={MILVUS_COLLECTION}")
        return _collection

    # Define schema
    fields = [
        FieldSchema(name="id",          dtype=DataType.INT64,         is_primary=True, auto_id=True),
        FieldSchema(name="user_id",     dtype=DataType.VARCHAR,       max_length=64),
        FieldSchema(name="message_id",  dtype=DataType.VARCHAR,       max_length=64),
        FieldSchema(name="lineage_id",  dtype=DataType.VARCHAR,       max_length=64),
        FieldSchema(name="embedding",   dtype=DataType.FLOAT_VECTOR,  dim=MILVUS_DIM),
    ]
    schema = CollectionSchema(fields=fields, description="User message embeddings")
    _collection = Collection(name=MILVUS_COLLECTION, schema=schema)

    # IVF_FLAT index — good recall at prototype scale
    index_params = {
        "metric_type": "COSINE",
        "index_type":  "IVF_FLAT",
        "params":      {"nlist": 128},
    }
    _collection.create_index(field_name="embedding", index_params=index_params)
    _collection.load()
    logger.info(f"Milvus collection created | name={MILVUS_COLLECTION} dim={MILVUS_DIM}")
    return _collection


# ── Write ─────────────────────────────────────────────────────

def insert_vectors(
    user_ids:    list[str],
    message_ids: list[str],
    lineage_ids: list[str],
    vectors:     list[list[float]],
) -> None:
    """
    Batch insert vectors. All lists must be the same length.
    """
    if not vectors:
        logger.warning("insert_vectors called with empty list — skipped")
        return

    col = get_collection()
    data = [user_ids, message_ids, lineage_ids, vectors]
    col.insert(data)
    col.flush()     # persist to disk
    logger.info(f"Milvus inserted {len(vectors)} vectors")


# ── Search ────────────────────────────────────────────────────

def search_similar_users(query_vector: list[float], top_k: int = 5) -> list[str]:
    """
    ANN search: returns top_k user_ids most similar to the query vector.
    Duplicates are removed (a user may have many messages).
    """
    col = get_collection()

    search_params = {"metric_type": "COSINE", "params": {"nprobe": 16}}
    results = col.search(
        data=[query_vector],
        anns_field="embedding",
        param=search_params,
        limit=top_k * 3,        # over-fetch to deduplicate user_ids
        output_fields=["user_id"],
    )

    seen: set[str] = set()
    user_ids: list[str] = []
    for hit in results[0]:
        uid = hit.entity.get("user_id")
        if uid and uid not in seen:
            seen.add(uid)
            user_ids.append(uid)
        if len(user_ids) >= top_k:
            break

    logger.debug(f"Milvus ANN search returned {len(user_ids)} unique users")
    return user_ids


def close() -> None:
    global _connected, _collection
    connections.disconnect("default")
    _connected = False
    _collection = None
    logger.info("Milvus disconnected")