from __future__ import annotations

from typing import Any
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import PyMongoError

from src.utils.config import (
    MONGO_URI,
    MONGO_DB,
    MONGO_COLLECTION_CONVERSATIONS,
    MONGO_COLLECTION_DLQ,
)
from src.utils.logger import get_logger

logger = get_logger("db.mongo")

# ── Singleton client ──────────────────────────────────────────
_client: MongoClient | None = None


def get_client() -> MongoClient:
    global _client
    if _client is None:
        _client = MongoClient(MONGO_URI)
        logger.info("MongoDB client initialised")
    return _client


def get_collection(name: str) -> Collection:
    return get_client()[MONGO_DB][name]


# ── Write helpers ─────────────────────────────────────────────

def insert_conversation(doc: dict[str, Any]) -> str | None:
    """
    Insert a validated conversation document.
    Returns the inserted _id as string, or None on failure.
    """
    try:
        col: Collection = get_collection(MONGO_COLLECTION_CONVERSATIONS)
        result = col.insert_one(doc)
        logger.debug(f"Inserted conversation _id={result.inserted_id}")
        return str(result.inserted_id)
    except PyMongoError as exc:
        logger.error(f"MongoDB insert failed: {exc}")
        return None


def insert_to_dlq(original_payload: dict[str, Any], error: str) -> None:
    """
    Route a failed record to the dead-letter queue with the error reason.
    """
    try:
        col: Collection = get_collection(MONGO_COLLECTION_DLQ)
        col.insert_one({"payload": original_payload, "error": error})
        logger.warning(f"Record sent to DLQ | error={error}")
    except PyMongoError as exc:
        logger.error(f"DLQ insert also failed: {exc}")


def bulk_insert_conversations(docs: list[dict[str, Any]]) -> int:
    """
    Bulk insert for batch ingestion.
    Returns the number of successfully inserted documents.
    """
    if not docs:
        return 0
    try:
        col: Collection = get_collection(MONGO_COLLECTION_CONVERSATIONS)
        result = col.insert_many(docs, ordered=False)
        count = len(result.inserted_ids)
        logger.info(f"Bulk inserted {count}/{len(docs)} documents")
        return count
    except PyMongoError as exc:
        logger.error(f"Bulk insert failed: {exc}")
        return 0


def close() -> None:
    global _client
    if _client:
        _client.close()
        _client = None
        logger.info("MongoDB connection closed")