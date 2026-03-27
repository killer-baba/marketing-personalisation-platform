from __future__ import annotations

import numpy as np
from sentence_transformers import SentenceTransformer

from src.utils.config import EMBEDDING_MODEL
from src.utils.logger import get_logger
from src.pipeline.ingest import ConversationRecord

logger = get_logger("pipeline.embed")

# ── Singleton model ───────────────────────────────────────────
_model: SentenceTransformer | None = None


def get_model() -> SentenceTransformer:
    global _model
    if _model is None:
        logger.info(f"Loading embedding model: {EMBEDDING_MODEL}")
        _model = SentenceTransformer(EMBEDDING_MODEL)
        logger.info("Embedding model loaded")
    return _model


# ── Anomaly detection ─────────────────────────────────────────

ZERO_NORM_THRESHOLD = 1e-6   # below this → treat as empty embedding

def is_empty_embedding(vector: list[float]) -> bool:
    """
    Detects a degenerate (all-zero or near-zero) embedding.
    This catches silent failures from the model.
    """
    norm = float(np.linalg.norm(vector))
    return norm < ZERO_NORM_THRESHOLD


# ── Embed a batch ─────────────────────────────────────────────

class EmbedResult:
    """Holds one record paired with its embedding (or an anomaly flag)."""
    __slots__ = ("record", "vector", "anomaly")

    def __init__(
        self,
        record:  ConversationRecord,
        vector:  list[float],
        anomaly: bool = False,
    ):
        self.record  = record
        self.vector  = vector
        self.anomaly = anomaly


def embed_batch(records: list[ConversationRecord]) -> list[EmbedResult]:
    """
    Generate embeddings for a batch of validated records.
    Automatically detects and flags empty/degenerate embeddings.

    Returns a list of EmbedResult objects in the same order as input.
    """
    if not records:
        return []

    model = get_model()
    messages = [r.message for r in records]

    # batch encode — sentence-transformers handles batching internally
    vectors: np.ndarray = model.encode(
        messages,
        batch_size=32,
        show_progress_bar=False,
        normalize_embeddings=True,    # unit-normalise for cosine similarity
    )

    results: list[EmbedResult] = []
    anomaly_count = 0

    for record, vec in zip(records, vectors):
        vec_list = vec.tolist()
        anomaly  = is_empty_embedding(vec_list)

        if anomaly:
            anomaly_count += 1
            logger.warning(
                f"Empty embedding detected | "
                f"user_id={record.user_id} message_id={record.message_id}"
            )

        results.append(EmbedResult(record=record, vector=vec_list, anomaly=anomaly))

    if anomaly_count:
        logger.warning(f"Anomaly summary | empty_embeddings={anomaly_count}/{len(records)}")
    else:
        logger.info(f"Embeddings generated | count={len(records)} anomalies=0")

    return results


def embed_single(text: str) -> list[float]:
    """
    Embed a single text string. Used by the API for query-time embedding.
    """
    model = get_model()
    vec = model.encode(text, normalize_embeddings=True)
    return vec.tolist()