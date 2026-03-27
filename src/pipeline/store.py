from __future__ import annotations

from src.pipeline.embed import EmbedResult
from src.db import mongo, milvus_client, neo4j_client, sqlite_client, redis_client
from src.utils.logger import get_logger, log_duration

logger = get_logger("pipeline.store")


def store_batch(embed_results: list[EmbedResult]) -> dict[str, int]:
    """
    Fan-out a batch of embedded records to all four stores.

    Write order:
      1. MongoDB  — raw text + metadata (source of truth)
      2. Milvus   — embedding vectors
      3. Neo4j    — user-campaign-intent graph edges
      4. SQLite   — aggregated interaction counts
      5. Redis    — invalidate any stale cached recommendations

    Returns a summary dict: { "mongo": N, "milvus": N, "neo4j": N, "sqlite": N }
    """
    if not embed_results:
        return {"mongo": 0, "milvus": 0, "neo4j": 0, "sqlite": 0}

    # Separate clean from anomalous — don't write bad vectors to Milvus
    clean    = [r for r in embed_results if not r.anomaly]
    anomalous = [r for r in embed_results if r.anomaly]

    if anomalous:
        logger.warning(
            f"Skipping {len(anomalous)} anomalous embeddings from Milvus write"
        )

    counts = {"mongo": 0, "milvus": 0, "neo4j": 0, "sqlite": 0}

    # ── 1. MongoDB ────────────────────────────────────────────
    with log_duration(logger, "store.mongo"):
        mongo_docs = [
            {
                "user_id":     r.record.user_id,
                "message_id":  r.record.message_id,
                "message":     r.record.message,
                "campaign_id": r.record.campaign_id,
                "intent":      r.record.intent,
                "channel":     r.record.channel,
                "timestamp":   r.record.timestamp,
                "lineage_id":  r.record.lineage_id,
            }
            for r in embed_results   # include anomalous records in MongoDB
        ]
        counts["mongo"] = mongo.bulk_insert_conversations(mongo_docs)

    # ── 2. Milvus ─────────────────────────────────────────────
    if clean:
        with log_duration(logger, "store.milvus"):
            milvus_client.insert_vectors(
                user_ids=    [r.record.user_id    for r in clean],
                message_ids= [r.record.message_id for r in clean],
                lineage_ids= [r.record.lineage_id for r in clean],
                vectors=     [r.vector            for r in clean],
            )
            counts["milvus"] = len(clean)

    # ── 3. Neo4j ──────────────────────────────────────────────
    with log_duration(logger, "store.neo4j"):
        neo4j_errors = 0
        for r in embed_results:
            try:
                neo4j_client.upsert_user_campaign_intent(
                    user_id=     r.record.user_id,
                    campaign_id= r.record.campaign_id,
                    intent=      r.record.intent,
                    lineage_id=  r.record.lineage_id,
                )
                counts["neo4j"] += 1
            except Exception as exc:
                neo4j_errors += 1
                logger.error(
                    f"Neo4j upsert failed | user={r.record.user_id} | {exc}"
                )
        if neo4j_errors:
            logger.warning(f"Neo4j errors: {neo4j_errors}/{len(embed_results)}")

    # ── 4. SQLite ─────────────────────────────────────────────
    with log_duration(logger, "store.sqlite"):
        for r in embed_results:
            try:
                sqlite_client.upsert_interaction(
                    user_id=     r.record.user_id,
                    campaign_id= r.record.campaign_id,
                    channel=     r.record.channel,
                    timestamp=   r.record.timestamp,
                )
                counts["sqlite"] += 1
            except Exception as exc:
                logger.error(
                    f"SQLite upsert failed | user={r.record.user_id} | {exc}"
                )

    # ── 5. Redis cache invalidation ───────────────────────────
    # New interactions mean cached recommendations are stale
    unique_users = {r.record.user_id for r in embed_results}
    for uid in unique_users:
        redis_client.invalidate_user(uid)

    logger.info(
        f"Store complete | "
        f"mongo={counts['mongo']} milvus={counts['milvus']} "
        f"neo4j={counts['neo4j']} sqlite={counts['sqlite']}"
    )
    return counts