from __future__ import annotations

import time
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from src.db import milvus_client, neo4j_client, sqlite_client, redis_client
from src.db.neo4j_client import ensure_constraints
from src.db.milvus_client import get_collection
from src.db.sqlite_client import ensure_schema
from src.pipeline.embed import embed_single
from src.utils.logger import get_logger

logger = get_logger("api.recommendations")


# ── Lifespan: runs once on startup / shutdown ─────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("API startup — initialising DB connections")
    ensure_schema()
    ensure_constraints()
    get_collection()          # warms up Milvus collection
    logger.info("API ready")
    yield
    logger.info("API shutdown — closing connections")
    milvus_client.close()
    neo4j_client.close()
    redis_client.close()


# ── App ───────────────────────────────────────────────────────

app = FastAPI(
    title="Marketing Personalisation API",
    description="Hybrid vector + graph + analytics recommendation engine",
    version="1.0.0",
    lifespan=lifespan,
)


# ── Response schemas ──────────────────────────────────────────

class CampaignResult(BaseModel):
    campaign_id:       str
    engagement_score:  int
    graph_weight:      int
    rank:              int


class RecommendationResponse(BaseModel):
    user_id:            str
    recommendations:    list[CampaignResult]
    similar_user_ids:   list[str]
    cache_hit:          bool
    latency_ms:         dict[str, float]
    total_ms:           float


# ── Health check ──────────────────────────────────────────────

@app.get("/health", tags=["ops"])
def health() -> dict[str, str]:
    return {"status": "ok"}


# ── Main endpoint ─────────────────────────────────────────────

@app.get(
    "/recommendations/{user_id}",
    response_model=RecommendationResponse,
    tags=["recommendations"],
    summary="Get top campaign recommendations for a user",
)
def get_recommendations(user_id: str, top_k: int = 5) -> RecommendationResponse:
    """
    Three-stage hybrid retrieval:

    1. **Milvus**  — find top-k users with the most similar embedding to `user_id`
    2. **Neo4j**   — fetch campaigns those similar users have engaged with (graph traversal)
    3. **SQLite**  — rank the campaigns by total engagement frequency

    Results are cached in Redis for 5 minutes (TTL configurable via .env).
    """
    t_total_start = time.perf_counter()
    latency: dict[str, float] = {}

    # ── 0. Redis cache check ──────────────────────────────────
    cached = redis_client.get_cached_recommendations(user_id)
    if cached:
        total_ms = (time.perf_counter() - t_total_start) * 1000
        logger.info(f"Cache HIT | user_id={user_id} | total={total_ms:.1f}ms")
        return RecommendationResponse(
            user_id=user_id,
            recommendations=[CampaignResult(**c) for c in cached["recommendations"]],
            similar_user_ids=cached["similar_user_ids"],
            cache_hit=True,
            latency_ms={"redis": total_ms},
            total_ms=total_ms,
        )

    # ── 1. Embed the user_id as a query ──────────────────────
    # Strategy: embed the user_id string itself as a lightweight proxy.
    # In production this would be the user's latest message embedding
    # retrieved from MongoDB, but for the prototype this is sufficient.
    t0 = time.perf_counter()
    try:
        query_vector = embed_single(f"user profile for {user_id}")
    except Exception as exc:
        logger.error(f"Embedding failed | user_id={user_id} | {exc}")
        raise HTTPException(status_code=500, detail="Embedding service unavailable")
    latency["embed_ms"] = (time.perf_counter() - t0) * 1000

    # ── 2. Milvus ANN — find similar users ───────────────────
    t0 = time.perf_counter()
    try:
        similar_users = milvus_client.search_similar_users(query_vector, top_k=top_k)
    except Exception as exc:
        logger.error(f"Milvus search failed | user_id={user_id} | {exc}")
        raise HTTPException(status_code=500, detail="Vector search unavailable")
    latency["milvus_ms"] = (time.perf_counter() - t0) * 1000

    if not similar_users:
        logger.warning(f"No similar users found | user_id={user_id}")
        return RecommendationResponse(
            user_id=user_id,
            recommendations=[],
            similar_user_ids=[],
            cache_hit=False,
            latency_ms=latency,
            total_ms=(time.perf_counter() - t_total_start) * 1000,
        )

    logger.info(
        f"Milvus returned {len(similar_users)} similar users "
        f"for user_id={user_id} in {latency['milvus_ms']:.1f}ms"
    )

    # ── 3. Neo4j — get campaigns for similar users ────────────
    t0 = time.perf_counter()
    try:
        graph_campaigns = neo4j_client.get_campaigns_for_users(similar_users)
    except Exception as exc:
        logger.error(f"Neo4j query failed | user_id={user_id} | {exc}")
        raise HTTPException(status_code=500, detail="Graph DB unavailable")
    latency["neo4j_ms"] = (time.perf_counter() - t0) * 1000

    if not graph_campaigns:
        logger.warning(f"No campaigns found in graph | user_id={user_id}")
        return RecommendationResponse(
            user_id=user_id,
            recommendations=[],
            similar_user_ids=similar_users,
            cache_hit=False,
            latency_ms=latency,
            total_ms=(time.perf_counter() - t_total_start) * 1000,
        )

    # Build a lookup: campaign_id → graph weight
    graph_weight_map: dict[str, int] = {
        c["campaign_id"]: c["weight"] for c in graph_campaigns
    }
    campaign_ids = list(graph_weight_map.keys())

    # ── 4. SQLite — rank by engagement frequency ──────────────
    t0 = time.perf_counter()
    try:
        engagement_scores = sqlite_client.get_engagement_scores(campaign_ids)
    except Exception as exc:
        logger.error(f"SQLite query failed | user_id={user_id} | {exc}")
        # Non-fatal — fall back to graph weights only
        engagement_scores = {}
        logger.warning("Falling back to graph weights only for ranking")
    latency["sqlite_ms"] = (time.perf_counter() - t0) * 1000

    # ── 5. Merge + rank ───────────────────────────────────────
    # Final score = engagement_score (SQLite) + graph_weight (Neo4j)
    # This blends historical frequency with social graph proximity.
    scored: list[dict[str, Any]] = []
    for campaign_id, graph_weight in graph_weight_map.items():
        eng_score = engagement_scores.get(campaign_id, 0)
        scored.append({
            "campaign_id":      campaign_id,
            "engagement_score": eng_score,
            "graph_weight":     graph_weight,
            "total_score":      eng_score + graph_weight,
        })

    scored.sort(key=lambda x: x["total_score"], reverse=True)
    top_campaigns = scored[:top_k]

    recommendations = [
        CampaignResult(
            campaign_id=c["campaign_id"],
            engagement_score=c["engagement_score"],
            graph_weight=c["graph_weight"],
            rank=i + 1,
        )
        for i, c in enumerate(top_campaigns)
    ]

    total_ms = (time.perf_counter() - t_total_start) * 1000
    latency["total_ms"] = total_ms

    logger.info(
        f"Recommendations built | user_id={user_id} "
        f"campaigns={len(recommendations)} total={total_ms:.1f}ms | "
        f"embed={latency['embed_ms']:.0f}ms "
        f"milvus={latency['milvus_ms']:.0f}ms "
        f"neo4j={latency['neo4j_ms']:.0f}ms "
        f"sqlite={latency['sqlite_ms']:.0f}ms"
    )

    # ── 6. Write to Redis cache ───────────────────────────────
    cache_payload = {
        "recommendations": [r.model_dump() for r in recommendations],
        "similar_user_ids": similar_users,
    }
    redis_client.cache_recommendations(user_id, cache_payload)

    return RecommendationResponse(
        user_id=user_id,
        recommendations=recommendations,
        similar_user_ids=similar_users,
        cache_hit=False,
        latency_ms=latency,
        total_ms=total_ms,
    )