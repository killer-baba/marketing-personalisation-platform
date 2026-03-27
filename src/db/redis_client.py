from __future__ import annotations

import json
from typing import Any

import redis

from src.utils.config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD, REDIS_TTL_SECONDS
from src.utils.logger import get_logger

logger = get_logger("db.redis")

# ── Singleton client ──────────────────────────────────────────
_client: redis.Redis | None = None


def get_client() -> redis.Redis:
    global _client
    if _client is None:
        _client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            password=REDIS_PASSWORD,
            decode_responses=True,   # always return str, not bytes
        )
        _client.ping()              # fail fast if Redis is down
        logger.info("Redis client initialised")
    return _client


# ── Key helpers ───────────────────────────────────────────────

def _session_key(user_id: str) -> str:
    return f"session:{user_id}"


def _reco_key(user_id: str) -> str:
    return f"recommendations:{user_id}"


# ── Session cache ─────────────────────────────────────────────

def cache_session(user_id: str, session_data: dict[str, Any]) -> None:
    """Cache the recent session context for a user."""
    try:
        r = get_client()
        r.setex(_session_key(user_id), REDIS_TTL_SECONDS, json.dumps(session_data))
        logger.debug(f"Session cached | user_id={user_id}")
    except redis.RedisError as exc:
        logger.warning(f"Session cache write failed | user_id={user_id} | {exc}")


def get_session(user_id: str) -> dict[str, Any] | None:
    """Retrieve cached session, returns None on miss."""
    try:
        r = get_client()
        raw = r.get(_session_key(user_id))
        if raw:
            logger.debug(f"Session cache HIT | user_id={user_id}")
            return json.loads(raw)
        logger.debug(f"Session cache MISS | user_id={user_id}")
        return None
    except redis.RedisError as exc:
        logger.warning(f"Session cache read failed | user_id={user_id} | {exc}")
        return None


# ── Recommendation cache ──────────────────────────────────────

def cache_recommendations(user_id: str, recommendations: list[dict]) -> None:
    """Cache the ranked recommendations list for a user."""
    try:
        r = get_client()
        r.setex(_reco_key(user_id), REDIS_TTL_SECONDS, json.dumps(recommendations))
        logger.debug(f"Reco cache SET | user_id={user_id}")
    except redis.RedisError as exc:
        logger.warning(f"Reco cache write failed | user_id={user_id} | {exc}")


def get_cached_recommendations(user_id: str) -> list[dict] | None:
    """Return cached recommendations or None on cache miss."""
    try:
        r = get_client()
        raw = r.get(_reco_key(user_id))
        if raw:
            logger.info(f"Reco cache HIT | user_id={user_id}")
            return json.loads(raw)
        logger.info(f"Reco cache MISS | user_id={user_id}")
        return None
    except redis.RedisError as exc:
        logger.warning(f"Reco cache read failed | user_id={user_id} | {exc}")
        return None


def invalidate_user(user_id: str) -> None:
    """Remove both session and reco cache entries for a user."""
    try:
        r = get_client()
        r.delete(_session_key(user_id), _reco_key(user_id))
        logger.debug(f"Cache invalidated | user_id={user_id}")
    except redis.RedisError as exc:
        logger.warning(f"Cache invalidation failed | user_id={user_id} | {exc}")


def close() -> None:
    global _client
    if _client:
        _client.close()
        _client = None
        logger.info("Redis connection closed")