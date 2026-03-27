from __future__ import annotations

from neo4j import GraphDatabase, Driver, Session
from neo4j.exceptions import Neo4jError

from src.utils.config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD
from src.utils.logger import get_logger

logger = get_logger("db.neo4j")

# ── Singleton driver ──────────────────────────────────────────
_driver: Driver | None = None


def get_driver() -> Driver:
    global _driver
    if _driver is None:
        _driver = GraphDatabase.driver(
            NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD)
        )
        _driver.verify_connectivity()
        logger.info("Neo4j driver initialised")
    return _driver


# ── Schema / constraints setup ────────────────────────────────

def ensure_constraints() -> None:
    """
    Create uniqueness constraints on first run.
    Safe to call repeatedly — Neo4j ignores if already exists.
    """
    constraints = [
        "CREATE CONSTRAINT IF NOT EXISTS FOR (u:User)     REQUIRE u.user_id    IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (c:Campaign) REQUIRE c.campaign_id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (i:Intent)   REQUIRE i.name        IS UNIQUE",
    ]
    with get_driver().session() as session:
        for cypher in constraints:
            session.run(cypher)
    logger.info("Neo4j constraints verified")


# ── Write helpers ─────────────────────────────────────────────

def upsert_user_campaign_intent(
    user_id: str,
    campaign_id: str,
    intent: str,
    lineage_id: str,
) -> None:
    """
    MERGE (create if not exists) the User, Campaign, and Intent nodes,
    then MERGE the relationships between them.
    The ENGAGED_WITH relationship carries a weight (interaction count)
    that increments on each call.
    """
    cypher = """
    MERGE (u:User {user_id: $user_id})
    MERGE (c:Campaign {campaign_id: $campaign_id})
    MERGE (i:Intent {name: $intent})

    MERGE (u)-[r:ENGAGED_WITH]->(c)
      ON CREATE SET r.weight = 1,
                    r.first_seen = timestamp(),
                    r.lineage_id = $lineage_id
      ON MATCH  SET r.weight = r.weight + 1,
                    r.last_seen = timestamp()

    MERGE (c)-[:TARGETS]->(i)
    MERGE (u)-[:EXPRESSED]->(i)
    """
    try:
        with get_driver().session() as session:
            session.run(
                cypher,
                user_id=user_id,
                campaign_id=campaign_id,
                intent=intent,
                lineage_id=lineage_id,
            )
        logger.debug(
            f"Graph upserted | user={user_id} campaign={campaign_id} intent={intent}"
        )
    except Neo4jError as exc:
        logger.error(f"Neo4j upsert failed | {exc}")
        raise


def get_campaigns_for_users(user_ids: list[str]) -> list[dict]:
    """
    Given a list of user_ids, return all campaigns those users have
    engaged with, ordered by total engagement weight descending.
    Used by the recommendation API.
    """
    cypher = """
    MATCH (u:User)-[r:ENGAGED_WITH]->(c:Campaign)
    WHERE u.user_id IN $user_ids
    RETURN c.campaign_id AS campaign_id,
           sum(r.weight)  AS total_weight
    ORDER BY total_weight DESC
    """
    try:
        with get_driver().session() as session:
            result = session.run(cypher, user_ids=user_ids)
            rows = [{"campaign_id": r["campaign_id"], "weight": r["total_weight"]}
                    for r in result]
        logger.debug(f"Graph query returned {len(rows)} campaigns for {len(user_ids)} users")
        return rows
    except Neo4jError as exc:
        logger.error(f"Neo4j query failed | {exc}")
        return []


def close() -> None:
    global _driver
    if _driver:
        _driver.close()
        _driver = None
        logger.info("Neo4j driver closed")