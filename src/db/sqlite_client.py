from __future__ import annotations

import sqlite3
from pathlib import Path

from src.utils.config import SQLITE_PATH
from src.utils.logger import get_logger

logger = get_logger("db.sqlite")


def _get_conn() -> sqlite3.Connection:
    Path(SQLITE_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(SQLITE_PATH)
    conn.row_factory = sqlite3.Row   # rows behave like dicts
    return conn


# ── Schema setup ──────────────────────────────────────────────

def ensure_schema() -> None:
    """
    Create tables on first run. Safe to call repeatedly.
    """
    ddl = """
    CREATE TABLE IF NOT EXISTS user_interaction_summary (
        user_id            TEXT    NOT NULL,
        campaign_id        TEXT    NOT NULL,
        interaction_count  INTEGER NOT NULL DEFAULT 1,
        last_seen          TEXT    NOT NULL,
        channel            TEXT,
        PRIMARY KEY (user_id, campaign_id)
    );

    CREATE TABLE IF NOT EXISTS pipeline_runs (
        run_id        TEXT    PRIMARY KEY,
        started_at    TEXT    NOT NULL,
        finished_at   TEXT,
        records_in    INTEGER DEFAULT 0,
        records_ok    INTEGER DEFAULT 0,
        records_dlq   INTEGER DEFAULT 0,
        status        TEXT    DEFAULT 'running'
    );
    """
    with _get_conn() as conn:
        conn.executescript(ddl)
    logger.info("SQLite schema verified")


# ── Write helpers ─────────────────────────────────────────────

def upsert_interaction(
    user_id:     str,
    campaign_id: str,
    channel:     str,
    timestamp:   str,
) -> None:
    """
    Increment interaction_count for (user_id, campaign_id).
    Inserts a new row if this pair hasn't been seen before.
    """
    sql = """
    INSERT INTO user_interaction_summary
        (user_id, campaign_id, interaction_count, last_seen, channel)
    VALUES (?, ?, 1, ?, ?)
    ON CONFLICT(user_id, campaign_id)
    DO UPDATE SET
        interaction_count = interaction_count + 1,
        last_seen         = excluded.last_seen
    """
    with _get_conn() as conn:
        conn.execute(sql, (user_id, campaign_id, timestamp, channel))
    logger.debug(f"Interaction upserted | user={user_id} campaign={campaign_id}")


def get_engagement_scores(campaign_ids: list[str]) -> dict[str, int]:
    """
    Returns total interaction_count per campaign_id for the given list.
    Used by the API to rank campaigns.
    """
    if not campaign_ids:
        return {}
    placeholders = ",".join("?" * len(campaign_ids))
    sql = f"""
    SELECT campaign_id, SUM(interaction_count) AS total
    FROM user_interaction_summary
    WHERE campaign_id IN ({placeholders})
    GROUP BY campaign_id
    """
    with _get_conn() as conn:
        rows = conn.execute(sql, campaign_ids).fetchall()
    return {r["campaign_id"]: r["total"] for r in rows}


# ── Pipeline run tracking ─────────────────────────────────────

def start_pipeline_run(run_id: str, started_at: str) -> None:
    sql = """
    INSERT INTO pipeline_runs (run_id, started_at)
    VALUES (?, ?)
    """
    with _get_conn() as conn:
        conn.execute(sql, (run_id, started_at))


def finish_pipeline_run(
    run_id:      str,
    finished_at: str,
    records_in:  int,
    records_ok:  int,
    records_dlq: int,
    status:      str,
) -> None:
    sql = """
    UPDATE pipeline_runs
    SET finished_at = ?, records_in = ?, records_ok = ?,
        records_dlq = ?, status = ?
    WHERE run_id = ?
    """
    with _get_conn() as conn:
        conn.execute(
            sql, (finished_at, records_in, records_ok, records_dlq, status, run_id)
        )