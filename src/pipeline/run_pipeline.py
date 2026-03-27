from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

from src.pipeline.ingest import validate_batch
from src.pipeline.embed  import embed_batch
from src.pipeline.store  import store_batch
from src.db import mongo, sqlite_client, neo4j_client, milvus_client
from src.utils.logger import get_logger, log_duration
from src.utils.config  import PIPELINE_BATCH_SIZE

logger = get_logger("pipeline.run")


# ── DB bootstrap ──────────────────────────────────────────────

def bootstrap() -> None:
    """Ensure all DB schemas and constraints exist before first run."""
    with log_duration(logger, "bootstrap"):
        sqlite_client.ensure_schema()
        neo4j_client.ensure_constraints()
        milvus_client.get_collection()    # creates collection + index if missing
    logger.info("Bootstrap complete — all schemas ready")


# ── Data loader ───────────────────────────────────────────────

def load_data(path: str) -> list[dict]:
    """Load raw records from a JSON file."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Data file not found: {path}")
    with open(p) as f:
        data = json.load(f)
    logger.info(f"Loaded {len(data)} records from {path}")
    return data


# ── Pipeline DAG ──────────────────────────────────────────────

def run(data_path: str) -> None:
    """
    Full pipeline DAG:
      load → validate → embed → store

    Processes records in PIPELINE_BATCH_SIZE chunks so memory stays bounded.
    """
    run_id     = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc).isoformat()

    logger.info(f"Pipeline run started | run_id={run_id}")
    sqlite_client.start_pipeline_run(run_id, started_at)

    # ── Stage 0: Load raw data ────────────────────────────────
    with log_duration(logger, "stage.load"):
        raw_records = load_data(data_path)

    records_in  = len(raw_records)
    records_ok  = 0
    records_dlq = 0

    # ── Process in batches ────────────────────────────────────
    for batch_start in range(0, records_in, PIPELINE_BATCH_SIZE):
        batch_raw = raw_records[batch_start : batch_start + PIPELINE_BATCH_SIZE]
        batch_num = batch_start // PIPELINE_BATCH_SIZE + 1
        logger.info(
            f"Processing batch {batch_num} | "
            f"records {batch_start+1}–{batch_start+len(batch_raw)}"
        )

        # ── Stage 1: Validate ─────────────────────────────────
        with log_duration(logger, f"stage.validate.batch{batch_num}"):
            valid, invalid = validate_batch(batch_raw)

        # Route invalid records to DLQ
        for raw, error in invalid:
            mongo.insert_to_dlq(raw, error)
            records_dlq += 1

        if not valid:
            logger.warning(f"Batch {batch_num} — no valid records, skipping")
            continue

        # ── Stage 2: Embed ────────────────────────────────────
        with log_duration(logger, f"stage.embed.batch{batch_num}"):
            embed_results = embed_batch(valid)

        # ── Stage 3: Store ────────────────────────────────────
        with log_duration(logger, f"stage.store.batch{batch_num}"):
            counts = store_batch(embed_results)

        records_ok += counts["mongo"]

    # ── Finish ────────────────────────────────────────────────
    finished_at = datetime.now(timezone.utc).isoformat()
    status      = "success" if records_dlq == 0 else "partial"

    sqlite_client.finish_pipeline_run(
        run_id=run_id,
        finished_at=finished_at,
        records_in=records_in,
        records_ok=records_ok,
        records_dlq=records_dlq,
        status=status,
    )

    logger.info(
        f"Pipeline run complete | run_id={run_id} "
        f"total={records_in} ok={records_ok} dlq={records_dlq} status={status}"
    )


# ── Entry point ───────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    data_file = sys.argv[1] if len(sys.argv) > 1 else "data/sample_conversations.json"
    bootstrap()
    run(data_file)