"""
Marketing Personalisation Pipeline — Airflow DAG
=================================================
Orchestrates the full ETL pipeline:
  bootstrap → load → validate → embed → store

Schedule: every 10 minutes (configurable)
Owner: data-engineering

Each task is a pure PythonOperator wrapping the same functions
used in the standalone run_pipeline.py — zero logic duplication.
XCom is used to pass record counts between tasks for the
final summary log.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator

# ── Default args applied to every task ───────────────────────
default_args = {
    "owner":            "data-engineering",
    "retries":          3,
    "retry_delay":      timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "email_on_failure": False,   # set to True + add email in production
    "email_on_retry":   False,
}

DATA_FILE = "data/sample_conversations.json"


# ══════════════════════════════════════════════════════════════
#  Task functions
#  Each function is completely self-contained so Airflow can
#  pickle and distribute them to any worker.
# ══════════════════════════════════════════════════════════════

def task_bootstrap(**context) -> None:
    """
    Ensure all DB schemas and constraints exist.
    Runs on every DAG execution — safe because all operations
    use CREATE IF NOT EXISTS / MERGE semantics.
    """
    from src.db import sqlite_client, neo4j_client, milvus_client
    sqlite_client.ensure_schema()
    neo4j_client.ensure_constraints()
    milvus_client.get_collection()


def task_load(**context) -> None:
    """
    Load raw records from the data file.
    Pushes the raw records list to XCom for downstream tasks.
    """
    import json
    from pathlib import Path

    data_file = context["params"].get("data_file", DATA_FILE)
    with open(Path(data_file)) as f:
        raw_records = json.load(f)

    # Push to XCom so next task can pull it
    context["ti"].xcom_push(key="raw_records", value=raw_records)
    print(f"Loaded {len(raw_records)} records from {data_file}")


def task_validate(**context) -> bool:
    """
    Validate raw records against the Pydantic schema.
    Routes invalid records to MongoDB DLQ.

    Returns False if zero valid records (ShortCircuitOperator
    will skip all downstream tasks gracefully).
    """
    from src.pipeline.ingest import validate_batch
    from src.db import mongo

    raw_records = context["ti"].xcom_pull(
        task_ids="load_data", key="raw_records"
    )

    valid, invalid = validate_batch(raw_records)

    # Route invalid to DLQ
    for raw, error in invalid:
        mongo.insert_to_dlq(raw, error)

    # Push valid records for downstream
    # Note: XCom serialises to JSON — convert Pydantic models to dicts
    context["ti"].xcom_push(
        key="valid_records",
        value=[r.model_dump() for r in valid],
    )
    context["ti"].xcom_push(key="dlq_count", value=len(invalid))

    print(f"Validation: {len(valid)} valid, {len(invalid)} invalid")

    # ShortCircuitOperator: return False to skip downstream if no valid records
    return len(valid) > 0


def task_embed(**context) -> None:
    """
    Generate sentence embeddings for all valid records.
    Flags anomalous (near-zero) vectors for exclusion from Milvus.
    """
    from src.pipeline.ingest import ConversationRecord
    from src.pipeline.embed import embed_batch

    valid_dicts = context["ti"].xcom_pull(
        task_ids="validate_records", key="valid_records"
    )

    # Reconstruct Pydantic models from dicts
    records = [ConversationRecord(**d) for d in valid_dicts]
    embed_results = embed_batch(records)

    # Serialise for XCom — store vector + record + anomaly flag
    serialised = [
        {
            "record":  r.record.model_dump(),
            "vector":  r.vector,
            "anomaly": r.anomaly,
        }
        for r in embed_results
    ]

    context["ti"].xcom_push(key="embed_results", value=serialised)
    anomaly_count = sum(1 for r in embed_results if r.anomaly)
    print(f"Embedded {len(embed_results)} records | anomalies={anomaly_count}")


def task_store(**context) -> None:
    """
    Fan-out writes to all four stores:
    MongoDB, Milvus, Neo4j, SQLite.
    Invalidates Redis cache for affected users.
    """
    from src.pipeline.ingest import ConversationRecord
    from src.pipeline.embed import EmbedResult
    from src.pipeline.store import store_batch

    serialised = context["ti"].xcom_pull(
        task_ids="generate_embeddings", key="embed_results"
    )

    # Reconstruct EmbedResult objects
    embed_results = [
        EmbedResult(
            record=ConversationRecord(**item["record"]),
            vector=item["vector"],
            anomaly=item["anomaly"],
        )
        for item in serialised
    ]

    counts = store_batch(embed_results)
    context["ti"].xcom_push(key="store_counts", value=counts)
    print(f"Store complete: {counts}")


def task_summarise(**context) -> None:
    """
    Log a final summary of the pipeline run.
    Pulls XCom values from all upstream tasks.
    """
    import uuid
    from datetime import timezone

    store_counts = context["ti"].xcom_pull(
        task_ids="store_to_databases", key="store_counts"
    ) or {}
    dlq_count = context["ti"].xcom_pull(
        task_ids="validate_records", key="dlq_count"
    ) or 0

    print(
        f"\n{'='*50}\n"
        f"Pipeline Run Summary\n"
        f"{'='*50}\n"
        f"DAG run ID  : {context['run_id']}\n"
        f"Execution   : {context['execution_date']}\n"
        f"MongoDB     : {store_counts.get('mongo', 0)} records\n"
        f"Milvus      : {store_counts.get('milvus', 0)} vectors\n"
        f"Neo4j       : {store_counts.get('neo4j', 0)} edges\n"
        f"SQLite      : {store_counts.get('sqlite', 0)} rows\n"
        f"DLQ         : {dlq_count} records\n"
        f"{'='*50}"
    )


# ══════════════════════════════════════════════════════════════
#  DAG definition
# ══════════════════════════════════════════════════════════════

with DAG(
    dag_id="marketing_personalisation_pipeline",
    description="Ingest conversations → embed → store in polyglot DB stack",
    default_args=default_args,
    schedule_interval=timedelta(minutes=10),
    start_date=datetime(2024, 1, 1),
    catchup=False,              # don't backfill missed runs on first deploy
    max_active_runs=1,          # prevent overlapping runs
    tags=["marketing", "nlp", "embeddings", "personalisation"],
    params={"data_file": DATA_FILE},
) as dag:

    # ── Task 1: Bootstrap ─────────────────────────────────────
    bootstrap = PythonOperator(
        task_id="bootstrap_schemas",
        python_callable=task_bootstrap,
    )

    # ── Task 2: Load ──────────────────────────────────────────
    load = PythonOperator(
        task_id="load_data",
        python_callable=task_load,
    )

    # ── Task 3: Validate (ShortCircuit) ──────────────────────
    # If validate returns False (zero valid records),
    # all downstream tasks are skipped gracefully — no failure.
    validate = ShortCircuitOperator(
        task_id="validate_records",
        python_callable=task_validate,
    )

    # ── Task 4: Embed ─────────────────────────────────────────
    embed = PythonOperator(
        task_id="generate_embeddings",
        python_callable=task_embed,
        execution_timeout=timedelta(minutes=15),  # embedding can be slow on CPU
    )

    # ── Task 5: Store ─────────────────────────────────────────
    store = PythonOperator(
        task_id="store_to_databases",
        python_callable=task_store,
    )

    # ── Task 6: Summarise ─────────────────────────────────────
    summarise = PythonOperator(
        task_id="log_run_summary",
        python_callable=task_summarise,
        trigger_rule="all_done",   # runs even if upstream tasks were skipped
    )

    # ── DAG dependency chain ──────────────────────────────────
    #
    #  bootstrap → load → validate → embed → store → summarise
    #                         │
    #                         └── (if no valid records, short-circuits here)
    #                             embed, store, summarise are skipped
    #
    bootstrap >> load >> validate >> embed >> store >> summarise