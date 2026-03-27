# Marketing Personalisation Platform — V2.0

A production-grade, multi-database data platform for AI-driven marketing personalisation.
Ingests conversation data in real time, generates semantic embeddings, maps user-campaign
relationships in a graph, and serves hybrid recommendations via a low-latency REST API.

---

## Architecture at a Glance

```
User Message
    │
    ▼
┌─────────────────────────────────────────────────────────┐
│  Pipeline  (validate → embed → fan-out)                 │
│                                                         │
│  MongoDB  ──── raw text + metadata (source of truth)    │
│  Milvus   ──── 768-dim sentence embeddings (ANN index)  │
│  Neo4j    ──── User → Campaign → Intent graph           │
│  SQLite   ──── aggregated engagement metrics            │
│  Redis    ──── session + recommendation cache (TTL 5m)  │
└─────────────────────────────────────────────────────────┘
    │
    ▼
GET /recommendations/<user_id>
    ├── Milvus  → top-5 similar users (vector ANN)
    ├── Neo4j   → campaigns those users engaged with
    └── SQLite  → rank by engagement frequency
```

Full architecture diagram and trade-off justification: [`architecture.md`](./architecture.md)
Scaling plan for 10M+ users: [`scaling_plan.md`](./scaling_plan.md)

---

## Tech Stack

| Layer | Technology | Role |
|---|---|---|
| Document store | MongoDB 7 | Raw conversations, dead-letter queue |
| Vector DB | Milvus 2.4 | Embedding storage + ANN search |
| Graph DB | Neo4j 5 | User–Campaign–Intent relationships |
| Analytics DB | SQLite | Aggregated interaction metrics |
| Cache | Redis 7 | Session + recommendation cache |
| Embeddings | sentence-transformers (`all-mpnet-base-v2`) | 768-dim semantic vectors |
| API | FastAPI + Uvicorn | Recommendation serving |
| Orchestration | Python DAG | ETL pipeline |
| Infra | Docker Compose | Multi-service local environment |

---

## Prerequisites

| Tool | Version |
|---|---|
| Docker Desktop | 20+ |
| Docker Compose | v2+ |
| Python | 3.10+ |

---

## Quick Start

### 1. Clone and enter the project

```bash
git clone <your-repo-url>
cd name_to_be_decided
```

### 2. Start all infrastructure services

```bash
docker compose pull        # download images (~4 GB, one-time)
docker compose up -d       # start all 6 services in background
```

Wait ~60 seconds for all services to become healthy:

```bash
docker compose ps          # all should show "healthy"
```

### 3. Set up Python environment

```bash
python -m venv .venv

# Activate
source .venv/bin/activate        # Mac / Linux
.venv\Scripts\activate           # Windows

# Install dependencies
pip install -r requirements.txt
```

### 4. Verify connectivity

```bash
python test_connections.py
# Expected: MongoDB ✓  Redis ✓  Neo4j ✓  Milvus ✓
```

### 5. Run the pipeline

```bash
python -u -m src.pipeline.run_pipeline data/sample_conversations.json
```

The first run downloads the embedding model (~420 MB, cached after that).
Expected output:

```
Pipeline run complete | total=30 ok=28 dlq=2 status=partial
```

The 2 DLQ records are intentional — the sample data includes one blank message
and one user_id with spaces to demonstrate schema validation.

### 6. Start the API

```bash
python -u -m uvicorn src.api.main:app --host 0.0.0.0 --port 8000 --reload
```

### 7. Hit the endpoint

```bash
# Get recommendations (cache miss — hits all 3 DBs)
curl http://localhost:8000/recommendations/u001

# Hit again — served from Redis in <5ms
curl http://localhost:8000/recommendations/u001

# Interactive API docs
open http://localhost:8000/docs
```

---

## Project Structure

```
.
├── src/
│   ├── utils/
│   │   ├── config.py          # central env-var loader
│   │   └── logger.py          # structured logger with timing context manager
│   ├── db/
│   │   ├── mongo.py           # MongoDB client + bulk write helpers
│   │   ├── redis_client.py    # Redis cache (session + recommendations)
│   │   ├── neo4j_client.py    # Graph upsert + Cypher query helpers
│   │   ├── milvus_client.py   # Collection setup, vector insert, ANN search
│   │   └── sqlite_client.py   # Schema setup, interaction upserts, scoring
│   ├── pipeline/
│   │   ├── ingest.py          # Pydantic validation + lineage tagging
│   │   ├── embed.py           # Sentence-Transformer inference + anomaly detection
│   │   ├── store.py           # Fan-out writes to all 4 stores
│   │   └── run_pipeline.py    # DAG orchestrator — entry point
│   └── api/
│       └── main.py            # FastAPI app — /recommendations/<user_id>
│
├── data/
│   ├── sample_conversations.json   # 30 synthetic records (28 valid, 2 invalid)
│   └── analytics.db                # SQLite file (auto-created on first run)
│
├── docker-compose.yml
├── .env                        # credentials (not committed)
├── requirements.txt
├── test_connections.py
├── architecture_diagram.md
├── architecture.md
└── scaling_plan.md
```

---

## Environment Variables

All config lives in `.env`. Defaults work out of the box with Docker Compose.

| Variable | Default | Description |
|---|---|---|
| `MONGO_URI` | `mongodb://admin:admin123@localhost:27017` | MongoDB connection string |
| `MONGO_DB` | `conversations` | Database name |
| `REDIS_HOST` | `localhost` | Redis host |
| `REDIS_PASSWORD` | `redis123` | Redis auth password |
| `REDIS_TTL_SECONDS` | `300` | Cache TTL (5 minutes) |
| `NEO4J_URI` | `bolt://localhost:7687` | Neo4j Bolt URI |
| `NEO4J_PASSWORD` | `neo4j123` | Neo4j password |
| `MILVUS_HOST` | `localhost` | Milvus host |
| `MILVUS_DIM` | `768` | Embedding dimension (must match model) |
| `SQLITE_PATH` | `./data/analytics.db` | SQLite file path |
| `EMBEDDING_MODEL` | `all-mpnet-base-v2` | Sentence-Transformers model name |
| `PIPELINE_BATCH_SIZE` | `32` | Records per pipeline batch |
| `LOG_LEVEL` | `INFO` | Logging verbosity |

---

## API Reference

### `GET /recommendations/{user_id}`

Returns top campaign recommendations for a user via three-stage hybrid retrieval.

**Parameters**

| Name | Type | Default | Description |
|---|---|---|---|
| `user_id` | path | required | The user to get recommendations for |
| `top_k` | query | `5` | Number of results to return |

**Example response**

```json
{
  "user_id": "u001",
  "recommendations": [
    {
      "campaign_id": "camp_tech_edu",
      "engagement_score": 4,
      "graph_weight": 4,
      "rank": 1
    }
  ],
  "similar_user_ids": ["u016", "u013", "u004", "u002", "u010"],
  "cache_hit": false,
  "latency_ms": {
    "embed_ms": 45.2,
    "milvus_ms": 18.4,
    "neo4j_ms": 31.7,
    "sqlite_ms": 2.1,
    "total_ms": 98.4
  },
  "total_ms": 98.4
}
```

**Latency profile**

| Path | Typical P50 |
|---|---|
| Cache hit (Redis) | ~2 ms |
| Cache miss (full retrieval) | ~100–200 ms |

### `GET /health`

Returns `{"status": "ok"}` when the API is running.

---

## Observability

Every pipeline run and API request emits structured logs:

```
2026-03-27 13:53:57 | INFO  | pipeline.store | Store complete | mongo=28 milvus=28 neo4j=28 sqlite=28
2026-03-27 13:53:58 | INFO  | pipeline.run   | Pipeline run complete | total=30 ok=28 dlq=2 status=partial
```

Pipeline run history is persisted to `pipeline_runs` table in SQLite:

```bash
python -c "
import sqlite3
conn = sqlite3.connect('data/analytics.db')
conn.row_factory = sqlite3.Row
for r in conn.execute('SELECT * FROM pipeline_runs').fetchall():
    print(dict(r))
"
```

Anomaly detection fires warnings on:
- Empty / near-zero norm embeddings
- Schema validation failures (routed to MongoDB dead-letter queue)
- Neo4j upsert failures (logged with lineage_id for tracing)

---

## Useful Commands

```bash
# Stop all services (keeps data)
docker compose stop

# Wipe everything and start fresh
docker compose down -v && docker compose up -d

# View logs for a specific service
docker compose logs -f neo4j

# Neo4j browser (graph visualisation)
open http://localhost:7474          # login: neo4j / neo4j123

# MinIO console (Milvus object storage)
open http://localhost:9001          # login: minioadmin / minioadmin123

# Re-run pipeline with fresh data
python -u -m src.pipeline.run_pipeline data/sample_conversations.json
```

---

## Design Choices — Summary

**Why polyglot storage?** No single database handles semantic search, graph traversal,
document flexibility, and analytical aggregation equally well. Each store is chosen for
exactly what it does best and nothing else.

**Why Python DAG over Airflow?** At prototype scale, a plain Python DAG is simpler to
run, easier to debug, and zero-infra. Every task function is a pure Python callable
that can be lifted into an Airflow `PythonOperator` with no code changes — only the
scheduler wrapper changes.

**Why `all-mpnet-base-v2`?** It produces high-quality 768-dim embeddings and runs
entirely on CPU with no GPU required, making it the right choice for a portable
prototype. At production scale, replace with a GPU-hosted model server
(e.g., HuggingFace Inference Endpoints) with no changes to the pipeline interface.

**Why Redis TTL caching?** The recommendation endpoint makes 3 database round-trips
on a cache miss. For active users queried repeatedly within a session, Redis reduces
P99 from ~200ms to ~2ms. A 5-minute TTL balances freshness vs. latency.

Full justification of every trade-off: [`architecture.md`](./architecture.md)