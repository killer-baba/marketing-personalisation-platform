# Architecture Diagram — Scalable Multi-Database Data Platform

## System Overview

```mermaid
flowchart TD
    %% ── Ingestion ──────────────────────────────────────────────
    A([🗣️ User Conversation\nuser_id · message · timestamp])

    %% ── Orchestration wrapper ──────────────────────────────────
    subgraph ORCH["⚙️  Pipeline Orchestrator  (Python DAG / Airflow)"]
        direction TB

        B["📥 Ingestion Layer\nSchema validation · Lineage tagging"]
        C["🧠 Embedding Service\nintfloat/e5-large-v2\n1024-dim vectors"]

        B --> C
    end

    A --> B

    %% ── Storage fan-out ────────────────────────────────────────
    subgraph STORES["🗄️  Polyglot Storage Layer"]
        direction LR

        M["🍃 MongoDB\nRaw text + metadata\n(document store)"]
        V["⚡ Milvus\n1024-dim embeddings\n+ user_id / message_id"]
        N["🕸️  Neo4j\nUser → Campaign → Intent\ngraph relationships"]
        S["📊 SQLite / BigQuery\nAggregated metrics\nbatch analytics"]
    end

    C -->|"text + meta"| M
    C -->|"1024-dim vector"| V
    C -->|"relationship upsert"| N
    C -->|"interaction event"| S

    %% ── Cache ──────────────────────────────────────────────────
    R["⚡ Redis\nRecent session cache\nTTL-keyed by user_id"]

    M -->|"hot sessions → cache"| R

    %% ── Serving API ────────────────────────────────────────────
    subgraph API["🚀  FastAPI — Hybrid Retrieval"]
        direction TB
        E1["Vector search\nTop-5 similar users\n(Milvus ANN)"]
        E2["Graph traversal\nCampaigns for similar users\n(Neo4j Cypher)"]
        E3["Rank by engagement\nfrequency from SQLite"]
        E1 --> E2 --> E3
    end

    R  -->|"cache hit → skip DB"| API
    V  -->|"ANN query"| E1
    N  -->|"Cypher query"| E2
    S  -->|"engagement scores"| E3

    %% ── Observability ──────────────────────────────────────────
    subgraph OBS["🔍  Observability"]
        direction LR
        L["Structured Logging\nPipeline status · latency"]
        AN["Anomaly Detection\nEmpty embeddings · missing\nrelationships · data drift"]
        DASH["📈 Dashboard\nStreamlit — pipeline metrics\n+ live recommendation demo"]
        L --- AN --- DASH
    end

    ORCH --> OBS
    API  --> OBS

    %% ── Client ─────────────────────────────────────────────────
    F([🌐 Client\nGET /recommendations/user_id])
    F --> API

    %% ── Styling ────────────────────────────────────────────────
    classDef source  fill:#E1F5EE,stroke:#0F6E56,color:#085041
    classDef store   fill:#E6F1FB,stroke:#185FA5,color:#0C447C
    classDef cache   fill:#FAEEDA,stroke:#854F0B,color:#633806
    classDef api     fill:#EEEDFE,stroke:#534AB7,color:#3C3489
    classDef obs     fill:#F1EFE8,stroke:#5F5E5A,color:#444441
    classDef client  fill:#FAECE7,stroke:#993C1D,color:#712B13

    class A source
    class M,V,N,S store
    class R cache
    class API,E1,E2,E3 api
    class OBS,L,AN,DASH obs
    class F client
```

---

## Data Flow Summary

### Real-Time Path (per message, P99 < 500 ms)

```
User message
  → Ingestion (validate + tag lineage)
  → Embedding (intfloat/e5-large-v2 — 1024-dim)
  → MongoDB  (store raw text + metadata)
  → Milvus   (upsert 1024-dim vector)
  → Neo4j    (upsert user–campaign–intent edges)
  → Redis    (cache session for active user)
```

### Batch Path (scheduled, every N minutes)

```
MongoDB change-stream / polling
  → Aggregate interaction counts per user/campaign
  → Upsert aggregated rows → SQLite / BigQuery
  → Refresh Redis TTL for top-active users
```

### Query Path (API, target P99 < 200 ms)

```
GET /recommendations/<user_id>
  → Redis  → cache hit?  → return immediately (~2ms)
  → Milvus → ANN top-5 similar user embeddings
  → Neo4j  → campaigns connected to those 5 users
  → SQLite → rank campaigns by engagement frequency
  → merge + return JSON response
  → write result to Redis (TTL = 5 min)
```

---

## Component Interaction Matrix

| From \ To       | MongoDB | Milvus | Neo4j | SQLite | Redis | FastAPI |
|----------------|---------|--------|-------|--------|-------|---------|
| **Pipeline**    | write   | write  | write | write  | write | —       |
| **Redis**       | read    | —      | —     | —      | —     | serve   |
| **FastAPI**     | —       | read   | read  | read   | r/w   | —       |
| **Observability** | read  | —      | —     | read   | read  | read    |

---

## Scaling & Fault-Tolerance

```mermaid
flowchart LR
    LB["Load Balancer\n(nginx / ALB)"]

    subgraph API_POOL["FastAPI Workers  (horizontal scale)"]
        W1["Worker 1"]
        W2["Worker 2"]
        W3["Worker N"]
    end

    subgraph DATA["Stateful Services  (vertical + sharding)"]
        MIL["Milvus Cluster\nHNSW index\nsharded collections"]
        NEO["Neo4j Cluster\nread replicas"]
        MON["MongoDB ReplicaSet\n3-node"]
        RD["Redis Cluster\nsentinel HA"]
        BQ["BigQuery\n(cloud, fully managed)"]
    end

    LB --> W1 & W2 & W3
    W1 & W2 & W3 --> MIL & NEO & MON & RD & BQ
```