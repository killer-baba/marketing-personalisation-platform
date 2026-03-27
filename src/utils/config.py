import os
from dotenv import load_dotenv

load_dotenv()

# ── MongoDB ───────────────────────────────────────────────────
MONGO_URI                    = os.getenv("MONGO_URI", "mongodb://admin:admin123@localhost:27017")
MONGO_DB                     = os.getenv("MONGO_DB", "conversations")
MONGO_COLLECTION_CONVERSATIONS = os.getenv("MONGO_COLLECTION_CONVERSATIONS", "raw_conversations")
MONGO_COLLECTION_DLQ         = os.getenv("MONGO_COLLECTION_DLQ", "dead_letter_queue")

# ── Redis ─────────────────────────────────────────────────────
REDIS_HOST                   = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT                   = int(os.getenv("REDIS_PORT", 6379))
REDIS_PASSWORD               = os.getenv("REDIS_PASSWORD", "redis123")
REDIS_TTL_SECONDS            = int(os.getenv("REDIS_TTL_SECONDS", 300))

# ── Neo4j ─────────────────────────────────────────────────────
NEO4J_URI                    = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER                   = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD               = os.getenv("NEO4J_PASSWORD", "neo4j123")

# ── Milvus ────────────────────────────────────────────────────
MILVUS_HOST                  = os.getenv("MILVUS_HOST", "localhost")
MILVUS_PORT                  = int(os.getenv("MILVUS_PORT", 19530))
MILVUS_COLLECTION            = os.getenv("MILVUS_COLLECTION", "user_embeddings")
MILVUS_DIM                   = int(os.getenv("MILVUS_DIM", 768))

# ── SQLite ────────────────────────────────────────────────────
SQLITE_PATH                  = os.getenv("SQLITE_PATH", "./data/analytics.db")

# ── Embedding model ───────────────────────────────────────────
EMBEDDING_MODEL              = os.getenv("EMBEDDING_MODEL", "all-mpnet-base-v2")

# ── Pipeline ──────────────────────────────────────────────────
PIPELINE_BATCH_SIZE          = int(os.getenv("PIPELINE_BATCH_SIZE", 32))
LOG_LEVEL                    = os.getenv("LOG_LEVEL", "INFO")