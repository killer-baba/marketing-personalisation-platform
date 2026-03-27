import os
from dotenv import load_dotenv
load_dotenv()

# MongoDB
from pymongo import MongoClient
c = MongoClient(os.getenv("MONGO_URI"))
print("MongoDB:", c.admin.command("ping"))

# Redis
import redis
r = redis.Redis(host="localhost", port=6379, password="redis123")
print("Redis:", r.ping())

# Neo4j
from neo4j import GraphDatabase
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "neo4j123"))
with driver.session() as s:
    print("Neo4j:", s.run("RETURN 1 AS n").single()["n"])

# Milvus
from pymilvus import connections
connections.connect(host="localhost", port="19530")
print("Milvus: connected")