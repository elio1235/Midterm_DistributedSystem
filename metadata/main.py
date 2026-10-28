import os
import time

from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI(title="Metadata Service")

HEARTBEAT_TIMEOUT = float(os.getenv("HEARTBEAT_TIMEOUT", "10"))
nodes: dict = {}


class Heartbeat(BaseModel):
    node_id: str
    url: str
    free_bytes: int
    chunk_count: int


@app.post("/heartbeat")
def heartbeat(hb: Heartbeat):
    nodes[hb.node_id] = {**hb.model_dump(), "last_seen": time.time()}
    return {"ok": True}


@app.get("/nodes")
def list_nodes():
    now = time.time()
    result = []
    for n in sorted(nodes.values(), key=lambda x: x["node_id"]):
        age = now - n["last_seen"]
        result.append({
            "node_id": n["node_id"],
            "url": n["url"],
            "free_bytes": n["free_bytes"],
            "chunk_count": n["chunk_count"],
            "seconds_since_heartbeat": round(age, 1),
            "status": "alive" if age <= HEARTBEAT_TIMEOUT else "failed",
        })
    return result