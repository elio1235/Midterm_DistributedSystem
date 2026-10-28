import os
import re
from collections import Counter
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException, Query
from psycopg2.extras import Json
from pydantic import BaseModel

import db
from common.ring import HashRing

HEARTBEAT_TIMEOUT = float(os.getenv("HEARTBEAT_TIMEOUT", "10"))
REPLICATION_FACTOR = int(os.getenv("REPLICATION_FACTOR", "3"))
VNODES = int(os.getenv("VNODES", "128"))

HASH_RE = re.compile(r"^[0-9a-f]{64}$")
NAME_RE = re.compile(r"^[A-Za-z0-9._-]{1,63}$")


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db()
    yield


app = FastAPI(title="Metadata and Placement Service", lifespan=lifespan)


# ------------------------------------------------------------------ models
class Heartbeat(BaseModel):
    node_id: str
    url: str
    free_bytes: int
    chunk_count: int


class BucketRequest(BaseModel):
    redundancy: str = "replication"


class ChunkRef(BaseModel):
    hash: str
    size: int


class PlanRequest(BaseModel):
    chunks: list[ChunkRef]


class UploadedChunk(BaseModel):
    hash: str
    size: int
    nodes: list[str]


class CommitRequest(BaseModel):
    bucket: str
    key: str
    size: int
    chunking: str = "fixed"
    tags: dict[str, str] = {}
    chunk_hashes: list[str]            # ordered list of chunk hashes of the object
    uploaded: list[UploadedChunk] = []  # chunks the client really sent to the nodes


# ------------------------------------------------------------------ helpers
def check_hash(h):
    if not HASH_RE.match(h):
        raise HTTPException(400, f"invalid chunk hash: {h}")


_ring_cache = {"members": None, "ring": None}


def load_ring():
    """Return (ring, urls) built from the nodes that are in the ring."""
    with db.get_conn() as conn, db.cursor(conn) as cur:
        cur.execute("SELECT node_id, url FROM nodes WHERE in_ring ORDER BY node_id")
        rows = cur.fetchall()
    members = tuple(r["node_id"] for r in rows)
    if _ring_cache["members"] != members:
        _ring_cache["ring"] = HashRing(members, vnodes=VNODES)
        _ring_cache["members"] = members
    return _ring_cache["ring"], {r["node_id"]: r["url"] for r in rows}


def node_infos(node_ids):
    """node_id -> {node_id, url, status} for the given ids."""
    if not node_ids:
        return {}
    with db.get_conn() as conn, db.cursor(conn) as cur:
        cur.execute(
            "SELECT node_id, url, EXTRACT(EPOCH FROM (now() - last_seen))::float AS age "
            "FROM nodes WHERE node_id = ANY(%s)",
            (list(node_ids),),
        )
        rows = cur.fetchall()
    return {
        r["node_id"]: {
            "node_id": r["node_id"],
            "url": r["url"],
            "status": "alive" if r["age"] <= HEARTBEAT_TIMEOUT else "failed",
        }
        for r in rows
    }


# ------------------------------------------------------------------ health, heartbeat, nodes
@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/heartbeat")
def heartbeat(hb: Heartbeat):
    with db.get_conn() as conn, db.cursor(conn) as cur:
        cur.execute(
            """
            INSERT INTO nodes (node_id, url, free_bytes, chunk_count, last_seen)
            VALUES (%s, %s, %s, %s, now())
            ON CONFLICT (node_id) DO UPDATE
              SET url = EXCLUDED.url, free_bytes = EXCLUDED.free_bytes,
                  chunk_count = EXCLUDED.chunk_count, last_seen = now()
            """,
            (hb.node_id, hb.url, hb.free_bytes, hb.chunk_count),
        )
    return {"ok": True}


@app.get("/nodes")
def list_nodes():
    with db.get_conn() as conn, db.cursor(conn) as cur:
        cur.execute(
            "SELECT node_id, url, free_bytes, chunk_count, in_ring, "
            "EXTRACT(EPOCH FROM (now() - last_seen))::float AS age FROM nodes ORDER BY node_id"
        )
        rows = cur.fetchall()
    return [
        {
            "node_id": r["node_id"],
            "url": r["url"],
            "free_bytes": r["free_bytes"],
            "chunk_count": r["chunk_count"],
            "in_ring": r["in_ring"],
            "seconds_since_heartbeat": round(r["age"], 1),
            "status": "alive" if r["age"] <= HEARTBEAT_TIMEOUT else "failed",
        }
        for r in rows
    ]


# ------------------------------------------------------------------ consistent hashing ring
@app.get("/ring")
def get_ring():
    ring, urls = load_ring()
    return {
        "vnodes_per_node": ring.vnodes,
        "virtual_points": len(ring._keys),
        "replication_factor": REPLICATION_FACTOR,
        "nodes": ring.nodes,
    }


@app.get("/ring/placement/{chunk_hash}")
def placement(chunk_hash: str, n: int = Query(REPLICATION_FACTOR, ge=1, le=16)):
    check_hash(chunk_hash)
    ring, urls = load_ring()
    ids = ring.get_nodes(chunk_hash, n)
    return {"chunk_hash": chunk_hash, "targets": [{"node_id": i, "url": urls[i]} for i in ids]}


def set_in_ring(node_id, value):
    with db.get_conn() as conn, db.cursor(conn) as cur:
        cur.execute("UPDATE nodes SET in_ring = %s WHERE node_id = %s", (value, node_id))
        if cur.rowcount == 0:
            raise HTTPException(404, "unknown node (it has never sent a heartbeat)")
    return get_ring()


@app.post("/ring/nodes/{node_id}")
def add_node_to_ring(node_id: str):
    return set_in_ring(node_id, True)


@app.delete("/ring/nodes/{node_id}")
def remove_node_from_ring(node_id: str):
    return set_in_ring(node_id, False)


# ------------------------------------------------------------------ buckets
@app.post("/buckets/{name}")
def create_bucket(name: str, req: BucketRequest = BucketRequest()):
    if not NAME_RE.match(name):
        raise HTTPException(400, "bucket name: 1-63 characters, letters, digits, . _ -")
    if req.redundancy not in ("replication", "erasure"):
        raise HTTPException(400, "redundancy must be 'replication' or 'erasure'")
    with db.get_conn() as conn, db.cursor(conn) as cur:
        cur.execute(
            "INSERT INTO buckets (name, redundancy) VALUES (%s, %s) ON CONFLICT (name) DO NOTHING",
            (name, req.redundancy),
        )
        created = cur.rowcount == 1
    return {"name": name, "redundancy": req.redundancy, "created": created}


@app.get("/buckets")
def list_buckets():
    with db.get_conn() as conn, db.cursor(conn) as cur:
        cur.execute("SELECT name, redundancy, created_at FROM buckets ORDER BY name")
        return cur.fetchall()


# ------------------------------------------------------------------ chunks: plan (dedup check + placement)
@app.post("/chunks/plan")
def plan_chunks(req: PlanRequest):
    """For each chunk: does it already exist (dedup hit)? If not, which nodes should store it?"""
    hashes = [c.hash for c in req.chunks]
    for h in hashes:
        check_hash(h)
    ring, urls = load_ring()
    with db.get_conn() as conn, db.cursor(conn) as cur:
        cur.execute("SELECT hash, nodes FROM chunks WHERE hash = ANY(%s) AND ref_count > 0", (hashes,))
        existing = {r["hash"]: r["nodes"] for r in cur.fetchall()}
    stored_ids = {n for nodes in existing.values() for n in nodes}
    infos = node_infos(stored_ids)
    result = []
    for h in hashes:
        if h in existing:
            targets = [infos.get(n, {"node_id": n, "url": "", "status": "unknown"}) for n in existing[h]]
            result.append({"hash": h, "exists": True, "targets": targets})
        else:
            ids = ring.get_nodes(h, REPLICATION_FACTOR)
            result.append({"hash": h, "exists": False, "targets": [{"node_id": i, "url": urls[i]} for i in ids]})
    return {"chunks": result}


@app.get("/chunks/{chunk_hash}")
def chunk_info(chunk_hash: str):
    check_hash(chunk_hash)
    with db.get_conn() as conn, db.cursor(conn) as cur:
        cur.execute("SELECT hash, size, ref_count, nodes, created_at FROM chunks WHERE hash = %s", (chunk_hash,))
        row = cur.fetchone()
    if not row:
        raise HTTPException(404, "chunk not found")
    return row


# ------------------------------------------------------------------ objects
@app.post("/objects")
def commit_object(req: CommitRequest):
    """Save the metadata of an object: ordered chunk list, new version, reference counts."""
    if not NAME_RE.match(req.bucket):
        raise HTTPException(400, "invalid bucket name")
    if not req.key or len(req.key) > 1024:
        raise HTTPException(400, "key must be 1-1024 characters")
    if req.chunking not in ("fixed", "cdc"):
        raise HTTPException(400, "chunking must be 'fixed' or 'cdc'")
    for h in req.chunk_hashes:
        check_hash(h)

    counts = Counter(req.chunk_hashes)
    uploaded = {u.hash: u for u in req.uploaded}
    with db.get_conn() as conn, db.cursor(conn) as cur:
        cur.execute("SELECT 1 FROM buckets WHERE name = %s", (req.bucket,))
        if not cur.fetchone():
            raise HTTPException(404, "bucket not found")

        missing = []
        for h, count in counts.items():
            if h in uploaded:
                u = uploaded[h]
                cur.execute(
                    """
                    INSERT INTO chunks (hash, size, ref_count, nodes) VALUES (%s, %s, %s, %s)
                    ON CONFLICT (hash) DO UPDATE SET ref_count = chunks.ref_count + EXCLUDED.ref_count
                    """,
                    (h, u.size, count, u.nodes),
                )
            else:
                # deduplicated chunk: it must already exist, we only add references
                cur.execute("UPDATE chunks SET ref_count = ref_count + %s WHERE hash = %s", (count, h))
                if cur.rowcount == 0:
                    missing.append(h)
        if missing:   # the chunk was deleted by someone else in the meantime: client must upload it again
            raise HTTPException(409, {"error": "chunks missing, upload them again", "missing": missing})

        cur.execute(
            "SELECT COALESCE(MAX(version), 0) + 1 AS v FROM objects WHERE bucket = %s AND key = %s",
            (req.bucket, req.key),
        )
        version = cur.fetchone()["v"]
        cur.execute(
            """
            INSERT INTO objects (bucket, key, version, size, chunking, chunk_hashes, tags)
            VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING created_at
            """,
            (req.bucket, req.key, version, req.size, req.chunking, req.chunk_hashes, Json(req.tags)),
        )
        created_at = cur.fetchone()["created_at"]

    return {
        "bucket": req.bucket,
        "key": req.key,
        "version": version,
        "size": req.size,
        "created_at": created_at,
        "chunks_total": len(req.chunk_hashes),
        "chunks_uploaded": len(uploaded),
        "chunks_reused": len(req.chunk_hashes) - len(uploaded),
    }


@app.get("/objects/{bucket}/{key:path}")
def get_object(bucket: str, key: str, version: int | None = None):
    with db.get_conn() as conn, db.cursor(conn) as cur:
        if version is None:
            cur.execute(
                "SELECT * FROM objects WHERE bucket = %s AND key = %s ORDER BY version DESC LIMIT 1", (bucket, key)
            )
        else:
            cur.execute("SELECT * FROM objects WHERE bucket = %s AND key = %s AND version = %s", (bucket, key, version))
        obj = cur.fetchone()
        if not obj:
            raise HTTPException(404, "object not found")
        cur.execute("SELECT hash, size, nodes FROM chunks WHERE hash = ANY(%s)", (obj["chunk_hashes"],))
        chunk_rows = {r["hash"]: r for r in cur.fetchall()}
    infos = node_infos({n for r in chunk_rows.values() for n in r["nodes"]})
    chunks = []
    for h in obj["chunk_hashes"]:
        row = chunk_rows.get(h)
        nodes = [infos.get(n, {"node_id": n, "url": "", "status": "unknown"}) for n in row["nodes"]] if row else []
        chunks.append({"hash": h, "size": row["size"] if row else None, "nodes": nodes})
    return {
        "bucket": obj["bucket"],
        "key": obj["key"],
        "version": obj["version"],
        "size": obj["size"],
        "chunking": obj["chunking"],
        "tags": obj["tags"],
        "created_at": obj["created_at"],
        "chunks": chunks,
    }


@app.delete("/objects/{bucket}/{key:path}")
def delete_object(bucket: str, key: str):
    """Delete all versions of an object. A chunk is removed from the nodes only when nobody references it."""
    with db.get_conn() as conn, db.cursor(conn) as cur:
        cur.execute("SELECT version, chunk_hashes FROM objects WHERE bucket = %s AND key = %s FOR UPDATE", (bucket, key))
        rows = cur.fetchall()
        if not rows:
            raise HTTPException(404, "object not found")
        counts = Counter(h for r in rows for h in r["chunk_hashes"])
        cur.execute("DELETE FROM objects WHERE bucket = %s AND key = %s", (bucket, key))

        freed = []   # (hash, nodes) of chunks that nobody references anymore
        for h, count in counts.items():
            cur.execute(
                "UPDATE chunks SET ref_count = ref_count - %s WHERE hash = %s RETURNING ref_count, nodes", (count, h)
            )
            row = cur.fetchone()
            if row and row["ref_count"] <= 0:
                freed.append((h, row["nodes"]))
        if freed:
            cur.execute("DELETE FROM chunks WHERE hash = ANY(%s)", ([h for h, _ in freed],))

    # physically delete the freed chunks from the storage nodes (best effort)
    infos = node_infos({n for _, nodes in freed for n in nodes})
    node_errors = 0
    with httpx.Client(timeout=10) as client:
        for h, nodes in freed:
            for n in nodes:
                info = infos.get(n)
                try:
                    r = client.delete(f"{info['url']}/chunks/{h}")
                    if r.status_code not in (200, 404):
                        node_errors += 1
                except Exception:
                    node_errors += 1

    return {
        "bucket": bucket,
        "key": key,
        "versions_deleted": len(rows),
        "chunks_dereferenced": len(counts),
        "chunks_still_referenced": len(counts) - len(freed),
        "chunks_freed": len(freed),
        "freed_hashes": [h for h, _ in freed],
        "node_delete_errors": node_errors,
    }


# ------------------------------------------------------------------ statistics
@app.get("/stats/dedup")
def dedup_stats():
    with db.get_conn() as conn, db.cursor(conn) as cur:
        cur.execute("SELECT COUNT(*) AS versions, COALESCE(SUM(size), 0) AS logical FROM objects")
        o = cur.fetchone()
        cur.execute(
            "SELECT COUNT(*) AS n, COALESCE(SUM(size), 0) AS unique_bytes, "
            "COALESCE(SUM(size::bigint * cardinality(nodes)), 0) AS stored_bytes FROM chunks"
        )
        c = cur.fetchone()
    logical, unique = int(o["logical"]), int(c["unique_bytes"])
    return {
        "object_versions": o["versions"],
        "logical_bytes": logical,
        "unique_chunks": c["n"],
        "unique_bytes": unique,
        "stored_bytes_with_replicas": int(c["stored_bytes"]),
        "dedup_ratio": round(logical / unique, 2) if unique else None,
        "space_saved_percent": round((1 - unique / logical) * 100, 1) if logical else 0.0,
    }