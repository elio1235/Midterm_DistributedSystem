import re
import threading
from collections import Counter
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException, Query
from psycopg2.extras import Json
from pydantic import BaseModel

import api_extra
import db
import repair
from cluster import HEARTBEAT_TIMEOUT, REPLICATION_FACTOR, load_ring, node_infos

HASH_RE = re.compile(r"^[0-9a-f]{64}$")
NAME_RE = re.compile(r"^[A-Za-z0-9._-]{1,63}$")
MAX_SHARDS = 32


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db()
    repair.start_background()
    yield


app = FastAPI(title="Metadata and Placement Service", lifespan=lifespan)
app.include_router(api_extra.router)


# ------------------------------------------------------------------ models
class Heartbeat(BaseModel):
    node_id: str
    url: str
    free_bytes: int
    chunk_count: int


class BucketRequest(BaseModel):
    redundancy: str = "replication"     # "replication" or "erasure"
    replicas: int = 3                   # N
    write_quorum: int = 2               # W
    read_quorum: int = 2                # R   (W + R must be greater than N)
    ec_k: int = 4                       # data shards
    ec_m: int = 2                       # parity shards


class ChunkRef(BaseModel):
    hash: str
    size: int


class PlanRequest(BaseModel):
    bucket: str | None = None
    chunks: list[ChunkRef]


class ShardInfo(BaseModel):
    i: int          # shard number: 0..k-1 data, k..k+m-1 parity
    hash: str       # SHA-256 of the stored shard (= its name on the node)
    node: str
    size: int


class UploadedChunk(BaseModel):
    hash: str
    size: int
    scheme: str = "replication"
    nodes: list[str] = []               # replication: nodes that confirmed the write
    shards: list[ShardInfo] = []        # erasure coding: shards that were stored


class CommitRequest(BaseModel):
    bucket: str
    key: str
    size: int
    chunking: str = "fixed"
    tags: dict[str, str] = {}
    chunk_hashes: list[str]             # ordered list of chunk hashes of the object
    uploaded: list[UploadedChunk] = []  # chunks the client really sent to the nodes


# ------------------------------------------------------------------ helpers
def check_hash(h):
    if not HASH_RE.match(h):
        raise HTTPException(400, f"invalid chunk hash: {h}")


def get_bucket(cur, name):
    cur.execute("SELECT * FROM buckets WHERE name = %s", (name,))
    return cur.fetchone()


def bucket_config(b):
    """The redundancy settings of a bucket as a small dictionary."""
    if b["redundancy"] == "erasure":
        return {"scheme": "erasure", "k": b["ec_k"], "m": b["ec_m"], "want": b["ec_k"] + b["ec_m"],
                "min_ok": b["ec_k"] + 1}
    return {"scheme": "replication", "want": b["replicas"], "min_ok": b["write_quorum"],
            "write_quorum": b["write_quorum"], "read_quorum": b["read_quorum"]}


# ------------------------------------------------------------------ health, heartbeat, nodes
@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/heartbeat")
def heartbeat(hb: Heartbeat):
    with db.get_conn() as conn, db.cursor(conn) as cur:
        cur.execute(
            """
            INSERT INTO nodes (node_id, url, free_bytes, chunk_count, last_seen, state)
            VALUES (%s, %s, %s, %s, now(), 'alive')
            ON CONFLICT (node_id) DO UPDATE
              SET url = EXCLUDED.url, free_bytes = EXCLUDED.free_bytes,
                  chunk_count = EXCLUDED.chunk_count, last_seen = now(), state = 'alive', failed_at = NULL
            """,
            (hb.node_id, hb.url, hb.free_bytes, hb.chunk_count),
        )
    return {"ok": True}


@app.get("/nodes")
def list_nodes():
    with db.get_conn() as conn, db.cursor(conn) as cur:
        cur.execute(
            "SELECT node_id, url, free_bytes, chunk_count, in_ring, state, failed_at, "
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
            "state": r["state"],            # 'failed' once the failure detector has marked the node (repair is triggered then)
            "failed_at": r["failed_at"],
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
def placement(chunk_hash: str, n: int = Query(REPLICATION_FACTOR, ge=1, le=MAX_SHARDS)):
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
    if req.redundancy == "replication":
        n, w, r = req.replicas, req.write_quorum, req.read_quorum
        if not (1 <= n <= 16 and 1 <= w <= n and 1 <= r <= n):
            raise HTTPException(400, "need 1 <= N <= 16, 1 <= W <= N and 1 <= R <= N")
        if w + r <= n:
            raise HTTPException(400, f"quorum rule broken: W + R must be greater than N (W={w}, R={r}, N={n})")
    elif req.redundancy == "erasure":
        if not (1 <= req.ec_k and 1 <= req.ec_m and req.ec_k + req.ec_m <= MAX_SHARDS):
            raise HTTPException(400, f"need k >= 1, m >= 1 and k + m <= {MAX_SHARDS}")
    else:
        raise HTTPException(400, "redundancy must be 'replication' or 'erasure'")
    with db.get_conn() as conn, db.cursor(conn) as cur:
        cur.execute(
            "INSERT INTO buckets (name, redundancy, replicas, write_quorum, read_quorum, ec_k, ec_m) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (name) DO NOTHING",
            (name, req.redundancy, req.replicas, req.write_quorum, req.read_quorum, req.ec_k, req.ec_m),
        )
        created = cur.rowcount == 1
        b = get_bucket(cur, name)
    return {"name": name, "created": created, **bucket_config(b)}


@app.get("/buckets")
def list_buckets():
    with db.get_conn() as conn, db.cursor(conn) as cur:
        cur.execute("SELECT * FROM buckets ORDER BY name")
        return [{"name": b["name"], "created_at": b["created_at"], **bucket_config(b)} for b in cur.fetchall()]


# ------------------------------------------------------------------ chunks: plan (dedup check + placement)
@app.post("/chunks/plan")
def plan_chunks(req: PlanRequest):
    """For each chunk: does it already exist (dedup hit)? If not, which nodes should store it, and how?"""
    hashes = [c.hash for c in req.chunks]
    for h in hashes:
        check_hash(h)
    ring, urls = load_ring()
    with db.get_conn() as conn, db.cursor(conn) as cur:
        if req.bucket is None:
            cfg = {"scheme": "replication", "want": REPLICATION_FACTOR, "min_ok": 2}
        else:
            b = get_bucket(cur, req.bucket)
            if not b:
                raise HTTPException(404, "bucket not found")
            cfg = bucket_config(b)
        cur.execute("SELECT hash, scheme, nodes FROM chunks WHERE hash = ANY(%s) AND ref_count > 0", (hashes,))
        existing = {r["hash"]: r for r in cur.fetchall()}
    new_hashes = [h for h in hashes if h not in existing]
    if new_hashes and len(ring) < cfg["min_ok"]:
        raise HTTPException(503, f"only {len(ring)} alive nodes, but {cfg['min_ok']} are needed to store a chunk")
    infos = node_infos({n for r in existing.values() for n in r["nodes"]})
    result = []
    for h in hashes:
        if h in existing:
            targets = [infos.get(n, {"node_id": n, "url": "", "status": "unknown"}) for n in existing[h]["nodes"]]
            result.append({"hash": h, "exists": True, "scheme": existing[h]["scheme"], "targets": targets})
        else:
            ids = ring.get_nodes(h, cfg["want"])
            result.append({
                "hash": h, "exists": False, "scheme": cfg["scheme"], "want": cfg["want"], "min_ok": cfg["min_ok"],
                "k": cfg.get("k"), "m": cfg.get("m"),
                "targets": [{"node_id": i, "url": urls[i]} for i in ids],
            })
    return {"chunks": result}


@app.get("/chunks/{chunk_hash}")
def chunk_info(chunk_hash: str):
    check_hash(chunk_hash)
    with db.get_conn() as conn, db.cursor(conn) as cur:
        cur.execute(
            "SELECT hash, size, ref_count, scheme, want, ec_k, ec_m, nodes, shards, stored_bytes, created_at "
            "FROM chunks WHERE hash = %s", (chunk_hash,))
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
        b = get_bucket(cur, req.bucket)
        if not b:
            raise HTTPException(404, "bucket not found")
        cfg = bucket_config(b)

        missing = []
        for h, count in counts.items():
            if h in uploaded:
                u = uploaded[h]
                if u.scheme != cfg["scheme"]:
                    raise HTTPException(400, f"bucket uses {cfg['scheme']} but chunk {h[:12]} was sent as {u.scheme}")
                if cfg["scheme"] == "replication":
                    nodes = sorted(set(u.nodes))
                    if len(nodes) < cfg["min_ok"]:
                        raise HTTPException(400, f"write quorum not reached for chunk {h[:12]}: {len(nodes)} of W={cfg['min_ok']}")
                    shards, ec_k, ec_m = [], None, None
                    stored = u.size * len(nodes)
                else:
                    idx = {s.i for s in u.shards}
                    if len(idx) != len(u.shards) or not idx <= set(range(cfg["want"])) or len(idx) < cfg["min_ok"]:
                        raise HTTPException(400, f"write quorum not reached for chunk {h[:12]}: {len(idx)} shards, need {cfg['min_ok']}")
                    shards = [s.model_dump() for s in sorted(u.shards, key=lambda s: s.i)]
                    nodes = sorted({s["node"] for s in shards})
                    ec_k, ec_m = cfg["k"], cfg["m"]
                    stored = sum(s["size"] for s in shards)
                cur.execute(
                    """
                    INSERT INTO chunks (hash, size, ref_count, nodes, scheme, want, ec_k, ec_m, shards, stored_bytes)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (hash) DO UPDATE SET ref_count = chunks.ref_count + EXCLUDED.ref_count
                    """,
                    (h, u.size, count, nodes, cfg["scheme"], cfg["want"], ec_k, ec_m, Json(shards), stored),
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
        b = get_bucket(cur, bucket)
        cur.execute(
            "SELECT hash, size, scheme, want, ec_k, ec_m, nodes, shards FROM chunks WHERE hash = ANY(%s)",
            (obj["chunk_hashes"],),
        )
        chunk_rows = {r["hash"]: r for r in cur.fetchall()}
    infos = node_infos({n for r in chunk_rows.values() for n in r["nodes"]})

    def info(n):
        return infos.get(n, {"node_id": n, "url": "", "status": "unknown"})

    chunks = []
    for h in obj["chunk_hashes"]:
        row = chunk_rows.get(h)
        if not row:
            chunks.append({"hash": h, "size": None, "scheme": None, "nodes": [], "shards": []})
            continue
        chunks.append({
            "hash": h, "size": row["size"], "scheme": row["scheme"], "want": row["want"],
            "k": row["ec_k"], "m": row["ec_m"],
            "nodes": [info(n) for n in row["nodes"]],
            "shards": [{"i": s["i"], "hash": s["hash"], "size": s["size"], "node_id": s["node"],
                        "url": info(s["node"])["url"], "status": info(s["node"])["status"]} for s in row["shards"]],
        })
    return {
        "bucket": obj["bucket"],
        "key": obj["key"],
        "version": obj["version"],
        "size": obj["size"],
        "chunking": obj["chunking"],
        "tags": obj["tags"],
        "created_at": obj["created_at"],
        "redundancy": bucket_config(b),
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

        freed = []   # (hash, [(node, blob_hash)]) of chunks that nobody references anymore
        for h, count in counts.items():
            cur.execute(
                "UPDATE chunks SET ref_count = ref_count - %s WHERE hash = %s "
                "RETURNING ref_count, nodes, scheme, shards", (count, h)
            )
            row = cur.fetchone()
            if row and row["ref_count"] <= 0:
                row["hash"] = h
                freed.append((h, repair.blob_locations(row)))
        if freed:
            cur.execute("DELETE FROM chunks WHERE hash = ANY(%s)", ([h for h, _ in freed],))

    # physically delete the freed chunks (or erasure shards) from the storage nodes (best effort)
    infos = node_infos({n for _, blobs in freed for n, _ in blobs})
    node_errors = 0
    with httpx.Client(timeout=10) as client:
        for h, blobs in freed:
            for node, blob_hash in blobs:
                info = infos.get(node)
                try:
                    r = client.delete(f"{info['url']}/chunks/{blob_hash}")
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


# ------------------------------------------------------------------ repair and scrubbing
@app.post("/repair/run")
def run_repair(wait: bool = True, if_needed: bool = False):
    """Start a repair job now (it also runs by itself when a node fails).
    With if_needed=true no job is created when every chunk is complete."""
    if if_needed:
        dead = [n for n, i in node_infos().items() if i["status"] != "alive"]
        if not repair.chunks_needing_repair(dead):
            return {"skipped": True, "message": "nothing to repair"}
    job_id = repair.trigger_repair("manual", None, wait=wait)
    return get_repair_job(job_id)


@app.get("/repair/jobs")
def list_repair_jobs(limit: int = Query(10, ge=1, le=200)):
    with db.get_conn() as conn, db.cursor(conn) as cur:
        cur.execute("SELECT * FROM repair_jobs ORDER BY id DESC LIMIT %s", (limit,))
        return cur.fetchall()


@app.get("/repair/jobs/{job_id}")
def get_repair_job(job_id: int):
    with db.get_conn() as conn, db.cursor(conn) as cur:
        cur.execute("SELECT * FROM repair_jobs WHERE id = %s", (job_id,))
        row = cur.fetchone()
    if not row:
        raise HTTPException(404, "repair job not found")
    return row


@app.post("/scrub/run")
def run_scrub(orphans: bool = True, grace: float | None = None, wait: bool = True):
    """Run the scrubber now. grace = minimum age (seconds) of a file before it can be deleted as an orphan."""
    if not wait:
        threading.Thread(target=repair.scrub_once, args=("manual", orphans, grace), daemon=True).start()
        return {"started": True}
    run_id = repair.scrub_once("manual", orphans, grace)
    with db.get_conn() as conn, db.cursor(conn) as cur:
        cur.execute("SELECT * FROM scrub_runs WHERE id = %s", (run_id,))
        return cur.fetchone()


@app.get("/scrub/runs")
def list_scrub_runs(limit: int = Query(10, ge=1, le=200)):
    with db.get_conn() as conn, db.cursor(conn) as cur:
        cur.execute("SELECT * FROM scrub_runs ORDER BY id DESC LIMIT %s", (limit,))
        return cur.fetchall()


# ------------------------------------------------------------------ statistics
@app.get("/stats/dedup")
def dedup_stats():
    with db.get_conn() as conn, db.cursor(conn) as cur:
        cur.execute("SELECT COUNT(*) AS versions, COALESCE(SUM(size), 0) AS logical FROM objects")
        o = cur.fetchone()
        cur.execute(
            "SELECT scheme, COUNT(*) AS chunks, COALESCE(SUM(size), 0) AS unique_bytes, "
            "COALESCE(SUM(stored_bytes), 0) AS stored_bytes FROM chunks GROUP BY scheme ORDER BY scheme"
        )
        by_scheme = cur.fetchall()
    logical = int(o["logical"])
    unique = sum(int(s["unique_bytes"]) for s in by_scheme)
    stored = sum(int(s["stored_bytes"]) for s in by_scheme)
    return {
        "object_versions": o["versions"],
        "logical_bytes": logical,
        "unique_chunks": sum(s["chunks"] for s in by_scheme),
        "unique_bytes": unique,
        "stored_bytes": stored,
        "stored_bytes_with_replicas": stored,
        "storage_overhead": round(stored / unique, 3) if unique else None,
        "dedup_ratio": round(logical / unique, 2) if unique else None,
        "space_saved_percent": round((1 - unique / logical) * 100, 1) if logical else 0.0,
        "by_scheme": [
            {"scheme": s["scheme"], "chunks": s["chunks"], "unique_bytes": int(s["unique_bytes"]),
             "stored_bytes": int(s["stored_bytes"]),
             "storage_overhead": round(int(s["stored_bytes"]) / int(s["unique_bytes"]), 3) if s["unique_bytes"] else None}
            for s in by_scheme
        ],
    }
