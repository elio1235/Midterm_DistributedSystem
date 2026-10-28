"""Extra endpoints of the metadata service used by the gateway (Phase 5):
object search / listing, bucket info and delete, cluster status (nodes, chunks per node, degraded objects).
"""
import json
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query

import cluster
import db

router = APIRouter()

SORTS = {"key": "key", "created_at": "created_at", "size": "size"}


def _like_escape(text):
    return text.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _parse_time(value, name):
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise HTTPException(400, f"{name} must be an ISO date, for example 2026-10-28 or 2026-10-28T12:00:00Z")


# ------------------------------------------------------------------ search and listing
@router.get("/search")
def search_objects(
    bucket: str | None = None,
    prefix: str | None = None,
    q: str | None = Query(None, description="part of the key (case-insensitive)"),
    min_size: int | None = None,
    max_size: int | None = None,
    created_after: str | None = None,
    created_before: str | None = None,
    tag: list[str] = Query([], description="user-defined tag as name:value, can be repeated"),
    sort: str = "key",
    desc: bool = False,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """Latest version of every object that matches ALL the given filters."""
    if sort not in SORTS:
        raise HTTPException(400, f"sort must be one of {list(SORTS)}")
    where, params = [], {"bucket": bucket}
    if prefix:
        where.append("key LIKE %(prefix)s ESCAPE '\\'")
        params["prefix"] = _like_escape(prefix) + "%"
    if q:
        where.append("key ILIKE %(q)s ESCAPE '\\'")
        params["q"] = "%" + _like_escape(q) + "%"
    if min_size is not None:
        where.append("size >= %(min_size)s")
        params["min_size"] = min_size
    if max_size is not None:
        where.append("size <= %(max_size)s")
        params["max_size"] = max_size
    if created_after:
        where.append("created_at >= %(after)s")
        params["after"] = _parse_time(created_after, "created_after")
    if created_before:
        where.append("created_at <= %(before)s")
        params["before"] = _parse_time(created_before, "created_before")
    for i, t in enumerate(tag):
        name, sep, value = t.partition(":")
        if not sep or not name:
            raise HTTPException(400, f"tag filter '{t}' must look like name:value")
        where.append(f"tags @> %(tag{i})s::jsonb")
        params[f"tag{i}"] = json.dumps({name: value})
    sql = (
        "WITH latest AS (SELECT DISTINCT ON (bucket, key) * FROM objects "
        "WHERE (%(bucket)s::text IS NULL OR bucket = %(bucket)s) ORDER BY bucket, key, version DESC) "
        "SELECT bucket, key, version, size, chunking, tags, created_at, cardinality(chunk_hashes) AS chunks, "
        "count(*) OVER () AS total FROM latest "
        + ("WHERE " + " AND ".join(where) + " " if where else "")
        + f"ORDER BY {SORTS[sort]} {'DESC' if desc else 'ASC'}, key ASC LIMIT %(limit)s OFFSET %(offset)s"
    )
    params.update(limit=limit, offset=offset)
    with db.get_conn() as conn, db.cursor(conn) as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    total = rows[0]["total"] if rows else 0
    for r in rows:
        r.pop("total")
    return {"total": total, "limit": limit, "offset": offset, "items": rows}


# ------------------------------------------------------------------ buckets
@router.get("/buckets/{name}")
def bucket_info(name: str):
    with db.get_conn() as conn, db.cursor(conn) as cur:
        cur.execute("SELECT * FROM buckets WHERE name = %s", (name,))
        b = cur.fetchone()
        if not b:
            raise HTTPException(404, "bucket not found")
        cur.execute(
            "SELECT count(*) AS objects, COALESCE(sum(size), 0) AS bytes FROM "
            "(SELECT DISTINCT ON (key) key, size FROM objects WHERE bucket = %s ORDER BY key, version DESC) t",
            (name,),
        )
        s = cur.fetchone()
        cur.execute("SELECT count(*) AS versions FROM objects WHERE bucket = %s", (name,))
        v = cur.fetchone()
    if b["redundancy"] == "erasure":
        config = {"scheme": "erasure", "k": b["ec_k"], "m": b["ec_m"], "want": b["ec_k"] + b["ec_m"]}
    else:
        config = {"scheme": "replication", "want": b["replicas"], "write_quorum": b["write_quorum"],
                  "read_quorum": b["read_quorum"]}
    return {"name": name, "created_at": b["created_at"], **config,
            "objects": s["objects"], "bytes": int(s["bytes"]), "versions": v["versions"]}


@router.delete("/buckets/{name}")
def delete_bucket(name: str):
    with db.get_conn() as conn, db.cursor(conn) as cur:
        cur.execute("SELECT 1 FROM buckets WHERE name = %s", (name,))
        if not cur.fetchone():
            raise HTTPException(404, "bucket not found")
        cur.execute("SELECT count(*) AS n FROM objects WHERE bucket = %s", (name,))
        n = cur.fetchone()["n"]
        if n:
            raise HTTPException(409, f"bucket is not empty ({n} object versions): delete the objects first")
        cur.execute("DELETE FROM buckets WHERE name = %s", (name,))
    return {"name": name, "deleted": True}


# ------------------------------------------------------------------ cluster status
@router.get("/cluster/status")
def cluster_status():
    """Nodes, free space, chunks and bytes per node, degraded chunks / objects and the repair progress."""
    timeout = cluster.HEARTBEAT_TIMEOUT
    with db.get_conn() as conn, db.cursor(conn) as cur:
        cur.execute(
            "SELECT node_id, url, free_bytes, in_ring, state, failed_at, "
            "EXTRACT(EPOCH FROM (now() - last_seen))::float AS age FROM nodes ORDER BY node_id"
        )
        node_rows = cur.fetchall()
        alive = [n["node_id"] for n in node_rows if n["age"] <= timeout]

        cur.execute("SELECT n AS node_id, count(*) AS chunks FROM chunks, unnest(nodes) n WHERE ref_count > 0 GROUP BY n")
        per_node = {r["node_id"]: r["chunks"] for r in cur.fetchall()}
        cur.execute(
            "SELECT node_id, sum(size) AS bytes FROM ("
            " SELECT n AS node_id, size::bigint AS size FROM chunks, unnest(nodes) n WHERE ref_count > 0 AND scheme = 'replication'"
            " UNION ALL"
            " SELECT s->>'node', (s->>'size')::bigint FROM chunks, jsonb_array_elements(shards) s WHERE ref_count > 0 AND scheme = 'erasure'"
            ") t GROUP BY node_id"
        )
        bytes_per_node = {r["node_id"]: int(r["bytes"]) for r in cur.fetchall()}

        # a chunk is degraded when fewer pieces than wanted are on alive nodes, unavailable when it cannot be read any more
        cur.execute(
            "SELECT scheme, count(*) AS total, "
            "count(*) FILTER (WHERE alive < want) AS degraded, "
            "count(*) FILTER (WHERE (scheme = 'erasure' AND alive < ec_k) OR (scheme = 'replication' AND alive < 1)) AS unavailable "
            "FROM (SELECT scheme, want, ec_k, cardinality(ARRAY(SELECT x FROM unnest(nodes) x WHERE x = ANY(%s))) AS alive "
            "      FROM chunks WHERE ref_count > 0) c GROUP BY scheme",
            (alive,),
        )
        by_scheme = cur.fetchall()
        cur.execute(
            "WITH bad AS (SELECT hash, "
            "  (CASE WHEN scheme = 'erasure' THEN cardinality(ARRAY(SELECT x FROM unnest(nodes) x WHERE x = ANY(%(alive)s))) < ec_k "
            "        ELSE cardinality(ARRAY(SELECT x FROM unnest(nodes) x WHERE x = ANY(%(alive)s))) < 1 END) AS lost "
            " FROM chunks WHERE ref_count > 0 AND cardinality(ARRAY(SELECT x FROM unnest(nodes) x WHERE x = ANY(%(alive)s))) < want) "
            "SELECT o.bucket, o.key, o.version, o.size, bool_or(b.lost) AS unavailable, count(*) AS degraded_chunks "
            "FROM objects o JOIN bad b ON b.hash = ANY(o.chunk_hashes) "
            "GROUP BY o.bucket, o.key, o.version, o.size ORDER BY o.bucket, o.key, o.version",
            {"alive": alive},
        )
        degraded_objects = cur.fetchall()

        cur.execute("SELECT * FROM repair_jobs ORDER BY id DESC LIMIT 5")
        jobs = cur.fetchall()
        cur.execute("SELECT * FROM scrub_runs ORDER BY id DESC LIMIT 1")
        scrub = cur.fetchone()
        cur.execute("SELECT count(*) AS n, COALESCE(sum(size), 0) AS bytes FROM chunks WHERE ref_count > 0")
        totals = cur.fetchone()

    nodes = [
        {
            "node_id": n["node_id"], "url": n["url"], "status": "alive" if n["age"] <= timeout else "failed",
            "state": n["state"], "failed_at": n["failed_at"], "in_ring": n["in_ring"],
            "seconds_since_heartbeat": round(n["age"], 1), "free_bytes": n["free_bytes"],
            "chunks": per_node.get(n["node_id"], 0), "stored_bytes": bytes_per_node.get(n["node_id"], 0),
        }
        for n in node_rows
    ]
    running = next((j for j in jobs if j["status"] in ("queued", "running")), None)
    return {
        "nodes": nodes,
        "nodes_alive": len(alive),
        "nodes_total": len(nodes),
        "free_bytes_total": sum(n["free_bytes"] for n in nodes if n["status"] == "alive"),
        "chunks": {
            "unique": totals["n"], "unique_bytes": int(totals["bytes"]),
            "degraded": sum(r["degraded"] for r in by_scheme),
            "unavailable": sum(r["unavailable"] for r in by_scheme),
            "by_scheme": by_scheme,
        },
        "degraded_objects": {
            "count": len(degraded_objects),
            "unavailable": sum(1 for o in degraded_objects if o["unavailable"]),
            "items": degraded_objects[:50],
        },
        "repair": {"running": running, "recent_jobs": jobs, "last_scrub": scrub},
    }
