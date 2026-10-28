"""Failure detection, repair jobs and the background scrubber of the metadata service.

Repair job : finds chunks that lost a copy/shard (node failed, or a write reached only part of the nodes)
             and rebuilds the missing pieces on other alive nodes chosen by the hash ring.
Scrubber   : asks every node to re-read its chunks from the disk and check the SHA-256.
             Corrupted or missing copies are rebuilt from healthy ones. It also removes orphan files.
"""
import hashlib
import os
import threading
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import httpx
from psycopg2.extras import Json

import cluster
import db
from common import erasure

DETECT_INTERVAL = float(os.getenv("DETECT_INTERVAL", "2"))              # how often failed nodes are looked for
REPAIR_SWEEP_INTERVAL = float(os.getenv("REPAIR_SWEEP_INTERVAL", "30"))  # how often under-replicated chunks are looked for
SCRUB_INTERVAL = float(os.getenv("SCRUB_INTERVAL", "120"))               # time between two scrub runs
ORPHAN_GRACE = float(os.getenv("ORPHAN_GRACE", "3600"))                  # files younger than this are never deleted as orphans
REPAIR_WORKERS = int(os.getenv("REPAIR_WORKERS", "8"))

_http = httpx.Client(timeout=httpx.Timeout(60, connect=3), limits=httpx.Limits(max_connections=100, max_keepalive_connections=40))
_maint_lock = threading.Lock()          # one repair job or scrub run at a time
_sweep_state = {"signature": None}


def call(method, url, **kw):
    """HTTP request to a node, repeated once if the connection was broken (a restarted node leaves dead keep-alive connections)."""
    try:
        return _http.request(method, url, **kw)
    except httpx.TransportError as e:
        if isinstance(e, (httpx.ConnectError, httpx.ConnectTimeout)):
            raise                                            # the node is down: do not wait for it a second time
        return _http.request(method, url, **kw)


class Traffic:
    """Counts the bytes moved between nodes by one job."""

    def __init__(self):
        self._lock = threading.Lock()
        self.read = 0
        self.written = 0
        self.copies = 0

    def add_read(self, n):
        with self._lock:
            self.read += n

    def add_written(self, n):
        with self._lock:
            self.written += n
            self.copies += 1


def sha(data):
    return hashlib.sha256(data).hexdigest()


def fetch_blob(url, blob_hash, traffic):
    """Download a stored blob from a node and check its hash. Returns bytes or None."""
    try:
        r = call("GET", f"{url}/chunks/{blob_hash}")
    except httpx.HTTPError:
        return None
    if r.status_code != 200:
        return None
    traffic.add_read(len(r.content))
    return r.content if sha(r.content) == blob_hash else None


def put_blob(url, blob_hash, data, traffic):
    try:
        r = call("PUT", f"{url}/chunks/{blob_hash}", content=data)
    except httpx.HTTPError:
        return False
    if r.status_code in (200, 201):
        traffic.add_written(len(data))
        return True
    return False


def blob_locations(row):
    """[(node_id, blob_hash)] for every stored copy (replication) or shard (erasure coding) of a chunk."""
    if row["scheme"] == "erasure":
        return [(s["node"], s["hash"]) for s in row["shards"]]
    return [(n, row["hash"]) for n in row["nodes"]]


def is_alive(infos, node):
    return infos.get(node, {}).get("status") == "alive"


def ring_candidates(ring, chunk_hash, exclude):
    """Alive nodes in the order of the ring for this chunk, without the nodes that already hold a piece."""
    return [n for n in ring.get_nodes(chunk_hash, len(ring)) if n not in exclude]


def set_locations(row, nodes, shards=None):
    with db.get_conn() as conn, db.cursor(conn) as cur:
        if row["scheme"] == "erasure":
            cur.execute(
                "UPDATE chunks SET nodes = %s, shards = %s, stored_bytes = %s WHERE hash = %s AND ref_count > 0",
                (sorted({s["node"] for s in shards}), Json(shards), sum(s["size"] for s in shards), row["hash"]),
            )
        else:
            cur.execute(
                "UPDATE chunks SET nodes = %s, stored_bytes = %s WHERE hash = %s AND ref_count > 0",
                (nodes, row["size"] * len(nodes), row["hash"]),
            )


# ---------------------------------------------------------------------------------------- repairing one chunk
def repair_replicated(row, infos, ring, traffic):
    alive_nodes = [n for n in row["nodes"] if is_alive(infos, n)]
    missing = row["want"] - len(alive_nodes)
    if missing <= 0:
        return "ok"
    data = None
    for n in alive_nodes:                                   # read ONE healthy copy
        data = fetch_blob(infos[n]["url"], row["hash"], traffic)
        if data is not None:
            break
    if data is None:
        return "unrecoverable"
    new_nodes = []
    for n in ring_candidates(ring, row["hash"], set(alive_nodes)):
        if len(new_nodes) == missing:
            break
        if put_blob(infos[n]["url"], row["hash"], data, traffic):
            new_nodes.append(n)
    nodes = alive_nodes + new_nodes
    if len(nodes) >= row["want"]:
        set_locations(row, nodes)                           # complete: forget the failed node
        return "repaired"
    set_locations(row, nodes + [n for n in row["nodes"] if n not in nodes])   # incomplete: keep failed nodes listed
    return "partial"


def repair_erasure(row, infos, ring, traffic):
    k, m = row["ec_k"], row["ec_m"]
    alive_shards = [s for s in row["shards"] if is_alive(infos, s["node"])]
    have = {s["i"] for s in alive_shards}
    missing_idx = [i for i in range(k + m) if i not in have]
    if not missing_idx:
        return "ok"
    if len(alive_shards) < k:
        return "unrecoverable"
    got = {}
    for s in sorted(alive_shards, key=lambda s: s["i"]):    # read k healthy shards (data shards first)
        blob = fetch_blob(infos[s["node"]]["url"], s["hash"], traffic)
        if blob is None:
            continue
        chunk_hash, index, payload = erasure.unpack_shard(blob)
        if chunk_hash == row["hash"] and index == s["i"]:
            got[index] = payload
        if len(got) == k:
            break
    if len(got) < k:
        return "unrecoverable"
    payloads = erasure.rebuild_shards(got, k, m, missing_idx)
    holders = {s["node"] for s in alive_shards}
    new_shards = []
    for i, node in zip(missing_idx, ring_candidates(ring, row["hash"], holders)):
        blob = erasure.pack_shard(row["hash"], i, payloads[i])
        blob_hash = sha(blob)
        if put_blob(infos[node]["url"], blob_hash, blob, traffic):
            new_shards.append({"i": i, "hash": blob_hash, "node": node, "size": len(blob)})
    shards = sorted(alive_shards + new_shards, key=lambda s: s["i"])
    if len(shards) >= k + m:
        set_locations(row, None, shards)
        return "repaired"
    covered = {s["i"] for s in shards}
    set_locations(row, None, sorted(shards + [s for s in row["shards"] if s["i"] not in covered], key=lambda s: s["i"]))
    return "partial"


def repair_chunk(row, infos, ring, traffic):
    if row["scheme"] == "erasure":
        return repair_erasure(row, infos, ring, traffic)
    return repair_replicated(row, infos, ring, traffic)


# ---------------------------------------------------------------------------------------- repair jobs
def chunks_needing_repair(dead_nodes):
    """Chunks with fewer pieces than wanted, or with a piece on a failed node."""
    with db.get_conn() as conn, db.cursor(conn) as cur:
        cur.execute(
            "SELECT hash, size, scheme, want, ec_k, ec_m, nodes, shards FROM chunks "
            "WHERE ref_count > 0 AND (cardinality(nodes) < want OR nodes && %s::text[])",
            (list(dead_nodes),),
        )
        return cur.fetchall()


def create_job(reason, node_id=None):
    with db.get_conn() as conn, db.cursor(conn) as cur:
        cur.execute("INSERT INTO repair_jobs (reason, node_id) VALUES (%s, %s) RETURNING id", (reason, node_id))
        return cur.fetchone()["id"]


def update_job(job_id, **fields):
    cols = ", ".join(f"{k} = %s" for k in fields)
    with db.get_conn() as conn, db.cursor(conn) as cur:
        cur.execute(f"UPDATE repair_jobs SET {cols} WHERE id = %s", (*fields.values(), job_id))


def run_job(job_id):
    with _maint_lock:
        t0 = time.time()
        update_job(job_id, status="running", started_at=_now())
        try:
            infos = cluster.node_infos()
            ring, _ = cluster.load_ring()
            dead = [n for n, i in infos.items() if i["status"] != "alive"]
            rows = chunks_needing_repair(dead)
            update_job(job_id, chunks_total=len(rows))
            traffic, results = Traffic(), Counter()

            def work(row):
                try:
                    return repair_chunk(row, infos, ring, traffic)
                except Exception as e:                      # one bad chunk must not stop the whole job
                    print(f"[repair] chunk {row['hash'][:12]} failed: {e}", flush=True)
                    return "error"

            with ThreadPoolExecutor(REPAIR_WORKERS) as pool:
                for done, outcome in enumerate(pool.map(work, rows), 1):
                    results[outcome] += 1
                    if done % 100 == 0:
                        update_job(job_id, chunks_done=done)
            update_job(
                job_id, status="done", finished_at=_now(), chunks_done=len(rows),
                chunks_repaired=results["repaired"], chunks_partial=results["partial"],
                chunks_unrecoverable=results["unrecoverable"] + results["error"],
                copies_rebuilt=traffic.copies, bytes_read=traffic.read, bytes_written=traffic.written,
                seconds=round(time.time() - t0, 3),
            )
            print(f"[repair] job {job_id} done: {dict(results)} read={traffic.read} written={traffic.written}", flush=True)
        except Exception as e:
            print(f"[repair] job {job_id} failed: {e}", flush=True)
            update_job(job_id, status="failed", finished_at=_now(), seconds=round(time.time() - t0, 3))


def _now():
    return datetime.now(timezone.utc)


def trigger_repair(reason, node_id=None, wait=False):
    job_id = create_job(reason, node_id)
    t = threading.Thread(target=run_job, args=(job_id,), daemon=True)
    t.start()
    if wait:
        t.join()
    return job_id


def _sweep_signature():
    infos = cluster.node_infos()
    dead = [n for n, i in infos.items() if i["status"] != "alive"]
    alive = tuple(sorted(n for n, i in infos.items() if i["status"] == "alive"))
    return (len(chunks_needing_repair(dead)), alive)


def sweep():
    """Start a repair job if some chunks have too few pieces (for example after a write that reached only W nodes)."""
    if _maint_lock.locked():                                 # a repair job or a scrub run is already working
        return None
    signature = _sweep_signature()
    if signature[0] == 0 or signature == _sweep_state["signature"]:
        return None
    job_id = trigger_repair("under-replicated chunks found", None, wait=True)
    _sweep_state["signature"] = _sweep_signature()           # do not repeat the same hopeless job again and again
    return job_id


def failure_detector_loop():
    time.sleep(cluster.HEARTBEAT_TIMEOUT + 2)               # give the nodes time to send their first heartbeat
    last_sweep = time.time()
    while True:
        try:
            with db.get_conn() as conn, db.cursor(conn) as cur:
                cur.execute(
                    "UPDATE nodes SET state = 'failed', failed_at = now() "
                    "WHERE state = 'alive' AND last_seen < now() - make_interval(secs => %s) RETURNING node_id",
                    (cluster.HEARTBEAT_TIMEOUT,),
                )
                failed = [r["node_id"] for r in cur.fetchall()]
            if failed:
                print(f"[detector] nodes marked FAILED (no heartbeat): {failed}", flush=True)
                trigger_repair("node failed", ",".join(failed))
            elif time.time() - last_sweep >= REPAIR_SWEEP_INTERVAL:
                last_sweep = time.time()
                sweep()
        except Exception as e:
            print(f"[detector] error: {e}", flush=True)
        time.sleep(DETECT_INTERVAL)


# ---------------------------------------------------------------------------------------- scrubber
def repair_bad_copies(row, bad, infos, traffic):
    """bad = set of (node_id, blob_hash) of this chunk that are corrupted or missing. Returns (repaired, failed)."""
    if row["scheme"] == "erasure":
        k, m = row["ec_k"], row["ec_m"]
        bad_idx = {s["i"] for s in row["shards"] if (s["node"], s["hash"]) in bad}
        good = [s for s in row["shards"] if s["i"] not in bad_idx and is_alive(infos, s["node"])]
        got = {}
        for s in sorted(good, key=lambda s: s["i"]):
            blob = fetch_blob(infos[s["node"]]["url"], s["hash"], traffic)
            if blob is None:
                continue
            chunk_hash, index, payload = erasure.unpack_shard(blob)
            if chunk_hash == row["hash"] and index == s["i"]:
                got[index] = payload
            if len(got) == k:
                break
        if len(got) < k:
            return 0, len(bad)
        payloads = erasure.rebuild_shards(got, k, m, sorted(bad_idx))
        repaired = 0
        for s in row["shards"]:
            if s["i"] in bad_idx:
                blob = erasure.pack_shard(row["hash"], s["i"], payloads[s["i"]])
                if sha(blob) == s["hash"] and put_blob(infos[s["node"]]["url"], s["hash"], blob, traffic):
                    repaired += 1
        return repaired, len(bad) - repaired
    good_nodes = [n for n in row["nodes"] if (n, row["hash"]) not in bad and is_alive(infos, n)]
    data = None
    for n in good_nodes:
        data = fetch_blob(infos[n]["url"], row["hash"], traffic)
        if data is not None:
            break
    if data is None:
        return 0, len(bad)
    repaired = sum(1 for n, h in bad if put_blob(infos[n]["url"], h, data, traffic))
    return repaired, len(bad) - repaired


def verify_node(url, hashes):
    """Ask one node to re-read the given blobs. Returns (bytes_read, corrupt, missing)."""
    bytes_read, corrupt, missing = 0, [], []
    for i in range(0, len(hashes), 1000):
        r = call("POST", f"{url}/verify", json={"hashes": hashes[i:i + 1000]}, timeout=300)
        r.raise_for_status()
        j = r.json()
        bytes_read += j["bytes_read"]
        corrupt += j["corrupt"]
        missing += j["missing"]
    return bytes_read, corrupt, missing


def read_chunk_row(chunk_hash):
    with db.get_conn() as conn, db.cursor(conn) as cur:
        cur.execute(
            "SELECT hash, size, scheme, want, ec_k, ec_m, nodes, shards FROM chunks WHERE hash = %s AND ref_count > 0",
            (chunk_hash,),
        )
        return cur.fetchone()


def scrub_once(trigger="manual", orphans=True, grace=None):
    grace = ORPHAN_GRACE if grace is None else grace
    with _maint_lock:
        t0 = time.time()
        with db.get_conn() as conn, db.cursor(conn) as cur:
            cur.execute("INSERT INTO scrub_runs (trigger) VALUES (%s) RETURNING id", (trigger,))
            run_id = cur.fetchone()["id"]
        stats = Counter()
        traffic = Traffic()
        try:
            infos = cluster.node_infos()
            with db.get_conn() as conn, db.cursor(conn) as cur:
                cur.execute("SELECT hash, size, scheme, want, ec_k, ec_m, nodes, shards FROM chunks WHERE ref_count > 0")
                rows = cur.fetchall()

            expected = defaultdict(list)                    # node -> [(blob_hash, chunk_hash)]
            for r in rows:
                for node, blob_hash in blob_locations(r):
                    if is_alive(infos, node):
                        expected[node].append((blob_hash, r["hash"]))

            # 1. every node re-reads its own files and checks the hashes
            bad = defaultdict(set)                          # chunk_hash -> {(node, blob_hash)}
            corrupt_set, missing_set = set(), set()
            blob_to_chunk = {}
            for node, items in expected.items():
                for blob_hash, chunk_hash in items:
                    blob_to_chunk[(node, blob_hash)] = chunk_hash
                stats["copies_checked"] += len(items)

            def check(node):
                try:
                    return node, verify_node(infos[node]["url"], [b for b, _ in expected[node]])
                except Exception as e:
                    print(f"[scrub] node {node} could not be verified: {e}", flush=True)
                    return node, (0, [], [])

            with ThreadPoolExecutor(max(1, len(expected))) as pool:
                for node, (bytes_read, corrupt, missing) in pool.map(check, list(expected)):
                    stats["bytes_verified"] += bytes_read
                    stats["corrupt_found"] += len(corrupt)
                    stats["missing_found"] += len(missing)
                    corrupt_set.update((node, b) for b in corrupt)
                    missing_set.update((node, b) for b in missing)
                    for blob_hash in corrupt + missing:
                        bad[blob_to_chunk[(node, blob_hash)]].add((node, blob_hash))

            # 2. repair every bad copy from healthy ones
            for chunk_hash, bad_set in bad.items():
                row = read_chunk_row(chunk_hash)             # read it again: it may have been deleted since the scan started
                if row is None:
                    stats["copies_checked"] -= len(bad_set)
                    stats["corrupt_found"] -= sum(1 for n, b in bad_set if (n, b) in corrupt_set)
                    stats["missing_found"] -= sum(1 for n, b in bad_set if (n, b) in missing_set)
                    continue
                repaired, failed = repair_bad_copies(row, bad_set, infos, traffic)
                stats["repaired"] += repaired
                stats["unrepairable"] += failed
                print(f"[scrub] chunk {chunk_hash[:12]}: {len(bad_set)} bad copy(ies), repaired {repaired}", flush=True)

            # 3. orphan files: stored on a node but not known to the metadata (old copies of a returned node, failed uploads)
            if orphans:
                with db.get_conn() as conn, db.cursor(conn) as cur:
                    cur.execute("SELECT hash AS h FROM chunks UNION SELECT s->>'hash' FROM chunks, jsonb_array_elements(shards) s")
                    referenced = {r["h"] for r in cur.fetchall()}
                for node, info in infos.items():
                    if info["status"] != "alive":
                        continue
                    try:
                        listing = call("GET", f"{info['url']}/chunks", timeout=120).json()["chunks"]
                    except Exception:
                        continue
                    for item in listing:
                        if item["hash"] not in referenced and item["age_seconds"] >= grace:
                            try:
                                if call("DELETE", f"{info['url']}/chunks/{item['hash']}").status_code == 200:
                                    stats["orphans_deleted"] += 1
                            except httpx.HTTPError:
                                pass

            with db.get_conn() as conn, db.cursor(conn) as cur:
                cur.execute(
                    "UPDATE scrub_runs SET status='done', finished_at=now(), copies_checked=%s, bytes_verified=%s, "
                    "corrupt_found=%s, missing_found=%s, repaired=%s, unrepairable=%s, repair_bytes_read=%s, "
                    "repair_bytes_written=%s, orphans_deleted=%s, seconds=%s WHERE id=%s",
                    (stats["copies_checked"], stats["bytes_verified"], stats["corrupt_found"], stats["missing_found"],
                     stats["repaired"], stats["unrepairable"], traffic.read, traffic.written, stats["orphans_deleted"],
                     round(time.time() - t0, 3), run_id),
                )
            print(f"[scrub] run {run_id} done: {dict(stats)}", flush=True)
        except Exception as e:
            print(f"[scrub] run {run_id} failed: {e}", flush=True)
            with db.get_conn() as conn, db.cursor(conn) as cur:
                cur.execute("UPDATE scrub_runs SET status='failed', finished_at=now() WHERE id=%s", (run_id,))
        return run_id


def scrub_loop():
    time.sleep(SCRUB_INTERVAL)
    while True:
        try:
            scrub_once("background")
        except Exception as e:
            print(f"[scrub] error: {e}", flush=True)
        time.sleep(SCRUB_INTERVAL)


def start_background():
    threading.Thread(target=failure_detector_loop, daemon=True, name="failure-detector").start()
    threading.Thread(target=scrub_loop, daemon=True, name="scrubber").start()