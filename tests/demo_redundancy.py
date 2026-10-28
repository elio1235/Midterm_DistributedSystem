"""End-to-end demo of Step 3: quorum replication, erasure coding, node failure + repair, scrubber.

Run from the project root (all containers must be running):
    python tests/demo_redundancy.py quorum     # replication with W and R
    python tests/demo_redundancy.py ec         # erasure coding 4+2
    python tests/demo_redundancy.py repair     # a node fails, the repair job rebuilds its chunks
    python tests/demo_redundancy.py scrub      # bit rot is found and repaired by the scrubber
    python tests/demo_redundancy.py            # all four phases
"""
import os
import random
import subprocess
import sys
import time
from collections import Counter

import httpx

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from common.chunking import chunk_bytes  # noqa: E402
from common.client import ClusterError, ObjectClient, QuorumError, local_node_url  # noqa: E402

META = "http://127.0.0.1:9000"
MB = 1024 * 1024
client = ObjectClient()
http = httpx.Client(timeout=120)
failures = 0
KEYS = ["q-write.bin", "q-partial.bin", "q-refused.bin", "q-read.bin", "ec-object.bin", "repair-object.bin", "scrub-object.bin"]


def check(name, condition, detail=""):
    global failures
    if not condition:
        failures += 1
    print(f"[{'PASS' if condition else 'FAIL'}] {name} {detail}")


def data(seed, size):
    return random.Random(seed).randbytes(size)


# ------------------------------------------------------------------ helpers: containers and cluster state
def docker(*args):
    return subprocess.run(["docker", *args], capture_output=True, text=True)


STOPPED = set()


def stop_node(node_id):
    docker("stop", f"storage-{node_id}")
    STOPPED.add(node_id)


def start_node(node_id):
    docker("start", f"storage-{node_id}")
    STOPPED.discard(node_id)


def node_up(node_id):
    try:
        return http.get(f"{local_node_url(node_id)}/health", timeout=2).status_code == 200
    except httpx.HTTPError:
        return False


def nodes():
    return http.get(f"{META}/nodes").json()


def wait_until(condition, timeout, what):
    start = time.time()
    while time.time() - start < timeout:
        if condition():
            return time.time() - start
        time.sleep(1)
    raise SystemExit(f"Timeout while waiting for: {what}")


def all_alive():
    ns = nodes()
    return len(ns) >= 6 and all(n["status"] == "alive" and n["state"] == "alive" for n in ns)


def prepare():
    """Start from a healthy cluster with the demo buckets and without objects of earlier runs."""
    for n in nodes():
        if not node_up(n["node_id"]):
            start_node(n["node_id"])
    wait_until(lambda: all(node_up(n["node_id"]) for n in nodes()), 60, "all nodes answering")
    wait_until(all_alive, 60, "all nodes alive")
    client.create_bucket("rep", "replication", replicas=3, write_quorum=2, read_quorum=2)
    client.create_bucket("ec", "erasure", k=4, m=2)
    for bucket in ("rep", "ec"):
        for key in KEYS:
            client._meta("DELETE", f"/objects/{bucket}/{key}")
    http.post(f"{META}/repair/run", params={"wait": "true", "if_needed": "true"})


def restore():
    """Start every stopped container and wait until the cluster is healthy again."""
    for node_id in sorted(STOPPED):
        start_node(node_id)
    wait_until(lambda: all(node_up(n["node_id"]) for n in nodes()), 60, "all nodes answering again")
    wait_until(all_alive, 60, "all nodes alive again")
    http.post(f"{META}/repair/run", params={"wait": "true", "if_needed": "true"})


def chunk_meta(bucket, key):
    return http.get(f"{META}/objects/{bucket}/{key}").json()


def chunk_row(h):
    return http.get(f"{META}/chunks/{h}").json()


def placement(h, n=3):
    return [t["node_id"] for t in http.get(f"{META}/ring/placement/{h}", params={"n": n}).json()["targets"]]


def chunk_hash_of(payload):
    return next(iter(chunk_bytes(payload, "fixed")))[0]


def find_data(start_seed, size, must_include, n=3):
    """Find random data whose (first) chunk is placed on all the given nodes by the ring."""
    seed = start_seed
    while True:
        payload = data(seed, size)
        if set(must_include) <= set(placement(chunk_hash_of(payload), n)):
            return payload, seed
        seed += 1


def node_get_status(node_id, blob_hash):
    return http.get(f"{local_node_url(node_id)}/chunks/{blob_hash}").status_code


def blob_path(blob_hash):
    return f"/data/{blob_hash[:2]}/{blob_hash}"


# ------------------------------------------------------------------ phase 1: replication with quorum
def phase_quorum():
    prepare()
    print("=== 1. The quorum rule W + R > N is checked when a bucket is created")
    r = http.post(f"{META}/buckets/bad-bucket", json={"redundancy": "replication", "replicas": 3, "write_quorum": 1, "read_quorum": 1})
    check("N=3, W=1, R=1 is refused", r.status_code == 400, f"-> {r.json()['detail']}")
    cfg = http.get(f"{META}/buckets").json()
    rep = next(b for b in cfg if b["name"] == "rep")
    check("bucket 'rep': N=3, W=2, R=2 (2 + 2 > 3)", (rep["want"], rep["write_quorum"], rep["read_quorum"]) == (3, 2, 2), f"-> {rep}")

    print("\n=== 2. Normal write: all 3 replicas confirm")
    payload = data(101, 4 * MB)
    res = client.put_object("rep", "q-write.bin", payload)
    h = chunk_hash_of(payload)
    check("3 of 3 replicas confirmed", res["acks"][h] == 3, f"-> chunk {h[:12]} stored on {chunk_row(h)['nodes']}")

    print("\n=== 3. One replica node is down: the write still succeeds, because W = 2")
    victim = "node-3"
    payload, _ = find_data(200, 4 * MB, [victim])
    h = chunk_hash_of(payload)
    targets = placement(h)
    print(f"   the ring wants to store chunk {h[:12]} on {targets}")
    stop_node(victim)
    print(f"   {victim} stopped")
    res = client.put_object("rep", "q-partial.bin", payload)
    row = chunk_row(h)
    check("write accepted with 2 of 3 replicas (W = 2 reached)", res["acks"][h] == 2 and victim not in row["nodes"], f"-> stored on {row['nodes']}")
    check("the chunk is now under-replicated (2 copies, 3 wanted)", len(row["nodes"]) < row["want"], f"-> copies {len(row['nodes'])}, wanted {row['want']}")

    print("\n=== 4. Two replica nodes are down: the write is refused, because only 1 < W confirmations are possible")
    second = next(n for n in targets if n != victim)
    payload2, _ = find_data(400, 4 * MB, [victim, second])
    h2 = chunk_hash_of(payload2)
    stop_node(second)
    print(f"   {second} stopped too (chunk {h2[:12]} wanted on {placement(h2)})")
    try:
        client.put_object("rep", "q-refused.bin", payload2)
        refused = False
        why = ""
    except QuorumError as e:
        refused, why = True, str(e)
    check("QuorumError: write refused", refused, f"-> {why}")
    check("nothing was saved in the metadata", http.get(f"{META}/objects/rep/q-refused.bin").status_code == 404)
    restore()
    row = chunk_row(h)
    check("after the nodes are back, the repair job restored the missing replica", len(row["nodes"]) == 3, f"-> stored on {row['nodes']}")
    check("the chunk is still readable and identical", client.get_object("rep", "q-partial.bin") == payload)

    print("\n=== 5. Quorum read (R = 2): one node sends the data, another node confirms that it holds the chunk")
    payload = data(501, 4 * MB)
    client.put_object("rep", "q-read.bin", payload)
    h = chunk_hash_of(payload)
    a, b, c = chunk_row(h)["nodes"]
    check("all replicas up: read works", client.get_object("rep", "q-read.bin") == payload, f"-> replicas on {[a, b, c]}")
    stop_node(a)
    check(f"{a} down, 2 replicas left (R = 2): read works", client.get_object("rep", "q-read.bin") == payload)
    stop_node(b)
    try:
        client.get_object("rep", "q-read.bin")
        refused, why = False, ""
    except QuorumError as e:
        refused, why = True, str(e)
    check(f"{a} and {b} down, only 1 replica left (< R): read refused", refused, f"-> {why}")
    restore()
    check("all nodes back: read works again", client.get_object("rep", "q-read.bin") == payload)


# ------------------------------------------------------------------ phase 2: erasure coding
def phase_ec():
    prepare()
    print("=== 1. Upload 8 MB into the bucket 'ec' (Reed-Solomon 4+2): every 4 MB chunk becomes 6 shards")
    b = next(x for x in http.get(f"{META}/buckets").json() if x["name"] == "ec")
    check("bucket 'ec' is erasure coded 4+2", (b["scheme"], b["k"], b["m"]) == ("erasure", 4, 2), f"-> {b}")
    payload = data(601, 8 * MB)
    res = client.put_object("ec", "ec-object.bin", payload)
    meta = chunk_meta("ec", "ec-object.bin")
    print("   shard placement (shards 0-3 = data, 4-5 = parity):")
    for i, c in enumerate(meta["chunks"], 1):
        print(f"   chunk {i} ({c['hash'][:12]}):  " + "  ".join(f"#{s['i']}->{s['node_id']}" for s in c["shards"]))
    check("every chunk has 6 shards on 6 different nodes",
          all(len(c["shards"]) == 6 and len({s['node_id'] for s in c['shards']}) == 6 for c in meta["chunks"]))
    check("6 of 6 shards confirmed for every chunk", all(n == 6 for n in res["acks"].values()))

    print("\n=== 2. Storage overhead")
    stored = sum(chunk_row(c["hash"])["stored_bytes"] for c in meta["chunks"])
    ratio = stored / len(payload)
    print(f"   logical size {len(payload) / MB:.2f} MB, stored {stored / MB:.2f} MB on the nodes, overhead {ratio:.3f}x (3 replicas would need 3.000x)")
    check("overhead is about 1.5x (6/4)", 1.49 < ratio < 1.52)
    check("download is identical", client.get_object("ec", "ec-object.bin") == payload)

    print("\n=== 3. Up to m = 2 nodes can be lost: degraded read")
    first = [s["node_id"] for s in meta["chunks"][0]["shards"]]
    stop_node(first[0])
    stop_node(first[1])
    print(f"   {first[0]} and {first[1]} stopped (each of them holds one shard of chunk 1)")
    check("2 shards of chunk 1 are lost, the 4 others rebuild it: download identical", client.get_object("ec", "ec-object.bin") == payload)

    print("\n=== 4. Three nodes down (more than m): the data cannot be rebuilt")
    stop_node(first[2])
    print(f"   {first[2]} stopped too")
    try:
        client.get_object("ec", "ec-object.bin")
        refused, why = False, ""
    except ClusterError as e:
        refused, why = True, str(e)
    check("read refused: only 3 shards left, k = 4 are needed", refused, f"-> {why}")
    restore()
    check("nodes back: download identical again", client.get_object("ec", "ec-object.bin") == payload)


# ------------------------------------------------------------------ phase 3: node failure and repair
def phase_repair():
    prepare()
    print("=== 1. Store 12 MB with replication and 12 MB with erasure coding")
    pr, pe = data(701, 12 * MB), data(702, 12 * MB)
    client.put_object("rep", "repair-object.bin", pr)
    client.put_object("ec", "repair-object.bin", pe)
    holders = Counter()
    for bucket in ("rep", "ec"):
        for c in chunk_meta(bucket, "repair-object.bin")["chunks"]:
            for n in c["nodes"]:
                holders[n["node_id"]] += 1
    victim = holders.most_common(1)[0][0]
    print(f"   pieces per node (chunks or shards): {dict(sorted(holders.items()))}")
    check("both objects downloaded identical before the failure",
          client.get_object("rep", "repair-object.bin") == pr and client.get_object("ec", "repair-object.bin") == pe)
    last_job = (http.get(f"{META}/repair/jobs", params={"limit": 1}).json() or [{"id": 0}])[0]["id"]

    print(f"\n=== 2. {victim} is stopped (its container is killed). The metadata service waits for missed heartbeats")
    stop_node(victim)
    took = wait_until(lambda: next(n for n in nodes() if n["node_id"] == victim)["state"] == "failed", 40, "node marked failed")
    n = next(n for n in nodes() if n["node_id"] == victim)
    check(f"{victim} marked FAILED after {took:.0f} s without heartbeat", n["status"] == "failed" and n["state"] == "failed", f"-> failed_at {n['failed_at']}")

    print("\n=== 3. The repair job is started automatically")
    job = None

    def job_done():
        nonlocal job
        jobs = [j for j in http.get(f"{META}/repair/jobs", params={"limit": 5}).json() if j["id"] > last_job and j["node_id"] == victim]
        job = jobs[0] if jobs else None
        return job is not None and job["status"] in ("done", "failed")

    wait_until(job_done, 120, "repair job finished")
    print(f"   job {job['id']}: reason '{job['reason']}', {job['chunks_total']} chunks to repair, {job['seconds']} s")
    print(f"   repaired {job['chunks_repaired']}, partial {job['chunks_partial']}, unrecoverable {job['chunks_unrecoverable']}, "
          f"pieces rebuilt {job['copies_rebuilt']}")
    print(f"   repair traffic: read {job['bytes_read'] / MB:.1f} MB from the surviving nodes, wrote {job['bytes_written'] / MB:.1f} MB to the new nodes")
    check("repair job finished and nothing was lost", job["status"] == "done" and job["chunks_unrecoverable"] == 0 and job["chunks_partial"] == 0)

    print("\n=== 4. Check the result")
    healthy = True
    for bucket in ("rep", "ec"):
        for c in chunk_meta(bucket, "repair-object.bin")["chunks"]:
            alive = [x for x in c["nodes"] if x["status"] == "alive"]
            healthy = healthy and len(alive) == c["want"] and all(x["node_id"] != victim for x in c["nodes"])
    check(f"every chunk is complete again (3 replicas / 6 shards) and no longer uses {victim}", healthy)
    check("replicated object downloads identical", client.get_object("rep", "repair-object.bin") == pr)
    check("erasure coded object downloads identical", client.get_object("ec", "repair-object.bin") == pe)
    restore()


# ------------------------------------------------------------------ phase 4: scrubber
def phase_scrub():
    prepare()
    print("=== 1. Store 8 MB (2 chunks) with replication and 4 MB (1 chunk) with erasure coding")
    pr, pe = data(801, 8 * MB), data(802, 4 * MB)
    client.put_object("rep", "scrub-object.bin", pr)
    client.put_object("ec", "scrub-object.bin", pe)
    rep_chunks = chunk_meta("rep", "scrub-object.bin")["chunks"]
    (hr1, nodes1), (hr2, nodes2) = [(c["hash"], [x["node_id"] for x in c["nodes"]]) for c in rep_chunks]
    he = chunk_meta("ec", "scrub-object.bin")["chunks"][0]["hash"]
    shard = chunk_meta("ec", "scrub-object.bin")["chunks"][0]["shards"][1]

    print("\n=== 2. Damage the files on the disks of the nodes (bit rot)")
    docker("exec", f"storage-{nodes1[0]}", "sh", "-c", f"printf 'BITROT' >> {blob_path(hr1)}")
    docker("exec", f"storage-{shard['node_id']}", "sh", "-c", f"printf 'BITROT' >> {blob_path(shard['hash'])}")
    docker("exec", f"storage-{nodes2[1]}", "sh", "-c", f"rm {blob_path(hr2)}")
    print(f"   replica of chunk {hr1[:12]} on {nodes1[0]}: bytes appended (corrupted)")
    print(f"   shard #{shard['i']} of chunk {he[:12]} on {shard['node_id']}: bytes appended (corrupted)")
    print(f"   replica of chunk {hr2[:12]} on {nodes2[1]}: file deleted")
    check("the node refuses to serve the corrupted replica (HTTP 422)", node_get_status(nodes1[0], hr1) == 422)
    check("the node refuses to serve the corrupted shard (HTTP 422)", node_get_status(shard["node_id"], shard["hash"]) == 422)
    check("the deleted replica is missing (HTTP 404)", node_get_status(nodes2[1], hr2) == 404)
    check("reads still work: the hash of every copy is checked and the bad copies are skipped",
          client.get_object("rep", "scrub-object.bin") == pr and client.get_object("ec", "scrub-object.bin") == pe)

    print("\n=== 3. Run the scrubber (it also runs by itself every SCRUB_INTERVAL seconds)")
    run = http.post(f"{META}/scrub/run", params={"grace": 0, "wait": "true"}, timeout=300).json()
    print(f"   run {run['id']}: {run['copies_checked']} copies checked ({run['bytes_verified'] / MB:.1f} MB re-read on the nodes) in {run['seconds']} s")
    print(f"   corrupted found {run['corrupt_found']}, missing found {run['missing_found']}, repaired {run['repaired']}, "
          f"not repairable {run['unrepairable']}, orphan files deleted {run['orphans_deleted']}")
    print(f"   repair traffic: read {run['repair_bytes_read'] / MB:.1f} MB, wrote {run['repair_bytes_written'] / MB:.1f} MB")
    check("the scrubber found the 2 corrupted copies and the missing copy", run["corrupt_found"] == 2 and run["missing_found"] == 1)
    check("all 3 were repaired from healthy copies", run["repaired"] == 3 and run["unrepairable"] == 0)

    print("\n=== 4. Check the result")
    check("corrupted replica is good again (HTTP 200)", node_get_status(nodes1[0], hr1) == 200)
    check("corrupted shard is good again (HTTP 200)", node_get_status(shard["node_id"], shard["hash"]) == 200)
    check("deleted replica is back (HTTP 200)", node_get_status(nodes2[1], hr2) == 200)
    run2 = http.post(f"{META}/scrub/run", params={"grace": 0, "wait": "true"}, timeout=300).json()
    check("a second scrub run finds nothing wrong", run2["corrupt_found"] == 0 and run2["missing_found"] == 0,
          f"-> checked {run2['copies_checked']} copies, problems 0")
    check("both objects downloaded identical", client.get_object("rep", "scrub-object.bin") == pr and client.get_object("ec", "scrub-object.bin") == pe)


PHASES = {"quorum": phase_quorum, "ec": phase_ec, "repair": phase_repair, "scrub": phase_scrub}


def main():
    which = sys.argv[1] if len(sys.argv) > 1 else "all"
    if which not in PHASES and which != "all":
        print(__doc__)
        sys.exit(2)
    for name, fn in PHASES.items():
        if which in (name, "all"):
            if which == "all":
                print(f"\n################ phase: {name}")
            fn()
    print(f"\n{'ALL TESTS PASSED' if failures == 0 else str(failures) + ' TEST(S) FAILED'}")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()