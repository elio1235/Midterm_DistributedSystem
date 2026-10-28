"""End-to-end demo of Step 2: chunking, placement on the ring, metadata in PostgreSQL,
deduplication and reference counting.

Run from the project root (containers must be running):
    python tests/demo_metadata.py upload     # store 4 objects, show dedup
    python tests/demo_metadata.py delete     # delete them one by one, show reference counting
    python tests/demo_metadata.py            # both phases
"""
import hashlib
import os
import random
import sys

import httpx

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from common.chunking import chunk_bytes  # noqa: E402

META = "http://localhost:9000"
BUCKET = "demo"
MB = 1024 * 1024
client = httpx.Client(timeout=120)
failures = 0


def check(name, condition, detail=""):
    global failures
    if not condition:
        failures += 1
    print(f"[{'PASS' if condition else 'FAIL'}] {name} {detail}")


def brief(res):
    return (f"-> version {res['version']}, chunks {res['chunks_total']}, "
            f"uploaded {res['chunks_uploaded']}, reused {res['chunks_reused']}")


def brief_delete(res):
    return (f"-> versions deleted {res['versions_deleted']}, chunks freed {res['chunks_freed']}, "
            f"still referenced {res['chunks_still_referenced']}, node errors {res['node_delete_errors']}")


def host_url(node_id):
    """The nodes are published on my computer as localhost:8001 ... localhost:8004."""
    return f"http://localhost:{8000 + int(node_id.split('-')[1])}"


def make_data():
    a = random.Random(42).randbytes(12 * MB)                  # 3 chunks of 4 MB
    b = a[:8 * MB] + random.Random(43).randbytes(4 * MB)     # same first 2 chunks, new last chunk
    return a, b


def upload(key, data, method="fixed"):
    chunks = list(chunk_bytes(data, method))                  # [(hash, bytes)] in file order
    unique = {}
    for h, piece in chunks:
        unique.setdefault(h, piece)

    # 1. ask the metadata service: which chunks already exist, where should the new ones go?
    plan = client.post(f"{META}/chunks/plan", json={"chunks": [{"hash": h, "size": len(p)} for h, p in unique.items()]})
    plan.raise_for_status()

    # 2. send only the missing chunks to their target nodes
    uploaded = []
    for item in plan.json()["chunks"]:
        if item["exists"]:
            continue
        piece = unique[item["hash"]]
        for target in item["targets"]:
            r = client.put(f"{host_url(target['node_id'])}/chunks/{item['hash']}", content=piece)
            r.raise_for_status()
        uploaded.append({"hash": item["hash"], "size": len(piece), "nodes": [t["node_id"] for t in item["targets"]]})

    # 3. save the metadata: ordered chunk list, version, reference counts
    r = client.post(f"{META}/objects", json={
        "bucket": BUCKET, "key": key, "size": len(data), "chunking": method,
        "chunk_hashes": [h for h, _ in chunks], "uploaded": uploaded,
    })
    r.raise_for_status()
    return r.json(), [h for h, _ in chunks]


def download(key):
    meta = client.get(f"{META}/objects/{BUCKET}/{key}").json()
    out = bytearray()
    for c in meta["chunks"]:
        for n in c["nodes"]:
            r = client.get(f"{host_url(n['node_id'])}/chunks/{c['hash']}")
            if r.status_code == 200:
                out += r.content
                break
        else:
            raise RuntimeError(f"chunk {c['hash'][:12]} not readable from any node")
    return bytes(out)


def chunk_hashes(data):
    return [h for h, _ in chunk_bytes(data, "fixed")]


def head_ok(node_id, h):
    return client.head(f"{host_url(node_id)}/chunks/{h}").status_code == 200


def ref_count(h):
    r = client.get(f"{META}/chunks/{h}")
    return r.json()["ref_count"] if r.status_code == 200 else None


def phase_upload():
    a, b = make_data()
    ha, hb = chunk_hashes(a), chunk_hashes(b)
    c1, c2, c3 = ha
    c4 = hb[2]

    print("=== 0. Clean start and ring information ===")
    for key in ("a.bin", "a-copy.bin", "b.bin"):
        client.delete(f"{META}/objects/{BUCKET}/{key}")
    r = client.post(f"{META}/buckets/{BUCKET}")
    check("bucket 'demo' ready", r.status_code == 200, f"-> {r.json()}")
    ring = client.get(f"{META}/ring").json()
    check("ring has 4 nodes", len(ring["nodes"]) == 4, f"-> {ring['nodes']}, {ring['vnodes_per_node']} virtual nodes each")

    print("\n=== 1. Upload a.bin (12 MB, fixed 4 MB chunks) ===")
    res, _ = upload("a.bin", a)
    check("version 1, 3 chunks, all new", res["version"] == 1 and res["chunks_total"] == 3 and res["chunks_uploaded"] == 3, f"{brief(res)}")
    meta = client.get(f"{META}/objects/{BUCKET}/a.bin").json()
    print("Placement of the chunks of a.bin on the ring:")
    for i, c in enumerate(meta["chunks"], 1):
        print(f"   chunk {i}: {c['hash'][:16]}...  {c['size'] // MB} MB  ->  " + ", ".join(n["node_id"] for n in c["nodes"]))

    print("\n=== 2. Upload the same data again as a-copy.bin (deduplication) ===")
    res, _ = upload("a-copy.bin", a)
    check("0 chunks uploaded, 3 reused", res["chunks_uploaded"] == 0 and res["chunks_reused"] == 3, f"{brief(res)}")

    print("\n=== 3. Upload b.bin (first 8 MB equal to a.bin, last 4 MB different) ===")
    res, _ = upload("b.bin", b)
    check("1 chunk uploaded, 2 reused", res["chunks_uploaded"] == 1 and res["chunks_reused"] == 2, f"{brief(res)}")

    print("\n=== 4. Upload a.bin again (new version of the same key) ===")
    res, _ = upload("a.bin", a)
    check("version 2 created, nothing uploaded", res["version"] == 2 and res["chunks_uploaded"] == 0, f"{brief(res)}")

    print("\n=== 5. Reference counts and replicas ===")
    expected = {"chunk 1": (c1, 4), "chunk 2": (c2, 4), "chunk 3": (c3, 3), "chunk 4 (only in b.bin)": (c4, 1)}
    for name, (h, refs) in expected.items():
        info = client.get(f"{META}/chunks/{h}").json()
        on_nodes = all(head_ok(n, h) for n in info["nodes"])
        ring_nodes = {t["node_id"] for t in client.get(f"{META}/ring/placement/{h}").json()["targets"]}
        check(f"{name}: ref_count = {refs}", info["ref_count"] == refs, f"-> ref_count={info['ref_count']}")
        check(f"{name}: stored on 3 different nodes, as the ring says", len(set(info["nodes"])) == 3 and set(info["nodes"]) == ring_nodes and on_nodes,
              f"-> {info['nodes']}")

    print("\n=== 6. Download and verify ===")
    check("a.bin downloads identical", hashlib.sha256(download("a.bin")).digest() == hashlib.sha256(a).digest())
    check("b.bin downloads identical", hashlib.sha256(download("b.bin")).digest() == hashlib.sha256(b).digest())

    print("\n=== 7. Deduplication statistics ===")
    s = client.get(f"{META}/stats/dedup").json()
    print(f"   object versions stored : {s['object_versions']}")
    print(f"   logical size           : {s['logical_bytes'] / MB:.0f} MB (what the users uploaded)")
    print(f"   unique chunks          : {s['unique_chunks']} = {s['unique_bytes'] / MB:.0f} MB (stored once)")
    print(f"   with 3 replicas        : {s['stored_bytes_with_replicas'] / MB:.0f} MB on disk")
    print(f"   dedup ratio            : {s['dedup_ratio']}x, space saved {s['space_saved_percent']} %")
    check("4 unique chunks (16 MB) for 48 MB of objects", s["unique_chunks"] == 4 and s["unique_bytes"] == 16 * MB and s["logical_bytes"] == 48 * MB)


def phase_delete():
    a, b = make_data()
    ha, hb = chunk_hashes(a), chunk_hashes(b)
    c1, c2, c3 = ha
    c4 = hb[2]
    if client.get(f"{META}/objects/{BUCKET}/b.bin").status_code != 200:
        print("Nothing to delete. Run:  python tests/demo_metadata.py upload  first.")
        sys.exit(1)

    def nodes_of(h):
        return client.get(f"{META}/chunks/{h}").json()["nodes"]

    n1, n3, n4 = nodes_of(c1), nodes_of(c3), nodes_of(c4)

    print("=== 1. Delete a.bin (2 versions). Its chunks are still used by a-copy.bin and b.bin ===")
    r = client.delete(f"{META}/objects/{BUCKET}/a.bin").json()
    check("2 versions deleted, no chunk freed", r["versions_deleted"] == 2 and r["chunks_freed"] == 0, brief_delete(r))
    check("ref counts: chunk1=2 chunk2=2 chunk3=1", (ref_count(c1), ref_count(c2), ref_count(c3)) == (2, 2, 1))
    check("chunk 1 still on its nodes", all(head_ok(n, c1) for n in n1))

    print("\n=== 2. Delete a-copy.bin. Chunk 3 is not used by anybody anymore ===")
    r = client.delete(f"{META}/objects/{BUCKET}/a-copy.bin").json()
    check("1 chunk freed (chunk 3)", r["chunks_freed"] == 1 and r["freed_hashes"] == [c3], brief_delete(r))
    check("chunk 3 removed from all its nodes", not any(head_ok(n, c3) for n in n3))
    check("chunk 1 and 2 are kept for b.bin (ref_count = 1)", ref_count(c1) == 1 and ref_count(c2) == 1 and all(head_ok(n, c1) for n in n1))

    print("\n=== 3. Delete b.bin. Now nothing references the chunks ===")
    r = client.delete(f"{META}/objects/{BUCKET}/b.bin").json()
    check("3 chunks freed", r["chunks_freed"] == 3 and r["node_delete_errors"] == 0, brief_delete(r))
    check("chunk 1 and chunk 4 removed from the nodes", not any(head_ok(n, c1) for n in n1) and not any(head_ok(n, c4) for n in n4))
    s = client.get(f"{META}/stats/dedup").json()
    check("no chunks left in the metadata", s["unique_chunks"] == 0 and s["logical_bytes"] == 0,
          f"-> unique chunks {s['unique_chunks']}, logical bytes {s['logical_bytes']}")
    check("deleting again returns 404", client.delete(f"{META}/objects/{BUCKET}/b.bin").status_code == 404)


def main():
    phase = sys.argv[1] if len(sys.argv) > 1 else "all"
    if phase not in ("upload", "delete", "all"):
        print(__doc__)
        sys.exit(2)
    if phase in ("upload", "all"):
        phase_upload()
    if phase in ("delete", "all"):
        if phase == "all":
            print()
        phase_delete()
    print(f"\n{'ALL TESTS PASSED' if failures == 0 else str(failures) + ' TEST(S) FAILED'}")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()