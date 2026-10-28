"""Benchmark: replication (N=3) versus erasure coding (4+2) on the two workloads of the exam.

  (a) many small files   : 5,000 files of 1-50 KB
  (b) a few large files  : 10 files (scaled down: 16-48 MB each, use --large-min-mb / --large-max-mb for more)

For every workload and both schemes it measures
  - the storage overhead (real bytes on the disks of the nodes / bytes uploaded by the user)
  - the repair traffic when one node dies (bytes read from the surviving nodes + bytes written to the new nodes)

Run from the project root (all containers running):
    python tests/bench_redundancy.py
    python tests/bench_redundancy.py --quick          # small version, about 1 minute
Results are saved in results/redundancy_benchmark.json
"""
import argparse
import json
import math
import os
import random
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import httpx

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from common.chunking import CHUNK_SIZE  # noqa: E402
from common.client import ObjectClient, local_node_url, sha256_hex  # noqa: E402

META = "http://127.0.0.1:9000"
MB = 1024 * 1024
http = httpx.Client(timeout=600)
client = ObjectClient(workers=32, timeout=600)       # used for the PUT/GET of the chunks (the uploads run in other threads)


def docker(*args):
    return subprocess.run(["docker", *args], capture_output=True, text=True)


def nodes():
    return http.get(f"{META}/nodes").json()


def wait_until(condition, timeout, what):
    start = time.time()
    while time.time() - start < timeout:
        if condition():
            return time.time() - start
        time.sleep(0.5)
    raise SystemExit(f"Timeout while waiting for: {what}")


def disk_stats():
    """Real files and bytes on the disk of every node."""
    out = {}
    for n in nodes():
        s = http.get(f"{local_node_url(n['node_id'])}/stats").json()
        out[n["node_id"]] = (s["chunk_count"], s["used_bytes"])
    return out


def make_sizes(spec):
    rng = random.Random(f"sizes-{spec['name']}")
    return [rng.randint(spec["min_bytes"], spec["max_bytes"]) for _ in range(spec["files"])]


def make_data(spec, scheme, i, size):
    # the content is different for the two schemes, so deduplication never mixes the two experiments
    return random.Random(f"{spec['name']}-{scheme}-{i}").randbytes(size)


def run_case(spec, scheme, victim, workers):
    name = spec["name"]
    bucket = f"bench-{name}-{scheme}"
    if scheme == "replication":
        client.create_bucket(bucket, "replication", replicas=3, write_quorum=2, read_quorum=2)
    else:
        client.create_bucket(bucket, "erasure", k=4, m=2)
    sizes = make_sizes(spec)
    logical = sum(sizes)
    chunks = sum(math.ceil(s / CHUNK_SIZE) for s in sizes)
    base = disk_stats()
    print(f"\n--- {name} / {scheme}: {len(sizes)} files, {logical / MB:.1f} MB, {chunks} chunks", flush=True)

    # ---------------- upload
    digests = {}

    def upload(i):
        data = make_data(spec, scheme, i, sizes[i])
        client.put_object(bucket, f"obj-{i:05d}", data)
        digests[i] = sha256_hex(data)

    t0 = time.time()
    list(ThreadPoolExecutor(workers).map(upload, range(len(sizes))))
    upload_s = time.time() - t0
    after = disk_stats()
    disk_bytes = sum(after[n][1] - base[n][1] for n in after)
    disk_files = sum(after[n][0] - base[n][0] for n in after)
    print(f"    uploaded in {upload_s:.1f} s ({logical / MB / upload_s:.1f} MB/s); on the disks: {disk_bytes / MB:.1f} MB in {disk_files} files "
          f"-> overhead {disk_bytes / logical:.3f}x", flush=True)

    # ---------------- node failure and repair
    lost_files, lost_bytes = after[victim][0] - base[victim][0], after[victim][1] - base[victim][1]
    last = (http.get(f"{META}/repair/jobs", params={"limit": 1}).json() or [{"id": 0}])[0]["id"]
    docker("stop", f"storage-{victim}")
    detect_s = wait_until(lambda: next(n for n in nodes() if n["node_id"] == victim)["state"] == "failed", 120, "node marked failed")
    job = {}

    def done():
        nonlocal job
        js = [j for j in http.get(f"{META}/repair/jobs", params={"limit": 5}).json() if j["id"] > last and j["node_id"] == victim]
        job = js[0] if js else {}
        return job.get("status") in ("done", "failed")

    wait_until(done, 3600, "repair job finished")
    traffic = job["bytes_read"] + job["bytes_written"]
    print(f"    {victim} killed: {lost_files} files / {lost_bytes / MB:.1f} MB lost; marked failed after {detect_s:.0f} s; "
          f"repair job {job['seconds']} s: read {job['bytes_read'] / MB:.1f} MB + wrote {job['bytes_written'] / MB:.1f} MB = {traffic / MB:.1f} MB "
          f"(unrecoverable chunks: {job['chunks_unrecoverable']})", flush=True)

    # ---------------- integrity after the repair
    sample = random.Random(5).sample(range(len(sizes)), min(len(sizes), spec["verify"]))
    ok = 0
    for i in sample:
        ok += sha256_hex(client.get_object(bucket, f"obj-{i:05d}")) == digests[i]
    print(f"    after the repair {ok} of {len(sample)} sampled objects downloaded identical", flush=True)

    # ---------------- clean up (the returned node still has old files: the scrubber deletes these orphans)
    docker("start", f"storage-{victim}")
    wait_until(lambda: all(n["status"] == "alive" and n["state"] == "alive" for n in nodes()), 120, "all nodes alive again")
    list(ThreadPoolExecutor(workers).map(lambda i: client.delete_object(bucket, f"obj-{i:05d}"), range(len(sizes))))
    http.post(f"{META}/scrub/run", params={"grace": 0, "wait": "true"}, timeout=3600)
    end = disk_stats()
    leftover = sum(end[n][1] - base[n][1] for n in end)
    print(f"    cleaned up (bytes left on the disks: {leftover})", flush=True)

    return {
        "workload": name, "scheme": scheme, "files": len(sizes), "chunks": chunks, "logical_bytes": logical,
        "stored_bytes": disk_bytes, "stored_files": disk_files, "overhead": round(disk_bytes / logical, 3),
        "upload_seconds": round(upload_s, 1), "upload_mb_per_s": round(logical / MB / upload_s, 1),
        "failed_node": victim, "lost_files": lost_files, "lost_bytes": lost_bytes, "detect_seconds": round(detect_s, 1),
        "repair_seconds": job["seconds"], "repair_bytes_read": job["bytes_read"], "repair_bytes_written": job["bytes_written"],
        "repair_traffic_bytes": traffic, "repair_traffic_per_lost_byte": round(traffic / lost_bytes, 2) if lost_bytes else None,
        "repair_traffic_vs_logical_percent": round(traffic / logical * 100, 1),
        "pieces_rebuilt": job["copies_rebuilt"], "chunks_repaired": job["chunks_repaired"], "chunks_unrecoverable": job["chunks_unrecoverable"],
        "verified_ok": ok, "verified_total": len(sample), "leftover_bytes": leftover,
    }


def print_tables(results):
    print("\n\n================ STORAGE OVERHEAD ================")
    print(f"{'workload':8} {'scheme':12} {'files':>6} {'logical MB':>11} {'stored MB':>10} {'overhead':>9} {'files on nodes':>15} {'upload MB/s':>12}")
    print("-" * 92)
    for r in results:
        print(f"{r['workload']:8} {r['scheme']:12} {r['files']:>6} {r['logical_bytes'] / MB:>11.1f} {r['stored_bytes'] / MB:>10.1f} "
              f"{r['overhead']:>8.2f}x {r['stored_files']:>15} {r['upload_mb_per_s']:>12.1f}")
    print("\n================ REPAIR TRAFFIC (one node killed) ================")
    print(f"{'workload':8} {'scheme':12} {'lost MB':>8} {'read MB':>8} {'written MB':>11} {'total MB':>9} {'total/lost':>11} {'% of data':>10} {'repair s':>9}")
    print("-" * 96)
    for r in results:
        print(f"{r['workload']:8} {r['scheme']:12} {r['lost_bytes'] / MB:>8.1f} {r['repair_bytes_read'] / MB:>8.1f} {r['repair_bytes_written'] / MB:>11.1f} "
              f"{r['repair_traffic_bytes'] / MB:>9.1f} {r['repair_traffic_per_lost_byte']:>10.2f}x {r['repair_traffic_vs_logical_percent']:>9.1f}% {r['repair_seconds']:>9.1f}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--small-files", type=int, default=5000)
    ap.add_argument("--small-min-kb", type=int, default=1)
    ap.add_argument("--small-max-kb", type=int, default=50)
    ap.add_argument("--large-files", type=int, default=10)
    ap.add_argument("--large-min-mb", type=int, default=16)
    ap.add_argument("--large-max-mb", type=int, default=48)
    ap.add_argument("--victim", default="node-3", help="node that is killed to measure the repair")
    ap.add_argument("--only", choices=["small", "large"])
    ap.add_argument("--quick", action="store_true", help="500 small files and 4 large files of 8-16 MB")
    a = ap.parse_args()
    if a.quick:
        a.small_files, a.large_files, a.large_min_mb, a.large_max_mb = 500, 4, 8, 16

    small = {"name": "small", "files": a.small_files, "min_bytes": a.small_min_kb * 1024, "max_bytes": a.small_max_kb * 1024, "verify": 50, "workers": 16}
    large = {"name": "large", "files": a.large_files, "min_bytes": a.large_min_mb * MB, "max_bytes": a.large_max_mb * MB, "verify": 10, "workers": 3}
    specs = [s for s in (small, large) if a.only in (None, s["name"])]

    for n in nodes():                                  # all nodes must be up
        if not (http.get(f"{local_node_url(n['node_id'])}/health").status_code == 200):
            raise SystemExit(f"{n['node_id']} is not running")
    results = []
    for spec in specs:
        for scheme in ("replication", "erasure"):
            results.append(run_case(spec, scheme, a.victim, spec["workers"]))
    print_tables(results)
    os.makedirs(os.path.join(ROOT, "results"), exist_ok=True)
    out = os.path.join(ROOT, "results", "redundancy_benchmark.json")
    with open(out, "w") as f:
        json.dump({"nodes": len(nodes()), "replication": "N=3 W=2 R=2", "erasure": "Reed-Solomon 4+2", "results": results}, f, indent=2)
    print("\nSaved to results/redundancy_benchmark.json")


if __name__ == "__main__":
    main()