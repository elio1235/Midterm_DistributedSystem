"""Test the consistent hashing ring: balance, and how many chunks move when a node is added or removed.

Run from the project root:
    python tests/test_ring_movement.py
"""
import hashlib
import json
import os
import statistics
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from common.ring import HashRing  # noqa: E402

NUM_CHUNKS = 100_000
REPLICAS = 3
VNODES = 128

HASHES = [hashlib.sha256(f"chunk-{i}".encode()).hexdigest() for i in range(NUM_CHUNKS)]


def place(ring, n):
    return {h: ring.get_nodes(h, n) for h in HASHES}


def movement(old, new, n):
    """% of chunks whose primary node changed, and % of replica copies that must be created on another node."""
    primary = sum(1 for h in HASHES if old[h][0] != new[h][0]) / len(HASHES) * 100
    copies = sum(len(set(new[h]) - set(old[h])) for h in HASHES) / (len(HASHES) * n) * 100
    return primary, copies


def modulo_movement(old_count, new_count):
    """The naive way: node = hash mod number_of_nodes."""
    pos = [int(h[:16], 16) for h in HASHES]
    return sum(1 for p in pos if p % old_count != p % new_count) / len(pos) * 100


def main():
    results = {"chunks": NUM_CHUNKS, "replicas": REPLICAS, "vnodes": VNODES}
    nodes4 = ["node-1", "node-2", "node-3", "node-4"]

    # 1. load balance for different numbers of virtual nodes
    print(f"1) Load balance: {NUM_CHUNKS:,} chunks on 4 nodes (primary copy), by number of virtual nodes\n")
    print(f"{'vnodes/node':>12} {'min %':>8} {'max %':>8} {'std dev':>8}")
    print("-" * 40)
    results["balance"] = []
    for v in (1, 8, 32, 128, 512):
        ring = HashRing(nodes4, vnodes=v)
        counts = {n: 0 for n in nodes4}
        for h in HASHES:
            counts[ring.get_nodes(h, 1)[0]] += 1
        shares = [c / NUM_CHUNKS * 100 for c in counts.values()]
        print(f"{v:>12} {min(shares):>8.1f} {max(shares):>8.1f} {statistics.pstdev(shares):>8.2f}")
        results["balance"].append({"vnodes": v, "min": round(min(shares), 1), "max": round(max(shares), 1),
                                   "std": round(statistics.pstdev(shares), 2)})
    print("(ideal share is 25.0 % per node)")

    # 2. movement when a node is added or removed
    ring4 = HashRing(nodes4, vnodes=VNODES)
    before = place(ring4, REPLICAS)

    ring5 = HashRing(nodes4 + ["node-5"], vnodes=VNODES)
    after_add = place(ring5, REPLICAS)

    ring3 = HashRing(["node-1", "node-2", "node-3"], vnodes=VNODES)
    after_remove = place(ring3, REPLICAS)

    print(f"\n2) Movement of data ({NUM_CHUNKS:,} chunks, {REPLICAS} replicas, {VNODES} virtual nodes per node)\n")
    print(f"{'event':32} {'primary moved':>14} {'replica copies moved':>21} {'modulo hashing':>15}")
    print("-" * 86)
    results["movement"] = {}
    for label, new, old_n, new_n, ideal in (
        ("add node-5 (4 -> 5 nodes)", after_add, 4, 5, 1 / 5 * 100),
        ("remove node-4 (4 -> 3 nodes)", after_remove, 4, 3, 1 / 4 * 100),
    ):
        primary, copies = movement(before, new, REPLICAS)
        naive = modulo_movement(old_n, new_n)
        print(f"{label:32} {primary:>13.1f}% {copies:>20.1f}% {naive:>14.1f}%")
        results["movement"][label] = {"primary_moved_percent": round(primary, 1), "replica_copies_moved_percent": round(copies, 1),
                                      "modulo_primary_moved_percent": round(naive, 1), "ideal_percent": round(ideal, 1)}
    print("\nIdeal value for the ring: 1/5 = 20.0 % when adding a node to 4, 1/4 = 25.0 % when removing one of 4.")

    os.makedirs(os.path.join(ROOT, "results"), exist_ok=True)
    with open(os.path.join(ROOT, "results", "ring_movement.json"), "w") as f:
        json.dump(results, f, indent=2)
    print("Saved to results/ring_movement.json")


if __name__ == "__main__":
    main()