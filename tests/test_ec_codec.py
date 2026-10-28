"""Test the Reed-Solomon codec (zfec): lose shards on purpose and rebuild the chunk.

Run from the project root (no Docker needed):
    python tests/test_ec_codec.py
"""
import itertools
import os
import random
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from common import erasure  # noqa: E402

MB = 1024 * 1024
failures = 0


def check(name, condition, detail=""):
    global failures
    if not condition:
        failures += 1
    print(f"[{'PASS' if condition else 'FAIL'}] {name} {detail}")


def try_rebuild(shards, lost, k, m, size, original):
    """Remove the 'lost' shards and try to rebuild the chunk from the rest."""
    available = {i: p for i, p in enumerate(shards) if i not in lost}
    if len(available) < k:
        return False
    return erasure.decode(available, k, m, size) == original


def main():
    data = random.Random(7).randbytes(4 * MB + 123)          # not a multiple of k: tests the padding too
    print(f"Chunk of {len(data):,} bytes, cut in k data blocks, m parity blocks are added.\n")

    for k, m in ((4, 2), (6, 3), (2, 1)):
        shards = erasure.encode(data, k, m)
        n = k + m
        print(f"=== Reed-Solomon {k}+{m}: {n} shards of {len(shards[0]):,} bytes, overhead {n / k:.2f}x (replication x3 = 3.00x)")
        print(f"{'shards lost':>12} {'combinations':>13} {'rebuilt OK':>11}   expected")
        block_ok = True
        for lost_count in range(0, m + 2):
            combos = list(itertools.combinations(range(n), lost_count))
            ok = sum(try_rebuild(shards, set(c), k, m, len(data), data) for c in combos)
            expected = len(combos) if lost_count <= m else 0
            block_ok = block_ok and ok == expected
            print(f"{lost_count:>12} {len(combos):>13} {ok:>11}   {'all' if lost_count <= m else 'none (more than m lost)'}")
        check(f"{k}+{m}: any {m} lost shards can be rebuilt, {m + 1} lost shards cannot", block_ok)
        print()

    k, m = 4, 2
    shards = erasure.encode(data, k, m)
    print("=== Rebuilding single shards (what the repair job does)")
    available = {i: shards[i] for i in (0, 2, 3, 5)}         # shards 1 and 4 are lost
    rebuilt = erasure.rebuild_shards(available, k, m, [1, 4])
    check("lost data shard 1 rebuilt exactly", rebuilt[1] == shards[1])
    check("lost parity shard 4 rebuilt exactly", rebuilt[4] == shards[4])

    print("\n=== Shard blobs (header = chunk hash + shard number)")
    chunk_hash = erasure.sha256_hex(data)
    blob = erasure.pack_shard(chunk_hash, 3, shards[3])
    ch, idx, payload = erasure.unpack_shard(blob)
    check("header can be read back", ch == chunk_hash and idx == 3 and payload == shards[3],
          f"-> blob {len(blob):,} bytes = {erasure.HEADER_LEN} header + {len(payload):,} data")

    print("\n=== Speed (4 MB chunk, 4+2)")
    t = time.time()
    for _ in range(20):
        erasure.encode(data, k, m)
    enc = (time.time() - t) / 20
    avail = {i: shards[i] for i in (1, 2, 4, 5)}
    t = time.time()
    for _ in range(20):
        erasure.decode(avail, k, m, len(data))
    dec = (time.time() - t) / 20
    print(f"   encode {enc * 1000:.1f} ms ({4 / enc:.0f} MB/s), decode with 2 shards lost {dec * 1000:.1f} ms ({4 / dec:.0f} MB/s)")

    print(f"\n{'ALL TESTS PASSED' if failures == 0 else str(failures) + ' TEST(S) FAILED'}")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()