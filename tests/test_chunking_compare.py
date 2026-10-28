"""Compare fixed-size chunking with FastCDC (content-defined chunking).

Run from the project root:
    python tests/test_chunking_compare.py
"""
import json
import os
import random
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from common.chunking import chunk_bytes  # noqa: E402

MB = 1024 * 1024
FILE_MB = 16        # size of the test file
CHUNK = 1 * MB      # target chunk size for this comparison (the system default is 4 MB)


def chunk_info(data, method):
    start = time.time()
    chunks = [(h, len(b)) for h, b in chunk_bytes(data, method, CHUNK)]
    return chunks, time.time() - start


def reuse(old, new):
    """How much of the new version can be deduplicated against the old version."""
    old_hashes = {h for h, _ in old}
    reused = sum(1 for h, _ in new if h in old_hashes)
    new_bytes = sum(s for h, s in new if h not in old_hashes)
    return reused, new_bytes


def main():
    original = random.Random(1).randbytes(FILE_MB * MB)
    noise = random.Random(2).randbytes(100)
    mid = len(original) // 2
    inserted = original[:1000] + noise + original[1000:]            # 100 bytes inserted near the start
    edited = original[:mid] + noise + original[mid + 100:]          # 100 bytes overwritten in the middle

    results = {"file_mb": FILE_MB, "target_chunk_kb": CHUNK // 1024, "methods": {}}
    print(f"Test file: {FILE_MB} MB of random data, target chunk size {CHUNK // 1024} KB\n")

    print(f"{'method':8} {'chunks':>7} {'min KB':>8} {'avg KB':>8} {'max KB':>8} {'time s':>8} {'MB/s':>8}")
    print("-" * 62)
    chunked = {}
    for method in ("fixed", "cdc"):
        old, secs = chunk_info(original, method)
        sizes = [s for _, s in old]
        chunked[method] = old
        print(f"{method:8} {len(old):>7} {min(sizes)/1024:>8.0f} {sum(sizes)/len(sizes)/1024:>8.0f} "
              f"{max(sizes)/1024:>8.0f} {secs:>8.2f} {FILE_MB/secs:>8.1f}")
        results["methods"][method] = {
            "chunks": len(old), "min_kb": round(min(sizes) / 1024), "avg_kb": round(sum(sizes) / len(sizes) / 1024),
            "max_kb": round(max(sizes) / 1024), "seconds": round(secs, 2), "mb_per_s": round(FILE_MB / secs, 1),
        }

    print("\nDeduplication when a new version of the file is stored (same file, small change)\n")
    print(f"{'change':38} {'method':8} {'chunks':>7} {'reused':>7} {'reused %':>9} {'new MB to store':>16}")
    print("-" * 90)
    for label, version in (("100 bytes inserted at the start", inserted), ("100 bytes overwritten in the middle", edited)):
        for method in ("fixed", "cdc"):
            new, _ = chunk_info(version, method)
            reused, new_bytes = reuse(chunked[method], new)
            pct = reused / len(new) * 100
            print(f"{label:38} {method:8} {len(new):>7} {reused:>7} {pct:>8.0f}% {new_bytes / MB:>16.2f}")
            results["methods"][method].setdefault("changes", {})[label] = {
                "chunks": len(new), "reused": reused, "reused_percent": round(pct, 1),
                "new_mb_to_store": round(new_bytes / MB, 2),
            }

    os.makedirs(os.path.join(ROOT, "results"), exist_ok=True)
    with open(os.path.join(ROOT, "results", "chunking_comparison.json"), "w") as f:
        json.dump(results, f, indent=2)
    print("\nSaved to results/chunking_comparison.json")


if __name__ == "__main__":
    main()