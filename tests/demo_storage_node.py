"""Demo/test of the storage node API.  Run from the project root:
    python tests/demo_storage_node.py
"""
import hashlib
import subprocess
import sys

import httpx

NODES = {
    "node-1": "http://localhost:8001",
    "node-2": "http://localhost:8002",
    "node-3": "http://localhost:8003",
    "node-4": "http://localhost:8004",
}
failures = 0


def check(name, condition, detail=""):
    global failures
    if not condition:
        failures += 1
    print(f"[{'PASS' if condition else 'FAIL'}] {name} {detail}")


def main():
    client = httpx.Client(timeout=10)

    print("=== 1. Health of all 4 storage nodes ===")
    for node_id, url in NODES.items():
        r = client.get(f"{url}/health")
        check(f"{node_id} is up", r.status_code == 200, f"-> {r.json()}")

    url = NODES["node-1"]
    data = b"distributed systems midterm " * 5000
    h = hashlib.sha256(data).hexdigest()
    print(f"\nTest chunk: {len(data)} bytes, sha256 = {h}")

    print("\n=== 2. PUT / HEAD / GET ===")
    r = client.put(f"{url}/chunks/{h}", content=data)
    check("PUT new chunk returns 201", r.status_code == 201, f"-> {r.json()}")

    r = client.put(f"{url}/chunks/{h}", content=data)
    check("PUT same chunk again returns 200 (overwritten)", r.status_code == 200, f"-> {r.json()}")

    r = client.head(f"{url}/chunks/{h}")
    check("HEAD returns 200 with correct size", r.status_code == 200 and int(r.headers["content-length"]) == len(data),
          f"-> content-length={r.headers.get('content-length')}")

    r = client.get(f"{url}/chunks/{h}")
    check("GET returns identical bytes", r.status_code == 200 and r.content == data and hashlib.sha256(r.content).hexdigest() == h)

    print("\n=== 3. Hash verification on write ===")
    wrong = hashlib.sha256(b"something else").hexdigest()
    r = client.put(f"{url}/chunks/{wrong}", content=data)
    check("PUT with wrong hash is rejected (400)", r.status_code == 400, f"-> {r.json()['detail']}")

    r = client.put(f"{url}/chunks/not-a-hash", content=data)
    check("PUT with invalid chunk id is rejected (400)", r.status_code == 400, f"-> {r.json()['detail']}")

    print("\n=== 4. Corruption detection on read ===")
    subprocess.run(
        ["docker", "exec", "storage-node-1", "sh", "-c", f"echo corrupted >> /data/{h[:2]}/{h}"],
        check=True,
    )
    print("(corrupted the file on node-1's disk)")
    r = client.get(f"{url}/chunks/{h}")
    check("GET on corrupted chunk is rejected (422)", r.status_code == 422, f"-> {r.json()['detail']}")

    r = client.put(f"{url}/chunks/{h}", content=data)
    r2 = client.get(f"{url}/chunks/{h}")
    check("Re-PUT of good data repairs the chunk", r.status_code == 200 and r2.status_code == 200 and r2.content == data)

    print("\n=== 5. DELETE ===")
    r = client.delete(f"{url}/chunks/{h}")
    check("DELETE returns 200", r.status_code == 200, f"-> {r.json()}")
    check("HEAD after delete returns 404", client.head(f"{url}/chunks/{h}").status_code == 404)
    check("GET after delete returns 404", client.get(f"{url}/chunks/{h}").status_code == 404)
    check("DELETE again returns 404", client.delete(f"{url}/chunks/{h}").status_code == 404)

    print(f"\n{'ALL TESTS PASSED' if failures == 0 else str(failures) + ' TEST(S) FAILED'}")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()