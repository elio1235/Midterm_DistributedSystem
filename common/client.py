"""Client library: writes and reads objects with replication (quorum) or erasure coding.

This is the logic of the write path and of the read path. For now the test scripts use it directly;
later the FastAPI gateway will import the same class.
"""
import hashlib
import threading
from concurrent.futures import ThreadPoolExecutor

import httpx

from common import erasure
from common.chunking import CHUNK_SIZE, chunk_bytes


class ClusterError(Exception):
    pass


class QuorumError(ClusterError):
    """Not enough nodes answered to meet W (write) or R (read)."""


def local_node_url(node_id, url=None):
    """The nodes are published on this computer as localhost:8001 ... localhost:800N."""
    return f"http://127.0.0.1:{8000 + int(node_id.split('-')[1])}"


def sha256_hex(data):
    return hashlib.sha256(data).hexdigest()


class ObjectClient:
    def __init__(self, meta_url="http://127.0.0.1:9000", node_url=local_node_url, workers=16, timeout=120):
        self.meta_url = meta_url.rstrip("/")
        self.node_url = node_url
        self.http = httpx.Client(timeout=httpx.Timeout(timeout, connect=3), limits=httpx.Limits(max_connections=200, max_keepalive_connections=64))
        self.pool = ThreadPoolExecutor(workers)
        self._lock = threading.Lock()
        self.bytes_sent = 0
        self.bytes_received = 0

    # ------------------------------------------------------------ small helpers
    def _meta(self, method, path, **kw):
        return self.http.request(method, f"{self.meta_url}{path}", **kw)

    def _send(self, method, url, **kw):
        """HTTP request that is repeated once if the connection was broken (for example by a node that was restarted)."""
        try:
            return self.http.request(method, url, **kw)
        except httpx.TransportError as e:
            if isinstance(e, (httpx.ConnectError, httpx.ConnectTimeout)):
                raise                                        # the node is down: do not wait for it a second time
            return self.http.request(method, url, **kw)

    def _count(self, sent=0, received=0):
        with self._lock:
            self.bytes_sent += sent
            self.bytes_received += received

    def create_bucket(self, name, redundancy="replication", replicas=3, write_quorum=2, read_quorum=2, k=4, m=2):
        r = self._meta("POST", f"/buckets/{name}", json={
            "redundancy": redundancy, "replicas": replicas, "write_quorum": write_quorum,
            "read_quorum": read_quorum, "ec_k": k, "ec_m": m})
        if r.status_code != 200:
            raise ClusterError(r.json().get("detail", r.text))
        return r.json()

    def _put_blob(self, node, blob_hash, data):
        """Send one blob to one node. Returns True when the node confirmed it."""
        try:
            r = self._send("PUT", f"{self.node_url(node['node_id'], node.get('url'))}/chunks/{blob_hash}", content=data)
        except httpx.HTTPError:
            return False
        if r.status_code in (200, 201):
            self._count(sent=len(data))
            return True
        return False

    def _get_blob(self, node, blob_hash):
        """Read one blob from one node and check its hash. Returns bytes, or None if the node cannot give a good copy."""
        try:
            r = self._send("GET", f"{self.node_url(node['node_id'], node.get('url'))}/chunks/{blob_hash}")
        except httpx.HTTPError:
            return None
        if r.status_code != 200:
            return None
        self._count(received=len(r.content))
        return r.content if sha256_hex(r.content) == blob_hash else None

    # ------------------------------------------------------------ write path
    def _write_chunk(self, item, data):
        """Store one new chunk. Returns the 'uploaded' record for the commit and the number of acknowledgements."""
        targets = item["targets"]
        if item["scheme"] == "erasure":
            k, m = item["k"], item["m"]
            payloads = erasure.encode(data, k, m)
            blobs = [erasure.pack_shard(item["hash"], i, p) for i, p in enumerate(payloads)]
            hashes = [sha256_hex(b) for b in blobs]
            futures = [(i, self.pool.submit(self._put_blob, targets[i], hashes[i], blobs[i])) for i in range(min(len(targets), k + m))]
            shards = [{"i": i, "hash": hashes[i], "node": targets[i]["node_id"], "size": len(blobs[i])}
                      for i, f in futures if f.result()]
            if len(shards) < item["min_ok"]:
                raise QuorumError(f"chunk {item['hash'][:12]}: only {len(shards)} of {k + m} shards stored, need at least {item['min_ok']}")
            return {"hash": item["hash"], "size": len(data), "scheme": "erasure", "shards": shards}, len(shards)

        futures = [(t, self.pool.submit(self._put_blob, t, item["hash"], data)) for t in targets]
        acked = [t["node_id"] for t, f in futures if f.result()]
        if len(acked) < item["min_ok"]:                      # write quorum W not reached
            raise QuorumError(f"chunk {item['hash'][:12]}: only {len(acked)} of {len(targets)} replicas confirmed, W={item['min_ok']} needed")
        return {"hash": item["hash"], "size": len(data), "scheme": "replication", "nodes": acked}, len(acked)

    def put_object(self, bucket, key, data, method="fixed", chunk_size=CHUNK_SIZE, tags=None):
        """Chunk -> plan (dedup + placement) -> write missing chunks -> commit metadata."""
        for attempt in range(2):
            chunks = list(chunk_bytes(data, method, chunk_size))
            unique = {}
            for h, piece in chunks:
                unique.setdefault(h, piece)
            r = self._meta("POST", "/chunks/plan", json={"bucket": bucket, "chunks": [{"hash": h, "size": len(p)} for h, p in unique.items()]})
            if r.status_code != 200:
                raise ClusterError(r.json().get("detail", r.text))
            uploaded, acks = [], {}
            for item in r.json()["chunks"]:
                if not item["exists"]:
                    record, n = self._write_chunk(item, unique[item["hash"]])
                    uploaded.append(record)
                    acks[item["hash"]] = n
            r = self._meta("POST", "/objects", json={
                "bucket": bucket, "key": key, "size": len(data), "chunking": method, "tags": tags or {},
                "chunk_hashes": [h for h, _ in chunks], "uploaded": uploaded})
            if r.status_code == 409 and attempt == 0:        # a deduplicated chunk was deleted in the meantime: try again
                continue
            if r.status_code != 200:
                raise ClusterError(str(r.json().get("detail", r.text)))
            result = r.json()
            result["acks"] = acks                            # chunk hash -> number of nodes (or shards) that confirmed
            return result

    # ------------------------------------------------------------ read path
    def _read_replicated(self, chunk, r_quorum):
        """Quorum read: one node sends the data, other nodes only confirm that they hold the chunk (HEAD).
        R nodes must answer, otherwise the read is refused."""
        nodes = [n for n in chunk["nodes"] if n["status"] == "alive"]
        data, source = None, None
        for n in nodes:
            data = self._get_blob(n, chunk["hash"])
            if data is not None:
                source = n
                break
        if data is None:
            raise ClusterError(f"no valid copy of chunk {chunk['hash'][:12]} could be read")
        confirmed = 1
        for n in nodes:
            if confirmed >= r_quorum:
                break
            if n is source:
                continue
            try:
                resp = self._send("HEAD", f"{self.node_url(n['node_id'], n.get('url'))}/chunks/{chunk['hash']}")
                if resp.status_code == 200 and int(resp.headers.get("X-Chunk-Size", -1)) == len(data):
                    confirmed += 1
            except httpx.HTTPError:
                pass
        if confirmed < r_quorum:
            raise QuorumError(f"chunk {chunk['hash'][:12]}: only {confirmed} replica(s) answered, R={r_quorum} needed")
        return data

    def _read_erasure(self, chunk):
        """Read any k valid shards (data shards first) and rebuild the chunk."""
        k, m = chunk["k"], chunk["m"]
        candidates = sorted((s for s in chunk["shards"] if s["status"] == "alive"), key=lambda s: s["i"])
        got = {}
        while len(got) < k and candidates:
            batch, candidates = candidates[:k - len(got)], candidates[k - len(got):]
            futures = [(s, self.pool.submit(self._get_blob, {"node_id": s["node_id"], "url": s["url"]}, s["hash"])) for s in batch]
            for s, f in futures:
                blob = f.result()
                if blob is None:
                    continue
                chunk_hash, index, payload = erasure.unpack_shard(blob)
                if chunk_hash == chunk["hash"] and index == s["i"]:
                    got[index] = payload
        if len(got) < k:
            raise QuorumError(f"chunk {chunk['hash'][:12]}: only {len(got)} valid shards could be read, k={k} needed")
        data = erasure.decode(got, k, m, chunk["size"])
        if sha256_hex(data) != chunk["hash"]:
            raise ClusterError(f"chunk {chunk['hash'][:12]} rebuilt from shards does not match its hash")
        return data, sorted(got)

    def get_object(self, bucket, key, version=None):
        r = self._meta("GET", f"/objects/{bucket}/{key}", params={"version": version} if version else None)
        if r.status_code == 404:
            raise ClusterError("object not found")
        meta = r.json()
        r_quorum = meta["redundancy"].get("read_quorum", 1)
        parts = []
        for c in meta["chunks"]:
            if c["scheme"] == "erasure":
                data, _ = self._read_erasure(c)
            elif c["scheme"] == "replication":
                data = self._read_replicated(c, r_quorum)
            else:
                raise ClusterError(f"chunk {c['hash'][:12]} has no metadata")
            parts.append(data)
        out = b"".join(parts)
        if len(out) != meta["size"]:
            raise ClusterError("object size does not match the metadata")
        return out

    def delete_object(self, bucket, key):
        return self._meta("DELETE", f"/objects/{bucket}/{key}").json()