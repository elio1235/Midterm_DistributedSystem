"""The gateway talks to the storage cluster through this class.

It extends ObjectClient (Phase 4) with what a web service needs:
  - streaming upload: the file arrives in small pieces and only one 4 MB chunk is kept in memory
  - download of any byte range: only the chunks that overlap the range are read
"""
import hashlib
from concurrent.futures import ThreadPoolExecutor

from common.chunking import CHUNK_SIZE
from common.client import ClusterError, ObjectClient, QuorumError


class GatewayError(Exception):
    """An error that the gateway turns into an HTTP answer."""

    def __init__(self, status, detail):
        super().__init__(detail)
        self.status = status
        self.detail = detail


class StreamUpload:
    """Upload of one object in pieces. Call write_chunk() for each chunk (at most CHUNK_SIZE bytes), then commit()."""

    def __init__(self, store, bucket, key, tags, chunk_size=CHUNK_SIZE):
        self.store, self.bucket, self.key, self.tags = store, bucket, key, tags
        self.chunk_size = chunk_size
        self.hashes, self.uploaded, self.size = [], {}, 0
        self._buf = bytearray()

    def write_chunk(self, piece):
        h = hashlib.sha256(piece).hexdigest()
        r = self.store._meta("POST", "/chunks/plan", json={"bucket": self.bucket, "chunks": [{"hash": h, "size": len(piece)}]})
        if r.status_code != 200:
            raise GatewayError(r.status_code, r.json().get("detail", r.text))
        item = r.json()["chunks"][0]
        if not item["exists"] and h not in self.uploaded:
            try:
                record, _ = self.store._write_chunk(item, piece)
            except QuorumError as e:
                raise GatewayError(503, str(e))
            self.uploaded[h] = record
        self.hashes.append(h)
        self.size += len(piece)

    def feed(self, data):
        """Synchronous helper: collect bytes and write a chunk whenever 4 MB are available."""
        self._buf += data
        while len(self._buf) >= self.chunk_size:
            self.write_chunk(bytes(self._buf[:self.chunk_size]))
            del self._buf[:self.chunk_size]

    def commit(self):
        if self._buf:
            self.write_chunk(bytes(self._buf))
            self._buf.clear()
        r = self.store._meta("POST", "/objects", json={
            "bucket": self.bucket, "key": self.key, "size": self.size, "chunking": "fixed", "tags": self.tags,
            "chunk_hashes": self.hashes, "uploaded": list(self.uploaded.values())})
        if r.status_code == 409:
            raise GatewayError(409, "a deduplicated chunk was deleted during the upload, please upload again")
        if r.status_code != 200:
            raise GatewayError(r.status_code, r.json().get("detail", r.text))
        return r.json()


class GatewayStore(ObjectClient):
    def __init__(self, meta_url, workers=32):
        # the node addresses come from the metadata service (http://storage-node-1:8000 inside Docker)
        super().__init__(meta_url, node_url=lambda node_id, url=None: url, workers=workers, timeout=120)
        self._prefetch = ThreadPoolExecutor(8)

    def object_meta(self, bucket, key, version=None):
        """Metadata with the placement of every chunk, or None if the object does not exist."""
        r = self._meta("GET", f"/objects/{bucket}/{key}", params={"version": version} if version else None)
        if r.status_code == 404:
            return None
        if r.status_code != 200:
            raise GatewayError(r.status_code, r.text)
        return r.json()

    def begin_upload(self, bucket, key, tags):
        return StreamUpload(self, bucket, key, tags)

    def _read_chunk(self, meta, chunk):
        if chunk["scheme"] == "erasure":
            return self._read_erasure(chunk)[0]
        if chunk["scheme"] == "replication":
            return self._read_replicated(chunk, meta["redundancy"].get("read_quorum", 1))
        raise ClusterError(f"chunk {chunk['hash'][:12]} has no metadata")

    def iter_range(self, meta, start, end):
        """Yield the bytes start..end (both included) of the object, reading only the chunks that are needed.
        The next chunk is read in the background while the current one is sent to the client."""
        wanted, pos = [], 0
        for c in meta["chunks"]:
            c_start, c_end = pos, pos + c["size"] - 1
            pos += c["size"]
            if c_end < start:
                continue
            if c_start > end:
                break
            wanted.append((c_start, c))
        pending = self._prefetch.submit(self._read_chunk, meta, wanted[0][1]) if wanted else None
        for i, (c_start, c) in enumerate(wanted):
            data = pending.result()
            pending = self._prefetch.submit(self._read_chunk, meta, wanted[i + 1][1]) if i + 1 < len(wanted) else None
            lo, hi = max(start - c_start, 0), min(end - c_start + 1, len(data))
            yield data[lo:hi]
