import asyncio
import hashlib
import os
import re
import shutil
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException, Request, Response
from pydantic import BaseModel

NODE_ID = os.getenv("NODE_ID", "node-1")
DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
METADATA_URL = os.getenv("METADATA_URL", "http://metadata:9000")
NODE_URL = os.getenv("NODE_URL", f"http://{NODE_ID}:8000")
HEARTBEAT_INTERVAL = float(os.getenv("HEARTBEAT_INTERVAL", "3"))

HASH_RE = re.compile(r"^[0-9a-f]{64}$")

DATA_DIR.mkdir(parents=True, exist_ok=True)


def chunk_path(chunk_hash: str) -> Path:
    # /data/ab/abcdef... (first 2 hex chars used as a sub-folder)
    return DATA_DIR / chunk_hash[:2] / chunk_hash


def validate_hash(chunk_hash: str) -> None:
    if not HASH_RE.match(chunk_hash):
        raise HTTPException(status_code=400, detail="chunk id must be 64 lowercase hex chars (SHA-256)")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def iter_chunk_files():
    for p in DATA_DIR.glob("*/*"):
        if p.is_file() and HASH_RE.match(p.name):
            yield p


def count_chunks() -> int:
    return sum(1 for _ in iter_chunk_files())


async def heartbeat_loop() -> None:
    async with httpx.AsyncClient(timeout=3) as client:
        while True:
            try:
                payload = {
                    "node_id": NODE_ID,
                    "url": NODE_URL,
                    "free_bytes": shutil.disk_usage(DATA_DIR).free,
                    "chunk_count": await asyncio.to_thread(count_chunks),
                }
                r = await client.post(f"{METADATA_URL}/heartbeat", json=payload)
                r.raise_for_status()
            except Exception as e:
                print(f"[{NODE_ID}] heartbeat failed: {e}", flush=True)
            await asyncio.sleep(HEARTBEAT_INTERVAL)


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(heartbeat_loop())
    yield
    task.cancel()


app = FastAPI(title=f"Storage Node ({NODE_ID})", lifespan=lifespan)


@app.put("/chunks/{chunk_hash}")
async def put_chunk(chunk_hash: str, request: Request, response: Response):
    validate_hash(chunk_hash)
    data = await request.body()
    actual = sha256_hex(data)
    if actual != chunk_hash:
        raise HTTPException(status_code=400, detail=f"hash mismatch: content hashes to {actual}")

    path = chunk_path(chunk_hash)
    existed = path.exists()
    path.parent.mkdir(parents=True, exist_ok=True)

    # write to a temp file first, then rename: a crash never leaves a half-written chunk
    tmp = DATA_DIR / f".tmp-{uuid.uuid4().hex}"
    tmp.write_bytes(data)
    os.replace(tmp, path)

    response.status_code = 200 if existed else 201
    return {
        "node_id": NODE_ID,
        "chunk_hash": chunk_hash,
        "size": len(data),
        "status": "overwritten" if existed else "stored",
    }


@app.get("/chunks/{chunk_hash}")
def get_chunk(chunk_hash: str):
    validate_hash(chunk_hash)
    path = chunk_path(chunk_hash)
    if not path.is_file():
        raise HTTPException(status_code=404, detail="chunk not found")

    data = path.read_bytes()
    if sha256_hex(data) != chunk_hash:
        print(f"[{NODE_ID}] CORRUPTED chunk detected on read: {chunk_hash}", flush=True)
        raise HTTPException(status_code=422, detail="chunk is corrupted: stored content does not match its hash")

    return Response(content=data, media_type="application/octet-stream", headers={"X-Chunk-Hash": chunk_hash})


@app.head("/chunks/{chunk_hash}")
def head_chunk(chunk_hash: str):
    validate_hash(chunk_hash)
    path = chunk_path(chunk_hash)
    if not path.is_file():
        return Response(status_code=404)
    size = path.stat().st_size
    return Response(status_code=200, headers={"Content-Length": str(size), "X-Chunk-Size": str(size)})


@app.delete("/chunks/{chunk_hash}")
def delete_chunk(chunk_hash: str):
    validate_hash(chunk_hash)
    path = chunk_path(chunk_hash)
    if not path.is_file():
        raise HTTPException(status_code=404, detail="chunk not found")
    path.unlink()
    return {"node_id": NODE_ID, "chunk_hash": chunk_hash, "status": "deleted"}


# ---------------------------------------------------------------- Step 3: scrubbing support
class VerifyRequest(BaseModel):
    hashes: list[str]


@app.post("/verify")
def verify_chunks(req: VerifyRequest):
    """Re-read the given chunks from the disk and check their SHA-256 (the scrubber uses this).
    The data never leaves the node: only the list of bad chunks is returned."""
    corrupt, missing, bytes_read = [], [], 0
    for h in req.hashes:
        validate_hash(h)
        path = chunk_path(h)
        if not path.is_file():
            missing.append(h)
            continue
        data = path.read_bytes()
        bytes_read += len(data)
        if sha256_hex(data) != h:
            corrupt.append(h)
    return {"node_id": NODE_ID, "checked": len(req.hashes), "bytes_read": bytes_read, "corrupt": corrupt, "missing": missing}


@app.get("/chunks")
def list_chunks():
    """All chunk files on this node, with their size and age (used to find orphan files)."""
    now = time.time()
    items = []
    for p in iter_chunk_files():
        st = p.stat()
        items.append({"hash": p.name, "size": st.st_size, "age_seconds": round(now - st.st_mtime, 1)})
    return {"node_id": NODE_ID, "count": len(items), "chunks": items}


@app.get("/stats")
def stats():
    """Real number of files and bytes on the disk of this node (used by the benchmarks)."""
    count, used = 0, 0
    for p in iter_chunk_files():
        count += 1
        used += p.stat().st_size
    return {"node_id": NODE_ID, "chunk_count": count, "used_bytes": used}


@app.get("/health")
def health():
    return {
        "node_id": NODE_ID,
        "status": "ok",
        "free_bytes": shutil.disk_usage(DATA_DIR).free,
        "chunk_count": count_chunks(),
    }