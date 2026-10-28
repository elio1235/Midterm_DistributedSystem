"""Gateway: the HTTP API that users and the web UI talk to (port 8080).

   user / UI  --->  gateway  --->  metadata service (PostgreSQL)   where are the chunks? who owns what?
                         |------>  storage nodes                   the chunks themselves
                         |------>  Elasticsearch                   search over the object metadata
"""
import math
import re
import threading
import time
from contextlib import asynccontextmanager

import httpx
from fastapi import Depends, FastAPI, File, Form, HTTPException, Query, Request, Response, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel
from starlette.concurrency import run_in_threadpool

import auth
import config
from common.chunking import CHUNK_SIZE
from ratelimit import RateLimiter
from search import Search
from store import GatewayError, GatewayStore

store = GatewayStore(config.METADATA_URL)
engine = Search()
limiter = RateLimiter(config.RATE_LIMIT_PER_MIN, config.RATE_LIMIT_BURST)
login_limiter = RateLimiter(config.LOGIN_LIMIT_PER_MIN, 10)

TAG_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
MAX_TAGS = 20


# ------------------------------------------------------------------ start up
def keep_index_in_sync():
    """Wait for Elasticsearch, then rebuild the index from PostgreSQL if the two do not match."""
    for _ in range(120):
        if engine.ensure_index():
            try:
                total = meta("GET", "/search", params={"limit": 1})["total"]
                if engine.count() != total:
                    print(f"[search] index rebuilt: {rebuild_index()} objects", flush=True)
            except Exception as e:
                print(f"[search] start-up index check failed: {e}", flush=True)
            return
        time.sleep(5)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await run_in_threadpool(auth.init)
    if engine.enabled:
        threading.Thread(target=keep_index_in_sync, daemon=True, name="index-sync").start()
    yield


app = FastAPI(title="Object Storage Gateway", version="1.0", lifespan=lifespan,
              description="S3-like API: buckets, streaming upload, range download, search, cluster status. "
                          "Authenticate with the header X-API-Key or Authorization: Bearer <JWT> (POST /auth/login).")
app.add_middleware(CORSMiddleware, allow_origins=config.CORS_ORIGINS, allow_methods=["*"], allow_headers=["*"],
                   expose_headers=["Content-Range", "Accept-Ranges", "ETag", "X-Object-Version", "X-RateLimit-Remaining", "Retry-After"])


@app.exception_handler(GatewayError)
async def gateway_error(request, exc):
    return JSONResponse({"detail": exc.detail}, status_code=exc.status)


# ------------------------------------------------------------------ helpers
def meta(method, path, **kw):
    """Call the metadata service and pass its errors on to the user."""
    if "params" in kw:                                          # httpx would send None as an empty text: leave those out
        kw["params"] = {k: (str(v).lower() if isinstance(v, bool) else v) for k, v in kw["params"].items() if v is not None}
    try:
        r = store._meta(method, path, **kw)
    except httpx.HTTPError as e:
        raise GatewayError(503, f"metadata service not reachable: {e}")
    if r.status_code >= 400:
        try:
            detail = r.json().get("detail", r.text)
        except ValueError:
            detail = r.text
        raise GatewayError(r.status_code, detail)
    return r.json()


def current_user(request: Request, response: Response):
    """Authentication (API key or JWT) followed by the rate limit of the user."""
    user = None
    api_key = request.headers.get("x-api-key")
    if api_key:
        user = auth.lookup_api_key(api_key)
    else:
        header = request.headers.get("authorization", "")
        token = header[7:] if header.lower().startswith("bearer ") else None
        if token is None and request.method in ("GET", "HEAD"):
            token = request.query_params.get("access_token")        # lets a browser download a file with a plain link
        if token:
            user = auth.read_token(token)
    if not user:
        client = request.client.host if request.client else "?"
        if not login_limiter.check("bad-credentials:" + client)[0]:   # slow down guessing of keys and tokens
            raise HTTPException(429, "too many failed attempts", headers={"Retry-After": "60"})
        raise HTTPException(401, "missing or invalid credentials: use the header X-API-Key or Authorization: Bearer <token>",
                            headers={"WWW-Authenticate": "Bearer"})
    per_min, burst = auth.limits_for(user["username"])
    allowed, remaining, wait = limiter.check(user["username"], per_min, burst)
    response.headers["X-RateLimit-Limit"] = str(burst or config.RATE_LIMIT_BURST)
    response.headers["X-RateLimit-Remaining"] = str(remaining)
    if not allowed:
        raise HTTPException(429, "rate limit exceeded, slow down",
                            headers={"Retry-After": str(max(1, math.ceil(wait))), "X-RateLimit-Remaining": "0"})
    return user


def admin_only(user=Depends(current_user)):
    if user["role"] != "admin":
        raise HTTPException(403, "this action needs the admin role")
    return user


def parse_tags(headers, query_value=None):
    """User-defined tags from the headers 'X-Tag-<name>: value' and/or the query parameter ?tags=a=1,b=2."""
    tags = {}
    for name, value in headers.items():
        if name.lower().startswith("x-tag-"):
            tags[name[6:].lower()] = value
    for pair in (query_value or "").split(","):
        if pair.strip():
            name, sep, value = pair.partition("=")
            if not sep:
                raise HTTPException(400, f"tag '{pair}' must look like name=value")
            tags[name.strip()] = value.strip()
    if len(tags) > MAX_TAGS:
        raise HTTPException(400, f"at most {MAX_TAGS} tags per object")
    for name, value in tags.items():
        if not TAG_NAME_RE.match(name) or len(value) > 256:
            raise HTTPException(400, f"invalid tag '{name}': name 1-64 characters (letters, digits, . _ -), value up to 256 characters")
    return tags


def public_tags(tags):
    return {k: v for k, v in (tags or {}).items() if not k.startswith("_")}


def describe_item(item):
    tags = item.get("tags") or {}
    return {"bucket": item["bucket"], "key": item["key"], "version": item["version"], "size": item["size"],
            "content_type": tags.get("_content_type") or item.get("content_type") or "application/octet-stream",
            "created_at": item["created_at"], "tags": public_tags(tags)}


def search_doc(item):
    d = describe_item(item)
    d["created_at"] = str(d["created_at"])
    return d


def etag_of(meta_obj):
    import hashlib
    return hashlib.sha256("".join(c["hash"] for c in meta_obj["chunks"]).encode()).hexdigest()[:32]


def chunk_state(c):
    pieces = c["shards"] if c["scheme"] == "erasure" else c["nodes"]
    alive = sum(1 for p in pieces if p["status"] == "alive")
    need = c["k"] if c["scheme"] == "erasure" else 1
    return alive, ("unavailable" if alive < need else "degraded" if alive < c["want"] else "healthy")


def describe_object(m):
    chunks, worst = [], "healthy"
    for i, c in enumerate(m["chunks"]):
        alive, state = chunk_state(c)
        if state != "healthy" and worst != "unavailable":
            worst = state
        if c["scheme"] == "erasure":
            pieces = [{"shard": s["i"], "node_id": s["node_id"], "status": s["status"]} for s in c["shards"]]
        else:
            pieces = [{"node_id": n["node_id"], "status": n["status"]} for n in c["nodes"]]
        chunks.append({"index": i, "hash": c["hash"], "size": c["size"], "scheme": c["scheme"], "pieces_alive": alive,
                       "pieces_wanted": c["want"], "state": state, "pieces": pieces})
    tags = m.get("tags") or {}
    return {"bucket": m["bucket"], "key": m["key"], "version": m["version"], "size": m["size"], "etag": etag_of(m),
            "content_type": tags.get("_content_type", "application/octet-stream"), "tags": public_tags(tags),
            "created_at": m["created_at"], "chunking": m["chunking"], "redundancy": m["redundancy"],
            "health": worst, "chunks": chunks}


def parse_range(header, size):
    """'bytes=0-99', 'bytes=100-' or 'bytes=-500'. Returns (start, end) or None (no range) or raises 416."""
    m = re.fullmatch(r"bytes=(\d*)-(\d*)", (header or "").strip())
    if not m or (not m.group(1) and not m.group(2)):
        return None                                               # not a single range: ignore the header, send everything
    first, last = m.group(1), m.group(2)
    if not first:                                                 # suffix: the last N bytes
        n = int(last)
        if n == 0:
            raise HTTPException(416, "range not satisfiable", headers={"Content-Range": f"bytes */{size}"})
        return max(size - n, 0), size - 1
    start = int(first)
    end = min(int(last), size - 1) if last else size - 1
    if start >= size or end < start:
        raise HTTPException(416, "range not satisfiable", headers={"Content-Range": f"bytes */{size}"})
    return start, end


def index_result(result, tags):
    """Put the new object into the search index (best effort: the upload is already safe in the cluster)."""
    ctype = tags.get("_content_type", "application/octet-stream")
    engine.index({"bucket": result["bucket"], "key": result["key"], "version": result["version"], "size": result["size"],
                  "content_type": ctype, "created_at": str(result["created_at"]), "tags": public_tags(tags)})
    return {"bucket": result["bucket"], "key": result["key"], "version": result["version"], "size": result["size"],
            "content_type": ctype, "tags": public_tags(tags), "created_at": result["created_at"],
            "chunks": result["chunks_total"], "chunks_uploaded": result["chunks_uploaded"], "chunks_deduplicated": result["chunks_reused"]}


def rebuild_index():
    """Drop the index and fill it again from PostgreSQL (the source of truth). Returns the number of indexed objects."""
    if engine.http and engine.enabled:
        try:
            engine.http.delete(f"/{config.ES_INDEX}")
        except httpx.HTTPError:
            pass
    if not engine.ensure_index():
        return 0
    done, offset = 0, 0
    while True:
        page = meta("GET", "/search", params={"limit": 1000, "offset": offset})
        done += engine.bulk_index([search_doc(i) for i in page["items"]])
        offset += 1000
        if offset >= page["total"]:
            return done


# ------------------------------------------------------------------ health and login (no API key needed)
@app.get("/health", tags=["system"])
def health():
    out = {"status": "ok", "metadata": "ok", "search": "elasticsearch" if engine.available() else "postgresql (fallback)"}
    try:
        store._meta("GET", "/health").raise_for_status()
    except httpx.HTTPError:
        out.update(status="degraded", metadata="unreachable")
    return out


class Login(BaseModel):
    username: str
    password: str


@app.post("/auth/login", tags=["auth"])
def login(body: Login, request: Request):
    client = request.client.host if request.client else "?"
    if not login_limiter.check("login:" + client)[0]:
        raise HTTPException(429, "too many login attempts, wait a minute", headers={"Retry-After": "60"})
    user = auth.check_login(body.username, body.password)
    if not user:
        raise HTTPException(401, "wrong username or password")
    return {"access_token": auth.make_token(user), "token_type": "bearer", "expires_in": config.JWT_MINUTES * 60,
            "username": user["username"], "role": user["role"]}


# ------------------------------------------------------------------ users and API keys
@app.get("/v1/me", tags=["auth"])
def me(user=Depends(current_user)):
    return user


class NewUser(BaseModel):
    username: str
    password: str
    role: str = "user"
    rate_limit_per_min: float | None = None      # own limit for this user (default: RATE_LIMIT_PER_MIN)
    rate_burst: int | None = None


@app.post("/v1/users", status_code=201, tags=["auth"])
def create_user(body: NewUser, admin=Depends(admin_only)):
    if not TAG_NAME_RE.match(body.username) or len(body.password) < 6 or body.role not in ("user", "admin"):
        raise HTTPException(400, "username: letters, digits, . _ - ; password: at least 6 characters; role: user or admin")
    if not auth.create_user(body.username, body.password, body.role, body.rate_limit_per_min, body.rate_burst):
        raise HTTPException(409, "user already exists")
    return {"username": body.username, "role": body.role, "rate_limit_per_min": body.rate_limit_per_min, "rate_burst": body.rate_burst}


class NewKey(BaseModel):
    name: str = ""


@app.post("/v1/keys", status_code=201, tags=["auth"])
def create_key(body: NewKey = NewKey(), user=Depends(current_user)):
    key_id, key = auth.create_api_key(user["username"], body.name)
    return {"id": key_id, "api_key": key, "note": "Copy the key now. It is not shown again."}


@app.get("/v1/keys", tags=["auth"])
def list_keys(user=Depends(current_user)):
    return auth.list_api_keys(user["username"])


@app.delete("/v1/keys/{key_id}", tags=["auth"])
def revoke_key(key_id: int, user=Depends(current_user)):
    if not auth.revoke_api_key(key_id, user["username"]):
        raise HTTPException(404, "key not found")
    return {"id": key_id, "revoked": True}


# ------------------------------------------------------------------ buckets
class BucketIn(BaseModel):
    redundancy: str = "replication"      # "replication" or "erasure"
    replicas: int = 3
    write_quorum: int = 2
    read_quorum: int = 2
    k: int = 4
    m: int = 2


@app.put("/v1/buckets/{bucket}", tags=["buckets"])
def create_bucket(bucket: str, response: Response, body: BucketIn = BucketIn(), user=Depends(current_user)):
    result = meta("POST", f"/buckets/{bucket}", json={
        "redundancy": body.redundancy, "replicas": body.replicas, "write_quorum": body.write_quorum,
        "read_quorum": body.read_quorum, "ec_k": body.k, "ec_m": body.m})
    response.status_code = 201 if result.get("created") else 200
    return result


@app.get("/v1/buckets", tags=["buckets"])
def list_buckets(user=Depends(current_user)):
    return meta("GET", "/buckets")


@app.get("/v1/buckets/{bucket}", tags=["buckets"])
def bucket_info(bucket: str, user=Depends(current_user)):
    return meta("GET", f"/buckets/{bucket}")


@app.delete("/v1/buckets/{bucket}", tags=["buckets"])
def delete_bucket(bucket: str, user=Depends(current_user)):
    return meta("DELETE", f"/buckets/{bucket}")


# ------------------------------------------------------------------ objects: list, upload, download, metadata, delete
@app.get("/v1/buckets/{bucket}/objects", tags=["objects"])
def list_objects(bucket: str, prefix: str | None = None, limit: int = Query(100, ge=1, le=1000), offset: int = Query(0, ge=0),
                 sort: str = "key", desc: bool = False, user=Depends(current_user)):
    meta("GET", f"/buckets/{bucket}")                                     # 404 if the bucket does not exist
    page = meta("GET", "/search", params={"bucket": bucket, "prefix": prefix, "limit": limit, "offset": offset, "sort": sort, "desc": desc})
    return {"bucket": bucket, "prefix": prefix or "", "total": page["total"], "limit": limit, "offset": offset,
            "objects": [describe_item(i) for i in page["items"]]}


@app.put("/v1/buckets/{bucket}/objects/{key:path}", status_code=201, tags=["objects"])
async def upload_object(bucket: str, key: str, request: Request, user=Depends(current_user)):
    """Streaming upload: send the file as the body of the request. The gateway never holds more than one 4 MB chunk."""
    tags = parse_tags(request.headers, request.query_params.get("tags"))
    tags["_content_type"] = request.headers.get("content-type", "application/octet-stream")
    up = store.begin_upload(bucket, key, tags)
    buf = bytearray()
    async for piece in request.stream():
        buf += piece
        while len(buf) >= CHUNK_SIZE:
            await run_in_threadpool(up.write_chunk, bytes(buf[:CHUNK_SIZE]))
            del buf[:CHUNK_SIZE]
    if buf:
        await run_in_threadpool(up.write_chunk, bytes(buf))
    result = await run_in_threadpool(up.commit)
    return await run_in_threadpool(index_result, result, tags)


@app.post("/v1/buckets/{bucket}/upload", status_code=201, tags=["objects"])
def upload_form(bucket: str, file: UploadFile = File(...), key: str | None = Form(None), tags: str | None = Form(None),
                user=Depends(current_user)):
    """Multipart upload (HTML form / curl -F). Fields: file, key (default: the file name), tags (a=1,b=2)."""
    object_tags = parse_tags({}, tags)
    object_tags["_content_type"] = file.content_type or "application/octet-stream"
    up = store.begin_upload(bucket, key or file.filename, object_tags)
    while True:
        piece = file.file.read(CHUNK_SIZE)
        if not piece:
            break
        up.write_chunk(piece)
    return index_result(up.commit(), object_tags)


def object_headers(m, size):
    return {"Accept-Ranges": "bytes", "ETag": f'"{etag_of(m)}"', "X-Object-Version": str(m["version"]), "Content-Length": str(size)}


def load_object(bucket, key, version):
    m = store.object_meta(bucket, key, version)
    if m is None:
        raise HTTPException(404, "object not found")
    return m


@app.get("/v1/buckets/{bucket}/objects/{key:path}", tags=["objects"])
def download_object(bucket: str, key: str, request: Request, version: int | None = None, download: bool = False,
                    user=Depends(current_user)):
    """Download the whole object or a part of it (header Range: bytes=0-99, bytes=100-, bytes=-500)."""
    m = load_object(bucket, key, version)
    size = m["size"]
    ctype = (m.get("tags") or {}).get("_content_type", "application/octet-stream")
    rng = parse_range(request.headers.get("range"), size) if size > 0 else None
    start, end = rng if rng else (0, size - 1)
    length = end - start + 1 if size else 0
    headers = object_headers(m, length)
    if download:
        headers["Content-Disposition"] = 'attachment; filename="%s"' % key.rsplit("/", 1)[-1].replace('"', "")
    if rng:
        headers["Content-Range"] = f"bytes {start}-{end}/{size}"
    if size == 0:
        return Response(b"", media_type=ctype, headers=headers)
    return StreamingResponse(store.iter_range(m, start, end), status_code=206 if rng else 200, media_type=ctype, headers=headers)


@app.head("/v1/buckets/{bucket}/objects/{key:path}", tags=["objects"])
def head_object(bucket: str, key: str, version: int | None = None, user=Depends(current_user)):
    m = load_object(bucket, key, version)
    ctype = (m.get("tags") or {}).get("_content_type", "application/octet-stream")
    return Response(status_code=200, media_type=ctype, headers=object_headers(m, m["size"]))


@app.get("/v1/buckets/{bucket}/metadata/{key:path}", tags=["objects"])
def object_metadata(bucket: str, key: str, version: int | None = None, user=Depends(current_user)):
    """Size, tags, version, redundancy and the placement of every chunk (which node holds which copy or shard)."""
    return describe_object(load_object(bucket, key, version))


@app.delete("/v1/buckets/{bucket}/objects/{key:path}", tags=["objects"])
def delete_object(bucket: str, key: str, user=Depends(current_user)):
    result = meta("DELETE", f"/objects/{bucket}/{key}")
    engine.remove(bucket, key)
    return {"bucket": bucket, "key": key, "versions_deleted": result["versions_deleted"], "chunks_freed": result["chunks_freed"]}


# ------------------------------------------------------------------ search
@app.get("/v1/search", tags=["search"])
def search_objects(q: str | None = Query(None, description="words or part of the object key"),
                   bucket: str | None = None, prefix: str | None = None,
                   min_size: int | None = None, max_size: int | None = None,
                   created_after: str | None = Query(None, description="ISO date, for example 2026-10-28"),
                   created_before: str | None = None,
                   tag: list[str] = Query([], description="name:value, can be repeated"),
                   sort: str | None = Query(None, description="relevance, key, created_at or size"),
                   desc: bool | None = None, limit: int = Query(50, ge=1, le=500), offset: int = Query(0, ge=0),
                   engine_name: str = Query("auto", alias="engine", description="auto, elasticsearch or postgres"),
                   user=Depends(current_user)):
    """Search by key prefix, part of the key, size range, creation date and user-defined tags."""
    tag_filter = {}
    for t in tag:
        name, sep, value = t.partition(":")
        if not sep or not name:
            raise HTTPException(400, f"tag filter '{t}' must look like name:value")
        tag_filter[name] = value
    sort = sort or ("relevance" if q else "created_at")
    if sort not in ("relevance", "key", "created_at", "size"):
        raise HTTPException(400, "sort must be relevance, key, created_at or size")
    if desc is None:
        desc = sort != "key"
    if engine_name not in ("auto", "elasticsearch", "postgres"):
        raise HTTPException(400, "engine must be auto, elasticsearch or postgres")
    if engine_name != "postgres":
        found = engine.search(sort=sort, desc=desc, limit=limit, offset=offset, bucket=bucket, prefix=prefix, q=q,
                              min_size=min_size, max_size=max_size, created_after=created_after,
                              created_before=created_before, tags=tag_filter)
        if found is not None:
            return {"engine": "elasticsearch", "total": found["total"], "limit": limit, "offset": offset,
                    "results": [describe_item(i) for i in found["items"]]}
        if engine_name == "elasticsearch":
            raise HTTPException(503, "Elasticsearch is not available")
    page = meta("GET", "/search", params={
        "bucket": bucket, "prefix": prefix, "q": q, "min_size": min_size, "max_size": max_size,
        "created_after": created_after, "created_before": created_before,
        "tag": [f"{k}:{v}" for k, v in tag_filter.items()], "sort": "key" if sort == "relevance" else sort,
        "desc": desc, "limit": limit, "offset": offset})
    return {"engine": "postgresql", "total": page["total"], "limit": limit, "offset": offset,
            "results": [describe_item(i) for i in page["items"]]}


@app.post("/v1/admin/reindex", tags=["search"])
def reindex(admin=Depends(admin_only)):
    if not engine.enabled:
        raise HTTPException(503, "Elasticsearch is switched off (ES_URL is empty)")
    count = rebuild_index()
    return {"indexed": count, "documents_in_index": engine.count()}


# ------------------------------------------------------------------ cluster
@app.get("/v1/cluster/status", tags=["cluster"])
def cluster_status(user=Depends(current_user)):
    """Nodes (health, free space, chunks and bytes per node), degraded chunks and objects, repair progress."""
    status = meta("GET", "/cluster/status")
    status["search"] = {"engine": "elasticsearch" if engine.available() else "postgresql", "documents": engine.count()}
    return status


@app.get("/v1/cluster/repair/jobs", tags=["cluster"])
def repair_jobs(limit: int = Query(10, ge=1, le=100), user=Depends(current_user)):
    return meta("GET", "/repair/jobs", params={"limit": limit})


@app.post("/v1/cluster/repair/run", tags=["cluster"])
def run_repair(admin=Depends(admin_only)):
    return meta("POST", "/repair/run", params={"wait": "false", "if_needed": "true"})


@app.post("/v1/cluster/scrub/run", tags=["cluster"])
def run_scrub(admin=Depends(admin_only)):
    return meta("POST", "/scrub/run", params={"wait": "false"})
