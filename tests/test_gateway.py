"""Tests of the gateway API (pytest). The whole stack must be running (docker compose up -d).

    pytest -v                       all tests
    pytest -v -m "not docker"       without the test that stops a storage node (faster)

Environment variables (optional): GATEWAY_URL (default http://127.0.0.1:8080), ADMIN_USER, ADMIN_PASSWORD.
"""
import hashlib
import os
import random
import subprocess
import time
import uuid

import httpx
import pytest

GATEWAY = os.getenv("GATEWAY_URL", "http://127.0.0.1:8080").rstrip("/")
ADMIN = (os.getenv("ADMIN_USER", "admin"), os.getenv("ADMIN_PASSWORD", "admin123"))
MB = 1024 * 1024


def sha(data):
    return hashlib.sha256(data).hexdigest()


def rand_bytes(seed, size):
    return random.Random(seed).randbytes(size)


@pytest.fixture(scope="session")
def http():
    with httpx.Client(base_url=GATEWAY, timeout=120) as c:
        yield c


def login(http, username, password):
    r = http.post("/auth/login", json={"username": username, "password": password})
    assert r.status_code == 200, r.text
    return r.json()["access_token"]


@pytest.fixture(scope="session")
def admin_token(http):
    """The tests run as their own admin with a high rate limit (they send many requests very fast)."""
    boss = {"Authorization": "Bearer " + login(http, *ADMIN)}
    name = "pytest-runner-" + uuid.uuid4().hex[:6]
    r = http.post("/v1/users", headers=boss, json={"username": name, "password": "secret123", "role": "admin",
                                                    "rate_limit_per_min": 60000, "rate_burst": 5000})
    assert r.status_code == 201, r.text
    return login(http, name, "secret123")


@pytest.fixture(scope="session")
def admin(admin_token):
    return {"Authorization": f"Bearer {admin_token}"}


@pytest.fixture(scope="session")
def search_engine(http, admin):
    """'elasticsearch' when Elasticsearch is running, otherwise 'postgresql'."""
    return http.get("/health").json()["search"].split()[0]


@pytest.fixture()
def bucket(http, admin):
    """A fresh replicated bucket for one test. It is emptied and deleted afterwards."""
    name = "pytest-" + uuid.uuid4().hex[:10]
    assert http.put(f"/v1/buckets/{name}", headers=admin).status_code == 201
    yield name
    for o in http.get(f"/v1/buckets/{name}/objects", headers=admin, params={"limit": 1000}).json()["objects"]:
        http.delete(f"/v1/buckets/{name}/objects/{o['key']}", headers=admin)
    http.delete(f"/v1/buckets/{name}", headers=admin)


@pytest.fixture()
def ec_bucket(http, admin):
    name = "pytest-ec-" + uuid.uuid4().hex[:8]
    assert http.put(f"/v1/buckets/{name}", headers=admin, json={"redundancy": "erasure", "k": 4, "m": 2}).status_code == 201
    yield name
    for o in http.get(f"/v1/buckets/{name}/objects", headers=admin, params={"limit": 1000}).json()["objects"]:
        http.delete(f"/v1/buckets/{name}/objects/{o['key']}", headers=admin)
    http.delete(f"/v1/buckets/{name}", headers=admin)


def put(http, admin, bucket, key, data, **headers):
    r = http.put(f"/v1/buckets/{bucket}/objects/{key}", headers={**admin, **headers}, content=data)
    assert r.status_code == 201, r.text
    return r.json()


# ------------------------------------------------------------------ authentication
def test_health_needs_no_login(http):
    r = http.get("/health")
    assert r.status_code == 200 and r.json()["status"] == "ok"


def test_request_without_credentials_is_refused(http):
    r = http.get("/v1/buckets")
    assert r.status_code == 401
    assert "Bearer" in r.headers["www-authenticate"]


def test_wrong_password_and_wrong_token(http):
    assert http.post("/auth/login", json={"username": ADMIN[0], "password": "wrong-password"}).status_code == 401
    assert http.get("/v1/buckets", headers={"Authorization": "Bearer not.a.token"}).status_code == 401
    assert http.get("/v1/buckets", headers={"X-API-Key": "osk_wrong"}).status_code == 401


def test_login_gives_a_working_jwt(http, admin):
    r = http.get("/v1/me", headers=admin)
    assert r.status_code == 200 and r.json()["role"] == "admin" and r.json()["username"].startswith("pytest-runner-")


def test_api_key_can_be_created_used_and_revoked(http, admin):
    r = http.post("/v1/keys", headers=admin, json={"name": "pytest"})
    assert r.status_code == 201
    key, key_id = r.json()["api_key"], r.json()["id"]
    assert key.startswith("osk_")
    assert http.get("/v1/buckets", headers={"X-API-Key": key}).status_code == 200
    listed = http.get("/v1/keys", headers=admin).json()
    assert any(k["id"] == key_id for k in listed) and key not in str(listed)    # the secret itself is never listed
    assert http.delete(f"/v1/keys/{key_id}", headers=admin).status_code == 200
    time.sleep(0.1)
    assert http.get("/v1/buckets", headers={"X-API-Key": key}).status_code == 401


def test_normal_user_cannot_do_admin_things(http, admin):
    name = "pytest-user-" + uuid.uuid4().hex[:6]
    assert http.post("/v1/users", headers=admin, json={"username": name, "password": "secret123"}).status_code == 201
    assert http.post("/v1/users", headers=admin, json={"username": name, "password": "secret123"}).status_code == 409
    user = {"Authorization": "Bearer " + login(http, name, "secret123")}
    assert http.get("/v1/buckets", headers=user).status_code == 200
    assert http.post("/v1/users", headers=user, json={"username": name + "x", "password": "secret123"}).status_code == 403
    assert http.post("/v1/cluster/repair/run", headers=user).status_code == 403
    assert http.post("/v1/admin/reindex", headers=user).status_code == 403


def test_rate_limit_returns_429_with_retry_after(http, admin):
    name = "pytest-rate-" + uuid.uuid4().hex[:6]
    http.post("/v1/users", headers=admin, json={"username": name, "password": "secret123",
                                                "rate_limit_per_min": 60, "rate_burst": 20})      # 1 request per second, burst of 20
    user = {"Authorization": "Bearer " + login(http, name, "secret123")}
    codes, retry = [], None
    for _ in range(200):                                   # far more than the burst of 20 requests: it stops at the first 429
        r = http.get("/v1/me", headers=user)
        codes.append(r.status_code)
        if r.status_code == 429:
            retry = r.headers.get("retry-after")
            break
    assert codes[0] == 200 and codes[-1] == 429, codes[-5:]
    assert retry is not None and int(retry) >= 1
    assert http.get("/v1/me", headers=admin).status_code == 200      # the limit is per user: the admin is not affected


# ------------------------------------------------------------------ buckets
def test_bucket_create_list_info_delete(http, admin):
    name = "pytest-b-" + uuid.uuid4().hex[:8]
    r = http.put(f"/v1/buckets/{name}", headers=admin)
    assert r.status_code == 201 and r.json()["scheme"] == "replication"
    assert http.put(f"/v1/buckets/{name}", headers=admin).status_code == 200            # already exists
    assert any(b["name"] == name for b in http.get("/v1/buckets", headers=admin).json())
    info = http.get(f"/v1/buckets/{name}", headers=admin).json()
    assert info["objects"] == 0 and info["write_quorum"] == 2 and info["read_quorum"] == 2
    assert http.delete(f"/v1/buckets/{name}", headers=admin).status_code == 200
    assert http.get(f"/v1/buckets/{name}", headers=admin).status_code == 404


def test_bucket_rules(http, admin):
    bad = http.put("/v1/buckets/pytest-bad", headers=admin, json={"replicas": 3, "write_quorum": 1, "read_quorum": 1})
    assert bad.status_code == 400 and "W + R must be greater than N" in bad.json()["detail"]
    assert http.put("/v1/buckets/bad%20name!", headers=admin).status_code == 400
    ec = http.put("/v1/buckets/pytest-ec-rules", headers=admin, json={"redundancy": "erasure", "k": 4, "m": 2})
    assert ec.status_code in (200, 201) and ec.json()["scheme"] == "erasure" and ec.json()["want"] == 6
    http.delete("/v1/buckets/pytest-ec-rules", headers=admin)


def test_non_empty_bucket_cannot_be_deleted(http, admin, bucket):
    put(http, admin, bucket, "a.txt", b"hello")
    assert http.delete(f"/v1/buckets/{bucket}", headers=admin).status_code == 409


# ------------------------------------------------------------------ upload and download
@pytest.mark.parametrize("size", [0, 1, 1000, 4 * MB, 4 * MB + 1, 9 * MB + 123])
def test_upload_then_download_is_identical(http, admin, bucket, size):
    data = rand_bytes(size, size)
    r = put(http, admin, bucket, f"obj-{size}.bin", data)
    assert r["size"] == size and r["version"] == 1
    got = http.get(f"/v1/buckets/{bucket}/objects/obj-{size}.bin", headers=admin)
    assert got.status_code == 200 and sha(got.content) == sha(data)
    assert got.headers["accept-ranges"] == "bytes" and int(got.headers["content-length"]) == size


def test_erasure_coded_bucket_upload_and_download(http, admin, ec_bucket):
    data = rand_bytes(5, 9 * MB)
    r = put(http, admin, ec_bucket, "ec.bin", data)
    assert r["chunks"] == 3
    assert sha(http.get(f"/v1/buckets/{ec_bucket}/objects/ec.bin", headers=admin).content) == sha(data)
    meta = http.get(f"/v1/buckets/{ec_bucket}/metadata/ec.bin", headers=admin).json()
    assert meta["redundancy"]["scheme"] == "erasure" and all(len(c["pieces"]) == 6 for c in meta["chunks"])


def test_streaming_upload_without_content_length(http, admin, bucket):
    """A generator as body is sent with 'Transfer-Encoding: chunked': the gateway must handle a stream of unknown size."""
    parts = [rand_bytes(i, 700_000) for i in range(12)]            # 8.4 MB in 12 pieces
    r = http.put(f"/v1/buckets/{bucket}/objects/streamed.bin", headers=admin, content=iter(parts))
    assert r.status_code == 201 and r.json()["size"] == sum(map(len, parts))
    assert http.get(f"/v1/buckets/{bucket}/objects/streamed.bin", headers=admin).content == b"".join(parts)


def test_multipart_form_upload(http, admin, bucket):
    data = rand_bytes(9, 5 * MB)
    r = http.post(f"/v1/buckets/{bucket}/upload", headers=admin, files={"file": ("report.pdf", data, "application/pdf")},
                  data={"key": "docs/report.pdf", "tags": "team=blue,year=2026"})
    assert r.status_code == 201, r.text
    assert r.json()["content_type"] == "application/pdf" and r.json()["tags"] == {"team": "blue", "year": "2026"}
    got = http.get(f"/v1/buckets/{bucket}/objects/docs/report.pdf", headers=admin)
    assert got.headers["content-type"] == "application/pdf" and sha(got.content) == sha(data)


def test_same_content_is_stored_once(http, admin, bucket):
    data = rand_bytes(3, 4 * MB)
    first = put(http, admin, bucket, "one.bin", data)
    second = put(http, admin, bucket, "two.bin", data)
    assert first["chunks_uploaded"] == 1 and second["chunks_uploaded"] == 0 and second["chunks_deduplicated"] == 1


def test_upload_to_missing_bucket(http, admin):
    r = http.put("/v1/buckets/pytest-does-not-exist/objects/x.bin", headers=admin, content=b"abc")
    assert r.status_code == 404


def test_new_upload_makes_a_new_version(http, admin, bucket):
    put(http, admin, bucket, "v.txt", b"first")
    second = put(http, admin, bucket, "v.txt", b"second version")
    assert second["version"] == 2
    assert http.get(f"/v1/buckets/{bucket}/objects/v.txt", headers=admin).content == b"second version"
    assert http.get(f"/v1/buckets/{bucket}/objects/v.txt", headers=admin, params={"version": 1}).content == b"first"


# ------------------------------------------------------------------ byte ranges
@pytest.fixture()
def ranged(http, admin, bucket):
    data = rand_bytes(77, 9 * MB + 5000)                           # 3 chunks: 4 MB + 4 MB + the rest
    put(http, admin, bucket, "range.bin", data)
    return bucket, data


def get_range(http, admin, bucket, spec):
    return http.get(f"/v1/buckets/{bucket}/objects/range.bin", headers={**admin, "Range": spec})


@pytest.mark.parametrize("spec, start, end", [
    ("bytes=0-99", 0, 99),
    ("bytes=1000-1999", 1000, 1999),
    ("bytes=4194300-4194310", 4194300, 4194310),                    # crosses the border between chunk 1 and chunk 2
    ("bytes=4000000-9000000", 4000000, 9000000),                    # crosses two borders
])
def test_range_request(http, admin, ranged, spec, start, end):
    bucket, data = ranged
    r = get_range(http, admin, bucket, spec)
    assert r.status_code == 206
    assert r.content == data[start:end + 1]
    assert r.headers["content-range"] == f"bytes {start}-{end}/{len(data)}"
    assert int(r.headers["content-length"]) == end - start + 1


def test_open_ended_and_suffix_ranges(http, admin, ranged):
    bucket, data = ranged
    r = get_range(http, admin, bucket, "bytes=9000000-")
    assert r.status_code == 206 and r.content == data[9000000:]
    r = get_range(http, admin, bucket, "bytes=-1234")
    assert r.status_code == 206 and r.content == data[-1234:]
    r = get_range(http, admin, bucket, "bytes=100-99999999999")           # end after the file: cut at the end
    assert r.status_code == 206 and r.content == data[100:]


def test_range_not_satisfiable(http, admin, ranged):
    bucket, data = ranged
    r = get_range(http, admin, bucket, f"bytes={len(data)}-")
    assert r.status_code == 416 and r.headers["content-range"] == f"bytes */{len(data)}"
    assert get_range(http, admin, bucket, "bytes=500-100").status_code == 416


def test_head_and_metadata(http, admin, bucket):
    data = rand_bytes(1, 5 * MB)
    put(http, admin, bucket, "docs/info.bin", data, **{"Content-Type": "image/png", "X-Tag-project": "alpha", "X-Tag-env": "test"})
    h = http.head(f"/v1/buckets/{bucket}/objects/docs/info.bin", headers=admin)
    assert h.status_code == 200 and int(h.headers["content-length"]) == len(data) and h.headers["content-type"] == "image/png"
    m = http.get(f"/v1/buckets/{bucket}/metadata/docs/info.bin", headers=admin).json()
    assert m["size"] == len(data) and m["version"] == 1 and m["content_type"] == "image/png"
    assert m["tags"] == {"project": "alpha", "env": "test"}
    assert m["health"] == "healthy" and len(m["chunks"]) == 2
    assert all(c["pieces_alive"] == 3 and c["state"] == "healthy" for c in m["chunks"])
    assert http.get(f"/v1/buckets/{bucket}/metadata/missing.bin", headers=admin).status_code == 404


def test_download_missing_object_and_delete(http, admin, bucket):
    assert http.get(f"/v1/buckets/{bucket}/objects/nope.bin", headers=admin).status_code == 404
    put(http, admin, bucket, "del.bin", b"bye")
    d = http.delete(f"/v1/buckets/{bucket}/objects/del.bin", headers=admin)
    assert d.status_code == 200 and d.json()["versions_deleted"] == 1
    assert http.get(f"/v1/buckets/{bucket}/objects/del.bin", headers=admin).status_code == 404
    assert http.delete(f"/v1/buckets/{bucket}/objects/del.bin", headers=admin).status_code == 404


def test_download_with_token_in_the_link(http, admin_token, admin, bucket):
    put(http, admin, bucket, "link.txt", b"for the browser")
    r = http.get(f"/v1/buckets/{bucket}/objects/link.txt", params={"access_token": admin_token, "download": "true"})
    assert r.status_code == 200 and r.content == b"for the browser"
    assert 'attachment; filename="link.txt"' in r.headers["content-disposition"]


# ------------------------------------------------------------------ list and search
def test_list_with_prefix_and_pages(http, admin, bucket):
    for key in ["photos/a.jpg", "photos/b.jpg", "photos/c.jpg", "docs/x.txt", "root.txt"]:
        put(http, admin, bucket, key, key.encode())
    everything = http.get(f"/v1/buckets/{bucket}/objects", headers=admin).json()
    assert everything["total"] == 5 and [o["key"] for o in everything["objects"]] == sorted(o["key"] for o in everything["objects"])
    photos = http.get(f"/v1/buckets/{bucket}/objects", headers=admin, params={"prefix": "photos/"}).json()
    assert [o["key"] for o in photos["objects"]] == ["photos/a.jpg", "photos/b.jpg", "photos/c.jpg"]
    page2 = http.get(f"/v1/buckets/{bucket}/objects", headers=admin, params={"limit": 2, "offset": 2}).json()
    assert page2["total"] == 5 and len(page2["objects"]) == 2 and page2["objects"][0]["key"] == "photos/b.jpg"
    assert http.get("/v1/buckets/pytest-missing/objects", headers=admin).status_code == 404


@pytest.fixture()
def corpus(http, admin, bucket):
    """Six objects with different names, sizes and tags."""
    items = [("reports/2026/q1-sales.pdf", 3000, {"dept": "sales", "year": "2026"}),
             ("reports/2026/q2-sales.pdf", 20000, {"dept": "sales", "year": "2026"}),
             ("reports/2025/annual-review.pdf", 50000, {"dept": "hr", "year": "2025"}),
             ("images/logo.png", 800, {"dept": "marketing"}),
             ("images/banner.png", 120000, {"dept": "marketing", "year": "2026"}),
             ("notes.txt", 10, {})]
    for key, size, tags in items:
        put(http, admin, bucket, key, rand_bytes(size, size), **{f"X-Tag-{k}": v for k, v in tags.items()})
    return bucket


def keys(resp):
    assert resp.status_code == 200, resp.text
    return sorted(r["key"] for r in resp.json()["results"])


@pytest.mark.parametrize("engine", ["auto", "postgres"])
def test_search_filters(http, admin, corpus, engine):
    s = lambda **p: keys(http.get("/v1/search", headers=admin, params={"bucket": corpus, "engine": engine, **p}))
    assert s(prefix="reports/2026/") == ["reports/2026/q1-sales.pdf", "reports/2026/q2-sales.pdf"]
    assert s(min_size=10000, max_size=60000) == ["reports/2025/annual-review.pdf", "reports/2026/q2-sales.pdf"]
    assert s(max_size=100) == ["notes.txt"]
    assert s(tag=["dept:marketing"]) == ["images/banner.png", "images/logo.png"]
    assert s(tag=["dept:marketing", "year:2026"]) == ["images/banner.png"]
    assert s(q="sales") == ["reports/2026/q1-sales.pdf", "reports/2026/q2-sales.pdf"]
    assert s(q="LOGO") == ["images/logo.png"]
    assert s(q="review", prefix="reports/") == ["reports/2025/annual-review.pdf"]
    assert s(created_after="2020-01-01") and s(created_after="2999-01-01") == []
    assert s(created_before="2020-01-01") == []
    assert len(s()) == 6


def test_search_answer_shape_and_sorting(http, admin, corpus):
    r = http.get("/v1/search", headers=admin, params={"bucket": corpus, "sort": "size", "desc": "true", "limit": 2, "engine": "postgres"}).json()
    assert r["engine"] == "postgresql" and r["total"] == 6 and len(r["results"]) == 2
    assert [x["size"] for x in r["results"]] == [120000, 50000]
    assert set(r["results"][0]) == {"bucket", "key", "version", "size", "content_type", "created_at", "tags"}
    assert http.get("/v1/search", headers=admin, params={"tag": "no-colon"}).status_code == 400
    assert http.get("/v1/search", headers=admin, params={"sort": "nonsense"}).status_code == 400


def test_elasticsearch_and_postgres_give_the_same_answer(http, admin, corpus, search_engine):
    if search_engine != "elasticsearch":
        pytest.skip("Elasticsearch is not running")
    for params in ({"q": "sales"}, {"tag": ["dept:sales"]}, {"min_size": 1000, "max_size": 60000}, {"prefix": "images/"}):
        a = keys(http.get("/v1/search", headers=admin, params={"bucket": corpus, "engine": "elasticsearch", **params}))
        b = keys(http.get("/v1/search", headers=admin, params={"bucket": corpus, "engine": "postgres", **params}))
        assert a == b, params


def test_deleted_object_disappears_from_search(http, admin, bucket):
    put(http, admin, bucket, "unique-needle-name.txt", b"x")
    assert keys(http.get("/v1/search", headers=admin, params={"q": "needle", "bucket": bucket})) == ["unique-needle-name.txt"]
    http.delete(f"/v1/buckets/{bucket}/objects/unique-needle-name.txt", headers=admin)
    assert keys(http.get("/v1/search", headers=admin, params={"q": "needle", "bucket": bucket})) == []


def test_reindex_rebuilds_the_index_from_postgresql(http, admin, corpus, search_engine):
    if search_engine != "elasticsearch":
        pytest.skip("Elasticsearch is not running")
    r = http.post("/v1/admin/reindex", headers=admin)
    assert r.status_code == 200 and r.json()["indexed"] == r.json()["documents_in_index"] >= 6
    assert len(keys(http.get("/v1/search", headers=admin, params={"bucket": corpus, "engine": "elasticsearch"}))) == 6


# ------------------------------------------------------------------ cluster status
def test_cluster_status(http, admin, bucket):
    put(http, admin, bucket, "status.bin", rand_bytes(2, 5 * MB))
    s = http.get("/v1/cluster/status", headers=admin).json()
    assert s["nodes_total"] >= 4 and s["nodes_alive"] == s["nodes_total"]
    assert all(n["status"] == "alive" and n["free_bytes"] > 0 for n in s["nodes"])
    assert sum(n["chunks"] for n in s["nodes"]) > 0 and sum(n["stored_bytes"] for n in s["nodes"]) > 0
    assert s["chunks"]["unique"] > 0 and s["chunks"]["unavailable"] == 0
    assert set(s) >= {"nodes", "free_bytes_total", "chunks", "degraded_objects", "repair", "search"}
    assert isinstance(s["repair"]["recent_jobs"], list)


@pytest.mark.docker
def test_degraded_object_is_reported_and_then_repaired(http, admin, ec_bucket):
    """Stop one storage node: objects with a piece on it become 'degraded' but stay readable; the repair job heals them."""
    data = rand_bytes(11, 9 * MB)
    put(http, admin, ec_bucket, "degrade.bin", data)
    meta = http.get(f"/v1/buckets/{ec_bucket}/metadata/degrade.bin", headers=admin).json()
    victim = meta["chunks"][0]["pieces"][0]["node_id"]
    container = f"storage-{victim}"
    subprocess.run(["docker", "stop", container], capture_output=True)
    try:
        def degraded():
            s = http.get("/v1/cluster/status", headers=admin).json()
            return [o for o in s["degraded_objects"]["items"] if o["bucket"] == ec_bucket], s
        deadline = time.time() + 40
        found, status = degraded()
        while not found and time.time() < deadline:
            time.sleep(1)
            found, status = degraded()
        assert found, "the object was never reported as degraded"
        assert next(n for n in status["nodes"] if n["node_id"] == victim)["status"] == "failed"
        assert sha(http.get(f"/v1/buckets/{ec_bucket}/objects/degrade.bin", headers=admin).content) == sha(data)     # still readable
        deadline = time.time() + 90
        while time.time() < deadline:                      # the failure detector starts the repair job by itself
            found, status = degraded()
            if not found:
                break
            time.sleep(2)
        assert not found, "the repair job did not heal the object"
        assert sha(http.get(f"/v1/buckets/{ec_bucket}/objects/degrade.bin", headers=admin).content) == sha(data)
    finally:
        subprocess.run(["docker", "start", container], capture_output=True)
        deadline = time.time() + 60
        while time.time() < deadline:
            if all(n["status"] == "alive" and n["state"] == "alive" for n in http.get("/v1/cluster/status", headers=admin).json()["nodes"]):
                break
            time.sleep(1)
