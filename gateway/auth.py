"""Users, API keys and JWT tokens. Everything is stored in PostgreSQL (tables gw_users and gw_api_keys).

- Password login (POST /auth/login) gives a JWT (HS256, expires after JWT_MINUTES).
- An API key (header X-API-Key) is a long random secret. Only its SHA-256 is stored, so a stolen database
  does not reveal the keys. The key is shown once, when it is created.
"""
import hashlib
import hmac
import secrets
import time
from datetime import datetime, timedelta, timezone

import jwt
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool

import config

SCHEMA = """
CREATE TABLE IF NOT EXISTS gw_users (
    username      TEXT PRIMARY KEY,
    password_hash TEXT NOT NULL,
    role          TEXT NOT NULL DEFAULT 'user',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE gw_users ADD COLUMN IF NOT EXISTS rate_per_min DOUBLE PRECISION;     -- own rate limit of this user (NULL = default)
ALTER TABLE gw_users ADD COLUMN IF NOT EXISTS rate_burst   INTEGER;
CREATE TABLE IF NOT EXISTS gw_api_keys (
    id          BIGSERIAL PRIMARY KEY,
    key_hash    TEXT NOT NULL UNIQUE,
    key_prefix  TEXT NOT NULL,
    username    TEXT NOT NULL REFERENCES gw_users(username) ON DELETE CASCADE,
    name        TEXT NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_used   TIMESTAMPTZ,
    revoked     BOOLEAN NOT NULL DEFAULT FALSE
);
"""

_pool = None
_key_cache = {}                 # key_hash -> (expires_at, user dict); short cache so we do not query on every request
CACHE_SECONDS = 10


def _conn():
    return _pool.getconn()


def _run(fn):
    conn = _conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            result = fn(cur)
        conn.commit()
        return result
    except Exception:
        conn.rollback()
        raise
    finally:
        _pool.putconn(conn)


def init(retries=30, delay=2):
    global _pool
    last = None
    for attempt in range(1, retries + 1):
        try:
            _pool = ThreadedConnectionPool(1, 10, config.DATABASE_URL)
            break
        except psycopg2.OperationalError as e:
            last = e
            print(f"[gateway] database not ready (try {attempt}/{retries}): {e}", flush=True)
            time.sleep(delay)
    else:
        raise last
    _run(lambda cur: cur.execute(SCHEMA))
    if not get_user(config.ADMIN_USER):
        create_user(config.ADMIN_USER, config.ADMIN_PASSWORD, "admin")
        print(f"[gateway] admin user '{config.ADMIN_USER}' created", flush=True)
    if config.BOOTSTRAP_API_KEY:
        add_api_key(config.ADMIN_USER, "bootstrap", config.BOOTSTRAP_API_KEY)


# ------------------------------------------------------------------ passwords
def hash_password(password, salt=None):
    salt = salt or secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode(), bytes.fromhex(salt), 200_000).hex()
    return f"pbkdf2_sha256${salt}${digest}"


def verify_password(password, stored):
    try:
        _, salt, digest = stored.split("$")
    except ValueError:
        return False
    return hmac.compare_digest(hash_password(password, salt), stored)


def get_user(username):
    return _run(lambda cur: (cur.execute("SELECT username, role, created_at FROM gw_users WHERE username = %s", (username,)), cur.fetchone())[1])


def list_users():
    return _run(lambda cur: (cur.execute("SELECT username, role, created_at FROM gw_users ORDER BY username"), cur.fetchall())[1])


def create_user(username, password, role="user", rate_per_min=None, rate_burst=None):
    def go(cur):
        cur.execute(
            "INSERT INTO gw_users (username, password_hash, role, rate_per_min, rate_burst) VALUES (%s, %s, %s, %s, %s) "
            "ON CONFLICT (username) DO NOTHING",
            (username, hash_password(password), role, rate_per_min, rate_burst),
        )
        return cur.rowcount == 1
    return _run(go)


_limit_cache = {}


def limits_for(username):
    """(rate_per_min, burst) of a user; (None, None) means the default limits."""
    cached = _limit_cache.get(username)
    if cached and cached[0] > time.monotonic():
        return cached[1]
    row = _run(lambda cur: (cur.execute("SELECT rate_per_min, rate_burst FROM gw_users WHERE username = %s", (username,)), cur.fetchone())[1])
    value = (row["rate_per_min"], row["rate_burst"]) if row else (None, None)
    _limit_cache[username] = (time.monotonic() + CACHE_SECONDS, value)
    return value


def check_login(username, password):
    row = _run(lambda cur: (cur.execute("SELECT username, role, password_hash FROM gw_users WHERE username = %s", (username,)), cur.fetchone())[1])
    # always do one hash, so the answer time does not tell whether the user exists
    ok = verify_password(password, row["password_hash"]) if row else verify_password(password, hash_password("x"))
    return {"username": row["username"], "role": row["role"]} if row and ok else None


# ------------------------------------------------------------------ JWT
def make_token(user):
    now = datetime.now(timezone.utc)
    payload = {"sub": user["username"], "role": user["role"], "iat": now, "exp": now + timedelta(minutes=config.JWT_MINUTES)}
    return jwt.encode(payload, config.JWT_SECRET, algorithm="HS256")


def read_token(token):
    """Returns {'username', 'role'} or None when the token is wrong or expired."""
    try:
        p = jwt.decode(token, config.JWT_SECRET, algorithms=["HS256"])
    except jwt.PyJWTError:
        return None
    return {"username": p["sub"], "role": p.get("role", "user")}


# ------------------------------------------------------------------ API keys
def _hash_key(key):
    return hashlib.sha256(key.encode()).hexdigest()


def add_api_key(username, name, key):
    def go(cur):
        cur.execute(
            "INSERT INTO gw_api_keys (key_hash, key_prefix, username, name) VALUES (%s, %s, %s, %s) "
            "ON CONFLICT (key_hash) DO NOTHING RETURNING id", (_hash_key(key), key[:8], username, name))
        row = cur.fetchone()
        return row["id"] if row else None
    return _run(go)


def create_api_key(username, name=""):
    key = "osk_" + secrets.token_urlsafe(32)
    return add_api_key(username, name, key), key


def lookup_api_key(key):
    h = _hash_key(key)
    cached = _key_cache.get(h)
    if cached and cached[0] > time.monotonic():
        return cached[1]

    def go(cur):
        cur.execute(
            "SELECT k.id, u.username, u.role FROM gw_api_keys k JOIN gw_users u ON u.username = k.username "
            "WHERE k.key_hash = %s AND NOT k.revoked", (h,))
        row = cur.fetchone()
        if row:
            cur.execute("UPDATE gw_api_keys SET last_used = now() WHERE id = %s", (row["id"],))
        return row
    row = _run(go)
    user = {"username": row["username"], "role": row["role"]} if row else None
    _key_cache[h] = (time.monotonic() + CACHE_SECONDS, user)
    return user


def list_api_keys(username):
    return _run(lambda cur: (cur.execute(
        "SELECT id, key_prefix, name, created_at, last_used, revoked FROM gw_api_keys WHERE username = %s ORDER BY id",
        (username,)), cur.fetchall())[1])


def revoke_api_key(key_id, username):
    def go(cur):
        cur.execute("UPDATE gw_api_keys SET revoked = TRUE WHERE id = %s AND username = %s", (key_id, username))
        return cur.rowcount == 1
    ok = _run(go)
    _key_cache.clear()
    return ok
