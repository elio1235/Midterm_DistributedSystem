import os
import time
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/postgres")

SCHEMA = """
CREATE TABLE IF NOT EXISTS nodes (
    node_id      TEXT PRIMARY KEY,
    url          TEXT NOT NULL,
    free_bytes   BIGINT NOT NULL DEFAULT 0,
    chunk_count  INTEGER NOT NULL DEFAULT 0,
    last_seen    TIMESTAMPTZ NOT NULL DEFAULT now(),
    in_ring      BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS buckets (
    name        TEXT PRIMARY KEY,
    redundancy  TEXT NOT NULL DEFAULT 'replication',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS objects (
    id            BIGSERIAL PRIMARY KEY,
    bucket        TEXT NOT NULL REFERENCES buckets(name),
    key           TEXT NOT NULL,
    version       INTEGER NOT NULL,
    size          BIGINT NOT NULL,
    chunking      TEXT NOT NULL DEFAULT 'fixed',
    chunk_hashes  TEXT[] NOT NULL,
    tags          JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (bucket, key, version)
);

CREATE TABLE IF NOT EXISTS chunks (
    hash        TEXT PRIMARY KEY,
    size        INTEGER NOT NULL,
    ref_count   INTEGER NOT NULL DEFAULT 0,
    nodes       TEXT[] NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

pool = None


@contextmanager
def get_conn():
    """Borrow a connection; commit on success, roll back on any error."""
    conn = pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)


def cursor(conn):
    return conn.cursor(cursor_factory=RealDictCursor)


def init_db(retries=30, delay=2):
    """Connect to PostgreSQL (retrying while it starts) and create the tables."""
    global pool
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            pool = ThreadedConnectionPool(1, 20, DATABASE_URL)
            break
        except psycopg2.OperationalError as e:
            last_error = e
            print(f"[metadata] database not ready (try {attempt}/{retries}): {e}", flush=True)
            time.sleep(delay)
    else:
        raise last_error
    with get_conn() as conn:
        with cursor(conn) as cur:
            cur.execute(SCHEMA)
    print("[metadata] database ready, tables created", flush=True)