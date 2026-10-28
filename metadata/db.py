import os
import time
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import PoolError, ThreadedConnectionPool

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

-- Step 3: redundancy settings per bucket, failure state of nodes, erasure coding info per chunk.
-- ADD COLUMN IF NOT EXISTS lets the tables from Step 2 grow without losing the existing data.
ALTER TABLE nodes   ADD COLUMN IF NOT EXISTS state         TEXT NOT NULL DEFAULT 'alive';
ALTER TABLE nodes   ADD COLUMN IF NOT EXISTS failed_at     TIMESTAMPTZ;

ALTER TABLE buckets ADD COLUMN IF NOT EXISTS replicas      INTEGER NOT NULL DEFAULT 3;
ALTER TABLE buckets ADD COLUMN IF NOT EXISTS write_quorum  INTEGER NOT NULL DEFAULT 2;
ALTER TABLE buckets ADD COLUMN IF NOT EXISTS read_quorum   INTEGER NOT NULL DEFAULT 2;
ALTER TABLE buckets ADD COLUMN IF NOT EXISTS ec_k          INTEGER NOT NULL DEFAULT 4;
ALTER TABLE buckets ADD COLUMN IF NOT EXISTS ec_m          INTEGER NOT NULL DEFAULT 2;

ALTER TABLE chunks  ADD COLUMN IF NOT EXISTS scheme        TEXT NOT NULL DEFAULT 'replication';
ALTER TABLE chunks  ADD COLUMN IF NOT EXISTS want          INTEGER NOT NULL DEFAULT 3;
ALTER TABLE chunks  ADD COLUMN IF NOT EXISTS ec_k          INTEGER;
ALTER TABLE chunks  ADD COLUMN IF NOT EXISTS ec_m          INTEGER;
ALTER TABLE chunks  ADD COLUMN IF NOT EXISTS shards        JSONB NOT NULL DEFAULT '[]'::jsonb;
ALTER TABLE chunks  ADD COLUMN IF NOT EXISTS stored_bytes  BIGINT NOT NULL DEFAULT 0;
UPDATE chunks SET stored_bytes = size::bigint * cardinality(nodes) WHERE stored_bytes = 0;

CREATE TABLE IF NOT EXISTS repair_jobs (
    id                  BIGSERIAL PRIMARY KEY,
    reason              TEXT NOT NULL,
    node_id             TEXT,
    status              TEXT NOT NULL DEFAULT 'queued',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at          TIMESTAMPTZ,
    finished_at         TIMESTAMPTZ,
    chunks_total        INTEGER NOT NULL DEFAULT 0,
    chunks_done         INTEGER NOT NULL DEFAULT 0,
    chunks_repaired     INTEGER NOT NULL DEFAULT 0,
    chunks_partial      INTEGER NOT NULL DEFAULT 0,
    chunks_unrecoverable INTEGER NOT NULL DEFAULT 0,
    copies_rebuilt      INTEGER NOT NULL DEFAULT 0,
    bytes_read          BIGINT NOT NULL DEFAULT 0,
    bytes_written       BIGINT NOT NULL DEFAULT 0,
    seconds             DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS scrub_runs (
    id               BIGSERIAL PRIMARY KEY,
    trigger          TEXT NOT NULL,
    status           TEXT NOT NULL DEFAULT 'running',
    started_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at      TIMESTAMPTZ,
    copies_checked   INTEGER NOT NULL DEFAULT 0,
    bytes_verified   BIGINT NOT NULL DEFAULT 0,
    corrupt_found    INTEGER NOT NULL DEFAULT 0,
    missing_found    INTEGER NOT NULL DEFAULT 0,
    repaired         INTEGER NOT NULL DEFAULT 0,
    unrepairable     INTEGER NOT NULL DEFAULT 0,
    repair_bytes_read    BIGINT NOT NULL DEFAULT 0,
    repair_bytes_written BIGINT NOT NULL DEFAULT 0,
    orphans_deleted  INTEGER NOT NULL DEFAULT 0,
    seconds          DOUBLE PRECISION
);
"""

pool = None


def _borrow():
    """Get a connection from the pool; wait a little if all of them are busy."""
    for _ in range(400):
        try:
            return pool.getconn()
        except PoolError:
            time.sleep(0.05)
    raise PoolError("no free database connection")


@contextmanager
def get_conn():
    """Borrow a connection; commit on success, roll back on any error."""
    conn = _borrow()
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
    """Connect to PostgreSQL (retrying while it starts) and create / upgrade the tables."""
    global pool
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            pool = ThreadedConnectionPool(2, 60, DATABASE_URL)
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