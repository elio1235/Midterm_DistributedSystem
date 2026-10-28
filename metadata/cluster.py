"""Shared helpers of the metadata service: settings, the hash ring of the alive nodes, node status."""
import os
import threading

import db
from common.ring import HashRing

HEARTBEAT_TIMEOUT = float(os.getenv("HEARTBEAT_TIMEOUT", "10"))
REPLICATION_FACTOR = int(os.getenv("REPLICATION_FACTOR", "3"))
VNODES = int(os.getenv("VNODES", "128"))

_ring_cache = {"members": None, "ring": None}
_ring_lock = threading.Lock()


def load_ring():
    """Return (ring, urls). The ring contains the nodes that are in_ring AND sent a heartbeat recently."""
    with db.get_conn() as conn, db.cursor(conn) as cur:
        cur.execute(
            "SELECT node_id, url FROM nodes WHERE in_ring "
            "AND last_seen > now() - make_interval(secs => %s) ORDER BY node_id",
            (HEARTBEAT_TIMEOUT,),
        )
        rows = cur.fetchall()
    members = tuple(r["node_id"] for r in rows)
    with _ring_lock:
        if _ring_cache["members"] != members:
            _ring_cache["ring"] = HashRing(members, vnodes=VNODES)
            _ring_cache["members"] = members
        ring = _ring_cache["ring"]
    return ring, {r["node_id"]: r["url"] for r in rows}


def node_infos(node_ids=None):
    """node_id -> {node_id, url, status}; status is 'alive' or 'failed' (no recent heartbeat)."""
    with db.get_conn() as conn, db.cursor(conn) as cur:
        if node_ids is None:
            cur.execute(
                "SELECT node_id, url, EXTRACT(EPOCH FROM (now() - last_seen))::float AS age FROM nodes"
            )
        else:
            if not node_ids:
                return {}
            cur.execute(
                "SELECT node_id, url, EXTRACT(EPOCH FROM (now() - last_seen))::float AS age "
                "FROM nodes WHERE node_id = ANY(%s)",
                (list(node_ids),),
            )
        rows = cur.fetchall()
    return {
        r["node_id"]: {
            "node_id": r["node_id"],
            "url": r["url"],
            "status": "alive" if r["age"] <= HEARTBEAT_TIMEOUT else "failed",
        }
        for r in rows
    }