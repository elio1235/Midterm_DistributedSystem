"""Consistent hashing ring with virtual nodes.

Every storage node is placed on the ring many times (virtual nodes).
A chunk is mapped to a point on the ring (the first 64 bits of its SHA-256),
and its replicas live on the next distinct nodes found walking clockwise.
Adding or removing one node only moves the chunks next to its points.
"""
import bisect
import hashlib

DEFAULT_VNODES = 128


class HashRing:
    def __init__(self, nodes=None, vnodes=DEFAULT_VNODES):
        self.vnodes = vnodes
        self._keys = []      # sorted positions on the ring
        self._owner = {}     # position -> node id
        self._nodes = set()
        for node_id in nodes or []:
            self.add_node(node_id)

    # ---- hashing helpers ----
    @staticmethod
    def _hash(text):
        return int.from_bytes(hashlib.sha256(text.encode()).digest()[:8], "big")

    @staticmethod
    def position(key):
        """Ring position of a chunk hash (hex string). Other keys are hashed first."""
        try:
            return int(key[:16], 16)
        except ValueError:
            return HashRing._hash(key)

    # ---- membership ----
    @property
    def nodes(self):
        return sorted(self._nodes)

    def __len__(self):
        return len(self._nodes)

    def add_node(self, node_id):
        if node_id in self._nodes:
            return
        self._nodes.add(node_id)
        for i in range(self.vnodes):
            pos = self._hash(f"{node_id}#{i}")
            if pos in self._owner:      # astronomically rare collision: skip this point
                continue
            self._owner[pos] = node_id
            bisect.insort(self._keys, pos)

    def remove_node(self, node_id):
        if node_id not in self._nodes:
            return
        self._nodes.discard(node_id)
        self._owner = {p: o for p, o in self._owner.items() if o != node_id}
        self._keys = sorted(self._owner)

    # ---- lookup ----
    def get_nodes(self, key, n=1):
        """Return up to n distinct node ids responsible for this key (first = primary)."""
        if not self._keys:
            return []
        n = min(n, len(self._nodes))
        start = bisect.bisect_right(self._keys, self.position(key))
        result = []
        total = len(self._keys)
        for i in range(total):
            node = self._owner[self._keys[(start + i) % total]]
            if node not in result:
                result.append(node)
                if len(result) == n:
                    break
        return result