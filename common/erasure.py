"""Reed-Solomon erasure coding with zfec.

A chunk is cut into k equal data blocks and m parity blocks are computed from them.
Any k of the k+m blocks are enough to rebuild the chunk.

Every block is stored on a node as a "shard" with a small header:
    32 bytes = SHA-256 of the whole chunk, 1 byte = shard number.
The header makes every stored shard unique to its chunk, so two different chunks never share
the same shard file (deleting one chunk can never delete a shard that another chunk needs).
"""
import functools
import hashlib

import zfec

HEADER_LEN = 33


@functools.lru_cache(maxsize=32)
def _encoder(k, n):
    return zfec.Encoder(k, n)


@functools.lru_cache(maxsize=32)
def _decoder(k, n):
    return zfec.Decoder(k, n)


def sha256_hex(data):
    return hashlib.sha256(data).hexdigest()


def split_blocks(data, k):
    """Cut data into k blocks of the same length (the end is padded with zero bytes)."""
    length = max(1, -(-len(data) // k))                      # ceil(len / k)
    padded = bytes(data) + b"\0" * (length * k - len(data))
    return [padded[i * length:(i + 1) * length] for i in range(k)]


def encode(data, k, m):
    """Return k + m shard payloads: indexes 0..k-1 are data blocks, k..k+m-1 are parity blocks."""
    blocks = split_blocks(data, k)
    parity = _encoder(k, k + m).encode(blocks, list(range(k, k + m)))
    return blocks + [bytes(p) for p in parity]


def decode_blocks(available, k, m):
    """available = {shard_index: payload}. Return the k original data blocks (needs at least k shards)."""
    if len(available) < k:
        raise ValueError(f"need {k} shards to rebuild, only {len(available)} available")
    idx = sorted(available)[:k]                              # data shards first: they are the cheapest to use
    blocks = _decoder(k, k + m).decode([available[i] for i in idx], idx)
    return [bytes(b) for b in blocks]


def decode(available, k, m, size):
    """Rebuild the original chunk bytes from any k shards."""
    return b"".join(decode_blocks(available, k, m))[:size]


def rebuild_shards(available, k, m, wanted):
    """Compute the shard payloads listed in 'wanted' from any k available shards."""
    result = {i: available[i] for i in wanted if i in available}
    missing = [i for i in wanted if i not in result]
    if not missing:
        return result
    blocks = decode_blocks(available, k, m)
    parity_needed = [i for i in missing if i >= k]
    for i in missing:
        if i < k:
            result[i] = blocks[i]
    if parity_needed:
        for i, p in zip(parity_needed, _encoder(k, k + m).encode(blocks, parity_needed)):
            result[i] = bytes(p)
    return result


def pack_shard(chunk_hash, index, payload):
    """Shard blob stored on a node = header + payload. Its SHA-256 is the name of the file on the node."""
    return bytes.fromhex(chunk_hash) + bytes([index]) + payload


def unpack_shard(blob):
    """Return (chunk_hash, index, payload) of a shard blob."""
    return blob[:32].hex(), blob[32], blob[HEADER_LEN:]