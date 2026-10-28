"""Splitting data into chunks: fixed-size and content-defined (FastCDC).

Both functions return/yield (sha256_hex, chunk_bytes) in file order.
"""
import hashlib
import mmap
import os

CHUNK_SIZE = 4 * 1024 * 1024          # default fixed chunk size: 4 MB
CDC_AVG_SIZE = 4 * 1024 * 1024        # default FastCDC average size: 4 MB


def sha256_hex(data):
    return hashlib.sha256(data).hexdigest()


# ---------------- fixed-size ----------------
def fixed_chunks_bytes(data, size=CHUNK_SIZE):
    for i in range(0, len(data), size):
        piece = bytes(data[i:i + size])
        yield sha256_hex(piece), piece


def fixed_chunks_file(path, size=CHUNK_SIZE):
    with open(path, "rb") as f:
        while True:
            piece = f.read(size)
            if not piece:
                break
            yield sha256_hex(piece), piece


# ---------------- content-defined (FastCDC) ----------------
def _cdc_sizes(avg):
    return max(64, avg // 4), avg, avg * 4      # min, avg, max


def cdc_chunks_bytes(data, avg=CDC_AVG_SIZE):
    from fastcdc import fastcdc     # imported here so services that do not chunk do not need it
    mn, av, mx = _cdc_sizes(avg)
    view = memoryview(data)
    for c in fastcdc(view, min_size=mn, avg_size=av, max_size=mx):
        piece = bytes(view[c.offset:c.offset + c.length])
        yield sha256_hex(piece), piece


def cdc_chunks_file(path, avg=CDC_AVG_SIZE):
    from fastcdc import fastcdc
    if os.path.getsize(path) == 0:
        return
    mn, av, mx = _cdc_sizes(avg)
    with open(path, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        try:
            for c in fastcdc(mm, min_size=mn, avg_size=av, max_size=mx):
                piece = mm[c.offset:c.offset + c.length]
                yield sha256_hex(piece), piece
        finally:
            mm.close()


# ---------------- one entry point ----------------
def chunk_bytes(data, method="fixed", size=CHUNK_SIZE):
    """method = 'fixed' or 'cdc'. For 'cdc', size is the average chunk size."""
    if method == "fixed":
        return fixed_chunks_bytes(data, size)
    if method == "cdc":
        return cdc_chunks_bytes(data, size)
    raise ValueError("method must be 'fixed' or 'cdc'")


def chunk_file(path, method="fixed", size=CHUNK_SIZE):
    if method == "fixed":
        return fixed_chunks_file(path, size)
    if method == "cdc":
        return cdc_chunks_file(path, size)
    raise ValueError("method must be 'fixed' or 'cdc'")