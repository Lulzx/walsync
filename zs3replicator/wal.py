"""Reading the SQLite write-ahead log (WAL) file.

The WAL is an append-only file next to the database (``<db>-wal``). It starts with a
32-byte header (magic, page size, checkpoint sequence, salt, checksum) followed by frames.
We treat it as an opaque byte stream: segments are contiguous byte ranges of the WAL, and
the first segment of a generation (offset 0) carries that generation's header.
"""

from __future__ import annotations

import os

WAL_HEADER_SIZE = 32


def wal_path(db: str) -> str:
    return db + "-wal"


def wal_size(db: str) -> int:
    """Size of the WAL file in bytes, or 0 if it doesn't exist yet."""
    try:
        return os.path.getsize(wal_path(db))
    except FileNotFoundError:
        return 0


def read_wal_range(db: str, start: int, end: int) -> bytes:
    """Read WAL bytes ``[start, end)``."""
    with open(wal_path(db), "rb") as f:
        f.seek(start)
        return f.read(end - start)


def wal_header(db: str) -> bytes:
    """The 32-byte WAL header, or b"" if the WAL doesn't exist."""
    with open(wal_path(db), "rb") as f:
        return f.read(WAL_HEADER_SIZE)
