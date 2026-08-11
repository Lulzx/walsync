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


def wal_salt(db: str) -> bytes:
    """The 8-byte salt from the WAL header (bytes 12-20), or b"" if no WAL.

    SQLite generates a fresh salt for every new WAL, so a change in salt means the WAL
    was truncated and a new generation started (e.g. an app-initiated checkpoint).
    """
    h = wal_header(db)
    if len(h) < 20:
        return b""
    return h[12:20]
