"""Restore a database from zs3.

Downloads the latest snapshot (a checkpointed main DB, no WAL), then reconstructs the
current generation's WAL from its segments and lets SQLite recover from it.
"""

from __future__ import annotations

import sqlite3

from .store import Store


def restore(store: Store, dest: str) -> int:
    """Restore to ``dest``. Returns the snapshot generation used."""
    gens = store.list_snapshots()
    if not gens:
        raise RuntimeError("no snapshots found for prefix")

    gen = gens[-1]

    # Latest snapshot: a checkpointed main DB with no WAL.
    with open(dest, "wb") as f:
        f.write(store.get(store._snap_key(gen)))

    # Reconstruct the WAL from the next generation's segments. The first segment
    # (offset 0) carries the WAL header, so concatenating in offset order yields a
    # valid WAL file.
    next_gen = gen + 1
    offsets = store.list_segments(next_gen)
    if offsets:
        wal_data = bytearray()
        for off in offsets:
            wal_data += store.get(store._seg_key(next_gen, off))
        with open(dest + "-wal", "wb") as f:
            f.write(wal_data)

    # Opening the DB triggers WAL recovery; checkpoint to fold it into the main file.
    conn = sqlite3.connect(dest)
    try:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    finally:
        conn.close()

    return gen
