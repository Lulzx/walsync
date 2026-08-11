"""Restore a database from zs3, or follow it continuously.

Downloads the latest snapshot (a checkpointed main DB, no WAL), then reconstructs the
current generation's WAL from its segments and lets SQLite recover from it.
"""

from __future__ import annotations

import os
import sqlite3
import threading
import time

from .store import Store


def download_snapshot(store: Store, dest: str) -> int:
    """Fetch the latest snapshot to ``dest``. Returns its generation."""
    gens = store.list_snapshots()
    if not gens:
        raise RuntimeError("no snapshots found for prefix")
    gen = gens[-1]
    with open(dest, "wb") as f:
        f.write(store.get(store._snap_key(gen)))
    # A stale WAL from a previous generation must not survive a fresh snapshot.
    try:
        os.unlink(dest + "-wal")
    except FileNotFoundError:
        pass
    return gen


def apply_segments(store: Store, dest: str, gen: int, last_applied: int) -> int:
    """Apply segments of generation ``gen`` after ``last_applied`` to ``dest``.

    ``last_applied`` is the start offset of the last segment already applied (-1 for
    none). Returns the start offset of the last segment applied, or ``last_applied``
    unchanged if there is nothing new.
    """
    offsets = store.list_segments(gen)
    new = [o for o in offsets if o >= last_applied]
    if not new:
        return last_applied

    if last_applied < 0:
        # Fresh: segment 0 carries the WAL header, so concatenating all segments in
        # offset order yields a valid WAL file.
        wal_data = bytearray()
        for off in offsets:
            wal_data += store.get(store._seg_key(gen, off))
    else:
        # Incremental: the DB already has [0, last_applied); prepend the header from
        # segment 0 and append only the new frames.
        wal_data = bytearray(store.get(store._seg_key(gen, 0))[:32])
        for off in new:
            wal_data += store.get(store._seg_key(gen, off))

    with open(dest + "-wal", "wb") as f:
        f.write(wal_data)

    # Opening the DB triggers WAL recovery; checkpoint to fold it into the main file.
    conn = sqlite3.connect(dest)
    try:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    finally:
        conn.close()

    return new[-1]


def restore(store: Store, dest: str) -> int:
    """Restore to ``dest``. Returns the snapshot generation used."""
    gen = download_snapshot(store, dest)
    apply_segments(store, dest, gen + 1, -1)
    return gen


def follow(store: Store, dest: str, interval: float = 1.0,
           stop: "threading.Event | None" = None) -> None:
    """Continuously keep ``dest`` in sync with the latest snapshot + segments.

    Re-downloads the snapshot when the writer takes a new one, and applies new
    segments as they land in between. Runs until interrupted (or ``stop`` is set).
    """
    last_gen = None
    last_applied = -1
    while True:
        if stop is not None and stop.is_set():
            return
        gens = store.list_snapshots()
        if gens:
            gen = gens[-1]
            if gen != last_gen:
                gen = download_snapshot(store, dest)
                last_gen = gen
                last_applied = -1
            last_applied = apply_segments(store, dest, last_gen + 1, last_applied)
        time.sleep(interval)
