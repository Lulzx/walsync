"""The replication loop.

Continuously copies the SQLite WAL to zs3 as it grows, and periodically takes a snapshot
(a checkpointed copy of the main database). See README for the model.
"""

from __future__ import annotations

import os
import shutil
import sqlite3
import tempfile
import time

from . import wal
from .store import Store


class Replicator:
    def __init__(
        self,
        store: Store,
        db: str,
        interval: float = 1.0,
        snapshot_interval: float = 60.0,
        wal_threshold: int = 16 * 1024 * 1024,
    ):
        self.store = store
        self.db = db
        self.interval = interval
        self.snapshot_interval = snapshot_interval
        self.wal_threshold = wal_threshold
        self.gen = 0
        self.last_uploaded = 0
        # Start the clock now so the first snapshot waits a full snapshot_interval
        # instead of firing immediately (time.monotonic() is large, not 0).
        self.last_snapshot = time.monotonic()
        self._stop = False

    def stop(self) -> None:
        self._stop = True

    def _open(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA wal_autocheckpoint=0")
        return conn

    def run(self) -> None:
        conn = self._open()
        try:
            while not self._stop:
                self._tick(conn)
                time.sleep(self.interval)
        finally:
            conn.close()

    def _tick(self, conn: sqlite3.Connection) -> None:
        size = wal.wal_size(self.db)
        if size > self.last_uploaded:
            data = wal.read_wal_range(self.db, self.last_uploaded, size)
            self.store.put_segment(self.gen, self.last_uploaded, data)
            self.last_uploaded = size

        now = time.monotonic()
        if now - self.last_snapshot >= self.snapshot_interval or size >= self.wal_threshold:
            self._snapshot(conn)

    def _snapshot(self, conn: sqlite3.Connection) -> None:
        # Merge the current generation's WAL into the main DB and empty the WAL.
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")

        # Copy the (now stable) main DB to a temp file and upload it. The snapshot
        # has no WAL, so it's disjoint from the segments uploaded afterwards.
        fd, tmp_path = tempfile.mkstemp()
        os.close(fd)
        try:
            shutil.copy2(self.db, tmp_path)
            with open(tmp_path, "rb") as f:
                self.store.put_snapshot(self.gen, f.read())
            self.store.put_snapshot_meta(self.gen)
        finally:
            os.unlink(tmp_path)

        # Segments from this and earlier generations are now folded into the
        # snapshot; drop them so they don't accumulate.
        for g in range(self.gen + 1):
            for off in self.store.list_segments(g):
                self.store.delete(self.store._seg_key(g, off))

        self.last_uploaded = 0
        self.gen += 1
        self.last_snapshot = time.monotonic()
