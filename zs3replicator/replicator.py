"""The replication loop.

Continuously copies the SQLite WAL to zs3 as it grows, and periodically takes a snapshot
(a checkpointed copy of the main database). See README for the model.
"""

from __future__ import annotations

import os
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
        # Salt of the current generation's WAL header. None means "unknown" — the
        # WAL was just truncated by our own snapshot (or we haven't seen it yet), so
        # the next salt we read is the start of a fresh generation, not an app
        # checkpoint. A salt that changes while this is set means the app checkpointed.
        self.wal_salt = None
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

        # Detect an app-initiated checkpoint: it truncates the WAL and starts a new
        # generation (fresh salt). If we just keep uploading, segments would span two
        # generations and restore would miss the writes that landed after the latest
        # snapshot. So snapshot immediately to fold the checkpointed state in.
        if size == 0:
            if self.last_uploaded > 0:
                self._snapshot(conn)
                return
        else:
            salt = wal.wal_salt(self.db)
            if self.wal_salt is None:
                self.wal_salt = salt  # fresh WAL (startup or after our snapshot)
            elif salt != self.wal_salt:
                self._snapshot(conn)
                return

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

        # Take a consistent snapshot via the backup API rather than copying the main
        # DB file. A raw copy can be torn if the app checkpoints (rewrites the main
        # DB) mid-copy; the backup API is safe under concurrent access.
        fd, tmp_path = tempfile.mkstemp()
        os.close(fd)
        try:
            dst = sqlite3.connect(tmp_path)
            try:
                conn.backup(dst)
            finally:
                dst.close()
            # The backup may carry a WAL; fold it in so the snapshot is a clean
            # no-WAL main DB, keeping it disjoint from the segments uploaded after.
            d2 = sqlite3.connect(tmp_path)
            try:
                d2.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            finally:
                d2.close()
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
        # Our own checkpoint truncated the WAL; the next salt we see is the start of
        # the new generation, not an app checkpoint.
        self.wal_salt = None
        self.last_snapshot = time.monotonic()
