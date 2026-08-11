"""End-to-end test: replicate a live SQLite DB to zs3, then restore it.

Requires a zs3 server running on :9000 (see the zs3 repo). Uses only the stdlib plus
boto3.
"""

import os
import sqlite3
import tempfile
import threading
import time

from zs3replicator.replicator import Replicator
from zs3replicator.restore import restore
from zs3replicator.store import Store

ENDPOINT = os.environ.get("ZS3_ENDPOINT", "http://localhost:9000")
BUCKET = "repl-test"
PREFIX = "app"
ACCESS = "minioadmin"
SECRET = "minioadmin"


def make_store() -> Store:
    return Store(ENDPOINT, BUCKET, PREFIX, ACCESS, SECRET)


def clean_prefix(store: Store) -> None:
    for obj in store._list(f"{PREFIX}/"):
        store.delete(obj["Key"])


def test_roundtrip() -> None:
    store = make_store()
    store.client.create_bucket(Bucket=BUCKET)
    clean_prefix(store)

    db = tempfile.mktemp(suffix=".db")
    conn = sqlite3.connect(db)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
    conn.execute("INSERT INTO t VALUES(1, 'one')")
    conn.commit()

    rep = Replicator(store, db, interval=0.05, snapshot_interval=0.4)
    thread = threading.Thread(target=rep.run, daemon=True)
    thread.start()

    try:
        # Mutate over time so segments accumulate between snapshots.
        for i in range(2, 6):
            time.sleep(0.15)
            conn.execute("INSERT INTO t VALUES(?, ?)", (i, f"val{i}"))
            conn.commit()
        # Let a snapshot land.
        time.sleep(0.8)
    finally:
        rep.stop()
        thread.join(timeout=2)
        conn.close()

    assert store.list_snapshots(), "expected at least one snapshot"

    dest = tempfile.mktemp(suffix=".db")
    restore(store, dest)

    restored = sqlite3.connect(dest)
    rows = restored.execute("SELECT id, v FROM t ORDER BY id").fetchall()
    restored.close()

    expected = [(1, "one")] + [(i, f"val{i}") for i in range(2, 6)]
    assert rows == expected, f"mismatch: {rows!r} != {expected!r}"
    print(f"OK: restored {len(rows)} rows, snapshot gen {store.list_snapshots()[-1]}")


def test_segment_replay() -> None:
    """A write after the last snapshot is recovered from WAL segments, not the snapshot."""
    store = make_store()
    store.client.create_bucket(Bucket=BUCKET)
    clean_prefix(store)

    db = tempfile.mktemp(suffix=".db")
    conn = sqlite3.connect(db)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("CREATE TABLE t(x)")
    conn.execute("INSERT INTO t VALUES('row1')")
    conn.commit()

    rep = Replicator(store, db, interval=0.05, snapshot_interval=0.5)
    thread = threading.Thread(target=rep.run, daemon=True)
    thread.start()
    try:
        time.sleep(0.6)  # first snapshot lands (gen 0), gen -> 1
        conn.execute("INSERT INTO t VALUES('row2')")  # after the snapshot
        conn.commit()
        time.sleep(0.15)  # segment uploaded (gen 1); next snapshot not due until t=1.0
    finally:
        rep.stop()
        thread.join(timeout=2)
        conn.close()

    # The write is only in a segment, not in the latest snapshot.
    assert store.list_segments(1), "expected a segment in generation 1"

    dest = tempfile.mktemp(suffix=".db")
    restore(store, dest)
    rows = sqlite3.connect(dest).execute("SELECT x FROM t ORDER BY x").fetchall()
    assert rows == [("row1",), ("row2",)], f"segment replay failed: {rows!r}"
    print("OK: post-snapshot write recovered via segment replay")


if __name__ == "__main__":
    test_roundtrip()
    test_segment_replay()
    print("All tests passed!")
