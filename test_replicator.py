"""End-to-end test: replicate a live SQLite DB to zs3, then restore it.

Requires a zs3 server running on :9000 (see the zs3 repo). Uses only the stdlib plus
boto3.
"""

import os
import sqlite3
import tempfile
import threading
import time

from walsync.replicator import Replicator
from walsync.restore import follow, restore
from walsync.store import Store

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


def test_app_checkpoint() -> None:
    """An app-initiated checkpoint mid-replication must not lose writes."""
    store = make_store()
    store.client.create_bucket(Bucket=BUCKET)
    clean_prefix(store)

    db = tempfile.mktemp(suffix=".db")
    conn = sqlite3.connect(db)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA wal_autocheckpoint=0")
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
    conn.execute("INSERT INTO t VALUES(1, 'one')")
    conn.commit()

    # Long snapshot cadence: the only snapshot is the one forced by the app's
    # checkpoint, which must fold the checkpointed writes in.
    rep = Replicator(store, db, interval=0.05, snapshot_interval=10.0)
    thread = threading.Thread(target=rep.run, daemon=True)
    thread.start()
    try:
        conn.execute("INSERT INTO t VALUES(2, 'two')")
        conn.commit()
        time.sleep(0.2)
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")  # app checkpoint
        conn.execute("INSERT INTO t VALUES(3, 'three')")
        conn.commit()
        time.sleep(0.2)
    finally:
        rep.stop()
        thread.join(timeout=2)
        conn.close()

    dest = tempfile.mktemp(suffix=".db")
    restore(store, dest)
    rows = sqlite3.connect(dest).execute("SELECT id, v FROM t ORDER BY id").fetchall()
    assert rows == [(1, "one"), (2, "two"), (3, "three")], f"lost writes: {rows!r}"
    print("OK: app checkpoint mid-replication recovered all writes")


def test_follow() -> None:
    """A follower keeps a local DB in sync as the writer replicates."""
    store = make_store()
    store.client.create_bucket(Bucket=BUCKET)
    clean_prefix(store)

    db = tempfile.mktemp(suffix=".db")
    conn = sqlite3.connect(db)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA wal_autocheckpoint=0")
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
    conn.execute("INSERT INTO t VALUES(1, 'one')")
    conn.commit()

    rep = Replicator(store, db, interval=0.05, snapshot_interval=0.4)
    rep_thread = threading.Thread(target=rep.run, daemon=True)
    rep_thread.start()

    dest = tempfile.mktemp(suffix=".db")
    stop_follow = threading.Event()
    fol_thread = threading.Thread(
        target=follow, args=(store, dest),
        kwargs={"interval": 0.05, "stop": stop_follow}, daemon=True)
    fol_thread.start()

    try:
        for i in range(2, 6):
            time.sleep(0.15)
            conn.execute("INSERT INTO t VALUES(?, ?)", (i, f"val{i}"))
            conn.commit()
        time.sleep(0.8)  # let snapshots + segments propagate
    finally:
        rep.stop()
        rep_thread.join(timeout=2)
        stop_follow.set()
        fol_thread.join(timeout=2)
        conn.close()

    rows = sqlite3.connect(dest).execute("SELECT id, v FROM t ORDER BY id").fetchall()
    expected = [(1, "one")] + [(i, f"val{i}") for i in range(2, 6)]
    assert rows == expected, f"follower out of sync: {rows!r} != {expected!r}"
    print("OK: follower kept local DB in sync")


def test_follow_stress() -> None:
    """Heavy: many segments in one generation, follower applies them incrementally.

    Long snapshot cadence + fast poll means the WAL grows into many segments between
    snapshots, so the follower must reconstruct the WAL (header + [last_applied, ...))
    repeatedly as new segments land. The follower's DB must match the writer's exactly.
    """
    store = make_store()
    store.client.create_bucket(Bucket=BUCKET)
    clean_prefix(store)

    db = tempfile.mktemp(suffix=".db")
    conn = sqlite3.connect(db)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA wal_autocheckpoint=0")
    conn.execute("PRAGMA page_size=8192")
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, n REAL)")
    conn.commit()

    # Snapshot every 1s so the follower has a snapshot to start from, but the fast
    # poll + continuous writes still pile many segments into each generation, which
    # the follower must apply incrementally (header + [last_applied, ...)).
    rep = Replicator(store, db, interval=0.01, snapshot_interval=1.0,
                     wal_threshold=64 * 1024 * 1024)
    rep_thread = threading.Thread(target=rep.run, daemon=True)
    rep_thread.start()

    dest = tempfile.mktemp(suffix=".db")
    stop_follow = threading.Event()
    fol_thread = threading.Thread(
        target=follow, args=(store, dest),
        kwargs={"interval": 0.01, "stop": stop_follow}, daemon=True)
    fol_thread.start()

    try:
        # Spread the writes out so the follower applies segments in many batches.
        for i in range(1, 3001):
            conn.execute("INSERT INTO t VALUES(?, ?, ?)", (i, f"row{i}", i * 1.5))
            if i % 50 == 0:
                conn.commit()
                time.sleep(0.05)
        conn.commit()
        time.sleep(1.0)  # let the tail segments propagate
    finally:
        rep.stop()
        rep_thread.join(timeout=2)
        stop_follow.set()
        fol_thread.join(timeout=2)
        conn.close()

    src = sqlite3.connect(db)
    dst = sqlite3.connect(dest)
    try:
        src_rows = src.execute("SELECT id, v, n FROM t ORDER BY id").fetchall()
        dst_rows = dst.execute("SELECT id, v, n FROM t ORDER BY id").fetchall()
    finally:
        src.close()
        dst.close()

    assert len(dst_rows) == 3000, f"follower has {len(dst_rows)} rows, expected 3000"
    assert dst_rows == src_rows, "follower diverged from writer"
    print("OK: follow stress — 3000 rows, incremental segments, follower matches writer")


if __name__ == "__main__":
    test_roundtrip()
    test_segment_replay()
    test_app_checkpoint()
    test_follow()
    test_follow_stress()
    print("All tests passed!")
