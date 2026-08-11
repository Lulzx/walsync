"""Adversarial follow tests against a 2-node distributed zs3 cluster.

Writer replicates to node A (:9001); the follower pulls from node B (:9002), so every
object crosses zs3's p2p layer. Covers the hard cases:

- follower starts mid-generation (segments already exist, must catch up)
- follower starts before any snapshot (waits, then catches up)
- continuous writes while the follower keeps up

Requires: two `zs3 --distributed` nodes, A on :9001 and B on :9002 (B bootstrapped from
A). Uses the stdlib plus boto3.
"""

import os
import sqlite3
import tempfile
import threading
import time

from zs3replicator.replicator import Replicator
from zs3replicator.restore import follow
from zs3replicator.store import Store

A = Store("http://localhost:9001", "p2p", "app", "minioadmin", "minioadmin")
B = Store("http://localhost:9002", "p2p", "app", "minioadmin", "minioadmin")


def clean() -> None:
    for s in (A, B):
        s.client.create_bucket(Bucket="p2p")
        for obj in s._list("app/"):
            s.delete(obj["Key"])


def make_writer() -> tuple[sqlite3.Connection, str]:
    db = tempfile.mktemp(suffix=".db")
    conn = sqlite3.connect(db)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA wal_autocheckpoint=0")
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
    conn.commit()
    return conn, db


def write(conn: sqlite3.Connection, lo: int, hi: int, step: int = 100) -> None:
    for i in range(lo, hi + 1):
        conn.execute("INSERT INTO t VALUES(?, ?)", (i, f"row{i}"))
        if i % step == 0:
            conn.commit()
    conn.commit()


def assert_synced(db: str, dest: str, expected: int) -> None:
    src = sqlite3.connect(db)
    dst = sqlite3.connect(dest)
    try:
        src_rows = src.execute("SELECT id, v FROM t ORDER BY id").fetchall()
        dst_rows = dst.execute("SELECT id, v FROM t ORDER BY id").fetchall()
    finally:
        src.close()
        dst.close()
    assert len(dst_rows) == expected, f"follower has {len(dst_rows)}, expected {expected}"
    assert dst_rows == src_rows, "follower diverged from writer"


def test_mid_generation_catchup() -> None:
    """Follower starts after segments exist in the current generation, must catch up."""
    clean()
    conn, db = make_writer()
    rep = Replicator(A, db, interval=0.02, snapshot_interval=0.5)
    rt = threading.Thread(target=rep.run, daemon=True)
    rt.start()

    # Writer runs past the first snapshot, so a snapshot exists and gen 1 has segments.
    write(conn, 1, 500)
    time.sleep(0.8)

    # Follower starts now, mid-generation.
    dest = tempfile.mktemp(suffix=".db")
    stop = threading.Event()
    ft = threading.Thread(
        target=follow, args=(B, dest),
        kwargs={"interval": 0.02, "stop": stop}, daemon=True)
    ft.start()

    # Writer keeps going while the follower catches up.
    write(conn, 501, 1000)
    time.sleep(1.0)

    rep.stop(); rt.join(timeout=2)
    stop.set(); ft.join(timeout=2)
    conn.close()

    assert_synced(db, dest, 1000)
    print("OK: mid-generation catch-up, 1000 rows synced A->B")


def test_start_before_snapshot() -> None:
    """Follower starts before any snapshot exists, waits, then catches up."""
    clean()
    conn, db = make_writer()
    rep = Replicator(A, db, interval=0.02, snapshot_interval=1.0)
    rt = threading.Thread(target=rep.run, daemon=True)
    rt.start()

    dest = tempfile.mktemp(suffix=".db")
    stop = threading.Event()
    ft = threading.Thread(
        target=follow, args=(B, dest),
        kwargs={"interval": 0.02, "stop": stop}, daemon=True)
    ft.start()

    # Follower is already polling; writer writes continuously from the start.
    write(conn, 1, 800)
    time.sleep(1.5)

    rep.stop(); rt.join(timeout=2)
    stop.set(); ft.join(timeout=2)
    conn.close()

    assert_synced(db, dest, 800)
    print("OK: follower started before first snapshot, 800 rows synced A->B")


def test_continuous_writes() -> None:
    """Writer writes continuously across several snapshots; follower keeps up."""
    clean()
    conn, db = make_writer()
    rep = Replicator(A, db, interval=0.02, snapshot_interval=0.4)
    rt = threading.Thread(target=rep.run, daemon=True)
    rt.start()

    dest = tempfile.mktemp(suffix=".db")
    stop = threading.Event()
    ft = threading.Thread(
        target=follow, args=(B, dest),
        kwargs={"interval": 0.02, "stop": stop}, daemon=True)
    ft.start()

    # Several generations of writes, spread out so segments land in many batches.
    for lo in range(1, 2001, 200):
        write(conn, lo, lo + 199)
        time.sleep(0.1)
    time.sleep(1.0)

    rep.stop(); rt.join(timeout=2)
    stop.set(); ft.join(timeout=2)
    conn.close()

    assert_synced(db, dest, 2000)
    print("OK: continuous writes across snapshots, 2000 rows synced A->B")


if __name__ == "__main__":
    test_mid_generation_catchup()
    test_start_before_snapshot()
    test_continuous_writes()
    print("All distributed follow tests passed!")
