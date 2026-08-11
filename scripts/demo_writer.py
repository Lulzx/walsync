"""Demo writer — run on Mac A.

Continuously inserts a row into a local SQLite DB and replicates it to a local zs3
distributed node. Run alongside ``demo_reader.py`` on the other Mac to watch the rows
appear on the replica.

    python3 scripts/demo_writer.py --endpoint http://localhost:9001

Credentials come from ``AWS_ACCESS_KEY_ID``/``AWS_SECRET_ACCESS_KEY`` (defaults to
minioadmin for a demo).
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from walsync.replicator import Replicator
from walsync.store import Store


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--endpoint", default="http://localhost:9001")
    p.add_argument("--bucket", default="demo")
    p.add_argument("--prefix", default="app")
    p.add_argument("--db", default="app.db")
    p.add_argument("--every", type=float, default=2.0, help="seconds between inserts")
    p.add_argument("--access", default="minioadmin")
    p.add_argument("--secret", default="minioadmin")
    args = p.parse_args()

    store = Store(args.endpoint, args.bucket, args.prefix,
                  os.environ.get("AWS_ACCESS_KEY_ID", args.access),
                  os.environ.get("AWS_SECRET_ACCESS_KEY", args.secret))
    try:
        store.client.create_bucket(Bucket=args.bucket)
    except Exception:
        pass  # already exists

    conn = sqlite3.connect(args.db)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA wal_autocheckpoint=0")
    conn.execute("CREATE TABLE IF NOT EXISTS t(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT, ts TEXT)")
    conn.commit()

    rep = Replicator(store, args.db, interval=0.2, snapshot_interval=5.0)
    threading.Thread(target=rep.run, daemon=True).start()

    print(f"writer: replicating {args.db} to {args.endpoint} ({args.bucket}/{args.prefix})")
    i = 0
    try:
        while True:
            i += 1
            conn.execute("INSERT INTO t(v, ts) VALUES(?, ?)",
                         (f"row-{i}", time.strftime("%H:%M:%S")))
            conn.commit()
            print(f"wrote row-{i} at {time.strftime('%H:%M:%S')}", flush=True)
            time.sleep(args.every)
    except KeyboardInterrupt:
        pass
    finally:
        rep.stop()
        conn.close()


if __name__ == "__main__":
    main()
