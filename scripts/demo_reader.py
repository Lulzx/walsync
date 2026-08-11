"""Demo reader — run on Mac B.

Follows a local zs3 distributed node and prints the live row count of the replica DB as
it grows. Each poll prints a line only when the count changes, so the sync from the
writer Mac is visible.

    python3 scripts/demo_reader.py --endpoint http://localhost:9002

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

from walsync.restore import follow
from walsync.store import Store


def replica_count(dest: str) -> int:
    conn = sqlite3.connect(dest, timeout=5)
    try:
        return conn.execute("SELECT count(*) FROM t").fetchone()[0]
    finally:
        conn.close()


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--endpoint", default="http://localhost:9002")
    p.add_argument("--bucket", default="demo")
    p.add_argument("--prefix", default="app")
    p.add_argument("--dest", default="replica.db")
    p.add_argument("--every", type=float, default=1.0, help="seconds between polls")
    p.add_argument("--access", default="minioadmin")
    p.add_argument("--secret", default="minioadmin")
    args = p.parse_args()

    store = Store(args.endpoint, args.bucket, args.prefix,
                  os.environ.get("AWS_ACCESS_KEY_ID", args.access),
                  os.environ.get("AWS_SECRET_ACCESS_KEY", args.secret))

    stop = threading.Event()
    threading.Thread(target=follow, args=(store, args.dest),
                     kwargs={"interval": 0.2, "stop": stop}, daemon=True).start()

    print(f"reader: following {args.endpoint} ({args.bucket}/{args.prefix}) -> {args.dest}")
    last = -1
    try:
        while True:
            time.sleep(args.every)
            try:
                n = replica_count(args.dest)
            except sqlite3.DatabaseError:
                continue  # mid-rewrite; retry next poll
            if n != last:
                print(f"replica has {n} rows", flush=True)
                last = n
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()


if __name__ == "__main__":
    main()
