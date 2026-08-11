"""Command-line interface: ``python3 -m zs3replicator replicate|restore``.

Credentials come from the standard ``AWS_ACCESS_KEY_ID`` / ``AWS_SECRET_ACCESS_KEY``
environment variables.
"""

from __future__ import annotations

import argparse
import os

from .replicator import Replicator
from .restore import restore
from .store import Store


def _store(args: argparse.Namespace) -> Store:
    return Store(
        endpoint=args.endpoint,
        bucket=args.bucket,
        prefix=args.prefix,
        access_key=os.environ.get("AWS_ACCESS_KEY_ID", ""),
        secret_key=os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
    )


def _cmd_replicate(args: argparse.Namespace) -> None:
    store = _store(args)
    Replicator(
        store,
        args.db,
        interval=args.interval,
        snapshot_interval=args.snapshot_interval,
        wal_threshold=args.wal_threshold,
    ).run()


def _cmd_restore(args: argparse.Namespace) -> None:
    store = _store(args)
    gen = restore(store, args.dest)
    print(f"restored from snapshot generation {gen} to {args.dest}")


def main() -> None:
    parser = argparse.ArgumentParser(prog="zs3replicator")
    sub = parser.add_subparsers(dest="command", required=True)

    rep = sub.add_parser("replicate", help="continuously replicate a SQLite DB to zs3")
    rep.add_argument("--db", required=True, help="path to the SQLite database")
    rep.add_argument("--endpoint", required=True, help="zs3/S3 endpoint URL")
    rep.add_argument("--bucket", required=True)
    rep.add_argument("--prefix", required=True, help="object key prefix")
    rep.add_argument("--interval", type=float, default=1.0, help="poll interval (s)")
    rep.add_argument("--snapshot-interval", type=float, default=60.0, help="snapshot cadence (s)")
    rep.add_argument("--wal-threshold", type=int, default=16 * 1024 * 1024,
                     help="snapshot when the WAL exceeds this many bytes")
    rep.set_defaults(func=_cmd_replicate)

    res = sub.add_parser("restore", help="restore a database from zs3")
    res.add_argument("--endpoint", required=True)
    res.add_argument("--bucket", required=True)
    res.add_argument("--prefix", required=True)
    res.add_argument("--dest", required=True, help="path to write the restored database")
    res.set_defaults(func=_cmd_restore)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
