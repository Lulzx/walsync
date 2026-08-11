# zs3-replicator

**SQLite for objects, replicated.** Continuously replicate a SQLite database to
[zs3](https://github.com/your/zs3) (or any S3-compatible store) and restore it on demand.
A small, Litestream-style agent: watch the WAL, upload segments, snapshot periodically,
restore from the latest snapshot + WAL.

## Why

SQLite is the most reliable database on earth and the worst at being a server. zs3 is a
tiny S3-compatible store. This agent bridges them: your app keeps writing to a local
SQLite file, and every change is continuously copied to zs3. If the machine dies, restore
the database from zs3 and lose at most a few seconds of writes.

## How it works

SQLite in WAL mode keeps an append-only `-wal` file; the main `.db` file is only rewritten
during a checkpoint. The agent exploits that:

- **Segments** — contiguous byte ranges of the WAL, uploaded as it grows
  (`seg-<gen>-<offset>.wal`). Written with `If-None-Match: *` so a segment is never
  clobbered.
- **Snapshot** — a checkpointed copy of the main `.db` (no WAL), taken periodically
  (`snapshot-<gen>.db` + `snapshot-<gen>.meta`).
- **Generation** — the WAL between two checkpoints. Each generation has its own WAL header
  and salt; the first segment of a generation (offset 0) carries that header.

Because a snapshot is a checkpointed main DB (no WAL) and segments are post-checkpoint
frames, the two are disjoint — restore never double-applies a transaction.

**Restore** downloads the latest snapshot, reconstructs the current generation's WAL from
its segments, writes it to `dest-wal`, and opens the DB so SQLite recovers from it.

## Install

```bash
pip install -r requirements.txt   # boto3
```

## Usage

Start zs3 (see the zs3 repo):

```bash
zig build -Doptimize=ReleaseSmall
./zig-out/bin/zs3 --port=9000
```

Replicate a database (credentials via `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`):

```bash
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin

python3 -m zs3replicator replicate \
  --db /path/to/app.db \
  --endpoint http://localhost:9000 \
  --bucket repl --prefix app \
  --snapshot-interval 60
```

Restore it elsewhere:

```bash
python3 -m zs3replicator restore \
  --endpoint http://localhost:9000 \
  --bucket repl --prefix app \
  --dest /path/to/restored.db
```

### Options

| Flag | Default | Meaning |
|------|---------|---------|
| `--interval` | 1.0s | how often to poll the WAL |
| `--snapshot-interval` | 60s | how often to take a snapshot |
| `--wal-threshold` | 16MB | snapshot early when the WAL exceeds this |

## Assumptions & limitations

- The application must use **WAL mode** (`PRAGMA journal_mode=WAL`). The agent sets
  `wal_autocheckpoint=0` on its own connection.
- A concurrent checkpoint from the application during the snapshot copy is the one race
  not fully closed (the agent's own checkpoints are coordinated). For most single-writer
  apps this never happens.
- Restore is to the **latest snapshot + current generation's segments** — no point-in-time
  history yet.

## Layout

```
zs3replicator/
├── wal.py          # read the SQLite WAL as an opaque byte stream
├── store.py        # thin boto3 wrapper (segments, snapshots, listing)
├── replicator.py   # the replication loop
├── restore.py      # restore from snapshot + WAL segments
└── __main__.py     # CLI
```

## Test

```bash
python3 test_replicator.py   # end-to-end against a live zs3 on :9000
```
