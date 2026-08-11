# zs3-replicator

SQLite, replicated to S3. Continuously. Restore on demand. Sync across machines.

Your app keeps writing to a local SQLite file. This agent copies every change to zs3
(or any S3-compatible store) as it happens. Machine dies? Restore the DB, lose seconds.
Run zs3 in distributed mode and the same DB is available on every machine in the cluster.

## The trick

SQLite in WAL mode keeps an append-only `-wal` file; the main `.db` only changes on a
checkpoint. So:

- **Segments** — copy the WAL as it grows (`seg-<gen>-<offset>.wal`). Written with
  `If-None-Match: *` so a segment is never clobbered.
- **Snapshots** — checkpoint, then take a consistent copy via SQLite's backup API
  (`snapshot-<gen>.db`). Safe even if the app checkpoints mid-copy.
- **Restore** — grab the latest snapshot, rebuild the WAL from segments, let SQLite
  recover.

Snapshot has no WAL. Segments are post-checkpoint. Disjoint. No double-apply. That's the
whole design.

If the app checkpoints on its own (its `wal_autocheckpoint`), the replicator spots the
truncated WAL and snapshots immediately — so a restore never misses a write.

## Use

```bash
pip install -r requirements.txt

# replicate (writer machine)
python3 -m walsync replicate \
  --db app.db --endpoint http://localhost:9000 \
  --bucket repl --prefix app --snapshot-interval 60

# restore (one-shot)
python3 -m walsync restore \
  --endpoint http://localhost:9000 --bucket repl --prefix app --dest restored.db

# follow (keep a local DB in sync, continuously)
python3 -m walsync follow \
  --endpoint http://localhost:9000 --bucket repl --prefix app --dest replica.db
```

Credentials: `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`.

## Sync across machines (p2p)

zs3's distributed mode is a p2p store: write to any node, read from any node. The
replicator just speaks S3, so it works unchanged. Point each machine at its local zs3
node and the data fans out:

```
machine A (writer)          machine B (reader)
  app.db  --replicate-->  zs3 -d  <--p2p-->  zs3 -d  --follow-->  replica.db
```

- Writer runs `replicate` against its local `zs3 --distributed` node.
- Readers run `follow` against their local `zs3 --distributed` node. `follow` re-downloads
  the snapshot when a new one lands and applies new segments as they arrive — near-real-time.
- One writer, many readers. SQLite is single-writer; multi-writer is out of scope.

## Layout

```
wal.py        read the WAL as bytes
store.py      boto3 wrapper
replicator.py the loop
restore.py    restore + follow
__main__.py   CLI
```

## Caveats

- App must use WAL mode.
- Restore is to the latest snapshot + current segments. No point-in-time history yet.
- The snapshot copy is race-free (backup API); an app-initiated checkpoint is folded in
  by an immediate snapshot.

## Test

```bash
python3 test_replicator.py   # needs zs3 on :9000
```
