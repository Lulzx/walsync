# zs3-replicator

SQLite, replicated to S3. Continuously. Restore on demand.

Your app keeps writing to a local SQLite file. This agent copies every change to zs3
(or any S3-compatible store) as it happens. Machine dies? Restore the DB, lose seconds.

## The trick

SQLite in WAL mode keeps an append-only `-wal` file; the main `.db` only changes on a
checkpoint. So:

- **Segments** — copy the WAL as it grows (`seg-<gen>-<offset>.wal`). Written with
  `If-None-Match: *` so a segment is never clobbered.
- **Snapshots** — checkpoint, then copy the main `.db` (`snapshot-<gen>.db`).
- **Restore** — grab the latest snapshot, rebuild the WAL from segments, let SQLite
  recover.

Snapshot has no WAL. Segments are post-checkpoint. Disjoint. No double-apply. That's the
whole design.

## Use

```bash
pip install -r requirements.txt

# replicate
python3 -m zs3replicator replicate \
  --db app.db --endpoint http://localhost:9000 \
  --bucket repl --prefix app --snapshot-interval 60

# restore
python3 -m zs3replicator restore \
  --endpoint http://localhost:9000 --bucket repl --prefix app --dest restored.db
```

Credentials: `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`.

## Layout

```
wal.py        read the WAL as bytes
store.py      boto3 wrapper
replicator.py the loop
restore.py    restore
__main__.py   CLI
```

## Caveats

- App must use WAL mode.
- Restore is to the latest snapshot + current segments. No point-in-time history yet.
- A concurrent checkpoint during the snapshot copy is the one race not closed. Fine for
  single-writer apps.

## Test

```bash
python3 test_replicator.py   # needs zs3 on :9000
```
