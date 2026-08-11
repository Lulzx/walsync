"""Thin boto3 wrapper for the zs3 S3 API.

Segments and snapshots are stored under a prefix:

    <prefix>/seg-<gen>-<offset>.wal     WAL byte range [offset, next)
    <prefix>/snapshot-<gen>.db          checkpointed main database
    <prefix>/snapshot-<gen>.meta        {"gen": G}

Segments are immutable once written, so ``put_segment`` uses ``If-None-Match: *`` to
never clobber an existing segment (a retry that 412s means the segment is already there
with identical content).
"""

from __future__ import annotations

import json
from typing import Iterable

import boto3
from botocore.exceptions import ClientError


class Store:
    def __init__(self, endpoint: str, bucket: str, prefix: str,
                 access_key: str, secret_key: str, region: str = "us-east-1"):
        self.bucket = bucket
        self.prefix = prefix.rstrip("/")
        self.client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
        )

    # -- key helpers -----------------------------------------------------
    def _seg_key(self, gen: int, offset: int) -> str:
        return f"{self.prefix}/seg-{gen}-{offset}.wal"

    def _snap_key(self, gen: int) -> str:
        return f"{self.prefix}/snapshot-{gen}.db"

    def _meta_key(self, gen: int) -> str:
        return f"{self.prefix}/snapshot-{gen}.meta"

    # -- writes ----------------------------------------------------------
    def put_segment(self, gen: int, offset: int, data: bytes) -> None:
        """Upload a WAL segment, never clobbering an existing one."""
        try:
            self.client.put_object(
                Bucket=self.bucket, Key=self._seg_key(gen, offset),
                Body=data, IfNoneMatch="*",
            )
        except ClientError as e:
            if e.response["ResponseMetadata"]["HTTPStatusCode"] == 412:
                return  # already present with identical content
            raise

    def put_snapshot(self, gen: int, data: bytes) -> None:
        self.client.put_object(Bucket=self.bucket, Key=self._snap_key(gen), Body=data)

    def put_snapshot_meta(self, gen: int) -> None:
        body = json.dumps({"gen": gen}).encode()
        self.client.put_object(Bucket=self.bucket, Key=self._meta_key(gen), Body=body)

    # -- reads -----------------------------------------------------------
    def get(self, key: str) -> bytes:
        return self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    def list_segments(self, gen: int) -> list[int]:
        """Offsets of all segments in a generation, sorted ascending."""
        prefix = f"{self.prefix}/seg-{gen}-"
        offsets = []
        for obj in self._list(prefix):
            name = obj["Key"][len(prefix):]
            if name.endswith(".wal"):
                offsets.append(int(name[: -len(".wal")]))
        return sorted(offsets)

    def list_snapshots(self) -> list[int]:
        """Generations that have a snapshot, sorted ascending."""
        prefix = f"{self.prefix}/snapshot-"
        gens = []
        for obj in self._list(prefix):
            name = obj["Key"][len(prefix):]
            if name.endswith(".meta"):
                gens.append(int(name[: -len(".meta")]))
        return sorted(gens)

    def delete(self, key: str) -> None:
        self.client.delete_object(Bucket=self.bucket, Key=key)

    def _list(self, prefix: str) -> Iterable[dict]:
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                yield obj
