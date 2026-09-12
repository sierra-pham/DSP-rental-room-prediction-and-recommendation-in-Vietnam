"""S3 batch writer: buffers JSONL records and flushes as gzipped parts."""

import gzip
import io
import json
from pathlib import Path


class S3BatchWriter:
    def __init__(self, bucket: str | None, prefix: str, batch_mb: float = 128):
        self._bucket = bucket
        self._prefix = prefix
        self._batch_bytes = int(batch_mb * 1024 * 1024)
        self._buffer = io.BytesIO()
        self._gz = gzip.GzipFile(fileobj=self._buffer, mode="wb")
        self._part = 0

    def write(self, record: dict) -> None:
        line = json.dumps(record, ensure_ascii=False) + "\n"
        self._gz.write(line.encode("utf-8"))
        if self._buffer.tell() >= self._batch_bytes:
            self._flush_part()

    def flush(self) -> None:
        if self._buffer.tell() > 0:
            self._flush_part()

    def _flush_part(self) -> None:
        self._gz.close()
        data = self._buffer.getvalue()
        key = f"part-{self._part:04d}.jsonl.gz"

        if self._bucket is None:
            path = Path(self._prefix) / key
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
        else:
            import boto3

            s3 = boto3.client("s3")
            s3.put_object(Bucket=self._bucket, Key=f"{self._prefix}/{key}", Body=data)

        self._part += 1
        self._buffer = io.BytesIO()
        self._gz = gzip.GzipFile(fileobj=self._buffer, mode="wb")
