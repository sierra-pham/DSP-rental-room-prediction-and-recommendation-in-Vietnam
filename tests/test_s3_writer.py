import gzip
import json

from crawler.s3_writer import S3BatchWriter


def test_writer_batches_and_flushes(tmp_path):
    w = S3BatchWriter(bucket=None, prefix=str(tmp_path), batch_mb=0.001)
    for i in range(50):
        w.write({"listing_id": f"x:{i}", "html_gz_b64": "A" * 500})
    w.flush()
    files = list(tmp_path.rglob("*.jsonl.gz"))
    assert len(files) >= 1
    with gzip.open(files[0], "rt", encoding="utf-8") as fh:
        rows = [json.loads(line) for line in fh]
    assert rows[0]["listing_id"] == "x:0"
