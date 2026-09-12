"""Local Spark tests for the bronze -> silver parse job.

Spark never touches the local filesystem here: this machine has no Hadoop
`winutils.exe`, so `spark.read.json(<local path>)` and parquet writes die in
`NativeIO$Windows.access0`. Every bronze DataFrame is therefore built from JSON
lines in memory --- the same bytes `crawler/spiders/base.py` writes to bronze ---
read through BRONZE_SCHEMA, the schema the real job reads with.
"""

import base64
import gzip
import json
import re
from datetime import date
from pathlib import Path

import pytest

from spark.parse_bronze import (
    BRONZE_SCHEMA,
    SILVER_SCHEMA,
    parse_partition,
    pii_gate,
    quality_gate,
)

FIXTURES = Path(__file__).parent / "fixtures"
DT = "2026-09-20"

# The URLs from tests/test_parsers.py -- each parser reads its native id out of
# the path, so a fixture only parses cleanly under its own URL.
SOURCE_URLS = {
    "batdongsan": "https://batdongsan.com.vn/cho-thue-can-ho/pr46260999",
    "phongtro123": "https://phongtro123.com/x-pr712920.html",
    "mogi": "https://mogi.vn/x-id22742913",
    "nhatot": "https://www.nhatot.com/x/134122706.htm",
}


def _fixture_html(source: str) -> str:
    return (FIXTURES / f"{source}_sample_01.html").read_text(encoding="utf-8")


def _bronze_row(html: str, listing_id: str, source: str = "batdongsan",
                http_status: int = 200, html_gz_b64: str | None = None) -> dict:
    if html_gz_b64 is None:
        html_gz_b64 = base64.b64encode(gzip.compress(html.encode("utf-8"))).decode("ascii")
    return {
        "listing_id": listing_id,
        "source": source,
        "url": SOURCE_URLS.get(source, f"https://{source}.example.vn/x/1"),
        "crawl_ts": "2026-09-20T03:00:00+00:00",
        "http_status": http_status,
        "content_hash": "sha256:abc",
        "html_gz_b64": html_gz_b64,
        "fetch_ms": 500.0,
        "crawl_kind": "discovery",
    }


def _bronze_df(spark, rows: list[dict]):
    lines = spark.sparkContext.parallelize(
        [json.dumps(row, ensure_ascii=False) for row in rows]
    )
    return spark.read.json(lines, schema=BRONZE_SCHEMA)


_CLEAN_SILVER = {
    "listing_id": "batdongsan:pr1", "source": "batdongsan",
    "url": SOURCE_URLS["batdongsan"], "crawl_ts": "2026-09-20T03:00:00+00:00",
    "crawl_date": date(2026, 9, 20), "title": "Cho thue",
    "asking_rent_vnd": 9_500_000, "area_sqm": 45.0, "bedrooms": 1, "bathrooms": 1,
    "property_type": "apartment", "province": "HCM", "district": "Binh Thanh",
    "ward": None, "address": "280/75", "latitude": 10.79, "longitude": 106.69,
    "furnishing": "full", "amenities": [], "description_clean": "can ho dep",
    "posted_date": None, "image_count": None, "content_hash": "sha256:abc",
    "is_active": True, "parse_ok": True, "parse_error": None, "dt": DT,
}


def _silver_df(spark, *overrides: dict):
    """A hand-built silver DataFrame: one row per override, explicit schema."""
    names = [field.name for field in SILVER_SCHEMA.fields]
    rows = [tuple({**_CLEAN_SILVER, **row}[name] for name in names) for row in overrides]
    return spark.createDataFrame(rows, schema=SILVER_SCHEMA)


def test_parse_bronze_local(spark):
    # The bronze listing_id deliberately differs from the one the parser reads
    # out of the URL: bronze, frontier and probes all key on the bronze id, so
    # that is the one silver must carry.
    df = _bronze_df(spark, [_bronze_row(_fixture_html("batdongsan"), "batdongsan:pr1")])

    rows = parse_partition(df, DT).collect()

    assert len(rows) == 1
    row = rows[0]
    assert row["listing_id"] == "batdongsan:pr1"
    assert row["source"] == "batdongsan"
    assert row["province"] == "HCM"
    assert row["parse_ok"] is True
    assert row["parse_error"] is None
    assert row["is_active"] is True
    assert row["crawl_date"] == date(2026, 9, 20)
    assert row["dt"] == DT
    assert row["asking_rent_vnd"] == 9_500_000
    assert row["area_sqm"] == 45.0
    assert row["property_type"] == "apartment"
    assert row["content_hash"] == "sha256:abc"
    assert row["amenities"] == []
    assert row["ward"] is None
    assert row["posted_date"] is None
    assert row["image_count"] is None


def test_parse_bronze_is_idempotent(spark):
    """Running twice over the same input yields identical rows."""
    df = _bronze_df(spark, [_bronze_row(_fixture_html("batdongsan"), "batdongsan:pr1")])

    first = [r.asDict() for r in parse_partition(df, DT).collect()]
    second = [r.asDict() for r in parse_partition(df, DT).collect()]

    assert first == second


def test_parse_bronze_parses_every_source(spark):
    rows = [
        _bronze_row(_fixture_html(source), f"{source}:bronze-1", source=source)
        for source in SOURCE_URLS
    ]
    df = _bronze_df(spark, rows)

    out = {r["source"]: r for r in parse_partition(df, DT).collect()}

    assert set(out) == set(SOURCE_URLS)
    for source, row in out.items():
        assert row["parse_ok"] is True, f"{source}: {row['parse_error']}"
        assert row["listing_id"] == f"{source}:bronze-1"
        assert row["province"] == "HCM"
        assert row["dt"] == DT


def test_parse_bronze_handles_bad_html(spark):
    """Garbage HTML is not an error: the parsers return nulls for what is absent."""
    df = _bronze_df(spark, [_bronze_row("<html><body>garbage</body></html>", "bad:1")])

    rows = parse_partition(df, DT).collect()

    assert len(rows) == 1
    assert rows[0]["listing_id"] == "bad:1"
    assert rows[0]["parse_ok"] is True
    assert rows[0]["asking_rent_vnd"] is None
    assert rows[0]["province"] is None


def test_parse_bronze_records_a_parse_exception(spark):
    """An empty document makes lxml raise; the row survives, carrying the error."""
    df = _bronze_df(spark, [_bronze_row("", "batdongsan:empty")])

    rows = parse_partition(df, DT).collect()

    assert len(rows) == 1
    assert rows[0]["parse_ok"] is False
    assert rows[0]["parse_error"].startswith("ParserError: ")
    assert rows[0]["is_active"] is True


def test_parse_bronze_records_a_decompress_failure(spark):
    df = _bronze_df(
        spark, [_bronze_row("", "batdongsan:trash", html_gz_b64="not base64 at all")]
    )

    rows = parse_partition(df, DT).collect()

    assert rows[0]["parse_ok"] is False
    assert rows[0]["parse_error"].startswith("decompress: ")


def test_parse_bronze_records_an_unknown_source(spark):
    df = _bronze_df(spark, [_bronze_row("<html></html>", "zillow:1", source="zillow")])

    rows = parse_partition(df, DT).collect()

    assert rows[0]["parse_ok"] is False
    assert rows[0]["parse_error"] == "unknown source: zillow"


def test_parse_bronze_skips_non_200_rows(spark):
    df = _bronze_df(spark, [_bronze_row("", "batdongsan:gone", http_status=404)])

    rows = parse_partition(df, DT).collect()

    assert len(rows) == 1
    row = rows[0]
    assert row["is_active"] is False
    assert row["parse_ok"] is False
    assert row["parse_error"] == "http_status: 404"
    assert row["title"] is None


def test_parse_bronze_strips_pii(spark):
    df = _bronze_df(spark, [_bronze_row(_fixture_html("batdongsan"), "batdongsan:pr1")])

    rows = parse_partition(df, DT).collect()

    assert not re.search(r"(?:\+?84|0)\d{9}", rows[0]["description_clean"] or "")


def test_pii_gate_passes_clean_rows(spark):
    pii_gate(_silver_df(spark, {}))


def test_pii_gate_fails_on_a_phone_number(spark):
    df = _silver_df(spark, {}, {"description_clean": "lien he 0901234567 de xem phong"})

    with pytest.raises(RuntimeError, match="PII gate failed: 1 rows contain phone numbers"):
        pii_gate(df)


def test_quality_gate_passes_clean_rows(spark):
    quality_gate(_silver_df(spark, {}, {"listing_id": "batdongsan:pr2"}))


def test_quality_gate_allows_a_null_province(spark):
    quality_gate(_silver_df(spark, {"province": None}))


def test_quality_gate_rejects_an_empty_frame(spark):
    with pytest.raises(RuntimeError, match="row_count"):
        quality_gate(_silver_df(spark))


def test_quality_gate_rejects_rent_out_of_range(spark):
    df = _silver_df(spark, {}, {"listing_id": "batdongsan:pr2", "asking_rent_vnd": 1_000})

    with pytest.raises(RuntimeError, match=r"asking_rent_vnd.*1 rows"):
        quality_gate(df)


def test_quality_gate_rejects_area_out_of_range(spark):
    df = _silver_df(spark, {}, {"listing_id": "batdongsan:pr2", "area_sqm": 4_000.0})

    with pytest.raises(RuntimeError, match=r"area_sqm.*1 rows"):
        quality_gate(df)


def test_quality_gate_rejects_an_unknown_province(spark):
    df = _silver_df(spark, {}, {"listing_id": "batdongsan:pr2", "province": "Ho Chi Minh"})

    with pytest.raises(RuntimeError, match=r"province.*1 rows"):
        quality_gate(df)


def test_quality_gate_rejects_a_duplicate_listing_id(spark):
    df = _silver_df(spark, {}, {})

    with pytest.raises(RuntimeError, match=r"listing_id_unique.*1"):
        quality_gate(df)


def test_quality_gate_rejects_a_low_parse_rate(spark):
    rows = [{"listing_id": f"batdongsan:pr{i}"} for i in range(18)]
    rows.append({"listing_id": "batdongsan:bad", "parse_ok": False,
                 "parse_error": "ParserError: Document is empty"})

    with pytest.raises(RuntimeError, match="parse_rate"):
        quality_gate(_silver_df(spark, *rows))
