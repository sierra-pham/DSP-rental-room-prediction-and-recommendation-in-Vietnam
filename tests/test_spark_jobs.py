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

# Without the [spark] extra this whole module is unrunnable; skipping says so,
# where a collection error would read as a broken test file.
pytest.importorskip("pyspark")

from parsers.normalise import FURNISHING_VALUES, _PHONE_RE

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


# Measured against the four fixtures, not assumed. The raw values behind them
# are "Bình Thạnh"/"Đầy đủ", "Quận Gò Vấp"/"basic", "Quận 3"/None and
# "Quận 7"/"Nội thất đầy đủ".
EXPECTED_DISTRICT = {
    "batdongsan": "binh thanh",
    "phongtro123": "go vap",
    "mogi": "3",
    "nhatot": "7",
}
EXPECTED_FURNISHING = {
    "batdongsan": "full",
    "phongtro123": "basic",
    "mogi": "unknown",
    "nhatot": "full",
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
    "property_type": "apartment", "province": "HCM", "district": "binh thanh",
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
    # Measured: the parser returns "Bình Thạnh" / "Đầy đủ"; silver stores the
    # diacritic-stripped district and the furnishing enum (spec 3.2).
    assert row["district"] == "binh thanh"
    assert row["furnishing"] == "full"
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
        assert row["district"] == EXPECTED_DISTRICT[source], source
        assert row["furnishing"] == EXPECTED_FURNISHING[source], source


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
    """title and address go through the stripper too, not just description."""
    df = _bronze_df(spark, [_bronze_row(_fixture_html("batdongsan"), "batdongsan:pr1")])

    rows = parse_partition(df, DT).collect()

    for field in ("title", "address", "description_clean"):
        value = rows[0][field] or ""
        assert not _PHONE_RE.search(value), f"{field} leaked: {value!r}"


def test_pii_gate_passes_clean_rows(spark):
    pii_gate(_silver_df(spark, {}))


def test_pii_gate_fails_on_a_phone_number(spark):
    df = _silver_df(spark, {}, {"description_clean": "lien he 0901234567 de xem phong"})

    with pytest.raises(RuntimeError, match="PII gate failed: 1 rows contain phone numbers"):
        pii_gate(df)


@pytest.mark.parametrize("column", ["title", "address", "ward", "description_clean"])
def test_pii_gate_fails_on_a_phone_number_in_any_free_text_column(spark, column):
    """description_clean is not the only place a phone number can land."""
    df = _silver_df(spark, {}, {"listing_id": "batdongsan:pr2",
                                "description_clean": "can ho dep",
                                column: "lien he 0901234567 de xem phong"})

    with pytest.raises(RuntimeError, match="PII gate failed: 1 rows contain phone numbers"):
        pii_gate(df)


def test_pii_gate_scans_every_free_text_column_in_the_schema(spark):
    """The predicate is built from SILVER_SCHEMA, so a new column is covered
    the day it is added --- nobody has to remember to extend the gate."""
    from spark.parse_bronze import PII_SCAN_COLUMNS

    identifiers = {"listing_id", "source", "url", "content_hash", "crawl_ts", "dt"}
    expected = [f.name for f in SILVER_SCHEMA.fields
                if f.dataType.simpleString() == "string" and f.name not in identifiers]

    assert PII_SCAN_COLUMNS == expected
    assert "title" in PII_SCAN_COLUMNS and "address" in PII_SCAN_COLUMNS
    assert not identifiers & set(PII_SCAN_COLUMNS)


def test_pii_gate_ignores_the_identifier_columns(spark):
    """A listing id that happens to look like a phone number is not a leak:
    failing on it would abort the day over an id the site chose."""
    df = _silver_df(spark, {"listing_id": "batdongsan:0901234567",
                            "url": "https://batdongsan.com.vn/x/0901234567"})

    pii_gate(df)


def test_pii_gate_uses_the_strippers_own_pattern(spark):
    """A separated number ("090.123.4567") is what the stripper catches; the
    gate must not use a narrower pattern than the thing it audits."""
    df = _silver_df(spark, {"description_clean": "goi 090.123.4567 nhe"})

    with pytest.raises(RuntimeError, match="PII gate failed: 1 rows"):
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


def test_quality_gate_ignores_non_200_rows_in_the_parse_rate(spark):
    """Delistings are not parse failures: the rate is over fetched rows only."""
    fetched = [{"listing_id": f"batdongsan:pr{i}"} for i in range(10)]
    gone = [{"listing_id": f"batdongsan:gone{i}", "is_active": False,
             "parse_ok": False, "parse_error": "http_status: 404",
             "title": None, "asking_rent_vnd": None, "area_sqm": None}
            for i in range(5)]

    quality_gate(_silver_df(spark, *(fetched + gone)))


def test_quality_gate_rejects_rows_with_no_core_fields(spark):
    """A site redesign returns all-None without raising: parse_ok stays True."""
    rows = [{"listing_id": f"batdongsan:pr{i}", "title": None,
             "asking_rent_vnd": None, "area_sqm": None} for i in range(20)]

    with pytest.raises(RuntimeError, match=r"core_fields_null.*20"):
        quality_gate(_silver_df(spark, *rows))


def test_quality_gate_rejects_a_low_parse_rate(spark):
    rows = [{"listing_id": f"batdongsan:pr{i}"} for i in range(18)]
    rows.append({"listing_id": "batdongsan:bad", "parse_ok": False,
                 "parse_error": "ParserError: Document is empty"})

    with pytest.raises(RuntimeError, match="parse_rate"):
        quality_gate(_silver_df(spark, *rows))


def test_quality_gate_rejects_a_furnishing_outside_the_enum(spark):
    df = _silver_df(spark, {}, {"listing_id": "batdongsan:pr2",
                                "furnishing": "Đầy đủ"})

    with pytest.raises(RuntimeError, match=r"furnishing.*1 rows"):
        quality_gate(df)


def test_quality_gate_rejects_a_null_furnishing(spark):
    """Silver stores "unknown", never a null: a null would read as "not yet
    parsed" downstream, which is a different claim."""
    df = _silver_df(spark, {}, {"listing_id": "batdongsan:pr2", "furnishing": None})

    with pytest.raises(RuntimeError, match=r"furnishing.*1 rows"):
        quality_gate(df)


@pytest.mark.parametrize("value", ["none", "basic", "full", "unknown"])
def test_quality_gate_allows_every_enum_furnishing(spark, value):
    quality_gate(_silver_df(spark, {"furnishing": value}))


def test_parse_bronze_never_leaves_furnishing_null(spark):
    """Including the rows that never reach a parser."""
    rows = [
        _bronze_row(_fixture_html("batdongsan"), "batdongsan:ok"),
        _bronze_row("", "batdongsan:gone", http_status=404),
        _bronze_row("<html></html>", "zillow:1", source="zillow"),
        _bronze_row("", "batdongsan:empty"),
    ]

    out = parse_partition(_bronze_df(spark, rows), DT).collect()

    assert len(out) == 4
    for row in out:
        assert row["furnishing"] in FURNISHING_VALUES, row["listing_id"]
    by_id = {row["listing_id"]: row["furnishing"] for row in out}
    assert by_id["batdongsan:ok"] == "full"
    assert by_id["batdongsan:gone"] == "unknown"
    assert by_id["zillow:1"] == "unknown"
    assert by_id["batdongsan:empty"] == "unknown"


def test_quality_gate_warns_on_a_crawl_date_that_is_not_the_partition_day(spark, caplog):
    """A run crossing 00:00 UTC files under its start date. That is expected,
    so it is a warning --- but never a silent one."""
    df = _silver_df(
        spark,
        {},
        {"listing_id": "batdongsan:pr2", "crawl_ts": "2026-09-21T00:30:00+00:00",
         "crawl_date": date(2026, 9, 21)},
    )

    with caplog.at_level("WARNING", logger="spark.parse_bronze"):
        quality_gate(df)

    assert "crawl_date" in caplog.text
    assert "1 of 2" in caplog.text


def test_quality_gate_is_silent_when_every_crawl_date_matches_dt(spark, caplog):
    with caplog.at_level("WARNING", logger="spark.parse_bronze"):
        quality_gate(_silver_df(spark, {}, {"listing_id": "batdongsan:pr2"}))

    assert "crawl_date" not in caplog.text


def test_parse_partition_on_an_empty_bronze_frame(spark):
    """A day with no bronze rows is an empty silver frame with the right shape,
    not a crash --- the quality gate is what decides an empty day is a failure."""
    out = parse_partition(_bronze_df(spark, []), DT)

    assert out.schema == SILVER_SCHEMA
    assert out.count() == 0


def test_parse_bronze_records_a_decode_failure(spark):
    """Valid gzip, invalid UTF-8: that is a decode problem, not a decompress
    one, and the parse_error has to say which so the fix is obvious."""
    payload = base64.b64encode(gzip.compress(bytes([0xFF, 0xFE]) + b" not utf-8")).decode("ascii")
    df = _bronze_df(spark, [_bronze_row("", "batdongsan:mojibake", html_gz_b64=payload)])

    rows = parse_partition(df, DT).collect()

    assert rows[0]["parse_ok"] is False
    assert rows[0]["parse_error"] == "decode: UnicodeDecodeError"


# ---------------------------------------------------------------------------
# Task 11 — MinHash LSH deduplication
# ---------------------------------------------------------------------------

def test_dedupe_clusters_reposts(spark):
    """Identical descriptions in the same block must share a canonical_id;
    a distinct listing must not be merged."""
    from spark.dedupe import build_clusters

    rows = [
        ("a", "HCM", "Q7", 25.0, 7_000_000,
         "phòng trọ đẹp gần lotte mart có gác"),
        ("b", "HCM", "Q7", 25.0, 7_000_000,
         "phòng trọ đẹp gần lotte mart có gác"),
        ("c", "HCM", "Q7", 60.0, 20_000_000,
         "căn hộ cao cấp view sông 2 phòng ngủ"),
    ]
    df = spark.createDataFrame(
        rows,
        "listing_id string, province string, district string, "
        "area_sqm double, asking_rent_vnd long, description_clean string",
    )
    out = {r["listing_id"]: r for r in build_clusters(df).collect()}

    assert out["a"]["canonical_id"] == out["b"]["canonical_id"], \
        "identical re-posts must share a canonical_id"
    assert out["c"]["canonical_id"] != out["a"]["canonical_id"], \
        "a distinct listing must not be merged"
    assert out["a"]["cluster_size"] == 2
    assert out["c"]["cluster_size"] == 1
    assert sum(1 for r in out.values() if r["is_canonical"]) >= 2


def test_dedupe_singletons_survive(spark):
    """Every listing gets an output row even if it has no duplicate."""
    from spark.dedupe import build_clusters

    df = spark.createDataFrame(
        [("x", "HCM", "Q1", 30.0, 10_000_000, "căn hộ quận một")],
        "listing_id string, province string, district string, "
        "area_sqm double, asking_rent_vnd long, description_clean string",
    )
    out = build_clusters(df).collect()

    assert len(out) == 1
    assert out[0]["listing_id"] == "x"
    assert out[0]["canonical_id"] == "x"
    assert out[0]["cluster_size"] == 1
    assert out[0]["is_canonical"] is True


# ---------------------------------------------------------------------------
# Task 13 — Panel fact table and survival labels
# ---------------------------------------------------------------------------

def test_panel_survival_labels(spark):
    """Censoring logic: two consecutive absences = event; single absence is
    transient and does not fire."""
    from spark.build_panel import survival_labels

    d = lambda s: date.fromisoformat(s)
    rows = [
        # listing a: present 3 days, then absent twice -> event at day 3
        ("a", d("2026-09-25"), True), ("a", d("2026-09-26"), True),
        ("a", d("2026-09-27"), True), ("a", d("2026-09-28"), False),
        ("a", d("2026-09-29"), False),
        # listing b: present throughout -> right-censored
        ("b", d("2026-09-25"), True), ("b", d("2026-09-26"), True),
        ("b", d("2026-09-27"), True), ("b", d("2026-09-28"), True),
        ("b", d("2026-09-29"), True),
        # listing c: one absence then present again -> NOT an event
        ("c", d("2026-09-25"), True), ("c", d("2026-09-26"), False),
        ("c", d("2026-09-27"), True), ("c", d("2026-09-28"), True),
        ("c", d("2026-09-29"), True),
    ]
    df = spark.createDataFrame(
        rows, "listing_id string, obs_date date, is_present boolean"
    )
    out = {
        r["listing_id"]: r
        for r in survival_labels(df, censor_date=d("2026-09-29")).collect()
    }

    assert out["a"]["event_observed"] is True
    assert out["a"]["duration_days"] == 3
    assert out["b"]["event_observed"] is False
    assert out["b"]["duration_days"] == 4
    assert out["c"]["event_observed"] is False, \
        "a single absence must not fire an event"


def test_panel_survival_labels_no_presence(spark):
    """A listing that is never present has no first_seen and is excluded."""
    from spark.build_panel import survival_labels

    d = lambda s: date.fromisoformat(s)
    rows = [
        ("x", d("2026-09-25"), False),
        ("x", d("2026-09-26"), False),
    ]
    df = spark.createDataFrame(
        rows, "listing_id string, obs_date date, is_present boolean"
    )
    out = survival_labels(df, censor_date=d("2026-09-29")).collect()

    assert len(out) == 0
