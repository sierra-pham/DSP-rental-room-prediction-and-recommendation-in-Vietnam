"""Spark job: parse bronze HTML into the silver listings table.

Bronze is immutable; this job only reads it. The parse runs on the executors
--- `mapPartitions` over the bronze rows, one set of parser instances per
partition --- and never on the driver: a day of bronze is tens of thousands of
rows of gzipped HTML, and `collect()` would drag all of it back.

One bronze row in, one silver row out, always: a non-200 fetch, a corrupt
payload or a parser exception produces a row carrying `parse_ok = False` and
the reason, so one bad page can never kill the day's job.

Usage:
    spark-submit spark/parse_bronze.py --date 2026-09-20
"""

import argparse
import base64
import datetime
import functools
import gzip
import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from parsers.normalise import (
    FURNISHING_VALUES,
    _PHONE_RE,
    normalise_district,
    normalise_furnishing,
    normalise_province,
    strip_pii,
)

_log = logging.getLogger(__name__)

# Exactly the keys crawler/spiders/base.py:build_bronze_record writes. Read
# with, never inferred: schema inference would sample every gzipped page.
BRONZE_SCHEMA = StructType([
    StructField("listing_id", StringType()),
    StructField("source", StringType()),
    StructField("url", StringType()),
    StructField("crawl_ts", StringType()),
    StructField("http_status", IntegerType()),
    StructField("content_hash", StringType()),
    StructField("html_gz_b64", StringType()),
    StructField("fetch_ms", DoubleType()),
    StructField("crawl_kind", StringType()),
])

# Spec 3.2, plus the parse diagnostics the gates read and the two partition
# columns (province, dt) in partition order.
SILVER_SCHEMA = StructType([
    StructField("listing_id", StringType()),
    StructField("source", StringType()),
    StructField("url", StringType()),
    StructField("crawl_ts", StringType()),
    StructField("crawl_date", DateType()),
    StructField("title", StringType()),
    StructField("asking_rent_vnd", LongType()),
    StructField("area_sqm", DoubleType()),
    StructField("bedrooms", IntegerType()),
    StructField("bathrooms", IntegerType()),
    StructField("property_type", StringType()),
    StructField("province", StringType()),
    StructField("district", StringType()),
    StructField("ward", StringType()),
    StructField("address", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("furnishing", StringType()),
    StructField("amenities", ArrayType(StringType())),
    StructField("description_clean", StringType()),
    StructField("posted_date", DateType()),
    StructField("image_count", IntegerType()),
    StructField("content_hash", StringType()),
    StructField("is_active", BooleanType()),
    StructField("parse_ok", BooleanType()),
    StructField("parse_error", StringType()),
    StructField("dt", StringType()),
])

SILVER_FIELDS = [field.name for field in SILVER_SCHEMA.fields]

PROVINCE_CODES = ("HCM", "HN", "DN", "BD", "DNA", "CT")
RENT_RANGE_VND = (300_000, 500_000_000)
AREA_RANGE_SQM = (5.0, 1000.0)
MIN_PARSE_RATE = 0.95
# A parser that stops finding anything does not raise --- it returns None for
# every field --- so a site redesign shows up as parse_ok rows with nothing in
# them, which every other rule allows. Cap the share of those too.
MAX_EMPTY_PARSE_RATE = 0.05
# The gate audits the stripper, so it uses the stripper's own pattern: a
# narrower one would pass numbers `strip_pii` is meant to have removed.
PHONE = _PHONE_RE.pattern

# Identifiers are ours, not the seller's. A listing id or a URL full of digits
# can match a phone pattern, and failing the day over one would be a false
# positive; everything else in silver is free text a seller could have typed.
PII_EXEMPT_COLUMNS = frozenset(
    {"listing_id", "source", "url", "content_hash", "crawl_ts", "dt"}
)
# Derived from the schema, so a column added to silver tomorrow is scanned
# tomorrow --- nobody has to remember to extend the gate.
PII_SCAN_COLUMNS = [
    field.name for field in SILVER_SCHEMA.fields
    if isinstance(field.dataType, StringType) and field.name not in PII_EXEMPT_COLUMNS
]

# Parquet parts of 128-256 MB. A day of silver is ~30k listings without the
# HTML, comfortably one part; writing it with the cluster's 24 shuffle
# partitions would leave 24 files of a couple of MB each. `repartition`, not
# `coalesce`: coalesce is not a shuffle boundary, so asking it for one output
# partition would also collapse the parse upstream of it into a single task.
OUTPUT_PARTS = 1


def _build_parsers() -> dict:
    """Built inside the partition function: parser instances wrap lxml state
    that is not worth shipping to the executors."""
    from parsers.batdongsan import BatdongsanParser
    from parsers.mogi import MogiParser
    from parsers.nhatot import NhatotParser
    from parsers.phongtro123 import Phongtro123Parser

    instances = (BatdongsanParser(), Phongtro123Parser(), MogiParser(), NhatotParser())
    return {parser.source: parser for parser in instances}


def _crawl_date(crawl_ts: str | None) -> datetime.date | None:
    if not crawl_ts:
        return None
    try:
        return datetime.date.fromisoformat(crawl_ts[:10])
    except ValueError:
        return None


def _silver_row(bronze: dict, dt: str, parsers: dict) -> dict:
    """One bronze record -> one silver record. Never raises."""
    row = {
        # The bronze listing_id, not the parser's: bronze, the frontier and
        # the panel probes all key on it.
        "listing_id": bronze.get("listing_id"),
        "source": bronze.get("source"),
        "url": bronze.get("url"),
        "crawl_ts": bronze.get("crawl_ts"),
        "crawl_date": _crawl_date(bronze.get("crawl_ts")),
        "amenities": [],
        # Spec 3.2 has no null furnishing: "unknown" is a state, a null would
        # read as "not parsed yet". Rows that never reach a parser keep it.
        "furnishing": "unknown",
        "content_hash": bronze.get("content_hash"),
        "is_active": bronze.get("http_status") == 200,
        "parse_ok": False,
        "parse_error": None,
        "dt": dt,
    }

    if not row["is_active"]:
        row["parse_error"] = f"http_status: {bronze.get('http_status')}"
        return row

    parser = parsers.get(bronze.get("source"))
    if parser is None:
        row["parse_error"] = f"unknown source: {bronze.get('source')}"
        return row

    try:
        raw = gzip.decompress(base64.b64decode(bronze["html_gz_b64"]))
    except Exception as exc:
        row["parse_error"] = f"decompress: {type(exc).__name__}"
        return row

    # Decoding is its own failure: a page that gunzips but is not UTF-8 is a
    # crawler encoding bug, not a corrupt payload, and the two get fixed in
    # different places. Decoding stays strict --- silently replacing bytes
    # would put mojibake in silver and no error anywhere.
    try:
        html = raw.decode("utf-8")
    except UnicodeDecodeError:
        row["parse_error"] = "decode: UnicodeDecodeError"
        return row

    try:
        parsed = parser.parse(html, bronze.get("url") or "")
    except Exception as exc:
        row["parse_error"] = f"{type(exc).__name__}: {exc}"
        return row

    rent = parsed.get("asking_rent_vnd")
    area = parsed.get("area_sqm")
    bedrooms = parsed.get("bedrooms")
    bathrooms = parsed.get("bathrooms")
    latitude = parsed.get("latitude")
    longitude = parsed.get("longitude")
    # PII is stripped on the way into silver, not trusted to have been
    # stripped upstream: a phone number in a title is as much a leak as one in
    # the description, and `pii_gate` fails the whole day over it.
    title = parsed.get("title")
    address = parsed.get("address")
    furnishing = normalise_furnishing(bronze.get("source"), parsed.get("furnishing"))
    row.update({
        "title": strip_pii(title) if title else None,
        "asking_rent_vnd": None if rent is None else int(rent),
        "area_sqm": None if area is None else float(area),
        "bedrooms": None if bedrooms is None else int(bedrooms),
        "bathrooms": None if bathrooms is None else int(bathrooms),
        "property_type": parsed.get("property_type"),
        # The parsers return the site's own spelling; silver stores the code.
        "province": normalise_province(parsed.get("province")),
        # Spec 3.2: stored normalised and diacritic-stripped, so "Quận 3",
        # "Q.3" and "quan 3" are one district across the four sources.
        "district": normalise_district(parsed.get("district")),
        "ward": parsed.get("ward"),
        "address": strip_pii(address) if address else None,
        "latitude": None if latitude is None else float(latitude),
        "longitude": None if longitude is None else float(longitude),
        "furnishing": furnishing,
        "description_clean": parsed.get("description_clean"),
        "parse_ok": True,
    })
    return row


def _parse_rows(rows, dt: str):
    parsers = _build_parsers()
    for row in rows:
        silver = _silver_row(row.asDict(), dt, parsers)
        yield tuple(silver.get(name) for name in SILVER_FIELDS)


def parse_partition(df: DataFrame, dt: str) -> DataFrame:
    """Bronze DataFrame in, silver DataFrame out, parsed on the executors.

    `dt` is the bronze partition being parsed --- the job's `--date` --- and
    becomes the second partition column of silver.
    """
    rows = df.rdd.mapPartitions(lambda partition: _parse_rows(partition, dt))
    return df.sparkSession.createDataFrame(rows, schema=SILVER_SCHEMA)


def pii_gate(df: DataFrame) -> None:
    """No phone number may survive into silver (WBS 4.2.1).

    The parsers and `_silver_row` already run `strip_pii`; this is the audit
    that says so, over every free-text column at once rather than the one it
    used to check.
    """
    predicate = functools.reduce(
        lambda acc, name: acc | F.col(name).rlike(PHONE),
        PII_SCAN_COLUMNS[1:],
        F.col(PII_SCAN_COLUMNS[0]).rlike(PHONE),
    )
    leaks = df.filter(predicate).count()
    if leaks > 0:
        raise RuntimeError(f"PII gate failed: {leaks} rows contain phone numbers")


def quality_gate(df: DataFrame) -> None:
    """Fail the job, loudly and by name, on any violated rule (WBS 4.2.2)."""
    total = df.count()
    if total == 0:
        raise RuntimeError("Quality gate [row_count]: silver is empty, 0 rows")

    rent_min, rent_max = RENT_RANGE_VND
    bad_rent = df.filter(
        F.col("asking_rent_vnd").isNotNull()
        & ~F.col("asking_rent_vnd").between(rent_min, rent_max)
    ).count()
    if bad_rent > 0:
        raise RuntimeError(
            f"Quality gate [asking_rent_vnd]: {bad_rent} rows outside "
            f"{rent_min}-{rent_max} VND"
        )

    area_min, area_max = AREA_RANGE_SQM
    bad_area = df.filter(
        F.col("area_sqm").isNotNull() & ~F.col("area_sqm").between(area_min, area_max)
    ).count()
    if bad_area > 0:
        raise RuntimeError(
            f"Quality gate [area_sqm]: {bad_area} rows outside {area_min}-{area_max} m2"
        )

    bad_province = df.filter(
        F.col("province").isNotNull() & ~F.col("province").isin(*PROVINCE_CODES)
    ).count()
    if bad_province > 0:
        raise RuntimeError(
            f"Quality gate [province]: {bad_province} rows outside {list(PROVINCE_CODES)}"
        )

    # A province we could not map is allowed --- the six codes are the ones we
    # crawl, not the ones that exist --- but it is never silent.
    unmapped = df.filter(F.col("province").isNull()).count()
    if unmapped > 0:
        _log.warning("province unmapped on %d of %d rows", unmapped, total)

    # Spec 3.2 enum. Null is a violation too: `_silver_row` writes "unknown"
    # on every path, so a null here means a row got into silver past it.
    bad_furnishing = df.filter(
        F.col("furnishing").isNull() | ~F.col("furnishing").isin(*FURNISHING_VALUES)
    ).count()
    if bad_furnishing > 0:
        raise RuntimeError(
            f"Quality gate [furnishing]: {bad_furnishing} rows outside "
            f"{list(FURNISHING_VALUES)}"
        )

    # A crawl that crosses 00:00 UTC keeps writing to the partition it started
    # on, so crawl_date drifting from dt is expected and must not fail the day
    # --- but a large share of it means the crawl ran far longer than planned.
    drifted = df.filter(
        F.col("crawl_date").isNotNull() & (F.col("crawl_date") != F.to_date(F.col("dt")))
    ).count()
    if drifted > 0:
        _log.warning(
            "crawl_date differs from dt on %d of %d rows (%.1f%%)",
            drifted, total, 100 * drifted / total,
        )

    duplicates = (df.groupBy("dt", "source", "listing_id").count()
                    .filter(F.col("count") > 1).count())
    if duplicates > 0:
        raise RuntimeError(
            f"Quality gate [listing_id_unique]: {duplicates} listing_id values "
            "repeat within (dt, source)"
        )

    # The parse rate is over fetched rows only. A non-200 row was never handed
    # to a parser, and listings disappear every day --- counting delistings as
    # parse failures would abort a healthy day and blame the parsers for it.
    fetched = df.filter(F.col("is_active")).count()
    if fetched < total:
        missed = total - fetched
        _log.warning("non-200 on %d of %d rows (%.1f%%)", missed, total, 100 * missed / total)
    if fetched == 0:
        raise RuntimeError(
            f"Quality gate [parse_rate]: none of the {total} rows were fetched (http 200)"
        )

    parsed = df.filter(F.col("parse_ok")).count()
    rate = parsed / fetched
    if rate < MIN_PARSE_RATE:
        raise RuntimeError(
            f"Quality gate [parse_rate]: {parsed} of {fetched} fetched rows parsed "
            f"({rate:.1%}), below {MIN_PARSE_RATE:.0%}"
        )

    # `parsed` is non-zero here: the rule above would have failed otherwise.
    empty = df.filter(
        F.col("parse_ok")
        & F.col("title").isNull()
        & F.col("asking_rent_vnd").isNull()
        & F.col("area_sqm").isNull()
    ).count()
    empty_rate = empty / parsed
    if empty_rate > MAX_EMPTY_PARSE_RATE:
        raise RuntimeError(
            f"Quality gate [core_fields_null]: {empty} of {parsed} parsed rows carry "
            f"no title, rent or area ({empty_rate:.1%}), above {MAX_EMPTY_PARSE_RATE:.0%}"
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")

    ap = argparse.ArgumentParser(description="Parse one day of bronze into silver.")
    ap.add_argument("--date", required=True, help="bronze partition to parse, YYYY-MM-DD")
    ap.add_argument("--input", default=None, help="override the bronze path")
    ap.add_argument("--output", default=None, help="override the silver path")
    args = ap.parse_args()

    from spark.common import get_spark, s3_path

    spark = get_spark("parse_bronze")
    input_path = args.input or s3_path("bronze", "listings", f"dt={args.date}")
    output_path = args.output or s3_path("silver", "listings")

    bronze = spark.read.json(input_path, schema=BRONZE_SCHEMA)
    # Both gates and the write walk the result; parsing it three times would
    # mean gunzipping the day three times.
    silver = parse_partition(bronze, args.date).cache()

    pii_gate(silver)
    quality_gate(silver)

    (silver.repartition(OUTPUT_PARTS)
           .write
           .partitionBy("province", "dt")
           .mode("overwrite")
           .parquet(output_path))
    _log.info("wrote %s province=*/dt=%s", output_path, args.date)
    spark.stop()
