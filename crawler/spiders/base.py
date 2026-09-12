"""Shared bronze spider base: fetches raw HTML and writes it to S3.

Subclasses set only `name`; the frontier source, the S3 prefix
(``bronze/listings/dt=<today>/source=<name>``) and each record's
``listing_id`` prefix are all derived from it. A source with different
fetch behaviour (e.g. nhatot's Playwright rendering) overrides
`custom_settings` and/or `start_requests` instead of duplicating the
whole spider.

`BronzeSpider` itself sets no `name`, so Scrapy's SpiderLoader does not
collect it as a runnable spider: `iter_spider_classes` only yields classes
where `getattr(cls, "name", None)` is truthy.
"""

import base64
import datetime as dt
import gzip
import hashlib

import scrapy

import config
from crawler.frontier import Frontier
from crawler.s3_writer import S3BatchWriter


def build_bronze_record(source: str, url: str, status: int, raw: bytes,
                         fetch_ms: float, crawl_kind: str = "discovery") -> dict:
    """Build the bronze record dict for one fetched page.

    Pure and network-free so it can be unit tested without a live Scrapy
    Response: `listing_id` is `<source>:<last path segment of url>`.
    """
    content_hash = "sha256:" + hashlib.sha256(raw).hexdigest()
    html_gz_b64 = base64.b64encode(gzip.compress(raw)).decode("ascii")
    slug = url.rstrip("/").split("/")[-1]
    return {
        "listing_id": f"{source}:{slug}",
        "source": source,
        "url": url,
        "crawl_ts": dt.datetime.now(dt.timezone.utc).isoformat(),
        "http_status": status,
        "content_hash": content_hash,
        "html_gz_b64": html_gz_b64,
        "fetch_ms": fetch_ms,
        "crawl_kind": crawl_kind,
    }


class BronzeSpider(scrapy.Spider):
    """Abstract base for the estate's bronze-layer spiders. Not runnable
    itself — it has no `name`."""

    custom_settings = {
        "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
        "DOWNLOAD_DELAY": 1.0,
    }

    def __init__(self, db_path="frontier.db", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.frontier = Frontier(db_path)
        aws_cfg = config.load("aws")
        today = dt.date.today().isoformat()
        self.writer = S3BatchWriter(
            bucket=aws_cfg["bucket"],
            prefix=f"bronze/listings/dt={today}/source={self.name}",
        )

    def start_requests(self):
        batch = self.frontier.next_batch(self.name, kind="discovery", limit=10000)
        for url in batch:
            yield scrapy.Request(url, callback=self.parse_page, errback=self.on_error)

    def parse_page(self, response):
        record = build_bronze_record(
            source=self.name,
            url=response.url,
            status=response.status,
            raw=response.body,
            fetch_ms=response.meta.get("download_latency", 0) * 1000,
        )
        self.writer.write(record)
        self.frontier.mark_fetched(response.url, response.status, record["content_hash"])

    def on_error(self, failure):
        self.frontier.mark_failed(failure.request.url)

    def closed(self, reason):
        self.writer.flush()
