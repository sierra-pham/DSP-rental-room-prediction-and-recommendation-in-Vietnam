"""Batdongsan spider: fetches raw HTML and writes to S3 bronze layer."""

import base64
import datetime as dt
import gzip
import hashlib

import scrapy

import config
from crawler.frontier import Frontier
from crawler.s3_writer import S3BatchWriter


class BatdongsanSpider(scrapy.Spider):
    name = "batdongsan"
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
            prefix=f"bronze/listings/dt={today}/source=batdongsan",
        )

    def start_requests(self):
        batch = self.frontier.next_batch("batdongsan", kind="discovery", limit=10000)
        for url in batch:
            yield scrapy.Request(url, callback=self.parse_page, errback=self.on_error)

    def parse_page(self, response):
        raw = response.body
        content_hash = "sha256:" + hashlib.sha256(raw).hexdigest()
        html_gz_b64 = base64.b64encode(gzip.compress(raw)).decode("ascii")

        self.writer.write({
            "listing_id": f"batdongsan:{response.url.rstrip('/').split('/')[-1]}",
            "source": "batdongsan",
            "url": response.url,
            "crawl_ts": dt.datetime.now(dt.timezone.utc).isoformat(),
            "http_status": response.status,
            "content_hash": content_hash,
            "html_gz_b64": html_gz_b64,
            "fetch_ms": response.meta.get("download_latency", 0) * 1000,
            "crawl_kind": "discovery",
        })
        self.frontier.mark_fetched(response.url, response.status, content_hash)

    def on_error(self, failure):
        self.frontier.mark_failed(failure.request.url)

    def closed(self, reason):
        self.writer.flush()
