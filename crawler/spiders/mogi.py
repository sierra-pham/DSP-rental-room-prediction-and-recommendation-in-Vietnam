"""Mogi spider: fetches raw HTML and writes to S3 bronze layer."""

from crawler.spiders.base import BronzeSpider


class MogiSpider(BronzeSpider):
    name = "mogi"
