"""Phongtro123 spider: fetches raw HTML and writes to S3 bronze layer."""

from crawler.spiders.base import BronzeSpider


class Phongtro123Spider(BronzeSpider):
    name = "phongtro123"
