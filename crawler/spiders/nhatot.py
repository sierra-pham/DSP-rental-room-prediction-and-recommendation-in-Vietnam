"""Nhatot spider: fetches JS-rendered HTML via Playwright and writes to S3 bronze.

nhatot.com renders listing data client-side into a <script id="__NEXT_DATA__">
tag. Playwright is required to obtain the fully rendered page.
"""

import scrapy

from crawler.spiders.base import BronzeSpider


class NhatotSpider(BronzeSpider):
    name = "nhatot"
    custom_settings = {
        **BronzeSpider.custom_settings,
        "DOWNLOAD_DELAY": 1.5,
        "DOWNLOAD_HANDLERS": {
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        },
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
        "PLAYWRIGHT_LAUNCH_OPTIONS": {"headless": True},
        "CLOSESPIDER_PAGECOUNT": 20000,
    }

    def start_requests(self):
        batch = self.frontier.next_batch(self.name, kind="discovery", limit=10000)
        for url in batch:
            yield scrapy.Request(
                url,
                callback=self.parse_page,
                errback=self.on_error,
                meta={"playwright": True, "playwright_include_page": False},
            )
