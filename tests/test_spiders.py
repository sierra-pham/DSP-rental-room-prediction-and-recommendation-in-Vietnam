import asyncio
from datetime import datetime, timezone

import scrapy
import pytest

from crawler.spiders.base import BronzeSpider, build_bronze_record, utc_today
from crawler.spiders.batdongsan import BatdongsanSpider
from crawler.spiders.mogi import MogiSpider
from crawler.spiders.phongtro123 import Phongtro123Spider
from crawler.spiders.nhatot import NhatotSpider

ALL_SPIDERS = [BatdongsanSpider, MogiSpider, Phongtro123Spider, NhatotSpider]
NAMES = ["batdongsan", "mogi", "phongtro123", "nhatot"]


def _collect_start(spider):
    async def _gather():
        return [r async for r in spider.start()]
    return asyncio.run(_gather())


def test_base_spider_has_no_name():
    # BronzeSpider must not be collected by Scrapy's SpiderLoader as a
    # runnable spider: it needs no `name` attribute at all.
    assert getattr(BronzeSpider, "name", None) is None


@pytest.mark.parametrize("spider_cls,name", list(zip(ALL_SPIDERS, NAMES)), ids=NAMES)
def test_spider_class_has_expected_name(spider_cls, name):
    assert spider_cls.name == name


@pytest.mark.parametrize("spider_cls,name", list(zip(ALL_SPIDERS, NAMES)), ids=NAMES)
def test_writer_prefix_ends_with_source(spider_cls, name, tmp_path):
    spider = spider_cls(db_path=str(tmp_path / "f.db"))
    assert spider.writer._prefix.endswith(f"source={name}")


def test_utc_today_is_the_utc_day():
    assert utc_today() == datetime.now(timezone.utc).date().isoformat()


@pytest.mark.parametrize("spider_cls,name", list(zip(ALL_SPIDERS, NAMES)), ids=NAMES)
def test_writer_prefix_carries_the_utc_day(spider_cls, name, tmp_path):
    spider = spider_cls(db_path=str(tmp_path / "f.db"))
    assert f"dt={utc_today()}/" in spider.writer._prefix


@pytest.mark.parametrize("spider_cls,name", list(zip(ALL_SPIDERS, NAMES)), ids=NAMES)
def test_writer_prefix_uses_the_utc_day(spider_cls, name, tmp_path, monkeypatch):
    """dt= is the UTC day, the same clock crawl_ts uses. Local time here is
    UTC+7, so a run between 00:00 and 07:00 local would otherwise file the
    day's pages under tomorrow's partition and the parse job would miss them."""
    monkeypatch.setattr("crawler.spiders.base.utc_today", lambda: "2026-01-02")

    spider = spider_cls(db_path=str(tmp_path / "f.db"))

    assert "dt=2026-01-02/" in spider.writer._prefix


def test_build_bronze_record_derives_listing_id_and_source():
    record = build_bronze_record(
        source="mogi",
        url="https://mogi.vn/x-id22742913",
        status=200,
        raw=b"<html></html>",
        fetch_ms=12.5,
    )
    assert record["listing_id"] == "mogi:x-id22742913"
    assert record["source"] == "mogi"


@pytest.mark.parametrize("spider_cls,name", list(zip(ALL_SPIDERS, NAMES)), ids=NAMES)
def test_download_delay_meets_politeness_ceiling(spider_cls, name, tmp_path):
    spider = spider_cls(db_path=str(tmp_path / "f.db"))
    assert spider.custom_settings["DOWNLOAD_DELAY"] >= 1.0


def test_only_nhatot_enables_playwright():
    assert "DOWNLOAD_HANDLERS" in NhatotSpider.custom_settings
    assert NhatotSpider.custom_settings["CLOSESPIDER_PAGECOUNT"] == 20000

    for spider_cls in (BatdongsanSpider, MogiSpider, Phongtro123Spider):
        assert "DOWNLOAD_HANDLERS" not in spider_cls.custom_settings


@pytest.mark.parametrize(
    "spider_cls,name", list(zip([BatdongsanSpider, MogiSpider, Phongtro123Spider],
                                 ["batdongsan", "mogi", "phongtro123"])),
    ids=["batdongsan", "mogi", "phongtro123"],
)
def test_non_playwright_spiders_send_plain_requests(spider_cls, name, tmp_path):
    spider = spider_cls(db_path=str(tmp_path / "f.db"))
    spider.frontier.add_urls(name, [f"https://{name}.example/x-1"])
    requests = _collect_start(spider)
    assert len(requests) == 1
    assert "playwright" not in requests[0].meta


def test_nhatot_sends_playwright_meta_on_requests(tmp_path):
    spider = NhatotSpider(db_path=str(tmp_path / "f.db"))
    spider.frontier.add_urls("nhatot", ["https://nhatot.example/x-1"])
    requests = _collect_start(spider)
    assert len(requests) == 1
    assert requests[0].meta["playwright"] is True
