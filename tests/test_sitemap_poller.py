from pathlib import Path
from crawler.sitemap_poller import extract_urls

FIXTURES = Path(__file__).parent / "fixtures"


def test_extract_urls_filters_by_pattern():
    xml = (FIXTURES / "sitemap_sample.xml").read_text(encoding="utf-8")
    urls = extract_urls(xml, pattern="/cho-thue-")
    assert all("/cho-thue-" in u for u in urls)
    assert len(urls) == 3


def test_extract_urls_handles_sitemap_index():
    xml = """<?xml version="1.0"?>
    <sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
      <sitemap><loc>https://x.com/sitemap-1.xml</loc></sitemap>
    </sitemapindex>"""
    assert extract_urls(xml, pattern="") == ["https://x.com/sitemap-1.xml"]
