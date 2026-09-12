"""Sitemap poller: extract rental URLs from XML sitemaps."""

import argparse

from lxml import etree

import config
from crawler.frontier import Frontier


def extract_urls(xml: str, pattern: str) -> list[str]:
    root = etree.fromstring(xml.encode("utf-8"))
    local = etree.QName(root).localname

    if local == "sitemapindex":
        locs = root.xpath("//*[local-name()='sitemap']/*[local-name()='loc']/text()")
        return [loc.strip() for loc in locs]

    locs = root.xpath("//*[local-name()='url']/*[local-name()='loc']/text()")
    if pattern:
        return [loc.strip() for loc in locs if pattern in loc]
    return [loc.strip() for loc in locs]


def poll_source(source: str, frontier: Frontier) -> int:
    import urllib.request

    cfg = config.load("sources")
    src_cfg = cfg["sources"][source]
    pattern = src_cfg.get("rental_url_pattern", "")
    total_added = 0

    for sitemap_url in src_cfg["sitemap_urls"]:
        req = urllib.request.Request(
            sitemap_url,
            headers={"User-Agent": cfg["user_agent"]},
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            xml = resp.read().decode("utf-8")

        urls = extract_urls(xml, pattern)

        index_root = etree.fromstring(xml.encode("utf-8"))
        if etree.QName(index_root).localname == "sitemapindex":
            for sub_url in urls:
                sub_req = urllib.request.Request(
                    sub_url,
                    headers={"User-Agent": cfg["user_agent"]},
                )
                with urllib.request.urlopen(sub_req, timeout=30) as sub_resp:
                    sub_xml = sub_resp.read().decode("utf-8")
                sub_urls = extract_urls(sub_xml, pattern)
                total_added += frontier.add_urls(source, sub_urls)
        else:
            total_added += frontier.add_urls(source, urls)

    return total_added


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", help="Single source to poll")
    parser.add_argument("--all", action="store_true", help="Poll all sources")
    parser.add_argument("--db", required=True, help="Path to frontier SQLite DB")
    args = parser.parse_args()

    cfg = config.load("sources")
    frontier = Frontier(args.db)

    sources = list(cfg["sources"]) if args.all else [args.source]
    for src in sources:
        added = poll_source(src, frontier)
        print(f"{src}: {added} new URLs")


if __name__ == "__main__":
    main()
