"""Pre-flight checks for launching the crawler: everything `scrapy crawl` and the
cron wrapper need before the first discovery pass. See docs/03-implementation-plan.md
Task 6 steps 5-7."""
import importlib.util
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
from scrapy.utils.misc import load_object

import config
from crawler.frontier import default_db_path
from crawler.spiders.batdongsan import BatdongsanSpider
from crawler.spiders.nhatot import NhatotSpider

ROOT = Path(__file__).resolve().parent.parent
SOURCES = ["batdongsan", "mogi", "nhatot", "phongtro123"]


# --- item 1: scrapy.cfg -------------------------------------------------------

def test_scrapy_cli_discovers_all_spiders_from_repo_root():
    proc = subprocess.run(
        [sys.executable, "-m", "scrapy", "list"],
        cwd=ROOT, capture_output=True, text=True, timeout=120,
    )
    assert proc.returncode == 0, proc.stderr
    assert sorted(proc.stdout.split()) == SOURCES


# --- items 2 and 3: sitemap URLs as declared by each site's robots.txt --------

@pytest.mark.parametrize("source,expected", [
    # Sitemap: lines read from https://<domain>/robots.txt on 2026-09-12.
    ("mogi", ["https://mogi.vn/sitemap/sitemap-detail.xml"]),
    ("nhatot", ["https://www.nhatot.com/sitemap-index.xml"]),
    ("phongtro123", ["https://phongtro123.com/sitemap.xml"]),
])
def test_sitemap_url_matches_robots_declaration(source, expected):
    assert config.load("sources")["sources"][source]["sitemap_urls"] == expected


# --- item 4: Playwright extra installed ---------------------------------------

def test_nhatot_download_handler_is_importable():
    assert importlib.util.find_spec("playwright") is not None, "pip install -e '.[js]'"
    handler = NhatotSpider.custom_settings["DOWNLOAD_HANDLERS"]["https"]
    assert load_object(handler) is not None


# --- item 6: one frontier path shared by spider, poller and cron --------------

def test_default_db_path_comes_from_env(monkeypatch):
    monkeypatch.setenv("FRONTIER_DB", "/var/data/frontier.db")
    assert default_db_path() == "/var/data/frontier.db"


def test_default_db_path_falls_back_to_cwd_file(monkeypatch):
    monkeypatch.delenv("FRONTIER_DB", raising=False)
    assert default_db_path() == "frontier.db"


def test_spider_without_db_path_uses_shared_default(tmp_path, monkeypatch):
    db = tmp_path / "shared.db"
    monkeypatch.setenv("FRONTIER_DB", str(db))
    BatdongsanSpider()
    assert db.exists()


def test_poller_db_flag_is_optional(tmp_path, monkeypatch):
    db = tmp_path / "shared.db"
    monkeypatch.setenv("FRONTIER_DB", str(db))
    monkeypatch.setattr(sys, "argv", ["sitemap_poller", "--source", "phongtro123"])
    from crawler import sitemap_poller
    monkeypatch.setattr(sitemap_poller, "poll_source", lambda src, frontier: 0)
    sitemap_poller.main()
    assert db.exists()


# --- item 5: run_discovery.sh -------------------------------------------------

@pytest.mark.skipif(shutil.which("bash") is None, reason="bash not on PATH")
def test_run_discovery_crawls_every_source_against_one_frontier():
    env = {**os.environ, "SCRAPY": "echo", "PYTHON": sys.executable,
           "FRONTIER_DB": "/var/data/frontier.db"}
    proc = subprocess.run(
        ["bash", str(ROOT / "run_discovery.sh")],
        cwd=ROOT, capture_output=True, text=True, timeout=120, env=env,
    )
    assert proc.returncode == 0, proc.stderr
    for src in SOURCES:
        assert f"crawl {src} -a db_path=/var/data/frontier.db" in proc.stdout


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash not on PATH")
def test_run_discovery_keeps_going_when_a_spider_fails():
    # `false` stands in for scrapy: every crawl exits non-zero. All four sources must
    # still be attempted, and the pass must report failure.
    env = {**os.environ, "SCRAPY": "false", "PYTHON": sys.executable}
    proc = subprocess.run(
        ["bash", str(ROOT / "run_discovery.sh")],
        cwd=ROOT, capture_output=True, text=True, timeout=120, env=env,
    )
    assert proc.returncode == 1
    for src in SOURCES:
        assert f"{src} FAILED" in proc.stdout
