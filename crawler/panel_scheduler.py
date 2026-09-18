"""Daily panel probe scheduler and liveness classifier.

The panel cohort is a frozen set of listings re-fetched daily. Each probe
records whether the listing is still present on the site. The tricky part
is soft-delete detection: these sites often return HTTP 200 with a
"listing removed" page rather than a proper 404, so treating status alone
as the signal would silently censor every real event.

Usage:
    python -m crawler.panel_scheduler --run
"""

import argparse
import datetime as dt
import logging
import re

import config

_log = logging.getLogger(__name__)

_REMOVAL_PHRASES = [
    "tin đăng không tồn tại",
    "đã bị gỡ",
    "không tìm thấy",
    "hết hạn",
    "tin đã hết hạn",
    "bài đăng không tồn tại",
    "nội dung đã bị xóa",
    "đã được gỡ bỏ",
]

_REMOVAL_RE = re.compile("|".join(re.escape(p) for p in _REMOVAL_PHRASES), re.IGNORECASE)


def classify_probe(status: int, html: str) -> dict:
    """Classify a single probe response as present or absent.

    Returns a dict with at minimum ``is_present`` (bool) and ``reason``
    (str or None explaining why it was marked absent).
    """
    if status != 200:
        return {"is_present": False, "reason": f"http_status:{status}"}

    if html and _REMOVAL_RE.search(html):
        return {"is_present": False, "reason": "soft_delete"}

    return {"is_present": True, "reason": None}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")

    ap = argparse.ArgumentParser(description="Run daily panel probes.")
    ap.add_argument("--run", action="store_true", help="execute the daily probe run")
    ap.add_argument("--db", default=None, help="frontier database path")
    args = ap.parse_args()

    if args.run:
        from crawler.frontier import Frontier, default_db_path

        db_path = args.db or default_db_path()
        frontier = Frontier(db_path)
        today = dt.date.today()
        urls = frontier.panel_due(as_of=today)
        _log.info("panel due: %d URLs for %s", len(urls), today.isoformat())
