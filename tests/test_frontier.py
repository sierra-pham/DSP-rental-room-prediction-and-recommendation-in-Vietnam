import datetime as dt
import pytest
from crawler.frontier import Frontier


@pytest.fixture
def fr(tmp_path):
    return Frontier(str(tmp_path / "frontier.db"))


def test_add_urls_deduplicates(fr):
    assert fr.add_urls("mogi", ["https://mogi.vn/a", "https://mogi.vn/b"]) == 2
    assert fr.add_urls("mogi", ["https://mogi.vn/b", "https://mogi.vn/c"]) == 1


def test_next_batch_returns_unfetched_only(fr):
    fr.add_urls("mogi", ["https://mogi.vn/a", "https://mogi.vn/b"])
    fr.mark_fetched("https://mogi.vn/a", 200, "hash1")
    batch = fr.next_batch("mogi", kind="discovery", limit=10)
    assert batch == ["https://mogi.vn/b"]


def test_two_consecutive_failures_marks_gone(fr):
    fr.add_urls("mogi", ["https://mogi.vn/a"])
    fr.mark_failed("https://mogi.vn/a")
    assert fr.is_gone("https://mogi.vn/a") is False
    fr.mark_failed("https://mogi.vn/a")
    assert fr.is_gone("https://mogi.vn/a") is True


def test_success_resets_failure_counter(fr):
    fr.add_urls("mogi", ["https://mogi.vn/a"])
    fr.mark_failed("https://mogi.vn/a")
    fr.mark_fetched("https://mogi.vn/a", 200, "hash1")
    fr.mark_failed("https://mogi.vn/a")
    assert fr.is_gone("https://mogi.vn/a") is False


def test_freeze_panel_cohort_selects_requested_size(fr):
    urls = [f"https://mogi.vn/{i}" for i in range(500)]
    fr.add_urls("mogi", urls)
    for u in urls:
        fr.mark_fetched(u, 200, "h")
    assert fr.freeze_panel_cohort(size=200) == 200
    assert len(fr.panel_due(as_of=dt.date(2026, 9, 20))) == 200
