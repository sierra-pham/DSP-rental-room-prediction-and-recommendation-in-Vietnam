"""Tests for the panel probe classifier.

The third test is the one that matters most: these sites return HTTP 200
with a "listing removed" page rather than a 404. Treating that as
"present" would silently censor every real event in the panel.
"""

from crawler.panel_scheduler import classify_probe


def test_200_is_present():
    assert classify_probe(200, "<html>...listing...</html>")["is_present"] is True


def test_404_is_absent():
    result = classify_probe(404, "")
    assert result["is_present"] is False
    assert "http_status" in result["reason"]


def test_soft_delete_page_is_absent():
    html = "<html><body>Tin đăng không tồn tại hoặc đã bị gỡ</body></html>"
    result = classify_probe(200, html)
    assert result["is_present"] is False
    assert result["reason"] == "soft_delete"


def test_expired_listing_is_absent():
    html = "<html><body><div class='error'>Tin đã hết hạn</div></body></html>"
    assert classify_probe(200, html)["is_present"] is False


def test_500_is_absent():
    result = classify_probe(500, "")
    assert result["is_present"] is False


def test_real_listing_page_is_present():
    html = "<html><body><h1>Cho thuê căn hộ 2PN Quận 7</h1><p>9.5 triệu/tháng</p></body></html>"
    assert classify_probe(200, html)["is_present"] is True


def test_empty_html_with_200_is_present():
    """An empty 200 response is ambiguous but not a known removal page."""
    assert classify_probe(200, "")["is_present"] is True
