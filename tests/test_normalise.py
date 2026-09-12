import pytest
from parsers.normalise import parse_price_vnd, parse_area_sqm, strip_pii


@pytest.mark.parametrize("text,expected", [
    ("7 triệu/tháng", 7_000_000),
    ("7tr5", 7_500_000),
    ("7,500,000 đ", 7_500_000),
    ("7.5 triệu", 7_500_000),
    ("12tr500", 12_500_000),
    ("850 nghìn", 850_000),
    ("Thỏa thuận", None),
    ("", None),
])
def test_parse_price_vnd(text, expected):
    assert parse_price_vnd(text) == expected


@pytest.mark.parametrize("text,expected", [
    ("25 m²", 25.0), ("25m2", 25.0), ("25,5 m²", 25.5), ("Đang cập nhật", None),
])
def test_parse_area_sqm(text, expected):
    assert parse_area_sqm(text) == expected


def test_strip_pii_removes_phone_and_zalo():
    raw = "Liên hệ 0901234567 hoặc zalo 090.123.4567, mail a@b.com"
    out = strip_pii(raw)
    assert "0901234567" not in out
    assert "090.123.4567" not in out
    assert "a@b.com" not in out
    assert "<PHONE>" in out and "<EMAIL>" in out
