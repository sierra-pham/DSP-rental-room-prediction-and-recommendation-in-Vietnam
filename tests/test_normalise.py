import pytest
from parsers.normalise import (
    FURNISHING_VALUES,
    normalise_district,
    normalise_furnishing,
    normalise_province,
    parse_area_sqm,
    parse_price_vnd,
    strip_pii,
)


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


@pytest.mark.parametrize("raw,expected", [
    ("Hồ Chí Minh", "HCM"),
    ("TP. Hồ Chí Minh", "HCM"),
    ("TPHCM", "HCM"),
    ("TP HCM", "HCM"),
    ("Tp Hồ Chí Minh", "HCM"),
    ("  sài gòn ", "HCM"),
    ("Ho Chi Minh", "HCM"),
    ("Hà Nội", "HN"),
    ("Thành phố Hà Nội", "HN"),
    ("hanoi", "HN"),
    ("Đà Nẵng", "DN"),
    ("da nang", "DN"),
    ("Bình Dương", "BD"),
    ("Tỉnh Bình Dương", "BD"),
    ("Đồng Nai", "DNA"),
    ("Cần Thơ", "CT"),
    ("Khánh Hòa", None),
    ("", None),
    (None, None),
])
def test_normalise_province(raw, expected):
    assert normalise_province(raw) == expected


@pytest.mark.parametrize("source,raw,expected", [
    # Measured on the four fixtures: batdongsan "Đầy đủ", phongtro123 "basic",
    # mogi None, nhatot "Nội thất đầy đủ".
    ("batdongsan", "Đầy đủ", "full"),
    ("batdongsan", "Cơ bản", "basic"),
    ("batdongsan", "Không nội thất", "none"),
    ("batdongsan", "đầy đủ", "full"),          # case-insensitive fallback
    ("nhatot", "Nội thất đầy đủ", "full"),
    ("nhatot", "Nội thất cơ bản", "basic"),
    ("nhatot", "Không nội thất", "none"),
    ("mogi", "Đầy đủ", "full"),
    ("mogi", "Cơ bản", "basic"),
    # phongtro123 emits the enum values itself; they pass through untouched.
    ("phongtro123", "basic", "basic"),
    ("phongtro123", "full", "full"),
    # Nothing to map is "unknown", never None.
    ("mogi", None, "unknown"),
    ("mogi", "", "unknown"),
    ("batdongsan", "unknown", "unknown"),
])
def test_normalise_furnishing(source, raw, expected):
    assert normalise_furnishing(source, raw) == expected


def test_normalise_furnishing_warns_on_an_unmapped_value(caplog):
    with caplog.at_level("WARNING", logger="parsers.normalise"):
        assert normalise_furnishing("batdongsan", "Nội thất kiểu Bắc Âu") == "unknown"
    assert "Unmapped furnishing" in caplog.text


def test_normalise_furnishing_never_returns_none_for_an_unknown_source():
    assert normalise_furnishing("zillow", "Đầy đủ") == "unknown"


def test_every_furnishing_map_value_is_in_the_enum():
    """The YAML is data; a typo in it would otherwise reach silver."""
    import config

    mapping = config.load("furnishing_map")
    for source, pairs in mapping.items():
        for raw, value in pairs.items():
            assert value in FURNISHING_VALUES, f"{source}/{raw!r} -> {value!r}"


@pytest.mark.parametrize("raw,expected", [
    ("Bình Thạnh", "binh thanh"),
    ("Quận Gò Vấp", "go vap"),
    ("Quận 3", "3"),
    ("Q.7", "7"),
    ("Huyện Bình Chánh", "binh chanh"),
    ("TP. Thủ Đức", "thu duc"),
    ("Thành phố Thủ Đức", "thu duc"),
    ("Thị xã Dĩ An", "di an"),
    ("  Quận   Tân   Bình  ", "tan binh"),
    ("Quế Võ", "que vo"),          # a leading "q" that is not a prefix
    ("Quận", "quan"),              # the prefix must be followed by space or dot
    ("Q.", None),                  # nothing left after the prefix
    ("", None),
    (None, None),
])
def test_normalise_district(raw, expected):
    assert normalise_district(raw) == expected
