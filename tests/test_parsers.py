import re
from pathlib import Path

import pytest

from parsers.batdongsan import BatdongsanParser
from parsers.phongtro123 import Phongtro123Parser
from parsers.mogi import MogiParser
from parsers.nhatot import NhatotParser

FIXTURES = Path(__file__).parent / "fixtures"

REQUIRED = ("listing_id", "source", "asking_rent_vnd", "area_sqm",
            "province", "district", "property_type", "description_clean")

VALID_TYPES = {"apartment", "house", "room", "studio", "shophouse", "townhouse", "other"}

PARSER_CASES = [
    (BatdongsanParser,  "batdongsan_sample_01.html",
     "https://batdongsan.com.vn/cho-thue-can-ho/pr46260999"),
    (Phongtro123Parser, "phongtro123_sample_01.html",
     "https://phongtro123.com/x-pr712920.html"),
    (MogiParser,        "mogi_sample_01.html",
     "https://mogi.vn/x-id22742913"),
    (NhatotParser,      "nhatot_sample_01.html",
     "https://www.nhatot.com/x/134122706.htm"),
]


@pytest.mark.parametrize("parser_cls,fixture,url", PARSER_CASES,
                         ids=["batdongsan", "phongtro123", "mogi", "nhatot"])
def test_every_parser_returns_required_fields(parser_cls, fixture, url):
    html = (FIXTURES / fixture).read_text(encoding="utf-8")
    out = parser_cls().parse(html, url)
    for field in REQUIRED:
        assert field in out, f"{parser_cls.source} missing {field}"
    assert out["source"] == parser_cls.source


@pytest.mark.parametrize("parser_cls,fixture,url", PARSER_CASES,
                         ids=["batdongsan", "phongtro123", "mogi", "nhatot"])
def test_every_parser_normalises_property_type(parser_cls, fixture, url):
    html = (FIXTURES / fixture).read_text(encoding="utf-8")
    out = parser_cls().parse(html, url)
    assert out["property_type"] in VALID_TYPES, \
        f"{parser_cls.source} returned {out['property_type']!r}"


@pytest.mark.parametrize("parser_cls,fixture,url", PARSER_CASES,
                         ids=["batdongsan", "phongtro123", "mogi", "nhatot"])
def test_every_parser_strips_pii(parser_cls, fixture, url):
    html = (FIXTURES / fixture).read_text(encoding="utf-8")
    out = parser_cls().parse(html, url)
    desc = out.get("description_clean", "") or ""
    assert not re.search(r"(?:\+?84|0)\d{9}", desc), \
        f"{parser_cls.source} leaked a phone number in description_clean"


@pytest.mark.parametrize("parser_cls,fixture,url", PARSER_CASES,
                         ids=["batdongsan", "phongtro123", "mogi", "nhatot"])
def test_listing_id_has_source_prefix(parser_cls, fixture, url):
    html = (FIXTURES / fixture).read_text(encoding="utf-8")
    out = parser_cls().parse(html, url)
    assert out["listing_id"].startswith(f"{parser_cls.source}:"), \
        f"listing_id should start with '{parser_cls.source}:'"


@pytest.mark.parametrize("parser_cls,fixture,url", PARSER_CASES,
                         ids=["batdongsan", "phongtro123", "mogi", "nhatot"])
def test_rent_is_positive_or_none(parser_cls, fixture, url):
    html = (FIXTURES / fixture).read_text(encoding="utf-8")
    out = parser_cls().parse(html, url)
    rent = out["asking_rent_vnd"]
    assert rent is None or rent > 0
