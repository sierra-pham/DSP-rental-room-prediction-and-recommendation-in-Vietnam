"""Vietnamese text normalisation for listing fields.

Pure functions only — no network, no I/O. Every parser routes its raw strings
through here so that price and area are comparable across the four sources.
"""

import logging
import re
import unicodedata

import config

_log = logging.getLogger(__name__)
_PT_MAP = None
_FURN_MAP = None

_PHONE_RE = re.compile(r"(?:\+?84|0)[\s.\-]?\d{2,3}[\s.\-]?\d{3}[\s.\-]?\d{3,4}")
_EMAIL_RE = re.compile(r"[\w.+-]+@[\w-]+\.[\w.]+")
_SOCIAL_RE = re.compile(r"(?:zalo|facebook|fb|viber)[\s:]*[\w./]+", re.IGNORECASE)

# "7tr5", "7.5 triệu", "12tr500" -- the whole part may carry a decimal separator,
# in which case there is no trailing fraction group to read.
_MILLION_RE = re.compile(r"(\d+(?:[.,]\d+)?)\s*tr(?:iệu)?\s*(\d+)?")
_THOUSAND_RE = re.compile(r"([\d.,]+)\s*(?:nghìn|ngàn|k)\b")
_BARE_DIGITS_RE = re.compile(r"([\d.,]{7,})")
_AREA_RE = re.compile(r"([\d]+(?:[.,]\d+)?)\s*m(?:2|²)")

# The six provinces silver partitions by. Sites spell them a dozen ways
# ("TPHCM", "Tp Hồ Chí Minh", "Sài Gòn"), so match on a lowered,
# diacritic-free key with the administrative prefix removed.
_PROVINCE_PREFIX_RE = re.compile(r"^(?:tp\.?|thanh pho|tinh)\s*")
# District spellings carry the administrative prefix inconsistently: "Quận 3"
# on mogi, "Q.7" elsewhere, a bare "Bình Thạnh" on batdongsan. Silver stores the
# bare name so the blocking key in spec 4.x matches across sources.
_DISTRICT_PREFIX_RE = re.compile(r"^(?:quan|huyen|thanh pho|thi xa|tp|q)\s*[.\s]\s*")

# Spec 3.2: furnishing is this enum and nothing else.
FURNISHING_VALUES = ("none", "basic", "full", "unknown")

_PROVINCE_CODES = {
    "ho chi minh": "HCM",
    "hcm": "HCM",
    "sai gon": "HCM",
    "saigon": "HCM",
    "ha noi": "HN",
    "hanoi": "HN",
    "da nang": "DN",
    "danang": "DN",
    "binh duong": "BD",
    "dong nai": "DNA",
    "can tho": "CT",
}


def strip_pii(text: str) -> str:
    if not text:
        return ""
    text = _EMAIL_RE.sub("<EMAIL>", text)
    text = _SOCIAL_RE.sub("<SOCIAL>", text)
    text = _PHONE_RE.sub("<PHONE>", text)
    return text


def normalise_text(text: str) -> str:
    if not text:
        return ""
    text = unicodedata.normalize("NFC", text)
    return re.sub(r"\s+", " ", text).strip().lower()


def parse_price_vnd(text: str) -> int | None:
    """Handle 7tr5 / 7.5 triệu / 7,500,000 / 850 nghìn. Returns None if not a number."""
    if not text:
        return None
    t = normalise_text(text)
    if "thỏa thuận" in t or "thoa thuan" in t:
        return None

    m = _MILLION_RE.search(t)
    if m:
        whole, frac = m.group(1), m.group(2)
        if "." in whole or "," in whole:
            # "7.5 triệu" -> 7.5 million. A decimal whole part consumes the value,
            # so any trailing digits are not a fraction.
            return int(float(whole.replace(",", ".")) * 1_000_000)
        if frac is None:
            return int(whole) * 1_000_000
        # "7tr5" -> 7.5 million; "12tr500" -> 12.5 million
        return int(whole) * 1_000_000 + int(frac) * (10 ** (6 - len(frac)))

    m = _THOUSAND_RE.search(t)
    if m:
        return int(float(m.group(1).replace(",", ".")) * 1_000)

    m = _BARE_DIGITS_RE.search(t)
    if m:
        digits = re.sub(r"[.,]", "", m.group(1))
        return int(digits)
    return None


def parse_area_sqm(text: str) -> float | None:
    if not text:
        return None
    t = normalise_text(text)
    m = _AREA_RE.search(t)
    return float(m.group(1).replace(",", ".")) if m else None


def _strip_diacritics(text: str) -> str:
    """Drop combining marks; "đ" has none to drop, so it is mapped by hand."""
    decomposed = unicodedata.normalize("NFD", text)
    bare = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    return bare.replace("đ", "d").replace("Đ", "D")


def normalise_province(raw: str | None) -> str | None:
    """Map a site's province string onto one of the six silver province codes.

    Returns None for anything outside them --- the parse job keeps the row and
    the quality gate counts it rather than guessing.
    """
    if not raw:
        return None
    key = _PROVINCE_PREFIX_RE.sub("", _strip_diacritics(normalise_text(raw))).strip()
    return _PROVINCE_CODES.get(key)


def normalise_district(raw: str | None) -> str | None:
    """Bare, lowered, diacritic-free district name --- "Quận Gò Vấp" -> "go vap".

    Spec 3.2 stores `district` normalised and diacritic-stripped so the four
    sources' spellings collide on one key. The administrative prefix is dropped
    because only some sources write it; "Quận 3" and "Q.3" are the same place.
    Returns None when there is nothing left to store.
    """
    if not raw:
        return None
    key = _DISTRICT_PREFIX_RE.sub("", _strip_diacritics(normalise_text(raw))).strip()
    return key or None


def normalise_furnishing(source: str, raw: str | None) -> str:
    """Map a site's furnishing string onto the spec 3.2 enum. Never None.

    Unmapped is "unknown", not a guess: a wrong furnishing reads as a fact in
    the mart, whereas "unknown" reads as the absence of one. The warning is what
    tells us a site added a value.
    """
    global _FURN_MAP
    if _FURN_MAP is None:
        _FURN_MAP = config.load("furnishing_map")
    if not raw:
        return "unknown"
    mapping = _FURN_MAP.get(source, {})
    if raw in mapping:
        return mapping[raw]
    raw_lower = raw.lower().strip()
    for key, val in mapping.items():
        if key.lower().strip() == raw_lower:
            return val
    # phongtro123's parser resolves NTĐB/NTCB itself, so it hands us the enum.
    if raw_lower in FURNISHING_VALUES:
        return raw_lower
    _log.warning("Unmapped furnishing: source=%s raw=%r", source, raw)
    return "unknown"


def normalise_property_type(source: str, raw: str) -> str:
    global _PT_MAP
    if _PT_MAP is None:
        _PT_MAP = config.load("property_type_map")
    if not raw:
        return "other"
    mapping = _PT_MAP.get(source, {})
    if raw in mapping:
        return mapping[raw]
    raw_lower = raw.lower().strip()
    for key, val in mapping.items():
        if key.lower().strip() == raw_lower:
            return val
    _log.warning("Unmapped property type: source=%s raw=%r", source, raw)
    return "other"
