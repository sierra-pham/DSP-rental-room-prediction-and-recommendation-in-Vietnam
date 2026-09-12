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
