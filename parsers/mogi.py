"""Parser for mogi.vn rental listings."""

import re
from urllib.parse import urlparse, parse_qs

import lxml.html

from parsers.base import Parser
from parsers.normalise import (
    parse_price_vnd,
    parse_area_sqm,
    strip_pii,
    normalise_property_type,
)

_CAT_RE = re.compile(r"var\s+catLv3\s*=\s*'([^']+)'")
_CAT_JS_RE = re.compile(r'"postCategory3"\s*:\s*"([^"]+)"')
_PRICE_JS_RE = re.compile(r'"price"\s*:\s*([\d.]+)')
_PROP_ID_URL_RE = re.compile(r'-id(\d+)$')
_PROP_ID_JS_RE = re.compile(r'"propertyId"\s*:\s*(\d+)')


class MogiParser(Parser):
    source = "mogi"

    def parse(self, html: str, url: str) -> dict:
        doc = lxml.html.fromstring(html)

        # Build info-attrs dict from all div.info-attr elements
        info_attrs = {}
        for div in doc.cssselect('div.info-attr'):
            spans = div.cssselect('span')
            if len(spans) == 2:
                key = spans[0].text_content().strip()
                val = spans[1].text_content().strip()
                info_attrs[key] = val

        # listing_id: prefer "Mã BĐS" attr, then URL, then JS
        listing_id = None
        raw_id = info_attrs.get("Mã BĐS")
        if raw_id:
            listing_id = f"mogi:{raw_id}"
        if not listing_id:
            m = _PROP_ID_URL_RE.search(url)
            if m:
                listing_id = f"mogi:{m.group(1)}"
        if not listing_id:
            m = _PROP_ID_JS_RE.search(html)
            if m:
                listing_id = f"mogi:{m.group(1)}"

        # title
        title = None
        h1_els = doc.cssselect('div.title h1')
        if h1_els:
            title = h1_els[0].text_content().strip() or None

        # price: DOM first, JS numeric fallback
        asking_rent_vnd = None
        price_els = doc.cssselect('div.price')
        if price_els:
            asking_rent_vnd = parse_price_vnd(price_els[0].text_content().strip())
        if asking_rent_vnd is None:
            m = _PRICE_JS_RE.search(html)
            if m:
                asking_rent_vnd = int(float(m.group(1)))

        # area
        area_sqm = parse_area_sqm(info_attrs.get("Diện tích sử dụng", ""))

        # address, province, district
        province = None
        district = None
        address = None
        addr_els = doc.cssselect('div.main-info div.address')
        if addr_els:
            address = addr_els[0].text_content().strip() or None
            if address:
                parts = [p.strip() for p in address.split(',')]
                if parts:
                    province = parts[-1] or None
                if len(parts) >= 2:
                    district = parts[-2] or None

        # property_type: JS catLv3 variable, then pageData postCategory3
        property_type = "other"
        raw_type = None
        m = _CAT_RE.search(html)
        if m:
            raw_type = m.group(1)
        if not raw_type:
            m = _CAT_JS_RE.search(html)
            if m:
                raw_type = m.group(1)
        if raw_type:
            property_type = normalise_property_type("mogi", raw_type)

        # description: strip PII before storing
        description_clean = None
        desc_els = doc.cssselect('div.info-content-body')
        if desc_els:
            raw_desc = desc_els[0].text_content().strip()
            description_clean = strip_pii(raw_desc) or None

        # bedrooms
        bedrooms = None
        if "Phòng ngủ" in info_attrs:
            try:
                bedrooms = int(info_attrs["Phòng ngủ"])
            except (ValueError, TypeError):
                pass

        # bathrooms
        bathrooms = None
        bath_raw = info_attrs.get("Phòng tắm") or info_attrs.get("Toilet")
        if bath_raw:
            try:
                bathrooms = int(bath_raw)
            except (ValueError, TypeError):
                pass

        # latitude / longitude from lazy-loaded iframe data-src
        latitude = None
        longitude = None
        for iframe in doc.cssselect('iframe[data-src]'):
            data_src = iframe.get('data-src', '')
            if 'maps/embed' in data_src:
                qs = parse_qs(urlparse(data_src).query)
                q_vals = qs.get('q', [])
                if q_vals:
                    coords = q_vals[0].split(',')
                    if len(coords) == 2:
                        try:
                            latitude = float(coords[0])
                            longitude = float(coords[1])
                        except ValueError:
                            pass
                break

        # furnishing
        furnishing = info_attrs.get("Nội thất") or None

        return {
            "listing_id": listing_id,
            "source": self.source,
            "title": title,
            "asking_rent_vnd": asking_rent_vnd,
            "area_sqm": area_sqm,
            "province": province,
            "district": district,
            "property_type": property_type,
            "description_clean": description_clean,
            "bedrooms": bedrooms,
            "bathrooms": bathrooms,
            "latitude": latitude,
            "longitude": longitude,
            "furnishing": furnishing,
            "address": address,
        }
