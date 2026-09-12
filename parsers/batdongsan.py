import json
import re

import lxml.html

from parsers.base import Parser
from parsers.normalise import (
    normalise_property_type,
    parse_area_sqm,
    parse_price_vnd,
    strip_pii,
)

_ID_RE = re.compile(r"pr(\d+)", re.IGNORECASE)


def _ld_vacation_rental(doc):
    for script in doc.cssselect('script[type="application/ld+json"]'):
        raw = script.text_content().strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(data, dict) and data.get("@type") == "VacationRental":
            return data
    return {}


def _first_text(nodes):
    return nodes[0].text_content().strip() if nodes else None


class BatdongsanParser(Parser):
    source = "batdongsan"

    def parse(self, html: str, url: str) -> dict:
        doc = lxml.html.fromstring(html)
        ld = _ld_vacation_rental(doc)
        cp = ld.get("ContainsPlace", {})
        addr = cp.get("Address", {})

        # listing_id
        raw_id = ld.get("identifier")
        if not raw_id:
            prid_nodes = doc.cssselect("#product-detail-web")
            raw_id = prid_nodes[0].get("prid") if prid_nodes else None
        if not raw_id:
            m = _ID_RE.search(url)
            raw_id = m.group(1) if m else None
        listing_id = f"batdongsan:{raw_id}" if raw_id else None

        # title
        title = ld.get("name") or _first_text(doc.cssselect("h1.re__pr-title"))

        # price
        price_text = ld.get("priceRange")
        if not price_text:
            for item in doc.cssselect("div.re__pr-short-info-item"):
                title_spans = item.cssselect("span.title")
                if title_spans and "giá" in title_spans[0].text_content().lower():
                    val_spans = item.cssselect("span.value")
                    price_text = val_spans[0].text_content().strip() if val_spans else None
                    break
        asking_rent_vnd = parse_price_vnd(price_text)

        # area — JSON-LD FloorSize.Value is a numeric string; convert directly
        floor_size = cp.get("FloorSize", {})
        fs_val = floor_size.get("Value") if floor_size.get("UnitCode") == "SQM" else None
        if fs_val:
            area_sqm = float(fs_val)
        else:
            area_text = None
            for item in doc.cssselect("div.re__pr-short-info-item"):
                title_spans = item.cssselect("span.title")
                if title_spans and "diện tích" in title_spans[0].text_content().lower():
                    val_spans = item.cssselect("span.value")
                    area_text = val_spans[0].text_content().strip() if val_spans else None
                    break
            area_sqm = parse_area_sqm(area_text)

        # province from breadcrumb level 2; district from JSON-LD then breadcrumb level 3
        province_nodes = doc.cssselect('div.re__breadcrumb a[level="2"]')
        district_nodes = doc.cssselect('div.re__breadcrumb a[level="3"]')
        province = _first_text(province_nodes)
        district = addr.get("@addressRegion") or _first_text(district_nodes)

        # property_type
        raw_type = ld.get("additionalType", "")
        property_type = normalise_property_type("batdongsan", raw_type)

        # description
        desc_raw = ld.get("description")
        if not desc_raw:
            desc_nodes = doc.cssselect("div.re__detail-content")
            desc_raw = desc_nodes[0].text_content().strip() if desc_nodes else ""
        description_clean = strip_pii(desc_raw or "")

        # bedrooms / bathrooms — JSON-LD values are digit strings
        nb = cp.get("numberOfBedrooms")
        bedrooms = int(nb) if nb and nb.isdigit() else None
        nbt = cp.get("numberOfBathroomsTotal")
        bathrooms = int(nbt) if nbt and nbt.isdigit() else None

        # coordinates
        lat = ld.get("Latitude")
        lon = ld.get("Longitude")
        latitude = float(lat) if lat else None
        longitude = float(lon) if lon else None

        # furnishing — first amenityFeature whose Name is "Furnished"
        furnishing = None
        for feature in cp.get("amenityFeature", []):
            if isinstance(feature, dict) and feature.get("Name") == "Furnished":
                furnishing = feature.get("Value")
                break

        # address
        address = addr.get("@streetAddress") or _first_text(
            doc.cssselect("span.re__address-line-1")
        )

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
