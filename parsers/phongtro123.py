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

_ID_RE = re.compile(r"-pr(\d+)\.html", re.IGNORECASE)
_BED_RE = re.compile(r"(\d+)\s*(?:PN|phòng ngủ|p\.?\s*ngủ)", re.IGNORECASE)
_BATH_RE = re.compile(r"(\d+)\s*(?:wc|toilet|phòng tắm|vệ sinh)", re.IGNORECASE)


def _first_text(nodes):
    return nodes[0].text_content().strip() if nodes else None


def _ld_apartment(doc):
    """Return the JSON-LD block whose @type is an accommodation type, or {}."""
    for script in doc.cssselect('script[type="application/ld+json"]'):
        raw = script.text_content().strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(data, dict) and data.get("@type") in (
            "Apartment", "SingleFamilyResidence", "House", "Residence",
        ):
            return data
    return {}


def _detail_table(doc):
    """Return dict mapping label text → value text for the detail table rows."""
    table = {}
    tds = doc.cssselect("td")
    for i, td in enumerate(tds):
        label = td.text_content().strip().rstrip(":")
        if label and i + 1 < len(tds):
            table[label] = tds[i + 1].text_content().strip()
    return table


class Phongtro123Parser(Parser):
    source = "phongtro123"

    def parse(self, html: str, url: str) -> dict:  # noqa: C901
        doc = lxml.html.fromstring(html)
        ld = _ld_apartment(doc)
        addr_ld = ld.get("address", {})

        # listing_id
        body = doc.cssselect("body")
        raw_id = body[0].get("data-id") if body else None
        if not raw_id:
            m = _ID_RE.search(url)
            raw_id = m.group(1) if m else None
        listing_id = f"phongtro123:{raw_id}" if raw_id else None

        # title
        title = _first_text(doc.cssselect("h1"))

        # price
        price_nodes = doc.cssselect("span.text-green.fw-bold")
        asking_rent_vnd = parse_price_vnd(
            price_nodes[0].text_content().strip() if price_nodes else None
        )

        # area — JSON-LD floorSize.value is a bare numeric string
        fs_val = ld.get("floorSize", {}).get("value")
        area_sqm = parse_area_sqm(f"{fs_val}m2") if fs_val else None
        if area_sqm is None and title:
            area_sqm = parse_area_sqm(title)

        # province — JSON-LD addressLocality first, then detail table
        province = addr_ld.get("addressLocality")
        if not province:
            tbl = _detail_table(doc)
            province = tbl.get("Tỉnh thành")

        # district — breadcrumb item [2] (0-indexed) is the district span
        district = None
        bc_items = doc.cssselect("ol.breadcrumb li.breadcrumb-item")
        if len(bc_items) > 2:
            district = bc_items[2].text_content().strip() or None
        if not district:
            # fallback: detail table, strip "Cho thuê <word> " prefix
            tbl = _detail_table(doc) if "tbl" not in dir() else tbl  # type: ignore[name-defined]
            raw_district = tbl.get("Quận huyện") if isinstance(tbl, dict) else None  # type: ignore[union-attr]
            if raw_district:
                district = re.sub(r"^Cho thuê \S+\s+", "", raw_district).strip() or raw_district

        # property_type — JSON-LD @type is most reliable; fallback to active nav link
        raw_type = ld.get("@type") or ""
        if not raw_type:
            nav_link = doc.cssselect("a.border-orange")
            raw_type = nav_link[0].get("title", "") if nav_link else ""
        property_type = normalise_property_type("phongtro123", raw_type)

        # description — <p> children of the container that holds <h2>Thông tin mô tả</h2>
        desc_paras = []
        for h2 in doc.cssselect("h2"):
            if "mô tả" in h2.text_content():
                parent = h2.getparent()
                collecting = False
                for child in parent:
                    if child is h2:
                        collecting = True
                        continue
                    if collecting:
                        if child.tag == "h2":
                            break
                        if child.tag == "p":
                            desc_paras.append(child.text_content().strip())
                break
        desc_raw = "\n".join(desc_paras)
        description_clean = strip_pii(desc_raw)

        # combine title + description for bedroom/bathroom extraction
        search_text = f"{title or ''} {desc_raw}"

        # bedrooms
        m = _BED_RE.search(search_text)
        bedrooms = int(m.group(1)) if m else None

        # bathrooms
        m = _BATH_RE.search(desc_raw)
        bathrooms = int(m.group(1)) if m else None

        # furnishing — check title for NTCB / NTĐB abbreviations first
        furnishing = None
        title_upper = (title or "").upper()
        if "NTĐB" in title_upper or "NT ĐB" in title_upper:
            furnishing = "full"
        elif "NTCB" in title_upper or "NT CB" in title_upper:
            furnishing = "basic"
        else:
            # check Nổi bật section: green check icon next to "Đầy đủ nội thất"
            for div in doc.cssselect("div.text-body"):
                if div.cssselect("i.green") and "Đầy đủ nội thất" in div.text_content():
                    furnishing = "full"
                    break

        # coordinates — not available on phongtro123 detail pages
        latitude = None
        longitude = None

        # address
        address = (
            addr_ld.get("streetAddress")
            or _first_text(doc.cssselect("td[data-address]"))
        )
        if not address:
            tbl2 = _detail_table(doc)
            address = tbl2.get("Địa chỉ")

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
