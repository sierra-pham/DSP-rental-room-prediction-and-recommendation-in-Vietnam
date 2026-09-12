"""Parser for nhatot.com (Chợ Tốt) rental listings.

All listing data is embedded in a <script id="__NEXT_DATA__"> JSON blob.
The rendered DOM is not used.
"""

import json
import logging

import lxml.html

from parsers.base import Parser
from parsers.normalise import normalise_property_type, strip_pii

_log = logging.getLogger(__name__)


class NhatotParser(Parser):
    source = "nhatot"

    def parse(self, html: str, url: str) -> dict:
        try:
            doc = lxml.html.fromstring(html)
            tags = doc.cssselect("script#__NEXT_DATA__")
            if not tags:
                _log.warning("nhatot: __NEXT_DATA__ script tag not found in %s", url)
                return self._empty()
            data = json.loads(tags[0].text_content())
        except Exception as exc:
            _log.warning("nhatot: failed to parse JSON from %s: %s", url, exc)
            return self._empty()

        # Navigate to the ad objects with safe chained .get() calls
        props = data.get("props", {})
        page_props = props.get("pageProps", {})
        initial_state = page_props.get("initialState", {})
        ad_view = initial_state.get("adView", {})
        ad_info = ad_view.get("adInfo", {})
        ad = ad_info.get("ad", {})
        ad_params = ad_info.get("ad_params", {})

        # listing_id
        list_id = ad.get("list_id")
        listing_id = f"nhatot:{list_id}" if list_id is not None else None

        # title
        title = ad.get("subject")

        # price — already numeric VND integer
        price_raw = ad.get("price")
        asking_rent_vnd = int(price_raw) if price_raw is not None else None

        # area — use "size" (floor area in m²), NOT "area" (district code)
        size_raw = ad.get("size")
        area_sqm = float(size_raw) if size_raw is not None else None

        # location
        province = ad.get("region_name")
        district = ad.get("area_name")
        ward = ad.get("ward_name")
        street = ad.get("street_name")
        address = ad_params.get("address", {}).get("value")

        # coordinates
        lat_raw = ad.get("latitude")
        lon_raw = ad.get("longitude")
        latitude = float(lat_raw) if lat_raw is not None else None
        longitude = float(lon_raw) if lon_raw is not None else None

        # property type — prefer ad_params apartment_type, fall back to category_name
        raw_type = (
            ad_params.get("apartment_type", {}).get("value")
            or ad.get("category_name")
        )
        property_type = normalise_property_type("nhatot", raw_type or "")

        # description
        body = ad.get("body", "")
        description_clean = strip_pii(body or "")

        # rooms
        bedrooms_raw = ad.get("rooms")
        bedrooms = int(bedrooms_raw) if bedrooms_raw is not None else None
        bathrooms_raw = ad.get("toilets")
        bathrooms = int(bathrooms_raw) if bathrooms_raw is not None else None

        # furnishing
        furnishing = ad_params.get("furnishing_sell", {}).get("value")

        return {
            "listing_id": listing_id,
            "source": self.source,
            "title": title,
            "asking_rent_vnd": asking_rent_vnd,
            "area_sqm": area_sqm,
            "province": province,
            "district": district,
            "ward": ward,
            "street": street,
            "address": address,
            "latitude": latitude,
            "longitude": longitude,
            "property_type": property_type,
            "description_clean": description_clean,
            "bedrooms": bedrooms,
            "bathrooms": bathrooms,
            "furnishing": furnishing,
        }

    @staticmethod
    def _empty() -> dict:
        """Return a minimal dict with required keys set to None on parse failure."""
        return {
            "listing_id": None,
            "source": "nhatot",
            "title": None,
            "asking_rent_vnd": None,
            "area_sqm": None,
            "province": None,
            "district": None,
            "ward": None,
            "street": None,
            "address": None,
            "latitude": None,
            "longitude": None,
            "property_type": "other",
            "description_clean": "",
            "bedrooms": None,
            "bathrooms": None,
            "furnishing": None,
        }
