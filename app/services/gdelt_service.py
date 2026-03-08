# Built by Hamzy - ETS Montreal
# ConflictWatch - Global Conflict Intelligence Dashboard
# GDELT Project data ingestion service: 15-min CSV updates + DOC API queries

from __future__ import annotations

import asyncio
import csv
import io
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote_plus

import httpx
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from app.models.schemas import (
    ConflictEvent,
    ConflictEventType,
    SeverityLevel,
    _country_to_region,
)

# ---------------------------------------------------------------------------
# CAMEO event codes that represent conflict / violence
# 14x = protest  17x = coerce  18x = assault  19x = fight  20x = mass violence
# ---------------------------------------------------------------------------

CONFLICT_CAMEO_PREFIXES = {"14", "17", "18", "19", "20"}

# ---------------------------------------------------------------------------
# Country code (ISO 3166-1 alpha-3 / FIPS) -> common name
# Covers top ~80 conflict-prone countries
# ---------------------------------------------------------------------------

COUNTRY_CODE_MAP: Dict[str, str] = {
    # Middle East
    "IRN": "Iran", "IRQ": "Iraq", "SYR": "Syria", "LBN": "Lebanon",
    "ISR": "Israel", "PSE": "Palestine", "JOR": "Jordan", "SAU": "Saudi Arabia",
    "YEM": "Yemen", "OMN": "Oman", "ARE": "UAE", "KWT": "Kuwait",
    "BHR": "Bahrain", "QAT": "Qatar", "TUR": "Turkey", "EGY": "Egypt",
    # Europe
    "UKR": "Ukraine", "RUS": "Russia", "BLR": "Belarus", "POL": "Poland",
    "DEU": "Germany", "FRA": "France", "GBR": "UK", "SRB": "Serbia",
    "KOS": "Kosovo", "ARM": "Armenia", "AZE": "Azerbaijan", "GEO": "Georgia",
    # Asia
    "CHN": "China", "IND": "India", "PAK": "Pakistan", "AFG": "Afghanistan",
    "MMR": "Myanmar", "PRK": "North Korea", "KOR": "South Korea",
    "TWN": "Taiwan", "PHL": "Philippines", "BGD": "Bangladesh",
    "LKA": "Sri Lanka", "NPL": "Nepal", "KAZ": "Kazakhstan",
    "TJK": "Tajikistan", "KGZ": "Kyrgyzstan",
    # Africa
    "SDN": "Sudan", "SSD": "South Sudan", "ETH": "Ethiopia",
    "SOM": "Somalia", "NGA": "Nigeria", "MLI": "Mali",
    "BFA": "Burkina Faso", "NER": "Niger", "TCD": "Chad",
    "MOZ": "Mozambique", "COD": "DRC", "CAF": "Central African Republic",
    "CMR": "Cameroon", "LBY": "Libya",
    # Americas
    "COL": "Colombia", "VEN": "Venezuela", "MEX": "Mexico",
    "HTI": "Haiti", "PER": "Peru", "ECU": "Ecuador",
    "BRA": "Brazil", "SLV": "El Salvador", "HND": "Honduras",
    "USA": "United States", "CUB": "Cuba",
    # FIPS2 codes also used in GDELT
    "IR": "Iran", "IZ": "Iraq", "SY": "Syria", "LE": "Lebanon",
    "IS": "Israel", "WE": "Palestine", "JO": "Jordan", "SA": "Saudi Arabia",
    "YM": "Yemen", "TU": "Turkey", "EG": "Egypt", "UP": "Ukraine",
    "RS": "Russia", "BO": "Belarus", "CH": "China", "IN": "India",
    "PK": "Pakistan", "AF": "Afghanistan", "BM": "Myanmar",
    "KN": "North Korea", "KS": "South Korea", "TW": "Taiwan",
    "RP": "Philippines", "SU": "Sudan", "ET": "Ethiopia",
    "SO": "Somalia", "NI": "Nigeria", "ML": "Mali", "UV": "Burkina Faso",
    "NG": "Niger", "CD": "Chad", "MZ": "Mozambique", "CG": "DRC",
    "LY": "Libya", "CO": "Colombia", "VE": "Venezuela", "MX": "Mexico",
    "HA": "Haiti", "PE": "Peru", "EC": "Ecuador", "BR": "Brazil",
    "ES": "El Salvador", "HO": "Honduras", "US": "United States",
    "CU": "Cuba", "RU": "Russia", "UK": "UK",
}

# ---------------------------------------------------------------------------
# Static lat/lon for city lookup by closest country centroid
# (used when GDELT provides ActionGeo_Lat/Lon = 0 or missing)
# ---------------------------------------------------------------------------

COUNTRY_CENTROIDS: Dict[str, Tuple[float, float]] = {
    "Iran": (32.4279, 53.6880), "Iraq": (33.2232, 43.6793),
    "Syria": (34.8021, 38.9968), "Lebanon": (33.8547, 35.8623),
    "Israel": (31.0461, 34.8516), "Palestine": (31.9522, 35.2332),
    "Jordan": (30.5852, 36.2384), "Saudi Arabia": (23.8859, 45.0792),
    "Yemen": (15.5527, 48.5164), "UAE": (23.4241, 53.8478),
    "Turkey": (38.9637, 35.2433), "Egypt": (26.8206, 30.8025),
    "Ukraine": (48.3794, 31.1656), "Russia": (61.5240, 105.3188),
    "China": (35.8617, 104.1954), "India": (20.5937, 78.9629),
    "Pakistan": (30.3753, 69.3451), "Afghanistan": (33.9391, 67.7100),
    "Myanmar": (21.9162, 95.9560), "North Korea": (40.3399, 127.5101),
    "Sudan": (12.8628, 30.2176), "Ethiopia": (9.1450, 40.4897),
    "Somalia": (5.1521, 46.1996), "Nigeria": (9.0820, 8.6753),
    "Libya": (26.3351, 17.2283), "Mali": (17.5707, -3.9962),
    "Colombia": (4.5709, -74.2973), "Venezuela": (6.4238, -66.5897),
    "Mexico": (23.6345, -102.5528), "Ukraine": (48.3794, 31.1656),
    "Unknown": (0.0, 0.0),
}

GDELT_LASTUPDATE_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
GDELT_DOC_API_URL = "https://api.gdeltproject.org/api/v2/doc/doc"

# Default queries covering Middle East + global hotspots
DEFAULT_DOC_QUERIES = [
    "Iran military attack nuclear",
    "Israel Gaza airstrike",
    "Ukraine Russia war offensive",
    "Middle East conflict missile",
    "Syria airstrike bombing",
    "Yemen Houthi strike",
    "Sudan Ethiopia conflict",
    "North Korea missile launch",
]

_LAST_REQUEST_TIME: float = 0.0
_MIN_REQUEST_INTERVAL: float = 5.0  # 1 request per 5 seconds to GDELT


async def _rate_limited_sleep() -> None:
    global _LAST_REQUEST_TIME
    elapsed = time.monotonic() - _LAST_REQUEST_TIME
    if elapsed < _MIN_REQUEST_INTERVAL:
        await asyncio.sleep(_MIN_REQUEST_INTERVAL - elapsed)
    _LAST_REQUEST_TIME = time.monotonic()


class GDELTService:
    """
    Ingests conflict events from GDELT Project:
      - 15-minute CSV updates (GDELTv2 events file)
      - GDELT DOC API for keyword-based article discovery
    """

    def __init__(self) -> None:
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(30.0),
                follow_redirects=True,
                headers={"User-Agent": "ConflictWatch/1.0 (research; ETS Montreal)"},
            )
        return self._client

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    # ------------------------------------------------------------------
    # 15-minute CSV ingestion
    # ------------------------------------------------------------------

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
    async def fetch_latest_events(self) -> List[ConflictEvent]:
        """
        1. Fetch lastupdate.txt to get the URL of the latest 15-min export CSV.
        2. Download and parse the CSV.
        3. Filter rows by CAMEO conflict event codes.
        4. Return list of ConflictEvent objects.
        """
        client = await self._get_client()
        await _rate_limited_sleep()

        try:
            resp = await client.get(GDELT_LASTUPDATE_URL)
            resp.raise_for_status()
        except httpx.HTTPError as exc:
            logger.warning(f"GDELT lastupdate.txt fetch failed: {exc}")
            return []

        # lastupdate.txt has 3 lines: export.CSV.zip, mentions.CSV.zip, gkg.CSV.zip
        # We want the first line (events export)
        lines = resp.text.strip().split("\n")
        if not lines:
            logger.warning("GDELT lastupdate.txt was empty")
            return []

        # Format: "<size> <md5> <url>"
        parts = lines[0].strip().split(" ")
        if len(parts) < 3:
            logger.warning(f"Unexpected lastupdate.txt format: {lines[0]}")
            return []

        csv_url = parts[2].strip()
        logger.info(f"GDELT: fetching events CSV from {csv_url}")

        await _rate_limited_sleep()
        try:
            csv_resp = await client.get(csv_url)
            csv_resp.raise_for_status()
        except httpx.HTTPError as exc:
            logger.warning(f"GDELT CSV download failed: {exc}")
            return []

        # Decompress and parse
        import zipfile

        try:
            with zipfile.ZipFile(io.BytesIO(csv_resp.content)) as zf:
                names = zf.namelist()
                if not names:
                    return []
                csv_bytes = zf.read(names[0])
        except zipfile.BadZipFile:
            # Some mirrors serve plain CSV
            csv_bytes = csv_resp.content

        return self._parse_gdelt_csv(csv_bytes)

    def _parse_gdelt_csv(self, csv_bytes: bytes) -> List[ConflictEvent]:
        """Parse raw GDELT v2 events CSV bytes into ConflictEvent objects."""
        events: List[ConflictEvent] = []
        try:
            content = csv_bytes.decode("utf-8", errors="replace")
            reader = csv.reader(io.StringIO(content), delimiter="\t")
        except Exception as exc:
            logger.error(f"GDELT CSV decode error: {exc}")
            return []

        for row in reader:
            try:
                event = self._parse_gdelt_row(row)
                if event:
                    events.append(event)
            except Exception as exc:
                logger.debug(f"GDELT row parse error: {exc}")
                continue

        logger.info(f"GDELT CSV: parsed {len(events)} conflict events")
        return events

    def _parse_gdelt_row(self, row: List[str]) -> Optional[ConflictEvent]:
        """
        GDELTv2 CSV columns (tab-separated, 61 columns):
        Col 0:  GLOBALEVENTID
        Col 5:  EventCode (CAMEO)
        Col 26: Actor1CountryCode
        Col 37: ActionGeo_CountryCode
        Col 39: ActionGeo_Lat
        Col 40: ActionGeo_Long
        Col 53: SOURCEURL
        Col 1:  SQLDATE (YYYYMMDD)
        Col 27: Actor1Name
        """
        if len(row) < 58:
            return None

        event_code = row[28].strip() if len(row) > 28 else ""
        if not event_code:
            return None

        # Filter: only conflict CAMEO codes
        prefix_2 = event_code[:2]
        prefix_3 = event_code[:3]
        if not any(event_code.startswith(p) for p in CONFLICT_CAMEO_PREFIXES):
            return None

        global_event_id = row[0].strip()
        source_url = row[57].strip() if len(row) > 57 else ""

        # Date
        sql_date = row[1].strip()
        try:
            published_at = datetime.strptime(sql_date, "%Y%m%d").replace(
                tzinfo=timezone.utc
            )
        except ValueError:
            published_at = datetime.utcnow().replace(tzinfo=timezone.utc)

        # Geography
        country_code = row[37].strip() if len(row) > 37 else ""
        country_name = COUNTRY_CODE_MAP.get(
            country_code, COUNTRY_CODE_MAP.get(country_code.upper(), "Unknown")
        )

        try:
            lat = float(row[39]) if row[39].strip() else None
        except (ValueError, IndexError):
            lat = None
        try:
            lon = float(row[40]) if len(row) > 40 and row[40].strip() else None
        except (ValueError, IndexError):
            lon = None

        # Fall back to country centroid
        if lat is None or lon is None or (lat == 0.0 and lon == 0.0):
            centroid = COUNTRY_CENTROIDS.get(country_name, (0.0, 0.0))
            lat, lon = centroid

        # Actor names
        actor1 = row[6].strip() if len(row) > 6 else ""
        actor2 = row[16].strip() if len(row) > 16 else ""
        entities = [e for e in [actor1, actor2] if e and len(e) > 2]

        # Build title from available data
        geo_name = row[38].strip() if len(row) > 38 else ""
        title = _build_title_from_cameo(event_code, country_name, actor1, geo_name)

        event_type = _cameo_to_event_type(event_code)
        severity = _cameo_to_severity(event_code)

        region = _country_to_region(country_name)
        city = _resolve_city(lat, lon, country_name, geo_name)

        return ConflictEvent(
            id=f"gdelt-{global_event_id}",
            title=title,
            description=f"GDELT Event {global_event_id} | CAMEO: {event_code} | "
                        f"Actor1: {actor1} | Actor2: {actor2} | Geo: {geo_name}",
            event_type=event_type,
            severity=severity,
            country=country_name,
            region=region,
            city=city,
            lat=lat,
            lon=lon,
            source_url=source_url or f"https://www.gdeltproject.org/events/{global_event_id}",
            source_name="GDELT Project",
            published_at=published_at,
            entities=entities,
            keywords=[event_code, country_name],
        )

    # ------------------------------------------------------------------
    # GDELT DOC API
    # ------------------------------------------------------------------

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=5, max=30))
    async def fetch_gdelt_doc_api(
        self,
        query: str,
        timespan: str = "1d",
        max_records: int = 250,
    ) -> List[ConflictEvent]:
        """
        Query GDELT DOC API for news articles matching a keyword query.
        Returns articles parsed into ConflictEvent objects.
        """
        client = await self._get_client()
        await _rate_limited_sleep()

        params = {
            "query": query,
            "mode": "artlist",
            "maxrecords": str(max_records),
            "timespan": timespan,
            "format": "json",
            "sort": "DateDesc",
        }

        try:
            resp = await client.get(GDELT_DOC_API_URL, params=params)
            resp.raise_for_status()
            data = resp.json()
        except httpx.HTTPError as exc:
            logger.warning(f"GDELT DOC API error for query '{query}': {exc}")
            return []
        except ValueError:
            logger.warning(f"GDELT DOC API returned non-JSON for query '{query}'")
            return []

        articles = data.get("articles", [])
        if not articles:
            return []

        events: List[ConflictEvent] = []
        for art in articles:
            event = self._parse_doc_article(art, query)
            if event:
                events.append(event)

        logger.info(f"GDELT DOC API '{query}': {len(events)} articles parsed")
        return events

    def _parse_doc_article(self, art: dict, query: str) -> Optional[ConflictEvent]:
        """Parse a GDELT DOC API article dict into a ConflictEvent."""
        title = art.get("title", "").strip()
        if not title or len(title) < 5:
            return None

        url = art.get("url", "")
        source_name = art.get("domain", "Unknown")
        seendate = art.get("seendate", "")

        try:
            published_at = datetime.strptime(seendate[:14], "%Y%m%dT%H%M%S").replace(
                tzinfo=timezone.utc
            )
        except (ValueError, TypeError):
            published_at = datetime.utcnow().replace(tzinfo=timezone.utc)

        # Extract country from query hint
        country = _extract_country_from_text(query + " " + title)
        region = _country_to_region(country)
        lat, lon = COUNTRY_CENTROIDS.get(country, (0.0, 0.0))

        from app.models.schemas import ConflictEventType, SeverityLevel
        event_type = _classify_event_type_from_title(title)
        severity = _assess_severity_from_title(title)

        return ConflictEvent(
            title=title,
            description=f"Source: {source_name} | Query: {query}",
            event_type=event_type,
            severity=severity,
            country=country,
            region=region,
            lat=lat if lat != 0.0 else None,
            lon=lon if lon != 0.0 else None,
            source_url=url,
            source_name=source_name,
            published_at=published_at,
            keywords=query.split(),
        )

    async def fetch_all_default_queries(self) -> List[ConflictEvent]:
        """Run all DEFAULT_DOC_QUERIES and return combined results."""
        all_events: List[ConflictEvent] = []
        for query in DEFAULT_DOC_QUERIES:
            try:
                events = await self.fetch_gdelt_doc_api(query, timespan="1d", max_records=50)
                all_events.extend(events)
            except Exception as exc:
                logger.warning(f"GDELT DOC query '{query}' failed: {exc}")
        logger.info(f"GDELT DOC API: {len(all_events)} total events across all queries")
        return all_events


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def _cameo_to_event_type(cameo: str) -> ConflictEventType:
    """Map CAMEO event code prefix to ConflictEventType."""
    if cameo.startswith("14"):
        return ConflictEventType.PROTEST
    if cameo.startswith("172") or cameo.startswith("173"):
        return ConflictEventType.SANCTIONS
    if cameo.startswith("17"):
        return ConflictEventType.DIPLOMATIC
    if cameo.startswith("180") or cameo.startswith("181"):
        return ConflictEventType.GROUND_OPERATION
    if cameo.startswith("182") or cameo.startswith("190"):
        return ConflictEventType.AIRSTRIKE
    if cameo.startswith("195") or cameo.startswith("196"):
        return ConflictEventType.MISSILE_ATTACK
    if cameo.startswith("18"):
        return ConflictEventType.GROUND_OPERATION
    if cameo.startswith("19"):
        return ConflictEventType.AIRSTRIKE
    if cameo.startswith("20"):
        return ConflictEventType.GROUND_OPERATION
    return ConflictEventType.UNKNOWN


def _cameo_to_severity(cameo: str) -> SeverityLevel:
    """Map CAMEO code to a severity level based on violence escalation."""
    code_num = cameo[:3]
    critical_codes = {"200", "201", "202", "203", "204"}
    high_codes = {"190", "191", "192", "193", "194", "195", "196"}
    medium_codes = {"180", "181", "182", "183", "184", "185", "186"}
    if code_num in critical_codes:
        return SeverityLevel.CRITICAL
    if code_num in high_codes or cameo.startswith("20"):
        return SeverityLevel.HIGH
    if code_num in medium_codes or cameo.startswith("19"):
        return SeverityLevel.HIGH
    if cameo.startswith("18"):
        return SeverityLevel.MEDIUM
    if cameo.startswith("17"):
        return SeverityLevel.MEDIUM
    return SeverityLevel.LOW


def _build_title_from_cameo(
    cameo: str, country: str, actor: str, geo: str
) -> str:
    """Construct a human-readable title from CAMEO code and geo info."""
    action_map = {
        "14": "Protest in",
        "170": "Coercive action in",
        "171": "Sanctions imposed on",
        "172": "Blockade imposed on",
        "173": "Occupation in",
        "174": "Assassination in",
        "175": "Threats against",
        "180": "Ground assault in",
        "181": "Armed attack in",
        "182": "Bombing/airstrike in",
        "183": "Violent assault in",
        "190": "Use of force in",
        "191": "Shooting in",
        "192": "Bombing in",
        "193": "Military operation in",
        "194": "Artillery fire in",
        "195": "Missile attack in",
        "196": "Drone strike in",
        "200": "Mass atrocity in",
        "201": "Massacre in",
        "202": "Ethnic cleansing in",
        "203": "Genocide in",
        "204": "Nuclear/chemical attack in",
    }
    for prefix, desc in action_map.items():
        if cameo.startswith(prefix):
            location = geo if geo else country
            actor_str = f" by {actor}" if actor else ""
            return f"{desc} {location}{actor_str}"

    return f"Conflict event ({cameo}) in {country}"


def _extract_country_from_text(text: str) -> str:
    """Extract the first recognized country name from free text."""
    text_lower = text.lower()
    # Ordered by priority (Middle East first for this dashboard)
    priority_order = [
        "Iran", "Israel", "Gaza", "Lebanon", "Syria", "Iraq", "Yemen",
        "Palestine", "Saudi Arabia", "Jordan", "Turkey", "Egypt",
        "Ukraine", "Russia", "China", "India", "Pakistan", "Afghanistan",
        "North Korea", "Sudan", "Ethiopia", "Somalia", "Nigeria",
        "Libya", "Mali", "Colombia", "Venezuela", "Myanmar",
    ]
    for country in priority_order:
        if country.lower() in text_lower:
            return country
    return "Unknown"


def _classify_event_type_from_title(title: str) -> ConflictEventType:
    t = title.lower()
    if any(w in t for w in ["airstrike", "air strike", "bombing", "warplane", "jet", "f-16"]):
        return ConflictEventType.AIRSTRIKE
    if any(w in t for w in ["missile", "rocket", "ballistic", "cruise missile"]):
        return ConflictEventType.MISSILE_ATTACK
    if any(w in t for w in ["troops", "ground", "infantry", "tank", "armored", "soldiers"]):
        return ConflictEventType.GROUND_OPERATION
    if any(w in t for w in ["navy", "ship", "vessel", "maritime", "port"]):
        return ConflictEventType.NAVAL
    if any(w in t for w in ["cyber", "hack", "malware"]):
        return ConflictEventType.CYBER
    if any(w in t for w in ["protest", "demonstration", "rally", "march"]):
        return ConflictEventType.PROTEST
    if any(w in t for w in ["ceasefire", "truce", "peace deal"]):
        return ConflictEventType.CEASEFIRE
    if any(w in t for w in ["sanctions", "embargo", "ban", "restrict"]):
        return ConflictEventType.SANCTIONS
    if any(w in t for w in ["negotiations", "talks", "envoy", "diplomatic"]):
        return ConflictEventType.DIPLOMATIC
    return ConflictEventType.UNKNOWN


def _assess_severity_from_title(title: str) -> SeverityLevel:
    t = title.lower()
    if any(w in t for w in ["nuclear", "chemical", "massacre", "genocide", "hundreds killed", "mass"]):
        return SeverityLevel.CRITICAL
    if any(w in t for w in ["killed", "dead", "casualties", "wounded", "destroyed", "capital"]):
        return SeverityLevel.HIGH
    if any(w in t for w in ["attack", "strike", "clash", "fighting", "assault", "fire"]):
        return SeverityLevel.MEDIUM
    return SeverityLevel.LOW


def _resolve_city(
    lat: Optional[float],
    lon: Optional[float],
    country: str,
    geo_name: str,
) -> str:
    """Best-effort city name from GDELT geo field or country lookup."""
    if geo_name and len(geo_name) > 2:
        # GDELT geo_name is often "City, Country" or just "Country"
        parts = geo_name.split(",")
        if len(parts) >= 2:
            return parts[0].strip()
        if parts[0].strip() != country:
            return parts[0].strip()
    # Return capital city as fallback
    capitals = {
        "Iran": "Tehran", "Iraq": "Baghdad", "Syria": "Damascus",
        "Lebanon": "Beirut", "Israel": "Jerusalem", "Palestine": "Ramallah",
        "Jordan": "Amman", "Saudi Arabia": "Riyadh", "Yemen": "Sanaa",
        "Turkey": "Ankara", "Egypt": "Cairo", "Ukraine": "Kyiv",
        "Russia": "Moscow", "China": "Beijing", "India": "New Delhi",
        "Pakistan": "Islamabad", "Afghanistan": "Kabul",
        "North Korea": "Pyongyang", "Sudan": "Khartoum",
        "Ethiopia": "Addis Ababa", "Somalia": "Mogadishu",
        "Nigeria": "Abuja", "Libya": "Tripoli",
    }
    return capitals.get(country, "")
