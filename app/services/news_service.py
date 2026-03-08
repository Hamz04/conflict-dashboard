# Built by Hamzy - ETS Montreal
# ConflictWatch - Global Conflict Intelligence Dashboard
# Multi-source RSS news ingestion service with conflict keyword filtering

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from email.utils import parsedate_to_datetime

import feedparser
import httpx
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from app.models.schemas import (
    ConflictEvent,
    ConflictEventType,
    SeverityLevel,
    _country_to_region,
)
from app.services.gdelt_service import (
    COUNTRY_CENTROIDS,
    _assess_severity_from_title,
    _classify_event_type_from_title,
    _extract_country_from_text,
)

# ---------------------------------------------------------------------------
# RSS feed sources — all free, no API key required
# ---------------------------------------------------------------------------

RSS_FEEDS: Dict[str, str] = {
    "Reuters World": "https://feeds.reuters.com/reuters/worldNews",
    "BBC World": "http://feeds.bbci.co.uk/news/world/rss.xml",
    "Al Jazeera": "https://www.aljazeera.com/xml/rss/all.xml",
    "AP News": "https://rsshub.app/apnews/topics/world-news",
    "Times of Israel": "https://www.timesofisrael.com/feed/",
    "Iran International": "https://www.iranintl.com/en/rss",
}

# Fallback feeds if primary ones fail
FALLBACK_FEEDS: Dict[str, str] = {
    "Guardian World": "https://www.theguardian.com/world/rss",
    "DW World": "https://rss.dw.com/rdf/rss-en-world",
    "France24 Middle East": "https://www.france24.com/en/middle-east/rss",
    "Jerusalem Post": "https://www.jpost.com/rss/rssfeedsheadlines.aspx",
}

# ---------------------------------------------------------------------------
# Conflict keyword filter
# ---------------------------------------------------------------------------

CONFLICT_KEYWORDS = [
    "war", "attack", "strike", "missile", "bomb", "killed", "troops",
    "military", "conflict", "invasion", "ceasefire", "sanctions", "nuclear",
    "drone", "explosion", "casualties", "offensive", "airstrike", "rocket",
    "artillery", "hostage", "siege", "blockade", "escalation", "assault",
    "shelling", "gunfire", "ambush", "raid", "killing", "massacre", "terror",
    "extremist", "militant", "insurgent", "rebel", "occupation", "resistance",
    "warplane", "fighter jet", "naval", "submarine", "warship", "ballistic",
    "chemical weapon", "biological", "cyber attack", "sabotage", "assassination",
    "coup", "uprising", "crackdown", "detention", "disappeared", "execut",
    "genocide", "ethnic cleansing", "refugee", "displacement", "humanitarian",
    "iran", "israel", "gaza", "hezbollah", "hamas", "houthi", "ukraine",
    "russia", "nato", "idf", "irgc", "islamic state", "isis", "al-qaeda",
]

# Country name patterns for extraction from article text
COUNTRY_PATTERNS = [
    "Iran", "Iraq", "Syria", "Lebanon", "Israel", "Palestine", "Gaza",
    "Jordan", "Saudi Arabia", "Yemen", "Oman", "UAE", "Kuwait", "Bahrain",
    "Qatar", "Turkey", "Egypt", "Libya", "Ukraine", "Russia", "Belarus",
    "Poland", "Germany", "France", "United Kingdom", "UK", "Serbia", "Kosovo",
    "Armenia", "Azerbaijan", "Georgia", "China", "India", "Pakistan",
    "Afghanistan", "Myanmar", "North Korea", "South Korea", "Taiwan",
    "Philippines", "Bangladesh", "Sri Lanka", "Nepal", "Kazakhstan",
    "Sudan", "South Sudan", "Ethiopia", "Somalia", "Nigeria", "Mali",
    "Burkina Faso", "Niger", "Chad", "Mozambique", "Congo", "DRC",
    "Central African Republic", "Cameroon", "Colombia", "Venezuela",
    "Mexico", "Haiti", "Peru", "Ecuador", "Brazil", "El Salvador",
    "Honduras", "United States", "Cuba",
]

_COUNTRY_REGEX = re.compile(
    r"\b(" + "|".join(re.escape(c) for c in sorted(COUNTRY_PATTERNS, key=len, reverse=True)) + r")\b",
    re.IGNORECASE,
)


def _is_conflict_article(title: str, summary: str) -> bool:
    """Return True if any conflict keyword appears in title or summary."""
    combined = (title + " " + summary).lower()
    return any(kw in combined for kw in CONFLICT_KEYWORDS)


def _extract_country_from_article(title: str, summary: str) -> str:
    """Extract the first country name from title then summary."""
    for text_chunk in [title, summary]:
        match = _COUNTRY_REGEX.search(text_chunk)
        if match:
            # Normalise casing
            found = match.group(0)
            for canonical in COUNTRY_PATTERNS:
                if canonical.lower() == found.lower():
                    return canonical
            return found
    return "Unknown"


def _parse_date(entry) -> datetime:
    """Parse feedparser entry published date into a UTC datetime."""
    # Try feedparser's parsed date tuple
    if hasattr(entry, "published_parsed") and entry.published_parsed:
        try:
            return datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
        except Exception:
            pass
    # Try raw published string
    if hasattr(entry, "published") and entry.published:
        try:
            return parsedate_to_datetime(entry.published).astimezone(timezone.utc).replace(tzinfo=timezone.utc)
        except Exception:
            pass
    # Try updated
    if hasattr(entry, "updated_parsed") and entry.updated_parsed:
        try:
            return datetime(*entry.updated_parsed[:6], tzinfo=timezone.utc)
        except Exception:
            pass
    return datetime.utcnow().replace(tzinfo=timezone.utc)


def _title_fingerprint(title: str) -> str:
    """Return a normalised fingerprint of a title for dedup."""
    normalised = re.sub(r"[^a-z0-9 ]", "", title.lower())
    tokens = sorted(normalised.split())
    return " ".join(tokens[:8])  # first 8 tokens, sorted


class NewsService:
    """
    Aggregates conflict news from multiple RSS feeds.
    Performs keyword filtering, country extraction, and deduplication.
    """

    def __init__(self) -> None:
        self._client: Optional[httpx.AsyncClient] = None
        self._seen_fingerprints: set[str] = set()

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(20.0),
                follow_redirects=True,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (compatible; ConflictWatch/1.0; "
                        "+https://github.com/Hamz04)"
                    )
                },
            )
        return self._client

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    # ------------------------------------------------------------------
    # Single feed parser
    # ------------------------------------------------------------------

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=15))
    async def parse_feed(self, source_name: str, url: str) -> List[ConflictEvent]:
        """
        Fetch and parse a single RSS feed URL.
        Returns filtered ConflictEvent objects.
        """
        client = await self._get_client()

        try:
            resp = await client.get(url)
            resp.raise_for_status()
            raw_content = resp.content
        except httpx.HTTPError as exc:
            logger.warning(f"RSS fetch failed [{source_name}]: {exc}")
            return []

        try:
            feed = feedparser.parse(raw_content)
        except Exception as exc:
            logger.warning(f"RSS parse error [{source_name}]: {exc}")
            return []

        if not feed.entries:
            logger.debug(f"RSS feed empty [{source_name}] — {url}")
            return []

        events: List[ConflictEvent] = []
        for entry in feed.entries:
            try:
                event = self._parse_entry(entry, source_name, url)
                if event:
                    events.append(event)
            except Exception as exc:
                logger.debug(f"RSS entry parse error [{source_name}]: {exc}")
                continue

        logger.info(f"RSS [{source_name}]: {len(events)} conflict events parsed from {len(feed.entries)} entries")
        return events

    def _parse_entry(self, entry, source_name: str, feed_url: str) -> Optional[ConflictEvent]:
        """Convert a feedparser entry into a ConflictEvent, or None if not conflict-related."""
        title = getattr(entry, "title", "").strip()
        if not title or len(title) < 5:
            return None

        # Summary / description
        summary = ""
        if hasattr(entry, "summary"):
            summary = re.sub(r"<[^>]+>", " ", entry.summary).strip()
        elif hasattr(entry, "description"):
            summary = re.sub(r"<[^>]+>", " ", entry.description).strip()

        # Conflict keyword filter
        if not _is_conflict_article(title, summary):
            return None

        # Dedup by title fingerprint (within this ingestion cycle)
        fp = _title_fingerprint(title)
        if fp in self._seen_fingerprints:
            return None
        self._seen_fingerprints.add(fp)

        # Source URL
        link = getattr(entry, "link", "").strip()
        if not link:
            link = getattr(entry, "id", "").strip()

        published_at = _parse_date(entry)

        # Geography
        country = _extract_country_from_article(title, summary)
        region = _country_to_region(country)
        lat, lon = COUNTRY_CENTROIDS.get(country, (None, None))

        # NLP light classification
        full_text = title + " " + summary
        event_type = _classify_event_type_from_title(full_text)
        severity = _assess_severity_from_title(full_text)

        # Extract basic keywords from title
        keywords = _extract_keywords_from_text(title + " " + summary)

        return ConflictEvent(
            title=title,
            description=summary[:2000] if summary else "",
            event_type=event_type,
            severity=severity,
            country=country,
            region=region,
            lat=lat,
            lon=lon,
            source_url=link,
            source_name=source_name,
            published_at=published_at,
            keywords=keywords,
        )

    # ------------------------------------------------------------------
    # Aggregate from all feeds
    # ------------------------------------------------------------------

    async def fetch_all(self, include_fallbacks: bool = False) -> List[ConflictEvent]:
        """
        Fetch from all primary RSS feeds (and optionally fallbacks).
        Deduplicates across all sources by title fingerprint.
        Returns deduplicated list sorted by published_at descending.
        """
        import asyncio

        # Reset dedup set per full cycle
        self._seen_fingerprints.clear()

        feeds_to_fetch = dict(RSS_FEEDS)
        if include_fallbacks:
            feeds_to_fetch.update(FALLBACK_FEEDS)

        tasks = [
            self.parse_feed(name, url)
            for name, url in feeds_to_fetch.items()
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        all_events: List[ConflictEvent] = []
        for i, result in enumerate(results):
            source_name = list(feeds_to_fetch.keys())[i]
            if isinstance(result, Exception):
                logger.warning(f"RSS feed failed [{source_name}]: {result}")
                continue
            all_events.extend(result)

        # Global dedup by source_url
        seen_urls: set[str] = set()
        deduped: List[ConflictEvent] = []
        for event in all_events:
            if event.source_url and event.source_url in seen_urls:
                continue
            if event.source_url:
                seen_urls.add(event.source_url)
            deduped.append(event)

        # Sort newest first
        deduped.sort(key=lambda e: e.published_at, reverse=True)

        logger.info(
            f"RSS aggregate: {len(all_events)} total → {len(deduped)} after dedup "
            f"(from {len(feeds_to_fetch)} feeds)"
        )
        return deduped

    async def fetch_middle_east_focus(self) -> List[ConflictEvent]:
        """
        Fetch specifically from Middle East / Iran-focused sources and
        filter for relevant countries.
        """
        all_events = await self.fetch_all(include_fallbacks=True)
        me_countries = {
            "Iran", "Israel", "Gaza", "Lebanon", "Syria", "Iraq", "Yemen",
            "Palestine", "Saudi Arabia", "Jordan", "UAE", "Turkey", "Egypt",
        }
        me_events = [e for e in all_events if e.country in me_countries]
        logger.info(f"Middle East focus: {len(me_events)} events")
        return me_events


# ---------------------------------------------------------------------------
# Keyword extraction helper
# ---------------------------------------------------------------------------

def _extract_keywords_from_text(text: str) -> List[str]:
    """
    Extract conflict-relevant keywords that appear in the text.
    Returns up to 10 matching CONFLICT_KEYWORDS.
    """
    text_lower = text.lower()
    found = []
    for kw in CONFLICT_KEYWORDS:
        if kw in text_lower and kw not in found:
            found.append(kw)
            if len(found) >= 10:
                break
    return found
