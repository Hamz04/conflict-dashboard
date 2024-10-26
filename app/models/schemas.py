# Built by Hamza Ahmad - ETS Montreal
# ConflictWatch - Global Conflict Intelligence Dashboard
# Pydantic v2 data models for all conflict events and intelligence objects

from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------

class ConflictEventType(str, Enum):
    AIRSTRIKE = "AIRSTRIKE"
    MISSILE_ATTACK = "MISSILE_ATTACK"
    GROUND_OPERATION = "GROUND_OPERATION"
    NAVAL = "NAVAL"
    CYBER = "CYBER"
    PROTEST = "PROTEST"
    CEASEFIRE = "CEASEFIRE"
    DIPLOMATIC = "DIPLOMATIC"
    SANCTIONS = "SANCTIONS"
    UNKNOWN = "UNKNOWN"


class SeverityLevel(str, Enum):
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


class TrendDirection(str, Enum):
    ESCALATING = "ESCALATING"
    DE_ESCALATING = "DE-ESCALATING"
    STABLE = "STABLE"


class RegionFilter(str, Enum):
    MIDDLE_EAST = "MIDDLE_EAST"
    EUROPE = "EUROPE"
    ASIA = "ASIA"
    AFRICA = "AFRICA"
    AMERICAS = "AMERICAS"
    ALL = "ALL"


class ThreatLevel(str, Enum):
    LOW = "LOW"
    MODERATE = "MODERATE"
    ELEVATED = "ELEVATED"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


# ---------------------------------------------------------------------------
# Core event model
# ---------------------------------------------------------------------------

class ConflictEvent(BaseModel):
    """
    Represents a single parsed conflict or geopolitical event.
    Sourced from GDELT CSVs, RSS feeds, or manual entry.
    """

    model_config = {"from_attributes": True}

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    title: str = Field(..., min_length=5, max_length=1024)
    description: str = Field(default="", max_length=8192)
    event_type: ConflictEventType = Field(default=ConflictEventType.UNKNOWN)
    severity: SeverityLevel = Field(default=SeverityLevel.MEDIUM)

    # Geographic
    country: str = Field(default="Unknown")
    region: str = Field(default="Unknown")
    city: str = Field(default="")
    lat: Optional[float] = Field(default=None, ge=-90.0, le=90.0)
    lon: Optional[float] = Field(default=None, ge=-180.0, le=180.0)

    # Source metadata
    source_url: str = Field(default="")
    source_name: str = Field(default="")

    # Timestamps
    published_at: datetime = Field(default_factory=datetime.utcnow)
    ingested_at: datetime = Field(default_factory=datetime.utcnow)

    # NLP outputs
    entities: List[str] = Field(default_factory=list)
    keywords: List[str] = Field(default_factory=list)
    sentiment_score: float = Field(default=0.0, ge=-1.0, le=1.0)
    escalation_score: float = Field(default=0.0, ge=0.0, le=1.0)

    # Quality flag
    verified: bool = Field(default=False)

    @field_validator("entities", "keywords", mode="before")
    @classmethod
    def ensure_list(cls, v):
        if v is None:
            return []
        if isinstance(v, str):
            import json
            try:
                parsed = json.loads(v)
                return parsed if isinstance(parsed, list) else [v]
            except (json.JSONDecodeError, ValueError):
                return [v] if v else []
        return v

    @field_validator("lat", "lon", mode="before")
    @classmethod
    def coerce_float_or_none(cls, v):
        if v is None or v == "" or v != v:  # NaN check
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    @field_validator("sentiment_score", "escalation_score", mode="before")
    @classmethod
    def clamp_score(cls, v):
        try:
            return float(v)
        except (TypeError, ValueError):
            return 0.0

    @model_validator(mode="after")
    def set_region_from_country(self) -> "ConflictEvent":
        if self.region == "Unknown" and self.country != "Unknown":
            self.region = _country_to_region(self.country)
        return self

    def severity_weight(self) -> float:
        weights = {
            SeverityLevel.CRITICAL: 10.0,
            SeverityLevel.HIGH: 5.0,
            SeverityLevel.MEDIUM: 2.0,
            SeverityLevel.LOW: 0.5,
        }
        return weights.get(self.severity, 1.0)

    def to_map_point(self) -> dict:
        """Returns a minimal dict suitable for pydeck ScatterplotLayer."""
        return {
            "id": self.id,
            "lat": self.lat or 0.0,
            "lon": self.lon or 0.0,
            "title": self.title,
            "country": self.country,
            "severity": self.severity.value,
            "event_type": self.event_type.value,
            "escalation_score": self.escalation_score,
        }


# ---------------------------------------------------------------------------
# Aggregated summary models
# ---------------------------------------------------------------------------

class ConflictSummary(BaseModel):
    """Per-region aggregated summary for the last 24h and 7d."""

    region: str
    event_count_24h: int = Field(default=0, ge=0)
    event_count_7d: int = Field(default=0, ge=0)
    dominant_type: ConflictEventType = Field(default=ConflictEventType.UNKNOWN)
    avg_severity: float = Field(default=0.0, ge=0.0)
    trend: TrendDirection = Field(default=TrendDirection.STABLE)
    top_countries: List[str] = Field(default_factory=list)
    last_event_at: Optional[datetime] = None


class GlobalThreatIndex(BaseModel):
    """
    Composite 0-100 threat index computed from recent events.
    Score formula:
        sum(CRITICAL*10 + HIGH*5 + MEDIUM*2 + LOW*0.5)
        normalised and capped at 100.
    """

    score: float = Field(..., ge=0.0, le=100.0)
    level: ThreatLevel
    top_hotspots: List[str] = Field(default_factory=list)
    event_counts: dict = Field(default_factory=dict)
    computed_at: datetime = Field(default_factory=datetime.utcnow)
    trend_24h: float = Field(
        default=0.0,
        description="Delta vs previous 24h score. Positive = escalating.",
    )

    @field_validator("level", mode="before")
    @classmethod
    def derive_level(cls, v, info):
        """Allow level to be derived from score if not supplied explicitly."""
        if isinstance(v, ThreatLevel):
            return v
        # If called as string literal pass-through:
        if isinstance(v, str):
            return ThreatLevel(v)
        return v

    @classmethod
    def from_score(cls, score: float, top_hotspots: List[str], event_counts: dict, trend_24h: float = 0.0) -> "GlobalThreatIndex":
        if score < 20:
            level = ThreatLevel.LOW
        elif score < 40:
            level = ThreatLevel.MODERATE
        elif score < 60:
            level = ThreatLevel.ELEVATED
        elif score < 80:
            level = ThreatLevel.HIGH
        else:
            level = ThreatLevel.CRITICAL
        return cls(
            score=round(score, 2),
            level=level,
            top_hotspots=top_hotspots,
            event_counts=event_counts,
            trend_24h=round(trend_24h, 2),
        )


# ---------------------------------------------------------------------------
# API request / response wrappers
# ---------------------------------------------------------------------------

class EventQueryParams(BaseModel):
    region: RegionFilter = RegionFilter.ALL
    severity: Optional[SeverityLevel] = None
    event_type: Optional[ConflictEventType] = None
    hours: int = Field(default=24, ge=1, le=720)
    limit: int = Field(default=100, ge=1, le=1000)
    offset: int = Field(default=0, ge=0)


class SearchQuery(BaseModel):
    q: str = Field(..., min_length=2, max_length=256)
    limit: int = Field(default=50, ge=1, le=500)


class HotspotEntry(BaseModel):
    country: str
    region: str
    event_count_24h: int
    dominant_severity: SeverityLevel
    dominant_type: ConflictEventType
    lat: Optional[float] = None
    lon: Optional[float] = None


class TimelineEntry(BaseModel):
    date: str  # YYYY-MM-DD
    total: int
    by_type: dict  # {ConflictEventType: count}
    avg_escalation: float


class IngestionStats(BaseModel):
    total_events: int
    events_24h: int
    events_7d: int
    last_ingestion_at: Optional[datetime]
    last_gdelt_update: Optional[datetime]
    last_rss_update: Optional[datetime]
    sources_active: List[str]
    duplicates_skipped_total: int


# ---------------------------------------------------------------------------
# Helper: country -> region mapping
# ---------------------------------------------------------------------------

_COUNTRY_REGION_MAP: dict[str, str] = {
    # Middle East
    "Iran": "MIDDLE_EAST", "Iraq": "MIDDLE_EAST", "Syria": "MIDDLE_EAST",
    "Lebanon": "MIDDLE_EAST", "Israel": "MIDDLE_EAST", "Palestine": "MIDDLE_EAST",
    "Gaza": "MIDDLE_EAST", "West Bank": "MIDDLE_EAST", "Jordan": "MIDDLE_EAST",
    "Saudi Arabia": "MIDDLE_EAST", "Yemen": "MIDDLE_EAST", "Oman": "MIDDLE_EAST",
    "UAE": "MIDDLE_EAST", "Kuwait": "MIDDLE_EAST", "Bahrain": "MIDDLE_EAST",
    "Qatar": "MIDDLE_EAST", "Turkey": "MIDDLE_EAST", "Egypt": "MIDDLE_EAST",
    "Libya": "AFRICA",
    # Europe
    "Ukraine": "EUROPE", "Russia": "EUROPE", "Belarus": "EUROPE",
    "Poland": "EUROPE", "Germany": "EUROPE", "France": "EUROPE",
    "UK": "EUROPE", "Serbia": "EUROPE", "Kosovo": "EUROPE",
    "Armenia": "EUROPE", "Azerbaijan": "EUROPE", "Georgia": "EUROPE",
    # Asia
    "China": "ASIA", "India": "ASIA", "Pakistan": "ASIA",
    "Afghanistan": "ASIA", "Myanmar": "ASIA", "North Korea": "ASIA",
    "South Korea": "ASIA", "Taiwan": "ASIA", "Philippines": "ASIA",
    "Bangladesh": "ASIA", "Sri Lanka": "ASIA", "Nepal": "ASIA",
    "Kazakhstan": "ASIA", "Tajikistan": "ASIA", "Kyrgyzstan": "ASIA",
    # Africa
    "Sudan": "AFRICA", "South Sudan": "AFRICA", "Ethiopia": "AFRICA",
    "Somalia": "AFRICA", "Nigeria": "AFRICA", "Mali": "AFRICA",
    "Burkina Faso": "AFRICA", "Niger": "AFRICA", "Chad": "AFRICA",
    "Mozambique": "AFRICA", "DRC": "AFRICA", "Congo": "AFRICA",
    "Central African Republic": "AFRICA", "Cameroon": "AFRICA",
    # Americas
    "Colombia": "AMERICAS", "Venezuela": "AMERICAS", "Mexico": "AMERICAS",
    "Haiti": "AMERICAS", "Peru": "AMERICAS", "Ecuador": "AMERICAS",
    "Brazil": "AMERICAS", "El Salvador": "AMERICAS", "Honduras": "AMERICAS",
    "United States": "AMERICAS", "Cuba": "AMERICAS",
}


def _country_to_region(country: str) -> str:
    """Return the region string for a given country name."""
    return _COUNTRY_REGION_MAP.get(country, "Unknown")
