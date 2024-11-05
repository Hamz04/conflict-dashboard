# Built by Hamza Ahmad - ETS Montreal
# ConflictWatch - Global Conflict Intelligence Dashboard
# NLP pipeline: event classification, severity assessment, entity extraction,
# sentiment scoring, escalation scoring, and Global Threat Index computation

from __future__ import annotations

import math
import re
from collections import Counter
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from loguru import logger

from app.models.schemas import (
    ConflictEvent,
    ConflictEventType,
    GlobalThreatIndex,
    SeverityLevel,
    ThreatLevel,
)

# ---------------------------------------------------------------------------
# Event type keyword maps
# ---------------------------------------------------------------------------

EVENT_TYPE_KEYWORDS: Dict[str, List[str]] = {
    "AIRSTRIKE": [
        "airstrike", "air strike", "air raid", "bombing run", "warplane",
        "fighter jet", "f-16", "f-35", "b-52", "apache", "helicopter gunship",
        "precision strike", "sorties", "jets bombed", "aerial bombardment",
        "drone strike", "drone attack", "unmanned aerial", "uav strike",
    ],
    "MISSILE_ATTACK": [
        "missile", "ballistic missile", "cruise missile", "rocket attack",
        "rocket barrage", "intercept", "iron dome", "patriot", "s-400",
        "hypersonic", "iskander", "kalibr", "shahab", "zelzal", "burkan",
        "projectile", "launched rockets", "fired missiles", "missile salvo",
    ],
    "GROUND_OPERATION": [
        "ground operation", "ground offensive", "troops advance", "infantry",
        "tank", "armored vehicle", "apc", "ground forces", "soldiers entered",
        "troops deployed", "military incursion", "boots on the ground",
        "mechanized", "artillery barrage", "shelling", "mortar fire",
        "sniper", "ambush", "firefight", "engagement", "raid",
    ],
    "NAVAL": [
        "navy", "naval", "warship", "destroyer", "frigate", "submarine",
        "aircraft carrier", "maritime", "port attack", "sea mines",
        "naval blockade", "coast guard", "vessel seized", "ship attacked",
        "tanker", "strait of hormuz", "red sea", "persian gulf naval",
    ],
    "CYBER": [
        "cyberattack", "cyber attack", "hacked", "malware", "ransomware",
        "infrastructure attack", "power grid attacked", "stuxnet",
        "ddos attack", "critical infrastructure", "data breach military",
        "espionage", "zero-day", "nation-state hacker",
    ],
    "PROTEST": [
        "protest", "demonstration", "rally", "march", "riot", "unrest",
        "civil unrest", "uprising", "dissent", "opposition rally",
        "street protests", "crackdown on protesters", "crowd dispersed",
        "tear gas", "water cannon", "rubber bullets protesters",
    ],
    "CEASEFIRE": [
        "ceasefire", "cease-fire", "truce", "peace deal", "armistice",
        "halt in fighting", "suspension of hostilities", "peace agreement",
        "mediation", "de-escalation", "pull back", "withdrawal agreed",
    ],
    "DIPLOMATIC": [
        "negotiations", "diplomatic talks", "envoy", "ambassador",
        "foreign minister", "secretary of state", "summit", "bilateral talks",
        "peace negotiations", "diplomatic mission", "backchannel",
        "memorandum of understanding", "joint statement", "communique",
    ],
    "SANCTIONS": [
        "sanctions", "embargo", "ban", "economic restriction", "asset freeze",
        "travel ban", "trade restrictions", "export controls", "blacklist",
        "sanctioned", "financial penalties", "oil embargo", "arms embargo",
    ],
}

# ---------------------------------------------------------------------------
# Severity keyword weights
# ---------------------------------------------------------------------------

CRITICAL_INDICATORS = [
    "nuclear", "chemical weapon", "biological weapon", "mass casualty",
    "hundreds killed", "thousands killed", "genocide", "ethnic cleansing",
    "nuclear facility", "radiological", "wmd", "weapons of mass",
    "capital city bombed", "government collapsed",
]

HIGH_INDICATORS = [
    "killed", "dead", "casualties", "wounded", "injured", "fatalities",
    "airstrike on city", "bombing campaign", "major offensive",
    "capital", "civilians killed", "children killed", "hospital bombed",
    "massacre", "destroyed", "annihilated", "heavily bombed",
]

MEDIUM_INDICATORS = [
    "attack", "strike", "clash", "fighting", "assault", "fire opened",
    "troops moved", "aircraft deployed", "missile test", "military exercise",
    "exchange of fire", "shooting", "explosion",
]

LOW_INDICATORS = [
    "diplomatic", "talks", "negotiations", "sanctions announced",
    "protest", "rally", "demonstration", "warning issued",
    "condemnation", "statement", "concern", "monitoring",
]

# ---------------------------------------------------------------------------
# Escalation signal weights
# ---------------------------------------------------------------------------

ESCALATION_POSITIVE_SIGNALS = {
    "nuclear": 0.35,
    "chemical": 0.30,
    "wmd": 0.30,
    "ballistic missile": 0.25,
    "hypersonic": 0.25,
    "capital city": 0.20,
    "mass casualty": 0.20,
    "hundreds killed": 0.20,
    "warplane": 0.15,
    "airstrike": 0.15,
    "invasion": 0.20,
    "offensive": 0.15,
    "escalat": 0.15,
    "war": 0.10,
    "bombing": 0.10,
    "missile": 0.10,
    "attack": 0.08,
    "troops": 0.07,
    "military": 0.05,
}

ESCALATION_NEGATIVE_SIGNALS = {
    "ceasefire": -0.25,
    "truce": -0.25,
    "peace deal": -0.20,
    "withdrawal": -0.15,
    "de-escalation": -0.20,
    "diplomatic": -0.10,
    "negotiations": -0.10,
    "talks": -0.08,
    "sanctions": -0.05,
    "protest": -0.05,
}

# ---------------------------------------------------------------------------
# Simple sentiment lexicon (faster than FinBERT for high-volume news)
# ---------------------------------------------------------------------------

NEGATIVE_WORDS = {
    "killed", "dead", "death", "killed", "massacre", "attack", "bomb",
    "explosion", "war", "conflict", "violence", "terror", "fear", "crisis",
    "threat", "danger", "missile", "airstrike", "casualties", "wounded",
    "destroyed", "assault", "invasion", "occupation", "siege", "blockade",
    "hostage", "kidnapped", "executed", "torture", "ethnic cleansing",
    "genocide", "displacement", "refugee", "famine", "collapse",
}

POSITIVE_WORDS = {
    "ceasefire", "peace", "truce", "agreement", "deal", "negotiations",
    "talks", "diplomatic", "cooperation", "aid", "relief", "freed",
    "released", "withdrawal", "de-escalation", "calm", "stability",
    "reconstruction", "hope", "progress", "resolution",
}

# ---------------------------------------------------------------------------
# NLPService
# ---------------------------------------------------------------------------


class NLPService:
    """
    NLP pipeline for conflict event analysis.
    Uses keyword matching as primary method (fast, no GPU needed),
    with optional spaCy for entity extraction and transformers for
    zero-shot classification as enhancement.
    """

    def __init__(self) -> None:
        self._spacy_nlp = None
        self._zero_shot = None
        self._spacy_loaded = False
        self._zero_shot_loaded = False

    # ------------------------------------------------------------------
    # Lazy model loaders
    # ------------------------------------------------------------------

    def _load_spacy(self) -> bool:
        if self._spacy_loaded:
            return self._spacy_nlp is not None
        self._spacy_loaded = True
        try:
            import spacy
            self._spacy_nlp = spacy.load("en_core_web_sm")
            logger.info("spaCy en_core_web_sm loaded successfully")
            return True
        except Exception as exc:
            logger.warning(f"spaCy not available: {exc}. Entity extraction will use regex fallback.")
            return False

    def _load_zero_shot(self) -> bool:
        if self._zero_shot_loaded:
            return self._zero_shot is not None
        self._zero_shot_loaded = True
        try:
            from transformers import pipeline as hf_pipeline
            self._zero_shot = hf_pipeline(
                "zero-shot-classification",
                model="facebook/bart-large-mnli",
                device=-1,  # CPU
            )
            logger.info("Zero-shot classifier (bart-large-mnli) loaded")
            return True
        except Exception as exc:
            logger.warning(f"Transformers zero-shot not available: {exc}. Using keyword classification only.")
            return False

    # ------------------------------------------------------------------
    # Event type classification
    # ------------------------------------------------------------------

    def classify_event_type(self, text: str) -> ConflictEventType:
        """
        Classify the conflict event type from text.
        Strategy:
          1. Keyword match (fast, high precision for common types)
          2. Zero-shot classification fallback (if transformers available)
        """
        text_lower = text.lower()

        # Score each type by keyword hits
        scores: Dict[str, int] = {}
        for event_type, keywords in EVENT_TYPE_KEYWORDS.items():
            score = sum(1 for kw in keywords if kw in text_lower)
            if score > 0:
                scores[event_type] = score

        if scores:
            best_type = max(scores, key=scores.__getitem__)
            if scores[best_type] >= 1:
                try:
                    return ConflictEventType(best_type)
                except ValueError:
                    pass

        # Zero-shot fallback
        if self._load_zero_shot() and self._zero_shot is not None:
            try:
                candidate_labels = [t.value for t in ConflictEventType if t != ConflictEventType.UNKNOWN]
                result = self._zero_shot(text[:512], candidate_labels)
                top_label = result["labels"][0]
                return ConflictEventType(top_label)
            except Exception as exc:
                logger.debug(f"Zero-shot classification failed: {exc}")

        return ConflictEventType.UNKNOWN

    # ------------------------------------------------------------------
    # Severity assessment
    # ------------------------------------------------------------------

    def assess_severity(self, text: str, event_type: ConflictEventType) -> SeverityLevel:
        """
        Assess event severity from text content and event type.
        Rules-based: keyword matching with type-specific boosting.
        """
        text_lower = text.lower()

        # Critical: WMD / mass casualties
        if any(ind in text_lower for ind in CRITICAL_INDICATORS):
            return SeverityLevel.CRITICAL

        # HIGH: fatalities + military action
        high_matches = sum(1 for ind in HIGH_INDICATORS if ind in text_lower)
        if high_matches >= 2:
            return SeverityLevel.HIGH

        # Specific casualty count detection
        casualty_match = re.search(
            r"(\d+)\s*(people|civilians|soldiers|fighters|militants)?\s*(killed|dead|wounded|casualties)",
            text_lower,
        )
        if casualty_match:
            count = int(casualty_match.group(1))
            if count >= 100:
                return SeverityLevel.CRITICAL
            if count >= 10:
                return SeverityLevel.HIGH
            if count >= 1:
                return SeverityLevel.MEDIUM

        # Type-based defaults
        if event_type in (ConflictEventType.AIRSTRIKE, ConflictEventType.MISSILE_ATTACK):
            if high_matches >= 1:
                return SeverityLevel.HIGH
            return SeverityLevel.MEDIUM

        if event_type == ConflictEventType.GROUND_OPERATION:
            return SeverityLevel.MEDIUM

        if event_type in (ConflictEventType.DIPLOMATIC, ConflictEventType.SANCTIONS, ConflictEventType.PROTEST):
            return SeverityLevel.LOW

        if event_type == ConflictEventType.CEASEFIRE:
            return SeverityLevel.LOW

        # Medium: generic conflict indicators
        if any(ind in text_lower for ind in MEDIUM_INDICATORS):
            return SeverityLevel.MEDIUM

        return SeverityLevel.LOW

    # ------------------------------------------------------------------
    # Escalation score
    # ------------------------------------------------------------------

    def calculate_escalation_score(
        self,
        event: ConflictEvent,
        recent_event_count: int = 0,
    ) -> float:
        """
        Compute escalation probability 0.0–1.0.
        Factors:
          - Positive/negative keyword signals from text
          - Severity base weight
          - Regional event frequency (recent_event_count in same region, last 24h)
          - Event type weight
        """
        text = (event.title + " " + event.description).lower()
        score = 0.0

        # Severity base
        severity_base = {
            SeverityLevel.CRITICAL: 0.50,
            SeverityLevel.HIGH: 0.35,
            SeverityLevel.MEDIUM: 0.20,
            SeverityLevel.LOW: 0.05,
        }
        score += severity_base.get(event.severity, 0.10)

        # Positive escalation signals
        for signal, weight in ESCALATION_POSITIVE_SIGNALS.items():
            if signal in text:
                score += weight

        # Negative de-escalation signals
        for signal, weight in ESCALATION_NEGATIVE_SIGNALS.items():
            if signal in text:
                score += weight  # weights are negative

        # Event type contribution
        type_weights = {
            ConflictEventType.MISSILE_ATTACK: 0.15,
            ConflictEventType.AIRSTRIKE: 0.10,
            ConflictEventType.GROUND_OPERATION: 0.08,
            ConflictEventType.NAVAL: 0.07,
            ConflictEventType.CYBER: 0.05,
            ConflictEventType.SANCTIONS: -0.05,
            ConflictEventType.DIPLOMATIC: -0.08,
            ConflictEventType.CEASEFIRE: -0.15,
            ConflictEventType.PROTEST: 0.02,
        }
        score += type_weights.get(event.event_type, 0.0)

        # Regional frequency boost: more events in region = higher escalation risk
        if recent_event_count > 20:
            score += 0.10
        elif recent_event_count > 10:
            score += 0.06
        elif recent_event_count > 5:
            score += 0.03

        # Clamp to [0, 1]
        return round(max(0.0, min(1.0, score)), 3)

    # ------------------------------------------------------------------
    # Named entity extraction
    # ------------------------------------------------------------------

    def extract_entities(self, text: str) -> List[str]:
        """
        Extract PERSON, ORG, GPE (geopolitical) entities from text.
        Uses spaCy if available, otherwise falls back to regex patterns.
        """
        if self._load_spacy() and self._spacy_nlp is not None:
            return self._spacy_extract(text)
        return self._regex_extract(text)

    def _spacy_extract(self, text: str) -> List[str]:
        """Extract entities using spaCy NER."""
        try:
            doc = self._spacy_nlp(text[:1000])
            entities = []
            seen = set()
            for ent in doc.ents:
                if ent.label_ in ("PERSON", "ORG", "GPE", "NORP") and len(ent.text) > 2:
                    normalised = ent.text.strip()
                    if normalised.lower() not in seen:
                        seen.add(normalised.lower())
                        entities.append(normalised)
            return entities[:15]
        except Exception as exc:
            logger.debug(f"spaCy entity extraction failed: {exc}")
            return self._regex_extract(text)

    def _regex_extract(self, text: str) -> List[str]:
        """
        Fallback regex-based entity extraction.
        Detects capitalised phrases likely to be proper nouns.
        Also matches known military/political organisation names.
        """
        known_entities = [
            "Iran", "IRGC", "Hezbollah", "Hamas", "IDF", "Israel", "Gaza",
            "Lebanon", "Syria", "Iraq", "Yemen", "Houthi", "Islamic State",
            "ISIS", "Al-Qaeda", "Taliban", "NATO", "UN", "UNSC",
            "Biden", "Netanyahu", "Putin", "Zelensky", "Khamenei", "Raisi",
            "Nasrallah", "Sinwar", "Erdogan", "MBS", "Mohammed bin Salman",
            "Kremlin", "Pentagon", "White House", "State Department",
            "Russia", "Ukraine", "China", "United States", "Turkey",
            "Saudi Arabia", "UAE", "Qatar", "Jordan", "Egypt",
            "CENTCOM", "Mossad", "CIA", "MI6", "FSB", "GRU",
        ]
        found = []
        text_lower = text.lower()
        seen = set()
        for entity in known_entities:
            if entity.lower() in text_lower and entity not in seen:
                seen.add(entity)
                found.append(entity)

        # Also extract capitalised multi-word sequences (likely proper nouns)
        cap_pattern = re.compile(r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})\b')
        for match in cap_pattern.finditer(text):
            phrase = match.group(0)
            if phrase not in seen and len(phrase) > 4:
                seen.add(phrase)
                found.append(phrase)
                if len(found) >= 15:
                    break

        return found[:15]

    # ------------------------------------------------------------------
    # Sentiment scoring
    # ------------------------------------------------------------------

    def calculate_sentiment(self, text: str) -> float:
        """
        Calculate sentiment score from -1.0 (very negative) to 1.0 (positive).
        Uses fast lexicon-based approach suitable for high-volume news processing.
        Conflict news is typically negative; ceasefire / diplomatic events are less negative.
        """
        tokens = re.findall(r'\b\w+\b', text.lower())
        if not tokens:
            return -0.2  # Default slightly negative for conflict context

        neg_count = sum(1 for t in tokens if t in NEGATIVE_WORDS)
        pos_count = sum(1 for t in tokens if t in POSITIVE_WORDS)
        total = len(tokens)

        if total == 0:
            return -0.2

        # Raw score: positive fraction minus negative fraction, amplified
        raw_score = (pos_count - neg_count) / math.sqrt(total)

        # Normalise to [-1, 1] with sigmoid-like clamp
        normalised = max(-1.0, min(1.0, raw_score * 3.0))
        return round(normalised, 3)

    # ------------------------------------------------------------------
    # Full event enrichment pipeline
    # ------------------------------------------------------------------

    def enrich_event(self, event: ConflictEvent, recent_event_count: int = 0) -> ConflictEvent:
        """
        Run all NLP enrichment steps on a ConflictEvent in-place.
        1. Classify event type (if UNKNOWN)
        2. Assess severity
        3. Extract entities
        4. Calculate sentiment
        5. Calculate escalation score
        6. Extract keywords
        Returns the enriched event.
        """
        full_text = f"{event.title} {event.description}"

        # 1. Event type
        if event.event_type == ConflictEventType.UNKNOWN or not event.event_type:
            event.event_type = self.classify_event_type(full_text)

        # 2. Severity (re-assess even if set, NLP may refine it)
        event.severity = self.assess_severity(full_text, event.event_type)

        # 3. Entity extraction
        if not event.entities:
            event.entities = self.extract_entities(full_text)

        # 4. Sentiment
        event.sentiment_score = self.calculate_sentiment(full_text)

        # 5. Escalation score
        event.escalation_score = self.calculate_escalation_score(event, recent_event_count)

        # 6. Keywords (supplement existing)
        if not event.keywords:
            from app.services.news_service import _extract_keywords_from_text
            event.keywords = _extract_keywords_from_text(full_text)

        return event

    def enrich_batch(
        self,
        events: List[ConflictEvent],
        region_counts: Optional[Dict[str, int]] = None,
    ) -> List[ConflictEvent]:
        """Enrich a batch of events. region_counts maps region -> recent 24h event count."""
        region_counts = region_counts or {}
        enriched = []
        for event in events:
            recent_count = region_counts.get(event.region, 0)
            try:
                enriched.append(self.enrich_event(event, recent_event_count=recent_count))
            except Exception as exc:
                logger.warning(f"NLP enrichment failed for event '{event.title[:50]}': {exc}")
                enriched.append(event)
        return enriched

    # ------------------------------------------------------------------
    # Global Threat Index
    # ------------------------------------------------------------------

    def compute_global_threat_index(
        self,
        events_24h: List[ConflictEvent],
        events_48h: Optional[List[ConflictEvent]] = None,
    ) -> GlobalThreatIndex:
        """
        Compute the Global Threat Index (0–100) from recent events.

        Score formula:
            raw = sum(CRITICAL*10 + HIGH*5 + MEDIUM*2 + LOW*0.5)
                  + sum(escalation_scores * 3)
            Normalised and capped at 100.

        Trend = delta vs previous 24h window (events_48h minus events_24h).
        """
        if not events_24h:
            return GlobalThreatIndex.from_score(
                score=0.0,
                top_hotspots=[],
                event_counts={"CRITICAL": 0, "HIGH": 0, "MEDIUM": 0, "LOW": 0, "total": 0},
                trend_24h=0.0,
            )

        # Count by severity
        sev_counts: Dict[str, int] = {"CRITICAL": 0, "HIGH": 0, "MEDIUM": 0, "LOW": 0}
        total_escalation = 0.0
        country_counter: Counter = Counter()

        for event in events_24h:
            sev_counts[event.severity.value] = sev_counts.get(event.severity.value, 0) + 1
            total_escalation += event.escalation_score
            if event.country != "Unknown":
                country_counter[event.country] += 1

        # Raw score
        raw_score = (
            sev_counts["CRITICAL"] * 10.0
            + sev_counts["HIGH"] * 5.0
            + sev_counts["MEDIUM"] * 2.0
            + sev_counts["LOW"] * 0.5
            + total_escalation * 3.0
        )

        # Normalise: assume 100 events/day is baseline for score=50
        # Scale by log to handle spikes gracefully
        if raw_score > 0:
            normalised = min(100.0, raw_score / (1 + math.log1p(raw_score / 20)) * 10)
        else:
            normalised = 0.0

        # Trend calculation
        trend_24h = 0.0
        if events_48h:
            # Previous window = events_48h excluding events_24h (by id)
            ids_24h = {e.id for e in events_24h}
            prev_events = [e for e in events_48h if e.id not in ids_24h]
            if prev_events:
                prev_raw = (
                    sum(1 for e in prev_events if e.severity == SeverityLevel.CRITICAL) * 10.0
                    + sum(1 for e in prev_events if e.severity == SeverityLevel.HIGH) * 5.0
                    + sum(1 for e in prev_events if e.severity == SeverityLevel.MEDIUM) * 2.0
                    + sum(1 for e in prev_events if e.severity == SeverityLevel.LOW) * 0.5
                    + sum(e.escalation_score for e in prev_events) * 3.0
                )
                prev_normalised = min(100.0, prev_raw / (1 + math.log1p(prev_raw / 20)) * 10) if prev_raw > 0 else 0.0
                trend_24h = normalised - prev_normalised

        top_hotspots = [country for country, _ in country_counter.most_common(5)]
        event_counts = {**sev_counts, "total": len(events_24h)}

        return GlobalThreatIndex.from_score(
            score=round(normalised, 2),
            top_hotspots=top_hotspots,
            event_counts=event_counts,
            trend_24h=round(trend_24h, 2),
        )
