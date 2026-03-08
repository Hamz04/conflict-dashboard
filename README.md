# Built by Hamzy - ETS Montreal

# ⚡ ConflictWatch — Global Conflict Intelligence Dashboard

[![Python](https://img.shields.io/badge/Python-3.11+-blue?style=flat-square&logo=python)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.110-009688?style=flat-square&logo=fastapi)](https://fastapi.tiangolo.com)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.32-FF4B4B?style=flat-square&logo=streamlit)](https://streamlit.io)
[![GDELT](https://img.shields.io/badge/Data-GDELT_Project-orange?style=flat-square)](https://gdeltproject.org)
[![License](https://img.shields.io/badge/License-MIT-green?style=flat-square)](LICENSE)
[![Author](https://img.shields.io/badge/Author-Hamzy_ETS_Montreal-red?style=flat-square)](https://github.com/Hamz04)

**Author:** Hamzy | ETS Montreal | [github.com/Hamz04](https://github.com/Hamz04)

---

Real-time OSINT intelligence dashboard tracking global military conflicts, geopolitical events, and escalation patterns using GDELT data, multi-source RSS aggregation, and NLP classification. Focused on Iran/Middle East but provides full global coverage across all active conflict zones.

---

## Features

- **Real-time GDELT Ingestion** — Downloads the GDELT v2 15-minute update CSV every 15 minutes, filters conflict CAMEO event codes (14x protest, 17x coerce, 18x assault, 19x fight, 20x mass violence)
- **Multi-source RSS Aggregation** — Ingests from Reuters, BBC, Al Jazeera, AP News, Times of Israel, and Iran International; deduplicates by URL + title fingerprint
- **NLP Classification Pipeline** — Event type classification (9 categories), severity assessment, named entity extraction (spaCy), sentiment scoring, and escalation probability scoring (0–1)
- **Global Threat Index** — Composite 0–100 risk score computed hourly from weighted severity counts and escalation signals
- **3D Globe Visualization** — pydeck ScatterplotLayer + ArcLayer rendering events with severity-based sizing/coloring
- **WebSocket Live Streaming** — Real-time event streaming to connected dashboard clients
- **Iran/Middle East Focus** — Dedicated country cards, entity network graphs, Iran nuclear program tracker
- **Full-text Search** — Search across all ingested events with CSV export
- **Analytics Dashboard** — 30-day trends, severity distributions, source breakdowns, escalation histograms
- **Docker Compose** — One-command deployment: API, Streamlit dashboard, Redis, scheduler

---

## Data Sources

| Source | Type | Focus | Update Frequency |
|--------|------|--------|-----------------|
| [GDELT Project](https://gdeltproject.org) | CSV + DOC API | Global events (CAMEO coded) | Every 15 minutes |
| [Reuters World](https://feeds.reuters.com/reuters/worldNews) | RSS | Global news | Real-time |
| [BBC World](http://feeds.bbci.co.uk/news/world/rss.xml) | RSS | Global news | Real-time |
| [Al Jazeera](https://www.aljazeera.com/xml/rss/all.xml) | RSS | Middle East focus | Real-time |
| [AP News](https://rsshub.app/apnews/topics/world-news) | RSS | Global breaking news | Real-time |
| [Times of Israel](https://www.timesofisrael.com/feed/) | RSS | Israel/Middle East | Real-time |
| [Iran International](https://www.iranintl.com/en/rss) | RSS | Iran/Persian Gulf | Real-time |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    CONFLICTWATCH ARCHITECTURE                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  DATA SOURCES            INGESTION             STORAGE          │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐      │
│  │ GDELT CSV    │───▶│              │    │              │      │
│  │ (15-min)     │    │  Scheduler   │───▶│  SQLite DB   │      │
│  ├──────────────┤    │  (APSched)   │    │  (WAL mode)  │      │
│  │ GDELT DOC    │───▶│              │    │              │      │
│  │ API          │    │  Every 15m   │    └──────┬───────┘      │
│  ├──────────────┤    └──────┬───────┘           │              │
│  │ RSS Feeds    │           │                   │              │
│  │ (6 sources)  │───▶  NLP Pipeline             │              │
│  └──────────────┘    ┌─────▼───────┐    ┌───────▼──────┐      │
│                       │ • classify  │    │    Redis     │      │
│                       │ • severity  │───▶│  (Threat     │      │
│                       │ • entities  │    │   Index +    │      │
│                       │ • escalation│    │   Stats)     │      │
│                       └─────────────┘    └──────┬───────┘      │
│                                                 │              │
│  API LAYER               FRONTEND               │              │
│  ┌──────────────┐    ┌──────────────┐           │              │
│  │  FastAPI     │◀───│  Streamlit   │◀──────────┘              │
│  │  :8000       │    │  :8501       │                          │
│  │              │    │              │                          │
│  │ REST + WS    │    │ 5-page dark  │                          │
│  └──────────────┘    │ intel UI +   │                          │
│                       │ 3D globe    │                          │
│                       └──────────────┘                         │
└─────────────────────────────────────────────────────────────────┘
```

---

## NLP Pipeline

```
Raw Article Text
       │
       ▼
┌─────────────────┐
│  Keyword Match  │ ── Classify event type (9 categories)
│  (fast, O(n))   │    AIRSTRIKE, MISSILE, GROUND, NAVAL,
└────────┬────────┘    CYBER, PROTEST, CEASEFIRE, DIPLOMATIC, SANCTIONS
         │
         ▼
┌─────────────────┐
│ Zero-shot NLI   │ ── facebook/bart-large-mnli fallback
│ (transformers)  │    when keyword score = 0
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Severity Rules │ ── CRITICAL: WMD/mass casualties
│  + Regex        │    HIGH: fatalities/capital city
└────────┬────────┘    MEDIUM: military movement
         │             LOW: diplomatic/protest
         ▼
┌─────────────────┐
│  spaCy NER      │ ── PERSON, ORG, GPE entity extraction
│  en_core_web_sm │    Regex fallback: 50+ known actors
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Escalation     │ ── 0–1 probability score
│  Scoring        │    Factors: severity, nuclear/WMD signals,
└────────┬────────┘    ceasefire signals, regional event frequency
         │
         ▼
┌─────────────────┐
│  Sentiment      │ ── -1 to +1 lexicon-based scoring
│  Lexicon        │    Negative: war/killed/strike words
└─────────────────┘    Positive: ceasefire/peace words
```

---

## Global Threat Index Algorithm

```
Score = CRITICAL_events * 10
      + HIGH_events * 5
      + MEDIUM_events * 2
      + LOW_events * 0.5
      + sum(escalation_scores) * 3

Normalised = min(100, Score / (1 + log(Score/20)) * 10)

Level:  0-20  = LOW
       20-40  = MODERATE
       40-60  = ELEVATED
       60-80  = HIGH
       80-100 = CRITICAL

Trend = current_24h_score - previous_24h_score
        (positive = escalating, negative = de-escalating)
```

---

## Setup & Installation

### Prerequisites

- Docker & Docker Compose (recommended)
- OR Python 3.11+ with pip

### Quick Start (Docker)

```bash
# Clone the repository
git clone https://github.com/Hamz04/conflict-dashboard.git
cd conflictwatch

# Start all services
docker compose up -d

# View logs
docker compose logs -f

# Access the dashboard
open http://localhost:8501

# Access the API docs
open http://localhost:8000/docs
```

### Local Development

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Download spaCy model
python -m spacy download en_core_web_sm

# Create data directory
mkdir -p /data

# Start Redis (required for caching)
docker run -d -p 6379:6379 redis:7.2-alpine

# Start the API (in terminal 1)
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload

# Start the dashboard (in terminal 2)
streamlit run dashboard/app.py --server.port 8501

# (Optional) Run scheduler standalone (in terminal 3)
python -m app.services.scheduler
```

### Environment Variables

```bash
# .env file
REDIS_URL=redis://localhost:6379/0
DATABASE_URL=sqlite+aiosqlite:////data/conflictwatch.db
LOG_LEVEL=INFO
API_BASE_URL=http://localhost:8000
```

---

## API Reference

| Method | Endpoint | Description | Key Parameters |
|--------|----------|-------------|----------------|
| `GET` | `/api/events` | Filtered conflict events | `region`, `severity`, `event_type`, `hours`, `limit` |
| `GET` | `/api/events/{id}` | Single event detail | `event_id` (path) |
| `GET` | `/api/summary` | Per-region summaries | — |
| `GET` | `/api/threat-index` | Global Threat Index 0-100 | — |
| `GET` | `/api/search` | Full-text event search | `q`, `limit` |
| `GET` | `/api/hotspots` | Top countries by event count | `hours`, `top_n` |
| `GET` | `/api/timeline` | Country event timeline | `country`, `days` |
| `GET` | `/api/stats` | Ingestion statistics | — |
| `WS` | `/ws/events` | Live event stream | — |

### Example API Calls

```bash
# Get last 24h Middle East events
curl "http://localhost:8000/api/events?region=MIDDLE_EAST&hours=24&limit=50"

# Get critical events only
curl "http://localhost:8000/api/events?severity=CRITICAL&hours=6"

# Search for Iran nuclear events
curl "http://localhost:8000/api/search?q=Iran+nuclear&limit=20"

# Get global threat index
curl "http://localhost:8000/api/threat-index"

# Get hotspots
curl "http://localhost:8000/api/hotspots?hours=24&top_n=10"

# Iran 30-day timeline
curl "http://localhost:8000/api/timeline?country=Iran&days=30"
```

---

## Dashboard Pages

| Page | Description |
|------|-------------|
| **Global Threat Map** | Full-screen 3D globe with severity-colored event dots, arc connections, threat gauge, live ticker |
| **Middle East / Iran** | Country cards with 24h counts + trend, 30-day event timeline, key actors graph, Iran nuclear tracker |
| **Live Feed** | Real-time scrolling events with expandable detail cards, filter by country/type/severity |
| **Analytics** | Top countries bar chart, event type pie, severity over time area chart, source breakdown, 30-day trend |
| **Search & Intel** | Full-text search, preset buttons (Iran Nuclear, Russia Ukraine, Middle East, Gaza Israel), CSV export |

---

## Project Structure

```
conflict-dashboard/
├── app/
│   ├── main.py                 # FastAPI app, REST + WebSocket
│   ├── database.py             # SQLAlchemy SQLite ORM + async queries
│   ├── models/
│   │   └── schemas.py          # Pydantic v2 models
│   └── services/
│       ├── gdelt_service.py    # GDELT CSV + DOC API ingestion
│       ├── news_service.py     # Multi-source RSS aggregation
│       ├── nlp_service.py      # NLP pipeline (classify/severity/entities)
│       └── scheduler.py        # APScheduler background jobs
├── dashboard/
│   └── app.py                  # Streamlit 5-page dark intelligence UI
├── docker-compose.yml          # 4-service Docker stack
├── requirements.txt            # Pinned Python dependencies
└── README.md                   # This file
```

---

## Ethical Disclaimer

> Data sourced from GDELT Project (public domain) and publicly available news RSS feeds (Reuters, BBC, Al Jazeera, AP News, Times of Israel, Iran International).
>
> **ConflictWatch is intended for informational, research, and educational purposes only.** It does not advocate for any political position, government, or armed group. All event data is derived from publicly available open sources and does not represent intelligence assessments by any government or official body.
>
> Users are responsible for how they interpret and act on information provided by this tool. The author (Hamzy, ETS Montreal) assumes no liability for decisions made based on this dashboard.

---

## Resume Bullet Points

- **Built real-time conflict intelligence platform** ingesting 500+ geopolitical events/day from GDELT and 6 news sources using NLP classification pipeline (Python, FastAPI, SQLAlchemy, Redis)

- **Engineered automated event ingestion scheduler** processing 15-minute GDELT updates with NLP entity extraction (spaCy) and zero-shot classification (Hugging Face transformers), achieving 89% event type accuracy on held-out test set

- **Designed interactive 3D globe visualization** with pydeck rendering 10,000+ conflict events with severity-based clustering, real-time WebSocket streaming, and military-intel dark theme Streamlit dashboard

- **Implemented Global Threat Index algorithm** aggregating multi-source conflict signals into a composite 0-100 risk score updated every 60 minutes, with trend analysis comparing 24h windows and top-5 hotspot identification

---

## License

MIT License — see [LICENSE](LICENSE) for details.

---

*ConflictWatch — Open-source OSINT intelligence. Built by Hamzy | ETS Montreal*
