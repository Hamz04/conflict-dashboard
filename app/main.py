# Built by Hamzy - ETS Montreal
# ConflictWatch - Global Conflict Intelligence Dashboard
# FastAPI application: REST API + WebSocket streaming

from __future__ import annotations

import asyncio
import json
import os
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import List, Optional, Set

from fastapi import (
    Depends,
    FastAPI,
    HTTPException,
    Query,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from loguru import logger
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import (
    ConflictEventORM,
    IngestionStatsORM,
    get_db,
    get_events_filtered,
    get_hotspots,
    get_timeline,
    init_db,
    search_events,
)
from app.models.schemas import (
    ConflictEvent,
    ConflictEventType,
    ConflictSummary,
    GlobalThreatIndex,
    HotspotEntry,
    IngestionStats,
    RegionFilter,
    SeverityLevel,
    TimelineEntry,
    TrendDirection,
)
from app.services.scheduler import (
    get_cached_threat_index,
    get_ingestion_stats,
    run_threat_index_computation,
    shutdown_sequence,
    startup_sequence,
)

# ---------------------------------------------------------------------------
# WebSocket connection manager
# ---------------------------------------------------------------------------


class ConnectionManager:
    def __init__(self) -> None:
        self.active_connections: Set[WebSocket] = set()

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self.active_connections.add(websocket)
        logger.info(f"WS client connected. Total: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket) -> None:
        self.active_connections.discard(websocket)
        logger.info(f"WS client disconnected. Total: {len(self.active_connections)}")

    async def broadcast(self, message: dict) -> None:
        dead: Set[WebSocket] = set()
        payload = json.dumps(message, default=str)
        for ws in list(self.active_connections):
            try:
                await ws.send_text(payload)
            except Exception:
                dead.add(ws)
        for ws in dead:
            self.active_connections.discard(ws)


ws_manager = ConnectionManager()


# ---------------------------------------------------------------------------
# App lifespan
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup: init DB + scheduler. Shutdown: clean up."""
    logger.info("ConflictWatch API starting up...")
    await startup_sequence()
    yield
    logger.info("ConflictWatch API shutting down...")
    await shutdown_sequence()


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(
    title="ConflictWatch API",
    description=(
        "Real-time Global Conflict Intelligence API. "
        "Built by Hamzy | ETS Montreal. "
        "Data sources: GDELT Project, Reuters, BBC, Al Jazeera, AP News, Iran International."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Custom header middleware
@app.middleware("http")
async def add_custom_headers(request, call_next):
    response = await call_next(request)
    response.headers["X-Built-By"] = "Hamzy - ETS Montreal"
    response.headers["X-App"] = "ConflictWatch v1.0"
    return response


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------


@app.get("/health", tags=["System"])
async def health_check():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat(), "service": "ConflictWatch API"}


# ---------------------------------------------------------------------------
# GET /api/events
# ---------------------------------------------------------------------------


@app.get("/api/events", response_model=List[ConflictEvent], tags=["Events"])
async def get_events(
    region: RegionFilter = Query(RegionFilter.ALL, description="Filter by world region"),
    severity: Optional[SeverityLevel] = Query(None, description="Filter by severity"),
    event_type: Optional[ConflictEventType] = Query(None, description="Filter by event type"),
    hours: int = Query(24, ge=1, le=720, description="Lookback window in hours"),
    limit: int = Query(100, ge=1, le=1000, description="Max results"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    db: AsyncSession = Depends(get_db),
):
    """Return filtered conflict events ordered by published_at descending."""
    region_val = None if region == RegionFilter.ALL else region.value
    rows = await get_events_filtered(
        db,
        region=region_val,
        severity=severity.value if severity else None,
        event_type=event_type.value if event_type else None,
        hours=hours,
        limit=limit,
        offset=offset,
    )
    return [row.to_schema() for row in rows]


# ---------------------------------------------------------------------------
# GET /api/events/{event_id}
# ---------------------------------------------------------------------------


@app.get("/api/events/{event_id}", response_model=ConflictEvent, tags=["Events"])
async def get_event_detail(event_id: str, db: AsyncSession = Depends(get_db)):
    """Return a single event by ID."""
    result = await db.execute(
        select(ConflictEventORM).where(ConflictEventORM.id == event_id)
    )
    row = result.scalar_one_or_none()
    if not row:
        raise HTTPException(status_code=404, detail=f"Event {event_id} not found")
    return row.to_schema()


# ---------------------------------------------------------------------------
# GET /api/summary
# ---------------------------------------------------------------------------


@app.get("/api/summary", response_model=List[ConflictSummary], tags=["Intelligence"])
async def get_summary(db: AsyncSession = Depends(get_db)):
    """Return per-region conflict summaries for last 24h and 7d."""
    summaries = []
    regions = [r.value for r in RegionFilter if r != RegionFilter.ALL]

    cutoff_24h = datetime.utcnow() - timedelta(hours=24)
    cutoff_7d = datetime.utcnow() - timedelta(days=7)
    cutoff_14d = datetime.utcnow() - timedelta(days=14)

    for region in regions:
        # 24h count
        r24 = await db.execute(
            select(func.count(ConflictEventORM.id)).where(
                ConflictEventORM.region == region,
                ConflictEventORM.published_at >= cutoff_24h,
            )
        )
        count_24h = r24.scalar() or 0

        # 7d count
        r7 = await db.execute(
            select(func.count(ConflictEventORM.id)).where(
                ConflictEventORM.region == region,
                ConflictEventORM.published_at >= cutoff_7d,
            )
        )
        count_7d = r7.scalar() or 0

        if count_7d == 0:
            continue  # Skip regions with no activity

        # 14d count for trend comparison (prior 7d = 14d window minus last 7d)
        r14 = await db.execute(
            select(func.count(ConflictEventORM.id)).where(
                ConflictEventORM.region == region,
                ConflictEventORM.published_at >= cutoff_14d,
                ConflictEventORM.published_at < cutoff_7d,
            )
        )
        count_prev_7d = r14.scalar() or 0

        trend = TrendDirection.STABLE
        if count_prev_7d > 0:
            change = (count_7d - count_prev_7d) / count_prev_7d
            if change > 0.15:
                trend = TrendDirection.ESCALATING
            elif change < -0.15:
                trend = TrendDirection.DE_ESCALATING

        # Dominant event type
        type_result = await db.execute(
            select(
                ConflictEventORM.event_type,
                func.count(ConflictEventORM.id).label("cnt"),
            )
            .where(
                ConflictEventORM.region == region,
                ConflictEventORM.published_at >= cutoff_7d,
            )
            .group_by(ConflictEventORM.event_type)
            .order_by(text("cnt DESC"))
            .limit(1)
        )
        dom_row = type_result.first()
        dominant_type = ConflictEventType(dom_row[0]) if dom_row else ConflictEventType.UNKNOWN

        # Avg severity (map to numeric)
        sev_map = {"CRITICAL": 4, "HIGH": 3, "MEDIUM": 2, "LOW": 1}
        sev_result = await db.execute(
            select(ConflictEventORM.severity).where(
                ConflictEventORM.region == region,
                ConflictEventORM.published_at >= cutoff_7d,
            )
        )
        sev_values = [sev_map.get(row[0], 1) for row in sev_result.all()]
        avg_sev = sum(sev_values) / len(sev_values) if sev_values else 1.0

        # Top countries
        country_result = await db.execute(
            select(
                ConflictEventORM.country,
                func.count(ConflictEventORM.id).label("cnt"),
            )
            .where(
                ConflictEventORM.region == region,
                ConflictEventORM.published_at >= cutoff_7d,
            )
            .group_by(ConflictEventORM.country)
            .order_by(text("cnt DESC"))
            .limit(5)
        )
        top_countries = [row[0] for row in country_result.all()]

        # Last event time
        last_evt = await db.execute(
            select(ConflictEventORM.published_at)
            .where(ConflictEventORM.region == region)
            .order_by(ConflictEventORM.published_at.desc())
            .limit(1)
        )
        last_at = last_evt.scalar()

        summaries.append(
            ConflictSummary(
                region=region,
                event_count_24h=count_24h,
                event_count_7d=count_7d,
                dominant_type=dominant_type,
                avg_severity=round(avg_sev, 2),
                trend=trend,
                top_countries=top_countries,
                last_event_at=last_at,
            )
        )

    summaries.sort(key=lambda s: s.event_count_24h, reverse=True)
    return summaries


# ---------------------------------------------------------------------------
# GET /api/threat-index
# ---------------------------------------------------------------------------


@app.get("/api/threat-index", response_model=GlobalThreatIndex, tags=["Intelligence"])
async def get_threat_index():
    """Return the Global Threat Index (0-100). Cached in Redis, recomputed hourly."""
    cached = await get_cached_threat_index()
    if cached:
        return cached
    # Not cached — compute on demand
    result = await run_threat_index_computation()
    if result:
        return result
    raise HTTPException(status_code=503, detail="Threat index computation failed")


# ---------------------------------------------------------------------------
# GET /api/search
# ---------------------------------------------------------------------------


@app.get("/api/search", response_model=List[ConflictEvent], tags=["Events"])
async def search(
    q: str = Query(..., min_length=2, max_length=256, description="Search query"),
    limit: int = Query(50, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    """Full-text search on event title, description, and country."""
    rows = await search_events(db, q, limit=limit)
    return [row.to_schema() for row in rows]


# ---------------------------------------------------------------------------
# GET /api/hotspots
# ---------------------------------------------------------------------------


@app.get("/api/hotspots", response_model=List[HotspotEntry], tags=["Intelligence"])
async def get_hotspot_list(
    hours: int = Query(24, ge=1, le=168),
    top_n: int = Query(10, ge=1, le=50),
    db: AsyncSession = Depends(get_db),
):
    """Top N countries by event count in the last `hours` hours."""
    rows = await get_hotspots(db, hours=hours, top_n=top_n)
    result = []
    cutoff = datetime.utcnow() - timedelta(hours=hours)

    for row in rows:
        country = row[0]
        region = row[1]
        event_count = row[2]
        avg_lat = row[3]
        avg_lon = row[4]

        # Dominant severity for this country
        sev_result = await db.execute(
            select(
                ConflictEventORM.severity,
                func.count(ConflictEventORM.id).label("cnt"),
            )
            .where(
                ConflictEventORM.country == country,
                ConflictEventORM.published_at >= cutoff,
            )
            .group_by(ConflictEventORM.severity)
            .order_by(text("cnt DESC"))
            .limit(1)
        )
        sev_row = sev_result.first()
        dom_sev = SeverityLevel(sev_row[0]) if sev_row else SeverityLevel.MEDIUM

        # Dominant event type
        type_result = await db.execute(
            select(
                ConflictEventORM.event_type,
                func.count(ConflictEventORM.id).label("cnt"),
            )
            .where(
                ConflictEventORM.country == country,
                ConflictEventORM.published_at >= cutoff,
            )
            .group_by(ConflictEventORM.event_type)
            .order_by(text("cnt DESC"))
            .limit(1)
        )
        type_row = type_result.first()
        dom_type = ConflictEventType(type_row[0]) if type_row else ConflictEventType.UNKNOWN

        result.append(
            HotspotEntry(
                country=country,
                region=region or "Unknown",
                event_count_24h=event_count,
                dominant_severity=dom_sev,
                dominant_type=dom_type,
                lat=avg_lat,
                lon=avg_lon,
            )
        )
    return result


# ---------------------------------------------------------------------------
# GET /api/timeline
# ---------------------------------------------------------------------------


@app.get("/api/timeline", response_model=List[TimelineEntry], tags=["Analytics"])
async def get_country_timeline(
    country: str = Query(..., description="Country name"),
    days: int = Query(30, ge=1, le=365),
    db: AsyncSession = Depends(get_db),
):
    """Events per day for a country over the last N days."""
    rows = await get_timeline(db, country=country, days=days)
    if not rows:
        return []

    # Bucket by date
    daily: dict = defaultdict(lambda: {"total": 0, "by_type": defaultdict(int), "escalation_sum": 0.0})
    for row in rows:
        date_key = row.published_at.strftime("%Y-%m-%d") if row.published_at else "unknown"
        daily[date_key]["total"] += 1
        daily[date_key]["by_type"][row.event_type] += 1
        daily[date_key]["escalation_sum"] += row.escalation_score or 0.0

    result = []
    for date_str in sorted(daily.keys()):
        d = daily[date_str]
        total = d["total"]
        result.append(
            TimelineEntry(
                date=date_str,
                total=total,
                by_type=dict(d["by_type"]),
                avg_escalation=round(d["escalation_sum"] / total, 3) if total > 0 else 0.0,
            )
        )
    return result


# ---------------------------------------------------------------------------
# GET /api/stats
# ---------------------------------------------------------------------------


@app.get("/api/stats", response_model=IngestionStats, tags=["System"])
async def get_stats(db: AsyncSession = Depends(get_db)):
    """Ingestion statistics: total events, last update, source status."""
    # DB counts
    total_result = await db.execute(select(func.count(ConflictEventORM.id)))
    total_events = total_result.scalar() or 0

    cutoff_24h = datetime.utcnow() - timedelta(hours=24)
    cnt_24h = await db.execute(
        select(func.count(ConflictEventORM.id)).where(
            ConflictEventORM.published_at >= cutoff_24h
        )
    )
    events_24h = cnt_24h.scalar() or 0

    cutoff_7d = datetime.utcnow() - timedelta(days=7)
    cnt_7d = await db.execute(
        select(func.count(ConflictEventORM.id)).where(
            ConflictEventORM.published_at >= cutoff_7d
        )
    )
    events_7d = cnt_7d.scalar() or 0

    # Last ingestion from DB
    last_ingestion_row = await db.execute(
        select(IngestionStatsORM.run_at)
        .order_by(IngestionStatsORM.run_at.desc())
        .limit(1)
    )
    last_ingestion_at = last_ingestion_row.scalar()

    # Redis stats
    redis_stats = await get_ingestion_stats()
    duplicates_skipped = redis_stats.get("total_duplicates_skipped", 0)

    from app.services.news_service import RSS_FEEDS
    sources_active = list(RSS_FEEDS.keys()) + ["GDELT CSV", "GDELT DOC API"]

    return IngestionStats(
        total_events=total_events,
        events_24h=events_24h,
        events_7d=events_7d,
        last_ingestion_at=last_ingestion_at,
        last_gdelt_update=last_ingestion_at,
        last_rss_update=last_ingestion_at,
        sources_active=sources_active,
        duplicates_skipped_total=duplicates_skipped,
    )


# ---------------------------------------------------------------------------
# WebSocket /ws/events
# ---------------------------------------------------------------------------


@app.websocket("/ws/events")
async def websocket_events(websocket: WebSocket):
    """
    WebSocket endpoint that streams new conflict events as they are ingested.
    Sends a ping every 30 seconds to keep the connection alive.
    New events are polled from DB every 10 seconds and broadcast to all clients.
    """
    await ws_manager.connect(websocket)
    last_seen_id: Optional[str] = None
    last_check = datetime.utcnow()

    try:
        # Send welcome message
        await websocket.send_text(
            json.dumps({
                "type": "connected",
                "message": "ConflictWatch live feed connected",
                "timestamp": datetime.utcnow().isoformat(),
            })
        )

        while True:
            # Check for new events every 10 seconds
            await asyncio.sleep(10)

            async with __import__("app.database", fromlist=["AsyncSessionLocal"]).AsyncSessionLocal() as db_session:
                from sqlalchemy import desc
                stmt = (
                    select(ConflictEventORM)
                    .where(ConflictEventORM.ingested_at >= last_check)
                    .order_by(desc(ConflictEventORM.ingested_at))
                    .limit(20)
                )
                result = await db_session.execute(stmt)
                new_rows = result.scalars().all()

            if new_rows:
                last_check = datetime.utcnow()
                for row in reversed(new_rows):
                    event = row.to_schema()
                    payload = {
                        "type": "new_event",
                        "event": json.loads(event.model_dump_json()),
                    }
                    await ws_manager.broadcast(payload)

            # Ping to keep alive
            await websocket.send_text(
                json.dumps({"type": "ping", "timestamp": datetime.utcnow().isoformat()})
            )

    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)
    except Exception as exc:
        logger.error(f"WebSocket error: {exc}")
        ws_manager.disconnect(websocket)


# ---------------------------------------------------------------------------
# Root
# ---------------------------------------------------------------------------


@app.get("/", tags=["System"])
async def root():
    return {
        "app": "ConflictWatch - Global Conflict Intelligence Dashboard",
        "author": "Hamzy | ETS Montreal",
        "version": "1.0.0",
        "docs": "/docs",
        "endpoints": {
            "events": "/api/events",
            "summary": "/api/summary",
            "threat_index": "/api/threat-index",
            "search": "/api/search",
            "hotspots": "/api/hotspots",
            "timeline": "/api/timeline",
            "stats": "/api/stats",
            "websocket": "/ws/events",
        },
    }
