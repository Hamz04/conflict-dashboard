# Built by Hamzy - ETS Montreal
# ConflictWatch - Global Conflict Intelligence Dashboard
# APScheduler background ingestion service:
#   - Every 15 min: GDELT + RSS ingestion
#   - Every 1 hour: Global Threat Index computation + Redis cache
#   - Every 6 hours: DB cleanup (events older than 30 days)
#   - On startup: immediate ingestion run

from __future__ import annotations

import asyncio
import json
import os
import sys
from datetime import datetime, timedelta
from typing import List, Optional

import redis.asyncio as aioredis
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from loguru import logger

from app.database import (
    AsyncSessionLocal,
    bulk_upsert_events,
    delete_old_events,
    get_events_filtered,
    init_db,
    record_ingestion_stats,
)
from app.models.schemas import ConflictEvent, GlobalThreatIndex
from app.services.gdelt_service import GDELTService
from app.services.news_service import NewsService
from app.services.nlp_service import NLPService

# ---------------------------------------------------------------------------
# Redis keys
# ---------------------------------------------------------------------------
REDIS_KEY_THREAT_INDEX = "conflictwatch:threat_index"
REDIS_KEY_LAST_INGESTION = "conflictwatch:last_ingestion"
REDIS_KEY_LAST_GDELT = "conflictwatch:last_gdelt"
REDIS_KEY_LAST_RSS = "conflictwatch:last_rss"
REDIS_KEY_STATS = "conflictwatch:stats"
REDIS_KEY_INGESTION_TOTAL = "conflictwatch:total_ingested"
REDIS_KEY_DEDUP_TOTAL = "conflictwatch:total_dedup_skipped"

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# ---------------------------------------------------------------------------
# Singleton services
# ---------------------------------------------------------------------------
_gdelt_service: Optional[GDELTService] = None
_news_service: Optional[NewsService] = None
_nlp_service: Optional[NLPService] = None
_redis_client: Optional[aioredis.Redis] = None
_scheduler: Optional[AsyncIOScheduler] = None


def get_gdelt_service() -> GDELTService:
    global _gdelt_service
    if _gdelt_service is None:
        _gdelt_service = GDELTService()
    return _gdelt_service


def get_news_service() -> NewsService:
    global _news_service
    if _news_service is None:
        _news_service = NewsService()
    return _news_service


def get_nlp_service() -> NLPService:
    global _nlp_service
    if _nlp_service is None:
        _nlp_service = NLPService()
    return _nlp_service


async def get_redis() -> aioredis.Redis:
    global _redis_client
    if _redis_client is None:
        try:
            _redis_client = await aioredis.from_url(
                REDIS_URL,
                encoding="utf-8",
                decode_responses=True,
                socket_connect_timeout=5,
            )
            await _redis_client.ping()
            logger.info("Redis connection established")
        except Exception as exc:
            logger.warning(f"Redis not available: {exc}. Cache disabled.")
            _redis_client = None
    return _redis_client


# ---------------------------------------------------------------------------
# Core ingestion logic
# ---------------------------------------------------------------------------

async def run_ingestion_cycle() -> dict:
    """
    Full ingestion cycle:
      1. Fetch GDELT 15-min CSV events
      2. Fetch GDELT DOC API queries
      3. Fetch all RSS feeds
      4. Run NLP enrichment on all new events
      5. Bulk upsert to SQLite DB
      6. Update Redis stats
    Returns stats dict.
    """
    logger.info("=== Starting ingestion cycle ===")
    cycle_start = datetime.utcnow()

    gdelt = get_gdelt_service()
    news = get_news_service()
    nlp = get_nlp_service()

    # --- Step 1: GDELT CSV ---
    gdelt_csv_events: List[ConflictEvent] = []
    try:
        gdelt_csv_events = await gdelt.fetch_latest_events()
        logger.info(f"GDELT CSV: {len(gdelt_csv_events)} conflict events fetched")
    except Exception as exc:
        logger.error(f"GDELT CSV ingestion failed: {exc}")

    # --- Step 2: GDELT DOC API ---
    gdelt_doc_events: List[ConflictEvent] = []
    try:
        gdelt_doc_events = await gdelt.fetch_all_default_queries()
        logger.info(f"GDELT DOC API: {len(gdelt_doc_events)} events fetched")
    except Exception as exc:
        logger.error(f"GDELT DOC API ingestion failed: {exc}")

    # --- Step 3: RSS feeds ---
    rss_events: List[ConflictEvent] = []
    try:
        rss_events = await news.fetch_all(include_fallbacks=False)
        logger.info(f"RSS feeds: {len(rss_events)} conflict events fetched")
    except Exception as exc:
        logger.error(f"RSS ingestion failed: {exc}")

    # --- Combine all events ---
    all_raw_events = gdelt_csv_events + gdelt_doc_events + rss_events
    logger.info(f"Total raw events before NLP: {len(all_raw_events)}")

    if not all_raw_events:
        logger.warning("No events fetched in this cycle")
        return {"new_events": 0, "duplicates": 0, "errors": 0}

    # --- Step 4: NLP enrichment ---
    # Build region frequency map from DB for escalation context
    region_counts = await _get_region_counts_from_db()
    try:
        enriched_events = nlp.enrich_batch(all_raw_events, region_counts=region_counts)
        logger.info(f"NLP enrichment complete: {len(enriched_events)} events")
    except Exception as exc:
        logger.error(f"NLP batch enrichment failed: {exc}")
        enriched_events = all_raw_events

    # --- Step 5: DB upsert ---
    total_inserted = 0
    total_duplicates = 0

    async with AsyncSessionLocal() as session:
        try:
            inserted, duplicates = await bulk_upsert_events(session, enriched_events)
            total_inserted += inserted
            total_duplicates += duplicates

            # Record per-source stats
            await record_ingestion_stats(
                session,
                source="full_cycle",
                new_events=inserted,
                duplicates_skipped=duplicates,
            )
        except Exception as exc:
            logger.error(f"DB upsert failed: {exc}")
            await session.rollback()

    # --- Step 6: Redis stats update ---
    await _update_redis_stats(
        new_events=total_inserted,
        duplicates=total_duplicates,
        cycle_start=cycle_start,
    )

    elapsed = (datetime.utcnow() - cycle_start).total_seconds()
    logger.info(
        f"=== Ingestion cycle complete in {elapsed:.1f}s: "
        f"{total_inserted} new events, {total_duplicates} duplicates skipped ==="
    )

    return {
        "new_events": total_inserted,
        "duplicates": total_duplicates,
        "errors": 0,
        "elapsed_seconds": elapsed,
    }


async def run_threat_index_computation() -> Optional[GlobalThreatIndex]:
    """
    Compute and cache the Global Threat Index from recent 24h events.
    Stores result in Redis with 2h TTL.
    """
    logger.info("Computing Global Threat Index...")
    nlp = get_nlp_service()

    try:
        async with AsyncSessionLocal() as session:
            # 24h events
            events_24h_orm = await get_events_filtered(session, hours=24, limit=2000)
            events_24h = [e.to_schema() for e in events_24h_orm]

            # 48h events for trend
            events_48h_orm = await get_events_filtered(session, hours=48, limit=4000)
            events_48h = [e.to_schema() for e in events_48h_orm]

        threat_index = nlp.compute_global_threat_index(events_24h, events_48h)

        # Cache in Redis
        redis = await get_redis()
        if redis:
            try:
                payload = threat_index.model_dump_json()
                await redis.setex(REDIS_KEY_THREAT_INDEX, 7200, payload)  # 2h TTL
                logger.info(
                    f"Global Threat Index: {threat_index.score:.1f} ({threat_index.level.value}) "
                    f"| Trend: {threat_index.trend_24h:+.1f} "
                    f"| Hotspots: {', '.join(threat_index.top_hotspots[:3])}"
                )
            except Exception as exc:
                logger.warning(f"Redis cache write failed: {exc}")

        return threat_index

    except Exception as exc:
        logger.error(f"Threat index computation failed: {exc}")
        return None


async def run_db_cleanup() -> int:
    """Delete events older than 30 days. Returns count deleted."""
    logger.info("Running database cleanup (events > 30 days)...")
    try:
        async with AsyncSessionLocal() as session:
            count = await delete_old_events(session, days=30)
        logger.info(f"DB cleanup: {count} old events deleted")
        return count
    except Exception as exc:
        logger.error(f"DB cleanup failed: {exc}")
        return 0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _get_region_counts_from_db() -> dict:
    """Get event counts per region for the last 24h (for escalation context)."""
    try:
        async with AsyncSessionLocal() as session:
            from sqlalchemy import func, select
            from app.database import ConflictEventORM
            cutoff = datetime.utcnow() - timedelta(hours=24)
            stmt = (
                select(
                    ConflictEventORM.region,
                    func.count(ConflictEventORM.id).label("cnt"),
                )
                .where(ConflictEventORM.published_at >= cutoff)
                .group_by(ConflictEventORM.region)
            )
            result = await session.execute(stmt)
            return {row.region: row.cnt for row in result.all()}
    except Exception:
        return {}


async def _update_redis_stats(
    new_events: int, duplicates: int, cycle_start: datetime
) -> None:
    """Persist ingestion stats to Redis."""
    redis = await get_redis()
    if not redis:
        return
    try:
        now_iso = datetime.utcnow().isoformat()
        await redis.set(REDIS_KEY_LAST_INGESTION, now_iso)
        await redis.incrby(REDIS_KEY_INGESTION_TOTAL, new_events)
        await redis.incrby(REDIS_KEY_DEDUP_TOTAL, duplicates)

        stats = {
            "last_ingestion_at": now_iso,
            "last_cycle_new_events": new_events,
            "last_cycle_duplicates": duplicates,
        }
        await redis.setex(REDIS_KEY_STATS, 3600, json.dumps(stats))
    except Exception as exc:
        logger.warning(f"Redis stats update failed: {exc}")


async def get_cached_threat_index() -> Optional[GlobalThreatIndex]:
    """Retrieve Global Threat Index from Redis cache."""
    redis = await get_redis()
    if not redis:
        return None
    try:
        raw = await redis.get(REDIS_KEY_THREAT_INDEX)
        if raw:
            return GlobalThreatIndex.model_validate_json(raw)
    except Exception as exc:
        logger.warning(f"Redis threat index read failed: {exc}")
    return None


async def get_ingestion_stats() -> dict:
    """Retrieve ingestion stats from Redis."""
    redis = await get_redis()
    result = {
        "last_ingestion_at": None,
        "total_ingested": 0,
        "total_duplicates_skipped": 0,
    }
    if not redis:
        return result
    try:
        last = await redis.get(REDIS_KEY_LAST_INGESTION)
        total = await redis.get(REDIS_KEY_INGESTION_TOTAL)
        dedup = await redis.get(REDIS_KEY_DEDUP_TOTAL)
        result["last_ingestion_at"] = last
        result["total_ingested"] = int(total or 0)
        result["total_duplicates_skipped"] = int(dedup or 0)
    except Exception as exc:
        logger.warning(f"Redis stats read failed: {exc}")
    return result


# ---------------------------------------------------------------------------
# Scheduler setup
# ---------------------------------------------------------------------------

def create_scheduler() -> AsyncIOScheduler:
    """Create and configure the APScheduler instance."""
    scheduler = AsyncIOScheduler(timezone="UTC")

    # Every 15 minutes: ingest GDELT + RSS
    scheduler.add_job(
        run_ingestion_cycle,
        trigger=IntervalTrigger(minutes=15),
        id="ingestion_cycle",
        name="GDELT + RSS Ingestion",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=120,
    )

    # Every 60 minutes: compute Global Threat Index
    scheduler.add_job(
        run_threat_index_computation,
        trigger=IntervalTrigger(hours=1),
        id="threat_index",
        name="Global Threat Index Computation",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=300,
    )

    # Every 6 hours: clean old events
    scheduler.add_job(
        run_db_cleanup,
        trigger=IntervalTrigger(hours=6),
        id="db_cleanup",
        name="Database Cleanup",
        max_instances=1,
        coalesce=True,
    )

    return scheduler


async def startup_sequence() -> None:
    """
    Full startup: init DB, run immediate ingestion, compute threat index, start scheduler.
    Called by FastAPI lifespan or standalone __main__.
    """
    global _scheduler

    logger.info("ConflictWatch scheduler starting up...")

    # Init DB
    await init_db()

    # Run immediate ingestion so dashboard has data right away
    logger.info("Running startup ingestion cycle...")
    try:
        stats = await run_ingestion_cycle()
        logger.info(f"Startup ingestion: {stats['new_events']} events ingested")
    except Exception as exc:
        logger.error(f"Startup ingestion failed: {exc}")

    # Compute initial threat index
    try:
        await run_threat_index_computation()
    except Exception as exc:
        logger.error(f"Startup threat index failed: {exc}")

    # Start scheduler
    _scheduler = create_scheduler()
    _scheduler.start()
    logger.info("APScheduler started: 15-min ingestion, 1-hour threat index, 6-hour cleanup")


async def shutdown_sequence() -> None:
    """Graceful shutdown: stop scheduler, close service clients."""
    global _scheduler, _gdelt_service, _news_service, _redis_client

    logger.info("ConflictWatch scheduler shutting down...")

    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)

    if _gdelt_service:
        await _gdelt_service.close()

    if _news_service:
        await _news_service.close()

    if _redis_client:
        await _redis_client.aclose()

    logger.info("Scheduler shutdown complete")


# ---------------------------------------------------------------------------
# Standalone entrypoint: python -m app.services.scheduler
# ---------------------------------------------------------------------------

async def _standalone_main() -> None:
    """Run scheduler as a standalone process (for Docker scheduler service)."""
    logger.remove()
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | {message}",
        level="INFO",
    )

    await startup_sequence()

    # Keep running forever
    try:
        while True:
            await asyncio.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler interrupted")
    finally:
        await shutdown_sequence()


if __name__ == "__main__":
    asyncio.run(_standalone_main())
