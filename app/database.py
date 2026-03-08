# Built by Hamzy - ETS Montreal
# ConflictWatch - Global Conflict Intelligence Dashboard
# SQLAlchemy SQLite async database layer with ORM models and deduplication

from __future__ import annotations

import json
from datetime import datetime
from typing import AsyncGenerator, List, Optional

from loguru import logger
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    Index,
    Integer,
    String,
    Text,
    event,
    select,
    text,
)
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from app.models.schemas import (
    ConflictEvent,
    ConflictEventType,
    SeverityLevel,
)

# ---------------------------------------------------------------------------
# Engine / session factory
# ---------------------------------------------------------------------------

DATABASE_URL = "sqlite+aiosqlite:////data/conflictwatch.db"

engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    connect_args={"check_same_thread": False, "timeout": 30},
)

AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
    autocommit=False,
)


# ---------------------------------------------------------------------------
# ORM base
# ---------------------------------------------------------------------------

class Base(DeclarativeBase):
    pass


# ---------------------------------------------------------------------------
# ORM model: ConflictEventORM
# ---------------------------------------------------------------------------

class ConflictEventORM(Base):
    __tablename__ = "conflict_events"

    id = Column(String(36), primary_key=True, index=True)
    title = Column(String(1024), nullable=False)
    description = Column(Text, default="")
    event_type = Column(String(32), nullable=False, default="UNKNOWN")
    severity = Column(String(16), nullable=False, default="MEDIUM")

    # Geography
    country = Column(String(128), default="Unknown", index=True)
    region = Column(String(64), default="Unknown", index=True)
    city = Column(String(128), default="")
    lat = Column(Float, nullable=True)
    lon = Column(Float, nullable=True)

    # Source
    source_url = Column(String(2048), default="", unique=True, index=True)
    source_name = Column(String(256), default="")

    # Timestamps
    published_at = Column(DateTime, default=datetime.utcnow, index=True)
    ingested_at = Column(DateTime, default=datetime.utcnow, index=True)

    # NLP outputs stored as JSON strings
    entities_json = Column(Text, default="[]")
    keywords_json = Column(Text, default="[]")
    sentiment_score = Column(Float, default=0.0)
    escalation_score = Column(Float, default=0.0)

    verified = Column(Boolean, default=False)

    # Dedup tracking
    title_hash = Column(String(64), default="", index=True)

    __table_args__ = (
        Index("ix_country_published", "country", "published_at"),
        Index("ix_region_published", "region", "published_at"),
        Index("ix_event_type_published", "event_type", "published_at"),
        Index("ix_severity_published", "severity", "published_at"),
        Index("ix_escalation", "escalation_score"),
    )

    def to_schema(self) -> ConflictEvent:
        """Convert ORM row to Pydantic ConflictEvent."""
        return ConflictEvent(
            id=self.id,
            title=self.title,
            description=self.description or "",
            event_type=ConflictEventType(self.event_type),
            severity=SeverityLevel(self.severity),
            country=self.country or "Unknown",
            region=self.region or "Unknown",
            city=self.city or "",
            lat=self.lat,
            lon=self.lon,
            source_url=self.source_url or "",
            source_name=self.source_name or "",
            published_at=self.published_at or datetime.utcnow(),
            ingested_at=self.ingested_at or datetime.utcnow(),
            entities=json.loads(self.entities_json or "[]"),
            keywords=json.loads(self.keywords_json or "[]"),
            sentiment_score=self.sentiment_score or 0.0,
            escalation_score=self.escalation_score or 0.0,
            verified=self.verified or False,
        )


# ---------------------------------------------------------------------------
# Ingestion stats tracking
# ---------------------------------------------------------------------------

class IngestionStatsORM(Base):
    __tablename__ = "ingestion_stats"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_at = Column(DateTime, default=datetime.utcnow, index=True)
    source = Column(String(64), default="")
    new_events = Column(Integer, default=0)
    duplicates_skipped = Column(Integer, default=0)
    errors = Column(Integer, default=0)


# ---------------------------------------------------------------------------
# Database initialisation
# ---------------------------------------------------------------------------

async def init_db() -> None:
    """Create all tables. Called once at startup."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        # Enable WAL mode for better concurrent read performance on SQLite
        await conn.execute(text("PRAGMA journal_mode=WAL"))
        await conn.execute(text("PRAGMA synchronous=NORMAL"))
        await conn.execute(text("PRAGMA cache_size=-32000"))  # 32 MB
    logger.info("Database initialised (SQLite WAL mode)")


# ---------------------------------------------------------------------------
# Dependency injection helper
# ---------------------------------------------------------------------------

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


# ---------------------------------------------------------------------------
# Repository helpers
# ---------------------------------------------------------------------------

def _make_title_hash(title: str) -> str:
    """Normalise title for fuzzy dedup: lowercase, strip whitespace, first 80 chars."""
    import hashlib
    normalised = " ".join(title.lower().split())[:80]
    return hashlib.sha256(normalised.encode()).hexdigest()[:16]


async def upsert_event(session: AsyncSession, event: ConflictEvent) -> tuple[bool, bool]:
    """
    Insert a ConflictEvent into the DB.
    Returns (inserted: bool, duplicate: bool).
    Deduplication logic:
      1. If source_url already exists -> skip.
      2. If title_hash already exists in last 24h -> skip.
    """
    from datetime import timedelta

    # 1. URL dedup
    if event.source_url:
        existing = await session.execute(
            select(ConflictEventORM).where(
                ConflictEventORM.source_url == event.source_url
            )
        )
        if existing.scalar_one_or_none():
            return False, True  # duplicate

    # 2. Title hash dedup (last 24h)
    title_hash = _make_title_hash(event.title)
    cutoff = datetime.utcnow() - timedelta(hours=24)
    existing_hash = await session.execute(
        select(ConflictEventORM).where(
            ConflictEventORM.title_hash == title_hash,
            ConflictEventORM.ingested_at >= cutoff,
        )
    )
    if existing_hash.scalar_one_or_none():
        return False, True  # duplicate

    # 3. Insert
    orm_obj = ConflictEventORM(
        id=event.id,
        title=event.title,
        description=event.description,
        event_type=event.event_type.value,
        severity=event.severity.value,
        country=event.country,
        region=event.region,
        city=event.city,
        lat=event.lat,
        lon=event.lon,
        source_url=event.source_url,
        source_name=event.source_name,
        published_at=event.published_at,
        ingested_at=event.ingested_at,
        entities_json=json.dumps(event.entities),
        keywords_json=json.dumps(event.keywords),
        sentiment_score=event.sentiment_score,
        escalation_score=event.escalation_score,
        verified=event.verified,
        title_hash=title_hash,
    )
    session.add(orm_obj)
    return True, False


async def bulk_upsert_events(
    session: AsyncSession, events: List[ConflictEvent]
) -> tuple[int, int]:
    """Bulk insert a list of events. Returns (inserted, duplicates)."""
    inserted = 0
    duplicates = 0
    for event in events:
        ok, dup = await upsert_event(session, event)
        if ok:
            inserted += 1
        elif dup:
            duplicates += 1
    await session.commit()
    return inserted, duplicates


async def get_events_filtered(
    session: AsyncSession,
    region: Optional[str] = None,
    severity: Optional[str] = None,
    event_type: Optional[str] = None,
    hours: int = 24,
    limit: int = 100,
    offset: int = 0,
) -> List[ConflictEventORM]:
    from datetime import timedelta
    from sqlalchemy import desc

    cutoff = datetime.utcnow() - timedelta(hours=hours)
    stmt = select(ConflictEventORM).where(
        ConflictEventORM.published_at >= cutoff
    )
    if region and region != "ALL":
        stmt = stmt.where(ConflictEventORM.region == region)
    if severity:
        stmt = stmt.where(ConflictEventORM.severity == severity)
    if event_type:
        stmt = stmt.where(ConflictEventORM.event_type == event_type)

    stmt = stmt.order_by(desc(ConflictEventORM.published_at)).limit(limit).offset(offset)
    result = await session.execute(stmt)
    return result.scalars().all()


async def search_events(
    session: AsyncSession, query: str, limit: int = 50
) -> List[ConflictEventORM]:
    """Full-text search on title and description (SQLite LIKE)."""
    from sqlalchemy import or_

    pattern = f"%{query}%"
    stmt = (
        select(ConflictEventORM)
        .where(
            or_(
                ConflictEventORM.title.ilike(pattern),
                ConflictEventORM.description.ilike(pattern),
                ConflictEventORM.country.ilike(pattern),
            )
        )
        .order_by(ConflictEventORM.published_at.desc())
        .limit(limit)
    )
    result = await session.execute(stmt)
    return result.scalars().all()


async def get_hotspots(session: AsyncSession, hours: int = 24, top_n: int = 10) -> list:
    """Return top N countries by event count in the last `hours` hours."""
    from datetime import timedelta
    from sqlalchemy import func

    cutoff = datetime.utcnow() - timedelta(hours=hours)
    stmt = (
        select(
            ConflictEventORM.country,
            ConflictEventORM.region,
            func.count(ConflictEventORM.id).label("event_count"),
            func.avg(ConflictEventORM.lat).label("avg_lat"),
            func.avg(ConflictEventORM.lon).label("avg_lon"),
        )
        .where(ConflictEventORM.published_at >= cutoff)
        .group_by(ConflictEventORM.country, ConflictEventORM.region)
        .order_by(text("event_count DESC"))
        .limit(top_n)
    )
    result = await session.execute(stmt)
    return result.all()


async def get_timeline(
    session: AsyncSession, country: str, days: int = 30
) -> List[ConflictEventORM]:
    from datetime import timedelta

    cutoff = datetime.utcnow() - timedelta(days=days)
    stmt = (
        select(ConflictEventORM)
        .where(
            ConflictEventORM.country == country,
            ConflictEventORM.published_at >= cutoff,
        )
        .order_by(ConflictEventORM.published_at.asc())
    )
    result = await session.execute(stmt)
    return result.scalars().all()


async def delete_old_events(session: AsyncSession, days: int = 30) -> int:
    """Delete events older than `days` days. Returns count deleted."""
    from datetime import timedelta
    from sqlalchemy import delete

    cutoff = datetime.utcnow() - timedelta(days=days)
    stmt = delete(ConflictEventORM).where(ConflictEventORM.published_at < cutoff)
    result = await session.execute(stmt)
    await session.commit()
    return result.rowcount


async def record_ingestion_stats(
    session: AsyncSession,
    source: str,
    new_events: int,
    duplicates_skipped: int,
    errors: int = 0,
) -> None:
    stats = IngestionStatsORM(
        source=source,
        new_events=new_events,
        duplicates_skipped=duplicates_skipped,
        errors=errors,
    )
    session.add(stats)
    await session.commit()
