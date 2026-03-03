"""Research Paper API endpoints for browsing and analyzing collected papers.

Provides endpoints for:
- Paginated paper listings with filters
- Topic analysis and distribution
- Contrarian/emerging paper discovery
- Research signal trends
"""

from datetime import datetime, timedelta, UTC
from typing import Any
from uuid import UUID

import structlog
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import func, select, and_, or_, desc
from src.storage.models import ResearchPaper, ResearchSignal
from src.storage.timescale import get_db

logger = structlog.get_logger()
router = APIRouter(prefix="/research", tags=["research"])


class PaperResponse(BaseModel):
    """Response model for a research paper."""

    id: str
    doi: str | None
    arxiv_id: str | None
    title: str
    abstract: str | None
    authors: list[str]
    institutions: list[str]
    published_date: str
    source: str
    topics: list[str]
    quick_score: float
    citation_count: int
    reference_count: int
    research_type: str
    contrarian_confidence: float
    url: str
    pdf_url: str | None
    code_url: str | None


class PaperListResponse(BaseModel):
    """Response model for paginated paper list."""

    papers: list[PaperResponse]
    total: int
    page: int
    page_size: int
    has_more: bool


class TopicStats(BaseModel):
    """Statistics for a single topic."""

    topic: str
    paper_count: int
    avg_score: float
    contrarian_count: int
    emerging_count: int
    recent_count: int  # Last 7 days


class TopicListResponse(BaseModel):
    """Response model for topic list."""

    topics: list[TopicStats]
    total_papers: int


class ResearchStatsResponse(BaseModel):
    """Dashboard summary statistics."""

    total_papers: int
    papers_last_7_days: int
    papers_last_30_days: int
    avg_quick_score: float
    contrarian_count: int
    emerging_count: int
    consensus_count: int
    topics_covered: int
    sources: dict[str, int]
    top_topics: list[dict[str, Any]]


class SignalPoint(BaseModel):
    """Single point in research signal time series."""

    timestamp: str
    topic: str
    paper_count: int
    new_paper_count: int
    contrarian_count: int
    emerging_count: int
    avg_quick_score: float


class SignalsResponse(BaseModel):
    """Response model for research signals."""

    signals: list[SignalPoint]
    topics: list[str]


def _paper_to_response(paper: ResearchPaper) -> PaperResponse:
    """Convert ORM model to response."""
    return PaperResponse(
        id=str(paper.id),
        doi=paper.doi,
        arxiv_id=paper.arxiv_id,
        title=paper.title,
        abstract=paper.abstract,
        authors=paper.authors or [],
        institutions=paper.institutions or [],
        published_date=paper.published_date.isoformat(),
        source=paper.source,
        topics=paper.topics or [],
        quick_score=paper.quick_score,
        citation_count=paper.citation_count,
        reference_count=paper.reference_count,
        research_type=paper.research_type,
        contrarian_confidence=paper.contrarian_confidence,
        url=paper.url,
        pdf_url=paper.pdf_url,
        code_url=paper.code_url,
    )


@router.get("/papers", response_model=PaperListResponse)
async def get_papers(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Items per page"),
    topic: str | None = Query(None, description="Filter by topic"),
    research_type: str | None = Query(None, description="Filter by type: consensus/emerging/contrarian"),
    min_score: float | None = Query(None, ge=0, le=1, description="Minimum quick_score"),
    start_date: datetime | None = Query(None, description="Start date filter"),
    end_date: datetime | None = Query(None, description="End date filter"),
    search: str | None = Query(None, description="Search in title/abstract"),
    has_code: bool | None = Query(None, description="Filter papers with code repos"),
    source: str | None = Query(None, description="Filter by source"),
    sort_by: str = Query("published_date", description="Sort field"),
    sort_order: str = Query("desc", description="Sort order: asc/desc"),
) -> PaperListResponse:
    """Get paginated list of research papers with filters."""
    try:
        db = get_db()
        async with db.session() as session:
            query = select(ResearchPaper)
            count_query = select(func.count(ResearchPaper.id))

            conditions = []

            if topic:
                conditions.append(ResearchPaper.topics.any(topic))
            if research_type:
                conditions.append(ResearchPaper.research_type == research_type)
            if min_score is not None:
                conditions.append(ResearchPaper.quick_score >= min_score)
            if start_date:
                conditions.append(ResearchPaper.published_date >= start_date)
            if end_date:
                conditions.append(ResearchPaper.published_date <= end_date)
            if has_code is True:
                conditions.append(ResearchPaper.code_url.isnot(None))
            if source:
                conditions.append(ResearchPaper.source == source)
            if search:
                search_filter = or_(
                    ResearchPaper.title.ilike(f"%{search}%"),
                    ResearchPaper.abstract.ilike(f"%{search}%"),
                )
                conditions.append(search_filter)

            if conditions:
                query = query.where(and_(*conditions))
                count_query = count_query.where(and_(*conditions))

            total_result = await session.execute(count_query)
            total = total_result.scalar() or 0

            sort_column = getattr(ResearchPaper, sort_by, ResearchPaper.published_date)
            if sort_order == "desc":
                query = query.order_by(desc(sort_column))
            else:
                query = query.order_by(sort_column)

            offset = (page - 1) * page_size
            query = query.offset(offset).limit(page_size)

            result = await session.execute(query)
            papers = result.scalars().all()

            return PaperListResponse(
                papers=[_paper_to_response(p) for p in papers],
                total=total,
                page=page,
                page_size=page_size,
                has_more=(offset + len(papers)) < total,
            )

    except Exception as e:
        logger.error("Failed to get papers", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/papers/{paper_id}", response_model=PaperResponse)
async def get_paper(paper_id: UUID) -> PaperResponse:
    """Get a single paper by ID."""
    try:
        db = get_db()
        async with db.session() as session:
            result = await session.execute(
                select(ResearchPaper).where(ResearchPaper.id == paper_id)
            )
            paper = result.scalar_one_or_none()

            if not paper:
                raise HTTPException(status_code=404, detail="Paper not found")

            return _paper_to_response(paper)

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get paper", paper_id=str(paper_id), error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/topics", response_model=TopicListResponse)
async def get_topics() -> TopicListResponse:
    """Get all topics with paper counts and statistics."""
    try:
        db = get_db()
        async with db.session() as session:
            result = await session.execute(select(ResearchPaper))
            papers = result.scalars().all()

            topic_stats: dict[str, dict[str, Any]] = {}
            seven_days_ago = datetime.now(UTC) - timedelta(days=7)

            for paper in papers:
                for topic in paper.topics or []:
                    if topic not in topic_stats:
                        topic_stats[topic] = {
                            "topic": topic,
                            "paper_count": 0,
                            "total_score": 0.0,
                            "contrarian_count": 0,
                            "emerging_count": 0,
                            "recent_count": 0,
                        }

                    stats = topic_stats[topic]
                    stats["paper_count"] += 1
                    stats["total_score"] += paper.quick_score

                    if paper.research_type == "contrarian":
                        stats["contrarian_count"] += 1
                    elif paper.research_type == "emerging":
                        stats["emerging_count"] += 1

                    if paper.published_date.replace(tzinfo=UTC) > seven_days_ago:
                        stats["recent_count"] += 1

            topics = []
            for stats in topic_stats.values():
                topics.append(
                    TopicStats(
                        topic=stats["topic"],
                        paper_count=stats["paper_count"],
                        avg_score=stats["total_score"] / stats["paper_count"] if stats["paper_count"] > 0 else 0,
                        contrarian_count=stats["contrarian_count"],
                        emerging_count=stats["emerging_count"],
                        recent_count=stats["recent_count"],
                    )
                )

            topics.sort(key=lambda x: x.paper_count, reverse=True)

            return TopicListResponse(
                topics=topics,
                total_papers=len(papers),
            )

    except Exception as e:
        logger.error("Failed to get topics", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/topics/{topic}/papers", response_model=PaperListResponse)
async def get_papers_by_topic(
    topic: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
) -> PaperListResponse:
    """Get papers for a specific topic."""
    return await get_papers(page=page, page_size=page_size, topic=topic)


@router.get("/contrarian", response_model=PaperListResponse)
async def get_contrarian_papers(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    include_emerging: bool = Query(True, description="Include emerging papers"),
) -> PaperListResponse:
    """Get contrarian and emerging papers only."""
    try:
        db = get_db()
        async with db.session() as session:
            if include_emerging:
                type_filter = ResearchPaper.research_type.in_(["contrarian", "emerging"])
            else:
                type_filter = ResearchPaper.research_type == "contrarian"

            count_query = select(func.count(ResearchPaper.id)).where(type_filter)
            total_result = await session.execute(count_query)
            total = total_result.scalar() or 0

            offset = (page - 1) * page_size
            query = (
                select(ResearchPaper)
                .where(type_filter)
                .order_by(desc(ResearchPaper.contrarian_confidence), desc(ResearchPaper.published_date))
                .offset(offset)
                .limit(page_size)
            )

            result = await session.execute(query)
            papers = result.scalars().all()

            return PaperListResponse(
                papers=[_paper_to_response(p) for p in papers],
                total=total,
                page=page,
                page_size=page_size,
                has_more=(offset + len(papers)) < total,
            )

    except Exception as e:
        logger.error("Failed to get contrarian papers", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/signals", response_model=SignalsResponse)
async def get_research_signals(
    topic: str | None = Query(None, description="Filter by topic"),
    days: int = Query(30, ge=1, le=365, description="Number of days to include"),
) -> SignalsResponse:
    """Get research signal time series."""
    try:
        db = get_db()
        async with db.session() as session:
            cutoff = datetime.now(UTC) - timedelta(days=days)

            conditions = [ResearchSignal.timestamp >= cutoff]
            if topic:
                conditions.append(ResearchSignal.topic == topic)

            query = (
                select(ResearchSignal)
                .where(and_(*conditions))
                .order_by(ResearchSignal.timestamp)
            )

            result = await session.execute(query)
            signals = result.scalars().all()

            topics_set = set()
            signal_points = []

            for signal in signals:
                topics_set.add(signal.topic)
                signal_points.append(
                    SignalPoint(
                        timestamp=signal.timestamp.isoformat(),
                        topic=signal.topic,
                        paper_count=signal.paper_count,
                        new_paper_count=signal.new_paper_count,
                        contrarian_count=signal.contrarian_count,
                        emerging_count=signal.emerging_count,
                        avg_quick_score=signal.avg_quick_score,
                    )
                )

            return SignalsResponse(
                signals=signal_points,
                topics=sorted(topics_set),
            )

    except Exception as e:
        logger.error("Failed to get research signals", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats", response_model=ResearchStatsResponse)
async def get_research_stats() -> ResearchStatsResponse:
    """Get dashboard summary statistics."""
    try:
        db = get_db()
        async with db.session() as session:
            result = await session.execute(select(ResearchPaper))
            papers = list(result.scalars().all())

            if not papers:
                return ResearchStatsResponse(
                    total_papers=0,
                    papers_last_7_days=0,
                    papers_last_30_days=0,
                    avg_quick_score=0.0,
                    contrarian_count=0,
                    emerging_count=0,
                    consensus_count=0,
                    topics_covered=0,
                    sources={},
                    top_topics=[],
                )

            now = datetime.now(UTC)
            seven_days_ago = now - timedelta(days=7)
            thirty_days_ago = now - timedelta(days=30)

            papers_7d = 0
            papers_30d = 0
            total_score = 0.0
            contrarian = 0
            emerging = 0
            consensus = 0
            sources: dict[str, int] = {}
            topic_counts: dict[str, int] = {}

            for paper in papers:
                pub_date = paper.published_date.replace(tzinfo=UTC) if paper.published_date.tzinfo is None else paper.published_date

                if pub_date > seven_days_ago:
                    papers_7d += 1
                if pub_date > thirty_days_ago:
                    papers_30d += 1

                total_score += paper.quick_score

                if paper.research_type == "contrarian":
                    contrarian += 1
                elif paper.research_type == "emerging":
                    emerging += 1
                else:
                    consensus += 1

                sources[paper.source] = sources.get(paper.source, 0) + 1

                for topic in paper.topics or []:
                    topic_counts[topic] = topic_counts.get(topic, 0) + 1

            sorted_topics = sorted(topic_counts.items(), key=lambda x: x[1], reverse=True)
            top_topics = [{"topic": t, "count": c} for t, c in sorted_topics[:10]]

            return ResearchStatsResponse(
                total_papers=len(papers),
                papers_last_7_days=papers_7d,
                papers_last_30_days=papers_30d,
                avg_quick_score=total_score / len(papers),
                contrarian_count=contrarian,
                emerging_count=emerging,
                consensus_count=consensus,
                topics_covered=len(topic_counts),
                sources=sources,
                top_topics=top_topics,
            )

    except Exception as e:
        logger.error("Failed to get research stats", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))
