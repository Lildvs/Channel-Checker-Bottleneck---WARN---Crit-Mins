"""API routes for the Reports tab -- system logs, errors, and operational events."""

from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Query
from pydantic import BaseModel, Field
from sqlalchemy import desc, func, select, and_

from src.storage.models import CollectionJob, SystemLog
from src.storage.timescale import get_db

router = APIRouter(prefix="/reports", tags=["reports"])


class LogEntry(BaseModel):
    """A single system log entry."""

    id: int
    timestamp: str
    level: str
    logger_name: str | None = None
    event: str
    source_module: str | None = None
    extra_data: dict = Field(default_factory=dict)


class LogsResponse(BaseModel):
    """Paginated log entries."""

    logs: list[LogEntry]
    total: int
    page: int
    page_size: int
    has_more: bool


class LogStats(BaseModel):
    """Summary statistics for logs."""

    total_logs: int
    error_count: int
    warning_count: int
    critical_count: int
    info_count: int
    recent_errors: int  # Last 24 hours
    top_sources: list[dict]
    oldest_log: str | None = None
    newest_log: str | None = None


class CollectorJobEntry(BaseModel):
    """A collection job entry."""

    id: str
    collector_name: str
    started_at: str
    completed_at: str | None = None
    status: str
    records_collected: int
    error_message: str | None = None
    duration_seconds: float | None = None


class CollectorJobsResponse(BaseModel):
    """Paginated collector job history."""

    jobs: list[CollectorJobEntry]
    total: int
    page: int
    page_size: int
    has_more: bool


@router.get("/logs", response_model=LogsResponse)
async def get_system_logs(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    level: str | None = Query(default=None, description="Filter by level: WARNING, ERROR, CRITICAL, INFO"),
    source: str | None = Query(default=None, description="Filter by source module (partial match)"),
    search: str | None = Query(default=None, description="Search in event text"),
    start_date: str | None = Query(default=None, description="Start date ISO format"),
    end_date: str | None = Query(default=None, description="End date ISO format"),
) -> LogsResponse:
    """Get paginated system logs with optional filters."""
    db = get_db()

    async with db.session() as session:
        conditions = []

        if level:
            conditions.append(SystemLog.level == level.upper())
        if source:
            conditions.append(SystemLog.source_module.ilike(f"%{source}%"))
        if search:
            conditions.append(SystemLog.event.ilike(f"%{search}%"))
        if start_date:
            conditions.append(SystemLog.timestamp >= datetime.fromisoformat(start_date))
        if end_date:
            conditions.append(SystemLog.timestamp <= datetime.fromisoformat(end_date))

        where_clause = and_(*conditions) if conditions else True

        count_q = select(func.count()).select_from(SystemLog).where(where_clause)
        total = (await session.execute(count_q)).scalar() or 0

        offset = (page - 1) * page_size
        data_q = (
            select(SystemLog)
            .where(where_clause)
            .order_by(desc(SystemLog.timestamp))
            .offset(offset)
            .limit(page_size)
        )
        rows = (await session.execute(data_q)).scalars().all()

        logs = [
            LogEntry(
                id=row.id,
                timestamp=row.timestamp.isoformat(),
                level=row.level,
                logger_name=row.logger_name,
                event=row.event,
                source_module=row.source_module,
                extra_data=row.extra_data or {},
            )
            for row in rows
        ]

    return LogsResponse(
        logs=logs,
        total=total,
        page=page,
        page_size=page_size,
        has_more=(offset + page_size) < total,
    )


@router.get("/logs/stats", response_model=LogStats)
async def get_log_stats() -> LogStats:
    """Get summary statistics for system logs."""
    db = get_db()

    async with db.session() as session:
        level_counts = (
            await session.execute(
                select(SystemLog.level, func.count())
                .group_by(SystemLog.level)
            )
        ).all()

        counts = {row[0]: row[1] for row in level_counts}
        total = sum(counts.values())

        cutoff = datetime.now(UTC) - timedelta(hours=24)
        recent_q = select(func.count()).select_from(SystemLog).where(
            and_(
                SystemLog.level.in_(["ERROR", "CRITICAL"]),
                SystemLog.timestamp >= cutoff,
            )
        )
        recent_errors = (await session.execute(recent_q)).scalar() or 0

        top_q = (
            select(SystemLog.source_module, func.count().label("cnt"))
            .where(SystemLog.source_module.isnot(None))
            .group_by(SystemLog.source_module)
            .order_by(desc("cnt"))
            .limit(10)
        )
        top_rows = (await session.execute(top_q)).all()
        top_sources = [{"source": r[0], "count": r[1]} for r in top_rows]

        range_q = select(
            func.min(SystemLog.timestamp), func.max(SystemLog.timestamp)
        )
        range_row = (await session.execute(range_q)).one_or_none()
        oldest = range_row[0].isoformat() if range_row and range_row[0] else None
        newest = range_row[1].isoformat() if range_row and range_row[1] else None

    return LogStats(
        total_logs=total,
        error_count=counts.get("ERROR", 0),
        warning_count=counts.get("WARNING", 0),
        critical_count=counts.get("CRITICAL", 0),
        info_count=counts.get("INFO", 0),
        recent_errors=recent_errors,
        top_sources=top_sources,
        oldest_log=oldest,
        newest_log=newest,
    )


@router.get("/collection-jobs", response_model=CollectorJobsResponse)
async def get_collection_jobs(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    collector: str | None = Query(default=None, description="Filter by collector name"),
    status: str | None = Query(default=None, description="Filter by status: running, completed, failed"),
) -> CollectorJobsResponse:
    """Get paginated collection job history."""
    db = get_db()

    async with db.session() as session:
        conditions = []
        if collector:
            conditions.append(CollectionJob.collector_name == collector)
        if status:
            conditions.append(CollectionJob.status == status)

        where_clause = and_(*conditions) if conditions else True

        count_q = select(func.count()).select_from(CollectionJob).where(where_clause)
        total = (await session.execute(count_q)).scalar() or 0

        offset = (page - 1) * page_size
        data_q = (
            select(CollectionJob)
            .where(where_clause)
            .order_by(desc(CollectionJob.started_at))
            .offset(offset)
            .limit(page_size)
        )
        rows = (await session.execute(data_q)).scalars().all()

        jobs = []
        for row in rows:
            duration = None
            if row.completed_at and row.started_at:
                duration = (row.completed_at - row.started_at).total_seconds()
            jobs.append(
                CollectorJobEntry(
                    id=str(row.id),
                    collector_name=row.collector_name,
                    started_at=row.started_at.isoformat(),
                    completed_at=row.completed_at.isoformat() if row.completed_at else None,
                    status=row.status,
                    records_collected=row.records_collected,
                    error_message=row.error_message,
                    duration_seconds=duration,
                )
            )

    return CollectorJobsResponse(
        jobs=jobs,
        total=total,
        page=page,
        page_size=page_size,
        has_more=(offset + page_size) < total,
    )
