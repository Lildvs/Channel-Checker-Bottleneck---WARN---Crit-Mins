"""WARN Notice API endpoints for layoff data access and visualization.

Provides endpoints for:
- Paginated notice listings with filters
- Aggregate statistics
- Time series trends
- Geographic (state-level) breakdowns
- Sector analysis
- Company size distributions
"""

from datetime import datetime, timedelta, UTC
from typing import Any

import structlog
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import Integer, func, select, and_
from src.storage.models import WARNNotice
from src.storage.timescale import get_db

logger = structlog.get_logger()
router = APIRouter(prefix="/warn", tags=["warn"])



class WARNNoticeResponse(BaseModel):
    """Response model for a single WARN notice."""

    id: str
    company_name: str
    state: str
    city: str | None
    county: str | None
    notice_date: str
    effective_date: str | None
    employees_affected: int
    layoff_type: str
    naics_code: str | None
    naics_description: str | None
    sector_category: str | None
    is_temporary: bool
    is_closure: bool
    reason: str | None


class WARNNoticeListResponse(BaseModel):
    """Response model for paginated WARN notices."""

    notices: list[WARNNoticeResponse]
    total: int
    page: int
    page_size: int
    has_more: bool


class WARNStatsResponse(BaseModel):
    """Response model for aggregate WARN statistics."""

    total_notices: int
    total_employees_affected: int
    states_reporting: int
    avg_employees_per_notice: float
    closures_count: int
    temporary_count: int
    date_range: dict[str, str | None]


class WARNTrendPoint(BaseModel):
    """Single data point in a trend."""

    period: str
    notice_count: int
    employees_affected: int


class WARNTrendsResponse(BaseModel):
    """Response model for WARN trends."""

    granularity: str  # daily, weekly, monthly
    data: list[WARNTrendPoint]


class StateBreakdown(BaseModel):
    """WARN data for a single state."""

    state: str
    state_name: str
    notice_count: int
    employees_affected: int
    pct_of_total: float


class WARNByStateResponse(BaseModel):
    """Response model for state-level breakdown."""

    states: list[StateBreakdown]
    total_states: int


class SectorBreakdown(BaseModel):
    """WARN data for a single sector."""

    sector: str
    notice_count: int
    employees_affected: int
    pct_of_total: float
    avg_employees_per_notice: float


class WARNBySectorResponse(BaseModel):
    """Response model for sector breakdown."""

    sectors: list[SectorBreakdown]


class SizeBucket(BaseModel):
    """Company size distribution bucket."""

    min_employees: int
    max_employees: int | None
    label: str
    count: int
    pct_of_total: float


class WARNSizeDistributionResponse(BaseModel):
    """Response model for company size distribution."""

    buckets: list[SizeBucket]
    total_notices: int


# State name mapping
STATE_NAMES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
    "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
    "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
    "WI": "Wisconsin", "WY": "Wyoming", "DC": "District of Columbia",
    "PR": "Puerto Rico", "VI": "Virgin Islands", "GU": "Guam",
}



def _build_date_filter(
    start_date: datetime | None,
    end_date: datetime | None,
) -> list:
    """Build SQLAlchemy date filter conditions."""
    conditions = []
    if start_date:
        conditions.append(WARNNotice.notice_date >= start_date)
    if end_date:
        conditions.append(WARNNotice.notice_date <= end_date)
    return conditions



@router.get("/notices", response_model=WARNNoticeListResponse)
async def get_warn_notices(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=200, description="Items per page"),
    state: str | None = Query(None, description="Filter by state code (e.g., CA, TX)"),
    sector: str | None = Query(None, description="Filter by sector category"),
    min_employees: int | None = Query(None, ge=0, description="Minimum employees affected"),
    start_date: datetime | None = Query(None, description="Start date filter"),
    end_date: datetime | None = Query(None, description="End date filter"),
    layoff_type: str | None = Query(None, description="Filter by layoff type"),
    search: str | None = Query(None, description="Search company name"),
) -> WARNNoticeListResponse:
    """Get paginated list of WARN notices with optional filters.

    Returns:
        Paginated list of WARN notices
    """
    try:
        db = get_db()
        async with db.session() as session:
            query = select(WARNNotice)
            count_query = select(func.count(WARNNotice.id))

            conditions = []

            if state:
                conditions.append(WARNNotice.state == state.upper())
            if sector:
                conditions.append(WARNNotice.sector_category == sector)
            if min_employees is not None:
                conditions.append(WARNNotice.employees_affected >= min_employees)
            if layoff_type:
                conditions.append(WARNNotice.layoff_type == layoff_type)
            if search:
                conditions.append(WARNNotice.company_name.ilike(f"%{search}%"))

            conditions.extend(_build_date_filter(start_date, end_date))

            if conditions:
                query = query.where(and_(*conditions))
                count_query = count_query.where(and_(*conditions))

            total_result = await session.execute(count_query)
            total = total_result.scalar() or 0

            offset = (page - 1) * page_size
            query = (
                query
                .order_by(WARNNotice.notice_date.desc())
                .offset(offset)
                .limit(page_size)
            )

            result = await session.execute(query)
            notices = result.scalars().all()

            notice_responses = [
                WARNNoticeResponse(
                    id=str(n.id),
                    company_name=n.company_name,
                    state=n.state,
                    city=n.city,
                    county=n.county,
                    notice_date=n.notice_date.isoformat(),
                    effective_date=n.effective_date.isoformat() if n.effective_date else None,
                    employees_affected=n.employees_affected,
                    layoff_type=n.layoff_type,
                    naics_code=n.naics_code,
                    naics_description=n.naics_description,
                    sector_category=n.sector_category,
                    is_temporary=n.is_temporary,
                    is_closure=n.is_closure,
                    reason=n.reason,
                )
                for n in notices
            ]

            return WARNNoticeListResponse(
                notices=notice_responses,
                total=total,
                page=page,
                page_size=page_size,
                has_more=(offset + len(notices)) < total,
            )

    except Exception as e:
        logger.error("Failed to get WARN notices", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/notices/stats", response_model=WARNStatsResponse)
async def get_warn_stats(
    start_date: datetime | None = Query(None, description="Start date filter"),
    end_date: datetime | None = Query(None, description="End date filter"),
    state: str | None = Query(None, description="Filter by state"),
) -> WARNStatsResponse:
    """Get aggregate WARN statistics.

    Returns:
        Summary statistics for WARN notices
    """
    try:
        db = get_db()
        async with db.session() as session:
            conditions = _build_date_filter(start_date, end_date)
            if state:
                conditions.append(WARNNotice.state == state.upper())

            where_clause = and_(*conditions) if conditions else True

            stats_query = select(
                func.count(WARNNotice.id).label("total_notices"),
                func.sum(WARNNotice.employees_affected).label("total_employees"),
                func.count(func.distinct(WARNNotice.state)).label("states"),
                func.avg(WARNNotice.employees_affected).label("avg_employees"),
                func.sum(func.cast(WARNNotice.is_closure, Integer)).label("closures"),
                func.sum(func.cast(WARNNotice.is_temporary, Integer)).label("temporary"),
                func.min(WARNNotice.notice_date).label("min_date"),
                func.max(WARNNotice.notice_date).label("max_date"),
            ).where(where_clause)

            result = await session.execute(stats_query)
            row = result.one()

            return WARNStatsResponse(
                total_notices=row.total_notices or 0,
                total_employees_affected=row.total_employees or 0,
                states_reporting=row.states or 0,
                avg_employees_per_notice=float(row.avg_employees or 0),
                closures_count=row.closures or 0,
                temporary_count=row.temporary or 0,
                date_range={
                    "start": row.min_date.isoformat() if row.min_date else None,
                    "end": row.max_date.isoformat() if row.max_date else None,
                },
            )

    except Exception as e:
        logger.error("Failed to get WARN stats", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/notices/trends", response_model=WARNTrendsResponse)
async def get_warn_trends(
    granularity: str = Query("monthly", description="Aggregation: daily, weekly, monthly"),
    start_date: datetime | None = Query(None, description="Start date"),
    end_date: datetime | None = Query(None, description="End date"),
    state: str | None = Query(None, description="Filter by state"),
) -> WARNTrendsResponse:
    """Get WARN notice trends over time.

    Returns:
        Time series of notice counts and employees affected
    """
    if granularity not in ("daily", "weekly", "monthly"):
        raise HTTPException(
            status_code=400,
            detail="Granularity must be one of: daily, weekly, monthly",
        )

    try:
        db = get_db()
        async with db.session() as session:
            conditions = _build_date_filter(start_date, end_date)
            if state:
                conditions.append(WARNNotice.state == state.upper())

            where_clause = and_(*conditions) if conditions else True

            if granularity == "daily":
                date_col = func.date_trunc("day", WARNNotice.notice_date)
            elif granularity == "weekly":
                date_col = func.date_trunc("week", WARNNotice.notice_date)
            else:  # monthly
                date_col = func.date_trunc("month", WARNNotice.notice_date)

            query = (
                select(
                    date_col.label("period"),
                    func.count(WARNNotice.id).label("notice_count"),
                    func.sum(WARNNotice.employees_affected).label("employees_affected"),
                )
                .where(where_clause)
                .group_by(date_col)
                .order_by(date_col)
            )

            result = await session.execute(query)
            rows = result.all()

            data = [
                WARNTrendPoint(
                    period=row.period.isoformat() if row.period else "",
                    notice_count=row.notice_count,
                    employees_affected=row.employees_affected or 0,
                )
                for row in rows
            ]

            return WARNTrendsResponse(
                granularity=granularity,
                data=data,
            )

    except Exception as e:
        logger.error("Failed to get WARN trends", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/notices/by-state", response_model=WARNByStateResponse)
async def get_warn_by_state(
    start_date: datetime | None = Query(None, description="Start date filter"),
    end_date: datetime | None = Query(None, description="End date filter"),
) -> WARNByStateResponse:
    """Get WARN notices aggregated by state.

    Returns:
        Breakdown of notices and employees by state
    """
    try:
        db = get_db()
        async with db.session() as session:
            conditions = _build_date_filter(start_date, end_date)
            where_clause = and_(*conditions) if conditions else True

            total_query = select(
                func.sum(WARNNotice.employees_affected)
            ).where(where_clause)
            total_result = await session.execute(total_query)
            total_employees = total_result.scalar() or 1

            query = (
                select(
                    WARNNotice.state,
                    func.count(WARNNotice.id).label("notice_count"),
                    func.sum(WARNNotice.employees_affected).label("employees_affected"),
                )
                .where(where_clause)
                .group_by(WARNNotice.state)
                .order_by(func.sum(WARNNotice.employees_affected).desc())
            )

            result = await session.execute(query)
            rows = result.all()

            states = [
                StateBreakdown(
                    state=row.state,
                    state_name=STATE_NAMES.get(row.state, row.state),
                    notice_count=row.notice_count,
                    employees_affected=row.employees_affected or 0,
                    pct_of_total=round((row.employees_affected or 0) / total_employees * 100, 2),
                )
                for row in rows
            ]

            return WARNByStateResponse(
                states=states,
                total_states=len(states),
            )

    except Exception as e:
        logger.error("Failed to get WARN by state", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/notices/by-sector", response_model=WARNBySectorResponse)
async def get_warn_by_sector(
    start_date: datetime | None = Query(None, description="Start date filter"),
    end_date: datetime | None = Query(None, description="End date filter"),
    state: str | None = Query(None, description="Filter by state"),
) -> WARNBySectorResponse:
    """Get WARN notices aggregated by sector.

    Returns:
        Breakdown of notices and employees by sector category
    """
    try:
        db = get_db()
        async with db.session() as session:
            conditions = _build_date_filter(start_date, end_date)
            if state:
                conditions.append(WARNNotice.state == state.upper())
            where_clause = and_(*conditions) if conditions else True

            total_query = select(
                func.sum(WARNNotice.employees_affected)
            ).where(where_clause)
            total_result = await session.execute(total_query)
            total_employees = total_result.scalar() or 1

            query = (
                select(
                    func.coalesce(WARNNotice.sector_category, "Unknown").label("sector"),
                    func.count(WARNNotice.id).label("notice_count"),
                    func.sum(WARNNotice.employees_affected).label("employees_affected"),
                    func.avg(WARNNotice.employees_affected).label("avg_employees"),
                )
                .where(where_clause)
                .group_by(WARNNotice.sector_category)
                .order_by(func.sum(WARNNotice.employees_affected).desc())
            )

            result = await session.execute(query)
            rows = result.all()

            sectors = [
                SectorBreakdown(
                    sector=row.sector,
                    notice_count=row.notice_count,
                    employees_affected=row.employees_affected or 0,
                    pct_of_total=round((row.employees_affected or 0) / total_employees * 100, 2),
                    avg_employees_per_notice=round(float(row.avg_employees or 0), 1),
                )
                for row in rows
            ]

            return WARNBySectorResponse(sectors=sectors)

    except Exception as e:
        logger.error("Failed to get WARN by sector", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/notices/company-sizes", response_model=WARNSizeDistributionResponse)
async def get_warn_company_sizes(
    start_date: datetime | None = Query(None, description="Start date filter"),
    end_date: datetime | None = Query(None, description="End date filter"),
    state: str | None = Query(None, description="Filter by state"),
) -> WARNSizeDistributionResponse:
    """Get distribution of WARN notices by company size (employees affected).

    Returns:
        Histogram-style buckets of notices by employee count
    """
    try:
        db = get_db()
        async with db.session() as session:
            conditions = _build_date_filter(start_date, end_date)
            if state:
                conditions.append(WARNNotice.state == state.upper())
            where_clause = and_(*conditions) if conditions else True

            total_query = select(func.count(WARNNotice.id)).where(where_clause)
            total_result = await session.execute(total_query)
            total_notices = total_result.scalar() or 1

            buckets_config = [
                (0, 50, "Small (1-50)"),
                (50, 100, "Medium-Small (51-100)"),
                (100, 250, "Medium (101-250)"),
                (250, 500, "Medium-Large (251-500)"),
                (500, 1000, "Large (501-1000)"),
                (1000, None, "Very Large (1000+)"),
            ]

            buckets = []
            for min_emp, max_emp, label in buckets_config:
                bucket_conditions = list(conditions)
                bucket_conditions.append(WARNNotice.employees_affected >= min_emp)
                if max_emp is not None:
                    bucket_conditions.append(WARNNotice.employees_affected < max_emp)

                query = select(func.count(WARNNotice.id)).where(and_(*bucket_conditions))
                result = await session.execute(query)
                count = result.scalar() or 0

                buckets.append(
                    SizeBucket(
                        min_employees=min_emp,
                        max_employees=max_emp,
                        label=label,
                        count=count,
                        pct_of_total=round(count / total_notices * 100, 2),
                    )
                )

            return WARNSizeDistributionResponse(
                buckets=buckets,
                total_notices=total_notices,
            )

    except Exception as e:
        logger.error("Failed to get WARN company sizes", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))
