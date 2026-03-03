"""Data-related API endpoints."""

from datetime import datetime, timedelta
from typing import Any

import structlog
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from src.data_ingestion.scheduler import get_scheduler
from src.storage.redis_cache import get_cache
from src.storage.timescale import get_db

logger = structlog.get_logger()
router = APIRouter()


class DataPointResponse(BaseModel):
    """Response model for a data point.
    
    Note: The internal DB column is 'extra_data' but the API exposes it as
    'metadata' for backward compatibility. The validation_alias allows
    accepting 'extra_data' from the DB while serializing as 'metadata'.
    """

    id: str
    source_id: str
    series_id: str
    timestamp: str
    value: float | None
    unit: str | None
    quality_score: float
    is_preliminary: bool
    metadata: dict[str, Any] = Field(default_factory=dict, validation_alias="extra_data")


class TimeSeriesResponse(BaseModel):
    """Response model for time series data."""

    series_id: str
    source_id: str | None
    data_points: list[DataPointResponse]
    count: int
    start_date: str | None
    end_date: str | None


class SeriesStatistics(BaseModel):
    """Response model for series statistics."""

    series_id: str
    count: int
    mean: float | None
    stddev: float | None
    min: float | None
    max: float | None
    median: float | None


class CollectorStatus(BaseModel):
    """Response model for collector status."""

    name: str
    last_run: str | None
    next_run: str | None
    status: str
    records_collected: int | None


@router.get("/series/{series_id}", response_model=TimeSeriesResponse)
async def get_series_data(
    series_id: str,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    limit: int = Query(1000, ge=1, le=10000),
) -> TimeSeriesResponse:
    """Get time series data for a specific series.

    Args:
        series_id: ID of the data series
        start_date: Optional start date filter
        end_date: Optional end date filter
        limit: Maximum number of data points

    Returns:
        Time series data
    """
    try:
        db = get_db()
        data = await db.get_series_data(
            series_id=series_id,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )

        if not data:
            raise HTTPException(
                status_code=404,
                detail=f"No data found for series {series_id}",
            )

        data_points = [
            DataPointResponse(
                id=str(d["id"]),
                source_id=d["source_id"],
                series_id=d["series_id"],
                timestamp=d["timestamp"].isoformat() if isinstance(d["timestamp"], datetime) else d["timestamp"],
                value=d["value"],
                unit=d.get("unit"),
                quality_score=d.get("quality_score", 1.0),
                is_preliminary=d.get("is_preliminary", False),
                metadata=d.get("extra_data", {}),  # DB column is 'extra_data'
            )
            for d in data
        ]

        return TimeSeriesResponse(
            series_id=series_id,
            source_id=data[0]["source_id"] if data else None,
            data_points=data_points,
            count=len(data_points),
            start_date=data_points[-1].timestamp if data_points else None,
            end_date=data_points[0].timestamp if data_points else None,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get series data", series_id=series_id, error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/series/{series_id}/latest")
async def get_latest_value(series_id: str) -> dict[str, Any]:
    """Get the most recent value for a series.

    Args:
        series_id: ID of the data series

    Returns:
        Latest data point
    """
    try:
        cache = get_cache()
        cached = await cache.get_series_latest(series_id)

        if cached:
            return cached

        db = get_db()
        latest = await db.get_latest_value(series_id)

        if not latest:
            raise HTTPException(
                status_code=404,
                detail=f"No data found for series {series_id}",
            )

        return {
            "series_id": series_id,
            "value": latest["value"],
            "timestamp": latest["timestamp"].isoformat() if isinstance(latest["timestamp"], datetime) else latest["timestamp"],
            "unit": latest.get("unit"),
            "is_preliminary": latest.get("is_preliminary", False),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get latest value", series_id=series_id, error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/series/{series_id}/statistics", response_model=SeriesStatistics)
async def get_series_statistics(
    series_id: str,
    lookback_days: int = Query(730, ge=7, le=7300),
) -> SeriesStatistics:
    """Get statistical summary for a series.

    Args:
        series_id: ID of the data series
        lookback_days: Number of days to analyze

    Returns:
        Statistical summary
    """
    try:
        db = get_db()
        stats = await db.get_series_statistics(series_id, lookback_days)

        if not stats:
            raise HTTPException(
                status_code=404,
                detail=f"No statistics available for series {series_id}",
            )

        return SeriesStatistics(
            series_id=series_id,
            count=stats.get("count", 0),
            mean=stats.get("mean"),
            stddev=stats.get("stddev"),
            min=stats.get("min"),
            max=stats.get("max"),
            median=stats.get("median"),
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get statistics", series_id=series_id, error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/collectors/status", response_model=list[CollectorStatus])
async def get_collector_status() -> list[CollectorStatus]:
    """Get status of all data collectors.

    Returns:
        List of collector statuses
    """
    try:
        scheduler = get_scheduler()
        jobs = scheduler.get_job_status()

        statuses = []
        for job in jobs:
            statuses.append(
                CollectorStatus(
                    name=job["id"],
                    last_run=None,  # Would need to track this
                    next_run=job.get("next_run"),
                    status="scheduled",
                    records_collected=None,
                )
            )

        return statuses

    except Exception as e:
        logger.error("Failed to get collector status", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/collectors/{collector_name}/run")
async def run_collector(collector_name: str) -> dict[str, Any]:
    """Manually trigger a collector run.

    Args:
        collector_name: Name of the collector to run

    Returns:
        Collection result
    """
    try:
        scheduler = get_scheduler()
        result = await scheduler.run_collector_now(collector_name)

        return {
            "collector": collector_name,
            "success": result.success,
            "records_collected": result.records_collected,
            "duration_seconds": result.duration_seconds,
            "error": result.error_message,
        }

    except Exception as e:
        logger.error("Failed to run collector", collector=collector_name, error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/collectors/run-all")
async def run_all_collectors() -> dict[str, Any]:
    """Run every registered data collector and return a summary.

    This endpoint is called by the "Run Detection Now" flow to ensure
    the database contains fresh data before bottleneck detection.
    """
    try:
        scheduler = get_scheduler()
        results = await scheduler.run_all_collectors()

        succeeded = [r for r in results if r.success]
        failed = [r for r in results if not r.success]
        total_records = sum(r.records_collected for r in results)

        return {
            "success": len(failed) == 0,
            "total_collectors": len(results),
            "succeeded": len(succeeded),
            "failed": len(failed),
            "total_records": total_records,
            "collectors": [
                {
                    "name": r.collector_name,
                    "success": r.success,
                    "records": r.records_collected,
                    "duration": r.duration_seconds,
                    "error": r.error_message,
                }
                for r in results
            ],
        }

    except Exception as e:
        logger.error("Failed to run all collectors", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/sources")
async def list_data_sources() -> list[dict[str, str]]:
    """List available data sources.

    Returns:
        List of data sources
    """
    sources = [
        {"id": "fred", "name": "Federal Reserve Economic Data (FRED)", "type": "government"},
        {"id": "bls", "name": "Bureau of Labor Statistics", "type": "government"},
        {"id": "eia", "name": "Energy Information Administration", "type": "government"},
        {"id": "census", "name": "Census Bureau", "type": "government"},
        {"id": "gdelt", "name": "GDELT Project", "type": "alternative"},
        {"id": "google_trends", "name": "Google Trends", "type": "alternative"},
    ]
    return sources
