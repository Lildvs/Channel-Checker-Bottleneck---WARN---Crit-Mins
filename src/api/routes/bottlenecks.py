"""Bottleneck-related API endpoints."""

import asyncio
import json
from datetime import UTC, datetime, timedelta
from typing import Any, AsyncGenerator
from uuid import UUID

import structlog
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from src.analysis.bottleneck_detector import BottleneckDetector
from src.analysis.sector_mapper import get_sector_mapper
from src.analysis.signals import BottleneckCategory, BottleneckSignalData
from src.services.alert_service import AlertService, AlertPriority, AlertRecord
from sqlalchemy import select
from src.storage.redis_cache import get_cache
from src.storage.timescale import get_db

logger = structlog.get_logger()
router = APIRouter()


class BottleneckResponse(BaseModel):
    """Response model for a bottleneck signal."""

    id: str
    detected_at: str
    category: str
    subcategory: str | None
    severity: float
    confidence: float
    strength: str
    affected_sectors: list[str]
    affected_commodities: list[str]
    source_series: list[str]
    evidence: dict[str, Any]
    description: str
    status: str


class BottleneckListResponse(BaseModel):
    """Response model for list of bottlenecks."""

    bottlenecks: list[BottleneckResponse]
    total: int
    active_count: int


class SectorImpactResponse(BaseModel):
    """Response model for sector impact."""

    sector_code: str
    sector_name: str
    impact_score: float
    impact_type: str
    propagation_path: list[str]
    lag_days: int


class ImpactPropagationResponse(BaseModel):
    """Response model for impact propagation."""

    bottleneck_id: str
    impacts: list[SectorImpactResponse]
    total_sectors_affected: int


class PropagationPathResponse(BaseModel):
    """Response model for a propagation path."""

    nodes: list[str]
    node_names: list[str]
    coefficients: list[float]
    cumulative_impact: float
    hop_count: int
    has_cycle: bool


class FullPropagationResponse(BaseModel):
    """Response model for full propagation analysis."""

    bottleneck_id: str
    origin_category: str
    origin_severity: float
    total_economic_impact: float
    propagation_rounds: int
    convergence_reached: bool
    severity_classification: str
    amplification_detected: list[str]
    affected_sectors: list[SectorImpactResponse]
    propagation_paths: list[PropagationPathResponse]
    analysis_timestamp: str


class TrendDataPoint(BaseModel):
    """A single data point in the bottleneck trend."""
    date: str
    count: int


class BottleneckTrendResponse(BaseModel):
    """Response model for bottleneck trend data."""
    data: list[TrendDataPoint]


@router.get("/trend", response_model=BottleneckTrendResponse)
async def get_bottleneck_trend(
    days: int = Query(30, ge=1, le=365),
) -> BottleneckTrendResponse:
    """Get bottleneck detection counts over time.

    Returns daily counts of active bottlenecks for the last N days.
    """
    from sqlalchemy import func, cast, Date

    db = get_db()

    try:
        start_date = datetime.now(UTC) - timedelta(days=days)

        async with db.session() as session:
            from src.storage.models import Anomaly

            query = (
                select(
                    cast(Anomaly.detected_at, Date).label("date"),
                    func.count().label("count"),
                )
                .where(Anomaly.detected_at >= start_date)
                .group_by(cast(Anomaly.detected_at, Date))
                .order_by(cast(Anomaly.detected_at, Date))
            )
            result = await session.execute(query)
            rows = result.all()

        data = [
            TrendDataPoint(date=str(row.date), count=row.count)
            for row in rows
        ]

        return BottleneckTrendResponse(data=data)

    except Exception as e:
        logger.error("Failed to get bottleneck trend", error=str(e))
        return BottleneckTrendResponse(data=[])


@router.get("/active", response_model=BottleneckListResponse)
async def get_active_bottlenecks(
    category: BottleneckCategory | None = None,
    min_severity: float = Query(0.0, ge=0.0, le=1.0),
    limit: int = Query(50, ge=1, le=100),
) -> BottleneckListResponse:
    """Get currently active bottleneck signals.

    Args:
        category: Optional category filter
        min_severity: Minimum severity threshold
        limit: Maximum number of results

    Returns:
        List of active bottlenecks
    """
    try:
        import asyncio

        # Check cache first -- never block the request with inline detection
        cache = get_cache()
        cached = await cache.get_active_bottlenecks()

        if cached:
            bottlenecks = cached
        else:
            # Cache is empty -- schedule background detection and return empty
            # The worker pre-warms the cache on a schedule; if it hasn't run yet,
            # kick off a background task so the NEXT request will have data.
            async def _background_detect() -> None:
                try:
                    db = get_db()
                    detector = BottleneckDetector(db=db)
                    signals = await detector.detect_all()
                    result = [s.to_dict() for s in signals]
                    await cache.cache_active_bottlenecks(result)
                    logger.info("Background detection complete", signals=len(result))
                except Exception as exc:
                    logger.error("Background detection failed", error=str(exc))

            asyncio.create_task(_background_detect())
            bottlenecks = []

        if category:
            bottlenecks = [b for b in bottlenecks if b["category"] == category.value]

        bottlenecks = [b for b in bottlenecks if b["severity"] >= min_severity]
        bottlenecks = bottlenecks[:limit]

        return BottleneckListResponse(
            bottlenecks=[BottleneckResponse(**b) for b in bottlenecks],
            total=len(bottlenecks),
            active_count=len([b for b in bottlenecks if b["status"] == "active"]),
        )

    except Exception as e:
        logger.error("Failed to get active bottlenecks", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/detect", response_model=BottleneckListResponse)
async def detect_bottlenecks(
    lookback_days: int = Query(400, ge=7, le=1095),
) -> BottleneckListResponse:
    """Run bottleneck detection on recent data.

    Args:
        lookback_days: Number of days to analyze

    Returns:
        Newly detected bottlenecks
    """
    try:
        db = get_db()
        detector = BottleneckDetector(db=db)
        signals = await detector.detect_all(lookback_days=lookback_days)

        bottlenecks = [s.to_dict() for s in signals]

        cache = get_cache()
        await cache.cache_active_bottlenecks(bottlenecks)

        for signal in signals:
            if signal.severity >= 0.7:
                await cache.publish_bottleneck_alert(signal.to_dict())

        return BottleneckListResponse(
            bottlenecks=[BottleneckResponse(**b) for b in bottlenecks],
            total=len(bottlenecks),
            active_count=len(signals),
        )

    except Exception as e:
        logger.error("Bottleneck detection failed", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{bottleneck_id}", response_model=BottleneckResponse)
async def get_bottleneck(bottleneck_id: str) -> BottleneckResponse:
    """Get a specific bottleneck by ID.

    Args:
        bottleneck_id: UUID of the bottleneck

    Returns:
        Bottleneck details
    """
    try:
        cache = get_cache()
        cached = await cache.get_active_bottlenecks()

        for b in cached:
            if b["id"] == bottleneck_id:
                return BottleneckResponse(**b)

        raise HTTPException(status_code=404, detail="Bottleneck not found")

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get bottleneck", bottleneck_id=bottleneck_id, error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{bottleneck_id}/impact", response_model=ImpactPropagationResponse)
async def get_bottleneck_impact(
    bottleneck_id: str,
    max_hops: int = Query(3, ge=1, le=5),
) -> ImpactPropagationResponse:
    """Get sector impact propagation for a bottleneck.

    Args:
        bottleneck_id: UUID of the bottleneck
        max_hops: Maximum propagation depth

    Returns:
        Impact propagation details
    """
    try:
        cache = get_cache()
        cached = await cache.get_active_bottlenecks()

        bottleneck_data = None
        for b in cached:
            if b["id"] == bottleneck_id:
                bottleneck_data = b
                break

        if not bottleneck_data:
            raise HTTPException(status_code=404, detail="Bottleneck not found")

        signal = BottleneckSignalData(
            category=BottleneckCategory(bottleneck_data["category"]),
            subcategory=bottleneck_data.get("subcategory"),
            severity=bottleneck_data["severity"],
            confidence=bottleneck_data["confidence"],
            affected_sectors=bottleneck_data.get("affected_sectors", []),
        )

        mapper = get_sector_mapper()
        impacts = mapper.propagate_impact(signal, max_hops=max_hops)

        return ImpactPropagationResponse(
            bottleneck_id=bottleneck_id,
            impacts=[
                SectorImpactResponse(
                    sector_code=i.sector_code,
                    sector_name=i.sector_name,
                    impact_score=i.impact_score,
                    impact_type=i.impact_type,
                    propagation_path=i.propagation_path,
                    lag_days=i.lag_days,
                )
                for i in impacts
            ],
            total_sectors_affected=len(impacts),
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get bottleneck impact", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{bottleneck_id}/propagation", response_model=FullPropagationResponse)
async def get_bottleneck_propagation(
    bottleneck_id: str,
    max_rounds: int = Query(5, ge=1, le=10),
    include_paths: bool = Query(True),
    use_io_data: bool = Query(True),
) -> FullPropagationResponse:
    """Get full supply chain propagation analysis for a bottleneck.

    This endpoint uses the PropagationEngine with I-O-derived dependencies
    to calculate realistic economic cascade effects through the supply chain.

    Args:
        bottleneck_id: UUID of the bottleneck
        max_rounds: Maximum propagation rounds (depth)
        include_paths: Whether to include detailed propagation paths
        use_io_data: Whether to use I-O table data (vs hardcoded)

    Returns:
        Full propagation analysis with paths and amplification detection
    """
    try:
        from src.analysis.propagation_engine import (
            get_propagation_engine,
            PropagationConfig,
        )
        from uuid import UUID

        cache = get_cache()
        cached = await cache.get_active_bottlenecks()

        bottleneck_data = None
        for b in cached:
            if b["id"] == bottleneck_id:
                bottleneck_data = b
                break

        if not bottleneck_data:
            raise HTTPException(status_code=404, detail="Bottleneck not found")

        signal = BottleneckSignalData(
            id=UUID(bottleneck_id),
            category=BottleneckCategory(bottleneck_data["category"]),
            subcategory=bottleneck_data.get("subcategory"),
            severity=bottleneck_data["severity"],
            confidence=bottleneck_data["confidence"],
            affected_sectors=bottleneck_data.get("affected_sectors", []),
        )

        config = PropagationConfig(
            max_rounds=max_rounds,
            include_paths=include_paths,
            use_total_requirements=use_io_data,
        )

        engine = get_propagation_engine()
        result = await engine.propagate_bottleneck(signal, config_override=config)

        return FullPropagationResponse(
            bottleneck_id=bottleneck_id,
            origin_category=result.origin_bottleneck.category.value,
            origin_severity=result.origin_bottleneck.severity,
            total_economic_impact=result.total_economic_impact,
            propagation_rounds=result.propagation_rounds,
            convergence_reached=result.convergence_reached,
            severity_classification=result.severity_classification.value,
            amplification_detected=result.amplification_detected,
            affected_sectors=[
                SectorImpactResponse(
                    sector_code=s.sector_code,
                    sector_name=s.sector_name,
                    impact_score=s.impact_score,
                    impact_type=s.impact_type,
                    propagation_path=s.propagation_path,
                    lag_days=s.lag_days,
                )
                for s in result.affected_sectors
            ],
            propagation_paths=[
                PropagationPathResponse(
                    nodes=p.nodes,
                    node_names=p.node_names,
                    coefficients=p.coefficients,
                    cumulative_impact=p.cumulative_impact,
                    hop_count=p.hop_count,
                    has_cycle=p.has_cycle,
                )
                for p in result.propagation_paths
            ],
            analysis_timestamp=result.analysis_timestamp.isoformat(),
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get bottleneck propagation", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stream/alerts")
async def stream_bottleneck_alerts(
    priority: str = Query(
        None,
        description="Comma-separated priority filter (e.g., 'critical,high')",
    ),
    include_history: bool = Query(
        True,
        description="Include recent unacknowledged alerts on connect",
    ),
) -> StreamingResponse:
    """Server-Sent Events stream for real-time bottleneck alerts.

    Enhanced with priority filtering and initial history batch.

    Query Parameters:
        priority: Filter by priorities (comma-separated: critical,high,medium,low)
        include_history: Send recent unacknowledged alerts on connect

    Returns:
        SSE stream of bottleneck alerts with event types:
        - event: connected (initial connection)
        - event: history (batch of recent alerts)
        - event: alert (real-time alert)
        - comment: keepalive
    """
    priority_filter: list[AlertPriority] | None = None
    if priority:
        try:
            priority_filter = [
                AlertPriority(p.strip().lower())
                for p in priority.split(",")
                if p.strip()
            ]
        except ValueError:
            pass  # Invalid priority, ignore filter

    async def event_generator() -> AsyncGenerator[str, None]:
        cache = get_cache()
        alert_service = AlertService(cache)
        pubsub = await cache.subscribe("bottleneck_alerts")

        try:
            yield f"event: connected\ndata: {json.dumps({'type': 'connected', 'timestamp': datetime.now(UTC).isoformat()})}\n\n"

            if include_history:
                try:
                    recent_alerts = await alert_service.get_recent_unacknowledged(
                        limit=20,
                        priority_filter=priority_filter,
                    )
                    if recent_alerts:
                        history_data = {
                            "type": "history",
                            "count": len(recent_alerts),
                            "alerts": [a.to_dict() for a in recent_alerts],
                        }
                        yield f"event: history\ndata: {json.dumps(history_data)}\n\n"
                except Exception as e:
                    logger.warning("Failed to send alert history", error=str(e))

            last_keepalive = datetime.now(UTC)

            while True:
                try:
                    # Use wait_for with timeout to allow keep-alive checks
                    message = await asyncio.wait_for(
                        pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0),
                        timeout=5.0,
                    )

                    if message and message["type"] == "message":
                        data = message["data"]

                    if priority_filter:
                        try:
                            alert_data = json.loads(data)
                            alert_priority = AlertPriority(
                                alert_data.get("priority", "medium")
                            )
                            if alert_priority not in priority_filter:
                                continue
                        except (json.JSONDecodeError, ValueError):
                            pass  # Send anyway if can't parse

                        yield f"event: alert\ndata: {data}\n\n"

                except asyncio.TimeoutError:
                    pass  # No message received, check if we need keep-alive

                now = datetime.now(UTC)
                if (now - last_keepalive).total_seconds() >= 30:
                    yield f": keepalive\n\n"
                    last_keepalive = now

        except asyncio.CancelledError:
            pass
        finally:
            await pubsub.unsubscribe("bottleneck_alerts")

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


class AlertHistoryResponse(BaseModel):
    """Response model for alert history."""

    alerts: list[dict[str, Any]]
    total: int
    has_more: bool


class AlertStatsResponse(BaseModel):
    """Response model for alert statistics."""

    total: int
    by_priority: dict[str, int]
    by_category: dict[str, int]
    by_day: dict[str, int]


@router.get("/alerts/history", response_model=AlertHistoryResponse)
async def get_alert_history(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    priority: str = Query(None, description="Filter by priority (comma-separated)"),
    category: str = Query(None, description="Filter by category"),
    unacknowledged: bool = Query(False, description="Only unacknowledged alerts"),
) -> AlertHistoryResponse:
    """Get paginated alert history with optional filters.

    Args:
        limit: Maximum number of alerts to return
        offset: Offset for pagination
        priority: Comma-separated priorities to filter
        category: Category to filter
        unacknowledged: Only return unacknowledged alerts

    Returns:
        AlertHistoryResponse with alerts and pagination info
    """
    try:
        cache = get_cache()
        alert_service = AlertService(cache)

        priority_filter = None
        if priority:
            try:
                priority_filter = [
                    AlertPriority(p.strip().lower())
                    for p in priority.split(",")
                ]
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid priority. Must be one of: {[p.value for p in AlertPriority]}",
                )

        alerts = await alert_service.get_alert_history(
            limit=limit + 1,  # Fetch one extra to check has_more
            offset=offset,
            priority_filter=priority_filter,
            category_filter=category,
            unacknowledged_only=unacknowledged,
        )

        has_more = len(alerts) > limit
        if has_more:
            alerts = alerts[:limit]

        return AlertHistoryResponse(
            alerts=[a.to_dict() for a in alerts],
            total=len(alerts),
            has_more=has_more,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get alert history", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/alerts/{alert_id}/acknowledge")
async def acknowledge_alert(alert_id: UUID) -> dict[str, Any]:
    """Mark an alert as acknowledged.

    Args:
        alert_id: UUID of the alert to acknowledge

    Returns:
        Success status and timestamp
    """
    try:
        cache = get_cache()
        alert_service = AlertService(cache)

        success = await alert_service.acknowledge_alert(alert_id)

        if not success:
            raise HTTPException(status_code=404, detail="Alert not found")

        return {
            "success": True,
            "alert_id": str(alert_id),
            "acknowledged_at": datetime.now(UTC).isoformat(),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to acknowledge alert", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/alerts/stats", response_model=AlertStatsResponse)
async def get_alert_stats(
    days: int = Query(7, ge=1, le=30, description="Number of days to include"),
) -> AlertStatsResponse:
    """Get alert statistics for the specified time period.

    Args:
        days: Number of days to include in stats

    Returns:
        AlertStatsResponse with counts by priority, category, and day
    """
    try:
        cache = get_cache()
        alert_service = AlertService(cache)

        stats = await alert_service.get_stats(days=days)

        return AlertStatsResponse(**stats)

    except Exception as e:
        logger.error("Failed to get alert stats", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/categories", response_model=list[dict[str, str]])
async def get_bottleneck_categories() -> list[dict[str, str]]:
    """Get all bottleneck categories.

    Returns:
        List of category information
    """
    return [
        {"value": cat.value, "name": cat.value.replace("_", " ").title()}
        for cat in BottleneckCategory
    ]
