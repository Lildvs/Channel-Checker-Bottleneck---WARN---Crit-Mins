"""Analysis monitor API endpoints.

Provides endpoints for:
- Inventory-to-Sales ratio monitoring
- Price spike detection
- Shipping congestion index
- Energy crunch detection
- Critical mineral supply risk
- Labor tightness monitoring
- Capacity utilization monitoring
- Composite dashboard
"""

from datetime import datetime, UTC
from typing import Any

import structlog
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from src.analysis.monitors import (
    InventorySalesMonitor,
    PriceSpikeDetector,
    ShippingCongestionIndex,
    EnergyCrunchDetector,
    CriticalMineralRisk,
    LaborTightnessMonitor,
    CapacityUtilizationMonitor,
)
from src.storage.redis_cache import get_cache
from src.storage.timescale import get_db

logger = structlog.get_logger()
router = APIRouter(prefix="/analysis", tags=["analysis"])


class MonitorScoreResponse(BaseModel):
    """Generic response for monitor scores."""

    score: float = Field(..., description="Composite score (0-100)")
    severity: float = Field(..., description="Severity (0-1)")
    confidence: float = Field(..., description="Confidence (0-1)")
    alert_level: str = Field(..., description="Alert level: normal, elevated, critical")
    components: dict[str, float] = Field(default_factory=dict)
    evidence: dict[str, Any] = Field(default_factory=dict)
    description: str = ""
    timestamp: str = Field(..., description="ISO timestamp")


class ISRatioResponse(BaseModel):
    """Response for inventory-to-sales ratio."""

    sector: str
    current_ratio: float
    mean_ratio: float
    z_score: float
    percentile: float
    status: str
    score: float


class InventorySalesResponse(BaseModel):
    """Full inventory-sales monitor response."""

    composite_score: float
    alert_level: str
    sectors: dict[str, ISRatioResponse]
    timestamp: str


class PriceBreakoutResponse(BaseModel):
    """Response for a single price breakout."""

    commodity: str
    commodity_group: str
    current_price: float
    pct_change: float
    z_score: float
    timeframe: str
    direction: str
    score: float


class PriceSpikeResponse(BaseModel):
    """Full price spike detector response."""

    composite_score: float
    alert_level: str
    breakouts: list[PriceBreakoutResponse]
    by_group: dict[str, float]
    timestamp: str


class ShippingCongestionResponse(BaseModel):
    """Full shipping congestion index response."""

    score: float
    alert_level: str
    components: dict[str, float]
    port_scores: dict[str, float]
    timestamp: str


class EnergyCrunchResponse(BaseModel):
    """Full energy crunch detector response."""

    score: float
    alert_level: str
    components: dict[str, float]
    spr_status: dict[str, Any] | None
    refinery_status: dict[str, Any] | None
    inventory_deviations: list[dict[str, Any]]
    timestamp: str


class MineralRiskResponse(BaseModel):
    """Response for single mineral risk."""

    mineral: str
    composite_score: float
    import_dependency: float
    concentration_risk: float
    geopolitical_risk: float
    top_suppliers: list[dict[str, Any]]
    alert_level: str


class CriticalMineralResponse(BaseModel):
    """Full critical mineral risk response."""

    aggregate_score: float
    alert_level: str
    minerals: dict[str, MineralRiskResponse]
    high_risk_minerals: list[str]
    china_exposure: dict[str, Any]
    timestamp: str


class DashboardResponse(BaseModel):
    """Composite dashboard with all monitors."""

    timestamp: str
    overall_stress: float
    monitors: dict[str, MonitorScoreResponse]
    alerts: list[dict[str, Any]]


@router.get("/inventory-sales", response_model=InventorySalesResponse)
async def get_inventory_sales_ratios(
    sigma_threshold: float = Query(2.0, description="Standard deviation threshold"),
) -> InventorySalesResponse:
    """Get inventory-to-sales ratio analysis for all sectors.

    Monitors retail, wholesale, and manufacturing I/S ratios,
    flagging when ratios drop below sigma threshold (inventory squeeze)
    or rise above (demand weakness).
    """
    try:
        db = get_db()
        monitor = InventorySalesMonitor(db=db, sigma_threshold=sigma_threshold)

        ratio_results = await monitor.calculate_ratios()
        result = await monitor.calculate_score()

        sectors = {}
        for sector, r in ratio_results.items():
            sectors[sector] = ISRatioResponse(
                sector=r.sector,
                current_ratio=r.current_ratio,
                mean_ratio=r.mean_ratio,
                z_score=r.z_score,
                percentile=r.percentile,
                status=r.status,
                score=r.score,
            )

        return InventorySalesResponse(
            composite_score=result.score,
            alert_level=result.alert_level,
            sectors=sectors,
            timestamp=datetime.now(UTC).isoformat(),
        )

    except Exception as e:
        logger.error("Failed to get inventory-sales ratios", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/inventory-sales/{sector}", response_model=ISRatioResponse)
async def get_sector_is_ratio(sector: str) -> ISRatioResponse:
    """Get inventory-to-sales ratio for a specific sector.

    Args:
        sector: One of: retail, wholesale, manufacturing
    """
    valid_sectors = ["retail", "wholesale", "manufacturing"]
    if sector not in valid_sectors:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid sector. Must be one of: {valid_sectors}",
        )

    try:
        db = get_db()
        monitor = InventorySalesMonitor(db=db)

        ratios = await monitor.calculate_ratios()
        if sector not in ratios:
            raise HTTPException(status_code=404, detail=f"No data for sector: {sector}")

        r = ratios[sector]
        return ISRatioResponse(
            sector=r.sector,
            current_ratio=r.current_ratio,
            mean_ratio=r.mean_ratio,
            z_score=r.z_score,
            percentile=r.percentile,
            status=r.status,
            score=r.score,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get sector I/S ratio", sector=sector, error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/price-spikes", response_model=PriceSpikeResponse)
async def get_price_spikes(
    pct_threshold: float = Query(0.10, description="Percentage change threshold"),
    z_threshold: float = Query(2.5, description="Z-score threshold"),
) -> PriceSpikeResponse:
    """Detect commodity price breakouts.

    Monitors energy, metals, and agriculture commodities for
    significant price movements using percentage change and Z-score analysis.
    """
    try:
        db = get_db()
        detector = PriceSpikeDetector(
            db=db,
            pct_threshold=pct_threshold,
            z_score_threshold=z_threshold,
        )

        breakouts = await detector.detect_breakouts()
        result = await detector.calculate_score()

        breakout_responses = [
            PriceBreakoutResponse(
                commodity=b.commodity,
                commodity_group=b.commodity_group,
                current_price=b.current_price,
                pct_change=b.pct_change,
                z_score=b.z_score,
                timeframe=b.timeframe,
                direction=b.direction,
                score=b.score,
            )
            for b in breakouts[:20]  # Top 20
        ]

        return PriceSpikeResponse(
            composite_score=result.score,
            alert_level=result.alert_level,
            breakouts=breakout_responses,
            by_group=result.components,
            timestamp=datetime.now(UTC).isoformat(),
        )

    except Exception as e:
        logger.error("Failed to detect price spikes", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/price-spikes/{group}", response_model=dict)
async def get_price_spikes_by_group(group: str) -> dict[str, Any]:
    """Get price spike analysis for a commodity group.

    Args:
        group: One of: energy, metals, agriculture
    """
    valid_groups = ["energy", "metals", "agriculture"]
    if group not in valid_groups:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid group. Must be one of: {valid_groups}",
        )

    try:
        db = get_db()
        detector = PriceSpikeDetector(db=db)

        summary = await detector.get_group_summary(group)
        return summary

    except Exception as e:
        logger.error("Failed to get group summary", group=group, error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/shipping-congestion", response_model=ShippingCongestionResponse)
async def get_shipping_congestion() -> ShippingCongestionResponse:
    """Get shipping congestion index.

    Composite metric combining Baltic indices, port dwell times,
    TEU throughput, and container shipping rates.
    """
    try:
        db = get_db()
        index = ShippingCongestionIndex(db=db)

        sci_result = await index.calculate_index()

        return ShippingCongestionResponse(
            score=sci_result.score,
            alert_level=sci_result.alert_level,
            components=sci_result.components,
            port_scores=sci_result.port_scores,
            timestamp=datetime.now(UTC).isoformat(),
        )

    except Exception as e:
        logger.error("Failed to get shipping congestion", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/shipping-congestion/port/{port_name}")
async def get_port_details(port_name: str) -> dict[str, Any]:
    """Get detailed metrics for a specific port.

    Args:
        port_name: Port name (e.g., "Port of Los Angeles")
    """
    try:
        db = get_db()
        index = ShippingCongestionIndex(db=db)

        details = await index.get_port_drilldown(port_name)
        return details

    except Exception as e:
        logger.error("Failed to get port details", port=port_name, error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/shipping-congestion/historical")
async def get_shipping_congestion_historical(
    months: int = Query(12, ge=1, le=60, description="Months of history"),
) -> dict[str, Any]:
    """Get historical comparison for shipping congestion index."""
    try:
        db = get_db()
        index = ShippingCongestionIndex(db=db)

        comparison = await index.compare_historical(lookback_months=months)
        return comparison

    except Exception as e:
        logger.error("Failed to get historical comparison", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/energy-crunch", response_model=EnergyCrunchResponse)
async def get_energy_crunch() -> EnergyCrunchResponse:
    """Get energy crunch detection analysis.

    Monitors SPR levels, commercial stocks, refinery utilization,
    and price-storage composite signals.
    """
    try:
        db = get_db()
        detector = EnergyCrunchDetector(db=db)

        crunch = await detector.calculate_crunch_score()

        return EnergyCrunchResponse(
            score=crunch.score,
            alert_level=crunch.alert_level,
            components=crunch.components,
            spr_status=crunch.spr_status.to_dict() if crunch.spr_status else None,
            refinery_status=crunch.refinery_status.to_dict() if crunch.refinery_status else None,
            inventory_deviations=[d.to_dict() for d in crunch.inventory_deviations],
            timestamp=datetime.now(UTC).isoformat(),
        )

    except Exception as e:
        logger.error("Failed to get energy crunch", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/energy-crunch/spr")
async def get_spr_status() -> dict[str, Any]:
    """Get Strategic Petroleum Reserve status."""
    try:
        db = get_db()
        detector = EnergyCrunchDetector(db=db)

        spr = await detector.check_spr_levels()
        if spr is None:
            raise HTTPException(status_code=404, detail="SPR data not available")

        return spr.to_dict()

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get SPR status", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/energy-crunch/days-of-supply")
async def get_days_of_supply() -> dict[str, float]:
    """Get days of supply for petroleum products."""
    try:
        db = get_db()
        detector = EnergyCrunchDetector(db=db)

        return await detector.calculate_days_of_supply()

    except Exception as e:
        logger.error("Failed to get days of supply", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/mineral-risk", response_model=CriticalMineralResponse)
async def get_critical_mineral_risk() -> CriticalMineralResponse:
    """Get critical mineral supply risk analysis.

    Assesses import dependency, supplier concentration,
    price volatility, and geopolitical risk for key minerals.
    """
    try:
        db = get_db()
        monitor = CriticalMineralRisk(db=db)

        mineral_results = await monitor.calculate_all_minerals()
        aggregate = await monitor.calculate_aggregate_risk()
        china_exposure = await monitor.get_china_exposure_summary()

        minerals = {}
        for mineral, r in mineral_results.items():
            minerals[mineral] = MineralRiskResponse(
                mineral=r.mineral,
                composite_score=r.composite_risk_score,
                import_dependency=r.import_dependency,
                concentration_risk=r.concentration_risk,
                geopolitical_risk=r.geopolitical_risk,
                top_suppliers=r.top_suppliers,
                alert_level=r.alert_level,
            )

        return CriticalMineralResponse(
            aggregate_score=aggregate["aggregate_score"],
            alert_level=aggregate["alert_level"],
            minerals=minerals,
            high_risk_minerals=aggregate.get("high_risk_minerals", []),
            china_exposure=china_exposure,
            timestamp=datetime.now(UTC).isoformat(),
        )

    except Exception as e:
        logger.error("Failed to get mineral risk", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/mineral-risk/{mineral}", response_model=MineralRiskResponse)
async def get_mineral_risk(mineral: str) -> MineralRiskResponse:
    """Get risk analysis for a specific mineral.

    Args:
        mineral: One of: lithium, cobalt, nickel, graphite, rare_earths, manganese
    """
    valid_minerals = ["lithium", "cobalt", "nickel", "graphite", "rare_earths", "manganese"]
    if mineral not in valid_minerals:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid mineral. Must be one of: {valid_minerals}",
        )

    try:
        db = get_db()
        monitor = CriticalMineralRisk(db=db)

        result = await monitor.composite_risk_score(mineral)
        if result is None:
            raise HTTPException(status_code=404, detail=f"No data for mineral: {mineral}")

        return MineralRiskResponse(
            mineral=result.mineral,
            composite_score=result.composite_risk_score,
            import_dependency=result.import_dependency,
            concentration_risk=result.concentration_risk,
            geopolitical_risk=result.geopolitical_risk,
            top_suppliers=result.top_suppliers,
            alert_level=result.alert_level,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get mineral risk", mineral=mineral, error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/mineral-risk/exposure/china")
async def get_china_exposure() -> dict[str, Any]:
    """Get summary of China exposure across all critical minerals."""
    try:
        db = get_db()
        monitor = CriticalMineralRisk(db=db)

        return await monitor.get_china_exposure_summary()

    except Exception as e:
        logger.error("Failed to get China exposure", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


class LaborMetricsResponse(BaseModel):
    """Response for labor market metrics."""

    openings_ratio: float = Field(..., description="Job openings per unemployed")
    quits_rate: float = Field(..., description="Quits rate as decimal")
    wage_growth_yoy: float = Field(..., description="YoY wage growth as decimal")
    unemployment_rate: float = Field(..., description="Unemployment rate as decimal")
    job_openings_thousands: float
    unemployed_thousands: float


class SectorLaborResponse(BaseModel):
    """Response for sector labor status."""

    sector: str
    tightness_score: float
    status: str
    job_openings_rate: float | None
    wage_growth: float | None


class LaborTightnessResponse(BaseModel):
    """Full labor tightness monitor response."""

    composite_score: float
    alert_level: str
    metrics: LaborMetricsResponse
    components: dict[str, float]
    sectors: dict[str, SectorLaborResponse]
    tight_sectors: list[str]
    timestamp: str


@router.get("/labor-tightness", response_model=LaborTightnessResponse)
async def get_labor_tightness() -> LaborTightnessResponse:
    """Get labor market tightness analysis.

    Monitors JOLTS job openings ratio, quits rate, wage growth,
    and sector-specific labor constraints.
    """
    try:
        db = get_db()
        monitor = LaborTightnessMonitor(db=db)

        result = await monitor.calculate_score()
        metrics_data = result.evidence.get("metrics", {})
        sector_constraints = result.evidence.get("sector_constraints", {})

        metrics = LaborMetricsResponse(
            openings_ratio=metrics_data.get("openings_ratio", 0),
            quits_rate=metrics_data.get("quits_rate", 0),
            wage_growth_yoy=metrics_data.get("wage_growth_yoy", 0),
            unemployment_rate=metrics_data.get("unemployment_rate", 0),
            job_openings_thousands=metrics_data.get("job_openings_thousands", 0),
            unemployed_thousands=metrics_data.get("unemployed_thousands", 0),
        )

        sectors = {}
        for sector_id, sector_data in sector_constraints.items():
            sectors[sector_id] = SectorLaborResponse(
                sector=sector_data.get("sector", sector_id),
                tightness_score=sector_data.get("tightness_score", 0),
                status=sector_data.get("status", "normal"),
                job_openings_rate=sector_data.get("job_openings_rate"),
                wage_growth=sector_data.get("wage_growth"),
            )

        return LaborTightnessResponse(
            composite_score=result.score,
            alert_level=result.alert_level,
            metrics=metrics,
            components=result.components,
            sectors=sectors,
            tight_sectors=result.evidence.get("tight_sectors", []),
            timestamp=datetime.now(UTC).isoformat(),
        )

    except Exception as e:
        logger.error("Failed to get labor tightness", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/labor-tightness/sectors")
async def get_labor_sectors() -> dict[str, Any]:
    """Get labor market status by sector."""
    try:
        db = get_db()
        monitor = LaborTightnessMonitor(db=db)

        return await monitor.get_sector_summary()

    except Exception as e:
        logger.error("Failed to get labor sectors", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


class SectorUtilizationResponse(BaseModel):
    """Response for sector utilization."""

    sector: str
    sector_name: str
    current_utilization: float
    threshold: float
    historical_avg: float
    deviation_from_avg: float
    is_above_threshold: bool
    score: float
    status: str


class CapacityUtilizationResponse(BaseModel):
    """Full capacity utilization monitor response."""

    composite_score: float
    alert_level: str
    components: dict[str, float]
    sectors: dict[str, SectorUtilizationResponse]
    elevated_sectors: list[str]
    critical_sectors: list[str]
    timestamp: str


@router.get("/capacity-utilization", response_model=CapacityUtilizationResponse)
async def get_capacity_utilization() -> CapacityUtilizationResponse:
    """Get capacity utilization analysis.

    Monitors industrial capacity utilization by sector
    (Total, Manufacturing, Mining, Utilities).
    """
    try:
        db = get_db()
        monitor = CapacityUtilizationMonitor(db=db)

        result = await monitor.calculate_score()
        sector_data = result.evidence.get("sector_utilizations", {})

        sectors = {}
        for sector_id, data in sector_data.items():
            sectors[sector_id] = SectorUtilizationResponse(
                sector=data.get("sector", sector_id),
                sector_name=data.get("sector_name", sector_id),
                current_utilization=data.get("current_utilization", 0),
                threshold=data.get("threshold", 0.85),
                historical_avg=data.get("historical_avg", 0),
                deviation_from_avg=data.get("deviation_from_avg", 0),
                is_above_threshold=data.get("is_above_threshold", False),
                score=data.get("score", 0),
                status=data.get("status", "normal"),
            )

        return CapacityUtilizationResponse(
            composite_score=result.score,
            alert_level=result.alert_level,
            components=result.components,
            sectors=sectors,
            elevated_sectors=result.evidence.get("elevated_sectors", []),
            critical_sectors=result.evidence.get("critical_sectors", []),
            timestamp=datetime.now(UTC).isoformat(),
        )

    except Exception as e:
        logger.error("Failed to get capacity utilization", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/capacity-utilization/{sector}", response_model=SectorUtilizationResponse)
async def get_sector_utilization(sector: str) -> SectorUtilizationResponse:
    """Get capacity utilization for a specific sector.

    Args:
        sector: One of: total, manufacturing, mining, utilities
    """
    valid_sectors = ["total", "manufacturing", "mining", "utilities"]
    if sector not in valid_sectors:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid sector. Must be one of: {valid_sectors}",
        )

    try:
        db = get_db()
        monitor = CapacityUtilizationMonitor(db=db)

        util = await monitor.get_sector_utilization(sector)
        if util is None:
            raise HTTPException(status_code=404, detail=f"No data for sector: {sector}")

        return SectorUtilizationResponse(
            sector=util.sector,
            sector_name=util.sector_name,
            current_utilization=util.current_utilization,
            threshold=util.threshold,
            historical_avg=util.historical_avg,
            deviation_from_avg=util.deviation_from_avg,
            is_above_threshold=util.is_above_threshold,
            score=util.score,
            status=util.status,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to get sector utilization", sector=sector, error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/capacity-utilization/historical/all")
async def get_capacity_historical() -> dict[str, Any]:
    """Get historical deviation for all sectors."""
    try:
        db = get_db()
        monitor = CapacityUtilizationMonitor(db=db)

        return await monitor.calculate_historical_deviation()

    except Exception as e:
        logger.error("Failed to get capacity historical", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/dashboard", response_model=DashboardResponse)
async def get_analysis_dashboard() -> DashboardResponse:
    """Get composite dashboard with all analysis monitors.

    Returns scores and alerts from all monitors in a single response.
    """
    try:
        db = get_db()

        import asyncio

        is_monitor = InventorySalesMonitor(db=db)
        price_monitor = PriceSpikeDetector(db=db)
        shipping_monitor = ShippingCongestionIndex(db=db)
        energy_monitor = EnergyCrunchDetector(db=db)
        mineral_monitor = CriticalMineralRisk(db=db)
        labor_monitor = LaborTightnessMonitor(db=db)
        capacity_monitor = CapacityUtilizationMonitor(db=db)

        results = await asyncio.gather(
            is_monitor.calculate_score(),
            price_monitor.calculate_score(),
            shipping_monitor.calculate_score(),
            energy_monitor.calculate_score(),
            mineral_monitor.calculate_score(),
            labor_monitor.calculate_score(),
            capacity_monitor.calculate_score(),
            return_exceptions=True,
        )

        monitors = {}
        alerts = []
        scores = []

        monitor_names = [
            "inventory_sales",
            "price_spikes",
            "shipping_congestion",
            "energy_crunch",
            "mineral_risk",
            "labor_tightness",
            "capacity_utilization",
        ]

        for name, result in zip(monitor_names, results):
            if isinstance(result, Exception):
                logger.warning(f"Monitor {name} failed", error=str(result))
                continue

            monitors[name] = MonitorScoreResponse(
                score=result.score,
                severity=result.severity,
                confidence=result.confidence,
                alert_level=result.alert_level,
                components=result.components,
                evidence=result.evidence,
                description=result.description,
                timestamp=result.timestamp.isoformat(),
            )

            scores.append(result.score)

            if result.score >= 60:
                alerts.append({
                    "monitor": name,
                    "score": result.score,
                    "alert_level": result.alert_level,
                    "description": result.description,
                })

        overall_stress = sum(scores) / len(scores) if scores else 0.0

        alerts.sort(key=lambda x: x["score"], reverse=True)

        return DashboardResponse(
            timestamp=datetime.now(UTC).isoformat(),
            overall_stress=overall_stress,
            monitors=monitors,
            alerts=alerts,
        )

    except Exception as e:
        logger.error("Failed to get dashboard", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))
