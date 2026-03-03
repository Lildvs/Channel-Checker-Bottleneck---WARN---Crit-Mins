"""Forecast-related API endpoints.

Supports both statistical and LLM-powered forecasting.
"""

from typing import Any

import structlog
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from src.analysis.signals import BottleneckCategory, BottleneckSignalData
from src.config.settings import get_settings
from src.forecasting.integration import get_forecaster
from src.forecasting.llm_config import is_llm_available, get_available_models
from src.storage.redis_cache import get_cache

logger = structlog.get_logger()
router = APIRouter()


class ForecastRequest(BaseModel):
    """Request model for forecast generation."""

    bottleneck_id: str
    horizon_days: int = Field(default=30, ge=1, le=365)
    use_llm: bool | None = Field(
        default=None,
        description="Force LLM or statistical. None uses system default.",
    )


class DurationForecastResponse(BaseModel):
    """Response model for duration forecast."""

    bottleneck_id: str
    expected_duration_days: int
    expected_resolution_date: str
    probability_persists_30_days: float
    probability_persists_60_days: float
    probability_persists_90_days: float
    confidence_lower_days: int
    confidence_upper_days: int
    model_used: str
    reasoning: str
    llm_cost_usd: float | None = None


class LLMStatusResponse(BaseModel):
    """Response model for LLM status check."""

    llm_available: bool
    llm_enabled: bool
    default_model: str
    available_models: list[str]
    total_cost_usd: float


class TrajectoryPoint(BaseModel):
    """Single point in severity trajectory."""

    day: int
    date: str
    severity: float
    lower_bound: float
    upper_bound: float


class TrajectoryForecastResponse(BaseModel):
    """Response model for severity trajectory forecast."""

    bottleneck_id: str
    trajectory: list[TrajectoryPoint]
    expected_resolution_day: int | None
    final_severity: float


class ResearchReportResponse(BaseModel):
    """Response model for research report."""

    bottleneck_id: str
    summary: str
    key_factors: list[str]
    historical_precedents: list[dict[str, Any]]
    data_sources: list[str]


@router.get("/llm-status", response_model=LLMStatusResponse)
async def get_llm_status() -> LLMStatusResponse:
    """Get current LLM forecasting status.

    Returns:
        Status of LLM availability and configuration
    """
    settings = get_settings()
    forecaster = get_forecaster()

    return LLMStatusResponse(
        llm_available=is_llm_available(),
        llm_enabled=forecaster.llm_enabled,
        default_model=settings.forecasting_default_model,
        available_models=get_available_models(),
        total_cost_usd=forecaster.total_llm_cost,
    )


@router.post("/duration", response_model=DurationForecastResponse)
async def forecast_duration(
    request: ForecastRequest,
    use_llm: bool | None = Query(
        None,
        description="Override LLM usage. None uses request body or system default.",
    ),
) -> DurationForecastResponse:
    """Forecast how long a bottleneck will persist.

    Supports both statistical and LLM-powered forecasting.
    LLM forecasting provides richer reasoning when available.

    Args:
        request: Forecast request with bottleneck ID
        use_llm: Query param to override LLM usage

    Returns:
        Duration forecast
    """
    try:
        cache = get_cache()
        cached = await cache.get_active_bottlenecks()

        bottleneck_data = None
        for b in cached:
            if b["id"] == request.bottleneck_id:
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
            description=bottleneck_data.get("description", ""),
        )

        # Determine LLM usage (query param > request body > system default)
        llm_override = use_llm if use_llm is not None else request.use_llm

        forecaster = get_forecaster()
        forecast = await forecaster.forecast_bottleneck_duration(
            signal, use_llm=llm_override
        )

        llm_cost = forecast.metadata.get("llm_cost_usd") if forecast.metadata else None

        return DurationForecastResponse(
            bottleneck_id=request.bottleneck_id,
            expected_duration_days=forecast.prediction["expected_duration_days"],
            expected_resolution_date=forecast.prediction["expected_resolution_date"],
            probability_persists_30_days=forecast.prediction["probability_persists_30_days"],
            probability_persists_60_days=forecast.prediction["probability_persists_60_days"],
            probability_persists_90_days=forecast.prediction["probability_persists_90_days"],
            confidence_lower_days=forecast.confidence_interval["lower_days"],
            confidence_upper_days=forecast.confidence_interval["upper_days"],
            model_used=forecast.model_used,
            reasoning=forecast.reasoning,
            llm_cost_usd=llm_cost,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to generate duration forecast", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/trajectory", response_model=TrajectoryForecastResponse)
async def forecast_trajectory(
    request: ForecastRequest,
    use_llm: bool | None = Query(
        None,
        description="Override LLM usage. None uses request body or system default.",
    ),
) -> TrajectoryForecastResponse:
    """Forecast how bottleneck severity will evolve.

    Supports both statistical and LLM-powered forecasting.

    Args:
        request: Forecast request with bottleneck ID and horizon
        use_llm: Query param to override LLM usage

    Returns:
        Severity trajectory forecast
    """
    try:
        cache = get_cache()
        cached = await cache.get_active_bottlenecks()

        bottleneck_data = None
        for b in cached:
            if b["id"] == request.bottleneck_id:
                bottleneck_data = b
                break

        if not bottleneck_data:
            raise HTTPException(status_code=404, detail="Bottleneck not found")

        signal = BottleneckSignalData(
            category=BottleneckCategory(bottleneck_data["category"]),
            severity=bottleneck_data["severity"],
            confidence=bottleneck_data["confidence"],
        )

        llm_override = use_llm if use_llm is not None else request.use_llm

        forecaster = get_forecaster()
        forecast = await forecaster.forecast_severity_trajectory(
            signal, horizon_days=request.horizon_days, use_llm=llm_override
        )

        return TrajectoryForecastResponse(
            bottleneck_id=request.bottleneck_id,
            trajectory=[
                TrajectoryPoint(**point)
                for point in forecast.prediction["trajectory"]
            ],
            expected_resolution_day=forecast.prediction.get("expected_resolution_day"),
            final_severity=forecast.prediction["final_severity"],
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to generate trajectory forecast", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{bottleneck_id}/research", response_model=ResearchReportResponse)
async def get_research_report(bottleneck_id: str) -> ResearchReportResponse:
    """Get research context for a bottleneck.

    Args:
        bottleneck_id: UUID of the bottleneck

    Returns:
        Research report
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
            source_series=bottleneck_data.get("source_series", []),
            description=bottleneck_data.get("description", ""),
        )

        forecaster = get_forecaster()
        report = await forecaster.research_bottleneck_context(signal)

        return ResearchReportResponse(
            bottleneck_id=bottleneck_id,
            summary=report.summary,
            key_factors=report.key_factors,
            historical_precedents=report.historical_precedents,
            data_sources=report.data_sources,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to generate research report", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))
