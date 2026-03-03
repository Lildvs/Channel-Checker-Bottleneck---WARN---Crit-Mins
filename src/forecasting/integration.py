"""Integration with forecasting tools for bottleneck predictions.

This module provides the main BottleneckForecaster class that supports
both statistical and LLM-powered forecasting with automatic fallback.
"""

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd
import structlog
from scipy import stats

from src.analysis.signals import BottleneckSignalData
from src.forecasting.llm_config import is_llm_available

logger = structlog.get_logger()


@dataclass
class BottleneckForecast:
    """Forecast for a bottleneck's resolution or evolution."""

    bottleneck_id: str
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    forecast_type: str = "duration"  # duration, severity_trajectory, resolution
    prediction: dict[str, Any] = field(default_factory=dict)
    confidence_interval: dict[str, float] = field(default_factory=dict)
    model_used: str = "statistical"
    reasoning: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "bottleneck_id": self.bottleneck_id,
            "created_at": self.created_at.isoformat(),
            "forecast_type": self.forecast_type,
            "prediction": self.prediction,
            "confidence_interval": self.confidence_interval,
            "model_used": self.model_used,
            "reasoning": self.reasoning,
            "metadata": self.metadata,
        }


@dataclass
class ResearchReport:
    """Research report on a bottleneck."""

    bottleneck_id: str
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    summary: str = ""
    key_factors: list[str] = field(default_factory=list)
    historical_precedents: list[dict[str, Any]] = field(default_factory=list)
    expert_opinions: list[str] = field(default_factory=list)
    data_sources: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


class BottleneckForecaster:
    """Forecaster for bottleneck duration and evolution.

    Supports both statistical forecasting (default) and LLM-powered
    forecasting when available and enabled.
    """

    def __init__(self, use_llm: bool = True):
        """Initialize the forecaster.

        Args:
            use_llm: Whether to use LLM forecasting when available.
                Falls back to statistical methods if LLM is unavailable.
        """
        self.logger = logger.bind(component="bottleneck_forecaster")
        self.use_llm = use_llm
        self._llm_forecaster: Any | None = None
        self._llm_checked = False

        # Historical average durations by category (in days)
        self.category_duration_priors: dict[str, tuple[float, float]] = {
            "inventory_squeeze": (45.0, 30.0),  # mean, std
            "price_spike": (30.0, 20.0),
            "shipping_congestion": (60.0, 40.0),
            "labor_tightness": (90.0, 45.0),
            "capacity_ceiling": (120.0, 60.0),
            "demand_surge": (40.0, 25.0),
            "supply_disruption": (75.0, 50.0),
            "energy_crunch": (50.0, 35.0),
            "credit_tightening": (180.0, 90.0),
            "sentiment_shift": (20.0, 15.0),
        }

    def _llm_available(self) -> bool:
        """Check if LLM forecasting is available."""
        if not self._llm_checked:
            self._llm_checked = True
            if is_llm_available():
                try:
                    from src.forecasting.llm_forecaster import BottleneckForecastBot

                    self._llm_forecaster = BottleneckForecastBot()
                    self.logger.info("LLM forecasting enabled")
                except ImportError as e:
                    self.logger.warning(
                        "LLM forecaster not available", error=str(e)
                    )
                    self._llm_forecaster = None
            else:
                self.logger.info("LLM forecasting not configured")
                self._llm_forecaster = None
        return self._llm_forecaster is not None

    async def forecast_bottleneck_duration(
        self,
        bottleneck: BottleneckSignalData,
        historical_data: pd.DataFrame | None = None,
        use_llm: bool | None = None,
    ) -> BottleneckForecast:
        """Forecast how long a bottleneck will persist.

        Args:
            bottleneck: The bottleneck signal to forecast
            historical_data: Optional historical data for the related series
            use_llm: Override instance setting for LLM usage

        Returns:
            Forecast with duration prediction
        """
        should_use_llm = use_llm if use_llm is not None else self.use_llm

        if should_use_llm and self._llm_available():
            return await self._forecast_duration_with_llm(bottleneck)

        return await self._forecast_duration_statistical(bottleneck, historical_data)

    async def _forecast_duration_with_llm(
        self,
        bottleneck: BottleneckSignalData,
    ) -> BottleneckForecast:
        """Forecast duration using LLM.

        Args:
            bottleneck: The bottleneck signal

        Returns:
            LLM-powered forecast
        """
        try:
            result = await self._llm_forecaster.forecast_duration(bottleneck)

            prediction = result.prediction.prediction_value
            expected_days = prediction.get("expected_duration_days", 60)
            resolution_date = datetime.now(UTC) + timedelta(days=expected_days)

            forecast = BottleneckForecast(
                bottleneck_id=str(bottleneck.id),
                forecast_type="duration",
                prediction={
                    "expected_duration_days": expected_days,
                    "expected_resolution_date": resolution_date.isoformat(),
                    "probability_persists_30_days": prediction.get(
                        "probability_persists_30_days", 0.5
                    ),
                    "probability_persists_60_days": prediction.get(
                        "probability_persists_60_days", 0.3
                    ),
                    "probability_persists_90_days": prediction.get(
                        "probability_persists_90_days", 0.15
                    ),
                },
                confidence_interval={
                    "lower_days": prediction.get("percentile_25", int(expected_days * 0.6)),
                    "upper_days": prediction.get("percentile_75", int(expected_days * 1.4)),
                    "confidence_level": 0.5,  # IQR
                },
                model_used=f"llm:{result.prediction.model_used}",
                reasoning=result.prediction.reasoning,
                metadata={
                    "llm_cost_usd": result.prediction.cost_usd,
                    "research_context_length": len(result.research_context),
                    "confidence": result.prediction.confidence,
                },
            )

            self.logger.info(
                "Generated LLM duration forecast",
                bottleneck_id=str(bottleneck.id),
                predicted_days=expected_days,
                model=result.prediction.model_used,
                cost_usd=result.prediction.cost_usd,
            )

            return forecast

        except Exception as e:
            self.logger.warning(
                "LLM forecast failed, falling back to statistical",
                error=str(e),
            )
            return await self._forecast_duration_statistical(bottleneck, None)

    async def _forecast_duration_statistical(
        self,
        bottleneck: BottleneckSignalData,
        historical_data: pd.DataFrame | None = None,
    ) -> BottleneckForecast:
        """Forecast duration using statistical methods.

        Args:
            bottleneck: The bottleneck signal
            historical_data: Optional historical data

        Returns:
            Statistical forecast
        """
        category = bottleneck.category.value

        prior_mean, prior_std = self.category_duration_priors.get(
            category, (60.0, 40.0)
        )

        severity_adjustment = 1.0 + (bottleneck.severity - 0.5) * 0.5
        adjusted_mean = prior_mean * severity_adjustment

        # Adjust based on confidence (higher confidence = tighter bounds)
        confidence_adjustment = 0.5 + bottleneck.confidence * 0.5
        adjusted_std = prior_std / confidence_adjustment

        predicted_days = int(adjusted_mean)
        lower_bound = int(max(7, adjusted_mean - 1.96 * adjusted_std))
        upper_bound = int(adjusted_mean + 1.96 * adjusted_std)

        prob_30_days = float(1 - stats.norm.cdf(30, adjusted_mean, adjusted_std))
        prob_60_days = float(1 - stats.norm.cdf(60, adjusted_mean, adjusted_std))
        prob_90_days = float(1 - stats.norm.cdf(90, adjusted_mean, adjusted_std))

        resolution_date = datetime.now(UTC) + timedelta(days=predicted_days)

        forecast = BottleneckForecast(
            bottleneck_id=str(bottleneck.id),
            forecast_type="duration",
            prediction={
                "expected_duration_days": predicted_days,
                "expected_resolution_date": resolution_date.isoformat(),
                "probability_persists_30_days": prob_30_days,
                "probability_persists_60_days": prob_60_days,
                "probability_persists_90_days": prob_90_days,
            },
            confidence_interval={
                "lower_days": lower_bound,
                "upper_days": upper_bound,
                "confidence_level": 0.95,
            },
            model_used="statistical:bayesian_prior_adjustment",
            reasoning=self._generate_reasoning(bottleneck, predicted_days),
            metadata={
                "prior_mean": prior_mean,
                "prior_std": prior_std,
                "severity_adjustment": severity_adjustment,
                "confidence_adjustment": confidence_adjustment,
            },
        )

        self.logger.info(
            "Generated statistical duration forecast",
            bottleneck_id=str(bottleneck.id),
            category=category,
            predicted_days=predicted_days,
        )

        return forecast

    def _generate_reasoning(
        self,
        bottleneck: BottleneckSignalData,
        predicted_days: int,
    ) -> str:
        """Generate human-readable reasoning for the forecast."""
        category = bottleneck.category.value.replace("_", " ")
        severity_desc = (
            "severe" if bottleneck.severity > 0.7
            else "moderate" if bottleneck.severity > 0.4
            else "mild"
        )

        sectors = ", ".join(bottleneck.affected_sectors[:3]) if bottleneck.affected_sectors else "multiple sectors"

        return (
            f"Based on historical patterns for {category} bottlenecks "
            f"and the current {severity_desc} severity level ({bottleneck.severity:.0%}), "
            f"this bottleneck is expected to persist for approximately {predicted_days} days. "
            f"The affected sectors ({sectors}) typically experience recovery periods "
            f"consistent with this estimate. Confidence in this forecast is "
            f"{bottleneck.confidence:.0%} based on available data quality."
        )

    async def forecast_severity_trajectory(
        self,
        bottleneck: BottleneckSignalData,
        horizon_days: int = 30,
        use_llm: bool | None = None,
    ) -> BottleneckForecast:
        """Forecast how severity will evolve over time.

        Args:
            bottleneck: The bottleneck signal
            horizon_days: Days to forecast ahead
            use_llm: Override instance setting for LLM usage

        Returns:
            Forecast with severity trajectory
        """
        should_use_llm = use_llm if use_llm is not None else self.use_llm

        if should_use_llm and self._llm_available():
            return await self._forecast_trajectory_with_llm(bottleneck, horizon_days)

        return await self._forecast_trajectory_statistical(bottleneck, horizon_days)

    async def _forecast_trajectory_with_llm(
        self,
        bottleneck: BottleneckSignalData,
        horizon_days: int = 30,
    ) -> BottleneckForecast:
        """Forecast trajectory using LLM.

        Args:
            bottleneck: The bottleneck signal
            horizon_days: Forecast horizon

        Returns:
            LLM-powered trajectory forecast
        """
        try:
            result = await self._llm_forecaster.forecast_trajectory(
                bottleneck, horizon_days=horizon_days
            )

            prediction = result.prediction.prediction_value

            # Build trajectory from LLM response
            trajectory = []
            for day in [7, 14, 21, 30]:
                if day <= horizon_days:
                    severity = prediction.get(f"day_{day}_severity", bottleneck.severity * 0.9)
                    trajectory.append({
                        "day": day,
                        "date": (datetime.now(UTC) + timedelta(days=day)).strftime("%Y-%m-%d"),
                        "severity": float(severity),
                        "lower_bound": float(max(0, severity - 0.1)),
                        "upper_bound": float(min(1, severity + 0.1)),
                    })

            forecast = BottleneckForecast(
                bottleneck_id=str(bottleneck.id),
                forecast_type="severity_trajectory",
                prediction={
                    "trajectory": trajectory,
                    "expected_resolution_day": prediction.get("expected_resolution_day"),
                    "trajectory_type": prediction.get("trajectory_type", "unknown"),
                    "final_severity": trajectory[-1]["severity"] if trajectory else bottleneck.severity,
                },
                confidence_interval={
                    "model_uncertainty": 0.1,
                    "data_uncertainty": 1 - bottleneck.confidence,
                },
                model_used=f"llm:{result.prediction.model_used}",
                reasoning=result.prediction.reasoning,
                metadata={
                    "llm_cost_usd": result.prediction.cost_usd,
                },
            )

            return forecast

        except Exception as e:
            self.logger.warning(
                "LLM trajectory forecast failed, falling back to statistical",
                error=str(e),
            )
            return await self._forecast_trajectory_statistical(bottleneck, horizon_days)

    async def _forecast_trajectory_statistical(
        self,
        bottleneck: BottleneckSignalData,
        horizon_days: int = 30,
    ) -> BottleneckForecast:
        """Forecast trajectory using statistical methods.

        Args:
            bottleneck: The bottleneck signal
            horizon_days: Forecast horizon

        Returns:
            Statistical trajectory forecast
        """
        current_severity = bottleneck.severity
        current_age_days = (datetime.now(UTC) - bottleneck.detected_at).days

        trajectory: list[dict[str, Any]] = []

        for day in range(1, horizon_days + 1):
            total_days = current_age_days + day

            base_decay = np.exp(-total_days / 60)  # 60-day half-life roughly
            severity = current_severity * (0.3 + 0.7 * base_decay)

            uncertainty = 0.1 * np.sqrt(day / 30)

            trajectory.append({
                "day": day,
                "date": (datetime.now(UTC) + timedelta(days=day)).strftime("%Y-%m-%d"),
                "severity": float(np.clip(severity, 0, 1)),
                "lower_bound": float(np.clip(severity - uncertainty, 0, 1)),
                "upper_bound": float(np.clip(severity + uncertainty, 0, 1)),
            })

        resolution_threshold = 0.2
        resolution_day = next(
            (t["day"] for t in trajectory if t["severity"] < resolution_threshold),
            None,
        )

        forecast = BottleneckForecast(
            bottleneck_id=str(bottleneck.id),
            forecast_type="severity_trajectory",
            prediction={
                "trajectory": trajectory,
                "expected_resolution_day": resolution_day,
                "resolution_threshold": resolution_threshold,
                "final_severity": trajectory[-1]["severity"] if trajectory else current_severity,
            },
            confidence_interval={
                "model_uncertainty": 0.15,
                "data_uncertainty": 1 - bottleneck.confidence,
            },
            model_used="statistical:exponential_decay_with_uncertainty",
            reasoning=(
                f"Severity is modeled to decay exponentially from {current_severity:.0%} "
                f"with growing uncertainty over the {horizon_days}-day horizon."
            ),
        )

        return forecast

    @property
    def llm_enabled(self) -> bool:
        """Check if LLM is enabled and available."""
        return self.use_llm and self._llm_available()

    @property
    def total_llm_cost(self) -> float:
        """Get total LLM costs incurred."""
        if self._llm_forecaster:
            return self._llm_forecaster.total_cost
        return 0.0

    async def research_bottleneck_context(
        self,
        bottleneck: BottleneckSignalData,
    ) -> ResearchReport | None:
        """Generate research context for a bottleneck.

        Args:
            bottleneck: The bottleneck to research

        Returns:
            Research report with context, or None if no research data available
        """
        category = bottleneck.category.value

        # Research requires external data sources - return empty report if unavailable
        self.logger.warning(
            "Research context requires external data sources (not configured)",
            bottleneck_id=str(bottleneck.id),
            category=category,
        )

        return ResearchReport(
            bottleneck_id=str(bottleneck.id),
            summary="",
            key_factors=[],
            historical_precedents=[],
            data_sources=list(bottleneck.source_series),
            metadata={"data_unavailable": True},
        )

    def _generate_summary(self, bottleneck: BottleneckSignalData) -> str:
        """Generate summary of the bottleneck."""
        category = bottleneck.category.value.replace("_", " ").title()
        sectors = ", ".join(bottleneck.affected_sectors[:3]) if bottleneck.affected_sectors else "multiple sectors"

        return (
            f"{category} detected with {bottleneck.severity:.0%} severity, "
            f"affecting {sectors}. "
            f"{bottleneck.description}"
        )

    def _identify_key_factors(self, bottleneck: BottleneckSignalData) -> list[str]:
        """Identify key factors driving the bottleneck."""
        factors = []

        if bottleneck.severity > 0.7:
            factors.append("High severity indicates systemic stress")

        if len(bottleneck.affected_sectors) > 2:
            factors.append("Multi-sector impact amplifies economic effects")

        if bottleneck.anomalies:
            factors.append(f"{len(bottleneck.anomalies)} anomalous data points detected")

        if bottleneck.category.value in ["energy_crunch", "supply_disruption"]:
            factors.append("Supply-side constraints typically have longer resolution times")

        if bottleneck.category.value in ["demand_surge", "sentiment_shift"]:
            factors.append("Demand-side factors may self-correct more quickly")

        return factors or ["Insufficient data to identify specific factors"]

    def _find_precedents(self, bottleneck: BottleneckSignalData) -> list[dict[str, Any]]:
        """Find historical precedents for this type of bottleneck.

        Returns:
            Empty list - historical precedent data requires database configuration
        """
        # Historical precedent lookup requires database query
        # Return empty list until data source is configured
        self.logger.warning(
            "Historical precedent lookup requires database configuration",
            category=bottleneck.category.value,
        )
        return []


# Global forecaster instance
_forecaster: BottleneckForecaster | None = None


def get_forecaster() -> BottleneckForecaster:
    """Get the global forecaster instance."""
    global _forecaster
    if _forecaster is None:
        _forecaster = BottleneckForecaster()
    return _forecaster
