"""Forecasting module for bottleneck predictions.

Supports both statistical and LLM-powered forecasting with automatic fallback.
"""

from src.forecasting.integration import (
    BottleneckForecast,
    BottleneckForecaster,
    ResearchReport,
    get_forecaster,
)
from src.forecasting.llm_config import (
    LLMConfig,
    get_llm_config,
    get_forecasting_llm,
    is_llm_available,
    estimate_cost,
)
from src.forecasting.llm_forecaster import (
    BottleneckForecastBot,
    BottleneckQuestion,
    ForecastResult,
    ReasonedPrediction,
    get_forecast_bot,
)

__all__ = [
    # Core forecaster
    "BottleneckForecaster",
    "BottleneckForecast",
    "ResearchReport",
    "get_forecaster",
    # LLM configuration
    "LLMConfig",
    "get_llm_config",
    "get_forecasting_llm",
    "is_llm_available",
    "estimate_cost",
    # LLM forecaster
    "BottleneckForecastBot",
    "BottleneckQuestion",
    "ForecastResult",
    "ReasonedPrediction",
    "get_forecast_bot",
]
