"""Base monitor class with shared utilities for analysis monitors.

Provides common functionality for:
- Normalization (0-100 scaling)
- Z-score calculations
- Historical baseline comparisons
- Alert threshold checking
- Signal generation
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta, UTC
from typing import Any

import numpy as np
import pandas as pd
import structlog

from src.analysis.signals import (
    BottleneckCategory,
    BottleneckSignalData,
)
from src.storage.timescale import TimescaleDB

logger = structlog.get_logger()


@dataclass
class MonitorResult:
    """Base result class for all monitors."""

    score: float  # 0-100 normalized score
    severity: float  # 0-1 severity for signal generation
    confidence: float  # 0-1 confidence level
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    components: dict[str, float] = field(default_factory=dict)
    evidence: dict[str, Any] = field(default_factory=dict)
    description: str = ""

    @property
    def alert_level(self) -> str:
        """Get alert level based on score."""
        if self.score >= 80:
            return "critical"
        elif self.score >= 60:
            return "elevated"
        elif self.score >= 40:
            return "moderate"
        else:
            return "normal"

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "score": self.score,
            "severity": self.severity,
            "confidence": self.confidence,
            "alert_level": self.alert_level,
            "timestamp": self.timestamp.isoformat(),
            "components": self.components,
            "evidence": self.evidence,
            "description": self.description,
        }


class BaseMonitor(ABC):
    """Abstract base class for analysis monitors.

    Provides shared utilities for:
    - Data normalization to 0-100 scale
    - Z-score calculations with rolling windows
    - Historical baseline comparisons
    - Signal generation from monitor results
    """

    def __init__(
        self,
        db: TimescaleDB | None = None,
        alert_threshold: float = 60.0,
        critical_threshold: float = 80.0,
    ):
        """Initialize monitor.

        Args:
            db: Database connection for fetching data
            alert_threshold: Score threshold for elevated alert (0-100)
            critical_threshold: Score threshold for critical alert (0-100)
        """
        self.db = db
        self.alert_threshold = alert_threshold
        self.critical_threshold = critical_threshold
        self.logger = logger.bind(component=self.__class__.__name__)

    @abstractmethod
    async def calculate_score(self) -> MonitorResult:
        """Calculate the monitor's composite score.

        Returns:
            MonitorResult with score, severity, and evidence
        """
        pass

    @abstractmethod
    def get_category(self) -> BottleneckCategory:
        """Get the bottleneck category for this monitor."""
        pass

    @staticmethod
    def normalize_to_100(
        value: float,
        min_val: float,
        max_val: float,
        invert: bool = False,
    ) -> float:
        """Normalize a value to 0-100 scale.

        Args:
            value: The value to normalize
            min_val: Minimum value (maps to 0)
            max_val: Maximum value (maps to 100)
            invert: If True, invert the scale (high value = low score)

        Returns:
            Normalized value between 0 and 100
        """
        if max_val == min_val:
            return 50.0

        normalized = (value - min_val) / (max_val - min_val) * 100
        normalized = max(0.0, min(100.0, normalized))

        if invert:
            normalized = 100.0 - normalized

        return normalized

    @staticmethod
    def normalize_z_score_to_100(z_score: float, max_z: float = 4.0) -> float:
        """Convert Z-score to 0-100 scale.

        Args:
            z_score: The Z-score value
            max_z: Maximum Z-score (maps to 100)

        Returns:
            Normalized score between 0 and 100
        """
        abs_z = abs(z_score)
        normalized = min(100.0, (abs_z / max_z) * 100)
        return normalized

    @staticmethod
    def normalize_percentile_to_100(
        value: float,
        historical: pd.Series | np.ndarray,
    ) -> float:
        """Convert value to percentile-based 0-100 score.

        Args:
            value: Current value
            historical: Historical values for comparison

        Returns:
            Percentile score (0-100)
        """
        if len(historical) == 0:
            return 50.0

        percentile = (historical < value).sum() / len(historical) * 100
        return percentile

    @staticmethod
    def calculate_z_score(
        value: float,
        mean: float,
        std: float,
    ) -> float:
        """Calculate Z-score for a single value.

        Args:
            value: Current value
            mean: Historical mean
            std: Historical standard deviation

        Returns:
            Z-score
        """
        if std == 0 or np.isnan(std):
            return 0.0
        return (value - mean) / std

    @staticmethod
    def calculate_rolling_z_scores(
        series: pd.Series,
        window: int = 90,
        min_periods: int | None = None,
    ) -> pd.Series:
        """Calculate rolling Z-scores for a time series.

        Args:
            series: Time series data
            window: Rolling window size
            min_periods: Minimum periods for calculation

        Returns:
            Series of Z-scores
        """
        if min_periods is None:
            min_periods = window // 2

        rolling_mean = series.rolling(window=window, min_periods=min_periods).mean()
        rolling_std = series.rolling(window=window, min_periods=min_periods).std()

        z_scores = (series - rolling_mean) / rolling_std
        return z_scores.fillna(0.0)

    @staticmethod
    def get_latest_z_score(
        series: pd.Series,
        window: int = 90,
    ) -> tuple[float, float, float]:
        """Get Z-score for the latest value in a series.

        Args:
            series: Time series data
            window: Rolling window size for baseline

        Returns:
            Tuple of (z_score, mean, std)
        """
        if len(series) < 2:
            return 0.0, 0.0, 0.0

        # Use all but latest value for baseline
        baseline = series.iloc[:-1].tail(window)
        mean = baseline.mean()
        std = baseline.std()

        latest = series.iloc[-1]
        z_score = BaseMonitor.calculate_z_score(latest, mean, std)

        return z_score, mean, std

    @staticmethod
    def calculate_deviation_from_baseline(
        current: float,
        baseline: float,
    ) -> tuple[float, float]:
        """Calculate deviation from baseline.

        Args:
            current: Current value
            baseline: Baseline value

        Returns:
            Tuple of (absolute_deviation, percent_deviation)
        """
        absolute = current - baseline
        percent = (absolute / baseline * 100) if baseline != 0 else 0.0
        return absolute, percent

    @staticmethod
    def calculate_seasonal_deviation(
        series: pd.Series,
        current_value: float,
        period: int = 52,  # Weekly data, 52 weeks
        years_back: int = 5,
    ) -> tuple[float, float, float]:
        """Calculate deviation from seasonal average.

        Args:
            series: Historical time series
            current_value: Current value to compare
            period: Seasonal period (e.g., 52 for weekly, 12 for monthly)
            years_back: Number of years for seasonal average

        Returns:
            Tuple of (seasonal_avg, deviation, percent_deviation)
        """
        if len(series) < period:
            return current_value, 0.0, 0.0

        historical_values = []
        for year in range(1, years_back + 1):
            idx = -1 - (year * period)
            if abs(idx) <= len(series):
                historical_values.append(series.iloc[idx])

        if not historical_values:
            return current_value, 0.0, 0.0

        seasonal_avg = np.mean(historical_values)
        deviation = current_value - seasonal_avg
        percent_dev = (deviation / seasonal_avg * 100) if seasonal_avg != 0 else 0.0

        return seasonal_avg, deviation, percent_dev

    def check_sigma_threshold(
        self,
        z_score: float,
        sigma: float = 2.0,
    ) -> bool:
        """Check if Z-score exceeds sigma threshold.

        Args:
            z_score: The Z-score to check
            sigma: Number of standard deviations

        Returns:
            True if |z_score| >= sigma
        """
        return abs(z_score) >= sigma

    def check_alert_threshold(self, score: float) -> str:
        """Check which alert threshold is exceeded.

        Args:
            score: The score to check (0-100)

        Returns:
            Alert level string
        """
        if score >= self.critical_threshold:
            return "critical"
        elif score >= self.alert_threshold:
            return "elevated"
        else:
            return "normal"

    def create_signal(
        self,
        result: MonitorResult,
        subcategory: str,
        affected_sectors: list[str] | None = None,
        affected_commodities: list[str] | None = None,
        source_series: list[str] | None = None,
    ) -> BottleneckSignalData:
        """Create a bottleneck signal from monitor result.

        Args:
            result: MonitorResult from calculate_score()
            subcategory: Signal subcategory
            affected_sectors: List of affected sector codes
            affected_commodities: List of affected commodities
            source_series: List of source data series IDs

        Returns:
            BottleneckSignalData ready for storage/alerting
        """
        return BottleneckSignalData(
            category=self.get_category(),
            subcategory=subcategory,
            severity=result.severity,
            confidence=result.confidence,
            affected_sectors=affected_sectors or [],
            affected_commodities=affected_commodities or [],
            source_series=source_series or [],
            evidence={
                "score": result.score,
                "alert_level": result.alert_level,
                **result.evidence,
            },
            description=result.description,
        )

    async def run_and_alert(
        self,
        subcategory: str = "monitor_alert",
        affected_sectors: list[str] | None = None,
    ) -> tuple[MonitorResult, BottleneckSignalData | None]:
        """Run monitor and generate alert if threshold exceeded.

        Args:
            subcategory: Signal subcategory
            affected_sectors: List of affected sectors

        Returns:
            Tuple of (result, signal or None if below threshold)
        """
        result = await self.calculate_score()

        signal = None
        if result.score >= self.alert_threshold:
            signal = self.create_signal(
                result=result,
                subcategory=subcategory,
                affected_sectors=affected_sectors,
            )
            self.logger.info(
                "Alert generated",
                score=result.score,
                alert_level=result.alert_level,
                severity=result.severity,
            )

        return result, signal

    async def fetch_series(
        self,
        series_id: str,
        lookback_days: int = 365,
    ) -> pd.Series | None:
        """Fetch a time series from the database.

        Args:
            series_id: Series identifier
            lookback_days: Number of days to look back

        Returns:
            pandas Series with timestamp index, or None if not available
        """
        if self.db is None:
            return None

        try:
            start_date = datetime.now(UTC) - timedelta(days=lookback_days)
            data = await self.db.get_series_data(series_id, start_date=start_date)

            if data:
                df = pd.DataFrame(data)
                df["timestamp"] = pd.to_datetime(df["timestamp"])
                df.set_index("timestamp", inplace=True)
                return df["value"].sort_index()
        except Exception as e:
            self.logger.warning(
                "Failed to fetch series",
                series_id=series_id,
                error=str(e),
            )

        return None

    async def fetch_multiple_series(
        self,
        series_ids: list[str],
        lookback_days: int = 365,
    ) -> dict[str, pd.Series]:
        """Fetch multiple time series from the database.

        Args:
            series_ids: List of series identifiers
            lookback_days: Number of days to look back

        Returns:
            Dictionary mapping series_id to pandas Series
        """
        import asyncio

        results = {}

        async def fetch_one(sid: str) -> tuple[str, pd.Series | None]:
            series = await self.fetch_series(sid, lookback_days)
            return sid, series

        fetched = await asyncio.gather(*[fetch_one(sid) for sid in series_ids])

        for sid, series in fetched:
            if series is not None:
                results[sid] = series

        return results

    @staticmethod
    def weighted_composite(
        components: dict[str, float],
        weights: dict[str, float],
    ) -> float:
        """Calculate weighted composite score.

        Args:
            components: Dictionary of component name to score (0-100)
            weights: Dictionary of component name to weight (should sum to 1.0)

        Returns:
            Weighted composite score (0-100)
        """
        total = 0.0
        weight_sum = 0.0

        for name, score in components.items():
            weight = weights.get(name, 0.0)
            total += score * weight
            weight_sum += weight

        if weight_sum == 0:
            return 0.0

        # Normalize if weights don't sum to 1
        return total / weight_sum

    @staticmethod
    def score_to_severity(score: float) -> float:
        """Convert 0-100 score to 0-1 severity.

        Args:
            score: Score from 0-100

        Returns:
            Severity from 0-1
        """
        return min(1.0, score / 100.0)

    @staticmethod
    def calculate_confidence(
        data_completeness: float,
        sample_size: int,
        min_samples: int = 30,
    ) -> float:
        """Calculate confidence based on data quality.

        Args:
            data_completeness: Fraction of expected data available (0-1)
            sample_size: Number of data points
            min_samples: Minimum samples for full confidence

        Returns:
            Confidence score from 0.5 to 1.0
        """
        size_factor = min(1.0, sample_size / min_samples)
        confidence = 0.5 + (data_completeness * size_factor * 0.5)
        return min(1.0, confidence)
