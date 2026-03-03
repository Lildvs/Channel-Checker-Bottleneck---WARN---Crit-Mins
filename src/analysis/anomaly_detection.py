"""Anomaly detection for time series data."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

import numpy as np
import pandas as pd
import structlog
from scipy import stats

logger = structlog.get_logger()


class AnomalyDetectionMethod(Protocol):
    """Protocol for anomaly detection methods."""

    def detect(
        self,
        series: pd.Series,
        **kwargs: Any,
    ) -> list["DetectedAnomaly"]: ...


@dataclass
class DetectedAnomaly:
    """A detected anomaly in time series data."""

    timestamp: datetime
    value: float
    expected_value: float
    z_score: float
    severity: float  # 0-1 scale
    detection_method: str
    anomaly_type: str  # "spike", "drop", "trend_break"
    metadata: dict[str, Any] | None = None


class ZScoreDetector:
    """Detect anomalies using rolling Z-score."""

    def __init__(
        self,
        window: int = 90,
        threshold: float = 2.5,
        min_periods: int = 30,
    ):
        """Initialize Z-score detector.

        Args:
            window: Rolling window size in days
            threshold: Z-score threshold for anomaly detection
            min_periods: Minimum periods required for calculation
        """
        self.window = window
        self.threshold = threshold
        self.min_periods = min_periods

    def detect(
        self,
        series: pd.Series,
        **kwargs: Any,
    ) -> list[DetectedAnomaly]:
        """Detect anomalies using rolling Z-score.

        Args:
            series: Time series data (index should be datetime)

        Returns:
            List of detected anomalies
        """
        if len(series) < self.min_periods:
            return []

        rolling_mean = series.rolling(window=self.window, min_periods=self.min_periods).mean()
        rolling_std = series.rolling(window=self.window, min_periods=self.min_periods).std()
        z_scores = (series - rolling_mean) / rolling_std

        anomalies: list[DetectedAnomaly] = []

        for idx, z in z_scores.items():
            if pd.isna(z):
                continue

            if abs(z) >= self.threshold:
                value = series[idx]
                expected = rolling_mean[idx]
                anomaly_type = "spike" if z > 0 else "drop"

                severity = min(1.0, abs(z) / 5.0)

                anomalies.append(
                    DetectedAnomaly(
                        timestamp=idx if isinstance(idx, datetime) else pd.Timestamp(idx).to_pydatetime(),
                        value=float(value),
                        expected_value=float(expected),
                        z_score=float(z),
                        severity=severity,
                        detection_method="z_score",
                        anomaly_type=anomaly_type,
                        metadata={
                            "window": self.window,
                            "threshold": self.threshold,
                            "rolling_std": float(rolling_std[idx]),
                        },
                    )
                )

        return anomalies


class IQRDetector:
    """Detect anomalies using Interquartile Range method.

    IQR-based detection is robust to non-normal distributions and less
    sensitive to extreme outliers compared to Z-score methods.
    """

    def __init__(
        self,
        window: int = 90,
        iqr_multiplier: float = 1.5,
        min_periods: int = 30,
    ):
        """Initialize IQR detector.

        Args:
            window: Rolling window size in days
            iqr_multiplier: Multiplier for IQR bounds (1.5=outlier, 3.0=extreme)
            min_periods: Minimum periods required for calculation
        """
        self.window = window
        self.iqr_multiplier = iqr_multiplier
        self.min_periods = min_periods

    def detect(
        self,
        series: pd.Series,
        **kwargs: Any,
    ) -> list[DetectedAnomaly]:
        """Detect anomalies using rolling IQR method.

        Detection logic:
        - Calculate rolling Q1 (25th percentile) and Q3 (75th percentile)
        - Compute IQR = Q3 - Q1
        - Lower bound = Q1 - (multiplier * IQR)
        - Upper bound = Q3 + (multiplier * IQR)
        - Flag values outside bounds as anomalies

        Args:
            series: Time series data (index should be datetime)

        Returns:
            List of detected anomalies
        """
        if len(series) < self.min_periods:
            return []

        rolling_q1 = series.rolling(window=self.window, min_periods=self.min_periods).quantile(0.25)
        rolling_q3 = series.rolling(window=self.window, min_periods=self.min_periods).quantile(0.75)
        iqr = rolling_q3 - rolling_q1
        lower_bound = rolling_q1 - (self.iqr_multiplier * iqr)
        upper_bound = rolling_q3 + (self.iqr_multiplier * iqr)

        rolling_median = series.rolling(window=self.window, min_periods=self.min_periods).median()

        anomalies: list[DetectedAnomaly] = []

        for idx in series.index:
            value = series[idx]
            lb = lower_bound[idx]
            ub = upper_bound[idx]
            median = rolling_median[idx]
            current_iqr = iqr[idx]

            if pd.isna(lb) or pd.isna(ub) or pd.isna(median) or pd.isna(current_iqr):
                continue

            if value < lb or value > ub:
                if current_iqr > 0:
                    if value > ub:
                        iqr_distance = (value - ub) / current_iqr
                        anomaly_type = "spike"
                    else:
                        iqr_distance = (lb - value) / current_iqr
                        anomaly_type = "drop"
                else:
                    # IQR is zero (constant values), use distance from median
                    iqr_distance = abs(value - median) if median != 0 else abs(value)
                    anomaly_type = "spike" if value > median else "drop"

                severity = min(1.0, iqr_distance / 3.0)

                anomalies.append(
                    DetectedAnomaly(
                        timestamp=idx if isinstance(idx, datetime) else pd.Timestamp(idx).to_pydatetime(),
                        value=float(value),
                        expected_value=float(median),
                        z_score=float(iqr_distance),  # Using IQR distance as pseudo-z-score
                        severity=severity,
                        detection_method="iqr",
                        anomaly_type=anomaly_type,
                        metadata={
                            "window": self.window,
                            "iqr_multiplier": self.iqr_multiplier,
                            "q1": float(rolling_q1[idx]),
                            "q3": float(rolling_q3[idx]),
                            "iqr": float(current_iqr),
                            "lower_bound": float(lb),
                            "upper_bound": float(ub),
                        },
                    )
                )

        return anomalies


class SeasonalAnomalyDetector:
    """Detect anomalies accounting for seasonality."""

    def __init__(
        self,
        period: int = 12,  # Monthly data
        threshold: float = 2.5,
    ):
        """Initialize seasonal anomaly detector.

        Args:
            period: Seasonal period (12 for monthly, 52 for weekly)
            threshold: Threshold for anomaly detection
        """
        self.period = period
        self.threshold = threshold

    def detect(
        self,
        series: pd.Series,
        **kwargs: Any,
    ) -> list[DetectedAnomaly]:
        """Detect anomalies using seasonal decomposition.

        Args:
            series: Time series data

        Returns:
            List of detected anomalies
        """
        if len(series) < 2 * self.period:
            return []

        try:
            from statsmodels.tsa.seasonal import seasonal_decompose

            decomposition = seasonal_decompose(
                series,
                model="additive",
                period=self.period,
                extrapolate_trend="freq",
            )

            residual = decomposition.resid.dropna()

            mean_resid = residual.mean()
            std_resid = residual.std()

            anomalies: list[DetectedAnomaly] = []

            for idx, value in residual.items():
                z = (value - mean_resid) / std_resid

                if abs(z) >= self.threshold:
                    original_value = series[idx]
                    expected = decomposition.trend[idx] + decomposition.seasonal[idx]
                    anomaly_type = "spike" if z > 0 else "drop"
                    severity = min(1.0, abs(z) / 5.0)

                    anomalies.append(
                        DetectedAnomaly(
                            timestamp=idx if isinstance(idx, datetime) else pd.Timestamp(idx).to_pydatetime(),
                            value=float(original_value),
                            expected_value=float(expected),
                            z_score=float(z),
                            severity=severity,
                            detection_method="seasonal_decomposition",
                            anomaly_type=anomaly_type,
                            metadata={
                                "period": self.period,
                                "trend": float(decomposition.trend[idx]),
                                "seasonal": float(decomposition.seasonal[idx]),
                            },
                        )
                    )

            return anomalies

        except Exception as e:
            logger.warning("Seasonal decomposition failed", error=str(e))
            return []


class TrendBreakDetector:
    """Detect structural breaks in trends."""

    def __init__(
        self,
        min_segment: int = 20,
        significance: float = 0.05,
    ):
        """Initialize trend break detector.

        Args:
            min_segment: Minimum segment size
            significance: P-value threshold for significance
        """
        self.min_segment = min_segment
        self.significance = significance

    def detect(
        self,
        series: pd.Series,
        **kwargs: Any,
    ) -> list[DetectedAnomaly]:
        """Detect trend breaks using change point detection.

        Args:
            series: Time series data

        Returns:
            List of detected anomalies
        """
        if len(series) < 2 * self.min_segment:
            return []

        anomalies: list[DetectedAnomaly] = []
        values = series.values
        indices = list(series.index)

        mean_val = np.mean(values)
        cumsum = np.cumsum(values - mean_val)

        for i in range(self.min_segment, len(values) - self.min_segment):
            before = values[:i]
            after = values[i:]

            # T-test for difference in means
            t_stat, p_value = stats.ttest_ind(before, after)

            if p_value < self.significance:
                idx = indices[i]
                value = values[i]
                expected = np.mean(before)

                effect = (np.mean(after) - np.mean(before)) / np.std(values)
                severity = min(1.0, abs(effect) / 2.0)

                anomaly_type = "trend_break_up" if np.mean(after) > np.mean(before) else "trend_break_down"

                anomalies.append(
                    DetectedAnomaly(
                        timestamp=idx if isinstance(idx, datetime) else pd.Timestamp(idx).to_pydatetime(),
                        value=float(value),
                        expected_value=float(expected),
                        z_score=float(t_stat),
                        severity=severity,
                        detection_method="trend_break",
                        anomaly_type=anomaly_type,
                        metadata={
                            "p_value": float(p_value),
                            "mean_before": float(np.mean(before)),
                            "mean_after": float(np.mean(after)),
                            "effect_size": float(effect),
                        },
                    )
                )

                # Skip ahead to avoid detecting same break multiple times
                break

        return anomalies


class LSTMDetector:
    """Placeholder for LSTM-based anomaly detection.

    Future implementation will use:
    - PyTorch or TensorFlow for deep learning
    - Sequence-to-sequence autoencoder architecture
    - Reconstruction error for anomaly scoring
    - Pre-trained models for common time series patterns

    This detector is not currently implemented and will raise
    NotImplementedError when called. Check the `is_available` property
    before use.
    """

    def __init__(
        self,
        model_path: str | None = None,
        sequence_length: int = 30,
        threshold: float = 0.95,
    ):
        """Initialize LSTM detector placeholder.

        Args:
            model_path: Path to pre-trained model weights
            sequence_length: Number of timesteps for input sequences
            threshold: Reconstruction error threshold percentile for anomaly detection
        """
        self.model_path = model_path
        self.sequence_length = sequence_length
        self.threshold = threshold
        self._model = None

    def detect(
        self,
        series: pd.Series,
        **kwargs: Any,
    ) -> list[DetectedAnomaly]:
        """Detect anomalies using LSTM autoencoder.

        This method is not yet implemented.

        Args:
            series: Time series data (index should be datetime)

        Raises:
            NotImplementedError: LSTM detection is not yet implemented

        Returns:
            List of detected anomalies (never returns - always raises)
        """
        raise NotImplementedError(
            "LSTM-based anomaly detection requires ML dependencies. "
            "Install with: pip install torch  "
            "Then implement model loading and inference. "
            "See https://pytorch.org/tutorials/ for LSTM autoencoder examples."
        )

    @property
    def is_available(self) -> bool:
        """Check if LSTM detection is available.

        Returns:
            False - LSTM detection is not yet implemented
        """
        return False

    def load_model(self, model_path: str | None = None) -> None:
        """Load pre-trained LSTM model weights.

        Args:
            model_path: Path to model weights (uses self.model_path if None)

        Raises:
            NotImplementedError: Model loading is not yet implemented
        """
        raise NotImplementedError(
            "LSTM model loading requires implementation. "
            "Expected file format: PyTorch state dict (.pt/.pth) "
            "or TensorFlow SavedModel."
        )


class AnomalyDetector:
    """Ensemble anomaly detector combining multiple methods."""

    def __init__(self):
        """Initialize anomaly detector with multiple methods."""
        self.methods = [
            ZScoreDetector(window=90, threshold=2.5),
            ZScoreDetector(window=30, threshold=3.0),
            IQRDetector(window=90, iqr_multiplier=1.5),
            IQRDetector(window=30, iqr_multiplier=2.0),  # Stricter short-term
            SeasonalAnomalyDetector(period=12, threshold=2.5),
            TrendBreakDetector(min_segment=20, significance=0.01),
        ]
        self.logger = logger.bind(component="anomaly_detector")

    async def detect(
        self,
        series: pd.Series,
        series_id: str,
        min_votes: int = 1,
    ) -> list[DetectedAnomaly]:
        """Detect anomalies using ensemble of methods.

        Args:
            series: Time series data
            series_id: ID of the series
            min_votes: Minimum methods that must agree

        Returns:
            List of detected anomalies
        """
        if len(series) < 30:
            self.logger.debug(
                "Series too short for anomaly detection",
                series_id=series_id,
                length=len(series),
            )
            return []

        all_anomalies: list[DetectedAnomaly] = []
        anomaly_votes: dict[str, list[DetectedAnomaly]] = {}

        for method in self.methods:
            try:
                detected = method.detect(series)
                for anomaly in detected:
                    key = anomaly.timestamp.strftime("%Y-%m-%d")
                    if key not in anomaly_votes:
                        anomaly_votes[key] = []
                    anomaly_votes[key].append(anomaly)
            except Exception as e:
                self.logger.warning(
                    "Anomaly detection method failed",
                    method=type(method).__name__,
                    error=str(e),
                )

        for date_key, votes in anomaly_votes.items():
            if len(votes) >= min_votes:
                best_anomaly = max(votes, key=lambda a: a.severity)
                all_anomalies.append(best_anomaly)

        self.logger.debug(
            "Anomaly detection complete",
            series_id=series_id,
            anomalies_found=len(all_anomalies),
        )

        return all_anomalies

    async def detect_multi_series(
        self,
        series_dict: dict[str, pd.Series],
        min_votes: int = 1,
    ) -> dict[str, list[DetectedAnomaly]]:
        """Detect anomalies across multiple series.

        Args:
            series_dict: Dictionary of series ID to time series
            min_votes: Minimum methods that must agree

        Returns:
            Dictionary of series ID to detected anomalies
        """
        import asyncio

        async def detect_with_error_handling(
            series_id: str, series: pd.Series
        ) -> tuple[str, list[DetectedAnomaly]]:
            try:
                anomalies = await self.detect(series, series_id, min_votes)
                return series_id, anomalies
            except Exception as e:
                self.logger.error(
                    "Failed to detect anomalies",
                    series_id=series_id,
                    error=str(e),
                )
                return series_id, []

        tasks = [
            detect_with_error_handling(series_id, series)
            for series_id, series in series_dict.items()
        ]
        detection_results = await asyncio.gather(*tasks)

        results: dict[str, list[DetectedAnomaly]] = {
            series_id: anomalies
            for series_id, anomalies in detection_results
            if anomalies
        }

        return results
