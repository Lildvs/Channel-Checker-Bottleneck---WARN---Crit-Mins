"""Data validation and quality scoring."""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import pandas as pd
import structlog

from src.data_ingestion.base_collector import DataPoint

logger = structlog.get_logger()


@dataclass
class ValidationResult:
    """Result of data validation."""

    is_valid: bool
    quality_score: float
    issues: list[str]
    warnings: list[str]


class DataValidator:
    """Validates data quality and assigns quality scores."""

    def __init__(self):
        """Initialize the validator."""
        self.logger = logger.bind(component="validator")

    def validate_data_point(
        self,
        data_point: DataPoint,
        expected_range: tuple[float, float] | None = None,
    ) -> ValidationResult:
        """Validate a single data point.

        Args:
            data_point: Data point to validate
            expected_range: Optional expected value range (min, max)

        Returns:
            Validation result
        """
        issues: list[str] = []
        warnings: list[str] = []
        quality_score = 1.0

        if data_point.value is None:
            issues.append("Missing value")
            quality_score *= 0.0

        if data_point.timestamp is None:
            issues.append("Missing timestamp")
            quality_score *= 0.0
        elif data_point.timestamp > datetime.now(UTC):
            warnings.append("Future timestamp")
            quality_score *= 0.9

        if data_point.value is not None and expected_range:
            min_val, max_val = expected_range
            if data_point.value < min_val or data_point.value > max_val:
                warnings.append(f"Value {data_point.value} outside expected range [{min_val}, {max_val}]")
                quality_score *= 0.8

        if data_point.is_preliminary:
            warnings.append("Preliminary data")
            quality_score *= 0.9

        if data_point.timestamp:
            age_days = (datetime.now(UTC) - data_point.timestamp).days
            if age_days > 365:
                warnings.append("Data older than 1 year")
                quality_score *= 0.95

        is_valid = len(issues) == 0

        return ValidationResult(
            is_valid=is_valid,
            quality_score=max(0.0, min(1.0, quality_score)),
            issues=issues,
            warnings=warnings,
        )

    def validate_series(
        self,
        series: pd.Series,
        expected_frequency: str | None = None,
        expected_range: tuple[float, float] | None = None,
    ) -> ValidationResult:
        """Validate a time series.

        Args:
            series: Time series to validate
            expected_frequency: Expected data frequency ('D', 'W', 'M')
            expected_range: Expected value range

        Returns:
            Validation result
        """
        issues: list[str] = []
        warnings: list[str] = []
        quality_score = 1.0

        if series.empty:
            issues.append("Empty series")
            return ValidationResult(
                is_valid=False,
                quality_score=0.0,
                issues=issues,
                warnings=warnings,
            )

        missing_pct = series.isna().sum() / len(series)
        if missing_pct > 0.5:
            issues.append(f"More than 50% missing values ({missing_pct:.0%})")
            quality_score *= 0.5
        elif missing_pct > 0.1:
            warnings.append(f"Missing values: {missing_pct:.1%}")
            quality_score *= 0.9

        if expected_range:
            min_val, max_val = expected_range
            out_of_range = ((series < min_val) | (series > max_val)).sum()
            if out_of_range > 0:
                warnings.append(f"{out_of_range} values outside expected range")
                quality_score *= 0.95

        if isinstance(series.index, pd.DatetimeIndex):
            duplicates = series.index.duplicated().sum()
            if duplicates > 0:
                warnings.append(f"{duplicates} duplicate timestamps")
                quality_score *= 0.95

        q1 = series.quantile(0.25)
        q3 = series.quantile(0.75)
        iqr = q3 - q1
        outliers = ((series < (q1 - 3 * iqr)) | (series > (q3 + 3 * iqr))).sum()
        if outliers > 0:
            warnings.append(f"{outliers} potential outliers detected")

        if expected_frequency and isinstance(series.index, pd.DatetimeIndex):
            inferred_freq = pd.infer_freq(series.index)
            if inferred_freq != expected_frequency:
                warnings.append(f"Expected frequency {expected_frequency}, got {inferred_freq}")

        is_valid = len(issues) == 0

        return ValidationResult(
            is_valid=is_valid,
            quality_score=max(0.0, min(1.0, quality_score)),
            issues=issues,
            warnings=warnings,
        )

    def calculate_completeness(
        self,
        series: pd.Series,
        start_date: datetime,
        end_date: datetime,
        expected_frequency: str = "D",
    ) -> float:
        """Calculate data completeness for a date range.

        Args:
            series: Time series data
            start_date: Expected start date
            end_date: Expected end date
            expected_frequency: Expected data frequency

        Returns:
            Completeness score (0-1)
        """
        if series.empty:
            return 0.0

        expected_dates = pd.date_range(start=start_date, end=end_date, freq=expected_frequency)
        expected_count = len(expected_dates)

        if expected_count == 0:
            return 1.0

        actual_count = len(series.dropna())

        return min(1.0, actual_count / expected_count)

    def calculate_freshness(
        self,
        last_timestamp: datetime,
        expected_frequency: str = "D",
    ) -> float:
        """Calculate data freshness score.

        Args:
            last_timestamp: Most recent data timestamp
            expected_frequency: Expected update frequency

        Returns:
            Freshness score (0-1)
        """
        now = datetime.now(UTC)
        age = now - last_timestamp

        # Expected update intervals
        frequency_intervals = {
            "H": timedelta(hours=2),
            "D": timedelta(days=2),
            "W": timedelta(weeks=2),
            "M": timedelta(days=45),
            "Q": timedelta(days=120),
        }

        expected_interval = frequency_intervals.get(expected_frequency, timedelta(days=2))

        if age <= expected_interval:
            return 1.0
        elif age <= expected_interval * 2:
            return 0.8
        elif age <= expected_interval * 4:
            return 0.6
        else:
            return 0.4
