"""Data normalization for consistent processing."""

from datetime import datetime, timezone
from typing import Any

import pandas as pd
import structlog

from src.data_ingestion.base_collector import DataPoint

logger = structlog.get_logger()


class DataNormalizer:
    """Normalizes data from various sources to a consistent format."""

    def __init__(self):
        """Initialize the normalizer."""
        self.logger = logger.bind(component="normalizer")

    def normalize_timestamp(self, timestamp: Any) -> datetime:
        """Normalize a timestamp to UTC datetime.

        Args:
            timestamp: Input timestamp in various formats

        Returns:
            UTC datetime
        """
        if isinstance(timestamp, datetime):
            if timestamp.tzinfo is None:
                return timestamp.replace(tzinfo=timezone.utc)
            return timestamp.astimezone(timezone.utc)

        if isinstance(timestamp, str):
            # Try various formats
            formats = [
                "%Y-%m-%d",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%dT%H:%M:%SZ",
                "%Y-%m-%dT%H:%M:%S.%f",
                "%Y-%m-%dT%H:%M:%S.%fZ",
                "%Y/%m/%d",
                "%m/%d/%Y",
            ]

            for fmt in formats:
                try:
                    dt = datetime.strptime(timestamp, fmt)
                    return dt.replace(tzinfo=timezone.utc)
                except ValueError:
                    continue

            # Try pandas
            try:
                dt = pd.to_datetime(timestamp)
                if dt.tzinfo is None:
                    return dt.replace(tzinfo=timezone.utc)
                return dt.tz_convert(timezone.utc).to_pydatetime()
            except Exception:
                pass

        raise ValueError(f"Cannot parse timestamp: {timestamp}")

    def normalize_value(
        self,
        value: Any,
        source_unit: str | None = None,
        target_unit: str | None = None,
    ) -> float | None:
        """Normalize a numeric value.

        Args:
            value: Input value
            source_unit: Original unit
            target_unit: Target unit (if conversion needed)

        Returns:
            Normalized float value or None
        """
        if value is None:
            return None

        if isinstance(value, str):
            value = value.strip()
            if value in [".", "N/A", "NA", "", "-", "--"]:
                return None

            value = value.replace("$", "").replace(",", "").replace("%", "")

            try:
                return float(value)
            except ValueError:
                return None

        if isinstance(value, (int, float)):
            return float(value)

        return None

    def normalize_data_point(self, data_point: DataPoint) -> DataPoint:
        """Normalize a data point.

        Args:
            data_point: Raw data point

        Returns:
            Normalized data point
        """
        normalized = DataPoint(
            source_id=data_point.source_id,
            series_id=self.normalize_series_id(data_point.series_id),
            timestamp=self.normalize_timestamp(data_point.timestamp),
            value=self.normalize_value(data_point.value),
            value_text=data_point.value_text,
            unit=data_point.unit,
            quality_score=data_point.quality_score,
            is_preliminary=data_point.is_preliminary,
            revision_number=data_point.revision_number,
            metadata=data_point.metadata,
            collected_at=self.normalize_timestamp(data_point.collected_at),
            id=data_point.id,
        )

        return normalized

    def normalize_series_id(self, series_id: str) -> str:
        """Normalize a series ID.

        Args:
            series_id: Raw series ID

        Returns:
            Normalized series ID
        """
        # Uppercase, remove special characters except underscore and dash
        normalized = series_id.upper().strip()
        return normalized

    def normalize_dataframe(
        self,
        df: pd.DataFrame,
        timestamp_col: str = "timestamp",
        value_col: str = "value",
    ) -> pd.DataFrame:
        """Normalize a pandas DataFrame.

        Args:
            df: Input DataFrame
            timestamp_col: Name of timestamp column
            value_col: Name of value column

        Returns:
            Normalized DataFrame
        """
        result = df.copy()

        if timestamp_col in result.columns:
            result[timestamp_col] = pd.to_datetime(result[timestamp_col], utc=True)

        if value_col in result.columns:
            result[value_col] = pd.to_numeric(result[value_col], errors="coerce")

        if timestamp_col in result.columns:
            result = result.sort_values(timestamp_col)

        return result

    def resample_to_frequency(
        self,
        series: pd.Series,
        frequency: str = "D",
        method: str = "mean",
    ) -> pd.Series:
        """Resample a time series to a specific frequency.

        Args:
            series: Time series with datetime index
            frequency: Target frequency ('D' for daily, 'W' for weekly, 'M' for monthly)
            method: Aggregation method ('mean', 'sum', 'last', 'first')

        Returns:
            Resampled series
        """
        if not isinstance(series.index, pd.DatetimeIndex):
            raise ValueError("Series must have a DatetimeIndex")

        resampler = series.resample(frequency)

        if method == "mean":
            return resampler.mean()
        elif method == "sum":
            return resampler.sum()
        elif method == "last":
            return resampler.last()
        elif method == "first":
            return resampler.first()
        else:
            raise ValueError(f"Unknown method: {method}")
