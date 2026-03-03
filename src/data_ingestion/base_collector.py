"""Base collector class for all data sources."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any
from uuid import UUID, uuid4

import structlog

logger = structlog.get_logger()


class DataFrequency(str, Enum):
    """Frequency of data updates."""

    REALTIME = "realtime"
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    ANNUAL = "annual"


class DataQuality(str, Enum):
    """Data quality classification."""

    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    PRELIMINARY = "preliminary"


@dataclass
class DataPoint:
    """Represents a single data observation."""

    source_id: str
    series_id: str
    timestamp: datetime
    value: float | None = None
    value_text: str | None = None
    unit: str | None = None
    quality_score: float = 1.0
    is_preliminary: bool = False
    revision_number: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)
    collected_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    id: UUID = field(default_factory=uuid4)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for database insertion."""
        return {
            "id": str(self.id),
            "source_id": self.source_id,
            "series_id": self.series_id,
            "timestamp": self.timestamp,
            "collected_at": self.collected_at,
            "value": self.value,
            "value_text": self.value_text,
            "unit": self.unit,
            "quality_score": self.quality_score,
            "is_preliminary": self.is_preliminary,
            "revision_number": self.revision_number,
            "extra_data": self.metadata,  # Dataclass field 'metadata' -> DB column 'extra_data'
        }


@dataclass
class SeriesMetadata:
    """Metadata about a data series."""

    series_id: str
    source_id: str
    name: str
    description: str | None = None
    unit: str | None = None
    frequency: DataFrequency = DataFrequency.DAILY
    seasonal_adjustment: str | None = None
    sector_codes: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class CollectionResult:
    """Result of a data collection operation."""

    collector_name: str
    started_at: datetime
    completed_at: datetime
    success: bool
    data_points: list[DataPoint] = field(default_factory=list)
    series_metadata: list[SeriesMetadata] = field(default_factory=list)
    error_message: str | None = None
    records_collected: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def duration_seconds(self) -> float:
        """Calculate collection duration in seconds."""
        return (self.completed_at - self.started_at).total_seconds()


class BaseCollector(ABC):
    """Abstract base class for data collectors."""

    def __init__(
        self,
        name: str,
        source_id: str,
    ):
        """Initialize the collector.

        Args:
            name: Human-readable name for the collector
            source_id: Unique identifier for the data source
        """
        self.name = name
        self.source_id = source_id
        self.logger = logger.bind(collector=name)

    @abstractmethod
    async def collect(
        self,
        series_ids: list[str] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> list[DataPoint]:
        """Collect data from the source.

        Args:
            series_ids: Optional list of specific series to collect
            start_date: Optional start date for data range
            end_date: Optional end date for data range

        Returns:
            List of collected data points
        """
        ...

    @abstractmethod
    def get_default_series(self) -> list[str]:
        """Get the default list of series to collect.

        Returns:
            List of series IDs to collect by default
        """
        ...

    @abstractmethod
    def get_schedule(self) -> str:
        """Get the cron schedule for this collector.

        Returns:
            Cron expression string (e.g., "0 6 * * *" for daily at 6 AM)
        """
        ...

    @property
    @abstractmethod
    def frequency(self) -> DataFrequency:
        """Get the typical update frequency for this source."""
        ...

    async def run_collection(
        self,
        series_ids: list[str] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> CollectionResult:
        """Run a full collection cycle with logging and error handling.

        Args:
            series_ids: Optional list of specific series to collect
            start_date: Optional start date for data range
            end_date: Optional end date for data range

        Returns:
            CollectionResult with status and collected data
        """
        started_at = datetime.now(UTC)
        self.logger.info(
            "Starting collection",
            series_count=len(series_ids) if series_ids else "all",
            start_date=start_date,
            end_date=end_date,
        )

        try:
            data_points = await self.collect(
                series_ids=series_ids or self.get_default_series(),
                start_date=start_date,
                end_date=end_date,
            )

            completed_at = datetime.now(UTC)
            result = CollectionResult(
                collector_name=self.name,
                started_at=started_at,
                completed_at=completed_at,
                success=True,
                data_points=data_points,
                records_collected=len(data_points),
            )

            self.logger.info(
                "Collection completed successfully",
                records_collected=len(data_points),
                duration_seconds=result.duration_seconds,
            )

            return result

        except Exception as e:
            completed_at = datetime.now(UTC)
            self.logger.error(
                "Collection failed",
                error=str(e),
                duration_seconds=(completed_at - started_at).total_seconds(),
            )

            return CollectionResult(
                collector_name=self.name,
                started_at=started_at,
                completed_at=completed_at,
                success=False,
                error_message=str(e),
            )

    async def validate_api_key(self) -> bool:
        """Validate that the API key is configured and working.

        Returns:
            True if API key is valid, False otherwise
        """
        # Default implementation - subclasses should override
        return True

    def calculate_quality_score(
        self,
        is_preliminary: bool = False,
        revision_number: int = 0,
        data_age_days: int = 0,
    ) -> float:
        """Calculate a quality score for a data point.

        Args:
            is_preliminary: Whether the data is preliminary
            revision_number: How many times the data has been revised
            data_age_days: Age of the data in days

        Returns:
            Quality score between 0 and 1
        """
        score = 1.0

        # Preliminary data is less reliable
        if is_preliminary:
            score *= 0.8

        # More revisions generally mean more accurate
        if revision_number > 0:
            score *= min(1.0, 0.9 + (revision_number * 0.05))

        # Very fresh data might not be final
        if data_age_days < 7:
            score *= 0.95

        return round(score, 3)
