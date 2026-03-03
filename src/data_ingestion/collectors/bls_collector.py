"""BLS (Bureau of Labor Statistics) collector."""

from datetime import datetime
from typing import Any

import httpx
import structlog

from src.config.settings import get_settings
from src.data_ingestion.base_collector import (
    BaseCollector,
    DataFrequency,
    DataPoint,
)
from src.data_ingestion.rate_limiter import get_rate_limiter

logger = structlog.get_logger()

BLS_DEFAULT_SERIES = [
    "CES0000000001",  # Total Nonfarm Payrolls
    "CES0500000001",  # Total Private
    "CES3000000001",  # Manufacturing Employment
    "CES4000000001",  # Trade, Transportation, Utilities
    "CES4200000001",  # Retail Trade
    "CES6000000001",  # Professional Services
    "CES6500000001",  # Education and Health
    "LNS14000000",  # Unemployment Rate
    "LNS11300000",  # Labor Force Participation Rate
    "LNS12300000",  # Employment-Population Ratio
    # Consumer Price Index
    "CUUR0000SA0",  # CPI-U All Items
    "CUUR0000SA0L1E",  # CPI-U Less Food and Energy
    "CUUR0000SAF1",  # CPI-U Food
    "CUUR0000SETA01",  # CPI-U New Vehicles
    "CUUR0000SETA02",  # CPI-U Used Cars
    "CUUR0000SETB01",  # CPI-U Gasoline
    "CUUR0000SAH1",  # CPI-U Shelter
    "WPUFD4",  # PPI Final Demand
    "WPUFD41",  # PPI Final Demand Goods
    "WPUFD42",  # PPI Final Demand Services
    "WPU0911",  # PPI Gasoline
    # JOLTS (Job Openings and Labor Turnover)
    "JTS000000000000000JOL",  # Total Job Openings
    "JTS000000000000000HIR",  # Total Hires
    "JTS000000000000000TSL",  # Total Separations
    "JTS000000000000000QUR",  # Quits Rate
    "CES0500000003",  # Average Hourly Earnings, Private
    "CES3000000003",  # Average Hourly Earnings, Manufacturing
    # Average Weekly Hours
    "CES0500000002",  # Average Weekly Hours, Private
    "CES3000000002",  # Average Weekly Hours, Manufacturing
    "EIUIR",  # Import Price Index
    "EIUIQ",  # Export Price Index
]


class BLSCollector(BaseCollector):
    """Collector for Bureau of Labor Statistics data."""

    BASE_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

    def __init__(self):
        """Initialize BLS collector."""
        super().__init__(name="BLS", source_id="bls")
        settings = get_settings()
        self.api_key = settings.bls_api_key
        self.rate_limiter = get_rate_limiter("bls")

    @property
    def frequency(self) -> DataFrequency:
        """BLS releases data monthly."""
        return DataFrequency.MONTHLY

    def get_schedule(self) -> str:
        """Run on the 8th of each month at 8:30 AM ET (13:30 UTC)."""
        return "30 13 8 * *"

    def get_default_series(self) -> list[str]:
        """Get default BLS series to collect."""
        return BLS_DEFAULT_SERIES

    async def validate_api_key(self) -> bool:
        """Validate the BLS API key."""
        if not self.api_key:
            self.logger.warning("BLS API key not configured - will use v1 (limited)")
            return True  # Can still work without key, just limited

        # Test with a simple request
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    self.BASE_URL,
                    json={
                        "seriesid": ["CES0000000001"],
                        "registrationkey": self.api_key.get_secret_value(),
                        "latest": True,
                    },
                )
                return response.status_code == 200
        except Exception as e:
            self.logger.error("BLS API validation failed", error=str(e))
            return False

    async def collect(
        self,
        series_ids: list[str] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> list[DataPoint]:
        """Collect data from BLS API.

        Args:
            series_ids: List of BLS series IDs to collect
            start_date: Start date for observations
            end_date: End date for observations

        Returns:
            List of collected data points
        """
        series_to_collect = series_ids or self.get_default_series()
        all_data_points: list[DataPoint] = []

        # BLS API allows up to 50 series per request with registration
        batch_size = 50 if self.api_key else 25

        async with httpx.AsyncClient(timeout=60.0) as client:
            for i in range(0, len(series_to_collect), batch_size):
                batch = series_to_collect[i : i + batch_size]
                try:
                    data_points = await self._collect_batch(
                        client, batch, start_date, end_date
                    )
                    all_data_points.extend(data_points)
                except Exception as e:
                    self.logger.error(
                        "Failed to collect batch",
                        batch_start=i,
                        error=str(e),
                    )

        return all_data_points

    async def _collect_batch(
        self,
        client: httpx.AsyncClient,
        series_ids: list[str],
        start_date: datetime | None,
        end_date: datetime | None,
    ) -> list[DataPoint]:
        """Collect observations for a batch of series.

        Args:
            client: HTTP client
            series_ids: List of BLS series IDs
            start_date: Start date
            end_date: End date

        Returns:
            List of data points for the series
        """
        async with self.rate_limiter:
            payload: dict[str, Any] = {
                "seriesid": series_ids,
            }

            if self.api_key:
                payload["registrationkey"] = self.api_key.get_secret_value()

            # BLS uses years, not dates
            current_year = datetime.now().year
            if start_date:
                payload["startyear"] = str(start_date.year)
            else:
                payload["startyear"] = str(current_year - 2)

            if end_date:
                payload["endyear"] = str(end_date.year)
            else:
                payload["endyear"] = str(current_year)

            response = await client.post(
                self.BASE_URL,
                json=payload,
            )
            response.raise_for_status()
            data = response.json()

        if data.get("status") != "REQUEST_SUCCEEDED":
            self.logger.error(
                "BLS request failed",
                status=data.get("status"),
                message=data.get("message"),
            )
            return []

        results = data.get("Results", {}).get("series", [])
        data_points: list[DataPoint] = []

        for series in results:
            series_id = series.get("seriesID", "")

            for obs in series.get("data", []):
                try:
                    value = float(obs["value"])
                except (ValueError, KeyError):
                    continue

                year = int(obs["year"])
                period = obs.get("period", "M01")

                if period.startswith("M"):
                    month = int(period[1:])
                    timestamp = datetime(year, month, 1)
                elif period.startswith("Q"):
                    quarter = int(period[1:])
                    month = (quarter - 1) * 3 + 1
                    timestamp = datetime(year, month, 1)
                elif period == "A01":
                    timestamp = datetime(year, 1, 1)
                else:
                    continue

                data_points.append(
                    DataPoint(
                        source_id=self.source_id,
                        series_id=series_id,
                        timestamp=timestamp,
                        value=value,
                        is_preliminary=obs.get("preliminary", "N") == "Y",
                        metadata={
                            "period": period,
                            "period_name": obs.get("periodName"),
                            "footnotes": obs.get("footnotes", []),
                        },
                    )
                )

        self.logger.debug(
            "Collected BLS batch",
            series_count=len(series_ids),
            observations=len(data_points),
        )

        return data_points
