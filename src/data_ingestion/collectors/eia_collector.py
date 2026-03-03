"""EIA (Energy Information Administration) collector."""

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

# Key EIA series for energy bottleneck detection
EIA_DEFAULT_SERIES = [
    # Petroleum - Weekly
    "PET.WCRSTUS1.W",  # Crude Oil Stocks (Weekly)
    "PET.WCRFPUS2.W",  # Refinery Crude Input
    "PET.WPULEUS3.W",  # Refinery Utilization
    "PET.WGTSTUS1.W",  # Motor Gasoline Stocks
    "PET.WDISTUS1.W",  # Distillate Stocks
    "PET.WRPUPUS2.W",  # Refinery Production
    # Petroleum - Prices
    "PET.RWTC.D",  # WTI Crude Spot Price (Daily)
    "PET.RBRTE.D",  # Brent Crude Spot Price (Daily)
    "PET.EMM_EPMR_PTE_NUS_DPG.W",  # Regular Gas Retail Price
    # Natural Gas - Weekly
    "NG.NW2_EPG0_SWO_R48_BCF.W",  # Natural Gas Storage
    "NG.RNGWHHD.D",  # Henry Hub Spot Price (Daily)
    "NG.N9050US2.M",  # Dry Gas Production (Monthly)
    # Electricity - Monthly
    "ELEC.GEN.ALL-US-99.M",  # Total Net Generation
    "ELEC.GEN.COL-US-99.M",  # Coal Generation
    "ELEC.GEN.NG-US-99.M",  # Natural Gas Generation
    "ELEC.GEN.NUC-US-99.M",  # Nuclear Generation
    "ELEC.GEN.SUN-US-99.M",  # Solar Generation
    "ELEC.GEN.WND-US-99.M",  # Wind Generation
    # Coal
    "COAL.PRODUCTION.TOT-US-TOT.W",  # Coal Production Weekly
    # Renewables
    "STEO.SOLEGPUS.A",  # Solar Electricity Generation
    "STEO.WNELEPUS.A",  # Wind Electricity Generation
]


class EIACollector(BaseCollector):
    """Collector for Energy Information Administration data."""

    BASE_URL = "https://api.eia.gov/v2"

    def __init__(self):
        """Initialize EIA collector."""
        super().__init__(name="EIA", source_id="eia")
        settings = get_settings()
        self.api_key = settings.eia_api_key
        self.rate_limiter = get_rate_limiter("eia")

    @property
    def frequency(self) -> DataFrequency:
        """EIA releases data weekly for key series."""
        return DataFrequency.WEEKLY

    def get_schedule(self) -> str:
        """Run on Wednesdays at 10:30 AM ET (15:30 UTC) after petroleum report."""
        return "30 15 * * 3"

    def get_default_series(self) -> list[str]:
        """Get default EIA series to collect."""
        return EIA_DEFAULT_SERIES

    async def validate_api_key(self) -> bool:
        """Validate the EIA API key."""
        if not self.api_key:
            self.logger.warning("EIA API key not configured")
            return False

        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self.BASE_URL}/petroleum/sum/sndw/data/",
                    params={
                        "api_key": self.api_key.get_secret_value(),
                        "length": 1,
                    },
                )
                return response.status_code == 200
        except Exception as e:
            self.logger.error("EIA API validation failed", error=str(e))
            return False

    async def collect(
        self,
        series_ids: list[str] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> list[DataPoint]:
        """Collect data from EIA API.

        Args:
            series_ids: List of EIA series IDs to collect
            start_date: Start date for observations
            end_date: End date for observations

        Returns:
            List of collected data points
        """
        if not self.api_key:
            self.logger.error("EIA API key not configured")
            return []

        series_to_collect = series_ids or self.get_default_series()
        all_data_points: list[DataPoint] = []

        async with httpx.AsyncClient(timeout=30.0) as client:
            for series_id in series_to_collect:
                try:
                    data_points = await self._collect_series(
                        client, series_id, start_date, end_date
                    )
                    all_data_points.extend(data_points)
                except Exception as e:
                    self.logger.error(
                        "Failed to collect series",
                        series_id=series_id,
                        error=str(e),
                    )

        return all_data_points

    async def _collect_series(
        self,
        client: httpx.AsyncClient,
        series_id: str,
        start_date: datetime | None,
        end_date: datetime | None,
    ) -> list[DataPoint]:
        """Collect observations for a single series using v2 API.

        Args:
            client: HTTP client
            series_id: EIA series ID (format: CATEGORY.SERIES_ID.FREQUENCY)
            start_date: Start date
            end_date: End date

        Returns:
            List of data points for the series
        """
        parts = series_id.split(".")
        if len(parts) < 3:
            self.logger.warning("Invalid EIA series ID format", series_id=series_id)
            return []

        async with self.rate_limiter:
            # For v2 API, we need to construct the proper route
            # This is a simplified approach - real implementation would
            # need to map series IDs to proper v2 routes
            params: dict[str, Any] = {
                "api_key": self.api_key.get_secret_value(),
                "data[]": "value",
                "sort[0][column]": "period",
                "sort[0][direction]": "desc",
                "length": 500,
            }

            if start_date:
                params["start"] = start_date.strftime("%Y-%m-%d")
            if end_date:
                params["end"] = end_date.strftime("%Y-%m-%d")

            category = parts[0].lower()
            if category == "pet":
                route = "petroleum/sum/sndw/data/"
            elif category == "ng":
                route = "natural-gas/sum/sndw/data/"
            elif category == "elec":
                route = "electricity/electric-power-operational-data/data/"
            elif category == "coal":
                route = "coal/production/production-by-rank/data/"
            else:
                route = "steo/data/"

            response = await client.get(
                f"{self.BASE_URL}/{route}",
                params=params,
            )

        if response.status_code != 200:
            self.logger.warning(
                "EIA request failed",
                series_id=series_id,
                status=response.status_code,
            )
            return []

        data = response.json()
        observations = data.get("response", {}).get("data", [])
        data_points: list[DataPoint] = []

        for obs in observations:
            value = obs.get("value")
            if value is None:
                continue

            try:
                value = float(value)
            except (ValueError, TypeError):
                continue

            period = obs.get("period", "")
            try:
                if len(period) == 10:  # YYYY-MM-DD
                    timestamp = datetime.strptime(period, "%Y-%m-%d")
                elif len(period) == 7:  # YYYY-MM
                    timestamp = datetime.strptime(period, "%Y-%m")
                elif len(period) == 4:  # YYYY
                    timestamp = datetime.strptime(period, "%Y")
                else:
                    continue
            except ValueError:
                continue

            data_points.append(
                DataPoint(
                    source_id=self.source_id,
                    series_id=series_id,
                    timestamp=timestamp,
                    value=value,
                    unit=obs.get("unit"),
                    metadata={
                        "product": obs.get("product"),
                        "process": obs.get("process"),
                        "area": obs.get("area"),
                    },
                )
            )

        self.logger.debug(
            "Collected EIA series",
            series_id=series_id,
            observations=len(data_points),
        )

        return data_points
