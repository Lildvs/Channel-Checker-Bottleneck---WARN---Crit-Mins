"""FRED (Federal Reserve Economic Data) collector."""

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

# Key FRED series for bottleneck detection across all sectors
FRED_DEFAULT_SERIES = [
    # Economic Indicators
    "GDP",  # Gross Domestic Product
    "GDPC1",  # Real GDP
    "INDPRO",  # Industrial Production Index
    "UNRATE",  # Unemployment Rate
    "PAYEMS",  # Total Nonfarm Payrolls
    # Inflation
    "CPIAUCSL",  # Consumer Price Index
    "PPIACO",  # Producer Price Index
    "PCE",  # Personal Consumption Expenditures
    # Consumer
    "RSXFS",  # Retail Sales
    "UMCSENT",  # Consumer Sentiment
    "CSCICP03USM665S",  # Consumer Confidence
    # Housing
    "HOUST",  # Housing Starts
    "PERMIT",  # Building Permits
    "HSN1F",  # New Home Sales
    "CSUSHPINSA",  # Case-Shiller Home Price Index
    "MORTGAGE30US",  # 30-Year Mortgage Rate
    # Manufacturing
    "DGORDER",  # Durable Goods Orders
    "NEWORDER",  # Manufacturers New Orders
    "BUSINV",  # Business Inventories
    "AMTMNO",  # Manufacturers Total Inventories
    # Energy
    "DCOILWTICO",  # WTI Crude Oil
    "DHHNGSP",  # Henry Hub Natural Gas
    "GASREGW",  # Regular Gas Price
    # Transportation/Shipping
    "TSIFRGHT",  # Transportation Services Index - Freight
    "RAILFRTCARLOADSD11",  # Rail Freight Carloads
    # Interest Rates
    "DFF",  # Federal Funds Rate
    "DGS10",  # 10-Year Treasury
    "DGS2",  # 2-Year Treasury
    "T10Y2Y",  # 10Y-2Y Spread
    # Labor Market -- structural
    "JTSJOL",  # Job Openings (thousands, SA)
    "UNEMPLOY",  # Unemployment Level (thousands, SA)
    # Labor Market -- flow
    "JTSHIR",  # JOLTS Hires Rate (%, SA)
    "JTSTSR",  # JOLTS Total Separations Rate (%, SA)
    "JTSQUR",  # JOLTS Quits Rate (%, SA)
    "AWHMAN",  # Average Weekly Hours Manufacturing
    # Labor Market -- composite / private sector
    "FRBKCLMCILA",  # KC Fed Labor Market Conditions Index - Level of Activity
    "FRBKCLMCIM",  # KC Fed Labor Market Conditions Index - Momentum
    "ADPWNUSNERSA",  # ADP Total Nonfarm Private Employment (persons, SA)
    # Credit
    "BAMLH0A0HYM2",  # High Yield Corporate Bond Spread
    "TOTALSL",  # Consumer Credit
    "BUSLOANS",  # Commercial and Industrial Loans
    # Inventories
    "RETAILIMSA",  # Retail Inventories
    # International
    "DEXUSEU",  # USD/EUR Exchange Rate
    "DTWEXBGS",  # Trade Weighted Dollar Index
    # Fiscal Dominance / Treasury
    "GFDEBTN",  # Total Public Debt Outstanding (quarterly, millions)
    "A091RC1Q027SBEA",  # Federal Government Interest Payments (quarterly, SAAR, billions)
    "W006RC1Q027SBEA",  # Federal Government Current Tax Receipts (quarterly, SAAR, billions)
    "WTREGEN",  # Treasury General Account balance at Fed (weekly, millions)
    # Fed Liquidity Plumbing
    "RRPONTSYD",  # Overnight Reverse Repo (daily, billions)
    "WALCL",  # Fed Total Assets / Balance Sheet (weekly, millions)
    "WRESBAL",  # Reserve Balances at Fed (weekly, millions)
    # Energy Crunch -- SPR/inventory/refinery
    "WCSSTUS1",  # Weekly Crude Oil Stocks, commercial excl SPR (thousands of barrels)
    "WGTSTUS1",  # Weekly Gasoline Stocks (thousands of barrels)
    "WPULEUS3",  # Weekly Refinery Utilization Rate (%)
    # Capacity Ceiling
    "MCUMFN",  # Manufacturing Capacity Utilization (%)
    # Inventory Squeeze -- inventory-to-sales ratios
    "ISRATIO",  # Total Business Inventory-to-Sales Ratio
    "RETAILIRSA",  # Retail Inventory-to-Sales Ratio
    "MNFCTRIRSA",  # Manufacturers Inventory-to-Sales Ratio
    # Sentiment Shift -- delinquencies, financial conditions, savings
    "NFCI",  # Chicago Fed National Financial Conditions Index (weekly)
    "DRCCLACBS",  # Delinquency Rate on Credit Card Loans (quarterly, %)
    "DRCLACBS",  # Delinquency Rate on Consumer Loans (quarterly, %)
    "DRSFRMACBS",  # Delinquency Rate on Student Loans (quarterly, %)
    "CCLACBW027SBOG",  # Credit Card Loans Outstanding (weekly)
    "SLOAS",  # Student Loans Outstanding (quarterly)
    "PSAVERT",  # Personal Savings Rate (monthly, %)
    # Supply Disruption -- delivery time proxy
    "DTCDFNA066MNFRBPHI",  # Philly Fed Delivery Time Diffusion Index (monthly)
]


class FREDCollector(BaseCollector):
    """Collector for FRED (Federal Reserve Economic Data)."""

    BASE_URL = "https://api.stlouisfed.org/fred"

    def __init__(self):
        """Initialize FRED collector."""
        super().__init__(name="FRED", source_id="fred")
        settings = get_settings()
        self.api_key = settings.fred_api_key
        self.rate_limiter = get_rate_limiter("fred")

    @property
    def frequency(self) -> DataFrequency:
        """FRED data updates vary by series but we check daily."""
        return DataFrequency.DAILY

    def get_schedule(self) -> str:
        """Run daily at 6 AM ET (11 AM UTC)."""
        return "0 11 * * *"

    def get_default_series(self) -> list[str]:
        """Get default FRED series to collect."""
        return FRED_DEFAULT_SERIES

    async def validate_api_key(self) -> bool:
        """Validate the FRED API key."""
        if not self.api_key:
            self.logger.warning("FRED API key not configured")
            return False

        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self.BASE_URL}/series",
                    params={
                        "series_id": "GDP",
                        "api_key": self.api_key.get_secret_value(),
                        "file_type": "json",
                    },
                )
                return response.status_code == 200
        except Exception as e:
            self.logger.error("FRED API key validation failed", error=str(e))
            return False

    async def collect(
        self,
        series_ids: list[str] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> list[DataPoint]:
        """Collect data from FRED API.

        Args:
            series_ids: List of FRED series IDs to collect
            start_date: Start date for observations
            end_date: End date for observations

        Returns:
            List of collected data points
        """
        import asyncio

        if not self.api_key:
            self.logger.error("FRED API key not configured")
            return []

        series_to_collect = series_ids or self.get_default_series()
        all_data_points: list[DataPoint] = []

        async with httpx.AsyncClient(timeout=30.0) as client:
            # Collect in parallel batches of 5 to respect rate limits
            batch_size = 5
            for i in range(0, len(series_to_collect), batch_size):
                batch = series_to_collect[i : i + batch_size]

                async def collect_with_error_handling(sid: str) -> list[DataPoint]:
                    try:
                        return await self._collect_series(client, sid, start_date, end_date)
                    except Exception as e:
                        self.logger.error(
                            "Failed to collect series",
                            series_id=sid,
                            error=str(e),
                        )
                        return []

                results = await asyncio.gather(
                    *[collect_with_error_handling(sid) for sid in batch]
                )

                for result in results:
                    all_data_points.extend(result)

        return all_data_points

    async def _collect_series(
        self,
        client: httpx.AsyncClient,
        series_id: str,
        start_date: datetime | None,
        end_date: datetime | None,
    ) -> list[DataPoint]:
        """Collect observations for a single series.

        Args:
            client: HTTP client
            series_id: FRED series ID
            start_date: Start date
            end_date: End date

        Returns:
            List of data points for the series
        """
        async with self.rate_limiter:
            params: dict[str, Any] = {
                "series_id": series_id,
                "api_key": self.api_key.get_secret_value(),
                "file_type": "json",
            }

            if start_date:
                params["observation_start"] = start_date.strftime("%Y-%m-%d")
            if end_date:
                params["observation_end"] = end_date.strftime("%Y-%m-%d")

            response = await client.get(
                f"{self.BASE_URL}/series/observations",
                params=params,
            )
            response.raise_for_status()
            data = response.json()

        observations = data.get("observations", [])
        data_points: list[DataPoint] = []

        for obs in observations:
            value_str = obs.get("value", ".")
            if value_str == "." or value_str is None:
                continue

            try:
                value = float(value_str)
            except ValueError:
                continue

            timestamp = datetime.strptime(obs["date"], "%Y-%m-%d")

            data_points.append(
                DataPoint(
                    source_id=self.source_id,
                    series_id=series_id,
                    timestamp=timestamp,
                    value=value,
                    metadata={
                        "realtime_start": obs.get("realtime_start"),
                        "realtime_end": obs.get("realtime_end"),
                    },
                )
            )

        self.logger.debug(
            "Collected series",
            series_id=series_id,
            observations=len(data_points),
        )

        return data_points

    async def get_series_info(self, series_id: str) -> dict[str, Any] | None:
        """Get metadata about a FRED series.

        Args:
            series_id: FRED series ID

        Returns:
            Series metadata or None
        """
        if not self.api_key:
            return None

        async with httpx.AsyncClient() as client:
            async with self.rate_limiter:
                response = await client.get(
                    f"{self.BASE_URL}/series",
                    params={
                        "series_id": series_id,
                        "api_key": self.api_key.get_secret_value(),
                        "file_type": "json",
                    },
                )

                if response.status_code != 200:
                    return None

                data = response.json()
                series_list = data.get("seriess", [])

                if not series_list:
                    return None

                series = series_list[0]
                return {
                    "id": series.get("id"),
                    "title": series.get("title"),
                    "frequency": series.get("frequency"),
                    "units": series.get("units"),
                    "seasonal_adjustment": series.get("seasonal_adjustment"),
                    "last_updated": series.get("last_updated"),
                    "notes": series.get("notes"),
                }
