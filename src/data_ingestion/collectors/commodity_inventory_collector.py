"""Commodity Inventory collector for tracking inventory levels.

Collects inventory data from multiple sources:
- EIA: Strategic Petroleum Reserve, crude oil stocks
- USDA NASS: Grain stocks (quarterly)
- LME: Metal warehouse stocks (delayed Excel downloads)
- COMEX: Precious metals registered stocks

API Documentation:
- EIA: https://www.eia.gov/opendata/documentation.php
- NASS: https://quickstats.nass.usda.gov/api
- LME: https://www.lme.com/market-data/reports-and-data/warehouse-and-stocks-reports
"""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import httpx
import structlog

from src.config.settings import get_settings
from src.data_ingestion.base_collector import (
    BaseCollector,
    DataFrequency,
    DataPoint,
)
from src.data_ingestion.rate_limiter import get_rate_limiter, RateLimitConfig, RateLimiterRegistry

logger = structlog.get_logger()


@dataclass
class InventoryRecord:
    """Represents an inventory data point."""

    commodity: str
    commodity_type: str  # metal, petroleum, grain
    source: str  # EIA, NASS, LME, COMEX
    location: str | None
    quantity: Decimal
    unit: str
    stock_type: str | None  # registered, eligible, total
    timestamp: datetime
    data_delay_days: int = 0
    metadata: dict[str, Any] | None = None


class CommodityInventoryCollector(BaseCollector):
    """Collector for commodity inventory data from multiple sources.

    Supports:
    - EIA petroleum stocks (weekly)
    - USDA NASS grain stocks (quarterly)
    - LME warehouse stocks (daily, delayed)
    - COMEX registered stocks (daily)
    """

    EIA_BASE_URL = "https://api.eia.gov/v2"

    EIA_SERIES = {
        "STEO.PASC_YCUUS.M": {
            "name": "U.S. Crude Oil Stocks",
            "commodity": "crude_oil",
            "commodity_type": "petroleum",
            "unit": "million_barrels",
        },
        "PET.WCSSTUS1.W": {
            "name": "U.S. Ending Stocks of Crude Oil (Excluding SPR)",
            "commodity": "crude_oil_excl_spr",
            "commodity_type": "petroleum",
            "unit": "thousand_barrels",
        },
        "PET.WCESTUS1.W": {
            "name": "U.S. Ending Stocks of Crude Oil",
            "commodity": "crude_oil_total",
            "commodity_type": "petroleum",
            "unit": "thousand_barrels",
        },
        "PET.WTTSTUS1.W": {
            "name": "U.S. Ending Stocks of Total Petroleum Products",
            "commodity": "total_petroleum",
            "commodity_type": "petroleum",
            "unit": "thousand_barrels",
        },
        "PET.WGTSTUS1.W": {
            "name": "U.S. Ending Stocks of Motor Gasoline",
            "commodity": "gasoline",
            "commodity_type": "petroleum",
            "unit": "thousand_barrels",
        },
        "PET.WDISTUS1.W": {
            "name": "U.S. Ending Stocks of Distillate Fuel Oil",
            "commodity": "distillate",
            "commodity_type": "petroleum",
            "unit": "thousand_barrels",
        },
    }

    NASS_BASE_URL = "https://quickstats.nass.usda.gov/api/api_GET"

    NASS_COMMODITIES = {
        "CORN": {
            "name": "Corn Stocks",
            "commodity": "corn",
            "commodity_type": "grain",
            "unit": "bushels",
        },
        "SOYBEANS": {
            "name": "Soybean Stocks",
            "commodity": "soybeans",
            "commodity_type": "grain",
            "unit": "bushels",
        },
        "WHEAT": {
            "name": "Wheat Stocks",
            "commodity": "wheat",
            "commodity_type": "grain",
            "unit": "bushels",
        },
    }

    LME_METALS = {
        "copper": {"symbol": "CA", "unit": "metric_tons"},
        "aluminum": {"symbol": "AH", "unit": "metric_tons"},
        "zinc": {"symbol": "ZS", "unit": "metric_tons"},
        "nickel": {"symbol": "NI", "unit": "metric_tons"},
        "lead": {"symbol": "PB", "unit": "metric_tons"},
        "tin": {"symbol": "SN", "unit": "metric_tons"},
    }

    def __init__(self):
        """Initialize the Commodity Inventory collector."""
        super().__init__(name="Commodity Inventory", source_id="commodity_inventory")
        self.settings = get_settings()

        self.eia_limiter = get_rate_limiter("eia")

        registry = RateLimiterRegistry()
        nass_config = RateLimitConfig(requests_per_minute=60, burst_size=10)
        self.nass_limiter = registry.get_or_create("nass", nass_config)

    @property
    def frequency(self) -> DataFrequency:
        """Primary frequency is weekly (EIA petroleum)."""
        return DataFrequency.WEEKLY

    def get_schedule(self) -> str:
        """Return cron schedule - Wednesday at 3:30 PM UTC (10:30 AM ET)."""
        return "30 15 * * 3"

    def get_default_series(self) -> list[str]:
        """Return default series to collect."""
        return list(self.EIA_SERIES.keys()) + list(self.NASS_COMMODITIES.keys())

    async def collect(
        self,
        series_ids: list[str] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> list[DataPoint]:
        """Collect commodity inventory data from all sources.

        Args:
            series_ids: Optional list of specific series to collect
            start_date: Start date for data
            end_date: End date for data

        Returns:
            List of DataPoint objects
        """
        if end_date is None:
            end_date = datetime.now(UTC)
        if start_date is None:
            start_date = end_date - timedelta(days=30)

        data_points: list[DataPoint] = []

        async with httpx.AsyncClient(timeout=60.0) as client:
            self.logger.info("Collecting EIA petroleum stocks")
            try:
                eia_points = await self._collect_eia_petroleum(
                    client, start_date, end_date
                )
                data_points.extend(eia_points)
            except Exception as e:
                self.logger.error("Failed to collect EIA data", error=str(e))

            self.logger.info("Collecting NASS grain stocks")
            try:
                nass_points = await self._collect_nass_grain_stocks(
                    client, start_date, end_date
                )
                data_points.extend(nass_points)
            except Exception as e:
                self.logger.error("Failed to collect NASS data", error=str(e))

            self.logger.info("Collecting LME/COMEX metal stocks via file downloads")
            try:
                metal_points = await self._collect_metal_stocks()
                data_points.extend(metal_points)
            except Exception as e:
                self.logger.error("Failed to collect LME/COMEX data", error=str(e))

        self.logger.info(
            "Collection complete",
            total_records=len(data_points),
        )

        return data_points

    async def _collect_eia_petroleum(
        self,
        client: httpx.AsyncClient,
        start_date: datetime,
        end_date: datetime,
    ) -> list[DataPoint]:
        """Collect EIA petroleum inventory data.

        Args:
            client: HTTP client
            start_date: Start date
            end_date: End date

        Returns:
            List of DataPoint objects
        """
        data_points: list[DataPoint] = []
        api_key = self.settings.eia_api_key

        if not api_key:
            self.logger.warning("EIA API key not configured")
            return data_points

        key_value = api_key.get_secret_value()

        for series_id, config in self.EIA_SERIES.items():
            try:
                # Format: CATEGORY.SERIES.FREQUENCY
                parts = series_id.split(".")

                if parts[0] == "PET":
                    url = f"{self.EIA_BASE_URL}/petroleum/sum/sndw/data/"
                    params = {
                        "api_key": key_value,
                        "data[]": "value",
                        "frequency": "weekly",
                        "start": start_date.strftime("%Y-%m-%d"),
                        "end": end_date.strftime("%Y-%m-%d"),
                        "sort[0][column]": "period",
                        "sort[0][direction]": "desc",
                    }
                elif parts[0] == "STEO":
                    url = f"{self.EIA_BASE_URL}/steo/data/"
                    params = {
                        "api_key": key_value,
                        "data[]": "value",
                        "frequency": "monthly",
                        "start": start_date.strftime("%Y-%m"),
                        "end": end_date.strftime("%Y-%m"),
                    }
                else:
                    continue

                async with self.eia_limiter:
                    response = await client.get(url, params=params)

                if response.status_code != 200:
                    self.logger.warning(
                        "EIA API error",
                        series=series_id,
                        status=response.status_code,
                    )
                    continue

                data = response.json()
                records = data.get("response", {}).get("data", [])

                for record in records[:50]:  # Limit records per series
                    try:
                        period = record.get("period", "")
                        value = record.get("value")

                        if value is None:
                            continue

                        if "T" in period or len(period) == 10:
                            timestamp = datetime.strptime(period[:10], "%Y-%m-%d").replace(tzinfo=UTC)
                        elif len(period) == 7:
                            timestamp = datetime.strptime(period, "%Y-%m").replace(tzinfo=UTC)
                        else:
                            continue

                        dp = DataPoint(
                            source_id=self.source_id,
                            series_id=f"EIA_{config['commodity'].upper()}",
                            timestamp=timestamp,
                            value=float(value),
                            unit=config["unit"],
                            metadata={
                                "commodity": config["commodity"],
                                "commodity_type": config["commodity_type"],
                                "source": "EIA",
                                "original_series": series_id,
                                "name": config["name"],
                            },
                        )
                        data_points.append(dp)

                    except Exception as e:
                        self.logger.debug(
                            "Failed to parse EIA record",
                            series=series_id,
                            error=str(e),
                        )
                        continue

            except Exception as e:
                self.logger.error(
                    "Failed to fetch EIA series",
                    series=series_id,
                    error=str(e),
                )
                continue

        return data_points

    async def _collect_nass_grain_stocks(
        self,
        client: httpx.AsyncClient,
        start_date: datetime,
        end_date: datetime,
    ) -> list[DataPoint]:
        """Collect USDA NASS grain stocks data.

        Args:
            client: HTTP client
            start_date: Start date
            end_date: End date

        Returns:
            List of DataPoint objects
        """
        data_points: list[DataPoint] = []
        api_key = self.settings.usda_nass_api_key

        if not api_key:
            self.logger.warning("NASS API key not configured")
            return data_points

        key_value = api_key.get_secret_value()

        for commodity_name, config in self.NASS_COMMODITIES.items():
            try:
                params = {
                    "key": key_value,
                    "commodity_desc": commodity_name,
                    "statisticcat_desc": "STOCKS",
                    "domain_desc": "TOTAL",
                    "agg_level_desc": "NATIONAL",
                    "freq_desc": "QUARTERLY",
                    "year__GE": str(start_date.year),
                    "format": "JSON",
                }

                async with self.nass_limiter:
                    response = await client.get(self.NASS_BASE_URL, params=params)

                if response.status_code != 200:
                    self.logger.warning(
                        "NASS API error",
                        commodity=commodity_name,
                        status=response.status_code,
                    )
                    continue

                data = response.json()
                records = data.get("data", [])

                for record in records[:20]:  # Limit records
                    try:
                        year = int(record.get("year", 0))
                        # NASS uses reference_period_desc like "MAR", "JUN", etc.
                        period = record.get("reference_period_desc", "")
                        value_str = record.get("Value", "").replace(",", "")

                        if not value_str or value_str == "(D)":
                            continue

                        value = float(value_str)

                        period_months = {
                            "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4,
                            "MAY": 5, "JUN": 6, "JUL": 7, "AUG": 8,
                            "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
                        }
                        month = period_months.get(period[:3].upper(), 1)
                        timestamp = datetime(year, month, 1, tzinfo=UTC)

                        if not (start_date <= timestamp <= end_date):
                            continue

                        dp = DataPoint(
                            source_id=self.source_id,
                            series_id=f"NASS_{config['commodity'].upper()}_STOCKS",
                            timestamp=timestamp,
                            value=value,
                            unit=config["unit"],
                            metadata={
                                "commodity": config["commodity"],
                                "commodity_type": config["commodity_type"],
                                "source": "NASS",
                                "name": config["name"],
                                "reference_period": period,
                                "location": record.get("location_desc", "US"),
                            },
                        )
                        data_points.append(dp)

                    except Exception as e:
                        self.logger.debug(
                            "Failed to parse NASS record",
                            commodity=commodity_name,
                            error=str(e),
                        )
                        continue

            except Exception as e:
                self.logger.error(
                    "Failed to fetch NASS commodity",
                    commodity=commodity_name,
                    error=str(e),
                )
                continue

        return data_points

    async def _collect_metal_stocks(self) -> list[DataPoint]:
        """Collect LME and COMEX metal warehouse stocks.

        Uses the file-based collector for downloading Excel reports
        from LME and CME websites.

        Returns:
            List of DataPoint objects for metal stocks
        """
        from src.data_ingestion.collectors.commodity_inventory_file_collector import (
            get_commodity_inventory_file_collector,
        )

        try:
            file_collector = get_commodity_inventory_file_collector()
            metal_points = await file_collector.collect()

            self.logger.info(
                "Collected metal stocks",
                records=len(metal_points),
            )
            return metal_points

        except Exception as e:
            self.logger.error(
                "Failed to collect metal stocks from file downloads",
                error=str(e),
            )
            return []

    async def collect_with_fallback(
        self,
        series_ids: list[str] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> list[DataPoint]:
        """Collect commodity data with fallback for failed sources.

        Tries all sources and continues even if some fail.

        Args:
            series_ids: Optional list of specific series to collect
            start_date: Start date for data
            end_date: End date for data

        Returns:
            List of DataPoint objects from all successful sources
        """
        return await self.collect(series_ids, start_date, end_date)

    async def validate_api_key(self) -> bool:
        """Validate API keys for EIA and NASS.

        Returns:
            True if at least one API is accessible
        """
        eia_valid = False
        nass_valid = False

        if self.settings.eia_api_key:
            try:
                async with httpx.AsyncClient(timeout=30.0) as client:
                    params = {
                        "api_key": self.settings.eia_api_key.get_secret_value(),
                    }
                    response = await client.get(
                        f"{self.EIA_BASE_URL}/petroleum/", params=params
                    )
                    eia_valid = response.status_code in (200, 400)  # 400 = missing params but key valid
            except Exception as e:
                self.logger.warning("EIA validation failed", error=str(e))

        if self.settings.usda_nass_api_key:
            try:
                async with httpx.AsyncClient(timeout=30.0) as client:
                    params = {
                        "key": self.settings.usda_nass_api_key.get_secret_value(),
                        "commodity_desc": "CORN",
                        "year": "2024",
                        "format": "JSON",
                    }
                    response = await client.get(self.NASS_BASE_URL, params=params)
                    nass_valid = response.status_code == 200
            except Exception as e:
                self.logger.warning("NASS validation failed", error=str(e))

        return eia_valid or nass_valid


def get_commodity_inventory_collector() -> CommodityInventoryCollector:
    """Get a Commodity Inventory collector instance."""
    return CommodityInventoryCollector()
