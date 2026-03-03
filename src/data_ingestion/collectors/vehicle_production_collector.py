"""Vehicle Production collector for tracking vehicle manufacturing activity.

Collects data from multiple free/public sources as alternatives to WardsAuto:
- Census M3: Manufacturer-level shipments and inventory (monthly)
- FRED AISRSA: Auto Inventory/Sales Ratio (monthly)
- Cox Automotive: Used vehicle market data (monthly Excel downloads)
- BEA: Motor vehicle output (quarterly, bonus source)

API Documentation:
- Census M3: https://api.census.gov/data/timeseries/eits/m3.html
- FRED: https://fred.stlouisfed.org/docs/api/fred/
- BEA: https://apps.bea.gov/api/signup/index.cfm
- Cox Automotive: https://www.coxautoinc.com/market-insights/
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta, UTC
from decimal import Decimal
from typing import Any
import io

import httpx
import pandas as pd
import structlog

from src.config.settings import get_settings
from src.data_ingestion.base_collector import (
    BaseCollector,
    DataFrequency,
    DataPoint,
)
from src.data_ingestion.rate_limiter import (
    get_rate_limiter,
    RateLimitConfig,
    RateLimiterRegistry,
)

logger = structlog.get_logger()


@dataclass
class VehicleProductionRecord:
    """Represents a vehicle production/inventory data point."""

    metric_type: str  # shipments, inventory, output, inventory_ratio, days_supply
    vehicle_type: str  # light_vehicle, truck, auto, total, used
    value: Decimal
    unit: str
    timestamp: datetime
    source: str  # CENSUS_M3, FRED, COX, BEA
    seasonal_adjustment: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


CENSUS_M3_SERIES = {
    "ASMVPSA": {
        "name": "Motor Vehicles and Parts - Shipments (SA)",
        "metric_type": "shipments",
        "vehicle_type": "total",
        "unit": "million_dollars",
        "seasonal_adjustment": "SA",
    },
    "ASMVPNSA": {
        "name": "Motor Vehicles and Parts - Shipments (NSA)",
        "metric_type": "shipments",
        "vehicle_type": "total",
        "unit": "million_dollars",
        "seasonal_adjustment": "NSA",
    },
    "AIMVPSA": {
        "name": "Motor Vehicles and Parts - Inventories (SA)",
        "metric_type": "inventory",
        "vehicle_type": "total",
        "unit": "million_dollars",
        "seasonal_adjustment": "SA",
    },
    "AIMVPNSA": {
        "name": "Motor Vehicles and Parts - Inventories (NSA)",
        "metric_type": "inventory",
        "vehicle_type": "total",
        "unit": "million_dollars",
        "seasonal_adjustment": "NSA",
    },
    "ASALTSA": {
        "name": "Autos and Light Trucks - Shipments (SA)",
        "metric_type": "shipments",
        "vehicle_type": "light_vehicle",
        "unit": "million_dollars",
        "seasonal_adjustment": "SA",
    },
}

FRED_VEHICLE_SERIES = {
    "AISRSA": {
        "name": "Auto Inventory/Sales Ratio",
        "metric_type": "inventory_ratio",
        "vehicle_type": "total",
        "unit": "ratio",
        "description": "Ratio of inventories to sales for motor vehicle dealers",
    },
    "AUTONSA": {
        "name": "Motor Vehicle Retail Sales: Autos and Other Motor Vehicles",
        "metric_type": "sales",
        "vehicle_type": "auto",
        "unit": "million_dollars",
    },
    "LAUTONSA": {
        "name": "Light Weight Motor Vehicle Sales: Autos and Lt Trucks",
        "metric_type": "sales",
        "vehicle_type": "light_vehicle",
        "unit": "thousands_units",
    },
    "TOTALSA": {
        "name": "Total Vehicle Sales",
        "metric_type": "sales",
        "vehicle_type": "total",
        "unit": "million_units",
    },
}

# Cox Automotive report URLs (these may change - using known patterns)
COX_AUTOMOTIVE_REPORTS = {
    "used_vehicle_inventory": {
        "name": "Used Vehicle Inventory",
        "metric_type": "inventory",
        "vehicle_type": "used",
        "unit": "vehicles",
        "url_pattern": "https://www.coxautoinc.com/wp-content/uploads/{year}/{month:02d}/used-vehicle-inventory-data.xlsx",
    },
}


class VehicleProductionCollector(BaseCollector):
    """Collector for vehicle production and inventory data.

    Uses free/public data sources as alternatives to WardsAuto:
    - Census M3 API for manufacturer shipments/inventory
    - FRED for inventory/sales ratios and sales data
    - Cox Automotive for used vehicle market context
    - BEA for quarterly motor vehicle output
    """

    CENSUS_M3_BASE_URL = "https://api.census.gov/data/timeseries/eits/m3"
    FRED_BASE_URL = "https://api.stlouisfed.org/fred"
    BEA_BASE_URL = "https://apps.bea.gov/api/data"

    def __init__(self):
        """Initialize the Vehicle Production collector."""
        super().__init__(name="Vehicle Production", source_id="vehicle_production")
        self.settings = get_settings()

        self.census_limiter = get_rate_limiter("census")
        self.fred_limiter = get_rate_limiter("fred")
        self.bea_limiter = get_rate_limiter("bea")

        registry = RateLimiterRegistry()
        cox_config = RateLimitConfig(requests_per_minute=30, burst_size=5)
        self.cox_limiter = registry.get_or_create("cox_automotive", cox_config)

    @property
    def frequency(self) -> DataFrequency:
        """Primary frequency is monthly."""
        return DataFrequency.MONTHLY

    def get_schedule(self) -> str:
        """Return cron schedule - Wednesday at 4 PM UTC (after Census M3 release)."""
        return "0 16 * * 3"

    def get_default_series(self) -> list[str]:
        """Return default series to collect."""
        return list(CENSUS_M3_SERIES.keys()) + list(FRED_VEHICLE_SERIES.keys())

    async def collect(
        self,
        series_ids: list[str] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> list[DataPoint]:
        """Collect vehicle production and inventory data from all sources.

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
            start_date = end_date - timedelta(days=365)

        data_points: list[DataPoint] = []

        async with httpx.AsyncClient(timeout=60.0) as client:
            self.logger.info("Collecting Census M3 vehicle shipments/inventory")
            try:
                m3_points = await self._collect_census_m3(client, start_date, end_date)
                data_points.extend(m3_points)
            except Exception as e:
                self.logger.error("Failed to collect Census M3 data", error=str(e))

            self.logger.info("Collecting FRED inventory/sales ratio")
            try:
                fred_points = await self._collect_fred_inventory_ratio(
                    client, start_date, end_date
                )
                data_points.extend(fred_points)
            except Exception as e:
                self.logger.error("Failed to collect FRED data", error=str(e))

            self.logger.info("Collecting Cox Automotive used vehicle data")
            try:
                cox_points = await self._collect_cox_inventory(client)
                data_points.extend(cox_points)
            except Exception as e:
                self.logger.error("Failed to collect Cox Automotive data", error=str(e))

            self.logger.info("Collecting BEA motor vehicle output")
            try:
                bea_points = await self._collect_bea_vehicle_output(
                    client, start_date, end_date
                )
                data_points.extend(bea_points)
            except Exception as e:
                self.logger.error("Failed to collect BEA data", error=str(e))

        self.logger.info(
            "Vehicle production collection complete",
            total_records=len(data_points),
        )

        return data_points

    async def _collect_census_m3(
        self,
        client: httpx.AsyncClient,
        start_date: datetime,
        end_date: datetime,
    ) -> list[DataPoint]:
        """Collect Census M3 vehicle shipments and inventory data.

        The M3 survey provides monthly data on manufacturers' shipments,
        inventories, and orders for motor vehicles and parts.

        Args:
            client: HTTP client
            start_date: Start date
            end_date: End date

        Returns:
            List of DataPoint objects
        """
        data_points: list[DataPoint] = []
        api_key = self.settings.census_api_key

        for series_id, config in CENSUS_M3_SERIES.items():
            try:
                params: dict[str, Any] = {
                    "get": "cell_value,time_slot_id,data_type_code",
                    "category_code": "MVP" if "MVP" in series_id else "ALT",
                    "data_type_code": series_id,
                    "time": f"from {start_date.year}-{start_date.month:02d} to {end_date.year}-{end_date.month:02d}",
                }

                if api_key:
                    params["key"] = api_key.get_secret_value()

                async with self.census_limiter:
                    response = await client.get(self.CENSUS_M3_BASE_URL, params=params)

                if response.status_code != 200:
                    self.logger.warning(
                        "Census M3 API error",
                        series=series_id,
                        status=response.status_code,
                        response=response.text[:200],
                    )
                    continue

                data = response.json()

                # Census API returns data as array of arrays
                # First row is headers, subsequent rows are data
                if len(data) < 2:
                    continue

                headers = data[0]
                value_idx = headers.index("cell_value") if "cell_value" in headers else 0
                time_idx = headers.index("time_slot_id") if "time_slot_id" in headers else 1

                for row in data[1:]:
                    try:
                        value_str = row[value_idx]
                        time_str = row[time_idx]

                        if not value_str or value_str in ("N/A", "null", ""):
                            continue

                        value = float(value_str)

                        # Parse time (format: YYYY-MM)
                        if "-" in time_str:
                            year, month = time_str.split("-")[:2]
                            timestamp = datetime(int(year), int(month), 1)
                        else:
                            continue

                        dp = DataPoint(
                            source_id=self.source_id,
                            series_id=f"CENSUS_M3_{series_id}",
                            timestamp=timestamp,
                            value=value,
                            unit=config["unit"],
                            metadata={
                                "metric_type": config["metric_type"],
                                "vehicle_type": config["vehicle_type"],
                                "source": "CENSUS_M3",
                                "seasonal_adjustment": config.get("seasonal_adjustment"),
                                "name": config["name"],
                            },
                        )
                        data_points.append(dp)

                    except (ValueError, IndexError) as e:
                        self.logger.debug(
                            "Failed to parse Census M3 record",
                            series=series_id,
                            error=str(e),
                        )
                        continue

            except Exception as e:
                self.logger.error(
                    "Failed to fetch Census M3 series",
                    series=series_id,
                    error=str(e),
                )
                continue

        self.logger.info("Census M3 collection complete", records=len(data_points))
        return data_points

    async def _collect_fred_inventory_ratio(
        self,
        client: httpx.AsyncClient,
        start_date: datetime,
        end_date: datetime,
    ) -> list[DataPoint]:
        """Collect FRED vehicle inventory and sales data.

        Primary series: AISRSA (Auto Inventory/Sales Ratio)
        This replaces WardsAuto days-supply data.

        Args:
            client: HTTP client
            start_date: Start date
            end_date: End date

        Returns:
            List of DataPoint objects
        """
        data_points: list[DataPoint] = []
        api_key = self.settings.fred_api_key

        if not api_key:
            self.logger.warning("FRED API key not configured")
            return data_points

        key_value = api_key.get_secret_value()

        for series_id, config in FRED_VEHICLE_SERIES.items():
            try:
                params = {
                    "series_id": series_id,
                    "api_key": key_value,
                    "file_type": "json",
                    "observation_start": start_date.strftime("%Y-%m-%d"),
                    "observation_end": end_date.strftime("%Y-%m-%d"),
                }

                async with self.fred_limiter:
                    response = await client.get(
                        f"{self.FRED_BASE_URL}/series/observations",
                        params=params,
                    )

                if response.status_code != 200:
                    self.logger.warning(
                        "FRED API error",
                        series=series_id,
                        status=response.status_code,
                    )
                    continue

                data = response.json()
                observations = data.get("observations", [])

                for obs in observations:
                    try:
                        value_str = obs.get("value", ".")
                        if value_str == "." or value_str is None:
                            continue

                        value = float(value_str)
                        timestamp = datetime.strptime(obs["date"], "%Y-%m-%d")

                        dp = DataPoint(
                            source_id=self.source_id,
                            series_id=f"FRED_{series_id}",
                            timestamp=timestamp,
                            value=value,
                            unit=config["unit"],
                            metadata={
                                "metric_type": config["metric_type"],
                                "vehicle_type": config["vehicle_type"],
                                "source": "FRED",
                                "name": config["name"],
                                "realtime_start": obs.get("realtime_start"),
                                "realtime_end": obs.get("realtime_end"),
                            },
                        )
                        data_points.append(dp)

                    except (ValueError, KeyError) as e:
                        self.logger.debug(
                            "Failed to parse FRED observation",
                            series=series_id,
                            error=str(e),
                        )
                        continue

            except Exception as e:
                self.logger.error(
                    "Failed to fetch FRED series",
                    series=series_id,
                    error=str(e),
                )
                continue

        self.logger.info("FRED collection complete", records=len(data_points))
        return data_points

    async def _collect_cox_inventory(
        self,
        client: httpx.AsyncClient,
    ) -> list[DataPoint]:
        """Collect Cox Automotive used vehicle inventory data.

        Cox Automotive publishes monthly reports with used vehicle
        inventory and days-supply data. This is a free alternative
        to WardsAuto for used vehicle market trends.

        Args:
            client: HTTP client

        Returns:
            List of DataPoint objects
        """
        data_points: list[DataPoint] = []

        # Try to fetch recent Cox Automotive reports
        # Note: Cox may change their URL structure, so we try multiple patterns
        now = datetime.now(UTC)

        for months_ago in range(3):
            target_date = now - timedelta(days=30 * months_ago)
            year = target_date.year
            month = target_date.month

            url_patterns = [
                f"https://www.coxautoinc.com/wp-content/uploads/{year}/{month:02d}/used-vehicle-inventory-data.xlsx",
                f"https://www.coxautoinc.com/wp-content/uploads/{year}/{month:02d}/inventory-data.xlsx",
            ]

            for url in url_patterns:
                try:
                    async with self.cox_limiter:
                        response = await client.get(url, follow_redirects=True)

                    if response.status_code == 200:
                        content_type = response.headers.get("content-type", "")
                        if "spreadsheet" in content_type or "excel" in content_type or url.endswith(".xlsx"):
                            cox_points = await self._parse_cox_excel(
                                response.content, target_date
                            )
                            data_points.extend(cox_points)
                            self.logger.info(
                                "Parsed Cox Automotive report",
                                url=url,
                                records=len(cox_points),
                            )
                            break

                except Exception as e:
                    self.logger.debug(
                        "Failed to fetch Cox report",
                        url=url,
                        error=str(e),
                    )
                    continue

        if not data_points:
            try:
                summary_points = await self._collect_cox_summary(client)
                data_points.extend(summary_points)
            except Exception as e:
                self.logger.debug("Failed to collect Cox summary data", error=str(e))

        self.logger.info("Cox Automotive collection complete", records=len(data_points))
        return data_points

    async def _parse_cox_excel(
        self,
        content: bytes,
        report_date: datetime,
    ) -> list[DataPoint]:
        """Parse Cox Automotive Excel report.

        Args:
            content: Excel file content
            report_date: Date of the report

        Returns:
            List of DataPoint objects
        """
        data_points: list[DataPoint] = []

        try:
            df = pd.read_excel(io.BytesIO(content), engine="openpyxl")

            inventory_cols = ["inventory", "unsold", "units", "total_inventory"]
            days_supply_cols = ["days_supply", "days supply", "daysupply"]
            price_cols = ["avg_price", "average_price", "price", "listing_price"]

            df.columns = df.columns.str.lower().str.strip()

            for idx, row in df.iterrows():
                try:
                    for col in inventory_cols:
                        if col in df.columns and pd.notna(row.get(col)):
                            dp = DataPoint(
                                source_id=self.source_id,
                                series_id="COX_USED_INVENTORY",
                                timestamp=report_date,
                                value=float(row[col]),
                                unit="vehicles",
                                metadata={
                                    "metric_type": "inventory",
                                    "vehicle_type": "used",
                                    "source": "COX",
                                    "row_index": idx,
                                },
                            )
                            data_points.append(dp)
                            break

                    for col in days_supply_cols:
                        if col in df.columns and pd.notna(row.get(col)):
                            dp = DataPoint(
                                source_id=self.source_id,
                                series_id="COX_DAYS_SUPPLY",
                                timestamp=report_date,
                                value=float(row[col]),
                                unit="days",
                                metadata={
                                    "metric_type": "days_supply",
                                    "vehicle_type": "used",
                                    "source": "COX",
                                    "row_index": idx,
                                },
                            )
                            data_points.append(dp)
                            break

                except Exception as e:
                    self.logger.debug(f"Failed to parse Cox row {idx}", error=str(e))
                    continue

        except Exception as e:
            self.logger.error("Failed to parse Cox Excel file", error=str(e))

        return data_points

    async def _collect_cox_summary(
        self,
        client: httpx.AsyncClient,
    ) -> list[DataPoint]:
        """Collect summary data from Cox Automotive insights page.

        Falls back to scraping publicly available summary metrics
        when Excel files are not available.

        Args:
            client: HTTP client

        Returns:
            List of DataPoint objects
        """
        data_points: list[DataPoint] = []

        # Cox publishes insights at predictable URLs
        insights_urls = [
            "https://www.coxautoinc.com/market-insights/used-vehicle-inventory/",
            "https://www.coxautoinc.com/insights-hub/",
        ]

        for url in insights_urls:
            try:
                async with self.cox_limiter:
                    response = await client.get(url, follow_redirects=True)

                if response.status_code != 200:
                    continue

                # Note: Full HTML parsing would require BeautifulSoup
                # For now, we log that we found the page but didn't parse it
                self.logger.debug(
                    "Found Cox insights page",
                    url=url,
                    content_length=len(response.content),
                )

            except Exception as e:
                self.logger.debug("Failed to fetch Cox insights", url=url, error=str(e))
                continue

        return data_points

    async def _collect_bea_vehicle_output(
        self,
        client: httpx.AsyncClient,
        start_date: datetime,
        end_date: datetime,
    ) -> list[DataPoint]:
        """Collect BEA motor vehicle output data (quarterly).

        This provides quarterly gross output for motor vehicles
        as part of GDP accounting.

        Args:
            client: HTTP client
            start_date: Start date
            end_date: End date

        Returns:
            List of DataPoint objects
        """
        data_points: list[DataPoint] = []
        api_key = self.settings.bea_api_key

        if not api_key:
            self.logger.warning("BEA API key not configured")
            return data_points

        key_value = api_key.get_secret_value()

        try:
            # BEA NIPA Table 1.2.5 - Motor vehicles and parts output
            params = {
                "UserID": key_value,
                "method": "GetData",
                "datasetname": "NIPA",
                "TableName": "T10105",  # Gross Output by Industry
                "Frequency": "Q",
                "Year": ",".join(str(y) for y in range(start_date.year, end_date.year + 1)),
                "ResultFormat": "JSON",
            }

            async with self.bea_limiter:
                response = await client.get(self.BEA_BASE_URL, params=params)

            if response.status_code != 200:
                self.logger.warning(
                    "BEA API error",
                    status=response.status_code,
                )
                return data_points

            data = response.json()
            results = data.get("BEAAPI", {}).get("Results", {}).get("Data", [])

            for record in results:
                try:
                    line_desc = record.get("LineDescription", "").lower()
                    if "motor vehicle" not in line_desc:
                        continue

                    value_str = record.get("DataValue", "")
                    if not value_str or value_str == "--":
                        continue

                    value = float(value_str.replace(",", ""))

                    # Parse time period (format: 2024Q1)
                    time_period = record.get("TimePeriod", "")
                    if len(time_period) >= 5 and "Q" in time_period:
                        year = int(time_period[:4])
                        quarter = int(time_period[5])
                        month = (quarter - 1) * 3 + 1
                        timestamp = datetime(year, month, 1)
                    else:
                        continue

                    if not (start_date <= timestamp <= end_date):
                        continue

                    dp = DataPoint(
                        source_id=self.source_id,
                        series_id="BEA_MOTOR_VEHICLE_OUTPUT",
                        timestamp=timestamp,
                        value=value,
                        unit="billion_dollars",
                        metadata={
                            "metric_type": "output",
                            "vehicle_type": "total",
                            "source": "BEA",
                            "table": "T10105",
                            "line_description": record.get("LineDescription"),
                            "quarter": time_period,
                        },
                    )
                    data_points.append(dp)

                except (ValueError, KeyError) as e:
                    self.logger.debug(
                        "Failed to parse BEA record",
                        error=str(e),
                    )
                    continue

        except Exception as e:
            self.logger.error("Failed to fetch BEA data", error=str(e))

        self.logger.info("BEA collection complete", records=len(data_points))
        return data_points

    async def validate_api_key(self) -> bool:
        """Validate API keys for Census, FRED, and BEA.

        Returns:
            True if at least one API is accessible
        """
        census_valid = False
        fred_valid = False
        bea_valid = False

        async with httpx.AsyncClient(timeout=30.0) as client:
            if self.settings.census_api_key:
                try:
                    params = {"get": "cell_value", "time": "2024-01"}
                    if self.settings.census_api_key:
                        params["key"] = self.settings.census_api_key.get_secret_value()
                    response = await client.get(self.CENSUS_M3_BASE_URL, params=params)
                    census_valid = response.status_code in (200, 400)
                except Exception as e:
                    self.logger.warning("Census validation failed", error=str(e))

            if self.settings.fred_api_key:
                try:
                    response = await client.get(
                        f"{self.FRED_BASE_URL}/series",
                        params={
                            "series_id": "AISRSA",
                            "api_key": self.settings.fred_api_key.get_secret_value(),
                            "file_type": "json",
                        },
                    )
                    fred_valid = response.status_code == 200
                except Exception as e:
                    self.logger.warning("FRED validation failed", error=str(e))

            if self.settings.bea_api_key:
                try:
                    response = await client.get(
                        self.BEA_BASE_URL,
                        params={
                            "UserID": self.settings.bea_api_key.get_secret_value(),
                            "method": "GetParameterList",
                            "datasetname": "NIPA",
                            "ResultFormat": "JSON",
                        },
                    )
                    bea_valid = response.status_code == 200
                except Exception as e:
                    self.logger.warning("BEA validation failed", error=str(e))

        return census_valid or fred_valid or bea_valid


def get_vehicle_production_collector() -> VehicleProductionCollector:
    """Get a Vehicle Production collector instance."""
    return VehicleProductionCollector()
