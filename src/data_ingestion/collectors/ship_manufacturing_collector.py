"""Ship Manufacturing collector for tracking shipbuilding activity.

Collects data from:
- MARAD: Maritime Administration shipyard reports and US-flag fleet statistics
- UNCTAD: Review of Maritime Transport fleet and orderbook data

Data Documentation:
- MARAD: https://www.maritime.dot.gov/data-reports
- UNCTAD RMT: https://unctad.org/topic/transport-and-trade-logistics/review-of-maritime-transport
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta, UTC
from decimal import Decimal
from typing import Any
import io
import re

import httpx
import pandas as pd
import structlog

from src.config.settings import get_settings
from src.data_ingestion.base_collector import (
    BaseCollector,
    DataFrequency,
    DataPoint,
)
from src.data_ingestion.rate_limiter import RateLimitConfig, RateLimiterRegistry

logger = structlog.get_logger()


@dataclass
class ShipbuildingRecord:
    """Represents a shipbuilding data point."""

    shipyard: str | None
    vessel_type: str  # container, tanker, bulk, lng, cruise, naval, other
    metric_type: str  # orders, deliveries, backlog, capacity, fleet
    value: Decimal
    unit: str  # vessels, dwt, gt, teu
    timestamp: datetime
    source: str  # MARAD, UNCTAD
    country: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


MARAD_DATA_URLS = {
    "us_flag_fleet": {
        "name": "US-Flag Fleet Statistics",
        "url": "https://www.maritime.dot.gov/sites/marad.dot.gov/files/2024-01/DS_USFlag-Fleet.xlsx",
        "format": "excel",
        "metric_type": "fleet",
    },
    "shipyard_survey": {
        "name": "US Shipyard Survey",
        "url": "https://www.maritime.dot.gov/sites/marad.dot.gov/files/shipyard-survey.pdf",
        "format": "pdf",
        "metric_type": "capacity",
    },
}

VESSEL_TYPES = {
    "container": ["container", "containership", "container vessel"],
    "tanker": ["tanker", "crude", "oil", "product tanker", "chemical"],
    "bulk": ["bulk", "bulk carrier", "bulker", "dry bulk"],
    "lng": ["lng", "liquefied natural gas", "gas carrier"],
    "lpg": ["lpg", "liquefied petroleum gas"],
    "cruise": ["cruise", "passenger"],
    "roro": ["ro-ro", "roro", "roll-on", "vehicle carrier"],
    "naval": ["naval", "military", "navy", "coast guard"],
    "offshore": ["offshore", "supply vessel", "osv", "platform"],
    "other": ["general cargo", "ferry", "tug", "barge"],
}


class ShipManufacturingCollector(BaseCollector):
    """Collector for ship manufacturing and fleet data.

    Collects data from:
    - MARAD for US shipyard capacity and fleet statistics
    - UNCTAD for global fleet and orderbook trends
    """

    MARAD_BASE_URL = "https://www.maritime.dot.gov"
    UNCTAD_STAT_URL = "https://unctadstat.unctad.org/api"

    def __init__(self):
        """Initialize the Ship Manufacturing collector."""
        super().__init__(name="Ship Manufacturing", source_id="ship_manufacturing")
        self.settings = get_settings()

        registry = RateLimiterRegistry()
        marad_config = RateLimitConfig(requests_per_minute=30, burst_size=5)
        self.marad_limiter = registry.get_or_create("marad", marad_config)

        unctad_config = RateLimitConfig(requests_per_minute=20, burst_size=3)
        self.unctad_limiter = registry.get_or_create("unctad", unctad_config)

        # Track last collection to avoid duplicate downloads
        self._last_collection: datetime | None = None
        self._cached_data: dict[str, list[DataPoint]] = {}

    @property
    def frequency(self) -> DataFrequency:
        """Frequency is irregular - check daily but collect on change."""
        return DataFrequency.DAILY

    def get_schedule(self) -> str:
        """Return cron schedule - daily at 10 AM UTC."""
        return "0 10 * * *"

    def get_default_series(self) -> list[str]:
        """Return default series to collect."""
        return ["us_flag_fleet", "global_fleet", "orderbook"]

    async def collect(
        self,
        series_ids: list[str] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> list[DataPoint]:
        """Collect ship manufacturing and fleet data.

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
            start_date = end_date - timedelta(days=365 * 2)

        data_points: list[DataPoint] = []

        async with httpx.AsyncClient(timeout=120.0) as client:
            self.logger.info("Collecting MARAD US-flag fleet statistics")
            try:
                marad_points = await self._collect_marad_fleet(client)
                data_points.extend(marad_points)
            except Exception as e:
                self.logger.error("Failed to collect MARAD fleet data", error=str(e))

            self.logger.info("Collecting MARAD shipyard data")
            try:
                shipyard_points = await self._collect_marad_shipyards(client)
                data_points.extend(shipyard_points)
            except Exception as e:
                self.logger.error("Failed to collect MARAD shipyard data", error=str(e))

            self.logger.info("Collecting UNCTAD global fleet data")
            try:
                unctad_points = await self._collect_unctad_fleet(client, start_date, end_date)
                data_points.extend(unctad_points)
            except Exception as e:
                self.logger.error("Failed to collect UNCTAD data", error=str(e))

        self.logger.info(
            "Ship manufacturing collection complete",
            total_records=len(data_points),
        )

        return data_points

    async def _collect_marad_fleet(
        self,
        client: httpx.AsyncClient,
    ) -> list[DataPoint]:
        """Collect MARAD US-flag fleet statistics.

        MARAD publishes US-flag fleet data in Excel format.

        Args:
            client: HTTP client

        Returns:
            List of DataPoint objects
        """
        data_points: list[DataPoint] = []

        fleet_urls = [
            "https://www.maritime.dot.gov/sites/marad.dot.gov/files/2024-01/DS_USFlag-Fleet.xlsx",
            "https://www.maritime.dot.gov/sites/marad.dot.gov/files/DS_USFlag-Fleet.xlsx",
            "https://www.maritime.dot.gov/data-reports/data-statistics/us-flag-fleet",
        ]

        for url in fleet_urls:
            try:
                async with self.marad_limiter:
                    response = await client.get(url, follow_redirects=True)

                if response.status_code == 200:
                    content_type = response.headers.get("content-type", "")

                    if "spreadsheet" in content_type or "excel" in content_type or url.endswith(".xlsx"):
                        points = await self._parse_marad_fleet_excel(response.content)
                        data_points.extend(points)
                        self.logger.info(
                            "Parsed MARAD fleet data",
                            url=url,
                            records=len(points),
                        )
                        break

            except Exception as e:
                self.logger.debug(
                    "Failed to fetch MARAD fleet file",
                    url=url,
                    error=str(e),
                )
                continue

        return data_points

    async def _parse_marad_fleet_excel(
        self,
        content: bytes,
    ) -> list[DataPoint]:
        """Parse MARAD US-flag fleet Excel file.

        Args:
            content: Excel file content

        Returns:
            List of DataPoint objects
        """
        data_points: list[DataPoint] = []

        try:
            excel_file = pd.ExcelFile(io.BytesIO(content), engine="openpyxl")

            for sheet_name in excel_file.sheet_names:
                try:
                    df = pd.read_excel(excel_file, sheet_name=sheet_name)

                    df.columns = df.columns.astype(str).str.lower().str.strip()

                    vessel_cols = ["vessels", "number", "count", "ships"]
                    tonnage_cols = ["dwt", "grt", "gt", "tonnage", "deadweight"]

                    for idx, row in df.iterrows():
                        try:
                            first_col = df.columns[0]
                            vessel_type_raw = str(row.get(first_col, "")).lower()
                            vessel_type = self._classify_vessel_type(vessel_type_raw)

                            if "vessel" in vessel_type_raw or "type" in vessel_type_raw:
                                continue

                            for col in vessel_cols:
                                matching_cols = [c for c in df.columns if col in c]
                                for mc in matching_cols:
                                    if pd.notna(row.get(mc)):
                                        try:
                                            value = float(row[mc])
                                            if value > 0:
                                                dp = DataPoint(
                                                    source_id=self.source_id,
                                                    series_id=f"MARAD_FLEET_{vessel_type.upper()}_COUNT",
                                                    timestamp=datetime.now(UTC),
                                                    value=value,
                                                    unit="vessels",
                                                    metadata={
                                                        "metric_type": "fleet",
                                                        "vessel_type": vessel_type,
                                                        "source": "MARAD",
                                                        "country": "US",
                                                        "sheet": sheet_name,
                                                    },
                                                )
                                                data_points.append(dp)
                                        except (ValueError, TypeError):
                                            pass

                            for col in tonnage_cols:
                                matching_cols = [c for c in df.columns if col in c]
                                for mc in matching_cols:
                                    if pd.notna(row.get(mc)):
                                        try:
                                            value = float(str(row[mc]).replace(",", ""))
                                            if value > 0:
                                                unit = "dwt" if "dwt" in mc else "gt"
                                                dp = DataPoint(
                                                    source_id=self.source_id,
                                                    series_id=f"MARAD_FLEET_{vessel_type.upper()}_{unit.upper()}",
                                                    timestamp=datetime.now(UTC),
                                                    value=value,
                                                    unit=unit,
                                                    metadata={
                                                        "metric_type": "fleet",
                                                        "vessel_type": vessel_type,
                                                        "source": "MARAD",
                                                        "country": "US",
                                                        "sheet": sheet_name,
                                                    },
                                                )
                                                data_points.append(dp)
                                        except (ValueError, TypeError):
                                            pass

                        except Exception as e:
                            self.logger.debug(f"Failed to parse row {idx}", error=str(e))
                            continue

                except Exception as e:
                    self.logger.debug(f"Failed to parse sheet {sheet_name}", error=str(e))
                    continue

        except Exception as e:
            self.logger.error("Failed to parse MARAD Excel file", error=str(e))

        return data_points

    async def _collect_marad_shipyards(
        self,
        client: httpx.AsyncClient,
    ) -> list[DataPoint]:
        """Collect MARAD shipyard survey data.

        Note: Shipyard survey data is often in PDF format which requires
        additional parsing. This method attempts to find any available
        machine-readable data.

        Args:
            client: HTTP client

        Returns:
            List of DataPoint objects
        """
        data_points: list[DataPoint] = []

        shipyard_urls = [
            "https://www.maritime.dot.gov/sites/marad.dot.gov/files/shipyard-data.xlsx",
            "https://www.maritime.dot.gov/data-reports/shipyard-reports",
        ]

        for url in shipyard_urls:
            try:
                async with self.marad_limiter:
                    response = await client.get(url, follow_redirects=True)

                if response.status_code == 200:
                    content_type = response.headers.get("content-type", "")

                    if "spreadsheet" in content_type or "excel" in content_type:
                        points = await self._parse_marad_shipyard_excel(response.content)
                        data_points.extend(points)
                        self.logger.info(
                            "Parsed MARAD shipyard data",
                            url=url,
                            records=len(points),
                        )
                        break

            except Exception as e:
                self.logger.debug(
                    "Failed to fetch MARAD shipyard file",
                    url=url,
                    error=str(e),
                )
                continue

        return data_points

    async def _parse_marad_shipyard_excel(
        self,
        content: bytes,
    ) -> list[DataPoint]:
        """Parse MARAD shipyard Excel data.

        Args:
            content: Excel file content

        Returns:
            List of DataPoint objects
        """
        data_points: list[DataPoint] = []

        try:
            df = pd.read_excel(io.BytesIO(content), engine="openpyxl")
            df.columns = df.columns.astype(str).str.lower().str.strip()

            capacity_patterns = ["capacity", "berth", "dry dock", "graving dock"]
            order_patterns = ["order", "backlog", "contract"]

            for idx, row in df.iterrows():
                try:
                    shipyard_name = None
                    for col in df.columns:
                        if "name" in col or "shipyard" in col:
                            shipyard_name = str(row[col])
                            break

                    for col in df.columns:
                        if any(p in col for p in capacity_patterns):
                            if pd.notna(row[col]):
                                try:
                                    value = float(str(row[col]).replace(",", ""))
                                    dp = DataPoint(
                                        source_id=self.source_id,
                                        series_id="MARAD_SHIPYARD_CAPACITY",
                                        timestamp=datetime.now(UTC),
                                        value=value,
                                        unit="dwt",
                                        metadata={
                                            "metric_type": "capacity",
                                            "source": "MARAD",
                                            "shipyard": shipyard_name,
                                            "column": col,
                                        },
                                    )
                                    data_points.append(dp)
                                except (ValueError, TypeError):
                                    pass

                except Exception as e:
                    self.logger.debug(f"Failed to parse shipyard row {idx}", error=str(e))
                    continue

        except Exception as e:
            self.logger.error("Failed to parse MARAD shipyard Excel", error=str(e))

        return data_points

    async def _collect_unctad_fleet(
        self,
        client: httpx.AsyncClient,
        start_date: datetime,
        end_date: datetime,
    ) -> list[DataPoint]:
        """Collect UNCTAD global fleet and orderbook statistics.

        UNCTAD publishes annual data on world fleet by vessel type
        and flag of registration.

        Args:
            client: HTTP client
            start_date: Start date
            end_date: End date

        Returns:
            List of DataPoint objects
        """
        data_points: list[DataPoint] = []

        # UNCTAD Stat API endpoints for maritime data
        # Dataset codes from UNCTADStat
        unctad_datasets = {
            "MT.WorldFleet": {
                "name": "World Fleet by Vessel Type",
                "metric_type": "fleet",
                "unit": "dwt",
            },
            "MT.Orderbook": {
                "name": "World Orderbook",
                "metric_type": "orderbook",
                "unit": "dwt",
            },
        }

        for dataset_id, config in unctad_datasets.items():
            try:
                api_url = f"https://unctadstat-api.unctad.org/bulkdownload/{dataset_id}/en"

                async with self.unctad_limiter:
                    response = await client.get(api_url, follow_redirects=True)

                if response.status_code == 200:
                    content_type = response.headers.get("content-type", "")

                    if "csv" in content_type or "text" in content_type:
                        points = self._parse_unctad_csv(
                            response.text,
                            config["metric_type"],
                            config["unit"],
                            start_date,
                            end_date,
                        )
                        data_points.extend(points)
                        self.logger.info(
                            "Parsed UNCTAD data",
                            dataset=dataset_id,
                            records=len(points),
                        )

            except Exception as e:
                self.logger.debug(
                    "Failed to fetch UNCTAD dataset",
                    dataset=dataset_id,
                    error=str(e),
                )
                continue

        if not data_points:
            try:
                fallback_points = await self._collect_unctad_fallback(client)
                data_points.extend(fallback_points)
            except Exception as e:
                self.logger.debug("UNCTAD fallback collection failed", error=str(e))

        return data_points

    def _parse_unctad_csv(
        self,
        csv_content: str,
        metric_type: str,
        unit: str,
        start_date: datetime,
        end_date: datetime,
    ) -> list[DataPoint]:
        """Parse UNCTAD CSV data.

        Args:
            csv_content: CSV content string
            metric_type: Type of metric (fleet, orderbook)
            unit: Unit of measurement
            start_date: Start date filter
            end_date: End date filter

        Returns:
            List of DataPoint objects
        """
        data_points: list[DataPoint] = []

        try:
            df = pd.read_csv(io.StringIO(csv_content))
            df.columns = df.columns.astype(str).str.lower().str.strip()

            # UNCTAD typically has year, country, vessel_type, value columns
            year_col = next((c for c in df.columns if "year" in c), None)
            value_col = next((c for c in df.columns if "value" in c or "dwt" in c), None)
            type_col = next((c for c in df.columns if "type" in c or "vessel" in c), None)
            country_col = next((c for c in df.columns if "country" in c or "flag" in c), None)

            if not year_col or not value_col:
                return data_points

            for idx, row in df.iterrows():
                try:
                    year = int(row[year_col])
                    timestamp = datetime(year, 1, 1)

                    if not (start_date.year <= year <= end_date.year):
                        continue

                    value = float(str(row[value_col]).replace(",", ""))
                    vessel_type = self._classify_vessel_type(str(row.get(type_col, ""))) if type_col else "total"
                    country = str(row.get(country_col, "World")) if country_col else "World"

                    dp = DataPoint(
                        source_id=self.source_id,
                        series_id=f"UNCTAD_{metric_type.upper()}_{vessel_type.upper()}",
                        timestamp=timestamp,
                        value=value,
                        unit=unit,
                        metadata={
                            "metric_type": metric_type,
                            "vessel_type": vessel_type,
                            "source": "UNCTAD",
                            "country": country,
                            "year": year,
                        },
                    )
                    data_points.append(dp)

                except Exception as e:
                    self.logger.debug(f"Failed to parse UNCTAD row {idx}", error=str(e))
                    continue

        except Exception as e:
            self.logger.error("Failed to parse UNCTAD CSV", error=str(e))

        return data_points

    async def _collect_unctad_fallback(
        self,
        client: httpx.AsyncClient,
    ) -> list[DataPoint]:
        """Fallback collection from UNCTAD data hub.

        Attempts to fetch summary data from alternative UNCTAD sources.

        Args:
            client: HTTP client

        Returns:
            List of DataPoint objects
        """
        data_points: list[DataPoint] = []

        rmt_urls = [
            "https://unctad.org/topic/transport-and-trade-logistics/review-of-maritime-transport",
        ]

        for url in rmt_urls:
            try:
                async with self.unctad_limiter:
                    response = await client.get(url, follow_redirects=True)

                if response.status_code == 200:
                    self.logger.debug(
                        "Found UNCTAD RMT page",
                        url=url,
                        content_length=len(response.content),
                    )

            except Exception as e:
                self.logger.debug("Failed to fetch UNCTAD RMT", url=url, error=str(e))
                continue

        return data_points

    def _classify_vessel_type(self, vessel_text: str) -> str:
        """Classify vessel type from text description.

        Args:
            vessel_text: Text describing vessel type

        Returns:
            Standardized vessel type
        """
        vessel_lower = vessel_text.lower()

        for vessel_type, patterns in VESSEL_TYPES.items():
            if any(p in vessel_lower for p in patterns):
                return vessel_type

        return "other"

    async def validate_api_key(self) -> bool:
        """Validate that data sources are accessible.

        Returns:
            True if at least one source is accessible
        """
        marad_accessible = False
        unctad_accessible = False

        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                response = await client.get(
                    "https://www.maritime.dot.gov/data-reports",
                    follow_redirects=True,
                )
                marad_accessible = response.status_code == 200
            except Exception as e:
                self.logger.warning("MARAD accessibility check failed", error=str(e))

            try:
                response = await client.get(
                    "https://unctad.org/topic/transport-and-trade-logistics",
                    follow_redirects=True,
                )
                unctad_accessible = response.status_code == 200
            except Exception as e:
                self.logger.warning("UNCTAD accessibility check failed", error=str(e))

        return marad_accessible or unctad_accessible


def get_ship_manufacturing_collector() -> ShipManufacturingCollector:
    """Get a Ship Manufacturing collector instance."""
    return ShipManufacturingCollector()
