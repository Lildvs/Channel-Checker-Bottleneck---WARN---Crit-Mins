"""Critical Minerals collector for tracking mineral supply data.

Collects critical minerals data from multiple sources:
- USGS Mineral Commodity Summaries (annual download)
- UN Comtrade trade flows (monthly API)
- IEA Critical Minerals dataset (SDMX API + annual download)

Data Sources:
- USGS MCS: https://www.usgs.gov/centers/national-minerals-information-center/mineral-commodity-summaries
- UN Comtrade: https://comtradedeveloper.un.org/
- IEA SDMX: https://sdmx.iea.org/ (REST API for Critical Minerals)
- IEA Data: https://www.iea.org/data-and-statistics/data-product/critical-minerals-dataset

IEA Critical Minerals Data includes:
- Demand projections for 37 key minerals under different scenarios
- Long-term supply projections for mining and refining
- Technology-specific cases for clean energy transition
"""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import httpx
import pandas as pd
import structlog

from src.config.settings import get_settings
from src.data_ingestion.base_collector import DataFrequency, DataPoint
from src.data_ingestion.file_collector import (
    DatasetConfig,
    FileBasedCollector,
    FileFormat,
)
from src.data_ingestion.rate_limiter import RateLimitConfig, RateLimiterRegistry

logger = structlog.get_logger()


@dataclass
class MineralRecord:
    """Represents a mineral data record."""

    mineral: str
    country: str | None
    year: int
    data_type: str  # production, import, export, reserve
    value: Decimal
    unit: str
    source: str
    metadata: dict[str, Any] = field(default_factory=dict)


class CriticalMineralsCollector(FileBasedCollector):
    """Collector for critical minerals data from multiple sources.

    Collects:
    - USGS Mineral Commodity Summaries (annual)
    - UN Comtrade trade flows (monthly)
    - IEA Critical Minerals data (annual)
    """

    USGS_MCS_URL_TEMPLATE = "https://pubs.usgs.gov/periodicals/mcs{year}/mcs{year}.pdf"
    USGS_SCIENCEBASE_CATALOG = "https://www.sciencebase.gov/catalog/item/5f69f9c6d34e2a1f7dd1f8d3"

    # UN Comtrade API — path segments: /get/{typeCode}/{freqCode}/{clCode}
    COMTRADE_API_BASE = "https://comtradeapi.un.org/data/v1/get"

    # Public endpoint - no API key required for bulk data
    IEA_SDMX_BASE_URL = "https://sdmx.iea.org/rest"
    IEA_SDMX_DATA_URL = f"{IEA_SDMX_BASE_URL}/data"

    IEA_DATAFLOWS = {
        "CRITMIN": "Critical Minerals Dataset",
        "CRITMIN_DEMAND": "Critical Minerals Demand Projections",
        "CRITMIN_SUPPLY": "Critical Minerals Supply Projections",
    }

    IEA_MINERALS = {
        "LI": "lithium",
        "CO": "cobalt",
        "NI": "nickel",
        "GR": "graphite",
        "CU": "copper",
        "MN": "manganese",
        "AL": "aluminum",
        "REE": "rare_earths",
        "PT": "platinum",
        "PD": "palladium",
        "VA": "vanadium",
        "TI": "titanium",
        "W": "tungsten",
        "GA": "gallium",
        "GE": "germanium",
        "SI": "silicon",
        "MG": "magnesium",
    }

    IEA_SCENARIOS = {
        "NZE": "Net Zero Emissions by 2050",
        "APS": "Announced Pledges Scenario",
        "STEPS": "Stated Policies Scenario",
        "SDS": "Sustainable Development Scenario",
    }

    # IEA data download URL (requires free account for direct download)
    IEA_DATA_URL = "https://www.iea.org/data-and-statistics/data-product/critical-minerals-dataset"

    # Critical minerals of interest (DOE/DOI critical minerals list)
    CRITICAL_MINERALS = [
        "lithium",
        "cobalt",
        "nickel",
        "graphite",
        "manganese",
        "rare_earths",
        "platinum",
        "palladium",
        "copper",
        "zinc",
        "aluminum",
        "titanium",
        "tungsten",
        "vanadium",
        "gallium",
        "germanium",
        "indium",
    ]

    HS_CODES = {
        "lithium": ["282520", "283691"],  # Lithium oxide/hydroxide, carbonates
        "cobalt": ["8105"],  # Cobalt and articles thereof
        "nickel": ["7502"],  # Unwrought nickel
        "graphite": ["2504"],  # Natural graphite
        "manganese": ["2602", "8111"],  # Manganese ores, unwrought manganese
        "rare_earths": ["2846"],  # Rare-earth compounds
        "copper": ["7403"],  # Refined copper
        "zinc": ["7901"],  # Unwrought zinc
        "aluminum": ["7601"],  # Unwrought aluminum
        "titanium": ["8108"],  # Titanium and articles thereof
        "tungsten": ["8101"],  # Tungsten and articles thereof
        "platinum": ["7110"],  # Platinum
        "palladium": ["711021"],  # Palladium
    }

    def __init__(self):
        """Initialize the Critical Minerals collector."""
        super().__init__(
            name="Critical Minerals",
            source_id="critical_minerals",
            timeout=180.0,  # Longer timeout for large files
            max_retries=3,
        )
        self.settings = get_settings()
        self.trade_flow_records: list[dict[str, Any]] = []

        registry = RateLimiterRegistry()

        # Comtrade rate limiter (100 requests/day = ~4/hour)
        comtrade_config = RateLimitConfig(requests_per_minute=2, burst_size=2)
        self.comtrade_limiter = registry.get_or_create("un_comtrade", comtrade_config)

        # IEA rate limiter (be respectful - 30 requests/minute)
        iea_config = RateLimitConfig(requests_per_minute=30, burst_size=5)
        self.iea_limiter = registry.get_or_create("iea_sdmx", iea_config)

    @property
    def frequency(self) -> DataFrequency:
        """Mixed frequency - monthly for trade, annual for production."""
        return DataFrequency.MONTHLY

    def get_schedule(self) -> str:
        """Return cron schedule - 20th of month at 2 PM UTC."""
        return "0 14 20 * *"

    def get_datasets(self) -> list[DatasetConfig]:
        """Get the list of minerals datasets to collect.

        Note: USGS and IEA require manual URL updates each year.
        UN Comtrade is API-based and handled separately.

        Returns:
            List of dataset configurations
        """
        current_year = datetime.now().year
        return [
            DatasetConfig(
                dataset_id="USGS_MCS",
                url=f"{self.USGS_SCIENCEBASE_CATALOG}?format=json",
                format=FileFormat.CSV,
                filename=f"usgs_mcs_data_{current_year}.csv",
                description="USGS Mineral Commodity Summaries",
                expected_frequency=DataFrequency.ANNUAL,
                parser_options={"encoding": "utf-8"},
            ),
        ]

    def parse_dataframe_to_datapoints(
        self,
        df: pd.DataFrame,
        dataset_id: str,
    ) -> list[DataPoint]:
        """Convert parsed DataFrame to DataPoints.

        Args:
            df: Parsed pandas DataFrame
            dataset_id: The dataset identifier

        Returns:
            List of DataPoint objects
        """
        data_points: list[DataPoint] = []

        if dataset_id == "USGS_MCS":
            data_points = self._parse_usgs_mcs(df)
        elif dataset_id.startswith("COMTRADE_"):
            mineral = dataset_id.replace("COMTRADE_", "").lower()
            data_points = self._parse_comtrade_data(df, mineral)
        else:
            self.logger.warning(f"Unknown dataset: {dataset_id}")

        return data_points

    def _parse_usgs_mcs(self, df: pd.DataFrame) -> list[DataPoint]:
        """Parse USGS Mineral Commodity Summaries data.

        The exact format varies by year and data release.
        This is a flexible parser that tries to handle common formats.

        Args:
            df: DataFrame with USGS MCS data

        Returns:
            List of DataPoint objects
        """
        data_points: list[DataPoint] = []

        self.logger.debug("USGS MCS columns", columns=list(df.columns))

        mineral_col = None
        for col in df.columns:
            col_lower = col.lower()
            if any(term in col_lower for term in ["mineral", "commodity", "material"]):
                mineral_col = col
                break

        if mineral_col is None and len(df.columns) > 0:
            # Use first column as mineral identifier
            mineral_col = df.columns[0]

        year_cols = []
        for col in df.columns:
            try:
                year = int(col)
                if 1990 <= year <= 2030:
                    year_cols.append((col, year))
            except (ValueError, TypeError):
                continue

        for _, row in df.iterrows():
            try:
                mineral = str(row.get(mineral_col, "")).lower().strip()
                if not mineral or mineral in ["nan", "none", ""]:
                    continue

                is_critical = any(
                    cm in mineral for cm in self.CRITICAL_MINERALS
                )
                if not is_critical:
                    continue

                for col, year in year_cols:
                    value = row.get(col)
                    if pd.isna(value):
                        continue

                    try:
                        value = float(str(value).replace(",", ""))
                    except (ValueError, TypeError):
                        continue

                    timestamp = datetime(year, 1, 1)

                    dp = DataPoint(
                        source_id=self.source_id,
                        series_id=f"USGS_{mineral.upper()}_PRODUCTION",
                        timestamp=timestamp,
                        value=value,
                        unit="metric_tons",  # Assumed - varies by mineral
                        metadata={
                            "mineral": mineral,
                            "source": "USGS_MCS",
                            "data_type": "production",
                            "year": year,
                        },
                    )
                    data_points.append(dp)

            except Exception as e:
                self.logger.debug("Failed to parse USGS row", error=str(e))
                continue

        self.logger.info(
            "Parsed USGS MCS data",
            total_records=len(data_points),
        )

        return data_points

    def _parse_comtrade_data(
        self,
        df: pd.DataFrame,
        mineral: str,
        hs_code: str = "",
    ) -> list[DataPoint]:
        """Parse UN Comtrade trade flow data.

        Produces generic DataPoints for the data_points table AND structured
        trade flow records (appended to self.trade_flow_records) for the
        mineral_trade_flows table.

        Args:
            df: DataFrame with Comtrade data
            mineral: Mineral name
            hs_code: HS code used for this query

        Returns:
            List of DataPoint objects
        """
        from uuid import uuid4 as _uuid4

        data_points: list[DataPoint] = []

        for _, row in df.iterrows():
            try:
                period = str(row.get("period", ""))
                reporter = str(row.get("reporterDesc", row.get("reporterCode", "")))
                reporter_iso3 = str(row.get("reporterISO", row.get("reporterCode", "")))[:3] or None
                partner = str(row.get("partnerDesc", row.get("partnerCode", "")))
                partner_iso3 = str(row.get("partnerISO", row.get("partnerCode", "")))[:3] or None
                flow_code = str(row.get("flowCode", ""))
                value = row.get("primaryValue", row.get("tradeValue"))
                quantity = row.get("netWgt", row.get("qty"))
                hs_desc = str(row.get("cmdDescE", row.get("cmdDesc", ""))) or None
                qty_unit = str(row.get("qtyUnitAbbr", row.get("qtAltUnitAbbr", ""))) or None
                row_hs_code = str(row.get("cmdCode", hs_code))

                if pd.isna(value):
                    continue

                flow_type = "import" if flow_code in ("M", "1") else "export"

                if len(period) == 6:
                    year = int(period[:4])
                    month = int(period[4:])
                    timestamp = datetime(year, month, 1)
                elif len(period) == 4:
                    year = int(period)
                    timestamp = datetime(year, 1, 1)
                    period = f"{year}12"
                else:
                    continue

                series_id = f"COMTRADE_{mineral.upper()}_{flow_type.upper()}"

                dp = DataPoint(
                    source_id=self.source_id,
                    series_id=series_id,
                    timestamp=timestamp,
                    value=float(value),
                    unit="USD",
                    metadata={
                        "mineral": mineral,
                        "source": "UN_COMTRADE",
                        "flow_type": flow_type,
                        "reporter": reporter,
                        "partner": partner,
                        "quantity_kg": float(quantity) if pd.notna(quantity) else None,
                        "period": period,
                    },
                )
                data_points.append(dp)

                weight_kg = float(quantity) if pd.notna(quantity) else None

                self.trade_flow_records.append({
                    "id": str(_uuid4()),
                    "mineral": mineral,
                    "hs_code": row_hs_code,
                    "hs_description": hs_desc,
                    "reporter_country": reporter,
                    "reporter_iso3": reporter_iso3,
                    "partner_country": partner,
                    "partner_iso3": partner_iso3,
                    "flow_type": flow_type,
                    "value_usd": float(value),
                    "quantity": float(quantity) if pd.notna(quantity) else None,
                    "quantity_unit": qty_unit,
                    "weight_kg": weight_kg,
                    "period": period,
                    "period_start": timestamp,
                    "source": "un_comtrade",
                    "collected_at": datetime.now(UTC),
                    "raw_metadata": {},
                })

            except Exception as e:
                self.logger.debug("Failed to parse Comtrade row", error=str(e))
                continue

        return data_points

    async def collect_iea_data(
        self,
        dataflow: str = "CRITMIN",
        scenario: str | None = None,
    ) -> list[DataPoint]:
        """Collect IEA Critical Minerals data via SDMX REST API.

        The IEA provides critical minerals data through their SDMX endpoint.
        This includes demand projections under different scenarios and
        supply projections for mining and refining.

        Args:
            dataflow: SDMX dataflow ID (default: CRITMIN)
            scenario: Optional scenario filter (NZE, APS, STEPS, SDS)

        Returns:
            List of DataPoint objects
        """
        data_points: list[DataPoint] = []

        try:
            # Format: /data/{dataflow}/{key}?format=csv
            url = f"{self.IEA_SDMX_DATA_URL}/{dataflow}/all"

            params = {
                "format": "csv",  # Request CSV format for easier parsing
                "detail": "full",
            }

            if scenario:
                params["c[SCENARIO]"] = scenario

            async with httpx.AsyncClient(timeout=120.0) as client:
                async with self.iea_limiter:
                    response = await client.get(url, params=params)

                if response.status_code == 404:
                    url = f"{self.IEA_SDMX_BASE_URL}/data/{dataflow}"
                    async with self.iea_limiter:
                        response = await client.get(url, params=params)

                if response.status_code != 200:
                    self.logger.warning(
                        "IEA SDMX API error",
                        dataflow=dataflow,
                        status=response.status_code,
                        url=url,
                    )
                    return await self._collect_iea_fallback()

                content = response.text
                if content:
                    data_points = self._parse_iea_sdmx_response(content, dataflow)

        except (httpx.TimeoutException, httpx.ConnectError) as e:
            self.logger.warning(
                "IEA SDMX request failed (timeout or DNS), using fallback",
                error=str(e),
            )
            return await self._collect_iea_fallback()
        except Exception as e:
            self.logger.error(
                "Failed to fetch IEA data",
                dataflow=dataflow,
                error=str(e),
            )
            return await self._collect_iea_fallback()

        self.logger.info(
            "Collected IEA Critical Minerals data",
            dataflow=dataflow,
            records=len(data_points),
        )

        return data_points

    def _parse_iea_sdmx_response(
        self,
        content: str,
        dataflow: str,
    ) -> list[DataPoint]:
        """Parse IEA SDMX CSV response.

        Args:
            content: CSV content from SDMX response
            dataflow: Dataflow identifier

        Returns:
            List of DataPoint objects
        """
        data_points: list[DataPoint] = []

        try:
            from io import StringIO

            df = pd.read_csv(StringIO(content))

            if df.empty:
                return data_points

            self.logger.debug("IEA SDMX columns", columns=list(df.columns))

            # SDMX CSV typically has columns like:
            # DATAFLOW, FREQ, COMMODITY, SCENARIO, UNIT, TIME_PERIOD, OBS_VALUE
            for _, row in df.iterrows():
                try:
                    commodity_code = str(row.get("COMMODITY", row.get("REF_AREA", "")))
                    mineral = self.IEA_MINERALS.get(
                        commodity_code, commodity_code.lower()
                    )

                    scenario = str(row.get("SCENARIO", row.get("SCENARIO_CODE", "STEPS")))
                    scenario_name = self.IEA_SCENARIOS.get(scenario, scenario)

                    time_period = str(row.get("TIME_PERIOD", row.get("TIME", "")))
                    if not time_period:
                        continue

                    try:
                        year = int(time_period[:4])
                        timestamp = datetime(year, 1, 1)
                    except (ValueError, IndexError):
                        continue

                    value = row.get("OBS_VALUE", row.get("VALUE"))
                    if pd.isna(value):
                        continue

                    try:
                        value = float(value)
                    except (ValueError, TypeError):
                        continue

                    unit = str(row.get("UNIT", row.get("UNIT_MEASURE", "kt")))

                    if "DEMAND" in dataflow.upper():
                        data_type = "demand_projection"
                    elif "SUPPLY" in dataflow.upper():
                        data_type = "supply_projection"
                    else:
                        data_type = "projection"

                    series_id = f"IEA_{mineral.upper()}_{scenario}_{data_type.upper()}"

                    dp = DataPoint(
                        source_id=self.source_id,
                        series_id=series_id,
                        timestamp=timestamp,
                        value=value,
                        unit=unit,
                        metadata={
                            "mineral": mineral,
                            "source": "IEA_CRITICAL_MINERALS",
                            "data_type": data_type,
                            "scenario": scenario,
                            "scenario_name": scenario_name,
                            "dataflow": dataflow,
                            "year": year,
                            "commodity_code": commodity_code,
                        },
                    )
                    data_points.append(dp)

                except Exception as e:
                    self.logger.debug("Failed to parse IEA row", error=str(e))
                    continue

        except Exception as e:
            self.logger.error("Failed to parse IEA SDMX response", error=str(e))

        return data_points

    async def _collect_iea_fallback(self) -> list[DataPoint]:
        """Fallback collection for IEA data using cached/static dataset.

        If the SDMX API is unavailable, this provides static projections
        based on IEA's published Critical Minerals Outlook data.

        Returns:
            List of DataPoint objects with baseline projections
        """
        data_points: list[DataPoint] = []

        # Baseline demand projections from IEA Critical Minerals Outlook 2025
        # These are approximate values for key minerals under NZE scenario
        # Units: kt (kilotonnes) except where noted
        iea_baseline_projections = {
            "lithium": {
                2025: 190,
                2030: 490,
                2035: 810,
                2040: 1100,
                2050: 1850,
                "unit": "kt_LCE",  # Lithium Carbonate Equivalent
                "scenario": "NZE",
            },
            "cobalt": {
                2025: 210,
                2030: 340,
                2035: 400,
                2040: 440,
                2050: 520,
                "unit": "kt",
                "scenario": "NZE",
            },
            "nickel": {
                2025: 3200,
                2030: 4100,
                2035: 4900,
                2040: 5600,
                2050: 6800,
                "unit": "kt",
                "scenario": "NZE",
            },
            "graphite": {
                2025: 1800,
                2030: 3800,
                2035: 5200,
                2040: 6100,
                2050: 7500,
                "unit": "kt",
                "scenario": "NZE",
            },
            "copper": {
                2025: 26000,
                2030: 32000,
                2035: 37000,
                2040: 41000,
                2050: 48000,
                "unit": "kt",
                "scenario": "NZE",
            },
            "rare_earths": {
                2025: 95,
                2030: 160,
                2035: 220,
                2040: 280,
                2050: 370,
                "unit": "kt_REO",  # Rare Earth Oxide
                "scenario": "NZE",
            },
            "manganese": {
                2025: 2100,
                2030: 3200,
                2035: 3900,
                2040: 4400,
                2050: 5200,
                "unit": "kt",
                "scenario": "NZE",
            },
        }

        for mineral, projections in iea_baseline_projections.items():
            unit = projections.get("unit", "kt")
            scenario = projections.get("scenario", "NZE")

            for year, value in projections.items():
                if not isinstance(year, int):
                    continue

                timestamp = datetime(year, 1, 1)
                series_id = f"IEA_{mineral.upper()}_{scenario}_DEMAND"

                dp = DataPoint(
                    source_id=self.source_id,
                    series_id=series_id,
                    timestamp=timestamp,
                    value=float(value),
                    unit=unit,
                    metadata={
                        "mineral": mineral,
                        "source": "IEA_CRITICAL_MINERALS",
                        "data_type": "demand_projection",
                        "scenario": scenario,
                        "scenario_name": self.IEA_SCENARIOS.get(scenario, scenario),
                        "year": year,
                        "is_fallback": True,
                        "source_report": "Global Critical Minerals Outlook 2025",
                    },
                )
                data_points.append(dp)

        self.logger.info(
            "Using IEA fallback projections",
            records=len(data_points),
        )

        return data_points

    async def collect_comtrade_data(
        self,
        mineral: str,
        year: int,
        reporter_code: str = "all",
    ) -> list[DataPoint]:
        """Collect UN Comtrade trade flow data for a mineral.

        Args:
            mineral: Mineral name (must be in HS_CODES)
            year: Year to collect data for
            reporter_code: Reporter country code (default: all)

        Returns:
            List of DataPoint objects
        """
        data_points: list[DataPoint] = []

        hs_codes = self.HS_CODES.get(mineral)
        if not hs_codes:
            self.logger.warning(f"No HS codes configured for mineral: {mineral}")
            return data_points

        api_key = self.settings.un_comtrade_api_key
        if not api_key:
            self.logger.warning("UN Comtrade API key not configured")
            return data_points

        key_value = api_key.get_secret_value()

        async with httpx.AsyncClient(timeout=120.0) as client:
            for hs_code in hs_codes:
                try:
                    url = f"{self.COMTRADE_API_BASE}/C/A/HS"
                    params: dict[str, Any] = {
                        "subscription-key": key_value,
                        "period": str(year),
                        "cmdCode": hs_code,
                        "flowCode": "M,X",
                        "includeDesc": True,
                    }
                    if reporter_code and reporter_code.lower() != "all":
                        params["reportercode"] = reporter_code

                    async with self.comtrade_limiter:
                        response = await client.get(
                            url,
                            params=params,
                        )

                    if response.status_code != 200:
                        self.logger.warning(
                            "Comtrade API error",
                            mineral=mineral,
                            hs_code=hs_code,
                            status=response.status_code,
                            body=response.text[:500],
                        )
                        continue

                    data = response.json()

                    api_error = data.get("error")
                    if api_error:
                        self.logger.warning(
                            "Comtrade API returned error in response",
                            mineral=mineral,
                            hs_code=hs_code,
                            error=api_error,
                        )
                        continue

                    records = data.get("data", [])

                    self.logger.info(
                        "Comtrade API response",
                        mineral=mineral,
                        hs_code=hs_code,
                        record_count=len(records),
                    )

                    if records:
                        df = pd.DataFrame(records)
                        points = self._parse_comtrade_data(df, mineral, hs_code)
                        data_points.extend(points)

                except Exception as e:
                    self.logger.error(
                        "Failed to fetch Comtrade data",
                        mineral=mineral,
                        hs_code=hs_code,
                        error=str(e),
                    )
                    continue

        return data_points

    async def collect(
        self,
        series_ids: list[str] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> list[DataPoint]:
        """Collect minerals data from all sources.

        Sources:
        1. USGS Mineral Commodity Summaries (file-based, annual)
        2. UN Comtrade trade flows (API, monthly)
        3. IEA Critical Minerals (SDMX API + fallback, annual)

        Args:
            series_ids: Optional list of specific series to collect
            start_date: Start date for data
            end_date: End date for data

        Returns:
            List of DataPoint objects
        """
        all_data_points: list[DataPoint] = []
        self.trade_flow_records = []

        self.logger.info("Collecting USGS Mineral Commodity Summaries")
        try:
            file_points = await super().collect(series_ids, start_date, end_date)
            all_data_points.extend(file_points)
        except Exception as e:
            self.logger.error("Failed to collect file-based data", error=str(e))

        self.logger.info("Collecting IEA Critical Minerals data")
        try:
            iea_points = await self.collect_iea_data()
            all_data_points.extend(iea_points)
        except Exception as e:
            self.logger.error("Failed to collect IEA data", error=str(e))

        if self.settings.un_comtrade_api_key:
            self.logger.info("Collecting UN Comtrade trade flow data")
            current_year = datetime.now(UTC).year
            for mineral in ["lithium", "cobalt", "nickel", "rare_earths"]:
                try:
                    comtrade_points = await self.collect_comtrade_data(
                        mineral, current_year - 1  # Prior year for complete data
                    )
                    all_data_points.extend(comtrade_points)
                except Exception as e:
                    self.logger.error(
                        "Failed to collect Comtrade data",
                        mineral=mineral,
                        error=str(e),
                    )
                    continue

        self.logger.info(
            "Collection complete",
            total_records=len(all_data_points),
            sources=["USGS", "IEA", "Comtrade"],
        )

        return all_data_points

    async def validate_api_key(self) -> bool:
        """Validate UN Comtrade API key.

        Returns:
            True if API key is valid
        """
        if not self.settings.un_comtrade_api_key:
            self.logger.warning("UN Comtrade API key not configured")
            return False

        try:
            url = f"{self.COMTRADE_API_BASE}/C/A/HS"
            async with httpx.AsyncClient(timeout=30.0) as client:
                params = {
                    "subscription-key": self.settings.un_comtrade_api_key.get_secret_value(),
                    "period": "2023",
                    "reportercode": "840",  # USA
                    "cmdCode": "7403",  # Copper
                    "flowCode": "M",
                }
                response = await client.get(url, params=params)
                return response.status_code == 200

        except Exception as e:
            self.logger.error("Comtrade validation failed", error=str(e))
            return False


def get_critical_minerals_collector() -> CriticalMineralsCollector:
    """Get a Critical Minerals collector instance."""
    return CriticalMineralsCollector()
