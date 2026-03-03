"""Aircraft and Drone Parts collector for aerospace supply chain monitoring.

Collects data from:
- Census Bureau HS Trade (Chapter 88): Aircraft, spacecraft, and parts trade
- FAA Aircraft Registry: Daily aircraft registration database
- USASpending: DOD contract awards for aerospace and UAS

API Documentation:
- Census Trade: https://api.census.gov/data/timeseries/intltrade.html
- FAA Registry: https://www.faa.gov/licenses_certificates/aircraft_certification/aircraft_registry
- USASpending: https://api.usaspending.gov/docs/
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta, UTC
from decimal import Decimal
from typing import Any
import io
import re
import zipfile

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
class AerospaceTrade:
    """Represents aerospace trade data."""

    hs_code: str  # 6-digit HS code within Chapter 88
    commodity: str  # Description
    flow: str  # import, export
    value: Decimal
    quantity: Decimal | None
    unit: str
    partner_country: str | None
    timestamp: datetime
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class DODContractAward:
    """Represents a DOD contract award."""

    award_id: str
    recipient: str
    amount: Decimal
    description: str
    award_date: datetime
    psc_code: str  # Product/Service Code
    naics_code: str | None
    is_drone_related: bool
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class AircraftRegistration:
    """Represents an FAA aircraft registration."""

    n_number: str
    aircraft_type: str
    manufacturer: str
    model: str
    registration_date: datetime
    owner_type: str  # individual, corporation, government, etc.
    is_drone: bool
    metadata: dict[str, Any] = field(default_factory=dict)


HS88_CODES = {
    "8801": {"name": "Balloons and dirigibles; gliders, hang gliders", "category": "lighter_than_air"},
    "8802": {"name": "Powered aircraft (helicopters, airplanes)", "category": "powered_aircraft"},
    "8803": {"name": "Parts of goods of 8801 or 8802", "category": "aircraft_parts"},
    "8804": {"name": "Parachutes and rotochutes", "category": "parachutes"},
    "8805": {"name": "Aircraft launching gear; deck-arrestor", "category": "ground_equipment"},
    "8806": {"name": "Unmanned aircraft (drones, UAS)", "category": "drones"},
    "8807": {"name": "Parts of aircraft of 8801, 8802, or 8806", "category": "parts"},
}

AEROSPACE_PSC_CODES = [
    "15",  # Aircraft and Airframe Structural Components
    "16",  # Aircraft Components and Accessories
    "17",  # Aircraft Launching, Landing, and Ground Handling Equipment
]

AEROSPACE_NAICS = {
    "336411": "Aircraft Manufacturing",
    "336412": "Aircraft Engine and Engine Parts Manufacturing",
    "336413": "Other Aircraft Parts and Auxiliary Equipment Manufacturing",
    "336414": "Guided Missile and Space Vehicle Manufacturing",
    "336415": "Guided Missile and Space Vehicle Propulsion Unit and Parts",
    "334511": "Search, Detection, Navigation, Guidance, Aeronautical Systems",
}

DRONE_KEYWORDS = [
    "drone", "uas", "uav", "unmanned", "remotely piloted",
    "quadcopter", "multirotor", "suas", "rpas",
]


class AircraftPartsCollector(BaseCollector):
    """Collector for aircraft and drone parts supply chain data.

    Collects data from:
    - Census HS Trade API for Chapter 88 (aerospace) trade flows
    - FAA Aircraft Registry for registration trends
    - USASpending for DOD aerospace contract awards
    """

    CENSUS_TRADE_BASE = "https://api.census.gov/data/timeseries/intltrade"
    FAA_REGISTRY_URL = "https://registry.faa.gov/database/ReleasableAircraft.zip"
    USASPENDING_API = "https://api.usaspending.gov/api/v2"

    def __init__(self):
        """Initialize the Aircraft Parts collector."""
        super().__init__(name="Aircraft Parts", source_id="aircraft_parts")
        self.settings = get_settings()

        self.census_limiter = get_rate_limiter("census")

        registry = RateLimiterRegistry()
        usaspending_config = RateLimitConfig(requests_per_minute=60, burst_size=10)
        self.usaspending_limiter = registry.get_or_create("usaspending", usaspending_config)

        faa_config = RateLimitConfig(requests_per_minute=10, burst_size=2)
        self.faa_limiter = registry.get_or_create("faa", faa_config)

    @property
    def frequency(self) -> DataFrequency:
        """Primary frequency is daily."""
        return DataFrequency.DAILY

    def get_schedule(self) -> str:
        """Return cron schedule - daily at noon UTC."""
        return "0 12 * * *"

    def get_default_series(self) -> list[str]:
        """Return default series to collect."""
        return ["hs88_trade", "faa_registry", "dod_awards"]

    async def collect(
        self,
        series_ids: list[str] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> list[DataPoint]:
        """Collect aircraft and drone parts data.

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

        async with httpx.AsyncClient(timeout=120.0) as client:
            self.logger.info("Collecting Census HS Chapter 88 trade data")
            try:
                trade_points = await self._collect_census_hs88_trade(
                    client, start_date, end_date
                )
                data_points.extend(trade_points)
            except Exception as e:
                self.logger.error("Failed to collect Census HS88 data", error=str(e))

            self.logger.info("Collecting FAA Aircraft Registry data")
            try:
                faa_points = await self._collect_faa_registry(client)
                data_points.extend(faa_points)
            except Exception as e:
                self.logger.error("Failed to collect FAA registry data", error=str(e))

            self.logger.info("Collecting USASpending DOD aerospace awards")
            try:
                usaspending_points = await self._collect_usaspending_dod(
                    client, start_date, end_date
                )
                data_points.extend(usaspending_points)
            except Exception as e:
                self.logger.error("Failed to collect USASpending data", error=str(e))

        self.logger.info(
            "Aircraft parts collection complete",
            total_records=len(data_points),
        )

        return data_points

    async def _collect_census_hs88_trade(
        self,
        client: httpx.AsyncClient,
        start_date: datetime,
        end_date: datetime,
    ) -> list[DataPoint]:
        """Collect Census HS Chapter 88 trade data.

        Chapter 88 covers aircraft, spacecraft, drones, and parts.

        Args:
            client: HTTP client
            start_date: Start date
            end_date: End date

        Returns:
            List of DataPoint objects
        """
        data_points: list[DataPoint] = []
        api_key = self.settings.census_api_key

        for flow in ["imports", "exports"]:
            for hs_code, config in HS88_CODES.items():
                try:
                    time_range = f"from {start_date.year}-{start_date.month:02d} to {end_date.year}-{end_date.month:02d}"

                    params: dict[str, Any] = {
                        "get": "GEN_VAL_MO,CTY_CODE,CTY_NAME",
                        "time": time_range,
                        "COMM_LVL": "HS4",
                        "I_COMMODITY": hs_code,
                    }

                    if api_key:
                        params["key"] = api_key.get_secret_value()

                    url = f"{self.CENSUS_TRADE_BASE}/{flow}/hs"

                    async with self.census_limiter:
                        response = await client.get(url, params=params)

                    if response.status_code != 200:
                        self.logger.debug(
                            "Census trade API error",
                            hs_code=hs_code,
                            flow=flow,
                            status=response.status_code,
                        )
                        continue

                    data = response.json()

                    if len(data) < 2:
                        continue

                    headers = data[0]
                    value_idx = headers.index("GEN_VAL_MO") if "GEN_VAL_MO" in headers else 0
                    time_idx = headers.index("time") if "time" in headers else -1
                    country_idx = headers.index("CTY_NAME") if "CTY_NAME" in headers else -1

                    for row in data[1:]:
                        try:
                            value_str = row[value_idx]
                            if not value_str or value_str in ("null", "N/A", ""):
                                continue

                            value = float(value_str)

                            if time_idx >= 0:
                                time_str = row[time_idx]
                                year, month = time_str.split("-")[:2]
                                timestamp = datetime(int(year), int(month), 1)
                            else:
                                timestamp = datetime.now(UTC)

                            country = row[country_idx] if country_idx >= 0 else "World"

                            dp = DataPoint(
                                source_id=self.source_id,
                                series_id=f"CENSUS_HS88_{hs_code}_{flow.upper()}",
                                timestamp=timestamp,
                                value=value,
                                unit="dollars",
                                metadata={
                                    "hs_code": hs_code,
                                    "commodity": config["name"],
                                    "category": config["category"],
                                    "flow": flow,
                                    "source": "CENSUS_TRADE",
                                    "partner_country": country,
                                },
                            )
                            data_points.append(dp)

                        except (ValueError, IndexError) as e:
                            self.logger.debug(
                                "Failed to parse Census trade record",
                                hs_code=hs_code,
                                error=str(e),
                            )
                            continue

                except Exception as e:
                    self.logger.error(
                        "Failed to fetch Census HS trade",
                        hs_code=hs_code,
                        flow=flow,
                        error=str(e),
                    )
                    continue

        self.logger.info("Census HS88 trade collection complete", records=len(data_points))
        return data_points

    async def _collect_faa_registry(
        self,
        client: httpx.AsyncClient,
    ) -> list[DataPoint]:
        """Collect FAA Aircraft Registry data.

        Downloads the daily ReleasableAircraft.zip file and parses
        registration trends, focusing on drones and UAS.

        Args:
            client: HTTP client

        Returns:
            List of DataPoint objects
        """
        data_points: list[DataPoint] = []

        try:
            async with self.faa_limiter:
                response = await client.get(
                    self.FAA_REGISTRY_URL,
                    follow_redirects=True,
                    timeout=300.0,  # Large file, increase timeout
                )

            if response.status_code != 200:
                self.logger.warning(
                    "FAA registry download failed",
                    status=response.status_code,
                )
                return data_points

            points = await self._parse_faa_registry_zip(response.content)
            data_points.extend(points)

        except Exception as e:
            self.logger.error("Failed to download FAA registry", error=str(e))

        return data_points

    async def _parse_faa_registry_zip(
        self,
        zip_content: bytes,
    ) -> list[DataPoint]:
        """Parse FAA Registry ZIP file.

        The ZIP contains several files:
        - MASTER.txt: Main aircraft registration data
        - ACFTREF.txt: Aircraft reference by make/model
        - ENGINE.txt: Engine reference data

        Args:
            zip_content: ZIP file bytes

        Returns:
            List of DataPoint objects
        """
        data_points: list[DataPoint] = []

        try:
            with zipfile.ZipFile(io.BytesIO(zip_content)) as zf:
                file_list = zf.namelist()
                self.logger.debug("FAA registry files", files=file_list)

                master_files = [f for f in file_list if "MASTER" in f.upper()]
                if master_files:
                    master_file = master_files[0]
                    with zf.open(master_file) as f:
                        try:
                            content = f.read()
                            df = pd.read_csv(
                                io.BytesIO(content),
                                encoding="latin-1",
                                low_memory=False,
                                on_bad_lines="skip",
                            )

                            points = self._summarize_faa_registrations(df)
                            data_points.extend(points)

                        except Exception as e:
                            self.logger.error("Failed to parse MASTER file", error=str(e))

        except zipfile.BadZipFile as e:
            self.logger.error("Invalid FAA registry ZIP file", error=str(e))
        except Exception as e:
            self.logger.error("Failed to parse FAA registry ZIP", error=str(e))

        return data_points

    def _summarize_faa_registrations(
        self,
        df: pd.DataFrame,
    ) -> list[DataPoint]:
        """Summarize FAA registration data into aggregated metrics.

        Args:
            df: DataFrame with MASTER file data

        Returns:
            List of DataPoint objects with summary statistics
        """
        data_points: list[DataPoint] = []
        now = datetime.now(UTC)

        try:
            df.columns = df.columns.str.upper().str.strip()

            total_count = len(df)
            dp = DataPoint(
                source_id=self.source_id,
                series_id="FAA_TOTAL_REGISTRATIONS",
                timestamp=now,
                value=float(total_count),
                unit="registrations",
                metadata={
                    "metric_type": "registration_count",
                    "source": "FAA",
                    "as_of_date": now.isoformat(),
                },
            )
            data_points.append(dp)

            type_col = next((c for c in df.columns if "TYPE" in c and "AIRCRAFT" in c), None)
            if type_col:
                type_counts = df[type_col].value_counts()
                for aircraft_type, count in type_counts.items():
                    if pd.notna(aircraft_type) and count > 100:
                        dp = DataPoint(
                            source_id=self.source_id,
                            series_id=f"FAA_TYPE_{str(aircraft_type).strip()}",
                            timestamp=now,
                            value=float(count),
                            unit="registrations",
                            metadata={
                                "metric_type": "registration_by_type",
                                "aircraft_type": str(aircraft_type).strip(),
                                "source": "FAA",
                            },
                        )
                        data_points.append(dp)

            mfr_col = next((c for c in df.columns if "MFR" in c or "MANUFACTURER" in c), None)
            if mfr_col:
                mfr_counts = df[mfr_col].value_counts().head(20)
                for mfr, count in mfr_counts.items():
                    if pd.notna(mfr):
                        dp = DataPoint(
                            source_id=self.source_id,
                            series_id=f"FAA_MFR_{str(mfr).strip()[:30]}",
                            timestamp=now,
                            value=float(count),
                            unit="registrations",
                            metadata={
                                "metric_type": "registration_by_manufacturer",
                                "manufacturer": str(mfr).strip(),
                                "source": "FAA",
                            },
                        )
                        data_points.append(dp)

            model_col = next((c for c in df.columns if "MODEL" in c), None)
            if model_col:
                drone_mask = df[model_col].astype(str).str.lower().apply(
                    lambda x: any(kw in x for kw in DRONE_KEYWORDS)
                )
                drone_count = drone_mask.sum()
                if drone_count > 0:
                    dp = DataPoint(
                        source_id=self.source_id,
                        series_id="FAA_DRONE_REGISTRATIONS",
                        timestamp=now,
                        value=float(drone_count),
                        unit="registrations",
                        metadata={
                            "metric_type": "drone_registration_count",
                            "source": "FAA",
                            "keywords_matched": DRONE_KEYWORDS[:5],
                        },
                    )
                    data_points.append(dp)

        except Exception as e:
            self.logger.error("Failed to summarize FAA registrations", error=str(e))

        self.logger.info("FAA registry summarized", records=len(data_points))
        return data_points

    async def _collect_usaspending_dod(
        self,
        client: httpx.AsyncClient,
        start_date: datetime,
        end_date: datetime,
    ) -> list[DataPoint]:
        """Collect USASpending DOD aerospace contract awards.

        Queries the USASpending API for DOD contracts related to
        aircraft, drones, and aerospace components.

        Args:
            client: HTTP client
            start_date: Start date
            end_date: End date

        Returns:
            List of DataPoint objects
        """
        data_points: list[DataPoint] = []

        for psc_prefix in AEROSPACE_PSC_CODES:
            try:
                search_body = {
                    "filters": {
                        "time_period": [
                            {
                                "start_date": start_date.strftime("%Y-%m-%d"),
                                "end_date": end_date.strftime("%Y-%m-%d"),
                            }
                        ],
                        "agencies": [
                            {"type": "funding", "tier": "toptier", "name": "Department of Defense"}
                        ],
                        "psc_codes": {"require": [[psc_prefix]]},
                    },
                    "fields": [
                        "Award ID",
                        "Recipient Name",
                        "Award Amount",
                        "Description",
                        "Start Date",
                        "End Date",
                        "Awarding Agency",
                        "Product or Service Code",
                        "NAICS Code",
                    ],
                    "page": 1,
                    "limit": 100,
                    "sort": "Award Amount",
                    "order": "desc",
                }

                async with self.usaspending_limiter:
                    response = await client.post(
                        f"{self.USASPENDING_API}/search/spending_by_award/",
                        json=search_body,
                        headers={"Content-Type": "application/json"},
                    )

                if response.status_code != 200:
                    self.logger.debug(
                        "USASpending API error",
                        psc_prefix=psc_prefix,
                        status=response.status_code,
                    )
                    continue

                data = response.json()
                results = data.get("results", [])

                for award in results:
                    try:
                        description = str(award.get("Description", "")).lower()
                        is_drone = any(kw in description for kw in DRONE_KEYWORDS)

                        amount = award.get("Award Amount")
                        if not amount:
                            continue

                        date_str = award.get("Start Date", "")
                        if date_str:
                            timestamp = datetime.strptime(date_str[:10], "%Y-%m-%d")
                        else:
                            timestamp = datetime.now(UTC)

                        award_id = str(award.get("Award ID", "unknown"))
                        recipient = str(award.get("Recipient Name", "Unknown"))

                        dp = DataPoint(
                            source_id=self.source_id,
                            series_id=f"USASPENDING_DOD_{psc_prefix}",
                            timestamp=timestamp,
                            value=float(amount),
                            unit="dollars",
                            metadata={
                                "metric_type": "contract_award",
                                "award_id": award_id,
                                "recipient": recipient,
                                "description": description[:200],
                                "psc_code": award.get("Product or Service Code"),
                                "naics_code": award.get("NAICS Code"),
                                "is_drone_related": is_drone,
                                "source": "USASPENDING",
                            },
                        )
                        data_points.append(dp)

                    except (ValueError, KeyError) as e:
                        self.logger.debug(
                            "Failed to parse USASpending award",
                            error=str(e),
                        )
                        continue

            except Exception as e:
                self.logger.error(
                    "Failed to fetch USASpending data",
                    psc_prefix=psc_prefix,
                    error=str(e),
                )
                continue

        try:
            drone_points = await self._collect_usaspending_drone_contracts(
                client, start_date, end_date
            )
            data_points.extend(drone_points)
        except Exception as e:
            self.logger.error("Failed to collect drone contracts", error=str(e))

        self.logger.info("USASpending collection complete", records=len(data_points))
        return data_points

    async def _collect_usaspending_drone_contracts(
        self,
        client: httpx.AsyncClient,
        start_date: datetime,
        end_date: datetime,
    ) -> list[DataPoint]:
        """Collect drone/UAS-specific contracts from USASpending.

        Uses keyword search to find drone-related contracts.

        Args:
            client: HTTP client
            start_date: Start date
            end_date: End date

        Returns:
            List of DataPoint objects
        """
        data_points: list[DataPoint] = []

        for keyword in ["drone", "UAS", "unmanned aircraft"]:
            try:
                search_body = {
                    "filters": {
                        "time_period": [
                            {
                                "start_date": start_date.strftime("%Y-%m-%d"),
                                "end_date": end_date.strftime("%Y-%m-%d"),
                            }
                        ],
                        "agencies": [
                            {"type": "funding", "tier": "toptier", "name": "Department of Defense"}
                        ],
                        "keywords": [keyword],
                    },
                    "fields": [
                        "Award ID",
                        "Recipient Name",
                        "Award Amount",
                        "Description",
                        "Start Date",
                    ],
                    "page": 1,
                    "limit": 50,
                    "sort": "Award Amount",
                    "order": "desc",
                }

                async with self.usaspending_limiter:
                    response = await client.post(
                        f"{self.USASPENDING_API}/search/spending_by_award/",
                        json=search_body,
                        headers={"Content-Type": "application/json"},
                    )

                if response.status_code != 200:
                    continue

                data = response.json()
                results = data.get("results", [])

                for award in results:
                    try:
                        amount = award.get("Award Amount")
                        if not amount:
                            continue

                        date_str = award.get("Start Date", "")
                        if date_str:
                            timestamp = datetime.strptime(date_str[:10], "%Y-%m-%d")
                        else:
                            timestamp = datetime.now(UTC)

                        dp = DataPoint(
                            source_id=self.source_id,
                            series_id="USASPENDING_DOD_DRONE",
                            timestamp=timestamp,
                            value=float(amount),
                            unit="dollars",
                            metadata={
                                "metric_type": "drone_contract",
                                "award_id": str(award.get("Award ID", "")),
                                "recipient": str(award.get("Recipient Name", "")),
                                "description": str(award.get("Description", ""))[:200],
                                "keyword_matched": keyword,
                                "is_drone_related": True,
                                "source": "USASPENDING",
                            },
                        )
                        data_points.append(dp)

                    except (ValueError, KeyError) as e:
                        self.logger.debug("Failed to parse drone award", error=str(e))
                        continue

            except Exception as e:
                self.logger.debug(
                    "Failed to search for drone keyword",
                    keyword=keyword,
                    error=str(e),
                )
                continue

        return data_points

    async def validate_api_key(self) -> bool:
        """Validate that data sources are accessible.

        Returns:
            True if at least one source is accessible
        """
        census_valid = False
        faa_valid = False
        usaspending_valid = False

        async with httpx.AsyncClient(timeout=30.0) as client:
            if self.settings.census_api_key:
                try:
                    params = {"get": "GEN_VAL_MO", "time": "2024-01"}
                    if self.settings.census_api_key:
                        params["key"] = self.settings.census_api_key.get_secret_value()
                    response = await client.get(
                        f"{self.CENSUS_TRADE_BASE}/exports/hs",
                        params=params,
                    )
                    census_valid = response.status_code in (200, 400)
                except Exception as e:
                    self.logger.warning("Census validation failed", error=str(e))

            try:
                response = await client.head(self.FAA_REGISTRY_URL, follow_redirects=True)
                faa_valid = response.status_code == 200
            except Exception as e:
                self.logger.warning("FAA validation failed", error=str(e))

            try:
                response = await client.get(f"{self.USASPENDING_API}/awards/last_updated/")
                usaspending_valid = response.status_code == 200
            except Exception as e:
                self.logger.warning("USASpending validation failed", error=str(e))

        return census_valid or faa_valid or usaspending_valid


def get_aircraft_parts_collector() -> AircraftPartsCollector:
    """Get an Aircraft Parts collector instance."""
    return AircraftPartsCollector()
