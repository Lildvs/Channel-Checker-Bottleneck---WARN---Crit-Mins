"""EIA Energy Flows Collector.

Collects energy flow data from EIA API v2:
- Petroleum Inter-PADD Pipeline Flows
- Natural Gas Interstate/Regional Flows
- Refinery Utilization by PADD
- LNG Export Terminal Data
"""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
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


class EnergyFlowType(str, Enum):
    """Types of energy flow data collected."""

    PETROLEUM_PADD = "petroleum_padd"
    NATURAL_GAS = "natural_gas"
    REFINERY_UTIL = "refinery_util"
    LNG_EXPORTS = "lng_exports"


@dataclass
class EnergyFlowSeries:
    """Configuration for an energy flow series."""

    series_id: str
    flow_type: EnergyFlowType
    description: str
    unit: str
    frequency: str  # W=Weekly, M=Monthly, A=Annual


# Format: MTTMPP{destination_padd}P{source_padd}1 (crude) or 2 (products)
PETROLEUM_PADD_FLOWS = [
    EnergyFlowSeries("MTTMPP1P31", EnergyFlowType.PETROLEUM_PADD, "Crude: Gulf Coast (3) to East Coast (1)", "thousand barrels", "M"),
    EnergyFlowSeries("MTTMPP2P31", EnergyFlowType.PETROLEUM_PADD, "Crude: Gulf Coast (3) to Midwest (2)", "thousand barrels", "M"),
    EnergyFlowSeries("MTTMPP3P21", EnergyFlowType.PETROLEUM_PADD, "Crude: Midwest (2) to Gulf Coast (3)", "thousand barrels", "M"),
    EnergyFlowSeries("MTTMPP1P21", EnergyFlowType.PETROLEUM_PADD, "Crude: Midwest (2) to East Coast (1)", "thousand barrels", "M"),
    EnergyFlowSeries("MTTMPP2P41", EnergyFlowType.PETROLEUM_PADD, "Crude: Rocky Mountain (4) to Midwest (2)", "thousand barrels", "M"),
    EnergyFlowSeries("MTTMPP5P41", EnergyFlowType.PETROLEUM_PADD, "Crude: Rocky Mountain (4) to West Coast (5)", "thousand barrels", "M"),
    EnergyFlowSeries("MTTMPP2PCA1", EnergyFlowType.PETROLEUM_PADD, "Crude: Canada to Midwest (2)", "thousand barrels", "M"),
]

REFINERY_UTILIZATION = [
    EnergyFlowSeries("MOPUEUS2", EnergyFlowType.REFINERY_UTIL, "U.S. Total Refinery Utilization", "percent", "M"),
    EnergyFlowSeries("MOPUEP12", EnergyFlowType.REFINERY_UTIL, "PADD 1 (East Coast) Refinery Utilization", "percent", "M"),
    EnergyFlowSeries("MOPUEP22", EnergyFlowType.REFINERY_UTIL, "PADD 2 (Midwest) Refinery Utilization", "percent", "M"),
    EnergyFlowSeries("MOPUEP32", EnergyFlowType.REFINERY_UTIL, "PADD 3 (Gulf Coast) Refinery Utilization", "percent", "M"),
    EnergyFlowSeries("MOPUEP42", EnergyFlowType.REFINERY_UTIL, "PADD 4 (Rocky Mountain) Refinery Utilization", "percent", "M"),
    EnergyFlowSeries("MOPUEP52", EnergyFlowType.REFINERY_UTIL, "PADD 5 (West Coast) Refinery Utilization", "percent", "M"),
]

NATURAL_GAS_FLOWS = [
    EnergyFlowSeries("NA1240", EnergyFlowType.NATURAL_GAS, "U.S. Interstate Natural Gas Receipts", "million cubic feet", "A"),
    EnergyFlowSeries("NA1250", EnergyFlowType.NATURAL_GAS, "U.S. Interstate Natural Gas Deliveries", "million cubic feet", "A"),
]

LNG_EXPORT_TERMINALS = [
    EnergyFlowSeries("N9132US2", EnergyFlowType.LNG_EXPORTS, "U.S. Total LNG Exports", "million cubic feet", "M"),
    # Terminal-specific exports are available via point-of-exit data
]

ALL_ENERGY_FLOW_SERIES = (
    PETROLEUM_PADD_FLOWS
    + REFINERY_UTILIZATION
    + NATURAL_GAS_FLOWS
    + LNG_EXPORT_TERMINALS
)


class EnergyFlowsCollector(BaseCollector):
    """Collector for EIA energy flow data.

    Collects:
    - Petroleum Inter-PADD Pipeline Flows (monthly)
    - Natural Gas Interstate Flows (annual)
    - Refinery Utilization by PADD (monthly)
    - LNG Export Terminal Data (monthly)
    """

    BASE_URL = "https://api.eia.gov/v2"

    ENDPOINTS = {
        EnergyFlowType.PETROLEUM_PADD: "/petroleum/move/pipe/data",
        EnergyFlowType.NATURAL_GAS: "/natural-gas/move/ist/data",
        EnergyFlowType.REFINERY_UTIL: "/petroleum/pnp/unc/data",
        EnergyFlowType.LNG_EXPORTS: "/natural-gas/sum/lsum/data",
    }

    def __init__(self):
        """Initialize Energy Flows collector."""
        super().__init__(name="EIA Energy Flows", source_id="eia_energy_flows")
        settings = get_settings()
        self.api_key = settings.eia_api_key
        self.rate_limiter = get_rate_limiter("eia")
        self._series_map = {s.series_id: s for s in ALL_ENERGY_FLOW_SERIES}

    @property
    def frequency(self) -> DataFrequency:
        """Mixed frequencies - primarily monthly."""
        return DataFrequency.MONTHLY

    def get_schedule(self) -> str:
        """Run monthly on the 5th at 10:00 AM UTC (after EIA releases)."""
        return "0 10 5 * *"

    def get_default_series(self) -> list[str]:
        """Get default series to collect."""
        return [s.series_id for s in ALL_ENERGY_FLOW_SERIES]

    async def validate_api_key(self) -> bool:
        """Validate the EIA API key."""
        if not self.api_key:
            self.logger.warning("EIA API key not configured")
            return False

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(
                    f"{self.BASE_URL}/petroleum/move/pipe/data/",
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
        """Collect energy flow data from EIA API.

        Args:
            series_ids: List of series IDs to collect
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

        # Group series by flow type for efficient API calls
        series_by_type: dict[EnergyFlowType, list[str]] = {}
        for series_id in series_to_collect:
            if series_id in self._series_map:
                flow_type = self._series_map[series_id].flow_type
                if flow_type not in series_by_type:
                    series_by_type[flow_type] = []
                series_by_type[flow_type].append(series_id)

        async with httpx.AsyncClient(timeout=60.0) as client:
            for flow_type, series_list in series_by_type.items():
                try:
                    data_points = await self._collect_flow_type(
                        client, flow_type, series_list, start_date, end_date
                    )
                    all_data_points.extend(data_points)
                except Exception as e:
                    self.logger.error(
                        "Failed to collect flow type",
                        flow_type=flow_type.value,
                        error=str(e),
                    )

        return all_data_points

    async def _collect_flow_type(
        self,
        client: httpx.AsyncClient,
        flow_type: EnergyFlowType,
        series_ids: list[str],
        start_date: datetime | None,
        end_date: datetime | None,
    ) -> list[DataPoint]:
        """Collect data for a specific flow type.

        Args:
            client: HTTP client
            flow_type: Type of energy flow
            series_ids: Series IDs to collect
            start_date: Start date
            end_date: End date

        Returns:
            List of data points
        """
        endpoint = self.ENDPOINTS[flow_type]
        data_points: list[DataPoint] = []

        async with self.rate_limiter:
            params: dict[str, Any] = {
                "api_key": self.api_key.get_secret_value(),
                "data[]": "value",
                "sort[0][column]": "period",
                "sort[0][direction]": "desc",
                "length": 1000,
            }

            if start_date:
                params["start"] = start_date.strftime("%Y-%m")
            if end_date:
                params["end"] = end_date.strftime("%Y-%m")

            response = await client.get(
                f"{self.BASE_URL}{endpoint}",
                params=params,
            )

        if response.status_code != 200:
            self.logger.warning(
                "EIA request failed",
                flow_type=flow_type.value,
                status=response.status_code,
                response_text=response.text[:500] if response.text else None,
            )
            return []

        data = response.json()
        observations = data.get("response", {}).get("data", [])

        for obs in observations:
            data_point = self._parse_observation(obs, flow_type, series_ids)
            if data_point:
                data_points.append(data_point)

        self.logger.debug(
            "Collected energy flow data",
            flow_type=flow_type.value,
            observations=len(data_points),
        )

        return data_points

    def _parse_observation(
        self,
        obs: dict[str, Any],
        flow_type: EnergyFlowType,
        target_series: list[str],
    ) -> DataPoint | None:
        """Parse a single observation from EIA response.

        Args:
            obs: Observation dict from API
            flow_type: Type of energy flow
            target_series: Series IDs we're looking for

        Returns:
            DataPoint or None if parsing fails
        """
        value = obs.get("value")
        if value is None:
            return None

        try:
            value = float(value)
        except (ValueError, TypeError):
            return None

        # Parse period to timestamp
        period = obs.get("period", "")
        try:
            if len(period) == 10:  # YYYY-MM-DD
                timestamp = datetime.strptime(period, "%Y-%m-%d")
            elif len(period) == 7:  # YYYY-MM
                timestamp = datetime.strptime(period, "%Y-%m")
            elif len(period) == 4:  # YYYY
                timestamp = datetime.strptime(period, "%Y")
            else:
                return None
        except ValueError:
            return None

        series_id = self._build_series_id(obs, flow_type)
        if series_id not in target_series and series_id not in self._series_map:
            # Still collect data even if not in our predefined list
            pass

        series_info = self._series_map.get(series_id)
        unit = series_info.unit if series_info else obs.get("units")
        description = series_info.description if series_info else None

        return DataPoint(
            source_id=self.source_id,
            series_id=f"EIA_{flow_type.value.upper()}_{series_id}",
            timestamp=timestamp,
            value=value,
            unit=unit,
            metadata={
                "flow_type": flow_type.value,
                "description": description,
                "series": obs.get("series"),
                "duoarea": obs.get("duoarea"),
                "product": obs.get("product"),
                "process": obs.get("process"),
                "raw_series_id": series_id,
            },
        )

    def _build_series_id(self, obs: dict[str, Any], flow_type: EnergyFlowType) -> str:
        """Build a series ID from observation data.

        Args:
            obs: Observation dict
            flow_type: Type of energy flow

        Returns:
            Constructed series ID
        """
        if obs.get("series"):
            return obs["series"]

        if flow_type == EnergyFlowType.PETROLEUM_PADD:
            # Format: product_duoarea
            product = obs.get("product", "CRUDE")
            duoarea = obs.get("duoarea", "")
            return f"{product}_{duoarea}"

        elif flow_type == EnergyFlowType.REFINERY_UTIL:
            # Format: process_duoarea
            process = obs.get("process", "OPU")
            duoarea = obs.get("duoarea", "NUS")
            return f"{process}_{duoarea}"

        elif flow_type == EnergyFlowType.NATURAL_GAS:
            # Format: series
            return obs.get("series", "NATGAS_FLOW")

        elif flow_type == EnergyFlowType.LNG_EXPORTS:
            # Format: series
            return obs.get("series", "LNG_EXPORTS")

        return "UNKNOWN"

    async def collect_petroleum_padd_flows(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> list[DataPoint]:
        """Collect only petroleum PADD pipeline flows.

        Args:
            start_date: Start date
            end_date: End date

        Returns:
            List of petroleum flow data points
        """
        series_ids = [s.series_id for s in PETROLEUM_PADD_FLOWS]
        return await self.collect(series_ids, start_date, end_date)

    async def collect_refinery_utilization(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> list[DataPoint]:
        """Collect only refinery utilization data.

        Args:
            start_date: Start date
            end_date: End date

        Returns:
            List of refinery utilization data points
        """
        series_ids = [s.series_id for s in REFINERY_UTILIZATION]
        return await self.collect(series_ids, start_date, end_date)

    async def collect_natural_gas_flows(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> list[DataPoint]:
        """Collect only natural gas interstate flows.

        Args:
            start_date: Start date
            end_date: End date

        Returns:
            List of natural gas flow data points
        """
        series_ids = [s.series_id for s in NATURAL_GAS_FLOWS]
        return await self.collect(series_ids, start_date, end_date)

    async def collect_lng_exports(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> list[DataPoint]:
        """Collect only LNG export data.

        Args:
            start_date: Start date
            end_date: End date

        Returns:
            List of LNG export data points
        """
        series_ids = [s.series_id for s in LNG_EXPORT_TERMINALS]
        return await self.collect(series_ids, start_date, end_date)


_collector: EnergyFlowsCollector | None = None


def get_energy_flows_collector() -> EnergyFlowsCollector:
    """Get the EnergyFlowsCollector singleton instance.

    Returns:
        EnergyFlowsCollector instance
    """
    global _collector
    if _collector is None:
        _collector = EnergyFlowsCollector()
    return _collector
