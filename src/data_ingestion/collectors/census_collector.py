"""Census Bureau collector for economic indicators."""

from datetime import UTC, datetime
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


class CensusCollector(BaseCollector):
    """Collector for Census Bureau economic data."""

    BASE_URL = "https://api.census.gov/data"

    def __init__(self):
        """Initialize Census collector."""
        super().__init__(name="Census", source_id="census")
        settings = get_settings()
        self.api_key = settings.census_api_key
        self._api_key_validated = False
        self.rate_limiter = get_rate_limiter("census")

    @property
    def frequency(self) -> DataFrequency:
        """Census releases data monthly."""
        return DataFrequency.MONTHLY

    def get_schedule(self) -> str:
        """Run on the 15th of each month at 8 AM ET (13:00 UTC)."""
        return "0 13 15 * *"

    def get_default_series(self) -> list[str]:
        """Get default Census datasets to collect."""
        return [
            "timeseries/eits/marts",  # Monthly Retail Trade
            "timeseries/eits/advm3",  # Advance Durable Goods
            "timeseries/eits/mtis",   # Manufacturing Trade Inventories
        ]

    async def validate_api_key(self) -> bool:
        """Validate the Census API key (Census works without a key)."""
        return True

    def _add_api_key(self, params: dict[str, Any]) -> None:
        """Add the API key to params only if it has been validated."""
        if self.api_key and self._api_key_validated:
            params["key"] = self.api_key.get_secret_value()

    async def _validate_key_once(self, client: httpx.AsyncClient) -> None:
        """Validate the API key once per collector lifetime."""
        if self._api_key_validated or not self.api_key:
            return

        try:
            response = await client.get(
                f"{self.BASE_URL}/timeseries/eits/marts",
                params={
                    "get": "cell_value,category_code",
                    "time": "2024-01",
                    "data_type_code": "SM",
                    "seasonally_adj": "yes",
                    "category_code": "44X72",
                    "key": self.api_key.get_secret_value(),
                },
            )
            ct = response.headers.get("content-type", "")
            if "application/json" in ct:
                self._api_key_validated = True
                self.logger.info("Census API key validated successfully")
            else:
                self.logger.warning(
                    "Census API key is invalid -- continuing without key",
                )
                self.api_key = None
        except Exception as exc:
            self.logger.warning("Census key validation error", error=str(exc))
            self.api_key = None

    async def collect(
        self,
        series_ids: list[str] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> list[DataPoint]:
        """Collect data from Census API.

        Args:
            series_ids: List of Census dataset paths to collect
            start_date: Start date for observations
            end_date: End date for observations

        Returns:
            List of collected data points
        """
        datasets = series_ids or self.get_default_series()
        all_data_points: list[DataPoint] = []

        async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
            await self._validate_key_once(client)
            try:
                retail_data = await self._collect_retail_trade(
                    client, start_date, end_date
                )
                all_data_points.extend(retail_data)
            except Exception as e:
                self.logger.error("Failed to collect retail trade", error=str(e))

            try:
                housing_data = await self._collect_housing(
                    client, start_date, end_date
                )
                all_data_points.extend(housing_data)
            except Exception as e:
                self.logger.error("Failed to collect housing data", error=str(e))

            try:
                trade_data = await self._collect_trade(
                    client, start_date, end_date
                )
                all_data_points.extend(trade_data)
            except Exception as e:
                self.logger.error("Failed to collect trade data", error=str(e))

        return all_data_points

    async def _collect_retail_trade(
        self,
        client: httpx.AsyncClient,
        start_date: datetime | None,
        end_date: datetime | None,
    ) -> list[DataPoint]:
        """Collect Monthly Retail Trade Survey data."""
        async with self.rate_limiter:
            params: dict[str, Any] = {
                "get": "cell_value,category_code",
                "data_type_code": "SM",
                "seasonally_adj": "yes",
                "category_code": "44X72",
            }
            self._add_api_key(params)

            if start_date:
                params["time"] = f"from {start_date.strftime('%Y-%m')}"
            else:
                params["time"] = "from 2023-01"

            response = await client.get(
                f"{self.BASE_URL}/timeseries/eits/marts",
                params=params,
            )

        if response.status_code != 200:
            self.logger.warning(
                "Census retail request failed",
                status=response.status_code,
                body=response.text[:300],
            )
            return []

        try:
            data = response.json()
        except Exception:
            self.logger.warning(
                "Census retail returned non-JSON response",
                content_type=response.headers.get("content-type", ""),
            )
            return []
        data_points: list[DataPoint] = []

        if len(data) < 2:
            return []

        headers = data[0]
        value_idx = headers.index("cell_value") if "cell_value" in headers else 0
        time_idx = headers.index("time") if "time" in headers else 1
        cat_idx = headers.index("category_code") if "category_code" in headers else 2

        for row in data[1:]:
            try:
                value = float(row[value_idx])
                time_str = row[time_idx]
                category = row[cat_idx]

                timestamp = datetime.strptime(time_str, "%Y-%m").replace(tzinfo=UTC)

                data_points.append(
                    DataPoint(
                        source_id=self.source_id,
                        series_id=f"RETAIL_{category}",
                        timestamp=timestamp,
                        value=value,
                        unit="millions_of_dollars",
                        metadata={"category_code": category},
                    )
                )
            except (ValueError, IndexError):
                continue

        self.logger.debug("Collected retail trade data", count=len(data_points))
        return data_points

    async def _collect_housing(
        self,
        client: httpx.AsyncClient,
        start_date: datetime | None,
        end_date: datetime | None,
    ) -> list[DataPoint]:
        """Collect New Residential Construction data."""
        async with self.rate_limiter:
            params: dict[str, Any] = {
                "get": "cell_value,category_code",
                "data_type_code": "TOTAL",
                "seasonally_adj": "yes",
                "category_code": "APERMITS",
                "time_slot_id": "0",
            }
            self._add_api_key(params)

            if start_date:
                params["time"] = f"from {start_date.strftime('%Y-%m')}"
            else:
                params["time"] = "from 2023-01"

            response = await client.get(
                f"{self.BASE_URL}/timeseries/eits/resconst",
                params=params,
            )

        if response.status_code != 200:
            self.logger.warning(
                "Census housing request failed",
                status=response.status_code,
                body=response.text[:300],
            )
            return []

        try:
            data = response.json()
        except Exception:
            self.logger.warning("Census housing returned non-JSON response")
            return []
        data_points: list[DataPoint] = []

        if len(data) < 2:
            return []

        headers = data[0]
        value_idx = headers.index("cell_value") if "cell_value" in headers else 0
        time_idx = headers.index("time") if "time" in headers else 1

        for row in data[1:]:
            try:
                value = float(row[value_idx])
                time_str = row[time_idx]
                timestamp = datetime.strptime(time_str, "%Y-%m").replace(tzinfo=UTC)

                data_points.append(
                    DataPoint(
                        source_id=self.source_id,
                        series_id="HOUSING_STARTS",
                        timestamp=timestamp,
                        value=value,
                        unit="thousands_of_units",
                    )
                )
            except (ValueError, IndexError):
                continue

        self.logger.debug("Collected housing data", count=len(data_points))
        return data_points

    TRADE_PARTNERS: dict[str, str] = {
        "5700": "CHINA",
        "2010": "MEXICO",
        "1220": "CANADA",
        "5880": "JAPAN",
        "4280": "GERMANY",
    }

    async def _collect_trade(
        self,
        client: httpx.AsyncClient,
        start_date: datetime | None,
        end_date: datetime | None,
    ) -> list[DataPoint]:
        """Collect International Trade data for key trading partners."""
        data_points: list[DataPoint] = []

        for cty_code, cty_label in self.TRADE_PARTNERS.items():
            async with self.rate_limiter:
                params: dict[str, Any] = {
                    "get": "CTY_CODE,CTY_NAME,GEN_VAL_MO",
                    "CTY_CODE": cty_code,
                }
                self._add_api_key(params)

                if start_date:
                    params["time"] = f"from {start_date.strftime('%Y-%m')}"
                else:
                    params["time"] = "from 2023-01"

                response = await client.get(
                    f"{self.BASE_URL}/timeseries/intltrade/imports/hs",
                    params=params,
                )

            if response.status_code == 204:
                continue
            if response.status_code != 200:
                self.logger.warning(
                    "Census trade request failed",
                    cty_code=cty_code,
                    status=response.status_code,
                )
                continue

            try:
                data = response.json()
            except Exception:
                self.logger.warning(
                    "Census trade returned non-JSON", cty_code=cty_code,
                )
                continue

            if len(data) < 2:
                continue

            headers = data[0]

            for row in data[1:]:
                try:
                    row_dict = dict(zip(headers, row))
                    value = float(row_dict.get("GEN_VAL_MO", 0))
                    time_str = row_dict.get("time", "")
                    timestamp = datetime.strptime(time_str, "%Y-%m").replace(
                        tzinfo=UTC,
                    )

                    data_points.append(
                        DataPoint(
                            source_id=self.source_id,
                            series_id=f"IMPORTS_{cty_label}",
                            timestamp=timestamp,
                            value=value,
                            unit="dollars",
                            metadata={"country_code": cty_code},
                        )
                    )
                except (ValueError, KeyError):
                    continue

        self.logger.debug("Collected trade data", count=len(data_points))
        return data_points
