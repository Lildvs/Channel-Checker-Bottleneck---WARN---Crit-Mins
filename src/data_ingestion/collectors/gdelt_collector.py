"""GDELT Project collector for news sentiment and event data."""

from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import structlog

from src.data_ingestion.base_collector import (
    BaseCollector,
    DataFrequency,
    DataPoint,
)
from src.data_ingestion.rate_limiter import get_rate_limiter

logger = structlog.get_logger()

GDELT_EVENT_CODES = {
    # Economic events
    "0231": "Economic Cooperation",
    "0311": "Express intent to cooperate economically",
    "0831": "Engage in material cooperation, economic",
    # Protests/Unrest
    "140": "Protest",
    "141": "Demonstrate or rally",
    "145": "Protest violently",
    # Sanctions/Trade
    "163": "Impose embargo, boycott, or sanctions",
    "1631": "Impose economic sanctions",
    # Supply chain keywords
    "supply_chain": "Supply Chain",
    "shortage": "Shortage",
    "bottleneck": "Bottleneck",
    "logistics": "Logistics",
    "shipping": "Shipping",
    "port": "Port Congestion",
}


class GDELTCollector(BaseCollector):
    """Collector for GDELT Project data (news and events)."""

    DOC_API_URL = "https://api.gdeltproject.org/api/v2/doc/doc"
    GEO_API_URL = "https://api.gdeltproject.org/api/v2/geo/geo"

    def __init__(self):
        """Initialize GDELT collector."""
        super().__init__(name="GDELT", source_id="gdelt")
        self.rate_limiter = get_rate_limiter("gdelt")

    @property
    def frequency(self) -> DataFrequency:
        """GDELT updates every 15 minutes."""
        return DataFrequency.REALTIME

    def get_schedule(self) -> str:
        """Run every 15 minutes."""
        return "*/15 * * * *"

    def get_default_series(self) -> list[str]:
        """Get default GDELT queries."""
        return [
            "supply chain disruption",
            "shipping delay",
            "port congestion",
            "semiconductor shortage",
            "energy crisis",
            "labor shortage",
            "inflation surge",
            "commodity prices",
        ]

    async def validate_api_key(self) -> bool:
        """GDELT API is free and doesn't require a key."""
        try:
            async with httpx.AsyncClient(follow_redirects=True) as client:
                response = await client.get(
                    self.DOC_API_URL,
                    params={
                        "query": "test",
                        "mode": "artlist",
                        "maxrecords": 1,
                        "format": "json",
                    },
                )
                return response.status_code == 200
        except Exception as e:
            self.logger.error("GDELT API validation failed", error=str(e))
            return False

    async def collect(
        self,
        series_ids: list[str] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> list[DataPoint]:
        """Collect data from GDELT API.

        Args:
            series_ids: List of search queries
            start_date: Start date (defaults to last 24 hours)
            end_date: End date

        Returns:
            List of collected data points
        """
        queries = series_ids or self.get_default_series()
        all_data_points: list[DataPoint] = []

        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            for query in queries:
                try:
                    data_points = await self._collect_query(
                        client, query, start_date, end_date
                    )
                    all_data_points.extend(data_points)
                except Exception as e:
                    self.logger.error(
                        "Failed to collect GDELT query",
                        query=query,
                        error=str(e),
                    )

        return all_data_points

    async def _collect_query(
        self,
        client: httpx.AsyncClient,
        query: str,
        start_date: datetime | None,
        end_date: datetime | None,
    ) -> list[DataPoint]:
        """Collect article counts and sentiment for a query.

        Args:
            client: HTTP client
            query: Search query
            start_date: Start date
            end_date: End date

        Returns:
            List of data points
        """
        async with self.rate_limiter:
            params: dict[str, Any] = {
                "query": query,
                "mode": "timelinevol",
                "timeres": "day",
                "format": "json",
            }

            if start_date:
                params["startdatetime"] = start_date.strftime("%Y%m%d%H%M%S")
            else:
                # Default to last 30 days
                default_start = datetime.now(UTC) - timedelta(days=30)
                params["startdatetime"] = default_start.strftime("%Y%m%d%H%M%S")

            if end_date:
                params["enddatetime"] = end_date.strftime("%Y%m%d%H%M%S")

            response = await client.get(self.DOC_API_URL, params=params)

        if response.status_code != 200:
            self.logger.warning(
                "GDELT request failed",
                query=query,
                status=response.status_code,
            )
            return []

        content_type = response.headers.get("content-type", "")
        if "json" not in content_type:
            self.logger.warning(
                "GDELT returned non-JSON response",
                query=query,
                content_type=content_type,
            )
            return []

        try:
            data = response.json()
        except Exception:
            self.logger.warning("GDELT response not valid JSON", query=query)
            return []

        data_points: list[DataPoint] = []

        timeline = data.get("timeline", [])
        for entry in timeline:
            for item in entry.get("data", []):
                try:
                    date_str = item.get("date", "")
                    if len(date_str) == 8:  # YYYYMMDD
                        timestamp = datetime.strptime(date_str, "%Y%m%d")
                    elif len(date_str) == 14:  # YYYYMMDDHHMMSS
                        timestamp = datetime.strptime(date_str, "%Y%m%d%H%M%S")
                    else:
                        continue

                    value = float(item.get("value", 0))

                    series_id = f"GDELT_{query.replace(' ', '_').upper()}"

                    data_points.append(
                        DataPoint(
                            source_id=self.source_id,
                            series_id=series_id,
                            timestamp=timestamp,
                            value=value,
                            unit="article_count",
                            metadata={
                                "query": query,
                                "type": "volume",
                            },
                        )
                    )
                except (ValueError, KeyError):
                    continue

        try:
            sentiment_points = await self._collect_sentiment(
                client, query, start_date, end_date
            )
            data_points.extend(sentiment_points)
        except Exception as e:
            self.logger.debug("Failed to collect sentiment", query=query, error=str(e))

        self.logger.debug(
            "Collected GDELT query",
            query=query,
            observations=len(data_points),
        )

        return data_points

    async def _collect_sentiment(
        self,
        client: httpx.AsyncClient,
        query: str,
        start_date: datetime | None,
        end_date: datetime | None,
    ) -> list[DataPoint]:
        """Collect sentiment/tone data for a query."""
        async with self.rate_limiter:
            params: dict[str, Any] = {
                "query": query,
                "mode": "timelinetone",
                "timeres": "day",
                "format": "json",
            }

            if start_date:
                params["startdatetime"] = start_date.strftime("%Y%m%d%H%M%S")
            else:
                default_start = datetime.now(UTC) - timedelta(days=30)
                params["startdatetime"] = default_start.strftime("%Y%m%d%H%M%S")

            if end_date:
                params["enddatetime"] = end_date.strftime("%Y%m%d%H%M%S")

            response = await client.get(self.DOC_API_URL, params=params)

        if response.status_code != 200:
            return []

        content_type = response.headers.get("content-type", "")
        if "json" not in content_type:
            return []

        try:
            data = response.json()
        except Exception:
            return []

        data_points: list[DataPoint] = []

        timeline = data.get("timeline", [])
        for entry in timeline:
            for item in entry.get("data", []):
                try:
                    date_str = item.get("date", "")
                    if len(date_str) == 8:
                        timestamp = datetime.strptime(date_str, "%Y%m%d")
                    elif len(date_str) == 14:
                        timestamp = datetime.strptime(date_str, "%Y%m%d%H%M%S")
                    else:
                        continue

                    value = float(item.get("value", 0))
                    series_id = f"GDELT_{query.replace(' ', '_').upper()}_TONE"

                    data_points.append(
                        DataPoint(
                            source_id=self.source_id,
                            series_id=series_id,
                            timestamp=timestamp,
                            value=value,
                            unit="tone_score",
                            metadata={
                                "query": query,
                                "type": "sentiment",
                            },
                        )
                    )
                except (ValueError, KeyError):
                    continue

        return data_points
