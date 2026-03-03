"""NFIB Small Business Optimism Index collector.

Collects the NFIB Small Business Optimism Index directly from the NFIB
SBET DreamFactory REST API.  No API key required; authentication is via
the ``X-DreamFactory-Application-Name: sbet`` header.

Data available monthly since 1974.

The NFIB server omits the Sectigo intermediate certificate from its TLS
handshake.  We ship the intermediate in ``certs/nfib_intermediate.pem``
and pass it to httpx so SSL verification works without disabling it.
"""

import json
import ssl
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import httpx
import structlog

from src.data_ingestion.base_collector import (
    BaseCollector,
    DataFrequency,
    DataPoint,
)
from src.data_ingestion.rate_limiter import get_rate_limiter

logger = structlog.get_logger()

_CERT_DIR = Path(__file__).resolve().parents[3] / "certs"
_NFIB_INTERMEDIATE = _CERT_DIR / "nfib_intermediate.pem"


def _build_nfib_ssl_context() -> ssl.SSLContext | bool:
    """Build an SSL context that trusts the NFIB intermediate cert.

    Falls back to default verification if the bundled cert is missing.
    """
    if _NFIB_INTERMEDIATE.is_file():
        ctx = ssl.create_default_context()
        ctx.load_verify_locations(cafile=str(_NFIB_INTERMEDIATE))
        return ctx
    return True


class NFIBCollector(BaseCollector):
    """Collector for the NFIB Small Business Optimism Index.

    Uses the NFIB SBET DreamFactory REST API.  The stored procedure is
    ``getIndicators2`` and the DreamFactory app name is passed as an
    HTTP header, **not** in the request body.  The API expects
    ``application/x-www-form-urlencoded`` data with the ``params`` key
    serialised as a nested array (jQuery ``$.param`` style).
    """

    BASE_URL = "https://api.nfib-sbet.org/rest/sbetdb/_proc"

    INDICATORS = [
        "OPT_INDEX",
    ]

    DREAMFACTORY_HEADERS = {
        "X-DreamFactory-Application-Name": "sbet",
        "Accept": "application/json",
    }

    def __init__(self) -> None:
        super().__init__(name="NFIB", source_id="nfib")
        self.rate_limiter = get_rate_limiter("nfib")

    @property
    def frequency(self) -> DataFrequency:
        return DataFrequency.MONTHLY

    def get_schedule(self) -> str:
        return "0 14 15 * *"

    def get_default_series(self) -> list[str]:
        return ["NFIB_OPT_INDEX"]

    async def validate_api_key(self) -> bool:
        try:
            ssl_ctx = _build_nfib_ssl_context()
            async with httpx.AsyncClient(timeout=15.0, verify=ssl_ctx) as client:
                response = await client.get(
                    "https://api.nfib-sbet.org/rest/sbetdb/sbet_config",
                    headers=self.DREAMFACTORY_HEADERS,
                )
                return response.status_code == 200
        except Exception as e:
            self.logger.error("NFIB API validation failed", error=str(e))
            return False

    async def collect(
        self,
        series_ids: list[str] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> list[DataPoint]:
        now = datetime.now(UTC)

        if start_date is None:
            start_date = datetime(now.year - 2, 1, 1, tzinfo=UTC)
        if end_date is None:
            end_date = now

        data_points: list[DataPoint] = []
        ssl_ctx = _build_nfib_ssl_context()

        async with httpx.AsyncClient(
            timeout=30.0, verify=ssl_ctx, headers=self.DREAMFACTORY_HEADERS,
        ) as client:
            for indicator in self.INDICATORS:
                try:
                    points = await self._fetch_indicator(
                        client, indicator, start_date, end_date,
                    )
                    data_points.extend(points)
                except Exception as e:
                    self.logger.error(
                        "Failed to collect NFIB indicator",
                        indicator=indicator,
                        error=str(e),
                    )

        return data_points

    @staticmethod
    def _encode_params(params: list[dict[str, Any]]) -> str:
        """Encode stored-procedure params in the nested form format that
        jQuery ``$.param`` produces and that DreamFactory expects.

        Output example::

            params%5B0%5D%5Bname%5D=minYear&params%5B0%5D%5B...%5D=...
        """
        flat: dict[str, str] = {}
        for idx, p in enumerate(params):
            for key, val in p.items():
                flat[f"params[{idx}][{key}]"] = str(val)
        return urlencode(flat)

    async def _fetch_indicator(
        self,
        client: httpx.AsyncClient,
        indicator: str,
        start_date: datetime,
        end_date: datetime,
    ) -> list[DataPoint]:
        params = [
            {"name": "minYear", "param_type": "IN", "value": start_date.year},
            {"name": "minMonth", "param_type": "IN", "value": 1},
            {"name": "maxYear", "param_type": "IN", "value": end_date.year},
            {"name": "maxMonth", "param_type": "IN", "value": 12},
            {"name": "indicator", "param_type": "IN", "value": indicator},
        ]

        body = self._encode_params(params)

        async with self.rate_limiter:
            response = await client.post(
                f"{self.BASE_URL}/getIndicators2",
                content=body,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            response.raise_for_status()

        raw = response.json()
        data_points: list[DataPoint] = []

        records = raw if isinstance(raw, list) else raw.get("record", [])

        for record in records:
            try:
                monthyear = record.get("monthyear", "")
                parts = str(monthyear).split("/")
                if len(parts) < 2:
                    continue
                year = int(parts[0])
                month = int(parts[1])

                value_raw = record.get(indicator)
                if value_raw is None:
                    continue

                value = float(value_raw)
                timestamp = datetime(year, month, 1, tzinfo=UTC)

                if timestamp < start_date or timestamp > end_date:
                    continue

                data_points.append(
                    DataPoint(
                        source_id=self.source_id,
                        series_id=f"NFIB_{indicator}",
                        timestamp=timestamp,
                        value=value,
                        unit="index",
                        metadata={
                            "indicator": indicator,
                            "year": year,
                            "month": month,
                        },
                    )
                )
            except (ValueError, TypeError) as e:
                self.logger.debug(
                    "Skipping malformed NFIB record",
                    record=record,
                    error=str(e),
                )

        self.logger.info(
            "Collected NFIB data",
            indicator=indicator,
            observations=len(data_points),
        )

        return data_points
